package lnd

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/ecdsa"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/lightningnetwork/lnd/chainreg"
	"github.com/lightningnetwork/lnd/clock"
	"github.com/lightningnetwork/lnd/invoices"
	"github.com/lightningnetwork/lnd/keychain"
	"github.com/lightningnetwork/lnd/lncfg"
	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lnrpc/bridgerpc"
	"github.com/lightningnetwork/lnd/lnrpc/routerrpc"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/lightningnetwork/lnd/routing"
	"github.com/lightningnetwork/lnd/routing/route"
	"github.com/lightningnetwork/lnd/tor"
	"github.com/lightningnetwork/lnd/zpay32"
	"github.com/stretchr/testify/require"
)

// A node that has never made an invoice says so with its own error, and the
// bridge must read that as "no such invoice" too, or a new bridge refuses its
// first swap.
func TestBridgeInvoiceAbsent(t *testing.T) {
	require.True(t, bridgeInvoiceAbsent(invoices.ErrInvoiceNotFound))
	require.True(t, bridgeInvoiceAbsent(invoices.ErrNoInvoicesCreated))
	require.True(t, bridgeInvoiceAbsent(
		fmt.Errorf("wrapped: %w", invoices.ErrNoInvoicesCreated)))
	require.False(t, bridgeInvoiceAbsent(errors.New("database is closed")))
	require.False(t, bridgeInvoiceAbsent(nil))
}

// A payment the router refuses while building it, before it writes a payment
// record, never left: the bridge must hear ErrPaymentNotSent so the payer is
// refunded, rather than in flight with nothing for a lookup to find. A CLTV
// limit that does not exceed the invoice's final delta plus lnd's block
// padding is one such refusal, and an invoice can ask for a final delta that
// small.
func TestBridgePayRefusedBeforeARecordIsNotSent(t *testing.T) {
	key, err := btcec.NewPrivateKey()
	require.NoError(t, err)

	var hash, addr [32]byte
	hash[0], addr[0] = 1, 2
	invoice, err := zpay32.NewInvoice(
		&chaincfg.RegressionNetParams, hash, time.Now(),
		zpay32.Amount(lnwire.MilliSatoshi(100_000)),
		zpay32.Description("bridge payout"),
		zpay32.Destination(key.PubKey()),
		zpay32.CLTVExpiry(2),
		zpay32.PaymentAddr(addr),
		zpay32.Features(lnwire.NewFeatureVector(
			lnwire.NewRawFeatureVector(
				lnwire.Blake2bRequired,
				lnwire.TLVOnionPayloadRequired,
				lnwire.PaymentAddrRequired,
			),
			lnwire.Features,
		)),
	)
	require.NoError(t, err)
	payReq, err := invoice.Encode(zpay32.MessageSigner{
		SignCompact: func(msg []byte) ([]byte, error) {
			return ecdsa.SignCompact(
				key, chainhash.HashB(msg), true,
			), nil
		},
	})
	require.NoError(t, err)

	backend := &routerrpc.RouterBackend{
		MaxTotalTimelock:      2016,
		ActiveNetParams:       &chaincfg.RegressionNetParams,
		DefaultFinalCltvDelta: 40,
		Clock:                 clock.NewDefaultClock(),
		ShouldSetExpAccountability: func() bool {
			return false
		},
	}

	// The limit is refused while the intent is built, so the router is
	// never reached: the server needs nothing else to answer.
	_, err = (&server{}).payBridgeInvoice(
		context.Background(), backend, bridgerpc.PayRequest{
			Invoice: payReq, CLTVLimit: 4, Timeout: time.Minute,
		},
	)
	require.ErrorIs(t, err, bridgerpc.ErrPaymentNotSent)
	require.ErrorContains(t, err, "cltv limit")
}

// The bridge's market reads go through Tor whenever the node uses it, even
// when the node itself dials clearnet targets directly: an exchange polled
// all day would otherwise see the bridge's address with every poll.
func TestBridgeNetGoesThroughTorForEveryTarget(t *testing.T) {
	clearNet := &tor.ClearNet{}

	require.Equal(t, tor.Net(clearNet), bridgeNet(nil, clearNet))
	require.Equal(t, tor.Net(clearNet), bridgeNet(&lncfg.Tor{}, clearNet))

	onlyTor := &tor.ProxyNet{SOCKS: "127.0.0.1:9050"}
	require.Equal(t, tor.Net(onlyTor), bridgeNet(&lncfg.Tor{
		Active: true, SOCKS: "127.0.0.1:9050",
	}, onlyTor))

	hybrid := &tor.ProxyNet{
		SOCKS: "127.0.0.1:9050", SkipProxyForClearNetTargets: true,
	}
	got := bridgeNet(&lncfg.Tor{
		Active: true, SOCKS: "127.0.0.1:9050", DNS: "dns:53",
		StreamIsolation: true, SkipProxyForClearNetTargets: true,
	}, hybrid)
	require.Equal(t, &tor.ProxyNet{
		SOCKS: "127.0.0.1:9050", DNS: "dns:53", StreamIsolation: true,
	}, got)
}

// A bridge that stops while its payment is held by the payee must not wait for
// the HTLC: once the router has recorded the payment, the router finishes it
// and the bridge's resume looks it up. Before that, its answer is still
// needed, since a refusal there is a payment that never left.
func TestBridgePaymentLeftToTheRouterOnStop(t *testing.T) {
	const poll = time.Millisecond

	t.Run("an answer before the stop is returned", func(t *testing.T) {
		got := awaitBridgePayment(context.Background(),
			func() bridgeSendResult {
				return bridgeSendResult{preimage: [32]byte{1}}
			}, func() bool { return true }, poll,
		)
		require.False(t, got.left)
		require.Equal(t, [32]byte{1}, got.preimage)
	})

	t.Run("a recorded payment held by the payee", func(t *testing.T) {
		held := make(chan struct{})
		defer close(held)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		done := make(chan bridgeSendResult, 1)
		go func() {
			done <- awaitBridgePayment(ctx, func() bridgeSendResult {
				<-held

				return bridgeSendResult{}
			}, func() bool { return true }, poll)
		}()

		select {
		case got := <-done:
			require.True(t, got.left)
		case <-time.After(10 * time.Second):
			t.Fatal("the stop waited for a held HTLC")
		}
	})

	t.Run("a payment refused before it was recorded", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		refused := errors.New("refused")
		release := make(chan struct{})
		var asks atomic.Int32
		go func() {
			start := time.Now()
			for asks.Load() < 3 &&
				time.Since(start) < 10*time.Second {

				time.Sleep(poll)
			}
			close(release)
		}()

		got := awaitBridgePayment(ctx, func() bridgeSendResult {
			<-release

			return bridgeSendResult{err: refused}
		}, func() bool {
			asks.Add(1)

			return false
		}, poll)
		require.False(t, got.left)
		require.ErrorIs(t, got.err, refused)
	})

	t.Run("a payment recorded after the stop", func(t *testing.T) {
		held := make(chan struct{})
		defer close(held)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		var asks atomic.Int32
		done := make(chan bridgeSendResult, 1)
		go func() {
			done <- awaitBridgePayment(ctx, func() bridgeSendResult {
				<-held

				return bridgeSendResult{}
			}, func() bool {
				return asks.Add(1) > 3
			}, poll)
		}()

		select {
		case got := <-done:
			require.True(t, got.left)
		case <-time.After(10 * time.Second):
			t.Fatal("the stop waited for a held HTLC")
		}
	})
}

// The bridge searches for routes with this node's own router, through the
// same QueryRoutes the RPC server answers with.
func TestBridgeDepsSearchRoutesWithTheRouter(t *testing.T) {
	key, err := btcec.NewPrivateKey()
	require.NoError(t, err)

	s := &server{
		cfg: &Config{
			Invoices:        &lncfg.Invoices{},
			ActiveNetParams: chainreg.BitcoinRegTestNetParams,
			Tor:             &lncfg.Tor{},
			net:             &tor.ClearNet{},
		},
		identityECDH: &keychain.PrivKeyECDH{PrivKey: key},
	}

	searched := false
	backend := &routerrpc.RouterBackend{
		SelfNode:              route.NewVertex(key.PubKey()),
		MaxTotalTimelock:      2016,
		DefaultFinalCltvDelta: 40,
		ActiveNetParams:       &chaincfg.RegressionNetParams,
		FindRoute: func(*routing.RouteRequest) (*route.Route, float64,
			error) {

			searched = true

			return nil, 0, errors.New("unable to find a path to " +
				"destination")
		},
	}

	deps := s.bridgeDeps(backend)
	require.NotNil(t, deps.QueryRoutes)

	dest, err := btcec.NewPrivateKey()
	require.NoError(t, err)
	_, err = deps.QueryRoutes(context.Background(),
		&lnrpc.QueryRoutesRequest{
			PubKey: hex.EncodeToString(
				dest.PubKey().SerializeCompressed(),
			),
			AmtMsat: 100_000,
		})
	require.ErrorContains(t, err, "unable to find a path")
	require.True(t, searched)
}

// The in-process payment's timeout is the bridge's, in whole seconds rounded
// up, as the router builds the intent from it: the bridge cuts it to the
// outgoing invoice's last seconds, and truncating 10.3 s to 10, or 0.3 s to
// none at all, would undo that.
func TestBridgePaymentTimeoutReachesTheIntent(t *testing.T) {
	key, err := btcec.NewPrivateKey()
	require.NoError(t, err)

	var hash, addr [32]byte
	hash[0], addr[0] = 1, 2
	invoice, err := zpay32.NewInvoice(
		&chaincfg.RegressionNetParams, hash, time.Now(),
		zpay32.Amount(lnwire.MilliSatoshi(100_000)),
		zpay32.Description("bridge payout"),
		zpay32.Destination(key.PubKey()),
		zpay32.CLTVExpiry(18),
		zpay32.PaymentAddr(addr),
		zpay32.Features(lnwire.NewFeatureVector(
			lnwire.NewRawFeatureVector(
				lnwire.Blake2bRequired,
				lnwire.TLVOnionPayloadRequired,
				lnwire.PaymentAddrRequired,
			),
			lnwire.Features,
		)),
	)
	require.NoError(t, err)
	payReq, err := invoice.Encode(zpay32.MessageSigner{
		SignCompact: func(msg []byte) ([]byte, error) {
			return ecdsa.SignCompact(
				key, chainhash.HashB(msg), true,
			), nil
		},
	})
	require.NoError(t, err)

	backend := &routerrpc.RouterBackend{
		MaxTotalTimelock:      2016,
		ActiveNetParams:       &chaincfg.RegressionNetParams,
		DefaultFinalCltvDelta: 40,
		Clock:                 clock.NewDefaultClock(),
		ShouldSetExpAccountability: func() bool {
			return false
		},
	}

	for _, c := range []struct {
		timeout, want time.Duration
	}{
		{11 * time.Second, 11 * time.Second},
		{10*time.Second + 300*time.Millisecond, 11 * time.Second},
		{300 * time.Millisecond, time.Second},
		{2 * time.Minute, 2 * time.Minute},
	} {
		intent, err := backend.ExtractIntent(bridgeSendRequest(
			bridgerpc.PayRequest{
				Invoice: payReq, CLTVLimit: 200,
				MaxFeeMsat: 1000, Timeout: c.timeout,
			},
		))
		require.NoError(t, err)
		require.Equal(t, c.want, intent.PayAttemptTimeout, "%v",
			c.timeout)
		require.EqualValues(t, 200, intent.CltvLimit)
		require.EqualValues(t, 1000, intent.FeeLimit)
	}
}
