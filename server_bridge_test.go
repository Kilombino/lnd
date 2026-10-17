package lnd

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/ecdsa"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/lightningnetwork/lnd/clock"
	"github.com/lightningnetwork/lnd/invoices"
	"github.com/lightningnetwork/lnd/lnrpc/bridgerpc"
	"github.com/lightningnetwork/lnd/lnrpc/routerrpc"
	"github.com/lightningnetwork/lnd/lnwire"
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
