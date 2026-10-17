package lnd

import (
	"context"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/lightningnetwork/lnd/invoices"
	"github.com/lightningnetwork/lnd/keychain"
	"github.com/lightningnetwork/lnd/lnrpc/bridgerpc"
	"github.com/lightningnetwork/lnd/lnrpc/invoicesrpc"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/lightningnetwork/lnd/netann"
	"github.com/lightningnetwork/lnd/zpay32"
	"github.com/stretchr/testify/require"
)

// bridgeInvoiceConfig is the least the bridge's hold invoice path needs to
// encode an invoice: a signer, a network and the node's invoice features. The
// invoice it is asked to store is kept for the test.
func bridgeInvoiceConfig(t *testing.T,
	stored **invoices.Invoice) *invoicesrpc.AddInvoiceConfig {

	t.Helper()
	priv, err := btcec.NewPrivateKey()
	require.NoError(t, err)

	return &invoicesrpc.AddInvoiceConfig{
		AddInvoice: func(_ context.Context, inv *invoices.Invoice,
			_ lntypes.Hash) (uint64, error) {

			*stored = inv
			return 1, nil
		},
		NodeSigner: netann.NewNodeSigner(
			keychain.NewPrivKeyMessageSigner(
				priv, keychain.KeyLocator{},
			),
		),
		ChainParams:       &chaincfg.MainNetParams,
		DefaultCLTVExpiry: 40,
		GenInvoiceFeatures: func() *lnwire.FeatureVector {
			raw := lnwire.NewRawFeatureVector(
				lnwire.TLVOnionPayloadRequired,
				lnwire.PaymentAddrRequired,
			)

			return lnwire.NewFeatureVector(raw, lnwire.Features)
		},
	}
}

// TestBridgeHoldInvoiceCarriesMetadata drives the path the node itself uses
// for a bridge hold invoice: the destination's payment metadata is on the
// invoice as field m with the optional bit 49, next to the hash, amount, CLTV
// and expiry the bridge asked for, and it is a hold invoice with no preimage.
func TestBridgeHoldInvoiceCarriesMetadata(t *testing.T) {
	var stored *invoices.Invoice
	cfg := bridgeInvoiceConfig(t, &stored)
	metadata := []byte{0x01, 0xfa, 0xfa, 0xf0}
	req := bridgerpc.HoldInvoiceRequest{
		Hash:       [32]byte{7, 7, 7},
		AmountMsat: 123_000,
		CLTVDelta:  288,
		Expiry:     10 * time.Minute,
		Memo:       "bridge swap",
		Metadata:   metadata,
	}

	payReq, err := addBridgeHoldInvoiceWith(t.Context(), cfg, req)
	require.NoError(t, err)
	decoded, err := zpay32.Decode(payReq, &chaincfg.MainNetParams)
	require.NoError(t, err)

	require.Equal(t, metadata, decoded.Metadata)
	require.True(t, decoded.Features.HasFeature(
		lnwire.PaymentMetadataOptional,
	))
	require.Equal(t, req.Hash, [32]byte(*decoded.PaymentHash))
	require.EqualValues(t, req.AmountMsat, *decoded.MilliSat)
	require.EqualValues(t, req.CLTVDelta, decoded.MinFinalCLTVExpiry())
	require.Equal(t, req.Expiry, decoded.Expiry())
	require.True(t, stored.HodlInvoice)
	require.Nil(t, stored.Terms.PaymentPreimage)

	// Without metadata: no m field and no bit.
	req.Metadata = nil
	req.Hash = [32]byte{8}
	payReq, err = addBridgeHoldInvoiceWith(t.Context(), cfg, req)
	require.NoError(t, err)
	decoded, err = zpay32.Decode(payReq, &chaincfg.MainNetParams)
	require.NoError(t, err)
	require.Nil(t, decoded.Metadata)
	require.False(t, decoded.Features.HasFeature(
		lnwire.PaymentMetadataOptional,
	))
}

// TestBridgeHoldInvoiceRefusesIncompleteRequests checks the boundary checks
// of the same path: no amount, no CLTV delta or no expiry make no invoice.
func TestBridgeHoldInvoiceRefusesIncompleteRequests(t *testing.T) {
	var stored *invoices.Invoice
	cfg := bridgeInvoiceConfig(t, &stored)
	full := bridgerpc.HoldInvoiceRequest{
		Hash: [32]byte{9}, AmountMsat: 1000, CLTVDelta: 288,
		Expiry: time.Minute,
	}
	type edit func(*bridgerpc.HoldInvoiceRequest)
	for name, change := range map[string]edit{
		"amount": func(r *bridgerpc.HoldInvoiceRequest) {
			r.AmountMsat = 0
		},
		"cltv": func(r *bridgerpc.HoldInvoiceRequest) {
			r.CLTVDelta = 0
		},
		"expiry": func(r *bridgerpc.HoldInvoiceRequest) {
			r.Expiry = 0
		},
	} {
		req := full
		change(&req)
		_, err := addBridgeHoldInvoiceWith(t.Context(), cfg, req)
		require.Error(t, err, name)
	}
	require.Nil(t, stored,
		"an incomplete request reached the invoice store")
}
