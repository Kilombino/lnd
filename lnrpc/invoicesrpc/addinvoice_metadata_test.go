package invoicesrpc

import (
	"context"
	"testing"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/lightningnetwork/lnd/invoices"
	"github.com/lightningnetwork/lnd/keychain"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/lightningnetwork/lnd/netann"
	"github.com/lightningnetwork/lnd/zpay32"
	"github.com/stretchr/testify/require"
)

// metadataConfig is the least AddInvoice needs to encode a public hold
// invoice: a signer, a network and the node's invoice features, kept in
// shared so a test can see whether AddInvoice changed them.
func metadataConfig(t *testing.T, shared *lnwire.FeatureVector) *AddInvoiceConfig {
	t.Helper()

	priv, err := btcec.NewPrivateKey()
	require.NoError(t, err)

	return &AddInvoiceConfig{
		AddInvoice: func(context.Context, *invoices.Invoice,
			lntypes.Hash) (uint64, error) {

			return 1, nil
		},
		NodeSigner: netann.NewNodeSigner(keychain.NewPrivKeyMessageSigner(
			priv, keychain.KeyLocator{},
		)),
		ChainParams:        &chaincfg.MainNetParams,
		DefaultCLTVExpiry:  40,
		GenInvoiceFeatures: func() *lnwire.FeatureVector { return shared },
	}
}

func invoiceFeatures() *lnwire.FeatureVector {
	return lnwire.NewFeatureVector(lnwire.NewRawFeatureVector(
		lnwire.TLVOnionPayloadRequired, lnwire.PaymentAddrRequired,
	), lnwire.Features)
}

// TestAddInvoiceCarriesMetadata: payment metadata given to AddInvoice is put on
// the invoice as BOLT 11 field m, with the payment_metadata feature bit, and
// the node's shared feature vector is left alone.
func TestAddInvoiceCarriesMetadata(t *testing.T) {
	t.Parallel()

	shared := invoiceFeatures()
	cfg := metadataConfig(t, shared)
	hash := lntypes.Hash{1, 2, 3}
	metadata := []byte{0x01, 0xfa, 0xfa, 0xf0}

	_, inv, err := AddInvoice(t.Context(), cfg, &AddInvoiceData{
		Hash: &hash, Value: 1000, CltvExpiry: 144, HodlInvoice: true,
		Memo: "a coffee", Metadata: metadata,
	})
	require.NoError(t, err)

	decoded, err := zpay32.Decode(
		string(inv.PaymentRequest), &chaincfg.MainNetParams,
	)
	require.NoError(t, err)
	require.Equal(t, metadata, decoded.Metadata)
	require.True(t, decoded.Features.HasFeature(
		lnwire.PaymentMetadataOptional,
	))
	require.False(t, shared.HasFeature(lnwire.PaymentMetadataOptional),
		"the node's own invoice features were changed")
}

// TestAddInvoiceWithoutMetadata: no metadata, no m field and no bit, as
// before.
func TestAddInvoiceWithoutMetadata(t *testing.T) {
	t.Parallel()

	cfg := metadataConfig(t, invoiceFeatures())
	hash := lntypes.Hash{4, 5, 6}

	_, inv, err := AddInvoice(t.Context(), cfg, &AddInvoiceData{
		Hash: &hash, Value: 1000, CltvExpiry: 144, HodlInvoice: true,
	})
	require.NoError(t, err)

	decoded, err := zpay32.Decode(
		string(inv.PaymentRequest), &chaincfg.MainNetParams,
	)
	require.NoError(t, err)
	require.Empty(t, decoded.Metadata)
	require.False(t, decoded.Features.HasFeature(
		lnwire.PaymentMetadataOptional,
	))
}

// TestAddInvoiceRefusesOversizedMetadata: more than one BOLT 11 field can
// hold is an error, not a truncation.
func TestAddInvoiceRefusesOversizedMetadata(t *testing.T) {
	t.Parallel()

	cfg := metadataConfig(t, invoiceFeatures())
	hash := lntypes.Hash{7, 8, 9}

	_, _, err := AddInvoice(t.Context(), cfg, &AddInvoiceData{
		Hash: &hash, Value: 1000, CltvExpiry: 144, HodlInvoice: true,
		Metadata: make([]byte, MaxMetadataLength+1),
	})
	require.ErrorContains(t, err, "payment metadata too long")
}
