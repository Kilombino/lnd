package peer

import (
	"testing"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/stretchr/testify/require"
)

// TestInitRefusesRequiredSpliceWhenOff checks that a peer requiring splicing
// is refused by a node that has it turned off, and accepted by one that has
// it on. The bit is known to lnd, so the generic unknown-required check lets
// it through either way.
func TestInitRefusesRequiredSpliceWhenOff(t *testing.T) {
	t.Parallel()

	remote := lnwire.NewRawFeatureVector(
		lnwire.DataLossProtectRequired, lnwire.SpliceRequired,
		lnwire.QuiescenceOptional,
	)
	init := func() *lnwire.Init {
		return &lnwire.Init{
			Networks:       []chainhash.Hash{{}},
			GlobalFeatures: lnwire.NewRawFeatureVector(),
			Features:       remote.Clone(),
		}
	}

	off := createTestPeer(t).peer
	off.cfg.Features = lnwire.NewFeatureVector(
		lnwire.NewRawFeatureVector(lnwire.DataLossProtectRequired),
		lnwire.Features,
	)
	require.ErrorContains(t, off.handleInitMsg(init()), "requires splicing")

	on := createTestPeer(t).peer
	on.cfg.Features = lnwire.NewFeatureVector(
		lnwire.NewRawFeatureVector(
			lnwire.DataLossProtectRequired,
			lnwire.SpliceOptional, lnwire.QuiescenceOptional,
		),
		lnwire.Features,
	)
	require.NoError(t, on.handleInitMsg(init()))
}
