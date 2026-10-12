package feature

import (
	"testing"

	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/stretchr/testify/require"
)

// TestSpliceBitOffByDefault checks that splicing is advertised only when it
// is turned on, in init and in the node announcement.
func TestSpliceBitOffByDefault(t *testing.T) {
	t.Parallel()

	// The zero value of Config leaves splicing off.
	off, err := NewManager(Config{})
	require.NoError(t, err)
	for _, set := range []Set{SetInit, SetNodeAnn} {
		fv := off.Get(set)
		require.False(t, fv.HasFeature(lnwire.SpliceOptional), set)
		require.False(t, fv.IsSet(lnwire.SpliceRequired), set)
	}

	on, err := NewManager(Config{Splice: true})
	require.NoError(t, err)
	for _, set := range []Set{SetInit, SetNodeAnn} {
		fv := on.Get(set)
		require.True(t, fv.IsSet(lnwire.SpliceOptional), set)
		require.False(t, fv.IsSet(lnwire.SpliceRequired), set)

		// Splicing needs quiescence; both are advertised together.
		require.True(t, fv.IsSet(lnwire.QuiescenceOptional), set)
	}
}

// TestSpliceHasNoFeatureDependency checks that a peer advertising splicing
// without quiescence isn't refused at init: BOLT 9 lists no dependency for
// option_splice, and lnd checks its dependency table against every peer's
// features when it connects. Whether a given channel can be spliced is
// checked per channel instead.
func TestSpliceHasNoFeatureDependency(t *testing.T) {
	t.Parallel()

	raw := lnwire.NewRawFeatureVector(lnwire.SpliceOptional)
	fv := lnwire.NewFeatureVector(raw, lnwire.Features)
	require.NoError(t, ValidateDeps(fv))
}
