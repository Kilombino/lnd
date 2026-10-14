package commands

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSpliceEstimateFlags checks the estimate doesn't offer force_low_fee,
// which its request can't carry, and keeps every other splice flag.
func TestSpliceEstimateFlags(t *testing.T) {
	t.Parallel()

	flags := estimateFlags()
	require.Len(t, flags, len(spliceFlags)-1)
	for _, f := range flags {
		require.NotEqual(t, "force_low_fee", f.GetName())
	}
}
