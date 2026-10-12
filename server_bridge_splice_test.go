package lnd

import (
	"testing"

	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/stretchr/testify/require"
)

// TestLeastLocalBalance checks the bridge's view of a channel with pending
// splices: a splice-out lowers what it can spend at once, by the largest
// pending outflow (pending splices replace each other, they don't add up),
// and a splice-in doesn't raise it.
func TestLeastLocalBalance(t *testing.T) {
	t.Parallel()

	const balance = lnwire.MilliSatoshi(5_000_000_000)

	require.Equal(t, balance, leastLocalBalance(balance, nil))

	in := &channeldb.SpliceRecord{LocalDelta: 1_000_000}
	require.Equal(t, balance, leastLocalBalance(
		balance, []*channeldb.SpliceRecord{in},
	))

	out := &channeldb.SpliceRecord{LocalDelta: -2_000_000}
	bigger := &channeldb.SpliceRecord{LocalDelta: -3_000_000}
	require.Equal(t, balance-3_000_000_000, leastLocalBalance(
		balance, []*channeldb.SpliceRecord{in, out, bigger},
	))

	all := &channeldb.SpliceRecord{LocalDelta: -5_000_000}
	require.Zero(t, leastLocalBalance(
		balance, []*channeldb.SpliceRecord{all},
	))

	// A channel held in memory only has no splices.
	ch := &channeldb.OpenChannel{}
	ch.LocalCommitment.LocalBalance = balance
	require.Equal(t, balance, spliceSafeLocalBalance(ch))
}
