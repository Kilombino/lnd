//go:build !dev && !integration

package splice

import (
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/lightningnetwork/lnd/chainntnfs"
	"github.com/stretchr/testify/require"
)

// TestSplicePeerInputHold checks that the side whose peer put coins into a
// splice holds them once it locks, until the splice is PeerInputConfs deep;
// that the hold comes back after a restart until the watch says it is deep
// enough; and that the side that put the coins in holds nothing.
func TestSplicePeerInputHold(t *testing.T) {
	t.Parallel()

	const confs = 36
	p := newSplicePair(t)
	for _, n := range []*node{p.alice, p.bob} {
		n.machine.cfg.Policy.PeerInputConfs = confs
	}
	p.alice.fund(t, 1, 40_000_000)
	const amount = btcutil.Amount(30_000_000)

	updates, err := p.alice.machine.Splice(Request{
		Direction: DirectionIn, Amount: amount, FeeRate: 2_500,
	})
	require.NoError(t, err)
	u := requireUpdates(t, updates,
		StatePreparing, StateNegotiating, StateSigned, StateBroadcast,
	)
	require.Zero(t, p.bob.machine.PeerInputHold(), "held before the lock")

	p.confirm(t, u.Txid)
	requireUpdates(t, updates, StateLocked)
	p.requireMoved(t, u.Txid, p.alice.ch.State().Capacity)

	// Bob holds Alice's coins; Alice holds nothing of Bob's.
	require.Eventually(t, func() bool {
		return p.bob.machine.PeerInputHold() == amount
	}, 5*time.Second, 10*time.Millisecond)
	require.Zero(t, p.alice.machine.PeerInputHold())

	// Deep enough: released.
	p.chain.confirm(t, u.Txid, confs, 1, &chainntnfs.TxConfirmation{})
	require.Eventually(t, func() bool {
		return p.bob.machine.PeerInputHold() == 0
	}, 5*time.Second, 10*time.Millisecond)

	// A restart holds them again from the last splice, until the new
	// watch (on the funding the channel is on now) answers.
	restarted := NewMachine(p.bob.machine.cfg)
	startReestablished(t, restarted)
	t.Cleanup(restarted.Stop)
	require.Equal(t, amount, restarted.PeerInputHold())
	p.chain.confirm(t, u.Txid, confs, 2, &chainntnfs.TxConfirmation{})
	require.Eventually(t, func() bool {
		return restarted.PeerInputHold() == 0
	}, 5*time.Second, 10*time.Millisecond)

	// With no confirmations asked for, nothing is held.
	none := p.bob.machine.cfg
	none.Policy.PeerInputConfs = 0
	unheld := NewMachine(none)
	startReestablished(t, unheld)
	t.Cleanup(unheld.Stop)
	require.Zero(t, unheld.PeerInputHold())
}
