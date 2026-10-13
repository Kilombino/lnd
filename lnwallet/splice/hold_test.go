//go:build !dev && !integration

package splice

import (
	"errors"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/lightningnetwork/lnd/channeldb"
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

	// A later splice that ends without locking overwrites the last
	// splice's record; a restart still holds the coins, from the
	// channel's own record of the hold.
	require.NoError(t, p.bob.ch.State().PutLastSplice(&channeldb.LastSplice{
		Txid: chainhash.Hash{9}, Outcome: "aborted",
	}))
	restarted := NewMachine(p.bob.machine.cfg)
	startReestablished(t, restarted)
	t.Cleanup(restarted.Stop)
	require.Equal(t, amount, restarted.PeerInputHold())

	// Deep enough: released on both machines, and for good.
	p.chain.confirm(t, u.Txid, confs, 2, &chainntnfs.TxConfirmation{})
	for _, m := range []*Machine{p.bob.machine, restarted} {
		require.Eventually(t, func() bool {
			return m.PeerInputHold() == 0
		}, 5*time.Second, 10*time.Millisecond)
	}
	holds, err := p.bob.ch.State().SpliceHolds()
	require.NoError(t, err)
	require.Empty(t, holds)
	again := NewMachine(p.bob.machine.cfg)
	startReestablished(t, again)
	t.Cleanup(again.Stop)
	require.Zero(t, again.PeerInputHold())

	// With no confirmations asked for, nothing is held.
	none := p.bob.machine.cfg
	none.Policy.PeerInputConfs = 0
	unheld := NewMachine(none)
	startReestablished(t, unheld)
	t.Cleanup(unheld.Stop)
	require.Zero(t, unheld.PeerInputHold())
}

// TestSpliceHoldKeptWhenLockAnswerLost checks the hold on the peer's coins
// when the link locked the splice but its answer was an error (lost, or the
// machine stopping): the channel is on the splice's funding, so the hold is
// kept, in memory and on disk, rather than released for good.
func TestSpliceHoldKeptWhenLockAnswerLost(t *testing.T) {
	t.Parallel()

	p := newSplicePair(t)
	for _, n := range []*node{p.alice, p.bob} {
		n.machine.cfg.Policy.PeerInputConfs = 36
	}
	p.bob.link.lockErr = errors.New("answer lost")
	p.alice.fund(t, 1, 40_000_000)
	const amount = btcutil.Amount(30_000_000)

	updates, err := p.alice.machine.Splice(Request{
		Direction: DirectionIn, Amount: amount, FeeRate: 2_500,
	})
	require.NoError(t, err)
	u := requireUpdates(t, updates,
		StatePreparing, StateNegotiating, StateSigned, StateBroadcast,
	)
	p.confirm(t, u.Txid)
	require.Eventually(t, func() bool {
		return p.bob.ch.State().CurrentFundingOutpoint().Hash == u.Txid
	}, 5*time.Second, 10*time.Millisecond)

	time.Sleep(100 * time.Millisecond)
	require.Equal(t, amount, p.bob.machine.PeerInputHold())
	holds, err := p.bob.ch.State().SpliceHolds()
	require.NoError(t, err)
	require.Len(t, holds, 1)
}
