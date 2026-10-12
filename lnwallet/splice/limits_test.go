package splice

import (
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/lightningnetwork/lnd/chainntnfs"
	"github.com/lightningnetwork/lnd/fn/v2"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/stretchr/testify/require"
)

// TestLimits checks the limits on splices peers start: one in progress per
// peer, a node-wide cap, aborts per channel within a window, places held by
// the machine that took them, and nil limits limiting nothing.
func TestLimits(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_000, 0)
	l := NewLimits()
	l.MaxPending = 2
	l.Now = func() time.Time { return now }

	m1, m2, m3 := &Machine{}, &Machine{}, &Machine{}
	ch := func(b byte) lnwire.ChannelID { return lnwire.ChannelID{b} }
	peerA, peerB, peerC := [33]byte{1}, [33]byte{2}, [33]byte{3}

	require.Empty(t, l.admit(m1, ch(1), peerA, false))
	require.Equal(t, codeTooManySplices, l.admit(m2, ch(2), peerA, false))
	require.Empty(t, l.admit(m2, ch(2), peerB, false))
	require.Equal(t, codeTooManySplices, l.admit(m3, ch(3), peerC, false))

	// Forced, for a splice restored at start.
	require.Empty(t, l.admit(m3, ch(3), peerC, true))

	// Only the machine holding a place frees it.
	l.release(m2, ch(1), false)
	require.Len(t, l.pending, 3)
	l.release(m1, ch(1), false)
	l.release(m3, ch(3), false)
	require.Empty(t, l.admit(m1, ch(1), peerA, false))
	l.release(m1, ch(1), false)

	// Aborts count against the channel, within the window.
	for range DefaultMaxAborts {
		require.Empty(t, l.admit(m1, ch(1), peerA, false))
		l.release(m1, ch(1), true)
		now = now.Add(time.Hour)
	}
	require.Equal(t, codeTooManyAborts, l.admit(m1, ch(1), peerA, false))
	require.Empty(t, l.admit(m3, ch(3), peerC, false))
	l.release(m3, ch(3), false)

	now = now.Add(DefaultAbortWindow - 4*time.Hour)
	require.Equal(t, codeTooManyAborts, l.admit(m1, ch(1), peerA, false))
	now = now.Add(time.Hour)
	require.Empty(t, l.admit(m1, ch(1), peerA, false))

	var none *Limits
	require.Empty(t, none.admit(m1, ch(1), peerA, false))
	none.release(m1, ch(1), true)
}

// TestAcceptSpliceLimits checks a peer's splice_init is refused once the
// node's limits are reached, after the other checks, and that an accepted
// splice the peer then aborts gives its place back and counts against the
// channel.
func TestAcceptSpliceLimits(t *testing.T) {
	t.Parallel()

	init := func(h *acceptorHarness) {
		key, _ := btcec.NewPrivateKey()
		h.machine.HandleSpliceMessage(&lnwire.SpliceInit{
			ChannelID:           h.machine.cfg.ChanID,
			FundingContribution: 1_000_000,
			FundingFeeRatePerKw: 2_500,
			LockTime:            h.chain.height,
			FundingPubKey:       key.PubKey(),
		})
	}

	t.Run("node-wide cap", func(t *testing.T) {
		t.Parallel()

		h := newAcceptorHarness(t)
		limits := NewLimits()
		limits.MaxPending = 1
		require.Empty(t, limits.admit(
			&Machine{}, lnwire.ChannelID{9}, [33]byte{9}, false,
		))
		h.machine.cfg.Limits = limits

		init(h)
		require.Contains(t, abortReason(t, h), codeTooManySplices)
		requireResumed(t, h)
	})

	t.Run("stopped while negotiating", func(t *testing.T) {
		t.Parallel()

		h := newAcceptorHarness(t)
		limits := NewLimits()
		h.machine.cfg.Limits = limits

		init(h)
		next[*lnwire.SpliceAck](t, h)
		h.machine.Stop()

		// Negotiation doesn't survive the restart: the place is
		// free, and the splice no abort of the peer's.
		limits.mu.Lock()
		defer limits.mu.Unlock()
		require.Empty(t, limits.pending)
		require.Zero(t, limits.recentAborts(h.machine.cfg.ChanID))
	})

	t.Run("aborts", func(t *testing.T) {
		t.Parallel()

		h := newAcceptorHarness(t)
		limits := NewLimits()
		h.machine.cfg.Limits = limits

		for range DefaultMaxAborts {
			init(h)
			next[*lnwire.SpliceAck](t, h)
			limits.mu.Lock()
			require.Len(t, limits.pending, 1)
			limits.mu.Unlock()

			h.machine.HandleSpliceMessage(&lnwire.TxAbort{
				ChannelID: h.machine.cfg.ChanID,
				Data:      []byte("changed my mind"),
			})
			requireResumed(t, h)
			require.Eventually(t, func() bool {
				limits.mu.Lock()
				defer limits.mu.Unlock()

				return len(limits.pending) == 0
			}, 5*time.Second, 10*time.Millisecond)

			// Drain what the machine answered.
			for len(h.peer.msgs) > 0 {
				<-h.peer.msgs
			}
		}

		init(h)
		require.Contains(t, abortReason(t, h), codeTooManyAborts)
	})
}

// TestAcceptSpliceLimitsRestart checks a peer-started splice keeps its place
// across a restart, whichever of the old and new machine goes first, and
// gives it back without counting an abort when it locks.
func TestAcceptSpliceLimitsRestart(t *testing.T) {
	t.Parallel()

	h := newAcceptorHarness(t)
	limits := NewLimits()
	h.machine.cfg.Limits = limits
	s := splicePastSigning(t, h)
	txid := s.res.Tx.TxHash()

	held := func() *Machine {
		limits.mu.Lock()
		defer limits.mu.Unlock()

		require.Len(t, limits.pending, 1)
		return limits.pending[h.machine.cfg.ChanID].owner
	}
	require.Equal(t, h.machine, held())

	// The new machine restores the splice before the old one stops.
	old := h.machine
	h.machine = NewMachine(old.cfg)
	require.NoError(t, h.machine.Start())
	old.Stop()
	require.Equal(t, h.machine, held())

	// And the usual way round.
	old = h.machine
	old.Stop()
	h.machine = NewMachine(old.cfg)
	require.NoError(t, h.machine.Start())
	require.Equal(t, h.machine, held())

	h.machine.HandleReestablish(&lnwire.ChannelReestablish{
		NextFunding: fn.Some(lnwire.FundingTxidFlags{Txid: txid}),
	})
	next[*lnwire.TxSignatures](t, h)
	h.machine.HandleSpliceMessage(s.aliceTxSignatures(t, h))
	requireResumed(t, h)

	h.chain.confirm(t, txid, 6, 1, &chainntnfs.TxConfirmation{
		BlockHeight: 900_010,
	})
	next[*lnwire.SpliceLocked](t, h)
	h.machine.HandleSpliceMessage(&lnwire.SpliceLocked{
		ChannelID: h.machine.cfg.ChanID, SpliceTxid: txid,
	})
	require.Eventually(t, func() bool {
		limits.mu.Lock()
		defer limits.mu.Unlock()

		return len(limits.pending) == 0
	}, 5*time.Second, 20*time.Millisecond)

	limits.mu.Lock()
	require.Zero(t, limits.recentAborts(h.machine.cfg.ChanID))
	limits.mu.Unlock()
}
