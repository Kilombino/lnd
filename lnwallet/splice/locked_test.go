//go:build !dev && !integration

package splice

import (
	"testing"
	"time"

	"github.com/lightningnetwork/lnd/chainntnfs"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/stretchr/testify/require"
)

// requireNoSpliceLocked checks the machine sends no splice_locked.
func requireNoSpliceLocked(t *testing.T, h *acceptorHarness) {
	t.Helper()

	deadline := time.After(200 * time.Millisecond)
	for {
		select {
		case msg := <-h.peer.msgs:
			_, ok := msg.(*lnwire.SpliceLocked)
			require.False(t, ok, "splice_locked sent")

		case <-deadline:
			return
		}
	}
}

// TestSpliceLockedOncePerConnection checks splice_locked is sent once: never
// before the peer's channel_reestablish, and not as a message when our
// channel_reestablish already named the splice as locked (a peer takes that
// for it, and the Core Lightning port fails the channel on a duplicate).
func TestSpliceLockedOncePerConnection(t *testing.T) {
	t.Parallel()

	pending := func(t *testing.T) (*acceptorHarness, *aliceSplice) {
		h := newAcceptorHarness(t)
		s := splicePastSigning(t, h)
		h.machine.HandleSpliceMessage(s.aliceTxSignatures(t, h))
		requireResumed(t, h)

		return h, s
	}
	reconnect := func(t *testing.T, h *acceptorHarness) {
		h.machine.Stop()
		h.machine = NewMachine(h.machine.cfg)
		require.NoError(t, h.machine.Start())
	}
	confirm := func(t *testing.T, h *acceptorHarness, s *aliceSplice,
		n int) {

		h.chain.confirm(t, s.res.Tx.TxHash(), 6, n,
			&chainntnfs.TxConfirmation{BlockHeight: 900_010})
	}

	t.Run("sent, then named in the reestablish", func(t *testing.T) {
		t.Parallel()

		h, s := pending(t)
		txid := s.res.Tx.TxHash()
		confirm(t, h, s, 1)
		require.Equal(t, txid, next[*lnwire.SpliceLocked](t, h).SpliceTxid)

		// The new connection: the reestablishes, then the depth
		// seen again.
		reconnect(t, h)
		_, locked := h.machine.ReestablishTLVs()
		require.Equal(t, txid, locked.UnsafeFromSome().Txid)
		h.machine.HandleReestablish(&lnwire.ChannelReestablish{})
		requireNoSpliceLocked(t, h)
		confirm(t, h, s, 2)
		requireNoSpliceLocked(t, h)

		h.machine.HandleSpliceMessage(&lnwire.SpliceLocked{
			ChannelID: h.machine.cfg.ChanID, SpliceTxid: txid,
		})
		require.Eventually(t, func() bool {
			return h.bob.State().CurrentFundingOutpoint().Hash ==
				txid
		}, 5*time.Second, 20*time.Millisecond)
	})

	t.Run("due before the reestablish", func(t *testing.T) {
		t.Parallel()

		h, s := pending(t)
		txid := s.res.Tx.TxHash()

		// Our reestablish goes before the depth is reached.
		reconnect(t, h)
		_, locked := h.machine.ReestablishTLVs()
		require.NotEqual(t, txid, locked.UnsafeFromSome().Txid)
		confirm(t, h, s, 2)
		requireNoSpliceLocked(t, h)

		h.machine.HandleReestablish(&lnwire.ChannelReestablish{})
		require.Equal(t, txid, next[*lnwire.SpliceLocked](t, h).SpliceTxid)
		requireNoSpliceLocked(t, h)
	})
}
