package splice

import (
	"strings"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/lnwallet"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/stretchr/testify/require"
)

// dropCommitSigsFrom makes the pair lose every commitment_signed from one
// node: a peer that stops answering once the transaction is agreed.
func dropCommitSigsFrom(p *splicePair, from *node) {
	p.dropMu.Lock()
	p.drop = func(n *node, msg lnwire.Message) bool {
		_, isSig := msg.(*lnwire.CommitSig)
		return n == from && isSig
	}
	p.dropMu.Unlock()
}

// TestSpliceSigningTimeout checks that our splice, its transaction agreed
// and the peer's signatures never coming, ends at the timeout: aborted, out
// of the channel's funding set, its coins released, and the channel free to
// splice again.
func TestSpliceSigningTimeout(t *testing.T) {
	t.Parallel()

	p := newSplicePair(t)
	p.alice.machine.cfg.Policy.NegotiationTimeout = 100 * time.Millisecond
	p.alice.fund(t, 1, 5_000_000)
	dropCommitSigsFrom(p, p.bob)

	updates, err := p.alice.machine.Splice(Request{
		Direction: DirectionIn, Amount: 1_000_000, FeeRate: 2_500,
	})
	require.NoError(t, err)
	u := requireUpdates(
		t, updates, StatePreparing, StateNegotiating, StateAborted,
	)
	require.True(t, strings.HasPrefix(u.Reason, codeTimeout), u.Reason)

	require.Eventually(t, func() bool {
		p.alice.machine.mu.Lock()
		defer p.alice.machine.mu.Unlock()

		return p.alice.machine.active == nil
	}, 2*time.Second, 10*time.Millisecond)
	require.Zero(t, p.alice.wallet.leaseCount())
	require.Empty(t, p.alice.ch.PendingFundings())
	records, err := p.alice.ch.State().SpliceRecords()
	require.NoError(t, err)
	require.Empty(t, records)
}

// TestSpliceSigningTimeoutAcceptor checks the peer's splice stalled the same
// way: it ends at the timeout and gives back the node's place for the
// peer's splices, also when the node restarted in between.
func TestSpliceSigningTimeoutAcceptor(t *testing.T) {
	t.Parallel()

	for _, restart := range []bool{false, true} {
		p := newSplicePair(t)
		limits := NewLimits()
		p.alice.machine.cfg.Limits = limits
		timeout := 100 * time.Millisecond
		if restart {
			timeout = time.Hour
		}
		p.alice.machine.cfg.Policy.NegotiationTimeout = timeout
		p.alice.link.mu.Lock()
		p.alice.link.initiator = lntypes.Remote
		p.alice.link.mu.Unlock()
		p.bob.link.stfuResult = lntypes.Local
		p.bob.link.mu.Lock()
		p.bob.link.initiator = lntypes.Local
		p.bob.link.mu.Unlock()
		p.bob.fund(t, 1, 5_000_000)
		dropCommitSigsFrom(p, p.bob)

		_, err := p.bob.machine.Splice(Request{
			Direction: DirectionIn, Amount: 1_000_000,
			FeeRate: 2_500,
		})
		require.NoError(t, err)

		signing := func() bool {
			p.alice.machine.mu.Lock()
			defer p.alice.machine.mu.Unlock()

			a := p.alice.machine.active
			return a != nil && a.phase == phaseSigning
		}
		require.Eventually(t, signing, 5*time.Second,
			10*time.Millisecond)

		m := p.alice.machine
		if restart {
			cfg := m.cfg
			m.Stop()
			cfg.Policy.NegotiationTimeout = 100 * time.Millisecond
			m = NewMachine(cfg)
			require.NoError(t, m.Start())
			t.Cleanup(m.Stop)
		}

		require.Eventually(t, func() bool {
			m.mu.Lock()
			defer m.mu.Unlock()

			return m.active == nil
		}, 5*time.Second, 10*time.Millisecond, "restart %v", restart)
		limits.mu.Lock()
		pending := len(limits.pending)
		limits.mu.Unlock()
		require.Zero(t, pending, "restart %v", restart)
		require.Empty(t, p.alice.ch.PendingFundings())
	}
}

// TestSpliceUnsignedAtRestart checks a splice recorded but never signed by
// us, as after stopping between the two: at the start it is dropped at
// once, out of the funding set and its coins released, rather than leaving
// the channel unable to take a commitment.
func TestSpliceUnsignedAtRestart(t *testing.T) {
	t.Parallel()

	p := newSplicePair(t)
	p.alice.machine.cfg.Policy.NegotiationTimeout = time.Hour
	p.alice.fund(t, 1, 5_000_000)
	dropCommitSigsFrom(p, p.bob)

	_, err := p.alice.machine.Splice(Request{
		Direction: DirectionIn, Amount: 1_000_000, FeeRate: 2_500,
	})
	require.NoError(t, err)

	var txid chainhash.Hash
	require.Eventually(t, func() bool {
		p.alice.machine.mu.Lock()
		defer p.alice.machine.mu.Unlock()

		a := p.alice.machine.active
		if a == nil || a.phase != phaseSigning || !a.commitSent {
			return false
		}
		txid = a.txid

		return true
	}, 5*time.Second, 10*time.Millisecond)

	cfg := p.alice.machine.cfg
	p.alice.machine.Stop()
	require.NoError(t, p.alice.ch.State().UpdateSpliceRecord(txid,
		func(r *channeldb.SpliceRecord) error {
			r.SpliceSig = nil
			return nil
		}))

	// Nothing goes out before the peer's channel_reestablish: a
	// tx_abort first fails its link.
	fromAlice := make(chan lnwire.Message, 8)
	p.dropMu.Lock()
	p.drop = func(n *node, msg lnwire.Message) bool {
		if n == p.alice {
			fromAlice <- msg
		}

		return true
	}
	p.dropMu.Unlock()

	m := NewMachine(cfg)
	require.NoError(t, m.Start())
	t.Cleanup(m.Stop)
	select {
	case msg := <-fromAlice:
		t.Fatalf("%T sent before channel_reestablish", msg)
	case <-time.After(200 * time.Millisecond):
	}

	m.HandleReestablish(&lnwire.ChannelReestablish{
		ChanID: m.cfg.ChanID,
	})
	require.Eventually(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()

		return m.active == nil
	}, 2*time.Second, 10*time.Millisecond)
	require.Empty(t, p.alice.ch.PendingFundings())
	require.Zero(t, p.alice.wallet.leaseCount())
}

// TestSpliceUseMaxKeepsReserve checks that adding everything keeps the
// wallet's reserve for fee-bumping its anchor channels' closes: it comes
// back as change.
func TestSpliceUseMaxKeepsReserve(t *testing.T) {
	t.Parallel()

	p := newSplicePair(t)
	const reserve = btcutil.Amount(30_000)
	p.alice.machine.cfg.Reserve = func() (btcutil.Amount, error) {
		return reserve, nil
	}
	p.alice.fund(t, 1, 1_000_000)

	updates, err := p.alice.machine.Splice(Request{
		Direction: DirectionIn, UseMax: true, FeeRate: 2_500,
	})
	require.NoError(t, err)
	requireUpdates(t, updates, StatePreparing, StateNegotiating,
		StateSigned, StateBroadcast)

	p.alice.machine.mu.Lock()
	tx, err := p.alice.machine.signedTx()
	p.alice.machine.mu.Unlock()
	require.NoError(t, err)
	var kept bool
	for _, out := range tx.TxOut {
		if btcutil.Amount(out.Value) == reserve {
			kept = true
		}
	}
	require.True(t, kept, "no change of the reserve")
}

// TestSpliceDisconnected checks the connection ending under a splice: one
// still negotiating ends with it, its coins released and its request told;
// one past its signatures goes on with the next connection's machine, and
// only the request's updates end.
func TestSpliceDisconnected(t *testing.T) {
	t.Parallel()

	t.Run("negotiating", func(t *testing.T) {
		t.Parallel()

		p := newSplicePair(t)
		p.alice.fund(t, 1, 5_000_000)
		p.dropMu.Lock()
		p.drop = func(from *node, msg lnwire.Message) bool {
			switch msg.(type) {
			case *lnwire.TxAddInput, *lnwire.TxAddOutput,
				*lnwire.TxComplete:

				return from == p.bob
			}

			return false
		}
		p.dropMu.Unlock()

		updates, err := p.alice.machine.Splice(Request{
			Direction: DirectionIn, UseMax: true, FeeRate: 2_500,
		})
		require.NoError(t, err)
		requireUpdates(t, updates, StatePreparing, StateNegotiating)
		require.Equal(t, 1, p.alice.wallet.leaseCount())

		p.alice.machine.Stop()
		u := requireUpdates(t, updates, StateAborted)
		require.Equal(t, codeDisconnected, u.Reason)
		_, open := <-updates
		require.False(t, open)
		require.Zero(t, p.alice.wallet.leaseCount())
	})

	t.Run("broadcast", func(t *testing.T) {
		t.Parallel()

		p := newSplicePair(t)
		p.alice.fund(t, 1, 5_000_000)
		updates, err := p.alice.machine.Splice(Request{
			Direction: DirectionIn, Amount: 1_000_000,
			FeeRate: 2_500,
		})
		require.NoError(t, err)
		requireUpdates(t, updates, StatePreparing, StateNegotiating,
			StateSigned, StateBroadcast)

		p.alice.machine.Stop()
		select {
		case _, open := <-updates:
			require.False(t, open)
		case <-time.After(5 * time.Second):
			t.Fatal("updates not ended")
		}

		// The splice is still the channel's, its coins leased.
		require.Len(t, p.alice.ch.PendingFundings(), 1)
		require.Equal(t, 1, p.alice.wallet.leaseCount())
	})
}

// TestSpliceWithheldGivesBackPlace checks a peer's splice whose tx_signatures
// the peer withholds after ours: kept, as the peer could publish it, but its
// place for the peer's splices given back at the timeout; after a restart,
// taken again for the peer to send them, and given back again at the
// timeout.
func TestSpliceWithheldGivesBackPlace(t *testing.T) {
	t.Parallel()

	h := newAcceptorHarness(t)
	limits := NewLimits()
	h.machine.cfg.Limits = limits
	h.machine.cfg.Policy.NegotiationTimeout = time.Second
	s := startAliceSplice(t, h, 40_000_000, 2_500)
	s.construct(t, h)
	txid := s.res.Tx.TxHash()

	bobCommit := <-h.link.commits
	var bobKey *btcec.PublicKey
	for _, f := range h.bob.PendingFundings() {
		if f.OutPoint.Hash == txid {
			bobKey = f.LocalKey.PubKey
		}
	}
	require.NoError(t, h.alice.AddPendingFunding(
		s.pendingFunding(h, bobKey),
	))
	require.NoError(t, h.alice.ReceiveSpliceCommitment(txid,
		&lnwallet.CommitSigs{
			CommitSig: bobCommit.CommitSig,
			HtlcSigs:  bobCommit.HtlcSigs,
		}))
	aliceSigs, err := h.alice.SignSpliceCommitment(t.Context(), txid)
	require.NoError(t, err)
	h.machine.SpliceCommitReceived(txid,
		h.bob.ReceiveSpliceCommitment(txid, aliceSigs))
	next[*lnwire.TxSignatures](t, h)

	placed := func() int {
		limits.mu.Lock()
		defer limits.mu.Unlock()

		return len(limits.pending)
	}
	require.Eventually(t, func() bool { return placed() == 0 },
		5*time.Second, 10*time.Millisecond)
	require.Len(t, h.bob.PendingFundings(), 1)

	cfg := h.machine.cfg
	h.machine.Stop()
	cfg.Policy.NegotiationTimeout = 300 * time.Millisecond
	m := NewMachine(cfg)
	require.NoError(t, m.Start())
	t.Cleanup(m.Stop)
	require.Equal(t, 1, placed())
	require.Eventually(t, func() bool { return placed() == 0 },
		5*time.Second, 10*time.Millisecond)
	m.mu.Lock()
	require.NotNil(t, m.active)
	m.mu.Unlock()
}

// TestSpliceDisconnectedDuringReplacement checks the connection ending while
// the peer's replacement of our pending splice is negotiated: the splice it
// replaces is still ours and pending, its coins leased, and the request is
// not told it was aborted.
func TestSpliceDisconnectedDuringReplacement(t *testing.T) {
	t.Parallel()

	p := newSplicePair(t)
	p.alice.fund(t, 1, 5_000_000)
	updates, err := p.alice.machine.Splice(Request{
		Direction: DirectionIn, Amount: 1_000_000, FeeRate: 2_500,
	})
	require.NoError(t, err)
	requireUpdates(t, updates, StatePreparing, StateNegotiating,
		StateSigned, StateBroadcast)
	leases := p.alice.wallet.leaseCount()
	require.Positive(t, leases)

	fromAlice := make(chan lnwire.Message, 64)
	p.dropMu.Lock()
	p.drop = func(from *node, msg lnwire.Message) bool {
		if from == p.alice {
			fromAlice <- msg
		}

		return true
	}
	p.dropMu.Unlock()
	p.alice.link.mu.Lock()
	p.alice.link.initiator = lntypes.Remote
	p.alice.link.mu.Unlock()
	p.alice.machine.HandleSpliceMessage(&lnwire.TxInitRbf{
		ChannelID:    p.alice.machine.cfg.ChanID,
		LockTime:     p.chain.height,
		FeeRatePerKw: 3_000,
	})
	select {
	case msg := <-fromAlice:
		require.IsType(t, &lnwire.TxAckRbf{}, msg)
	case <-time.After(5 * time.Second):
		t.Fatal("no answer")
	}

	p.alice.machine.Stop()
	for u := range updates {
		require.NotEqual(t, StateAborted, u.State, u.Reason)
	}
	require.Equal(t, leases, p.alice.wallet.leaseCount())
	require.Len(t, p.alice.ch.PendingFundings(), 1)
}
