package splice

import (
	"strings"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/lntypes"
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

	m := NewMachine(cfg)
	require.NoError(t, m.Start())
	t.Cleanup(m.Stop)

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
