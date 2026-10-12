//go:build !dev && !integration

package splice

import (
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/lightningnetwork/lnd/chainntnfs"
	"github.com/lightningnetwork/lnd/fn/v2"
	"github.com/lightningnetwork/lnd/input"
	"github.com/lightningnetwork/lnd/lnwallet"
	"github.com/lightningnetwork/lnd/lnwallet/chainfee"
	"github.com/lightningnetwork/lnd/lnwallet/interactivetx"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/stretchr/testify/require"
)

// signSplice runs Alice's splice from its construction to both
// tx_signatures, Bob signing first; it returns Bob's funding key.
func signSplice(t *testing.T, h *acceptorHarness, s *aliceSplice,
	bobKey *btcec.PublicKey) *btcec.PublicKey {

	t.Helper()

	txid := s.res.Tx.TxHash()
	var bobCommit *lnwire.CommitSig
	select {
	case bobCommit = <-h.link.commits:
	case <-time.After(5 * time.Second):
		t.Fatal("no commitment_signed from the machine")
	}
	if bobKey == nil {
		for _, f := range h.bob.PendingFundings() {
			if f.OutPoint.Hash == txid {
				bobKey = f.LocalKey.PubKey
			}
		}
	}
	require.NotNil(t, bobKey)

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

	bobTxSigs := next[*lnwire.TxSignatures](t, h)
	require.Equal(t, txid, bobTxSigs.TxID)
	h.machine.HandleSpliceMessage(s.aliceTxSignatures(t, h))
	requireResumed(t, h)

	return bobKey
}

// replaceAliceSplice has Alice replace her signed splice at a higher fee
// rate: tx_init_rbf, Bob's tx_ack_rbf, and her new construction with the
// same coin and keys, not yet run.
func replaceAliceSplice(t *testing.T, h *acceptorHarness, prev *aliceSplice,
	bobKey *btcec.PublicKey, feeRate chainfee.SatPerKWeight) *aliceSplice {

	t.Helper()

	amount := prev.sel.Contribution
	h.machine.HandleSpliceMessage(&lnwire.TxInitRbf{
		ChannelID:                 h.machine.cfg.ChanID,
		LockTime:                  h.chain.height,
		FeeRatePerKw:              uint32(feeRate),
		FundingOutputContribution: fn.Some(amount),
		RequireConfirmedInputs:    true,
	})
	ack := next[*lnwire.TxAckRbf](t, h)
	require.Equal(t, fn.Some(btcutil.Amount(0)),
		ack.FundingOutputContribution)

	state := h.alice.State()
	_, out, err := input.GenFundingPkScript(
		state.LocalChanCfg.MultiSigKey.PubKey.SerializeCompressed(),
		state.RemoteChanCfg.MultiSigKey.PubKey.SerializeCompressed(),
		int64(state.Capacity),
	)
	require.NoError(t, err)
	_, newOut, err := input.GenFundingPkScript(
		prev.fundingKey.PubKey().SerializeCompressed(),
		bobKey.SerializeCompressed(), int64(state.Capacity+amount),
	)
	require.NoError(t, err)

	// Change to another address of hers: the transaction differs even at
	// the same fee rate, as the port's random serial ids make it.
	s := &aliceSplice{
		coinKey: prev.coinKey, coin: prev.coin,
		fundingKey: prev.fundingKey, change: p2wpkh(0x43),
	}
	s.sel, err = SelectAddFunds(Shape{
		SharedWitnessWeight: input.MultiSigWitnessSize,
		FundingScript:       newOut.PkScript,
	}, []Coin{s.coin}, amount, false, feeRate, s.change, dustLimit)
	require.NoError(t, err)

	s.session, err = interactivetx.NewSession(interactivetx.Config{
		Role:      interactivetx.Initiator,
		ChannelID: h.machine.cfg.ChanID,
		Shared: interactivetx.SharedInput{
			OutPoint:      state.CurrentFundingOutpoint(),
			Value:         state.Capacity,
			PkScript:      out.PkScript,
			WitnessWeight: input.MultiSigWitnessSize,
		},
		FundingScript:         newOut.PkScript,
		InitiatorContribution: amount,
		FeeRate:               feeRate,
		LockTime:              h.chain.height,
		CheckPeerInput:        func(*interactivetx.Input) error { return nil },
		CheckPeerOutput:       func(*interactivetx.Output) error { return nil },
		DustLimit:             dustLimit,
		WitnessWeight:         peerWitnessWeight,
		Reserve:               func(c btcutil.Amount) btcutil.Amount { return c / 100 },
		InitiatorBalance:      state.LocalCommitment.LocalBalance.ToSatoshis(),
		AcceptorBalance:       state.LocalCommitment.RemoteBalance.ToSatoshis(),
	})
	require.NoError(t, err)
	require.NoError(t, s.session.AddSharedInputAndFunding())
	s.session.AddLocalInput(interactivetx.Input{
		OutPoint: s.coin.OutPoint, Value: s.coin.Value,
		PkScript: s.coin.PkScript, Sequence: interactivetx.MaxSequence,
		MaxWitnessWeight: input.P2WKHWitnessSize, PrevTx: s.coin.PrevTx,
	})
	if s.sel.Change > 0 {
		s.session.AddLocalOutput(interactivetx.Output{
			Value: s.sel.Change, PkScript: s.change,
		})
	}

	return s
}

// rbfPair runs Alice's splice to signed, then her replacement to signed:
// two candidates.
func rbfPair(t *testing.T, h *acceptorHarness) (*aliceSplice,
	*aliceSplice) {

	t.Helper()

	first := startAliceSplice(t, h, 40_000_000, 2_500)
	first.construct(t, h)
	bobKey := signSplice(t, h, first, nil)

	second := replaceAliceSplice(t, h, first, bobKey, 5_000)
	second.construct(t, h)
	signSplice(t, h, second, bobKey)

	return first, second
}

// lockOn confirms txid to our depth and completes the lock with Alice's
// splice_locked.
func lockOn(t *testing.T, h *acceptorHarness, s *aliceSplice) {
	t.Helper()

	txid := s.res.Tx.TxHash()
	h.chain.confirm(t, txid, 6, 1, &chainntnfs.TxConfirmation{
		BlockHeight: 900_006, TxIndex: 3,
	})
	locked := next[*lnwire.SpliceLocked](t, h)
	require.Equal(t, txid, locked.SpliceTxid)
	h.machine.HandleSpliceMessage(&lnwire.SpliceLocked{
		ChannelID: h.machine.cfg.ChanID, SpliceTxid: txid,
	})
	require.Eventually(t, func() bool {
		return h.bob.State().CurrentFundingOutpoint().Hash == txid
	}, 5*time.Second, 20*time.Millisecond)
	require.Empty(t, h.bob.PendingFundings())
}

// TestAcceptSpliceReplaced checks a peer replacing its splice: both
// transactions are candidates, the replacement published, and the channel
// locks on whichever confirms.
func TestAcceptSpliceReplaced(t *testing.T) {
	t.Parallel()

	t.Run("the replacement confirms", func(t *testing.T) {
		t.Parallel()

		h := newAcceptorHarness(t)
		capacity := h.bob.State().Capacity
		first, second := rbfPair(t, h)
		require.NotEqual(t, first.res.Tx.TxHash(),
			second.res.Tx.TxHash())

		// Both fund the channel until one confirms; the
		// replacement is what was published last.
		require.Len(t, h.bob.PendingFundings(), 2)
		_, ok := h.wallet.publishedTxs()[second.res.Tx.TxHash()]
		require.True(t, ok)

		lockOn(t, h, second)
		require.Equal(t, capacity+40_000_000, h.bob.State().Capacity)
	})

	t.Run("the original confirms", func(t *testing.T) {
		t.Parallel()

		h := newAcceptorHarness(t)
		first, _ := rbfPair(t, h)

		// The peer's splice_locked for the original comes first:
		// noted, nothing moves on its word.
		h.machine.HandleSpliceMessage(&lnwire.SpliceLocked{
			ChannelID:  h.machine.cfg.ChanID,
			SpliceTxid: first.res.Tx.TxHash(),
		})
		time.Sleep(50 * time.Millisecond)
		require.Len(t, h.bob.PendingFundings(), 2)

		// The replacement's input spent by the original is no
		// dead splice.
		h.chain.spend(t, h.bob.State().CurrentFundingOutpoint(),
			first.res.Tx, 1)

		// At our depth the original is the splice, locked with
		// the peer's earlier splice_locked.
		txid := first.res.Tx.TxHash()
		h.chain.confirm(t, txid, 6, 1, &chainntnfs.TxConfirmation{
			BlockHeight: 900_006, TxIndex: 3,
		})
		locked := next[*lnwire.SpliceLocked](t, h)
		require.Equal(t, txid, locked.SpliceTxid)
		require.Eventually(t, func() bool {
			return h.bob.State().CurrentFundingOutpoint().Hash ==
				txid
		}, 5*time.Second, 20*time.Millisecond)
		require.Empty(t, h.bob.PendingFundings())
		last, err := h.bob.State().LastSplice()
		require.NoError(t, err)
		require.Equal(t, "locked", last.Outcome)
		require.Equal(t, txid, last.Txid)
	})

	t.Run("a replacement given up falls back", func(t *testing.T) {
		t.Parallel()

		h := newAcceptorHarness(t)
		first := startAliceSplice(t, h, 40_000_000, 2_500)
		first.construct(t, h)
		bobKey := signSplice(t, h, first, nil)

		second := replaceAliceSplice(t, h, first, bobKey, 5_000)
		msg, err := second.session.Next()
		require.NoError(t, err)
		h.machine.HandleSpliceMessage(msg)
		next[*lnwire.TxComplete](t, h)
		h.machine.HandleSpliceMessage(&lnwire.TxAbort{
			ChannelID: h.machine.cfg.ChanID, Data: []byte("nah"),
		})
		next[*lnwire.TxAbort](t, h)
		requireResumed(t, h)

		// The original is the splice again, and locks.
		require.Len(t, h.bob.PendingFundings(), 1)
		lockOn(t, h, first)
	})

	t.Run("after a restart", func(t *testing.T) {
		t.Parallel()

		h := newAcceptorHarness(t)
		first, _ := rbfPair(t, h)

		restarted := NewMachine(h.machine.cfg)
		startReestablished(t, restarted)
		t.Cleanup(restarted.Stop)
		h.machine.Stop()
		h.machine = restarted

		restarted.mu.Lock()
		latest, replaced := restarted.active, restarted.active.replaced
		restarted.mu.Unlock()
		require.Equal(t, chainfee.SatPerKWeight(5_000), latest.feeRate)
		require.Len(t, replaced, 1)
		require.Equal(t, first.res.Tx.TxHash(), replaced[0].txid)

		// The original confirms: the restarted machine follows it.
		lockOn(t, h, first)
	})
}

// TestAcceptSpliceReplacementRefused checks the replacements refused: one
// paying less in fees, one of a splice not yet signed both ways, and one
// after our splice_locked; and one at the same fee rate accepted. The
// splice carries on.
func TestAcceptSpliceReplacementRefused(t *testing.T) {
	t.Parallel()

	rbf := func(h *acceptorHarness, feeRate uint32) {
		h.machine.HandleSpliceMessage(&lnwire.TxInitRbf{
			ChannelID:                 h.machine.cfg.ChanID,
			LockTime:                  h.chain.height,
			FeeRatePerKw:              feeRate,
			FundingOutputContribution: fn.Some(btcutil.Amount(40_000_000)),
		})
	}

	// A replacement paying less in fees than what it replaces is
	// refused once built; one paying the same, as the Core Lightning port
	// sends at the fee floor, is a fine candidate.
	t.Run("less in fees", func(t *testing.T) {
		t.Parallel()

		h := newAcceptorHarness(t)
		first := startAliceSplice(t, h, 40_000_000, 2_500)
		first.construct(t, h)
		bobKey := signSplice(t, h, first, nil)

		second := replaceAliceSplice(t, h, first, bobKey, 2_000)
		second.construct(t, h)
		require.Contains(t, abortReason(t, h), codeFeeBelowFloor)
		requireResumed(t, h)
		require.Len(t, h.bob.PendingFundings(), 1)
		lockOn(t, h, first)
	})

	t.Run("the same fee rate", func(t *testing.T) {
		t.Parallel()

		h := newAcceptorHarness(t)
		first := startAliceSplice(t, h, 40_000_000, 2_500)
		first.construct(t, h)
		bobKey := signSplice(t, h, first, nil)

		second := replaceAliceSplice(t, h, first, bobKey, 2_500)
		second.construct(t, h)
		signSplice(t, h, second, bobKey)
		require.Len(t, h.bob.PendingFundings(), 2)
		lockOn(t, h, first)
	})

	t.Run("not yet signed", func(t *testing.T) {
		t.Parallel()

		h := newAcceptorHarness(t)
		first := startAliceSplice(t, h, 40_000_000, 2_500)
		first.construct(t, h)

		rbf(h, 5_000)
		require.Contains(t, abortReason(t, h), codeRbfUnsupported)
	})

	t.Run("after our splice_locked", func(t *testing.T) {
		t.Parallel()

		h := newAcceptorHarness(t)
		first := startAliceSplice(t, h, 40_000_000, 2_500)
		first.construct(t, h)
		signSplice(t, h, first, nil)
		txid := first.res.Tx.TxHash()
		h.chain.confirm(t, txid, 6, 1, &chainntnfs.TxConfirmation{
			BlockHeight: 900_006, TxIndex: 3,
		})
		next[*lnwire.SpliceLocked](t, h)

		rbf(h, 5_000)
		require.Contains(t, abortReason(t, h), codeRbfUnsupported)
	})
}
