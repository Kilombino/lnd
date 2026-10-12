package contractcourt

import (
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/chainntnfs"
	"github.com/lightningnetwork/lnd/input"
	"github.com/lightningnetwork/lnd/keychain"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/lnwallet"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/stretchr/testify/require"
)

// pendSplice gives the harness's channels a pending splice, Alice adding
// funds, with its own commitments exchanged, and returns its transaction:
// one spending the channel's funding output into the new one.
func pendSplice(t *testing.T, h *chainWatcherTestHarness) *wire.MsgTx {
	return pendSpliceOf(t, h, 100_000_000)
}

// pendSpliceOf is pendSplice with Alice adding the given amount.
func pendSpliceOf(t *testing.T, h *chainWatcherTestHarness,
	added btcutil.Amount) *wire.MsgTx {

	t.Helper()

	alice, bob := h.aliceChannel, h.bobChannel
	aliceKey, err := btcec.NewPrivateKey()
	require.NoError(t, err)
	bobKey, err := btcec.NewPrivateKey()
	require.NoError(t, err)
	as := alice.Signer.(*input.MockSigner)
	as.Privkeys = append(as.Privkeys, aliceKey)
	bs := bob.Signer.(*input.MockSigner)
	bs.Privkeys = append(bs.Privkeys, bobKey)

	capacity := alice.State().Capacity + added
	_, out, err := input.GenFundingPkScript(
		aliceKey.PubKey().SerializeCompressed(),
		bobKey.PubKey().SerializeCompressed(), int64(capacity),
	)
	require.NoError(t, err)

	tx := wire.NewMsgTx(2)
	tx.AddTxIn(&wire.TxIn{
		PreviousOutPoint: alice.State().FundingOutpoint,
		Sequence:         0xfffffffd,
	})
	tx.AddTxIn(&wire.TxIn{
		PreviousOutPoint: wire.OutPoint{Index: 7},
		Sequence:         0xfffffffd,
	})
	tx.AddTxOut(out)
	op := wire.OutPoint{Hash: tx.TxHash(), Index: 0}

	require.NoError(t, alice.AddPendingFunding(lnwallet.PendingFunding{
		OutPoint: op, Capacity: capacity,
		LocalKey:   keychain.KeyDescriptor{PubKey: aliceKey.PubKey()},
		RemoteKey:  bobKey.PubKey(),
		LocalDelta: added,
	}))
	require.NoError(t, bob.AddPendingFunding(lnwallet.PendingFunding{
		OutPoint: op, Capacity: capacity,
		LocalKey:    keychain.KeyDescriptor{PubKey: bobKey.PubKey()},
		RemoteKey:   aliceKey.PubKey(),
		RemoteDelta: added,
	}))

	txid := op.Hash
	sigs, err := alice.SignSpliceCommitment(t.Context(), txid)
	require.NoError(t, err)
	require.NoError(t, bob.ReceiveSpliceCommitment(txid, sigs))
	sigs, err = bob.SignSpliceCommitment(t.Context(), txid)
	require.NoError(t, err)
	require.NoError(t, alice.ReceiveSpliceCommitment(txid, sigs))

	return tx
}

// followSplice confirms the splice to the watcher and waits for the watch to
// move to its funding output.
func followSplice(t *testing.T, h *chainWatcherTestHarness,
	splice *wire.MsgTx) {

	t.Helper()

	h.sendSpend(splice)
	h.waitForSpendRegistration()
	require.Equal(t, wire.OutPoint{Hash: splice.TxHash()},
		h.notifier.lastSpendOutpoint())

	// Nothing closed, and the link stays up.
	select {
	case <-h.chanEvents.RemoteUnilateralClosure:
		t.Fatal("splice taken for a close")
	case <-h.chanEvents.CooperativeClosure:
		t.Fatal("splice taken for a cooperative close")
	case <-time.After(100 * time.Millisecond):
	}
	require.Zero(t, h.linkDownCalls.Load())
}

// TestChainWatcherFollowsSplice checks that a pending splice's transaction
// spending the funding output isn't a close: the watch moves to its funding
// output, and the peer's commitment on that output closing the channel is
// recognised there, with our outputs on it found. Alice's 3 BTC HTLC sits
// between her balance and Bob's on the old funding and below hers on the
// new one, so the outputs are in another order there: the HTLC must be
// claimed at its index on the new commitment.
func TestChainWatcherFollowsSplice(t *testing.T) {
	t.Parallel()

	h := newChainWatcherTestHarness(t, withRequiredConfs(1))
	addFakeHTLC(t, 300_000_000_000, 0, h.aliceChannel, h.bobChannel)
	require.NoError(t, lnwallet.ForceStateTransition(
		h.aliceChannel, h.bobChannel,
	))

	splice := pendSpliceOf(t, h, 150_000_000)
	followSplice(t, h, splice)

	bobCommit, err := h.bobChannel.SignedSpliceCommitment(splice.TxHash())
	require.NoError(t, err)
	h.sendSpend(bobCommit)

	closeInfo := h.waitForRemoteUnilateralClose(5 * time.Second)
	h.assertRemoteUnilateralCloseTx(closeInfo, bobCommit)
	require.NotNil(t, closeInfo.CommitResolution)
	require.Equal(t, bobCommit.TxHash(),
		closeInfo.CommitResolution.SelfOutPoint.Hash)

	// The close is the channel's, named as it was opened: the tower
	// client ends its sessions' interest in it by that name.
	require.Equal(t, h.aliceChannel.State().FundingOutpoint,
		closeInfo.ChannelCloseSummary.ChanPoint)
	require.NotEqual(t, splice.TxHash(),
		closeInfo.ChannelCloseSummary.ChanPoint.Hash)

	// The HTLC is claimed at its output on the new commitment.
	resolutions := closeInfo.HtlcResolutions
	require.NotNil(t, resolutions)
	require.Len(t, resolutions.OutgoingHTLCs, 1)
	claim := resolutions.OutgoingHTLCs[0].ClaimOutpoint
	require.Equal(t, bobCommit.TxHash(), claim.Hash)
	require.Equal(t, int64(300_000_000),
		bobCommit.TxOut[claim.Index].Value)

	oldIndex := -1
	for i, out := range h.bobChannel.State().LocalCommitment.CommitTx.TxOut {
		if out.Value == 300_000_000 {
			oldIndex = i
		}
	}
	require.NotEqual(t, oldIndex, int(claim.Index),
		"the HTLC must sit elsewhere on the old commitment for this "+
			"test to tell them apart")
}

// TestChainWatcherSpliceReorg checks that a reorg taking the splice out
// brings the watch back to the old funding output, where a close is
// recognised as before.
func TestChainWatcherSpliceReorg(t *testing.T) {
	t.Parallel()

	h := newChainWatcherTestHarness(t, withRequiredConfs(1))
	splice := pendSplice(t, h)
	followSplice(t, h, splice)

	h.triggerReorg(splice, 1)
	h.waitForSpendRegistration()
	require.Equal(t, h.aliceChannel.State().FundingOutpoint,
		h.notifier.lastSpendOutpoint())

	bobCommit := h.bobChannel.State().LocalCommitment.CommitTx
	h.sendSpend(bobCommit)
	closeInfo := h.waitForRemoteUnilateralClose(5 * time.Second)
	h.assertRemoteUnilateralCloseTx(closeInfo, bobCommit)
}

// TestChainWatcherSpliceBreach checks a revoked commitment of the peer's on
// a confirmed splice's funding output: recognised as a breach on that
// funding.
func TestChainWatcherSpliceBreach(t *testing.T) {
	t.Parallel()

	h := newChainWatcherTestHarness(t, withRequiredConfs(1))
	splice := pendSplice(t, h)

	// Bob's commitment on the splice's funding, then a state transition
	// revokes it.
	revoked, err := h.bobChannel.SignedSpliceCommitment(splice.TxHash())
	require.NoError(t, err)

	aCommit, err := h.aliceChannel.SignNextCommitment(t.Context())
	require.NoError(t, err)
	require.NoError(t, h.bobChannel.ReceiveNewCommitmentBatch(
		aCommit.CommitSigs, aCommit.SpliceSigs,
	))
	bRev, _, _, err := h.bobChannel.RevokeCurrentCommitment()
	require.NoError(t, err)
	_, _, err = h.aliceChannel.ReceiveRevocation(bRev)
	require.NoError(t, err)

	followSplice(t, h, splice)
	h.sendSpend(revoked)

	breach := h.waitForBreach(5 * time.Second)
	h.assertBreachTx(breach, revoked)
}

// TestChainWatcherSpliceAfterForceClose checks a force close racing a
// splice: we published our commitment on the old funding, the splice
// confirmed instead, so our commitment on the splice's funding is published,
// valid under the chain's rules.
func TestChainWatcherSpliceAfterForceClose(t *testing.T) {
	t.Parallel()

	h := newChainWatcherTestHarness(t, withRequiredConfs(1))
	splice := pendSplice(t, h)

	published := make(chan *wire.MsgTx, 1)
	h.chainWatcher.cfg.publishTx = func(tx *wire.MsgTx, _ string) error {
		published <- tx
		return nil
	}

	oldCommit := h.aliceChannel.State().LocalCommitment.CommitTx
	require.NoError(t, h.aliceChannel.State().MarkCommitmentBroadcasted(
		oldCommit, lntypes.Local,
	))

	followSplice(t, h, splice)

	var tx *wire.MsgTx
	select {
	case tx = <-published:
	case <-time.After(5 * time.Second):
		t.Fatal("nothing published")
	}
	require.Equal(t, wire.OutPoint{Hash: splice.TxHash()},
		tx.TxIn[0].PreviousOutPoint)

	prevOut := splice.TxOut[0]
	fetcher := txscript.NewCannedPrevOutputFetcher(
		prevOut.PkScript, prevOut.Value,
	)
	vm, err := txscript.NewEngine(
		prevOut.PkScript, tx, 0, txscript.StandardVerifyFlags, nil,
		txscript.NewTxSigHashes(tx, fetcher), prevOut.Value, fetcher,
	)
	require.NoError(t, err)
	require.NoError(t, vm.Execute())
}

// TestChainWatcherPreviousFundingBreach checks the funding output a locked
// splice replaced: it stays watched after a restart; its spend by the
// splice is followed; if a reorg takes the splice out and the peer then
// publishes a commitment on the old output, revoked since the lock, it is
// punished as a breach.
func TestChainWatcherPreviousFundingBreach(t *testing.T) {
	t.Parallel()

	h := newChainWatcherTestHarness(t, withRequiredConfs(1))
	splice := pendSplice(t, h)
	txid := splice.TxHash()
	oldFunding := h.aliceChannel.State().FundingOutpoint

	// Bob's commitment on the old funding at the lock.
	bobOld := h.bobChannel.State().LocalCommitment.CommitTx

	scid := lnwire.ShortChannelID{BlockHeight: 900_000}
	require.NoError(t, h.aliceChannel.LockPendingFunding(txid, scid))
	require.NoError(t, h.bobChannel.LockPendingFunding(txid, scid))

	// An update after the lock revokes it.
	addFakeHTLC(t, 10_000_000, 0, h.aliceChannel, h.bobChannel)
	require.NoError(t, lnwallet.ForceStateTransition(
		h.aliceChannel, h.bobChannel,
	))

	// A restart: a new watcher for the channel as it is now.
	require.NoError(t, h.chainWatcher.Stop())
	h.notifier.perOutpoint = true
	cfg := h.chainWatcher.cfg
	cfg.chanState = h.aliceChannel.State()
	w, err := newChainWatcher(cfg)
	require.NoError(t, err)
	require.NoError(t, w.Start())
	t.Cleanup(func() { _ = w.Stop() })
	events := w.SubscribeChannelEvents()

	spendOf := func(op wire.OutPoint, tx *wire.MsgTx) {
		t.Helper()

		var ch chan *chainntnfs.SpendDetail
		require.Eventually(t, func() bool {
			var ok bool
			ch, ok = h.notifier.spendChanFor(op)
			return ok
		}, 5*time.Second, 10*time.Millisecond)
		hash := tx.TxHash()
		ch <- &chainntnfs.SpendDetail{
			SpenderTxHash: &hash, SpendingTx: tx,
			SpendingHeight: 900_010,
		}
	}

	// The splice spends the old output; then a reorg takes it out.
	spendOf(oldFunding, splice)
	require.Eventually(t, func() bool {
		return h.notifier.confRegisteredFor(txid, reorgSafeDepth)
	}, 5*time.Second, 10*time.Millisecond)
	h.triggerReorg(splice, 3)

	// Bob publishes his old commitment on the old output.
	require.Eventually(t, func() bool {
		ch, ok := h.notifier.spendChanFor(oldFunding)
		return ok && len(ch) == 0
	}, 5*time.Second, 10*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	spendOf(oldFunding, bobOld)

	select {
	case breach := <-events.ContractBreach:
		require.Equal(t, bobOld.TxHash(), breach.CommitHash)
	case <-time.After(5 * time.Second):
		t.Fatal("no breach")
	}
}
