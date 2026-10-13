package contractcourt

import (
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/chainio"
	"github.com/lightningnetwork/lnd/chainntnfs"
	"github.com/lightningnetwork/lnd/channeldb"
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
		PreviousOutPoint: alice.State().CurrentFundingOutpoint(),
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

	// Not necessarily the last registration: the watch on a funding
	// output a locked splice replaced may register after it.
	out := wire.OutPoint{Hash: splice.TxHash()}
	require.Eventually(t, func() bool {
		return h.notifier.hasSpend(out)
	}, 5*time.Second, 10*time.Millisecond)

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

// TestChainWatcherSpliceAfterForceCloseRestart checks our commitment on a
// splice that beat our force close is published after a restart too: the
// new watcher sees the splice's spend of the old output again and publishes
// it, the arbitrator's own broadcast of the old commitment being doomed.
func TestChainWatcherSpliceAfterForceCloseRestart(t *testing.T) {
	t.Parallel()

	h := newChainWatcherTestHarness(t, withRequiredConfs(1))
	splice := pendSplice(t, h)

	oldCommit := h.aliceChannel.State().LocalCommitment.CommitTx
	require.NoError(t, h.aliceChannel.State().MarkCommitmentBroadcasted(
		oldCommit, lntypes.Local,
	))

	// A restart: a new watcher for the channel as it is now.
	require.NoError(t, h.chainWatcher.Stop())
	published := make(chan *wire.MsgTx, 1)
	cfg := h.chainWatcher.cfg
	cfg.chanState = h.aliceChannel.State()
	cfg.publishTx = func(tx *wire.MsgTx, _ string) error {
		published <- tx
		return nil
	}
	w, err := newChainWatcher(cfg)
	require.NoError(t, err)
	require.NoError(t, w.Start())
	t.Cleanup(func() { _ = w.Stop() })
	h.chainWatcher = w
	h.waitForSpendRegistration()

	followSplice(t, h, splice)

	select {
	case tx := <-published:
		require.Equal(t, wire.OutPoint{Hash: splice.TxHash()},
			tx.TxIn[0].PreviousOutPoint)
	case <-time.After(5 * time.Second):
		t.Fatal("nothing published")
	}
}

// TestChainWatcherForceCloseAfterSplice checks a force close made after a
// splice confirmed, while it waits to lock: our commitment on the funding
// the splice spent can't confirm, so at the next block our commitment on the
// splice's funding is published, valid under the chain's rules, and only
// once.
func TestChainWatcherForceCloseAfterSplice(t *testing.T) {
	t.Parallel()

	h := newChainWatcherTestHarness(t, withRequiredConfs(1))
	splice := pendSplice(t, h)

	published := make(chan *wire.MsgTx, 2)
	h.chainWatcher.cfg.publishTx = func(tx *wire.MsgTx, _ string) error {
		published <- tx
		return nil
	}

	// Read before the watcher refreshes the channel at each block.
	state := h.aliceChannel.State()
	fundingOutpoint := state.FundingOutpoint
	oldCommit := state.LocalCommitment.CommitTx
	db := state.Db

	followSplice(t, h, splice)

	beat := func() {
		t.Helper()

		b := &chainio.MockBlockbeat{}
		b.On("logger").Return(log)
		b.On("Height").Return(int32(1)).Maybe()
		require.NoError(t, h.chainWatcher.ProcessBlock(b))
	}
	nothing := func(why string) {
		t.Helper()

		select {
		case <-published:
			t.Fatal(why)
		case <-time.After(200 * time.Millisecond):
		}
	}

	// No force close yet: a block publishes nothing.
	beat()
	nothing("published without a force close")

	// The force close, on the funding the splice spent, made as the
	// arbitrator makes it: on its own copy of the channel.
	arbCopy, err := db.FetchChannel(fundingOutpoint)
	require.NoError(t, err)
	require.NoError(t, arbCopy.MarkCommitmentBroadcasted(
		oldCommit, lntypes.Local,
	))

	beat()
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

	// Published: the next block doesn't sign it again.
	beat()
	nothing("published twice")
}

// TestChainWatcherSecondSplice checks a splice of a channel a splice moved
// while the watcher ran, the first one locked since, is followed too: not
// taken for a spend it doesn't recognise. One taken for it before, by a
// watcher that didn't know the splice, is recognised and the flag cleared.
func TestChainWatcherSecondSplice(t *testing.T) {
	t.Parallel()

	h := newChainWatcherTestHarness(t, withRequiredConfs(1))
	first := pendSplice(t, h)
	followSplice(t, h, first)

	scid := lnwire.ShortChannelID{BlockHeight: 900_000}
	require.NoError(t, h.aliceChannel.LockPendingFunding(
		first.TxHash(), scid,
	))
	require.NoError(t, h.bobChannel.LockPendingFunding(
		first.TxHash(), scid,
	))

	second := pendSpliceOf(t, h, 50_000_000)
	require.Equal(t, wire.OutPoint{Hash: first.TxHash()},
		second.TxIn[0].PreviousOutPoint)

	// As if a watcher that didn't know it had flagged it.
	state := h.aliceChannel.State()
	require.NoError(t, state.ApplyChanStatus(
		channeldb.ChanStatusFundingSpentUnknown,
	))

	followSplice(t, h, second)
	require.False(t, state.HasChanStatus(
		channeldb.ChanStatusFundingSpentUnknown,
	))
}

// lockSplice locks the harness's pending splice on both channels, as the
// links do, and moves the channel on with an update.
func lockSplice(t *testing.T, h *chainWatcherTestHarness, splice *wire.MsgTx) {
	t.Helper()

	scid := lnwire.ShortChannelID{BlockHeight: 900_000}
	txid := splice.TxHash()
	require.NoError(t, h.aliceChannel.LockPendingFunding(txid, scid))
	require.NoError(t, h.bobChannel.LockPendingFunding(txid, scid))
	addFakeHTLC(t, 10_000_000, 0, h.aliceChannel, h.bobChannel)
	require.NoError(t, lnwallet.ForceStateTransition(
		h.aliceChannel, h.bobChannel,
	))
}

// TestChainWatcherSpliceLockWatchesPrevious checks that once a splice the
// watch follows has locked, the next block starts the watch on the funding
// output it replaced, without a restart: a revoked commitment confirmed
// there after a reorganisation is found.
func TestChainWatcherSpliceLockWatchesPrevious(t *testing.T) {
	t.Parallel()

	h := newChainWatcherTestHarness(t, withRequiredConfs(1))
	splice := pendSplice(t, h)
	oldFunding := h.aliceChannel.State().FundingOutpoint
	h.notifier.perOutpoint = true
	followSplice(t, h, splice)
	lockSplice(t, h, splice)

	b := &chainio.MockBlockbeat{}
	b.On("logger").Return(log)
	b.On("Height").Return(int32(1)).Maybe()
	require.NoError(t, h.chainWatcher.ProcessBlock(b))

	require.Eventually(t, func() bool {
		_, watched := h.notifier.spendChanFor(oldFunding)
		return watched
	}, 5*time.Second, 10*time.Millisecond)
}

// TestChainWatcherLockedSpliceReorg checks a reorganisation taking out a
// splice that locked while the watcher ran, reported before any block: the
// funding output it replaced is watched, rather than the watch staying on
// the splice's output, which no longer exists.
func TestChainWatcherLockedSpliceReorg(t *testing.T) {
	t.Parallel()

	h := newChainWatcherTestHarness(t, withRequiredConfs(1))
	splice := pendSplice(t, h)
	oldFunding := h.aliceChannel.State().FundingOutpoint
	h.notifier.perOutpoint = true
	followSplice(t, h, splice)
	lockSplice(t, h, splice)

	h.triggerReorg(splice, 7)
	require.Eventually(t, func() bool {
		_, watched := h.notifier.spendChanFor(oldFunding)
		return watched
	}, 5*time.Second, 10*time.Millisecond)
}

// TestChainWatcherForceCloseSpliceReorg checks a force close whose
// commitment we published on a confirmed splice, the splice then taken out
// by a reorganisation: our commitment on the funding output unspent again is
// published again.
func TestChainWatcherForceCloseSpliceReorg(t *testing.T) {
	t.Parallel()

	h := newChainWatcherTestHarness(t, withRequiredConfs(1))
	splice := pendSplice(t, h)

	published := make(chan *wire.MsgTx, 4)
	h.chainWatcher.cfg.publishTx = func(tx *wire.MsgTx, _ string) error {
		published <- tx
		return nil
	}
	oldCommit := h.aliceChannel.State().LocalCommitment.CommitTx
	require.NoError(t, h.aliceChannel.State().MarkCommitmentBroadcasted(
		oldCommit, lntypes.Local,
	))
	followSplice(t, h, splice)
	select {
	case tx := <-published:
		require.Equal(t, splice.TxHash(),
			tx.TxIn[0].PreviousOutPoint.Hash)
	case <-time.After(5 * time.Second):
		t.Fatal("nothing published on the splice")
	}

	h.triggerReorg(splice, 1)
	select {
	case tx := <-published:
		require.Equal(t, oldCommit.TxHash(), tx.TxHash())
	case <-time.After(5 * time.Second):
		t.Fatal("our commitment not published again")
	}
}

// TestChainWatcherDroppedSpliceBreach checks a splice transaction dropped
// after signing (declared dead, say) that a reorganisation confirms after
// all: its output is watched rather than the spend flagged as unknown, and
// the peer's commitment on it, revoked since, is a breach found in the
// splice's own log.
func TestChainWatcherDroppedSpliceBreach(t *testing.T) {
	t.Parallel()

	h := newChainWatcherTestHarness(t, withRequiredConfs(1))
	splice := pendSplice(t, h)
	revoked, err := h.bobChannel.SignedSpliceCommitment(splice.TxHash())
	require.NoError(t, err)

	require.NoError(t, h.aliceChannel.RemovePendingFunding(
		splice.TxHash(),
	))
	require.NoError(t, h.bobChannel.RemovePendingFunding(splice.TxHash()))
	addFakeHTLC(t, 10_000_000, 0, h.aliceChannel, h.bobChannel)
	require.NoError(t, lnwallet.ForceStateTransition(
		h.aliceChannel, h.bobChannel,
	))

	followSplice(t, h, splice)
	require.False(t, h.aliceChannel.State().HasChanStatus(
		channeldb.ChanStatusFundingSpentUnknown,
	))

	h.sendSpend(revoked)
	breach := h.waitForBreach(5 * time.Second)
	h.assertBreachTx(breach, revoked)
}

// TestChainWatcherDroppedSpliceRemoteClose checks the same splice confirmed
// before the peer revoked its commitment on it: that commitment is the
// peer's close, its outputs resolved.
func TestChainWatcherDroppedSpliceRemoteClose(t *testing.T) {
	t.Parallel()

	h := newChainWatcherTestHarness(t, withRequiredConfs(1))
	splice := pendSplice(t, h)
	latest, err := h.bobChannel.SignedSpliceCommitment(splice.TxHash())
	require.NoError(t, err)

	require.NoError(t, h.aliceChannel.RemovePendingFunding(
		splice.TxHash(),
	))
	followSplice(t, h, splice)

	h.sendSpend(latest)
	closeInfo := h.waitForRemoteUnilateralClose(5 * time.Second)
	h.assertRemoteUnilateralCloseTx(closeInfo, latest)
}

// TestChainWatcherPreviousFundingRemoteClose checks the peer's commitment on
// the funding output a lock replaced, unrevoked since no update followed the
// lock, published once a reorganisation took the splice out: it is the
// peer's close, its outputs resolved, not a spend nobody recognises.
func TestChainWatcherPreviousFundingRemoteClose(t *testing.T) {
	t.Parallel()

	h := newChainWatcherTestHarness(t, withRequiredConfs(1))
	splice := pendSplice(t, h)
	txid := splice.TxHash()
	oldFunding := h.aliceChannel.State().FundingOutpoint
	bobOld := h.bobChannel.State().LocalCommitment.CommitTx

	scid := lnwire.ShortChannelID{BlockHeight: 900_000}
	require.NoError(t, h.aliceChannel.LockPendingFunding(txid, scid))
	require.NoError(t, h.bobChannel.LockPendingFunding(txid, scid))

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

	spendOf(oldFunding, splice)
	require.Eventually(t, func() bool {
		return h.notifier.confRegisteredFor(txid, reorgSafeDepth)
	}, 5*time.Second, 10*time.Millisecond)
	h.triggerReorg(splice, 3)

	require.Eventually(t, func() bool {
		ch, ok := h.notifier.spendChanFor(oldFunding)
		return ok && len(ch) == 0
	}, 5*time.Second, 10*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	spendOf(oldFunding, bobOld)

	select {
	case closeInfo := <-events.RemoteUnilateralClosure:
		require.Equal(t, bobOld.TxHash(), *closeInfo.SpenderTxHash)
	case <-time.After(5 * time.Second):
		t.Fatal("no remote close")
	}
	require.False(t, h.aliceChannel.State().HasChanStatus(
		channeldb.ChanStatusFundingSpentUnknown,
	))
}

// TestChainWatcherForceCloseUnconfirmedSplice checks a force close while a
// splice is unconfirmed: our commitment on the splice's funding output, its
// child, is published at the next block, valid under the chain's rules, so
// the anchor sweep can raise the fee of both; and only once.
func TestChainWatcherForceCloseUnconfirmedSplice(t *testing.T) {
	t.Parallel()

	h := newChainWatcherTestHarness(t, withRequiredConfs(1))
	splice := pendSplice(t, h)

	published := make(chan *wire.MsgTx, 2)
	h.chainWatcher.cfg.publishTx = func(tx *wire.MsgTx, _ string) error {
		published <- tx
		return nil
	}
	state := h.aliceChannel.State()
	fundingOutpoint := state.FundingOutpoint
	oldCommit := state.LocalCommitment.CommitTx
	db := state.Db

	beat := func() {
		t.Helper()

		b := &chainio.MockBlockbeat{}
		b.On("logger").Return(log)
		b.On("Height").Return(int32(1)).Maybe()
		require.NoError(t, h.chainWatcher.ProcessBlock(b))
	}

	beat()
	select {
	case <-published:
		t.Fatal("published without a force close")
	case <-time.After(200 * time.Millisecond):
	}

	arbCopy, err := db.FetchChannel(fundingOutpoint)
	require.NoError(t, err)
	require.NoError(t, arbCopy.MarkCommitmentBroadcasted(
		oldCommit, lntypes.Local,
	))

	beat()
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

	beat()
	select {
	case <-published:
		t.Fatal("published twice")
	case <-time.After(200 * time.Millisecond):
	}
}
