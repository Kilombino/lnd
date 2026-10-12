package contractcourt

import (
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/input"
	"github.com/lightningnetwork/lnd/keychain"
	"github.com/lightningnetwork/lnd/lnwallet"
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
