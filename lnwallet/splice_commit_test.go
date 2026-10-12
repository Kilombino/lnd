//go:build !dev && !integration

// Release builds only, like the unified signature hash tests: a spliceable
// channel has option_unified_sigs, which development builds never sign with.

package lnwallet

import (
	"testing"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/input"
	"github.com/lightningnetwork/lnd/keychain"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/stretchr/testify/require"
)

// spliceChanTypes are the channel types a splice applies to: unified
// signatures, with and without anchors.
var spliceChanTypes = map[string]channeldb.ChannelType{
	"tweakless": channeldb.SingleFunderTweaklessBit |
		channeldb.UnifiedSigsBit,
	"anchors": channeldb.SingleFunderTweaklessBit |
		channeldb.AnchorOutputsBit | channeldb.ZeroHtlcTxFeeBit |
		channeldb.UnifiedSigsBit,
}

// withHTLCs locks in HTLCs both ways, one of them below Bob's dust limit, so
// commitments carry HTLC outputs and a trimmed one.
func withHTLCs(t *testing.T, alice, bob *LightningChannel) {
	t.Helper()

	amounts := []btcutil.Amount{150_000, 900, 220_000}
	for i, amt := range amounts {
		htlc, _ := createHTLC(i, lnwire.NewMSatFromSatoshis(amt))
		addAndReceiveHTLC(t, alice, bob, htlc, nil)
	}
	htlc, _ := createHTLC(0, lnwire.NewMSatFromSatoshis(310_000))
	addAndReceiveHTLC(t, bob, alice, htlc, nil)

	// Bob's HTLC reaches his own commitment in the second round.
	require.NoError(t, ForceStateTransition(alice, bob))
	require.NoError(t, ForceStateTransition(alice, bob))
	require.True(t, alice.quiescent())
	require.True(t, bob.quiescent())
}

// TestRebuildCommitmentIdentity checks the rebuild every pending splice's
// commitments come from: on the current funding with no deltas it gives back
// exactly the commitment it started from, on both chains, for both sides.
func TestRebuildCommitmentIdentity(t *testing.T) {
	t.Parallel()

	for name, chanType := range spliceChanTypes {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			alice, bob, err := CreateTestChannels(t, chanType)
			require.NoError(t, err)
			withHTLCs(t, alice, bob)

			for _, lc := range []*LightningChannel{alice, bob} {
				local := lc.commitChains.Local.tail()
				keyRing, err := lc.localKeyRing(local.height)
				require.NoError(t, err)
				got, err := lc.rebuildCommitment(
					local, keyRing,
					lc.currentCommitFunding(),
				)
				require.NoError(t, err)
				require.Equal(t, local.txn.TxHash(),
					got.txn.TxHash())
				require.Equal(t, local.ourBalance,
					got.ourBalance)
				require.Equal(t, local.theirBalance,
					got.theirBalance)
				require.Equal(t, local.fee, got.fee)
				require.Len(t, got.txn.TxOut,
					len(local.txn.TxOut))

				remote := lc.commitChains.Remote.tail()
				keyRing, err = lc.remoteKeyRing()
				require.NoError(t, err)
				got, err = lc.rebuildCommitment(
					remote, keyRing,
					lc.currentCommitFunding(),
				)
				require.NoError(t, err)
				require.Equal(t, remote.txn.TxHash(),
					got.txn.TxHash())
			}
		})
	}
}

// spliceKeys are fresh funding keys for a splice, which each side's signer
// can sign with.
type spliceKeys struct {
	alice, bob *btcec.PrivateKey
}

func newSpliceKeys(t *testing.T, alice, bob *LightningChannel) spliceKeys {
	t.Helper()

	var k spliceKeys
	var err error
	k.alice, err = btcec.NewPrivateKey()
	require.NoError(t, err)
	k.bob, err = btcec.NewPrivateKey()
	require.NoError(t, err)

	aliceSigner := alice.Signer.(*input.MockSigner)
	aliceSigner.Privkeys = append(aliceSigner.Privkeys, k.alice)
	bobSigner := bob.Signer.(*input.MockSigner)
	bobSigner.Privkeys = append(bobSigner.Privkeys, k.bob)

	return k
}

// addSplice adds the same pending splice to both channels: Alice's and
// Bob's balances move by aliceDelta and bobDelta.
func addSplice(t *testing.T, alice, bob *LightningChannel, keys spliceKeys,
	txid chainhash.Hash, aliceDelta, bobDelta btcutil.Amount) wire.OutPoint {

	t.Helper()

	op := wire.OutPoint{Hash: txid, Index: 1}
	capacity := alice.channelState.Capacity + aliceDelta + bobDelta

	require.NoError(t, alice.AddPendingFunding(PendingFunding{
		OutPoint: op, Capacity: capacity,
		LocalKey:   keychain.KeyDescriptor{PubKey: keys.alice.PubKey()},
		RemoteKey:  keys.bob.PubKey(),
		LocalDelta: aliceDelta, RemoteDelta: bobDelta,
	}))
	require.NoError(t, bob.AddPendingFunding(PendingFunding{
		OutPoint: op, Capacity: capacity,
		LocalKey:   keychain.KeyDescriptor{PubKey: keys.bob.PubKey()},
		RemoteKey:  keys.alice.PubKey(),
		LocalDelta: bobDelta, RemoteDelta: aliceDelta,
	}))

	return op
}

// exchangeSpliceCommitments runs the splice's own commitment_signed both
// ways.
func exchangeSpliceCommitments(t *testing.T, alice, bob *LightningChannel,
	txid chainhash.Hash) {

	t.Helper()

	sigs, err := alice.SignSpliceCommitment(ctxb, txid)
	require.NoError(t, err)
	require.NoError(t, bob.ReceiveSpliceCommitment(txid, sigs))

	sigs, err = bob.SignSpliceCommitment(ctxb, txid)
	require.NoError(t, err)
	require.NoError(t, alice.ReceiveSpliceCommitment(txid, sigs))
}

// requireSpends checks with the script engine that tx spends the pending
// funding output of lc's splice txid.
func requireSpends(t *testing.T, lc *LightningChannel, txid chainhash.Hash,
	tx *wire.MsgTx) {

	t.Helper()

	p, err := lc.pendingFunding(txid)
	require.NoError(t, err)
	require.Equal(t, p.OutPoint, tx.TxIn[0].PreviousOutPoint)

	fetcher := txscript.NewCannedPrevOutputFetcher(
		p.fundingOutput.PkScript, p.fundingOutput.Value,
	)
	vm, err := txscript.NewEngine(
		p.fundingOutput.PkScript, tx, 0, txscript.StandardVerifyFlags,
		nil, txscript.NewTxSigHashes(tx, fetcher),
		p.fundingOutput.Value, fetcher,
	)
	require.NoError(t, err)
	require.NoError(t, vm.Execute())
}

// outputValues returns a commitment's outputs by script.
func outputValues(tx *wire.MsgTx) map[string]int64 {
	m := make(map[string]int64)
	for _, out := range tx.TxOut {
		m[string(out.PkScript)] = out.Value
	}

	return m
}

// TestSpliceCommitmentExchange checks the splice's own commitments: each
// side signs the other's current commitment on the new funding, the result
// spends the new funding output under the chain's rules, and the balances on
// it are the current ones moved by the splice.
func TestSpliceCommitmentExchange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		aliceDelta, bobDelta btcutil.Amount
	}{
		{"alice adds funds", 100_000_000, 0},
		{"bob adds funds", 0, 75_000_000},
		{"alice moves funds out", -40_000_000, 0},
		{"bob moves nearly all out", 0, -490_000_000},
	}

	for name, chanType := range spliceChanTypes {
		for _, tc := range tests {
			t.Run(name+"/"+tc.name, func(t *testing.T) {
				t.Parallel()

				alice, bob, err := CreateTestChannels(
					t, chanType,
				)
				require.NoError(t, err)
				withHTLCs(t, alice, bob)

				keys := newSpliceKeys(t, alice, bob)
				txid := chainhash.Hash{0x5a, byte(len(name))}
				addSplice(
					t, alice, bob, keys, txid,
					tc.aliceDelta, tc.bobDelta,
				)
				exchangeSpliceCommitments(t, alice, bob, txid)

				for _, side := range []struct {
					lc                *LightningChannel
					ownDelta, peerDel btcutil.Amount
				}{
					{alice, tc.aliceDelta, tc.bobDelta},
					{bob, tc.bobDelta, tc.aliceDelta},
				} {
					tx, err := side.lc.SignedSpliceCommitment(
						txid,
					)
					require.NoError(t, err)
					requireSpends(t, side.lc, txid, tx)

					// The same commitment as on the
					// current funding, but for the two
					// balance outputs.
					current := side.lc.commitChains.Local.
						tail()
					requireMovedBalances(
						t, current, tx, side.ownDelta,
						side.peerDel,
					)
				}
			})
		}
	}
}

// requireMovedBalances checks that tx is the commitment current on another
// funding with our and the peer's balance outputs moved by the deltas, and
// every other output (HTLCs, anchors) the same.
func requireMovedBalances(t *testing.T, current *commitment, tx *wire.MsgTx,
	ourDelta, theirDelta btcutil.Amount) {

	t.Helper()

	before := outputValues(current.txn)
	after := outputValues(tx)
	require.Equal(t, len(before), len(after))

	var moved int
	for script, v := range before {
		w, ok := after[script]
		require.True(t, ok, "output gone")
		if v != w {
			moved++
		}
	}
	require.LessOrEqual(t, moved, 2)

	var totalBefore, totalAfter int64
	for _, v := range before {
		totalBefore += v
	}
	for _, v := range after {
		totalAfter += v
	}
	require.Equal(t, totalBefore+int64(ourDelta+theirDelta), totalAfter)
	require.Equal(t, current.txn.LockTime, tx.LockTime)
	require.Equal(t, current.txn.TxIn[0].Sequence, tx.TxIn[0].Sequence)
}

// TestSpliceCommitmentRefused checks what the splice's own commitments
// refuse: a splice the other side sees differently, a forged signature, an
// unknown splice, updates in flight, a capacity that doesn't add up, and a
// splice taking more than a balance.
func TestSpliceCommitmentRefused(t *testing.T) {
	t.Parallel()

	chanType := spliceChanTypes["anchors"]

	t.Run("different deltas", func(t *testing.T) {
		t.Parallel()

		alice, bob, err := CreateTestChannels(t, chanType)
		require.NoError(t, err)
		keys := newSpliceKeys(t, alice, bob)
		txid := chainhash.Hash{0x61}
		op := wire.OutPoint{Hash: txid, Index: 1}
		capacity := alice.channelState.Capacity + 100_000

		require.NoError(t, alice.AddPendingFunding(PendingFunding{
			OutPoint: op, Capacity: capacity,
			LocalKey: keychain.KeyDescriptor{
				PubKey: keys.alice.PubKey(),
			},
			RemoteKey:  keys.bob.PubKey(),
			LocalDelta: 100_000,
		}))
		// Bob thinks the funds are his.
		require.NoError(t, bob.AddPendingFunding(PendingFunding{
			OutPoint: op, Capacity: capacity,
			LocalKey: keychain.KeyDescriptor{
				PubKey: keys.bob.PubKey(),
			},
			RemoteKey:  keys.alice.PubKey(),
			LocalDelta: 100_000,
		}))

		sigs, err := alice.SignSpliceCommitment(ctxb, txid)
		require.NoError(t, err)
		var sigErr *InvalidCommitSigError
		require.ErrorAs(t, bob.ReceiveSpliceCommitment(txid, sigs),
			&sigErr)
	})

	t.Run("forged and misdirected signatures", func(t *testing.T) {
		t.Parallel()

		alice, bob, err := CreateTestChannels(t, chanType)
		require.NoError(t, err)
		withHTLCs(t, alice, bob)
		keys := newSpliceKeys(t, alice, bob)
		txid := chainhash.Hash{0x62}
		addSplice(t, alice, bob, keys, txid, 50_000, 0)

		sigs, err := alice.SignSpliceCommitment(ctxb, txid)
		require.NoError(t, err)

		// The commitment signature for the current funding instead.
		current, err := alice.SignNextCommitmentForTest()
		require.NoError(t, err)
		misdirected := *sigs
		misdirected.CommitSig = current
		var sigErr *InvalidCommitSigError
		require.ErrorAs(t, bob.ReceiveSpliceCommitment(
			txid, &misdirected,
		), &sigErr)

		// An HTLC signature swapped for another.
		require.GreaterOrEqual(t, len(sigs.HtlcSigs), 2)
		swapped := *sigs
		swapped.HtlcSigs = append([]lnwire.Sig(nil), sigs.HtlcSigs...)
		swapped.HtlcSigs[0], swapped.HtlcSigs[1] =
			swapped.HtlcSigs[1], swapped.HtlcSigs[0]
		require.Error(t, bob.ReceiveSpliceCommitment(txid, &swapped))

		// One HTLC signature missing.
		short := *sigs
		short.HtlcSigs = sigs.HtlcSigs[1:]
		require.Error(t, bob.ReceiveSpliceCommitment(txid, &short))

		// Nothing was recorded by the refusals.
		_, err = bob.SignedSpliceCommitment(txid)
		require.Error(t, err)

		// The real one passes.
		require.NoError(t, bob.ReceiveSpliceCommitment(txid, sigs))
	})

	t.Run("unknown splice", func(t *testing.T) {
		t.Parallel()

		alice, _, err := CreateTestChannels(t, chanType)
		require.NoError(t, err)
		_, err = alice.SignSpliceCommitment(ctxb, chainhash.Hash{1})
		require.ErrorIs(t, err, ErrUnknownFunding)
	})

	t.Run("updates in flight", func(t *testing.T) {
		t.Parallel()

		alice, bob, err := CreateTestChannels(t, chanType)
		require.NoError(t, err)
		keys := newSpliceKeys(t, alice, bob)
		txid := chainhash.Hash{0x63}
		addSplice(t, alice, bob, keys, txid, 50_000, 0)

		htlc, _ := createHTLC(0, 1_000_000)
		addAndReceiveHTLC(t, alice, bob, htlc, nil)

		_, err = alice.SignSpliceCommitment(ctxb, txid)
		require.ErrorIs(t, err, ErrNotQuiescent)
		require.ErrorIs(t, bob.ReceiveSpliceCommitment(
			txid, &CommitSigs{},
		), ErrNotQuiescent)
	})

	t.Run("capacity mismatch and limits", func(t *testing.T) {
		t.Parallel()

		alice, bob, err := CreateTestChannels(t, chanType)
		require.NoError(t, err)
		keys := newSpliceKeys(t, alice, bob)

		f := PendingFunding{
			OutPoint: wire.OutPoint{Hash: chainhash.Hash{0x64}},
			Capacity: alice.channelState.Capacity + 99_999,
			LocalKey: keychain.KeyDescriptor{
				PubKey: keys.alice.PubKey(),
			},
			RemoteKey:  keys.bob.PubKey(),
			LocalDelta: 100_000,
		}
		require.Error(t, alice.AddPendingFunding(f))

		f.Capacity++
		f.RemoteKey = nil
		require.Error(t, alice.AddPendingFunding(f))

		f.RemoteKey = keys.bob.PubKey()
		f.OutPoint = alice.channelState.CurrentFundingOutpoint()
		require.Error(t, alice.AddPendingFunding(f))

		f.OutPoint = wire.OutPoint{Hash: chainhash.Hash{0x64}}
		require.NoError(t, alice.AddPendingFunding(f))
		require.Error(t, alice.AddPendingFunding(f), "twice")
		require.Len(t, alice.PendingFundings(), 1)
	})

	t.Run("more than the balance", func(t *testing.T) {
		t.Parallel()

		alice, bob, err := CreateTestChannels(t, chanType)
		require.NoError(t, err)
		keys := newSpliceKeys(t, alice, bob)
		txid := chainhash.Hash{0x65}

		// Bob has 5 BTC; a splice can't take 5.1 out of it.
		addSplice(t, alice, bob, keys, txid, 0, -510_000_000)
		_, err = alice.SignSpliceCommitment(ctxb, txid)
		require.ErrorIs(t, err, ErrBelowChanReserve)
	})
}

// SignNextCommitmentForTest returns a commitment signature over the current
// funding's remote tail, for tests that need a valid signature for the wrong
// funding.
func (lc *LightningChannel) SignNextCommitmentForTest() (lnwire.Sig, error) {
	lc.Lock()
	defer lc.Unlock()

	tail := lc.commitChains.Remote.tail()
	lc.signDesc.SigHashes = input.NewTxSigHashesV0Only(tail.txn)
	raw, err := lc.Signer.SignOutputRaw(tail.txn, lc.signDesc)
	if err != nil {
		return lnwire.Sig{}, err
	}

	return lnwire.NewSigFromSignature(raw)
}

// forceBatchTransition is ForceStateTransition for channels with pending
// splices: every commitment goes out as a batch.
func forceBatchTransition(t *testing.T, a, b *LightningChannel) {
	t.Helper()

	aCommit, err := a.SignNextCommitment(ctxb)
	require.NoError(t, err)
	require.NoError(t, b.ReceiveNewCommitmentBatch(
		aCommit.CommitSigs, aCommit.SpliceSigs,
	))
	bRev, _, _, err := b.RevokeCurrentCommitment()
	require.NoError(t, err)
	bCommit, err := b.SignNextCommitment(ctxb)
	require.NoError(t, err)
	_, _, err = a.ReceiveRevocation(bRev)
	require.NoError(t, err)
	require.NoError(t, a.ReceiveNewCommitmentBatch(
		bCommit.CommitSigs, bCommit.SpliceSigs,
	))
	aRev, _, _, err := a.RevokeCurrentCommitment()
	require.NoError(t, err)
	_, _, err = b.ReceiveRevocation(aRev)
	require.NoError(t, err)
}

// splicedPair returns two channels with HTLCs and a pending splice whose own
// commitments have been exchanged.
func splicedPair(t *testing.T, aliceDelta,
	bobDelta btcutil.Amount) (*LightningChannel, *LightningChannel,
	chainhash.Hash) {

	t.Helper()

	alice, bob, err := CreateTestChannels(t, spliceChanTypes["anchors"])
	require.NoError(t, err)
	withHTLCs(t, alice, bob)

	keys := newSpliceKeys(t, alice, bob)
	txid := chainhash.Hash{0x5c}
	addSplice(t, alice, bob, keys, txid, aliceDelta, bobDelta)
	exchangeSpliceCommitments(t, alice, bob, txid)

	return alice, bob, txid
}

// TestSpliceBatchTransitions checks state transitions while a splice is
// pending: every commitment is signed and checked on both fundings, the
// commitment on the splice's funding follows the channel (a new HTLC shows
// up on it), the peer's revoked commitments on it are logged with their own
// txid and output indexes, and all of it survives restarts at rest and with
// a commitment in flight.
func TestSpliceBatchTransitions(t *testing.T) {
	t.Parallel()

	alice, bob, txid := splicedPair(t, 100_000_000, 0)

	before, err := bob.SignedSpliceCommitment(txid)
	require.NoError(t, err)

	// A new HTLC from Alice, locked in on both fundings.
	htlc, _ := createHTLC(3, lnwire.NewMSatFromSatoshis(400_000))
	addAndReceiveHTLC(t, alice, bob, htlc, nil)

	// Bob's commitment on the splice's funding at his current height:
	// what Alice will log once he revokes it.
	bobHeight := bob.commitChains.Local.tail().height
	p, err := bob.pendingFunding(txid)
	require.NoError(t, err)
	bobRevoked := p.local[bobHeight]
	require.NotNil(t, bobRevoked)

	forceBatchTransition(t, alice, bob)
	forceBatchTransition(t, alice, bob)

	after, err := bob.SignedSpliceCommitment(txid)
	require.NoError(t, err)
	requireSpends(t, bob, txid, after)
	require.Len(t, after.TxOut, len(before.TxOut)+1)

	tx, err := alice.SignedSpliceCommitment(txid)
	require.NoError(t, err)
	requireSpends(t, alice, txid, tx)

	// Alice's log of Bob's revoked commitment on the splice's funding.
	rl, err := alice.channelState.FetchSpliceRevocationLog(
		txid, bobHeight,
	)
	require.NoError(t, err)
	require.Equal(t, [32]byte(bobRevoked.txn.TxHash()), rl.CommitTxHash.Val)
	ours := bobRevoked.txn.TxOut[rl.OurOutputIndex.Val]
	require.Equal(t, int64(bobRevoked.theirBalance.ToSatoshis()),
		ours.Value)
	theirs := bobRevoked.txn.TxOut[rl.TheirOutputIndex.Val]
	require.Equal(t, int64(bobRevoked.ourBalance.ToSatoshis()),
		theirs.Value)

	// It isn't the channel's own entry at that height.
	own, _, err := alice.channelState.FindPreviousState(bobHeight)
	require.NoError(t, err)
	require.NotEqual(t, own.CommitTxHash.Val, rl.CommitTxHash.Val)

	// Restart both at rest: the same commitments come back.
	alice, err = restartChannel(alice)
	require.NoError(t, err)
	bob, err = restartChannel(bob)
	require.NoError(t, err)
	again, err := bob.SignedSpliceCommitment(txid)
	require.NoError(t, err)
	require.Equal(t, after.TxHash(), again.TxHash())

	// Restart Alice with her commitment in flight.
	htlc, _ = createHTLC(4, lnwire.NewMSatFromSatoshis(120_000))
	addAndReceiveHTLC(t, alice, bob, htlc, nil)
	aCommit, err := alice.SignNextCommitment(ctxb)
	require.NoError(t, err)
	require.Len(t, aCommit.SpliceSigs, 1)

	alice, err = restartChannel(alice)
	require.NoError(t, err)

	require.NoError(t, bob.ReceiveNewCommitmentBatch(
		aCommit.CommitSigs, aCommit.SpliceSigs,
	))
	bRev, _, _, err := bob.RevokeCurrentCommitment()
	require.NoError(t, err)
	_, _, err = alice.ReceiveRevocation(bRev)
	require.NoError(t, err)
	forceBatchTransition(t, alice, bob)

	final, err := bob.SignedSpliceCommitment(txid)
	require.NoError(t, err)
	requireSpends(t, bob, txid, final)
	require.Len(t, final.TxOut, len(after.TxOut)+1)
}

// TestSpliceBatchRefused checks that a commitment batch must name exactly
// the pending splices, each once, with valid signatures, and that no
// revocation goes out without the next commitment signed on every funding.
func TestSpliceBatchRefused(t *testing.T) {
	t.Parallel()

	alice, bob, txid := splicedPair(t, 0, 60_000_000)

	htlc, _ := createHTLC(3, lnwire.NewMSatFromSatoshis(400_000))
	addAndReceiveHTLC(t, alice, bob, htlc, nil)
	aCommit, err := alice.SignNextCommitment(ctxb)
	require.NoError(t, err)
	require.Len(t, aCommit.SpliceSigs, 1)
	member := aCommit.SpliceSigs[0]
	require.Equal(t, txid, member.FundingTxid)

	// The current funding's signatures alone.
	require.Error(t, bob.ReceiveNewCommitment(aCommit.CommitSigs))

	// A member for a splice Bob doesn't know.
	unknown := member
	unknown.FundingTxid = chainhash.Hash{0xdd}
	require.ErrorIs(t, bob.ReceiveNewCommitmentBatch(
		aCommit.CommitSigs, []SpliceCommitSigs{unknown},
	), ErrUnknownFunding)

	// The same splice twice, or an extra member.
	require.Error(t, bob.ReceiveNewCommitmentBatch(
		aCommit.CommitSigs, []SpliceCommitSigs{member, member},
	))
	require.Error(t, bob.ReceiveNewCommitmentBatch(
		aCommit.CommitSigs, []SpliceCommitSigs{member, unknown},
	))

	// Nothing was accepted: no revocation can go out.
	_, _, _, err = bob.RevokeCurrentCommitment()
	require.Error(t, err)

	// The splice member signed for the current funding instead.
	forged := member
	forgedSigs := *member.CommitSigs
	forgedSigs.CommitSig = aCommit.CommitSig
	forged.CommitSigs = &forgedSigs
	var sigErr *InvalidCommitSigError
	require.ErrorAs(t, bob.ReceiveNewCommitmentBatch(
		aCommit.CommitSigs, []SpliceCommitSigs{forged},
	), &sigErr)

	_, _, _, err = bob.RevokeCurrentCommitment()
	require.Error(t, err)
}

// TestSpliceBatchTwoPending checks a batch with two pending splices (the
// shape replacement attempts take): both are signed, a batch naming one of
// them twice is refused, and both commitments follow the channel.
func TestSpliceBatchTwoPending(t *testing.T) {
	t.Parallel()

	alice, bob, first := splicedPair(t, 100_000_000, 0)

	keys := newSpliceKeys(t, alice, bob)
	second := chainhash.Hash{0x5d}
	addSplice(t, alice, bob, keys, second, 90_000_000, 0)
	exchangeSpliceCommitments(t, alice, bob, second)

	htlc, _ := createHTLC(3, lnwire.NewMSatFromSatoshis(400_000))
	addAndReceiveHTLC(t, alice, bob, htlc, nil)
	aCommit, err := alice.SignNextCommitment(ctxb)
	require.NoError(t, err)
	require.Len(t, aCommit.SpliceSigs, 2)

	twice := []SpliceCommitSigs{aCommit.SpliceSigs[0], aCommit.SpliceSigs[0]}
	require.ErrorContains(t, bob.ReceiveNewCommitmentBatch(
		aCommit.CommitSigs, twice,
	), "twice")

	// In either order, the right members pass.
	reversed := []SpliceCommitSigs{aCommit.SpliceSigs[1], aCommit.SpliceSigs[0]}
	require.NoError(t, bob.ReceiveNewCommitmentBatch(
		aCommit.CommitSigs, reversed,
	))
	bRev, _, _, err := bob.RevokeCurrentCommitment()
	require.NoError(t, err)
	_, _, err = alice.ReceiveRevocation(bRev)
	require.NoError(t, err)
	forceBatchTransition(t, bob, alice)

	for _, txid := range []chainhash.Hash{first, second} {
		for _, lc := range []*LightningChannel{alice, bob} {
			tx, err := lc.SignedSpliceCommitment(txid)
			require.NoError(t, err)
			requireSpends(t, lc, txid, tx)
		}
	}

	// Members swapped between the two splices. Last: a signature that
	// doesn't verify ends the channel's updates, as on any channel.
	htlc, _ = createHTLC(4, lnwire.NewMSatFromSatoshis(100_000))
	addAndReceiveHTLC(t, alice, bob, htlc, nil)
	aCommit, err = alice.SignNextCommitment(ctxb)
	require.NoError(t, err)
	swapped := []SpliceCommitSigs{{
		FundingTxid: aCommit.SpliceSigs[0].FundingTxid,
		CommitSigs:  aCommit.SpliceSigs[1].CommitSigs,
	}, {
		FundingTxid: aCommit.SpliceSigs[1].FundingTxid,
		CommitSigs:  aCommit.SpliceSigs[0].CommitSigs,
	}}
	var sigErr *InvalidCommitSigError
	require.ErrorAs(t, bob.ReceiveNewCommitmentBatch(
		aCommit.CommitSigs, swapped,
	), &sigErr)
}

// TestSpliceAvailableBalance checks that while a splice is pending, what we
// can send is the least on any funding: moving funds out lowers it by the
// amount, adding funds doesn't raise it, and the peer's splice doesn't change
// ours.
func TestSpliceAvailableBalance(t *testing.T) {
	t.Parallel()

	const out = btcutil.Amount(300_000_000)

	alice, bob, err := CreateTestChannels(t, spliceChanTypes["anchors"])
	require.NoError(t, err)
	withHTLCs(t, alice, bob)
	aliceBefore := alice.AvailableBalance()
	bobBefore := bob.AvailableBalance()

	keys := newSpliceKeys(t, alice, bob)
	addSplice(t, alice, bob, keys, chainhash.Hash{0x71}, -out, 0)
	require.Equal(t, aliceBefore-lnwire.NewMSatFromSatoshis(out),
		alice.AvailableBalance())
	require.Equal(t, bobBefore, bob.AvailableBalance())

	// A second, adding funds, changes nothing.
	keys = newSpliceKeys(t, alice, bob)
	addSplice(t, alice, bob, keys, chainhash.Hash{0x72}, 0, 50_000_000)
	require.Equal(t, aliceBefore-lnwire.NewMSatFromSatoshis(out),
		alice.AvailableBalance())
	require.Equal(t, bobBefore, bob.AvailableBalance())
}

// TestSpliceSanity checks that an HTLC must fit on every funding: one that
// fits on the current funding but not on a pending splice-out's is refused
// by the sender and by the receiver.
func TestSpliceSanity(t *testing.T) {
	t.Parallel()

	alice, bob, err := CreateTestChannels(t, spliceChanTypes["anchors"])
	require.NoError(t, err)
	withHTLCs(t, alice, bob)

	// Alice moves out all but 2,000,000 sats of what she can send.
	avail := alice.AvailableBalance().ToSatoshis()
	keys := newSpliceKeys(t, alice, bob)
	txid := chainhash.Hash{0x73}
	addSplice(t, alice, bob, keys, txid, -(avail - 2_000_000), 0)
	exchangeSpliceCommitments(t, alice, bob, txid)

	left := alice.AvailableBalance().ToSatoshis()
	require.InDelta(t, 2_000_000, float64(left), 1)

	// Too much for the splice's funding, fine for the current one.
	big, _ := createHTLC(3, lnwire.NewMSatFromSatoshis(left+100_000))
	_, err = alice.AddHTLC(big, nil)
	require.ErrorIs(t, err, ErrBelowChanReserve)

	// Bob refuses it too if Alice sends it anyway.
	_, err = bob.ReceiveHTLC(big)
	require.ErrorIs(t, err, ErrBelowChanReserve)

	// What fits on every funding goes through and locks in.
	small, _ := createHTLC(3, lnwire.NewMSatFromSatoshis(left-100_000))
	addAndReceiveHTLC(t, alice, bob, small, nil)
	forceBatchTransition(t, alice, bob)
	forceBatchTransition(t, alice, bob)

	tx, err := bob.SignedSpliceCommitment(txid)
	require.NoError(t, err)
	requireSpends(t, bob, txid, tx)
}

// requireCommitSpends checks with the script engine that lc's own signed
// commitment spends its current funding output.
func requireCommitSpends(t *testing.T, lc *LightningChannel) {
	t.Helper()

	tx, err := lc.getSignedCommitTx()
	require.NoError(t, err)
	require.Equal(t, lc.channelState.CurrentFundingOutpoint(),
		tx.TxIn[0].PreviousOutPoint)

	out := lc.fundingOutput
	fetcher := txscript.NewCannedPrevOutputFetcher(out.PkScript, out.Value)
	vm, err := txscript.NewEngine(
		out.PkScript, tx, 0, txscript.StandardVerifyFlags, nil,
		txscript.NewTxSigHashes(tx, fetcher), out.Value, fetcher,
	)
	require.NoError(t, err)
	require.NoError(t, vm.Execute())
}

// TestSpliceLock checks completing a splice: the channel moves to the new
// funding output with its commitments, updates go on without batches, the
// channel's own commitment spends the new output, and all of it survives a
// restart. The lock is taken in the middle of a state transition, with a
// commitment in flight on both chains.
func TestSpliceLock(t *testing.T) {
	t.Parallel()

	const added = btcutil.Amount(100_000_000)

	alice, bob, txid := splicedPair(t, added, 0)
	capacity := alice.channelState.Capacity + added
	p, err := alice.pendingFunding(txid)
	require.NoError(t, err)
	newFunding := p.OutPoint
	scid := lnwire.ShortChannelID{BlockHeight: 900_100, TxIndex: 4}

	htlc, _ := createHTLC(3, lnwire.NewMSatFromSatoshis(400_000))
	addAndReceiveHTLC(t, alice, bob, htlc, nil)
	forceBatchTransition(t, alice, bob)

	// Alice signs; Bob has it but hasn't revoked; then both lock.
	htlc, _ = createHTLC(4, lnwire.NewMSatFromSatoshis(150_000))
	addAndReceiveHTLC(t, alice, bob, htlc, nil)
	aCommit, err := alice.SignNextCommitment(ctxb)
	require.NoError(t, err)
	require.NoError(t, bob.ReceiveNewCommitmentBatch(
		aCommit.CommitSigs, aCommit.SpliceSigs,
	))

	require.NoError(t, alice.LockPendingFunding(txid, scid))
	require.NoError(t, bob.LockPendingFunding(txid, scid))

	for _, lc := range []*LightningChannel{alice, bob} {
		require.Empty(t, lc.PendingFundings())
		require.Equal(t, newFunding,
			lc.channelState.CurrentFundingOutpoint())
		require.Equal(t, capacity, lc.channelState.Capacity)
		require.Equal(t, capacity, lc.Capacity)
		require.Equal(t, int64(capacity), lc.fundingOutput.Value)
	}

	// The transition finishes, on the new funding alone.
	bRev, _, _, err := bob.RevokeCurrentCommitment()
	require.NoError(t, err)
	_, _, err = alice.ReceiveRevocation(bRev)
	require.NoError(t, err)
	require.NoError(t, ForceStateTransition(bob, alice))

	bCommit, err := bob.SignNextCommitment(ctxb)
	require.NoError(t, err)
	require.Empty(t, bCommit.SpliceSigs)
	require.NoError(t, alice.ReceiveNewCommitment(bCommit.CommitSigs))
	aRev, _, _, err := alice.RevokeCurrentCommitment()
	require.NoError(t, err)
	_, _, err = bob.ReceiveRevocation(aRev)
	require.NoError(t, err)
	require.NoError(t, ForceStateTransition(alice, bob))

	requireCommitSpends(t, alice)
	requireCommitSpends(t, bob)

	// Both restart: the new funding and commitments come back.
	alice, err = restartChannel(alice)
	require.NoError(t, err)
	bob, err = restartChannel(bob)
	require.NoError(t, err)
	require.Equal(t, newFunding, alice.channelState.CurrentFundingOutpoint())
	require.Empty(t, alice.PendingFundings())
	requireCommitSpends(t, alice)
	requireCommitSpends(t, bob)

	htlc, _ = createHTLC(5, lnwire.NewMSatFromSatoshis(90_000))
	addAndReceiveHTLC(t, alice, bob, htlc, nil)
	require.NoError(t, ForceStateTransition(alice, bob))
	require.NoError(t, ForceStateTransition(alice, bob))
	requireCommitSpends(t, alice)
	requireCommitSpends(t, bob)

	// The old funding is kept for reorgs.
	prev, err := alice.channelState.PreviousFundings()
	require.NoError(t, err)
	require.Len(t, prev, 1)
	require.Equal(t, alice.channelState.FundingOutpoint, prev[0].OutPoint)
	require.Equal(t, newFunding, prev[0].LockedTo)
}

// TestSpliceLockRefused checks that a splice can't lock before its
// commitments were exchanged, or under a txid that isn't pending.
func TestSpliceLockRefused(t *testing.T) {
	t.Parallel()

	alice, bob, err := CreateTestChannels(t, spliceChanTypes["anchors"])
	require.NoError(t, err)
	keys := newSpliceKeys(t, alice, bob)
	txid := chainhash.Hash{0x74}
	addSplice(t, alice, bob, keys, txid, 10_000_000, 0)

	scid := lnwire.ShortChannelID{BlockHeight: 900_100}
	require.Error(t, alice.LockPendingFunding(txid, scid))
	require.ErrorIs(t, alice.LockPendingFunding(chainhash.Hash{1}, scid),
		ErrUnknownFunding)

	// Signed by us only: not enough either.
	_, err = alice.SignSpliceCommitment(ctxb, txid)
	require.NoError(t, err)
	require.Error(t, alice.LockPendingFunding(txid, scid))
	require.Len(t, alice.PendingFundings(), 1)
}
