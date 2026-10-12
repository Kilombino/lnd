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
		LocalKey:  keychain.KeyDescriptor{PubKey: keys.alice.PubKey()},
		RemoteKey: keys.bob.PubKey(),
		LocalDelta: aliceDelta, RemoteDelta: bobDelta,
	}))
	require.NoError(t, bob.AddPendingFunding(PendingFunding{
		OutPoint: op, Capacity: capacity,
		LocalKey:  keychain.KeyDescriptor{PubKey: keys.bob.PubKey()},
		RemoteKey: keys.alice.PubKey(),
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

