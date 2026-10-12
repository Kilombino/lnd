package interactivetx

import (
	"testing"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/ecdsa"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/input"
	"github.com/stretchr/testify/require"
)

// splice is a completed construction with real keys: a 2-of-2 shared input,
// and two wallet inputs the acceptor added (P2WPKH and P2TR).
type splice struct {
	res           *Result
	witnessScript []byte
	fundA, fundB  *btcec.PrivateKey
	wpkh, tr      *btcec.PrivateKey
}

func newSplice(t *testing.T) *splice {
	t.Helper()

	sp := &splice{}
	var err error
	for _, k := range []**btcec.PrivateKey{
		&sp.fundA, &sp.fundB, &sp.wpkh, &sp.tr,
	} {
		*k, err = btcec.NewPrivateKey()
		require.NoError(t, err)
	}

	sp.witnessScript, err = input.GenMultiSigScript(
		sp.fundA.PubKey().SerializeCompressed(),
		sp.fundB.PubKey().SerializeCompressed(),
	)
	require.NoError(t, err)
	sharedPkScript, err := input.WitnessScriptHash(sp.witnessScript)
	require.NoError(t, err)

	wpkhScript, err := input.WitnessPubKeyHash(
		sp.wpkh.PubKey().SerializeCompressed(),
	)
	require.NoError(t, err)
	trScript, err := txscript.PayToTaprootScript(
		txscript.ComputeTaprootKeyNoScript(sp.tr.PubKey()),
	)
	require.NoError(t, err)

	p := defaultParams()
	p.accContrib = 150_000
	p.initContrib = -2_000
	initCfg := config(Initiator, p)
	initCfg.Shared.PkScript = sharedPkScript
	accCfg := config(Acceptor, p)
	accCfg.Shared.PkScript = sharedPkScript

	init, err := NewSession(initCfg)
	require.NoError(t, err)
	acc, err := NewSession(accCfg)
	require.NoError(t, err)
	require.NoError(t, init.AddSharedInputAndFunding())

	w := prevTx(21, wpkhScript, 100_000)
	acc.AddLocalInput(Input{
		OutPoint: wire.OutPoint{Hash: w.TxHash()}, Value: 100_000,
		PkScript: wpkhScript, Sequence: MaxSequence,
		MaxWitnessWeight: input.P2WKHWitnessSize, PrevTx: w,
	})
	tr := prevTx(22, trScript, 60_000)
	acc.AddLocalInput(Input{
		OutPoint: wire.OutPoint{Hash: tr.TxHash()}, Value: 60_000,
		PkScript: trScript, Sequence: MaxSequence,
		MaxWitnessWeight: 67, PrevTx: tr,
	})

	require.NoError(t, run(init, acc))
	sp.res, err = init.Build()
	require.NoError(t, err)

	return sp
}

// peerWitnesses signs the acceptor's two inputs with the given hash types.
func (sp *splice) peerWitnesses(t *testing.T, wpkhType,
	trType txscript.SigHashType) []wire.TxWitness {

	t.Helper()

	fetcher := PrevOutFetcher(sp.res)
	sigHashes := txscript.NewTxSigHashes(sp.res.Tx, fetcher)

	var witnesses []wire.TxWitness
	for i, in := range sp.res.Inputs {
		if in.Shared || addedBy(in.SerialID) != Acceptor {
			continue
		}
		var (
			w   wire.TxWitness
			err error
		)
		if txscript.IsPayToTaproot(in.PkScript) {
			w, err = txscript.TaprootWitnessSignature(
				sp.res.Tx, sigHashes, i, int64(in.Value),
				in.PkScript, trType, sp.tr,
			)
		} else {
			w, err = txscript.WitnessSignature(
				sp.res.Tx, sigHashes, i, int64(in.Value),
				in.PkScript, wpkhType, sp.wpkh, true,
			)
		}
		require.NoError(t, err)
		witnesses = append(witnesses, w)
	}

	return witnesses
}

func TestPeerWitnessesVerify(t *testing.T) {
	t.Parallel()

	sp := newSplice(t)
	all, unified := txscript.SigHashAll, SharedInputHashType

	// Accepted: 0x01 and 0x21, a 64-byte (default) and a 65-byte P2TR
	// signature.
	require.NoError(t, CheckPeerWitnesses(
		sp.res, Acceptor, sp.peerWitnesses(t, unified, unified), true,
	))
	require.NoError(t, CheckPeerWitnesses(
		sp.res, Acceptor, sp.peerWitnesses(t, all, txscript.SigHashDefault),
		true,
	))

	// Without option_unified_sigs, 0x21 isn't accepted.
	require.ErrorContains(t, CheckPeerWitnesses(
		sp.res, Acceptor, sp.peerWitnesses(t, unified, unified), false,
	), "not accepted")

	// Hash types that sign less than the whole transaction.
	for _, ht := range []txscript.SigHashType{
		txscript.SigHashNone, txscript.SigHashSingle,
		txscript.SigHashAll | txscript.SigHashAnyOneCanPay,
		unified | txscript.SigHashAnyOneCanPay,
	} {
		require.ErrorContains(t, CheckPeerWitnesses(
			sp.res, Acceptor, sp.peerWitnesses(t, ht, all), true,
		), "not accepted", "hash type %#x", ht)
	}
}

func TestPeerWitnessesRefused(t *testing.T) {
	t.Parallel()

	sp := newSplice(t)
	good := sp.peerWitnesses(t, SharedInputHashType, SharedInputHashType)

	// A valid shape over a wrong signature: script execution catches it.
	bad := []wire.TxWitness{
		{append([]byte(nil), good[0][0]...), good[0][1]},
		good[1],
	}
	bad[0][0][10] ^= 0xff
	require.Error(t, CheckPeerWitnesses(sp.res, Acceptor, bad, true))

	// A P2TR witness with an extra element (an annex, or a script path).
	annexed := []wire.TxWitness{good[0], {good[1][0], {0x50, 0x01}}}
	require.ErrorContains(t, CheckPeerWitnesses(
		sp.res, Acceptor, annexed, true,
	), "not a P2TR key path witness")

	// Too few witnesses.
	require.ErrorContains(t, CheckPeerWitnesses(
		sp.res, Acceptor, good[:1], true,
	), "2 inputs")

	// A P2WPKH witness with an oversized signature item.
	big := []wire.TxWitness{
		{make([]byte, 80), good[0][1]}, good[1],
	}
	require.ErrorContains(t, CheckPeerWitnesses(
		sp.res, Acceptor, big, true,
	), "not a P2WPKH witness")
}

// TestSharedInputSignature checks the 0x21 digest of the shared input end to
// end: both funding keys sign it, the witness carries 0x21, and the script
// engine with this chain's rules accepts it. A signature over the plain
// SIGHASH_ALL digest doesn't verify as the shared input's.
func TestSharedInputSignature(t *testing.T) {
	t.Parallel()

	sp := newSplice(t)

	digest, err := SharedInputSigHash(
		sp.res, sp.witnessScript, SharedInputHashType,
	)
	require.NoError(t, err)
	sigA := ecdsa.Sign(sp.fundA, digest)
	sigB := ecdsa.Sign(sp.fundB, digest)

	require.NoError(t, VerifySharedInputSig(
		sp.res, sp.witnessScript, sigB, sp.fundB.PubKey(),
		SharedInputHashType,
	))

	// The legacy digest is a different one.
	legacy, err := SharedInputSigHash(
		sp.res, sp.witnessScript, txscript.SigHashAll,
	)
	require.NoError(t, err)
	require.NotEqual(t, digest, legacy)
	require.Error(t, VerifySharedInputSig(
		sp.res, sp.witnessScript, ecdsa.Sign(sp.fundB, legacy),
		sp.fundB.PubKey(), SharedInputHashType,
	))

	// The full witness passes the chain's script engine.
	tx := sp.res.Tx.Copy()
	idx := int(sp.res.SharedIndex)
	tx.TxIn[idx].Witness = input.SpendMultiSig(
		sp.witnessScript,
		sp.fundA.PubKey().SerializeCompressed(), sigA,
		sp.fundB.PubKey().SerializeCompressed(), sigB,
		SharedInputHashType,
	)
	fetcher := PrevOutFetcher(sp.res)
	vm, err := txscript.NewEngine(
		sp.res.Inputs[idx].PkScript, tx, idx,
		txscript.StandardVerifyFlags, nil,
		txscript.NewTxSigHashes(tx, fetcher),
		int64(sp.res.Inputs[idx].Value), fetcher,
	)
	require.NoError(t, err)
	require.NoError(t, vm.Execute())

	// The unified digest commits to the other inputs' spent outputs:
	// change one and the signature no longer verifies.
	other := *sp.res
	other.Inputs = append([]*Input(nil), sp.res.Inputs...)
	for i, in := range other.Inputs {
		if !in.Shared {
			changed := *in
			changed.Value++
			other.Inputs[i] = &changed

			break
		}
	}
	require.Error(t, VerifySharedInputSig(
		&other, sp.witnessScript, sigB, sp.fundB.PubKey(),
		SharedInputHashType,
	))
}
