package interactivetx

import (
	"errors"
	"fmt"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/ecdsa"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
)

// The signatures of a splice transaction on this chain.
//
// The shared input, which spends the channel's current funding output, is
// signed with SIGHASH_ALL|SIGHASH_UNIFIED (0x21) on a channel with
// option_unified_sigs. The unified digest commits to every spent output's
// amount and script, so it needs all of them: the shared input's from the
// channel's funding output (its prevtx is never sent), the others from each
// prevtx. Wallet inputs are signed by their owners; a peer's are checked here
// before this node relies on them.

// SharedInputHashType is the hash type of the shared input's signatures on a
// channel with option_unified_sigs.
const SharedInputHashType = txscript.SigHashAll | txscript.SigHashUnified

// PrevOutFetcher returns the spent outputs of every input of the
// transaction, which the unified digest commits to.
func PrevOutFetcher(res *Result) *txscript.MultiPrevOutFetcher {
	fetcher := txscript.NewMultiPrevOutFetcher(nil)
	for _, in := range res.Inputs {
		fetcher.AddPrevOut(in.OutPoint, &wire.TxOut{
			Value: int64(in.Value), PkScript: in.PkScript,
		})
	}

	return fetcher
}

// SharedInputSigHash returns the digest both nodes sign for the shared input:
// witnessScript is the 2-of-2 script of the current funding output.
func SharedInputSigHash(res *Result, witnessScript []byte,
	hashType txscript.SigHashType) ([]byte, error) {

	idx := int(res.SharedIndex)
	if idx >= len(res.Inputs) || !res.Inputs[idx].Shared {
		return nil, errors.New("no shared input at its index")
	}

	fetcher := PrevOutFetcher(res)
	sigHashes := txscript.NewTxSigHashes(res.Tx, fetcher)

	return txscript.CalcWitnessSigHash(
		witnessScript, sigHashes, hashType, res.Tx, idx,
		int64(res.Inputs[idx].Value),
	)
}

// VerifySharedInputSig checks the peer's shared_input_signature, which comes
// without a hash type byte: it is verified against the digest for hashType,
// the one the channel's type implies.
func VerifySharedInputSig(res *Result, witnessScript []byte,
	sig *ecdsa.Signature, peerKey *btcec.PublicKey,
	hashType txscript.SigHashType) error {

	digest, err := SharedInputSigHash(res, witnessScript, hashType)
	if err != nil {
		return err
	}
	if !sig.Verify(digest, peerKey) {
		return errors.New("shared input signature doesn't verify")
	}

	return nil
}

// CheckPeerWitnesses checks the witnesses the peer sent in tx_signatures for
// the inputs it added, in serial id order: each must fit its input type's
// template exactly, use an accepted hash type, and pass full script execution
// against the complete transaction under this chain's rules. unified tells
// whether the channel has option_unified_sigs, which allows 0x21 alongside
// 0x01.
func CheckPeerWitnesses(res *Result, peer Role, witnesses []wire.TxWitness,
	unified bool) error {

	var peerInputs []int
	for i, in := range res.Inputs {
		if !in.Shared && addedBy(in.SerialID) == peer {
			peerInputs = append(peerInputs, i)
		}
	}
	if len(witnesses) != len(peerInputs) {
		return fmt.Errorf("%d witnesses for %d inputs",
			len(witnesses), len(peerInputs))
	}

	fetcher := PrevOutFetcher(res)
	tx := res.Tx.Copy()
	for n, i := range peerInputs {
		tx.TxIn[i].Witness = witnesses[n]
	}
	sigHashes := txscript.NewTxSigHashes(tx, fetcher)

	for n, i := range peerInputs {
		in := res.Inputs[i]
		err := checkWitnessTemplate(in.PkScript, witnesses[n], unified)
		if err != nil {
			return fmt.Errorf("input %d: %w", i, err)
		}

		vm, err := txscript.NewEngine(
			in.PkScript, tx, i, txscript.StandardVerifyFlags, nil,
			sigHashes, int64(in.Value), fetcher,
		)
		if err != nil {
			return fmt.Errorf("input %d: %w", i, err)
		}
		if err := vm.Execute(); err != nil {
			return fmt.Errorf("input %d doesn't verify: %w", i, err)
		}
	}

	return nil
}

// checkWitnessTemplate checks a peer witness against its input type: P2WPKH
// is a signature of at most 73 bytes and a 33-byte key, P2TR a key path
// signature of 64 or 65 bytes without an annex. Anything else was refused
// when the input was added, so it is refused here too.
func checkWitnessTemplate(pkScript []byte, witness wire.TxWitness,
	unified bool) error {

	switch {
	case txscript.IsPayToWitnessPubKeyHash(pkScript):
		if len(witness) != 2 || len(witness[0]) == 0 ||
			len(witness[0]) > 73 || len(witness[1]) != 33 {

			return errors.New("not a P2WPKH witness")
		}

		return checkHashType(
			txscript.SigHashType(witness[0][len(witness[0])-1]),
			unified,
		)

	case txscript.IsPayToTaproot(pkScript):
		if len(witness) != 1 {
			return errors.New("not a P2TR key path witness")
		}
		switch len(witness[0]) {
		// A 64-byte signature is SIGHASH_DEFAULT: it commits to every
		// spent output, like SIGHASH_ALL.
		case 64:
			return nil
		case 65:
			return checkHashType(
				txscript.SigHashType(witness[0][64]), unified,
			)
		default:
			return errors.New("not a P2TR key path signature")
		}

	default:
		return errors.New("input type not accepted from the peer")
	}
}

// checkHashType accepts SIGHASH_ALL, and SIGHASH_ALL|SIGHASH_UNIFIED on a
// unified channel. Anything that signs less than the whole transaction would
// let the peer's input be lifted into another one.
func checkHashType(hashType txscript.SigHashType, unified bool) error {
	switch {
	case hashType == txscript.SigHashAll:
		return nil
	case unified && hashType == SharedInputHashType:
		return nil
	default:
		return fmt.Errorf("hash type %#x not accepted", hashType)
	}
}
