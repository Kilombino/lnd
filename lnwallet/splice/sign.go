package splice

import (
	"errors"
	"fmt"

	"github.com/btcsuite/btcd/btcec/v2/ecdsa"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/fn/v2"
	"github.com/lightningnetwork/lnd/input"
	"github.com/lightningnetwork/lnd/lnwallet/interactivetx"
	"github.com/lightningnetwork/lnd/lnwire"
)

// The splice's signatures. Its own commitment_signed goes both ways first;
// then tx_signatures, the side that put in less first. From the moment ours
// leaves, the splice is ours to keep: it stays in the channel's funding set
// whatever the peer does.

// handleCommitReceived takes the link's result for the peer's
// commitment_signed for the splice.
func (m *Machine) handleCommitReceived(txid [32]byte, err error) {
	a := m.active
	if a == nil || a.txid != txid || a.phase != phaseSigning {
		log.Warnf("ChannelID(%v): commitment_signed for splice %x "+
			"not expected", m.cfg.ChanID, txid)
		return
	}

	if err != nil {
		// Refused before our tx_signatures left: nothing is lost.
		if a.ourTxSigs == nil {
			m.abort(codeInvalidCommitment, "%v", err)
			m.forget()
		}

		return
	}

	m.update(func(a *active) { a.commitReceived = true })
	m.maybeSendTxSignatures()
}

// signsFirst reports whether we send tx_signatures first.
func (m *Machine) signsFirst() bool {
	a := m.active

	return interactivetx.SignsFirst(
		a.res, a.role, m.cfg.OurNodeID[:], m.cfg.TheirNodeID[:],
	)
}

// maybeSendTxSignatures sends our tx_signatures once both commitments are
// exchanged, if it's our turn: we go first, or the peer's arrived.
func (m *Machine) maybeSendTxSignatures() {
	a := m.active
	if a == nil || a.phase != phaseSigning || a.ourTxSigs != nil ||
		!a.commitSent || !a.commitReceived {

		return
	}
	if !m.signsFirst() && a.theirTxSigs == nil {
		return
	}

	sigs, err := m.ourTxSignatures()
	if err != nil {
		m.abort(codeInternal, "%v", err)
		m.forget()

		return
	}

	// Signing last, the transaction is complete with ours: it must be
	// one the backend accepts before ours leave, since after that the
	// splice can't be dropped.
	if !m.signsFirst() && m.cfg.CheckMempool != nil {
		a.ourTxSigs = sigs
		tx, err := m.signedTx()
		a.ourTxSigs = nil
		if err == nil {
			err = m.cfg.CheckMempool(tx)
		}
		if err != nil {
			m.abort("relay_rejected", "%v", err)
			m.forget()

			return
		}
	}

	// Recorded before it leaves: from then on the splice is ours to
	// keep.
	err = m.cfg.Channel.State().UpdateSpliceRecord(a.txid,
		func(r *channeldb.SpliceRecord) error {
			if r.Negotiation == nil {
				return errors.New("splice record without " +
					"negotiation")
			}
			r.Negotiation.OurTxSignatures = sigs

			return nil
		})
	if err != nil {
		m.abort(codeInternal, "%v", err)
		m.forget()

		return
	}
	m.update(func(a *active) { a.ourTxSigs = sigs })
	m.send(sigs)
	m.notify("signed", a.txid, "", false)
	m.report(Update{State: StateSigned, Txid: a.txid})

	if a.theirTxSigs != nil {
		m.signed()
		return
	}

	// We went first: the peer holds what it needs to publish the
	// splice, with its tx_signatures or without sending them.
	m.watchWithheld()
}

// ourTxSignatures signs our part of the splice transaction: the shared
// input with our current funding key, and our wallet inputs.
func (m *Machine) ourTxSignatures() (*lnwire.TxSignatures, error) {
	a := m.active
	res := a.res
	fetcher := interactivetx.PrevOutFetcher(res)

	_, _, witnessScript, _, err := m.currentFunding()
	if err != nil {
		return nil, err
	}
	state := m.cfg.Channel.State()

	tx := res.Tx.Copy()
	sigHashes := txscript.NewTxSigHashes(tx, fetcher)
	shared := res.Inputs[res.SharedIndex]
	rawSig, err := m.cfg.Signer.SignOutputRaw(tx, &input.SignDescriptor{
		KeyDesc:       state.LocalChanCfg.MultiSigKey,
		WitnessScript: witnessScript,
		Output: &wire.TxOut{
			Value: int64(shared.Value), PkScript: shared.PkScript,
		},
		HashType:          interactivetx.SharedInputHashType,
		SigHashes:         sigHashes,
		PrevOutputFetcher: fetcher,
		InputIndex:        int(res.SharedIndex),
	})
	if err != nil {
		return nil, fmt.Errorf("shared input: %w", err)
	}
	sharedSig, err := lnwire.NewSigFromSignature(rawSig)
	if err != nil {
		return nil, err
	}

	// Our wallet inputs, in serial id order as they are in the
	// transaction.
	var witnesses []wire.TxWitness
	for i, in := range res.Inputs {
		if in.Shared || !ourSerial(a.role, in.SerialID) {
			continue
		}
		taproot := txscript.IsPayToTaproot(in.PkScript)
		signDesc := &input.SignDescriptor{
			Output: &wire.TxOut{
				Value: int64(in.Value), PkScript: in.PkScript,
			},
			HashType:          input.SoleSignerSigHash(taproot),
			SigHashes:         sigHashes,
			PrevOutputFetcher: fetcher,
			InputIndex:        i,
		}
		if taproot {
			signDesc.SignMethod =
				input.TaprootKeySpendBIP0086SignMethod
		}
		script, err := m.cfg.Signer.ComputeInputScript(tx, signDesc)
		if err != nil {
			return nil, fmt.Errorf("input %v: %w", in.OutPoint, err)
		}
		witnesses = append(witnesses, script.Witness)
	}

	return &lnwire.TxSignatures{
		ChannelID:            m.cfg.ChanID,
		TxID:                 a.txid,
		Witnesses:            witnesses,
		SharedInputSignature: fn.Some(sharedSig),
	}, nil
}

// ourSerial reports whether role added the item with serial id.
func ourSerial(role interactivetx.Role, serialID uint64) bool {
	initiator := serialID%2 == 0
	if role == interactivetx.Initiator {
		return initiator
	}

	return !initiator
}

// handleTxSignatures takes the peer's tx_signatures: checked against the
// transaction under the chain's rules before anything else.
func (m *Machine) handleTxSignatures(msg *lnwire.TxSignatures) {
	a := m.active
	switch {
	case a == nil || a.txid != msg.TxID:
		m.warnAndDisconnect("tx_signatures for unknown splice %v",
			msg.TxID)
		return

	case a.theirTxSigs != nil:
		// Sent again after a reconnection.
		return

	case a.phase != phaseSigning || !a.commitReceived:
		m.warnAndDisconnect("tx_signatures before commitment_signed")
		return

	case m.signsFirst() && a.ourTxSigs == nil:
		m.warnAndDisconnect("tx_signatures out of turn")
		return
	}

	if err := m.checkTheirTxSignatures(msg); err != nil {
		// Once ours left, the splice can't be forgotten: the peer
		// could still publish it. Otherwise refuse it.
		if a.ourTxSigs == nil {
			m.abort(codeInvalidSignatures, "%v", err)
			m.forget()
		} else {
			log.Errorf("ChannelID(%v): invalid tx_signatures for "+
				"splice %v after ours: %v", m.cfg.ChanID,
				a.txid, err)
			m.setMisbehaved()
		}

		return
	}

	err := m.cfg.Channel.State().UpdateSpliceRecord(a.txid,
		func(r *channeldb.SpliceRecord) error {
			r.Negotiation.TheirTxSignatures = msg
			return nil
		})
	if err != nil {
		log.Errorf("ChannelID(%v): unable to record tx_signatures: %v",
			m.cfg.ChanID, err)
		return
	}
	m.update(func(a *active) { a.theirTxSigs = msg })

	if a.ourTxSigs == nil {
		m.maybeSendTxSignatures()
		return
	}
	m.signed()
}

// checkTheirTxSignatures checks the peer's witnesses for its inputs and its
// signature for the shared input, which must be for the unified digest.
func (m *Machine) checkTheirTxSignatures(msg *lnwire.TxSignatures) error {
	a := m.active
	peer := interactivetx.Initiator
	if a.role == interactivetx.Initiator {
		peer = interactivetx.Acceptor
	}

	err := interactivetx.CheckPeerWitnesses(
		a.res, peer, msg.Witnesses, true,
	)
	if err != nil {
		return err
	}

	sig, err := msg.SharedInputSignature.UnwrapOrErr(
		errors.New("no shared input signature"),
	)
	if err != nil {
		return err
	}
	ecSig, err := sig.ToSignature()
	if err != nil {
		return err
	}
	parsed, ok := ecSig.(*ecdsa.Signature)
	if !ok {
		return errors.New("shared input signature isn't ECDSA")
	}
	_, _, witnessScript, _, err := m.currentFunding()
	if err != nil {
		return err
	}

	return interactivetx.VerifySharedInputSig(
		a.res, witnessScript, parsed,
		m.cfg.Channel.State().RemoteChanCfg.MultiSigKey.PubKey,
		interactivetx.SharedInputHashType,
	)
}

// signedTx assembles the splice transaction with every witness.
func (m *Machine) signedTx() (*wire.MsgTx, error) {
	a := m.active
	res := a.res
	tx := res.Tx.Copy()

	ourShared, err := a.ourTxSigs.SharedInputSignature.UnwrapOrErr(
		errors.New("no shared input signature of ours"),
	)
	if err != nil {
		return nil, err
	}
	theirShared, err := a.theirTxSigs.SharedInputSignature.UnwrapOrErr(
		errors.New("no shared input signature of theirs"),
	)
	if err != nil {
		return nil, err
	}
	ourSig, err := ourShared.ToSignature()
	if err != nil {
		return nil, err
	}
	theirSig, err := theirShared.ToSignature()
	if err != nil {
		return nil, err
	}

	_, _, witnessScript, _, err := m.currentFunding()
	if err != nil {
		return nil, err
	}
	state := m.cfg.Channel.State()
	tx.TxIn[res.SharedIndex].Witness = input.SpendMultiSig(
		witnessScript,
		state.LocalChanCfg.MultiSigKey.PubKey.SerializeCompressed(),
		ourSig,
		state.RemoteChanCfg.MultiSigKey.PubKey.SerializeCompressed(),
		theirSig, interactivetx.SharedInputHashType,
	)

	ours, theirs := a.ourTxSigs.Witnesses, a.theirTxSigs.Witnesses
	for i, in := range res.Inputs {
		if in.Shared {
			continue
		}
		if ourSerial(a.role, in.SerialID) {
			tx.TxIn[i].Witness, ours = ours[0], ours[1:]
		} else {
			tx.TxIn[i].Witness, theirs = theirs[0], theirs[1:]
		}
	}

	return tx, nil
}

// signed finishes the signing: the transaction is published, quiescence
// ends, and the splice waits for its confirmations.
func (m *Machine) signed() {
	a := m.active
	tx, err := m.signedTx()
	if err != nil {
		log.Errorf("ChannelID(%v): unable to assemble splice %v: %v",
			m.cfg.ChanID, a.txid, err)
		return
	}

	status := m.publish(tx)
	if status.State == RelayRejected {
		log.Errorf("ChannelID(%v): splice %v rejected: %v",
			m.cfg.ChanID, a.txid, status.Reason)
	}

	height, _ := m.cfg.Chain.BestHeight()
	err = m.cfg.Channel.State().UpdateSpliceRecord(a.txid,
		func(r *channeldb.SpliceRecord) error {
			if r.Negotiation.BroadcastHeight == 0 {
				r.Negotiation.BroadcastHeight = height
			}

			return nil
		})
	if err != nil {
		log.Errorf("ChannelID(%v): unable to record broadcast: %v",
			m.cfg.ChanID, err)
	}

	m.update(func(a *active) { a.phase = phasePending })
	m.report(Update{State: StateBroadcast, Txid: a.txid})
	m.resume()
	m.watchConfirmation(height)
	m.watchPending(height)
}

// label returns the splice transaction's wallet label.
func (m *Machine) label() string {
	dir := DirectionIn
	if m.active.ourContribution < 0 {
		dir = DirectionOut
	}

	return Label(m.cfg.Channel.State().ShortChannelID, dir)
}

// handleTxAbort takes the peer's tx_abort. Before our tx_signatures the
// splice is forgotten and the abort echoed back; after, it is kept, since
// the peer could still publish it, and the peer's splices are refused from
// then on.
func (m *Machine) handleTxAbort(msg *lnwire.TxAbort) {
	a := m.active
	reason := printable(msg.Data)

	switch {
	case a == nil:
		log.Debugf("ChannelID(%v): tx_abort without a splice: %v",
			m.cfg.ChanID, reason)

	case a.phase == phasePending || a.ourTxSigs != nil:
		log.Errorf("ChannelID(%v): peer aborted splice %v after our "+
			"tx_signatures: %v", m.cfg.ChanID, a.txid, reason)
		m.setMisbehaved()

	default:
		log.Infof("ChannelID(%v): peer aborted the splice: %v",
			m.cfg.ChanID, reason)
		m.update(func(a *active) {
			a.abortReason = reason
			a.reasonFromPeer = true
		})
		m.send(&lnwire.TxAbort{ChannelID: m.cfg.ChanID})
		m.forget()
	}
}

// printable returns the peer's text as printable ASCII, at most 160 bytes.
func printable(data []byte) string {
	if len(data) > 160 {
		data = data[:160]
	}
	out := make([]byte, 0, len(data))
	for _, c := range data {
		if c >= 0x20 && c < 0x7f {
			out = append(out, c)
		}
	}

	return string(out)
}
