package splice

import (
	"errors"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/fn/v2"
	"github.com/lightningnetwork/lnd/input"
	"github.com/lightningnetwork/lnd/lnwallet/chainfee"
	"github.com/lightningnetwork/lnd/lnwallet/interactivetx"
	"github.com/lightningnetwork/lnd/lnwire"
)

// A signed splice waits for its confirmations. At our lock depth we send
// splice_locked; once both sides have sent it, the link moves the channel to
// the new funding output. Everything here survives restarts: the machine
// rebuilds the splice in progress from its record.

// watchConfirmation waits for the splice to reach our lock depth.
func (m *Machine) watchConfirmation(heightHint uint32) {
	a := m.active
	depth := uint32(6)
	if m.cfg.Policy.LockDepth != nil {
		peer := a.theirContribution
		if peer < 0 {
			peer = 0
		}
		depth = m.cfg.Policy.LockDepth(peer)
	}

	txid := a.txid
	conf, err := m.cfg.Chain.RegisterConfirmationsNtfn(
		&txid, a.fundingScript, depth, heightHint,
	)
	if err != nil {
		log.Errorf("ChannelID(%v): unable to watch splice %v: %v",
			m.cfg.ChanID, txid, err)
		return
	}

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer conf.Cancel()

		select {
		case details, ok := <-conf.Confirmed:
			if !ok {
				return
			}
			scid := lnwire.ShortChannelID{
				BlockHeight: details.BlockHeight,
				TxIndex:     details.TxIndex,
				TxPosition:  uint16(a.fundingIndex),
			}
			m.enqueue(func() { m.reachedDepth(txid, scid) })

		case <-m.quit:
		}
	}()
}

// reachedDepth sends splice_locked once the splice is deep enough.
func (m *Machine) reachedDepth(txid chainhash.Hash,
	scid lnwire.ShortChannelID) {

	a := m.active
	if a == nil || a.txid != txid {
		return
	}

	err := m.cfg.Channel.State().UpdateSpliceRecord(txid,
		func(r *channeldb.SpliceRecord) error {
			r.Negotiation.LockedSent = true
			return nil
		})
	if err != nil {
		log.Errorf("ChannelID(%v): unable to record splice_locked: "+
			"%v", m.cfg.ChanID, err)
		return
	}
	m.update(func(a *active) {
		a.lockedSent = true
		a.scid = fn.Some(scid)
	})
	m.send(&lnwire.SpliceLocked{
		ChannelID: m.cfg.ChanID, SpliceTxid: txid,
	})

	m.maybeComplete()
}

// handleSpliceLocked takes the peer's splice_locked.
func (m *Machine) handleSpliceLocked(txid chainhash.Hash) {
	a := m.active
	switch {
	case a == nil:
		// Already completed: the peer sent it again on reconnecting.
		return

	case a.txid != txid || a.phase != phasePending:
		m.warnAndDisconnect("splice_locked for %v, not the pending "+
			"splice", txid)
		return

	case a.lockedReceived:
		return
	}

	err := m.cfg.Channel.State().UpdateSpliceRecord(txid,
		func(r *channeldb.SpliceRecord) error {
			r.Negotiation.LockedReceived = true
			return nil
		})
	if err != nil {
		log.Errorf("ChannelID(%v): unable to record the peer's "+
			"splice_locked: %v", m.cfg.ChanID, err)
		return
	}
	m.update(func(a *active) { a.lockedReceived = true })

	m.maybeComplete()
}

// maybeComplete moves the channel to the splice's funding output once both
// sides have locked it.
func (m *Machine) maybeComplete() {
	a := m.active
	if !a.lockedSent || !a.lockedReceived {
		return
	}
	scid, err := a.scid.UnwrapOrErr(errors.New("no scid"))
	if err != nil {
		return
	}

	if err := m.wait(m.cfg.Link.LockSplice(a.txid, scid)); err != nil {
		log.Errorf("ChannelID(%v): unable to complete splice %v: %v",
			m.cfg.ChanID, a.txid, err)
		return
	}
	log.Infof("ChannelID(%v): splice %v locked at %v", m.cfg.ChanID,
		a.txid, scid)

	if len(a.ourInputs) > 0 {
		if err := ReleaseCoins(m.cfg.Wallet, a.ourInputs); err != nil {
			log.Errorf("ChannelID(%v): unable to release coins: %v",
				m.cfg.ChanID, err)
		}
	}
	m.setActive(nil)
}

// ReestablishTLVs returns our splice fields for channel_reestablish.
func (m *Machine) ReestablishTLVs() (fn.Option[lnwire.FundingTxidFlags],
	fn.Option[lnwire.FundingTxidFlags]) {

	m.mu.Lock()
	defer m.mu.Unlock()

	var next fn.Option[lnwire.FundingTxidFlags]
	locked := fn.Some(lnwire.FundingTxidFlags{
		Txid: m.cfg.Channel.State().CurrentFundingOutpoint().Hash,
	})

	a := m.active
	switch {
	case a == nil:

	// Our commitment_signed went out and the peer's tx_signatures
	// haven't come: next_funding, asking for its commitment_signed if
	// that hasn't come either.
	case a.phase == phaseSigning && a.commitSent:
		var flags uint8
		if !a.commitReceived {
			flags |= lnwire.RetransmitCommitSig
		}
		next = fn.Some(lnwire.FundingTxidFlags{
			Txid: a.txid, RetransmitFlags: flags,
		})

	case a.phase == phasePending && a.lockedSent:
		locked = fn.Some(lnwire.FundingTxidFlags{Txid: a.txid})
	}

	return next, locked
}

// HandleReestablish takes the peer's channel_reestablish.
func (m *Machine) HandleReestablish(msg *lnwire.ChannelReestablish) {
	m.enqueue(func() { m.handleReestablish(msg) })
}

// handleReestablish resends what the peer's next_funding asks for, refuses
// a next_funding we don't know, and treats my_current_funding_locked naming
// the pending splice as its splice_locked.
func (m *Machine) handleReestablish(msg *lnwire.ChannelReestablish) {
	// A splice still negotiating can't survive the disconnection.
	if m.active != nil && m.active.phase == phaseNegotiating {
		m.forget()
	}
	a := m.active

	msg.NextFunding.WhenSome(func(next lnwire.FundingTxidFlags) {
		switch {
		case a == nil || a.txid != next.Txid:
			m.send(&lnwire.TxAbort{
				ChannelID: m.cfg.ChanID,
				Data:      []byte("unknown funding transaction"),
			})

		// Ours already went out: again.
		case a.ourTxSigs != nil:
			m.send(a.ourTxSigs)

		// Our turn now that both commitments are in.
		default:
			m.maybeSendTxSignatures()
		}
	})

	msg.MyCurrentFundingLocked.WhenSome(func(f lnwire.FundingTxidFlags) {
		if a != nil && a.phase == phasePending && a.txid == f.Txid {
			m.handleSpliceLocked(f.Txid)
		}
	})
}

// restore rebuilds the splice in progress from the channel's records after a
// restart.
func (m *Machine) restore() error {
	records, err := m.cfg.Channel.State().SpliceRecords()
	if err != nil {
		return err
	}

	for _, r := range records {
		n := r.Negotiation
		if r.Retired || n == nil || n.Tx == nil {
			continue
		}

		res := &interactivetx.Result{
			Tx:           n.Tx,
			FundingIndex: r.OutPoint.Index,
		}
		for i, in := range n.Inputs {
			if i >= len(n.Tx.TxIn) {
				return errors.New("splice record inputs don't " +
					"match its transaction")
			}
			res.Inputs = append(res.Inputs, &interactivetx.Input{
				SerialID: in.SerialID,
				OutPoint: n.Tx.TxIn[i].PreviousOutPoint,
				Value:    in.Value,
				PkScript: in.PkScript,
				Shared:   in.Shared,
			})
			if in.Shared {
				res.SharedIndex = uint32(i)
			}
		}

		witness, out, err := input.GenFundingPkScript(
			r.LocalKey.PubKey.SerializeCompressed(),
			r.RemoteKey.SerializeCompressed(), int64(r.Capacity),
		)
		if err != nil {
			return err
		}

		role := interactivetx.Acceptor
		if n.WeInitiated {
			role = interactivetx.Initiator
		}
		a := &active{
			phase:             phaseSigning,
			role:              role,
			res:               res,
			txid:              r.Txid(),
			fundingIndex:      r.OutPoint.Index,
			fundingScript:     out.PkScript,
			fundingWitness:    witness,
			ourKey:            r.LocalKey,
			theirKey:          r.RemoteKey,
			ourContribution:   r.LocalDelta,
			theirContribution: r.RemoteDelta,
			capacity:          r.Capacity,
			feeRate:           chainfee.SatPerKWeight(n.FeeRate),
			commitSent:        r.SpliceSig != nil,
			commitReceived:    r.LocalCommit != nil,
			ourTxSigs:         n.OurTxSignatures,
			theirTxSigs:       n.TheirTxSignatures,
			lockedSent:        n.LockedSent,
			lockedReceived:    n.LockedReceived,
		}
		for _, op := range n.OurInputs {
			a.ourInputs = append(a.ourInputs, Coin{OutPoint: op})
		}
		m.active = a

		if a.ourTxSigs != nil && a.theirTxSigs != nil {
			a.phase = phasePending
			m.resumePending(n.BroadcastHeight)
		}

		// One splice at a time in M1.
		break
	}

	return nil
}

// resumePending picks a signed splice up again after a restart: published
// again (the mempool may have dropped it) and watched until it locks.
func (m *Machine) resumePending(broadcastHeight uint32) {
	a := m.active
	tx, err := m.signedTx()
	if err == nil {
		Publish(m.cfg.Wallet, tx, m.label())
	} else {
		log.Errorf("ChannelID(%v): unable to assemble splice %v: %v",
			m.cfg.ChanID, a.txid, err)
	}

	m.watchConfirmation(broadcastHeight)
}

// spliceInputs returns the transaction's inputs as the record keeps them.
func spliceInputs(res *interactivetx.Result) []channeldb.SpliceInput {
	inputs := make([]channeldb.SpliceInput, len(res.Inputs))
	for i, in := range res.Inputs {
		inputs[i] = channeldb.SpliceInput{
			SerialID: in.SerialID,
			Value:    in.Value,
			PkScript: in.PkScript,
			Shared:   in.Shared,
		}
	}

	return inputs
}
