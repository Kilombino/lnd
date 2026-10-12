package splice

import (
	"errors"
	"fmt"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/chainntnfs"
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
	sent := a.lockedSent
	m.update(func(a *active) {
		a.lockedSent = true
		a.scid = fn.Some(scid)
	})

	// Sent before, on this connection or the last: the peer has it, or
	// gets it from our channel_reestablish. Before the reestablish it
	// waits for it.
	m.mu.Lock()
	reestablished := m.reestablished
	m.mu.Unlock()
	if !sent && reestablished {
		m.send(&lnwire.SpliceLocked{
			ChannelID: m.cfg.ChanID, SpliceTxid: txid,
		})
	}

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
	m.recordLast("locked", "", false)
	m.notify("locked", a.txid, "", false)
	m.report(Update{State: StateLocked, Txid: a.txid})
	m.update(func(a *active) { a.lockDone = true })
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
		m.lockedTLV = a.txid
	}

	// The announcement on the funding a splice moved the channel to, if
	// the peer's half hasn't come.
	state := m.cfg.Channel.State()
	current := state.CurrentFundingOutpoint().Hash
	moved := current != state.FundingOutpoint.Hash
	locked = fn.MapOption(func(
		f lnwire.FundingTxidFlags) lnwire.FundingTxidFlags {

		if moved && f.Txid == current &&
			m.cfg.AnnouncementPending != nil &&
			m.cfg.AnnouncementPending() {

			f.RetransmitFlags |= lnwire.RetransmitAnnounceSigs
		}

		return f
	})(locked)

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

	// Our splice_locked, if due and not named by our reestablish, goes
	// now.
	m.mu.Lock()
	m.reestablished = true
	namedLocked := m.lockedTLV
	m.mu.Unlock()
	if a != nil && a.phase == phasePending && a.lockedSent &&
		namedLocked != a.txid {

		m.send(&lnwire.SpliceLocked{
			ChannelID: m.cfg.ChanID, SpliceTxid: a.txid,
		})
	}

	msg.MyCurrentFundingLocked.WhenSome(func(f lnwire.FundingTxidFlags) {
		if a != nil && a.phase == phasePending && a.txid == f.Txid {
			m.handleSpliceLocked(f.Txid)
		}

		// The peer asks for our half of the announcement on the
		// funding a splice moved the channel to.
		state := m.cfg.Channel.State()
		current := state.CurrentFundingOutpoint().Hash
		if f.RetransmitFlags&lnwire.RetransmitAnnounceSigs != 0 &&
			f.Txid == current &&
			current != state.FundingOutpoint.Hash &&
			m.cfg.ResendAnnouncement != nil {

			m.cfg.ResendAnnouncement()
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
		if role == interactivetx.Acceptor {
			m.cfg.Limits.admit(
				m, m.cfg.ChanID, m.cfg.TheirNodeID, true,
			)
		}

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
	m.watchPending(broadcastHeight)
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

// Depths at which a splice whose input was spent elsewhere is given up on,
// and at which its trace leaves the wallet.
const (
	deadDepth   = 6
	forgetDepth = 144
)

// watchPending starts what a published splice needs until it locks: it is
// published again at every block until it confirms (an evicted splice would
// otherwise only go out again at restart), and its inputs are watched for a
// spend by another transaction, which kills it.
func (m *Machine) watchPending(heightHint uint32) {
	a := m.active
	txid := a.txid

	m.wg.Add(1)
	go m.rebroadcastUntilConfirmed(txid, a.fundingScript, heightHint)

	for _, in := range a.res.Inputs {
		m.wg.Add(1)
		go m.watchInput(txid, in.OutPoint, in.PkScript, heightHint)
	}
}

// rebroadcastUntilConfirmed publishes the splice again at every block until
// its first confirmation.
func (m *Machine) rebroadcastUntilConfirmed(txid chainhash.Hash,
	script []byte, heightHint uint32) {

	defer m.wg.Done()

	conf, err := m.cfg.Chain.RegisterConfirmationsNtfn(
		&txid, script, 1, heightHint,
	)
	if err != nil {
		log.Errorf("ChannelID(%v): unable to watch splice %v: %v",
			m.cfg.ChanID, txid, err)
		return
	}
	defer conf.Cancel()

	epochs, err := m.cfg.Chain.RegisterBlockEpochNtfn(nil)
	if err != nil {
		log.Errorf("ChannelID(%v): unable to watch blocks: %v",
			m.cfg.ChanID, err)
		return
	}
	defer epochs.Cancel()

	for {
		select {
		case _, ok := <-epochs.Epochs:
			if !ok {
				return
			}
			m.enqueue(func() {
				a := m.active
				if a == nil || a.txid != txid ||
					a.phase != phasePending {

					return
				}
				tx, err := m.signedTx()
				if err != nil {
					return
				}
				status := Publish(m.cfg.Wallet, tx, m.label())
				if status.State == RelayRejected {
					log.Warnf("ChannelID(%v): splice %v "+
						"not relayed: %v", m.cfg.ChanID,
						txid, status.Reason)
				}
			})

		case <-conf.Confirmed:
			return

		case <-m.quit:
			return
		}
	}
}

// watchInput watches an input of the splice. Spent by the splice, it is
// done; spent by anything else, the splice is dead once that spend is
// deadDepth deep, and leaves the wallet once forgetDepth deep. A reorg
// taking the other spend out puts the input back under watch.
func (m *Machine) watchInput(txid chainhash.Hash, op wire.OutPoint,
	script []byte, heightHint uint32) {

	defer m.wg.Done()

	for {
		spendNtfn, err := m.cfg.Chain.RegisterSpendNtfn(
			&op, script, heightHint,
		)
		if err != nil {
			log.Errorf("ChannelID(%v): unable to watch splice input "+
				"%v: %v", m.cfg.ChanID, op, err)
			return
		}

		var spend *chainntnfs.SpendDetail
		select {
		case s, ok := <-spendNtfn.Spend:
			spendNtfn.Cancel()
			if !ok {
				return
			}
			spend = s
		case <-m.quit:
			spendNtfn.Cancel()
			return
		}
		if *spend.SpenderTxHash == txid {
			return
		}

		conflict := *spend.SpenderTxHash
		height := uint32(spend.SpendingHeight)
		outScript := spend.SpendingTx.TxOut[0].PkScript

		dead, reorged := m.waitDepth(conflict, outScript, deadDepth,
			height)
		if reorged {
			continue
		}
		if !dead {
			return
		}
		m.enqueue(func() { m.spliceDead(txid, conflict) })

		forget, reorged := m.waitDepth(conflict, outScript, forgetDepth,
			height)
		if reorged {
			continue
		}
		if forget {
			m.enqueue(func() { m.forgetDead(txid) })
		}

		return
	}
}

// waitDepth waits for a transaction to reach a depth: true when it does,
// reorged when a reorg takes it out first, both false when stopping.
func (m *Machine) waitDepth(txid chainhash.Hash, script []byte,
	depth, heightHint uint32) (bool, bool) {

	conf, err := m.cfg.Chain.RegisterConfirmationsNtfn(
		&txid, script, depth, heightHint,
	)
	if err != nil {
		log.Errorf("ChannelID(%v): unable to watch %v: %v",
			m.cfg.ChanID, txid, err)
		return false, false
	}
	defer conf.Cancel()

	select {
	case <-conf.Confirmed:
		return true, false
	case <-conf.NegativeConf:
		return false, true
	case <-m.quit:
		return false, false
	}
}

// spliceDead gives up on a splice one of whose inputs another transaction
// spent: it leaves the channel's funding set, and its coins and wallet trace
// wait for forgetDead.
func (m *Machine) spliceDead(txid, conflict chainhash.Hash) {
	a := m.active
	if a == nil || a.txid != txid {
		return
	}

	tx, err := m.signedTx()
	if err != nil {
		tx = a.res.Tx
	}
	m.dead[txid] = deadSplice{tx: tx, inputs: a.ourInputs}

	if err := m.wait(m.cfg.Link.RemoveSpliceFunding(txid)); err != nil {
		log.Errorf("ChannelID(%v): unable to drop dead splice %v: %v",
			m.cfg.ChanID, txid, err)
	}

	reason := fmt.Sprintf("dead: an input was spent by %v", conflict)
	log.Warnf("ChannelID(%v): splice %v is %v", m.cfg.ChanID, txid,
		reason)
	m.recordLast("dead", reason, false)
	m.notify("dead", txid, reason, false)
	m.report(Update{State: StateAborted, Txid: txid, Reason: reason})
	m.setActive(nil)
}

// forgetDead takes a dead splice out of the wallet, so the coins it spent
// count again, and releases its leases.
func (m *Machine) forgetDead(txid chainhash.Hash) {
	d, ok := m.dead[txid]
	if !ok {
		return
	}
	delete(m.dead, txid)

	if err := Forget(m.cfg.Wallet, d.tx, d.inputs); err != nil {
		log.Errorf("ChannelID(%v): unable to forget dead splice %v: %v",
			m.cfg.ChanID, txid, err)
	}
}
