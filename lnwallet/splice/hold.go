package splice

import (
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/lnwallet/interactivetx"
)

// A peer that put coins into a splice could spend them through us once the
// splice locks, then reorganise the chain to double-spend its inputs: the
// channel would be back on its old funding, without those coins, after we
// paid out what they bought. On a chain with little hashrate that is
// affordable for a while. So until the splice has PeerInputConfs
// confirmations, the peer's spliced-in amount is held: the link fails back
// incoming HTLCs that would draw on it.

// Signing reports whether a splice is being signed, its tx_signatures not
// yet exchanged both ways, and which side started it: the channel stays
// quiescent until they are, across reconnections. Safe from any goroutine.
func (m *Machine) Signing() (bool, lntypes.ChannelParty) {
	m.mu.Lock()
	defer m.mu.Unlock()

	a := m.active
	if a == nil || a.phase != phaseSigning || a.disowned {
		return false, lntypes.Local
	}
	if a.role == interactivetx.Initiator {
		return true, lntypes.Local
	}

	return true, lntypes.Remote
}

// Disowned reports whether the peer disowned the splice, our updates then
// held, and whether the channel is being closed for it: no earlier
// transaction of the splice is live, which would lock in time. Safe from any
// goroutine.
func (m *Machine) Disowned() (bool, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	a := m.active
	if a == nil || !a.disowned {
		return false, false
	}

	return true, len(a.replaced) == 0
}

// LockComing reports whether the splice's lock is on its way to the link:
// both splice_locked are in, or ours is and the peer's for txid, just
// received, completes it. The peer, locked on its side, may send updates its
// new balance covers before our lock reaches the link. Safe from any
// goroutine.
func (m *Machine) LockComing(txid chainhash.Hash) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	a := m.active
	if a == nil || a.lockDone || !a.lockedSent {
		return false
	}

	// On its funding already: the lock reached the link.
	if m.cfg.Channel.State().CurrentFundingOutpoint().Hash == a.txid {
		return false
	}

	return a.lockedReceived || a.txid == txid
}

// Preparing reports whether a splice of ours is still waiting for
// quiescence, nothing of it sent but stfu: the peer, knowing nothing of it,
// may send shutdown meanwhile, and it yields. Safe from any goroutine.
func (m *Machine) Preparing() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	a := m.active

	return a != nil && a.phase == phasePreparing &&
		a.role == interactivetx.Initiator
}

// Busy reports whether a splice is under way, from its preparation on, also
// while ours waits for quiescence: a cooperative close then would race it.
// One the peer disowned doesn't count: the close, spending the output it
// spends, is how it ends. Safe from any goroutine.
func (m *Machine) Busy() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.active != nil && !m.active.disowned
}

// Constructing reports whether a splice's transaction is being negotiated,
// or agreed and not yet added to the channel's fundings: a commitment_signed
// naming a funding the channel doesn't have may be its own. Safe from any
// goroutine.
func (m *Machine) Constructing() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	a := m.active

	return a != nil && (a.phase == phaseNegotiating ||
		a.phase == phaseSigning)
}

// PeerInputHold returns what of the peer's balance is held: the sum of its
// contributions to splices that locked and haven't reached PeerInputConfs.
// Safe to call from any goroutine.
func (m *Machine) PeerInputHold() btcutil.Amount {
	m.mu.Lock()
	defer m.mu.Unlock()

	var held btcutil.Amount
	for _, amt := range m.holds {
		held += amt
	}

	return held
}

// holdPeerInput holds the peer's contribution to a splice about to lock
// until it has PeerInputConfs confirmations. The hold is recorded in the
// channel's state, so it outlives the connection and a restart whatever
// splices come after.
func (m *Machine) holdPeerInput(txid chainhash.Hash, peer btcutil.Amount,
	pkScript []byte, heightHint uint32) {

	if peer <= 0 || m.cfg.Policy.PeerInputConfs == 0 {
		return
	}

	h := channeldb.SpliceHold{
		Txid: txid, Amount: peer, PkScript: pkScript,
		HeightHint: heightHint,
	}
	if err := m.cfg.Channel.State().PutSpliceHold(h); err != nil {
		// Held on this connection anyway.
		log.Errorf("ChannelID(%v): unable to record the hold of the "+
			"peer's coins from splice %v: %v", m.cfg.ChanID, txid,
			err)
	}
	m.activateHold(h)
}

// releaseHold drops a splice's hold: it didn't lock after all.
func (m *Machine) releaseHold(txid chainhash.Hash) {
	m.mu.Lock()
	delete(m.holds, txid)
	m.mu.Unlock()

	if err := m.cfg.Channel.State().DeleteSpliceHold(txid); err != nil {
		log.Errorf("ChannelID(%v): unable to drop the hold of splice "+
			"%v: %v", m.cfg.ChanID, txid, err)
	}
}

// activateHold holds a recorded hold's amount until the watch says the
// splice is deep enough. Without the watch the hold stays: the next start
// tries again.
func (m *Machine) activateHold(h channeldb.SpliceHold) {
	confs := m.cfg.Policy.PeerInputConfs
	if confs == 0 {
		return
	}

	m.mu.Lock()
	m.holds[h.Txid] = h.Amount
	m.mu.Unlock()
	log.Infof("ChannelID(%v): holding the peer's %v from splice %v until "+
		"%d confirmations", m.cfg.ChanID, h.Amount, h.Txid, confs)

	txid := h.Txid
	conf, err := m.cfg.Chain.RegisterConfirmationsNtfn(
		&txid, h.PkScript, confs, h.HeightHint,
	)
	if err != nil {
		log.Errorf("ChannelID(%v): unable to watch splice %v for the "+
			"peer's coins, held until a restart: %v", m.cfg.ChanID,
			txid, err)
		return
	}

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer conf.Cancel()

		select {
		case _, ok := <-conf.Confirmed:
			if !ok {
				return
			}
			m.releaseHold(txid)
			log.Infof("ChannelID(%v): the peer's coins from splice "+
				"%v are %d deep, no longer held", m.cfg.ChanID,
				txid, confs)

		case <-m.quit:
		}
	}()
}

// restoreHold holds the peer's coins again after a restart or a
// reconnection, from every hold the channel records. A watch for a splice
// already deep enough releases its hold at once.
func (m *Machine) restoreHold() error {
	holds, err := m.cfg.Channel.State().SpliceHolds()
	if err != nil {
		return err
	}
	for _, h := range holds {
		m.activateHold(h)
	}

	return nil
}
