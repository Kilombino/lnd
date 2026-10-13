package splice

import (
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/lightningnetwork/lnd/channeldb"
)

// A peer that put coins into a splice could spend them through us once the
// splice locks, then reorganise the chain to double-spend its inputs: the
// channel would be back on its old funding, without those coins, after we
// paid out what they bought. On a chain with little hashrate that is
// affordable for a while. So until the splice has PeerInputConfs
// confirmations, the peer's spliced-in amount is held: the link fails back
// incoming HTLCs that would draw on it.

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
