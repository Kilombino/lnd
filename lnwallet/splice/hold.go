package splice

import (
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/lightningnetwork/lnd/input"
)

// A peer that put coins into a splice could spend them through us once the
// splice locks, then reorganise the chain to double-spend its inputs: the
// channel would be back on its old funding, without those coins, after we
// paid out what they bought. On a chain with little hashrate that is
// affordable for a while. So until the splice has PeerInputConfs
// confirmations, the peer's spliced-in amount is held: the link fails back
// incoming HTLCs that would draw on it.

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

// holdPeerInput holds the peer's contribution to a splice that just locked
// (or locked before a restart) until it has PeerInputConfs confirmations.
func (m *Machine) holdPeerInput(txid chainhash.Hash, peer btcutil.Amount,
	pkScript []byte, heightHint uint32) {

	confs := m.cfg.Policy.PeerInputConfs
	if peer <= 0 || confs == 0 {
		return
	}

	conf, err := m.cfg.Chain.RegisterConfirmationsNtfn(
		&txid, pkScript, confs, heightHint,
	)
	if err != nil {
		// Without the watch nothing would release it: the peer's
		// coins count as soon as they are deep enough to lock.
		log.Errorf("ChannelID(%v): unable to watch splice %v for the "+
			"peer's coins: %v", m.cfg.ChanID, txid, err)
		return
	}

	m.mu.Lock()
	m.holds[txid] = peer
	m.mu.Unlock()
	log.Infof("ChannelID(%v): holding the peer's %v from splice %v until "+
		"%d confirmations", m.cfg.ChanID, peer, txid, confs)

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer conf.Cancel()

		select {
		case _, ok := <-conf.Confirmed:
			if !ok {
				return
			}
			m.mu.Lock()
			delete(m.holds, txid)
			m.mu.Unlock()
			log.Infof("ChannelID(%v): the peer's coins from splice "+
				"%v are %d deep, no longer held", m.cfg.ChanID,
				txid, confs)

		case <-m.quit:
		}
	}()
}

// restoreHold holds the peer's contribution to the channel's last splice
// again after a restart, if it locked. The confirmation watch releases it at
// once when the splice is already deep enough.
func (m *Machine) restoreHold() error {
	state := m.cfg.Channel.State()
	last, err := state.LastSplice()
	if err != nil || last == nil || last.Outcome != "locked" ||
		last.RemoteDelta <= 0 {

		return err
	}

	// The channel is on that splice's funding output now.
	if state.CurrentFundingOutpoint().Hash != last.Txid {
		return nil
	}
	_, out, err := input.GenFundingPkScript(
		state.LocalChanCfg.MultiSigKey.PubKey.SerializeCompressed(),
		state.RemoteChanCfg.MultiSigKey.PubKey.SerializeCompressed(),
		int64(state.Capacity),
	)
	if err != nil {
		return err
	}

	m.holdPeerInput(
		last.Txid, last.RemoteDelta, out.PkScript,
		state.CurrentShortChanID().BlockHeight,
	)

	return nil
}
