package contractcourt

import (
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/chainntnfs"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/input"
	"github.com/lightningnetwork/lnd/lnwallet"
)

// A channel restored from a static backup knows its funding output as of the
// backup. A splice after the backup moved the channel to an output the backup
// doesn't name, and the peer's commitment, published once the peer learns
// this node lost its state, spends that output. So a spend of the restored
// funding output that isn't a commitment or a splice the backup knew is no
// close: the channel is flagged, and the peer's commitment is found by the
// output it pays us, whose script is fixed for the life of the channel on the
// channel types that don't tweak it.

// restoredToRemoteScript returns the script the peer's commitments pay us on,
// and false for a channel type where it changes with each commitment.
func restoredToRemoteScript(ch *channeldb.OpenChannel) ([]byte, bool,
	error) {

	if !ch.ChanType.IsTweakless() || ch.ChanType.IsTaproot() {
		return nil, false, nil
	}

	var leaseExpiry uint32
	if ch.ChanType.HasLeaseExpiration() {
		leaseExpiry = ch.ThawHeight
	}

	// On the peer's commitment the output is ours, with the peer as the
	// commitment's owner: the same arguments NewUnilateralCloseSummary
	// uses.
	script, _, err := lnwallet.CommitScriptToRemote(
		ch.ChanType, !ch.IsInitiator,
		ch.LocalChanCfg.PaymentBasePoint.PubKey, leaseExpiry,
		input.NoneTapLeaf(),
	)
	if err != nil {
		return nil, false, err
	}

	return script.PkScript(), true, nil
}

// watchMovedFunding starts looking for the peer's commitment on a funding
// output the backup didn't know, from heightHint. Once per chain watcher.
func (c *chainWatcher) watchMovedFunding(heightHint uint32) {
	c.movedWatch.Do(func() {
		chanState := c.cfg.chanState
		script, ok, err := restoredToRemoteScript(chanState)
		switch {
		case err != nil:
			log.Errorf("ChannelPoint(%v): unable to derive our "+
				"output script: %v", chanState.FundingOutpoint,
				err)
			return

		case !ok:
			log.Errorf("ChannelPoint(%v): restored channel moved "+
				"after its backup, and its type doesn't allow "+
				"finding the peer's commitment",
				chanState.FundingOutpoint)
			return
		}

		log.Warnf("ChannelPoint(%v): restored channel moved after "+
			"its backup; looking for the peer's commitment by our "+
			"output on it", chanState.FundingOutpoint)

		c.wg.Add(1)
		go c.waitMovedCommitment(
			chanState.FundingOutpoint, script,
			c.requiredConfsForSpend(), heightHint,
		)
	})
}

// waitMovedCommitment waits for the peer's commitment watchMovedFunding looks
// for and hands it to the close observer. Anything else paying our output's
// script is passed over.
//
// NOTE: the peer knows the script too, and can pay it from a commitment-
// shaped transaction of its own making: we'd take that for its commitment,
// sweep what it paid us, and leave the real one's output to be swept by hand.
// It gains nothing by it.
func (c *chainWatcher) waitMovedCommitment(chanPoint wire.OutPoint,
	script []byte, numConfs, heightHint uint32) {

	defer c.wg.Done()

	for {
		conf, err := c.cfg.notifier.RegisterConfirmationsNtfn(
			nil, script, numConfs, heightHint,
		)
		if err != nil {
			log.Errorf("ChannelPoint(%v): unable to watch our "+
				"output script: %v", chanPoint, err)
			return
		}

		var txConf *chainntnfs.TxConfirmation
		select {
		case txConf = <-conf.Confirmed:
			conf.Cancel()
		case <-c.quit:
			conf.Cancel()
			return
		}
		if txConf == nil || txConf.Tx == nil {
			return
		}

		tx := txConf.Tx
		if !isCommitmentShaped(tx) {
			log.Warnf("ChannelPoint(%v): tx %v pays our output "+
				"script but isn't a commitment", chanPoint,
				tx.TxHash())

			heightHint = txConf.BlockHeight + 1
			continue
		}

		txid := tx.TxHash()
		spend := &chainntnfs.SpendDetail{
			SpentOutPoint:  &tx.TxIn[0].PreviousOutPoint,
			SpenderTxHash:  &txid,
			SpendingTx:     tx,
			SpendingHeight: int32(txConf.BlockHeight),
		}
		select {
		case c.movedCommits <- spend:
		case <-c.quit:
		}

		return
	}
}

// handleMovedCommitment dispatches the peer's commitment on a funding output
// the backup didn't know as the peer's close of a state we lost.
func (c *chainWatcher) handleMovedCommitment(spend *chainntnfs.SpendDetail) {
	log.Infof("ChannelPoint(%v): peer's commitment %v found on funding "+
		"output %v", c.cfg.chanState.FundingOutpoint,
		spend.SpenderTxHash, spend.SpentOutPoint)

	chainSet, err := newChainSet(c.cfg.chanState)
	if err != nil {
		log.Errorf("ChannelPoint(%v): unable to read channel: %v",
			c.cfg.chanState.FundingOutpoint, err)
		return
	}
	stateNum := c.cfg.extractStateNumHint(
		spend.SpendingTx, c.stateHintObfuscator,
	)
	_, err = c.handleUnknownRemoteState(spend, stateNum, chainSet)
	if err != nil {
		log.Errorf("ChannelPoint(%v): unable to handle the peer's "+
			"commitment: %v", c.cfg.chanState.FundingOutpoint, err)
	}
}

// restoredCoopClose reports whether a spend of a restored channel's funding
// output, alone and with a cooperative close's sequence, is a close rather
// than a splice after the backup. A splice creates the channel's new funding
// output, a P2WSH 2-of-2 on every channel type that can be spliced; a close
// this node took part in pays it. So: a close when something pays us and no
// output is P2WSH. A close paying a P2WSH script is left flagged: what it
// paid us the wallet finds anyway.
func (c *chainWatcher) restoredCoopClose(tx *wire.MsgTx) bool {
	// Never spliced.
	if c.cfg.chanState.ChanType.IsTaproot() {
		return true
	}

	for _, out := range tx.TxOut {
		if txscript.IsPayToWitnessScriptHash(out.PkScript) {
			return false
		}
	}

	return c.toSelfAmount(tx) > 0
}
