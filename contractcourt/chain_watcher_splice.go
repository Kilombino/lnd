package contractcourt

import (
	"errors"
	"fmt"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/chainntnfs"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/input"
	"github.com/lightningnetwork/lnd/keychain"
	"github.com/lightningnetwork/lnd/labels"
	"github.com/lightningnetwork/lnd/lnwallet"
)

// A splice's transaction spends the channel's funding output, but it doesn't
// close the channel: it moves it to the splice's funding output. While the
// splice is pending, that spend is recognised by its txid and the watch moves
// to the new output, with the splice itself watched for a reorg that would
// bring the old output back. A spend of the new output is then judged
// against the channel's commitments on it: from the splice's record while it
// is pending, from the channel itself once it has locked.

// spliceWatch is what the chain watcher keeps of a pending splice that has
// confirmed.
type spliceWatch struct {
	// txid names the splice.
	txid chainhash.Hash

	// outpoint is the splice's funding output, now watched.
	outpoint wire.OutPoint

	// confNtfn watches the splice transaction, for a reorg taking it out.
	confNtfn *chainntnfs.ConfirmationEvent
}

// knownSplice returns the record of the pending splice whose transaction
// spent the funding output, or nil.
func (c *chainWatcher) knownSplice(
	spend *chainntnfs.SpendDetail) *channeldb.SpliceRecord {

	records, err := c.cfg.chanState.SpliceRecords()
	if err != nil {
		log.Errorf("ChannelPoint(%v): unable to read splices: %v",
			c.cfg.chanState.FundingOutpoint, err)

		return nil
	}
	for _, r := range records {
		if !r.Retired && r.Txid() == *spend.SpenderTxHash {
			return r
		}
	}

	return nil
}

// spliceFundingScript returns a splice's funding output script.
func spliceFundingScript(r *channeldb.SpliceRecord) ([]byte, error) {
	_, out, err := input.GenFundingPkScript(
		r.LocalKey.PubKey.SerializeCompressed(),
		r.RemoteKey.SerializeCompressed(), int64(r.Capacity),
	)
	if err != nil {
		return nil, err
	}

	return out.PkScript, nil
}

// followSplice moves the watch to a confirmed splice's funding output and
// starts watching the splice for a reorg. The old registration is replaced.
func (c *chainWatcher) followSplice(r *channeldb.SpliceRecord,
	spend *chainntnfs.SpendDetail) (*chainntnfs.SpendEvent, *spliceWatch,
	error) {

	script, err := spliceFundingScript(r)
	if err != nil {
		return nil, nil, err
	}
	height := uint32(spend.SpendingHeight)

	spendNtfn, err := c.cfg.notifier.RegisterSpendNtfn(
		&r.OutPoint, script, height,
	)
	if err != nil {
		return nil, nil, err
	}

	txid := r.Txid()
	confNtfn, err := c.cfg.notifier.RegisterConfirmationsNtfn(
		&txid, script, 1, height,
	)
	if err != nil {
		spendNtfn.Cancel()
		return nil, nil, err
	}

	log.Infof("ChannelPoint(%v): splice %v confirmed, watching %v",
		c.cfg.chanState.FundingOutpoint, txid, r.OutPoint)

	return spendNtfn, &spliceWatch{
		txid: txid, outpoint: r.OutPoint, confNtfn: confNtfn,
	}, nil
}

// spliceChainSet returns the channel's commitments on a pending splice's
// funding output from its record, and a view of the channel on that funding.
// Once the splice has locked its record is gone and the channel itself is on
// that funding: then the channel's own state is used.
func (c *chainWatcher) spliceChainSet(
	w *spliceWatch) (*chainSet, *channeldb.OpenChannel, error) {

	records, err := c.cfg.chanState.SpliceRecords()
	if err != nil {
		return nil, nil, err
	}
	for _, r := range records {
		if r.Retired || r.Txid() != w.txid {
			continue
		}

		// A channel restored from a backup has no commitments on
		// any funding: its own empty ones stand, and any spend is
		// the peer's unknown state.
		restored := c.cfg.chanState.HasChanStatus(
			channeldb.ChanStatusRestored,
		)
		if restored {
			view := c.cfg.chanState.SpliceView(r)
			set, err := newChainSet(view)

			return set, view, err
		}

		if r.LocalCommit == nil || r.RemoteTail == nil {
			return nil, nil, fmt.Errorf("splice %v without its "+
				"commitments", w.txid)
		}

		view := c.cfg.chanState.SpliceView(r)
		set := &chainSet{
			remoteStateNum: r.RemoteTail.CommitHeight,
			localCommit:    *r.LocalCommit,
			remoteCommit:   *r.RemoteTail,
			commitSet: CommitSet{
				HtlcSets: map[HtlcSetKey][]channeldb.HTLC{
					LocalHtlcSet:  r.LocalCommit.Htlcs,
					RemoteHtlcSet: r.RemoteTail.Htlcs,
				},
			},
		}
		if r.RemoteTip != nil {
			tip := *r.RemoteTip
			set.remotePendingCommit = &tip
			set.commitSet.HtlcSets[RemotePendingHtlcSet] = tip.Htlcs
		}
		if _, err := view.RemoteRevocationStore(); err != nil {
			return nil, nil, err
		}

		return set, view, nil
	}

	// Locked: the channel is on this funding now.
	if err := c.cfg.chanState.Refresh(); err != nil {
		return nil, nil, err
	}
	if c.cfg.chanState.CurrentFundingOutpoint() != w.outpoint {
		return nil, nil, errors.New("splice neither pending nor " +
			"locked")
	}
	set, err := newChainSet(c.cfg.chanState)

	return set, c.cfg.chanState, err
}

// maybeFollowSplice moves the watch when spend is a pending splice's own
// transaction, returning the new registration. Any other spend is left to
// the close logic.
func (c *chainWatcher) maybeFollowSplice(spend *chainntnfs.SpendDetail,
	current *chainntnfs.SpendEvent) (*chainntnfs.SpendEvent, bool) {

	r := c.knownSplice(spend)
	if r == nil {
		return nil, false
	}

	// The watch already follows a splice: one that has locked since, the
	// channel's current funding, which this new splice spends.
	if c.splice != nil {
		if err := c.cfg.chanState.Refresh(); err != nil {
			log.Errorf("ChannelPoint(%v): unable to read channel: "+
				"%v", c.cfg.chanState.FundingOutpoint, err)

			return nil, false
		}
		if c.cfg.chanState.CurrentFundingOutpoint() != c.splice.outpoint {
			return nil, false
		}

		// The output it replaced is watched until it is reorg-safe,
		// as at a start after the lock.
		c.splice.confNtfn.Cancel()
		c.splice = nil
		c.watchPreviousFundings()
	}

	moved, w, err := c.followSplice(r, spend)
	if err != nil {
		log.Errorf("ChannelPoint(%v): unable to follow splice %v: %v",
			c.cfg.chanState.FundingOutpoint, r.Txid(), err)

		return nil, false
	}
	current.Cancel()
	c.fundingSpendNtfn = moved
	c.splice = w

	// Taken for a spend it didn't recognise by a version, or a start,
	// that didn't know the splice: it is known now.
	chanState := c.cfg.chanState
	if chanState.HasChanStatus(channeldb.ChanStatusFundingSpentUnknown) {
		err := chanState.ClearChanStatus(
			channeldb.ChanStatusFundingSpentUnknown,
		)
		if err != nil {
			log.Errorf("ChannelPoint(%v): unable to clear the "+
				"unknown spend flag: %v",
				chanState.FundingOutpoint, err)
		} else {
			log.Infof("ChannelPoint(%v): spend %v recognised as "+
				"splice %v; flag cleared",
				chanState.FundingOutpoint, spend.SpenderTxHash,
				r.Txid())
		}
	}

	// We had force closed on the previous funding: that commitment can't
	// confirm now, ours on the splice's funding can.
	if c.cfg.chanState.HasChanStatus(channeldb.ChanStatusCommitBroadcasted) {
		c.publishSpliceCommitment(r)
	}

	return moved, true
}

// publishSpliceCommitment publishes our signed commitment on a splice's
// funding output.
func (c *chainWatcher) publishSpliceCommitment(r *channeldb.SpliceRecord) {
	if c.cfg.publishTx == nil || r.LocalCommit == nil {
		return
	}

	witness, out, err := input.GenFundingPkScript(
		r.LocalKey.PubKey.SerializeCompressed(),
		r.RemoteKey.SerializeCompressed(), int64(r.Capacity),
	)
	if err != nil {
		log.Errorf("ChannelPoint(%v): splice funding script: %v",
			c.cfg.chanState.FundingOutpoint, err)
		return
	}

	tx, err := lnwallet.GetSignedCommitTx(lnwallet.SignedCommitTxInputs{
		CommitTx:  r.LocalCommit.CommitTx,
		CommitSig: r.LocalCommit.CommitSig,
		OurKey:    r.LocalKey,
		TheirKey:  keychain.KeyDescriptor{PubKey: r.RemoteKey},
		SignDesc: &input.SignDescriptor{
			KeyDesc:       r.LocalKey,
			WitnessScript: witness,
			Output:        out,
			HashType: lnwallet.CommitSigHashType(
				c.cfg.chanState.ChanType,
			),
			InputIndex: 0,
		},
	}, c.cfg.signer)
	if err != nil {
		log.Errorf("ChannelPoint(%v): unable to sign our commitment "+
			"on splice %v: %v", c.cfg.chanState.FundingOutpoint,
			r.Txid(), err)
		return
	}

	log.Infof("ChannelPoint(%v): splice %v confirmed after our force "+
		"close; publishing our commitment %v on it",
		c.cfg.chanState.FundingOutpoint, r.Txid(), tx.TxHash())

	err = c.cfg.publishTx(tx, labels.MakeLabel(
		labels.LabelTypeChannelClose, &c.cfg.chanState.ShortChannelID,
	))
	if err != nil {
		log.Errorf("ChannelPoint(%v): unable to publish commitment: "+
			"%v", c.cfg.chanState.FundingOutpoint, err)
	}
}

// reorgSafeDepth is how deep a splice must be before the funding output it
// replaced stops being watched.
const reorgSafeDepth = 144

// watchPreviousFundings watches the funding outputs locked splices replaced
// until each splice is reorg-safe: if a reorg takes a splice out, the old
// output can be spent again, and every commitment on it is revoked by then.
func (c *chainWatcher) watchPreviousFundings() {
	// A channel restored from a backup knows neither the revoked
	// commitments on its old fundings nor every splice between them.
	if c.cfg.chanState.HasChanStatus(channeldb.ChanStatusRestored) {
		return
	}

	previous, err := c.cfg.chanState.PreviousFundings()
	if err != nil {
		log.Errorf("ChannelPoint(%v): unable to read previous fundings: "+
			"%v", c.cfg.chanState.FundingOutpoint, err)
		return
	}

	scripts := make(map[wire.OutPoint][]byte)
	for _, pf := range previous {
		_, out, err := input.GenFundingPkScript(
			pf.LocalKey.PubKey.SerializeCompressed(),
			pf.RemoteKey.SerializeCompressed(),
			int64(pf.Capacity),
		)
		if err != nil {
			continue
		}
		scripts[pf.OutPoint] = out.PkScript
	}
	current, err := deriveFundingPkScript(c.cfg.chanState)
	if err == nil {
		scripts[c.cfg.chanState.CurrentFundingOutpoint()] = current
	}

	for _, pf := range previous {
		script, ok := scripts[pf.OutPoint]
		if !ok || c.watchedPrevious[pf.OutPoint] {
			continue
		}
		c.watchedPrevious[pf.OutPoint] = true
		c.wg.Add(1)
		go c.watchPreviousFunding(pf, script, scripts[pf.LockedTo])
	}
}

// watchPreviousFunding watches one replaced funding output. Its spend by the
// splice is expected and followed until reorg-safe; any other spend goes to
// the close observer.
func (c *chainWatcher) watchPreviousFunding(pf channeldb.PreviousFunding,
	script, spliceScript []byte) {

	defer c.wg.Done()

	heightHint := c.cfg.chanState.DeriveHeightHint()
	for {
		spendNtfn, err := c.cfg.notifier.RegisterSpendNtfn(
			&pf.OutPoint, script, heightHint,
		)
		if err != nil {
			log.Errorf("ChannelPoint(%v): unable to watch previous "+
				"funding %v: %v", c.cfg.chanState.FundingOutpoint,
				pf.OutPoint, err)
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

		case <-c.quit:
			spendNtfn.Cancel()
			return
		}

		if *spend.SpenderTxHash != pf.LockedTo.Hash {
			select {
			case c.previousSpends <- spend:
			case <-c.quit:
			}

			return
		}

		// The splice: followed until reorg-safe, or watched again if a
		// reorg takes it out.
		height := uint32(spend.SpendingHeight)
		heightHint = height
		conf, err := c.cfg.notifier.RegisterConfirmationsNtfn(
			spend.SpenderTxHash, spliceScript, reorgSafeDepth,
			height,
		)
		if err != nil {
			log.Errorf("ChannelPoint(%v): unable to follow splice "+
				"%v: %v", c.cfg.chanState.FundingOutpoint,
				spend.SpenderTxHash, err)
			return
		}

		select {
		case <-conf.Confirmed:
			conf.Cancel()
			return

		case <-conf.NegativeConf:
			conf.Cancel()
			log.Warnf("ChannelPoint(%v): splice %v reorged out "+
				"after it locked, watching %v again",
				c.cfg.chanState.FundingOutpoint,
				spend.SpenderTxHash, pf.OutPoint)

		case <-c.quit:
			conf.Cancel()
			return
		}
	}
}

// handlePreviousFundingSpend judges a spend of a replaced funding output:
// every commitment on it is revoked once the channel updated after the lock,
// so a commitment is punished as a breach; anything else freezes the channel
// as a spend this node doesn't recognise.
func (c *chainWatcher) handlePreviousFundingSpend(
	spend *chainntnfs.SpendDetail) {

	log.Errorf("ChannelPoint(%v): funding output %v of a locked splice "+
		"spent by %v", c.cfg.chanState.FundingOutpoint,
		spend.SpendingTx.TxIn[0].PreviousOutPoint, spend.SpenderTxHash)

	if isCommitmentShaped(spend.SpendingTx) {
		chainSet, err := newChainSet(c.cfg.chanState)
		if err == nil {
			stateNum := c.cfg.extractStateNumHint(
				spend.SpendingTx, c.stateHintObfuscator,
			)
			ok, err := c.handlePossibleBreach(
				spend, stateNum, chainSet,
			)
			if err != nil {
				log.Errorf("ChannelPoint(%v): unable to handle "+
					"breach: %v", c.cfg.chanState.FundingOutpoint,
					err)
			}
			if ok {
				return
			}
		}
	}

	if err := c.handleUnknownSpend(spend); err != nil {
		log.Errorf("ChannelPoint(%v): unable to flag spend: %v",
			c.cfg.chanState.FundingOutpoint, err)
	}
}
