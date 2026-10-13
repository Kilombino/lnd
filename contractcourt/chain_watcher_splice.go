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

	// spent is the funding output the splice spent: the current one, or
	// one a locked splice replaced when a dropped transaction of it
	// confirmed after a reorganisation.
	spent wire.OutPoint

	// script and height are the output's script and the height the
	// splice confirmed at: the watch on the output registered again, as
	// after a spend of it reorged out.
	script []byte
	height uint32
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

	// A transaction of a splice dropped after signing (declared dead, or
	// a replacement another one beat), confirmed after all by a
	// reorganisation: its output holds the channel now.
	r, err := c.cfg.chanState.DroppedSplice(*spend.SpenderTxHash)
	if err == nil {
		log.Warnf("ChannelPoint(%v): dropped splice %v confirmed",
			c.cfg.chanState.FundingOutpoint, spend.SpenderTxHash)

		return r
	}

	return nil
}

// retiredChainSet returns the commitments to judge a spend of a retired
// funding output by: a splice dropped after signing, or the output a lock
// replaced. We have no commitment there to publish, as ours were revoked; the
// peer's that weren't revoked when it retired stand until they are, and every
// earlier one of the peer's is revoked, found in the funding's own log.
func (c *chainWatcher) retiredChainSet(
	r *channeldb.SpliceRecord) (*chainSet, *channeldb.OpenChannel, error) {

	if err := c.cfg.chanState.Refresh(); err != nil {
		return nil, nil, err
	}
	view := c.cfg.chanState.SpliceView(r)
	remote := view.RemoteCommitment
	set := &chainSet{
		remoteStateNum: c.cfg.chanState.RemoteCommitment.CommitHeight,
		localCommit:    view.LocalCommitment,
		remoteCommit:   remote,
		commitSet: CommitSet{
			HtlcSets: map[HtlcSetKey][]channeldb.HTLC{
				LocalHtlcSet:  view.LocalCommitment.Htlcs,
				RemoteHtlcSet: remote.Htlcs,
			},
		},
	}
	if r.RemoteTip != nil {
		tip := *r.RemoteTip
		set.remotePendingCommit = &tip
		set.commitSet.HtlcSets[RemotePendingHtlcSet] = tip.Htlcs
	}

	return set, view, nil
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

	var spent wire.OutPoint
	if spend.SpentOutPoint != nil {
		spent = *spend.SpentOutPoint
	}

	return spendNtfn, &spliceWatch{
		txid: txid, outpoint: r.OutPoint, confNtfn: confNtfn,
		spent: spent, script: script, height: height,
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

	// A splice dropped after signing that confirmed all the same.
	if r, err := c.cfg.chanState.DroppedSplice(w.txid); err == nil {
		return c.retiredChainSet(r)
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
		return c.maybeLockedAhead(spend, current)
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
	if c.cfg.chanState.HasChanStatus(channeldb.ChanStatusCommitBroadcasted) &&
		c.publishSpliceCommitment(r, false) {

		c.splicePublished = r.Txid()
	}

	return moved, true
}

// maybeLockedAhead moves the watch to the channel's current funding output
// when spend is the splice the channel has locked to since: at a start, the
// lock done at the peer's channel_reestablish while the notifier was still
// looking for the spend of the output watched, the splice's record gone with
// the lock. Taken for an unknown spend before, the channel was flagged for
// good and the output holding it unwatched.
func (c *chainWatcher) maybeLockedAhead(spend *chainntnfs.SpendDetail,
	current *chainntnfs.SpendEvent) (*chainntnfs.SpendEvent, bool) {

	if spend.SpenderTxHash == nil {
		return nil, false
	}
	if err := c.cfg.chanState.Refresh(); err != nil {
		log.Errorf("ChannelPoint(%v): unable to read channel: %v",
			c.cfg.chanState.FundingOutpoint, err)

		return nil, false
	}
	funding := c.cfg.chanState.CurrentFundingOutpoint()
	if funding.Hash != *spend.SpenderTxHash ||
		(spend.SpentOutPoint != nil && *spend.SpentOutPoint == funding) {

		return nil, false
	}
	script, err := deriveFundingPkScript(c.cfg.chanState)
	if err != nil {
		log.Errorf("ChannelPoint(%v): unable to derive funding "+
			"script: %v", c.cfg.chanState.FundingOutpoint, err)

		return nil, false
	}
	moved, err := c.cfg.notifier.RegisterSpendNtfn(
		&funding, script, uint32(spend.SpendingHeight),
	)
	if err != nil {
		log.Errorf("ChannelPoint(%v): unable to watch funding %v: %v",
			c.cfg.chanState.FundingOutpoint, funding, err)

		return nil, false
	}
	current.Cancel()
	c.fundingSpendNtfn = moved
	if c.splice != nil {
		c.splice.confNtfn.Cancel()
		c.splice = nil
	}
	c.watchPreviousFundings()
	log.Infof("ChannelPoint(%v): splice %v locked before its spend was "+
		"seen, watching %v", c.cfg.chanState.FundingOutpoint,
		spend.SpenderTxHash, funding)

	return moved, true
}

// maybeSpliceLocked stops following the splice the watch is on once it has
// locked: the channel is on its funding now, and the output it replaced is
// watched until the splice is reorg-safe, as at a start after the lock.
// Before, nothing watched that output until a restart, so a revoked
// commitment confirmed there after a reorganisation went unpunished. The
// channel is read again, as the lock is written by the link. Reports
// whether the splice had locked.
func (c *chainWatcher) maybeSpliceLocked() bool {
	if c.splice == nil {
		return false
	}

	chanState := c.cfg.chanState
	if err := chanState.Refresh(); err != nil {
		// Closed on the splice's output: the channel is gone from
		// the open ones while its close resolves.
		logf := log.Errorf
		if errors.Is(err, channeldb.ErrChannelNotFound) ||
			errors.Is(err, channeldb.ErrNoActiveChannels) {

			logf = log.Debugf
		}
		logf("ChannelPoint(%v): unable to read channel: %v",
			chanState.FundingOutpoint, err)

		return false
	}
	if chanState.CurrentFundingOutpoint() != c.splice.outpoint {
		return false
	}

	log.Infof("ChannelPoint(%v): splice %v locked, watching the funding "+
		"output it replaced", chanState.FundingOutpoint, c.splice.txid)
	c.splice.confNtfn.Cancel()
	c.splice = nil
	c.watchPreviousFundings()

	return true
}

// republishOnReorg publishes our force close commitment again once a reorg
// took out the splice we had published our commitment on: the commitment
// on the funding output that is unspent again is the one that can confirm,
// and the arbitrator, its first broadcast refused, doesn't send it again.
func (c *chainWatcher) republishOnReorg() {
	c.splicePublished = chainhash.Hash{}

	chanState := c.cfg.chanState
	if c.cfg.publishTx == nil ||
		!chanState.HasChanStatus(channeldb.ChanStatusCommitBroadcasted) {

		return
	}
	tx, err := chanState.BroadcastedCommitment()
	if err != nil {
		log.Errorf("ChannelPoint(%v): no commitment to publish again: "+
			"%v", chanState.FundingOutpoint, err)

		return
	}

	log.Infof("ChannelPoint(%v): publishing our commitment %v again "+
		"after the splice was reorged out", chanState.FundingOutpoint,
		tx.TxHash())
	err = c.cfg.publishTx(tx, labels.MakeLabel(
		labels.LabelTypeChannelClose, &chanState.ShortChannelID,
	))
	if err != nil {
		log.Errorf("ChannelPoint(%v): unable to publish commitment: "+
			"%v", chanState.FundingOutpoint, err)
	}
}

// maybePublishSpliceCommitment publishes our commitment on a splice's
// funding output once we have force closed. On the confirmed splice the watch
// follows: a force close after it confirmed, while it waits to lock,
// broadcasts our commitment on the funding output the splice spent, which
// can never confirm, and nothing else would publish one that can. On each
// splice transaction still unconfirmed: our commitment on the funding output
// spent can't replace the splice in the mempool, and one on the splice's
// output, its child, lets the anchor sweep raise the fee of both, so the
// splice confirms in time for the arbitrator's deadlines. Called at every
// block; the channel's status is read again, as the arbitrator sets it on its
// own copy, and a publish that failed is tried again at the next block.
func (c *chainWatcher) maybePublishSpliceCommitment() {
	if c.splice != nil && c.splicePublished == c.splice.txid {
		return
	}

	chanState := c.cfg.chanState
	records, err := chanState.SpliceRecords()
	if err != nil {
		log.Errorf("ChannelPoint(%v): unable to read splices: %v",
			chanState.FundingOutpoint, err)

		return
	}
	var candidates []*channeldb.SpliceRecord
	for _, r := range records {
		switch {
		case r.Retired || r.LocalCommit == nil:
		case c.splice != nil && r.Txid() != c.splice.txid:
		default:
			candidates = append(candidates, r)
		}
	}
	if len(candidates) == 0 {
		return
	}

	if err := chanState.Refresh(); err != nil {
		log.Errorf("ChannelPoint(%v): unable to read channel: %v",
			chanState.FundingOutpoint, err)

		return
	}
	if !chanState.HasChanStatus(channeldb.ChanStatusCommitBroadcasted) {
		return
	}

	if c.splice != nil {
		for _, r := range candidates {
			if c.publishSpliceCommitment(r, false) {
				c.splicePublished = r.Txid()
			}
		}

		return
	}

	// Unconfirmed: at every block, our commitment on each splice
	// transaction's output, its child, and ours on the funding output
	// they spend. Whichever can get in does: the splice may confirm,
	// die with its inputs double-spent, or leave the mempool, and the
	// arbitrator sends ours on the old funding only once. Each child is
	// signed once.
	for _, r := range candidates {
		tx, ok := c.pendingSigned[r.Txid()]
		if !ok {
			var err error
			tx, err = c.signSpliceCommitment(r)
			if err != nil {
				log.Errorf("ChannelPoint(%v): unable to sign our "+
					"commitment on splice %v: %v",
					chanState.FundingOutpoint, r.Txid(),
					err)

				continue
			}
			if c.pendingSigned == nil {
				c.pendingSigned = make(
					map[chainhash.Hash]*wire.MsgTx,
				)
			}
			c.pendingSigned[r.Txid()] = tx
			log.Infof("ChannelPoint(%v): splice %v unconfirmed, and "+
				"we force closed; publishing our commitment "+
				"%v on it", chanState.FundingOutpoint,
				r.Txid(), tx.TxHash())
		}
		c.publishQuietly(tx)
	}
	if tx, err := chanState.BroadcastedCommitment(); err == nil {
		c.publishQuietly(tx)
	}
}

// publishQuietly publishes a transaction that may well be refused, another
// spending the same output in the mempool, at debug level.
func (c *chainWatcher) publishQuietly(tx *wire.MsgTx) {
	if c.cfg.publishTx == nil {
		return
	}
	err := c.cfg.publishTx(tx, labels.MakeLabel(
		labels.LabelTypeChannelClose, &c.cfg.chanState.ShortChannelID,
	))
	if err != nil {
		log.Debugf("ChannelPoint(%v): %v not published: %v",
			c.cfg.chanState.FundingOutpoint, tx.TxHash(), err)
	}
}

// publishSpliceCommitment publishes our signed commitment on a splice's
// funding output, reporting whether it was. One on an unconfirmed splice
// may be refused, another transaction of the splice in the mempool: that
// is logged quietly.
func (c *chainWatcher) publishSpliceCommitment(r *channeldb.SpliceRecord,
	unconfirmed bool) bool {

	if c.cfg.publishTx == nil || r.LocalCommit == nil {
		return false
	}

	tx, err := c.signSpliceCommitment(r)
	if err != nil {
		log.Errorf("ChannelPoint(%v): unable to sign our commitment "+
			"on splice %v: %v", c.cfg.chanState.FundingOutpoint,
			r.Txid(), err)
		return false
	}

	state := "confirmed"
	if unconfirmed {
		state = "unconfirmed"
	}
	log.Infof("ChannelPoint(%v): splice %v %v, and we force closed; "+
		"publishing our commitment %v on it",
		c.cfg.chanState.FundingOutpoint, r.Txid(), state, tx.TxHash())

	err = c.cfg.publishTx(tx, labels.MakeLabel(
		labels.LabelTypeChannelClose, &c.cfg.chanState.ShortChannelID,
	))
	if err != nil {
		logf := log.Errorf
		if unconfirmed {
			logf = log.Debugf
		}
		logf("ChannelPoint(%v): unable to publish commitment on "+
			"splice %v: %v", c.cfg.chanState.FundingOutpoint,
			r.Txid(), err)

		return false
	}

	return true
}

// signSpliceCommitment returns our commitment on a splice's funding output,
// signed.
func (c *chainWatcher) signSpliceCommitment(
	r *channeldb.SpliceRecord) (*wire.MsgTx, error) {

	if r.LocalCommit == nil {
		return nil, errors.New("no commitment of ours")
	}
	witness, out, err := input.GenFundingPkScript(
		r.LocalKey.PubKey.SerializeCompressed(),
		r.RemoteKey.SerializeCompressed(), int64(r.Capacity),
	)
	if err != nil {
		return nil, err
	}

	return lnwallet.GetSignedCommitTx(lnwallet.SignedCommitTxInputs{
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

	// The peer's commitment there that was unrevoked at the lock, or a
	// revoked one: judged by the retired output's own commitments.
	spent := spend.SpendingTx.TxIn[0].PreviousOutPoint
	if r := c.retiredRecord(spent); r != nil &&
		isCommitmentShaped(spend.SpendingTx) {

		chainSet, view, err := c.retiredChainSet(r)
		if err == nil {
			stateNum := c.cfg.extractStateNumHint(
				spend.SpendingTx, c.stateHintObfuscator,
			)
			chanState := c.cfg.chanState
			c.cfg.chanState = view
			ok, err := c.handleKnownRemoteState(
				spend, stateNum, chainSet,
			)
			c.cfg.chanState = chanState
			if err != nil {
				log.Errorf("ChannelPoint(%v): unable to handle "+
					"close on %v: %v",
					c.cfg.chanState.FundingOutpoint, spent,
					err)
			}
			if ok {
				return
			}
		}
	}

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

// retiredRecord returns the retired record of a funding output, nil if there
// is none.
func (c *chainWatcher) retiredRecord(op wire.OutPoint) *channeldb.SpliceRecord {
	records, err := c.cfg.chanState.SpliceRecords()
	if err != nil {
		return nil
	}
	for _, r := range records {
		if r.Retired && r.OutPoint == op {
			return r
		}
	}

	return nil
}
