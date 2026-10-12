package contractcourt

import (
	"errors"
	"fmt"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/chainntnfs"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/input"
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

	if c.splice != nil {
		return nil, false
	}
	r := c.knownSplice(spend)
	if r == nil {
		return nil, false
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

	return moved, true
}
