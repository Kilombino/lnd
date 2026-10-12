package funding

import (
	"fmt"

	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/graph/db/models"
	"github.com/lightningnetwork/lnd/lnwire"
)

// AnnounceSplice puts a channel a splice locked on its new funding output in
// the graph there, under the splice's short channel id, with ourPolicy (nil
// for the default), when addEdge is set; and for a public channel sends our
// half of the proof of the announcement there. A splice locks at least 6
// blocks deep, so the announcement is due at once. The edge of the funding
// output the channel left must already be gone.
func (f *Manager) AnnounceSplice(ch *channeldb.OpenChannel,
	ourPolicy *models.ChannelEdgePolicy, addEdge bool) error {

	scid := ch.CurrentShortChanID()
	if scid == ch.ShortChanID() {
		return fmt.Errorf("channel %v not moved by a splice",
			ch.FundingOutpoint)
	}

	if addEdge {
		err := f.addToGraph(ch, &scid, nil, ourPolicy)
		if err != nil {
			return fmt.Errorf("unable to add the splice's edge: %w",
				err)
		}
	}

	if ch.ChannelFlags&lnwire.FFAnnounceChannel == 0 {
		return nil
	}

	log.Infof("Announcing ChannelPoint(%v) on its splice's funding %v, "+
		"short_chan_id=%v", ch.FundingOutpoint,
		ch.CurrentFundingOutpoint(), scid)

	return f.announceChannel(
		f.cfg.IDKey, ch.IdentityPub, &ch.LocalChanCfg.MultiSigKey,
		ch.RemoteChanCfg.MultiSigKey.PubKey, scid,
		lnwire.NewChanIDFromOutPoint(ch.FundingOutpoint), ch.ChanType,
	)
}
