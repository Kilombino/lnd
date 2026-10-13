package htlcswitch

import (
	"github.com/lightningnetwork/lnd/lnwallet"
	"github.com/lightningnetwork/lnd/lnwire"
)

// A splice moves a channel to a new funding output with a short channel id of
// its own, which the channel's new announcement and route hints name. The
// link keeps the one it was opened with, as do forwarding records; every
// other one the channel has had maps onto it, since a payment may name any.

// AddSpliceScids maps short channel ids a channel's splices gave it onto its
// link.
func (s *Switch) AddSpliceScids(chanID lnwire.ChannelID,
	scids ...lnwire.ShortChannelID) error {

	s.indexMtx.Lock()
	defer s.indexMtx.Unlock()

	link, err := s.getLink(chanID)
	if err != nil {
		return err
	}

	base := link.ShortChanID()
	for _, scid := range scids {
		if scid == base || scid == (lnwire.ShortChannelID{}) {
			continue
		}
		s.spliceIndex[scid] = base
	}

	return nil
}

// spliceLink returns the link a splice's short channel id maps onto.
//
// NOTE: This MUST be called with the indexMtx held.
func (s *Switch) spliceLink(scid lnwire.ShortChannelID) (ChannelLink, bool) {
	base, ok := s.spliceIndex[scid]
	if !ok {
		return nil, false
	}
	link, ok := s.forwardingIndex[base]

	return link, ok
}

// removeSpliceScids drops the splice short channel ids of a link going away.
//
// NOTE: This MUST be called with the indexMtx held.
func (s *Switch) removeSpliceScids(base lnwire.ShortChannelID) {
	for scid, b := range s.spliceIndex {
		if b == base {
			delete(s.spliceIndex, scid)
		}
	}
}

// spliceBase returns the short channel id of the link a splice's maps onto,
// or scid itself.
func (s *Switch) spliceBase(scid lnwire.ShortChannelID) lnwire.ShortChannelID {
	s.indexMtx.RLock()
	defer s.indexMtx.RUnlock()

	if base, ok := s.spliceIndex[scid]; ok {
		return base
	}

	return scid
}

// addChannelSpliceScids maps every short channel id the channel's splices
// gave it onto its link. A failure leaves the link up, reachable by the id it
// goes by.
func (s *Switch) addChannelSpliceScids(chanID lnwire.ChannelID,
	lnChan *lnwallet.LightningChannel) error {

	scids, err := lnChan.State().SpliceScids()
	if err == nil && len(scids) > 0 {
		err = s.AddSpliceScids(chanID, scids...)
	}
	if err != nil {
		log.Errorf("ChannelID(%v): unable to map its splices' short "+
			"channel ids: %v", chanID, err)
	}

	return nil
}
