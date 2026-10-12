package peer

import (
	"errors"
	"fmt"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/lightningnetwork/lnd/fn/v2"
	"github.com/lightningnetwork/lnd/htlcswitch"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/lnwallet"
	"github.com/lightningnetwork/lnd/lnwallet/splice"
	"github.com/lightningnetwork/lnd/lnwire"
)

// errSpliceUnlocked is the reason a cooperative close is refused while a
// splice of the channel hasn't locked: the spec forbids shutdown then.
var errSpliceUnlocked = errors.New("a splice of this channel hasn't " +
	"locked yet")

// spliceUnlocked reports whether the channel has a splice that hasn't
// locked.
func (p *Brontide) spliceUnlocked(chanID lnwire.ChannelID) bool {
	channel, ok := p.activeChannels.Load(chanID)
	if !ok || channel == nil {
		return false
	}

	return len(channel.PendingFundings()) > 0
}

// spliceNegotiated reports whether splicing applies to this connection: we
// advertise it (on, or kept on for a splice still to finish somewhere) and
// the peer supports it. A machine that can't start new splices refuses
// them.
func (p *Brontide) spliceNegotiated(lnChan *lnwallet.LightningChannel) bool {
	if !p.remoteFeatures.HasFeature(lnwire.SpliceOptional) {
		return false
	}
	if p.cfg.SpliceMachine == nil {
		return false
	}

	return p.cfg.SpliceEnabled || len(lnChan.PendingFundings()) > 0
}

// spliceLinkProxy is the channel's link as the splice machine sees it,
// looked up in the switch on each call: the link is created after the
// machine and replaced on reconnection.
type spliceLinkProxy struct {
	p      *Brontide
	chanID lnwire.ChannelID
}

// link returns the channel's link with its splice methods.
func (s *spliceLinkProxy) link() (htlcswitch.SpliceLink, error) {
	links, err := s.p.cfg.Switch.GetLinksByInterface(s.p.PubKey())
	if err != nil {
		return nil, err
	}
	for _, link := range links {
		if link.ChanID() != s.chanID {
			continue
		}
		sl, ok := link.(htlcswitch.SpliceLink)
		if !ok {
			return nil, fmt.Errorf("link of %v can't splice",
				s.chanID)
		}

		return sl, nil
	}

	return nil, fmt.Errorf("no link for %v", s.chanID)
}

func errChan(err error) <-chan error {
	c := make(chan error, 1)
	c <- err

	return c
}

// QuiescenceInitiator implements splice.Link.
func (s *spliceLinkProxy) QuiescenceInitiator() fn.Result[lntypes.ChannelParty] {
	l, err := s.link()
	if err != nil {
		return fn.Err[lntypes.ChannelParty](err)
	}

	return l.QuiescenceInitiator()
}

// InitStfu implements splice.Link.
func (s *spliceLinkProxy) InitStfu() <-chan fn.Result[lntypes.ChannelParty] {
	l, err := s.link()
	if err != nil {
		c := make(chan fn.Result[lntypes.ChannelParty], 1)
		c <- fn.Err[lntypes.ChannelParty](err)

		return c
	}

	return l.InitStfu()
}

// ResumeQuiescence implements splice.Link.
func (s *spliceLinkProxy) ResumeQuiescence() <-chan error {
	l, err := s.link()
	if err != nil {
		return errChan(err)
	}

	return l.ResumeQuiescence()
}

// AddSpliceFunding implements splice.Link.
func (s *spliceLinkProxy) AddSpliceFunding(
	f lnwallet.PendingFunding) <-chan error {

	l, err := s.link()
	if err != nil {
		return errChan(err)
	}

	return l.AddSpliceFunding(f)
}

// SendSpliceCommitment implements splice.Link.
func (s *spliceLinkProxy) SendSpliceCommitment(
	txid chainhash.Hash) <-chan error {

	l, err := s.link()
	if err != nil {
		return errChan(err)
	}

	return l.SendSpliceCommitment(txid)
}

// LockSplice implements splice.Link.
func (s *spliceLinkProxy) LockSplice(txid chainhash.Hash,
	scid lnwire.ShortChannelID) <-chan error {

	l, err := s.link()
	if err != nil {
		return errChan(err)
	}

	return l.LockSplice(txid, scid)
}

// SpliceMachine returns the splice machine of one of the peer's channels.
func (p *Brontide) SpliceMachine(chanID lnwire.ChannelID) (*splice.Machine,
	bool) {

	p.spliceMtx.Lock()
	defer p.spliceMtx.Unlock()

	m, ok := p.spliceMachines[chanID]

	return m, ok
}

// startSpliceMachine creates and starts a channel's splice machine, stopping
// one left from the previous connection.
func (p *Brontide) startSpliceMachine(chanID lnwire.ChannelID,
	lnChan *lnwallet.LightningChannel) (*splice.Machine, error) {

	m := p.cfg.SpliceMachine(splice.Config{
		ChanID:  chanID,
		Link:    &spliceLinkProxy{p: p, chanID: chanID},
		Channel: lnChan,
		Peer:    p,
	}, p.IdentityKey())
	if err := m.Start(); err != nil {
		return nil, err
	}

	p.spliceMtx.Lock()
	old := p.spliceMachines[chanID]
	p.spliceMachines[chanID] = m
	p.spliceMtx.Unlock()

	if old != nil {
		old.Stop()
	}

	return m, nil
}

// stopSpliceMachines stops every splice machine of the peer.
func (p *Brontide) stopSpliceMachines() {
	p.spliceMtx.Lock()
	machines := p.spliceMachines
	p.spliceMachines = make(map[lnwire.ChannelID]*splice.Machine)
	p.spliceMtx.Unlock()

	for _, m := range machines {
		m.Stop()
	}
}

// RemoveSpliceFunding implements splice.Link.
func (s *spliceLinkProxy) RemoveSpliceFunding(
	txid chainhash.Hash) <-chan error {

	l, err := s.link()
	if err != nil {
		return errChan(err)
	}

	return l.RemoveSpliceFunding(txid)
}
