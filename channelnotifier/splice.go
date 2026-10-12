package channelnotifier

import (
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
)

// SpliceEvent is a step of a channel's splice, or a funding spend this node
// doesn't recognise.
type SpliceEvent struct {
	// ChannelPoint is the channel's current funding output, and
	// OriginalChannelPoint the one it was opened with.
	ChannelPoint, OriginalChannelPoint wire.OutPoint

	// Kind is "started", "signed", "locked", "aborted", "dead" or
	// "funding_spent_unknown".
	Kind string

	// Txid names the splice transaction, when there is one.
	Txid chainhash.Hash

	// Reason, for "aborted", is a refusal code or the peer's text when
	// ReasonFromPeer.
	Reason         string
	ReasonFromPeer bool
}

// NotifySpliceEvent notifies the channel event subscribers of a splice
// step.
func (c *ChannelNotifier) NotifySpliceEvent(event SpliceEvent) {
	if err := c.ntfnServer.SendUpdate(event); err != nil {
		log.Warnf("Unable to send splice event update: %v", err)
	}
}
