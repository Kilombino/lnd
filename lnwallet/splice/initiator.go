package splice

import (
	"github.com/lightningnetwork/lnd/lnwire"
)

// handleSpliceAck takes the peer's splice_ack to our splice_init.
func (m *Machine) handleSpliceAck(msg *lnwire.SpliceAck) {
	m.warnAndDisconnect("splice_ack without our splice_init")
}
