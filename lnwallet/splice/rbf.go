package splice

import (
	"errors"
	"fmt"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/lnwallet"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/fn/v2"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/lnwallet/chainfee"
	"github.com/lightningnetwork/lnd/lnwallet/interactivetx"
	"github.com/lightningnetwork/lnd/lnwire"
)

// A peer may replace a splice it started that is signed both ways and not
// yet locked, with a transaction paying a higher fee (tx_init_rbf). This
// node takes part as it did in the splice: it adds nothing. The splice's
// earlier transactions stay candidates until one of them, or the
// replacement, confirms: they all spend the same funding output, so only
// one can. Our own splices aren't replaced yet.

// The replacement must pay at least the fee of the transaction it replaces,
// in total: checked once its construction is done (checkReplacementFee).

// maxReplacements bounds the replacements of one splice, each of which is
// another candidate funding the channel signs commitments for.
const maxReplacements = 10

// handleTxInitRbf answers the peer's tx_init_rbf: refused with tx_abort, or
// accepted with tx_ack_rbf and the replacement's construction started.
func (m *Machine) handleTxInitRbf(msg *lnwire.TxInitRbf) {
	initiator, err := m.cfg.Link.QuiescenceInitiator().Unpack()
	if err != nil || initiator != lntypes.Remote {
		m.warnAndDisconnect("tx_init_rbf without the peer having " +
			"started quiescence")
		return
	}

	a := m.active
	if code, why := m.checkTxInitRbf(msg); code != "" {
		m.abort(code, "%s", why)
		m.resume()

		return
	}

	contribution := msg.FundingOutputContribution.UnwrapOr(
		a.theirContribution,
	)
	session, b, err := m.newConstruction(
		interactivetx.Acceptor, contribution, 0,
		chainfee.SatPerKWeight(msg.FeeRatePerKw), msg.LockTime,
		a.ourKey, a.theirKey,
	)
	if err != nil {
		m.abort(codeInternal, "%v", err)
		m.resume()

		return
	}

	// The splice keeps its place in the limits: the replacement is the
	// same splice.
	b.session = session
	b.replaced = append(append([]*active(nil), a.replaced...), a)
	m.mu.Lock()
	m.active = b
	m.mu.Unlock()
	m.startTimeout()

	log.Infof("ChannelID(%v): peer replaces splice %v (%v) at %v",
		m.cfg.ChanID, a.txid, a.feeRate, b.feeRate)
	m.send(&lnwire.TxAckRbf{
		ChannelID:                 m.cfg.ChanID,
		FundingOutputContribution: fn.Some(btcutil.Amount(0)),
		RequireConfirmedInputs:    true,
	})
}

// checkTxInitRbf returns why the peer's replacement is refused, or "".
func (m *Machine) checkTxInitRbf(msg *lnwire.TxInitRbf) (string, string) {
	a := m.active
	switch {
	case a == nil || a.phase != phasePending:
		return codeRbfUnsupported, "no signed splice to replace"

	// Only the side that started a splice replaces it, and ours we
	// don't replace yet.
	case a.role != interactivetx.Acceptor:
		return codeRbfUnsupported, "our splice"

	case a.lockedSent || a.lockedReceived:
		return codeRbfUnsupported, "already locked"

	case len(a.replaced) >= maxReplacements:
		return codeTooManySplices, fmt.Sprintf("%d replacements",
			len(a.replaced))

	case m.cfg.Disabled:
		return codeDisabled, ""

	case m.misbehaved || m.cfg.Limits.isMisbehaved(m.cfg.ChanID):
		return codePeerMisbehaved, ""

	// The rate may stay (the Core Lightning port replaces at the fee
	// floor), not fall.
	case chainfee.SatPerKWeight(msg.FeeRatePerKw) < a.feeRate:
		return codeFeeBelowFloor, fmt.Sprintf("%d sat/kw replacing %v",
			msg.FeeRatePerKw, a.feeRate)

	case m.cfg.Chain != nil && !m.cfg.Chain.Synced():
		return codeChainNotSynced, ""
	}

	// The checks of a splice_init, with the splice's keys.
	return m.checkSpliceInit(&lnwire.SpliceInit{
		FundingContribution: msg.FundingOutputContribution.UnwrapOr(
			a.theirContribution,
		),
		FundingFeeRatePerKw: msg.FeeRatePerKw,
		LockTime:            msg.LockTime,
		FundingPubKey:       a.theirKey,
	})
}

// forgetReplacement drops a replacement that didn't get past negotiation or
// our signatures: the transaction it was to replace is the splice again.
func (m *Machine) forgetReplacement() {
	a := m.active
	if a.timeout != nil {
		a.timeout.Stop()
	}
	if a.res != nil {
		m.removeFunding(a.txid)
	}

	prev := a.replaced[len(a.replaced)-1]
	prev.replaced = a.replaced[:len(a.replaced)-1]
	m.mu.Lock()
	m.active = prev
	m.mu.Unlock()
	log.Infof("ChannelID(%v): replacement of splice %v dropped: %v",
		m.cfg.ChanID, prev.txid, a.abortReason)

	m.ensurePending()
	m.resume()
}

// removeFunding drops a transaction of the splice from the channel's
// funding set, and its record.
func (m *Machine) removeFunding(txid chainhash.Hash) {
	err := m.cfg.Channel.RemovePendingFunding(txid)
	switch {
	case errors.Is(err, lnwallet.ErrUnknownFunding):
		m.forgetRecord(txid)

	case err != nil:
		log.Errorf("ChannelID(%v): unable to drop splice %v: %v",
			m.cfg.ChanID, txid, err)
	}
}

// ensurePending republishes the splice's transaction and starts watching
// it, if that isn't running: for one that became the splice again, or that
// a restart left as a candidate only.
func (m *Machine) ensurePending() {
	a := m.active
	if a == nil || a.phase != phasePending || a.pendingWatched {
		return
	}
	if tx, err := m.signedTx(); err == nil {
		m.publish(tx)
	}
	height := a.broadcastHeight
	if height == 0 {
		height, _ = m.cfg.Chain.BestHeight()
	}
	m.watchConfirmationOf(a, height)
	m.watchPendingOf(a, height)
}

// dropCandidate drops an earlier transaction of the splice that can no
// longer confirm: one of its inputs was spent elsewhere.
func (m *Machine) dropCandidate(c *active) {
	a := m.active
	kept := a.replaced[:0]
	for _, r := range a.replaced {
		if r != c {
			kept = append(kept, r)
		}
	}
	m.mu.Lock()
	a.replaced = kept
	m.mu.Unlock()
	m.removeFunding(c.txid)
	log.Infof("ChannelID(%v): splice candidate %v can no longer confirm",
		m.cfg.ChanID, c.txid)
}

// liveCandidate returns the newest earlier transaction of the splice that
// doesn't spend op.
func (m *Machine) liveCandidate(op wire.OutPoint) *active {
	a := m.active
	for i := len(a.replaced) - 1; i >= 0; i-- {
		if !spends(a.replaced[i], op) {
			return a.replaced[i]
		}
	}

	return nil
}

// spends reports whether a transaction of the splice spends op.
func spends(a *active, op wire.OutPoint) bool {
	for _, in := range a.res.Inputs {
		if in.OutPoint == op {
			return true
		}
	}

	return false
}

// fallBackTo makes an earlier transaction of the splice the splice again:
// the latest is dead, op having been spent elsewhere. Candidates spending
// op go too.
func (m *Machine) fallBackTo(prev *active, op wire.OutPoint) {
	a := m.active
	m.removeFunding(a.txid)

	var kept []*active
	for _, c := range a.replaced {
		switch {
		case c == prev:
		case spends(c, op):
			m.removeFunding(c.txid)
		default:
			kept = append(kept, c)
		}
	}
	prev.replaced = kept
	m.mu.Lock()
	m.active = prev
	m.mu.Unlock()
	log.Warnf("ChannelID(%v): splice replacement %v can no longer "+
		"confirm (%v spent elsewhere); %v is the splice again",
		m.cfg.ChanID, a.txid, op, prev.txid)

	m.ensurePending()
}

// candidate returns the earlier transaction of the splice with txid, if
// any.
func (m *Machine) candidate(txid chainhash.Hash) *active {
	a := m.active
	if a == nil {
		return nil
	}
	for _, c := range a.replaced {
		if c.txid == txid {
			return c
		}
	}

	return nil
}

// adopt makes the splice's earlier transaction with txid the splice: it
// confirmed, so the others never can.
func (m *Machine) adopt(txid chainhash.Hash) *active {
	c := m.candidate(txid)
	if c == nil {
		return nil
	}

	a := m.active
	if a.timeout != nil {
		a.timeout.Stop()
	}
	c.replaced = nil
	m.mu.Lock()
	m.active = c
	m.mu.Unlock()
	log.Infof("ChannelID(%v): splice %v confirmed, not its replacement %v",
		m.cfg.ChanID, txid, a.txid)

	// A replacement still being built kept the channel quiescent.
	if a.phase != phasePending {
		m.resume()
	}
	m.ensurePending()

	return c
}

// candidateLocked records the peer's splice_locked for an earlier
// transaction of the splice. Which one we lock is decided by which reaches
// our own depth: the peer's word alone doesn't change it.
func (m *Machine) candidateLocked(c *active) {
	if c.lockedReceived {
		return
	}
	err := m.cfg.Channel.State().UpdateSpliceRecord(c.txid,
		func(r *channeldb.SpliceRecord) error {
			r.Negotiation.LockedReceived = true
			return nil
		})
	if err != nil {
		log.Errorf("ChannelID(%v): unable to record the peer's "+
			"splice_locked: %v", m.cfg.ChanID, err)
		return
	}
	m.mu.Lock()
	c.lockedReceived = true
	m.mu.Unlock()
}

// checkReplacementFee refuses a replacement paying less in fees than the
// transaction it replaces.
func (m *Machine) checkReplacementFee(res *interactivetx.Result) error {
	a := m.active
	if len(a.replaced) == 0 {
		return nil
	}
	prev := a.replaced[len(a.replaced)-1]
	if prev.res == nil || res.Fee >= prev.res.Fee {
		return nil
	}

	return fmt.Errorf("replacement pays %v in fees, less than the %v "+
		"of %v", res.Fee, prev.res.Fee, prev.txid)
}
