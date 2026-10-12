package splice

import (
	"fmt"

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
		err := m.cfg.Channel.RemovePendingFunding(a.txid)
		if err != nil {
			m.forgetRecord(a.txid)
		}
	}

	prev := a.replaced[len(a.replaced)-1]
	prev.replaced = a.replaced[:len(a.replaced)-1]
	m.mu.Lock()
	m.active = prev
	m.mu.Unlock()
	log.Infof("ChannelID(%v): replacement of splice %v dropped: %v",
		m.cfg.ChanID, prev.txid, a.abortReason)

	m.resume()
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
