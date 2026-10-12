package splice

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/input"
	"github.com/lightningnetwork/lnd/keychain"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/lnwallet"
	"github.com/lightningnetwork/lnd/lnwallet/chainfee"
	"github.com/lightningnetwork/lnd/lnwallet/interactivetx"
	"github.com/lightningnetwork/lnd/lnwire"
)

// A splice the peer starts. In M1 this node adds nothing to it: no inputs,
// no outputs, a contribution of zero. Everything it refuses is decided before
// its commitment_signed, since with nothing put in it signs first.

// handleSpliceInit answers the peer's splice_init: refused with tx_abort, or
// accepted with splice_ack and the construction started.
func (m *Machine) handleSpliceInit(msg *lnwire.SpliceInit) {
	// A splice_init while another splice is negotiating or unlocked, or
	// from the side that didn't start quiescence, breaks the protocol.
	if m.active != nil {
		m.warnAndDisconnect("splice_init while a splice is in progress")
		return
	}
	initiator, err := m.cfg.Link.QuiescenceInitiator().Unpack()
	if err != nil || initiator != lntypes.Remote {
		m.warnAndDisconnect("splice_init without the peer having " +
			"started quiescence")
		return
	}

	if code := m.spliceable(); code != "" {
		m.abort(code, "")
		m.resume()

		return
	}
	if code, why := m.checkSpliceInit(msg); code != "" {
		m.abort(code, "%s", why)
		m.resume()

		return
	}

	ourKey, err := m.cfg.KeyRing.DeriveNextKey(keychain.KeyFamilyMultiSig)
	if err != nil {
		m.abort(codeInternal, "")
		m.resume()

		return
	}

	session, a, err := m.newConstruction(
		interactivetx.Acceptor, msg.FundingContribution, 0,
		chainfee.SatPerKWeight(msg.FundingFeeRatePerKw), msg.LockTime,
		ourKey, msg.FundingPubKey,
	)
	if err != nil {
		m.abort(codeInternal, "%v", err)
		m.resume()

		return
	}
	a.session = session
	m.setActive(a)
	m.startTimeout()

	m.send(&lnwire.SpliceAck{
		ChannelID:              m.cfg.ChanID,
		FundingContribution:    0,
		FundingPubKey:          ourKey.PubKey,
		RequireConfirmedInputs: true,
	})
}

// checkSpliceInit applies this node's policy to a peer's splice_init: its fee
// rate, the new capacity, and what it takes out of the peer's balance.
func (m *Machine) checkSpliceInit(msg *lnwire.SpliceInit) (string, string) {
	if msg.FundingPubKey == nil {
		return codeInternal, "no funding key"
	}

	feeRate := chainfee.SatPerKWeight(msg.FundingFeeRatePerKw)
	if err := CheckAcceptorFeeRate(feeRate, m.cfg.Estimator); err != nil {
		return codeFeeBelowFloor, err.Error()
	}

	height, err := m.cfg.Chain.BestHeight()
	if err != nil {
		return codeChainNotSynced, err.Error()
	}
	if msg.LockTime > height+1 && msg.LockTime < txscript.LockTimeThreshold {
		return codeLocktime, fmt.Sprintf("locktime %d past tip %d",
			msg.LockTime, height)
	}

	state := m.cfg.Channel.State()
	contribution := msg.FundingContribution
	theirBalance := state.LocalCommitment.RemoteBalance.ToSatoshis()
	if contribution < 0 && -contribution > theirBalance {
		return codeAboveBalance, fmt.Sprintf("takes %v out of %v",
			-contribution, theirBalance)
	}

	capacity := state.Capacity + contribution
	if contribution > 0 && m.cfg.Policy.MaxChannelSize > 0 &&
		capacity > m.cfg.Policy.MaxChannelSize {

		return codeAboveMaxSize, fmt.Sprintf("capacity %v above %v",
			capacity, m.cfg.Policy.MaxChannelSize)
	}

	return "", ""
}

// newConstruction sets up the interactive construction of a splice of the
// channel: the shared input (the current funding output), the new funding
// output with both new keys, both contributions and both balances.
func (m *Machine) newConstruction(role interactivetx.Role,
	initContribution, accContribution btcutil.Amount,
	feeRate chainfee.SatPerKWeight, lockTime uint32,
	ourKey keychain.KeyDescriptor,
	theirKey *btcec.PublicKey) (*interactivetx.Session, *active, error) {

	outpoint, capacity, _, oldScript, err := m.currentFunding()
	if err != nil {
		return nil, nil, err
	}

	newCapacity := capacity + initContribution + accContribution
	if newCapacity <= 0 {
		return nil, nil, errors.New("splice empties the channel")
	}
	witness, out, err := input.GenFundingPkScript(
		ourKey.PubKey.SerializeCompressed(),
		theirKey.SerializeCompressed(), int64(newCapacity),
	)
	if err != nil {
		return nil, nil, err
	}

	state := m.cfg.Channel.State()
	ours := state.LocalCommitment.LocalBalance.ToSatoshis()
	theirs := state.LocalCommitment.RemoteBalance.ToSatoshis()
	initBalance, accBalance := ours, theirs
	ourContribution, theirContribution := initContribution, accContribution
	if role == interactivetx.Acceptor {
		initBalance, accBalance = theirs, ours
		ourContribution, theirContribution = accContribution,
			initContribution
	}

	cfg := interactivetx.Config{
		Role:      role,
		ChannelID: m.cfg.ChanID,
		Shared: interactivetx.SharedInput{
			OutPoint:      outpoint,
			Value:         capacity,
			PkScript:      oldScript,
			WitnessWeight: input.MultiSigWitnessSize,
		},
		FundingScript:         out.PkScript,
		InitiatorContribution: initContribution,
		AcceptorContribution:  accContribution,
		FeeRate:               feeRate,
		LockTime:              lockTime,
		MaxPeerAddMessages:    m.cfg.Policy.MaxPeerAddMessages,
		CheckPeerInput:        m.checkPeerInput,
		CheckPeerOutput:       m.checkPeerOutput,
		DustLimit:             m.cfg.DustLimit,
		WitnessWeight:         peerWitnessWeight,
		Reserve: func(c btcutil.Amount) btcutil.Amount {
			return c / 100
		},
		InitiatorBalance: initBalance,
		AcceptorBalance:  accBalance,
	}
	session, err := interactivetx.NewSession(cfg)
	if err != nil {
		return nil, nil, err
	}

	return session, &active{
		phase:             phaseNegotiating,
		role:              role,
		fundingScript:     out.PkScript,
		fundingWitness:    witness,
		ourKey:            ourKey,
		theirKey:          theirKey,
		ourContribution:   ourContribution,
		theirContribution: theirContribution,
		capacity:          newCapacity,
		feeRate:           feeRate,
	}, nil
}

// peerWitnessWeight accepts only the peer inputs whose witness size is known
// before it arrives: P2WPKH and P2TR key path.
func peerWitnessWeight(pkScript []byte) (int64, error) {
	w, ok := CoinWitnessWeight(pkScript)
	if !ok {
		return 0, errors.New("only P2WPKH and P2TR key path inputs")
	}

	return w, nil
}

// checkPeerInput applies this node's policy to an input the peer adds: not
// one of our own outpoints, and confirmed, unspent and not a coinbase output
// as our backend sees it, with the value and script the peer claims.
func (m *Machine) checkPeerInput(in *interactivetx.Input) error {
	if m.cfg.IsOurOutpoint != nil && m.cfg.IsOurOutpoint(in.OutPoint) {
		return fmt.Errorf("input %v is ours", in.OutPoint)
	}

	out, confs, coinbase, err := m.cfg.Chain.ConfirmedOutput(in.OutPoint)
	switch {
	case err != nil:
		return fmt.Errorf("input %v not found unspent: %w",
			in.OutPoint, err)

	case confs < 1:
		return fmt.Errorf("input %v unconfirmed", in.OutPoint)

	case coinbase:
		return fmt.Errorf("input %v is a coinbase output", in.OutPoint)

	case out.Value != int64(in.Value) ||
		!bytes.Equal(out.PkScript, in.PkScript):

		return fmt.Errorf("input %v doesn't match the chain",
			in.OutPoint)
	}

	return nil
}

// checkPeerOutput applies relay standardness to an output the peer adds:
// only witness v0 and v1 programs, which every policy relays.
func (m *Machine) checkPeerOutput(out *interactivetx.Output) error {
	script := out.PkScript
	switch {
	case txscript.IsPayToWitnessPubKeyHash(script),
		txscript.IsPayToWitnessScriptHash(script),
		txscript.IsPayToTaproot(script):

		return nil
	}

	return errors.New("output script not accepted")
}

// startTimeout ends a negotiation that doesn't finish in time.
func (m *Machine) startTimeout() {
	timeout := m.cfg.Policy.NegotiationTimeout
	a := m.active
	a.timeout = time.AfterFunc(timeout, func() {
		m.enqueue(func() {
			if m.active != a || a.phase != phaseNegotiating {
				return
			}
			m.abort(codeTimeout, "negotiation took over %v",
				timeout)
			m.forget()
		})
	})
}

// handleConstruction takes an interactive construction message.
func (m *Machine) handleConstruction(msg lnwire.Message) {
	a := m.active
	if a == nil || a.phase != phaseNegotiating || a.session == nil {
		m.warnAndDisconnect("%T outside a negotiation", msg)
		return
	}

	if err := a.session.Receive(msg); err != nil {
		m.constructionFailed(err)
		return
	}
	m.advanceConstruction()
}

// advanceConstruction sends what is ours to send, and finishes the
// construction once both sides are done.
func (m *Machine) advanceConstruction() {
	a := m.active
	for !a.session.Done() && a.session.OurTurn() {
		msg, err := a.session.Next()
		if err != nil {
			m.constructionFailed(err)
			return
		}
		m.send(msg)
	}
	if !a.session.Done() {
		return
	}

	res, err := a.session.Build()
	if err != nil {
		m.constructionFailed(err)
		return
	}
	m.constructionDone(res)
}

// constructionFailed aborts the negotiation.
func (m *Machine) constructionFailed(err error) {
	var abortErr *interactivetx.AbortError
	switch {
	case errors.As(err, &abortErr):
		m.abort(abortErr.Reason, "")

	case errors.Is(err, interactivetx.ErrNotOurTurn):
		m.warnAndDisconnect("%v", err)
		m.setActive(nil)

		return

	default:
		m.abort(codeInternal, "%v", err)
	}
	m.forget()
}

// constructionDone records the agreed transaction, adds the splice to the
// channel's funding set and sends our commitment_signed for it.
func (m *Machine) constructionDone(res *interactivetx.Result) {
	a := m.active
	if a.timeout != nil {
		a.timeout.Stop()
	}

	txid := res.Tx.TxHash()
	m.update(func(a *active) {
		a.res = res
		a.txid = txid
		a.fundingIndex = res.FundingIndex
		a.session = nil
		a.phase = phaseSigning
	})

	record := &channeldb.SpliceRecord{
		OutPoint:    wire.OutPoint{Hash: txid, Index: res.FundingIndex},
		Capacity:    a.capacity,
		LocalKey:    a.ourKey,
		RemoteKey:   a.theirKey,
		LocalDelta:  a.ourContribution,
		RemoteDelta: a.theirContribution,
		Negotiation: &channeldb.SpliceNegotiation{
			WeInitiated: a.role == interactivetx.Initiator,
			Tx:          res.Tx,
			FeeRate:     uint64(a.feeRate),
			OurInputs:   coinOutpoints(a.ourInputs),
			Inputs:      spliceInputs(res),
		},
	}
	if err := m.cfg.Channel.State().PutSpliceRecord(record); err != nil {
		m.abort(codeInternal, "%v", err)
		m.forget()

		return
	}

	err := m.wait(m.cfg.Link.AddSpliceFunding(lnwallet.PendingFunding{
		OutPoint:    record.OutPoint,
		Capacity:    a.capacity,
		LocalKey:    a.ourKey,
		RemoteKey:   a.theirKey,
		LocalDelta:  a.ourContribution,
		RemoteDelta: a.theirContribution,
	}))
	if err != nil {
		m.abort(codeInternal, "%v", err)
		m.forget()

		return
	}

	if err := m.wait(m.cfg.Link.SendSpliceCommitment(txid)); err != nil {
		m.abort(codeInternal, "%v", err)
		m.forget()

		return
	}
	m.update(func(a *active) { a.commitSent = true })

	m.maybeSendTxSignatures()
}

// forgetRecord deletes the record of a splice that never got into the
// channel's funding set.
func (m *Machine) forgetRecord(txid [32]byte) {
	err := m.cfg.Channel.State().DeleteSpliceRecord(txid)
	if err != nil && !errors.Is(err, channeldb.ErrSpliceRecordNotFound) {
		log.Errorf("ChannelID(%v): unable to delete splice record: %v",
			m.cfg.ChanID, err)
	}
}

// wait waits for a link request's result.
func (m *Machine) wait(result <-chan error) error {
	select {
	case err := <-result:
		return err
	case <-m.quit:
		return errors.New("splice machine stopping")
	}
}

// coinOutpoints returns the coins' outpoints.
func coinOutpoints(coins []Coin) []wire.OutPoint {
	ops := make([]wire.OutPoint, len(coins))
	for i, c := range coins {
		ops[i] = c.OutPoint
	}

	return ops
}
