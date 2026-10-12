package splice

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/chainntnfs"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/fn/v2"
	"github.com/lightningnetwork/lnd/input"
	"github.com/lightningnetwork/lnd/keychain"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/lnwallet"
	"github.com/lightningnetwork/lnd/lnwallet/chainfee"
	"github.com/lightningnetwork/lnd/lnwallet/interactivetx"
	"github.com/lightningnetwork/lnd/lnwire"
)

// The splice machine of one channel. It negotiates a splice with the peer,
// talks to the wallet and the chain, and persists what it agreed before
// sending anything that depends on it; everything that touches commitments
// it asks of the channel's link, which runs it in the order of the peer's
// messages. It runs as one goroutine fed by a queue, so the link can hand it
// messages without blocking.

// Link is the channel's link as the machine uses it.
type Link interface {
	// QuiescenceInitiator returns which side started the channel's
	// current quiescence.
	QuiescenceInitiator() fn.Result[lntypes.ChannelParty]

	// InitStfu asks the link to quiesce the channel.
	InitStfu() <-chan fn.Result[lntypes.ChannelParty]

	// ResumeQuiescence ends the channel's quiescence.
	ResumeQuiescence() <-chan error

	// AddSpliceFunding adds an agreed splice to the channel's funding
	// set.
	AddSpliceFunding(f lnwallet.PendingFunding) <-chan error

	// SendSpliceCommitment signs and sends our commitment_signed for a
	// splice.
	SendSpliceCommitment(txid chainhash.Hash) <-chan error

	// LockSplice completes a splice both sides have locked.
	LockSplice(txid chainhash.Hash,
		scid lnwire.ShortChannelID) <-chan error

	// RemoveSpliceFunding drops a splice that will never lock from the
	// channel's funding set.
	RemoveSpliceFunding(txid chainhash.Hash) <-chan error
}

// Channel is the channel's state machine as the machine reads it.
type Channel interface {
	// State returns the channel's persisted state.
	State() *channeldb.OpenChannel

	// PendingFundings returns the channel's pending splices.
	PendingFundings() []lnwallet.PendingFunding

	// RemovePendingFunding drops a splice that will never lock.
	RemovePendingFunding(txid chainhash.Hash) error

	// AvailableBalance returns what we can send now, on every funding.
	AvailableBalance() lnwire.MilliSatoshi
}

// Peer sends messages to the channel's peer.
type Peer interface {
	// SendMessage sends messages to the peer, in order.
	SendMessage(sync bool, msgs ...lnwire.Message) error

	// Disconnect ends the connection with the peer.
	Disconnect(reason error)
}

// Chain is what the machine needs from the chain backend.
type Chain interface {
	// RegisterConfirmationsNtfn notifies when a transaction reaches a
	// number of confirmations.
	RegisterConfirmationsNtfn(txid *chainhash.Hash, pkScript []byte,
		numConfs, heightHint uint32,
		opts ...chainntnfs.NotifierOption) (
		*chainntnfs.ConfirmationEvent, error)

	// RegisterSpendNtfn notifies when an output is spent.
	RegisterSpendNtfn(op *wire.OutPoint, pkScript []byte,
		heightHint uint32) (*chainntnfs.SpendEvent, error)

	// RegisterBlockEpochNtfn notifies of each new block.
	RegisterBlockEpochNtfn(*chainntnfs.BlockEpoch) (
		*chainntnfs.BlockEpochEvent, error)

	// BestHeight returns the height of the chain's tip.
	BestHeight() (uint32, error)

	// ConfirmedOutput returns an unspent output with its confirmations
	// and whether a coinbase transaction created it, or an error when
	// the backend doesn't know it as unspent.
	ConfirmedOutput(op wire.OutPoint) (*wire.TxOut, uint32, bool, error)
}

// Policy is this node's splice policy.
type Policy struct {
	// MaxChannelSize is the largest capacity a peer's splice may give a
	// channel, as for an inbound open.
	MaxChannelSize btcutil.Amount

	// LockDepth returns the confirmations at which a splice locks, given
	// what the peer put into it.
	LockDepth func(peerContribution btcutil.Amount) uint32

	// NegotiationTimeout ends a negotiation that doesn't finish: the
	// channel is quiescent meanwhile.
	NegotiationTimeout time.Duration

	// MaxPeerAddMessages bounds the peer's tx_add_input and
	// tx_add_output messages, each.
	MaxPeerAddMessages int
}

// Config is a channel's splice machine's environment.
type Config struct {
	// ChanID is the channel's id.
	ChanID lnwire.ChannelID

	// OurNodeID and TheirNodeID are the two nodes' public keys, which
	// order tx_signatures on a tie.
	OurNodeID, TheirNodeID [33]byte

	// Link, Channel and Peer are the channel's link, state machine and
	// peer.
	Link    Link
	Channel Channel
	Peer    Peer

	// Chain is the chain backend.
	Chain Chain

	// Wallet holds this node's coins, and CoinSource lists them.
	Wallet     Wallet
	CoinSource CoinSource

	// WithCoinSelectLock runs coin selection and leasing under the
	// wallet's coin selection lock. Nil runs it without.
	WithCoinSelectLock func(func() error) error

	// ChangeScript returns a fresh change script of the wallet.
	ChangeScript func() ([]byte, error)

	// CheckMempool tests whether the backend would accept a fully signed
	// transaction, before our tx_signatures leave when we sign last. Nil
	// skips the test.
	CheckMempool func(tx *wire.MsgTx) error

	// IsOurOutpoint reports whether an outpoint is one of this node's own:
	// a wallet coin, locked or not, or any channel's funding output. The
	// peer may not add one as its input.
	IsOurOutpoint func(op wire.OutPoint) bool

	// KeyRing derives this node's funding keys.
	KeyRing keychain.KeyRing

	// Signer signs the shared input and our wallet inputs.
	Signer input.Signer

	// Estimator gives fee estimates for the fee floors.
	Estimator chainfee.Estimator

	// DustLimit returns the dust limit of an output with a given
	// script, at our backend's relay rate.
	DustLimit func(pkScript []byte) btcutil.Amount

	// Policy is this node's splice policy.
	Policy Policy

	// Disabled refuses new splices, ours and the peer's, while a splice
	// already started carries on: splicing was turned off with one
	// pending somewhere.
	Disabled bool

	// NotifyEvent, if set, is told of each step of the channel's splices:
	// "started", "signed", "locked", "aborted".
	NotifyEvent func(kind string, txid chainhash.Hash, reason string,
		fromPeer bool)
}

// notify tells NotifyEvent of a step.
func (m *Machine) notify(kind string, txid chainhash.Hash, reason string,
	fromPeer bool) {

	if m.cfg.NotifyEvent != nil {
		m.cfg.NotifyEvent(kind, txid, reason, fromPeer)
	}
}

// Refusal codes, sent as tx_abort's reason and reported to the user.
const (
	codeDisabled          = "disabled"
	codeNoUnifiedSigs     = "no_unified_sigs"
	codeTaproot           = "taproot"
	codeZeroConf          = "zero_conf"
	codePending           = "pending"
	codeClosing           = "closing"
	codeNotQuiescent      = "not_quiescent"
	codeFeeBelowFloor     = "fee_below_floor"
	codeAboveMaxSize      = "above_max_size"
	codeAboveBalance      = "above_balance"
	codeChainNotSynced    = "chain_not_synced"
	codeTimeout           = "timeout"
	codeRbfUnsupported    = "rbf_unsupported"
	codeInvalidCommitment = "invalid_commitment"
	codeInvalidSignatures = "invalid_signatures"
	codeLocktime          = "locktime"
	codeInternal          = "internal_error"
	codePeerContribUnsupp = "peer_contribution_unsupported"
	codePeerMisbehaved    = "peer_misbehaved"
	codeInsufficientFunds = "insufficient_funds"
)

// phase is where an active splice is.
type phase uint8

const (
	// phasePreparing: a splice we start, funds chosen, waiting for the
	// channel to be quiescent.
	phasePreparing phase = iota

	// phaseNegotiating: splice_init/splice_ack exchanged, the
	// interactive construction running.
	phaseNegotiating

	// phaseSigning: the transaction is agreed; commitments and
	// tx_signatures are being exchanged.
	phaseSigning

	// phasePending: both tx_signatures exchanged and the transaction
	// published; waiting for the lock depth and splice_locked.
	phasePending
)

// deadSplice is a dead splice's trace in the wallet: the transaction and
// the coins we put in.
type deadSplice struct {
	tx     *wire.MsgTx
	inputs []Coin
}

// active is the splice in progress on the channel, if any.
type active struct {
	phase phase

	// role is our role in the interactive construction.
	role interactivetx.Role

	// session is the interactive construction, nil once done.
	session *interactivetx.Session

	// res is the agreed transaction.
	res *interactivetx.Result

	// txid names the splice.
	txid chainhash.Hash

	// fundingIndex is the new funding output's index.
	fundingIndex uint32

	// fundingScript and fundingWitness are the new funding output's
	// script and 2-of-2 witness script.
	fundingScript, fundingWitness []byte

	// ourKey is our key in the new funding output; theirKey the peer's.
	ourKey   keychain.KeyDescriptor
	theirKey *btcec.PublicKey

	// ourContribution and theirContribution are each side's change to
	// its own balance.
	ourContribution, theirContribution btcutil.Amount

	// capacity is the new funding output's value.
	capacity btcutil.Amount

	// feeRate is the transaction's fee rate.
	feeRate chainfee.SatPerKWeight

	// ourInputs are the wallet coins we put in.
	ourInputs []Coin

	// commitSent and commitReceived record the splice's own
	// commitment_signed each way.
	commitSent, commitReceived bool

	// ourTxSigs and theirTxSigs are the tx_signatures each way.
	ourTxSigs, theirTxSigs *lnwire.TxSignatures

	// lockedSent and lockedReceived record splice_locked each way, and
	// scid is the splice's short channel id once it reached our depth.
	lockedSent, lockedReceived bool
	scid                       fn.Option[lnwire.ShortChannelID]

	// timeout ends a negotiation that takes too long.
	timeout *time.Timer

	// request is the splice we started, nil for the peer's; updates
	// reports its steps to whoever started it.
	request *Request
	updates chan Update

	// ourOutputs are the outputs we add: change, or the destination of
	// funds moved out.
	ourOutputs []interactivetx.Output

	// lockTime is the transaction's locktime, from our splice_init.
	lockTime uint32

	// abortReason is why the splice ended, for the report.
	abortReason    string
	reasonFromPeer bool
}

// Machine is a channel's splice machine.
type Machine struct {
	cfg Config

	// events is the machine's input, read by its goroutine only.
	events chan func()

	// mu guards what the link reads from its own goroutine: the
	// reestablish fields.
	mu sync.Mutex

	// active is the splice in progress, if any. Only the machine's
	// goroutine changes it, under mu.
	active *active

	// misbehaved is set when the peer aborted after we signed: its
	// splices are refused from then on.
	misbehaved bool

	// dead are splices given up on, waiting to leave the wallet.
	dead map[chainhash.Hash]deadSplice

	quit     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// NewMachine returns a channel's splice machine.
func NewMachine(cfg Config) *Machine {
	if cfg.Policy.NegotiationTimeout == 0 {
		cfg.Policy.NegotiationTimeout = 30 * time.Second
	}
	if cfg.Policy.MaxPeerAddMessages == 0 {
		cfg.Policy.MaxPeerAddMessages =
			interactivetx.DefaultMaxPeerAddMessages
	}

	return &Machine{
		cfg:    cfg,
		events: make(chan func(), 64),
		dead:   make(map[chainhash.Hash]deadSplice),
		quit:   make(chan struct{}),
	}
}

// Start restores a splice in progress from the channel's records and starts
// the machine.
func (m *Machine) Start() error {
	if err := m.restore(); err != nil {
		return err
	}

	m.wg.Add(1)
	go m.run()

	return nil
}

// Stop stops the machine. Calling it again does nothing.
func (m *Machine) Stop() {
	m.stopOnce.Do(func() {
		close(m.quit)
		m.wg.Wait()
	})
}

// run is the machine's goroutine.
func (m *Machine) run() {
	defer m.wg.Done()

	for {
		select {
		case ev := <-m.events:
			ev()

		case <-m.quit:
			return
		}
	}
}

// enqueue runs f on the machine's goroutine. It never blocks the caller
// beyond the queue's capacity; a full queue means a peer flooding the
// channel, and the connection is dropped.
func (m *Machine) enqueue(f func()) {
	select {
	case m.events <- f:
	case <-m.quit:
	default:
		log.Errorf("ChannelID(%v): splice queue full, disconnecting",
			m.cfg.ChanID)
		m.cfg.Peer.Disconnect(errors.New("splice message flood"))
	}
}

// HandleSpliceMessage takes a splice message from the peer.
func (m *Machine) HandleSpliceMessage(msg lnwire.Message) {
	m.enqueue(func() { m.handleMessage(msg) })
}

// SpliceCommitReceived takes the link's result for the peer's
// commitment_signed for a splice.
func (m *Machine) SpliceCommitReceived(txid chainhash.Hash, err error) {
	m.enqueue(func() { m.handleCommitReceived(txid, err) })
}

// setActive changes the splice in progress, under mu.
func (m *Machine) setActive(a *active) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.active != nil && m.active.timeout != nil {
		m.active.timeout.Stop()
	}
	m.active = a
}

// update runs f on the splice in progress under mu, so the link's reads see
// a consistent state.
func (m *Machine) update(f func(a *active)) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.active != nil {
		f(m.active)
	}
}

// send sends messages to the peer, logging a failure: the peer gets them
// again on reconnecting where the protocol needs it.
func (m *Machine) send(msgs ...lnwire.Message) {
	if err := m.cfg.Peer.SendMessage(false, msgs...); err != nil {
		log.Errorf("ChannelID(%v): unable to send %T: %v",
			m.cfg.ChanID, msgs[0], err)
	}
}

// abort sends tx_abort with a reason.
func (m *Machine) abort(code string, format string, args ...interface{}) {
	reason := code
	if format != "" {
		reason = code + ": " + fmt.Sprintf(format, args...)
	}
	log.Infof("ChannelID(%v): splice aborted: %v", m.cfg.ChanID, reason)
	m.update(func(a *active) {
		if a.abortReason == "" {
			a.abortReason = reason
		}
	})

	m.send(&lnwire.TxAbort{
		ChannelID: m.cfg.ChanID, Data: []byte(reason),
	})
}

// warnAndDisconnect sends a warning and drops the connection, for a peer
// breaking the protocol.
func (m *Machine) warnAndDisconnect(format string, args ...interface{}) {
	text := fmt.Sprintf(format, args...)
	log.Warnf("ChannelID(%v): %v", m.cfg.ChanID, text)

	m.send(&lnwire.Warning{ChanID: m.cfg.ChanID, Data: []byte(text)})
	m.cfg.Peer.Disconnect(errors.New(text))
}

// resume ends the channel's quiescence.
func (m *Machine) resume() {
	select {
	case err := <-m.cfg.Link.ResumeQuiescence():
		if err != nil {
			log.Errorf("ChannelID(%v): unable to end quiescence: %v",
				m.cfg.ChanID, err)
		}
	case <-m.quit:
	}
}

// forget drops a splice that didn't get past negotiation or signing on our
// side: its funding leaves the channel's set, its coins are released, and
// quiescence ends.
func (m *Machine) forget() {
	a := m.active
	if a == nil {
		return
	}

	// Out of the channel's funding set, which deletes its record; one
	// that never got in has only its record.
	if a.res != nil {
		err := m.cfg.Channel.RemovePendingFunding(a.txid)
		switch {
		case errors.Is(err, lnwallet.ErrUnknownFunding):
			m.forgetRecord(a.txid)

		case err != nil:
			log.Errorf("ChannelID(%v): unable to drop splice %v: %v",
				m.cfg.ChanID, a.txid, err)
		}
	}
	if len(a.ourInputs) > 0 {
		if err := ReleaseCoins(m.cfg.Wallet, a.ourInputs); err != nil {
			log.Errorf("ChannelID(%v): unable to release coins: %v",
				m.cfg.ChanID, err)
		}
	}

	m.recordLast("aborted", a.abortReason, a.reasonFromPeer)
	m.notify("aborted", a.txid, a.abortReason, a.reasonFromPeer)
	m.report(Update{
		State: StateAborted, Txid: a.txid, Reason: a.abortReason,
		ReasonFromPeer: a.reasonFromPeer,
	})
	m.setActive(nil)
	m.resume()
}

// recordLast records how the splice in progress ended, for the channel's
// listing.
func (m *Machine) recordLast(outcome, reason string, fromPeer bool) {
	a := m.active
	if a == nil {
		return
	}

	err := m.cfg.Channel.State().PutLastSplice(&channeldb.LastSplice{
		WeInitiated:    a.role == interactivetx.Initiator,
		LocalDelta:     a.ourContribution,
		RemoteDelta:    a.theirContribution,
		Txid:           a.txid,
		Time:           time.Now().Unix(),
		Outcome:        outcome,
		Reason:         reason,
		ReasonFromPeer: fromPeer,
	})
	if err != nil {
		log.Errorf("ChannelID(%v): unable to record the last splice: "+
			"%v", m.cfg.ChanID, err)
	}
}

// handleMessage dispatches a splice message.
func (m *Machine) handleMessage(msg lnwire.Message) {
	switch msg := msg.(type) {
	case *lnwire.SpliceInit:
		m.handleSpliceInit(msg)

	case *lnwire.SpliceAck:
		m.handleSpliceAck(msg)

	case *lnwire.TxAddInput, *lnwire.TxAddOutput, *lnwire.TxRemoveInput,
		*lnwire.TxRemoveOutput, *lnwire.TxComplete:

		m.handleConstruction(msg)

	case *lnwire.TxSignatures:
		m.handleTxSignatures(msg)

	case *lnwire.TxAbort:
		m.handleTxAbort(msg)

	case *lnwire.TxInitRbf:
		// Replacing a pending splice comes later; the spec lets us
		// refuse it.
		m.abort(codeRbfUnsupported, "")

	case *lnwire.TxAckRbf:
		m.warnAndDisconnect("tx_ack_rbf without our tx_init_rbf")

	case *lnwire.SpliceLocked:
		m.handleSpliceLocked(msg.SpliceTxid)

	default:
		log.Warnf("ChannelID(%v): unexpected splice message %T",
			m.cfg.ChanID, msg)
	}
}

// currentFunding returns the channel's current funding output: outpoint,
// value and the 2-of-2 witness script.
func (m *Machine) currentFunding() (wire.OutPoint, btcutil.Amount, []byte,
	[]byte, error) {

	state := m.cfg.Channel.State()
	witness, out, err := input.GenFundingPkScript(
		state.LocalChanCfg.MultiSigKey.PubKey.SerializeCompressed(),
		state.RemoteChanCfg.MultiSigKey.PubKey.SerializeCompressed(),
		int64(state.Capacity),
	)
	if err != nil {
		return wire.OutPoint{}, 0, nil, nil, err
	}

	return state.CurrentFundingOutpoint(), state.Capacity, witness,
		out.PkScript, nil
}

// spliceable returns why the channel can't be spliced now, or "".
func (m *Machine) spliceable() string {
	state := m.cfg.Channel.State()
	switch {
	case m.cfg.Disabled:
		return codeDisabled

	case !state.ChanType.HasUnifiedSigs():
		return codeNoUnifiedSigs

	case state.ChanType.IsTaproot():
		return codeTaproot

	case state.ChanType.HasZeroConf():
		return codeZeroConf

	case m.misbehaved:
		return codePeerMisbehaved

	case m.active != nil || len(m.cfg.Channel.PendingFundings()) > 0:
		return codePending

	case state.HasChanStatus(channeldb.ChanStatusCoopBroadcasted),
		state.HasChanStatus(channeldb.ChanStatusLocalCloseInitiator),
		state.HasChanStatus(channeldb.ChanStatusRemoteCloseInitiator),
		state.HasChanStatus(channeldb.ChanStatusFundingSpentUnknown):

		return codeClosing
	}

	return ""
}
