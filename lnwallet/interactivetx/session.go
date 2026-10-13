// Package interactivetx builds a transaction together with a peer, as BOLT 2's
// interactive transaction construction describes, for splicing.
//
// The package is pure: it holds no locks, does no I/O and calls no wallet.
// Everything that needs the outside world (is this outpoint ours, is this
// input confirmed) is a callback in Config, so every rule here can be tested
// and fuzzed on its own.
package interactivetx

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/fn/v2"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/lnwallet/chainfee"
	"github.com/lightningnetwork/lnd/lnwire"
)

// Role is a node's role in the construction.
type Role uint8

const (
	// Initiator started the splice. Its serial ids are even, it adds the
	// shared input and the funding output, and it pays for the common
	// fields of the transaction.
	Initiator Role = iota

	// Acceptor answered the splice. Its serial ids are odd.
	Acceptor
)

// String returns the role's name.
func (r Role) String() string {
	if r == Initiator {
		return "initiator"
	}

	return "acceptor"
}

// other returns the peer's role.
func (r Role) other() Role {
	if r == Initiator {
		return Acceptor
	}

	return Initiator
}

// parity returns the serial id parity of a role: 0 for the initiator.
func (r Role) parity() uint64 {
	if r == Initiator {
		return 0
	}

	return 1
}

// Spec limits (BOLT 2).
const (
	// MaxInputsOrOutputs is the most inputs, and separately outputs, the
	// transaction may have.
	MaxInputsOrOutputs = 252

	// MaxStandardWeight is the largest standard transaction weight.
	MaxStandardWeight = 400_000

	// SpecMaxAddMessages is how many tx_add_input, and separately
	// tx_add_output, messages a peer may send.
	SpecMaxAddMessages = 4096

	// DefaultMaxPeerAddMessages is how many of each this node accepts
	// unless configured otherwise. Aborting is always allowed, and a
	// splice needs only a few.
	DefaultMaxPeerAddMessages = 64

	// maxExcessFee is how far beyond its share of the fee a side may
	// pay, on top of twice its share: a side can't hand its balance to
	// the miners and go below its reserve that way.
	maxExcessFee = btcutil.Amount(1_000)

	// MaxSequence is the highest sequence an input may use: the
	// transaction must stay replaceable.
	MaxSequence = 0xfffffffd

	// maxMoney is the most satoshis that can exist.
	maxMoney = btcutil.Amount(21_000_000 * btcutil.SatoshiPerBitcoin)
)

// SharedInput is the current funding output, spent by a splice.
type SharedInput struct {
	// OutPoint is the funding outpoint.
	OutPoint wire.OutPoint

	// Value is the channel's current capacity.
	Value btcutil.Amount

	// PkScript is the funding output's script.
	PkScript []byte

	// WitnessWeight is the weight of the 2-of-2 witness that will spend
	// it, for fees.
	WitnessWeight int64
}

// Input is an input of the transaction under construction.
type Input struct {
	// SerialID orders the input and says who added it.
	SerialID uint64

	// OutPoint is the spent output.
	OutPoint wire.OutPoint

	// Value is the spent output's value.
	Value btcutil.Amount

	// PkScript is the spent output's script.
	PkScript []byte

	// Sequence is the input's sequence number.
	Sequence uint32

	// Shared is true for the input spending the current funding output.
	Shared bool

	// MaxWitnessWeight is the largest witness this input can carry.
	MaxWitnessWeight int64

	// PrevTx is the transaction holding the spent output, for this
	// node's own inputs (it is sent in tx_add_input). For the peer's
	// inputs only the outpoint, value and script are kept, never the
	// whole transaction.
	PrevTx *wire.MsgTx
}

// Output is an output of the transaction under construction.
type Output struct {
	// SerialID orders the output and says who added it.
	SerialID uint64

	// Value is the output's value.
	Value btcutil.Amount

	// PkScript is the output's script.
	PkScript []byte

	// Funding is true for the new funding output.
	Funding bool
}

// RefusePeerItems makes the session refuse any input or output the peer
// adds, with reason: for a construction where the peer puts nothing in.
func (s *Session) RefusePeerItems(reason string) {
	s.cfg.CheckPeerInput = func(*Input) error {
		return &AbortError{Reason: reason}
	}
	s.cfg.CheckPeerOutput = func(*Output) error {
		return &AbortError{Reason: reason}
	}
}

// addedBy returns the role that added an item with the given serial id.
func addedBy(serialID uint64) Role {
	if serialID%2 == 0 {
		return Initiator
	}

	return Acceptor
}

// Config describes one construction.
type Config struct {
	// Role is this node's role.
	Role Role

	// ChannelID identifies the channel in every message.
	ChannelID lnwire.ChannelID

	// Shared is the current funding output the splice spends.
	Shared SharedInput

	// FundingScript is the new funding output's script.
	FundingScript []byte

	// InitiatorContribution and AcceptorContribution are the funding
	// contributions from splice_init and splice_ack.
	InitiatorContribution btcutil.Amount
	AcceptorContribution  btcutil.Amount

	// FeeRate is the transaction's feerate.
	FeeRate chainfee.SatPerKWeight

	// LockTime is the transaction's locktime.
	LockTime uint32

	// MaxPeerAddMessages is how many tx_add_input, and separately
	// tx_add_output, messages this node accepts from the peer. Aborting is
	// always allowed, so it can be far below the spec's 4096. Zero means
	// DefaultMaxPeerAddMessages.
	MaxPeerAddMessages int

	// CheckPeerInput applies this node's policy to an input the peer
	// adds: confirmed and unspent, not one of this node's own outpoints
	// (wallet coins, locked or not, and every channel's funding output).
	// An error aborts the construction. Required.
	CheckPeerInput func(in *Input) error

	// CheckPeerOutput applies this node's policy to an output the peer
	// adds (relay standardness). An error aborts. Required.
	CheckPeerOutput func(out *Output) error

	// DustLimit returns the dust limit of an output with the given script:
	// no output, the funding output included, may be below it. Required.
	DustLimit func(pkScript []byte) btcutil.Amount

	// WitnessWeight returns the largest witness for an input spending the
	// given script, or an error for a script this node won't accept from
	// the peer.
	WitnessWeight func(pkScript []byte) (int64, error)

	// Reserve returns the channel reserve for a channel of the given
	// capacity, applied at completion to a side that adds an output other
	// than the funding output.
	Reserve func(capacity btcutil.Amount) btcutil.Amount

	// InitiatorBalance and AcceptorBalance are each side's channel balance
	// before the splice.
	InitiatorBalance btcutil.Amount
	AcceptorBalance  btcutil.Amount
}

// AbortError ends a construction; its reason goes into tx_abort.
type AbortError struct {
	Reason string
}

// Error returns the reason.
func (e *AbortError) Error() string {
	return e.Reason
}

// abortf returns an AbortError with a formatted reason.
func abortf(format string, args ...interface{}) error {
	return &AbortError{Reason: fmt.Sprintf(format, args...)}
}

// ErrNotOurTurn is returned when a message arrives out of turn.
var ErrNotOurTurn = errors.New("message received out of turn")

// Session is one interactive construction.
type Session struct {
	cfg Config

	inputs  map[uint64]*Input
	outputs map[uint64]*Output

	// spent guards against the same outpoint added twice.
	spent map[wire.OutPoint]uint64

	// pendingInputs and pendingOutputs are what this node still has to
	// send.
	pendingInputs  []*Input
	pendingOutputs []*Output
	nextSerial     uint64

	// peerAddInputs and peerAddOutputs count the peer's add messages.
	peerAddInputs  int
	peerAddOutputs int

	ourTurn      bool
	sentComplete bool
	recvComplete bool
	failed       error
}

// NewSession starts a construction.
func NewSession(cfg Config) (*Session, error) {
	if cfg.WitnessWeight == nil {
		return nil, errors.New("WitnessWeight is required")
	}
	if cfg.Reserve == nil {
		return nil, errors.New("Reserve is required")
	}
	if cfg.CheckPeerInput == nil || cfg.CheckPeerOutput == nil ||
		cfg.DustLimit == nil {

		return nil, errors.New("CheckPeerInput, CheckPeerOutput and " +
			"DustLimit are required")
	}
	for _, amt := range []btcutil.Amount{
		cfg.Shared.Value, cfg.InitiatorBalance, cfg.AcceptorBalance,
	} {
		if amt < 0 || amt > maxMoney {
			return nil, fmt.Errorf("amount %d out of range", amt)
		}
	}
	for _, c := range []btcutil.Amount{
		cfg.InitiatorContribution, cfg.AcceptorContribution,
	} {
		if c < -maxMoney || c > maxMoney {
			return nil, fmt.Errorf("contribution %d out of range",
				c)
		}
	}
	if len(cfg.FundingScript) == 0 {
		return nil, errors.New("FundingScript is required")
	}
	if cfg.MaxPeerAddMessages <= 0 {
		cfg.MaxPeerAddMessages = DefaultMaxPeerAddMessages
	}
	if cfg.MaxPeerAddMessages > SpecMaxAddMessages {
		cfg.MaxPeerAddMessages = SpecMaxAddMessages
	}

	return &Session{
		cfg:        cfg,
		inputs:     make(map[uint64]*Input),
		outputs:    make(map[uint64]*Output),
		spent:      make(map[wire.OutPoint]uint64),
		nextSerial: cfg.Role.parity(),
		ourTurn:    cfg.Role == Initiator,
	}, nil
}

// serial returns this node's next serial id.
func (s *Session) serial() uint64 {
	id := s.nextSerial
	s.nextSerial += 2

	return id
}

// AddLocalInput queues one of this node's wallet inputs. Call before the
// construction starts.
func (s *Session) AddLocalInput(in Input) {
	in.SerialID = s.serial()
	s.pendingInputs = append(s.pendingInputs, &in)
}

// AddLocalOutput queues one of this node's outputs.
func (s *Session) AddLocalOutput(out Output) {
	out.SerialID = s.serial()
	s.pendingOutputs = append(s.pendingOutputs, &out)
}

// AddSharedInputAndFunding queues the shared input and the funding output.
// Only the initiator calls it.
func (s *Session) AddSharedInputAndFunding() error {
	if s.cfg.Role != Initiator {
		return errors.New("only the initiator adds the shared input")
	}

	s.pendingInputs = append([]*Input{{
		SerialID:         s.serial(),
		OutPoint:         s.cfg.Shared.OutPoint,
		Value:            s.cfg.Shared.Value,
		PkScript:         s.cfg.Shared.PkScript,
		Sequence:         MaxSequence,
		Shared:           true,
		MaxWitnessWeight: s.cfg.Shared.WitnessWeight,
	}}, s.pendingInputs...)

	s.pendingOutputs = append([]*Output{{
		SerialID: s.serial(),
		Value:    s.fundingValue(),
		PkScript: s.cfg.FundingScript,
		Funding:  true,
	}}, s.pendingOutputs...)

	return nil
}

// fundingValue is the new funding output's value: the current capacity with
// both contributions applied.
func (s *Session) fundingValue() btcutil.Amount {
	return s.cfg.Shared.Value + s.cfg.InitiatorContribution +
		s.cfg.AcceptorContribution
}

// Done reports whether both sides have sent tx_complete in a row.
func (s *Session) Done() bool {
	return s.failed == nil && s.sentComplete && s.recvComplete
}

// OurTurn reports whether this node sends next.
func (s *Session) OurTurn() bool {
	return s.ourTurn && !s.Done() && s.failed == nil
}

// Next returns the message this node sends next: its queued additions, then
// tx_complete.
func (s *Session) Next() (lnwire.Message, error) {
	if s.failed != nil {
		return nil, s.failed
	}
	if !s.OurTurn() {
		return nil, ErrNotOurTurn
	}
	s.ourTurn = false

	if len(s.pendingInputs) > 0 {
		in := s.pendingInputs[0]
		s.pendingInputs = s.pendingInputs[1:]
		s.sentComplete = false

		return s.sendInput(in)
	}

	if len(s.pendingOutputs) > 0 {
		out := s.pendingOutputs[0]
		s.pendingOutputs = s.pendingOutputs[1:]
		s.sentComplete = false
		s.outputs[out.SerialID] = out

		return &lnwire.TxAddOutput{
			ChannelID: s.cfg.ChannelID,
			SerialID:  out.SerialID,
			Amount:    out.Value,
			Script:    out.PkScript,
		}, nil
	}

	s.sentComplete = true

	return &lnwire.TxComplete{ChannelID: s.cfg.ChannelID}, nil
}

// sendInput records and encodes one of this node's inputs.
func (s *Session) sendInput(in *Input) (lnwire.Message, error) {
	// The peer may have added the same outpoint first (it can know one of
	// this node's coins); a transaction spending it twice could never
	// confirm.
	if prev, ok := s.spent[in.OutPoint]; ok {
		return nil, s.fail(abortf("outpoint %v already added as "+
			"serial id %d", in.OutPoint, prev))
	}

	s.inputs[in.SerialID] = in
	s.spent[in.OutPoint] = in.SerialID

	msg := &lnwire.TxAddInput{
		ChannelID:  s.cfg.ChannelID,
		SerialID:   in.SerialID,
		PrevTxVout: in.OutPoint.Index,
		Sequence:   in.Sequence,
	}
	if in.Shared {
		msg.SharedInputTxid = fn.Some(in.OutPoint.Hash)

		return msg, nil
	}

	// Without witnesses: the txid is the same, and the message stays
	// within its u16 length.
	var b bytes.Buffer
	if err := in.PrevTx.SerializeNoWitness(&b); err != nil {
		return nil, err
	}
	msg.PrevTx = b.Bytes()

	return msg, nil
}

// Receive processes a message from the peer.
func (s *Session) Receive(msg lnwire.Message) error {
	if s.failed != nil {
		return s.failed
	}
	if s.ourTurn || s.Done() {
		return s.fail(abortf("%v received out of turn", msg.MsgType()))
	}

	var err error
	switch m := msg.(type) {
	case *lnwire.TxAddInput:
		err = s.receiveInput(m)
		s.recvComplete = false

	case *lnwire.TxAddOutput:
		err = s.receiveOutput(m)
		s.recvComplete = false

	case *lnwire.TxRemoveInput:
		err = s.receiveRemove(m.SerialID, true)
		s.recvComplete = false

	case *lnwire.TxRemoveOutput:
		err = s.receiveRemove(m.SerialID, false)
		s.recvComplete = false

	case *lnwire.TxComplete:
		s.recvComplete = true

	default:
		err = abortf("unexpected %v during construction",
			msg.MsgType())
	}
	if err != nil {
		return s.fail(err)
	}

	// The peer's tx_complete answering ours ends the construction; there
	// is nothing more to send.
	if !s.Done() {
		s.ourTurn = true
	}

	return nil
}

// fail records the construction's failure.
func (s *Session) fail(err error) error {
	s.failed = err

	return err
}

// checkPeerSerial checks a serial id the peer uses for a new item.
func (s *Session) checkPeerSerial(serialID uint64) error {
	if addedBy(serialID) != s.cfg.Role.other() {
		return abortf("serial id %d has the wrong parity for the %v",
			serialID, s.cfg.Role.other())
	}
	if _, ok := s.inputs[serialID]; ok {
		return abortf("serial id %d already used", serialID)
	}
	if _, ok := s.outputs[serialID]; ok {
		return abortf("serial id %d already used", serialID)
	}

	return nil
}

// receiveInput validates and records an input the peer adds.
func (s *Session) receiveInput(m *lnwire.TxAddInput) error {
	s.peerAddInputs++
	if s.peerAddInputs > s.cfg.MaxPeerAddMessages {
		return abortf("more than %d tx_add_input",
			s.cfg.MaxPeerAddMessages)
	}
	if err := s.checkPeerSerial(m.SerialID); err != nil {
		return err
	}
	if len(s.inputs) >= MaxInputsOrOutputs {
		return abortf("more than %d inputs", MaxInputsOrOutputs)
	}
	if m.Sequence > MaxSequence {
		return abortf("input sequence %#x is final", m.Sequence)
	}

	// A relative lock (BIP 68) would keep the transaction from
	// confirming for up to 65,535 blocks or 388 days, the channel stuck
	// with it pending.
	if m.Sequence&wire.SequenceLockTimeDisabled == 0 {
		return abortf("input sequence %#x is a relative lock",
			m.Sequence)
	}

	in := &Input{
		SerialID: m.SerialID,
		Sequence: m.Sequence,
	}

	if m.SharedInputTxid.IsSome() {
		// Only the initiator adds the shared input, once, without a
		// prevtx, and it must be the current funding output.
		if addedBy(m.SerialID) != Initiator {
			return abortf("the acceptor added the shared input")
		}
		if len(m.PrevTx) != 0 {
			return abortf("the shared input carries a prevtx")
		}
		txid := m.SharedInputTxid.UnwrapOr(chainhash.Hash{})
		op := wire.OutPoint{Hash: txid, Index: m.PrevTxVout}
		if op != s.cfg.Shared.OutPoint {
			return abortf("shared input %v is not the funding "+
				"output %v", op, s.cfg.Shared.OutPoint)
		}
		in.OutPoint = op
		in.Shared = true
		in.Value = s.cfg.Shared.Value
		in.PkScript = s.cfg.Shared.PkScript
		in.MaxWitnessWeight = s.cfg.Shared.WitnessWeight
	} else {
		if err := checkTxCounts(m.PrevTx); err != nil {
			return abortf("prevtx doesn't parse: %v", err)
		}
		prevTx := wire.NewMsgTx(2)
		if err := prevTx.Deserialize(bytes.NewReader(m.PrevTx)); err != nil {
			return abortf("prevtx doesn't parse: %v", err)
		}
		if int(m.PrevTxVout) >= len(prevTx.TxOut) {
			return abortf("prevtx_vout %d out of range",
				m.PrevTxVout)
		}
		out := prevTx.TxOut[m.PrevTxVout]
		if !txscript.IsWitnessProgram(out.PkScript) {
			return abortf("input spends a non-segwit output")
		}
		if out.Value < 0 || btcutil.Amount(out.Value) > maxMoney {
			return abortf("input value %d out of range", out.Value)
		}

		// A coinbase output relays only after a long maturity on this
		// chain, so a splice spending one could sit unconfirmed with
		// the channel unable to close cooperatively or splice again.
		if blockchainIsCoinBase(prevTx) {
			return abortf("input spends a coinbase output")
		}
		weight, err := s.cfg.WitnessWeight(out.PkScript)
		if err != nil {
			return abortf("input type refused: %v", err)
		}

		// Only what's needed is kept; the rest of the prevtx is
		// ignored.
		in.OutPoint = wire.OutPoint{
			Hash: prevTx.TxHash(), Index: m.PrevTxVout,
		}
		in.Value = btcutil.Amount(out.Value)
		in.PkScript = out.PkScript
		in.MaxWitnessWeight = weight

		if in.OutPoint == s.cfg.Shared.OutPoint {
			return abortf("the funding output added as a plain " +
				"input")
		}
	}

	if prev, ok := s.spent[in.OutPoint]; ok {
		return abortf("outpoint %v already added as serial id %d",
			in.OutPoint, prev)
	}
	if in.Shared {
		for _, existing := range s.inputs {
			if existing.Shared {
				return abortf("a second shared input")
			}
		}
	}

	if !in.Shared {
		if err := s.cfg.CheckPeerInput(in); err != nil {
			return abortf("input %v refused: %v", in.OutPoint, err)
		}
	}

	s.inputs[in.SerialID] = in
	s.spent[in.OutPoint] = in.SerialID

	return nil
}

// receiveOutput validates and records an output the peer adds.
func (s *Session) receiveOutput(m *lnwire.TxAddOutput) error {
	s.peerAddOutputs++
	if s.peerAddOutputs > s.cfg.MaxPeerAddMessages {
		return abortf("more than %d tx_add_output",
			s.cfg.MaxPeerAddMessages)
	}
	if err := s.checkPeerSerial(m.SerialID); err != nil {
		return err
	}
	if len(s.outputs) >= MaxInputsOrOutputs {
		return abortf("more than %d outputs", MaxInputsOrOutputs)
	}
	if m.Amount < 0 || m.Amount > maxMoney {
		return abortf("output amount %d out of range", m.Amount)
	}
	if m.Amount < s.cfg.DustLimit(m.Script) {
		return abortf("output of %d is below the dust limit",
			m.Amount)
	}

	out := &Output{
		SerialID: m.SerialID,
		Value:    m.Amount,
		PkScript: m.Script,
		Funding:  bytes.Equal(m.Script, s.cfg.FundingScript),
	}

	// The funding output is the initiator's to add; anything else goes
	// through this node's policy.
	if out.Funding {
		if addedBy(m.SerialID) != Initiator {
			return abortf("the acceptor added the funding output")
		}
	} else {
		if err := s.cfg.CheckPeerOutput(out); err != nil {
			return abortf("output refused: %v", err)
		}
	}

	s.outputs[out.SerialID] = out

	return nil
}

// receiveRemove removes an input or output the peer added earlier.
func (s *Session) receiveRemove(serialID uint64, input bool) error {
	if addedBy(serialID) != s.cfg.Role.other() {
		return abortf("serial id %d isn't the peer's to remove",
			serialID)
	}

	if input {
		in, ok := s.inputs[serialID]
		if !ok {
			return abortf("no input with serial id %d", serialID)
		}
		delete(s.inputs, serialID)
		delete(s.spent, in.OutPoint)

		return nil
	}

	if _, ok := s.outputs[serialID]; !ok {
		return abortf("no output with serial id %d", serialID)
	}
	delete(s.outputs, serialID)

	return nil
}

// Result is a completed construction.
type Result struct {
	// Tx is the unsigned transaction, inputs and outputs in serial id
	// order.
	Tx *wire.MsgTx

	// Inputs and Outputs are in the same order as Tx's.
	Inputs  []*Input
	Outputs []*Output

	// FundingIndex is the new funding output's index in Tx.
	FundingIndex uint32

	// SharedIndex is the shared input's index in Tx.
	SharedIndex uint32

	// Fee is the transaction's total fee.
	Fee btcutil.Amount

	// Weight is the transaction's weight with every witness at its
	// largest.
	Weight int64
}

// Build checks the completed construction against every rule that applies
// at tx_complete and returns the transaction.
func (s *Session) Build() (*Result, error) {
	if !s.Done() {
		return nil, errors.New("construction not complete")
	}

	res := &Result{Tx: wire.NewMsgTx(2)}
	res.Tx.LockTime = s.cfg.LockTime

	for _, in := range s.inputs {
		res.Inputs = append(res.Inputs, in)
	}
	sort.Slice(res.Inputs, func(i, j int) bool {
		return res.Inputs[i].SerialID < res.Inputs[j].SerialID
	})
	for _, out := range s.outputs {
		res.Outputs = append(res.Outputs, out)
	}
	sort.Slice(res.Outputs, func(i, j int) bool {
		return res.Outputs[i].SerialID < res.Outputs[j].SerialID
	})

	if len(res.Inputs) > MaxInputsOrOutputs ||
		len(res.Outputs) > MaxInputsOrOutputs {

		return nil, s.fail(abortf("%d inputs and %d outputs exceed "+
			"%d", len(res.Inputs), len(res.Outputs),
			MaxInputsOrOutputs))
	}

	seen := make(map[wire.OutPoint]struct{}, len(res.Inputs))
	for _, in := range res.Inputs {
		if _, ok := seen[in.OutPoint]; ok {
			return nil, s.fail(abortf("outpoint %v spent twice",
				in.OutPoint))
		}
		seen[in.OutPoint] = struct{}{}
	}

	var shared, funding int
	for i, in := range res.Inputs {
		res.Tx.AddTxIn(&wire.TxIn{
			PreviousOutPoint: in.OutPoint,
			Sequence:         in.Sequence,
		})
		if in.Shared {
			shared++
			res.SharedIndex = uint32(i)
		}
	}
	for i, out := range res.Outputs {
		res.Tx.AddTxOut(&wire.TxOut{
			Value: int64(out.Value), PkScript: out.PkScript,
		})
		if out.Funding {
			funding++
			res.FundingIndex = uint32(i)
		}
	}

	if shared != 1 {
		return nil, s.fail(abortf("%d shared inputs, expected one",
			shared))
	}
	if funding != 1 {
		return nil, s.fail(abortf("%d funding outputs, expected one",
			funding))
	}
	if got := res.Outputs[res.FundingIndex].Value; got != s.fundingValue() {
		return nil, s.fail(abortf("funding output of %d, expected %d",
			got, s.fundingValue()))
	}
	if s.fundingValue() < s.cfg.DustLimit(s.cfg.FundingScript) {
		return nil, s.fail(abortf("funding output of %d is dust",
			s.fundingValue()))
	}

	// A transaction of a single input with the state hint markers would
	// look like a commitment to the chain watchers of both nodes.
	if len(res.Tx.TxIn) == 1 && res.Tx.TxIn[0].Sequence>>24 == 0x80 &&
		res.Tx.LockTime>>24 == 0x20 {

		return nil, s.fail(abortf("transaction shaped like a " +
			"commitment"))
	}

	weights := s.weights(res)
	res.Weight = weights.total
	if res.Weight > MaxStandardWeight {
		return nil, s.fail(abortf("weight %d exceeds %d", res.Weight,
			MaxStandardWeight))
	}

	if err := s.checkAccounting(res, weights); err != nil {
		return nil, s.fail(err)
	}
	if err := s.checkReserves(res); err != nil {
		return nil, s.fail(err)
	}

	// Every value was range checked, so neither sum can pass the 64-bit
	// range before the check below catches it.
	var in, out btcutil.Amount
	for _, i := range res.Inputs {
		in += i.Value
	}
	for _, o := range res.Outputs {
		out += o.Value
	}
	if in > maxMoney*MaxInputsOrOutputs || in < out {
		return nil, s.fail(abortf("inputs of %d don't cover outputs "+
			"of %d", in, out))
	}
	res.Fee = in - out

	// Each side's share is rounded down; the transaction as a whole must
	// still pay the agreed feerate.
	if res.Fee < s.cfg.FeeRate.FeeForWeight(lntypes.WeightUnit(res.Weight)) {
		return nil, s.fail(abortf("fee of %d below the feerate for "+
			"weight %d", res.Fee, res.Weight))
	}

	return res, nil
}

// sideWeights is the weight each side pays for, and the total.
type sideWeights struct {
	initiator int64
	acceptor  int64
	total     int64
}

// weights attributes the transaction's weight: the initiator pays for the
// common fields, the shared input and the funding output; each side pays for
// what it added.
func (s *Session) weights(res *Result) sideWeights {
	var w sideWeights

	common := CommonWeight(len(res.Inputs), len(res.Outputs))
	w.initiator += common
	w.total += common

	for _, in := range res.Inputs {
		iw := InputWeight(in.MaxWitnessWeight)
		w.total += iw
		if in.Shared || addedBy(in.SerialID) == Initiator {
			w.initiator += iw
		} else {
			w.acceptor += iw
		}
	}
	for _, out := range res.Outputs {
		ow := OutputWeight(out.PkScript)
		w.total += ow
		if out.Funding || addedBy(out.SerialID) == Initiator {
			w.initiator += ow
		} else {
			w.acceptor += ow
		}
	}

	return w
}

// CommonWeight is the weight of the fields every transaction has: version,
// locktime, the input and output counts, and the segwit marker and flag. The
// initiator pays for it.
func CommonWeight(numInputs, numOutputs int) int64 {
	base := 4 + 4 + wire.VarIntSerializeSize(uint64(numInputs)) +
		wire.VarIntSerializeSize(uint64(numOutputs))

	return int64(base)*4 + 2
}

// InputWeight is the weight of an input whose largest witness weighs
// maxWitnessWeight.
func InputWeight(maxWitnessWeight int64) int64 {
	// Outpoint, empty scriptSig length, sequence.
	return (32+4+1+4)*4 + maxWitnessWeight
}

// OutputWeight is an output's weight.
func OutputWeight(pkScript []byte) int64 {
	size := 8 + wire.VarIntSerializeSize(uint64(len(pkScript))) +
		len(pkScript)

	return int64(size) * 4
}

// checkAccounting applies the rule that keeps either side from paying for
// the other: the shared input and the funding output are left out of both
// sides' totals (the funding output equals the shared input plus both
// contributions, checked in Build), and for each side
// inputs - outputs - contribution must cover its share of the fee. A side
// can't take any part of the other's channel balance, because balances move
// only by the contributions.
func (s *Session) checkAccounting(res *Result, w sideWeights) error {
	var (
		inInit, inAcc   btcutil.Amount
		outInit, outAcc btcutil.Amount
	)
	for _, in := range res.Inputs {
		if in.Shared {
			continue
		}
		if addedBy(in.SerialID) == Initiator {
			inInit += in.Value
		} else {
			inAcc += in.Value
		}
	}
	for _, out := range res.Outputs {
		if out.Funding {
			continue
		}
		if addedBy(out.SerialID) == Initiator {
			outInit += out.Value
		} else {
			outAcc += out.Value
		}
	}

	check := func(role Role, in, out, contribution btcutil.Amount,
		weight int64) error {

		fee := s.cfg.FeeRate.FeeForWeight(lntypes.WeightUnit(weight))
		paid := in - out - contribution
		if paid < fee {
			return abortf("the %v's inputs (%d) less outputs (%d) "+
				"and contribution (%d) don't cover its fee "+
				"(%d)", role, in, out, contribution, fee)
		}

		// Paying far more than its share would let a side move its
		// balance to the miners, past the reserve rule below.
		if paid > 2*fee+maxExcessFee {
			return abortf("the %v pays %d in fees, far more than "+
				"its share of %d", role, paid, fee)
		}

		return nil
	}

	err := check(
		Initiator, inInit, outInit, s.cfg.InitiatorContribution,
		w.initiator,
	)
	if err != nil {
		return err
	}

	return check(
		Acceptor, inAcc, outAcc, s.cfg.AcceptorContribution,
		w.acceptor,
	)
}

// checkReserves applies the reserve rule: a side that adds an output other
// than the funding output must keep the reserve for the new capacity.
func (s *Session) checkReserves(res *Result) error {
	capacity := s.fundingValue()
	reserve := s.cfg.Reserve(capacity)

	// The reserve binds a side that adds an output, as the spec says,
	// and also one that takes funds out of the channel by any other
	// route, fees included.
	addedOutput := map[Role]bool{
		Initiator: s.cfg.InitiatorContribution < 0,
		Acceptor:  s.cfg.AcceptorContribution < 0,
	}
	for _, out := range res.Outputs {
		if !out.Funding {
			addedOutput[addedBy(out.SerialID)] = true
		}
	}

	balance := map[Role]btcutil.Amount{
		Initiator: s.cfg.InitiatorBalance + s.cfg.InitiatorContribution,
		Acceptor:  s.cfg.AcceptorBalance + s.cfg.AcceptorContribution,
	}
	for _, role := range []Role{Initiator, Acceptor} {
		// A contribution can't take a side below nothing; the peer that
		// asked for it already had to be refused at splice_init, but the
		// rule is checked here too.
		if balance[role] < 0 {
			return abortf("the %v's contribution takes its balance "+
				"below zero", role)
		}
		if addedOutput[role] && balance[role] < reserve {
			return abortf("the %v adds an output and keeps %d, "+
				"below the reserve of %d", role, balance[role],
				reserve)
		}
	}

	return nil
}

// blockchainIsCoinBase reports whether tx is a coinbase: a single input
// spending the null outpoint.
func blockchainIsCoinBase(tx *wire.MsgTx) bool {
	if len(tx.TxIn) != 1 {
		return false
	}
	prev := tx.TxIn[0].PreviousOutPoint

	return prev.Index == wire.MaxPrevOutIndex && prev.Hash == (chainhash.Hash{})
}

// errTxCount is a serialized transaction's count of inputs, outputs,
// witness items or bytes larger than what is left of it.
var errTxCount = errors.New("count larger than the transaction")

// checkTxCounts walks a serialized transaction and refuses it when a count
// is larger than the bytes left could hold. btcd's decoder allocates for a
// count before reading what it counts: a witness count of four million in a
// prevtx of a hundred bytes cost about 95 MB.
func checkTxCounts(b []byte) error {
	r := bytes.NewReader(b)
	skip := func(n uint64) error {
		if n > uint64(r.Len()) {
			return errTxCount
		}
		_, err := r.Seek(int64(n), io.SeekCurrent)

		return err
	}
	// count reads a count of things each at least size bytes long.
	count := func(size uint64) (uint64, error) {
		n, err := wire.ReadVarInt(r, 0)
		if err != nil {
			return 0, err
		}
		if n > uint64(r.Len())/size {
			return 0, errTxCount
		}

		return n, nil
	}
	// script skips a length-prefixed field.
	script := func() error {
		n, err := wire.ReadVarInt(r, 0)
		if err != nil {
			return err
		}

		return skip(n)
	}

	if err := skip(4); err != nil {
		return err
	}
	witness := len(b) > 5 && b[4] == 0x00 && b[5] != 0x00
	if witness {
		if err := skip(2); err != nil {
			return err
		}
	}

	// An input: outpoint 36, script length 1, sequence 4.
	inputs, err := count(41)
	if err != nil {
		return err
	}
	for i := uint64(0); i < inputs; i++ {
		if err := skip(36); err != nil {
			return err
		}
		if err := script(); err != nil {
			return err
		}
		if err := skip(4); err != nil {
			return err
		}
	}

	// An output: value 8, script length 1.
	outputs, err := count(9)
	if err != nil {
		return err
	}
	for i := uint64(0); i < outputs; i++ {
		if err := skip(8); err != nil {
			return err
		}
		if err := script(); err != nil {
			return err
		}
	}

	if witness {
		for i := uint64(0); i < inputs; i++ {
			items, err := count(1)
			if err != nil {
				return err
			}
			for j := uint64(0); j < items; j++ {
				if err := script(); err != nil {
					return err
				}
			}
		}
	}

	return skip(4)
}
