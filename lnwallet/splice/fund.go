// Package splice moves funds into and out of a channel without closing it.
package splice

import (
	"errors"
	"fmt"
	"sort"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/input"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/lnwallet/chainfee"
	"github.com/lightningnetwork/lnd/lnwallet/interactivetx"
)

// The splice transaction this node builds as initiator, in M1: the shared
// input, the new funding output, and either wallet inputs with an optional
// change output (add funds) or one output to a destination (move funds out).
// The peer adds nothing. The initiator pays for the common fields, the
// shared input and the funding output, and for what it adds; that is all the
// weight of the transaction.

var (
	// ErrInsufficientFunds is returned when the wallet's coins can't pay
	// for the amount and the fee.
	ErrInsufficientFunds = errors.New("insufficient_funds")

	// ErrBelowDust is returned when an amount, or what is left after the
	// fee, is below the dust limit.
	ErrBelowDust = errors.New("below_min")
)

// Coin is a wallet output that can fund a splice.
type Coin struct {
	// OutPoint is the coin's outpoint.
	OutPoint wire.OutPoint

	// Value is the coin's value.
	Value btcutil.Amount

	// PkScript is the coin's script.
	PkScript []byte

	// PrevTx is the transaction that created the coin, sent to the peer
	// in tx_add_input.
	PrevTx *wire.MsgTx
}

// CoinWitnessWeight returns the largest witness of an input spending
// pkScript, for the scripts a splice input may have: P2WPKH, and P2TR key
// path with a sighash byte. Others (nested P2SH has a scriptSig, P2WSH an
// unknown witness) can't be used.
func CoinWitnessWeight(pkScript []byte) (int64, bool) {
	switch {
	case txscript.IsPayToWitnessPubKeyHash(pkScript):
		return input.P2WKHWitnessSize, true

	case txscript.IsPayToTaproot(pkScript):
		return input.TaprootKeyPathCustomSighashWitnessSize, true

	default:
		return 0, false
	}
}

// Usable reports whether a coin can go into a splice: a script it can
// spend, and not a coinbase output (peers refuse them).
func (c *Coin) Usable() bool {
	if _, ok := CoinWitnessWeight(c.PkScript); !ok {
		return false
	}
	if c.PrevTx == nil || int(c.OutPoint.Index) >= len(c.PrevTx.TxOut) {
		return false
	}

	return !isCoinBase(c.PrevTx)
}

// isCoinBase reports whether tx is a coinbase transaction.
func isCoinBase(tx *wire.MsgTx) bool {
	if len(tx.TxIn) != 1 {
		return false
	}
	prev := tx.TxIn[0].PreviousOutPoint

	return prev.Index == wire.MaxPrevOutIndex && prev.Hash == [32]byte{}
}

// Shape is what every splice of a channel has: the shared input and the new
// funding output.
type Shape struct {
	// SharedWitnessWeight is the weight of the 2-of-2 witness spending
	// the current funding output.
	SharedWitnessWeight int64

	// FundingScript is the new funding output's script.
	FundingScript []byte
}

// weight returns the weight of a splice with the given wallet inputs and
// other outputs, all added by this node as initiator.
func (s Shape) weight(inputWitnesses []int64, outputs [][]byte) int64 {
	numIn, numOut := 1+len(inputWitnesses), 1+len(outputs)

	w := interactivetx.CommonWeight(numIn, numOut) +
		interactivetx.InputWeight(s.SharedWitnessWeight) +
		interactivetx.OutputWeight(s.FundingScript)
	for _, ww := range inputWitnesses {
		w += interactivetx.InputWeight(ww)
	}
	for _, o := range outputs {
		w += interactivetx.OutputWeight(o)
	}

	return w
}

// fee returns the fee of weight at feeRate.
func fee(feeRate chainfee.SatPerKWeight, weight int64) btcutil.Amount {
	return feeRate.FeeForWeight(lntypes.WeightUnit(weight))
}

// AddFunds is a funded splice-in.
type AddFunds struct {
	// Inputs are the wallet coins spent.
	Inputs []Coin

	// Change is the change output's value, zero for none.
	Change btcutil.Amount

	// Contribution is what goes into the channel: the amount asked for,
	// or with Use maximum, everything left after the fee.
	Contribution btcutil.Amount

	// Fee is the transaction's fee, all of it paid by this node.
	Fee btcutil.Amount

	// Weight is the transaction's weight.
	Weight int64
}

// SelectAddFunds picks wallet coins for adding amount to the channel at
// feeRate, largest first, with a change output to changeScript when what is
// left over is worth one. With useMax, every usable coin is spent and
// everything after the fee goes in, without change. dustLimit returns the
// dust limit of an output with the given script.
func SelectAddFunds(shape Shape, coins []Coin, amount btcutil.Amount,
	useMax bool, feeRate chainfee.SatPerKWeight, changeScript []byte,
	dustLimit func([]byte) btcutil.Amount) (*AddFunds, error) {

	var usable []Coin
	for _, c := range coins {
		if c.Usable() {
			usable = append(usable, c)
		}
	}
	sort.SliceStable(usable, func(i, j int) bool {
		return usable[i].Value > usable[j].Value
	})
	if len(usable) > interactivetx.MaxInputsOrOutputs-1 {
		usable = usable[:interactivetx.MaxInputsOrOutputs-1]
	}

	witnesses := func(cs []Coin) []int64 {
		ws := make([]int64, len(cs))
		for i, c := range cs {
			ws[i], _ = CoinWitnessWeight(c.PkScript)
		}

		return ws
	}
	sum := func(cs []Coin) btcutil.Amount {
		var total btcutil.Amount
		for _, c := range cs {
			total += c.Value
		}

		return total
	}

	if useMax {
		if len(usable) == 0 {
			return nil, ErrInsufficientFunds
		}
		w := shape.weight(witnesses(usable), nil)
		f := fee(feeRate, w)
		contribution := sum(usable) - f
		if contribution <= 0 {
			return nil, ErrInsufficientFunds
		}

		return &AddFunds{
			Inputs: usable, Contribution: contribution, Fee: f,
			Weight: w,
		}, nil
	}

	if amount <= 0 {
		return nil, ErrBelowDust
	}

	for n := 1; n <= len(usable); n++ {
		picked := usable[:n]
		total := sum(picked)

		wNoChange := shape.weight(witnesses(picked), nil)
		feeNoChange := fee(feeRate, wNoChange)
		if total < amount+feeNoChange {
			continue
		}

		// Change, when what is left after paying for it is above
		// dust; otherwise the rest goes to the fee. The engine
		// allows a side to overpay by its share plus 1,000 sats,
		// and less than dust plus a change output is always within
		// that.
		wChange := shape.weight(
			witnesses(picked), [][]byte{changeScript},
		)
		feeChange := fee(feeRate, wChange)
		change := total - amount - feeChange
		if change >= dustLimit(changeScript) {
			return &AddFunds{
				Inputs: picked, Change: change,
				Contribution: amount, Fee: feeChange,
				Weight: wChange,
			}, nil
		}

		return &AddFunds{
			Inputs: picked, Contribution: amount,
			Fee: total - amount, Weight: wNoChange,
		}, nil
	}

	return nil, fmt.Errorf("%w: %v available in usable coins",
		ErrInsufficientFunds, sum(usable))
}

// MoveOut is a splice-out to one destination.
type MoveOut struct {
	// Amount is what the destination receives.
	Amount btcutil.Amount

	// Contribution is the channel's negative contribution: the amount
	// and the fee, both from this node's balance.
	Contribution btcutil.Amount

	// Fee is the transaction's fee.
	Fee btcutil.Amount

	// Weight is the transaction's weight.
	Weight int64
}

// MoveOutFee returns the fee and weight of moving funds out of the channel
// to destScript at feeRate.
func MoveOutFee(shape Shape, destScript []byte,
	feeRate chainfee.SatPerKWeight) (btcutil.Amount, int64) {

	w := shape.weight(nil, [][]byte{destScript})

	return fee(feeRate, w), w
}

// NewMoveOut returns the splice-out of amount to destScript at feeRate; the
// fee comes from the channel on top of the amount.
func NewMoveOut(shape Shape, amount btcutil.Amount, destScript []byte,
	feeRate chainfee.SatPerKWeight,
	dustLimit func([]byte) btcutil.Amount) (*MoveOut, error) {

	if amount < dustLimit(destScript) {
		return nil, ErrBelowDust
	}
	f, w := MoveOutFee(shape, destScript, feeRate)

	return &MoveOut{
		Amount: amount, Contribution: -(amount + f), Fee: f, Weight: w,
	}, nil
}
