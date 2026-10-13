package splice

import (
	"errors"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/lightningnetwork/lnd/input"
	"github.com/lightningnetwork/lnd/lnwallet/chainfee"
)

// Estimate is what a splice this node would start would cost and allow,
// worked out without contacting the peer or leasing anything.
type Estimate struct {
	// Fee and Weight are the transaction's.
	Fee    btcutil.Amount
	Weight int64

	// FeeRate is the rate used.
	FeeRate chainfee.SatPerKWeight

	// Amount is the amount used: the one asked for, or the maximum.
	Amount btcutil.Amount

	// Min is the smallest amount accepted.
	Min btcutil.Amount

	// MaxIn is the most the wallet can add at this rate; MaxOut the most
	// that can be moved out.
	MaxIn, MaxOut btcutil.Amount

	// Floor is the lowest rate accepted without forcing it.
	Floor FeeFloor

	// NewCapacity and NewLocal are the channel's capacity and our
	// balance once it locks.
	NewCapacity, NewLocal btcutil.Amount

	// RefusalCode is why it can't be done, or "".
	RefusalCode string
}

// EstimateRequest is what an estimate needs.
type EstimateRequest struct {
	Request

	// Capacity is the channel's capacity; LocalBalance our balance.
	Capacity, LocalBalance btcutil.Amount

	// Available is what we can send now: our balance above the reserve
	// and the commitment fee.
	Available btcutil.Amount

	// Coins are the wallet's coins a splice can spend.
	Coins []Coin

	// ChangeScript stands for a change output.
	ChangeScript []byte

	// DustLimit is the dust limit of a script.
	DustLimit func([]byte) btcutil.Amount

	// Reserve is what the wallet keeps for fee-bumping the closes of
	// its anchor channels.
	Reserve btcutil.Amount

	// Estimator gives the floor.
	Estimator chainfee.Estimator

	// Refusal is a refusal already known (the channel's splice status),
	// or "".
	Refusal string
}

// EstimateSplice works out a splice's estimate. The new funding output's
// script isn't known before the peer's key arrives; any 2-of-2 P2WSH weighs
// the same, so a placeholder stands in.
func EstimateSplice(req EstimateRequest) (*Estimate, error) {
	if req.DustLimit == nil || req.Estimator == nil {
		return nil, errors.New("estimate without dust limit or " +
			"estimator")
	}

	placeholder := make([]byte, input.P2WSHSize)
	placeholder[0], placeholder[1] = 0x00, 0x20
	shape := Shape{
		SharedWitnessWeight: input.MultiSigWitnessSize,
		FundingScript:       placeholder,
	}

	est := &Estimate{
		FeeRate:     req.FeeRate,
		Floor:       InitiatorFloor(req.Estimator),
		RefusalCode: req.Refusal,
	}
	if est.FeeRate <= 0 {
		est.FeeRate = est.Floor.Rate
	}
	if est.FeeRate < est.Floor.Rate && !req.ForceLowFee &&
		est.RefusalCode == "" {

		est.RefusalCode = codeFeeBelowFloor
	}

	// The most in: everything the wallet can spend.
	if maxIn, err := SelectAddFunds(
		shape, req.Coins, 0, true, est.FeeRate, req.ChangeScript,
		req.DustLimit, req.Reserve,
	); err == nil {
		est.MaxIn = maxIn.Contribution
	}

	// The most out: what we can send, less the fee.
	dest := req.Destination
	if len(dest) == 0 {
		dest = req.ChangeScript
	}
	outFee, outWeight := MoveOutFee(shape, dest, est.FeeRate)
	if req.Available > outFee {
		est.MaxOut = req.Available - outFee
	}
	est.Min = req.DustLimit(dest)

	switch req.Direction {
	case DirectionIn:
		sel, err := SelectAddFunds(
			shape, req.Coins, req.Amount, req.UseMax, est.FeeRate,
			req.ChangeScript, req.DustLimit, req.Reserve,
		)
		switch {
		case errors.Is(err, ErrInsufficientFunds):
			setRefusal(est, codeInsufficientFunds)
			est.Amount = req.Amount
		case errors.Is(err, ErrBelowDust):
			setRefusal(est, "below_min")
			est.Amount = req.Amount
		case err != nil:
			return nil, err
		default:
			est.Amount = sel.Contribution
			est.Fee = sel.Fee
			est.Weight = sel.Weight
		}
		est.NewCapacity = req.Capacity + est.Amount
		est.NewLocal = req.LocalBalance + est.Amount

	case DirectionOut:
		est.Amount = req.Amount
		if req.UseMax {
			est.Amount = est.MaxOut
		}
		est.Fee, est.Weight = outFee, outWeight
		switch {
		case est.Amount < est.Min:
			setRefusal(est, "below_min")
		case est.Amount > est.MaxOut:
			setRefusal(est, "above_max_out")
		}
		est.NewCapacity = req.Capacity - est.Amount - est.Fee
		est.NewLocal = req.LocalBalance - est.Amount - est.Fee

	default:
		return nil, errors.New("no direction")
	}

	return est, nil
}

// setRefusal sets a refusal unless one is set.
func setRefusal(est *Estimate, code string) {
	if est.RefusalCode == "" {
		est.RefusalCode = code
	}
}
