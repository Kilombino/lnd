package splice

import (
	"errors"
	"fmt"

	"github.com/lightningnetwork/lnd/lnwallet/chainfee"
)

// FloorConfTarget is the confirmation target of the fee floors.
const FloorConfTarget = 24

// FeeSource says what a fee floor is based on.
type FeeSource string

const (
	// FeeSourceEstimate is the backend's estimate.
	FeeSourceEstimate FeeSource = "estimate"

	// FeeSourceRelayMinimum is the minimum relay fee, used when the
	// backend has no estimate. Without it the backend's fixed fallback
	// rate, far above what this chain's blocks need, would be the
	// floor.
	FeeSourceRelayMinimum FeeSource = "relay_minimum"
)

var (
	// ErrFeeBelowFloor is returned for a feerate under the floor.
	ErrFeeBelowFloor = errors.New("fee_below_floor")
)

// FeeFloor is the lowest feerate a splice may use.
type FeeFloor struct {
	// Rate is the floor.
	Rate chainfee.SatPerKWeight

	// Source is what it is based on.
	Source FeeSource
}

// estimate returns the backend's estimate at FloorConfTarget, or false when
// it has none: an error, or the estimator's fixed fallback rate.
func estimate(est chainfee.Estimator) (chainfee.SatPerKWeight, bool) {
	rate, err := est.EstimateFeePerKW(FloorConfTarget)
	if err != nil || rate <= 0 {
		return 0, false
	}
	if f, ok := est.(chainfee.FallbackReporter); ok &&
		rate == f.FallbackFeePerKW() {

		return 0, false
	}

	return rate, true
}

// InitiatorFloor returns the floor for a splice this node starts: the
// backend's estimate at FloorConfTarget, or the minimum relay fee when there
// is no estimate.
func InitiatorFloor(est chainfee.Estimator) FeeFloor {
	relay := est.RelayFeePerKW()
	rate, ok := estimate(est)
	if !ok || rate < relay {
		return FeeFloor{Rate: relay, Source: FeeSourceRelayMinimum}
	}

	return FeeFloor{Rate: rate, Source: FeeSourceEstimate}
}

// AcceptorFloor returns the floor for a splice the peer starts: the larger
// of the minimum relay fee and half this node's estimate. Estimates differ
// between nodes, and a floor equal to ours would refuse a peer just below it
// every time.
func AcceptorFloor(est chainfee.Estimator) chainfee.SatPerKWeight {
	relay := est.RelayFeePerKW()
	rate, ok := estimate(est)
	if !ok || rate/2 < relay {
		return relay
	}

	return rate / 2
}

// CheckInitiatorFeeRate refuses a feerate below the initiator floor unless
// the user forced it. The minimum relay fee is never waived: the splice
// wouldn't propagate.
func CheckInitiatorFeeRate(rate chainfee.SatPerKWeight,
	est chainfee.Estimator, force bool) error {

	relay := est.RelayFeePerKW()
	if rate < relay {
		return fmt.Errorf("%w: %v is below the minimum relay fee %v",
			ErrFeeBelowFloor, rate, relay)
	}
	if force {
		return nil
	}

	floor := InitiatorFloor(est)
	if rate < floor.Rate {
		return fmt.Errorf("%w: %v is below %v (%v)", ErrFeeBelowFloor,
			rate, floor.Rate, floor.Source)
	}

	return nil
}

// CheckAcceptorFeeRate refuses a peer's feerate below the acceptor floor.
func CheckAcceptorFeeRate(rate chainfee.SatPerKWeight,
	est chainfee.Estimator) error {

	floor := AcceptorFloor(est)
	if rate < floor {
		return fmt.Errorf("%w: %v is below %v", ErrFeeBelowFloor, rate,
			floor)
	}

	return nil
}
