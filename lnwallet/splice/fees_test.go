package splice

import (
	"errors"
	"testing"

	"github.com/lightningnetwork/lnd/lnwallet/chainfee"
	"github.com/stretchr/testify/require"
)

// testEstimator answers with a fixed estimate (or an error) and relay fee,
// and reports a fallback rate.
type testEstimator struct {
	rate     chainfee.SatPerKWeight
	err      error
	relay    chainfee.SatPerKWeight
	fallback chainfee.SatPerKWeight
}

func (e *testEstimator) EstimateFeePerKW(uint32) (chainfee.SatPerKWeight,
	error) {

	return e.rate, e.err
}

func (e *testEstimator) RelayFeePerKW() chainfee.SatPerKWeight {
	return e.relay
}

func (e *testEstimator) FallbackFeePerKW() chainfee.SatPerKWeight {
	return e.fallback
}

func (e *testEstimator) Start() error { return nil }
func (e *testEstimator) Stop() error  { return nil }

func TestFeeFloors(t *testing.T) {
	t.Parallel()

	const relay, fallback = 253, 12_500

	tests := []struct {
		name      string
		est       *testEstimator
		initiator FeeFloor
		acceptor  chainfee.SatPerKWeight
	}{{
		name:      "estimate",
		est:       &testEstimator{rate: 2_000},
		initiator: FeeFloor{2_000, FeeSourceEstimate},
		acceptor:  1_000,
	}, {
		// Half the estimate is under the relay fee.
		name:      "low estimate",
		est:       &testEstimator{rate: 400},
		initiator: FeeFloor{400, FeeSourceEstimate},
		acceptor:  relay,
	}, {
		name:      "below relay",
		est:       &testEstimator{rate: 200},
		initiator: FeeFloor{relay, FeeSourceRelayMinimum},
		acceptor:  relay,
	}, {
		// The backend's fallback, not an estimate.
		name:      "fallback",
		est:       &testEstimator{rate: fallback},
		initiator: FeeFloor{relay, FeeSourceRelayMinimum},
		acceptor:  relay,
	}, {
		name:      "error",
		est:       &testEstimator{err: errors.New("no data")},
		initiator: FeeFloor{relay, FeeSourceRelayMinimum},
		acceptor:  relay,
	}}

	for _, tc := range tests {
		tc.est.relay, tc.est.fallback = relay, fallback
		require.Equal(t, tc.initiator, InitiatorFloor(tc.est), tc.name)
		require.Equal(t, tc.acceptor, AcceptorFloor(tc.est), tc.name)
	}

	// A static estimator's every answer is a fallback.
	static := chainfee.NewStaticEstimator(fallback, relay)
	require.Equal(t, FeeFloor{relay, FeeSourceRelayMinimum},
		InitiatorFloor(static))
}

func TestCheckFeeRates(t *testing.T) {
	t.Parallel()

	est := &testEstimator{rate: 2_000, relay: 253, fallback: 12_500}

	require.NoError(t, CheckInitiatorFeeRate(2_000, est, false))
	require.ErrorIs(t, CheckInitiatorFeeRate(1_999, est, false),
		ErrFeeBelowFloor)

	// Forced: anything from the relay fee up, never below it.
	require.NoError(t, CheckInitiatorFeeRate(253, est, true))
	require.ErrorIs(t, CheckInitiatorFeeRate(252, est, true),
		ErrFeeBelowFloor)

	require.NoError(t, CheckAcceptorFeeRate(1_000, est))
	require.ErrorIs(t, CheckAcceptorFeeRate(999, est), ErrFeeBelowFloor)
}
