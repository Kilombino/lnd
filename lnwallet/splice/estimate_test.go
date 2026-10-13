package splice

import (
	"testing"

	"github.com/lightningnetwork/lnd/lnwallet/chainfee"
	"github.com/stretchr/testify/require"
)

func TestEstimateSplice(t *testing.T) {
	t.Parallel()

	est := &testEstimator{rate: 2_000, relay: 253, fallback: 12_500}
	base := EstimateRequest{
		Capacity: 1_000_000, LocalBalance: 600_000, Available: 500_000,
		Coins: []Coin{
			coin(1, p2wpkh(1), 300_000), coin(2, p2tr(2), 200_000),
		},
		ChangeScript: p2wpkh(9),
		DustLimit:    dustLimit,
		Estimator:    est,
	}

	// In, an amount: fee and new balances.
	req := base
	req.Direction, req.Amount, req.FeeRate = DirectionIn, 250_000, 2_500
	e, err := EstimateSplice(req)
	require.NoError(t, err)
	require.Empty(t, e.RefusalCode)
	require.Equal(t, int64(250_000), int64(e.Amount))
	require.Positive(t, e.Fee)
	require.Equal(t, 1_250_000, int(e.NewCapacity))
	require.Equal(t, 850_000, int(e.NewLocal))
	require.Equal(t, FeeFloor{2_000, FeeSourceEstimate}, e.Floor)

	// Use maximum in: every coin after the fee.
	req.UseMax = true
	e, err = EstimateSplice(req)
	require.NoError(t, err)
	require.Equal(t, e.MaxIn, e.Amount)
	require.Equal(t, 500_000-int(e.Fee), int(e.Amount))

	// More than the coins.
	req = base
	req.Direction, req.Amount, req.FeeRate = DirectionIn, 600_000, 2_500
	e, err = EstimateSplice(req)
	require.NoError(t, err)
	require.Equal(t, codeInsufficientFunds, e.RefusalCode)

	// Out: amount plus fee from our balance; the maximum is what we can
	// send less the fee.
	req = base
	req.Direction, req.Amount, req.FeeRate = DirectionOut, 100_000, 2_500
	req.Destination = p2tr(3)
	e, err = EstimateSplice(req)
	require.NoError(t, err)
	require.Empty(t, e.RefusalCode)
	require.Equal(t, 900_000-int(e.Fee), int(e.NewCapacity))
	require.Equal(t, 500_000-int(e.Fee), int(e.MaxOut))

	req.Amount = e.MaxOut + 1
	e, err = EstimateSplice(req)
	require.NoError(t, err)
	require.Equal(t, "above_max_out", e.RefusalCode)

	req.UseMax = true
	e, err = EstimateSplice(req)
	require.NoError(t, err)
	require.Empty(t, e.RefusalCode)
	require.Equal(t, e.MaxOut, e.Amount)

	// A channel grown since it opened: the most out keeps 1% of the new
	// capacity, as the construction does, beyond what we can send.
	grown := base
	grown.Capacity, grown.LocalBalance = 10_000_000, 120_000
	grown.Available = 110_000
	grown.Direction, grown.FeeRate, grown.UseMax = DirectionOut, 2_500, true
	grown.Destination = p2tr(3)
	e, err = EstimateSplice(grown)
	require.NoError(t, err)
	require.Empty(t, e.RefusalCode)
	require.Less(t, int(e.MaxOut), 110_000-int(e.Fee))
	require.True(t, keepsSpliceReserve(
		grown.LocalBalance, grown.Capacity, -(e.MaxOut+e.Fee),
	))
	grown.UseMax, grown.Amount = false, e.MaxOut+100
	e, err = EstimateSplice(grown)
	require.NoError(t, err)
	require.Equal(t, "above_max_out", e.RefusalCode)

	// In, with change, too little to bring our balance to it.
	grown = base
	grown.Capacity, grown.LocalBalance = 10_000_000, 0
	grown.Direction, grown.FeeRate = DirectionIn, 2_500
	grown.Amount = 50_000
	e, err = EstimateSplice(grown)
	require.NoError(t, err)
	require.Equal(t, "below_min", e.RefusalCode)
	require.Equal(t, 101_010, int(e.Min))
	grown.Amount = e.Min
	e, err = EstimateSplice(grown)
	require.NoError(t, err)
	require.Empty(t, e.RefusalCode)
	grown.Amount = 150_000
	e, err = EstimateSplice(grown)
	require.NoError(t, err)
	require.Empty(t, e.RefusalCode)

	// Below dust, and below the floor.
	req.UseMax, req.Amount = false, 100
	e, err = EstimateSplice(req)
	require.NoError(t, err)
	require.Equal(t, "below_min", e.RefusalCode)

	req.Amount, req.FeeRate = 100_000, 1_999
	e, err = EstimateSplice(req)
	require.NoError(t, err)
	require.Equal(t, codeFeeBelowFloor, e.RefusalCode)
	req.ForceLowFee = true
	e, err = EstimateSplice(req)
	require.NoError(t, err)
	require.Empty(t, e.RefusalCode)

	// No rate: the floor.
	req.FeeRate, req.ForceLowFee = 0, false
	e, err = EstimateSplice(req)
	require.NoError(t, err)
	require.Equal(t, chainfee.SatPerKWeight(2_000), e.FeeRate)

	// A refusal known in advance wins.
	req.Refusal = "peer_offline"
	e, err = EstimateSplice(req)
	require.NoError(t, err)
	require.Equal(t, "peer_offline", e.RefusalCode)
}
