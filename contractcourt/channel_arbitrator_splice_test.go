package contractcourt

import (
	"testing"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/input"
	"github.com/lightningnetwork/lnd/lnwallet"
	"github.com/stretchr/testify/require"
)

// TestPrepareAnchorSweepsSplices checks the anchors of the commitments on a
// pending splice's funding output are swept too, with the same deadlines as
// the channel's: if the splice confirms, those are the commitments that can.
func TestPrepareAnchorSweepsSplices(t *testing.T) {
	t.Parallel()

	log := &mockArbitratorLog{
		state:     StateDefault,
		newStates: make(chan ArbitratorState, 5),
	}
	chanArbCtx, err := createTestChannelArbitrator(t, log)
	require.NoError(t, err)
	chanArb := chanArbCtx.chanArb
	chanArb.activeHTLCs = map[HtlcSetKey]htlcSet{
		LocalHtlcSet:  newHtlcSet(nil),
		RemoteHtlcSet: newHtlcSet(nil),
	}

	anchor := func(txid byte) *lnwallet.AnchorResolution {
		return &lnwallet.AnchorResolution{
			CommitAnchor: wire.OutPoint{Hash: chainhash.Hash{txid}},
			AnchorSignDescriptor: input.SignDescriptor{
				Output: &wire.TxOut{Value: 330},
			},
		}
	}
	anchors := &lnwallet.AnchorResolutions{
		Local:  anchor(1),
		Remote: anchor(2),
		Splices: []*lnwallet.AnchorResolutions{
			{Local: anchor(3), Remote: anchor(4)},
		},
	}

	reqs, err := chanArb.prepareAnchorSweeps(1000, anchors)
	require.NoError(t, err)

	var swept []chainhash.Hash
	for _, r := range reqs {
		swept = append(swept, r.input.OutPoint().Hash)
	}
	require.ElementsMatch(t, []chainhash.Hash{
		{1}, {2}, {3}, {4},
	}, swept)

	// The splice's are swept like the channel's.
	require.Equal(t, reqs[0].params.DeadlineHeight,
		reqs[2].params.DeadlineHeight)
}
