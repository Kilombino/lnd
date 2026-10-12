package lnd

import (
	"testing"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/stretchr/testify/require"
)

// TestRPCPendingSplice checks how a pending splice is described: its state
// through the steps, deltas, new channel point and capacity, and the fee
// from its transaction.
func TestRPCPendingSplice(t *testing.T) {
	t.Parallel()

	tx := wire.NewMsgTx(2)
	tx.AddTxIn(&wire.TxIn{})
	tx.AddTxIn(&wire.TxIn{})
	tx.AddTxOut(&wire.TxOut{Value: 1_200_000})
	tx.AddTxOut(&wire.TxOut{Value: 49_000})

	rec := &channeldb.SpliceRecord{
		OutPoint:   wire.OutPoint{Hash: chainhash.Hash{7}, Index: 0},
		Capacity:   1_200_000,
		LocalDelta: 200_000,
	}

	ps := rpcPendingSplice(rec)
	require.Equal(t, "negotiating", ps.State)
	require.Equal(t, lnrpc.Initiator_INITIATOR_REMOTE, ps.Initiator)
	require.Equal(t, int64(200_000), ps.LocalDeltaSat)
	require.Equal(t, rec.OutPoint.String(), ps.NewChannelPoint)
	require.Equal(t, int64(1_200_000), ps.NewCapacitySat)

	rec.Negotiation = &channeldb.SpliceNegotiation{
		WeInitiated: true, Tx: tx, FeeRate: 2_500,
		Inputs: []channeldb.SpliceInput{
			{Value: 1_000_000, Shared: true}, {Value: 250_000},
		},
	}
	rec.SpliceSig = &lnwire.CommitSig{}
	ps = rpcPendingSplice(rec)
	require.Equal(t, "signed", ps.State)
	require.Equal(t, lnrpc.Initiator_INITIATOR_LOCAL, ps.Initiator)
	require.Equal(t, int64(1_000), ps.FeeSat)
	require.Equal(t, int64(1_000), ps.OurFeeSat)
	require.Equal(t, uint64(10), ps.SatPerVbyte)

	rec.Negotiation.OurTxSignatures = &lnwire.TxSignatures{}
	rec.Negotiation.TheirTxSignatures = &lnwire.TxSignatures{}
	rec.Negotiation.BroadcastHeight = 900_001
	ps = rpcPendingSplice(rec)
	require.Equal(t, "broadcast", ps.State)
	require.Equal(t, uint32(900_001), ps.BroadcastHeight)

	rec.Negotiation.LockedSent = true
	ps = rpcPendingSplice(rec)
	require.Equal(t, "confirmed", ps.State)
	require.True(t, ps.WeLocked)
	require.False(t, ps.PeerLocked)
}
