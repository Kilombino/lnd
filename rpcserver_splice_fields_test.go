package lnd

import (
	"testing"

	"github.com/btcsuite/btcd/btcutil"
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

// TestSetSpliceDepth checks a pending splice's confirmations and lock depth,
// and that its first confirmation makes it "confirmed".
func TestSetSpliceDepth(t *testing.T) {
	t.Parallel()

	ps := &lnrpc.PendingSplice{State: "broadcast"}
	setSpliceDepth(ps, 0, 6)
	require.Equal(t, "broadcast", ps.State)
	require.Equal(t, uint32(6), ps.LockDepth)

	setSpliceDepth(ps, 2, 6)
	require.Equal(t, "confirmed", ps.State)
	require.Equal(t, uint32(2), ps.Confirmations)

	// Still signing: confirmations can't be known yet.
	ps = &lnrpc.PendingSplice{State: "signed"}
	setSpliceDepth(ps, 0, 6)
	require.Equal(t, "signed", ps.State)
}

// TestSpliceLockDepth checks the lock depth: 6 at least, and the channel's
// own when larger. lnd's scale for opens tops out at 6, so a peer's
// contribution doesn't raise it further.
func TestSpliceLockDepth(t *testing.T) {
	t.Parallel()

	require.Equal(t, uint32(6), spliceLockDepth(3, 0))
	require.Equal(t, uint32(8), spliceLockDepth(8, 0))
	require.Equal(t, uint32(6), spliceLockDepth(3, -5))
	require.Equal(t, uint32(6), spliceLockDepth(3, 1_000_000_000))
}

// TestSpliceBalances checks what a splice adds to the channel balance's
// pending fields: nothing before its transaction went out, our
// contribution after, and what it pays our wallet only while unconfirmed.
func TestSpliceBalances(t *testing.T) {
	t.Parallel()

	tx := wire.NewMsgTx(2)
	tx.AddTxOut(&wire.TxOut{Value: 1_000_000})
	tx.AddTxOut(&wire.TxOut{Value: 300_000})
	ours := func(_ *wire.MsgTx, idx uint32) btcutil.Amount {
		require.Equal(t, uint32(0), idx)
		return 300_000
	}
	confs := uint32(0)
	confirmations := func() uint32 { return confs }

	in := &channeldb.SpliceRecord{
		LocalDelta:  200_000,
		Negotiation: &channeldb.SpliceNegotiation{Tx: tx},
	}
	a, b := spliceBalances(in, confirmations, ours)
	require.Zero(t, a, "counted while the wallet still holds the coins")
	require.Zero(t, b)

	in.Negotiation.OurTxSignatures = &lnwire.TxSignatures{}
	in.Negotiation.TheirTxSignatures = &lnwire.TxSignatures{}
	a, _ = spliceBalances(in, confirmations, ours)
	require.EqualValues(t, 200_000, a)

	out := &channeldb.SpliceRecord{
		LocalDelta: -300_200,
		Negotiation: &channeldb.SpliceNegotiation{
			Tx:              tx,
			OurTxSignatures: &lnwire.TxSignatures{},
		},
	}
	_, b = spliceBalances(out, confirmations, ours)
	require.Zero(t, b, "not out yet")

	out.Negotiation.TheirTxSignatures = &lnwire.TxSignatures{}
	a, b = spliceBalances(out, confirmations, ours)
	require.Zero(t, a)
	require.EqualValues(t, 300_000, b)

	confs = 1
	_, b = spliceBalances(out, confirmations, ours)
	require.Zero(t, b, "confirmed: the wallet counts it as confirmed")

	out.Retired = true
	confs = 0
	_, b = spliceBalances(out, confirmations, ours)
	require.Zero(t, b)
}
