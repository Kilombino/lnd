package channeldb

import (
	"testing"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/keychain"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/stretchr/testify/require"
)

// spliceTestCommit returns a commitment at height whose transaction spends
// the given funding outpoint, made distinct by balance.
func spliceTestCommit(base ChannelCommitment, height uint64,
	funding wire.OutPoint, balance lnwire.MilliSatoshi) *ChannelCommitment {

	c := base
	c.CommitHeight = height
	c.LocalBalance = balance
	c.CommitTx = base.CommitTx.Copy()
	c.CommitTx.TxIn[0].PreviousOutPoint = funding
	c.CommitTx.TxOut[0].Value = int64(balance.ToSatoshis())
	c.Htlcs = nil

	return &c
}

func testSpliceRecord(t *testing.T, channel *OpenChannel) *SpliceRecord {
	t.Helper()

	local, err := btcec.NewPrivateKey()
	require.NoError(t, err)
	remote, err := btcec.NewPrivateKey()
	require.NoError(t, err)

	op := wire.OutPoint{Hash: chainhash.Hash{0x5b, 1}, Index: 0}

	return &SpliceRecord{
		OutPoint: op,
		Capacity: channel.Capacity + 250_000,
		LocalKey: keychain.KeyDescriptor{
			KeyLocator: keychain.KeyLocator{
				Family: keychain.KeyFamilyMultiSig, Index: 41,
			},
			PubKey: local.PubKey(),
		},
		RemoteKey:   remote.PubKey(),
		LocalDelta:  300_000,
		RemoteDelta: -50_000,
		RemoteTail: spliceTestCommit(
			channel.RemoteCommitment,
			channel.RemoteCommitment.CommitHeight, op, 7e8,
		),
		SpliceSig: &lnwire.CommitSig{
			ChanID:    lnwire.ChannelID{9},
			CommitSig: wireSig,
		},
	}
}

func requireSigEqual(t *testing.T, want, got *lnwire.CommitSig) {
	t.Helper()

	if want == nil {
		require.Nil(t, got)
		return
	}
	require.NotNil(t, got)
	require.Equal(t, want.ChanID, got.ChanID)
	require.Equal(t, want.CommitSig, got.CommitSig)
	require.Equal(t, len(want.HtlcSigs), len(got.HtlcSigs))
}

// TestSpliceRecordRoundTrip checks a record through the database: every
// field, negative deltas, absent optional parts, and its removal.
func TestSpliceRecordRoundTrip(t *testing.T) {
	t.Parallel()

	fullDB, err := MakeTestDB(t)
	require.NoError(t, err)
	channel := createTestChannel(t, fullDB.ChannelStateDB())

	r := testSpliceRecord(t, channel)
	require.NoError(t, channel.PutSpliceRecord(r))

	records, err := channel.SpliceRecords()
	require.NoError(t, err)
	require.Len(t, records, 1)
	got := records[0]

	require.Equal(t, r.OutPoint, got.OutPoint)
	require.Equal(t, r.Capacity, got.Capacity)
	require.Equal(t, r.LocalKey.KeyLocator, got.LocalKey.KeyLocator)
	require.True(t, r.LocalKey.PubKey.IsEqual(got.LocalKey.PubKey))
	require.True(t, r.RemoteKey.IsEqual(got.RemoteKey))
	require.Equal(t, r.LocalDelta, got.LocalDelta)
	require.Equal(t, r.RemoteDelta, got.RemoteDelta)
	require.Nil(t, got.LocalCommit)
	require.Nil(t, got.RemoteTip)
	require.Nil(t, got.RemoteTipSig)
	require.Equal(t, r.RemoteTail, got.RemoteTail)
	requireSigEqual(t, r.SpliceSig, got.SpliceSig)

	require.NoError(t, channel.DeleteSpliceRecord(r.Txid()))
	records, err = channel.SpliceRecords()
	require.NoError(t, err)
	require.Empty(t, records)
	require.ErrorIs(t, channel.DeleteSpliceRecord(r.Txid()),
		ErrSpliceRecordNotFound)

	// A record without keys is refused.
	r.RemoteKey = nil
	require.Error(t, channel.PutSpliceRecord(r))
}

// TestSpliceCommitWrites checks the writes a pending splice makes with each
// commitment chain step: in the same transaction as the step, and the
// revocation of the peer's commitment on the splice's funding into its own
// log.
func TestSpliceCommitWrites(t *testing.T) {
	t.Parallel()

	fullDB, err := MakeTestDB(t)
	require.NoError(t, err)
	channel := createTestChannel(t, fullDB.ChannelStateDB())

	r := testSpliceRecord(t, channel)
	require.NoError(t, channel.PutSpliceRecord(r))
	txid := r.Txid()

	fetch := func() *SpliceRecord {
		t.Helper()

		records, err := channel.SpliceRecords()
		require.NoError(t, err)
		require.Len(t, records, 1)

		return records[0]
	}

	// Our new commitment, with the one on the splice's funding.
	height := channel.LocalCommitment.CommitHeight + 1
	local := channel.LocalCommitment
	local.CommitHeight = height
	splLocal := spliceTestCommit(local, height, r.OutPoint, 6e8)
	_, err = channel.UpdateCommitment(&local, nil, SpliceCommitWrite{
		Txid: txid, LocalCommit: splLocal,
	})
	require.NoError(t, err)
	assertCommitmentEqual(t, splLocal, fetch().LocalCommit)

	// The peer's next commitment and our signatures for it.
	tailHeight := channel.RemoteCommitment.CommitHeight
	remote := channel.RemoteCommitment
	remote.CommitHeight = tailHeight + 1
	diff := &CommitDiff{
		Commitment: remote,
		CommitSig: &lnwire.CommitSig{
			ChanID: lnwire.ChannelID(key), CommitSig: wireSig,
		},
	}
	splTip := spliceTestCommit(remote, tailHeight+1, r.OutPoint, 5e8)
	tipSig := &lnwire.CommitSig{
		ChanID: lnwire.ChannelID(key), CommitSig: wireSig,
		HtlcSigs: []lnwire.Sig{wireSig},
	}

	// A write naming an unknown splice fails the whole step.
	err = channel.AppendRemoteCommitChain(diff, SpliceCommitWrite{
		Txid: chainhash.Hash{0xee}, RemoteTip: splTip,
		RemoteTipSig: tipSig,
	})
	require.ErrorIs(t, err, ErrSpliceRecordNotFound)
	_, err = channel.RemoteCommitChainTip()
	require.ErrorIs(t, err, ErrNoPendingCommit)

	require.NoError(t, channel.AppendRemoteCommitChain(
		diff, SpliceCommitWrite{
			Txid: txid, RemoteTip: splTip, RemoteTipSig: tipSig,
		},
	))
	got := fetch()
	assertCommitmentEqual(t, splTip, got.RemoteTip)
	requireSigEqual(t, tipSig, got.RemoteTipSig)

	// The peer revokes: its commitment on the splice's funding at the
	// old height goes into the splice's revocation log with its own
	// output indexes, and the tip becomes the tail.
	oldTail := got.RemoteTail
	channel.RemoteCurrentRevocation = channel.RemoteNextRevocation
	next, err := btcec.NewPrivateKey()
	require.NoError(t, err)
	channel.RemoteNextRevocation = next.PubKey()
	fwdPkg := NewFwdPkg(channel.ShortChanID(), tailHeight, nil, nil)
	require.NoError(t, channel.AdvanceCommitChainTail(
		fwdPkg, nil, 0, 1, SpliceCommitWrite{
			Txid: txid, OurOutputIndex: 3, TheirOutputIndex: 2,
		},
	))

	rl, err := channel.FetchSpliceRevocationLog(txid, tailHeight)
	require.NoError(t, err)
	require.Equal(t, [32]byte(oldTail.CommitTx.TxHash()),
		rl.CommitTxHash.Val)
	require.EqualValues(t, 3, rl.OurOutputIndex.Val)
	require.EqualValues(t, 2, rl.TheirOutputIndex.Val)

	// The channel's own log has its own entry at that height.
	own, _, err := channel.FindPreviousState(tailHeight)
	require.NoError(t, err)
	require.NotEqual(t, own.CommitTxHash.Val, rl.CommitTxHash.Val)
	require.EqualValues(t, 0, own.OurOutputIndex.Val)

	got = fetch()
	assertCommitmentEqual(t, splTip, got.RemoteTail)
	require.Nil(t, got.RemoteTip)
	require.Nil(t, got.RemoteTipSig)

	// Nothing more to revoke on the splice: a second revocation write
	// without a tip fails.
	remote.CommitHeight = tailHeight + 2
	diff.Commitment = remote
	require.NoError(t, channel.AppendRemoteCommitChain(diff))
	err = channel.AdvanceCommitChainTail(
		NewFwdPkg(channel.ShortChanID(), tailHeight+1, nil, nil), nil,
		0, 1, SpliceCommitWrite{Txid: txid},
	)
	require.Error(t, err)

	_, err = channel.FetchSpliceRevocationLog(txid, tailHeight+1)
	require.ErrorIs(t, err, ErrLogEntryNotFound)
	_, err = channel.FetchSpliceRevocationLog(
		chainhash.Hash{0xee}, tailHeight,
	)
	require.ErrorIs(t, err, ErrLogEntryNotFound)
}
