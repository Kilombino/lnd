package channeldb

import (
	"testing"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/fn/v2"
	"github.com/lightningnetwork/lnd/keychain"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/stretchr/testify/require"
)

// TestSpliceFundingRoundTrip checks the splice funding record: a channel
// never spliced reports its original funding outpoint, and one with the
// record keeps it through the database while FundingOutpoint still names the
// channel.
func TestSpliceFundingRoundTrip(t *testing.T) {
	t.Parallel()

	fullDB, err := MakeTestDB(t)
	require.NoError(t, err)
	cdb := fullDB.ChannelStateDB()

	plain := createTestChannel(t, cdb, openChannelOption())
	require.Equal(t, plain.FundingOutpoint, plain.CurrentFundingOutpoint())
	require.True(t, plain.SpliceFundingInfo().IsNone())

	spliced := SpliceFunding{
		OutPoint: wire.OutPoint{Hash: chainhash.Hash{0x51}, Index: 2},
		Scid: lnwire.ShortChannelID{
			BlockHeight: 961_700, TxIndex: 7, TxPosition: 2,
		},
	}
	channel := createTestChannel(
		t, cdb, openChannelOption(),
		func(p *testChannelParams) {
			p.channel.spliceFunding = fn.Some(spliced)
		},
	)

	fetched, err := cdb.FetchChannel(channel.FundingOutpoint)
	require.NoError(t, err)
	require.Equal(t, spliced.OutPoint, fetched.CurrentFundingOutpoint())
	require.Equal(t, fn.Some(spliced), fetched.SpliceFundingInfo())

	// The channel is still found, and named, by its original outpoint.
	require.Equal(t, channel.FundingOutpoint, fetched.FundingOutpoint)
	require.NotEqual(t, fetched.FundingOutpoint,
		fetched.CurrentFundingOutpoint())

	// A refresh keeps it.
	require.NoError(t, fetched.Refresh())
	require.Equal(t, spliced.OutPoint, fetched.CurrentFundingOutpoint())
}

// TestSpliceFundingEncoding checks the record's layout and its refusal of a
// wrong length.
func TestSpliceFundingEncoding(t *testing.T) {
	t.Parallel()

	f := SpliceFunding{
		OutPoint: wire.OutPoint{Hash: chainhash.Hash{1, 2, 3}, Index: 9},
		Scid:     lnwire.NewShortChanIDFromInt(0x0102030405060708),
	}
	b := encodeSpliceFunding(f)
	require.Len(t, b, spliceFundingSize)

	got, err := decodeSpliceFunding(b)
	require.NoError(t, err)
	require.Equal(t, f, got)

	_, err = decodeSpliceFunding(b[:spliceFundingSize-1])
	require.Error(t, err)
}

// testSpliceLock returns a splice lock onto a new funding output with fresh
// funding keys.
func testSpliceLock(t *testing.T, txid byte,
	capacity btcutil.Amount) SpliceLock {

	t.Helper()

	key := func() keychain.KeyDescriptor {
		priv, err := btcec.NewPrivateKey()
		require.NoError(t, err)

		return keychain.KeyDescriptor{
			KeyLocator: keychain.KeyLocator{
				Family: keychain.KeyFamilyMultiSig, Index: 7,
			},
			PubKey: priv.PubKey(),
		}
	}

	return SpliceLock{
		Funding: SpliceFunding{
			OutPoint: wire.OutPoint{
				Hash: chainhash.Hash{txid}, Index: 1,
			},
			Scid: lnwire.ShortChannelID{
				BlockHeight: 961_000 + uint32(txid), TxIndex: 3,
				TxPosition: 1,
			},
		},
		Capacity:          capacity,
		LocalMultiSigKey:  key(),
		RemoteMultiSigKey: key(),
	}
}

// TestLockSplice checks that a locked splice moves the channel's funding
// output, capacity and keys, in memory and on disk, and that the channel is
// then found by every funding output it has had while keeping its name.
func TestLockSplice(t *testing.T) {
	t.Parallel()

	fullDB, err := MakeTestDB(t)
	require.NoError(t, err)
	cdb := fullDB.ChannelStateDB()

	channel := createTestChannel(t, cdb, openChannelOption())
	name := channel.FundingOutpoint
	scid := channel.ShortChannelID

	first := testSpliceLock(t, 0x61, 3_000_000)
	require.NoError(t, channel.LockSplice(first))

	check := func(c *OpenChannel, l SpliceLock) {
		t.Helper()

		require.Equal(t, name, c.FundingOutpoint)
		require.Equal(t, scid, c.ShortChannelID)
		require.Equal(t, l.Funding.OutPoint, c.CurrentFundingOutpoint())
		require.Equal(t, fn.Some(l.Funding), c.SpliceFundingInfo())
		require.Equal(t, l.Capacity, c.Capacity)
		require.True(t, l.LocalMultiSigKey.PubKey.IsEqual(
			c.LocalChanCfg.MultiSigKey.PubKey,
		))
		require.Equal(t, l.LocalMultiSigKey.KeyLocator,
			c.LocalChanCfg.MultiSigKey.KeyLocator)
		require.True(t, l.RemoteMultiSigKey.PubKey.IsEqual(
			c.RemoteChanCfg.MultiSigKey.PubKey,
		))
	}
	check(channel, first)

	for _, point := range []wire.OutPoint{name, first.Funding.OutPoint} {
		fetched, err := cdb.FetchChannel(point)
		require.NoError(t, err)
		check(fetched, first)
	}

	// A second splice: every funding output the channel has had still
	// finds it.
	second := testSpliceLock(t, 0x62, 2_500_000)
	require.NoError(t, channel.LockSplice(second))
	for _, point := range []wire.OutPoint{
		name, first.Funding.OutPoint, second.Funding.OutPoint,
	} {
		fetched, err := cdb.FetchChannel(point)
		require.NoError(t, err)
		check(fetched, second)
	}

	// An unknown outpoint is still not found.
	_, err = cdb.FetchChannel(wire.OutPoint{Hash: chainhash.Hash{0x63}})
	require.ErrorIs(t, err, ErrChannelNotFound)

	// Once closed, the channel is found by none of them.
	require.NoError(t, channel.CloseChannel(&ChannelCloseSummary{
		ChanPoint: name, RemotePub: channel.IdentityPub,
	}))
	for _, point := range []wire.OutPoint{
		name, first.Funding.OutPoint, second.Funding.OutPoint,
	} {
		_, err := cdb.FetchChannel(point)
		require.ErrorIs(t, err, ErrChannelNotFound)
	}
}

// TestLockSpliceRefused checks that a splice can't move a channel onto an
// outpoint that names a channel or already funds another one, and that a
// refused lock changes nothing.
func TestLockSpliceRefused(t *testing.T) {
	t.Parallel()

	fullDB, err := MakeTestDB(t)
	require.NoError(t, err)
	cdb := fullDB.ChannelStateDB()

	a := createTestChannel(t, cdb, openChannelOption())
	b := createTestChannel(
		t, cdb, openChannelOption(), fundingPointOption(wire.OutPoint{
			Hash: chainhash.Hash{0x71}, Index: 0,
		}),
	)

	// Onto its own name, or another channel's.
	own := testSpliceLock(t, 0x72, 1_000_000)
	own.Funding.OutPoint = a.FundingOutpoint
	require.ErrorIs(t, a.LockSplice(own), ErrSpliceFundingInUse)

	other := testSpliceLock(t, 0x73, 1_000_000)
	other.Funding.OutPoint = b.FundingOutpoint
	require.ErrorIs(t, a.LockSplice(other), ErrSpliceFundingInUse)

	// Onto another channel's spliced funding.
	moved := testSpliceLock(t, 0x74, 1_000_000)
	require.NoError(t, b.LockSplice(moved))
	require.ErrorIs(t, a.LockSplice(moved), ErrSpliceFundingInUse)

	// Without the peer's key.
	noKey := testSpliceLock(t, 0x75, 1_000_000)
	noKey.RemoteMultiSigKey.PubKey = nil
	require.Error(t, a.LockSplice(noKey))

	// Nothing changed on a.
	fetched, err := cdb.FetchChannel(a.FundingOutpoint)
	require.NoError(t, err)
	require.True(t, fetched.SpliceFundingInfo().IsNone())
	require.Equal(t, a.Capacity, fetched.Capacity)
	require.True(t, a.SpliceFundingInfo().IsNone())

	// The same lock again is fine (a retry after a crash).
	require.NoError(t, b.LockSplice(moved))
}
