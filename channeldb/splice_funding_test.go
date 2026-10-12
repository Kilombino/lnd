package channeldb

import (
	"testing"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/fn/v2"
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
