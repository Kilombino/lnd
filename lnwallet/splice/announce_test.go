package splice

import (
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/fn/v2"
	"github.com/lightningnetwork/lnd/keychain"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/stretchr/testify/require"
)

// TestSpliceAnnouncementRetransmit checks channel_reestablish asks for the
// peer's half of the announcement on the funding a splice moved the channel
// to while it lacks it, and that the peer asking for ours gets it sent; on
// the funding the channel was opened with, neither.
func TestSpliceAnnouncementRetransmit(t *testing.T) {
	t.Parallel()

	alice, _ := soloAlice(t)
	m := alice.machine

	var pending bool
	resent := make(chan struct{}, 4)
	m.mu.Lock()
	m.cfg.AnnouncementPending = func() bool { return pending }
	m.cfg.ResendAnnouncement = func() { resent <- struct{}{} }
	m.mu.Unlock()

	state := alice.ch.State()
	ask := func(txid chainhash.Hash) {
		m.HandleReestablish(&lnwire.ChannelReestablish{
			MyCurrentFundingLocked: someFlags(
				txid, lnwire.RetransmitAnnounceSigs,
			),
		})
	}
	requireResent := func(want bool) {
		t.Helper()

		select {
		case <-resent:
			require.True(t, want, "resent")
		case <-time.After(200 * time.Millisecond):
			require.False(t, want, "not resent")
		}
	}
	flags := func() uint8 {
		_, locked := m.ReestablishTLVs()
		return locked.UnsafeFromSome().RetransmitFlags
	}

	// Not spliced: the announcement is the funding manager's.
	pending = true
	require.Zero(t, flags())
	ask(state.FundingOutpoint.Hash)
	requireResent(false)

	priv, err := btcec.NewPrivateKey()
	require.NoError(t, err)
	key := keychain.KeyDescriptor{PubKey: priv.PubKey()}
	moved := wire.OutPoint{Hash: chainhash.Hash{0x91}}
	require.NoError(t, state.LockSplice(channeldb.SpliceLock{
		Funding: channeldb.SpliceFunding{
			OutPoint: moved,
			Scid:     lnwire.ShortChannelID{BlockHeight: 900_000},
		},
		Capacity:          state.Capacity,
		LocalMultiSigKey:  key,
		RemoteMultiSigKey: key,
	}))

	require.Equal(t, lnwire.RetransmitAnnounceSigs, flags())
	pending = false
	require.Zero(t, flags())

	ask(moved.Hash)
	requireResent(true)
	ask(chainhash.Hash{0x92})
	requireResent(false)
}

// someFlags returns funding flags for txid.
func someFlags(txid chainhash.Hash,
	flags uint8) fn.Option[lnwire.FundingTxidFlags] {

	return fn.Some(lnwire.FundingTxidFlags{
		Txid: txid, RetransmitFlags: flags,
	})
}
