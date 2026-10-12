package peer

import (
	"bytes"
	"testing"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/contractcourt"
	"github.com/lightningnetwork/lnd/fn/v2"
	"github.com/lightningnetwork/lnd/htlcswitch"
	"github.com/lightningnetwork/lnd/keychain"
	"github.com/lightningnetwork/lnd/lnwallet"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/stretchr/testify/require"
)

// TestPeerNoCloseDuringSplice checks that a channel with a splice that
// hasn't locked can't be closed cooperatively: our close request is refused,
// and the peer's shutdown gets a warning and ends the connection.
func TestPeerNoCloseDuringSplice(t *testing.T) {
	t.Parallel()

	harness, err := createTestPeerWithChannel(t, noUpdate)
	require.NoError(t, err)
	peer := harness.peer
	chanPoint := harness.channel.ChannelPoint()
	chanID := lnwire.NewChanIDFromOutPoint(chanPoint)

	ch, ok := peer.activeChannels.Load(chanID)
	require.True(t, ok)
	local, err := btcec.NewPrivateKey()
	require.NoError(t, err)
	remote, err := btcec.NewPrivateKey()
	require.NoError(t, err)
	require.NoError(t, ch.AddPendingFunding(lnwallet.PendingFunding{
		OutPoint:   wire.OutPoint{Hash: chainhash.Hash{1}},
		Capacity:   ch.State().Capacity + 1_000,
		LocalKey:   keychain.KeyDescriptor{PubKey: local.PubKey()},
		RemoteKey:  remote.PubKey(),
		LocalDelta: 1_000,
	}))
	require.True(t, peer.spliceUnlocked(chanID))

	// Ours.
	req := &htlcswitch.ChanClose{
		CloseType: contractcourt.CloseRegular,
		ChanPoint: &chanPoint,
		Updates:   make(chan interface{}, 1),
		Err:       make(chan error, 1),
	}
	peer.handleLocalCloseReq(req)
	require.ErrorIs(t, <-req.Err, errSpliceUnlocked)

	// The peer's.
	peer.remoteFeatures = lnwire.EmptyFeatureVector()
	peer.outgoingQueue = make(chan outgoingMsg, 4)
	var shutdown bytes.Buffer
	_, err = lnwire.WriteMessage(&shutdown, lnwire.NewShutdown(
		chanID, genScript(t, p2wshAddress),
	), 0)
	require.NoError(t, err)

	peer.cg.WgAdd(1)
	go peer.readHandler()
	harness.mockConn.readMessages <- shutdown.Bytes()

	queued, err := fn.RecvOrTimeout(peer.outgoingQueue, timeout)
	require.NoError(t, err)
	warning, ok := queued.msg.(*lnwire.Warning)
	require.True(t, ok, "%T", queued.msg)
	require.Equal(t, chanID, warning.ChanID)
}
