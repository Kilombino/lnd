package htlcswitch

import (
	"crypto/sha256"
	"testing"
	"time"

	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/stretchr/testify/require"
)

// TestSwitchSpliceScids checks a short channel id a splice gave a channel
// finds its link, for a payment of ours and for a forward, which is recorded
// under the link's own id; that a forward back over the same channel by it
// counts as circular; and that the mapping goes with the link.
func TestSwitchSpliceScids(t *testing.T) {
	t.Parallel()

	alicePeer, err := newMockServer(
		t, "alice", testStartingHeight, nil, testDefaultDelta,
	)
	require.NoError(t, err)
	bobPeer, err := newMockServer(
		t, "bob", testStartingHeight, nil, testDefaultDelta,
	)
	require.NoError(t, err)

	s, err := initSwitchWithTempDB(t, testStartingHeight)
	require.NoError(t, err)
	require.NoError(t, s.Start())
	t.Cleanup(func() { _ = s.Stop() })

	chanID1, chanID2, aliceScid, bobScid := genIDs()
	aliceLink := newMockChannelLink(
		s, chanID1, aliceScid, emptyScid, alicePeer, true, false,
		false, false,
	)
	bobLink := newMockChannelLink(
		s, chanID2, bobScid, emptyScid, bobPeer, true, false, false,
		false,
	)
	require.NoError(t, s.AddLink(aliceLink))
	require.NoError(t, s.AddLink(bobLink))

	spliceScid := lnwire.NewShortChanIDFromInt(bobScid.ToUint64() + 1000)
	_, err = s.GetLinkByShortID(spliceScid)
	require.ErrorIs(t, err, ErrChannelLinkNotFound)

	require.NoError(t, s.AddSpliceScids(chanID2, spliceScid, bobScid))
	link, err := s.GetLinkByShortID(spliceScid)
	require.NoError(t, err)
	require.Equal(t, bobScid, link.ShortChanID())

	// A forward naming it reaches Bob's link, recorded under its own id.
	preimage, err := genPreimage()
	require.NoError(t, err)
	rhash := sha256.Sum256(preimage[:])
	packet := &htlcPacket{
		incomingChanID: aliceLink.ShortChanID(),
		incomingHTLCID: 0,
		outgoingChanID: spliceScid,
		obfuscator:     NewMockObfuscator(),
		htlc: &lnwire.UpdateAddHTLC{
			PaymentHash: rhash,
			Amount:      1,
		},
	}
	require.NoError(t, s.ForwardPackets(nil, packet))
	select {
	case pkt := <-bobLink.packets:
		require.Equal(t, bobScid, pkt.outgoingChanID)
		require.NoError(t, bobLink.completeCircuit(pkt))
	case <-time.After(time.Second):
		t.Fatal("forward by the splice's scid not delivered")
	}
	require.True(t, s.IsForwardedHTLC(bobScid, 0))

	// Back over the same channel by the other id: circular.
	linkErr := s.checkCircularForward(
		bobScid, spliceScid, false, rhash,
	)
	require.NotNil(t, linkErr)
	require.Nil(t, s.checkCircularForward(
		aliceScid, spliceScid, false, rhash,
	))

	// Gone with the link.
	s.RemoveLink(chanID2)
	_, err = s.GetLinkByShortID(spliceScid)
	require.ErrorIs(t, err, ErrChannelLinkNotFound)
	s.indexMtx.RLock()
	require.Empty(t, s.spliceIndex)
	s.indexMtx.RUnlock()
}
