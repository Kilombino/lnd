package funding

import (
	"errors"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/chainntnfs"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/fn/v2"
	"github.com/lightningnetwork/lnd/graph/db/models"
	"github.com/lightningnetwork/lnd/keychain"
	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/stretchr/testify/require"
)

// TestAnnounceSplice checks a public channel a splice moved is announced on
// its new funding output: the announcement names the splice's scid and
// funding keys, our update carries the policy given, inbound fee included,
// and our half of the proof names the splice's scid and the channel's id.
func TestAnnounceSplice(t *testing.T) {
	t.Parallel()

	alice, bob := setupFundingManagers(t)
	t.Cleanup(func() { tearDownFundingManagers(t, alice, bob) })

	updateChan := make(chan *lnrpc.OpenStatusUpdate)
	fundingOutPoint, fundingTx := openChannel(
		t, alice, bob, 500000, 0, 1, updateChan, true, nil,
	)
	chanID := lnwire.NewChanIDFromOutPoint(*fundingOutPoint)
	sendAndCheckFirstConfirmation(t, alice, chanID, fundingTx)
	sendAndCheckFirstConfirmation(t, bob, chanID, fundingTx)
	assertMarkedOpen(t, alice, bob, fundingOutPoint)
	readyAlice, ok := assertFundingMsgSent(
		t, alice.msgChan, "ChannelReady",
	).(*lnwire.ChannelReady)
	require.True(t, ok)
	readyBob, ok := assertFundingMsgSent(
		t, bob.msgChan, "ChannelReady",
	).(*lnwire.ChannelReady)
	require.True(t, ok)
	assertChannelReadySent(t, alice, bob, fundingOutPoint)
	alice.fundingMgr.ProcessFundingMsg(readyBob, bob)
	bob.fundingMgr.ProcessFundingMsg(readyAlice, alice)
	assertHandleChannelReady(t, alice, bob)
	assertChannelAnnouncements(t, alice, bob, 500000, nil, nil, nil, nil)
	assertAddedToGraph(t, alice, bob, fundingOutPoint)
	waitForOpenUpdate(t, updateChan)
	alice.mockNotifier.sixConfChannel <- &chainntnfs.TxConfirmation{
		Tx: fundingTx,
	}
	for i := 0; i < 2; i++ {
		select {
		case <-alice.announceChan:
		case <-time.After(5 * time.Second):
			t.Fatal("alice did not announce the channel")
		}
	}

	channels, err := alice.fundingMgr.cfg.ChannelDB.FetchAllOpenChannels()
	require.NoError(t, err)
	require.Len(t, channels, 1)
	ch := channels[0]

	// Not moved: nothing to announce.
	require.ErrorContains(t, alice.fundingMgr.AnnounceSplice(ch, nil, true),
		"not moved")

	key := func() keychain.KeyDescriptor {
		priv, err := btcec.NewPrivateKey()
		require.NoError(t, err)
		return keychain.KeyDescriptor{PubKey: priv.PubKey()}
	}
	scid := lnwire.ShortChannelID{BlockHeight: 900_000, TxIndex: 4}
	local, remote := key(), key()
	require.NoError(t, ch.LockSplice(channeldb.SpliceLock{
		Funding: channeldb.SpliceFunding{
			OutPoint: wire.OutPoint{Hash: chainhash.Hash{0x31}},
			Scid:     scid,
		},
		Capacity:          600_000,
		LocalMultiSigKey:  local,
		RemoteMultiSigKey: remote,
	}))

	inbound := lnwire.Fee{BaseFee: -100, FeeRate: -20}
	policy := &models.ChannelEdgePolicy{
		TimeLockDelta:             90,
		MinHTLC:                   1_000,
		MaxHTLC:                   500_000_000,
		FeeBaseMSat:               2_000,
		FeeProportionalMillionths: 300,
		MessageFlags:              lnwire.ChanUpdateRequiredMaxHtlc,
		InboundFee:                fn.Some(inbound),
	}

	errChan := make(chan error, 1)
	go func() {
		errChan <- alice.fundingMgr.AnnounceSplice(ch, policy, true)
	}()

	var (
		ann   *lnwire.ChannelAnnouncement1
		upd   *lnwire.ChannelUpdate1
		proof *lnwire.AnnounceSignatures1
	)
	for ann == nil || upd == nil || proof == nil {
		select {
		case msg := <-alice.announceChan:
			switch m := msg.(type) {
			case *lnwire.ChannelAnnouncement1:
				ann = m
			case *lnwire.ChannelUpdate1:
				upd = m
			case *lnwire.AnnounceSignatures1:
				proof = m
			}
		case <-time.After(5 * time.Second):
			t.Fatal("splice not announced")
		}
	}
	// The node announcement follows.
	for done := false; !done; {
		select {
		case <-alice.announceChan:
		case err := <-errChan:
			require.NoError(t, err)
			done = true
		case <-time.After(5 * time.Second):
			t.Fatal("announcing did not finish")
		}
	}

	require.Equal(t, scid, ann.ShortChannelID)
	keys := [][33]byte{ann.BitcoinKey1, ann.BitcoinKey2}
	var localKey, remoteKey [33]byte
	copy(localKey[:], local.PubKey.SerializeCompressed())
	copy(remoteKey[:], remote.PubKey.SerializeCompressed())
	require.ElementsMatch(t, [][33]byte{localKey, remoteKey}, keys)

	require.Equal(t, scid, upd.ShortChannelID)
	require.EqualValues(t, 2_000, upd.BaseFee)
	require.EqualValues(t, 300, upd.FeeRate)
	got, err := upd.InboundFee.UnwrapOrErrV(errNoInbound)
	require.NoError(t, err)
	require.Equal(t, inbound, got)

	require.Equal(t, scid, proof.ShortChannelID)
	require.Equal(t, chanID, proof.ChannelID)
}

var errNoInbound = errorString("no inbound fee")

type errorString string

func (e errorString) Error() string { return string(e) }

// TestAnnounceSplicedBeforeAnnouncement checks a public channel a splice
// moved before it was announced isn't announced on the output it left when
// the announcement resumes after a restart: it is the splice's to announce.
func TestAnnounceSplicedBeforeAnnouncement(t *testing.T) {
	t.Parallel()

	alice, bob := setupFundingManagers(t)
	t.Cleanup(func() { tearDownFundingManagers(t, alice, bob) })

	updateChan := make(chan *lnrpc.OpenStatusUpdate)
	fundingOutPoint, fundingTx := openChannel(
		t, alice, bob, 500000, 0, 1, updateChan, true, nil,
	)
	chanID := lnwire.NewChanIDFromOutPoint(*fundingOutPoint)
	sendAndCheckFirstConfirmation(t, alice, chanID, fundingTx)
	sendAndCheckFirstConfirmation(t, bob, chanID, fundingTx)
	assertMarkedOpen(t, alice, bob, fundingOutPoint)
	readyAlice, ok := assertFundingMsgSent(
		t, alice.msgChan, "ChannelReady",
	).(*lnwire.ChannelReady)
	require.True(t, ok)
	readyBob, ok := assertFundingMsgSent(
		t, bob.msgChan, "ChannelReady",
	).(*lnwire.ChannelReady)
	require.True(t, ok)
	assertChannelReadySent(t, alice, bob, fundingOutPoint)
	alice.fundingMgr.ProcessFundingMsg(readyBob, bob)
	bob.fundingMgr.ProcessFundingMsg(readyAlice, alice)
	assertHandleChannelReady(t, alice, bob)
	assertChannelAnnouncements(t, alice, bob, 500000, nil, nil, nil, nil)
	assertAddedToGraph(t, alice, bob, fundingOutPoint)
	waitForOpenUpdate(t, updateChan)

	// The splice locks before six confirmations.
	channels, err := alice.fundingMgr.cfg.ChannelDB.FetchAllOpenChannels()
	require.NoError(t, err)
	require.Len(t, channels, 1)
	key := func() keychain.KeyDescriptor {
		priv, err := btcec.NewPrivateKey()
		require.NoError(t, err)
		return keychain.KeyDescriptor{PubKey: priv.PubKey()}
	}
	require.NoError(t, channels[0].LockSplice(channeldb.SpliceLock{
		Funding: channeldb.SpliceFunding{
			OutPoint: wire.OutPoint{Hash: chainhash.Hash{0x32}},
			Scid:     lnwire.ShortChannelID{BlockHeight: 900_000},
		},
		Capacity:          600_000,
		LocalMultiSigKey:  key(),
		RemoteMultiSigKey: key(),
	}))

	recreateAliceFundingManager(t, alice)

	// Nothing announced on the original output, and the opening is
	// finished.
	select {
	case msg := <-alice.announceChan:
		t.Fatalf("announced %T on the output the splice left", msg)
	case <-time.After(300 * time.Millisecond):
	}
	require.Eventually(t, func() bool {
		_, _, err := alice.fundingMgr.getChannelOpeningState(
			fundingOutPoint,
		)
		return errors.Is(err, channeldb.ErrChannelNotFound)
	}, 5*time.Second, 20*time.Millisecond)
}
