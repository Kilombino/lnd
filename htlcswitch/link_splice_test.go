package htlcswitch

import (
	"sync"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/fn/v2"
	"github.com/lightningnetwork/lnd/input"
	"github.com/lightningnetwork/lnd/keychain"
	"github.com/lightningnetwork/lnd/lnwallet"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/stretchr/testify/require"
)

// recordingSpliceHandler records what the link hands the splice machine,
// and gives it the reestablish fields to send.
type recordingSpliceHandler struct {
	mu            sync.Mutex
	messages      []lnwire.Message
	commits       []error
	reestablish   []*lnwire.ChannelReestablish
	nextFunding   fn.Option[lnwire.FundingTxidFlags]
	fundingLocked fn.Option[lnwire.FundingTxidFlags]
}

func (h *recordingSpliceHandler) ReestablishTLVs() (
	fn.Option[lnwire.FundingTxidFlags],
	fn.Option[lnwire.FundingTxidFlags]) {

	h.mu.Lock()
	defer h.mu.Unlock()

	return h.nextFunding, h.fundingLocked
}

func (h *recordingSpliceHandler) HandleReestablish(
	msg *lnwire.ChannelReestablish) {

	h.mu.Lock()
	defer h.mu.Unlock()

	h.reestablish = append(h.reestablish, msg)
}

func (h *recordingSpliceHandler) HandleSpliceMessage(msg lnwire.Message) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.messages = append(h.messages, msg)
}

func (h *recordingSpliceHandler) SpliceCommitReceived(_ chainhash.Hash,
	err error) {

	h.mu.Lock()
	defer h.mu.Unlock()

	h.commits = append(h.commits, err)
}

// messageCounter counts the messages a server receives, by type.
type messageCounter struct {
	mu     sync.Mutex
	counts map[lnwire.MessageType]int
	sigs   []*lnwire.CommitSig
}

func newMessageCounter(s *mockServer) *messageCounter {
	c := &messageCounter{counts: make(map[lnwire.MessageType]int)}
	s.intersect(func(m lnwire.Message) (bool, error) {
		c.mu.Lock()
		defer c.mu.Unlock()

		c.counts[m.MsgType()]++
		if sig, ok := m.(*lnwire.CommitSig); ok {
			c.sigs = append(c.sigs, sig)
		}

		return false, nil
	})

	return c
}

func (c *messageCounter) count(t lnwire.MessageType) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.counts[t]
}

// commitSigs returns the commitment_signed messages received so far.
func (c *messageCounter) commitSigs() []*lnwire.CommitSig {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]*lnwire.CommitSig(nil), c.sigs...)
}

// pendSplice gives both channels the same pending splice, Alice adding
// funds, and exchanges its own commitments, as the splice machine would
// before the links carry on.
func pendSplice(t *testing.T, alice, bob *lnwallet.LightningChannel,
	added btcutil.Amount) chainhash.Hash {

	t.Helper()

	aliceKey, err := btcec.NewPrivateKey()
	require.NoError(t, err)
	bobKey, err := btcec.NewPrivateKey()
	require.NoError(t, err)
	as := alice.Signer.(*input.MockSigner)
	as.Privkeys = append(as.Privkeys, aliceKey)
	bs := bob.Signer.(*input.MockSigner)
	bs.Privkeys = append(bs.Privkeys, bobKey)

	txid := chainhash.Hash{0x5e, 0x11}
	op := wire.OutPoint{Hash: txid, Index: 0}
	capacity := alice.State().Capacity + added

	require.NoError(t, alice.AddPendingFunding(lnwallet.PendingFunding{
		OutPoint: op, Capacity: capacity,
		LocalKey:   keychain.KeyDescriptor{PubKey: aliceKey.PubKey()},
		RemoteKey:  bobKey.PubKey(),
		LocalDelta: added,
	}))
	require.NoError(t, bob.AddPendingFunding(lnwallet.PendingFunding{
		OutPoint: op, Capacity: capacity,
		LocalKey:    keychain.KeyDescriptor{PubKey: bobKey.PubKey()},
		RemoteKey:   aliceKey.PubKey(),
		RemoteDelta: added,
	}))

	sigs, err := alice.SignSpliceCommitment(t.Context(), txid)
	require.NoError(t, err)
	require.NoError(t, bob.ReceiveSpliceCommitment(txid, sigs))
	sigs, err = bob.SignSpliceCommitment(t.Context(), txid)
	require.NoError(t, err)
	require.NoError(t, alice.ReceiveSpliceCommitment(txid, sigs))

	return txid
}

// pay sends amount from Alice to Bob over the two hop network and waits for
// it to settle.
func pay(t *testing.T, n *twoHopNetwork, amount lnwire.MilliSatoshi) {
	t.Helper()

	htlcAmt, totalTimelock, hops := generateHops(
		amount, testStartingHeight, n.bobChannelLink,
	)
	_, err := makePayment(
		n.aliceServer, n.bobServer, n.bobChannelLink.ShortChanID(),
		hops, amount, htlcAmt, totalTimelock,
	).Wait(30 * time.Second)
	require.NoError(t, err)
}

// waitClean waits until both channels have nothing in flight.
func waitClean(t *testing.T, n *twoHopNetwork) {
	t.Helper()

	require.Eventually(t, func() bool {
		return n.aliceChannelLink.channel.IsChannelClean() &&
			n.bobChannelLink.channel.IsChannelClean()
	}, 10*time.Second, 50*time.Millisecond)
}

// TestLinkSpliceBatches checks the link while a splice is pending: a payment
// goes through with every commitment sent as a batch, one commitment_signed
// per funding, each naming its funding; both sides end with a commitment on
// the splice's funding they can publish. Once the splice locks, through the
// links, commitments go out alone, on the new funding.
func TestLinkSpliceBatches(t *testing.T) {
	t.Parallel()

	alice, bob, err := createMirroredChannel(
		t, btcutil.SatoshiPerBitcoin*3, btcutil.SatoshiPerBitcoin*5,
	)
	require.NoError(t, err)

	const added = btcutil.SatoshiPerBitcoin
	txid := pendSplice(t, alice.channel, bob.channel, added)
	current := alice.channel.State().CurrentFundingOutpoint().Hash

	aliceHandler := &recordingSpliceHandler{}
	bobHandler := &recordingSpliceHandler{}
	n := newTwoHopNetwork(
		t, alice.channel, bob.channel, testStartingHeight,
		func(c *ChannelLinkConfig) { c.SpliceHandler = aliceHandler },
		func(c *ChannelLinkConfig) { c.SpliceHandler = bobHandler },
	)
	atBob := newMessageCounter(n.bobServer)
	atAlice := newMessageCounter(n.aliceServer)

	pay(t, n, lnwire.NewMSatFromSatoshis(10_000_000))
	waitClean(t, n)

	// Every commitment went out as a batch: a start_batch and two
	// commitment_signed messages, one per funding.
	for _, c := range []*messageCounter{atBob, atAlice} {
		batches := c.count(lnwire.MsgStartBatch)
		require.Positive(t, batches)
		require.Equal(t, 2*batches, c.count(lnwire.MsgCommitSig))

		var onCurrent, onSplice int
		for _, sig := range c.commitSigs() {
			switch sig.FundingTxid.UnwrapOr(chainhash.Hash{}) {
			case current:
				onCurrent++
			case txid:
				onSplice++
			default:
				t.Fatalf("commitment_signed names %v",
					sig.FundingTxid)
			}
		}
		require.Equal(t, batches, onCurrent)
		require.Equal(t, batches, onSplice)
	}

	for _, link := range []*channelLink{
		n.aliceChannelLink, n.bobChannelLink,
	} {
		tx, err := link.channel.SignedSpliceCommitment(txid)
		require.NoError(t, err)
		require.Equal(t, txid, tx.TxIn[0].PreviousOutPoint.Hash)
	}

	// Lock through both links.
	scid := lnwire.ShortChannelID{BlockHeight: 900_000, TxIndex: 1}
	require.NoError(t, <-n.aliceChannelLink.LockSplice(txid, scid))
	require.NoError(t, <-n.bobChannelLink.LockSplice(txid, scid))

	batchesBefore := atBob.count(lnwire.MsgStartBatch)
	sigsBefore := len(atBob.commitSigs())

	pay(t, n, lnwire.NewMSatFromSatoshis(5_000_000))
	waitClean(t, n)

	require.Equal(t, batchesBefore, atBob.count(lnwire.MsgStartBatch))
	after := atBob.commitSigs()[sigsBefore:]
	require.NotEmpty(t, after)
	for _, sig := range after {
		require.Equal(t, fn.Some(txid), sig.FundingTxid)
	}
	require.Equal(t, txid,
		n.aliceChannelLink.channel.State().CurrentFundingOutpoint().Hash)
	require.Equal(t,
		alice.channel.State().Capacity,
		btcutil.Amount(8*btcutil.SatoshiPerBitcoin+added),
	)
}

// spliceLinkHarness is a single link with splicing on, the peer's channel
// beside it, and what the link reports.
type spliceLinkHarness struct {
	link     *channelLink
	bob      *lnwallet.LightningChannel
	handler  *recordingSpliceHandler
	failures chan LinkFailureError
	sent     chan lnwire.Message
}

func newSpliceLinkHarness(t *testing.T) *spliceLinkHarness {
	t.Helper()

	h, err := newSingleLinkTestHarness(
		t, 5*btcutil.SatoshiPerBitcoin, 0,
	)
	require.NoError(t, err)

	link := h.aliceLink.(*channelLink)
	s := &spliceLinkHarness{
		link:     link,
		bob:      h.bobChannel,
		handler:  &recordingSpliceHandler{},
		failures: make(chan LinkFailureError, 1),
		sent:     link.cfg.Peer.(*mockPeer).sentMsgs,
	}
	link.cfg.SpliceHandler = s.handler
	link.cfg.OnChannelFailure = func(_ lnwire.ChannelID,
		_ lnwire.ShortChannelID, e LinkFailureError) {

		select {
		case s.failures <- e:
		default:
		}
	}
	require.NoError(t, h.start())

	return s
}

// requireFailure waits for the link to fail with the given action.
func (s *spliceLinkHarness) requireFailure(t *testing.T,
	action LinkFailureAction) {

	t.Helper()

	select {
	case e := <-s.failures:
		require.Equal(t, action, e.FailureAction, e.Error())
	case <-time.After(5 * time.Second):
		t.Fatal("link didn't fail")
	}
}

// requireNoFailure checks the link hasn't failed.
func (s *spliceLinkHarness) requireNoFailure(t *testing.T) {
	t.Helper()

	select {
	case e := <-s.failures:
		t.Fatalf("link failed: %v", e)
	case <-time.After(200 * time.Millisecond):
	}
}

// nextSent returns the next message of type T the link sends.
func nextSent[T lnwire.Message](t *testing.T, s *spliceLinkHarness) T {
	t.Helper()

	timeout := time.After(5 * time.Second)
	for {
		select {
		case msg := <-s.sent:
			if m, ok := msg.(T); ok {
				return m
			}
		case <-timeout:
			var zero T
			t.Fatalf("link sent no %T", zero)
			return zero
		}
	}
}

// TestLinkStartBatchRefused checks start_batch handling: one above 20
// messages or with another message inside is a protocol violation that ends
// the connection; one of a single message, or for another message type, is
// ignored.
func TestLinkStartBatchRefused(t *testing.T) {
	t.Parallel()

	commitType := fn.Some(lnwire.MessageType(lnwire.MsgCommitSig))

	t.Run("too big", func(t *testing.T) {
		t.Parallel()

		s := newSpliceLinkHarness(t)
		s.link.HandleChannelUpdate(&lnwire.StartBatch{
			ChannelID: s.link.ChanID(), BatchSize: 21,
			BatchMessageType: commitType,
		})
		s.requireFailure(t, LinkFailureDisconnect)
	})

	t.Run("other message inside", func(t *testing.T) {
		t.Parallel()

		s := newSpliceLinkHarness(t)
		s.link.HandleChannelUpdate(&lnwire.StartBatch{
			ChannelID: s.link.ChanID(), BatchSize: 2,
			BatchMessageType: commitType,
		})
		s.link.HandleChannelUpdate(&lnwire.UpdateFee{
			ChanID: s.link.ChanID(), FeePerKw: 1000,
		})
		s.requireFailure(t, LinkFailureDisconnect)
	})

	t.Run("ignored", func(t *testing.T) {
		t.Parallel()

		s := newSpliceLinkHarness(t)
		s.link.HandleChannelUpdate(&lnwire.StartBatch{
			ChannelID: s.link.ChanID(), BatchSize: 1,
			BatchMessageType: commitType,
		})
		s.link.HandleChannelUpdate(&lnwire.StartBatch{
			ChannelID: s.link.ChanID(), BatchSize: 3,
			BatchMessageType: fn.Some(
				lnwire.MessageType(lnwire.MsgUpdateFee),
			),
		})
		s.link.HandleChannelUpdate(&lnwire.StartBatch{
			ChannelID: s.link.ChanID(), BatchSize: 3,
		})
		s.requireNoFailure(t)
		require.Nil(t, s.link.batch)
	})
}

// bobBatch has Bob add an HTLC and sign, returning his batch: the main
// commitment_signed and one per pending splice.
func bobBatch(t *testing.T, s *spliceLinkHarness) (*lnwire.CommitSig,
	[]*lnwire.CommitSig) {

	t.Helper()

	amt := lnwire.NewMSatFromSatoshis(100_000)
	var blob [lnwire.OnionPacketSize]byte
	_, htlc, _, err := generatePayment(amt, amt, 5, blob)
	require.NoError(t, err)
	_, err = s.bob.AddHTLC(htlc, nil)
	require.NoError(t, err)
	s.link.HandleChannelUpdate(htlc)

	commit, err := s.bob.SignNextCommitment(t.Context())
	require.NoError(t, err)

	main := &lnwire.CommitSig{
		ChanID:    s.link.ChanID(),
		CommitSig: commit.CommitSig,
		HtlcSigs:  commit.HtlcSigs,
		FundingTxid: fn.Some(
			s.bob.State().CurrentFundingOutpoint().Hash,
		),
	}
	var members []*lnwire.CommitSig
	for _, m := range commit.SpliceSigs {
		members = append(members, &lnwire.CommitSig{
			ChanID:      s.link.ChanID(),
			CommitSig:   m.CommitSig,
			HtlcSigs:    m.HtlcSigs,
			FundingTxid: fn.Some(m.FundingTxid),
		})
	}

	return main, members
}

// sendBatch hands the link a start_batch and its members.
func sendBatch(s *spliceLinkHarness, sigs ...*lnwire.CommitSig) {
	s.link.HandleChannelUpdate(&lnwire.StartBatch{
		ChannelID: s.link.ChanID(), BatchSize: uint16(len(sigs)),
		BatchMessageType: fn.Some(
			lnwire.MessageType(lnwire.MsgCommitSig),
		),
	})
	for _, sig := range sigs {
		s.link.HandleChannelUpdate(sig)
	}
}

// TestLinkBatchRefused checks batches that don't cover every funding: the
// channel is failed, as for any invalid commitment.
func TestLinkBatchRefused(t *testing.T) {
	t.Parallel()

	t.Run("main alone while a splice is pending", func(t *testing.T) {
		t.Parallel()

		s := newSpliceLinkHarness(t)
		pendSplice(t, s.link.channel, s.bob, 50_000_000)
		main, _ := bobBatch(t, s)
		s.link.HandleChannelUpdate(main)
		s.requireFailure(t, LinkFailureForceClose)
	})

	t.Run("splice member twice", func(t *testing.T) {
		t.Parallel()

		s := newSpliceLinkHarness(t)
		pendSplice(t, s.link.channel, s.bob, 50_000_000)
		_, members := bobBatch(t, s)
		sendBatch(s, members[0], members[0])
		s.requireFailure(t, LinkFailureForceClose)
	})

	t.Run("unknown funding", func(t *testing.T) {
		t.Parallel()

		s := newSpliceLinkHarness(t)
		pendSplice(t, s.link.channel, s.bob, 50_000_000)
		main, members := bobBatch(t, s)
		unknown := *members[0]
		unknown.FundingTxid = fn.Some(chainhash.Hash{0xbb})
		sendBatch(s, main, members[0], &unknown)
		s.requireFailure(t, LinkFailureForceClose)
	})

	t.Run("member without funding_txid", func(t *testing.T) {
		t.Parallel()

		s := newSpliceLinkHarness(t)
		pendSplice(t, s.link.channel, s.bob, 50_000_000)
		main, members := bobBatch(t, s)
		bare := *members[0]
		bare.FundingTxid = fn.None[chainhash.Hash]()
		sendBatch(s, main, &bare)
		s.requireFailure(t, LinkFailureForceClose)
	})
}

// TestLinkBatchAccepted checks a good batch, in any order, and one carrying
// a member for a splice already dropped, which is ignored: the link answers
// with a revocation.
func TestLinkBatchAccepted(t *testing.T) {
	t.Parallel()

	s := newSpliceLinkHarness(t)
	txid := pendSplice(t, s.link.channel, s.bob, 50_000_000)

	// A second splice, which Alice then drops: Bob still signs for it.
	second := chainhash.Hash{0x5e, 0x22}
	aliceKey, err := btcec.NewPrivateKey()
	require.NoError(t, err)
	bobKey, err := btcec.NewPrivateKey()
	require.NoError(t, err)
	s.link.channel.Signer.(*input.MockSigner).Privkeys = append(
		s.link.channel.Signer.(*input.MockSigner).Privkeys, aliceKey,
	)
	s.bob.Signer.(*input.MockSigner).Privkeys = append(
		s.bob.Signer.(*input.MockSigner).Privkeys, bobKey,
	)
	capacity := s.bob.State().Capacity + 1_000
	op := wire.OutPoint{Hash: second}
	require.NoError(t, s.link.channel.AddPendingFunding(
		lnwallet.PendingFunding{
			OutPoint: op, Capacity: capacity,
			LocalKey: keychain.KeyDescriptor{
				PubKey: aliceKey.PubKey(),
			},
			RemoteKey: bobKey.PubKey(), RemoteDelta: 1_000,
		},
	))
	require.NoError(t, s.bob.AddPendingFunding(lnwallet.PendingFunding{
		OutPoint: op, Capacity: capacity,
		LocalKey:  keychain.KeyDescriptor{PubKey: bobKey.PubKey()},
		RemoteKey: aliceKey.PubKey(), LocalDelta: 1_000,
	}))
	sigs, err := s.link.channel.SignSpliceCommitment(t.Context(), second)
	require.NoError(t, err)
	require.NoError(t, s.bob.ReceiveSpliceCommitment(second, sigs))
	sigs, err = s.bob.SignSpliceCommitment(t.Context(), second)
	require.NoError(t, err)
	require.NoError(t, s.link.channel.ReceiveSpliceCommitment(second, sigs))
	require.NoError(t, s.link.channel.RemovePendingFunding(second))

	main, members := bobBatch(t, s)
	require.Len(t, members, 2)

	// Members out of order, the dropped one included.
	sendBatch(s, members[1], main, members[0])
	nextSent[*lnwire.RevokeAndAck](t, s)
	s.requireNoFailure(t)

	tx, err := s.link.channel.SignedSpliceCommitment(txid)
	require.NoError(t, err)
	require.Equal(t, txid, tx.TxIn[0].PreviousOutPoint.Hash)
}

// TestLinkSpliceCommitSig checks the splice's own commitment_signed while
// the channel is quiescent: checked, recorded and reported to the splice
// machine, not answered with a revocation; and the link sending its own on
// request. Splice messages reach the machine.
func TestLinkSpliceCommitSig(t *testing.T) {
	t.Parallel()

	s := newSpliceLinkHarness(t)

	// Quiesce: Alice asks, Bob answers.
	res := s.link.InitStfu()
	nextSent[*lnwire.Stfu](t, s)
	s.link.HandleChannelUpdate(&lnwire.Stfu{
		ChanID: s.link.ChanID(), Initiator: false,
	})
	select {
	case r := <-res:
		require.NoError(t, r.Err())
	case <-time.After(5 * time.Second):
		t.Fatal("no quiescence")
	}

	// Both add the splice, as their machines would after tx_complete.
	aliceKey, err := btcec.NewPrivateKey()
	require.NoError(t, err)
	bobKey, err := btcec.NewPrivateKey()
	require.NoError(t, err)
	s.link.channel.Signer.(*input.MockSigner).Privkeys = append(
		s.link.channel.Signer.(*input.MockSigner).Privkeys, aliceKey,
	)
	s.bob.Signer.(*input.MockSigner).Privkeys = append(
		s.bob.Signer.(*input.MockSigner).Privkeys, bobKey,
	)
	txid := chainhash.Hash{0x5e, 0x33}
	op := wire.OutPoint{Hash: txid}
	capacity := s.bob.State().Capacity + 20_000_000
	require.NoError(t, <-s.link.AddSpliceFunding(lnwallet.PendingFunding{
		OutPoint: op, Capacity: capacity,
		LocalKey:   keychain.KeyDescriptor{PubKey: aliceKey.PubKey()},
		RemoteKey:  bobKey.PubKey(),
		LocalDelta: 20_000_000,
	}))
	require.NoError(t, s.bob.AddPendingFunding(lnwallet.PendingFunding{
		OutPoint: op, Capacity: capacity,
		LocalKey:    keychain.KeyDescriptor{PubKey: bobKey.PubKey()},
		RemoteKey:   aliceKey.PubKey(),
		RemoteDelta: 20_000_000,
	}))

	// Splice messages go to the machine.
	s.link.HandleChannelUpdate(&lnwire.TxComplete{
		ChannelID: s.link.ChanID(),
	})

	// Alice's commitment_signed, on request.
	require.NoError(t, <-s.link.SendSpliceCommitment(txid))
	sent := nextSent[*lnwire.CommitSig](t, s)
	require.Equal(t, fn.Some(txid), sent.FundingTxid)
	require.NoError(t, s.bob.ReceiveSpliceCommitment(
		txid, &lnwallet.CommitSigs{
			CommitSig: sent.CommitSig, HtlcSigs: sent.HtlcSigs,
		},
	))

	// Bob's: checked and reported, no revocation.
	sigs, err := s.bob.SignSpliceCommitment(t.Context(), txid)
	require.NoError(t, err)
	s.link.HandleChannelUpdate(&lnwire.CommitSig{
		ChanID: s.link.ChanID(), CommitSig: sigs.CommitSig,
		HtlcSigs: sigs.HtlcSigs, FundingTxid: fn.Some(txid),
	})

	require.Eventually(t, func() bool {
		s.handler.mu.Lock()
		defer s.handler.mu.Unlock()

		return len(s.handler.commits) == 1 &&
			len(s.handler.messages) == 1
	}, 5*time.Second, 20*time.Millisecond)
	require.NoError(t, s.handler.commits[0])
	require.IsType(t, &lnwire.TxComplete{}, s.handler.messages[0])

	_, err = s.link.channel.SignedSpliceCommitment(txid)
	require.NoError(t, err)

	select {
	case msg := <-s.sent:
		t.Fatalf("link answered with %T", msg)
	case <-time.After(200 * time.Millisecond):
	}
	s.requireNoFailure(t)

	// A forged one is reported as refused.
	s.link.HandleChannelUpdate(&lnwire.CommitSig{
		ChanID: s.link.ChanID(), CommitSig: sent.CommitSig,
		HtlcSigs: sigs.HtlcSigs, FundingTxid: fn.Some(txid),
	})
	require.Eventually(t, func() bool {
		s.handler.mu.Lock()
		defer s.handler.mu.Unlock()

		return len(s.handler.commits) == 2
	}, 5*time.Second, 20*time.Millisecond)
	require.Error(t, s.handler.commits[1])
}

// TestLinkReestablishSplice checks reconnecting in the middle of a splice:
// Bob's commitment_signed for it was lost, so Alice's next_funding asks for
// it again. Bob's link resends it, Alice's link records it, each machine gets
// the other's channel_reestablish with its splice fields, channel_ready isn't
// resent because a splice is named, and the channel then updates in batches.
func TestLinkReestablishSplice(t *testing.T) {
	t.Parallel()

	alice, bob, err := createMirroredChannel(
		t, btcutil.SatoshiPerBitcoin*3, btcutil.SatoshiPerBitcoin*5,
	)
	require.NoError(t, err)

	aliceKey, err := btcec.NewPrivateKey()
	require.NoError(t, err)
	bobKey, err := btcec.NewPrivateKey()
	require.NoError(t, err)
	as := alice.channel.Signer.(*input.MockSigner)
	as.Privkeys = append(as.Privkeys, aliceKey)
	bs := bob.channel.Signer.(*input.MockSigner)
	bs.Privkeys = append(bs.Privkeys, bobKey)

	const added = btcutil.Amount(30_000_000)
	txid := chainhash.Hash{0x5e, 0x44}
	op := wire.OutPoint{Hash: txid}
	capacity := alice.channel.State().Capacity + added
	require.NoError(t, alice.channel.AddPendingFunding(
		lnwallet.PendingFunding{
			OutPoint: op, Capacity: capacity,
			LocalKey: keychain.KeyDescriptor{
				PubKey: aliceKey.PubKey(),
			},
			RemoteKey: bobKey.PubKey(), LocalDelta: added,
		},
	))
	require.NoError(t, bob.channel.AddPendingFunding(lnwallet.PendingFunding{
		OutPoint: op, Capacity: capacity,
		LocalKey:    keychain.KeyDescriptor{PubKey: bobKey.PubKey()},
		RemoteKey:   aliceKey.PubKey(),
		RemoteDelta: added,
	}))

	// Alice's reached Bob; Bob's was signed and sent, then lost.
	sigs, err := alice.channel.SignSpliceCommitment(t.Context(), txid)
	require.NoError(t, err)
	require.NoError(t, bob.channel.ReceiveSpliceCommitment(txid, sigs))
	_, err = bob.channel.SignSpliceCommitment(t.Context(), txid)
	require.NoError(t, err)

	original := alice.channel.State().FundingOutpoint.Hash
	aliceHandler := &recordingSpliceHandler{
		nextFunding: fn.Some(lnwire.FundingTxidFlags{
			Txid:            txid,
			RetransmitFlags: lnwire.RetransmitCommitSig,
		}),
		fundingLocked: fn.Some(lnwire.FundingTxidFlags{
			Txid: original,
		}),
	}
	bobHandler := &recordingSpliceHandler{
		nextFunding: fn.Some(lnwire.FundingTxidFlags{Txid: txid}),
		fundingLocked: fn.Some(lnwire.FundingTxidFlags{
			Txid: original,
		}),
	}
	n := newTwoHopNetwork(
		t, alice.channel, bob.channel, testStartingHeight,
		func(c *ChannelLinkConfig) { c.SpliceHandler = aliceHandler },
		func(c *ChannelLinkConfig) { c.SpliceHandler = bobHandler },
	)

	// Alice gets Bob's commitment_signed for the splice.
	require.Eventually(t, func() bool {
		aliceHandler.mu.Lock()
		defer aliceHandler.mu.Unlock()

		return len(aliceHandler.commits) == 1
	}, 10*time.Second, 20*time.Millisecond)
	require.NoError(t, aliceHandler.commits[0])

	// Each machine saw the other's splice fields.
	for _, h := range []*recordingSpliceHandler{aliceHandler, bobHandler} {
		h.mu.Lock()
		require.Len(t, h.reestablish, 1)
		got := h.reestablish[0]
		h.mu.Unlock()

		require.Equal(t, txid,
			got.NextFunding.UnsafeFromSome().Txid)
		require.Equal(t, original,
			got.MyCurrentFundingLocked.UnsafeFromSome().Txid)
	}

	// No channel_ready resent: a splice was named.
	for _, s := range []*mockServer{n.aliceServer, n.bobServer} {
		s.protocolTraceMtx.Lock()
		for _, m := range s.protocolTrace {
			require.NotEqual(t,
				lnwire.MessageType(lnwire.MsgChannelReady),
				m.MsgType())
		}
		s.protocolTraceMtx.Unlock()
	}

	// The channel carries on, in batches.
	pay(t, n, lnwire.NewMSatFromSatoshis(2_000_000))
	waitClean(t, n)
	for _, link := range []*channelLink{
		n.aliceChannelLink, n.bobChannelLink,
	} {
		_, err := link.channel.SignedSpliceCommitment(txid)
		require.NoError(t, err)
	}
}

// TestLinkNoSyncStillReestablishes checks a link of a channel just opened,
// which has no channel_reestablish to exchange, tells the splice machine it
// may send all the same: without it a splice's splice_locked would wait for
// a reestablish that never comes.
func TestLinkNoSyncStillReestablishes(t *testing.T) {
	t.Parallel()

	h, err := newSingleLinkTestHarness(t, btcutil.SatoshiPerBitcoin, 0)
	require.NoError(t, err)
	link := h.aliceLink.(*channelLink)
	handler := &recordingSpliceHandler{}
	link.cfg.SpliceHandler = handler
	link.cfg.SyncStates = false
	require.NoError(t, h.start())

	require.Eventually(t, func() bool {
		handler.mu.Lock()
		defer handler.mu.Unlock()

		return len(handler.reestablish) == 1
	}, 5*time.Second, 10*time.Millisecond)
}
