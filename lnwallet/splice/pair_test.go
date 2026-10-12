//go:build !dev && !integration

package splice

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/chainntnfs"
	"github.com/lightningnetwork/lnd/input"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/lnwallet"
	"github.com/lightningnetwork/lnd/lnwallet/chainfee"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/stretchr/testify/require"
)

// node is one side of a channel with its splice machine.
type node struct {
	ch      *lnwallet.LightningChannel
	machine *Machine
	link    *testLink
	peer    *testPeer
	chain   *testChain
	wallet  *fakeWallet
	signer  *input.MockSigner
}

// newNode sets up a side's machine.
func newNode(t *testing.T, ch *lnwallet.LightningChannel,
	other *lnwallet.LightningChannel, initiator lntypes.ChannelParty,
	chain *testChain) *node {

	t.Helper()

	signer := ch.Signer.(*input.MockSigner)
	signer.NetParams = &chaincfg.RegressionNetParams

	n := &node{
		ch: ch,
		link: &testLink{
			t: t, ch: ch, initiator: initiator,
			commits: make(chan *lnwire.CommitSig, 4),
			resumed: make(chan struct{}, 8),
		},
		peer:   &testPeer{msgs: make(chan lnwire.Message, 64)},
		chain:  chain,
		wallet: newFakeWallet(),
		signer: signer,
	}

	var ours, theirs [33]byte
	copy(ours[:], ch.State().IdentityPub.SerializeCompressed())
	copy(theirs[:], other.State().IdentityPub.SerializeCompressed())
	changeKey, err := btcec.NewPrivateKey()
	require.NoError(t, err)

	n.machine = NewMachine(Config{
		ChanID:      lnwire.NewChanIDFromOutPoint(ch.ChannelPoint()),
		OurNodeID:   ours,
		TheirNodeID: theirs,
		Link:        n.link,
		Channel:     ch,
		Peer:        n.peer,
		Chain:       chain,
		Wallet:      n.wallet,
		CoinSource:  n.wallet,
		KeyRing:     &testKeyRing{signer: signer},
		Signer:      signer,
		Estimator:   chainfee.NewStaticEstimator(2_500, 253),
		DustLimit:   dustLimit,
		ChangeScript: func() ([]byte, error) {
			return input.WitnessPubKeyHash(
				changeKey.PubKey().SerializeCompressed(),
			)
		},
		Policy: Policy{
			MaxChannelSize: 100 * btcutil.SatoshiPerBitcoin,
			LockDepth: func(btcutil.Amount) uint32 {
				return 6
			},
		},
	})
	require.NoError(t, n.machine.Start())
	t.Cleanup(n.machine.Stop)

	return n
}

// fund gives a node a P2WPKH coin its signer can spend, known to the
// chain as confirmed.
func (n *node) fund(t *testing.T, tag uint16, value btcutil.Amount) Coin {
	t.Helper()

	key, err := btcec.NewPrivateKey()
	require.NoError(t, err)
	n.signer.Privkeys = append(n.signer.Privkeys, key)
	script, err := input.WitnessPubKeyHash(
		key.PubKey().SerializeCompressed(),
	)
	require.NoError(t, err)

	c := coin(tag, script, value)
	n.wallet.txs[c.OutPoint.Hash] = c.PrevTx
	n.wallet.utxos = append(n.wallet.utxos, &lnwallet.Utxo{
		OutPoint: c.OutPoint, Value: c.Value, PkScript: c.PkScript,
	})
	n.chain.outs[c.OutPoint] = c.PrevTx.TxOut[0]

	return c
}

// splicePair is two nodes with their messages carried between them.
type splicePair struct {
	alice, bob *node
	chain      *testChain
}

func newSplicePair(t *testing.T) *splicePair {
	t.Helper()

	a, b, err := lnwallet.CreateTestChannels(t, spliceChanType)
	require.NoError(t, err)

	chain := newTestChain()
	p := &splicePair{
		alice: newNode(t, a, b, lntypes.Local, chain),
		bob:   newNode(t, b, a, lntypes.Remote, chain),
		chain: chain,
	}

	quit := make(chan struct{})
	t.Cleanup(func() { close(quit) })
	carry := func(from, to *node) {
		for {
			select {
			case msg := <-from.peer.msgs:
				to.machine.HandleSpliceMessage(msg)

			case sig := <-from.link.commits:
				txid := sig.FundingTxid.UnsafeFromSome()
				err := to.ch.ReceiveSpliceCommitment(
					txid, &lnwallet.CommitSigs{
						CommitSig: sig.CommitSig,
						HtlcSigs:  sig.HtlcSigs,
					},
				)
				to.machine.SpliceCommitReceived(txid, err)

			case <-quit:
				return
			}
		}
	}
	go carry(p.alice, p.bob)
	go carry(p.bob, p.alice)

	return p
}

// await returns the next update of a splice.
func await(t *testing.T, updates <-chan Update) Update {
	t.Helper()

	select {
	case u := <-updates:
		return u
	case <-time.After(10 * time.Second):
		t.Fatal("no splice update")
		return Update{}
	}
}

// requireUpdates checks the next updates are these states, returning the
// last.
func requireUpdates(t *testing.T, updates <-chan Update,
	states ...State) Update {

	t.Helper()

	var u Update
	for _, s := range states {
		u = await(t, updates)
		require.Equal(t, s, u.State, u.Reason)
	}

	return u
}

// confirm confirms the splice at lock depth for both machines.
func (p *splicePair) confirm(t *testing.T, txid chainhash.Hash) {
	t.Helper()

	p.chain.confirm(t, txid, 6, 2, &chainntnfs.TxConfirmation{
		BlockHeight: 900_006, TxIndex: 2,
	})
}

// requireMoved waits for both channels to be on the splice's funding.
func (p *splicePair) requireMoved(t *testing.T, txid chainhash.Hash,
	capacity btcutil.Amount) {

	t.Helper()

	for _, n := range []*node{p.alice, p.bob} {
		require.Eventually(t, func() bool {
			return n.ch.State().CurrentFundingOutpoint().Hash ==
				txid
		}, 10*time.Second, 20*time.Millisecond)
		require.Equal(t, capacity, n.ch.State().Capacity)
		require.Empty(t, n.ch.PendingFundings())
	}
}

// TestSpliceInBetweenMachines runs a splice Alice's machine starts, adding
// funds from her wallet, against Bob's machine, to the lock: both publish
// the same transaction, every input of which verifies, and both channels
// move to its funding output with the larger capacity.
func TestSpliceInBetweenMachines(t *testing.T) {
	t.Parallel()

	p := newSplicePair(t)
	p.alice.fund(t, 1, 40_000_000)
	p.alice.fund(t, 2, 15_000_000)
	capacity := p.alice.ch.State().Capacity
	const amount = btcutil.Amount(45_000_000)

	events := make(chan string, 8)
	p.alice.machine.cfg.NotifyEvent = func(kind string, _ chainhash.Hash,
		_ string, _ bool) {

		events <- kind
	}

	updates, err := p.alice.machine.Splice(Request{
		Direction: DirectionIn, Amount: amount, FeeRate: 2_500,
	})
	require.NoError(t, err)
	u := requireUpdates(t, updates,
		StatePreparing, StateNegotiating, StateSigned, StateBroadcast,
	)
	txid := u.Txid

	// Both published the same, fully valid transaction.
	aliceTxs := p.alice.wallet.publishedTxs()
	require.Contains(t, aliceTxs, txid)
	require.Equal(t, Label(p.alice.ch.State().ShortChannelID, DirectionIn),
		aliceTxs[txid])
	require.Eventually(t, func() bool {
		_, ok := p.bob.wallet.publishedTxs()[txid]
		return ok
	}, 10*time.Second, 10*time.Millisecond)

	p.alice.machine.mu.Lock()
	tx, err := p.alice.machine.signedTx()
	res := p.alice.machine.active.res
	p.alice.machine.mu.Unlock()
	require.NoError(t, err)
	requireValid(t, tx, res)
	require.Len(t, tx.TxIn, 3)

	// Alice's coins stay leased until the lock.
	require.Equal(t, 2, p.alice.wallet.leaseCount())

	p.confirm(t, txid)
	requireUpdates(t, updates, StateLocked)
	p.requireMoved(t, txid, capacity+amount)
	require.Zero(t, p.alice.wallet.leaseCount())

	// Both channels record it as their last splice.
	for _, n := range []*node{p.alice, p.bob} {
		require.Eventually(t, func() bool {
			last, err := n.ch.State().LastSplice()
			return err == nil && last != nil &&
				last.Outcome == "locked" && last.Txid == txid
		}, 5*time.Second, 10*time.Millisecond)
	}
	last, err := p.alice.ch.State().LastSplice()
	require.NoError(t, err)
	require.True(t, last.WeInitiated)
	require.Equal(t, amount, last.LocalDelta)

	_, open := <-updates
	require.False(t, open)

	// The channel event stream heard of each step.
	for _, kind := range []string{"started", "signed", "locked"} {
		require.Equal(t, kind, <-events)
	}
}

// TestSpliceOutBetweenMachines runs a splice moving funds out of Alice's
// balance to an address: the destination gets the amount, the fee comes
// from her balance too, and the channel shrinks by both.
func TestSpliceOutBetweenMachines(t *testing.T) {
	t.Parallel()

	p := newSplicePair(t)
	capacity := p.alice.ch.State().Capacity
	const amount = btcutil.Amount(20_000_000)
	dest := p2tr(0x77)

	updates, err := p.alice.machine.Splice(Request{
		Direction: DirectionOut, Amount: amount, FeeRate: 2_500,
		Destination: dest,
	})
	require.NoError(t, err)
	u := requireUpdates(t, updates,
		StatePreparing, StateNegotiating, StateSigned, StateBroadcast,
	)

	p.alice.machine.mu.Lock()
	tx, err := p.alice.machine.signedTx()
	res := p.alice.machine.active.res
	p.alice.machine.mu.Unlock()
	require.NoError(t, err)
	requireValid(t, tx, res)
	require.Len(t, tx.TxIn, 1)

	var paid bool
	var fundingValue int64
	for i, out := range tx.TxOut {
		if string(out.PkScript) == string(dest) {
			require.Equal(t, int64(amount), out.Value)
			paid = true
		}
		if uint32(i) == res.FundingIndex {
			fundingValue = out.Value
		}
	}
	require.True(t, paid)
	fee := btcutil.Amount(int64(capacity) - fundingValue - int64(amount))
	require.Positive(t, fee)
	require.Equal(t, Label(p.alice.ch.State().ShortChannelID, DirectionOut),
		p.alice.wallet.publishedTxs()[u.Txid])

	p.confirm(t, u.Txid)
	requireUpdates(t, updates, StateLocked)
	p.requireMoved(t, u.Txid, btcutil.Amount(fundingValue))
}

// TestSpliceRequestRefused checks requests refused before anything is sent:
// not enough coins, a fee below the floor, a destination other than the
// channel's close address, more out than the balance; and one ending when
// the peer quiesced first.
func TestSpliceRequestRefused(t *testing.T) {
	t.Parallel()

	t.Run("insufficient funds", func(t *testing.T) {
		t.Parallel()

		p := newSplicePair(t)
		p.alice.fund(t, 1, 1_000_000)
		_, err := p.alice.machine.Splice(Request{
			Direction: DirectionIn, Amount: 2_000_000,
			FeeRate: 2_500,
		})
		require.ErrorIs(t, err, ErrRefused)
		require.ErrorContains(t, err, codeInsufficientFunds)
		require.Zero(t, p.alice.wallet.leaseCount())
	})

	t.Run("fee below floor", func(t *testing.T) {
		t.Parallel()

		p := newSplicePair(t)
		p.alice.machine.cfg.Estimator = estimateOnly{rate: 10_000}
		_, err := p.alice.machine.Splice(Request{
			Direction: DirectionOut, Amount: 1_000_000,
			FeeRate: 9_999, Destination: p2tr(1),
		})
		require.ErrorContains(t, err, codeFeeBelowFloor)

		// Forced, it goes ahead.
		updates, err := p.alice.machine.Splice(Request{
			Direction: DirectionOut, Amount: 1_000_000,
			FeeRate: 9_999, Destination: p2tr(1),
			ForceLowFee: true,
		})
		require.NoError(t, err)
		requireUpdates(t, updates, StatePreparing)
	})

	t.Run("close address set", func(t *testing.T) {
		t.Parallel()

		p := newSplicePair(t)
		p.alice.ch.State().LocalShutdownScript = p2wpkh(5)
		_, err := p.alice.machine.Splice(Request{
			Direction: DirectionOut, Amount: 1_000_000,
			FeeRate: 2_500, Destination: p2tr(1),
		})
		require.ErrorContains(t, err, "close_address_set")
	})

	t.Run("more than the balance", func(t *testing.T) {
		t.Parallel()

		p := newSplicePair(t)
		avail := p.alice.ch.AvailableBalance().ToSatoshis()
		_, err := p.alice.machine.Splice(Request{
			Direction: DirectionOut, Amount: avail,
			FeeRate: 2_500, Destination: p2tr(1),
		})
		require.ErrorContains(t, err, "above_max_out")
	})

	t.Run("peer quiesced first", func(t *testing.T) {
		t.Parallel()

		p := newSplicePair(t)
		p.alice.fund(t, 1, 5_000_000)
		p.alice.link.stfuResult = lntypes.Remote
		updates, err := p.alice.machine.Splice(Request{
			Direction: DirectionIn, Amount: 1_000_000,
			FeeRate: 2_500,
		})
		require.NoError(t, err)
		u := requireUpdates(t, updates, StatePreparing, StateAborted)
		require.Contains(t, u.Reason, codeNotQuiescent)
		require.Zero(t, p.alice.wallet.leaseCount())

		// The machine takes a new splice afterwards.
		p.alice.link.stfuResult = lntypes.Local
		updates, err = p.alice.machine.Splice(Request{
			Direction: DirectionIn, Amount: 1_000_000,
			FeeRate: 2_500,
		})
		require.NoError(t, err)
		requireUpdates(t, updates, StatePreparing, StateNegotiating,
			StateSigned, StateBroadcast)
	})
}

// soloAlice is Alice's machine as initiator with the test playing Bob.
func soloAlice(t *testing.T) (*node, *lnwallet.LightningChannel) {
	t.Helper()

	a, b, err := lnwallet.CreateTestChannels(t, spliceChanType)
	require.NoError(t, err)
	chain := newTestChain()

	return newNode(t, a, b, lntypes.Local, chain), b
}

// nextFrom returns the node's next message to the peer.
func nextFrom[T lnwire.Message](t *testing.T, n *node) T {
	t.Helper()

	select {
	case msg := <-n.peer.msgs:
		m, ok := msg.(T)
		require.True(t, ok, "got %T", msg)

		return m
	case <-time.After(5 * time.Second):
		var zero T
		t.Fatalf("no %T", zero)

		return zero
	}
}

// TestSpliceInitiatorRefusesPeerContribution checks that in our splice the
// peer may put nothing in: a splice_ack contributing, or an input it adds,
// ends the splice with tx_abort before anything is signed.
func TestSpliceInitiatorRefusesPeerContribution(t *testing.T) {
	t.Parallel()

	start := func(t *testing.T) (*node, <-chan Update,
		*lnwire.SpliceInit) {

		alice, _ := soloAlice(t)
		alice.fund(t, 1, 5_000_000)
		updates, err := alice.machine.Splice(Request{
			Direction: DirectionIn, Amount: 1_000_000,
			FeeRate: 2_500,
		})
		require.NoError(t, err)
		requireUpdates(t, updates, StatePreparing, StateNegotiating)

		return alice, updates, nextFrom[*lnwire.SpliceInit](t, alice)
	}

	t.Run("contribution in splice_ack", func(t *testing.T) {
		t.Parallel()

		alice, updates, init := start(t)
		key, _ := btcec.NewPrivateKey()
		alice.machine.HandleSpliceMessage(&lnwire.SpliceAck{
			ChannelID: init.ChannelID, FundingContribution: 1_000,
			FundingPubKey: key.PubKey(),
		})
		abort := nextFrom[*lnwire.TxAbort](t, alice)
		require.Contains(t, string(abort.Data), codePeerContribUnsupp)
		u := requireUpdates(t, updates, StateAborted)
		require.Contains(t, u.Reason, codePeerContribUnsupp)
		require.Zero(t, alice.wallet.leaseCount())

		last, err := alice.ch.State().LastSplice()
		require.NoError(t, err)
		require.Equal(t, "aborted", last.Outcome)
		require.Contains(t, last.Reason, codePeerContribUnsupp)
	})

	t.Run("an input added by the peer", func(t *testing.T) {
		t.Parallel()

		alice, updates, init := start(t)
		key, _ := btcec.NewPrivateKey()
		alice.machine.HandleSpliceMessage(&lnwire.SpliceAck{
			ChannelID: init.ChannelID, FundingPubKey: key.PubKey(),
		})

		// Alice adds her items; Bob answers the first with an
		// input of his own.
		nextFrom[*lnwire.TxAddInput](t, alice)
		peerCoin := coin(9, p2wpkh(9), 2_000_000)
		var buf bytes.Buffer
		require.NoError(t, peerCoin.PrevTx.Serialize(&buf))
		alice.machine.HandleSpliceMessage(&lnwire.TxAddInput{
			ChannelID: init.ChannelID, SerialID: 1,
			PrevTx: buf.Bytes(), Sequence: 0xfffffffd,
		})

		for {
			msg := <-alice.peer.msgs
			if abort, ok := msg.(*lnwire.TxAbort); ok {
				require.Contains(t, string(abort.Data),
					codePeerContribUnsupp)
				break
			}
		}
		u := requireUpdates(t, updates, StateAborted)
		require.Contains(t, u.Reason, codePeerContribUnsupp)
		require.Zero(t, alice.wallet.leaseCount())
		require.Empty(t, alice.ch.PendingFundings())
	})
}

// TestSpliceInitiatorMempoolCheck checks that, signing last, Alice tests the
// complete transaction against the backend before her tx_signatures leave,
// and drops the splice if it would be refused.
func TestSpliceInitiatorMempoolCheck(t *testing.T) {
	t.Parallel()

	p := newSplicePair(t)
	p.alice.fund(t, 1, 5_000_000)
	p.alice.machine.cfg.CheckMempool = func(*wire.MsgTx) error {
		return errors.New("min relay fee not met")
	}

	updates, err := p.alice.machine.Splice(Request{
		Direction: DirectionIn, Amount: 1_000_000, FeeRate: 2_500,
	})
	require.NoError(t, err)
	u := requireUpdates(t, updates,
		StatePreparing, StateNegotiating, StateAborted,
	)
	require.Contains(t, u.Reason, "relay_rejected")
	require.Empty(t, p.alice.wallet.publishedTxs())
	require.Zero(t, p.alice.wallet.leaseCount())
	require.Empty(t, p.alice.ch.PendingFundings())

	// Bob signed first and can't forget the splice until one of its
	// inputs is spent: Alice could still complete and publish it. He
	// keeps it and marks her.
	require.Eventually(t, func() bool {
		done := make(chan bool)
		p.bob.machine.enqueue(func() { done <- p.bob.machine.misbehaved })
		return <-done
	}, 5*time.Second, 10*time.Millisecond)
	require.Len(t, p.bob.ch.PendingFundings(), 1)
	require.Empty(t, p.bob.wallet.publishedTxs())
}

// TestSpliceOutRecheckedWhenQuiescent checks that a splice-out is checked
// again once the channel is quiescent: an HTLC sent while it waited leaves
// too little, and the splice ends before splice_init.
func TestSpliceOutRecheckedWhenQuiescent(t *testing.T) {
	t.Parallel()

	alice, bob := soloAlice(t)
	alice.link.stfuGate = make(chan struct{})

	avail := alice.ch.AvailableBalance().ToSatoshis()
	updates, err := alice.machine.Splice(Request{
		Direction: DirectionOut, Amount: avail - 200_000,
		FeeRate: 2_500, Destination: p2tr(1),
	})
	require.NoError(t, err)
	requireUpdates(t, updates, StatePreparing)

	// An HTLC goes out while the machine waits for quiescence.
	var preimage [32]byte
	preimage[0] = 1
	hash := sha256Hash(preimage[:])
	htlc := &lnwire.UpdateAddHTLC{
		PaymentHash: hash, Amount: lnwire.NewMSatFromSatoshis(500_000),
		Expiry: 500,
	}
	_, err = alice.ch.AddHTLC(htlc, nil)
	require.NoError(t, err)
	_, err = bob.ReceiveHTLC(htlc)
	require.NoError(t, err)

	close(alice.link.stfuGate)
	u := requireUpdates(t, updates, StateAborted)
	require.Contains(t, u.Reason, "above_max_out")

	select {
	case msg := <-alice.peer.msgs:
		t.Fatalf("sent %T", msg)
	case <-time.After(100 * time.Millisecond):
	}
}

// sha256Hash returns the SHA256 of b.
func sha256Hash(b []byte) [32]byte {
	return sha256.Sum256(b)
}

// TestSpliceCancel checks canceling a splice we started: while preparing it
// just ends; once the splice is signed it carries on.
func TestSpliceCancel(t *testing.T) {
	t.Parallel()

	alice, _ := soloAlice(t)
	alice.fund(t, 1, 5_000_000)
	alice.link.stfuGate = make(chan struct{})

	updates, err := alice.machine.Splice(Request{
		Direction: DirectionIn, Amount: 1_000_000, FeeRate: 2_500,
	})
	require.NoError(t, err)
	requireUpdates(t, updates, StatePreparing)
	require.True(t, alice.machine.Cancel())
	u := requireUpdates(t, updates, StateAborted)
	require.Contains(t, u.Reason, "canceled")
	require.Zero(t, alice.wallet.leaseCount())
	require.False(t, alice.machine.Cancel())
	close(alice.link.stfuGate)

	p := newSplicePair(t)
	p.alice.fund(t, 1, 5_000_000)
	updates, err = p.alice.machine.Splice(Request{
		Direction: DirectionIn, Amount: 1_000_000, FeeRate: 2_500,
	})
	require.NoError(t, err)
	requireUpdates(t, updates, StatePreparing, StateNegotiating,
		StateSigned)
	require.False(t, p.alice.machine.Cancel())
	requireUpdates(t, updates, StateBroadcast)
}

// TestSpliceRebroadcast checks that a published splice goes out again at
// every block until it confirms, and not after.
func TestSpliceRebroadcast(t *testing.T) {
	t.Parallel()

	p := newSplicePair(t)
	p.alice.fund(t, 1, 5_000_000)
	updates, err := p.alice.machine.Splice(Request{
		Direction: DirectionIn, Amount: 1_000_000, FeeRate: 2_500,
	})
	require.NoError(t, err)
	u := requireUpdates(t, updates, StatePreparing, StateNegotiating,
		StateSigned, StateBroadcast)

	before := p.alice.wallet.publishCount()
	require.Eventually(t, func() bool {
		p.chain.mine()
		return p.alice.wallet.publishCount() >= before+2
	}, 5*time.Second, 20*time.Millisecond)

	// Confirmed: no more.
	p.chain.confirm(t, u.Txid, 1, 2, &chainntnfs.TxConfirmation{})
	time.Sleep(100 * time.Millisecond)
	settled := p.alice.wallet.publishCount()
	for i := 0; i < 3; i++ {
		p.chain.mine()
	}
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, settled, p.alice.wallet.publishCount())
}

// TestSpliceDead checks a splice one of whose inputs another transaction
// spends: at 6 confirmations of that spend both sides drop it from the
// channel and record it as dead, the starter hears why; at 144 it leaves the
// starter's wallet and its coins are released.
func TestSpliceDead(t *testing.T) {
	t.Parallel()

	p := newSplicePair(t)
	c := p.alice.fund(t, 1, 5_000_000)
	updates, err := p.alice.machine.Splice(Request{
		Direction: DirectionIn, Amount: 1_000_000, FeeRate: 2_500,
	})
	require.NoError(t, err)
	u := requireUpdates(t, updates, StatePreparing, StateNegotiating,
		StateSigned, StateBroadcast)
	require.Len(t, p.alice.ch.PendingFundings(), 1)

	// Alice's coin goes elsewhere: both machines watch it.
	conflict := wire.NewMsgTx(2)
	conflict.AddTxIn(&wire.TxIn{PreviousOutPoint: c.OutPoint})
	conflict.AddTxOut(&wire.TxOut{Value: 4_000_000, PkScript: p2tr(3)})
	p.chain.spend(t, c.OutPoint, conflict, 2)
	p.chain.confirm(t, conflict.TxHash(), deadDepth, 2,
		&chainntnfs.TxConfirmation{})

	u = requireUpdates(t, updates, StateAborted)
	require.Contains(t, u.Reason, "dead")
	for _, n := range []*node{p.alice, p.bob} {
		require.Eventually(t, func() bool {
			return len(n.ch.PendingFundings()) == 0
		}, 5*time.Second, 10*time.Millisecond)
		last, err := n.ch.State().LastSplice()
		require.NoError(t, err)
		require.Equal(t, "dead", last.Outcome)
	}

	// Deep enough: out of the wallet.
	p.chain.confirm(t, conflict.TxHash(), forgetDepth, 2,
		&chainntnfs.TxConfirmation{})
	require.Eventually(t, func() bool {
		removed := p.alice.wallet.removedTxs()
		return len(removed) == 1 && removed[0] == u.Txid
	}, 5*time.Second, 10*time.Millisecond)
	require.Zero(t, p.alice.wallet.leaseCount())

	// The channel works on as before.
	require.Equal(t, p.alice.ch.State().FundingOutpoint,
		p.alice.ch.State().CurrentFundingOutpoint())
}
