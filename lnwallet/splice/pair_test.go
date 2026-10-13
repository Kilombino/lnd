//go:build !dev && !integration

package splice

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"github.com/btcsuite/btcd/txscript"
	"github.com/lightningnetwork/lnd/lnwallet/interactivetx"
	"sync"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/chainntnfs"
	"github.com/lightningnetwork/lnd/channeldb"
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
	startReestablished(t, n.machine)
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

	// drop, if set, keeps a message from reaching the other side.
	dropMu sync.Mutex
	drop   func(from *node, msg lnwire.Message) bool
}

// dropped reports whether a message is kept from the other side.
func (p *splicePair) dropped(from *node, msg lnwire.Message) bool {
	p.dropMu.Lock()
	defer p.dropMu.Unlock()

	return p.drop != nil && p.drop(from, msg)
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
				if p.dropped(from, msg) {
					continue
				}
				to.machine.HandleSpliceMessage(msg)

			case sig := <-from.link.commits:
				if p.dropped(from, sig) {
					continue
				}
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

	t.Run("still catching up with the chain", func(t *testing.T) {
		t.Parallel()

		p := newSplicePair(t)
		p.alice.fund(t, 1, 1_000_000)
		p.chain.behind.Store(true)
		_, err := p.alice.machine.Splice(Request{
			Direction: DirectionIn, Amount: 500_000,
			FeeRate: 2_500,
		})
		require.ErrorIs(t, err, ErrRefused)
		require.ErrorContains(t, err, codeChainNotSynced)
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

		// To the close address itself, it goes ahead.
		updates, err := p.alice.machine.Splice(Request{
			Direction: DirectionOut, Amount: 1_000_000,
			FeeRate: 2_500, Destination: p2wpkh(5),
		})
		require.NoError(t, err)
		u := requireUpdates(t, updates, StatePreparing,
			StateNegotiating, StateSigned, StateBroadcast)
		p.alice.machine.mu.Lock()
		tx, err := p.alice.machine.signedTx()
		p.alice.machine.mu.Unlock()
		require.NoError(t, err)
		require.Equal(t, u.Txid, tx.TxHash())
		var paid bool
		for _, out := range tx.TxOut {
			paid = paid || (bytes.Equal(out.PkScript, p2wpkh(5)) &&
				out.Value == 1_000_000)
		}
		require.True(t, paid, "the close address isn't paid")
	})

	// Use maximum out: everything the channel lets go, the fee on top,
	// whatever the amount.
	t.Run("use maximum out", func(t *testing.T) {
		t.Parallel()

		p := newSplicePair(t)
		avail := p.alice.ch.AvailableBalance().ToSatoshis()
		updates, err := p.alice.machine.Splice(Request{
			Direction: DirectionOut, UseMax: true, Amount: 1,
			FeeRate: 2_500, Destination: p2tr(1),
		})
		require.NoError(t, err)
		requireUpdates(t, updates, StatePreparing,
			StateNegotiating, StateSigned, StateBroadcast)

		p.alice.machine.mu.Lock()
		tx, err := p.alice.machine.signedTx()
		p.alice.machine.mu.Unlock()
		require.NoError(t, err)
		placeholder := make([]byte, input.P2WSHSize)
		placeholder[0], placeholder[1] = 0x00, 0x20
		fee, _ := MoveOutFee(Shape{
			SharedWitnessWeight: input.MultiSigWitnessSize,
			FundingScript:       placeholder,
		}, p2tr(1), 2_500)
		var paid btcutil.Amount
		for _, out := range tx.TxOut {
			if bytes.Equal(out.PkScript, p2tr(1)) {
				paid = btcutil.Amount(out.Value)
			}
		}
		require.Equal(t, avail, paid+fee)
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

	// A channel grown since it opened: its own reserve is below 1% of
	// its capacity, which the construction keeps for a side adding an
	// output. Refused at once, not at tx_complete.
	t.Run("the reserve on the new capacity", func(t *testing.T) {
		t.Parallel()

		p := newSplicePair(t)
		p.alice.ch.State().Capacity = 400 * btcutil.SatoshiPerBitcoin
		_, err := p.alice.machine.Splice(Request{
			Direction: DirectionOut, Amount: 200_000_000,
			FeeRate: 2_500, Destination: p2tr(1),
		})
		require.ErrorContains(t, err, "above_max_out")

		p.alice.ch.State().Capacity = 2000 * btcutil.SatoshiPerBitcoin
		p.alice.fund(t, 1, 100_000_000)
		_, err = p.alice.machine.Splice(Request{
			Direction: DirectionIn, Amount: 50_000_000,
			FeeRate: 2_500,
		})
		require.ErrorContains(t, err, "below_min")
		require.Zero(t, p.alice.wallet.leaseCount())
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

		// Quiescence is the peer's to end, its splice_init coming:
		// not resumed under it.
		select {
		case <-p.alice.link.resumed:
			t.Fatal("quiescence ended under the peer")
		case <-time.After(200 * time.Millisecond):
		}

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

// TestSpliceInitiatorMempoolCheck checks a splice our backend's mempool
// refuses when we sign last: the peer signed it already and keeps it, so it
// isn't dropped (a tx_abort then would leave the two apart, and the Core
// Lightning port fails the channel on it). It goes on, both keeping it, the
// peer not marked; a splice that doesn't relay is bumped or dies as any
// stuck one.
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
	requireUpdates(t, updates,
		StatePreparing, StateNegotiating, StateSigned, StateBroadcast,
	)
	require.Len(t, p.alice.ch.PendingFundings(), 1)
	require.Eventually(t, func() bool {
		return len(p.bob.ch.PendingFundings()) == 1
	}, 5*time.Second, 10*time.Millisecond)

	done := make(chan bool)
	p.bob.machine.enqueue(func() { done <- p.bob.machine.misbehaved })
	require.False(t, <-done)
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

	require.Equal(t, RelayStatus{State: RelayAccepted},
		p.alice.machine.RelayStatus(u.Txid))
	require.Equal(t, RelayUnknown,
		p.alice.machine.RelayStatus(chainhash.Hash{1}).State)

	before := p.alice.wallet.publishCount()
	require.Eventually(t, func() bool {
		p.chain.mine()
		return p.alice.wallet.publishCount() >= before+2
	}, 5*time.Second, 20*time.Millisecond)

	// Refused at a later block: the status says so, with the reason.
	p.alice.wallet.mu.Lock()
	p.alice.wallet.publish = errors.New("min relay fee not met")
	p.alice.wallet.mu.Unlock()
	require.Eventually(t, func() bool {
		p.chain.mine()
		st := p.alice.machine.RelayStatus(u.Txid)
		return st.State == RelayRejected &&
			st.Reason == "min relay fee not met"
	}, 5*time.Second, 20*time.Millisecond)
	p.alice.wallet.mu.Lock()
	p.alice.wallet.publish = nil
	p.alice.wallet.mu.Unlock()

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
// spends: at 6 confirmations of that spend both sides record it as dead, the
// starter hears why, and both keep its funding in the channel's set, signed
// for, so neither side's commitment batch falls short of the other's (the
// Core Lightning port fails the channel on that); after a restart it is
// still dead and not followed; at 144 it leaves the starter's wallet and its
// coins are released.
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
			last, err := n.ch.State().LastSplice()
			return err == nil && last != nil &&
				last.Outcome == "dead"
		}, 5*time.Second, 10*time.Millisecond)
		require.Len(t, n.ch.PendingFundings(), 1)
		recs, err := n.ch.State().SpliceRecords()
		require.NoError(t, err)
		require.Len(t, recs, 1)
		require.True(t, recs[0].Negotiation.Dead)
		n.machine.mu.Lock()
		require.Nil(t, n.machine.active)
		n.machine.mu.Unlock()
		require.Equal(t, "pending", n.machine.spliceable())
	}

	// After a restart: still dead, nothing followed.
	restarted := NewMachine(p.bob.machine.cfg)
	startReestablished(t, restarted)
	t.Cleanup(restarted.Stop)
	restarted.mu.Lock()
	require.Nil(t, restarted.active)
	restarted.mu.Unlock()

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

// TestSpliceWithheldSignatures checks a peer whose tx_signatures don't
// reach us after ours went out, and that publishes the splice anyway: the
// side that signed first follows it to its lock without marking the peer
// (an honest peer cut off at the wrong moment looks the same), and takes
// the signatures if they come late.
func TestSpliceWithheldSignatures(t *testing.T) {
	t.Parallel()

	p := newSplicePair(t)

	// Alice adds funds: Bob put nothing in, so he signs first; Alice's
	// don't reach him.
	var withheld *lnwire.TxSignatures
	p.dropMu.Lock()
	p.drop = func(from *node, msg lnwire.Message) bool {
		sigs, ok := msg.(*lnwire.TxSignatures)
		if ok && from == p.alice {
			withheld = sigs
		}
		return ok && from == p.alice
	}
	p.dropMu.Unlock()

	p.alice.fund(t, 1, 40_000_000)
	updates, err := p.alice.machine.Splice(Request{
		Direction: DirectionIn, Amount: 20_000_000, FeeRate: 2_500,
	})
	require.NoError(t, err)
	u := requireUpdates(t, updates,
		StatePreparing, StateNegotiating, StateSigned, StateBroadcast,
	)

	p.bob.machine.mu.Lock()
	theirs := p.bob.machine.active.theirTxSigs
	p.bob.machine.mu.Unlock()
	require.Nil(t, theirs)

	// The splice confirms (Alice published it) and both lock.
	p.confirm(t, u.Txid)
	requireUpdates(t, updates, StateLocked)
	p.requireMoved(t, u.Txid, p.alice.ch.State().Capacity)
	require.NotEqual(t, codePeerMisbehaved, statusOf(p.bob.machine))

	// Late signatures are taken without a disconnection.
	p.dropMu.Lock()
	late := withheld
	p.dropMu.Unlock()
	require.NotNil(t, late)
	p.bob.machine.HandleSpliceMessage(late)
	time.Sleep(50 * time.Millisecond)
	p.bob.peer.mu.Lock()
	require.Nil(t, p.bob.peer.disconnected)
	p.bob.peer.mu.Unlock()
}

// statusOf asks a machine, on its loop, why it can't splice now.
func statusOf(m *Machine) string {
	code := make(chan string, 1)
	m.enqueue(func() { code <- m.spliceable() })

	return <-code
}

// TestSpliceCancelNegotiating checks a cancel while the transaction is
// being built: the peer gets a clean tx_abort, our coins are released and
// nothing stays in the channel's funding set.
func TestSpliceCancelNegotiating(t *testing.T) {
	t.Parallel()

	p := newSplicePair(t)
	p.alice.fund(t, 1, 5_000_000)

	// Bob's construction messages never reach Alice: she waits in the
	// negotiation. Her tx_abort is seen on its way.
	aborts := make(chan *lnwire.TxAbort, 1)
	p.dropMu.Lock()
	p.drop = func(from *node, msg lnwire.Message) bool {
		if abort, ok := msg.(*lnwire.TxAbort); ok && from == p.alice {
			aborts <- abort
		}
		switch msg.(type) {
		case *lnwire.TxAddInput, *lnwire.TxAddOutput, *lnwire.TxComplete:
			return from == p.bob
		}

		return false
	}
	p.dropMu.Unlock()

	updates, err := p.alice.machine.Splice(Request{
		Direction: DirectionIn, Amount: 1_000_000, FeeRate: 2_500,
	})
	require.NoError(t, err)
	requireUpdates(t, updates, StatePreparing, StateNegotiating)

	require.True(t, p.alice.machine.Cancel())
	u := requireUpdates(t, updates, StateAborted)
	require.Contains(t, u.Reason, "canceled")
	select {
	case <-aborts:
	case <-time.After(5 * time.Second):
		t.Fatal("no tx_abort sent")
	}
	require.Zero(t, p.alice.wallet.leaseCount())
	require.Empty(t, p.alice.ch.PendingFundings())
}

// TestSpliceOneAtATime checks that a second splice asked for while one is
// in progress is refused, not run beside it.
func TestSpliceOneAtATime(t *testing.T) {
	t.Parallel()

	p := newSplicePair(t)
	p.alice.fund(t, 1, 5_000_000)
	p.alice.fund(t, 2, 5_000_000)

	updates, err := p.alice.machine.Splice(Request{
		Direction: DirectionIn, Amount: 1_000_000, FeeRate: 2_500,
	})
	require.NoError(t, err)
	_, err = p.alice.machine.Splice(Request{
		Direction: DirectionIn, Amount: 1_000_000, FeeRate: 2_500,
	})
	require.ErrorIs(t, err, ErrRefused)
	require.ErrorContains(t, err, codePending)

	requireUpdates(t, updates, StatePreparing, StateNegotiating,
		StateSigned, StateBroadcast)
	_, err = p.alice.machine.Splice(Request{
		Direction: DirectionIn, Amount: 1_000_000, FeeRate: 2_500,
	})
	require.ErrorContains(t, err, codePending)
}

// wrongDigestSigner signs the shared input for the ordinary digest instead
// of the shared input's, as a misconfigured remote signer could.
type wrongDigestSigner struct {
	*input.MockSigner
}

func (s *wrongDigestSigner) SignOutputRaw(tx *wire.MsgTx,
	desc *input.SignDescriptor) (input.Signature, error) {

	if desc.HashType == interactivetx.SharedInputHashType {
		wrong := *desc
		wrong.HashType = txscript.SigHashAll
		desc = &wrong
	}

	return s.MockSigner.SignOutputRaw(tx, desc)
}

// TestSpliceOwnSharedSignatureChecked checks that our shared input
// signature is checked against the shared input's digest before our
// tx_signatures leave: a signer that signs another digest stops the splice
// while it can still be dropped.
func TestSpliceOwnSharedSignatureChecked(t *testing.T) {
	t.Parallel()

	p := newSplicePair(t)
	p.alice.fund(t, 1, 5_000_000)
	p.alice.machine.cfg.Signer = &wrongDigestSigner{p.alice.signer}

	updates, err := p.alice.machine.Splice(Request{
		Direction: DirectionIn, Amount: 1_000_000, FeeRate: 2_500,
	})
	require.NoError(t, err)
	u := requireUpdates(t, updates, StatePreparing, StateNegotiating,
		StateAborted)
	require.Contains(t, u.Reason, codeInternal)
	require.Empty(t, p.alice.wallet.publishedTxs())
	require.Zero(t, p.alice.wallet.leaseCount())
}

// TestSpliceWithheldRestart checks a restart of the side that locked a
// splice whose peer's tx_signatures never came: it comes back pending,
// naming the splice as its locked funding on reconnecting (so the peer
// gets splice_locked again), not asking for the splice's signatures.
func TestSpliceWithheldRestart(t *testing.T) {
	t.Parallel()

	p := newSplicePair(t)

	// Alice's signatures and her splice_locked don't reach Bob.
	p.dropMu.Lock()
	p.drop = func(from *node, msg lnwire.Message) bool {
		switch msg.(type) {
		case *lnwire.TxSignatures, *lnwire.SpliceLocked:
			return from == p.alice
		}

		return false
	}
	p.dropMu.Unlock()

	p.alice.fund(t, 1, 40_000_000)
	updates, err := p.alice.machine.Splice(Request{
		Direction: DirectionIn, Amount: 20_000_000, FeeRate: 2_500,
	})
	require.NoError(t, err)
	u := requireUpdates(t, updates,
		StatePreparing, StateNegotiating, StateSigned, StateBroadcast,
	)
	p.confirm(t, u.Txid)

	// Bob reached his depth and sent splice_locked; Alice's never came.
	require.Eventually(t, func() bool {
		recs, err := p.bob.ch.State().SpliceRecords()
		if err != nil || len(recs) == 0 {
			return false
		}
		n := recs[0].Negotiation
		return n != nil && n.LockedSent && n.TheirTxSignatures == nil
	}, 5*time.Second, 10*time.Millisecond)

	restarted := NewMachine(p.bob.machine.cfg)
	startReestablished(t, restarted)
	t.Cleanup(restarted.Stop)
	next, locked := restarted.ReestablishTLVs()
	require.True(t, next.IsNone(), "asked for the splice's signatures")
	require.Equal(t, u.Txid, locked.UnsafeFromSome().Txid)
}

// TestSpliceZeroBroadcastHeight checks a splice whose broadcast height was
// never recorded, as after a crash between publishing it and recording the
// height: a restart still watches it to its lock, from the current
// funding's confirmation. The notifier refuses a search from height zero,
// and the splice then never locked.
func TestSpliceZeroBroadcastHeight(t *testing.T) {
	t.Parallel()

	p := newSplicePair(t)
	p.alice.fund(t, 1, 5_000_000)
	updates, err := p.alice.machine.Splice(Request{
		Direction: DirectionIn, Amount: 1_000_000, FeeRate: 2_500,
	})
	require.NoError(t, err)
	u := requireUpdates(t, updates, StatePreparing, StateNegotiating,
		StateSigned, StateBroadcast)

	p.alice.machine.Stop()
	require.NoError(t, p.alice.ch.State().UpdateSpliceRecord(u.Txid,
		func(r *channeldb.SpliceRecord) error {
			r.Negotiation.BroadcastHeight = 0
			return nil
		}))
	restarted := NewMachine(p.alice.machine.cfg)
	startReestablished(t, restarted)
	t.Cleanup(restarted.Stop)

	// Alice's machine, its first registration and the restarted one's,
	// and Bob's.
	require.Eventually(t, func() bool {
		return p.chain.registered(u.Txid, 6, 3)
	}, 5*time.Second, 10*time.Millisecond)
}
