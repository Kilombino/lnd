//go:build !dev && !integration

package splice

import (
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/chainntnfs"
	"github.com/lightningnetwork/lnd/fn/v2"
	"github.com/lightningnetwork/lnd/input"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/lnwallet"
	"github.com/lightningnetwork/lnd/lnwallet/chainfee"
	"github.com/lightningnetwork/lnd/lnwallet/interactivetx"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/stretchr/testify/require"
)

// TestReplaceOurSplice checks the peer replacing a splice Alice started,
// adding funds of its own on top: Alice takes part as acceptor, putting in
// the same coin with its change at the new rate, and locks on the
// replacement with both amounts in.
func TestReplaceOurSplice(t *testing.T) {
	t.Parallel()

	p := newSplicePair(t)
	coinA := p.alice.fund(t, 1, 5_000_000)
	capacity := p.alice.ch.State().Capacity
	const amount = btcutil.Amount(1_000_000)

	updates, err := p.alice.machine.Splice(Request{
		Direction: DirectionIn, Amount: amount, FeeRate: 2_500,
	})
	require.NoError(t, err)
	requireUpdates(t, updates, StatePreparing, StateNegotiating,
		StateSigned, StateBroadcast)
	require.Len(t, p.bob.ch.PendingFundings(), 1)
	bobKey := p.bob.ch.PendingFundings()[0].LocalKey
	aliceKey := p.bob.ch.PendingFundings()[0].RemoteKey

	// From here Bob is played by hand: Alice's messages come to the
	// test, and Bob's quiescence started the replacement.
	fromAlice := make(chan lnwire.Message, 64)
	p.dropMu.Lock()
	p.drop = func(from *node, msg lnwire.Message) bool {
		if from == p.alice {
			fromAlice <- msg
			return true
		}

		return false
	}
	p.dropMu.Unlock()
	p.alice.link.mu.Lock()
	p.alice.link.initiator = lntypes.Remote
	p.alice.link.mu.Unlock()
	nextA := func() lnwire.Message {
		t.Helper()
		select {
		case m := <-fromAlice:
			return m
		case <-time.After(5 * time.Second):
			t.Fatal("Alice stopped answering")
			return nil
		}
	}

	// Bob adds 500,000 of his own on top, at twice the rate.
	const bobAmount = btcutil.Amount(500_000)
	const feeRate = chainfee.SatPerKWeight(5_000)
	p.alice.machine.HandleSpliceMessage(&lnwire.TxInitRbf{
		ChannelID:                 p.alice.machine.cfg.ChanID,
		LockTime:                  p.chain.height,
		FeeRatePerKw:              uint32(feeRate),
		FundingOutputContribution: fn.Some(bobAmount),
	})
	ack, isAck := nextA().(*lnwire.TxAckRbf)
	require.True(t, isAck)
	require.Equal(t, fn.Some(amount), ack.FundingOutputContribution)

	// Bob's construction: the shared input, the funding output with
	// both amounts, a coin of his and his change.
	state := p.bob.ch.State()
	_, oldOut, err := input.GenFundingPkScript(
		state.LocalChanCfg.MultiSigKey.PubKey.SerializeCompressed(),
		state.RemoteChanCfg.MultiSigKey.PubKey.SerializeCompressed(),
		int64(state.Capacity),
	)
	require.NoError(t, err)
	newCapacity := capacity + amount + bobAmount
	_, newOut, err := input.GenFundingPkScript(
		bobKey.PubKey.SerializeCompressed(),
		aliceKey.SerializeCompressed(), int64(newCapacity),
	)
	require.NoError(t, err)
	bob, err := interactivetx.NewSession(interactivetx.Config{
		Role:      interactivetx.Initiator,
		ChannelID: p.alice.machine.cfg.ChanID,
		Shared: interactivetx.SharedInput{
			OutPoint:      state.CurrentFundingOutpoint(),
			Value:         state.Capacity,
			PkScript:      oldOut.PkScript,
			WitnessWeight: input.MultiSigWitnessSize,
		},
		FundingScript:         newOut.PkScript,
		InitiatorContribution: bobAmount,
		AcceptorContribution:  amount,
		FeeRate:               feeRate,
		LockTime:              p.chain.height,
		CheckPeerInput:        func(*interactivetx.Input) error { return nil },
		CheckPeerOutput:       func(*interactivetx.Output) error { return nil },
		DustLimit:             dustLimit,
		WitnessWeight:         peerWitnessWeight,
		Reserve:               func(c btcutil.Amount) btcutil.Amount { return c / 100 },
		InitiatorBalance:      state.LocalCommitment.LocalBalance.ToSatoshis(),
		AcceptorBalance:       state.LocalCommitment.RemoteBalance.ToSatoshis(),
	})
	require.NoError(t, err)
	require.NoError(t, bob.AddSharedInputAndFunding())
	coinB := p.bob.fund(t, 2, 2_000_000)
	bob.AddLocalInput(interactivetx.Input{
		OutPoint: coinB.OutPoint, Value: coinB.Value,
		PkScript: coinB.PkScript, Sequence: interactivetx.MaxSequence,
		MaxWitnessWeight: input.P2WKHWitnessSize, PrevTx: coinB.PrevTx,
	})
	bob.AddLocalOutput(interactivetx.Output{
		Value: 2_000_000 - bobAmount - 10_000, PkScript: p2wpkh(0x44),
	})
	for !bob.Done() {
		if bob.OurTurn() {
			msg, err := bob.Next()
			require.NoError(t, err)
			p.alice.machine.HandleSpliceMessage(msg)

			continue
		}
		require.NoError(t, bob.Receive(nextA()))
	}
	res, err := bob.Build()
	require.NoError(t, err)
	txid := res.Tx.TxHash()

	// Alice's coin is in it, and a change of hers.
	var aliceIn bool
	for _, in := range res.Inputs {
		aliceIn = aliceIn || in.OutPoint == coinA.OutPoint
	}
	require.True(t, aliceIn, "Alice's coin isn't in the replacement")

	// The commitments on the replacement's funding.
	aliceCommit, ok := nextA().(*lnwire.CommitSig)
	require.True(t, ok)
	require.NoError(t, p.bob.ch.AddPendingFunding(lnwallet.PendingFunding{
		OutPoint: wire.OutPoint{Hash: txid, Index: res.FundingIndex},
		Capacity: newCapacity, LocalKey: bobKey, RemoteKey: aliceKey,
		LocalDelta: bobAmount, RemoteDelta: amount,
	}))
	require.NoError(t, p.bob.ch.ReceiveSpliceCommitment(txid,
		&lnwallet.CommitSigs{
			CommitSig: aliceCommit.CommitSig,
			HtlcSigs:  aliceCommit.HtlcSigs,
		}))
	bobSigs, err := p.bob.ch.SignSpliceCommitment(t.Context(), txid)
	require.NoError(t, err)
	p.alice.machine.SpliceCommitReceived(txid,
		p.alice.ch.ReceiveSpliceCommitment(txid, bobSigs))

	// Bob put in less: he signs first, his coin and the shared input.
	fetcher := interactivetx.PrevOutFetcher(res)
	sigHashes := txscript.NewTxSigHashes(res.Tx, fetcher)
	var witnesses []wire.TxWitness
	for i, in := range res.Inputs {
		if in.Shared || in.OutPoint != coinB.OutPoint {
			continue
		}
		script, err := p.bob.signer.ComputeInputScript(res.Tx,
			&input.SignDescriptor{
				Output: &wire.TxOut{
					Value: int64(in.Value), PkScript: in.PkScript,
				},
				HashType:          txscript.SigHashAll,
				SigHashes:         sigHashes,
				PrevOutputFetcher: fetcher,
				InputIndex:        i,
			})
		require.NoError(t, err)
		witnesses = append(witnesses, script.Witness)
	}
	witnessScript, _, err := input.GenFundingPkScript(
		state.LocalChanCfg.MultiSigKey.PubKey.SerializeCompressed(),
		state.RemoteChanCfg.MultiSigKey.PubKey.SerializeCompressed(),
		int64(state.Capacity),
	)
	require.NoError(t, err)
	shared := res.Inputs[res.SharedIndex]
	raw, err := p.bob.signer.SignOutputRaw(res.Tx, &input.SignDescriptor{
		KeyDesc:       state.LocalChanCfg.MultiSigKey,
		WitnessScript: witnessScript,
		Output: &wire.TxOut{
			Value: int64(shared.Value), PkScript: shared.PkScript,
		},
		HashType:          interactivetx.SharedInputHashType,
		SigHashes:         sigHashes,
		PrevOutputFetcher: fetcher,
		InputIndex:        int(res.SharedIndex),
	})
	require.NoError(t, err)
	sig, err := lnwire.NewSigFromSignature(raw)
	require.NoError(t, err)
	p.alice.machine.HandleSpliceMessage(&lnwire.TxSignatures{
		ChannelID: p.alice.machine.cfg.ChanID, TxID: txid,
		Witnesses: witnesses, SharedInputSignature: fn.Some(sig),
	})

	// Alice's, and the complete replacement published and valid.
	aliceSigs, ok := nextA().(*lnwire.TxSignatures)
	require.True(t, ok)
	require.Len(t, aliceSigs.Witnesses, 1)
	require.Eventually(t, func() bool {
		_, ok := p.alice.wallet.publishedTxs()[txid]
		return ok
	}, 5*time.Second, 10*time.Millisecond)
	p.alice.machine.mu.Lock()
	tx, err := p.alice.machine.signedTx()
	p.alice.machine.mu.Unlock()
	require.NoError(t, err)
	requireValid(t, tx, res)
	require.Len(t, p.alice.ch.PendingFundings(), 2)

	// The replacement confirms; Alice locks on it with both amounts.
	// Bob's coins in it lock deeper.
	depth := uint32(6)
	p.chain.confirm(t, txid, depth, 1, &chainntnfs.TxConfirmation{
		BlockHeight: 900_006, TxIndex: 2,
	})
	locked, ok := nextA().(*lnwire.SpliceLocked)
	require.True(t, ok)
	require.Equal(t, txid, locked.SpliceTxid)
	p.alice.machine.HandleSpliceMessage(&lnwire.SpliceLocked{
		ChannelID: p.alice.machine.cfg.ChanID, SpliceTxid: txid,
	})
	require.Eventually(t, func() bool {
		return p.alice.ch.State().CurrentFundingOutpoint().Hash == txid
	}, 5*time.Second, 20*time.Millisecond)
	require.Equal(t, newCapacity, p.alice.ch.State().Capacity)

	// The coins are released just after the lock.
	require.Eventually(t, func() bool {
		return p.alice.wallet.leaseCount() == 0
	}, 5*time.Second, 10*time.Millisecond)

	// The user's stream follows the splice to its replacement: signed,
	// published, locked.
	u := requireUpdates(t, updates, StateSigned, StateBroadcast,
		StateLocked)
	require.Equal(t, txid, u.Txid)

	// Still Alice's resize, with her amount.
	require.Eventually(t, func() bool {
		last, err := p.alice.ch.State().LastSplice()
		return err == nil && last != nil && last.Txid == txid
	}, 5*time.Second, 10*time.Millisecond)
	last, err := p.alice.ch.State().LastSplice()
	require.NoError(t, err)
	require.True(t, last.WeInitiated)
	require.Equal(t, amount, last.LocalDelta)
	require.Equal(t, bobAmount, last.RemoteDelta)
}

// TestOurPart checks what this node puts into a replacement of its own
// splice: moving out, the same destination and amount, the output's fee
// at the new rate from our side; adding funds, the same coins with the
// change left at the new rate, dropped when dust, refused when the coins
// can't pay for themselves.
func TestOurPart(t *testing.T) {
	t.Parallel()

	m := &Machine{cfg: Config{
		DustLimit: dustLimit,
		Channel:   &balanceOnly{available: 1_000_000_000},
	}}
	funding := p2wsh(1)

	// Moving out 300,000 to an address.
	dest := p2tr(7)
	tx := wire.NewMsgTx(2)
	tx.AddTxOut(&wire.TxOut{Value: 2_000_000, PkScript: funding})
	tx.AddTxOut(&wire.TxOut{Value: 300_000, PkScript: dest})
	out := &active{
		role: interactivetx.Initiator, fundingIndex: 0,
		ourContribution: -300_600,
		res: &interactivetx.Result{Tx: tx, Inputs: []*interactivetx.Input{
			{Shared: true, SerialID: 0},
		}},
	}
	c, coins, outs, err := m.ourPart(out, 2_000)
	require.NoError(t, err)
	require.Empty(t, coins)
	require.Equal(t, []interactivetx.Output{{Value: 300_000, PkScript: dest}},
		outs)
	fee := chainfee.SatPerKWeight(2_000).FeeForWeight(
		lntypes.WeightUnit(interactivetx.OutputWeight(dest)),
	)
	require.Equal(t, -(300_000 + fee), c)

	// More than the channel lets go: nothing available beyond the
	// move-out being replaced, and the replacement's output fee at
	// 10,000 sat/kw above the 600 its fee took: refused before the peer
	// hears.
	m.cfg.Channel = &balanceOnly{available: 0}
	_, _, _, err = m.ourPart(out, 10_000)
	require.ErrorContains(t, err, "available")

	// The same at 2,000 sat/kw fits in what the move-out took.
	_, _, _, err = m.ourPart(out, 2_000)
	require.NoError(t, err)
	m.cfg.Channel = &balanceOnly{available: 1_000_000_000}

	// Adding 1,000,000 from a coin of 1,500,000, with change.
	wallet := newFakeWallet()
	m.cfg.CoinSource = wallet
	in := coin(5, p2wpkh(5), 1_500_000)
	wallet.txs[in.OutPoint.Hash] = in.PrevTx
	change := p2wpkh(6)
	addTx := func(changeValue int64) *active {
		tx := wire.NewMsgTx(2)
		tx.AddTxOut(&wire.TxOut{Value: 3_000_000, PkScript: funding})
		if changeValue > 0 {
			tx.AddTxOut(&wire.TxOut{
				Value: changeValue, PkScript: change,
			})
		}
		return &active{
			role: interactivetx.Initiator, fundingIndex: 0,
			ourContribution: 1_000_000,
			res: &interactivetx.Result{Tx: tx,
				Inputs: []*interactivetx.Input{
					{Shared: true, SerialID: 0},
					{SerialID: 2, OutPoint: in.OutPoint,
						Value: in.Value, PkScript: in.PkScript},
				}},
		}
	}
	weight := interactivetx.InputWeight(input.P2WKHWitnessSize) +
		peerInputMargin
	withChange := chainfee.SatPerKWeight(5_000).FeeForWeight(
		lntypes.WeightUnit(weight + interactivetx.OutputWeight(change)),
	)
	c, coins, outs, err = m.ourPart(addTx(499_000), 5_000)
	require.NoError(t, err)
	require.Equal(t, btcutil.Amount(1_000_000), c)
	require.Len(t, coins, 1)
	require.Equal(t, in.PrevTx, coins[0].PrevTx)
	require.Equal(t, []interactivetx.Output{{
		Value: 500_000 - withChange, PkScript: change,
	}}, outs)

	// Change that would be dust at the new rate goes to the fee.
	in.Value = 1_000_400
	wallet.txs[in.OutPoint.Hash] = in.PrevTx
	a := addTx(300)
	a.res.Inputs[1].Value = in.Value
	_, _, outs, err = m.ourPart(a, 1_000)
	require.NoError(t, err)
	require.Empty(t, outs)

	// No change in the splice: a fresh change address of the wallet's.
	fresh := p2wpkh(0x55)
	m.cfg.ChangeScript = func() ([]byte, error) { return fresh, nil }
	a = addTx(0)
	a.res.Inputs[1].Value = 1_500_000
	in.Value = 1_500_000
	_, _, outs, err = m.ourPart(a, 5_000)
	require.NoError(t, err)
	require.Len(t, outs, 1)
	require.Equal(t, fresh, outs[0].PkScript)
	m.cfg.ChangeScript = nil

	// A coin that can't pay for itself at the new rate.
	a.res.Inputs[1].Value = 1_000_010
	_, _, _, err = m.ourPart(a, 50_000)
	require.Error(t, err)
}

// p2wsh returns a P2WSH script made unique by tag.
func p2wsh(tag byte) []byte {
	return append([]byte{0x00, 0x20}, make32(tag)...)
}

func make32(tag byte) []byte {
	b := make([]byte, 32)
	b[0] = tag

	return b
}

// TestReplaceOurSpliceAgain checks a second replacement of Alice's splice
// after a restart: the restarted machine still knows the splice is hers,
// takes no place in the limits for it, and puts her part in again; a rate
// far above hers is refused.
func TestReplaceOurSpliceAgain(t *testing.T) {
	t.Parallel()

	p := newSplicePair(t)
	coinA := p.alice.fund(t, 1, 5_000_000)
	const amount = btcutil.Amount(1_000_000)
	updates, err := p.alice.machine.Splice(Request{
		Direction: DirectionIn, Amount: amount, FeeRate: 2_500,
	})
	require.NoError(t, err)
	requireUpdates(t, updates, StatePreparing, StateNegotiating,
		StateSigned, StateBroadcast)

	fromAlice := make(chan lnwire.Message, 64)
	p.dropMu.Lock()
	p.drop = func(from *node, msg lnwire.Message) bool {
		if from == p.alice {
			fromAlice <- msg
			return true
		}

		return false
	}
	p.dropMu.Unlock()
	p.alice.link.mu.Lock()
	p.alice.link.initiator = lntypes.Remote
	p.alice.link.mu.Unlock()

	// A restart, with limits that count the peer's splices.
	limits := NewLimits()
	cfg := p.alice.machine.cfg
	cfg.Limits = limits
	restarted := NewMachine(cfg)
	startReestablished(t, restarted)
	t.Cleanup(restarted.Stop)
	p.alice.machine.Stop()
	p.alice.machine = restarted
	limits.mu.Lock()
	require.Empty(t, limits.pending, "our splice took a peer's place")
	limits.mu.Unlock()

	rbf := func(rate chainfee.SatPerKWeight) lnwire.Message {
		restarted.HandleSpliceMessage(&lnwire.TxInitRbf{
			ChannelID:                 restarted.cfg.ChanID,
			LockTime:                  p.chain.height,
			FeeRatePerKw:              uint32(rate),
			FundingOutputContribution: fn.Some(btcutil.Amount(0)),
		})
		select {
		case m := <-fromAlice:
			return m
		case <-time.After(5 * time.Second):
			t.Fatal("Alice stopped answering")
			return nil
		}
	}

	// Far above her rate and the node's estimate: refused.
	abort, ok := rbf(1_000_000).(*lnwire.TxAbort)
	require.True(t, ok)
	require.Contains(t, string(abort.Data), codeFeeBelowFloor)
	select {
	case <-p.alice.link.resumed:
	case <-time.After(5 * time.Second):
		t.Fatal("quiescence not ended")
	}

	// At a rate she'd pay: her amount, and her coin re-added.
	ack, ok := rbf(5_000).(*lnwire.TxAckRbf)
	require.True(t, ok)
	require.Equal(t, fn.Some(amount), ack.FundingOutputContribution)
	restarted.mu.Lock()
	b := restarted.active
	restarted.mu.Unlock()
	require.True(t, b.oursReplaced)
	require.Len(t, b.ourInputs, 1)
	require.Equal(t, coinA.OutPoint, b.ourInputs[0].OutPoint)
	require.Len(t, b.ourOutputs, 1)
}

// balanceOnly is a channel that only says what we can send.
type balanceOnly struct {
	Channel

	available lnwire.MilliSatoshi
}

func (b *balanceOnly) AvailableBalance() lnwire.MilliSatoshi {
	return b.available
}

// TestReplaceOurLargeMoveOut checks that the peer's replacement of our
// move-out of most of our balance is joined: the move-out already taken on
// the splice being replaced isn't counted against it a second time.
func TestReplaceOurLargeMoveOut(t *testing.T) {
	t.Parallel()

	p := newSplicePair(t)
	amount := p.alice.ch.AvailableBalance().ToSatoshis() * 6 / 10
	updates, err := p.alice.machine.Splice(Request{
		Direction: DirectionOut, Amount: amount, FeeRate: 2_500,
		Destination: p2tr(9),
	})
	require.NoError(t, err)
	requireUpdates(t, updates, StatePreparing, StateNegotiating,
		StateSigned, StateBroadcast)

	fromAlice := make(chan lnwire.Message, 64)
	p.dropMu.Lock()
	p.drop = func(from *node, msg lnwire.Message) bool {
		if from == p.alice {
			fromAlice <- msg
			return true
		}

		return false
	}
	p.dropMu.Unlock()
	p.alice.link.mu.Lock()
	p.alice.link.initiator = lntypes.Remote
	p.alice.link.mu.Unlock()

	p.alice.machine.HandleSpliceMessage(&lnwire.TxInitRbf{
		ChannelID:                 p.alice.machine.cfg.ChanID,
		LockTime:                  p.chain.height,
		FeeRatePerKw:              3_000,
		FundingOutputContribution: fn.Some(btcutil.Amount(-5_000)),
	})
	select {
	case msg := <-fromAlice:
		abort, refused := msg.(*lnwire.TxAbort)
		if refused {
			t.Fatalf("replacement refused: %s", abort.Data)
		}
		require.IsType(t, &lnwire.TxAckRbf{}, msg)

	case <-time.After(5 * time.Second):
		t.Fatal("no answer")
	}
}
