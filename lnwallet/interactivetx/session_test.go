package interactivetx

import (
	"bytes"
	"errors"
	"testing"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/fn/v2"
	"github.com/lightningnetwork/lnd/input"
	"github.com/lightningnetwork/lnd/lnwallet/chainfee"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

var (
	sharedOutPoint = wire.OutPoint{Hash: chainhash.Hash{0xf0}, Index: 1}
	fundingScript  = append([]byte{0x00, 0x20}, bytes.Repeat(
		[]byte{0xfa}, 32,
	)...)
	oldFundingScript = append([]byte{0x00, 0x20}, bytes.Repeat(
		[]byte{0xfb}, 32,
	)...)
)

func p2wpkh(tag byte) []byte {
	return append([]byte{0x00, 0x14}, bytes.Repeat([]byte{tag}, 20)...)
}

func p2tr(tag byte) []byte {
	return append([]byte{0x51, 0x20}, bytes.Repeat([]byte{tag}, 32)...)
}

func p2wsh(tag byte) []byte {
	return append([]byte{0x00, 0x20}, bytes.Repeat([]byte{tag}, 32)...)
}

// prevTx returns a transaction paying value to script at output 0, made
// unique by tag.
func prevTx(tag byte, script []byte, value btcutil.Amount) *wire.MsgTx {
	tx := wire.NewMsgTx(2)
	tx.AddTxIn(&wire.TxIn{
		PreviousOutPoint: wire.OutPoint{Hash: chainhash.Hash{tag}},
	})
	tx.AddTxOut(&wire.TxOut{Value: int64(value), PkScript: script})

	return tx
}

// witnessWeight accepts P2WPKH and P2TR key path inputs only.
func witnessWeight(pkScript []byte) (int64, error) {
	switch {
	case len(pkScript) == 22 && pkScript[0] == 0x00:
		return input.P2WKHWitnessSize, nil
	case len(pkScript) == 34 && pkScript[0] == 0x51:
		return 1 + 1 + 65, nil
	default:
		return 0, errors.New("only P2WPKH and P2TR key path inputs")
	}
}

type params struct {
	capacity      btcutil.Amount
	initBalance   btcutil.Amount
	accBalance    btcutil.Amount
	initContrib   btcutil.Amount
	accContrib    btcutil.Amount
	feeRate       chainfee.SatPerKWeight
	lockTime      uint32
	maxPeerAddMsg int
}

func defaultParams() params {
	return params{
		capacity:    1_000_000,
		initBalance: 600_000,
		accBalance:  400_000,
		feeRate:     1000,
		lockTime:    900_000,
	}
}

func config(role Role, p params) Config {
	return Config{
		Role: role,
		Shared: SharedInput{
			OutPoint:      sharedOutPoint,
			Value:         p.capacity,
			PkScript:      oldFundingScript,
			WitnessWeight: input.MultiSigWitnessSize,
		},
		FundingScript:         fundingScript,
		InitiatorContribution: p.initContrib,
		AcceptorContribution:  p.accContrib,
		FeeRate:               p.feeRate,
		LockTime:              p.lockTime,
		MaxPeerAddMessages:    p.maxPeerAddMsg,
		WitnessWeight:         witnessWeight,
		Reserve: func(c btcutil.Amount) btcutil.Amount {
			return c / 100
		},
		InitiatorBalance: p.initBalance,
		AcceptorBalance:  p.accBalance,
	}
}

// pair returns an initiator and an acceptor session.
func pair(t *testing.T, p params) (*Session, *Session) {
	t.Helper()

	init, err := NewSession(config(Initiator, p))
	require.NoError(t, err)
	acc, err := NewSession(config(Acceptor, p))
	require.NoError(t, err)
	require.NoError(t, init.AddSharedInputAndFunding())

	return init, acc
}

// run exchanges messages until both are done or one fails.
func run(a, b *Session) error {
	for i := 0; i < 10_000; i++ {
		if a.Done() && b.Done() {
			return nil
		}

		from, to := a, b
		if !a.OurTurn() {
			from, to = b, a
		}
		msg, err := from.Next()
		if err != nil {
			return err
		}
		if err := to.Receive(msg); err != nil {
			return err
		}
	}

	return errors.New("no end in sight")
}

func requireAbort(t *testing.T, err error, contains string) {
	t.Helper()

	var abort *AbortError
	require.ErrorAs(t, err, &abort)
	require.Contains(t, abort.Reason, contains)
}

func TestSpliceInByInitiator(t *testing.T) {
	t.Parallel()

	p := defaultParams()
	p.initContrib = 500_000
	init, acc := pair(t, p)

	wallet := prevTx(1, p2wpkh(1), 600_000)
	init.AddLocalInput(Input{
		OutPoint:         wire.OutPoint{Hash: wallet.TxHash()},
		Value:            600_000,
		PkScript:         p2wpkh(1),
		Sequence:         MaxSequence,
		MaxWitnessWeight: input.P2WKHWitnessSize,
		PrevTx:           wallet,
	})
	init.AddLocalOutput(Output{Value: 95_000, PkScript: p2wpkh(2)})

	require.NoError(t, run(init, acc))

	ri, err := init.Build()
	require.NoError(t, err)
	ra, err := acc.Build()
	require.NoError(t, err)

	// Both sides build the same transaction.
	require.Equal(t, ri.Tx.TxHash(), ra.Tx.TxHash())
	require.Equal(t, btcutil.Amount(1_500_000),
		btcutil.Amount(ra.Tx.TxOut[ra.FundingIndex].Value))
	require.Equal(t, btcutil.Amount(5_000), ra.Fee)
	require.GreaterOrEqual(t, int64(ra.Fee),
		int64(p.feeRate.FeeForWeight(0))+ra.Weight*int64(p.feeRate)/1000)
	require.Equal(t, uint32(p.lockTime), ra.Tx.LockTime)

	// Inputs and outputs are in serial id order: the shared input and the
	// funding output first, as the initiator queued them.
	require.True(t, ra.Inputs[0].Shared)
	require.Equal(t, sharedOutPoint, ra.Tx.TxIn[0].PreviousOutPoint)
	require.True(t, ra.Outputs[0].Funding)
}

func TestSpliceOutByInitiator(t *testing.T) {
	t.Parallel()

	// The amount entered arrives in full; the initiator's contribution
	// covers it and the fee.
	p := defaultParams()
	const fee = 3_000
	p.initContrib = -(300_000 + fee)
	init, acc := pair(t, p)
	init.AddLocalOutput(Output{Value: 300_000, PkScript: p2tr(9)})

	require.NoError(t, run(init, acc))
	res, err := acc.Build()
	require.NoError(t, err)
	require.Equal(t, btcutil.Amount(fee), res.Fee)
	require.Equal(t, btcutil.Amount(1_000_000-303_000),
		btcutil.Amount(res.Tx.TxOut[res.FundingIndex].Value))
}

func TestAcceptorAddsNothing(t *testing.T) {
	t.Parallel()

	// A peer's splice in which this node, the acceptor, contributes
	// nothing: it pays nothing and signs first.
	p := defaultParams()
	p.initContrib = 200_000
	init, acc := pair(t, p)
	wallet := prevTx(3, p2tr(3), 250_000)
	init.AddLocalInput(Input{
		OutPoint:         wire.OutPoint{Hash: wallet.TxHash()},
		Value:            250_000,
		PkScript:         p2tr(3),
		Sequence:         MaxSequence,
		MaxWitnessWeight: 67,
		PrevTx:           wallet,
	})

	require.NoError(t, run(init, acc))
	res, err := acc.Build()
	require.NoError(t, err)

	require.True(t, SignsFirst(res, Acceptor, []byte{9}, []byte{1}))
	require.False(t, SignsFirst(res, Initiator, []byte{1}, []byte{9}))
}

// The refusals below run the peer's messages straight into a session.

// acceptorFor returns an acceptor session that has had its turn to start:
// the initiator sends first.
func acceptorFor(t *testing.T, p params) *Session {
	t.Helper()

	acc, err := NewSession(config(Acceptor, p))
	require.NoError(t, err)

	return acc
}

func sharedAdd(serial uint64) *lnwire.TxAddInput {
	return &lnwire.TxAddInput{
		SerialID:        serial,
		PrevTxVout:      sharedOutPoint.Index,
		Sequence:        MaxSequence,
		SharedInputTxid: fn.Some(sharedOutPoint.Hash),
	}
}

func walletAdd(t *testing.T, serial uint64, tx *wire.MsgTx,
	vout uint32) *lnwire.TxAddInput {

	t.Helper()

	var b bytes.Buffer
	require.NoError(t, tx.Serialize(&b))

	return &lnwire.TxAddInput{
		SerialID:   serial,
		PrevTx:     b.Bytes(),
		PrevTxVout: vout,
		Sequence:   MaxSequence,
	}
}

// exchange feeds msgs to s as the peer's, answering each with s's own next
// message, and returns the first error.
func exchange(s *Session, msgs ...lnwire.Message) error {
	for _, m := range msgs {
		if err := s.Receive(m); err != nil {
			return err
		}
		if s.OurTurn() {
			if _, err := s.Next(); err != nil {
				return err
			}
		}
	}

	return nil
}

func TestPeerInputRefusals(t *testing.T) {
	t.Parallel()

	wallet := prevTx(4, p2wpkh(4), 100_000)
	legacy := prevTx(5, []byte{0x76, 0xa9, 0x14}, 100_000)
	scriptHash := prevTx(6, p2wsh(6), 100_000)

	cases := []struct {
		name     string
		msgs     func(t *testing.T) []lnwire.Message
		contains string
	}{{
		name: "odd serial from the initiator",
		msgs: func(t *testing.T) []lnwire.Message {
			return []lnwire.Message{walletAdd(t, 1, wallet, 0)}
		},
		contains: "parity",
	}, {
		name: "serial id reused",
		msgs: func(t *testing.T) []lnwire.Message {
			return []lnwire.Message{
				walletAdd(t, 2, wallet, 0),
				&lnwire.TxAddOutput{SerialID: 2, Amount: 1,
					Script: p2wpkh(7)},
			}
		},
		contains: "already used",
	}, {
		name: "same outpoint twice",
		msgs: func(t *testing.T) []lnwire.Message {
			return []lnwire.Message{
				walletAdd(t, 2, wallet, 0),
				walletAdd(t, 4, wallet, 0),
			}
		},
		contains: "already added",
	}, {
		name: "the funding output as a plain input",
		msgs: func(t *testing.T) []lnwire.Message {
			// A prevtx whose hash is the funding txid can't be
			// forged, so check by outpoint: the shared input added
			// twice.
			return []lnwire.Message{sharedAdd(0), sharedAdd(2)}
		},
		contains: "already added",
	}, {
		name: "the shared input with a prevtx",
		msgs: func(t *testing.T) []lnwire.Message {
			m := sharedAdd(0)
			m.PrevTx = []byte{0x01}

			return []lnwire.Message{m}
		},
		contains: "carries a prevtx",
	}, {
		name: "a shared input that isn't the funding output",
		msgs: func(t *testing.T) []lnwire.Message {
			m := sharedAdd(0)
			m.PrevTxVout = 7

			return []lnwire.Message{m}
		},
		contains: "is not the funding output",
	}, {
		name: "a non-segwit input",
		msgs: func(t *testing.T) []lnwire.Message {
			return []lnwire.Message{walletAdd(t, 2, legacy, 0)}
		},
		contains: "non-segwit",
	}, {
		name: "a P2WSH input, whose witness size is unknowable",
		msgs: func(t *testing.T) []lnwire.Message {
			return []lnwire.Message{
				walletAdd(t, 2, scriptHash, 0),
			}
		},
		contains: "type refused",
	}, {
		name: "a final sequence",
		msgs: func(t *testing.T) []lnwire.Message {
			m := walletAdd(t, 2, wallet, 0)
			m.Sequence = 0xffffffff

			return []lnwire.Message{m}
		},
		contains: "final",
	}, {
		name: "prevtx_vout out of range",
		msgs: func(t *testing.T) []lnwire.Message {
			return []lnwire.Message{walletAdd(t, 2, wallet, 3)}
		},
		contains: "out of range",
	}, {
		name: "a prevtx that doesn't parse",
		msgs: func(t *testing.T) []lnwire.Message {
			m := walletAdd(t, 2, wallet, 0)
			m.PrevTx = m.PrevTx[:10]

			return []lnwire.Message{m}
		},
		contains: "doesn't parse",
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			acc := acceptorFor(t, defaultParams())
			requireAbort(t, exchange(acc, tc.msgs(t)...),
				tc.contains)
		})
	}
}

func TestSharedInputFromAcceptor(t *testing.T) {
	t.Parallel()

	// This node is the initiator; the peer, the acceptor, tries to add
	// the shared input.
	p := defaultParams()
	init, err := NewSession(config(Initiator, p))
	require.NoError(t, err)
	_, err = init.Next() // our tx_complete, nothing queued
	require.NoError(t, err)

	requireAbort(t, init.Receive(sharedAdd(1)), "acceptor added the shared")
}

func TestPeerPolicyHooks(t *testing.T) {
	t.Parallel()

	wallet := prevTx(8, p2wpkh(8), 100_000)

	p := defaultParams()
	cfg := config(Acceptor, p)
	var seen *Input
	cfg.CheckPeerInput = func(in *Input) error {
		seen = in

		return errors.New("a coinbase")
	}
	acc, err := NewSession(cfg)
	require.NoError(t, err)
	requireAbort(t, acc.Receive(walletAdd(t, 2, wallet, 0)), "a coinbase")

	// The hook sees what was parsed from the prevtx.
	require.Equal(t, wire.OutPoint{Hash: wallet.TxHash()}, seen.OutPoint)
	require.Equal(t, btcutil.Amount(100_000), seen.Value)

	cfg = config(Acceptor, p)
	cfg.CheckPeerOutput = func(out *Output) error {
		return errors.New("an OP_RETURN")
	}
	acc, err = NewSession(cfg)
	require.NoError(t, err)
	requireAbort(t, acc.Receive(&lnwire.TxAddOutput{
		SerialID: 2, Amount: 0, Script: []byte{0x6a},
	}), "an OP_RETURN")
}

func TestPeerAddLimit(t *testing.T) {
	t.Parallel()

	p := defaultParams()
	p.maxPeerAddMsg = 2
	acc := acceptorFor(t, p)

	msgs := []lnwire.Message{
		&lnwire.TxAddOutput{SerialID: 2, Amount: 1000,
			Script: p2wpkh(1)},
		&lnwire.TxAddOutput{SerialID: 4, Amount: 1000,
			Script: p2wpkh(2)},
		&lnwire.TxAddOutput{SerialID: 6, Amount: 1000,
			Script: p2wpkh(3)},
	}
	requireAbort(t, exchange(acc, msgs...), "more than 2 tx_add_output")
}

func TestTurnTaking(t *testing.T) {
	t.Parallel()

	// The initiator sends first: the acceptor can't speak first, and a
	// peer can't send twice in a row.
	acc := acceptorFor(t, defaultParams())
	_, err := acc.Next()
	require.ErrorIs(t, err, ErrNotOurTurn)

	require.NoError(t, acc.Receive(sharedAdd(0)))
	requireAbort(t, acc.Receive(&lnwire.TxComplete{}), "out of turn")
}

func TestRemoveRules(t *testing.T) {
	t.Parallel()

	wallet := prevTx(9, p2wpkh(9), 100_000)

	acc := acceptorFor(t, defaultParams())
	requireAbort(t, exchange(acc,
		walletAdd(t, 2, wallet, 0),
		&lnwire.TxRemoveInput{SerialID: 4},
	), "no input")

	// The peer can't remove what this node added.
	acc = acceptorFor(t, defaultParams())
	requireAbort(t, exchange(acc,
		&lnwire.TxRemoveOutput{SerialID: 1},
	), "isn't the peer's")

	// A removed input frees its outpoint and doesn't count at the end.
	p := defaultParams()
	init, acc2 := pair(t, p)
	_, err := init.Next() // shared input
	require.NoError(t, err)
	require.NoError(t, exchange(acc2,
		sharedAdd(0),
		walletAdd(t, 2, wallet, 0),
		&lnwire.TxRemoveInput{SerialID: 2},
		walletAdd(t, 4, wallet, 0),
	))
}

func TestAcceptorAddsFundingOutput(t *testing.T) {
	t.Parallel()

	p := defaultParams()
	init, err := NewSession(config(Initiator, p))
	require.NoError(t, err)
	_, err = init.Next()
	require.NoError(t, err)

	requireAbort(t, init.Receive(&lnwire.TxAddOutput{
		SerialID: 1, Amount: 1, Script: fundingScript,
	}), "acceptor added the funding output")
}

// TestBuildRefusals covers what's only known once both sides are done.
func TestBuildRefusals(t *testing.T) {
	t.Parallel()

	t.Run("the peer pays itself from our balance", func(t *testing.T) {
		// The initiator adds an output to itself with no inputs and
		// no contribution: the shared input's value would pay for it
		// out of the channel.
		p := defaultParams()
		init, acc := pair(t, p)
		init.AddLocalOutput(Output{Value: 300_000, PkScript: p2wpkh(1)})
		require.NoError(t, run(init, acc))
		_, err := acc.Build()
		requireAbort(t, err, "don't cover its fee")
	})

	t.Run("the funding output's value is wrong", func(t *testing.T) {
		// The initiator claims a bigger contribution than it told
		// the acceptor.
		p := defaultParams()
		pi := p
		pi.initContrib = 100_000
		init, err := NewSession(config(Initiator, pi))
		require.NoError(t, err)
		require.NoError(t, init.AddSharedInputAndFunding())
		acc := acceptorFor(t, p)
		require.NoError(t, run(init, acc))
		_, err = acc.Build()
		requireAbort(t, err, "funding output of")
	})

	t.Run("an output below the reserve", func(t *testing.T) {
		// The initiator moves almost its whole balance out to an
		// output: what it keeps is under 1% of the new capacity.
		p := defaultParams()
		p.initContrib = -(598_000 + 2_000)
		init, acc := pair(t, p)
		init.AddLocalOutput(Output{Value: 598_000, PkScript: p2wpkh(1)})
		require.NoError(t, run(init, acc))
		_, err := acc.Build()
		requireAbort(t, err, "below the reserve")
	})

	t.Run("a contribution below the balance", func(t *testing.T) {
		p := defaultParams()
		p.initContrib = -700_000
		init, acc := pair(t, p)
		init.AddLocalOutput(Output{Value: 690_000, PkScript: p2wpkh(1)})
		require.NoError(t, run(init, acc))
		_, err := acc.Build()
		requireAbort(t, err, "below zero")
	})

	t.Run("shaped like a commitment", func(t *testing.T) {
		p := defaultParams()
		p.lockTime = 0x20000001
		p.initContrib = -2_000
		init, err := NewSession(config(Initiator, p))
		require.NoError(t, err)
		acc := acceptorFor(t, p)

		// The initiator's shared input with a state hint sequence.
		m := sharedAdd(0)
		m.Sequence = 0x80000001
		require.NoError(t, acc.Receive(m))
		_, err = acc.Next()
		require.NoError(t, err)
		require.NoError(t, acc.Receive(&lnwire.TxAddOutput{
			SerialID: 2, Amount: 998_000, Script: fundingScript,
		}))
		_, err = acc.Next()
		require.NoError(t, err)
		require.NoError(t, acc.Receive(&lnwire.TxComplete{}))
		_ = init

		_, err = acc.Build()
		requireAbort(t, err, "shaped like a commitment")
	})

	t.Run("two funding outputs", func(t *testing.T) {
		p := defaultParams()
		init, acc := pair(t, p)
		init.AddLocalOutput(Output{Value: 1, PkScript: fundingScript})
		require.NoError(t, run(init, acc))
		_, err := acc.Build()
		requireAbort(t, err, "2 funding outputs")
	})

	t.Run("no shared input", func(t *testing.T) {
		p := defaultParams()
		acc := acceptorFor(t, p)
		require.NoError(t, acc.Receive(&lnwire.TxAddOutput{
			SerialID: 0, Amount: 1_000_000, Script: fundingScript,
		}))
		_, err := acc.Next()
		require.NoError(t, err)
		require.NoError(t, acc.Receive(&lnwire.TxComplete{}))
		_, err = acc.Build()
		requireAbort(t, err, "0 shared inputs")
	})

	t.Run("over the standard weight", func(t *testing.T) {
		p := defaultParams()
		p.initContrib = -50_000
		init, acc := pair(t, p)
		for i := 0; i < 8; i++ {
			init.AddLocalOutput(Output{
				Value:    1,
				PkScript: bytes.Repeat([]byte{0x6a}, 15_000),
			})
		}
		require.NoError(t, run(init, acc))
		_, err := acc.Build()
		requireAbort(t, err, "exceeds 400000")
	})
}

// TestHonestSplicesProperty runs random honest splices from both sides and
// checks that both agree, that the funding output is the old capacity plus
// both contributions, and that the fee covers the feerate.
func TestHonestSplicesProperty(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(rt *rapid.T) {
		p := defaultParams()
		p.feeRate = chainfee.SatPerKWeight(
			rapid.IntRange(253, 20_000).Draw(rt, "feeRate"),
		)

		// The initiator either adds funds (an input and change) or
		// moves funds out (an output, paid for by its contribution).
		spliceIn := rapid.Bool().Draw(rt, "spliceIn")
		amt := btcutil.Amount(rapid.IntRange(10_000, 300_000).Draw(
			rt, "amount",
		))
		const feeBudget = 60_000

		if spliceIn {
			p.initContrib = amt
		} else {
			p.initContrib = -(amt + feeBudget)
		}

		init, err := NewSession(config(Initiator, p))
		require.NoError(rt, err)
		acc, err := NewSession(config(Acceptor, p))
		require.NoError(rt, err)
		require.NoError(rt, init.AddSharedInputAndFunding())

		if spliceIn {
			wallet := prevTx(1, p2wpkh(1), amt+feeBudget+20_000)
			init.AddLocalInput(Input{
				OutPoint: wire.OutPoint{Hash: wallet.TxHash()},
				Value:    amt + feeBudget + 20_000,
				PkScript: p2wpkh(1), Sequence: MaxSequence,
				MaxWitnessWeight: input.P2WKHWitnessSize,
				PrevTx:           wallet,
			})
			init.AddLocalOutput(Output{
				Value: 20_000, PkScript: p2wpkh(2),
			})
		} else {
			init.AddLocalOutput(Output{
				Value: amt, PkScript: p2tr(3),
			})
		}

		require.NoError(rt, run(init, acc))
		ri, err := init.Build()
		require.NoError(rt, err)
		ra, err := acc.Build()
		require.NoError(rt, err)

		require.Equal(rt, ri.Tx.TxHash(), ra.Tx.TxHash())
		require.Equal(rt, p.capacity+p.initContrib,
			btcutil.Amount(ra.Tx.TxOut[ra.FundingIndex].Value))
		require.GreaterOrEqual(rt, int64(ra.Fee),
			int64(p.feeRate)*ra.Weight/1000)
	})
}

func TestAcceptorPaysForItsOwn(t *testing.T) {
	t.Parallel()

	// An acceptor that contributes pays for its own input and change at
	// the feerate; one that leaves too little for that is refused by the
	// initiator.
	for _, tc := range []struct {
		name   string
		change btcutil.Amount
		ok     bool
	}{
		{"enough left for its fee", 49_000, true},
		{"not enough left for its fee", 49_990, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The initiator adds no inputs: it pays its share
			// (common fields, shared input, funding output) out of
			// its channel balance.
			p := defaultParams()
			p.initContrib = -2_000
			p.accContrib = 50_000
			init, acc := pair(t, p)

			wallet := prevTx(7, p2wpkh(7), 100_000)
			acc.AddLocalInput(Input{
				OutPoint: wire.OutPoint{Hash: wallet.TxHash()},
				Value:    100_000, PkScript: p2wpkh(7),
				Sequence:         MaxSequence,
				MaxWitnessWeight: input.P2WKHWitnessSize,
				PrevTx:           wallet,
			})
			acc.AddLocalOutput(Output{
				Value: tc.change, PkScript: p2wpkh(8),
			})

			require.NoError(t, run(init, acc))
			_, err := init.Build()
			if tc.ok {
				require.NoError(t, err)
				return
			}
			requireAbort(t, err, "acceptor's inputs")
		})
	}
}

func TestSignsFirstTieBreak(t *testing.T) {
	t.Parallel()

	// Equal contributions: the lower node id signs first.
	res := &Result{Inputs: []*Input{
		{SerialID: 0, Value: 100, Shared: true},
		{SerialID: 1, Value: 100},
	}}
	require.True(t, SignsFirst(res, Initiator, []byte{2}, []byte{3}))
	require.False(t, SignsFirst(res, Acceptor, []byte{3}, []byte{2}))
}

// FuzzSessionReceive feeds arbitrary peer messages to an acceptor session:
// it must never panic, and anything it builds must still satisfy the rules.
func FuzzSessionReceive(f *testing.F) {
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		acc, err := NewSession(config(Acceptor, defaultParams()))
		require.NoError(t, err)

		r := bytes.NewReader(data)
		for r.Len() > 0 {
			msg, err := lnwire.ReadMessage(r, 0)
			if err != nil {
				return
			}
			if acc.Receive(msg) != nil {
				return
			}
			if acc.OurTurn() {
				if _, err := acc.Next(); err != nil {
					return
				}
			}
			if acc.Done() {
				res, err := acc.Build()
				if err == nil {
					require.Len(t, res.Tx.TxIn,
						len(res.Inputs))
					require.LessOrEqual(t, res.Weight,
						int64(MaxStandardWeight))
				}

				return
			}
		}
	})
}
