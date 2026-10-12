package splice

import (
	"bytes"
	"errors"
	"testing"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/input"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/lnwallet/chainfee"
	"github.com/lightningnetwork/lnd/lnwallet/interactivetx"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

var (
	testFundingScript = append([]byte{0x00, 0x20}, bytes.Repeat(
		[]byte{0xfa}, 32,
	)...)
	testOldFundingScript = append([]byte{0x00, 0x20}, bytes.Repeat(
		[]byte{0xfb}, 32,
	)...)
	testShape = Shape{
		SharedWitnessWeight: input.MultiSigWitnessSize,
		FundingScript:       testFundingScript,
	}
)

func p2wpkh(tag byte) []byte {
	return append([]byte{0x00, 0x14}, bytes.Repeat([]byte{tag}, 20)...)
}

func p2tr(tag byte) []byte {
	return append([]byte{0x51, 0x20}, bytes.Repeat([]byte{tag}, 32)...)
}

func dustLimit(pkScript []byte) btcutil.Amount {
	if len(pkScript) == 22 {
		return 294
	}

	return 330
}

// coin returns a confirmed wallet coin of value paying to script, made
// unique by tag.
func coin(tag uint16, script []byte, value btcutil.Amount) Coin {
	prev := wire.NewMsgTx(2)
	prev.AddTxIn(&wire.TxIn{PreviousOutPoint: wire.OutPoint{
		Hash: chainhash.Hash{byte(tag), byte(tag >> 8), 0xcc},
	}})
	prev.AddTxOut(&wire.TxOut{Value: int64(value), PkScript: script})

	return Coin{
		OutPoint: wire.OutPoint{Hash: prev.TxHash()},
		Value:    value, PkScript: script, PrevTx: prev,
	}
}

// coinbaseCoin returns a coin from a coinbase transaction.
func coinbaseCoin(value btcutil.Amount) Coin {
	prev := wire.NewMsgTx(2)
	prev.AddTxIn(&wire.TxIn{PreviousOutPoint: wire.OutPoint{
		Index: wire.MaxPrevOutIndex,
	}})
	prev.AddTxOut(&wire.TxOut{Value: int64(value), PkScript: p2wpkh(9)})

	return Coin{
		OutPoint: wire.OutPoint{Hash: prev.TxHash()},
		Value:    value, PkScript: p2wpkh(9), PrevTx: prev,
	}
}

// engineConfig is a construction of a splice of a 1,000,000 sat channel
// with the given contribution from the initiator.
func engineConfig(role interactivetx.Role, contribution btcutil.Amount,
	feeRate chainfee.SatPerKWeight) interactivetx.Config {

	return interactivetx.Config{
		Role:      role,
		ChannelID: lnwire.ChannelID{1},
		Shared: interactivetx.SharedInput{
			OutPoint: wire.OutPoint{Hash: chainhash.Hash{0xf0}},
			Value:    1_000_000, PkScript: testOldFundingScript,
			WitnessWeight: input.MultiSigWitnessSize,
		},
		FundingScript:         testFundingScript,
		InitiatorContribution: contribution,
		FeeRate:               feeRate,
		LockTime:              900_000,
		WitnessWeight: func(pkScript []byte) (int64, error) {
			w, ok := CoinWitnessWeight(pkScript)
			if !ok {
				return 0, errors.New("not accepted")
			}

			return w, nil
		},
		Reserve: func(c btcutil.Amount) btcutil.Amount {
			return c / 100
		},
		CheckPeerInput:   func(*interactivetx.Input) error { return nil },
		CheckPeerOutput:  func(*interactivetx.Output) error { return nil },
		DustLimit:        dustLimit,
		InitiatorBalance: 900_000,
		AcceptorBalance:  100_000,
	}
}

// negotiate runs a construction where the initiator adds the given coins
// and outputs, and returns both sides' results.
func negotiate(t require.TestingT, contribution btcutil.Amount,
	feeRate chainfee.SatPerKWeight, coins []Coin,
	outputs []interactivetx.Output) (*interactivetx.Result,
	*interactivetx.Result, error) {

	init, err := interactivetx.NewSession(engineConfig(
		interactivetx.Initiator, contribution, feeRate,
	))
	require.NoError(t, err)
	acc, err := interactivetx.NewSession(engineConfig(
		interactivetx.Acceptor, contribution, feeRate,
	))
	require.NoError(t, err)

	require.NoError(t, init.AddSharedInputAndFunding())
	for _, c := range coins {
		w, ok := CoinWitnessWeight(c.PkScript)
		require.True(t, ok)
		init.AddLocalInput(interactivetx.Input{
			OutPoint: c.OutPoint, Value: c.Value,
			PkScript: c.PkScript, PrevTx: c.PrevTx,
			Sequence: interactivetx.MaxSequence, MaxWitnessWeight: w,
		})
	}
	for _, o := range outputs {
		init.AddLocalOutput(o)
	}

	for i := 0; !(init.Done() && acc.Done()); i++ {
		if i > 2_000 {
			return nil, nil, errors.New("no end")
		}
		from, to := init, acc
		if !init.OurTurn() {
			from, to = acc, init
		}
		msg, err := from.Next()
		if err != nil {
			return nil, nil, err
		}
		if err := to.Receive(msg); err != nil {
			return nil, nil, err
		}
	}

	ri, err := init.Build()
	if err != nil {
		return nil, nil, err
	}
	ra, err := acc.Build()

	return ri, ra, err
}

// TestSelectAddFundsAgreesWithEngine checks, over random wallets, amounts
// and feerates, that a selection is a splice both sides' engines accept,
// with the weight and fee the selection computed.
func TestSelectAddFundsAgreesWithEngine(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.IntRange(0, 12).Draw(rt, "coins")
		var coins []Coin
		for i := 0; i < n; i++ {
			script := p2wpkh(byte(i))
			if rapid.Bool().Draw(rt, "taproot") {
				script = p2tr(byte(i))
			}
			coins = append(coins, coin(uint16(i), script,
				btcutil.Amount(rapid.Int64Range(
					300, 3_000_000,
				).Draw(rt, "value"))))
		}
		amount := btcutil.Amount(
			rapid.Int64Range(1_000, 5_000_000).Draw(rt, "amount"),
		)
		useMax := rapid.Bool().Draw(rt, "useMax")
		feeRate := chainfee.SatPerKWeight(
			rapid.Int64Range(253, 100_000).Draw(rt, "feeRate"),
		)

		sel, err := SelectAddFunds(
			testShape, coins, amount, useMax, feeRate, p2tr(0xee),
			dustLimit,
		)
		if err != nil {
			require.ErrorIs(rt, err, ErrInsufficientFunds)

			return
		}

		if !useMax {
			require.Equal(rt, amount, sel.Contribution)
		}
		var outputs []interactivetx.Output
		if sel.Change > 0 {
			require.GreaterOrEqual(rt, sel.Change,
				dustLimit(p2tr(0xee)))
			outputs = append(outputs, interactivetx.Output{
				Value: sel.Change, PkScript: p2tr(0xee),
			})
		}
		var in btcutil.Amount
		for _, c := range sel.Inputs {
			in += c.Value
		}
		require.Equal(rt, in, sel.Contribution+sel.Change+sel.Fee)
		require.GreaterOrEqual(rt, sel.Fee,
			feeRate.FeeForWeight(lntypesWeight(sel.Weight)))

		ri, ra, err := negotiate(
			rt, sel.Contribution, feeRate, sel.Inputs, outputs,
		)
		require.NoError(rt, err)
		// The selection's weight adds the margin for a peer that
		// overcounts each input.
		require.Equal(rt, sel.Weight,
			ri.Weight+int64(len(ri.Tx.TxIn))*peerInputMargin)
		require.Equal(rt, sel.Fee, ri.Fee)
		require.Equal(rt, ri.Tx.TxHash(), ra.Tx.TxHash())
		require.Equal(rt, int64(1_000_000+sel.Contribution),
			ri.Tx.TxOut[ri.FundingIndex].Value)
	})
}

// TestSelectAddFunds checks the selection's choices: largest coins first,
// unusable coins skipped, change only above dust, Use maximum spending
// everything.
func TestSelectAddFunds(t *testing.T) {
	t.Parallel()

	const feeRate = chainfee.SatPerKWeight(2_500)
	change := p2tr(0xee)

	big := coin(1, p2wpkh(1), 500_000)
	small := coin(2, p2tr(2), 120_000)
	wsh := coin(3, append([]byte{0x00, 0x20}, make([]byte, 32)...),
		900_000)
	nested := coin(4, append([]byte{0xa9, 0x14}, append(
		make([]byte, 20), 0x87)...), 900_000)
	cb := coinbaseCoin(900_000)
	all := []Coin{small, wsh, nested, cb, big}

	// Largest first; one coin is enough.
	sel, err := SelectAddFunds(
		testShape, all, 300_000, false, feeRate, change, dustLimit,
	)
	require.NoError(t, err)
	require.Equal(t, []Coin{big}, sel.Inputs)
	require.Positive(t, sel.Change)

	// Two needed; the unusable coins, worth more, are never picked.
	sel, err = SelectAddFunds(
		testShape, all, 550_000, false, feeRate, change, dustLimit,
	)
	require.NoError(t, err)
	require.Equal(t, []Coin{big, small}, sel.Inputs)

	// More than the usable coins.
	_, err = SelectAddFunds(
		testShape, all, 650_000, false, feeRate, change, dustLimit,
	)
	require.ErrorIs(t, err, ErrInsufficientFunds)

	// Use maximum: every usable coin, no change, all after the fee.
	sel, err = SelectAddFunds(
		testShape, all, 0, true, feeRate, change, dustLimit,
	)
	require.NoError(t, err)
	require.ElementsMatch(t, []Coin{big, small}, sel.Inputs)
	require.Zero(t, sel.Change)
	require.Equal(t, big.Value+small.Value-sel.Fee, sel.Contribution)

	// Leftover below dust after paying for a change output goes to the
	// fee instead.
	feeNoChange, _ := func() (btcutil.Amount, int64) {
		w := testShape.weight([]int64{input.P2WKHWitnessSize}, nil)
		return fee(feeRate, w), w
	}()
	amount := big.Value - feeNoChange - 200
	sel, err = SelectAddFunds(
		testShape, []Coin{big}, amount, false, feeRate, change,
		dustLimit,
	)
	require.NoError(t, err)
	require.Zero(t, sel.Change)
	require.Equal(t, feeNoChange+200, sel.Fee)
	_, _, err = negotiate(t, sel.Contribution, feeRate, sel.Inputs, nil)
	require.NoError(t, err)

	// The same when a change output could be paid for but would be
	// below dust: 100 sats left after its fee.
	changeFee := fee(feeRate, interactivetx.OutputWeight(change))
	amount = big.Value - feeNoChange - changeFee - 100
	sel, err = SelectAddFunds(
		testShape, []Coin{big}, amount, false, feeRate, change,
		dustLimit,
	)
	require.NoError(t, err)
	require.Zero(t, sel.Change)
	require.Equal(t, big.Value-amount, sel.Fee)

	// And exactly at dust, there is change.
	amount = big.Value - feeNoChange - changeFee - dustLimit(change)
	sel, err = SelectAddFunds(
		testShape, []Coin{big}, amount, false, feeRate, change,
		dustLimit,
	)
	require.NoError(t, err)
	require.Equal(t, dustLimit(change), sel.Change)

	// Nothing to add.
	_, err = SelectAddFunds(
		testShape, all, 0, false, feeRate, change, dustLimit,
	)
	require.ErrorIs(t, err, ErrBelowDust)
	_, err = SelectAddFunds(
		testShape, []Coin{cb}, 0, true, feeRate, change, dustLimit,
	)
	require.ErrorIs(t, err, ErrInsufficientFunds)
}

// TestMoveOut checks a splice-out's fee and contribution against the
// engine.
func TestMoveOut(t *testing.T) {
	t.Parallel()

	for _, dest := range [][]byte{p2wpkh(5), p2tr(5)} {
		for _, feeRate := range []chainfee.SatPerKWeight{
			253, 2_500, 50_000,
		} {
			out, err := NewMoveOut(
				testShape, 250_000, dest, feeRate, dustLimit,
			)
			require.NoError(t, err)
			require.Equal(t, -(250_000 + out.Fee), out.Contribution)

			ri, _, err := negotiate(
				t, out.Contribution, feeRate, nil,
				[]interactivetx.Output{{
					Value: 250_000, PkScript: dest,
				}},
			)
			require.NoError(t, err)
			require.Equal(t, out.Weight,
				ri.Weight+peerInputMargin)
			require.Equal(t, out.Fee, ri.Fee)
		}
	}

	_, err := NewMoveOut(testShape, 293, p2wpkh(5), 253, dustLimit)
	require.ErrorIs(t, err, ErrBelowDust)
}

func lntypesWeight(w int64) lntypes.WeightUnit {
	return lntypes.WeightUnit(w)
}
