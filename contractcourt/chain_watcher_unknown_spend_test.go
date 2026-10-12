package contractcourt

import (
	"bytes"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/mempool"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/chainntnfs"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/clock"
	lnmock "github.com/lightningnetwork/lnd/lntest/mock"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/lnwallet"
	"github.com/stretchr/testify/require"
)

// TestIsCommitmentShaped checks the commitment shape against real
// commitments and against the transactions it must tell apart from them.
func TestIsCommitmentShaped(t *testing.T) {
	t.Parallel()

	alice, bob, err := lnwallet.CreateTestChannels(
		t, channeldb.SingleFunderTweaklessBit,
	)
	require.NoError(t, err)

	// Both sides' real commitments.
	require.True(t, isCommitmentShaped(
		alice.State().LocalCommitment.CommitTx,
	))
	require.True(t, isCommitmentShaped(
		bob.State().LocalCommitment.CommitTx,
	))

	// A state hint stays within its markers for any state number the
	// obfuscator can produce, the extremes included.
	var obfuscator [lnwallet.StateHintSize]byte
	for i := range obfuscator {
		obfuscator[i] = 0xff
	}
	for _, stateNum := range []uint64{0, 1, 1<<24 - 1, 1 << 24,
		1<<48 - 1} {

		tx := wire.NewMsgTx(2)
		tx.AddTxIn(&wire.TxIn{})
		require.NoError(t, lnwallet.SetStateNumHint(
			tx, stateNum, obfuscator,
		))
		require.True(t, isCommitmentShaped(tx), "state %d", stateNum)
	}

	fundingOutpoint := alice.State().FundingOutpoint
	other := wire.OutPoint{Hash: chainhash.Hash{1}, Index: 7}

	cases := []struct {
		name string
		tx   *wire.MsgTx
	}{{
		name: "cooperative close, final sequence",
		tx: &wire.MsgTx{TxIn: []*wire.TxIn{{
			PreviousOutPoint: fundingOutpoint,
			Sequence:         wire.MaxTxInSequenceNum,
		}}},
	}, {
		name: "cooperative close, RBF sequence",
		tx: &wire.MsgTx{TxIn: []*wire.TxIn{{
			PreviousOutPoint: fundingOutpoint,
			Sequence:         mempool.MaxRBFSequence,
		}}},
	}, {
		name: "two inputs with commitment markers",
		tx: &wire.MsgTx{
			TxIn: []*wire.TxIn{{
				PreviousOutPoint: fundingOutpoint,
				Sequence:         0x80000001,
			}, {
				PreviousOutPoint: other,
				Sequence:         0x80000001,
			}},
			LockTime: 0x20000001,
		},
	}, {
		name: "sequence marker without the locktime marker",
		tx: &wire.MsgTx{TxIn: []*wire.TxIn{{
			PreviousOutPoint: fundingOutpoint,
			Sequence:         0x80000001,
		}}},
	}, {
		name: "no inputs",
		tx:   &wire.MsgTx{},
	}}
	for _, tc := range cases {
		require.False(t, isCommitmentShaped(tc.tx), tc.name)
	}
}

// spliceShapedTx builds a transaction spending the funding output the way a
// splice does: an RBF sequence on every input, a wallet input of the peer's
// first when sharedFirst is false.
func spliceShapedTx(h *chainWatcherTestHarness, sharedFirst bool,
	value int64) *wire.MsgTx {

	shared := &wire.TxIn{
		PreviousOutPoint: h.aliceChannel.State().FundingOutpoint,
		Sequence:         mempool.MaxRBFSequence,
	}
	wallet := &wire.TxIn{
		PreviousOutPoint: wire.OutPoint{
			Hash: chainhash.Hash{0xaa}, Index: 3,
		},
		Sequence: mempool.MaxRBFSequence,
	}

	tx := &wire.MsgTx{
		Version: 2,
		TxOut: []*wire.TxOut{{
			Value:    value,
			PkScript: []byte{0x00, 0x20, byte(value % 255)},
		}},
	}
	if sharedFirst {
		tx.TxIn = []*wire.TxIn{shared, wallet}
	} else {
		tx.TxIn = []*wire.TxIn{wallet, shared}
	}

	return tx
}

// assertNoCloseEvents checks that the chain watcher reports no close of any
// kind within the timeout.
func assertNoCloseEvents(t *testing.T, h *chainWatcherTestHarness,
	timeout time.Duration) {

	t.Helper()

	select {
	case <-h.chanEvents.CooperativeClosure:
		t.Fatalf("unexpected cooperative close")
	case <-h.chanEvents.RemoteUnilateralClosure:
		t.Fatalf("unexpected remote unilateral close")
	case <-h.chanEvents.LocalUnilateralClosure:
		t.Fatalf("unexpected local unilateral close")
	case <-h.chanEvents.ContractBreach:
		t.Fatalf("unexpected breach")
	case <-time.After(timeout):
	}
}

// waitForStatus waits until the channel carries the given status flag.
func waitForStatus(t *testing.T, ch *channeldb.OpenChannel,
	status channeldb.ChannelStatus) {

	t.Helper()

	require.Eventually(t, func() bool {
		return ch.HasChanStatus(status)
	}, 5*time.Second, 10*time.Millisecond)
}

// TestChainWatcherUnknownSpendFlagged checks that a spend of the funding
// output that is neither a commitment nor a cooperative close this node
// negotiated is never dispatched as a close: no close event, no early
// cooperative close event, no close height, and the channel is flagged
// instead. The shared input is tried at index 0 and at index 1, since the
// peer chooses the input order in a splice.
func TestChainWatcherUnknownSpendFlagged(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		build func(h *chainWatcherTestHarness) *wire.MsgTx
	}{{
		name: "cooperative close sequence, no shutdown negotiated",
		build: func(h *chainWatcherTestHarness) *wire.MsgTx {
			return h.createUnnegotiatedSpendTx(90_000)
		},
	}, {
		name: "splice shaped, shared input first",
		build: func(h *chainWatcherTestHarness) *wire.MsgTx {
			return spliceShapedTx(h, true, 91_000)
		},
	}, {
		name: "splice shaped, shared input second",
		build: func(h *chainWatcherTestHarness) *wire.MsgTx {
			return spliceShapedTx(h, false, 92_000)
		},
	}, {
		// A shutdown on disk doesn't make a spend with more than one
		// input a cooperative close: a close spends the funding
		// output alone.
		name: "two inputs after a shutdown was sent",
		build: func(h *chainWatcherTestHarness) *wire.MsgTx {
			_ = h.createCoopCloseTx(1)

			return spliceShapedTx(h, true, 97_000)
		},
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newChainWatcherTestHarness(
				t, withRequiredConfs(3),
				withEarlyCoopCloseCapture(),
			)
			chanState := h.aliceChannel.State()
			tx := tc.build(h)

			h.sendSpend(tx)
			h.waitForConfRegistration()

			// The link comes down at once, before any
			// confirmation: payments must not keep flowing over a
			// channel whose funds may have moved.
			require.Positive(t, h.linkDownCalls.Load())

			// No early cooperative close event, and no close
			// height recorded on first detection.
			require.Zero(t, h.earlyCoopCloseCount())
			require.True(t, chanState.CloseConfirmationHeight.
				IsNone())

			h.mineBlocks(3)
			h.confirmTx(tx, h.currentHeight)

			waitForStatus(
				t, chanState,
				channeldb.ChanStatusFundingSpentUnknown,
			)
			assertNoCloseEvents(t, h, 300*time.Millisecond)
			require.Zero(t, h.earlyCoopCloseCount())
		})
	}
}

// TestChainWatcherUnknownSpendSingleConf checks the single confirmation
// path, which dispatches without the confirmation wait.
func TestChainWatcherUnknownSpendSingleConf(t *testing.T) {
	t.Parallel()

	h := newChainWatcherTestHarness(t, withRequiredConfs(1))
	chanState := h.aliceChannel.State()

	h.sendSpend(spliceShapedTx(h, false, 93_000))

	waitForStatus(t, chanState, channeldb.ChanStatusFundingSpentUnknown)
	assertNoCloseEvents(t, h, 300*time.Millisecond)
}

// TestChainWatcherUnknownSpendReorgedOut checks that a spend reorged out
// before its confirmations doesn't leave the channel flagged.
func TestChainWatcherUnknownSpendReorgedOut(t *testing.T) {
	t.Parallel()

	h := newChainWatcherTestHarness(t, withRequiredConfs(3))
	chanState := h.aliceChannel.State()
	tx := spliceShapedTx(h, true, 94_000)

	h.sendSpend(tx)
	h.waitForConfRegistration()
	h.triggerReorg(tx, 1)
	h.waitForSpendRegistration()

	assertNoCloseEvents(t, h, 300*time.Millisecond)
	require.False(t, chanState.HasChanStatus(
		channeldb.ChanStatusFundingSpentUnknown,
	))
}

// TestChainWatcherCoopCloseWithShutdown checks that a cooperative close is
// still recognised for every way this node can know it negotiated one: a
// persisted shutdown (the normal case) and the cooperative close broadcast
// status alone. A channel restored from a static backup has neither: see
// TestChainWatcherRestoredCoopClose.
func TestChainWatcherCoopCloseWithShutdown(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		prepare func(t *testing.T, h *chainWatcherTestHarness,
			tx *wire.MsgTx)
	}{{
		name: "shutdown persisted",
		prepare: func(t *testing.T, h *chainWatcherTestHarness,
			_ *wire.MsgTx) {

			require.NoError(t, h.aliceChannel.State().
				MarkShutdownSent(channeldb.NewShutdownInfo(
					[]byte{0x00, 0x14}, false,
				)))
		},
	}, {
		name: "cooperative close broadcast status only",
		prepare: func(t *testing.T, h *chainWatcherTestHarness,
			tx *wire.MsgTx) {

			require.NoError(t, h.aliceChannel.State().
				MarkCoopBroadcasted(tx, lntypes.Remote))
		},
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newChainWatcherTestHarness(t, withRequiredConfs(3))
			tx := h.createUnnegotiatedSpendTx(95_000)
			tc.prepare(t, h, tx)

			h.sendSpend(tx)
			h.waitForConfRegistration()
			h.mineBlocks(3)
			h.confirmTx(tx, h.currentHeight)

			closeInfo := h.waitForCoopClose(5 * time.Second)
			require.Equal(
				t, tx.TxHash(), closeInfo.ClosingTXID,
			)
			require.Zero(t, h.linkDownCalls.Load())
			require.False(t, h.aliceChannel.State().HasChanStatus(
				channeldb.ChanStatusFundingSpentUnknown,
			))
		})
	}
}

// TestChainWatcherForceCloseStillDetected checks that commitments keep
// their existing paths: the classification only takes over spends that are
// not shaped like a commitment.
func TestChainWatcherForceCloseStillDetected(t *testing.T) {
	t.Parallel()

	h := newChainWatcherTestHarness(t, withRequiredConfs(3))
	tx := h.createRemoteForceCloseTx()

	h.sendSpend(tx)
	h.waitForConfRegistration()
	h.mineBlocks(3)
	h.confirmTx(tx, h.currentHeight)

	h.waitForRemoteUnilateralClose(5 * time.Second)
	require.False(t, h.aliceChannel.State().HasChanStatus(
		channeldb.ChanStatusFundingSpentUnknown,
	))
}

// TestChainWatcherUnknownSpendRecheckedAtStart checks that a restarted
// watcher, which sees the historical spend again, flags the channel the same
// way without side effects, and that a channel flagged earlier stays
// flagged.
func TestChainWatcherUnknownSpendRecheckedAtStart(t *testing.T) {
	t.Parallel()

	h := newChainWatcherTestHarness(t, withRequiredConfs(1))
	chanState := h.aliceChannel.State()
	tx := spliceShapedTx(h, true, 96_000)

	h.sendSpend(tx)
	waitForStatus(t, chanState, channeldb.ChanStatusFundingSpentUnknown)

	// The same spend again, as after a restart.
	h.sendSpend(tx)
	assertNoCloseEvents(t, h, 300*time.Millisecond)
	require.True(t, chanState.HasChanStatus(
		channeldb.ChanStatusFundingSpentUnknown,
	))
	require.True(t, chanState.CloseConfirmationHeight.IsNone())
}

// TestArbChannelRefusesForceCloseAfterUnknownSpend checks that the
// arbitrator won't build and broadcast a commitment for a channel whose
// funding output was spent by an unrecognised transaction: it would spend an
// output that no longer exists, forever.
func TestArbChannelRefusesForceCloseAfterUnknownSpend(t *testing.T) {
	t.Parallel()

	db := channeldb.OpenForTesting(t, t.TempDir())

	lChannel, _, err := lnwallet.CreateTestChannels(
		t, channeldb.SingleFunderTweaklessBit,
	)
	require.NoError(t, err)

	channel := lChannel.State()
	channel.Db = db.ChannelStateDB()
	require.NoError(t, channel.SyncPending(
		&net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 18556}, 101,
	))
	require.NoError(t, channel.ApplyChanStatus(
		channeldb.ChanStatusFundingSpentUnknown,
	))

	chainArb := NewChainArbitrator(ChainArbitratorConfig{
		ChainIO: &lnmock.ChainIO{},
		Notifier: &lnmock.ChainNotifier{
			SpendChan: make(chan *chainntnfs.SpendDetail),
			ConfChan:  make(chan *chainntnfs.TxConfirmation),
		},
		MarkLinkInactive: func(wire.OutPoint) error { return nil },
		Clock:            clock.NewDefaultClock(),
		Budget:           *DefaultBudgetConfig(),
	}, db)

	arb := &arbChannel{channel: channel, c: chainArb}
	closeTx, err := arb.ForceCloseChan()
	require.Nil(t, closeTx)
	require.True(t, errors.Is(err, ErrFundingSpentUnknown), err)
}

// TestChainWatcherRestoredCoopClose checks a restored channel takes a spend
// of its funding output alone with a close's sequence for a close when it
// pays our wallet and no P2WSH script; with a P2WSH output, the shape of a
// splice's new funding output, or paying us nothing, it is flagged instead.
func TestChainWatcherRestoredCoopClose(t *testing.T) {
	t.Parallel()

	ourScript := append([]byte{0x00, 0x14}, make([]byte, 20)...)
	p2wsh := append([]byte{0x00, 0x20}, make([]byte, 32)...)
	other := append([]byte{0x00, 0x14}, bytes.Repeat([]byte{1}, 20)...)

	cases := []struct {
		name    string
		outputs [][]byte
		close   bool
	}{
		{"pays us", [][]byte{ourScript, other}, true},
		{"a splice", [][]byte{ourScript, p2wsh}, false},
		{"pays us nothing", [][]byte{other}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newChainWatcherTestHarness(
				t, withRequiredConfs(1),
			)
			alice := h.aliceChannel.State()
			require.NoError(t, alice.ApplyChanStatus(
				channeldb.ChanStatusRestored,
			))
			h.chainWatcher.cfg.isOurAddr = func(
				a btcutil.Address) bool {

				return bytes.Equal(
					a.ScriptAddress(), ourScript[2:],
				)
			}

			tx := wire.NewMsgTx(2)
			tx.AddTxIn(&wire.TxIn{
				PreviousOutPoint: alice.FundingOutpoint,
				Sequence:         wire.MaxTxInSequenceNum,
			})
			for _, script := range tc.outputs {
				tx.AddTxOut(&wire.TxOut{
					Value: 50_000, PkScript: script,
				})
			}
			h.sendSpend(tx)

			if tc.close {
				closeInfo := h.waitForCoopClose(5 * time.Second)
				require.Equal(
					t, tx.TxHash(), closeInfo.ClosingTXID,
				)
				return
			}
			h.assertNoCoopClose(200 * time.Millisecond)
			require.Eventually(t, func() bool {
				return alice.HasChanStatus(
					channeldb.ChanStatusFundingSpentUnknown,
				)
			}, 5*time.Second, 10*time.Millisecond)
		})
	}
}
