package lnd

import (
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcwallet/waddrmgr"
	basewallet "github.com/btcsuite/btcwallet/wallet"
	"github.com/lightningnetwork/lnd/lnwallet"
	"github.com/stretchr/testify/require"
)

// stallingWallet answers about unspent outputs after a delay, and says
// whether it is synced.
type stallingWallet struct {
	lnwallet.WalletController

	delay  time.Duration
	synced bool
}

func (w *stallingWallet) GetUtxoDetails(*wire.OutPoint) (*wire.TxOut, uint32,
	bool, error) {

	time.Sleep(w.delay)

	return &wire.TxOut{Value: 1}, 3, false, nil
}

func (w *stallingWallet) IsSynced() (bool, int64, error) {
	return w.synced, 0, nil
}

// TestSpliceChainBackend checks the splice machines' view of the backend:
// an answer about a peer's coin within the budget is passed on, one that
// takes longer is an error (the channel is quiescent meanwhile), and the
// wallet's sync state is reported.
func TestSpliceChainBackend(t *testing.T) {
	old := confirmedOutputTimeout
	confirmedOutputTimeout = 100 * time.Millisecond
	t.Cleanup(func() { confirmedOutputTimeout = old })

	fast := &spliceChain{
		wallet: &stallingWallet{synced: true}, chainIO: tipAt(100),
	}
	out, confs, _, err := fast.ConfirmedOutput(wire.OutPoint{})
	require.NoError(t, err)
	require.EqualValues(t, 1, out.Value)
	require.EqualValues(t, 3, confs)
	require.True(t, fast.Synced())

	slow := &spliceChain{
		wallet: &stallingWallet{delay: time.Second}, chainIO: tipAt(100),
	}
	start := time.Now()
	_, _, _, err = slow.ConfirmedOutput(wire.OutPoint{})
	require.ErrorContains(t, err, "didn't answer")
	require.Less(t, time.Since(start), 500*time.Millisecond)
	require.False(t, slow.Synced())
}

// backendTip is a chain backend at a height.
type backendTip struct {
	lnwallet.BlockChainIO

	height int32
}

func tipAt(height int32) *backendTip { return &backendTip{height: height} }

func (b *backendTip) GetBestBlock() (*chainhash.Hash, int32, error) {
	return &chainhash.Hash{}, b.height, nil
}

// processedTip is the tip this node has processed.
type processedTip uint32

func (p processedTip) BestHeight() (uint32, error) { return uint32(p), nil }

// syncState is a wallet's own sync state.
type syncState struct {
	basewallet.Interface

	chainSynced bool
	to          waddrmgr.BlockStamp
}

func (s *syncState) ChainSynced() bool { return s.chainSynced }

func (s *syncState) SyncedTo() waddrmgr.BlockStamp { return s.to }

// walletWithState is a wallet that shows its sync state.
type walletWithState struct {
	lnwallet.WalletController

	state *syncState
}

func (w *walletWithState) InternalWallet() basewallet.Interface {
	return w.state
}

// TestSpliceChainSynced checks when this node counts as caught up for a
// splice: it has processed the backend's tip, the wallet has done its
// initial sync and reached it, and the tip isn't a day old. A tip a few
// hours old, this chain on a slow day, still counts.
func TestSpliceChainSynced(t *testing.T) {
	stamp := func(height int32, age time.Duration) waddrmgr.BlockStamp {
		return waddrmgr.BlockStamp{
			Height: height, Timestamp: time.Now().Add(-age),
		}
	}
	chain := func(processed uint32, state *syncState) *spliceChain {
		return &spliceChain{
			chainIO:   tipAt(100),
			processed: processedTip(processed),
			wallet:    &walletWithState{state: state},
		}
	}

	require.True(t, chain(100, &syncState{
		chainSynced: true, to: stamp(100, 3*time.Hour),
	}).Synced(), "a tip three hours old")
	require.False(t, chain(99, &syncState{
		chainSynced: true, to: stamp(100, time.Minute),
	}).Synced(), "blocks not yet processed")
	require.False(t, chain(100, &syncState{
		chainSynced: false, to: stamp(100, time.Minute),
	}).Synced(), "the wallet's initial sync not done")
	require.False(t, chain(100, &syncState{
		chainSynced: true, to: stamp(99, time.Minute),
	}).Synced(), "the wallet a block behind")
	require.False(t, chain(100, &syncState{
		chainSynced: true, to: stamp(100, 25*time.Hour),
	}).Synced(), "a tip a day old: the backend is behind")
}
