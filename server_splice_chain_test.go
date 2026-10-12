package lnd

import (
	"testing"
	"time"

	"github.com/btcsuite/btcd/wire"
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

	fast := &spliceChain{wallet: &stallingWallet{synced: true}}
	out, confs, _, err := fast.ConfirmedOutput(wire.OutPoint{})
	require.NoError(t, err)
	require.EqualValues(t, 1, out.Value)
	require.EqualValues(t, 3, confs)
	require.True(t, fast.Synced())

	slow := &spliceChain{wallet: &stallingWallet{delay: time.Second}}
	start := time.Now()
	_, _, _, err = slow.ConfirmedOutput(wire.OutPoint{})
	require.ErrorContains(t, err, "didn't answer")
	require.Less(t, time.Since(start), 500*time.Millisecond)
	require.False(t, slow.Synced())
}
