package splice

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcwallet/wtxmgr"
	"github.com/lightningnetwork/lnd/lnwallet"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/stretchr/testify/require"
)

// fakeWallet keeps leases, published and removed transactions, and coins.
type fakeWallet struct {
	mu        sync.Mutex
	leases    map[wire.OutPoint]wtxmgr.LockID
	failLease map[wire.OutPoint]bool
	published map[chainhash.Hash]string
	removed   []chainhash.Hash
	publish   error
	utxos     []*lnwallet.Utxo
	txs       map[chainhash.Hash]*wire.MsgTx
	minConfs  int32
	account   string
}

func newFakeWallet() *fakeWallet {
	return &fakeWallet{
		leases:    make(map[wire.OutPoint]wtxmgr.LockID),
		failLease: make(map[wire.OutPoint]bool),
		published: make(map[chainhash.Hash]string),
		txs:       make(map[chainhash.Hash]*wire.MsgTx),
	}
}

func (w *fakeWallet) LeaseOutput(id wtxmgr.LockID, op wire.OutPoint,
	_ time.Duration) (time.Time, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.failLease[op] {
		return time.Time{}, wtxmgr.ErrOutputAlreadyLocked
	}
	w.leases[op] = id

	return time.Now(), nil
}

func (w *fakeWallet) ReleaseOutput(id wtxmgr.LockID, op wire.OutPoint) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.leases[op] != id {
		return wtxmgr.ErrUnknownOutput
	}
	delete(w.leases, op)

	return nil
}

func (w *fakeWallet) PublishTransaction(tx *wire.MsgTx, label string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.publish != nil {
		return w.publish
	}
	w.published[tx.TxHash()] = label

	return nil
}

// leaseCount returns how many outputs are leased.
func (w *fakeWallet) leaseCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()

	return len(w.leases)
}

// publishedTxs returns a copy of what was published.
func (w *fakeWallet) publishedTxs() map[chainhash.Hash]string {
	w.mu.Lock()
	defer w.mu.Unlock()

	out := make(map[chainhash.Hash]string, len(w.published))
	for k, v := range w.published {
		out[k] = v
	}

	return out
}

func (w *fakeWallet) RemoveDescendants(tx *wire.MsgTx) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.removed = append(w.removed, tx.TxHash())

	return nil
}

func (w *fakeWallet) ListUnspentWitness(minConfs, _ int32,
	account string) ([]*lnwallet.Utxo, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.minConfs, w.account = minConfs, account

	return w.utxos, nil
}

func (w *fakeWallet) FetchTx(h chainhash.Hash) (*wire.MsgTx, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	tx, ok := w.txs[h]
	if !ok {
		return nil, errors.New("not found")
	}

	return tx, nil
}

func TestLeases(t *testing.T) {
	t.Parallel()

	w := newFakeWallet()
	coins := []Coin{
		coin(1, p2wpkh(1), 1_000), coin(2, p2wpkh(2), 2_000),
		coin(3, p2tr(3), 3_000),
	}

	require.NoError(t, LeaseCoins(w, coins))
	require.Len(t, w.leases, 3)
	for _, id := range w.leases {
		require.Equal(t, LockID, id)
	}
	require.NoError(t, ReleaseCoins(w, coins))
	require.Empty(t, w.leases)

	// Releasing again is harmless.
	require.NoError(t, ReleaseCoins(w, coins))

	// One coin can't be leased: the ones before it are released.
	w.failLease[coins[2].OutPoint] = true
	require.ErrorIs(t, LeaseCoins(w, coins), wtxmgr.ErrOutputAlreadyLocked)
	require.Empty(t, w.leases)
}

func TestForget(t *testing.T) {
	t.Parallel()

	w := newFakeWallet()
	coins := []Coin{coin(1, p2wpkh(1), 1_000)}
	require.NoError(t, LeaseCoins(w, coins))

	tx := wire.NewMsgTx(2)
	tx.AddTxIn(&wire.TxIn{PreviousOutPoint: coins[0].OutPoint})
	require.NoError(t, Forget(w, tx, coins))
	require.Equal(t, []chainhash.Hash{tx.TxHash()}, w.removed)
	require.Empty(t, w.leases)
}

func TestPublish(t *testing.T) {
	t.Parallel()

	w := newFakeWallet()
	tx := wire.NewMsgTx(2)
	scid := lnwire.ShortChannelID{BlockHeight: 900_000, TxIndex: 2}
	label := Label(scid, DirectionIn)
	require.Equal(t, "splice:989560464998531072:in", label)
	require.True(t, IsLabel(label))
	require.False(t, IsLabel("0:openchannel"))

	require.Equal(t, RelayStatus{State: RelayAccepted},
		Publish(w, tx, label))
	require.Equal(t, label, w.published[tx.TxHash()])

	w.publish = lnwallet.ErrDoubleSpend
	st := Publish(w, tx, label)
	require.Equal(t, RelayRejected, st.State)
	require.Contains(t, st.Reason, "already spent")

	w.publish = errors.New("min relay fee not met")
	st = Publish(w, tx, label)
	require.Equal(t, RelayRejected, st.State)
	require.Equal(t, "min relay fee not met", st.Reason)
}

func TestListCoins(t *testing.T) {
	t.Parallel()

	w := newFakeWallet()
	add := func(c Coin) {
		w.txs[c.OutPoint.Hash] = c.PrevTx
		w.utxos = append(w.utxos, &lnwallet.Utxo{
			OutPoint: c.OutPoint, Value: c.Value,
			PkScript: c.PkScript,
		})
	}
	good := coin(1, p2wpkh(1), 1_000)
	tr := coin(2, p2tr(2), 2_000)
	nested := coin(3, append([]byte{0xa9, 0x14}, append(
		make([]byte, 20), 0x87)...), 3_000)
	cb := coinbaseCoin(4_000)
	for _, c := range []Coin{good, tr, nested, cb} {
		add(c)
	}

	coins, err := ListCoins(w)
	require.NoError(t, err)
	require.Equal(t, []Coin{good, tr}, coins)
	require.Equal(t, int32(1), w.minConfs)
	require.Equal(t, lnwallet.DefaultAccountName, w.account)

	// A coin whose transaction the wallet can't find is an error, not
	// silently skipped.
	missing := coin(5, p2wpkh(5), 5_000)
	w.utxos = append(w.utxos, &lnwallet.Utxo{
		OutPoint: missing.OutPoint, Value: missing.Value,
		PkScript: missing.PkScript,
	})
	_, err = ListCoins(w)
	require.Error(t, err)
}
