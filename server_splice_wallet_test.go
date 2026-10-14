package lnd

import (
	"testing"

	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/lnwallet"
	"github.com/stretchr/testify/require"
)

// detailedWallet answers GetUtxoDetails, as btcwallet does.
type detailedWallet struct {
	lnwallet.WalletController
}

func (detailedWallet) GetUtxoDetails(*wire.OutPoint) (*wire.TxOut, uint32,
	bool, error) {

	return &wire.TxOut{Value: 1}, 7, false, nil
}

// remoteSignerWallet wraps a wallet, as rpcwallet's RPCKeyRing does.
type remoteSignerWallet struct {
	lnwallet.WalletController
	inner lnwallet.WalletController
}

func (r remoteSignerWallet) UnwrapWalletController() lnwallet.WalletController {
	return r.inner
}

// TestSpliceChainRemoteSigner checks the splice's chain checks see through
// a remote signer's wallet to the one it wraps: a peer's coin is checked,
// not refused, and a splice's confirmations read.
func TestSpliceChainRemoteSigner(t *testing.T) {
	t.Parallel()

	chain := &spliceChain{wallet: remoteSignerWallet{
		inner: detailedWallet{},
	}}
	out, confs, _, err := chain.ConfirmedOutput(wire.OutPoint{})
	require.NoError(t, err)
	require.EqualValues(t, 1, out.Value)
	require.EqualValues(t, 7, confs)
}
