package chainreg

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/lightningnetwork/lnd/lncfg"
	"github.com/stretchr/testify/require"
)

// TestChainIdentityStatusPathSelection: the status file lives next to
// channel.backup unless the operator moved it.
func TestChainIdentityStatusPathSelection(t *testing.T) {
	cfg := &Config{
		Bitcoin:         &lncfg.Chain{ChainDir: "/data/chain/bitcoin"},
		ActiveNetParams: BitcoinMainNetParams,
	}
	require.Equal(t,
		filepath.Join("/data/chain/bitcoin", "mainnet", ChainIdentityFileName),
		chainIdentityStatusPath(cfg))

	cfg.Bitcoin.ChainIdentityFile = "/status/chain-identity.json"
	require.Equal(t, "/status/chain-identity.json",
		chainIdentityStatusPath(cfg))

	// The network name is normalized the way the chain directory is, so
	// testnet3 aliases land in the same place as the wallet.
	cfg.Bitcoin.ChainIdentityFile = ""
	cfg.ActiveNetParams = BitcoinTestNetParams
	require.Equal(t,
		filepath.Join("/data/chain/bitcoin",
			lncfg.NormalizeNetwork(BitcoinTestNetParams.Name),
			ChainIdentityFileName),
		chainIdentityStatusPath(cfg))
}

// TestChainIdentityWaitStopsOnShutdown pins that a daemon still waiting for
// the backend to reach the activation height stops when asked to.
func TestChainIdentityWaitStopsOnShutdown(t *testing.T) {
	shutdown := make(chan struct{})
	cfg := &Config{
		Bitcoin:         &lncfg.Chain{ChainDir: t.TempDir()},
		ActiveNetParams: BitcoinMainNetParams,
		Shutdown:        shutdown,
	}
	// A backend that never gets there.
	rpc := &neutrinoIdentityRPC{
		src:   &fakeNeutrino{height: 961000},
		chain: "main",
	}

	done := make(chan error, 1)
	go func() {
		done <- verifyBlake2bChain(rpc, cfg, make(chan struct{}))
	}()
	close(shutdown)

	select {
	case err := <-done:
		require.ErrorContains(t, err, "shutting down")
	case <-time.After(10 * time.Second):
		t.Fatal("the wait did not stop on shutdown")
	}
}
