package chainreg

import (
	"bytes"
	"encoding/hex"
	"errors"
	"path/filepath"
	"testing"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightninglabs/neutrino/headerfs"
	"github.com/stretchr/testify/require"
)

// fakeNeutrino serves the chain-identity check from a fixed set of headers,
// as the light client would from the ones it validated.
type fakeNeutrino struct {
	height   int32
	byHash   map[chainhash.Hash]*wire.BlockHeader
	byHeight map[int64]chainhash.Hash
}

func newFakeNeutrino(t *testing.T, height int32, at int64,
	hexStr string) *fakeNeutrino {

	t.Helper()
	raw, err := hex.DecodeString(hexStr)
	require.NoError(t, err)
	var h wire.BlockHeader
	require.NoError(t, h.Deserialize(bytes.NewReader(raw)))
	hash := h.BlockHash()
	return &fakeNeutrino{
		height:   height,
		byHash:   map[chainhash.Hash]*wire.BlockHeader{hash: &h},
		byHeight: map[int64]chainhash.Hash{at: hash},
	}
}

func (f *fakeNeutrino) BestBlock() (*headerfs.BlockStamp, error) {
	return &headerfs.BlockStamp{Height: f.height}, nil
}

func (f *fakeNeutrino) GetBlockHash(h int64) (*chainhash.Hash, error) {
	hash, ok := f.byHeight[h]
	if !ok {
		return nil, errors.New("no header at that height")
	}
	return &hash, nil
}

func (f *fakeNeutrino) GetBlockHeader(hash *chainhash.Hash) (
	*wire.BlockHeader, error) {

	h, ok := f.byHash[*hash]
	if !ok {
		return nil, errors.New("unknown header")
	}
	return h, nil
}

// TestNeutrinoChainParamsCheckpoint pins that the light client gets a
// checkpoint at the mainnet activation block, on a copy, and nothing extra on
// a network without a pinned activation hash.
func TestNeutrinoChainParamsCheckpoint(t *testing.T) {
	before := len(chaincfg.MainNetParams.Checkpoints)

	p := NeutrinoChainParams(BitcoinMainNetParams)
	require.Len(t, chaincfg.MainNetParams.Checkpoints, before,
		"shared parameters changed")
	require.Len(t, p.Checkpoints, before+1)

	last := p.Checkpoints[len(p.Checkpoints)-1]
	require.EqualValues(t, Blake2bMainnetActivationHeight, last.Height)
	require.Equal(t, *Blake2bMainnetActivationHash, *last.Hash)
	for i := 1; i < len(p.Checkpoints); i++ {
		require.Less(
			t, p.Checkpoints[i-1].Height, p.Checkpoints[i].Height,
		)
	}

	t4 := NeutrinoChainParams(BitcoinTestNet4Params)
	require.Equal(t, chaincfg.TestNet4Params.Checkpoints, t4.Checkpoints)
}

// TestNeutrinoChainIdentity runs the chain-identity check against the light
// client's headers: the real activation header confirms mainnet and writes
// the status file; the last SHA256d header in its place is refused.
func TestNeutrinoChainIdentity(t *testing.T) {
	t.Run("activation header confirms", func(t *testing.T) {
		status := filepath.Join(t.TempDir(), ChainIdentityFileName)
		rpc := &neutrinoIdentityRPC{
			src: newFakeNeutrino(
				t, 976700, 961640, activationHeaderHex,
			),
			chain: "main",
		}
		c := newTestChecker(t, rpc, BitcoinMainNetParams, status)
		height, err := c.run(make(chan struct{}))
		require.NoError(t, err)
		require.EqualValues(t, 961640, height)

		st, err := ReadChainIdentityStatus(status)
		require.NoError(t, err)
		require.Equal(t, ChainIdentityConfirmed, st.State)
		require.Equal(
			t, Blake2bMainnetActivationHash.String(),
			st.ActivationHash,
		)
	})

	t.Run("classic header refused", func(t *testing.T) {
		status := filepath.Join(t.TempDir(), ChainIdentityFileName)
		rpc := &neutrinoIdentityRPC{
			src: newFakeNeutrino(
				t, 976700, 961640, lastSHA256HeaderHex,
			),
			chain: "main",
		}
		c := newTestChecker(t, rpc, BitcoinMainNetParams, status)
		_, err := c.run(make(chan struct{}))
		var wrong *ErrWrongChain
		require.ErrorAs(t, err, &wrong)

		st, err := ReadChainIdentityStatus(status)
		require.NoError(t, err)
		require.Equal(t, ChainIdentityRefused, st.State)
	})

	t.Run("other calls are not available", func(t *testing.T) {
		rpc := &neutrinoIdentityRPC{src: &fakeNeutrino{}, chain: "main"}
		_, err := rpc.RawRequest("getdeploymentinfo", nil)
		require.Error(t, err)
	})
}
