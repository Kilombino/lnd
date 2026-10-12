package chainreg

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/btcsuite/btcd/btcjson"
	bitcoinCfg "github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightninglabs/neutrino/headerfs"
)

// NeutrinoChainParams returns the btcd parameters handed to the neutrino light
// client: the network's own, plus, where the activation block is pinned
// (mainnet), a checkpoint at the activation height with its hash. The light
// client checks BLAKE2b proof of work itself, but would follow any chain with
// the most work that passes those rules from the activation height on; the
// checkpoint ties it to the Bitcoin BLAKE2b chain's activation block. The
// shared parameters are left untouched: the checkpoint goes on a copy.
func NeutrinoChainParams(params BitcoinNetParams) *bitcoinCfg.Params {
	p := *params.Params
	if params.Blake2bActivationHash == nil ||
		params.Blake2bActivationHeight == 0 {

		return &p
	}

	height := int32(params.Blake2bActivationHeight)
	checkpoints := make([]bitcoinCfg.Checkpoint, 0, len(p.Checkpoints)+1)
	for _, c := range p.Checkpoints {
		if c.Height != height {
			checkpoints = append(checkpoints, c)
		}
	}
	hash := *params.Blake2bActivationHash
	checkpoints = append(checkpoints, bitcoinCfg.Checkpoint{
		Height: height, Hash: &hash,
	})
	sort.Slice(checkpoints, func(i, j int) bool {
		return checkpoints[i].Height < checkpoints[j].Height
	})
	p.Checkpoints = checkpoints

	return &p
}

// neutrinoHeaderSource is what the chain-identity check reads from the light
// client: its best block, and the headers it validated.
type neutrinoHeaderSource interface {
	BestBlock() (*headerfs.BlockStamp, error)
	GetBlockHash(int64) (*chainhash.Hash, error)
	GetBlockHeader(*chainhash.Hash) (*wire.BlockHeader, error)
}

// neutrinoIdentityRPC lets the chain-identity check, written against a
// bitcoind backend, run against the light client: it answers the few calls
// the check makes from neutrino's validated headers. Anything else (the
// deployment info, say) is reported as not available, which the check treats
// as the node not saying.
type neutrinoIdentityRPC struct {
	src   neutrinoHeaderSource
	chain string
}

// GetBlockChainInfo reports the light client's header height under the chain
// name the check expects, so the wait for the activation height works as it
// does with a node.
func (n *neutrinoIdentityRPC) GetBlockChainInfo() (
	*btcjson.GetBlockChainInfoResult, error) {

	best, err := n.src.BestBlock()
	if err != nil {
		return nil, err
	}

	return &btcjson.GetBlockChainInfoResult{
		Chain:   n.chain,
		Blocks:  best.Height,
		Headers: best.Height,
	}, nil
}

// GetBlockHash returns the hash of the light client's header at height.
func (n *neutrinoIdentityRPC) GetBlockHash(height int64) (*chainhash.Hash,
	error) {

	return n.src.GetBlockHash(height)
}

// RawRequest answers getblockheader (non-verbose) from the light client's
// headers.
func (n *neutrinoIdentityRPC) RawRequest(method string,
	params []json.RawMessage) (json.RawMessage, error) {

	if method != "getblockheader" || len(params) < 1 {
		return nil, fmt.Errorf("%s is not available from the neutrino "+
			"light client", method)
	}

	var hashStr string
	if err := json.Unmarshal(params[0], &hashStr); err != nil {
		return nil, err
	}
	hash, err := chainhash.NewHashFromStr(hashStr)
	if err != nil {
		return nil, err
	}
	header, err := n.src.GetBlockHeader(hash)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err := header.Serialize(&buf); err != nil {
		return nil, err
	}

	return json.Marshal(hex.EncodeToString(buf.Bytes()))
}
