package graph

import (
	"math/rand"
	"testing"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/graph/db/models"
	"github.com/lightningnetwork/lnd/routing/chainview"
	"github.com/lightningnetwork/lnd/routing/route"
	"github.com/stretchr/testify/require"
)

// TestPruneKeepsOwnSplice checks a block spending a channel's funding output
// by one of our splices leaves the channel's edge in the graph, though not
// another channel's spent by the same transaction, and one spending it
// otherwise prunes it.
func TestPruneKeepsOwnSplice(t *testing.T) {
	t.Parallel()

	const height = 101
	ctx := createTestCtxSingleNode(t, height)

	script, fundingTx, chanUTXO, chanID := createChannelEdge(
		t, bitcoinKey1.SerializeCompressed(),
		bitcoinKey2.SerializeCompressed(), 10_000, height+1,
	)
	block := &wire.MsgBlock{Transactions: []*wire.MsgTx{fundingTx}}
	ctx.chain.addBlock(block, height+1, rand.Uint32())

	node1 := createTestNode(t)
	node2 := createTestNode(t)
	edge, err := models.NewV1Channel(
		chanID.ToUint64(), *chaincfg.SimNetParams.GenesisHash,
		node1.PubKeyBytes, node2.PubKeyBytes, &models.ChannelV1Fields{
			BitcoinKey1Bytes: route.NewVertex(bitcoinKey1),
			BitcoinKey2Bytes: route.NewVertex(bitcoinKey2),
		},
		models.WithCapacity(btcutil.Amount(10_000)),
		models.WithChannelPoint(*chanUTXO),
		models.WithFundingScript(script),
	)
	require.NoError(t, err)
	require.NoError(t, ctx.builder.AddEdge(t.Context(), edge))

	// Another channel's funding output, spent by the same splice as the
	// peer's input.
	script2, fundingTx2, chanUTXO2, chanID2 := createChannelEdge(
		t, bitcoinKey1.SerializeCompressed(),
		bitcoinKey2.SerializeCompressed(), 20_000, height+2,
	)
	block2 := &wire.MsgBlock{Transactions: []*wire.MsgTx{fundingTx2}}
	ctx.chain.addBlock(block2, height+2, rand.Uint32())
	edge2, err := models.NewV1Channel(
		chanID2.ToUint64(), *chaincfg.SimNetParams.GenesisHash,
		node1.PubKeyBytes, node2.PubKeyBytes, &models.ChannelV1Fields{
			BitcoinKey1Bytes: route.NewVertex(bitcoinKey1),
			BitcoinKey2Bytes: route.NewVertex(bitcoinKey2),
		},
		models.WithCapacity(btcutil.Amount(20_000)),
		models.WithChannelPoint(*chanUTXO2),
		models.WithFundingScript(script2),
	)
	require.NoError(t, err)
	require.NoError(t, ctx.builder.AddEdge(t.Context(), edge2))

	splice := wire.NewMsgTx(2)
	splice.AddTxIn(&wire.TxIn{PreviousOutPoint: *chanUTXO})
	splice.AddTxIn(&wire.TxIn{PreviousOutPoint: *chanUTXO2})
	ctx.builder.cfg.OwnSplice = func(
		txid chainhash.Hash) (wire.OutPoint, bool) {

		return *chanUTXO, txid == splice.TxHash()
	}

	hasChan := func() bool {
		has, _, err := ctx.graph.HasChannelEdge(
			t.Context(), chanID.ToUint64(),
		)
		require.NoError(t, err)

		return has
	}
	hasOther := func() bool {
		has, _, err := ctx.graph.HasChannelEdge(
			t.Context(), chanID2.ToUint64(),
		)
		require.NoError(t, err)

		return has
	}

	require.NoError(t, ctx.builder.updateGraphWithClosedChannels(
		&chainview.FilteredBlock{
			Height:       height + 3,
			Transactions: []*wire.MsgTx{splice},
		},
	))
	require.True(t, hasChan())
	require.False(t, hasOther())

	closing := wire.NewMsgTx(2)
	closing.AddTxIn(&wire.TxIn{PreviousOutPoint: *chanUTXO})
	require.NoError(t, ctx.builder.updateGraphWithClosedChannels(
		&chainview.FilteredBlock{
			Height:       height + 4,
			Transactions: []*wire.MsgTx{closing},
		},
	))
	require.False(t, hasChan())
}
