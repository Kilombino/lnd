package lnd

import (
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/channeldb"
	graphdb "github.com/lightningnetwork/lnd/graph/db"
	"github.com/lightningnetwork/lnd/graph/db/models"
	"github.com/lightningnetwork/lnd/keychain"
	"github.com/lightningnetwork/lnd/lnwallet"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/lightningnetwork/lnd/routing/route"
	"github.com/stretchr/testify/require"
)

// testSpliceLock returns a lock onto a new funding output.
func testSpliceLock(t *testing.T, txid byte, capacity btcutil.Amount,
	height uint32) channeldb.SpliceLock {

	t.Helper()

	key := func() keychain.KeyDescriptor {
		priv, err := btcec.NewPrivateKey()
		require.NoError(t, err)

		return keychain.KeyDescriptor{PubKey: priv.PubKey()}
	}

	return channeldb.SpliceLock{
		Funding: channeldb.SpliceFunding{
			OutPoint: wire.OutPoint{Hash: chainhash.Hash{txid}},
			Scid: lnwire.ShortChannelID{
				BlockHeight: height, TxIndex: 1,
			},
		},
		Capacity:          capacity,
		LocalMultiSigKey:  key(),
		RemoteMultiSigKey: key(),
	}
}

// TestSpliceGraph checks what the node keeps in memory of its splices: the
// pending ones a block may hold, read from the channels and kept by events,
// and the scid every id a channel has had maps to.
func TestSpliceGraph(t *testing.T) {
	t.Parallel()

	alice, _, err := lnwallet.CreateTestChannels(
		t, channeldb.SingleFunderTweaklessBit,
	)
	require.NoError(t, err)
	state := alice.State()
	db := state.Db
	identity := state.ShortChanID()

	pending := chainhash.Hash{0x51}
	require.NoError(t, state.PutSpliceRecord(&channeldb.SpliceRecord{
		OutPoint: wire.OutPoint{Hash: pending},
		LocalKey: keychain.KeyDescriptor{
			PubKey: state.LocalChanCfg.MultiSigKey.PubKey,
		},
		RemoteKey: state.RemoteChanCfg.MultiSigKey.PubKey,
	}))

	g := newSpliceGraph(db)
	require.True(t, g.isOwnSplice(pending))
	require.False(t, g.isOwnSplice(chainhash.Hash{0x52}))
	g.pending(chainhash.Hash{0x52}, true)
	require.True(t, g.isOwnSplice(chainhash.Hash{0x52}))
	g.pending(pending, false)
	require.False(t, g.isOwnSplice(pending))

	_, ok := g.latestScid(identity)
	require.False(t, ok)

	first := testSpliceLock(t, 0x61, 6_000_000, 900_000)
	require.NoError(t, state.LockSplice(first))
	g.locked(state)
	latest, ok := g.latestScid(identity)
	require.True(t, ok)
	require.Equal(t, first.Funding.Scid, latest)

	// Read afresh after a second splice: every older id maps to the
	// newest.
	second := testSpliceLock(t, 0x62, 7_000_000, 900_100)
	require.NoError(t, state.LockSplice(second))
	g = newSpliceGraph(db)
	for _, scid := range []lnwire.ShortChannelID{
		identity, first.Funding.Scid,
	} {
		latest, ok := g.latestScid(scid)
		require.True(t, ok)
		require.Equal(t, second.Funding.Scid, latest)
	}
	_, ok = g.latestScid(second.Funding.Scid)
	require.False(t, ok)
}

// testEdge adds an edge between us and the peer on op, with our policy.
func testEdge(t *testing.T, graph *graphdb.ChannelGraph, ours,
	theirs route.Vertex, scid lnwire.ShortChannelID, op wire.OutPoint,
	proof bool) *models.ChannelEdgePolicy {

	t.Helper()

	opts := []models.EdgeModifier{
		models.WithChannelPoint(op), models.WithCapacity(1_000_000),
	}
	if proof {
		opts = append(opts, models.WithChanProof(
			models.NewV1ChannelAuthProof(
				[]byte{1}, []byte{2}, []byte{3}, []byte{4},
			),
		))
	}
	edge, err := models.NewV1Channel(
		scid.ToUint64(), chainhash.Hash{}, ours, theirs,
		&models.ChannelV1Fields{
			BitcoinKey1Bytes: ours, BitcoinKey2Bytes: theirs,
		}, opts...,
	)
	require.NoError(t, err)
	require.NoError(t, graph.AddChannelEdge(t.Context(), edge))

	policy := &models.ChannelEdgePolicy{
		Version:       lnwire.GossipVersion1,
		SigBytes:      []byte{1},
		ChannelID:     scid.ToUint64(),
		LastUpdate:    time.Unix(1_700_000_000, 0),
		MessageFlags:  lnwire.ChanUpdateRequiredMaxHtlc,
		TimeLockDelta: 80,
		MaxHTLC:       990_000_000,
		FeeBaseMSat:   1234,
		ToNode:        theirs,
	}
	require.NoError(t, graph.UpdateEdgePolicy(t.Context(), policy))

	return policy
}

// TestMoveSpliceEdge checks a channel a splice moved gets its graph edge on
// the new funding output: the old edge goes, our policy carries over with its
// maximum within the new capacity, and once moved a public channel without
// its proof is only announced again.
func TestMoveSpliceEdge(t *testing.T) {
	t.Parallel()

	alice, _, err := lnwallet.CreateTestChannels(
		t, channeldb.SingleFunderTweaklessBit,
	)
	require.NoError(t, err)
	state := alice.State()
	state.ChannelFlags |= lnwire.FFAnnounceChannel

	graph := graphdb.MakeTestGraph(t)
	vgraph := graphdb.NewVersionedGraph(graph, lnwire.GossipVersion1)

	vertex := func() route.Vertex {
		priv, err := btcec.NewPrivateKey()
		require.NoError(t, err)
		return route.NewVertex(priv.PubKey())
	}
	ours, theirs := vertex(), vertex()

	type call struct {
		policy  *models.ChannelEdgePolicy
		addEdge bool
	}
	var calls []call
	announce := func(_ *channeldb.OpenChannel,
		policy *models.ChannelEdgePolicy, addEdge bool) error {

		calls = append(calls, call{policy, addEdge})
		return nil
	}
	move := func() {
		t.Helper()
		require.NoError(t, moveSpliceEdge(
			t.Context(), vgraph, ours, announce, state,
		))
	}

	// Not spliced: nothing.
	old := testEdge(
		t, graph, ours, theirs, state.ShortChanID(),
		state.FundingOutpoint, true,
	)
	move()
	require.Empty(t, calls)

	// Spliced out to less than the old maximum.
	lock := testSpliceLock(t, 0x71, 500_000, 900_000)
	require.NoError(t, state.LockSplice(lock))
	move()
	require.Len(t, calls, 1)
	require.True(t, calls[0].addEdge)
	require.Equal(t, old.FeeBaseMSat, calls[0].policy.FeeBaseMSat)
	require.Equal(t, lnwire.NewMSatFromSatoshis(500_000),
		calls[0].policy.MaxHTLC)
	_, _, _, err = graph.FetchChannelEdgesByOutpoint(
		t.Context(), &state.FundingOutpoint,
	)
	require.ErrorIs(t, err, graphdb.ErrEdgeNotFound)

	// The announcement added the new edge, without its proof yet: only
	// the proof is asked for again.
	calls = nil
	testEdge(
		t, graph, ours, theirs, lock.Funding.Scid, lock.Funding.OutPoint,
		false,
	)
	move()
	require.Equal(t, []call{{nil, false}}, calls)

	// A private channel: nothing more.
	calls = nil
	state.ChannelFlags &^= lnwire.FFAnnounceChannel
	move()
	require.Empty(t, calls)
}

// TestSpliceAwareGraph checks a channel's edge is found by the outpoint the
// channel was opened with once a splice moved it.
func TestSpliceAwareGraph(t *testing.T) {
	t.Parallel()

	alice, _, err := lnwallet.CreateTestChannels(
		t, channeldb.SingleFunderTweaklessBit,
	)
	require.NoError(t, err)
	state := alice.State()

	graph := graphdb.MakeTestGraph(t)
	aware := &spliceAwareGraph{ChannelGraph: graph, db: state.Db}

	lock := testSpliceLock(t, 0x81, 2_000_000, 900_000)
	require.NoError(t, state.LockSplice(lock))

	_, _, _, err = aware.FetchChannelEdgesByOutpoint(
		t.Context(), &state.FundingOutpoint,
	)
	require.ErrorIs(t, err, graphdb.ErrEdgeNotFound)

	priv1, err := btcec.NewPrivateKey()
	require.NoError(t, err)
	priv2, err := btcec.NewPrivateKey()
	require.NoError(t, err)
	testEdge(
		t, graph, route.NewVertex(priv1.PubKey()),
		route.NewVertex(priv2.PubKey()), lock.Funding.Scid,
		lock.Funding.OutPoint, false,
	)

	for _, op := range []wire.OutPoint{
		state.FundingOutpoint, lock.Funding.OutPoint,
	} {
		info, _, _, err := aware.FetchChannelEdgesByOutpoint(
			t.Context(), &op,
		)
		require.NoError(t, err)
		require.Equal(t, lock.Funding.Scid.ToUint64(), info.ChannelID)
	}

	// Anything else is as it was.
	other := wire.OutPoint{Hash: chainhash.Hash{0x99}}
	_, _, _, err = aware.FetchChannelEdgesByOutpoint(t.Context(), &other)
	require.ErrorIs(t, err, graphdb.ErrEdgeNotFound)
}
