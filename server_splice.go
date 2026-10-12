package lnd

import (
	"context"
	"errors"
	"sync"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/chainntnfs"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/channelnotifier"
	graphdb "github.com/lightningnetwork/lnd/graph/db"
	"github.com/lightningnetwork/lnd/graph/db/models"
	"github.com/lightningnetwork/lnd/lnwallet"
	"github.com/lightningnetwork/lnd/lnwallet/splice"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/lightningnetwork/lnd/netann"
	"github.com/lightningnetwork/lnd/routing/route"
)

// spliceChain is the chain backend as the splice machines use it.
type spliceChain struct {
	notifier chainntnfs.ChainNotifier
	chainIO  lnwallet.BlockChainIO
	wallet   lnwallet.WalletController
}

// RegisterConfirmationsNtfn implements splice.Chain.
func (c *spliceChain) RegisterConfirmationsNtfn(txid *chainhash.Hash,
	pkScript []byte, numConfs, heightHint uint32,
	opts ...chainntnfs.NotifierOption) (*chainntnfs.ConfirmationEvent,
	error) {

	return c.notifier.RegisterConfirmationsNtfn(
		txid, pkScript, numConfs, heightHint, opts...,
	)
}

// RegisterSpendNtfn implements splice.Chain.
func (c *spliceChain) RegisterSpendNtfn(op *wire.OutPoint, pkScript []byte,
	heightHint uint32) (*chainntnfs.SpendEvent, error) {

	return c.notifier.RegisterSpendNtfn(op, pkScript, heightHint)
}

// RegisterBlockEpochNtfn implements splice.Chain.
func (c *spliceChain) RegisterBlockEpochNtfn(
	best *chainntnfs.BlockEpoch) (*chainntnfs.BlockEpochEvent, error) {

	return c.notifier.RegisterBlockEpochNtfn(best)
}

// BestHeight implements splice.Chain.
func (c *spliceChain) BestHeight() (uint32, error) {
	_, height, err := c.chainIO.GetBestBlock()
	if err != nil {
		return 0, err
	}

	return uint32(height), nil
}

// utxoDetailer is a wallet that can ask the backend about an unspent output
// in detail.
type utxoDetailer interface {
	GetUtxoDetails(op *wire.OutPoint) (*wire.TxOut, uint32, bool, error)
}

// ConfirmedOutput implements splice.Chain. A backend that can't tell refuses
// every peer input.
func (c *spliceChain) ConfirmedOutput(op wire.OutPoint) (*wire.TxOut, uint32,
	bool, error) {

	d, ok := c.wallet.(utxoDetailer)
	if !ok {
		return nil, 0, false, errors.New("backend can't check " +
			"unspent outputs")
	}

	return d.GetUtxoDetails(&op)
}

// newSpliceMachine fills the node's part of a channel's splice machine's
// configuration.
func (s *server) newSpliceMachine(base splice.Config,
	peer *btcec.PublicKey) *splice.Machine {

	cfg := base
	copy(cfg.OurNodeID[:], s.identityECDH.PubKey().SerializeCompressed())
	copy(cfg.TheirNodeID[:], peer.SerializeCompressed())

	wallet := s.cc.Wallet
	cfg.Chain = &spliceChain{
		notifier: s.cc.ChainNotifier,
		chainIO:  s.cc.ChainIO,
		wallet:   wallet.WalletController,
	}
	cfg.Wallet = wallet.WalletController
	cfg.CoinSource = wallet.WalletController
	cfg.WithCoinSelectLock = wallet.WithCoinSelectLock
	cfg.ChangeScript = func() ([]byte, error) {
		addr, err := wallet.NewAddress(
			lnwallet.WitnessPubKey, true,
			lnwallet.DefaultAccountName,
		)
		if err != nil {
			return nil, err
		}

		return txscript.PayToAddrScript(addr)
	}
	cfg.CheckMempool = wallet.WalletController.CheckMempoolAcceptance
	cfg.IsOurOutpoint = func(op wire.OutPoint) bool {
		if _, err := wallet.FetchOutpointInfo(&op); err == nil {
			return true
		}
		_, err := s.chanStateDB.FetchChannel(op)

		return err == nil
	}
	cfg.Disabled = !s.cfg.ProtocolOptions.Splice()
	cfg.Limits = s.spliceLimits
	cfg.KeyRing = s.cc.KeyRing
	cfg.Signer = s.cc.Signer
	cfg.Estimator = s.cc.FeeEstimator
	cfg.DustLimit = func(pkScript []byte) btcutil.Amount {
		return lnwallet.DustLimitForSize(len(pkScript))
	}

	// The lock depth: at least 6, at least the channel's own, and more for
	// a large amount the peer put in (it could double-spend it in a
	// reorg).
	numConfs := uint32(base.Channel.State().NumConfsRequired)
	cfg.Policy = splice.Policy{
		MaxChannelSize: btcutil.Amount(s.cfg.MaxChanSize),
		LockDepth: func(peer btcutil.Amount) uint32 {
			depth := max(uint32(6), numConfs)
			scaled := uint32(lnwallet.ScaleNumConfs(peer, 0))

			return max(depth, scaled)
		},
	}

	ch := base.Channel
	cfg.NotifyEvent = func(kind string, txid chainhash.Hash,
		reason string, fromPeer bool) {

		state := ch.State()
		s.spliceStep(kind, txid, state.FundingOutpoint)
		s.channelNotifier.NotifySpliceEvent(channelnotifier.SpliceEvent{
			ChannelPoint:         state.CurrentFundingOutpoint(),
			OriginalChannelPoint: state.FundingOutpoint,
			Kind:                 kind,
			Txid:                 txid,
			Reason:               reason,
			ReasonFromPeer:       fromPeer,
		})
	}

	cfg.AnnouncementPending = func() bool {
		return s.spliceAnnouncementPending(ch.State())
	}
	cfg.ResendAnnouncement = func() {
		state := ch.State()
		go func() {
			err := s.resendSpliceAnnouncement(state)
			if err != nil {
				srvrLog.Errorf("ChannelPoint(%v): unable to "+
					"resend its announcement signatures: %v",
					state.FundingOutpoint, err)
			}
		}()
	}

	return splice.NewMachine(cfg)
}

// spliceAnnouncementPending reports whether a public channel a splice moved
// lacks the full proof of its announcement on its current funding output.
func (s *server) spliceAnnouncementPending(ch *channeldb.OpenChannel) bool {
	current := ch.CurrentFundingOutpoint()
	if current == ch.FundingOutpoint ||
		ch.ChannelFlags&lnwire.FFAnnounceChannel == 0 {

		return false
	}

	info, _, _, err := s.graphDB.FetchChannelEdgesByOutpoint(
		context.TODO(), &current,
	)
	if errors.Is(err, graphdb.ErrEdgeNotFound) {
		return true
	}

	return err == nil && info.AuthProof == nil
}

// resendSpliceAnnouncement sends our half of a spliced public channel's
// announcement on its current funding output again, the edge moved there
// first if it isn't.
func (s *server) resendSpliceAnnouncement(ch *channeldb.OpenChannel) error {
	if ch.CurrentFundingOutpoint() == ch.FundingOutpoint ||
		ch.ChannelFlags&lnwire.FFAnnounceChannel == 0 {

		return nil
	}

	// Moving announces while the proof is missing.
	pending := s.spliceAnnouncementPending(ch)
	if err := s.moveSpliceEdge(ch); err != nil {
		return err
	}
	if pending {
		return nil
	}

	return s.fundingMgr.AnnounceSplice(ch, nil, false)
}

// releaseForgottenSpliceLeases releases the wallet leases of splices no
// channel remembers: a crash between leasing coins and recording the splice
// leaves them locked until they expire otherwise.
func (s *server) releaseForgottenSpliceLeases() error {
	leases, err := s.cc.Wallet.ListLeasedOutputs()
	if err != nil {
		return err
	}

	channels, err := s.chanStateDB.FetchAllOpenChannels()
	if err != nil {
		return err
	}
	kept := make(map[wire.OutPoint]bool)
	for _, ch := range channels {
		records, err := ch.SpliceRecords()
		if err != nil {
			return err
		}
		for _, r := range records {
			if r.Negotiation == nil {
				continue
			}
			for _, op := range r.Negotiation.OurInputs {
				kept[op] = true
			}
		}
	}

	for _, l := range leases {
		if l.LockedOutput == nil || l.LockID != splice.LockID ||
			kept[l.Outpoint] {

			continue
		}
		err := s.cc.Wallet.ReleaseOutput(splice.LockID, l.Outpoint)
		if err != nil {
			srvrLog.Errorf("Unable to release splice lease of %v: %v",
				l.Outpoint, err)
		}
	}

	return nil
}

// pendingSplices reports whether any open channel has a splice to finish:
// splicing stays advertised then, even turned off, or the peer would fail
// the channel at the next batch or reestablish.
func pendingSplices(db *channeldb.ChannelStateDB) bool {
	channels, err := db.FetchAllOpenChannels()
	if err != nil {
		srvrLog.Errorf("Unable to read channels for pending splices: "+
			"%v", err)

		return false
	}
	for _, ch := range channels {
		records, err := ch.SpliceRecords()
		if err != nil {
			continue
		}
		for _, r := range records {
			if !r.Retired {
				return true
			}
		}
	}

	return false
}

// spliceGraph is what the node keeps in memory of its channels' splices for
// the graph: the splice transactions of ours a block may hold, whose spend of
// a funding output moves a channel rather than closing it, and the short
// channel id each id a channel has had now maps to.
type spliceGraph struct {
	db *channeldb.ChannelStateDB

	mu     sync.Mutex
	loaded bool
	txids  map[chainhash.Hash]struct{}
	latest map[lnwire.ShortChannelID]lnwire.ShortChannelID
}

// newSpliceGraph returns a spliceGraph read from db when first used.
func newSpliceGraph(db *channeldb.ChannelStateDB) *spliceGraph {
	return &spliceGraph{
		db:     db,
		txids:  make(map[chainhash.Hash]struct{}),
		latest: make(map[lnwire.ShortChannelID]lnwire.ShortChannelID),
	}
}

// load reads the open channels' splices, once.
//
// NOTE: must be called with mu held.
func (g *spliceGraph) load() {
	if g.loaded {
		return
	}
	channels, err := g.db.FetchAllOpenChannels()
	if err != nil {
		srvrLog.Errorf("Unable to read channels for their splices: %v",
			err)
		return
	}
	g.loaded = true

	for _, ch := range channels {
		records, err := ch.SpliceRecords()
		if err != nil {
			srvrLog.Errorf("ChannelPoint(%v): unable to read "+
				"splices: %v", ch.FundingOutpoint, err)
		}
		for _, r := range records {
			if !r.Retired {
				g.txids[r.Txid()] = struct{}{}
			}
		}
		g.lockedLocked(ch)
	}
}

// isOwnSplice reports whether txid is a pending splice of one of our
// channels.
func (g *spliceGraph) isOwnSplice(txid chainhash.Hash) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.load()
	_, ok := g.txids[txid]

	return ok
}

// pending records a splice of ours that may confirm, or with ok false, one
// that won't.
func (g *spliceGraph) pending(txid chainhash.Hash, ok bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.load()
	if ok {
		g.txids[txid] = struct{}{}
	} else {
		delete(g.txids, txid)
	}
}

// locked records a channel's short channel ids after a splice locked.
func (g *spliceGraph) locked(ch *channeldb.OpenChannel) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.load()
	g.lockedLocked(ch)
}

// lockedLocked is locked with mu held.
func (g *spliceGraph) lockedLocked(ch *channeldb.OpenChannel) {
	current := ch.CurrentShortChanID()
	if current == ch.ShortChanID() {
		return
	}
	scids, err := ch.SpliceScids()
	if err != nil {
		srvrLog.Errorf("ChannelPoint(%v): unable to read its splices' "+
			"short channel ids: %v", ch.FundingOutpoint, err)
	}
	g.latest[ch.ShortChanID()] = current
	for _, scid := range scids {
		g.latest[scid] = current
	}
}

// latestScid returns the short channel id a channel that went by scid goes
// by now, if a splice moved it.
func (g *spliceGraph) latestScid(
	scid lnwire.ShortChannelID) (lnwire.ShortChannelID, bool) {

	g.mu.Lock()
	defer g.mu.Unlock()

	g.load()
	latest, ok := g.latest[scid]

	return latest, ok && latest != scid
}

// spliceStep handles a splice step for the graph and the switch: a splice
// signed may confirm, one locked moves the channel, one gone won't.
func (s *server) spliceStep(kind string, txid chainhash.Hash,
	chanPoint wire.OutPoint) {

	switch kind {
	case "signed":
		s.spliceGraph.pending(txid, true)

	case "aborted", "dead":
		s.spliceGraph.pending(txid, false)

	case "locked":
		s.spliceGraph.pending(txid, false)

		ch, err := s.chanStateDB.FetchChannel(chanPoint)
		if err != nil {
			srvrLog.Errorf("ChannelPoint(%v): unable to read the "+
				"channel after its splice locked: %v", chanPoint,
				err)
			return
		}
		s.spliceGraph.locked(ch)

		chanID := lnwire.NewChanIDFromOutPoint(chanPoint)
		err = s.htlcSwitch.AddSpliceScids(
			chanID, ch.CurrentShortChanID(),
		)
		if err != nil {
			srvrLog.Debugf("ChannelPoint(%v): no link for the "+
				"splice's short channel id: %v", chanPoint, err)
		}

		// The edge waits on the gossiper, which mustn't hold up the
		// splice machine.
		go func() {
			if err := s.moveSpliceEdge(ch); err != nil {
				srvrLog.Errorf("ChannelPoint(%v): unable to "+
					"move its graph edge to the splice's "+
					"funding: %v", chanPoint, err)
			}
		}()
	}
}

// moveSpliceEdge puts a channel a splice moved in the graph on its current
// funding output.
func (s *server) moveSpliceEdge(ch *channeldb.OpenChannel) error {
	var ourKey route.Vertex
	copy(ourKey[:], s.identityECDH.PubKey().SerializeCompressed())

	return moveSpliceEdge(
		context.TODO(), s.v1Graph, ourKey, s.fundingMgr.AnnounceSplice,
		ch,
	)
}

// spliceEdgeGraph is the graph as moving a spliced channel's edge uses it.
type spliceEdgeGraph interface {
	FetchChannelEdgesByOutpoint(ctx context.Context, op *wire.OutPoint) (
		*models.ChannelEdgeInfo, *models.ChannelEdgePolicy,
		*models.ChannelEdgePolicy, error)

	DeleteChannelEdges(ctx context.Context, strictZombiePruning,
		markZombie bool, chanIDs ...uint64) error
}

// spliceAnnouncer adds a spliced channel's edge on its current funding
// output, and announces a public one there.
type spliceAnnouncer func(ch *channeldb.OpenChannel,
	ourPolicy *models.ChannelEdgePolicy, addEdge bool) error

// moveSpliceEdge puts a channel a splice moved in the graph on its current
// funding output: the edges of the outputs it left go, our policy carried
// over, and a public channel is announced there. Repeatable: a channel
// already moved is only announced again while its proof is missing.
func moveSpliceEdge(ctx context.Context, graph spliceEdgeGraph,
	ourKey route.Vertex, announce spliceAnnouncer,
	ch *channeldb.OpenChannel) error {

	current := ch.CurrentFundingOutpoint()
	if current == ch.FundingOutpoint {
		return nil
	}

	public := ch.ChannelFlags&lnwire.FFAnnounceChannel != 0
	info, _, _, err := graph.FetchChannelEdgesByOutpoint(ctx, &current)
	switch {
	case err == nil:
		if public && info.AuthProof == nil {
			return announce(ch, nil, false)
		}

		return nil

	case !errors.Is(err, graphdb.ErrEdgeNotFound):
		return err
	}

	previous, err := ch.PreviousFundings()
	if err != nil {
		return err
	}

	var ourPolicy *models.ChannelEdgePolicy
	for _, pf := range previous {
		info, e1, e2, err := graph.FetchChannelEdgesByOutpoint(
			ctx, &pf.OutPoint,
		)
		switch {
		case errors.Is(err, graphdb.ErrEdgeNotFound):
			continue

		case err != nil:
			return err
		}

		policy := e2
		if info.NodeKey1Bytes == ourKey {
			policy = e1
		}
		if policy != nil {
			ourPolicy = policy
		}

		err = graph.DeleteChannelEdges(ctx, false, false, info.ChannelID)
		if err != nil {
			return err
		}
	}

	// A splice out can leave the old maximum above the new capacity.
	if ourPolicy != nil {
		policy := *ourPolicy
		capacity := lnwire.NewMSatFromSatoshis(ch.Capacity)
		if policy.MaxHTLC > capacity {
			policy.MaxHTLC = capacity
		}
		ourPolicy = &policy
	} else {
		srvrLog.Warnf("ChannelPoint(%v): no edge on the funding it "+
			"left; announcing the splice's with the default "+
			"policy", ch.FundingOutpoint)
	}

	srvrLog.Infof("ChannelPoint(%v): moving its graph edge to %v, "+
		"short_chan_id=%v", ch.FundingOutpoint, current,
		ch.CurrentShortChanID())

	return announce(ch, ourPolicy, true)
}

// reconcileSpliceEdges moves the graph edges of channels whose splice locked
// while the node stopped before it could.
func (s *server) reconcileSpliceEdges() {
	channels, err := s.chanStateDB.FetchAllOpenChannels()
	if err != nil {
		srvrLog.Errorf("Unable to read channels for their splices: %v",
			err)
		return
	}
	for _, ch := range channels {
		if err := s.moveSpliceEdge(ch); err != nil {
			srvrLog.Errorf("ChannelPoint(%v): unable to move its "+
				"graph edge to the splice's funding: %v",
				ch.FundingOutpoint, err)
		}
	}
}

// spliceAwareGraph finds a channel's edge by the outpoint it was opened with
// once a splice has moved the edge to the channel's current funding output.
type spliceAwareGraph struct {
	netann.ChannelGraph

	db *channeldb.ChannelStateDB
}

// FetchChannelEdgesByOutpoint implements netann.ChannelGraph.
func (g *spliceAwareGraph) FetchChannelEdgesByOutpoint(ctx context.Context,
	op *wire.OutPoint) (*models.ChannelEdgeInfo,
	*models.ChannelEdgePolicy, *models.ChannelEdgePolicy, error) {

	info, e1, e2, err := g.ChannelGraph.FetchChannelEdgesByOutpoint(ctx, op)
	if !errors.Is(err, graphdb.ErrEdgeNotFound) {
		return info, e1, e2, err
	}

	ch, chErr := g.db.FetchChannel(*op)
	if chErr != nil {
		return info, e1, e2, err
	}
	current := ch.CurrentFundingOutpoint()
	if current == *op {
		return info, e1, e2, err
	}

	return g.ChannelGraph.FetchChannelEdgesByOutpoint(ctx, &current)
}

// updateForwardingPolicies hands policies keyed by the funding output a
// channel's graph edge names to the switch, which knows a channel by the
// outpoint it was opened with.
func (s *server) updateForwardingPolicies(
	policies map[wire.OutPoint]models.ForwardingPolicy) {

	named := make(map[wire.OutPoint]models.ForwardingPolicy, len(policies))
	for op, policy := range policies {
		if ch, err := s.chanStateDB.FetchChannel(op); err == nil {
			op = ch.FundingOutpoint
		}
		named[op] = policy
	}

	s.htlcSwitch.UpdateForwardingPolicies(named)
}
