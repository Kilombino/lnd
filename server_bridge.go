package lnd

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/aezeed"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/feature"
	"github.com/lightningnetwork/lnd/invoices"
	"github.com/lightningnetwork/lnd/keychain"
	"github.com/lightningnetwork/lnd/lncfg"
	"github.com/lightningnetwork/lnd/lnrpc/bridgerpc"
	"github.com/lightningnetwork/lnd/lnrpc/invoicesrpc"
	"github.com/lightningnetwork/lnd/lnrpc/routerrpc"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/lnwire"
	paymentsdb "github.com/lightningnetwork/lnd/payments/db"
	"github.com/lightningnetwork/lnd/routing/route"
	"github.com/lightningnetwork/lnd/tor"
	"github.com/lightningnetwork/lnd/zpay32"
)

// bridgeDeps is what the bridge sub-server gets from the node: the local half
// of a cross-chain swap, wired in process.
//
// Dialling this node's own RPC server from inside it would mean an operator
// supplying a macaroon and a TLS path for their own node, and the sub-server
// waiting for that server to come up before it could work. Going straight to
// the registry and the router removes both.
func (s *server) bridgeDeps(
	routerBackend *routerrpc.RouterBackend) *bridgerpc.Deps {

	return &bridgerpc.Deps{
		HoldExpiryDelta: s.cfg.Invoices.HoldExpiryDelta,

		AddHoldInvoice: s.addBridgeHoldInvoice,

		LookupInvoice: func(ctx context.Context, hash [32]byte) (
			bridgerpc.InvoiceStatus, bool, error) {

			inv, err := s.invoices.LookupInvoice(ctx, hash)
			if err != nil {
				if bridgeInvoiceAbsent(err) {
					return bridgerpc.InvoiceStatus{}, false,
						nil
				}

				return bridgerpc.InvoiceStatus{}, false, err
			}

			return bridgerpc.TranslateInvoice(&inv), true, nil
		},

		SettleInvoice: func(ctx context.Context, pre [32]byte) error {
			return s.invoices.SettleHodlInvoice(
				ctx, lntypes.Preimage(pre),
			)
		},

		CancelInvoice: func(ctx context.Context, hash [32]byte) error {
			return s.invoices.CancelInvoice(ctx, lntypes.Hash(hash))
		},

		ForgetInvoice: s.forgetBridgeInvoice,

		NodeKey: hex.EncodeToString(
			s.identityECDH.PubKey().SerializeCompressed(),
		),

		Network: bridgeNetworkName(s.cfg.ActiveNetParams.Params.Name),

		Dial: bridgeNet(s.cfg.Tor, s.cfg.net).Dial,

		DeriveSha256Seed: s.deriveSha256Seed,

		DecodeInvoice: func(_ context.Context,
			invoice string) (*zpay32.Invoice, error) {

			return zpay32.Decode(
				invoice, s.cfg.ActiveNetParams.Params,
			)
		},

		CheckPayable: func(_ context.Context, invoice string) error {
			_, err := routerBackend.ExtractIntent(
				&routerrpc.SendPaymentRequest{
					PaymentRequest: invoice,
					TimeoutSeconds: 60,
					// The quote refuses those itself, by
					// name.
					AllowSelfPayment: true,
				},
			)

			return err
		},

		QueryRoutes: routerBackend.QueryRoutes,

		PayInvoice: func(ctx context.Context, req bridgerpc.PayRequest) (
			bridgerpc.PaymentStatus, error) {

			return s.payBridgeInvoice(ctx, routerBackend, req)
		},

		LookupPayment: s.lookupBridgePayment,

		BestBlock: s.bridgeBestBlock,

		BlockAt: s.bridgeBlockAt,

		ChannelBalance: s.bridgeChannelBalance,

		Blake2bActivation: s.bridgeBlake2bActivation,
	}
}

// bridgeNet is how the bridge reaches the world: the node's own network,
// except that a node using Tor goes through it for every target.
//
// The node's network skips Tor for clearnet targets when
// tor.skip-proxy-for-clearnet-targets is set, which a node that is reachable
// on both is often set to. What the bridge dials is a market, polled all day,
// and an exchange that sees this node's address with every poll learns where
// a bridge, and the funds behind it, are. Nothing it reaches needs a direct
// connection.
func bridgeNet(torCfg *lncfg.Tor, nodeNet tor.Net) tor.Net {
	if torCfg == nil || !torCfg.Active ||
		!torCfg.SkipProxyForClearNetTargets {

		return nodeNet
	}

	return &tor.ProxyNet{
		SOCKS:           torCfg.SOCKS,
		DNS:             torCfg.DNS,
		StreamIsolation: torCfg.StreamIsolation,
	}
}

// bridgeBlake2bActivation is this chain's first BLAKE2b block, which the
// bridge's SHA256 node must not have. Pinned on mainnet; read from this node's
// own chain elsewhere, where the height is configured or fixed by the spec.
func (s *server) bridgeBlake2bActivation(_ context.Context) (int32, [32]byte,
	bool, error) {

	params := s.cfg.ActiveNetParams
	height := params.GossipFloor()
	strict := params.Net == wire.MainNet
	if height == 0 {
		return 0, [32]byte{}, strict, nil
	}
	if params.Blake2bActivationHash != nil {
		return int32(height), *params.Blake2bActivationHash, strict, nil
	}

	// A test network this node has not taken past the activation yet:
	// there is no BLAKE2b block to compare with, so nothing to check.
	_, best, err := s.cc.ChainIO.GetBestBlock()
	if err == nil && best < int32(height) && !strict {
		return 0, [32]byte{}, strict, nil
	}

	hash, err := s.cc.ChainIO.GetBlockHash(int64(height))
	if err != nil {
		return 0, [32]byte{}, strict, fmt.Errorf("this node's own "+
			"block at the BLAKE2b activation height %d: %w",
			height, err)
	}

	return int32(height), *hash, strict, nil
}

// bridgeNetworkName is a chain parameters name as lnd's GetInfo reports it,
// which is how the SHA256 node will name its own network.
func bridgeNetworkName(params string) string {
	if params == "testnet3" {
		return "testnet"
	}

	return params
}

// forgetBridgeInvoice deletes a cancelled invoice, the same way the
// DeleteCanceledInvoice RPC does, so the bridge can quote its hash again.
//
// Anything but a cancelled invoice is refused: deleting one that is held or
// was paid would erase the record of real money. A hash the node has never
// seen is already forgotten, which is what makes a retry after a crash safe.
func (s *server) forgetBridgeInvoice(ctx context.Context, hash [32]byte) error {
	inv, err := s.invoices.LookupInvoice(ctx, lntypes.Hash(hash))
	switch {
	case bridgeInvoiceAbsent(err):
		return nil
	case err != nil:
		return err
	case inv.State != invoices.ContractCanceled:
		return fmt.Errorf("bridge invoice %x is %v, and only a "+
			"cancelled invoice may be forgotten", hash, inv.State)
	}

	return s.invoicesDB.DeleteInvoice(ctx, []invoices.InvoiceDeleteRef{{
		PayHash:  lntypes.Hash(hash),
		AddIndex: inv.AddIndex,
	}})
}

// addBridgeHoldInvoice creates a hold invoice on this node.
//
// It is the same path lnrpc/invoicesrpc's AddHoldInvoice takes, with the
// request built here rather than unmarshalled from the wire.
//
// No route hints are attached. That is correct while the bridge is its own
// users' LSP, which is the topology this is built for, and wrong the moment it
// is not: a payer with no public path to this node needs a hint to find one.
// Adding them is a change here plus a source of channel information, not a
// change anywhere above.
func (s *server) addBridgeHoldInvoice(ctx context.Context,
	req bridgerpc.HoldInvoiceRequest) (string, error) {

	hash := lntypes.Hash(req.Hash)

	// The bridge's own adapter rejects these before they reach here. They
	// are checked again because this is the boundary where a mistake stops
	// being recoverable: an invoice for any amount lets the payer choose
	// what the bridge is paid, and one with the node's default CLTV will
	// not outlive the outgoing leg.
	if req.AmountMsat == 0 {
		return "", fmt.Errorf("bridge hold invoice %x: no amount", hash)
	}
	if req.CLTVDelta == 0 {
		return "", fmt.Errorf("bridge hold invoice %x: no CLTV delta",
			hash)
	}
	if req.Expiry <= 0 {
		return "", fmt.Errorf("bridge hold invoice %x: no expiry", hash)
	}

	addInvoiceCfg := &invoicesrpc.AddInvoiceConfig{
		AddInvoice:        s.invoices.AddInvoice,
		IsChannelActive:   s.htlcSwitch.HasActiveLink,
		ChainParams:       s.cfg.ActiveNetParams.Params,
		NodeSigner:        s.nodeSigner,
		DefaultCLTVExpiry: s.cfg.Bitcoin.TimeLockDelta,
		ChanDB:            s.chanStateDB,
		Graph:             s.v1Graph,
		GenInvoiceFeatures: func() *lnwire.FeatureVector {
			return s.featureMgr.Get(feature.SetInvoice)
		},
		GenAmpInvoiceFeatures: func() *lnwire.FeatureVector {
			return s.featureMgr.Get(feature.SetInvoiceAmp)
		},
		GetAlias: s.aliasMgr.GetPeerAlias,
	}

	value := lnwire.MilliSatoshi(req.AmountMsat)
	data := &invoicesrpc.AddInvoiceData{
		Memo:        req.Memo,
		Hash:        &hash,
		Value:       value,
		Expiry:      int64(req.Expiry.Seconds()),
		CltvExpiry:  uint64(req.CLTVDelta),
		HodlInvoice: true,
		Metadata:    req.Metadata,

		// The preimage is deliberately absent: it belongs to whoever
		// issued the invoice on the other chain, and the entire point
		// of the swap is that this node cannot claim without being
		// handed it.
		Preimage: nil,
	}

	_, dbInvoice, err := invoicesrpc.AddInvoice(ctx, addInvoiceCfg, data)
	if err != nil {
		return "", err
	}

	return string(dbInvoice.PaymentRequest), nil
}

// payBridgeInvoice sends a payment and blocks until it resolves.
//
// The intent is built by the router backend's own ExtractIntent rather than
// assembled here, so the bridge gets exactly the rules an RPC client gets: the
// invoice's expiry is validated, a payment address or blinded paths are
// required, the amount comes from the invoice, and whether it may be split is
// decided by the invoice's own features. A second reading of a payment request
// that disagreed by one field would have the bridge hold one hash while the
// node paid another, and the whole security of a swap is that those are equal.
func (s *server) payBridgeInvoice(ctx context.Context,
	routerBackend *routerrpc.RouterBackend,
	req bridgerpc.PayRequest) (bridgerpc.PaymentStatus, error) {

	if req.Invoice == "" {
		return bridgerpc.PaymentStatus{}, errors.New("bridge payment: " +
			"no invoice to pay")
	}
	if req.CLTVLimit == 0 {
		return bridgerpc.PaymentStatus{}, errors.New("bridge payment: " +
			"no CLTV limit; the incoming leg is sized against " +
			"this and an unbounded route could outlive it")
	}
	if req.CLTVLimit > math.MaxInt32 {
		return bridgerpc.PaymentStatus{}, fmt.Errorf("bridge payment: "+
			"CLTV limit %d does not fit", req.CLTVLimit)
	}
	if req.Timeout <= 0 {
		return bridgerpc.PaymentStatus{}, errors.New("bridge payment: " +
			"no timeout; an attempt that never gives up holds the " +
			"incoming HTLC until it expires")
	}

	intent, err := routerBackend.ExtractIntent(
		&routerrpc.SendPaymentRequest{
			PaymentRequest: req.Invoice,
			TimeoutSeconds: int32(req.Timeout.Seconds()),
			FeeLimitMsat:   int64(req.MaxFeeMsat),
			CltvLimit:      int32(req.CLTVLimit),
		},
	)
	if err != nil {
		return bridgerpc.PaymentStatus{}, fmt.Errorf("%w: %v",
			bridgerpc.ErrPaymentNotSent, err)
	}

	recorded := func() bool {
		return s.bridgePaymentRecorded(ctx, intent.Identifier())
	}
	sent := awaitBridgePayment(ctx, func() bridgeSendResult {
		pre, rt, err := s.chanRouter.SendPayment(ctx, intent)

		return bridgeSendResult{preimage: pre, route: rt, err: err}
	}, recorded, bridgeRecordPoll)
	if sent.left {
		// The bridge is stopping and the payment is recorded, so the
		// router finishes it and a lookup finds how. Waiting here
		// would hold the stop for as long as the payee holds the HTLC.
		srvrLog.Infof("Bridge leaving payment %x to the router while "+
			"it stops; it is recorded, and looked up on resume",
			intent.Identifier())

		return bridgerpc.PaymentStatus{Known: true, InFlight: true}, nil
	}
	preimage, route, err := sent.preimage, sent.route, sent.err
	if err != nil && !recorded() {
		// The router writes its record before it sends anything, so a
		// payment it holds no record of after the call has returned
		// never left: it was refused (a blinded path it could not use,
		// for one) and nothing is in flight.
		return bridgerpc.PaymentStatus{}, fmt.Errorf("%w: %v",
			bridgerpc.ErrPaymentNotSent, err)
	}
	if err != nil {
		// Not a failure, and this is the distinction the whole design
		// turns on. The router gives up on its own deadline, but an
		// HTLC it already sent is out there regardless, and concluding
		// failure here would cancel the incoming claim against a
		// payment that may still settle. Report what is actually
		// known: the node has a record and its outcome is not decided.
		//
		// The bridge resolves this by looking the payment up, which is
		// the only thing that can answer it.
		srvrLog.Debugf("Bridge payment did not resolve in time or "+
			"failed to dispatch, reporting in flight: %v", err)

		return bridgerpc.PaymentStatus{Known: true, InFlight: true}, nil
	}

	status := bridgerpc.PaymentStatus{
		Known: true, Settled: true, Preimage: preimage,
	}
	if route != nil {
		status.FeeMsat = uint64(route.TotalFees())
	}

	return status, nil
}

// bridgeRecordPoll is how often a stopping bridge asks whether its payment
// has been recorded yet, while the router is still setting it up.
const bridgeRecordPoll = 100 * time.Millisecond

// bridgeSendResult is what sending a bridge payment came to. left is set
// instead of the rest when the bridge stopped waiting (see awaitBridgePayment).
type bridgeSendResult struct {
	preimage [32]byte
	route    *route.Route
	err      error
	left     bool
}

// awaitBridgePayment runs send and waits for its answer, unless ctx ends
// first. Then it stops waiting as soon as recorded says the router holds a
// record of the payment, and reports only that it left.
//
// The router waits for every HTLC it has sent to resolve, whatever its context
// says, and a payee holding one can keep it for hours: the bridge's stop, and
// so a rebuild, would wait as long. Once the payment is recorded, nothing
// about it depends on this call: the router finishes it, or resumes it after
// a restart, and the bridge's resume looks it up. Before it is recorded, the
// call is still deciding whether to send it at all, which is quick, and its
// answer is needed: a refusal there is a payment that never left.
func awaitBridgePayment(ctx context.Context, send func() bridgeSendResult,
	recorded func() bool, poll time.Duration) bridgeSendResult {

	done := make(chan bridgeSendResult, 1)
	go func() {
		done <- send()
	}()

	select {
	case r := <-done:
		return r
	case <-ctx.Done():
	}

	tick := time.NewTicker(poll)
	defer tick.Stop()
	for {
		// An answer already in wins over a record: it says what a
		// lookup would.
		select {
		case r := <-done:
			return r
		default:
		}
		if recorded() {
			return bridgeSendResult{left: true}
		}

		select {
		case r := <-done:
			return r
		case <-tick.C:
		}
	}
}

// bridgePaymentRecorded reports whether the router holds a record of a
// payment. Anything but a clear "never initiated" counts as recorded, so a
// lookup that fails keeps the payment in flight.
//
// It is asked after SendPayment returns, whose context may have ended with
// it (the bridge shutting down, or the payment's deadline), so the lookup
// has a context of its own: one ended already would fail the lookup and
// leave a payment that never left reading as in flight until an operator
// looks.
func (s *server) bridgePaymentRecorded(_ context.Context,
	hash [32]byte) bool {

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	_, err := s.controlTower.FetchPayment(ctx, lntypes.Hash(hash))

	return !errors.Is(err, paymentsdb.ErrPaymentNotInitiated)
}

// lookupBridgePayment reports what became of a payment, keeping "never seen"
// distinct from "failed".
//
// Those two are the same shape to most callers and must not be here. After a
// restart the bridge asks this about every unfinished swap, and the answer
// decides whether it may cancel the incoming claim. Reading "I have no record"
// as "it failed" would cancel a claim against an HTLC that is still in flight.
func (s *server) lookupBridgePayment(ctx context.Context, hash [32]byte) (
	bridgerpc.PaymentStatus, error) {

	payment, err := s.controlTower.FetchPayment(ctx, lntypes.Hash(hash))
	if err != nil {
		// Never initiated is an answer, not a failure: it is how the
		// bridge tells a swap it never paid out from one it paid and
		// lost track of, and the difference decides whether it may
		// cancel the incoming claim.
		if errors.Is(err, paymentsdb.ErrPaymentNotInitiated) {
			return bridgerpc.TranslatePayment(nil), nil
		}

		return bridgerpc.PaymentStatus{}, err
	}

	return bridgerpc.TranslatePayment(payment), nil
}

// bridgeBestBlock is the tip this node sees, when it was mined, and whether
// this node has caught up with it.
//
// The height comes from the chain backend rather than from the wallet's view,
// so it is the freshest number available, and the sync flag says whether this
// node's own view has caught up with it. The bridge refuses to size anything
// while that is false: every margin decision is a comparison against this
// height, and a stale one says an HTLC has more time left than it does, which
// is the direction that pays out against a claim that can no longer be
// collected.
//
// The timestamp is the block's own, not when this node heard about it. Block
// spacing is measured from it, and a node catching up sees a hundred blocks in
// a minute, which local arrival times would read as a chain running a hundred
// times too fast.
func (s *server) bridgeBestBlock(_ context.Context) (bridgerpc.BlockInfo,
	error) {

	_, bestHeight, err := s.cc.ChainIO.GetBestBlock()
	if err != nil {
		return bridgerpc.BlockInfo{}, fmt.Errorf("reading the best "+
			"block: %w", err)
	}

	info := bridgerpc.BlockInfo{Height: bestHeight}

	walletSynced, bestHeaderTimestamp, err := s.cc.Wallet.IsSynced()
	if err != nil {
		return info, fmt.Errorf("reading wallet sync state: %w", err)
	}
	if bestHeaderTimestamp > 0 {
		info.Time = time.Unix(bestHeaderTimestamp, 0)
	}
	if !walletSynced {
		return info, nil
	}

	// The router validates each block's channels when it is not assuming
	// them valid, so it can lag the wallet under load. Waiting for it too
	// matches what GetInfo reports as synced_to_chain, which is what the
	// out-of-process adapter checks, and the two must not disagree about
	// whether this node is ready to price a swap.
	if !s.cfg.Routing.AssumeChannelValid {
		if uint32(bestHeight) != s.graphBuilder.SyncedHeight() {
			return info, nil
		}
	}

	info.SyncedToChain = true

	return info, nil
}

// bridgeBlockAt is a block's header by height, for seeding the chain observer.
//
// Straight to the chain backend, which is the only thing here that has the
// history. The alternative is learning block spacing from tips as they arrive,
// which takes a hundred blocks before the bridge will quote at all.
func (s *server) bridgeBlockAt(_ context.Context, height int32) (
	bridgerpc.BlockInfo, error) {

	hash, err := s.cc.ChainIO.GetBlockHash(int64(height))
	if err != nil {
		return bridgerpc.BlockInfo{}, fmt.Errorf("the hash at height "+
			"%d: %w", height, err)
	}

	// The header alone: the timestamp and the difficulty are all the
	// observer reads, and a neutrino backend would otherwise download each
	// whole block.
	header, err := s.cc.ChainIO.GetBlockHeader(hash)
	if err != nil {
		return bridgerpc.BlockInfo{}, fmt.Errorf("the header at height "+
			"%d: %w", height, err)
	}

	return bridgerpc.BlockInfo{
		Height: height,
		Time:   header.Timestamp,
		Bits:   header.Bits,

		// Historical, so by definition already in the chain.
		SyncedToChain: true,
	}, nil
}

// bridgeChannelBalance is what this node can still send over its channels.
//
// Local balance rather than total capacity: the inventory policy prices how
// drained the paying side is, and what the far end holds is not something this
// node can spend.
func (s *server) bridgeChannelBalance(_ context.Context) (uint64, error) {
	channels, err := s.chanStateDB.FetchAllOpenChannels()
	if err != nil {
		return 0, fmt.Errorf("reading open channels: %w", err)
	}

	return spendableOutbound(channels, s.htlcSwitch.HasActiveLink), nil
}

// spendableOutbound is what a set of channels can actually send.
//
// Split from its caller so the rule can be tested without a node. It is the
// same rule the SHA256 side applies to its own channels, and the two must
// agree: a bridge that measures its two sides differently prices one of them
// against a quantity the other does not mean.
func spendableOutbound(channels []*channeldb.OpenChannel,
	active func(lnwire.ChannelID) bool) uint64 {

	var outbound uint64
	for _, channel := range channels {
		if channel == nil || channel.IsPending {
			continue
		}

		// Only channels with a live link can carry a payment, so a
		// peer that is offline holds balance the bridge cannot use.
		// Counting it would have the inventory policy think it is
		// better funded than it is, and quote a spread too thin for
		// what it can actually do.
		chanID := lnwire.NewChanIDFromOutPoint(channel.FundingOutpoint)
		if active != nil && !active(chanID) {
			continue
		}

		// The channel reserve is the operator's own balance but is not
		// spendable: the commitment must leave it behind. Counting it
		// lets the bridge quote a swap it cannot pay, which fails
		// safely but wastes an HTLC and the payer's time.
		reserve := lnwire.NewMSatFromSatoshis(
			channel.LocalChanCfg.ChanReserve,
		)
		balance := spliceSafeLocalBalance(channel)
		if balance <= reserve {
			continue
		}

		outbound += uint64(balance - reserve)
	}

	return outbound
}

// spliceSafeLocalBalance is the channel's local balance, or less while a
// splice that moves funds out of it is pending: until it locks, a payment
// must fit on every funding, so the least of them is what can be spent. A
// channel whose splices can't be read counts as empty.
func spliceSafeLocalBalance(channel *channeldb.OpenChannel) lnwire.MilliSatoshi {
	balance := channel.LocalCommitment.LocalBalance

	records, err := channel.SpliceRecords()
	if err != nil {
		srvrLog.Errorf("ChannelPoint(%v): unable to read pending "+
			"splices: %v", channel.FundingOutpoint, err)

		return 0
	}

	return leastLocalBalance(balance, records)
}

// leastLocalBalance is balance after the largest outflow of any pending
// splice.
func leastLocalBalance(balance lnwire.MilliSatoshi,
	records []*channeldb.SpliceRecord) lnwire.MilliSatoshi {

	least := balance
	for _, r := range records {
		if r.Retired || r.LocalDelta >= 0 {
			continue
		}
		out := lnwire.NewMSatFromSatoshis(-r.LocalDelta)
		if out >= balance {
			return 0
		}
		if balance-out < least {
			least = balance - out
		}
	}

	return least
}

// deriveSha256Seed is the supervised SHA256 node's seed entropy: HKDF over the
// private key this wallet holds at the bridge's own key family (see
// lnrpc/bridgerpc/seed.go and docs/bridge-sha256-node.md).
//
// A remote signer holds no private keys here, and refusing is the right answer
// for it: the derivation is only reproducible from the wallet that has them.
func (s *server) deriveSha256Seed() ([aezeed.EntropySize]byte, error) {
	var none [aezeed.EntropySize]byte

	priv, err := s.cc.KeyRing.DerivePrivKey(keychain.KeyDescriptor{
		KeyLocator: keychain.KeyLocator{
			Family: bridgerpc.Sha256SeedFamily,
			Index:  bridgerpc.Sha256SeedIndex,
		},
	})
	if err != nil {
		return none, fmt.Errorf("deriving the key the SHA256 node's "+
			"seed comes from (a remote signer cannot run a "+
			"supervised SHA256 node): %w", err)
	}

	return bridgerpc.Sha256SeedEntropy(priv)
}

// bridgeInvoiceAbsent reports whether a lookup failed because the node has no
// such invoice.
//
// Two errors mean that. A node that has never created an invoice answers every
// lookup with ErrNoInvoicesCreated rather than ErrInvoiceNotFound, so a bridge
// on a new node read its first quote's check for an earlier invoice as a
// failure, and refused every swap until something else had made an invoice.
func bridgeInvoiceAbsent(err error) bool {
	return errors.Is(err, invoices.ErrInvoiceNotFound) ||
		errors.Is(err, invoices.ErrNoInvoicesCreated)
}
