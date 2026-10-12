package lnd

import (
	"errors"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/chainntnfs"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/channelnotifier"
	"github.com/lightningnetwork/lnd/lnwallet"
	"github.com/lightningnetwork/lnd/lnwallet/splice"
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
		s.channelNotifier.NotifySpliceEvent(channelnotifier.SpliceEvent{
			ChannelPoint:         state.CurrentFundingOutpoint(),
			OriginalChannelPoint: state.FundingOutpoint,
			Kind:                 kind,
			Txid:                 txid,
			Reason:               reason,
			ReasonFromPeer:       fromPeer,
		})
	}

	return splice.NewMachine(cfg)
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
