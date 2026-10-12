package lnd

import (
	"context"
	"errors"
	"fmt"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/channelnotifier"
	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lnwallet"
	"github.com/lightningnetwork/lnd/lnwallet/chainfee"
	"github.com/lightningnetwork/lnd/lnwallet/splice"
	"github.com/lightningnetwork/lnd/lnwire"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Splice status codes of a channel, as Channel.splice_status reports them.
const (
	spliceStatusOK              = "ok"
	spliceStatusDisabled        = "disabled"
	spliceStatusPeerOffline     = "peer_offline"
	spliceStatusPeerUnsupported = "peer_unsupported"
	spliceStatusNoUnifiedSigs   = "no_unified_sigs"
	spliceStatusTaproot         = "taproot"
	spliceStatusZeroConf        = "zero_conf"
	spliceStatusPending         = "pending"
	spliceStatusClosing         = "closing"
	spliceStatusSpentUnknown    = "funding_spent_unknown"
)

// spliceChannel finds a channel by its current or original channel point.
func (r *rpcServer) spliceChannel(
	cp *lnrpc.ChannelPoint) (*channeldb.OpenChannel, error) {

	if cp == nil {
		return nil, status.Error(codes.InvalidArgument,
			"channel point required")
	}
	txid, err := lnrpc.GetChanPointFundingTxid(cp)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	op := wire.OutPoint{Hash: *txid, Index: cp.OutputIndex}

	ch, err := r.server.chanStateDB.FetchChannel(op)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "channel %v: %v",
			op, err)
	}

	return ch, nil
}

// spliceStatus returns whether this node can start a splice of the channel
// now, and its splice machine when it can.
func (r *rpcServer) spliceStatus(
	ch *channeldb.OpenChannel) (string, *splice.Machine) {

	pending := false
	if records, err := ch.SpliceRecords(); err == nil {
		for _, rec := range records {
			if !rec.Retired {
				pending = true
			}
		}
	}

	switch {
	case ch.HasChanStatus(channeldb.ChanStatusFundingSpentUnknown):
		return spliceStatusSpentUnknown, nil

	case ch.HasChanStatus(channeldb.ChanStatusCoopBroadcasted),
		ch.HasChanStatus(channeldb.ChanStatusLocalCloseInitiator),
		ch.HasChanStatus(channeldb.ChanStatusRemoteCloseInitiator):

		return spliceStatusClosing, nil

	case !ch.ChanType.HasUnifiedSigs():
		return spliceStatusNoUnifiedSigs, nil

	case ch.ChanType.IsTaproot():
		return spliceStatusTaproot, nil

	case ch.ChanType.HasZeroConf():
		return spliceStatusZeroConf, nil

	case pending:
		return spliceStatusPending, nil

	case !r.cfg.ProtocolOptions.Splice():
		return spliceStatusDisabled, nil
	}

	p, err := r.server.FindPeer(ch.IdentityPub)
	if err != nil {
		return spliceStatusPeerOffline, nil
	}
	chanID := lnwire.NewChanIDFromOutPoint(ch.FundingOutpoint)
	m, ok := p.SpliceMachine(chanID)
	if !ok {
		return spliceStatusPeerUnsupported, nil
	}

	return spliceStatusOK, m
}

// spliceFeeRate returns the fee rate a request asks for: a rate, or a
// confirmation target (6 when neither is given).
func (r *rpcServer) spliceFeeRate(satPerVByte uint64,
	targetConf int32) (chainfee.SatPerKWeight, error) {

	if satPerVByte > 0 {
		return chainfee.SatPerKVByte(satPerVByte * 1000).FeePerKWeight(),
			nil
	}
	if targetConf <= 0 {
		targetConf = 6
	}

	return r.server.cc.FeeEstimator.EstimateFeePerKW(uint32(targetConf))
}

// spliceDestination returns where funds moved out go: the address asked
// for, which must be a segwit v0 or v1 address on this chain, or a new one
// of the wallet's. It reports whether the destination is the wallet's own.
func (r *rpcServer) spliceDestination(addr string) ([]byte, bool, error) {
	wallet := r.server.cc.Wallet
	if addr == "" {
		a, err := wallet.NewAddress(
			lnwallet.TaprootPubkey, false,
			lnwallet.DefaultAccountName,
		)
		if err != nil {
			return nil, false, err
		}
		script, err := txscript.PayToAddrScript(a)

		return script, true, err
	}

	a, err := btcutil.DecodeAddress(addr, r.cfg.ActiveNetParams.Params)
	if err != nil {
		return nil, false, status.Errorf(codes.InvalidArgument,
			"address: %v", err)
	}
	if !a.IsForNet(r.cfg.ActiveNetParams.Params) {
		return nil, false, status.Error(codes.InvalidArgument,
			"address of another network")
	}
	switch a.(type) {
	case *btcutil.AddressWitnessPubKeyHash,
		*btcutil.AddressWitnessScriptHash, *btcutil.AddressTaproot:

	default:
		return nil, false, status.Error(codes.InvalidArgument,
			"only segwit v0 and v1 addresses")
	}
	script, err := txscript.PayToAddrScript(a)

	return script, wallet.IsOurAddress(a), err
}

// spliceRequest turns the RPC's request into the machine's.
func (r *rpcServer) spliceRequest(direction lnrpc.SpliceDirection,
	amount int64, useMax bool, satPerVByte uint64, targetConf int32,
	addr string, force bool) (splice.Request, bool, error) {

	req := splice.Request{
		Amount: btcutil.Amount(amount), UseMax: useMax,
		ForceLowFee: force,
	}
	if amount < 0 {
		return req, false, status.Error(codes.InvalidArgument,
			"negative amount")
	}

	var err error
	req.FeeRate, err = r.spliceFeeRate(satPerVByte, targetConf)
	if err != nil {
		return req, false, err
	}

	ours := false
	switch direction {
	case lnrpc.SpliceDirection_SPLICE_IN:
		req.Direction = splice.DirectionIn

	case lnrpc.SpliceDirection_SPLICE_OUT:
		req.Direction = splice.DirectionOut
		req.Destination, ours, err = r.spliceDestination(addr)
		if err != nil {
			return req, false, err
		}

	default:
		return req, false, status.Error(codes.InvalidArgument,
			"unknown direction")
	}

	return req, ours, nil
}

// EstimateSplice reports what a splice of the channel this node would start
// would cost and allow. Purely local: the peer isn't contacted and nothing
// is leased.
func (r *rpcServer) EstimateSplice(_ context.Context,
	in *lnrpc.EstimateSpliceRequest) (*lnrpc.EstimateSpliceResponse,
	error) {

	ch, err := r.spliceChannel(in.ChannelPoint)
	if err != nil {
		return nil, err
	}
	code, _ := r.spliceStatus(ch)
	refusal := ""
	if code != spliceStatusOK {
		refusal = code
	}

	// The same fee rate the splice would use, and the destination it
	// would pay; an estimate of funds out to our wallet doesn't make a
	// fresh address, a P2TR placeholder weighs the same.
	feeRate, err := r.spliceFeeRate(in.SatPerVbyte, in.TargetConf)
	if err != nil {
		return nil, err
	}
	req := splice.Request{
		Amount: btcutil.Amount(in.AmountSat), UseMax: in.UseMax,
		FeeRate: feeRate,
	}
	ours := false
	switch in.Direction {
	case lnrpc.SpliceDirection_SPLICE_IN:
		req.Direction = splice.DirectionIn

	case lnrpc.SpliceDirection_SPLICE_OUT:
		req.Direction = splice.DirectionOut
		if in.Addr == "" {
			req.Destination = make([]byte, 34)
			req.Destination[0], req.Destination[1] = 0x51, 0x20
			ours = true
		} else {
			req.Destination, ours, err = r.spliceDestination(
				in.Addr,
			)
			if err != nil {
				return nil, err
			}
		}

	default:
		return nil, status.Error(codes.InvalidArgument,
			"unknown direction")
	}

	wallet := r.server.cc.Wallet
	var coins []splice.Coin
	err = wallet.WithCoinSelectLock(func() error {
		var err error
		coins, err = splice.ListCoins(wallet.WalletController)

		return err
	})
	if err != nil {
		return nil, err
	}

	var available btcutil.Amount
	chanID := lnwire.NewChanIDFromOutPoint(ch.FundingOutpoint)
	if link, err := r.server.htlcSwitch.GetLink(chanID); err == nil {
		available = link.Bandwidth().ToSatoshis()
	}

	est, err := splice.EstimateSplice(splice.EstimateRequest{
		Request:      req,
		Capacity:     ch.Capacity,
		LocalBalance: ch.LocalCommitment.LocalBalance.ToSatoshis(),
		Available:    available,
		Coins:        coins,
		ChangeScript: make([]byte, 22),
		DustLimit: func(script []byte) btcutil.Amount {
			return lnwallet.DustLimitForSize(len(script))
		},
		Estimator: r.server.cc.FeeEstimator,
		Refusal:   refusal,
	})
	if err != nil {
		return nil, err
	}

	numAnchors, err := wallet.CurrentNumAnchorChans()
	if err != nil {
		return nil, err
	}

	return &lnrpc.EstimateSpliceResponse{
		FeeSat:            int64(est.Fee),
		Vbytes:            (est.Weight + 3) / 4,
		SatPerVbyte:       uint64(feeRate.FeePerVByte()),
		AmountSat:         int64(est.Amount),
		MinSat:            int64(est.Min),
		MaxInSat:          int64(est.MaxIn),
		MaxOutSat:         int64(est.MaxOut),
		MinSatPerVbyte:    uint64(est.Floor.Rate.FeePerVByte()),
		FeeEstimateSource: string(est.Floor.Source),
		AnchorReserveSat: int64(wallet.RequiredReserve(
			uint32(numAnchors),
		)),
		NewCapacitySat:    int64(est.NewCapacity),
		NewLocalSat:       int64(est.NewLocal),
		LockDepth:         max(6, uint32(ch.NumConfsRequired)),
		DestinationIsOurs: ours,
		RefusalCode:       est.RefusalCode,
	}, nil
}

// SpliceChannel starts a splice of the channel and streams its progress.
// The splice doesn't depend on the stream: if the stream ends before the
// construction is done the splice is canceled cleanly; after, it carries on.
func (r *rpcServer) SpliceChannel(in *lnrpc.SpliceChannelRequest,
	stream lnrpc.Lightning_SpliceChannelServer) error {

	ch, err := r.spliceChannel(in.ChannelPoint)
	if err != nil {
		return err
	}
	code, machine := r.spliceStatus(ch)
	if machine == nil {
		return status.Errorf(codes.FailedPrecondition, "%s", code)
	}

	req, _, err := r.spliceRequest(
		in.Direction, in.AmountSat, in.UseMax, in.SatPerVbyte,
		in.TargetConf, in.Addr, in.ForceLowFee,
	)
	if err != nil {
		return err
	}

	updates, err := machine.Splice(req)
	if err != nil {
		if errors.Is(err, splice.ErrRefused) {
			return status.Error(codes.FailedPrecondition,
				err.Error())
		}

		return err
	}

	for {
		select {
		case u, ok := <-updates:
			if !ok {
				return nil
			}
			msg := &lnrpc.SpliceUpdate{
				State:          string(u.State),
				Reason:         u.Reason,
				ReasonFromPeer: u.ReasonFromPeer,
			}
			if u.Txid != [32]byte{} {
				msg.Txid = u.Txid.String()
				msg.NewChannelPoint = fmt.Sprintf("%v:%d",
					u.Txid, u.FundingIndex)
			}
			if err := stream.Send(msg); err != nil {
				machine.Cancel()
				return err
			}

		case <-stream.Context().Done():
			machine.Cancel()
			return stream.Context().Err()

		case <-r.quit:
			return nil
		}
	}
}

// addSpliceFields fills a channel listing's splice fields: the current
// funding output as its channel point and the original beside it, whether a
// splice can start, the splice in progress, the last that ended, and the
// local balance as the least over every funding while one is pending.
func (r *rpcServer) addSpliceFields(ch *channeldb.OpenChannel,
	channel *lnrpc.Channel) {

	channel.ChannelPoint = ch.CurrentFundingOutpoint().String()
	channel.OriginalChannelPoint = ch.FundingOutpoint.String()

	// The scid the graph and new route hints know the channel by.
	// Forwarding records keep the one it was opened with.
	channel.OriginalChanId = ch.ShortChanID().ToUint64()
	if current := ch.CurrentShortChanID(); current != ch.ShortChanID() {
		channel.ChanId = current.ToUint64()
	}
	channel.SpliceStatus, _ = r.spliceStatus(ch)

	records, err := ch.SpliceRecords()
	if err != nil {
		rpcsLog.Errorf("ChannelPoint(%v): unable to read splices: %v",
			ch.FundingOutpoint, err)
	}
	for _, rec := range records {
		if rec.Retired {
			continue
		}
		channel.PendingSplice = rpcPendingSplice(rec)

		// What we can count on is the least on any funding.
		if rec.LocalCommit != nil {
			local := int64(rec.LocalCommit.LocalBalance.ToSatoshis())
			if local < channel.LocalBalance {
				channel.LocalBalance = local
			}
		}
	}

	last, err := ch.LastSplice()
	if err == nil && last != nil {
		initiator := lnrpc.Initiator_INITIATOR_REMOTE
		if last.WeInitiated {
			initiator = lnrpc.Initiator_INITIATOR_LOCAL
		}
		channel.LastSplice = &lnrpc.LastSplice{
			Initiator:      initiator,
			LocalDeltaSat:  int64(last.LocalDelta),
			RemoteDeltaSat: int64(last.RemoteDelta),
			Txid:           last.Txid.String(),
			Time:           last.Time,
			Outcome:        last.Outcome,
			Reason:         last.Reason,
			ReasonFromPeer: last.ReasonFromPeer,
		}
		if last.Txid == [32]byte{} {
			channel.LastSplice.Txid = ""
		}
	}
}

// rpcPendingSplice describes a pending splice from its record.
func rpcPendingSplice(rec *channeldb.SpliceRecord) *lnrpc.PendingSplice {
	ps := &lnrpc.PendingSplice{
		Initiator:       lnrpc.Initiator_INITIATOR_REMOTE,
		State:           "negotiating",
		LocalDeltaSat:   int64(rec.LocalDelta),
		RemoteDeltaSat:  int64(rec.RemoteDelta),
		Txid:            rec.Txid().String(),
		NewChannelPoint: rec.OutPoint.String(),
		NewCapacitySat:  int64(rec.Capacity),
	}

	n := rec.Negotiation
	if n == nil {
		return ps
	}
	if n.WeInitiated {
		ps.Initiator = lnrpc.Initiator_INITIATOR_LOCAL
	}
	ps.SatPerVbyte = uint64(chainfee.SatPerKWeight(n.FeeRate).FeePerVByte())
	ps.BroadcastHeight = n.BroadcastHeight
	ps.WeLocked = n.LockedSent
	ps.PeerLocked = n.LockedReceived

	switch {
	case n.LockedSent:
		ps.State = "confirmed"
	case n.OurTxSignatures != nil && n.TheirTxSignatures != nil:
		ps.State = "broadcast"
	case n.OurTxSignatures != nil || rec.SpliceSig != nil:
		ps.State = "signed"
	}

	// The fee: what the inputs spend less what the outputs pay.
	if n.Tx != nil {
		var in, out int64
		for _, i := range n.Inputs {
			in += int64(i.Value)
		}
		for _, o := range n.Tx.TxOut {
			out += int64(o.Value)
		}
		if in > out {
			ps.FeeSat = in - out
		}
		if n.WeInitiated {
			ps.OurFeeSat = ps.FeeSat
		}
	}

	return ps
}

// addSpliceBalances adds the splice amounts to a channel balance: our
// contributions to splices that haven't locked (in a channel once they do),
// our wallet outputs in splices not yet confirmed, and the local balance as
// the least over every funding of a channel with a splice pending.
func (r *rpcServer) addSpliceBalances(channels []*channeldb.OpenChannel,
	resp *lnrpc.ChannelBalanceResponse) {

	var (
		in, toWallet btcutil.Amount
		lower        lnwire.MilliSatoshi
	)
	for _, ch := range channels {
		records, err := ch.SpliceRecords()
		if err != nil {
			rpcsLog.Errorf("ChannelPoint(%v): unable to read "+
				"splices: %v", ch.FundingOutpoint, err)
			continue
		}

		var least lnwire.MilliSatoshi
		for _, rec := range records {
			if rec.Retired {
				continue
			}
			if rec.LocalDelta > 0 {
				in += rec.LocalDelta
			}
			if rec.LocalCommit != nil {
				local := ch.LocalCommitment.LocalBalance
				cand := rec.LocalCommit.LocalBalance
				if cand < local && local-cand > least {
					least = local - cand
				}
			}
			if rec.LocalDelta < 0 && rec.Negotiation != nil &&
				rec.Negotiation.Tx != nil {

				toWallet += r.ourOutputs(
					rec.Negotiation.Tx, rec.OutPoint.Index,
				)
			}
		}
		lower += least
	}

	resp.PendingSpliceInSat = int64(in)
	resp.PendingSpliceToWalletSat = int64(toWallet)
	if lower > 0 && resp.LocalBalance != nil &&
		lnwire.MilliSatoshi(resp.LocalBalance.Msat) >= lower {

		local := lnwire.MilliSatoshi(resp.LocalBalance.Msat) - lower
		resp.LocalBalance = &lnrpc.Amount{
			Sat: uint64(local.ToSatoshis()), Msat: uint64(local),
		}
		resp.Balance = int64(local.ToSatoshis())
	}
}

// ourOutputs sums the outputs of a splice transaction paying this node's
// wallet, the funding output aside.
func (r *rpcServer) ourOutputs(tx *wire.MsgTx,
	fundingIndex uint32) btcutil.Amount {

	var sum btcutil.Amount
	for i, out := range tx.TxOut {
		if uint32(i) == fundingIndex {
			continue
		}
		_, addrs, _, err := txscript.ExtractPkScriptAddrs(
			out.PkScript, r.cfg.ActiveNetParams.Params,
		)
		if err != nil || len(addrs) != 1 {
			continue
		}
		if r.server.cc.Wallet.IsOurAddress(addrs[0]) {
			sum += btcutil.Amount(out.Value)
		}
	}

	return sum
}

// rpcSpliceEvent describes a splice step for the channel event stream.
func rpcSpliceEvent(e channelnotifier.SpliceEvent) *lnrpc.ChannelEventUpdate {
	txid := ""
	if e.Txid != [32]byte{} {
		txid = e.Txid.String()
	}

	return &lnrpc.ChannelEventUpdate{
		Type: lnrpc.ChannelEventUpdate_SPLICE_EVENT,
		Channel: &lnrpc.ChannelEventUpdate_SpliceEvent{
			SpliceEvent: &lnrpc.SpliceEvent{
				ChannelPoint:         e.ChannelPoint.String(),
				OriginalChannelPoint: e.OriginalChannelPoint.String(),
				Kind:                 e.Kind,
				Txid:                 txid,
				Reason:               e.Reason,
				ReasonFromPeer:       e.ReasonFromPeer,
			},
		},
	}
}

// currentFunding returns the funding output a channel named by either of its
// outpoints is on now, which its graph edge names; op itself for anything
// else.
func (r *rpcServer) currentFunding(op wire.OutPoint) wire.OutPoint {
	ch, err := r.server.chanStateDB.FetchChannel(op)
	if err != nil {
		return op
	}

	return ch.CurrentFundingOutpoint()
}
