package lnwallet

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/fn/v2"
	"github.com/lightningnetwork/lnd/input"
	"github.com/lightningnetwork/lnd/keychain"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/lnwire"
)

// A splice moves a channel to a new funding output. From the moment the
// splice's commitments are signed until it locks, every commitment exists
// once per funding output: the current one and each pending splice's. They
// share the HTLCs, the fee rate, the commitment number and point; they differ
// in the funding input, the capacity and the two balances, which move by the
// splice's contributions. A pending splice's commitments are rebuilt from the
// current funding's, so the two can't drift apart.

// MaxPendingFundings is how many splices of one channel may be pending at
// once: a batch carries at most lnwire.MaxBatchSize signatures, one of them
// for the current funding.
const MaxPendingFundings = lnwire.MaxBatchSize - 1

var (
	// ErrNotQuiescent is returned when the splice's own commitments are
	// exchanged while either commitment chain has an unacknowledged
	// state or updates are waiting to be signed.
	ErrNotQuiescent = errors.New("channel has updates in flight")

	// ErrUnknownFunding is returned for a funding txid that is neither
	// the channel's nor a pending splice's.
	ErrUnknownFunding = errors.New("unknown funding transaction")
)

// commitFunding is the funding output a commitment spends.
type commitFunding struct {
	// txIn spends the funding output.
	txIn wire.TxIn

	// capacity is the funding output's value.
	capacity btcutil.Amount

	// localDelta and remoteDelta are the changes a pending splice makes
	// to each side's balance, in millisatoshis; zero for the current
	// funding.
	localDelta, remoteDelta int64
}

// applyDeltas moves pre-fee balances by the funding's deltas. A balance that
// would go negative is an error: the splice took more than the side had.
func (f commitFunding) applyDeltas(ours,
	theirs lnwire.MilliSatoshi) (lnwire.MilliSatoshi, lnwire.MilliSatoshi,
	error) {

	apply := func(b lnwire.MilliSatoshi, delta int64,
		who string) (lnwire.MilliSatoshi, error) {

		if b > math.MaxInt64 {
			return 0, fmt.Errorf("%s balance out of range", who)
		}
		v := int64(b) + delta
		if (delta > 0 && v < int64(b)) || v < 0 {
			return 0, fmt.Errorf("%w: %s balance %v moved by %d msat",
				ErrBelowChanReserve, who, b, delta)
		}

		return lnwire.MilliSatoshi(v), nil
	}

	ours, err := apply(ours, f.localDelta, "our")
	if err != nil {
		return 0, 0, err
	}
	theirs, err = apply(theirs, f.remoteDelta, "their")
	if err != nil {
		return 0, 0, err
	}

	return ours, theirs, nil
}

// currentCommitFunding returns the channel's current funding output.
func (lc *LightningChannel) currentCommitFunding() commitFunding {
	return commitFunding{
		txIn:     fundingTxIn(lc.channelState),
		capacity: lc.channelState.Capacity,
	}
}

// PendingFunding is a splice's funding output, which the channel moves to
// once the splice locks.
type PendingFunding struct {
	// OutPoint is the new funding output.
	OutPoint wire.OutPoint

	// Capacity is its value: the current capacity plus both deltas.
	Capacity btcutil.Amount

	// LocalKey is our key in its 2-of-2 script.
	LocalKey keychain.KeyDescriptor

	// RemoteKey is the peer's key in its 2-of-2 script.
	RemoteKey *btcec.PublicKey

	// LocalDelta and RemoteDelta are the changes to each side's balance:
	// the splice's funding contributions.
	LocalDelta, RemoteDelta btcutil.Amount
}

// pendingFunding is a pending splice with what this channel needs to sign
// and check its commitments.
type pendingFunding struct {
	PendingFunding

	// fundingOutput is the new funding output.
	fundingOutput wire.TxOut

	// signDesc signs for our key in it.
	signDesc *input.SignDescriptor

	// local is our commitment at each height on this funding, with the
	// peer's signatures once received.
	local map[uint64]*commitment

	// remote is the peer's commitment at each height on this funding, as
	// we signed it.
	remote map[uint64]*commitment
}

// txid returns the splice transaction's id.
func (p *pendingFunding) txid() chainhash.Hash {
	return p.OutPoint.Hash
}

// commitFunding returns the funding a commitment on this splice spends.
func (p *pendingFunding) commitFunding() commitFunding {
	return commitFunding{
		txIn:        *wire.NewTxIn(&p.OutPoint, nil, nil),
		capacity:    p.Capacity,
		localDelta:  int64(p.LocalDelta) * 1000,
		remoteDelta: int64(p.RemoteDelta) * 1000,
	}
}

// AddPendingFunding adds a splice to the channel's funding set, once its
// transaction is agreed and before its commitments are exchanged. Its
// capacity must be the current one plus both deltas.
func (lc *LightningChannel) AddPendingFunding(f PendingFunding) error {
	lc.Lock()
	defer lc.Unlock()

	switch {
	case lc.channelState.ChanType.IsTaproot():
		return errors.New("taproot channels can't be spliced")

	case len(lc.pendingFundings) >= MaxPendingFundings:
		return fmt.Errorf("already %d pending splices",
			len(lc.pendingFundings))

	case f.RemoteKey == nil || f.LocalKey.PubKey == nil:
		return errors.New("pending funding without both keys")

	case f.OutPoint == lc.channelState.CurrentFundingOutpoint():
		return errors.New("pending funding is the current one")

	case f.Capacity != lc.channelState.Capacity+f.LocalDelta+
		f.RemoteDelta:

		return fmt.Errorf("pending funding of %v doesn't match "+
			"capacity %v moved by %v and %v", f.Capacity,
			lc.channelState.Capacity, f.LocalDelta, f.RemoteDelta)
	}
	for _, p := range lc.pendingFundings {
		if p.OutPoint == f.OutPoint {
			return fmt.Errorf("funding %v already pending",
				f.OutPoint)
		}
	}

	witnessScript, err := input.GenMultiSigScript(
		f.LocalKey.PubKey.SerializeCompressed(),
		f.RemoteKey.SerializeCompressed(),
	)
	if err != nil {
		return err
	}
	pkScript, err := input.WitnessScriptHash(witnessScript)
	if err != nil {
		return err
	}

	p := &pendingFunding{
		PendingFunding: f,
		fundingOutput: wire.TxOut{
			PkScript: pkScript, Value: int64(f.Capacity),
		},
		local:  make(map[uint64]*commitment),
		remote: make(map[uint64]*commitment),
	}
	p.signDesc = &input.SignDescriptor{
		KeyDesc:       f.LocalKey,
		WitnessScript: witnessScript,
		Output:        &p.fundingOutput,
		HashType:      CommitSigHashType(lc.channelState.ChanType),
		InputIndex:    0,
	}
	lc.pendingFundings = append(lc.pendingFundings, p)

	return nil
}

// PendingFundings returns the channel's pending splices.
func (lc *LightningChannel) PendingFundings() []PendingFunding {
	lc.RLock()
	defer lc.RUnlock()

	fs := make([]PendingFunding, len(lc.pendingFundings))
	for i, p := range lc.pendingFundings {
		fs[i] = p.PendingFunding
	}

	return fs
}

// pendingFunding returns the pending splice with the given txid.
func (lc *LightningChannel) pendingFunding(
	txid chainhash.Hash) (*pendingFunding, error) {

	for _, p := range lc.pendingFundings {
		if p.txid() == txid {
			return p, nil
		}
	}

	return nil, fmt.Errorf("%w: %v", ErrUnknownFunding, txid)
}

// rebuildCommitment builds c, a commitment on the current funding, again on
// another funding: same HTLCs, fee rate, height and keys, the funding's input
// and capacity, the balances moved by its deltas.
func (lc *LightningChannel) rebuildCommitment(c *commitment,
	keyRing *CommitmentKeyRing, funding commitFunding) (*commitment,
	error) {

	// Back to the balances before the commitment fee, the way computeView
	// starts from a previous commitment.
	ours, theirs := c.ourBalance, c.theirBalance
	fee := lnwire.NewMSatFromSatoshis(c.fee)
	if lc.channelState.IsInitiator {
		ours += fee
	} else {
		theirs += fee
	}

	view := &HtlcView{
		NextHeight: c.height,
		FeePerKw:   c.feePerKw,
		Updates: lntypes.Dual[[]*paymentDescriptor]{
			Local:  make([]*paymentDescriptor, len(c.outgoingHTLCs)),
			Remote: make([]*paymentDescriptor, len(c.incomingHTLCs)),
		},
	}
	for i := range c.outgoingHTLCs {
		htlc := c.outgoingHTLCs[i]
		view.Updates.Local[i] = &htlc
	}
	for i := range c.incomingHTLCs {
		htlc := c.incomingHTLCs[i]
		view.Updates.Remote[i] = &htlc
	}

	tx, err := lc.commitBuilder.createUnsignedCommitmentTx(
		ours, theirs, c.whoseCommit, c.feePerKw, c.height, view, view,
		keyRing, c, funding,
	)
	if err != nil {
		return nil, err
	}

	rebuilt := &commitment{
		height:         c.height,
		whoseCommit:    c.whoseCommit,
		messageIndices: c.messageIndices,
		ourHtlcIndex:   c.ourHtlcIndex,
		theirHtlcIndex: c.theirHtlcIndex,
		txn:            tx.txn,
		ourBalance:     tx.ourBalance,
		theirBalance:   tx.theirBalance,
		fee:            tx.fee,
		feePerKw:       c.feePerKw,
		dustLimit:      c.dustLimit,
		customBlob:     c.customBlob,
		outgoingHTLCs: make(
			[]paymentDescriptor, len(c.outgoingHTLCs),
		),
		incomingHTLCs: make(
			[]paymentDescriptor, len(c.incomingHTLCs),
		),
	}
	copy(rebuilt.outgoingHTLCs, c.outgoingHTLCs)
	copy(rebuilt.incomingHTLCs, c.incomingHTLCs)

	err = rebuilt.populateHtlcIndexes(lc.channelState.ChanType, tx.cltvs)
	if err != nil {
		return nil, err
	}

	return rebuilt, nil
}

// localKeyRing returns the key ring of our commitment at height.
func (lc *LightningChannel) localKeyRing(
	height uint64) (*CommitmentKeyRing, error) {

	secret, err := lc.channelState.RevocationProducer.AtIndex(height)
	if err != nil {
		return nil, err
	}
	point := input.ComputeCommitmentPoint(secret[:])

	return DeriveCommitmentKeys(
		point, lntypes.Local, lc.channelState.ChanType,
		&lc.channelState.LocalChanCfg, &lc.channelState.RemoteChanCfg,
	), nil
}

// remoteKeyRing returns the key ring of the peer's current commitment, the
// tail of its chain.
func (lc *LightningChannel) remoteKeyRing() (*CommitmentKeyRing, error) {
	point := lc.channelState.RemoteCurrentRevocation
	if point == nil {
		return nil, errors.New("no remote commitment point")
	}

	return DeriveCommitmentKeys(
		point, lntypes.Remote, lc.channelState.ChanType,
		&lc.channelState.LocalChanCfg, &lc.channelState.RemoteChanCfg,
	), nil
}

// quiescent reports whether the splice's own commitments can be exchanged:
// each chain holds one commitment and nothing waits to be signed.
func (lc *LightningChannel) quiescent() bool {
	return !lc.commitChains.Local.hasUnackedCommitment() &&
		!lc.commitChains.Remote.hasUnackedCommitment() &&
		!lc.oweCommitment(lntypes.Local) &&
		!lc.oweCommitment(lntypes.Remote)
}

// signCommitmentOn signs the peer's commitment remote, built on a pending
// splice's funding: the commitment signature and one per HTLC, in output
// order.
func (lc *LightningChannel) signCommitmentOn(ctx context.Context,
	p *pendingFunding, remote *commitment,
	keyRing *CommitmentKeyRing) (*CommitSigs, error) {

	var leaseExpiry uint32
	if lc.channelState.ChanType.HasLeaseExpiration() {
		leaseExpiry = lc.channelState.ThawHeight
	}
	sigBatch, _, cancelChan, err := genRemoteHtlcSigJobs(
		keyRing, lc.channelState, leaseExpiry, remote, lc.leafStore,
	)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(sigBatch, func(i, j SignJob) int {
		return cmp.Compare(i.OutputIndex, j.OutputIndex)
	})
	lc.sigPool.SubmitSignBatch(sigBatch)

	signDesc := *p.signDesc
	signDesc.SigHashes = input.NewTxSigHashesV0Only(remote.txn)
	rawSig, err := lc.Signer.SignOutputRaw(remote.txn, &signDesc)
	if err != nil {
		close(cancelChan)
		return nil, err
	}
	sig, err := lnwire.NewSigFromSignature(rawSig)
	if err != nil {
		close(cancelChan)
		return nil, err
	}

	htlcSigs := make([]lnwire.Sig, 0, len(sigBatch))
	for i := range sigBatch {
		var resp SignJobResp
		select {
		case resp = <-sigBatch[i].Resp:
		case <-ctx.Done():
			return nil, errQuit
		}
		if resp.Err != nil {
			close(cancelChan)
			return nil, resp.Err
		}
		htlcSigs = append(htlcSigs, resp.Sig)
	}

	return &CommitSigs{CommitSig: sig, HtlcSigs: htlcSigs}, nil
}

// verifyCommitmentOn checks the peer's signatures for our commitment local,
// built on a pending splice's funding, and records them on it.
func (lc *LightningChannel) verifyCommitmentOn(p *pendingFunding,
	local *commitment, keyRing *CommitmentKeyRing,
	sigs *CommitSigs) error {

	var leaseExpiry uint32
	if lc.channelState.ChanType.HasLeaseExpiration() {
		leaseExpiry = lc.channelState.ThawHeight
	}
	verifyJobs, _, err := genHtlcSigValidationJobs(
		lc.channelState, local, keyRing, sigs.HtlcSigs, leaseExpiry,
		lc.leafStore, fn.None[AuxSigner](), fn.None[[]byte](),
	)
	if err != nil {
		return err
	}
	cancelChan := make(chan struct{})
	verifyResps := lc.sigPool.SubmitVerifyBatch(verifyJobs, cancelChan)

	// The unified digest commits to the spent output's script, so the
	// fetcher carries the pending funding output.
	prevFetcher := txscript.NewCannedPrevOutputFetcher(
		p.fundingOutput.PkScript, p.fundingOutput.Value,
	)
	sigHash, err := txscript.CalcWitnessSigHash(
		p.signDesc.WitnessScript,
		txscript.NewTxSigHashes(local.txn, prevFetcher),
		CommitSigHashType(lc.channelState.ChanType), local.txn, 0,
		p.fundingOutput.Value,
	)
	if err != nil {
		close(cancelChan)
		return err
	}
	cSig, err := sigs.CommitSig.ToSignature()
	if err != nil {
		close(cancelChan)
		return err
	}
	if !cSig.Verify(sigHash, p.RemoteKey) {
		close(cancelChan)

		var txBytes bytes.Buffer
		_ = local.txn.Serialize(&txBytes)

		return &InvalidCommitSigError{
			commitHeight: local.height,
			commitSig:    sigs.CommitSig.ToSignatureBytes(),
			sigHash:      sigHash,
			commitTx:     txBytes.Bytes(),
		}
	}

	for range verifyJobs {
		if htlcErr := <-verifyResps; htlcErr != nil {
			close(cancelChan)

			return fmt.Errorf("htlc %d signature on splice %v: %w",
				htlcErr.HtlcIndex, p.txid(), htlcErr)
		}
	}

	local.sig = sigs.CommitSig.ToSignatureBytes()

	return nil
}

// SignSpliceCommitment signs the peer's current commitment on a pending
// splice's funding: the splice's own commitment_signed, sent once its
// transaction is agreed, at the same commitment number and without a
// revocation.
func (lc *LightningChannel) SignSpliceCommitment(ctx context.Context,
	txid chainhash.Hash) (*CommitSigs, error) {

	lc.Lock()
	defer lc.Unlock()

	if !lc.quiescent() {
		return nil, ErrNotQuiescent
	}
	p, err := lc.pendingFunding(txid)
	if err != nil {
		return nil, err
	}

	keyRing, err := lc.remoteKeyRing()
	if err != nil {
		return nil, err
	}
	tail := lc.commitChains.Remote.tail()
	remote, err := lc.rebuildCommitment(tail, keyRing, p.commitFunding())
	if err != nil {
		return nil, err
	}

	sigs, err := lc.signCommitmentOn(ctx, p, remote, keyRing)
	if err != nil {
		return nil, err
	}
	p.remote[remote.height] = remote

	return sigs, nil
}

// ReceiveSpliceCommitment checks the peer's signatures for our current
// commitment on a pending splice's funding, the splice's own
// commitment_signed. Once it passes, the commitment on the new funding can be
// broadcast if the splice confirms.
func (lc *LightningChannel) ReceiveSpliceCommitment(txid chainhash.Hash,
	sigs *CommitSigs) error {

	lc.Lock()
	defer lc.Unlock()

	if !lc.quiescent() {
		return ErrNotQuiescent
	}
	p, err := lc.pendingFunding(txid)
	if err != nil {
		return err
	}

	tail := lc.commitChains.Local.tail()
	keyRing, err := lc.localKeyRing(tail.height)
	if err != nil {
		return err
	}
	local, err := lc.rebuildCommitment(tail, keyRing, p.commitFunding())
	if err != nil {
		return err
	}

	if err := lc.verifyCommitmentOn(p, local, keyRing, sigs); err != nil {
		return err
	}
	p.local[local.height] = local

	return nil
}

// SignedSpliceCommitment returns our latest commitment on a pending splice's
// funding with the peer's signature: the transaction to publish if the
// splice confirms and the channel must be closed from it.
func (lc *LightningChannel) SignedSpliceCommitment(
	txid chainhash.Hash) (*wire.MsgTx, error) {

	lc.RLock()
	defer lc.RUnlock()

	p, err := lc.pendingFunding(txid)
	if err != nil {
		return nil, err
	}
	local, ok := p.local[lc.commitChains.Local.tail().height]
	if !ok || local.sig == nil {
		return nil, fmt.Errorf("no signed commitment on splice %v", txid)
	}

	signDesc := *p.signDesc

	return GetSignedCommitTx(SignedCommitTxInputs{
		CommitTx:  local.txn,
		CommitSig: local.sig,
		OurKey:    p.LocalKey,
		TheirKey:  keychain.KeyDescriptor{PubKey: p.RemoteKey},
		SignDesc:  &signDesc,
	}, lc.Signer)
}
