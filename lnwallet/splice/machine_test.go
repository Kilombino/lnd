//go:build !dev && !integration

// Release builds only: a spliceable channel has option_unified_sigs, which
// development builds never sign with.

package splice

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/chainntnfs"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/fn/v2"
	"github.com/lightningnetwork/lnd/input"
	"github.com/lightningnetwork/lnd/keychain"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/lnwallet"
	"github.com/lightningnetwork/lnd/lnwallet/chainfee"
	"github.com/lightningnetwork/lnd/lnwallet/interactivetx"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/stretchr/testify/require"
)

// spliceChanType is a channel a splice applies to.
const spliceChanType = channeldb.SingleFunderTweaklessBit |
	channeldb.AnchorOutputsBit | channeldb.ZeroHtlcTxFeeBit |
	channeldb.UnifiedSigsBit

// testLink is the channel's link as the machine sees it, run inline on a
// LightningChannel. Our commitment_signed goes to the test's peer.
type testLink struct {
	t          *testing.T
	ch         *lnwallet.LightningChannel
	initiator  lntypes.ChannelParty
	stfuResult lntypes.ChannelParty
	stfuGate   chan struct{}
	commits    chan *lnwire.CommitSig
	resumed    chan struct{}
}

func done(err error) <-chan error {
	c := make(chan error, 1)
	c <- err

	return c
}

func (l *testLink) QuiescenceInitiator() fn.Result[lntypes.ChannelParty] {
	return fn.Ok(l.initiator)
}

func (l *testLink) InitStfu() <-chan fn.Result[lntypes.ChannelParty] {
	c := make(chan fn.Result[lntypes.ChannelParty], 1)
	if l.stfuGate == nil {
		c <- fn.Ok(l.stfuResult)
		return c
	}

	// The test decides when the channel is quiescent.
	go func() {
		<-l.stfuGate
		c <- fn.Ok(l.stfuResult)
	}()

	return c
}

func (l *testLink) ResumeQuiescence() <-chan error {
	l.resumed <- struct{}{}

	return done(nil)
}

func (l *testLink) AddSpliceFunding(f lnwallet.PendingFunding) <-chan error {
	return done(l.ch.AddPendingFunding(f))
}

func (l *testLink) SendSpliceCommitment(txid chainhash.Hash) <-chan error {
	sigs, err := l.ch.SignSpliceCommitment(l.t.Context(), txid)
	if err != nil {
		return done(err)
	}
	l.commits <- &lnwire.CommitSig{
		CommitSig: sigs.CommitSig, HtlcSigs: sigs.HtlcSigs,
		FundingTxid: fn.Some(txid),
	}

	return done(nil)
}

func (l *testLink) RemoveSpliceFunding(txid chainhash.Hash) <-chan error {
	return done(l.ch.RemovePendingFunding(txid))
}

func (l *testLink) LockSplice(txid chainhash.Hash,
	scid lnwire.ShortChannelID) <-chan error {

	return done(l.ch.LockPendingFunding(txid, scid))
}

// testPeer collects what the machine sends.
type testPeer struct {
	msgs         chan lnwire.Message
	mu           sync.Mutex
	disconnected error
}

func (p *testPeer) SendMessage(_ bool, msgs ...lnwire.Message) error {
	for _, m := range msgs {
		p.msgs <- m
	}

	return nil
}

func (p *testPeer) Disconnect(reason error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.disconnected = reason
}

// testChain answers for the coins it knows and lets the test confirm
// transactions, spend outputs and mine blocks.
type testChain struct {
	height uint32
	outs   map[wire.OutPoint]*wire.TxOut

	mu     sync.Mutex
	confs  map[confKey][]*chainntnfs.ConfirmationEvent
	spends map[wire.OutPoint][]chan *chainntnfs.SpendDetail
	epochs []chan *chainntnfs.BlockEpoch
}

// confKey names a confirmation registration.
type confKey struct {
	txid     chainhash.Hash
	numConfs uint32
}

func newTestChain() *testChain {
	return &testChain{
		height: 900_000,
		outs:   make(map[wire.OutPoint]*wire.TxOut),
		confs:  make(map[confKey][]*chainntnfs.ConfirmationEvent),
		spends: make(map[wire.OutPoint][]chan *chainntnfs.SpendDetail),
	}
}

func (c *testChain) RegisterConfirmationsNtfn(txid *chainhash.Hash, _ []byte,
	numConfs, _ uint32, _ ...chainntnfs.NotifierOption) (
	*chainntnfs.ConfirmationEvent, error) {

	ev := &chainntnfs.ConfirmationEvent{
		Confirmed:    make(chan *chainntnfs.TxConfirmation, 1),
		NegativeConf: make(chan int32, 1),
		Cancel:       func() {},
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	k := confKey{*txid, numConfs}
	c.confs[k] = append(c.confs[k], ev)

	return ev, nil
}

func (c *testChain) RegisterSpendNtfn(op *wire.OutPoint, _ []byte,
	_ uint32) (*chainntnfs.SpendEvent, error) {

	ch := make(chan *chainntnfs.SpendDetail, 1)

	c.mu.Lock()
	defer c.mu.Unlock()

	c.spends[*op] = append(c.spends[*op], ch)

	return &chainntnfs.SpendEvent{Spend: ch, Cancel: func() {}}, nil
}

func (c *testChain) RegisterBlockEpochNtfn(
	*chainntnfs.BlockEpoch) (*chainntnfs.BlockEpochEvent, error) {

	ch := make(chan *chainntnfs.BlockEpoch, 4)

	c.mu.Lock()
	defer c.mu.Unlock()

	c.epochs = append(c.epochs, ch)

	return &chainntnfs.BlockEpochEvent{Epochs: ch, Cancel: func() {}}, nil
}

// registered reports whether txid is watched for numConfs, n times.
func (c *testChain) registered(txid chainhash.Hash, numConfs uint32,
	n int) bool {

	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.confs[confKey{txid, numConfs}]) >= n
}

// confirm confirms txid to every registration for numConfs, waiting for n of
// them.
func (c *testChain) confirm(t *testing.T, txid chainhash.Hash,
	numConfs uint32, n int, conf *chainntnfs.TxConfirmation) {

	t.Helper()

	require.Eventually(t, func() bool {
		return c.registered(txid, numConfs, n)
	}, 5*time.Second, 10*time.Millisecond)

	c.mu.Lock()
	evs := c.confs[confKey{txid, numConfs}]
	c.mu.Unlock()
	for _, ev := range evs {
		ev.Confirmed <- conf
	}
}

// reorg sends a reorg to every registration of txid for numConfs.
func (c *testChain) reorg(t *testing.T, txid chainhash.Hash,
	numConfs uint32) {

	t.Helper()

	c.mu.Lock()
	evs := c.confs[confKey{txid, numConfs}]
	c.mu.Unlock()
	require.NotEmpty(t, evs)
	for _, ev := range evs {
		ev.NegativeConf <- 1
	}
}

// spend spends op by tx to every watcher of op, waiting for n of them.
func (c *testChain) spend(t *testing.T, op wire.OutPoint, tx *wire.MsgTx,
	n int) {

	t.Helper()

	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()

		return len(c.spends[op]) >= n
	}, 5*time.Second, 10*time.Millisecond)

	c.mu.Lock()
	chans := c.spends[op]
	c.spends[op] = nil
	height := c.height
	c.mu.Unlock()

	hash := tx.TxHash()
	for _, ch := range chans {
		ch <- &chainntnfs.SpendDetail{
			SpentOutPoint: &op, SpenderTxHash: &hash,
			SpendingTx: tx, SpendingHeight: int32(height),
		}
	}
}

// mine sends a block to every block watcher.
func (c *testChain) mine() {
	c.mu.Lock()
	chans := c.epochs
	c.height++
	height := c.height
	c.mu.Unlock()

	for _, ch := range chans {
		select {
		case ch <- &chainntnfs.BlockEpoch{Height: int32(height)}:
		default:
		}
	}
}

func (c *testChain) BestHeight() (uint32, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.height, nil
}

func (c *testChain) ConfirmedOutput(op wire.OutPoint) (*wire.TxOut, uint32,
	bool, error) {

	out, ok := c.outs[op]
	if !ok {
		return nil, 0, false, errors.New("unknown output")
	}

	return out, 10, false, nil
}

// testKeyRing derives fresh keys the channel's signer can sign with.
type testKeyRing struct {
	signer *input.MockSigner
}

func (k *testKeyRing) DeriveNextKey(
	fam keychain.KeyFamily) (keychain.KeyDescriptor, error) {

	priv, err := btcec.NewPrivateKey()
	if err != nil {
		return keychain.KeyDescriptor{}, err
	}
	k.signer.Privkeys = append(k.signer.Privkeys, priv)

	return keychain.KeyDescriptor{
		KeyLocator: keychain.KeyLocator{Family: fam, Index: 1},
		PubKey:     priv.PubKey(),
	}, nil
}

func (k *testKeyRing) DeriveKey(
	loc keychain.KeyLocator) (keychain.KeyDescriptor, error) {

	return keychain.KeyDescriptor{}, errors.New("not used")
}

// acceptorHarness is our machine as acceptor on Bob's side of a channel,
// with Alice's side driven by the test as the peer.
type acceptorHarness struct {
	alice, bob *lnwallet.LightningChannel
	machine    *Machine
	link       *testLink
	peer       *testPeer
	chain      *testChain
	wallet     *fakeWallet
}

func newAcceptorHarness(t *testing.T) *acceptorHarness {
	t.Helper()

	alice, bob, err := lnwallet.CreateTestChannels(t, spliceChanType)
	require.NoError(t, err)

	h := &acceptorHarness{
		alice: alice,
		bob:   bob,
		link: &testLink{
			t: t, ch: bob, initiator: lntypes.Remote,
			commits: make(chan *lnwire.CommitSig, 4),
			resumed: make(chan struct{}, 4),
		},
		peer:   &testPeer{msgs: make(chan lnwire.Message, 64)},
		chain:  newTestChain(),
		wallet: newFakeWallet(),
	}

	var aliceID, bobID [33]byte
	copy(aliceID[:], alice.State().IdentityPub.SerializeCompressed())
	copy(bobID[:], bob.State().IdentityPub.SerializeCompressed())

	h.machine = NewMachine(Config{
		ChanID:      lnwire.NewChanIDFromOutPoint(bob.ChannelPoint()),
		OurNodeID:   bobID,
		TheirNodeID: aliceID,
		Link:        h.link,
		Channel:     bob,
		Peer:        h.peer,
		Chain:       h.chain,
		Wallet:      h.wallet,
		KeyRing: &testKeyRing{
			signer: bob.Signer.(*input.MockSigner),
		},
		Signer:    bob.Signer,
		Estimator: chainfee.NewStaticEstimator(2_500, 253),
		DustLimit: dustLimit,
		Policy: Policy{
			MaxChannelSize: 100 * btcutil.SatoshiPerBitcoin,
			LockDepth: func(btcutil.Amount) uint32 {
				return 6
			},
		},
	})
	startReestablished(t, h.machine)
	t.Cleanup(h.machine.Stop)

	return h
}

// next returns the machine's next message to the peer.
func next[T lnwire.Message](t *testing.T, h *acceptorHarness) T {
	t.Helper()

	select {
	case msg := <-h.peer.msgs:
		m, ok := msg.(T)
		require.True(t, ok, "got %T", msg)

		return m

	case <-time.After(5 * time.Second):
		var zero T
		t.Fatalf("no %T from the machine", zero)

		return zero
	}
}

// aliceSplice is the splice-in Alice drives: her coin, her new funding key,
// and her side of the construction.
type aliceSplice struct {
	coinKey    *btcec.PrivateKey
	coin       Coin
	fundingKey *btcec.PrivateKey
	sel        *AddFunds
	session    *interactivetx.Session
	res        *interactivetx.Result
	change     []byte
}

// startAliceSplice sends Alice's splice_init for adding funds from one P2WPKH
// coin and returns her side, with Bob's splice_ack checked.
func startAliceSplice(t *testing.T, h *acceptorHarness,
	amount btcutil.Amount, feeRate chainfee.SatPerKWeight) *aliceSplice {

	t.Helper()

	var err error
	s := &aliceSplice{}
	s.coinKey, err = btcec.NewPrivateKey()
	require.NoError(t, err)
	s.fundingKey, err = btcec.NewPrivateKey()
	require.NoError(t, err)
	aliceSigner := h.alice.Signer.(*input.MockSigner)
	aliceSigner.Privkeys = append(aliceSigner.Privkeys, s.fundingKey)

	pkScript, err := input.WitnessPubKeyHash(
		s.coinKey.PubKey().SerializeCompressed(),
	)
	require.NoError(t, err)
	s.coin = coin(77, pkScript, amount+500_000)
	h.chain.outs[s.coin.OutPoint] = s.coin.PrevTx.TxOut[0]
	s.change = p2wpkh(0x42)

	h.machine.HandleSpliceMessage(&lnwire.SpliceInit{
		ChannelID:              h.machine.cfg.ChanID,
		FundingContribution:    amount,
		FundingFeeRatePerKw:    uint32(feeRate),
		LockTime:               h.chain.height,
		FundingPubKey:          s.fundingKey.PubKey(),
		RequireConfirmedInputs: true,
	})
	ack := next[*lnwire.SpliceAck](t, h)
	require.Zero(t, ack.FundingContribution)
	require.True(t, ack.RequireConfirmedInputs)

	state := h.alice.State()
	_, out, err := input.GenFundingPkScript(
		state.LocalChanCfg.MultiSigKey.PubKey.SerializeCompressed(),
		state.RemoteChanCfg.MultiSigKey.PubKey.SerializeCompressed(),
		int64(state.Capacity),
	)
	require.NoError(t, err)
	_, newOut, err := input.GenFundingPkScript(
		s.fundingKey.PubKey().SerializeCompressed(),
		ack.FundingPubKey.SerializeCompressed(),
		int64(state.Capacity+amount),
	)
	require.NoError(t, err)

	s.sel, err = SelectAddFunds(Shape{
		SharedWitnessWeight: input.MultiSigWitnessSize,
		FundingScript:       newOut.PkScript,
	}, []Coin{s.coin}, amount, false, feeRate, s.change, dustLimit)
	require.NoError(t, err)

	s.session, err = interactivetx.NewSession(interactivetx.Config{
		Role:      interactivetx.Initiator,
		ChannelID: h.machine.cfg.ChanID,
		Shared: interactivetx.SharedInput{
			OutPoint:      state.CurrentFundingOutpoint(),
			Value:         state.Capacity,
			PkScript:      out.PkScript,
			WitnessWeight: input.MultiSigWitnessSize,
		},
		FundingScript:         newOut.PkScript,
		InitiatorContribution: amount,
		FeeRate:               feeRate,
		LockTime:              h.chain.height,
		CheckPeerInput:        func(*interactivetx.Input) error { return nil },
		CheckPeerOutput:       func(*interactivetx.Output) error { return nil },
		DustLimit:             dustLimit,
		WitnessWeight:         peerWitnessWeight,
		Reserve:               func(c btcutil.Amount) btcutil.Amount { return c / 100 },
		InitiatorBalance:      state.LocalCommitment.LocalBalance.ToSatoshis(),
		AcceptorBalance:       state.LocalCommitment.RemoteBalance.ToSatoshis(),
	})
	require.NoError(t, err)
	require.NoError(t, s.session.AddSharedInputAndFunding())
	s.session.AddLocalInput(interactivetx.Input{
		OutPoint: s.coin.OutPoint, Value: s.coin.Value,
		PkScript: s.coin.PkScript, Sequence: interactivetx.MaxSequence,
		MaxWitnessWeight: input.P2WKHWitnessSize, PrevTx: s.coin.PrevTx,
	})
	if s.sel.Change > 0 {
		s.session.AddLocalOutput(interactivetx.Output{
			Value: s.sel.Change, PkScript: s.change,
		})
	}

	return s
}

// construct runs the interactive construction between Alice and the
// machine.
func (s *aliceSplice) construct(t *testing.T, h *acceptorHarness) {
	t.Helper()

	for !s.session.Done() {
		if s.session.OurTurn() {
			msg, err := s.session.Next()
			require.NoError(t, err)
			h.machine.HandleSpliceMessage(msg)

			continue
		}

		select {
		case msg := <-h.peer.msgs:
			require.NoError(t, s.session.Receive(msg))
		case <-time.After(5 * time.Second):
			t.Fatal("machine stopped answering")
		}
	}

	var err error
	s.res, err = s.session.Build()
	require.NoError(t, err)
}

// pendingFunding is Alice's view of the splice's funding.
func (s *aliceSplice) pendingFunding(h *acceptorHarness,
	bobKey *btcec.PublicKey) lnwallet.PendingFunding {

	return lnwallet.PendingFunding{
		OutPoint: wire.OutPoint{
			Hash: s.res.Tx.TxHash(), Index: s.res.FundingIndex,
		},
		Capacity: h.alice.State().Capacity + s.sel.Contribution,
		LocalKey: keychain.KeyDescriptor{
			PubKey: s.fundingKey.PubKey(),
		},
		RemoteKey:  bobKey,
		LocalDelta: s.sel.Contribution,
	}
}

// aliceTxSignatures is Alice's tx_signatures: her coin's witness and her
// shared input signature over the unified digest.
func (s *aliceSplice) aliceTxSignatures(t *testing.T,
	h *acceptorHarness) *lnwire.TxSignatures {

	t.Helper()

	fetcher := interactivetx.PrevOutFetcher(s.res)
	sigHashes := txscript.NewTxSigHashes(s.res.Tx, fetcher)
	var witnesses []wire.TxWitness
	for i, in := range s.res.Inputs {
		if in.Shared {
			continue
		}
		w, err := txscript.WitnessSignature(
			s.res.Tx, sigHashes, i, int64(in.Value), in.PkScript,
			txscript.SigHashAll, s.coinKey, true,
		)
		require.NoError(t, err)
		witnesses = append(witnesses, w)
	}

	state := h.alice.State()
	witnessScript, _, err := input.GenFundingPkScript(
		state.LocalChanCfg.MultiSigKey.PubKey.SerializeCompressed(),
		state.RemoteChanCfg.MultiSigKey.PubKey.SerializeCompressed(),
		int64(state.Capacity),
	)
	require.NoError(t, err)
	shared := s.res.Inputs[s.res.SharedIndex]
	raw, err := h.alice.Signer.SignOutputRaw(s.res.Tx, &input.SignDescriptor{
		KeyDesc:       state.LocalChanCfg.MultiSigKey,
		WitnessScript: witnessScript,
		Output: &wire.TxOut{
			Value: int64(shared.Value), PkScript: shared.PkScript,
		},
		HashType:          interactivetx.SharedInputHashType,
		SigHashes:         sigHashes,
		PrevOutputFetcher: fetcher,
		InputIndex:        int(s.res.SharedIndex),
	})
	require.NoError(t, err)
	sig, err := lnwire.NewSigFromSignature(raw)
	require.NoError(t, err)

	return &lnwire.TxSignatures{
		ChannelID: h.machine.cfg.ChanID, TxID: s.res.Tx.TxHash(),
		Witnesses: witnesses, SharedInputSignature: fn.Some(sig),
	}
}

// requireValid checks every input of tx with the script engine.
func requireValid(t *testing.T, tx *wire.MsgTx, res *interactivetx.Result) {
	t.Helper()

	fetcher := interactivetx.PrevOutFetcher(res)
	sigHashes := txscript.NewTxSigHashes(tx, fetcher)
	for i, in := range res.Inputs {
		vm, err := txscript.NewEngine(
			in.PkScript, tx, i, txscript.StandardVerifyFlags, nil,
			sigHashes, int64(in.Value), fetcher,
		)
		require.NoError(t, err)
		require.NoError(t, vm.Execute(), "input %d", i)
	}
}

// TestAcceptSpliceIn runs a splice the peer starts, adding funds, through to
// completion: the machine answers splice_init, joins the construction adding
// nothing, exchanges commitments, signs first, checks the peer's witnesses,
// publishes a transaction every input of which verifies, sends
// splice_locked at its depth and moves the channel once the peer has too.
func TestAcceptSpliceIn(t *testing.T) {
	t.Parallel()

	h := newAcceptorHarness(t)
	const amount = btcutil.Amount(50_000_000)
	capacity := h.bob.State().Capacity

	s := startAliceSplice(t, h, amount, 2_500)
	s.construct(t, h)
	txid := s.res.Tx.TxHash()

	// Bob added the splice and signed his commitment_signed.
	var bobCommit *lnwire.CommitSig
	select {
	case bobCommit = <-h.link.commits:
	case <-time.After(5 * time.Second):
		t.Fatal("no commitment_signed from the machine")
	}
	require.Len(t, h.bob.PendingFundings(), 1)
	bobKey := h.bob.PendingFundings()[0].LocalKey.PubKey

	// Alice's side of the commitment exchange.
	require.NoError(t, h.alice.AddPendingFunding(
		s.pendingFunding(h, bobKey),
	))
	require.NoError(t, h.alice.ReceiveSpliceCommitment(txid,
		&lnwallet.CommitSigs{
			CommitSig: bobCommit.CommitSig,
			HtlcSigs:  bobCommit.HtlcSigs,
		}))
	aliceSigs, err := h.alice.SignSpliceCommitment(t.Context(), txid)
	require.NoError(t, err)
	h.machine.SpliceCommitReceived(txid,
		h.bob.ReceiveSpliceCommitment(txid, aliceSigs))

	// Bob put nothing in: he signs first, with no witnesses and his
	// shared input signature over the unified digest.
	bobTxSigs := next[*lnwire.TxSignatures](t, h)
	require.Equal(t, txid, bobTxSigs.TxID)
	require.Empty(t, bobTxSigs.Witnesses)

	// Alice's, then the published transaction is complete and valid.
	h.machine.HandleSpliceMessage(s.aliceTxSignatures(t, h))
	select {
	case <-h.link.resumed:
	case <-time.After(5 * time.Second):
		t.Fatal("quiescence not ended")
	}
	published := h.wallet.publishedTxs()
	require.Len(t, published, 1)
	label, ok := published[txid]
	require.True(t, ok)
	require.Equal(t, Label(h.bob.State().ShortChannelID, DirectionIn),
		label)
	require.Eventually(t, func() bool {
		return h.chain.registered(txid, 6, 1)
	}, 5*time.Second, 10*time.Millisecond)

	// The machine's own copy of the signed transaction.
	h.machine.mu.Lock()
	tx, err := h.machine.signedTx()
	h.machine.mu.Unlock()
	require.NoError(t, err)
	requireValid(t, tx, s.res)

	// Confirmed at depth: splice_locked; then Alice's; then moved.
	h.chain.confirm(t, txid, 6, 1, &chainntnfs.TxConfirmation{
		BlockHeight: 900_006, TxIndex: 3,
	})
	locked := next[*lnwire.SpliceLocked](t, h)
	require.Equal(t, txid, locked.SpliceTxid)
	h.machine.HandleSpliceMessage(&lnwire.SpliceLocked{
		ChannelID: h.machine.cfg.ChanID, SpliceTxid: txid,
	})

	require.Eventually(t, func() bool {
		return h.bob.State().CurrentFundingOutpoint().Hash == txid
	}, 5*time.Second, 20*time.Millisecond)
	require.Equal(t, capacity+amount, h.bob.State().Capacity)
	require.Empty(t, h.bob.PendingFundings())
}

// abortReason returns the reason of the machine's next message, a tx_abort.
func abortReason(t *testing.T, h *acceptorHarness) string {
	t.Helper()

	return string(next[*lnwire.TxAbort](t, h).Data)
}

// requireResumed waits for the machine to end quiescence.
func requireResumed(t *testing.T, h *acceptorHarness) {
	t.Helper()

	select {
	case <-h.link.resumed:
	case <-time.After(5 * time.Second):
		t.Fatal("quiescence not ended")
	}
}

// requireDisconnected waits for the machine to drop the peer.
func requireDisconnected(t *testing.T, h *acceptorHarness) {
	t.Helper()

	require.Eventually(t, func() bool {
		h.peer.mu.Lock()
		defer h.peer.mu.Unlock()

		return h.peer.disconnected != nil
	}, 5*time.Second, 10*time.Millisecond)
}

// estimateOnly is a fee estimator with a real estimate and no fallback.
type estimateOnly struct {
	rate chainfee.SatPerKWeight
}

func (e estimateOnly) EstimateFeePerKW(uint32) (chainfee.SatPerKWeight,
	error) {

	return e.rate, nil
}
func (e estimateOnly) RelayFeePerKW() chainfee.SatPerKWeight { return 253 }
func (e estimateOnly) Start() error                          { return nil }
func (e estimateOnly) Stop() error                           { return nil }

// splicePastSigning runs Alice's splice-in until Bob has sent his
// tx_signatures, and returns Alice's side.
func splicePastSigning(t *testing.T, h *acceptorHarness) *aliceSplice {
	t.Helper()

	s := startAliceSplice(t, h, 30_000_000, 2_500)
	s.construct(t, h)
	txid := s.res.Tx.TxHash()

	bobCommit := <-h.link.commits
	bobKey := h.bob.PendingFundings()[0].LocalKey.PubKey
	require.NoError(t, h.alice.AddPendingFunding(
		s.pendingFunding(h, bobKey),
	))
	require.NoError(t, h.alice.ReceiveSpliceCommitment(txid,
		&lnwallet.CommitSigs{
			CommitSig: bobCommit.CommitSig,
			HtlcSigs:  bobCommit.HtlcSigs,
		}))
	aliceSigs, err := h.alice.SignSpliceCommitment(t.Context(), txid)
	require.NoError(t, err)
	h.machine.SpliceCommitReceived(txid,
		h.bob.ReceiveSpliceCommitment(txid, aliceSigs))
	next[*lnwire.TxSignatures](t, h)

	return s
}

// TestAcceptSpliceRefused checks splice_init refusals: each gets a tx_abort
// with its code and ends quiescence, leaving nothing behind; one from the
// side that didn't start quiescence breaks the protocol.
func TestAcceptSpliceRefused(t *testing.T) {
	t.Parallel()

	init := func(h *acceptorHarness, contribution btcutil.Amount,
		feeRate uint32) {

		key, _ := btcec.NewPrivateKey()
		h.machine.HandleSpliceMessage(&lnwire.SpliceInit{
			ChannelID:           h.machine.cfg.ChanID,
			FundingContribution: contribution,
			FundingFeeRatePerKw: feeRate,
			LockTime:            h.chain.height,
			FundingPubKey:       key.PubKey(),
		})
	}

	t.Run("fee below floor", func(t *testing.T) {
		t.Parallel()

		h := newAcceptorHarness(t)
		h.machine.cfg.Estimator = estimateOnly{rate: 10_000}
		init(h, 1_000_000, 4_999)
		require.Contains(t, abortReason(t, h), codeFeeBelowFloor)
		requireResumed(t, h)
		require.Empty(t, h.bob.PendingFundings())
	})

	t.Run("more than the peer's balance", func(t *testing.T) {
		t.Parallel()

		h := newAcceptorHarness(t)
		balance := h.bob.State().LocalCommitment.RemoteBalance
		init(h, -balance.ToSatoshis()-1, 2_500)
		require.Contains(t, abortReason(t, h), codeAboveBalance)
		requireResumed(t, h)
	})

	// Contributions no amount can be, before any arithmetic on them:
	// the extremes would overflow the balance and capacity sums.
	for _, c := range []btcutil.Amount{
		math.MinInt64, math.MaxInt64, -maxMoney - 1, maxMoney + 1,
	} {
		t.Run(fmt.Sprintf("contribution %d", int64(c)),
			func(t *testing.T) {
				t.Parallel()

				h := newAcceptorHarness(t)
				init(h, c, 2_500)
				require.Contains(t, abortReason(t, h),
					codePeerMisbehaved)
				requireResumed(t, h)
				require.Empty(t, h.bob.PendingFundings())
			})
	}

	t.Run("locktime as a time", func(t *testing.T) {
		t.Parallel()

		h := newAcceptorHarness(t)
		key, _ := btcec.NewPrivateKey()
		h.machine.HandleSpliceMessage(&lnwire.SpliceInit{
			ChannelID:           h.machine.cfg.ChanID,
			FundingContribution: 1_000_000,
			FundingFeeRatePerKw: 2_500,
			LockTime:            1_600_000_000,
			FundingPubKey:       key.PubKey(),
		})
		require.Contains(t, abortReason(t, h), codeLocktime)
		requireResumed(t, h)
	})

	t.Run("above the largest channel", func(t *testing.T) {
		t.Parallel()

		h := newAcceptorHarness(t)
		h.machine.cfg.Policy.MaxChannelSize =
			h.bob.State().Capacity + 1_000
		init(h, 1_001, 2_500)
		require.Contains(t, abortReason(t, h), codeAboveMaxSize)
		requireResumed(t, h)
	})

	t.Run("locktime in the future", func(t *testing.T) {
		t.Parallel()

		h := newAcceptorHarness(t)
		key, _ := btcec.NewPrivateKey()
		h.machine.HandleSpliceMessage(&lnwire.SpliceInit{
			ChannelID:           h.machine.cfg.ChanID,
			FundingContribution: 1_000_000,
			FundingFeeRatePerKw: 2_500,
			LockTime:            h.chain.height + 2,
			FundingPubKey:       key.PubKey(),
		})
		require.Contains(t, abortReason(t, h), codeLocktime)
		requireResumed(t, h)
	})

	t.Run("peer didn't start quiescence", func(t *testing.T) {
		t.Parallel()

		h := newAcceptorHarness(t)
		h.link.initiator = lntypes.Local
		init(h, 1_000_000, 2_500)
		next[*lnwire.Warning](t, h)
		requireDisconnected(t, h)
	})

	t.Run("splicing turned off", func(t *testing.T) {
		t.Parallel()

		h := newAcceptorHarness(t)
		h.machine.cfg.Disabled = true
		init(h, 1_000_000, 2_500)
		require.Contains(t, abortReason(t, h), codeDisabled)
		requireResumed(t, h)

		_, err := h.machine.Splice(Request{
			Direction: DirectionOut, Amount: 1_000_000,
			FeeRate: 2_500, Destination: p2tr(1),
		})
		require.ErrorContains(t, err, codeDisabled)
	})

	t.Run("replacement", func(t *testing.T) {
		t.Parallel()

		h := newAcceptorHarness(t)
		h.machine.HandleSpliceMessage(&lnwire.TxInitRbf{
			ChannelID: h.machine.cfg.ChanID,
		})
		require.Contains(t, abortReason(t, h), codeRbfUnsupported)
	})
}

// TestAcceptSpliceNotUnified checks a channel without option_unified_sigs
// can't be spliced.
func TestAcceptSpliceNotUnified(t *testing.T) {
	t.Parallel()

	alice, bob, err := lnwallet.CreateTestChannels(
		t, channeldb.SingleFunderTweaklessBit,
	)
	require.NoError(t, err)
	_ = alice

	peer := &testPeer{msgs: make(chan lnwire.Message, 8)}
	link := &testLink{
		t: t, ch: bob, initiator: lntypes.Remote,
		resumed: make(chan struct{}, 2),
	}
	m := NewMachine(Config{
		ChanID:    lnwire.NewChanIDFromOutPoint(bob.ChannelPoint()),
		Link:      link,
		Channel:   bob,
		Peer:      peer,
		Chain:     newTestChain(),
		Estimator: chainfee.NewStaticEstimator(2_500, 253),
	})
	startReestablished(t, m)
	t.Cleanup(m.Stop)

	key, _ := btcec.NewPrivateKey()
	m.HandleSpliceMessage(&lnwire.SpliceInit{
		ChannelID: m.cfg.ChanID, FundingContribution: 1_000_000,
		FundingFeeRatePerKw: 2_500, FundingPubKey: key.PubKey(),
	})
	select {
	case msg := <-peer.msgs:
		abort, ok := msg.(*lnwire.TxAbort)
		require.True(t, ok, "%T", msg)
		require.Contains(t, string(abort.Data), codeNoUnifiedSigs)
	case <-time.After(5 * time.Second):
		t.Fatal("no answer")
	}
}

// TestAcceptConstructionRefused checks inputs the machine refuses during the
// construction: one the backend doesn't see confirmed and unspent, and one
// whose witness size isn't known in advance. The construction ends with
// tx_abort and nothing is left behind.
func TestAcceptConstructionRefused(t *testing.T) {
	t.Parallel()

	t.Run("unknown to the backend", func(t *testing.T) {
		t.Parallel()

		h := newAcceptorHarness(t)
		s := startAliceSplice(t, h, 10_000_000, 2_500)
		delete(h.chain.outs, s.coin.OutPoint)

		// Alice's messages until the machine aborts.
		for {
			msg, err := s.session.Next()
			require.NoError(t, err)
			h.machine.HandleSpliceMessage(msg)

			reply := <-h.peer.msgs
			if abort, ok := reply.(*lnwire.TxAbort); ok {
				require.Contains(t, string(abort.Data),
					"not found unspent")
				break
			}
			require.NoError(t, s.session.Receive(reply))
		}
		requireResumed(t, h)
		require.Empty(t, h.bob.PendingFundings())
	})

	t.Run("P2WSH input", func(t *testing.T) {
		t.Parallel()

		h := newAcceptorHarness(t)
		key, _ := btcec.NewPrivateKey()
		h.machine.HandleSpliceMessage(&lnwire.SpliceInit{
			ChannelID:           h.machine.cfg.ChanID,
			FundingContribution: 1_000_000,
			FundingFeeRatePerKw: 2_500,
			LockTime:            h.chain.height,
			FundingPubKey:       key.PubKey(),
		})
		next[*lnwire.SpliceAck](t, h)

		wsh := coin(5, append([]byte{0x00, 0x20}, make(
			[]byte, 32)...), 2_000_000)
		h.chain.outs[wsh.OutPoint] = wsh.PrevTx.TxOut[0]
		var buf bytes.Buffer
		require.NoError(t, wsh.PrevTx.Serialize(&buf))
		h.machine.HandleSpliceMessage(&lnwire.TxAddInput{
			ChannelID: h.machine.cfg.ChanID, SerialID: 2,
			PrevTx: buf.Bytes(), PrevTxVout: 0,
			Sequence: interactivetx.MaxSequence,
		})
		require.Contains(t, abortReason(t, h),
			"only P2WPKH and P2TR key path inputs")
		requireResumed(t, h)
	})
}

// TestAcceptSpliceAborts checks the peer's tx_abort: before our
// tx_signatures the splice is forgotten and the abort echoed; after, it is
// kept, since the peer could still publish it, and the peer is marked.
func TestAcceptSpliceAborts(t *testing.T) {
	t.Parallel()

	t.Run("during the construction", func(t *testing.T) {
		t.Parallel()

		h := newAcceptorHarness(t)
		startAliceSplice(t, h, 10_000_000, 2_500)
		h.machine.HandleSpliceMessage(&lnwire.TxAbort{
			ChannelID: h.machine.cfg.ChanID, Data: []byte("bye"),
		})
		next[*lnwire.TxAbort](t, h)
		requireResumed(t, h)
	})

	t.Run("after the commitments, before our signatures",
		func(t *testing.T) {
			t.Parallel()

			h := newAcceptorHarness(t)
			s := startAliceSplice(t, h, 10_000_000, 2_500)
			s.construct(t, h)
			<-h.link.commits
			require.Len(t, h.bob.PendingFundings(), 1)

			h.machine.HandleSpliceMessage(&lnwire.TxAbort{
				ChannelID: h.machine.cfg.ChanID,
			})
			next[*lnwire.TxAbort](t, h)
			requireResumed(t, h)
			require.Empty(t, h.bob.PendingFundings())
			records, err := h.bob.State().SpliceRecords()
			require.NoError(t, err)
			require.Empty(t, records)
		})

	t.Run("after our signatures", func(t *testing.T) {
		t.Parallel()

		h := newAcceptorHarness(t)
		splicePastSigning(t, h)

		h.machine.HandleSpliceMessage(&lnwire.TxAbort{
			ChannelID: h.machine.cfg.ChanID,
		})
		require.Eventually(t, func() bool {
			done := make(chan bool)
			h.machine.enqueue(func() { done <- h.machine.misbehaved })
			return <-done
		}, 5*time.Second, 10*time.Millisecond)
		require.Len(t, h.bob.PendingFundings(), 1)
	})

	t.Run("invalid signatures after ours", func(t *testing.T) {
		t.Parallel()

		h := newAcceptorHarness(t)
		s := splicePastSigning(t, h)
		sigs := s.aliceTxSignatures(t, h)
		sigs.Witnesses[0][0][10] ^= 0xff
		h.machine.HandleSpliceMessage(sigs)

		require.Eventually(t, func() bool {
			done := make(chan bool)
			h.machine.enqueue(func() { done <- h.machine.misbehaved })
			return <-done
		}, 5*time.Second, 10*time.Millisecond)
		require.Len(t, h.bob.PendingFundings(), 1)
		require.Empty(t, h.wallet.publishedTxs())
	})

	t.Run("timeout", func(t *testing.T) {
		t.Parallel()

		h := newAcceptorHarness(t)
		h.machine.cfg.Policy.NegotiationTimeout = 50 * time.Millisecond
		startAliceSplice(t, h, 10_000_000, 2_500)
		require.Contains(t, abortReason(t, h), codeTimeout)
		requireResumed(t, h)
	})
}

// TestAcceptSpliceRestart checks restarts: in the middle of signing (Bob's
// tx_signatures sent, Alice's not yet in) the machine comes back asking for
// nothing but her tx_signatures, and finishes when they arrive; once pending,
// it publishes again and waits for the lock.
func TestAcceptSpliceRestart(t *testing.T) {
	t.Parallel()

	h := newAcceptorHarness(t)
	s := splicePastSigning(t, h)
	txid := s.res.Tx.TxHash()

	restart := func() {
		h.machine.Stop()
		h.machine = NewMachine(h.machine.cfg)
		startReestablished(t, h.machine)
	}
	restart()

	// next_funding without a request: both commitments are in.
	nextFunding, locked := h.machine.ReestablishTLVs()
	nf := nextFunding.UnsafeFromSome()
	require.Equal(t, txid, nf.Txid)
	require.Zero(t, nf.RetransmitFlags)
	require.Equal(t, h.bob.State().CurrentFundingOutpoint().Hash,
		locked.UnsafeFromSome().Txid)

	// Alice reconnects naming the splice: Bob sends his tx_signatures
	// again.
	h.machine.HandleReestablish(&lnwire.ChannelReestablish{
		NextFunding: fn.Some(lnwire.FundingTxidFlags{Txid: txid}),
	})
	require.Equal(t, txid, next[*lnwire.TxSignatures](t, h).TxID)

	h.machine.HandleSpliceMessage(s.aliceTxSignatures(t, h))
	requireResumed(t, h)
	require.Len(t, h.wallet.publishedTxs(), 1)

	// Restart while pending: published again, watched again.
	h.wallet.mu.Lock()
	h.wallet.published = make(map[chainhash.Hash]string)
	h.wallet.mu.Unlock()
	restart()
	require.Len(t, h.wallet.publishedTxs(), 1)

	h.chain.confirm(t, txid, 6, 2, &chainntnfs.TxConfirmation{
		BlockHeight: 900_010,
	})
	next[*lnwire.SpliceLocked](t, h)
	h.machine.HandleSpliceMessage(&lnwire.SpliceLocked{
		ChannelID: h.machine.cfg.ChanID, SpliceTxid: txid,
	})
	require.Eventually(t, func() bool {
		return h.bob.State().CurrentFundingOutpoint().Hash == txid
	}, 5*time.Second, 20*time.Millisecond)

	// An unknown next_funding gets tx_abort.
	h.machine.HandleReestablish(&lnwire.ChannelReestablish{
		NextFunding: fn.Some(lnwire.FundingTxidFlags{
			Txid: chainhash.Hash{0xee},
		}),
	})
	next[*lnwire.TxAbort](t, h)
}

// startReestablished starts a machine as the link does: then the peer's
// channel_reestablish, after which splice_locked may be sent.
func startReestablished(t *testing.T, m *Machine) {
	t.Helper()

	require.NoError(t, m.Start())
	m.HandleReestablish(&lnwire.ChannelReestablish{})
}
