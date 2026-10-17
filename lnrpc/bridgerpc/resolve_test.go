//go:build bridgerpc
// +build bridgerpc

package bridgerpc

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/paulscode/lightning-fork-bridge/node"
	"github.com/paulscode/lightning-fork-bridge/store"
	"github.com/paulscode/lightning-fork-bridge/swap"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// lookupOut is a paying node that reads "sideA-" invoices as the SHA256
// chain's and says what became of a payment.
type lookupOut struct {
	*fakeOutDecoder

	pmt node.Payment

	mu     sync.Mutex
	asked  int
	looked node.Hash
}

func (o *lookupOut) LookupPayment(_ context.Context, hash node.Hash) (
	node.Payment, error) {

	o.mu.Lock()
	defer o.mu.Unlock()
	o.asked++
	o.looked = hash

	return o.pmt, nil
}

func (o *lookupOut) lookups() (int, node.Hash) {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.asked, o.looked
}

// resolveFixture is a server whose toSHA256 swap h was stopped in paying,
// with the payer's payment held on this node. settled and cancelled receive
// what the runner does to that payment once the swap is resolved.
type resolveFixture struct {
	srv       *Server
	svc       *service
	out       *lookupOut
	pre       node.Preimage
	hash      node.Hash
	settled   chan node.Preimage
	cancelled chan node.Hash
}

func newResolveFixture(t *testing.T, pmt node.Payment) *resolveFixture {
	t.Helper()

	fx := &resolveFixture{
		settled:   make(chan node.Preimage, 1),
		cancelled: make(chan node.Hash, 1),
	}
	fx.pre[0] = 7
	fx.hash = fx.pre.Hash()

	f := &fakeNode{synced: true}
	deps := f.deps()
	deps.LookupInvoice = func(context.Context, [32]byte) (InvoiceStatus,
		bool, error) {

		return InvoiceStatus{
			State: InvoiceAccepted, AmountMsat: 1000,
			ExpiryHeight: 500,
		}, true, nil
	}
	deps.SettleInvoice = func(_ context.Context, pre [32]byte) error {
		select {
		case fx.settled <- pre:
		default:
		}

		return nil
	}
	deps.CancelInvoice = func(_ context.Context, hash [32]byte) error {
		select {
		case fx.cancelled <- hash:
		default:
		}

		return nil
	}

	cfg := usable()
	cfg.Journal = filepath.Join(t.TempDir(), "swaps.journal")
	svc, err := newService(&cfg, NewLocal(deps), remote(nil, nil, nil))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	svc.ctx = ctx
	t.Cleanup(func() {
		cancel()
		svc.wg.Wait()
		svc.close()
	})

	fx.out = &lookupOut{
		fakeOutDecoder: &fakeOutDecoder{accepts: "sideA-"},
		pmt:            pmt,
	}
	for _, sd := range svc.sides {
		if sd.name == "toSHA256" {
			sd.out = fx.out
			sd.runner.Driver.Out = fx.out
		}
	}

	now := time.Now()
	require.NoError(t, svc.journal.Put(ctx, store.Record{
		Hash: fx.hash, State: swap.Paying, Binding: swap.HashLinked,
		Invoice: "sideA-payout", IncomingMsat: 1000,
		OutgoingMsat: 900, OutgoingCLTVLimit: 80,
		Created: now, Updated: now,
	}))
	svc.markStopped(fx.hash, "the paying node has no record")

	fx.svc = svc
	fx.srv = serverWith(t, true, f)
	fx.srv.svc = svc

	return fx
}

func (fx *resolveFixture) resolve(t *testing.T) (*Swap, error) {
	t.Helper()

	return fx.srv.ResolveSwap(context.Background(),
		&ResolveSwapRequest{Hash: fx.hash[:]})
}

func (fx *resolveFixture) state(t *testing.T) swap.State {
	t.Helper()

	rec, err := fx.svc.journal.Get(context.Background(), fx.hash)
	require.NoError(t, err)

	return rec.State
}

// No record of the payment, once the operator has ruled out a restored or
// edited node: it never left, so the payer is refunded.
func TestResolveAPaymentTheNodeNeverRecordedRefunds(t *testing.T) {
	t.Parallel()

	fx := newResolveFixture(t, node.Payment{State: node.PaymentNone})

	got, err := fx.resolve(t)
	require.NoError(t, err)
	require.Equal(t, swap.Failing.String(), got.State)
	_, looked := fx.out.lookups()
	require.Equal(t, fx.hash, looked)

	select {
	case h := <-fx.cancelled:
		require.Equal(t, fx.hash, h)
	case <-time.After(10 * time.Second):
		t.Fatal("the payer's held payment was not cancelled")
	}
	require.Empty(t, fx.svc.needsOperator(context.Background()))
}

func TestResolveAFailedPaymentRefunds(t *testing.T) {
	t.Parallel()

	fx := newResolveFixture(t, node.Payment{State: node.PaymentFailed})

	got, err := fx.resolve(t)
	require.NoError(t, err)
	require.Equal(t, swap.Failing.String(), got.State)

	select {
	case <-fx.cancelled:
	case <-time.After(10 * time.Second):
		t.Fatal("the payer's held payment was not cancelled")
	}
}

// A payment that went through after all is claimed with its preimage, never
// refunded.
func TestResolveASucceededPaymentSettles(t *testing.T) {
	t.Parallel()

	fx := newResolveFixture(t, node.Payment{
		State: node.PaymentSucceeded, Preimage: node.Preimage{7},
	})

	got, err := fx.resolve(t)
	require.NoError(t, err)
	require.Equal(t, swap.Paid.String(), got.State)
	require.Equal(t, fx.pre[:], got.Preimage)

	select {
	case pre := <-fx.settled:
		require.Equal(t, fx.pre, node.Preimage(pre))
	case <-time.After(10 * time.Second):
		t.Fatal("the payer's held payment was not settled")
	}
	select {
	case <-fx.cancelled:
		t.Fatal("a paid swap's incoming payment was cancelled")
	default:
	}
}

// Still in flight, the payment may go either way: nothing is decided.
func TestResolveRefusesAPaymentStillInFlight(t *testing.T) {
	t.Parallel()

	fx := newResolveFixture(t, node.Payment{State: node.PaymentInFlight})

	_, err := fx.resolve(t)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), "in_flight")
	require.Equal(t, swap.Paying, fx.state(t))
	require.NotEmpty(t, fx.svc.needsOperator(context.Background()))
}

// Only a swap the bridge stopped in paying: one it is still driving decides
// itself, and one in another state has nothing to resolve.
func TestResolveRefusesASwapNotStoppedInPaying(t *testing.T) {
	t.Parallel()

	t.Run("still being driven", func(t *testing.T) {
		t.Parallel()

		fx := newResolveFixture(t, node.Payment{State: node.PaymentNone})
		fx.svc.stoppedMu.Lock()
		delete(fx.svc.stopped, fx.hash)
		fx.svc.stoppedMu.Unlock()

		_, err := fx.resolve(t)
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
		require.Contains(t, err.Error(), "not_stopped")
		require.Equal(t, swap.Paying, fx.state(t))
		asked, _ := fx.out.lookups()
		require.Zero(t, asked)
	})

	t.Run("not paying", func(t *testing.T) {
		t.Parallel()

		fx := newResolveFixture(t, node.Payment{State: node.PaymentNone})
		rec, err := fx.svc.journal.Get(context.Background(), fx.hash)
		require.NoError(t, err)
		rec.State = swap.Funded
		require.NoError(t, fx.svc.journal.Put(context.Background(), rec))

		_, err = fx.resolve(t)
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
		require.Equal(t, swap.Funded, fx.state(t))
	})

	t.Run("unknown", func(t *testing.T) {
		t.Parallel()

		fx := newResolveFixture(t, node.Payment{State: node.PaymentNone})
		_, err := fx.srv.ResolveSwap(context.Background(),
			&ResolveSwapRequest{Hash: make([]byte, 32)})
		require.Equal(t, codes.NotFound, status.Code(err))
	})
}

// Resolving settles or cancels a payer's payment on the operator's word, so a
// participant's macaroon, or any that cannot make macaroons, must not reach it.
func TestResolveIsTheOperatorsAlone(t *testing.T) {
	t.Parallel()

	ops := macPermissions["/bridgerpc.Bridge/ResolveSwap"]
	var generate bool
	for _, op := range ops {
		if op.Entity == "macaroon" && op.Action == "generate" {
			generate = true
		}
	}
	require.True(t, generate, "ResolveSwap must need macaroon:generate")
}
