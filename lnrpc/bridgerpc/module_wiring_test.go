//go:build bridgerpc
// +build bridgerpc

package bridgerpc

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/paulscode/lightning-fork-bridge/node"
	"github.com/paulscode/lightning-fork-bridge/quote"
	"github.com/paulscode/lightning-fork-bridge/store"
	"github.com/paulscode/lightning-fork-bridge/swap"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Both adapters tell the bridge how many HTLCs a cancelled invoice ever held,
// which is how a quote tells an invoice that lapsed unpaid from one that was
// funded and given back.
func TestBothAdaptersReportAnInvoicesHTLCs(t *testing.T) {
	t.Parallel()

	h := hash8(3)
	l := local(&fakeNode{invoices: map[[32]byte]InvoiceStatus{
		h: {State: InvoiceCancelled, HTLCs: 2},
	}})
	got, err := l.LookupInvoice(context.Background(), h)
	require.NoError(t, err)
	require.Equal(t, 2, got.HTLCs)

	r := remote(nil, &fakeInvoices{lookup: &lnrpc.Invoice{
		State: lnrpc.Invoice_CANCELED,
		Htlcs: []*lnrpc.InvoiceHTLC{
			{State: lnrpc.InvoiceHTLCState_CANCELED},
		},
	}}, nil)
	got, err = r.LookupInvoice(context.Background(), h)
	require.NoError(t, err)
	require.Equal(t, 1, got.HTLCs)
}

// A hash an earlier attempt funded is refused for good, with its own code and
// sentence: the payer needs a new invoice, not another try.
func TestFundedBeforeIsItsOwnRefusal(t *testing.T) {
	t.Parallel()

	err := refusal(errors.Join(quote.ErrRefused, quote.ErrFundedBefore))
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	require.Equal(t, string(quote.CodeFundedBefore)+": "+
		refusalSentence(quote.CodeFundedBefore),
		status.Convert(err).Message())
	require.Contains(t, refusalSentence(quote.CodeFundedBefore),
		"new invoice")
}

// The fork's own translation reports a node contradicting itself as the
// module's error, which the runner counts and raises to the operator.
func TestContradictionIsTheModulesError(t *testing.T) {
	t.Parallel()

	contradicting := &fakeStream{updates: []*lnrpc.Payment{
		{Status: lnrpc.Payment_SUCCEEDED},
	}}
	_, err := remote(nil, nil, &fakeRouter{track: contradicting}).
		LookupPayment(context.Background(), hash8(4))
	require.ErrorIs(t, err, node.ErrContradiction)
	require.ErrorIs(t, err, ErrNodeContradiction)

	contradicting = &fakeStream{updates: []*lnrpc.Payment{
		{Status: lnrpc.Payment_SUCCEEDED},
	}}
	_, err = remote(nil, nil, &fakeRouter{send: contradicting}).Pay(
		context.Background(), node.PayRequest{
			Invoice: "lnbc1payme", CLTVLimit: 80,
			Timeout: time.Minute,
		})
	require.ErrorIs(t, err, node.ErrContradiction)
}

// contradictingOut is a paying node that answers every lookup with an
// impossible payment, as Remote reports one.
type contradictingOut struct {
	*fakeOutDecoder
}

func (contradictingOut) LookupPayment(context.Context, node.Hash) (
	node.Payment, error) {

	return node.Payment{}, contradiction(fmt.Errorf("%w: succeeded "+
		"with no preimage", ErrNodeContradiction))
}

// A paying node that keeps contradicting itself about a swap's payment is
// raised to the operator in needs_operator while the bridge goes on asking,
// and the note goes once the swap is no longer being driven.
func TestAContradictingNodeIsRaisedToTheOperator(t *testing.T) {
	t.Parallel()

	fx := newResolveFixture(t, node.Payment{State: node.PaymentNone})
	fx.svc.stoppedMu.Lock()
	delete(fx.svc.stopped, fx.hash)
	fx.svc.stoppedMu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	fx.svc.ctx = ctx
	for _, sd := range fx.svc.sides {
		if sd.name != "toSHA256" {
			continue
		}
		out := contradictingOut{fx.out.fakeOutDecoder}
		sd.out = out
		sd.runner.Driver.Out = out
		sd.runner.Poll = time.Millisecond
		fx.svc.drive(sd, fx.hash)
	}

	raised := func() bool {
		for _, line := range fx.svc.needsOperator(ctx) {
			if strings.Contains(line, "contradicted") {
				return true
			}
		}

		return false
	}
	require.Eventually(t, raised, 10*time.Second, 5*time.Millisecond)
	require.Equal(t, swap.Paying, fx.state(t))

	cancel()
	fx.svc.wg.Wait()
	require.False(t, raised(), "the note outlived the swap's driving")
}

// The journal has one writer: a second service over it is refused until the
// first has closed it, which is the order a rebuild keeps (stop, then build).
func TestTheJournalHasOneService(t *testing.T) {
	t.Parallel()

	cfg := usable()
	cfg.Journal = filepath.Join(t.TempDir(), "swaps.journal")
	f := &fakeNode{synced: true}

	first, err := newService(&cfg, NewLocal(f.deps()), remote(nil, nil,
		nil))
	require.NoError(t, err)

	_, err = newService(&cfg, NewLocal(f.deps()), remote(nil, nil, nil))
	require.ErrorIs(t, err, store.ErrLocked)

	first.stop()
	again, err := newService(&cfg, NewLocal(f.deps()), remote(nil, nil,
		nil))
	require.NoError(t, err)
	again.close()
}
