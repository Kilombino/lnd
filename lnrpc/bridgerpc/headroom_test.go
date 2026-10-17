//go:build bridgerpc
// +build bridgerpc

package bridgerpc

import (
	"context"
	"strings"
	"testing"

	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/lightningnetwork/lnd/zpay32"
	"github.com/paulscode/lightning-fork-bridge/quote"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// headroomServer is measuredServer with toBLAKE2b's paying side (this node)
// holding room, halfway between the side's bounds, and a quote function
// that asks for a payout of the amount given, from the caller in ctx, and
// answers the refusal code: internal for one that passed every check, since
// the SHA256 node then refuses to create its hold invoice.
func headroomServer(t *testing.T) (*Server, *side,
	func(ctx context.Context, amt uint64) quote.Code) {

	t.Helper()

	srv, svc := measuredServer(t, 100_000_000_000)
	svc.ctx = context.Background()

	var sd *side
	for _, s := range svc.sides {
		if s.name == "toBLAKE2b" {
			sd = s
		}
	}
	require.NotNil(t, sd)
	p := sd.quoter.Policy
	require.Less(t, p.MinSwapMsat, p.MaxSwapMsat)
	room := p.MinSwapMsat + (p.MaxSwapMsat-p.MinSwapMsat)/2
	sd.balance = func(context.Context) (uint64, error) {
		return room, nil
	}

	var inv *zpay32.Invoice
	l := sd.out.(*Local)
	l.deps.DecodeInvoice = func(context.Context, string) (*zpay32.Invoice,
		error) {

		return inv, nil
	}
	l.deps.QueryRoutes = func(context.Context,
		*lnrpc.QueryRoutesRequest) (*lnrpc.QueryRoutesResponse, error) {

		return &lnrpc.QueryRoutesResponse{Routes: []*lnrpc.Route{{
			TotalTimeLock: 800_000 + 60,
		}}}, nil
	}

	// The hold invoice is refused, so a quote that gets that far records
	// a swap nothing holds against the headroom, and the next quote sees
	// the same room.
	held := &fakeInvoices{
		addErr:    status.Error(codes.Internal, "not today"),
		lookupErr: status.Error(codes.NotFound, "no invoice"),
	}
	for _, s := range svc.sides {
		if r, ok := s.in.(*Remote); ok {
			r.invoices = held
		}
	}

	ask := func(ctx context.Context, amt uint64) quote.Code {
		inv, _ = routedInvoice(t, lnwire.MilliSatoshi(amt))
		_, err := srv.Quote(ctx, &QuoteRequest{
			Invoice: "lnbcrt1toblake2b",
		})
		msg := status.Convert(err).Message()
		if code, _, ok := strings.Cut(msg, ": "); ok {
			return quote.Code(code)
		}

		return ""
	}

	return srv, sd, ask
}

// Info's maximum is the largest payout a quote takes: one whose routing
// budget fits in what is left as well. It showed the whole of what was left,
// and a quote for that much was refused as no_liquidity.
func TestInfoMaximumFitsTheFeeBudget(t *testing.T) {
	t.Parallel()

	srv, _, ask := headroomServer(t)
	resp, err := srv.Info(context.Background(), &InfoRequest{})
	require.NoError(t, err)

	var most uint64
	for _, d := range resp.Directions {
		if d.Name == "toBLAKE2b" {
			require.True(t, d.Open, "%s", d.Refusal)
			most = d.MaxMsat
		}
	}
	require.NotZero(t, most)

	require.Equal(t, quote.CodeNoLiquidity, ask(context.Background(),
		most+1))
	require.Equal(t, quote.CodeInternal, ask(context.Background(), most))
}

func TestPayableWithin(t *testing.T) {
	t.Parallel()

	p := quote.Policy{FeeFraction: 0.003, MinFeeMsat: 10_000}
	for _, room := range []uint64{
		0, 9_999, 10_000, 10_001, 3_000_000, 3_343_333, 1 << 40,
		^uint64(0),
	} {
		most := payableWithin(p, room)
		if room < p.MinFeeMsat {
			require.Zero(t, most)
			continue
		}
		require.LessOrEqual(t, most+feeBudget(p, most), room)
		if most < ^uint64(0) {
			next := most + 1
			need := next + feeBudget(p, next)
			require.True(t, need < next || need > room,
				"%d also fits in %d", next, room)
		}
	}
}
