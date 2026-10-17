//go:build bridgerpc
// +build bridgerpc

package bridgerpc

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lnrpc/invoicesrpc"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/lightningnetwork/lnd/zpay32"
	"github.com/paulscode/lightning-fork-bridge/quote"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// headroomServer is measuredServer with toBLAKE2b's paying side (this node)
// holding room halfway between the side's bounds, and the payout invoice
// read as inv.
func headroomServer(t *testing.T, inv **zpay32.Invoice) *Server {
	t.Helper()

	srv, svc := measuredServer(t, 100_000_000_000)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	svc.ctx = ctx

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

	l := sd.out.(*Local)
	l.deps.DecodeInvoice = func(context.Context, string) (*zpay32.Invoice,
		error) {

		return *inv, nil
	}
	l.deps.QueryRoutes = func(context.Context,
		*lnrpc.QueryRoutesRequest) (*lnrpc.QueryRoutesResponse, error) {

		return &lnrpc.QueryRoutesResponse{Routes: []*lnrpc.Route{{
			TotalTimeLock: 800_000 + 60,
		}}}, nil
	}

	// The SHA256 node refuses to create the hold invoice, so a quote that
	// passed every check fails as internal.
	held := &fakeInvoices{
		addErr:    status.Error(codes.Internal, "not today"),
		lookupErr: status.Error(codes.NotFound, "no invoice"),
	}
	for _, s := range svc.sides {
		if r, ok := s.in.(*Remote); ok {
			r.invoices = held
		}
	}

	return srv
}

// headroomInfo is toBLAKE2b's maximum in Info, as the caller in ctx sees it.
func headroomInfo(t *testing.T, ctx context.Context) uint64 {
	t.Helper()

	var inv *zpay32.Invoice
	resp, err := headroomServer(t, &inv).Info(ctx, &InfoRequest{})
	require.NoError(t, err)
	for _, d := range resp.Directions {
		if d.Name == "toBLAKE2b" {
			require.True(t, d.Open, "%s", d.Refusal)

			return d.MaxMsat
		}
	}
	t.Fatal("no toBLAKE2b direction")

	return 0
}

// headroomQuote asks a fresh headroomServer for a toBLAKE2b payout of amt from
// the caller in ctx, and answers its refusal code: internal for a quote that
// passed every check.
func headroomQuote(t *testing.T, ctx context.Context, amt uint64) quote.Code {
	t.Helper()

	inv, _ := routedInvoice(t, lnwire.MilliSatoshi(amt))
	_, err := headroomServer(t, &inv).Quote(ctx, &QuoteRequest{
		Invoice: "lnbcrt1toblake2b",
	})
	code, _, _ := strings.Cut(status.Convert(err).Message(), ": ")

	return quote.Code(code)
}

// Info's maximum is the largest payout a quote takes: one whose routing
// budget fits in what is left as well. It showed the whole of what was left,
// and a quote for that much was refused as no_liquidity.
func TestInfoMaximumFitsTheFeeBudget(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	most := headroomInfo(t, ctx)
	require.Equal(t, quote.CodeNoLiquidity, headroomQuote(t, ctx, most+1))
	require.Equal(t, quote.CodeInternal, headroomQuote(t, ctx, most))
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

// A participant's quote above the maximum Info shows them is refused as
// no_liquidity, as one above the exact maximum is, so probing with quotes
// pins the headroom no closer than Info's rounding. The operator's quotes
// keep the exact limit.
func TestAParticipantIsHeldToTheShownMaximum(t *testing.T) {
	t.Parallel()

	op, them := context.Background(), macaroonFor(t, "17")
	shown, exact := headroomInfo(t, them), headroomInfo(t, op)
	require.Less(t, shown+1, exact, "the test needs a maximum that "+
		"rounds down")

	require.Equal(t, quote.CodeNoLiquidity, headroomQuote(t, them, shown+1))
	require.Equal(t, quote.CodeInternal, headroomQuote(t, them, shown))
	require.Equal(t, quote.CodeInternal, headroomQuote(t, op, shown+1))
	require.Equal(t, quote.CodeInternal, headroomQuote(t, op, exact))
	require.Equal(t, quote.CodeNoLiquidity, headroomQuote(t, op, exact+1))
}

// shortQuote quotes a toBLAKE2b payout of an invoice made now that expires
// after life, on a measuredServer whose SHA256 node creates the hold invoice,
// and answers the response or its refusal code, and what the hold invoice was
// asked for.
func shortQuote(t *testing.T, life time.Duration) (*QuoteResponse,
	quote.Code, *invoicesrpc.AddHoldInvoiceRequest, time.Time) {

	t.Helper()

	srv, svc := measuredServer(t, 100_000_000_000)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	svc.ctx = ctx

	held := &fakeInvoices{
		addResp:   &invoicesrpc.AddHoldInvoiceResp{PaymentRequest: "lnbc1h"},
		lookupErr: status.Error(codes.NotFound, "no invoice"),
	}
	svc.remote.invoices = held
	var inv *zpay32.Invoice
	for _, sd := range svc.sides {
		if r, ok := sd.in.(*Remote); ok {
			r.invoices = held
		}
		if sd.name != "toBLAKE2b" {
			continue
		}
		p := sd.quoter.Policy
		inv, _ = routedInvoice(t, lnwire.MilliSatoshi(
			p.MinSwapMsat+(p.MaxSwapMsat-p.MinSwapMsat)/2),
			zpay32.Expiry(life))
		l := sd.out.(*Local)
		l.deps.DecodeInvoice = func(context.Context, string) (
			*zpay32.Invoice, error) {

			return inv, nil
		}
		l.deps.QueryRoutes = func(context.Context,
			*lnrpc.QueryRoutesRequest) (*lnrpc.QueryRoutesResponse,
			error) {

			return &lnrpc.QueryRoutesResponse{Routes: []*lnrpc.Route{{
				TotalTimeLock: 800_000 + 60,
			}}}, nil
		}
	}
	require.NotNil(t, inv)

	resp, err := srv.Quote(ctx, &QuoteRequest{Invoice: "lnbcrt1toblake2b"})
	code, _, _ := strings.Cut(status.Convert(err).Message(), ": ")
	if err == nil {
		code = ""
	}

	return resp, quote.Code(code), held.added,
		inv.Timestamp.Add(inv.Expiry())
}

// A quote on an invoice with less life left than a quote's validity expires
// QuoteReserve (10 s) before the invoice does, and expires_at says so: the
// payer's deadline is never later than the bridge can still deliver. The
// hold invoice lives no longer. One with under 20 s left is refused as
// expires_soon.
func TestAQuoteOnAShortInvoiceExpiresBeforeIt(t *testing.T) {
	t.Parallel()

	reserve := quote.DefaultPolicy.QuoteReserve

	for _, life := range []time.Duration{45 * time.Second, 25 * time.Second} {
		resp, code, added, expiry := shortQuote(t, life)
		require.Empty(t, code, "%v", life)

		// Whole seconds, cut down: a second or two under the invoice's
		// expiry less the reserve, as the quote took a moment.
		latest := expiry.Add(-reserve)
		require.LessOrEqual(t, resp.ExpiresAt, latest.Unix(), "%v", life)
		require.Greater(t, resp.ExpiresAt, latest.Unix()-3, "%v", life)

		require.NotNil(t, added)
		require.LessOrEqual(t, added.Expiry,
			int64((life-reserve)/time.Second), "%v", life)
		require.Greater(t, added.Expiry,
			int64((life-reserve)/time.Second)-3, "%v", life)
	}

	// An hour: the quote's own minute and the hold invoice's ten.
	resp, code, added, _ := shortQuote(t, time.Hour)
	require.Empty(t, code)
	require.InDelta(t, time.Now().Add(DefaultQuoteValidity).Unix(),
		resp.ExpiresAt, 2)
	require.EqualValues(t, quote.DefaultPolicy.InvoiceExpiry/time.Second,
		added.Expiry)

	_, code, added, _ = shortQuote(t, 15*time.Second)
	require.Equal(t, quote.CodeExpiresSoon, code)
	require.Nil(t, added, "a hold invoice was created anyway")
}
