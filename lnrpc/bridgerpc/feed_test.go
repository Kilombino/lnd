//go:build bridgerpc
// +build bridgerpc

package bridgerpc

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/paulscode/lightning-fork-bridge/driver"
	"github.com/paulscode/lightning-fork-bridge/rate"
)

// TestMain keeps every feed in the package's tests off the network: a server
// started with the bridge on follows the market, and the tests that need a
// market give the feed one of their own (testFeed).
func TestMain(m *testing.M) {
	marketFetch = func(func(string, string, time.Duration) (net.Conn,
		error)) fetchFunc {

		return func(context.Context, string) ([]byte, error) {
			return nil, errors.New("tests do not reach the network")
		}
	}
	os.Exit(m.Run())
}

// market is a fake set of sources: each URL answers with what it holds, or
// fails when it holds nothing. Times in what it holds follow its clock.
type market struct {
	mu    sync.Mutex
	body  map[string]string
	now   time.Time
	asked map[string]int
}

// The fake market's BTC/USD, and its books: Neoxa's BTCB2_BTC at 0.006 bid
// and 0.00606 ask, and the two dollar books the same over it.
const fakeUSD = 85000

func newMarket(now time.Time) *market {
	m := &market{body: map[string]string{}, now: now,
		asked: map[string]int{}}
	m.set(pairDirect, 0.006, 0.00606)
	m.ticker(pairDirect, 0.006, 0.006, 0.00606)
	m.set(pairUSDC, 0.006*fakeUSD, 0.00606*fakeUSD)
	m.coinGecko(fakeUSD)
	m.nonKYC(0.006*fakeUSD, 0.00606*fakeUSD)

	return m
}

func bookURL(pair string) string {
	return fmt.Sprintf("%s%s?depth=%d", neoxaBookURL, pair, neoxaBookDepth)
}

// levels writes a side of a book as Neoxa does, numbers as numbers, or as
// NonKYC does, as strings.
func levels(ls []level, quoted bool) string {
	var parts []string
	for _, l := range ls {
		if quoted {
			parts = append(parts, fmt.Sprintf(`{"price":"%g",`+
				`"quantity":"%g"}`, l.price, l.quantity))
		} else {
			parts = append(parts, fmt.Sprintf(`{"price":%g,`+
				`"quantity":%g,"orders":1}`, l.price, l.quantity))
		}
	}

	return "[" + strings.Join(parts, ",") + "]"
}

// book gives one of Neoxa's pairs a book.
func (m *market) book(pair string, bids, asks []level) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.body[bookURL(pair)] = fmt.Sprintf(`{"success":true,"pair":%q,`+
		`"depth":%d,"bids":%s,"asks":%s}`, pair, neoxaBookDepth,
		levels(bids, false), levels(asks, false))
}

// set gives one of Neoxa's pairs a deep book at bid and ask.
func (m *market) set(pair string, bid, ask float64) {
	m.book(pair, []level{{bid, 1000}}, []level{{ask, 1000}})
}

// ticker gives one of Neoxa's pairs a ticker, computed now.
func (m *market) ticker(pair string, last, bid, ask float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.body[neoxaTickerURL+pair] = fmt.Sprintf(`{"success":true,`+
		`"pair":%q,"ticker":{"lastPrice":%g,"bestBid":%g,"bestAsk":%g,`+
		`"computedAt":%d}}`, pair, last, bid, ask, m.now.UnixMilli())
}

// nonKYC gives the second venue a deep book at bid and ask, in dollars (zero:
// the venue does not answer).
func (m *market) nonKYC(bid, ask float64) {
	if bid == 0 {
		m.fail(nonKYCBookURL)

		return
	}
	m.nonKYCBook([]level{{bid, 1000}}, []level{{ask, 1000}})
}

func (m *market) nonKYCBook(bids, asks []level) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.body[nonKYCBookURL] = fmt.Sprintf(`{"symbol":"BTCB2/USDT",`+
		`"timestamp":%d,"bids":%s,"asks":%s}`, m.now.UnixMilli(),
		levels(bids, true), levels(asks, true))
}

func (m *market) coinGecko(usd float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if usd == 0 {
		delete(m.body, coinGeckoRatesURL)

		return
	}
	m.body[coinGeckoRatesURL] = fmt.Sprintf(`{"rates":{"btc":{"value":1},`+
		`"usd":{"value":%g}}}`, usd)
}

func (m *market) mempool(usd float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if usd == 0 {
		delete(m.body, mempoolPricesURL)

		return
	}
	m.body[mempoolPricesURL] = fmt.Sprintf(`{"time":%d,"USD":%g}`,
		m.now.Unix(), usd)
}

// candles gives Neoxa's day the hourly closes, oldest first, the last ending
// now.
func (m *market) candles(closes ...float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var parts []string
	for i, c := range closes {
		at := m.now.Add(-time.Duration(len(closes)-i) * time.Hour)
		parts = append(parts, fmt.Sprintf(`{"time":%d,"open":%g,`+
			`"high":%g,"low":%g,"close":%g,"volume":1}`, at.Unix(),
			c, c, c, c))
	}
	m.body[neoxaCandlesURL] = `{"success":true,"pair":"BTCB2_BTC",` +
		`"interval":"1h","candles":[` + strings.Join(parts, ",") + `]}`
}

func (m *market) fail(url string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.body, url)
}

func (m *market) fetch(_ context.Context, url string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.asked[url]++
	b, ok := m.body[url]
	if !ok {
		return nil, errors.New("connection refused")
	}

	return []byte(b), nil
}

// testFeed is a feed over m whose clock is m's, with the shipped policy.
func testFeed(t *testing.T, m *market) *feed {
	t.Helper()

	c := usable()

	return testFeedWith(t, m, c.resolve().market)
}

func testFeedWith(t *testing.T, m *market, p feedPolicy) *feed {
	t.Helper()

	f, err := newFeed(p, nil)
	if err != nil {
		t.Fatal(err)
	}
	f.fetch = m.fetch
	f.now = func() time.Time {
		m.mu.Lock()
		defer m.mu.Unlock()

		return m.now
	}

	return f
}

// advance moves the clock, and the venues' own times with it.
func (m *market) advance(d time.Duration) {
	m.mu.Lock()
	m.now = m.now.Add(d)
	now := m.now
	for url, b := range m.body {
		for _, key := range []string{`"computedAt":`, `"timestamp":`} {
			i := strings.Index(b, key)
			if i < 0 {
				continue
			}
			j := strings.IndexAny(b[i:], ",}")
			b = b[:i] + fmt.Sprintf("%s%d", key, now.UnixMilli()) +
				b[i+j:]
		}
		m.body[url] = b
	}
	m.mu.Unlock()
}

var feedStart = time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)

// swapB2B is the largest swap, 0.0015 SHA256 coin by default, in BLAKE2b coin
// at the fake market's middle: the size its books are read at.
const swapB2B = 0.0015 / 0.00603

func near(a, b float64) bool {
	return math.Abs(a-b) <= 1e-9*math.Max(math.Abs(a), math.Abs(b))
}

// sideOf is a direction's rate and widening, failing the test on a refusal.
func sideOf(t *testing.T, m marketRate, invert bool) (float64, float64) {
	t.Helper()

	r, extra, err := m.forSide(invert)
	if err != nil {
		t.Fatalf("invert %v refused: %v", invert, err)
	}

	return r, extra
}

func hasNote(m marketRate, part string) bool {
	return strings.Contains(strings.Join(m.notes, "; "), part)
}

// Each direction trades at its own side of the book read at the largest
// swap, and the cross-check agreeing lets it through.
func TestTheFeedPricesFromTheBooks(t *testing.T) {
	m := newMarket(feedStart)
	f := testFeed(t, m)

	if _, err := f.quoteRate(); !errors.Is(err, errNoReading) {
		t.Fatalf("before any reading: %v", err)
	}

	f.read(context.Background())
	got, err := f.quoteRate()
	if err != nil {
		t.Fatal(err)
	}
	if !near(got.rate, 0.00603) || got.sources != 2 {
		t.Errorf("rate %g from %d sources, wanted the middle 0.00603",
			got.rate, got.sources)
	}
	if r, extra := sideOf(t, got, false); !near(r, 0.006) || extra != 0 {
		t.Errorf("toSHA256 at %g (+%g), wanted the bid 0.006", r, extra)
	}
	if r, extra := sideOf(t, got, true); !near(r, 0.00606) || extra != 0 {
		t.Errorf("toBLAKE2b at %g (+%g), wanted the ask 0.00606", r,
			extra)
	}
	if cross := f.crossCheck(); !near(cross, 0.00603) {
		t.Errorf("cross-check %g", cross)
	}
}

// A small order placed at the top of the book, as anyone can and cancel
// again, moves the price only by what it would fill of the largest swap. Read
// from the ticker, the middle of last, bid and ask, it would be the price.
func TestADustOrderDoesNotMoveThePrice(t *testing.T) {
	m := newMarket(feedStart)
	m.book(pairDirect, []level{{0.00605, 0.0001}, {0.006, 1000}},
		[]level{{0.00606, 1000}})
	m.ticker(pairDirect, 0.00605, 0.00605, 0.00606)
	// Neoxa alone, as without a second venue.
	m.nonKYC(0, 0)
	f := testFeed(t, m)

	f.read(context.Background())
	got, err := f.quoteRate()
	if err != nil {
		t.Fatal(err)
	}
	// 0.0015 BTC at 0.006 is 0.25 BLAKE2b coin, of which the dust is
	// 0.0001: 0.04% of it at a price 0.8% better.
	r, _ := sideOf(t, got, false)
	if r > 0.006*(1+0.0001) {
		t.Errorf("a dust bid moved toSHA256 to %g from 0.006", r)
	}
}

// The largest swap in BLAKE2b coin is what the books are read at: half of it
// on one level and half on a worse one is the average of the two.
func TestTheBookIsReadAtTheLargestSwap(t *testing.T) {
	m := newMarket(feedStart)
	m.book(pairDirect, []level{{0.006, swapB2B / 2}, {0.0059, 1000}},
		[]level{{0.00606, 1000}})
	f := testFeed(t, m)

	f.read(context.Background())
	got, err := f.quoteRate()
	if err != nil {
		t.Fatal(err)
	}
	// Half of it at 0.006 and the rest at 0.0059. The NonKYC bid, 0.006,
	// is no longer the worse one.
	want := (0.006 + 0.0059) / 2
	if r, _ := sideOf(t, got, false); !near(r, want) {
		t.Errorf("toSHA256 at %g, wanted %g", r, want)
	}
}

// A side of a book too thin for the largest swap still prices, at what it
// holds, with the fee widened for what it does not.
func TestAThinBookWidensTheFee(t *testing.T) {
	m := newMarket(feedStart)
	m.nonKYC(0, 0)
	// 0.1 of the largest swap's 0.25 BLAKE2b coin on the bids.
	m.book(pairDirect, []level{{0.006, 0.1}}, []level{{0.00606, 1000}})
	f := testFeed(t, m)

	f.read(context.Background())
	got, err := f.quoteRate()
	if err != nil {
		t.Fatalf("a thin book stopped quoting: %v", err)
	}
	short := 1 - 0.1/swapB2B
	r, extra := sideOf(t, got, false)
	if !near(r, 0.006) || !near(extra, thinBookSpread*short) {
		t.Errorf("toSHA256 at %g +%g, wanted 0.006 +%g", r, extra,
			thinBookSpread*short)
	}
	if _, extra := sideOf(t, got, true); extra != 0 {
		t.Errorf("the deep side widened by %g", extra)
	}
	if !hasNote(got, "holds 40% of the largest swap") {
		t.Errorf("notes %q", got.notes)
	}
}

// The median of the last few readings: one odd reading does not move it.
func TestTheFeedSmoothsWithAMedian(t *testing.T) {
	m := newMarket(feedStart)
	f := testFeed(t, m)

	for _, p := range []float64{0.006, 0.00602, 0.0063, 0.00601} {
		m.set(pairDirect, p, p*1.01)
		// The other books follow, so the odd one is not refused for
		// disagreeing: this is about what the median does with it.
		m.set(pairUSDC, p*fakeUSD, p*1.01*fakeUSD)
		m.nonKYC(p*fakeUSD, p*1.01*fakeUSD)
		f.read(context.Background())
		m.advance(30 * time.Second)
	}
	m.advance(-30 * time.Second)

	got, err := f.quoteRate()
	if err != nil {
		t.Fatal(err)
	}
	// Within 100 s of the newest: all four (the first is 90 s before the
	// newest): the median of four is the mean of the middle two, 0.006015.
	if r, _ := sideOf(t, got, false); !near(r, 0.006015) {
		t.Errorf("toSHA256 at %g, wanted the median 0.006015", r)
	}
}

// Two books that disagree stop quoting at once, whatever came before.
func TestTheFeedRefusesWhenTheBooksDisagree(t *testing.T) {
	m := newMarket(feedStart)
	f := testFeed(t, m)
	f.read(context.Background())

	// BTCB2_USDC over BTC/USD says 0.0066, 10% over the direct book.
	m.set(pairUSDC, 561, 566)
	m.advance(30 * time.Second)
	f.read(context.Background())

	_, err := f.quoteRate()
	if !errors.Is(err, rate.ErrDisagreement) {
		t.Fatalf("want a disagreement, got %v", err)
	}
	if !strings.Contains(rateRefusal(err), "disagree") {
		t.Errorf("Status says %q", rateRefusal(err))
	}
}

// The cross-check compares the books at the smallest swap: Neoxa's USDC book
// holding as much at the top as its BTC book and much less below reads 8%
// under it at the largest swap, a difference of depth, and quoting goes on.
func TestTheCrossCheckIgnoresDepth(t *testing.T) {
	m := newMarket(feedStart)
	m.book(pairUSDC, []level{{0.006 * fakeUSD, 0.01},
		{0.005 * fakeUSD, 1000}}, []level{{0.00606 * fakeUSD, 1000}})
	f := testFeed(t, m)
	f.read(context.Background())

	if _, err := f.quoteRate(); err != nil {
		t.Fatalf("a thinner USDC book stopped quoting: %v", err)
	}
	if cross := f.crossCheck(); !near(cross, 0.00603) {
		t.Errorf("cross-check %g, wanted the middle at the smallest "+
			"swap, 0.00603", cross)
	}
}

// Each direction trades at whichever venue is worse for its payer: BLAKE2b
// coin pushed up on Neoxa alone is priced at NonKYC for whoever sells it to
// the bridge, and the other direction keeps Neoxa's price.
func TestEachDirectionTakesTheWorseVenue(t *testing.T) {
	m := newMarket(feedStart)
	f := testFeed(t, m)

	// NonKYC 3% under Neoxa.
	m.nonKYC(0.00582*fakeUSD, 0.0058782*fakeUSD)
	f.read(context.Background())
	got, err := f.quoteRate()
	if err != nil {
		t.Fatal(err)
	}
	if !near(got.rate, 0.00603) {
		t.Errorf("rate %g, wanted Neoxa's 0.00603", got.rate)
	}
	if v, _ := sideOf(t, got, false); !near(v, 0.00582) {
		t.Errorf("paying out SHA256 at %g, wanted NonKYC's lower "+
			"0.00582", v)
	}
	if v, _ := sideOf(t, got, true); !near(v, 0.00606) {
		t.Errorf("paying out BLAKE2b at %g, wanted Neoxa's higher "+
			"0.00606", v)
	}

	// NonKYC 3% over: the other way round. Its median follows within the
	// window, as the main price's does.
	for i := 0; i < 4; i++ {
		m.nonKYC(0.00618*fakeUSD, 0.0062418*fakeUSD)
		m.advance(30 * time.Second)
		f.read(context.Background())
	}
	got, err = f.quoteRate()
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := sideOf(t, got, false); !near(v, 0.006) {
		t.Errorf("paying out SHA256 at %g, wanted Neoxa's lower 0.006", v)
	}
	if v, _ := sideOf(t, got, true); !near(v, 0.0062418) {
		t.Errorf("paying out BLAKE2b at %g, wanted NonKYC's higher "+
			"0.0062418", v)
	}
}

// Past maxvenuegap between the venues' middles nothing is quoted, and the
// refusal says why.
func TestTheVenueGapStops(t *testing.T) {
	m := newMarket(feedStart)
	// NonKYC's book 5% under Neoxa's.
	m.nonKYC(0.0057*fakeUSD, 0.005757*fakeUSD)
	f := testFeed(t, m)
	f.read(context.Background())

	got, err := f.quoteRate()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		invert bool
		name   string
	}{{false, "toSHA256"}, {true, "toBLAKE2b"}} {
		_, _, err = got.forSide(tc.invert)
		if !errors.Is(err, rate.ErrDisagreement) ||
			!strings.Contains(err.Error(), tc.name+" quotes nothing") ||
			!strings.Contains(err.Error(), "more than the 4.0% allowed") {

			t.Errorf("%s with the venues 5%% apart: %v", tc.name, err)
		}
	}

	// The option moves the bound.
	c := usable()
	c.MaxVenueGap = 0.06
	f = testFeedWith(t, m, c.resolve().market)
	f.read(context.Background())
	got, _ = f.quoteRate()
	if v, _ := sideOf(t, got, false); !near(v, 0.0057) {
		t.Errorf("toSHA256 at %g with a 6%% bound", v)
	}
	if v, _ := sideOf(t, got, true); !near(v, 0.00606) {
		t.Errorf("toBLAKE2b at %g with a 6%% bound", v)
	}
}

// The venues are compared at the smallest swap, not the largest: NonKYC's
// book as deep as Neoxa's at the top and thinner below reads 6.8% under it at
// the largest swap, which is a difference of depth, not of price. The
// direction selling into it trades there, at the worse venue, rather than
// stopping.
func TestBookDepthIsNotAVenueGap(t *testing.T) {
	m := newMarket(feedStart)
	m.nonKYCBook([]level{{0.006 * fakeUSD, 0.01},
		{0.0056 * fakeUSD, 1000}}, []level{{0.00606 * fakeUSD, 1000}})
	f := testFeed(t, m)
	f.read(context.Background())

	got, err := f.quoteRate()
	if err != nil {
		t.Fatal(err)
	}
	want := (0.01*0.006 + (swapB2B-0.01)*0.0056) / swapB2B
	if v, _ := sideOf(t, got, false); math.Abs(v-want) > 1e-9 {
		t.Errorf("toSHA256 at %g, wanted NonKYC's %g at the largest "+
			"swap", v, want)
	}
	if v, _ := sideOf(t, got, true); !near(v, 0.00606) {
		t.Errorf("toBLAKE2b at %g", v)
	}
}

// A quote is made at the venue worse for its payer: with BLAKE2b coin 3% dearer
// on Neoxa than on NonKYC, the bridge pays out SHA256 coin at NonKYC's rate
// and BLAKE2b coin at Neoxa's.
func TestQuotesTakeTheWorseVenuePerDirection(t *testing.T) {
	t.Parallel()

	cfg := usable()
	cfg.RateSource = RateSourceNeoxa
	cfg.FixedRate = 0

	m := newMarket(feedStart)
	m.nonKYC(0.00582*fakeUSD, 0.0058782*fakeUSD)
	mkt := testFeed(t, m)
	mkt.read(context.Background())

	f := &fakeNode{synced: true,
		balance: cfg.resolve().inventory.TargetOutgoingMsat}
	svc := serviceForMarket(t, cfg, f, mkt)
	svc.positionOf = f.deps().ChannelBalance
	fwd, rev := sidesOf(t, svc)

	got, err := svc.pricer(fwd)(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !near(got.OutgoingPerIncoming, 0.00582) {
		t.Errorf("paying out SHA256 at %g, wanted NonKYC's 0.00582",
			got.OutgoingPerIncoming)
	}
	back, err := svc.pricer(rev)(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !near(back.OutgoingPerIncoming, 1/0.00606) {
		t.Errorf("paying out BLAKE2b at %g, wanted 1/0.00606 (Neoxa)",
			back.OutgoingPerIncoming)
	}
}

// The second venue not answering leaves the bridge priced on Neoxa's books,
// and Status saying so: another exchange's outage does not stop it.
func TestTheSecondVenueIsNotRequired(t *testing.T) {
	m := newMarket(feedStart)
	m.nonKYC(0, 0)
	f := testFeed(t, m)

	f.read(context.Background())
	got, err := f.quoteRate()
	if err != nil {
		t.Fatalf("quoting stopped without the second venue: %v", err)
	}
	if r, _ := sideOf(t, got, false); !near(r, 0.006) {
		t.Errorf("toSHA256 at %g, wanted Neoxa's bid", r)
	}
	if r, _ := sideOf(t, got, true); !near(r, 0.00606) {
		t.Errorf("toBLAKE2b at %g, wanted Neoxa's ask", r)
	}
	if !hasNote(got, "second venue (NonKYC) is not being read") {
		t.Errorf("notes %q", got.notes)
	}

	// Nor does a garbled answer, one for another market, or one from a
	// book that stopped.
	for _, body := range []string{
		`{"symbol":"XYZ/USDT","bids":[{"price":"1","quantity":"1"}],` +
			`"asks":[{"price":"2","quantity":"1"}]}`,
		`{"symbol":"BTCB2/USDT","bids":[],"asks":[]}`,
		fmt.Sprintf(`{"symbol":"BTCB2/USDT","timestamp":%d,`+
			`"bids":[{"price":"510","quantity":"9"}],`+
			`"asks":[{"price":"515","quantity":"9"}]}`,
			feedStart.Add(-time.Hour).UnixMilli()),
	} {
		m.advance(30 * time.Second)
		m.mu.Lock()
		m.body[nonKYCBookURL] = body
		m.mu.Unlock()
		f.read(context.Background())
		if _, err := f.quoteRate(); err != nil {
			t.Fatalf("%s stopped quoting: %v", body, err)
		}
		if f.readings[len(f.readings)-1].second.bid != 0 {
			t.Errorf("%s was read as a book", body)
		}
	}
}

// A book without a time of its own, or a ticker that cannot confirm Neoxa's,
// is still used, with the fee widened a little and Status saying why.
func TestAnUnconfirmedBookWidensRatherThanStops(t *testing.T) {
	m := newMarket(feedStart)
	m.fail(neoxaTickerURL + pairDirect)
	f := testFeed(t, m)

	f.read(context.Background())
	got, err := f.quoteRate()
	if err != nil {
		t.Fatalf("no ticker stopped quoting: %v", err)
	}
	for _, invert := range []bool{false, true} {
		if _, extra := sideOf(t, got, invert); !near(extra,
			unverifiedSpread) {

			t.Errorf("invert %v widened %g", invert, extra)
		}
	}
	if !hasNote(got, "confirm the book is current") {
		t.Errorf("notes %q", got.notes)
	}

	// A ticker without computedAt, and NonKYC's book without a
	// timestamp: twice.
	m.mu.Lock()
	m.body[neoxaTickerURL+pairDirect] = `{"success":true,` +
		`"pair":"BTCB2_BTC","ticker":{"lastPrice":0.006}}`
	m.body[nonKYCBookURL] = `{"symbol":"BTCB2/USDT",` +
		`"bids":[{"price":"510","quantity":"1000"}],` +
		`"asks":[{"price":"515.1","quantity":"1000"}]}`
	m.mu.Unlock()
	m.advance(30 * time.Second)
	f.read(context.Background())
	got, err = f.quoteRate()
	if err != nil {
		t.Fatal(err)
	}
	if _, extra := sideOf(t, got, false); !near(extra, 2*unverifiedSpread) {
		t.Errorf("widened %g, wanted %g", extra, 2*unverifiedSpread)
	}
	if !hasNote(got, "carries no time") {
		t.Errorf("notes %q", got.notes)
	}
}

// A venue whose book has not changed at all in ten minutes while the other
// moved has probably stopped: the fee is widened for it.
func TestAStuckBookWidensTheFee(t *testing.T) {
	m := newMarket(feedStart)
	f := testFeed(t, m)

	// Neoxa drifts up 1% over ten minutes; NonKYC's book stays put (its
	// timestamp moves on, as a stuck engine's would).
	p := 0.006
	for i := 0; i <= 20; i++ {
		m.set(pairDirect, p, p*1.01)
		m.set(pairUSDC, p*fakeUSD, p*1.01*fakeUSD)
		f.read(context.Background())
		p *= 1.0005
		m.advance(30 * time.Second)
	}
	m.advance(-30 * time.Second)

	got, err := f.quoteRate()
	if err != nil {
		t.Fatal(err)
	}
	if !hasNote(got, "NonKYC's BTCB2_USDT book has not changed") {
		t.Errorf("notes %q", got.notes)
	}
	if _, extra := sideOf(t, got, true); extra < unverifiedSpread {
		t.Errorf("widened %g for a stuck venue", extra)
	}
}

// A market that cannot be read leaves the last reading usable only while it is
// fresh; after that nothing is quoted.
func TestTheFeedStopsWhenTheMarketCannotBeRead(t *testing.T) {
	m := newMarket(feedStart)
	f := testFeed(t, m)
	f.read(context.Background())

	m.fail(bookURL(pairDirect))
	m.advance(30 * time.Second)
	f.read(context.Background())
	if _, err := f.quoteRate(); err != nil {
		t.Fatalf("a reading 30 s old should still serve: %v", err)
	}

	m.advance(70 * time.Second)
	f.read(context.Background())
	_, err := f.quoteRate()
	if !errors.Is(err, rate.ErrStale) {
		t.Fatalf("want stale after 100 s, got %v", err)
	}
	if !strings.Contains(err.Error(), "could not be read") {
		t.Errorf("the refusal does not say why: %v", err)
	}
}

// Without the cross-check the bridge does not quote either, once the last
// checked reading is too old.
func TestTheCrossCheckIsRequired(t *testing.T) {
	m := newMarket(feedStart)
	m.fail(bookURL(pairUSDC))
	f := testFeed(t, m)

	f.read(context.Background())
	if _, err := f.quoteRate(); err == nil ||
		!strings.Contains(err.Error(), "cross-check") {

		t.Fatalf("quoted without a cross-check: %v", err)
	}
}

// BTC/USD is CoinGecko's, asked at most every five minutes; mempool.space's
// when CoinGecko does not answer; and only when neither has for fifteen
// minutes Neoxa's own BTC_USDC book, with the fee widened and Status saying
// so.
func TestBTCUSDSources(t *testing.T) {
	m := newMarket(feedStart)
	m.ticker(pairBTCUSDC, fakeUSD, fakeUSD-100, fakeUSD+100)
	f := testFeed(t, m)

	for i := 0; i < 20; i++ {
		f.read(context.Background())
		m.advance(30 * time.Second)
	}
	if n := m.asked[coinGeckoRatesURL]; n != 2 {
		t.Errorf("CoinGecko asked %d times in ten minutes, wanted 2", n)
	}

	// CoinGecko down, mempool.space up: no difference.
	m.coinGecko(0)
	m.mempool(fakeUSD)
	m.advance(5 * time.Minute)
	f.read(context.Background())
	got, err := f.quoteRate()
	if err != nil {
		t.Fatal(err)
	}
	if f.usd.source != "mempool.space" || hasNote(got, "BTC/USD") {
		t.Errorf("BTC/USD from %q, notes %q", f.usd.source, got.notes)
	}
	if _, extra := sideOf(t, got, false); extra != 0 {
		t.Errorf("widened %g with mempool.space's price", extra)
	}

	// Both down: the last price serves for fifteen minutes, then Neoxa's.
	m.mempool(0)
	m.advance(14 * time.Minute)
	f.read(context.Background())
	got, _ = f.quoteRate()
	if hasNote(got, "BTC/USD is Neoxa's") {
		t.Errorf("fell back with a price %v old", 14*time.Minute)
	}
	m.advance(2 * time.Minute)
	f.read(context.Background())
	got, err = f.quoteRate()
	if err != nil {
		t.Fatalf("no rate with both BTC/USD sources down: %v", err)
	}
	if !hasNote(got, "BTC/USD is Neoxa's own BTC_USDC market") {
		t.Errorf("notes %q", got.notes)
	}
	if _, extra := sideOf(t, got, false); !near(extra, unverifiedSpread) {
		t.Errorf("widened %g on Neoxa's own BTC/USD", extra)
	}
	if cross := f.crossCheck(); !near(cross, 0.00603) {
		t.Errorf("cross-check %g", cross)
	}
}

// A ticker whose own computedAt has stopped is a market that has stopped, and
// it refuses at once.
func TestAFrozenTickerRefuses(t *testing.T) {
	m := newMarket(feedStart)
	f := testFeed(t, m)
	f.read(context.Background())

	m.mu.Lock()
	m.now = m.now.Add(10 * time.Minute)
	m.mu.Unlock()
	f.read(context.Background())

	_, err := f.quoteRate()
	if !errors.Is(err, rate.ErrStale) ||
		!strings.Contains(err.Error(), "has not changed since") {

		t.Fatalf("want a frozen ticker refused, got %v", err)
	}
}

// A move past the breaker stops quoting for the cooldown.
func TestTheBreakerStopsQuoting(t *testing.T) {
	m := newMarket(feedStart)
	f := testFeed(t, m)
	f.read(context.Background())

	// 25% in a minute, every book together.
	m.set(pairDirect, 0.0075, 0.007575)
	m.set(pairUSDC, 0.0075*fakeUSD, 0.007575*fakeUSD)
	m.nonKYC(0.0075*fakeUSD, 0.007575*fakeUSD)
	m.advance(time.Minute)
	f.read(context.Background())

	if _, err := f.quoteRate(); !errors.Is(err, rate.ErrBroken) {
		t.Fatalf("want the breaker, got %v", err)
	}

	// Still tripped ten minutes on, though the market is calm.
	m.advance(10 * time.Minute)
	f.read(context.Background())
	if _, err := f.quoteRate(); !errors.Is(err, rate.ErrBroken) {
		t.Fatalf("the breaker cleared inside its cooldown: %v", err)
	}
}

// The volatility the oracle measured is what widens the fee.
func TestTheFeedReportsTheMarketsMovement(t *testing.T) {
	m := newMarket(feedStart)
	f := testFeed(t, m)
	f.read(context.Background())

	m.set(pairDirect, 0.0063, 0.006363)
	m.set(pairUSDC, 0.0063*fakeUSD, 0.006363*fakeUSD)
	m.nonKYC(0.0063*fakeUSD, 0.006363*fakeUSD)
	m.advance(30 * time.Second)
	f.read(context.Background())

	got, err := f.quoteRate()
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(got.volatility-0.05) > 1e-9 {
		t.Errorf("volatility %g, wanted 0.05", got.volatility)
	}
}

// The day's median starts from Neoxa's hourly candles. A market well above it
// widens the fee of the direction that buys BLAKE2b coin at that price, by a
// quarter of the distance past 10%; the direction selling it is not widened.
// Past 35% the buying direction quotes nothing.
func TestTheDaysMedianWidensTheBuyingSide(t *testing.T) {
	m := newMarket(feedStart)
	closes := make([]float64, 24)
	for i := range closes {
		closes[i] = 0.005
	}
	m.candles(closes...)
	f := testFeed(t, m)

	f.read(context.Background())
	got, err := f.quoteRate()
	if err != nil {
		t.Fatal(err)
	}
	// toSHA256 at 0.006 is 20% over 0.005: 0.25 of 10%.
	if _, extra := sideOf(t, got, false); !near(extra, 0.025) {
		t.Errorf("toSHA256 widened %g, wanted 0.025", extra)
	}
	if _, extra := sideOf(t, got, true); extra != 0 {
		t.Errorf("toBLAKE2b widened %g", extra)
	}
	if !hasNote(got, "above its day's median") {
		t.Errorf("notes %q", got.notes)
	}

	// The same market against a day at 0.0044: 36% over.
	m = newMarket(feedStart)
	for i := range closes {
		closes[i] = 0.0044
	}
	m.candles(closes...)
	f = testFeed(t, m)
	f.read(context.Background())
	got, err = f.quoteRate()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := got.forSide(false); !errors.Is(err, rate.ErrBroken) ||
		!strings.Contains(err.Error(), "day's median") {

		t.Errorf("toSHA256 36%% over the day: %v", err)
	}
	if _, _, err := got.forSide(true); err != nil {
		t.Errorf("toBLAKE2b refused for a market above its day: %v", err)
	}

	// And below it, the other way round.
	m = newMarket(feedStart)
	for i := range closes {
		closes[i] = 0.0075
	}
	m.candles(closes...)
	f = testFeed(t, m)
	f.read(context.Background())
	got, _ = f.quoteRate()
	// toBLAKE2b at 0.00606 is 19.2% under 0.0075.
	want := 0.25 * (1 - 0.00606/0.0075 - 0.10)
	if _, extra := sideOf(t, got, true); !near(extra, want) {
		t.Errorf("toBLAKE2b widened %g, wanted %g", extra, want)
	}
	if _, extra := sideOf(t, got, false); extra != 0 {
		t.Errorf("toSHA256 widened %g", extra)
	}
}

// Without candles the day's median is built from the readings, one every five
// minutes, and counts from an hour of them.
func TestTheDaysMedianBuildsFromReadings(t *testing.T) {
	m := newMarket(feedStart)
	f := testFeed(t, m)
	f.read(context.Background())
	got, _ := f.quoteRate()
	if !hasNote(got, "day's median is not known yet") {
		t.Errorf("notes %q", got.notes)
	}

	for i := 0; i < 12; i++ {
		m.advance(anchorEvery)
		f.read(context.Background())
	}
	f.mu.Lock()
	n, anchor := len(f.anchor), f.anchorOf()
	f.mu.Unlock()
	if n != 13 || !near(anchor, 0.00603) {
		t.Errorf("%d readings, median %g", n, anchor)
	}
	got, _ = f.quoteRate()
	if hasNote(got, "day's median") {
		t.Errorf("notes %q", got.notes)
	}
}

// The real answers each source gave when the feed was written, kept in
// testdata/market: the feed reads every one of them.
func TestTheFeedReadsTheRealSources(t *testing.T) {
	file := func(name string) string {
		b, err := os.ReadFile(filepath.Join("testdata", "market", name))
		if err != nil {
			t.Fatal(err)
		}

		return string(b)
	}

	// When they were fetched, and a minute later.
	at := time.UnixMilli(1792198354494).Add(time.Minute)
	m := &market{now: at, asked: map[string]int{}, body: map[string]string{
		neoxaTickerURL + pairDirect: file("neoxa_ticker_BTCB2_BTC.json"),
		neoxaTickerURL + pairBTCUSDC: file(
			"neoxa_ticker_BTC_USDC.json"),
		bookURL(pairDirect): file("neoxa_orderbook_BTCB2_BTC.json"),
		bookURL(pairUSDC):   file("neoxa_orderbook_BTCB2_USDC.json"),
		nonKYCBookURL:       file("nonkyc_orderbook_BTCB2_USDT.json"),
		neoxaCandlesURL:     file("neoxa_candles_BTCB2_BTC.json"),
		coinGeckoRatesURL:   file("coingecko_exchange_rates.json"),
	}}
	f := testFeed(t, m)
	f.read(context.Background())
	got, err := f.quoteRate()
	if err != nil {
		t.Fatal(err)
	}
	sell, _ := sideOf(t, got, false)
	buy, _ := sideOf(t, got, true)
	if !(sell < got.rate && got.rate < buy) || got.rate < 0.006 ||
		got.rate > 0.01 {

		t.Errorf("sell %g, middle %g, buy %g", sell, got.rate, buy)
	}
	if f.readings[0].second.bid == 0 || f.anchorOf() == 0 {
		t.Errorf("NonKYC %+v, day's median %g", f.readings[0].second,
			f.anchorOf())
	}
	t.Logf("sell %.8g middle %.8g buy %.8g cross %.8g day %.8g notes %q",
		sell, got.rate, buy, f.crossCheck(), f.anchorOf(), got.notes)

	// mempool.space's answer, read on its own.
	m.body[mempoolPricesURL] = file("mempool_prices.json")
	m.now = time.Unix(1792197904, 0)
	if v, err := f.mempool(context.Background(), m.now); err != nil ||
		v != 81448 {

		t.Errorf("mempool.space: %g, %v", v, err)
	}
}

// The feed's requests carry a browser's User-Agent, the same as Tor Browser's,
// rather than one naming the bridge.
func TestTheFeedDoesNotNameItself(t *testing.T) {
	ua := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			ua <- r.Header.Get("User-Agent")
			fmt.Fprint(w, "{}")
		}))
	defer srv.Close()

	if _, err := httpFetch(nil)(context.Background(), srv.URL); err != nil {
		t.Fatal(err)
	}
	got := <-ua
	if got != feedUserAgent || !strings.HasPrefix(got, "Mozilla/5.0") ||
		strings.Contains(strings.ToLower(got), "bridge") ||
		strings.Contains(strings.ToLower(got), "lightning") {

		t.Errorf("user agent %q", got)
	}
}

// The market's options are fractions, and the free band is under the bound.
func TestTheMarketOptionsAreChecked(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		set  func(*Config)
		ok   bool
	}{
		{"defaults", func(*Config) {}, true},
		{"gap", func(c *Config) { c.MaxVenueGap = 0.06 }, true},
		{"negative gap", func(c *Config) { c.MaxVenueGap = -1 }, false},
		{"gap of 4", func(c *Config) { c.MaxVenueGap = 4 }, false},
		{"nan", func(c *Config) { c.DriftWiden = math.NaN() }, false},
		{"band over max", func(c *Config) {
			c.DriftBand, c.DriftMax = 0.3, 0.2
		}, false},
		{"band over default max", func(c *Config) {
			c.DriftBand = 0.4
		}, false},
	} {
		c := usable()
		tc.set(&c)
		if err := c.Validate(); (err == nil) != tc.ok {
			t.Errorf("%s: %v", tc.name, err)
		}
	}

	r := (&Config{}).resolve()
	if r.market.maxVenueGap != 0.04 || r.market.driftBand != 0.10 ||
		r.market.driftWiden != 0.25 || r.market.driftMax != 0.35 ||
		r.market.sizeMsat != r.quote.MaxSwapMsat ||
		r.market.minSizeMsat != r.quote.MinSwapMsat ||
		r.quote.Validity != time.Minute {

		t.Errorf("defaults %+v, validity %v", r.market, r.quote.Validity)
	}
}

// sidesOf is a service's two directions.
func sidesOf(t *testing.T, svc *service) (forward, reverse *side) {
	t.Helper()
	for _, sd := range svc.sides {
		if sd.invert {
			reverse = sd
		} else {
			forward = sd
		}
	}
	if forward == nil || reverse == nil {
		t.Fatal("want both directions")
	}

	return forward, reverse
}

// Following the market, each direction charges its own fee, widened by how far
// the market has just moved.
func TestTheMarketRateIsQuotedWithEachDirectionsFee(t *testing.T) {
	t.Parallel()

	cfg := usable()
	cfg.RateSource = RateSourceNeoxa
	cfg.FixedRate = 0
	cfg.Spread = 0
	cfg.FeeToSHA256 = 0.02
	cfg.FeeToBLAKE2b = 0.01

	m := newMarket(feedStart)
	mkt := testFeed(t, m)
	mkt.read(context.Background())
	// The market moved 5% within the window, on both venues.
	m.set(pairDirect, 0.0063, 0.006363)
	m.set(pairUSDC, 0.0063*fakeUSD, 0.006363*fakeUSD)
	m.nonKYC(0.0063*fakeUSD, 0.006363*fakeUSD)
	m.advance(30 * time.Second)
	mkt.read(context.Background())

	f := &fakeNode{synced: true,
		balance: cfg.resolve().inventory.TargetOutgoingMsat}
	svc := serviceForMarket(t, cfg, f, mkt)
	svc.positionOf = f.deps().ChannelBalance
	fwd, rev := sidesOf(t, svc)

	// The median of the bids, 0.006 and 0.0063, and of the asks.
	want, wantBack := 0.00615, 0.0062115
	got, err := svc.pricer(fwd)(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(got.OutgoingPerIncoming-want) > 1e-12 {
		t.Errorf("forward rate %g, wanted %g", got.OutgoingPerIncoming,
			want)
	}
	if math.Abs(got.Spread-0.07) > 1e-9 {
		t.Errorf("forward spread %g, wanted the 2%% fee plus the 5%% "+
			"move", got.Spread)
	}

	back, err := svc.pricer(rev)(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(back.OutgoingPerIncoming-1/wantBack) > 1e-9 {
		t.Errorf("reverse rate %g", back.OutgoingPerIncoming)
	}
	if math.Abs(back.Spread-0.06) > 1e-9 {
		t.Errorf("reverse spread %g, wanted the 1%% fee plus the 5%% "+
			"move", back.Spread)
	}
}

// Without a current market rate nothing is quoted.
func TestNoMarketRateNoQuote(t *testing.T) {
	t.Parallel()

	cfg := usable()
	cfg.RateSource = RateSourceNeoxa
	m := newMarket(feedStart)
	mkt := testFeed(t, m)

	f := &fakeNode{synced: true, balance: 100_000_000_000}
	svc := serviceForMarket(t, cfg, f, mkt)
	svc.positionOf = f.deps().ChannelBalance
	fwd, _ := sidesOf(t, svc)

	if _, err := svc.pricer(fwd)(context.Background()); err == nil {
		t.Fatal("quoted before the market was read")
	}
	if !svc.heldForRate {
		t.Error("toBLAKE2b came up without a rate to convert its bounds at")
	}

	mkt.read(context.Background())
	m.advance(2 * time.Minute)
	if _, err := svc.pricer(fwd)(context.Background()); err == nil {
		t.Fatal("quoted on a reading two minutes old")
	}
}

// Just before paying, a swap whose payment in no longer covers the payout at the
// market's rate is given back; one that does is paid.
func TestThePriceIsCheckedBeforePaying(t *testing.T) {
	t.Parallel()

	cfg := usable()
	cfg.RateSource = RateSourceNeoxa
	m := newMarket(feedStart)
	mkt := testFeed(t, m)
	mkt.read(context.Background())

	f := &fakeNode{synced: true, balance: 100_000_000_000}
	svc := serviceForMarket(t, cfg, f, mkt)
	fwd, rev := sidesOf(t, svc)

	// toSHA256 at 0.006: 1,000,000 msat of BTCB2 is worth 6,000 msat of
	// SHA256 coin.
	sw := &driver.Swap{IncomingMsat: 1_000_000, OutgoingMsat: 5_900,
		MaxFeeMsat: 50}
	if ok, err := svc.priceCheck(fwd)(context.Background(), sw); err != nil ||
		!ok {

		t.Errorf("a swap still covered was refused: %v %v", ok, err)
	}
	sw.OutgoingMsat = 5_980
	if ok, _ := svc.priceCheck(fwd)(context.Background(), sw); ok {
		t.Error("paid a swap the market no longer covers")
	}

	// toBLAKE2b at 1/0.00606: 6,060 msat of SHA256 coin is worth
	// 1,000,000 msat of BTCB2.
	sw = &driver.Swap{IncomingMsat: 6_060, OutgoingMsat: 990_000,
		MaxFeeMsat: 3_000}
	if ok, err := svc.priceCheck(rev)(context.Background(), sw); err != nil ||
		!ok {

		t.Errorf("a toBLAKE2b swap still covered was refused: %v %v",
			ok, err)
	}
	sw.OutgoingMsat = 999_000
	if ok, _ := svc.priceCheck(rev)(context.Background(), sw); ok {
		t.Error("paid a toBLAKE2b swap the market no longer covers")
	}

	// No current rate is no decision.
	m.advance(5 * time.Minute)
	if _, err := svc.priceCheck(fwd)(context.Background(), sw); err == nil {
		t.Error("decided on a stale rate")
	}
}

// Following the market, the operator's SetRate is refused with a reason.
func TestSetRateIsRefusedWhileFollowingTheMarket(t *testing.T) {
	t.Parallel()

	srv, _, err := New(&Config{Enabled: true, ToSHA256: true,
		Deps: (&fakeNode{}).deps()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = srv.SetRate(context.Background(), &SetRateRequest{Rate: 0.006})
	if err == nil || !strings.Contains(err.Error(), "ratesource=fixed") {
		t.Fatalf("want SetRate refused, got %v", err)
	}
}

// serviceForMarket is serviceFor trading at mkt's rate.
func serviceForMarket(t *testing.T, cfg Config, f *fakeNode,
	mkt *feed) *service {

	t.Helper()

	cfg.Journal = filepath.Join(t.TempDir(), "swaps.journal")
	svc, err := newServiceWith(&cfg, NewLocal(f.deps()),
		remote(nil, nil, nil), mkt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(svc.close)

	return svc
}

// While the bridge waits on its SHA256 node, Status still says where the rate
// comes from and whether the market can be read.
func TestStatusReportsTheMarketBeforeTheBridgeIsUp(t *testing.T) {
	t.Parallel()

	srv, _, err := New(&Config{Enabled: true, ToSHA256: true,
		Deps: (&fakeNode{synced: true}).deps()})
	if err != nil {
		t.Fatal(err)
	}
	m := newMarket(feedStart)
	m.now = time.Now()
	m.advance(time.Since(m.now))
	srv.market = testFeed(t, m)

	resp, err := srv.Status(context.Background(), &StatusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.RateSource != RateSourceNeoxa || resp.FeeToSha256 != DefaultFee {
		t.Errorf("source %q, fee %g", resp.RateSource, resp.FeeToSha256)
	}
	if !strings.Contains(strings.Join(resp.Refusals, "; "),
		"reading the market") {

		t.Errorf("refusals %q", resp.Refusals)
	}

	srv.market.read(context.Background())
	resp, _ = srv.Status(context.Background(), &StatusRequest{})
	if !near(resp.Rate, 0.00603) || resp.RateCrossCheck == 0 {
		t.Errorf("rate %g, cross-check %g", resp.Rate,
			resp.RateCrossCheck)
	}
}

// A breaker that tripped is not undone by a fetch that fails next: the
// pre-move price must not come back while still fresh.
func TestAFailedFetchDoesNotUndoTheBreaker(t *testing.T) {
	m := newMarket(feedStart)
	f := testFeed(t, m)
	f.read(context.Background())

	m.set(pairDirect, 0.0075, 0.007575)
	m.set(pairUSDC, 0.0075*fakeUSD, 0.007575*fakeUSD)
	m.nonKYC(0.0075*fakeUSD, 0.007575*fakeUSD)
	m.advance(20 * time.Second)
	f.read(context.Background())
	if _, err := f.quoteRate(); !errors.Is(err, rate.ErrBroken) {
		t.Fatalf("want the breaker, got %v", err)
	}

	m.fail(bookURL(pairDirect))
	m.advance(20 * time.Second)
	f.read(context.Background())
	if got, err := f.quoteRate(); err == nil {
		t.Fatalf("quoted %g, the price from before the move", got.rate)
	}
}

// The same for a disagreement between the books.
func TestAFailedFetchDoesNotUndoADisagreement(t *testing.T) {
	m := newMarket(feedStart)
	f := testFeed(t, m)
	f.read(context.Background())

	m.set(pairUSDC, 561, 566)
	m.advance(20 * time.Second)
	f.read(context.Background())
	m.fail(bookURL(pairDirect))
	m.advance(20 * time.Second)
	f.read(context.Background())
	if got, err := f.quoteRate(); err == nil {
		t.Fatalf("quoted %g after the books disagreed", got.rate)
	}
}

// toBLAKE2b built before there was a rate quotes nothing: its bounds are not
// converted yet.
func TestAHeldDirectionQuotesNothing(t *testing.T) {
	t.Parallel()

	cfg := usable()
	cfg.RateSource = RateSourceNeoxa
	mkt := testFeed(t, newMarket(feedStart))
	svc := serviceForMarket(t, cfg, &fakeNode{synced: true}, mkt)
	_, rev := sidesOf(t, svc)
	if !svc.heldForRate || rev.quoting() {
		t.Fatalf("held %v, toBLAKE2b quoting %v", svc.heldForRate,
			rev.quoting())
	}
}

// Just before paying, a swap is also given back when the market moved against
// the bridge by more than half the quote's fee since it was quoted, though
// what the payer sent still covers the payout: within the quote's validity
// the move is otherwise the payer's free option.
func TestThePriceCheckRefusesAMoveAgainstTheBridge(t *testing.T) {
	t.Parallel()

	cfg := usable()
	cfg.RateSource = RateSourceNeoxa
	m := newMarket(feedStart)
	mkt := testFeed(t, m)
	mkt.read(context.Background())

	f := &fakeNode{synced: true, balance: 100_000_000_000}
	svc := serviceForMarket(t, cfg, f, mkt)
	fwd, rev := sidesOf(t, svc)
	check := func(sd *side, sw *driver.Swap) bool {
		t.Helper()
		ok, err := svc.priceCheck(sd)(context.Background(), sw)
		if err != nil {
			t.Fatal(err)
		}

		return ok
	}

	// toSHA256 quoted at 0.00611 with a 2% fee, paying out little enough
	// that the amounts alone never refuse: the market now, 0.006, is
	// 1.8% under, more than half the fee.
	sw := &driver.Swap{IncomingMsat: 1_000_000, OutgoingMsat: 5_000,
		MaxFeeMsat: 50, Rate: 0.00611, Spread: 0.02}
	if check(fwd, sw) {
		t.Error("paid a swap the market moved 1.8% against, over half " +
			"its 2% fee")
	}
	// 0.00605: 0.8% under, inside half the fee.
	sw.Rate = 0.00605
	if !check(fwd, sw) {
		t.Error("refused a swap the market moved 0.8% against")
	}
	// A move in the bridge's favour is no reason to refuse.
	sw.Rate = 0.0058
	if !check(fwd, sw) {
		t.Error("refused a swap the market moved in the bridge's favour")
	}

	// toBLAKE2b quoted at 1/0.0059 (BTCB2 cheaper then): at 1/0.00606
	// now, the payout costs 2.7% more SHA256 coin than quoted.
	sw = &driver.Swap{IncomingMsat: 6_060, OutgoingMsat: 900_000,
		MaxFeeMsat: 3_000, Rate: 1 / 0.0059, Spread: 0.02}
	if check(rev, sw) {
		t.Error("paid a toBLAKE2b swap the market moved 2.7% against")
	}
	sw.Rate = 1 / 0.00604
	if !check(rev, sw) {
		t.Error("refused a toBLAKE2b swap the market moved 0.3% against")
	}
}

// The check before paying takes the newest reading when it is worse for the
// bridge than the median the quotes are made at: the median follows a sharp
// move a reading or two late, and a swap quoted before the move and funded
// just after it would pass at the price from before.
func TestThePriceCheckTakesTheNewestReading(t *testing.T) {
	t.Parallel()

	cfg := usable()
	cfg.RateSource = RateSourceNeoxa
	m := newMarket(feedStart)
	mkt := testFeed(t, m)
	at := func(bid float64) {
		m.set(pairDirect, bid, bid*1.01)
		m.ticker(pairDirect, bid, bid, bid*1.01)
		m.set(pairUSDC, bid*fakeUSD, bid*1.01*fakeUSD)
		m.nonKYC(bid*fakeUSD, bid*1.01*fakeUSD)
		mkt.read(context.Background())
		m.advance(30 * time.Second)
	}
	at(0.006)
	at(0.006)
	at(0.006)
	// 8% down in one reading, under the breaker: the median of the four
	// is still 0.006.
	at(0.00552)
	m.advance(-30 * time.Second)

	f := &fakeNode{synced: true, balance: 100_000_000_000}
	svc := serviceForMarket(t, cfg, f, mkt)
	fwd, rev := sidesOf(t, svc)
	got, err := mkt.quoteRate()
	if err != nil {
		t.Fatal(err)
	}
	if r, _ := sideOf(t, got, false); !near(r, 0.006) {
		t.Fatalf("toSHA256 quotes at %g, wanted the median 0.006", r)
	}

	// toSHA256 quoted at 0.006 with the 1.5% fee just before the drop.
	sw := &driver.Swap{IncomingMsat: 1_000_000, OutgoingMsat: 5_000,
		MaxFeeMsat: 50, Rate: 0.006, Spread: 0.015}
	if ok, err := svc.priceCheck(fwd)(context.Background(), sw); err != nil ||
		ok {

		t.Errorf("paid a swap the market fell 8%% under since its "+
			"quote: %v %v", ok, err)
	}

	// toBLAKE2b quoted at 1/0.00606, the ask before the drop: the
	// newest reading is in the bridge's favour there, and the median is
	// the check.
	sw = &driver.Swap{IncomingMsat: 6_060, OutgoingMsat: 900_000,
		MaxFeeMsat: 3_000, Rate: 1 / 0.00606, Spread: 0.015}
	if ok, err := svc.priceCheck(rev)(context.Background(), sw); err != nil ||
		!ok {

		t.Errorf("refused a toBLAKE2b swap the market moved in the "+
			"bridge's favour: %v %v", ok, err)
	}

	// The same the other way: BLAKE2b coin 8% dearer on the newest
	// reading refuses a toBLAKE2b swap quoted before it.
	m = newMarket(feedStart)
	mkt = testFeed(t, m)
	at(0.006)
	at(0.006)
	at(0.006)
	at(0.00648)
	m.advance(-30 * time.Second)
	svc = serviceForMarket(t, cfg, f, mkt)
	_, rev = sidesOf(t, svc)
	if ok, err := svc.priceCheck(rev)(context.Background(), sw); err != nil ||
		ok {

		t.Errorf("paid a toBLAKE2b swap the market rose 8%% against "+
			"since its quote: %v %v", ok, err)
	}
}

// Status names what the reading lacks, and a direction the market refuses on
// its own, while the other quotes.
func TestStatusNotesTheMarket(t *testing.T) {
	t.Parallel()

	srv, _, err := New(&Config{Enabled: true, ToSHA256: true,
		ToBLAKE2b: true, Deps: (&fakeNode{synced: true}).deps()})
	if err != nil {
		t.Fatal(err)
	}
	m := newMarket(feedStart)
	m.now = time.Now()
	m.advance(0)
	m.nonKYC(0, 0)
	srv.market = testFeed(t, m)
	srv.market.read(context.Background())

	resp, err := srv.Status(context.Background(), &StatusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	refused := func() int {
		n := 0
		for _, r := range resp.Refusals {
			if strings.Contains(r, "quotes nothing") {
				n++
			}
		}

		return n
	}
	if !strings.Contains(strings.Join(resp.RateNotes, "; "),
		"is not being read") || refused() != 0 {

		t.Errorf("notes %q, refusals %q", resp.RateNotes, resp.Refusals)
	}

	// NonKYC 5% under Neoxa: both directions stop, each saying so.
	m.nonKYC(0.0057*fakeUSD, 0.005757*fakeUSD)
	m.advance(time.Second)
	srv.market.read(context.Background())
	resp, _ = srv.Status(context.Background(), &StatusRequest{})
	if refused() != 2 || !strings.Contains(strings.Join(resp.Refusals,
		"; "), "toSHA256 quotes nothing") {

		t.Errorf("refusals %q", resp.Refusals)
	}
}

// What the reading could not vouch for widens the quoted fee of the direction
// it concerns: a book too thin on the bids widens toSHA256 alone.
func TestTheReadingsDoubtsWidenTheQuotedFee(t *testing.T) {
	t.Parallel()

	cfg := usable()
	cfg.RateSource = RateSourceNeoxa
	cfg.FixedRate = 0
	cfg.Spread = 0
	cfg.FeeToSHA256 = 0.02
	cfg.FeeToBLAKE2b = 0.01

	m := newMarket(feedStart)
	m.nonKYC(0, 0)
	m.book(pairDirect, []level{{0.006, 0.1}}, []level{{0.00606, 1000}})
	mkt := testFeed(t, m)
	mkt.read(context.Background())

	f := &fakeNode{synced: true,
		balance: cfg.resolve().inventory.TargetOutgoingMsat}
	svc := serviceForMarket(t, cfg, f, mkt)
	svc.positionOf = f.deps().ChannelBalance
	fwd, rev := sidesOf(t, svc)

	got, err := svc.pricer(fwd)(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if want := 0.02 + thinBookSpread*(1-0.1/swapB2B); !near(got.Spread,
		want) {

		t.Errorf("toSHA256 spread %g, wanted %g", got.Spread, want)
	}
	back, err := svc.pricer(rev)(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !near(back.Spread, 0.01) {
		t.Errorf("toBLAKE2b spread %g, wanted its fee alone", back.Spread)
	}
}
