//go:build bridgerpc
// +build bridgerpc

package bridgerpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/paulscode/lightning-fork-bridge/rate"
)

// priceSource is where the service gets the rate it trades at: the operator's
// own (ratebook) or the market's (feed).
type priceSource interface {
	// quoteRate is the rate to trade at now, or why there is none.
	quoteRate() (marketRate, error)

	// last is the most recent rate known, usable or not, and when it was
	// set or measured: what Status shows, and what the toBLAKE2b bounds
	// are converted at. Zero when there has never been one.
	last() (float64, time.Time)
}

// marketRate is a rate to trade at.
type marketRate struct {
	// rate is SHA256 coin per BLAKE2b coin: the operator's own, or the
	// middle of the market's book at the largest swap.
	rate float64

	// at is when it was set or measured.
	at time.Time

	// volatility is how far the market itself moved recently, as a
	// fraction, which the fee is widened by. Zero for a fixed rate, which
	// has no market to measure.
	volatility float64

	// sources is how many readings agreed on it: one for the operator's
	// own, two for the market cross-checked.
	sources int

	// toSHA256 and toBLAKE2b are what each direction trades at, read from
	// the side of the books it trades against (see forSide). Unset for a
	// fixed rate, which both directions trade at.
	toSHA256, toBLAKE2b sideRate

	// notes say what the market reading lacks or widened the fee for,
	// for Status: a venue not read, a timestamp missing, a book too thin.
	notes []string
}

// sideRate is what one direction trades at.
type sideRate struct {
	// rate is SHA256 coin per BLAKE2b coin, zero when unset.
	rate float64

	// extra is added to the fee: what the reading could not vouch for (a
	// book too thin for the largest swap, a venue's clock missing) and
	// how far the market is from its own day.
	extra float64

	// err is why the direction quotes nothing, when it does not.
	err error
}

// forSide is the rate a direction trades at and what its fee is widened by:
// paying out SHA256 coin for BLAKE2b coin (invert false) at what selling the
// largest swap's BLAKE2b coin into the bids fetches, on whichever venue
// fetches less; paying out BLAKE2b coin (invert true, the rate turned over
// afterwards) at what buying it from the asks costs, on whichever venue costs
// more. A venue moved on its own then moves nobody's price in their favour:
// whoever pushes BLAKE2b up on one exchange to sell it to the bridge is priced
// at the other. A fixed rate is the same both ways.
func (m marketRate) forSide(invert bool) (float64, float64, error) {
	s := m.toSHA256
	if invert {
		s = m.toBLAKE2b
	}
	if s.err != nil {
		return 0, 0, s.err
	}
	if s.rate <= 0 {
		return m.rate, 0, nil
	}

	return s.rate, s.extra, nil
}

// quoteRate is the operator's rate while it is fresh.
func (b *ratebook) quoteRate() (marketRate, error) {
	r, at, err := b.usable()
	if err != nil {
		return marketRate{}, err
	}

	return marketRate{rate: r, at: at, sources: 1}, nil
}

// last is the operator's rate, fresh or not.
func (b *ratebook) last() (float64, time.Time) {
	return b.current()
}

// The market the feed follows.
//
// Neoxa is where BTCB2 trades, and its BTCB2_BTC book is the price payers'
// own wallets check a service's quote against (the dashboard's reference
// rate). Its BTCB2_USDC book, over a BTC/USD price, is the cross-check: a
// different book that should say the same thing, and when it does not, one of
// them is wrong and there is no telling which. NonKYC's BTCB2_USDT book, over
// the same BTC/USD, is a second venue: another exchange, so that Neoxa alone
// cannot be moved to move the price.
//
// Every price is read from a book at the largest swap's size, the
// volume-weighted price of trading that much against the side a direction
// trades against, rather than from a ticker's last trade and best orders: an
// order placed at the top of a book this thin and cancelled again moves a
// ticker, and moves a price read at size only by what it would really fill.
const (
	neoxaTickerURL = "https://neoxa.exchange/api/exchange/ticker/"
	neoxaBookURL   = "https://neoxa.exchange/api/exchange/orderbook/"

	// neoxaBookDepth is how many levels a side of Neoxa's book is read to.
	// Its default is 50.
	neoxaBookDepth = 100

	// neoxaCandlesURL is the last day of Neoxa's market by the hour, which
	// the day's anchor starts from (see anchorOf).
	neoxaCandlesURL = "https://neoxa.exchange/api/exchange/candles/" +
		pairDirect + "?interval=1h&limit=24"

	// The pair priced from, and the one cross-checked against.
	pairDirect = "BTCB2_BTC"
	pairUSDC   = "BTCB2_USDC"

	// BTC/USD for the cross-check and the second venue: CoinGecko's, then
	// mempool.space's, and only when neither can be read Neoxa's own
	// BTC_USDC book, with the fee widened (see btcUSD). CoinGecko and
	// mempool.space refuse some Tor exits.
	coinGeckoRatesURL = "https://api.coingecko.com/api/v3/exchange_rates"
	mempoolPricesURL  = "https://mempool.space/api/v1/prices"
	pairBTCUSDC       = "BTC_USDC"

	// The second venue's book. It is the busiest BTCB2 book at the time of
	// writing.
	nonKYCBookURL = "https://api.nonkyc.io/api/v2/market/orderbook?" +
		"symbol=BTCB2/USDT&limit=100"
	pairNonKYC = "BTCB2_USDT"

	// feedUserAgent is the one Tor Browser sends: the sources are read
	// over Tor when the node uses it, and a name of its own would single
	// a bridge out among their users.
	feedUserAgent = "Mozilla/5.0 (Windows NT 10.0; rv:128.0) " +
		"Gecko/20100101 Firefox/128.0"
)

// Timing.
const (
	// feedInterval is how often the market is read.
	feedInterval = 30 * time.Second

	// feedTimeout bounds one reading of every source.
	feedTimeout = 20 * time.Second

	// smoothWindow is how far back readings are taken into the median
	// the bridge trades at: the last three or four. A median, not an
	// average: one odd reading moves it not at all, and it lags the
	// market by a reading at most, where a longer average would hand
	// anyone watching a stale price to trade against.
	smoothWindow = 100 * time.Second

	// tickerFrozenAfter is how old a venue's own timestamp may be. Neoxa
	// recomputes its tickers every few seconds; one minutes old is a
	// ticker that has stopped, whatever the request says. Generous,
	// because it compares the venue's clock with this node's.
	tickerFrozenAfter = 5 * time.Minute

	// btcUSDInterval is how often BTC/USD is asked for. It moves slowly
	// beside BTCB2, and it is only the cross-check's and the second
	// venue's conversion.
	btcUSDInterval = 5 * time.Minute

	// btcUSDMaxAge is how long a BTC/USD reading is used: three tries,
	// so that one round of both sources failing is not a fallback.
	btcUSDMaxAge = 3 * btcUSDInterval

	// anchorWindow is how far back the anchor's median reaches, and
	// anchorEvery how often a reading is added to it.
	anchorWindow = 24 * time.Hour
	anchorEvery  = 5 * time.Minute

	// anchorMinSamples is how many readings an anchor needs before it
	// counts: an hour of them, or the day's candles.
	anchorMinSamples = 12

	// stuckAfter is how long a venue's book may stay exactly the same
	// while the other venue moves by stuckMove before it is taken for a
	// feed that stopped updating.
	stuckAfter = 10 * time.Minute
	stuckMove  = 0.005

	// maxFeedBody bounds a response.
	maxFeedBody = 1 << 20
)

// What the fee is widened by for what a reading cannot vouch for. Each is
// small beside the fee: the reading is still the best there is, and quoting
// nothing would be an outage for a doubt.
const (
	// unverifiedSpread is added while a venue's book has no timestamp of
	// its own, or has not changed while the other venue moved, or BTC/USD
	// comes from Neoxa's own book.
	unverifiedSpread = 0.005

	// thinBookSpread is added in full for a side of a book that could
	// fill none of the largest swap, in proportion for part of it: what
	// the rest would cost beyond the levels shown is not known.
	thinBookSpread = 0.05
)

var (
	// errNoReading is the feed before its first reading.
	errNoReading = errors.New("the market has not been read yet")

	// errFeedUnreadable is a source that could not be read.
	errFeedUnreadable = errors.New("the market could not be read")
)

// fetchFunc reads a URL.
type fetchFunc func(ctx context.Context, url string) ([]byte, error)

// feedPolicy is what the feed is told.
type feedPolicy struct {
	// rate is the oracle's: freshness, cross-check, breaker.
	rate rate.Policy

	// sizeMsat is the largest swap, in SHA256 millisatoshis: the size the
	// books are read at.
	sizeMsat uint64

	// maxVenueGap is how far apart the two venues may be, as a fraction,
	// before a direction stops quoting.
	maxVenueGap float64

	// driftBand, driftWiden and driftMax price a market away from its
	// day's median: within driftBand nothing, past it driftWiden of the
	// excess is added to the fee, and past driftMax the direction that
	// would buy at the moved price stops quoting.
	driftBand, driftWiden, driftMax float64
}

// feed follows the market.
type feed struct {
	fetch fetchFunc
	now   func() time.Time

	policy feedPolicy
	oracle *rate.Oracle

	// onReading is called after every reading that produced a rate, for
	// the server to bring up a direction held for want of one. Outside
	// the lock.
	onReading func()

	// measureMu keeps one reading at a time, and guards what only a
	// reading touches: BTC/USD, the anchor's start, the stuck books.
	measureMu sync.Mutex
	usd       usdReading
	usdTried  time.Time
	booted    bool
	bootTried time.Time
	stuck     [2]stuckBook

	mu sync.Mutex

	// readings are the recent good ones, oldest first.
	readings []poll

	// anchor is the day's readings, one per anchorEvery, oldest first.
	anchor []rate.Reading

	// cross is the cross-check's last value, for Status.
	cross float64

	// err is why the last reading produced no rate, nil when it did.
	// unsafe says it was a refusal on the numbers (the books disagree,
	// the breaker tripped, a ticker stopped) rather than a source that
	// did not answer: that stops quoting at once, where an unanswered
	// reading leaves the last good one usable until it is too old.
	err    error
	unsafe bool
}

// poll is one reading of the market, in SHA256 coin per BLAKE2b coin.
type poll struct {
	at time.Time

	// neoxa is Neoxa's BTCB2_BTC book at the largest swap, and second
	// NonKYC's over BTC/USD, zero when it was not read.
	neoxa, second depth

	// mid is the middle of Neoxa's: what the breaker and Status follow.
	mid float64

	// volatility is the oracle's measure at this reading.
	volatility float64

	// extra is what the fee is widened by in both directions for what
	// this reading could not vouch for, and notes say what.
	extra float64
	notes []string
}

// depth is a book read at the largest swap.
type depth struct {
	// bid is what selling it fetches per coin, ask what buying it costs,
	// each over as much of it as the book could fill.
	bid, ask float64

	// bidShort and askShort are the fraction of it each side could not
	// fill.
	bidShort, askShort float64
}

func (d depth) mid() float64 {
	return (d.bid + d.ask) / 2
}

// scaled is the book in another unit, the price divided by per.
func (d depth) scaled(per float64) depth {
	d.bid /= per
	d.ask /= per

	return d
}

// usdReading is BTC/USD and where and when it was read.
type usdReading struct {
	value  float64
	at     time.Time
	source string
}

// stuckBook follows a venue whose book stays the same.
type stuckBook struct {
	// seen is the book last read, and since when it has been so, and
	// other the other venue's middle then.
	seen  depth
	since time.Time
	other float64
}

// newFeed builds a feed reading through dial: the node's own dialler, so that
// a node set to reach the world through Tor reads the market through Tor too.
// Nil dials directly.
func newFeed(policy feedPolicy,
	dial func(network, address string, timeout time.Duration) (net.Conn,
		error)) (*feed, error) {

	oracle, err := rate.New(policy.rate)
	if err != nil {
		return nil, err
	}

	return &feed{
		fetch: marketFetch(dial), now: time.Now, policy: policy,
		oracle: oracle,
	}, nil
}

// marketFetch builds what the feed reads with: httpFetch, except in tests,
// which must not reach the network.
var marketFetch = httpFetch

// httpFetch reads URLs over dial.
func httpFetch(dial func(network, address string,
	timeout time.Duration) (net.Conn, error)) fetchFunc {

	transport := &http.Transport{
		TLSHandshakeTimeout: feedTimeout,
		MaxIdleConns:        4,
		IdleConnTimeout:     2 * feedInterval,
	}
	if dial != nil {
		transport.DialContext = func(ctx context.Context, network,
			address string) (net.Conn, error) {

			timeout := feedTimeout
			if d, ok := ctx.Deadline(); ok {
				timeout = time.Until(d)
			}

			return dial(network, address, timeout)
		}
	}
	client := &http.Client{Transport: transport, Timeout: feedTimeout}

	return func(ctx context.Context, url string) ([]byte, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url,
			nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", feedUserAgent)
		req.Header.Set("Accept", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
		}

		return io.ReadAll(io.LimitReader(resp.Body, maxFeedBody))
	}
}

// run reads the market until ctx ends.
func (f *feed) run(ctx context.Context) {
	tick := time.NewTicker(feedInterval)
	defer tick.Stop()

	for {
		f.read(ctx)

		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// read takes one reading of the market.
func (f *feed) read(ctx context.Context) {
	f.measureMu.Lock()
	defer f.measureMu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, feedTimeout)
	defer cancel()

	p, cross, err, unsafe := f.measure(ctx)
	f.boot(ctx)

	f.mu.Lock()
	wasErr := f.err
	f.err, f.unsafe = err, unsafe
	if cross > 0 {
		f.cross = cross
	}
	if unsafe {
		// A refusal on the numbers (the books disagree, the breaker
		// tripped, a ticker stopped) voids the readings before it: a
		// fetch that merely fails next must not bring the pre-move
		// price back while it is still fresh. The next good reading
		// starts the median again, and the breaker, kept by the
		// oracle, refuses until it clears.
		f.readings = nil
	}
	if err == nil {
		f.readings = append(f.readings, p)
		// Twice the window is all the median and Status need.
		cut := 0
		for cut < len(f.readings)-1 &&
			p.at.Sub(f.readings[cut].at) > 2*smoothWindow {

			cut++
		}
		f.readings = f.readings[cut:]

		if n := len(f.anchor); n == 0 ||
			p.at.Sub(f.anchor[n-1].At) >= anchorEvery {

			f.anchor = append(f.anchor, rate.Reading{
				OutgoingPerIncoming: p.mid, At: p.at,
			})
		}
		cut = 0
		for cut < len(f.anchor) &&
			p.at.Sub(f.anchor[cut].At) > anchorWindow {

			cut++
		}
		f.anchor = f.anchor[cut:]
	}
	f.mu.Unlock()

	switch {
	case err != nil && (wasErr == nil ||
		wasErr.Error() != err.Error()):

		log.Warnf("Bridge market rate: %v", err)

	case err == nil && wasErr != nil:
		log.Infof("Bridge market rate: reading again, %g SHA256 coin "+
			"per BLAKE2b coin", p.mid)
	}

	if err == nil && f.onReading != nil {
		f.onReading()
	}
}

// sources is one round of fetches, read at once.
type sources struct {
	ticker, book, usdc, second         []byte
	tickerErr, bookErr, usdcErr, nkErr error
}

// fetchAll reads every per-reading source in parallel: over Tor each takes a
// second or more, and one that hangs must not take the others' time.
func (f *feed) fetchAll(ctx context.Context) sources {
	var s sources
	var wg sync.WaitGroup
	get := func(url string, body *[]byte, err *error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			*body, *err = f.fetch(ctx, url)
		}()
	}
	get(neoxaTickerURL+pairDirect, &s.ticker, &s.tickerErr)
	get(fmt.Sprintf("%s%s?depth=%d", neoxaBookURL, pairDirect,
		neoxaBookDepth), &s.book, &s.bookErr)
	get(fmt.Sprintf("%s%s?depth=%d", neoxaBookURL, pairUSDC,
		neoxaBookDepth), &s.usdc, &s.usdcErr)
	get(nonKYCBookURL, &s.second, &s.nkErr)
	wg.Wait()

	return s
}

// measure reads every source and decides.
func (f *feed) measure(ctx context.Context) (poll, float64, error, bool) {
	now := f.now()
	src := f.fetchAll(ctx)
	p := poll{at: now}
	note := func(extra float64, format string, args ...any) {
		p.extra += extra
		p.notes = append(p.notes, fmt.Sprintf(format, args...))
	}

	// Neoxa's own clock, which only its ticker carries. A ticker that
	// has stopped is a market that has; one that does not say, or does
	// not answer while the books do, is a reading not vouched for.
	computed, err := neoxaComputedAt(src.ticker, src.tickerErr, pairDirect)
	switch {
	case err != nil:
		log.Debugf("Bridge market rate: %v", err)
		note(unverifiedSpread, "Neoxa's %s ticker could not be read "+
			"to confirm the book is current; fee widened %.1f%%",
			pairDirect, unverifiedSpread*100)

	case computed.IsZero():
		note(unverifiedSpread, "Neoxa's %s ticker carries no time to "+
			"confirm the book is current; fee widened %.1f%%",
			pairDirect, unverifiedSpread*100)

	case now.Sub(computed) > tickerFrozenAfter:
		return poll{}, 0, fmt.Errorf("%w: Neoxa's %s ticker has not "+
			"changed since %s", rate.ErrStale, pairDirect,
			computed.UTC().Format(time.RFC3339)), true
	}

	if src.bookErr != nil {
		return poll{}, 0, fmt.Errorf("%w: Neoxa's %s market: %v",
			errFeedUnreadable, pairDirect, src.bookErr), false
	}
	direct, err := parseNeoxaBook(src.book, pairDirect)
	if err != nil {
		return poll{}, 0, err, false
	}

	// The largest swap in BLAKE2b coin, at the top of the book: the size
	// every book is read at.
	top := (direct.bids[0].price + direct.asks[0].price) / 2
	size := float64(f.policy.sizeMsat) / 1e11 / top
	if !positive(size) {
		// No largest swap: the top of the book.
		size = 1e-12
	}
	p.neoxa = direct.at(size)
	p.mid = p.neoxa.mid()

	usd, usdNote, err := f.btcUSD(ctx, now)
	if err != nil {
		return poll{}, 0, fmt.Errorf("the cross-check: %w", err),
			errors.Is(err, rate.ErrStale)
	}
	if usdNote != "" {
		note(unverifiedSpread, "%s; fee widened %.1f%%", usdNote,
			unverifiedSpread*100)
	}

	if src.usdcErr != nil {
		err := fmt.Errorf("the cross-check: %w: Neoxa's %s market: %v",
			errFeedUnreadable, pairUSDC, src.usdcErr)

		return poll{}, 0, err, false
	}
	usdc, err := parseNeoxaBook(src.usdc, pairUSDC)
	if err != nil {
		return poll{}, 0, fmt.Errorf("the cross-check: %w", err), false
	}
	cross := usdc.at(size).scaled(usd).mid()
	if gap := math.Abs(p.mid-cross) / math.Min(p.mid, cross); gap >
		f.policy.rate.MaxDisagreement {

		err := fmt.Errorf("%w: Neoxa's %s market says %.8g; its %s "+
			"market over BTC/USD says %.8g, %.1f%% apart, more "+
			"than the %.1f%% allowed", rate.ErrDisagreement,
			pairDirect, p.mid, pairUSDC, cross, gap*100,
			f.policy.rate.MaxDisagreement*100)

		return poll{}, cross, err, true
	}

	// The second venue, when it answers, is kept beside the price (see
	// forSide). When it does not, the bridge prices on Neoxa's two books
	// rather than stop for another exchange's outage, and says so.
	if book, err := f.nonKYC(src.second, src.nkErr, now); err != nil {
		log.Debugf("Bridge market rate: the second venue: %v", err)
	} else {
		p.second = book.at(size).scaled(usd)
		if book.at0.IsZero() {
			note(unverifiedSpread, "NonKYC's %s book carries no "+
				"time to confirm it is current; fee widened "+
				"%.1f%%", pairNonKYC, unverifiedSpread*100)
		}
	}

	// A book that has not changed at all while the other venue moved has
	// probably stopped being updated, whatever its clock says.
	if f.stillFor(0, p.neoxa, p.second.mid(), now) {
		note(unverifiedSpread, "Neoxa's %s book has not changed in "+
			"%v while NonKYC's moved; fee widened %.1f%%",
			pairDirect, stuckAfter, unverifiedSpread*100)
	}
	if p.second.bid > 0 && f.stillFor(1, p.second, p.mid, now) {
		note(unverifiedSpread, "NonKYC's %s book has not changed in "+
			"%v while Neoxa's moved; fee widened %.1f%%",
			pairNonKYC, stuckAfter, unverifiedSpread*100)
	}

	// The breaker and the market's movement follow the middle of Neoxa's
	// book; each direction's own side is applied in quoteRate.
	r, err := f.oracle.Update(now, []rate.Observation{{
		Source: "neoxa " + pairDirect, OutgoingPerIncoming: p.mid,
		At: now,
	}})
	if err != nil {
		return poll{}, cross, err, true
	}
	p.volatility = r.Volatility

	return p, cross, nil, false
}

// stillFor reports whether venue i's book has stayed exactly d for stuckAfter
// while the other venue's middle, now other, moved by more than stuckMove.
// Without the other venue there is nothing to compare with.
func (f *feed) stillFor(i int, d depth, other float64, now time.Time) bool {
	s := &f.stuck[i]
	if d != s.seen || s.other <= 0 {
		*s = stuckBook{seen: d, since: now, other: other}

		return false
	}

	return now.Sub(s.since) >= stuckAfter && other > 0 &&
		math.Abs(other/s.other-1) > stuckMove
}

// level is one price in a book and how much stands at it.
type level struct {
	price, quantity float64
}

// book is one side and the other of a venue's book, best first.
type book struct {
	bids, asks []level

	// at0 is when the venue says it is from, zero when it does not say.
	at0 time.Time
}

// at reads the book at size: what selling it into the bids fetches per coin,
// and buying it from the asks costs, each over as much as the book holds.
func (b book) at(size float64) depth {
	bid, bidFilled := fill(b.bids, size)
	ask, askFilled := fill(b.asks, size)

	return depth{
		bid: bid, ask: ask,
		bidShort: 1 - bidFilled/size, askShort: 1 - askFilled/size,
	}
}

// fill is the volume-weighted price of taking size from levels, best first,
// and how much of it they held.
func fill(levels []level, size float64) (float64, float64) {
	var got, cost float64
	for _, l := range levels {
		take := math.Min(l.quantity, size-got)
		got += take
		cost += take * l.price
		if got >= size {
			break
		}
	}

	return cost / got, got
}

// flexNum is a number sent as a JSON number or a string, as NonKYC sends
// them.
type flexNum float64

func (n *flexNum) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return err
	}
	*n = flexNum(v)

	return nil
}

// rawLevel is a level as both venues send it.
type rawLevel struct {
	Price    flexNum `json:"price"`
	Quantity flexNum `json:"quantity"`
}

// sides orders a book's raw levels best first and checks it is a book: both
// sides present, every level positive, not crossed.
func sides(rawBids, rawAsks []rawLevel) (book, bool) {
	var b book
	for _, side := range []struct {
		raw []rawLevel
		out *[]level
	}{{rawBids, &b.bids}, {rawAsks, &b.asks}} {
		for _, r := range side.raw {
			p, q := float64(r.Price), float64(r.Quantity)
			if !positive(p) || !positive(q) {
				return book{}, false
			}
			*side.out = append(*side.out, level{p, q})
		}
	}
	if len(b.bids) == 0 || len(b.asks) == 0 {
		return book{}, false
	}
	sort.Slice(b.bids, func(i, j int) bool {
		return b.bids[i].price > b.bids[j].price
	})
	sort.Slice(b.asks, func(i, j int) bool {
		return b.asks[i].price < b.asks[j].price
	})

	return b, b.bids[0].price < b.asks[0].price
}

// neoxaBook is Neoxa's book as it sends it.
type neoxaBook struct {
	Success bool       `json:"success"`
	Pair    string     `json:"pair"`
	Bids    []rawLevel `json:"bids"`
	Asks    []rawLevel `json:"asks"`
}

// parseNeoxaBook reads one of Neoxa's books. Its levels include those of its
// pool, which trades like any other order.
func parseNeoxaBook(raw []byte, pair string) (book, error) {
	var nb neoxaBook
	if err := json.Unmarshal(raw, &nb); err != nil || !nb.Success ||
		(nb.Pair != "" && nb.Pair != pair) {

		return book{}, fmt.Errorf("%w: Neoxa's %s market answered "+
			"with something other than its book", errFeedUnreadable,
			pair)
	}
	b, ok := sides(nb.Bids, nb.Asks)
	if !ok {
		return book{}, fmt.Errorf("%w: Neoxa's %s market has no "+
			"usable book", errFeedUnreadable, pair)
	}

	return b, nil
}

// nonKYCBook is NonKYC's book as it sends it.
type nonKYCBook struct {
	Symbol    string     `json:"symbol"`
	Timestamp int64      `json:"timestamp"`
	Bids      []rawLevel `json:"bids"`
	Asks      []rawLevel `json:"asks"`
}

// nonKYC reads NonKYC's BTCB2_USDT book, in dollars per BTCB2. A book whose
// own time is old is not read: that venue has stopped, and the bridge prices
// without it.
func (f *feed) nonKYC(raw []byte, err error, now time.Time) (book, error) {
	if err != nil {
		return book{}, fmt.Errorf("%w: NonKYC's %s market: %v",
			errFeedUnreadable, pairNonKYC, err)
	}

	var nb nonKYCBook
	if err := json.Unmarshal(raw, &nb); err != nil ||
		(nb.Symbol != "" && nb.Symbol != "BTCB2/USDT") {

		return book{}, fmt.Errorf("%w: NonKYC's %s market answered "+
			"with something other than its book", errFeedUnreadable,
			pairNonKYC)
	}
	b, ok := sides(nb.Bids, nb.Asks)
	if !ok {
		return book{}, fmt.Errorf("%w: NonKYC's %s market has no "+
			"usable book", errFeedUnreadable, pairNonKYC)
	}
	if nb.Timestamp > 0 {
		b.at0 = time.UnixMilli(nb.Timestamp)
		if now.Sub(b.at0) > tickerFrozenAfter {
			return book{}, fmt.Errorf("%w: NonKYC's %s book has "+
				"not changed since %s", rate.ErrStale,
				pairNonKYC, b.at0.UTC().Format(time.RFC3339))
		}
	}

	return b, nil
}

// neoxaTicker is the part of Neoxa's ticker the feed reads.
type neoxaTicker struct {
	Success bool   `json:"success"`
	Pair    string `json:"pair"`
	Ticker  struct {
		LastPrice  float64 `json:"lastPrice"`
		BestBid    float64 `json:"bestBid"`
		BestAsk    float64 `json:"bestAsk"`
		ComputedAt int64   `json:"computedAt"`
	} `json:"ticker"`
}

// parseNeoxaTicker reads one of Neoxa's tickers.
func parseNeoxaTicker(raw []byte, fetchErr error, pair string) (neoxaTicker,
	error) {

	if fetchErr != nil {
		return neoxaTicker{}, fmt.Errorf("%w: Neoxa's %s market: %v",
			errFeedUnreadable, pair, fetchErr)
	}
	var t neoxaTicker
	if err := json.Unmarshal(raw, &t); err != nil || !t.Success ||
		(t.Pair != "" && t.Pair != pair) {

		return neoxaTicker{}, fmt.Errorf("%w: Neoxa's %s market "+
			"answered with something other than its ticker",
			errFeedUnreadable, pair)
	}

	return t, nil
}

// neoxaComputedAt is when Neoxa last computed a ticker, zero when it does not
// say.
func neoxaComputedAt(raw []byte, fetchErr error, pair string) (time.Time,
	error) {

	t, err := parseNeoxaTicker(raw, fetchErr, pair)
	if err != nil || t.Ticker.ComputedAt <= 0 {
		return time.Time{}, err
	}

	return time.UnixMilli(t.Ticker.ComputedAt), nil
}

// tickerPrice reads one of Neoxa's tickers as a price: the middle of its last
// trade, best bid and best ask, or the last trade alone without a usable
// book. Only BTC/USD's last fallback reads a ticker as a price.
func (f *feed) tickerPrice(ctx context.Context, pair string,
	now time.Time) (float64, error) {

	raw, err := f.fetch(ctx, neoxaTickerURL+pair)
	t, err := parseNeoxaTicker(raw, err, pair)
	if err != nil {
		return 0, err
	}
	if t.Ticker.ComputedAt > 0 {
		computed := time.UnixMilli(t.Ticker.ComputedAt)
		if age := now.Sub(computed); age > tickerFrozenAfter {
			return 0, fmt.Errorf("%w: Neoxa's %s ticker has not "+
				"changed since %s", rate.ErrStale, pair,
				computed.UTC().Format(time.RFC3339))
		}
	}

	last, bid, ask := t.Ticker.LastPrice, t.Ticker.BestBid, t.Ticker.BestAsk
	if !positive(last) {
		return 0, fmt.Errorf("%w: Neoxa's %s market has no last price",
			errFeedUnreadable, pair)
	}
	if !positive(bid) || !positive(ask) || bid > ask {
		return last, nil
	}
	prices := []float64{last, bid, ask}
	sort.Float64s(prices)

	return prices[1], nil
}

// btcUSD is dollars per BTC, and a note when it is not from an outside source.
//
// CoinGecko is asked at most every btcUSDInterval, and mempool.space when it
// does not answer; either one's answer is used for btcUSDMaxAge. Only when
// both have failed for that long is Neoxa's own BTC_USDC book used, which
// the note says, for the fee to be widened: the same exchange then prices
// both sides of the cross-check, which is less of one.
func (f *feed) btcUSD(ctx context.Context, now time.Time) (float64, string,
	error) {

	if f.usdTried.IsZero() || now.Sub(f.usdTried) >= btcUSDInterval {
		f.usdTried = now
		if v, err := f.coinGecko(ctx); err == nil {
			f.usd = usdReading{v, now, "CoinGecko"}
		} else if v, err2 := f.mempool(ctx, now); err2 == nil {
			f.usd = usdReading{v, now, "mempool.space"}
		} else {
			log.Debugf("Bridge market rate: BTC/USD: %v; %v", err,
				err2)
		}
	}
	if positive(f.usd.value) && now.Sub(f.usd.at) <= btcUSDMaxAge {
		return f.usd.value, "", nil
	}

	v, err := f.tickerPrice(ctx, pairBTCUSDC, now)
	if err != nil {
		return 0, "", err
	}

	return v, "BTC/USD is Neoxa's own " + pairBTCUSDC + " market: " +
		"CoinGecko and mempool.space could not be read", nil
}

// coinGecko is CoinGecko's dollars per BTC.
func (f *feed) coinGecko(ctx context.Context) (float64, error) {
	raw, err := f.fetch(ctx, coinGeckoRatesURL)
	if err != nil {
		return 0, fmt.Errorf("CoinGecko: %w", err)
	}
	var cg struct {
		Rates map[string]struct {
			Value float64 `json:"value"`
		} `json:"rates"`
	}
	// The rates are per BTC, so the dollar's is dollars per BTC.
	if json.Unmarshal(raw, &cg) != nil || !positive(cg.Rates["usd"].Value) {
		return 0, errors.New("CoinGecko answered without a dollar rate")
	}

	return cg.Rates["usd"].Value, nil
}

// mempoolMaxAge is how old mempool.space's own time on its prices may be. It
// updates them every few minutes.
const mempoolMaxAge = time.Hour

// mempool is mempool.space's dollars per BTC.
func (f *feed) mempool(ctx context.Context, now time.Time) (float64, error) {
	raw, err := f.fetch(ctx, mempoolPricesURL)
	if err != nil {
		return 0, fmt.Errorf("mempool.space: %w", err)
	}
	var mp struct {
		Time int64   `json:"time"`
		USD  float64 `json:"USD"`
	}
	if json.Unmarshal(raw, &mp) != nil || !positive(mp.USD) {
		return 0, errors.New("mempool.space answered without a " +
			"dollar price")
	}
	if mp.Time > 0 && now.Sub(time.Unix(mp.Time, 0)) > mempoolMaxAge {
		return 0, fmt.Errorf("mempool.space's price is from %s",
			time.Unix(mp.Time, 0).UTC().Format(time.RFC3339))
	}

	return mp.USD, nil
}

// boot starts the anchor from the day's hourly candles, so that a node just
// started is anchored too. Tried at most every anchorEvery until it works.
// Called with measureMu held.
func (f *feed) boot(ctx context.Context) {
	now := f.now()
	if f.booted || (!f.bootTried.IsZero() &&
		now.Sub(f.bootTried) < anchorEvery) {

		return
	}
	f.bootTried = now

	raw, err := f.fetch(ctx, neoxaCandlesURL)
	if err != nil {
		log.Debugf("Bridge market rate: the day's candles: %v", err)

		return
	}
	var c struct {
		Success bool `json:"success"`
		Candles []struct {
			Time  int64   `json:"time"`
			Close float64 `json:"close"`
		} `json:"candles"`
	}
	if json.Unmarshal(raw, &c) != nil || !c.Success {
		log.Debugf("Bridge market rate: the day's candles answered " +
			"with something else")

		return
	}

	var past []rate.Reading
	for _, k := range c.Candles {
		at := time.Unix(k.Time, 0)
		if !positive(k.Close) || now.Sub(at) > anchorWindow ||
			at.After(now) {

			continue
		}
		past = append(past, rate.Reading{
			OutgoingPerIncoming: k.Close, At: at,
		})
	}
	sort.Slice(past, func(i, j int) bool {
		return past[i].At.Before(past[j].At)
	})

	f.mu.Lock()
	defer f.mu.Unlock()
	// Before the readings already taken, none of which they overlap.
	var first time.Time
	if len(f.anchor) > 0 {
		first = f.anchor[0].At
	}
	keep := past[:0]
	for _, r := range past {
		if first.IsZero() || r.At.Before(first) {
			keep = append(keep, r)
		}
	}
	f.anchor = append(keep, f.anchor...)
	f.booted = true
}

func positive(v float64) bool {
	return v > 0 && !math.IsInf(v, 0) && !math.IsNaN(v)
}

// quoteRate is the median of the recent readings, while the newest is fresh,
// with each direction priced from its own side of the books.
func (f *feed) quoteRate() (marketRate, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil && f.unsafe {
		return marketRate{}, f.err
	}
	if len(f.readings) == 0 {
		if f.err != nil {
			return marketRate{}, f.err
		}

		return marketRate{}, errNoReading
	}

	newest := f.readings[len(f.readings)-1]
	if age := f.now().Sub(newest.at); age > f.policy.rate.MaxAge {
		why := ""
		if f.err != nil {
			why = fmt.Sprintf(" (%v)", f.err)
		}

		return marketRate{}, fmt.Errorf("%w: the market was last read "+
			"%v ago, and the bridge trades only on a reading at most "+
			"%v old%s", rate.ErrStale, age.Round(time.Second),
			f.policy.rate.MaxAge, why)
	}

	of := func(get func(poll) float64) float64 {
		var recent []float64
		for _, p := range f.readings {
			if v := get(p); v > 0 &&
				newest.at.Sub(p.at) <= smoothWindow {

				recent = append(recent, v)
			}
		}

		return median(recent)
	}
	neoxa := depth{
		bid:      of(func(p poll) float64 { return p.neoxa.bid }),
		ask:      of(func(p poll) float64 { return p.neoxa.ask }),
		bidShort: newest.neoxa.bidShort,
		askShort: newest.neoxa.askShort,
	}
	second := depth{
		bid: of(func(p poll) float64 { return p.second.bid }),
		ask: of(func(p poll) float64 { return p.second.ask }),
	}
	for i := len(f.readings) - 1; i >= 0; i-- {
		if f.readings[i].second.bid > 0 {
			second.bidShort = f.readings[i].second.bidShort
			second.askShort = f.readings[i].second.askShort

			break
		}
	}

	m := marketRate{
		rate: of(func(p poll) float64 { return p.mid }), at: newest.at,
		volatility: newest.volatility, sources: 2,
		notes: append([]string(nil), newest.notes...),
	}
	if second.bid <= 0 {
		m.notes = append(m.notes, "the second venue (NonKYC) is not "+
			"being read, so the bridge prices on Neoxa alone; "+
			"NonKYC often refuses Tor exits")
	}
	anchor := f.anchorOf()
	if anchor <= 0 {
		m.notes = append(m.notes, "the day's median is not known yet, "+
			"so the fee is not widened for the market's distance "+
			"from it")
	}

	var notes []string
	m.toSHA256, notes = f.side(false, neoxa, second, anchor, newest.extra)
	m.notes = append(m.notes, notes...)
	m.toBLAKE2b, notes = f.side(true, neoxa, second, anchor, newest.extra)
	m.notes = append(m.notes, notes...)

	return m, nil
}

// side prices one direction from the books: toSHA256 (invert false) sells
// BLAKE2b coin into the bids, toBLAKE2b buys it from the asks, each at the
// worse of the two venues for its payer.
//
// Past maxVenueGap between the venues the direction quotes nothing: one of
// them is wrong, or being moved, by more than the fee can carry. A side of a
// book too thin for the largest swap widens the fee for what it could not
// fill. And a market far from its day's median widens the fee of the
// direction that would buy at the moved price, by driftWiden of the distance
// past driftBand; past driftMax that direction quotes nothing. The other
// direction sells at the moved price, which is the payer's risk, not the
// bridge's.
func (f *feed) side(invert bool, neoxa, second depth, anchor,
	extra float64) (sideRate, []string) {

	name, pick, short, venue := "toSHA256", neoxa.bid, neoxa.bidShort,
		"Neoxa"
	other, otherShort := second.bid, second.bidShort
	if invert {
		name, pick, short = "toBLAKE2b", neoxa.ask, neoxa.askShort
		other, otherShort = second.ask, second.askShort
	}

	if other > 0 {
		if gap := math.Abs(pick-other) / math.Min(pick, other); gap >
			f.policy.maxVenueGap {

			err := fmt.Errorf("%w: %s quotes nothing while "+
				"Neoxa (%.8g) and NonKYC (%.8g) are %.1f%% "+
				"apart, more than the %.1f%% allowed",
				rate.ErrDisagreement, name, pick, other,
				gap*100, f.policy.maxVenueGap*100)

			return sideRate{err: err}, nil
		}
		worse := other < pick
		if invert {
			worse = other > pick
		}
		if worse {
			pick, short, venue = other, otherShort, "NonKYC"
		}
	}

	var notes []string
	if short > 0 {
		widen := thinBookSpread * short
		extra += widen
		notes = append(notes, fmt.Sprintf("%s: %s's book holds %.0f%% "+
			"of the largest swap on the side it trades against; "+
			"fee widened %.2f%%", name, venue, (1-short)*100,
			widen*100))
	}

	if anchor > 0 {
		// How far the market is above its day (positive), for the
		// direction buying BLAKE2b coin, or below it, for the one
		// selling it.
		drift, way := pick/anchor-1, "above"
		if invert {
			drift, way = 1-pick/anchor, "below"
		}
		if drift > f.policy.driftMax {
			err := fmt.Errorf("%w: %s quotes nothing while the "+
				"market (%.8g) is %.1f%% %s its day's median "+
				"(%.8g), more than the %.1f%% allowed",
				rate.ErrBroken, name, pick, drift*100, way,
				anchor, f.policy.driftMax*100)

			return sideRate{err: err}, notes
		}
		if drift > f.policy.driftBand {
			widen := f.policy.driftWiden *
				(drift - f.policy.driftBand)
			extra += widen
			notes = append(notes, fmt.Sprintf("%s: the market is "+
				"%.1f%% %s its day's median; fee widened "+
				"%.2f%%", name, drift*100, way, widen*100))
		}
	}

	return sideRate{rate: pick, extra: extra}, notes
}

// anchorOf is the median of the day's readings, or zero with too few to
// say. Called with mu held.
func (f *feed) anchorOf() float64 {
	if len(f.anchor) < anchorMinSamples {
		return 0
	}
	vals := make([]float64, len(f.anchor))
	for i, r := range f.anchor {
		vals[i] = r.OutgoingPerIncoming
	}

	return median(vals)
}

// median is the median of vals, or zero when there are none. It sorts vals.
func median(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sort.Float64s(vals)
	mid := vals[len(vals)/2]
	if len(vals)%2 == 0 {
		mid = (vals[len(vals)/2-1] + mid) / 2
	}

	return mid
}

// last is the newest reading, fresh or not.
func (f *feed) last() (float64, time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(f.readings) == 0 {
		return 0, time.Time{}
	}
	r := f.readings[len(f.readings)-1]

	return r.mid, r.at
}

// crossCheck is the cross-check's last value, for Status.
func (f *feed) crossCheck() float64 {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.cross
}
