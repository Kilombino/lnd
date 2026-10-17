//go:build bridgerpc
// +build bridgerpc

package bridgerpc

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestDifficultyFromBits(t *testing.T) {
	t.Parallel()

	for bits, want := range map[uint32]float64{
		0x1d00ffff: 1,               // the genesis target
		0x1b0404cb: 16307.420938523, // block 100,000
	} {
		if got := difficultyFromBits(bits); math.Abs(got-want)/want > 1e-9 {
			t.Errorf("bits %08x: difficulty %v, want %v", bits, got, want)
		}
	}
	for _, bits := range []uint32{0, 0x1d000000, 0x1d800001} {
		if got := difficultyFromBits(bits); got != 0 {
			t.Errorf("bits %08x encode no positive target, got %v",
				bits, got)
		}
	}
}

// Early in a difficulty epoch the observer leans on the previous one, scaled
// by how much the difficulty changed. That needs each block's difficulty: the
// blocks used to go in without it, and every new epoch on either chain left
// the bridge refusing all quotes (chain_unmeasured) until it had seen a
// hundred blocks of it, most of a day.
func TestANewEpochIsMeasurableFromThePreviousOne(t *testing.T) {
	t.Parallel()

	const (
		spacing  = 10 * time.Minute
		boundary = 2016 * 396 // 798,336
		tip      = boundary + 10
	)

	now := time.Now()
	f := &fakeNode{synced: true, height: tip, blockTime: now}
	deps := f.deps()
	timeAt := func(h int32) time.Time {
		return now.Add(-time.Duration(tip-h) * spacing)
	}
	deps.BlockAt = func(_ context.Context, h int32) (BlockInfo, error) {
		bits := uint32(0x1d00ffff)
		if h >= boundary {
			bits = 0x1c7fff80 // twice the difficulty
		}

		return BlockInfo{
			Height: h, Time: timeAt(h), SyncedToChain: true,
			Difficulty: difficultyFromBits(bits),
		}, nil
	}
	tipNow := int32(tip)
	deps.BestBlock = func(context.Context) (BlockInfo, error) {
		// A tip has no difficulty of its own.
		return BlockInfo{
			Height: tipNow, Time: timeAt(tipNow), SyncedToChain: true,
		}, nil
	}

	svc := serviceFor(t, usable(), f, remote(nil, nil, nil))
	svc.local = NewLocal(deps)

	svc.backfill(context.Background())
	est, err := svc.b2bChain.Estimate(now)
	if err != nil {
		t.Fatalf("ten blocks into an epoch, with the previous one "+
			"read, the chain is still not measurable: %v", err)
	}
	// The previous epoch, scaled: twice the difficulty at the same
	// hashrate is twice the spacing. Blocks here still come every ten
	// minutes, so that is the slow bound's spacing, the more conservative
	// one for it, while the fast side keeps the shorter, observed one.
	if d := est.SlowSpacing - 2*spacing; d < -time.Second || d > time.Second {
		t.Errorf("slow spacing %v, want the previous epoch scaled, %v",
			est.SlowSpacing, 2*spacing)
	}

	// New tips, read with their header's difficulty, keep it measurable;
	// polling the same tip again must not strip the difficulty.
	for i := 0; i < 3; i++ {
		tipNow++
		svc.sample(context.Background())
		svc.sample(context.Background())
	}
	if _, err := svc.b2bChain.Estimate(timeAt(tipNow)); err != nil {
		t.Fatalf("new tips made the chain unmeasurable again: %v", err)
	}
}
