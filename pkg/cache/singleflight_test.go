package cache

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestGetOrCompute_CacheHit_NoComputeCalled proves the fast path: when the key
// is already cached, GetOrCompute returns the cached value WITHOUT invoking
// compute at all. A cache that calls compute on a hit is not caching.
func TestGetOrCompute_CacheHit_NoComputeCalled(t *testing.T) {
	fc := newFakeClock(baseTime)
	c := New(WithClock(fc.Now))
	if err := c.Set("k", "cached-v"); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	var calls int32
	compute := func() (string, time.Duration, error) {
		atomic.AddInt32(&calls, 1)
		return "computed-v", 0, nil
	}

	got, err := c.GetOrCompute("k", compute)
	if err != nil {
		t.Fatalf("GetOrCompute err: %v", err)
	}
	if got != "cached-v" {
		t.Fatalf("GetOrCompute = %q, want cached-v (hit path must not call compute)", got)
	}
	if n := atomic.LoadInt32(&calls); n != 0 {
		t.Fatalf("compute called %d times on a cache HIT, want 0", n)
	}
}

// TestGetOrCompute_MissComputesAndCaches proves the miss path: compute runs
// exactly once, its result is returned to the caller, AND the result is
// written back so a subsequent plain Get is a hit (the cache-fill contract).
func TestGetOrCompute_MissComputesAndCaches(t *testing.T) {
	fc := newFakeClock(baseTime)
	c := New(WithClock(fc.Now))

	var calls int32
	compute := func() (string, time.Duration, error) {
		atomic.AddInt32(&calls, 1)
		return "fresh-v", 0, nil
	}

	got, err := c.GetOrCompute("k", compute)
	if err != nil {
		t.Fatalf("GetOrCompute err: %v", err)
	}
	if got != "fresh-v" {
		t.Fatalf("GetOrCompute = %q, want fresh-v", got)
	}
	if n := atomic.LoadInt32(&calls); n != 1 {
		t.Fatalf("compute called %d times on a miss, want exactly 1", n)
	}

	// The cache-fill contract: a plain Get must now hit the computed value.
	mustGet(t, c, "k", "fresh-v")
}

// TestGetOrCompute_ComputeErrorNotCached proves an error from compute is (a)
// surfaced to the caller and (b) never cached — a poisoned cache entry from a
// failed computation would silently "succeed" on every future lookup. A
// second GetOrCompute call after the failure MUST invoke compute again.
func TestGetOrCompute_ComputeErrorNotCached(t *testing.T) {
	fc := newFakeClock(baseTime)
	c := New(WithClock(fc.Now))

	wantErr := errors.New("boom")
	var calls int32
	compute := func() (string, time.Duration, error) {
		atomic.AddInt32(&calls, 1)
		return "", 0, wantErr
	}

	_, err := c.GetOrCompute("k", compute)
	if !errors.Is(err, wantErr) {
		t.Fatalf("GetOrCompute err = %v, want %v", err, wantErr)
	}
	mustMiss(t, c, "k") // a failed compute MUST NOT leave a cached entry

	// Retrying after a failure must genuinely retry, not serve a cached error.
	_, err = c.GetOrCompute("k", compute)
	if !errors.Is(err, wantErr) {
		t.Fatalf("second GetOrCompute err = %v, want %v", err, wantErr)
	}
	if n := atomic.LoadInt32(&calls); n != 2 {
		t.Fatalf("compute called %d times across two failed attempts, want 2 (no sticky cached error)", n)
	}
}

// TestGetOrCompute_EmptyKey proves the boundary condition (§11.4.85): an
// empty key is rejected the same way Get/Set/Invalidate reject it, and
// compute is never invoked for a request the cache refuses outright.
func TestGetOrCompute_EmptyKey(t *testing.T) {
	c := New()
	var calls int32
	_, err := c.GetOrCompute("", func() (string, time.Duration, error) {
		atomic.AddInt32(&calls, 1)
		return "v", 0, nil
	})
	if !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("GetOrCompute(\"\") err = %v, want ErrEmptyKey", err)
	}
	if n := atomic.LoadInt32(&calls); n != 0 {
		t.Fatalf("compute called %d times for an empty key, want 0", n)
	}
}

// TestGetOrCompute_NilCompute proves the boundary condition of a nil compute
// function is rejected explicitly rather than panicking with a nil-pointer
// dereference when the miss path tries to invoke it.
func TestGetOrCompute_NilCompute(t *testing.T) {
	c := New()
	_, err := c.GetOrCompute("k", nil)
	if err == nil {
		t.Fatalf("GetOrCompute with nil compute returned nil error, want a rejection")
	}
}

// TestGetOrCompute_ConcurrentCallsSingleFlight_ComputeCalledOnce is the
// stress test (§11.4.85: N >= 10 parallel invocations) for the WS6
// "concurrent hot-key expiry (N contexts) -> single-flight lock" requirement
// (docs/research/tokens/ws6_caching_sync/DESIGN.md §4 invalidation-trigger
// matrix). N concurrent callers miss on the SAME key simultaneously; exactly
// ONE of them must run compute (the "winner"); every other caller (the
// "losers") must block on the winner's result instead of redundantly
// recomputing, and every caller must observe the identical correct value.
// Run under `go test -race`.
func TestGetOrCompute_ConcurrentCallsSingleFlight_ComputeCalledOnce(t *testing.T) {
	fc := newFakeClock(baseTime)
	c := New(WithClock(fc.Now))

	const n = 25
	var calls int32
	entered := make(chan struct{}, n)
	release := make(chan struct{})

	compute := func() (string, time.Duration, error) {
		atomic.AddInt32(&calls, 1)
		entered <- struct{}{}
		<-release // held open so every concurrent caller has time to arrive as a "loser"
		return "single-flight-v", 0, nil
	}

	var wg sync.WaitGroup
	results := make([]string, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, err := c.GetOrCompute("hot-key", compute)
			results[i] = v
			errs[i] = err
		}(i)
	}

	<-entered // the winner has entered compute and is now parked on release
	close(release)
	wg.Wait()

	if n := atomic.LoadInt32(&calls); n != 1 {
		t.Fatalf("compute invoked %d times for %d concurrent callers on one key, want exactly 1 (stampede)", n, cap(entered))
	}
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("caller %d err: %v", i, errs[i])
		}
		if results[i] != "single-flight-v" {
			t.Fatalf("caller %d result = %q, want single-flight-v", i, results[i])
		}
	}
	mustGet(t, c, "hot-key", "single-flight-v") // the winner's result was cached
}

// TestGetOrCompute_InvalidateDuringCompute_ResultNotCached is the correctness
// test the WS6 gap exists to close: an Invalidate(key) landing WHILE compute
// is still running (the underlying data changed mid-computation) MUST NOT be
// clobbered by the in-flight compute's eventual write-back. The in-flight
// caller still receives the value it asked for (that answer was correct as of
// when it was requested), but the cache itself MUST NOT retain it — a
// subsequent Get MUST be an honest miss, never a resurrected stale value.
func TestGetOrCompute_InvalidateDuringCompute_ResultNotCached(t *testing.T) {
	fc := newFakeClock(baseTime)
	c := New(WithClock(fc.Now))

	entered := make(chan struct{})
	release := make(chan struct{})
	compute := func() (string, time.Duration, error) {
		close(entered)
		<-release
		return "now-stale-v", 0, nil
	}

	type res struct {
		v   string
		err error
	}
	resCh := make(chan res, 1)
	go func() {
		v, err := c.GetOrCompute("k", compute)
		resCh <- res{v, err}
	}()

	<-entered // compute is running, parked before it returns
	fc.Advance(time.Millisecond)
	if err := c.Invalidate("k"); err != nil {
		t.Fatalf("concurrent Invalidate: %v", err)
	}
	close(release) // let compute finish and attempt its write-back

	r := <-resCh
	if r.err != nil {
		t.Fatalf("GetOrCompute err: %v", r.err)
	}
	if r.v != "now-stale-v" {
		t.Fatalf("in-flight caller got %q, want its own correct-at-request-time value now-stale-v", r.v)
	}

	// The load-bearing assertion: the cache must NOT have installed the
	// superseded result. A hit here would resurrect data invalidated while it
	// was being computed — exactly the "stale entry after an invalidating
	// change" bluff this test exists to forbid.
	mustMiss(t, c, "k")
}

// TestGetOrCompute_NoInvalidateDuringCompute_StillCaches is the contrast case
// for the previous test: it proves the invalidation guard is discriminating
// (only refuses the write-back when a REAL concurrent invalidation occurred)
// rather than a blanket "never cache a GetOrCompute result" regression that
// would silently defeat the entire feature.
func TestGetOrCompute_NoInvalidateDuringCompute_StillCaches(t *testing.T) {
	fc := newFakeClock(baseTime)
	c := New(WithClock(fc.Now))

	entered := make(chan struct{})
	release := make(chan struct{})
	compute := func() (string, time.Duration, error) {
		close(entered)
		<-release
		return "genuinely-fresh-v", 0, nil
	}

	type res struct {
		v   string
		err error
	}
	resCh := make(chan res, 1)
	go func() {
		v, err := c.GetOrCompute("k", compute)
		resCh <- res{v, err}
	}()

	<-entered
	fc.Advance(time.Millisecond) // time passes, but nothing invalidates "k"
	close(release)

	r := <-resCh
	if r.err != nil {
		t.Fatalf("GetOrCompute err: %v", r.err)
	}
	if r.v != "genuinely-fresh-v" {
		t.Fatalf("caller got %q, want genuinely-fresh-v", r.v)
	}
	mustGet(t, c, "k", "genuinely-fresh-v") // no invalidation raced in: write-back must land
}

// TestGetOrCompute_StressMixedWithGetSetInvalidate hammers GetOrCompute
// alongside Get/Set/Invalidate from many goroutines over a shared key space
// (§11.4.85 stress: N >= 10 parallel invocations, run under `go test -race`).
// It asserts no panic/deadlock/race and that a control key set once and never
// invalidated by any worker stays consistently readable throughout — the same
// discriminator TestConcurrentGetSetInvalidate uses, extended to also exercise
// the new single-flight code path under real contention.
func TestGetOrCompute_StressMixedWithGetSetInvalidate(t *testing.T) {
	fc := newFakeClock(baseTime)
	c := New(WithClock(fc.Now))

	const workers = 20
	const iters = 200
	const keySpace = 6

	const controlKey = "control-immutable"
	if err := c.Set(controlKey, "constant"); err != nil {
		t.Fatalf("seed control: %v", err)
	}

	var computeCalls int64
	var wg sync.WaitGroup
	var opCount int64
	errCh := make(chan error, workers)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				key := fmt.Sprintf("k%d", (w+i)%keySpace)
				switch (w + i) % 4 {
				case 0:
					if err := c.Set(key, fmt.Sprintf("v-%d-%d", w, i)); err != nil {
						errCh <- fmt.Errorf("Set: %w", err)
						return
					}
				case 1:
					if _, _, err := c.Get(key); err != nil {
						errCh <- fmt.Errorf("Get: %w", err)
						return
					}
				case 2:
					if err := c.Invalidate(key); err != nil {
						errCh <- fmt.Errorf("Invalidate: %w", err)
						return
					}
				case 3:
					_, err := c.GetOrCompute(key, func() (string, time.Duration, error) {
						atomic.AddInt64(&computeCalls, 1)
						return fmt.Sprintf("computed-%d-%d", w, i), 0, nil
					})
					if err != nil {
						errCh <- fmt.Errorf("GetOrCompute: %w", err)
						return
					}
				}
				got, hit, err := c.Get(controlKey)
				if err != nil {
					errCh <- fmt.Errorf("control Get: %w", err)
					return
				}
				if !hit || got != "constant" {
					errCh <- fmt.Errorf("control key corrupted: hit=%v val=%q", hit, got)
					return
				}
				atomic.AddInt64(&opCount, 1)
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent op failed: %v", err)
		}
	}
	if opCount == 0 {
		t.Fatalf("no concurrent ops ran")
	}
	if atomic.LoadInt64(&computeCalls) == 0 {
		t.Fatalf("GetOrCompute path never ran a compute (test did not exercise the new code)")
	}
}
