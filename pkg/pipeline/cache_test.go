package pipeline

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vasic-digital/token_optimizer/pkg/cache"
)

// --- RED-first proof: the cache is GENUINELY consulted before routing, and a
// HIT genuinely skips BOTH the routing decision AND the downstream execute
// step — closing the "correct-but-unused" gap §11.4.124 forbids shipping
// silently: before OptimizeCached existed, a *cache.Cache had NO reachable
// composition point with Optimize at all — a consumer wanting the
// cache -> router -> failover order documented in
// docs/research/tokens/ws6_caching_sync/DESIGN.md §1 had to hand-roll the
// exact wrapping this file now provides as a tested, reusable method. These
// tests prove OptimizeCached genuinely short-circuits downstream work on a
// hit, is a no-op wrapper (byte-identical effect to calling Optimize+execute
// by hand) when no cache is installed, never caches a failed execute
// (§11.4.1), and coalesces concurrent identical requests via the cache's own
// single-flight guarantee (§11.4.50 — run at -count=20 per the WS6
// precedent: a prior single-flight race was missed at -count=3).

// TestOptimizeCached_NoCacheInstalled_AlwaysRoutesAndExecutes proves the
// nil-safe / no-behavior-change-when-unset contract SetCache documents,
// mirroring SetEvidenceRecorder's own precedent: an Optimizer that never
// calls SetCache (or calls it with nil) has OptimizeCached call Optimize
// then execute on EVERY invocation, exactly as if the caller had composed
// them by hand with no cache in between.
func TestOptimizeCached_NoCacheInstalled_AlwaysRoutesAndExecutes(t *testing.T) {
	o := newOptimizer(t, ladder(t))

	var execCalls int32
	execute := func(d Decision) (string, time.Duration, error) {
		atomic.AddInt32(&execCalls, 1)
		return "resp-" + d.Tier.Name, 0, nil
	}
	r := req(t1LocalMicro, "", true) // direct-select path: T6 is live, no failover walk
	live := liveExcept()

	for i := 0; i < 3; i++ {
		v, d, hit, err := o.OptimizeCached("some-key", r, live, execute)
		if err != nil {
			t.Fatalf("iter %d: %v", i, err)
		}
		if hit {
			t.Fatalf("iter %d: hit = true, want false (no cache installed — every call must route+execute)", i)
		}
		if d.Tier.Name != t6Native {
			t.Fatalf("iter %d: tier = %q, want %q", i, d.Tier.Name, t6Native)
		}
		if v != "resp-"+t6Native {
			t.Fatalf("iter %d: value = %q, want %q", i, v, "resp-"+t6Native)
		}
	}
	if execCalls != 3 {
		t.Fatalf("execCalls = %d, want 3 (with no cache installed, OptimizeCached must call execute every time)", execCalls)
	}
}

// TestOptimizeCached_HitSkipsRouteAndExecute is THE core proof this file
// exists to provide: with a cache installed, a SECOND OptimizeCached call for
// the SAME key must be answered entirely from cache — routing (Optimize) and
// the downstream execute step must NOT run again. Both a route-call counter
// (wrapping `live`, called exactly once per real Optimize run on this
// direct-select, no-failover request) and an execute-call counter stay flat
// across the cached second call.
func TestOptimizeCached_HitSkipsRouteAndExecute(t *testing.T) {
	o := newOptimizer(t, ladder(t))
	o.SetCache(cache.New())

	var routeCalls, execCalls int32
	live := func(name string) bool {
		atomic.AddInt32(&routeCalls, 1)
		return true
	}
	execute := func(d Decision) (string, time.Duration, error) {
		atomic.AddInt32(&execCalls, 1)
		return "resp-" + d.Tier.Name, 0, nil
	}
	r := req(t1LocalMicro, "", true)

	v1, d1, hit1, err := o.OptimizeCached("k1", r, live, execute)
	if err != nil {
		t.Fatalf("1st call: %v", err)
	}
	if hit1 {
		t.Fatal("1st call: hit = true, want false (first call for a fresh key must be a miss)")
	}
	if d1.Tier.Name != t6Native {
		t.Fatalf("1st call tier = %q, want %q", d1.Tier.Name, t6Native)
	}
	if routeCalls != 1 || execCalls != 1 {
		t.Fatalf("after 1st call: routeCalls=%d execCalls=%d, want 1,1", routeCalls, execCalls)
	}

	// SAME key + SAME request: must be answered ENTIRELY from cache.
	v2, d2, hit2, err := o.OptimizeCached("k1", r, live, execute)
	if err != nil {
		t.Fatalf("2nd call: %v", err)
	}
	if !hit2 {
		t.Fatal("2nd call: hit = false, want true (identical key must hit the cache)")
	}
	if v2 != v1 {
		t.Fatalf("2nd call value = %q, want %q (identical to the cached first response)", v2, v1)
	}
	if routeCalls != 1 || execCalls != 1 {
		t.Fatalf("after 2nd (cached) call: routeCalls=%d execCalls=%d, want STILL 1,1 (a cache HIT must skip downstream route+compute)", routeCalls, execCalls)
	}
	if d2 != (Decision{}) {
		t.Fatalf("2nd call decision = %+v, want the zero Decision (no routing ran on a cache hit — there is nothing to report)", d2)
	}
}

// TestOptimizeCached_DifferentKeysDoNotShareCache proves the cache is
// genuinely keyed by cacheKey — a distinct key is a genuine miss even for an
// otherwise-identical request, never silently answered from another key's
// entry.
func TestOptimizeCached_DifferentKeysDoNotShareCache(t *testing.T) {
	o := newOptimizer(t, ladder(t))
	o.SetCache(cache.New())

	var execCalls int32
	execute := func(d Decision) (string, time.Duration, error) {
		atomic.AddInt32(&execCalls, 1)
		return "resp-" + d.Tier.Name, 0, nil
	}
	r := req(t1LocalMicro, "", true)
	live := liveExcept()

	if _, _, hit, err := o.OptimizeCached("key-a", r, live, execute); err != nil || hit {
		t.Fatalf("key-a: hit=%v err=%v, want hit=false err=nil", hit, err)
	}
	if _, _, hit, err := o.OptimizeCached("key-b", r, live, execute); err != nil || hit {
		t.Fatalf("key-b: hit=%v err=%v, want hit=false err=nil (a distinct key must NOT share key-a's cached entry)", hit, err)
	}
	if execCalls != 2 {
		t.Fatalf("execCalls = %d, want 2 (two distinct keys must each execute once)", execCalls)
	}
}

// TestOptimizeCached_ExecuteErrorNotCached proves a failed execute is NEVER
// cached — matching cache.ComputeFunc's own documented contract ("An error
// is NEVER cached ... the next call retries for real", §11.4.1: a poisoned
// "successful" cache entry born from a failed computation is a PASS-bluff at
// the correctness layer). The next identical-key call must genuinely retry
// Optimize+execute, not return a cached failure or a cached empty value.
func TestOptimizeCached_ExecuteErrorNotCached(t *testing.T) {
	o := newOptimizer(t, ladder(t))
	o.SetCache(cache.New())

	var execCalls int32
	wantErr := errors.New("boom: simulated downstream dispatch failure")
	failFirst := true
	execute := func(d Decision) (string, time.Duration, error) {
		atomic.AddInt32(&execCalls, 1)
		if failFirst {
			failFirst = false
			return "", 0, wantErr
		}
		return "ok-" + d.Tier.Name, 0, nil
	}
	r := req(t1LocalMicro, "", true)
	live := liveExcept()

	if _, _, _, err := o.OptimizeCached("kerr", r, live, execute); !errors.Is(err, wantErr) {
		t.Fatalf("1st call err = %v, want errors.Is %v", err, wantErr)
	}
	if execCalls != 1 {
		t.Fatalf("execCalls = %d, want 1", execCalls)
	}

	v, _, hit, err := o.OptimizeCached("kerr", r, live, execute)
	if err != nil {
		t.Fatalf("2nd call: %v", err)
	}
	if hit {
		t.Fatal("2nd call: hit = true, want false (a failed compute must NEVER be cached)")
	}
	if execCalls != 2 {
		t.Fatalf("execCalls = %d, want 2 (the error must not be cached; the next call must retry for real)", execCalls)
	}
	if v != "ok-"+t6Native {
		t.Fatalf("value = %q, want %q", v, "ok-"+t6Native)
	}
}

// TestOptimizeCached_NilExecute proves a nil execute is a loud, honest
// argument error (ErrNilExecute) — mirroring ErrNilLiveness's own precedent
// — rather than a nil-pointer crash (§11.4.1: a crash is not an honest error
// path).
func TestOptimizeCached_NilExecute(t *testing.T) {
	o := newOptimizer(t, ladder(t))
	if _, _, _, err := o.OptimizeCached("k", req("", "", false), liveExcept(), nil); !errors.Is(err, ErrNilExecute) {
		t.Fatalf("err = %v, want ErrNilExecute", err)
	}
}

// TestOptimizeCached_ConcurrentIdenticalRequestsCoalesce drives N goroutines
// through OptimizeCached concurrently for the SAME key, proving (a) no data
// race under -race and (b) the cache's own single-flight guarantee is
// genuinely reached through OptimizeCached: exactly ONE execute call serves
// ALL N callers, and every caller receives the IDENTICAL computed value —
// never a partial/interleaved/double-computed result. Run at -count=20 per
// the WS6 single-flight precedent (pkg/cache/singleflight_test.go's own doc:
// "a prior WS6 single-flight race was missed at -count=3").
func TestOptimizeCached_ConcurrentIdenticalRequestsCoalesce(t *testing.T) {
	o := newOptimizer(t, ladder(t))
	o.SetCache(cache.New())

	var execCalls int32
	execute := func(d Decision) (string, time.Duration, error) {
		n := atomic.AddInt32(&execCalls, 1)
		time.Sleep(2 * time.Millisecond) // widen the race window
		return fmt.Sprintf("resp-%s-%d", d.Tier.Name, n), 0, nil
	}
	r := req(t1LocalMicro, "", true)
	live := liveExcept()

	const n = 20
	results := make([]string, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			v, _, _, err := o.OptimizeCached("concurrent-key", r, live, execute)
			results[i] = v
			errs[i] = err
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
	}
	first := results[0]
	for i, v := range results {
		if v != first {
			t.Fatalf("goroutine %d result %q != %q (single-flight must coalesce concurrent identical requests onto ONE computed value)", i, v, first)
		}
	}
	if execCalls != 1 {
		t.Fatalf("execCalls = %d, want 1 (N=%d concurrent identical OptimizeCached calls must collapse into exactly one downstream execute)", execCalls, n)
	}
}
