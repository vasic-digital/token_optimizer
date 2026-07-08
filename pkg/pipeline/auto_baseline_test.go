package pipeline

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/vasic-digital/token_optimizer/pkg/cache"
	"github.com/vasic-digital/token_optimizer/pkg/config"
	"github.com/vasic-digital/token_optimizer/pkg/telemetry"
)

// WS1 follow-up (ATM-660 continuation, self-computed baseline).
//
// This file resolves the honest limitation savings_wiring_test.go's own
// "THE HONEST FINDING" comment recorded: pipeline.Request.BaselineCost
// required the CALLER to look up a baseline tier's price and call
// telemetry.ComputeCost itself, because neither router.Decision nor
// pipeline.Decision expose "the tier that would have been used with no
// optimizer present" and Request.Tokens is a single combined total that
// cannot be split into input/output tokens without guessing a ratio.
//
// REAL TIER DATA READ BEFORE WRITING THIS FILE (§11.4.6, never guessed):
//   - pkg/config/config.go: Tier{PricePerMTokIn, PricePerMTokOut} +
//     Config.Tiers() returns tiers cheapest-first (Priority ascending, ties
//     broken on Name) — a copy, safe to index.
//   - pkg/router/loadbearing.go (resolveFloor): "the STRONGEST registered
//     tier" is ALREADY a precisely-defined, engine-owned quantity —
//     tiers[len(tiers)-1] in that exact cheapest-first ordering — used for
//     the identical purpose (a load-bearing request's implicit
//     never-downgrade floor, absent an explicit FloorTier). This is NOT a
//     newly-invented, project-specific policy; it is the SAME definition
//     BaselineCost's own doc comment already names as "the native / heaviest
//     tier the router would have used with no optimizer present".
//
// THE SPLIT-TOKEN CONSTRAINT IS REAL AND IS NOT WORKED AROUND BY GUESSING.
// telemetry.ComputeCost prices input and output tokens SEPARATELY
// (inputTokens/1e6*priceIn + outputTokens/1e6*priceOut). A "combined-price"
// shortcut (pricing the pre-existing combined Tokens field against
// Tier.CombinedPrice(), i.e. PricePerMTokIn+PricePerMTokOut) was considered
// and REJECTED: CombinedPrice is a RANKING scalar (config.go's own doc:
// "the scalar the default downgrade predicate compares tiers on"), not a
// per-total-token blended rate — pricing a request's full combined token
// count against the SUM of the in/out rates roughly DOUBLE-COUNTS the real
// split-based cost whenever a tier has non-zero prices on both channels
// (e.g. T6_NATIVE: 3 in + 15 out = 18 combined; 1M totally-in-or-out tokens
// priced at "18/M" is not a real number this tier ever charges). That would
// be a FABRICATED baseline, exactly what §11.4.1/§11.4.6 forbid. Instead,
// this file adds two NEW, additive fields — Request.InputTokens /
// Request.OutputTokens — carrying the caller's own REAL measured
// per-channel counts for this exact request (the SAME real data
// telemetry.SavingsRecord's own pre-existing, previously-never-populated
// InputTokens/OutputTokens fields already expect), so ComputeCost's
// split-price formula is driven by real measurements, never an invented
// ratio or a doubled blended rate.

// autoBaselineReq builds a Request with AutoBaseline enabled and explicit
// per-channel token counts, reusing the req() helper for the routing signals
// (MinTier/FloorTier/LoadBearing) exactly as savingsReq does for the
// caller-supplied-baseline variant.
func autoBaselineReq(min string, loadBearing bool, inTok, outTok int64, optimizedCost float64) Request {
	r := req(min, "", loadBearing)
	r.AutoBaseline = true
	r.InputTokens = inTok
	r.OutputTokens = outTok
	r.Cost = optimizedCost
	return r
}

// TestOptimize_AutoBaseline_SelfComputesFromStrongestTier is THE core proof:
// a real Optimize decision with AutoBaseline=true emits a SavingsRecord whose
// BaselineCost is telemetry.ComputeCost(InputTokens, OutputTokens, ...)
// computed by the ENGINE against the STRONGEST registered tier (T6_NATIVE:
// PricePerMTokIn=3, PricePerMTokOut=15) — never the caller-supplied
// BaselineCost, which is deliberately set to an implausible 999 here to
// prove it is ignored/overridden once AutoBaseline is set.
func TestOptimize_AutoBaseline_SelfComputesFromStrongestTier(t *testing.T) {
	o := newOptimizer(t, ladder(t))
	rec := telemetry.NewSavingsRecorder()
	o.SetSavingsRecorder(rec)

	r := autoBaselineReq(t1LocalMicro, true, 1_000_000, 2_000_000, 0.01) // load-bearing -> floors to T6_NATIVE
	r.BaselineCost = 999                                                 // must be ignored: AutoBaseline overrides it
	got, err := o.Optimize(r, liveExcept())
	if err != nil {
		t.Fatalf("Optimize: %v", err)
	}
	if got.Tier.Name != t6Native {
		t.Fatalf("tier = %q, want %q", got.Tier.Name, t6Native)
	}

	if n := rec.Len(); n != 1 {
		t.Fatalf("SavingsRecorder.Len() = %d, want 1", n)
	}
	sr := rec.Records()[0]

	wantBaseline := telemetry.ComputeCost(1_000_000, 2_000_000, 3, 15)
	if wantBaseline != 33.00 {
		t.Fatalf("test arithmetic sanity check failed: wantBaseline = %v, want 33.00 (1M in @ $3/M + 2M out @ $15/M = $3 + $30)", wantBaseline)
	}
	if sr.BaselineCost != wantBaseline {
		t.Errorf("BaselineCost = %v, want %v (self-computed from the STRONGEST tier %q's real price, never the caller-supplied 999)", sr.BaselineCost, wantBaseline, t6Native)
	}
	if sr.OptimizedCost != 0.01 {
		t.Errorf("OptimizedCost = %v, want 0.01 (still the caller-supplied req.Cost — unaffected by AutoBaseline)", sr.OptimizedCost)
	}
	if want := wantBaseline - 0.01; sr.Savings() != want {
		t.Errorf("Savings() = %v, want %v (real self-computed-baseline-vs-optimized delta)", sr.Savings(), want)
	}
}

// TestOptimize_AutoBaseline_DefaultFalse_NilSafeNoEffect proves the additive,
// nil-safe contract: with AutoBaseline left at its zero value (false),
// populating InputTokens/OutputTokens has ZERO effect — BaselineCost is
// forwarded verbatim exactly as it was before this feature existed.
func TestOptimize_AutoBaseline_DefaultFalse_NilSafeNoEffect(t *testing.T) {
	o := newOptimizer(t, ladder(t))
	rec := telemetry.NewSavingsRecorder()
	o.SetSavingsRecorder(rec)

	r := savingsReq(t1LocalMicro, "", true, 18.00, 0.01, time.Time{})
	r.InputTokens = 999_999_999 // populated but AutoBaseline is false: must have NO effect
	r.OutputTokens = 999_999_999
	if _, err := o.Optimize(r, liveExcept()); err != nil {
		t.Fatalf("Optimize: %v", err)
	}
	sr := rec.Records()[0]
	if sr.BaselineCost != 18.00 {
		t.Fatalf("BaselineCost = %v, want 18.00 (caller-supplied value must be forwarded verbatim when AutoBaseline is false, ignoring populated InputTokens/OutputTokens)", sr.BaselineCost)
	}
}

// TestOptimize_AutoBaseline_ConcurrentCallsNeverRace drives N goroutines
// through Optimize concurrently with AutoBaseline enabled and ONE shared
// SavingsRecorder, proving no data race under -race and that every emitted
// record carries the IDENTICAL, correctly self-computed baseline (§11.4.50
// determinism — same tier data, same token counts, same result every time).
func TestOptimize_AutoBaseline_ConcurrentCallsNeverRace(t *testing.T) {
	o := newOptimizer(t, ladder(t))
	rec := telemetry.NewSavingsRecorder()
	o.SetSavingsRecorder(rec)

	const n = 50
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			r := autoBaselineReq(t1LocalMicro, true, 1_000_000, 2_000_000, float64(i))
			if _, err := o.Optimize(r, liveExcept()); err != nil {
				t.Errorf("Optimize(%d): %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	if got := rec.Len(); got != n {
		t.Fatalf("SavingsRecorder.Len() = %d, want %d (a lost or merged record indicates a race)", got, n)
	}
	want := telemetry.ComputeCost(1_000_000, 2_000_000, 3, 15)
	for i, sr := range rec.Records() {
		if sr.BaselineCost != want {
			t.Fatalf("record[%d].BaselineCost = %v, want %v on every concurrent call", i, sr.BaselineCost, want)
		}
	}
}

// TestOptimizeCached_CacheHit_AutoBaseline_SelfComputesFromStrongestTier
// proves the cache-hit path (OptimizeCached, cache.go) ALSO self-computes
// when AutoBaseline is set — not just Optimize's own routing-decision path
// (recordSavings). A cache hit skips Optimize entirely, so this record can
// ONLY come from OptimizeCached's own hit branch.
func TestOptimizeCached_CacheHit_AutoBaseline_SelfComputesFromStrongestTier(t *testing.T) {
	o := newOptimizer(t, ladder(t))
	o.SetCache(cache.New())
	rec := telemetry.NewSavingsRecorder()
	o.SetSavingsRecorder(rec)

	r := autoBaselineReq(t1LocalMicro, true, 1_000_000, 2_000_000, 0)
	live := liveExcept()
	execute := func(d Decision) (string, time.Duration, error) { return "resp-" + d.Tier.Name, 0, nil }

	if _, _, hit, err := o.OptimizeCached("kab1", r, live, execute); err != nil || hit {
		t.Fatalf("1st call: hit=%v err=%v, want hit=false err=nil (fresh key must miss)", hit, err)
	}
	if n := rec.Len(); n != 1 {
		t.Fatalf("after miss: SavingsRecorder.Len() = %d, want 1", n)
	}

	if _, _, hit, err := o.OptimizeCached("kab1", r, live, execute); err != nil || !hit {
		t.Fatalf("2nd call: hit=%v err=%v, want hit=true err=nil (identical key must hit)", hit, err)
	}
	if n := rec.Len(); n != 2 {
		t.Fatalf("after hit: SavingsRecorder.Len() = %d, want 2", n)
	}

	want := telemetry.ComputeCost(1_000_000, 2_000_000, 3, 15)
	hitRec := rec.Records()[1]
	if hitRec.Tag != "cache_hit" {
		t.Errorf("Tag = %q, want %q", hitRec.Tag, "cache_hit")
	}
	if hitRec.BaselineCost != want {
		t.Errorf("BaselineCost = %v, want %v (the cache-hit branch must ALSO self-compute, not just Optimize's own routing path)", hitRec.BaselineCost, want)
	}
	if hitRec.OptimizedCost != 0 {
		t.Errorf("OptimizedCost = %v, want 0 (no tier was invoked on a cache hit)", hitRec.OptimizedCost)
	}
}

// TestOptimizeCached_CacheHit_AutoBaseline_NoTiersRegistered_HonestError
// proves ErrNoBaselineTier is a REAL, reachable, honest failure — never a
// silently-fabricated zero or a silent fallback to the (unset) caller-
// supplied BaselineCost. It exercises the ONE path this error is reachable
// through: a *cache.Cache SHARED across two Optimizers (a real, permitted
// setup — cache.Cache is keyed on caller-chosen strings and has no relation
// to any particular *config.Config), where one Optimizer seeds a real MISS
// and a second Optimizer — constructed over an EMPTY config.Config, so it has
// zero tiers to resolve a baseline price from — observes a genuine cache HIT
// for that same key with AutoBaseline set.
func TestOptimizeCached_CacheHit_AutoBaseline_NoTiersRegistered_HonestError(t *testing.T) {
	shared := cache.New()
	execute := func(d Decision) (string, time.Duration, error) { return "v", 0, nil }

	// Optimizer A: a real tier ladder, seeds the shared cache with a real MISS.
	a := newOptimizer(t, ladder(t))
	a.SetCache(shared)
	if _, _, hit, err := a.OptimizeCached("shared-key", req(t1LocalMicro, "", false), liveExcept(), execute); err != nil || hit {
		t.Fatalf("seed miss via A: hit=%v err=%v, want hit=false err=nil", hit, err)
	}

	// Optimizer B: ZERO registered tiers, shares the SAME underlying cache.
	b, err := New(config.New())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	b.SetCache(shared)
	rec := telemetry.NewSavingsRecorder()
	b.SetSavingsRecorder(rec)

	r := autoBaselineReq("", false, 1, 1, 0)
	_, _, hit, err := b.OptimizeCached("shared-key", r, liveExcept(), execute)
	if err == nil {
		t.Fatal("err = nil, want a non-nil ErrNoBaselineTier (must never silently emit a fabricated/fallback baseline)")
	}
	if !hit {
		t.Fatalf("hit = false, want true (the value IS present in the shared cache — the honest failure is baseline RESOLUTION, not the cache lookup itself)")
	}
	if !errors.Is(err, ErrNoBaselineTier) {
		t.Fatalf("err = %v, want errors.Is ErrNoBaselineTier", err)
	}
	if n := rec.Len(); n != 0 {
		t.Fatalf("SavingsRecorder.Len() = %d, want 0 (a failed baseline resolution must not still emit a record)", n)
	}
}
