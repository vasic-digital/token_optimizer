package pipeline

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vasic-digital/token_optimizer/pkg/cache"
	"github.com/vasic-digital/token_optimizer/pkg/telemetry"
)

// --- RED-first proof: a real routing/cache decision emits a real
// telemetry.SavingsRecord — closing the WS1 gap the R.37 review flagged
// (pkg/telemetry/savings.go: "correct-but-unused" — ComputeCost + SavingsRecord
// + SavingsRecorder exist and are unit-tested in isolation, but nothing in the
// engine's actual decision path — Optimize / OptimizeCached, the same entry
// points evidence_wiring_test.go and cache_test.go already prove real traffic
// flows through — ever calls SavingsRecorder.Record). These tests prove the
// SAME §11.4.124 "correct but unreachable" pattern is closed for WS1 exactly
// as it was closed for WS5 evidence (evidence_wiring_test.go) and WS6 cache
// (cache_test.go): SetSavingsRecorder + the wiring inside Optimize/
// OptimizeCached is the ONLY path by which a real Decision produced by this
// package can reach a SavingsRecorder.
//
// Decoupling (§11.4.28): pipeline.Request carries the two new caller-supplied,
// opaque fields this file exercises — BaselineCost and At — mirroring the
// EXACT contract already established for TaskClass/Tokens/Cost in
// evidence_wiring_test.go's own doc comment: the pipeline never infers,
// re-derives, or fabricates either value; it only forwards verbatim what the
// caller measured and priced.
//
// THE HONEST FINDING THIS FILE RECORDS (§11.4.6 — determined by reading
// pkg/router/router.go's Decision, pkg/pipeline/decision.go's Decision, AND
// pkg/pipeline/pipeline.go's Request BEFORE writing this file, never guessed).
// Neither router.Decision nor pipeline.Decision expose a "baseline tier" the
// package could itself resolve a baseline price from — Optimize only ever
// returns the CHOSEN tier (Tier / SelectedTier), never "the tier that would
// have been used with no optimizer present". pipeline.Request's own existing
// Tokens field is furthermore a single COMBINED total (see its doc comment:
// "the consumer-supplied total token count for the turn"), not a
// input/output split — so telemetry.ComputeCost (which prices input and
// output tokens separately) cannot be driven from it without GUESSING a
// split ratio, which §11.4.6 forbids. Given both gaps, this file wires the
// CALLER-SUPPLIED-BASELINE variant this task's own instructions name as the
// correct fallback: pipeline.Request.BaselineCost is a new field with the
// IDENTICAL decoupling contract as the pre-existing Request.Cost field (see
// pipeline.go's own doc comment on Cost) — the consumer computes it however
// it likes (typically via telemetry.ComputeCost against the un-optimized/
// native tier's price, from pkg/config) and this package only forwards it
// verbatim into the emitted SavingsRecord.BaselineCost. Request.Cost (already
// wired into router.Evidence's "$" field for the WS5 evidence trail) doubles,
// unmodified, as SavingsRecord.OptimizedCost: it already documents itself as
// "the USD cost for the turn... from the consumer's own price table" — i.e.
// exactly the cost the request ACTUALLY incurred using its chosen tier. No
// per-request input/output token split is recorded on the SavingsRecord (both
// fields are left at their zero value) because Request.Tokens cannot be
// losslessly divided between them without guessing; SavingsRecord.Savings()
// is computed ONLY from BaselineCost/OptimizedCost (see savings.go's own doc:
// "not used in the $ computation itself"), so this omission does not affect
// the $-savings figure itself — only the (optional, informational) per-record
// token fields.

// savingsReq extends the req() test helper with the two new WS1
// savings-correlation fields (BaselineCost/At) plus the pre-existing Cost
// field, mirroring evReq's own pattern in evidence_wiring_test.go.
func savingsReq(min, floor string, loadBearing bool, baselineCost, optimizedCost float64, at time.Time) Request {
	r := req(min, floor, loadBearing)
	r.BaselineCost = baselineCost
	r.Cost = optimizedCost
	r.At = at
	return r
}

// TestOptimize_SavingsRecorder_EmitsRealSavingsRecordFromRealDecision proves
// that installing a *telemetry.SavingsRecorder on an Optimizer and driving a
// REAL routing decision through Optimize genuinely appends a SavingsRecord
// whose fields echo (a) the real routing decision (Tag == the tier the
// request was ACTUALLY sent to) and (b) the caller-measured $ figures
// (BaselineCost/OptimizedCost forwarded verbatim from the request) — never a
// fabricated number, and Savings() reports the REAL delta between them.
func TestOptimize_SavingsRecorder_EmitsRealSavingsRecordFromRealDecision(t *testing.T) {
	o := newOptimizer(t, ladder(t))
	rec := telemetry.NewSavingsRecorder()
	o.SetSavingsRecorder(rec)

	at := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	r := savingsReq(t1LocalMicro, "", true, 18.00, 0.01, at) // load-bearing -> floors to T6_NATIVE
	got, err := o.Optimize(r, liveExcept())
	if err != nil {
		t.Fatalf("Optimize: %v", err)
	}
	if got.Tier.Name != t6Native {
		t.Fatalf("tier = %q, want %q", got.Tier.Name, t6Native)
	}

	if n := rec.Len(); n != 1 {
		t.Fatalf("SavingsRecorder.Len() = %d, want 1 (Optimize must emit exactly one real record per decision)", n)
	}
	sr := rec.Records()[0]
	if sr.Tag != t6Native {
		t.Errorf("Tag = %q, want %q (must echo the tier the request was ACTUALLY sent to)", sr.Tag, t6Native)
	}
	if sr.BaselineCost != 18.00 {
		t.Errorf("BaselineCost = %v, want 18.00 (must come from req.BaselineCost, never fabricated)", sr.BaselineCost)
	}
	if sr.OptimizedCost != 0.01 {
		t.Errorf("OptimizedCost = %v, want 0.01 (must come from req.Cost, never fabricated)", sr.OptimizedCost)
	}
	if want := 18.00 - 0.01; sr.Savings() != want {
		t.Errorf("Savings() = %v, want %v (real baseline-vs-optimized delta)", sr.Savings(), want)
	}
	if !sr.At.Equal(at) {
		t.Errorf("At = %v, want %v (must come from req.At, never a wall-clock read)", sr.At, at)
	}
}

// TestOptimize_SavingsRecorder_FailoverTagsTheFinalBilledTier proves the
// emitted SavingsRecord's Tag names the tier the request was FINALLY billed
// against (Decision.Tier — what actually happened) even when Optimize fails
// over, NOT the pre-failover entitlement (Decision.SelectedTier) — the $
// ledger must reflect reality, distinguishing this from router.Evidence's own
// intentionally-different ChosenTier-is-the-entitlement contract (see
// TestOptimize_SelectWithEvidence_DescribesSelectedTierEvenOnFailover).
func TestOptimize_SavingsRecorder_FailoverTagsTheFinalBilledTier(t *testing.T) {
	c := ladder(t)
	if err := c.RegisterAlternative(t5AliasCheap, t6Native); err != nil {
		t.Fatalf("RegisterAlternative: %v", err)
	}
	o := newOptimizer(t, c)
	rec := telemetry.NewSavingsRecorder()
	o.SetSavingsRecorder(rec)

	r := savingsReq("", t5AliasCheap, true, 18.00, 18.00, time.Time{})
	got, err := o.Optimize(r, liveExcept(t5AliasCheap))
	if err != nil {
		t.Fatalf("Optimize: %v", err)
	}
	if got.Tier.Name != t6Native || !got.FailedOver {
		t.Fatalf("expected failover to %q, got tier=%q failedOver=%v", t6Native, got.Tier.Name, got.FailedOver)
	}

	if n := rec.Len(); n != 1 {
		t.Fatalf("SavingsRecorder.Len() = %d, want 1", n)
	}
	if tag := rec.Records()[0].Tag; tag != t6Native {
		t.Errorf("Tag = %q, want %q (the FINAL billed tier, not the pre-failover entitlement %q)", tag, t6Native, t5AliasCheap)
	}
}

// TestOptimize_SavingsRecorder_NoRegressionFabricated proves a request that
// costs MORE on its optimized path than the baseline (a genuine regression)
// reports a real NEGATIVE Savings() value — never silently clamped, hidden,
// or reported as a fabricated non-negative number (§11.4.1 / §11.4.6).
func TestOptimize_SavingsRecorder_NoRegressionFabricated(t *testing.T) {
	o := newOptimizer(t, ladder(t))
	rec := telemetry.NewSavingsRecorder()
	o.SetSavingsRecorder(rec)

	r := savingsReq(t1LocalMicro, "", true, 1.00, 5.00, time.Time{}) // optimized cost EXCEEDS baseline
	if _, err := o.Optimize(r, liveExcept()); err != nil {
		t.Fatalf("Optimize: %v", err)
	}
	sr := rec.Records()[0]
	if want := 1.00 - 5.00; sr.Savings() != want {
		t.Fatalf("Savings() = %v, want %v (a genuine regression must report a real negative value, never clamped/hidden)", sr.Savings(), want)
	}
	if sr.Savings() >= 0 {
		t.Fatalf("Savings() = %v, want < 0 (regression fabricated as non-negative)", sr.Savings())
	}
}

// TestOptimize_SavingsRecorder_NoSavingsCaseIsExactlyZero proves a request
// whose optimized path cost EXACTLY the baseline (no optimization actually
// helped) reports Savings() == 0 exactly — never a fabricated positive value
// when there genuinely was none.
func TestOptimize_SavingsRecorder_NoSavingsCaseIsExactlyZero(t *testing.T) {
	o := newOptimizer(t, ladder(t))
	rec := telemetry.NewSavingsRecorder()
	o.SetSavingsRecorder(rec)

	r := savingsReq(t1LocalMicro, "", true, 18.00, 18.00, time.Time{})
	if _, err := o.Optimize(r, liveExcept()); err != nil {
		t.Fatalf("Optimize: %v", err)
	}
	if s := rec.Records()[0].Savings(); s != 0 {
		t.Fatalf("Savings() = %v, want exactly 0 (no real savings occurred — must not be fabricated positive)", s)
	}
}

// TestOptimize_NoSavingsRecorderInstalled_NilSafeNoEmit proves Optimize with
// NO SavingsRecorder installed behaves exactly as before this wiring existed
// — populated-but-unrecorded BaselineCost/Cost/At fields have ZERO effect on
// the returned Decision, mirroring
// TestOptimize_NoRecorderInstalled_BehaviorUnchanged's own precedent for the
// WS5 evidence fields.
func TestOptimize_NoSavingsRecorderInstalled_NilSafeNoEmit(t *testing.T) {
	o := newOptimizer(t, ladder(t))
	withSavings := savingsReq(t1LocalMicro, "", true, 999, 1, time.Now())
	withoutSavings := req(t1LocalMicro, "", true)

	got1, err := o.Optimize(withSavings, liveExcept())
	if err != nil {
		t.Fatalf("Optimize(withSavings): %v", err)
	}
	got2, err := o.Optimize(withoutSavings, liveExcept())
	if err != nil {
		t.Fatalf("Optimize(withoutSavings): %v", err)
	}
	if got1 != got2 {
		t.Fatalf("populated-but-unrecorded savings fields changed the Decision: %+v != %+v", got1, got2)
	}
}

// TestOptimize_SavingsRecorder_NegativeCostNeverSilentlySwallowed proves that
// when the caller-supplied BaselineCost or Cost is negative (a caller price-
// table bug), SavingsRecorder.Record's ErrNegativeCost is surfaced through
// Optimize's return value rather than silently swallowed — matching
// TestOptimize_SelectWithEvidence_RecordFailureIsNeverSilentlySwallowed's own
// precedent for the WS5 evidence-sink failure case.
func TestOptimize_SavingsRecorder_NegativeCostNeverSilentlySwallowed(t *testing.T) {
	o := newOptimizer(t, ladder(t))
	rec := telemetry.NewSavingsRecorder()
	o.SetSavingsRecorder(rec)

	r := savingsReq(t1LocalMicro, "", true, -1, 0, time.Time{})
	if _, err := o.Optimize(r, liveExcept()); err == nil {
		t.Fatal("Optimize succeeded silently despite a negative BaselineCost — the SavingsRecorder rejection was swallowed")
	}
}

// TestOptimize_SavingsRecorder_ConcurrentCallsNeverRace drives N goroutines
// through Optimize concurrently with ONE shared SavingsRecorder installed,
// proving (a) no data race under -race and (b) exactly N well-formed records
// are accepted — the pipeline-composition-layer analogue of
// TestOptimize_SelectWithEvidence_ConcurrentCallsNeverRace, run at -count=20
// per the R.37 mandate.
func TestOptimize_SavingsRecorder_ConcurrentCallsNeverRace(t *testing.T) {
	o := newOptimizer(t, ladder(t))
	rec := telemetry.NewSavingsRecorder()
	o.SetSavingsRecorder(rec)

	const n = 50
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			r := savingsReq(t1LocalMicro, "", true, 18.00, float64(i), time.Time{})
			if _, err := o.Optimize(r, liveExcept()); err != nil {
				t.Errorf("Optimize(%d): %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	if n2 := rec.Len(); n2 != n {
		t.Fatalf("SavingsRecorder.Len() = %d, want %d (a lost or merged record indicates a race)", n2, n)
	}
}

// --- OptimizeCached (WS6 cache) savings wiring ------------------------------

// TestOptimizeCached_CacheHit_EmitsFullBaselineSavedRecord is THE core proof
// for the cache side of this task: a real cache HIT — where "no tier was
// ever invoked and the full baseline cost was avoided" (savings.go's own doc
// on SavingsRecord.OptimizedCost) — must emit exactly that: a SavingsRecord
// with OptimizedCost == 0 and BaselineCost == the caller-supplied baseline,
// tagged "cache_hit" per SavingsRecord.Tag's own documented example. Routing
// (Optimize) never runs on a hit, so this record can ONLY come from
// OptimizeCached's own cache-hit branch, not from Optimize's wiring.
func TestOptimizeCached_CacheHit_EmitsFullBaselineSavedRecord(t *testing.T) {
	o := newOptimizer(t, ladder(t))
	o.SetCache(cache.New())
	rec := telemetry.NewSavingsRecorder()
	o.SetSavingsRecorder(rec)

	r := savingsReq(t1LocalMicro, "", true, 18.00, 0, time.Time{})
	live := liveExcept()
	execute := func(d Decision) (string, time.Duration, error) {
		return "resp-" + d.Tier.Name, 0, nil
	}

	if _, _, hit, err := o.OptimizeCached("k1", r, live, execute); err != nil || hit {
		t.Fatalf("1st call: hit=%v err=%v, want hit=false err=nil (fresh key must miss)", hit, err)
	}
	if n := rec.Len(); n != 1 {
		t.Fatalf("after miss: SavingsRecorder.Len() = %d, want 1 (from Optimize's own routing-decision wiring)", n)
	}

	// SAME key: must be answered entirely from cache -> a real cache-hit
	// savings record, tagged "cache_hit", full baseline saved.
	if _, _, hit, err := o.OptimizeCached("k1", r, live, execute); err != nil || !hit {
		t.Fatalf("2nd call: hit=%v err=%v, want hit=true err=nil (identical key must hit)", hit, err)
	}
	if n := rec.Len(); n != 2 {
		t.Fatalf("after hit: SavingsRecorder.Len() = %d, want 2 (miss record + hit record, never double-counted, never dropped)", n)
	}
	sr := rec.Records()[1]
	if sr.Tag != "cache_hit" {
		t.Errorf("Tag = %q, want %q", sr.Tag, "cache_hit")
	}
	if sr.OptimizedCost != 0 {
		t.Errorf("OptimizedCost = %v, want 0 (no tier was invoked on a cache hit)", sr.OptimizedCost)
	}
	if sr.BaselineCost != 18.00 {
		t.Errorf("BaselineCost = %v, want 18.00 (the full baseline cost avoided, from req.BaselineCost)", sr.BaselineCost)
	}
	if sr.Savings() != 18.00 {
		t.Errorf("Savings() = %v, want 18.00 (the entire baseline cost was avoided on a hit)", sr.Savings())
	}
}

// TestOptimizeCached_NoCacheInstalled_MissAlwaysDelegatesToOptimize proves
// that with NO cache installed, OptimizeCached's savings accounting is
// EXACTLY Optimize's own (one record per call, from the routing decision) —
// no separate cache-layer emission, matching SetCache's own documented
// nil-safe/no-behavior-change-when-unset contract.
func TestOptimizeCached_NoCacheInstalled_MissAlwaysDelegatesToOptimize(t *testing.T) {
	o := newOptimizer(t, ladder(t))
	rec := telemetry.NewSavingsRecorder()
	o.SetSavingsRecorder(rec)

	r := savingsReq(t1LocalMicro, "", true, 18.00, 0.01, time.Time{})
	live := liveExcept()
	execute := func(d Decision) (string, time.Duration, error) { return "v", 0, nil }

	for i := 0; i < 3; i++ {
		if _, _, hit, err := o.OptimizeCached("k", r, live, execute); err != nil || hit {
			t.Fatalf("iter %d: hit=%v err=%v, want hit=false err=nil", i, hit, err)
		}
	}
	if n := rec.Len(); n != 3 {
		t.Fatalf("SavingsRecorder.Len() = %d, want 3 (one per Optimize call, no cache installed)", n)
	}
}

// TestOptimizeCached_NoSavingsRecorderInstalled_NilSafeNoEmit proves
// OptimizeCached with NO SavingsRecorder installed on a real cache hit has
// zero savings-emission side effects (nil-safe, matching SetCache's own
// pre-existing nil-safe contract for SetEvidenceRecorder/SetCache).
func TestOptimizeCached_NoSavingsRecorderInstalled_NilSafeNoEmit(t *testing.T) {
	o := newOptimizer(t, ladder(t))
	o.SetCache(cache.New())

	r := savingsReq(t1LocalMicro, "", true, 18.00, 0, time.Time{})
	live := liveExcept()
	execute := func(d Decision) (string, time.Duration, error) { return "v", 0, nil }

	if _, _, hit, err := o.OptimizeCached("k1", r, live, execute); err != nil || hit {
		t.Fatalf("1st call: hit=%v err=%v, want hit=false err=nil", hit, err)
	}
	v, d, hit, err := o.OptimizeCached("k1", r, live, execute)
	if err != nil {
		t.Fatalf("2nd call: %v", err)
	}
	if !hit {
		t.Fatal("2nd call: hit = false, want true")
	}
	if v == "" || d != (Decision{}) {
		t.Fatalf("2nd call v=%q d=%+v, want cached value + zero Decision", v, d)
	}
}

// TestOptimizeCached_SavingsRecorder_ConcurrentHitsNeverRace drives N
// goroutines through OptimizeCached concurrently for the SAME key with a
// SavingsRecorder installed, proving no data race under -race and every
// call (miss or hit) is accounted exactly once.
func TestOptimizeCached_SavingsRecorder_ConcurrentHitsNeverRace(t *testing.T) {
	o := newOptimizer(t, ladder(t))
	o.SetCache(cache.New())
	rec := telemetry.NewSavingsRecorder()
	o.SetSavingsRecorder(rec)

	r := savingsReq(t1LocalMicro, "", true, 18.00, 0.01, time.Time{})
	live := liveExcept()
	execute := func(d Decision) (string, time.Duration, error) { return "v", 0, nil }

	const n = 50
	var wg sync.WaitGroup
	var errs int32
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			if _, _, _, err := o.OptimizeCached("shared-key", r, live, execute); err != nil {
				atomic.AddInt32(&errs, 1)
			}
		}()
	}
	wg.Wait()

	if errs != 0 {
		t.Fatalf("%d/%d calls returned an error, want 0", errs, n)
	}
	if got := rec.Len(); got != n {
		t.Fatalf("SavingsRecorder.Len() = %d, want %d (every concurrent call — hit or miss — must be accounted exactly once)", got, n)
	}
}
