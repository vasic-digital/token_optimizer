package pipeline

import (
	"sync"
	"testing"
	"time"

	"github.com/vasic-digital/token_optimizer/pkg/cache"
	"github.com/vasic-digital/token_optimizer/pkg/telemetry"
)

// WS1 usage-forensic data-completeness follow-up (ATM-660 continuation,
// round 2).
//
// THE GAP THIS FILE CLOSES (§11.4.6 — confirmed by reading
// pkg/telemetry/savings.go's SavingsRecord struct AND this package's own
// recordSavings (pipeline.go) + OptimizeCached's cache-hit branch (cache.go)
// BEFORE writing any code, never guessed):
//
//   - telemetry.SavingsRecord.InputTokens / OutputTokens EXIST (savings.go)
//     and are documented as "Recorded for evidence + cross-reference with the
//     token-layer Recorder" — but at the time this file was written, EVERY
//     SavingsRecord{...} construction site in this package (recordSavings in
//     pipeline.go, and the cache-hit branch in cache.go) left both fields at
//     their Go zero value. Neither site copied req.InputTokens/req.OutputTokens
//     into the record it built, even though pipeline.Request has carried real,
//     caller-measured per-channel token counts in those exact fields since the
//     AutoBaseline follow-up (auto_baseline_test.go) — those counts were
//     consulted ONLY to compute AutoBaseline's self-derived BaselineCost, then
//     discarded; they never reached the emitted forensic record itself.
//
// THE FIX IS PURELY ADDITIVE, NEVER A GUESS (§11.4.1 / §11.4.6): both
// construction sites now copy req.InputTokens/req.OutputTokens into the
// record's own InputTokens/OutputTokens fields VERBATIM — the exact same
// real, caller-supplied data, never derived, never split from a combined
// total, and forwarded regardless of AutoBaseline (a caller may report real
// per-channel counts for cross-reference even when using a caller-supplied
// BaselineCost). This does not touch the $ computation at all:
// SavingsRecord.Savings() is computed ONLY from BaselineCost/OptimizedCost
// (savings.go's own doc: "not used in the $ computation itself") — these
// tests assert ONLY the two token fields, changing nothing about the
// pre-existing $-figure assertions already proven in savings_wiring_test.go
// and auto_baseline_test.go.

// savingsReqWithTokens extends savingsReq with explicit per-channel token
// counts, mirroring autoBaselineReq's own pattern but WITHOUT forcing
// AutoBaseline — proving the token fields are forwarded to the emitted
// record on their own merits, independent of the AutoBaseline pricing path.
func savingsReqWithTokens(min, floor string, loadBearing bool, baselineCost, optimizedCost float64, at time.Time, inTok, outTok int64) Request {
	r := savingsReq(min, floor, loadBearing, baselineCost, optimizedCost, at)
	r.InputTokens = inTok
	r.OutputTokens = outTok
	return r
}

// TestOptimize_SavingsRecorder_RecordsRealInputOutputTokens is THE core RED
// proof: a real routing decision through Optimize, with real per-channel
// token counts on the Request (AutoBaseline left at its default false — the
// caller supplies BaselineCost directly, as in the pre-existing
// savings_wiring_test.go tests), must emit a SavingsRecord whose
// InputTokens/OutputTokens echo the Request's EXACT values — never zero,
// never a fabricated/derived split.
func TestOptimize_SavingsRecorder_RecordsRealInputOutputTokens(t *testing.T) {
	o := newOptimizer(t, ladder(t))
	rec := telemetry.NewSavingsRecorder()
	o.SetSavingsRecorder(rec)

	r := savingsReqWithTokens(t1LocalMicro, "", true, 18.00, 0.01, time.Time{}, 12_345, 6_789)
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
	if sr.InputTokens != 12_345 {
		t.Errorf("InputTokens = %d, want 12345 (must echo req.InputTokens verbatim, never zero/fabricated)", sr.InputTokens)
	}
	if sr.OutputTokens != 6_789 {
		t.Errorf("OutputTokens = %d, want 6789 (must echo req.OutputTokens verbatim, never zero/fabricated)", sr.OutputTokens)
	}
	// The $ figures must be completely unaffected by this fix (§11.4.1: no new
	// behavior beyond the two token fields).
	if sr.BaselineCost != 18.00 || sr.OptimizedCost != 0.01 {
		t.Errorf("$ figures changed by the token-forwarding fix: baseline=%v optimized=%v, want 18.00/0.01", sr.BaselineCost, sr.OptimizedCost)
	}
}

// TestOptimize_SavingsRecorder_ZeroTokensRequest_RecordsZeroHonestly proves
// the honest counterpart: a request that never populated InputTokens/
// OutputTokens (the pre-existing zero-value default every other
// savings_wiring_test.go test already uses) must still emit InputTokens=0 /
// OutputTokens=0 — i.e. the forwarding fix reports the caller's REAL zero,
// never a fabricated non-zero placeholder.
func TestOptimize_SavingsRecorder_ZeroTokensRequest_RecordsZeroHonestly(t *testing.T) {
	o := newOptimizer(t, ladder(t))
	rec := telemetry.NewSavingsRecorder()
	o.SetSavingsRecorder(rec)

	r := savingsReq(t1LocalMicro, "", true, 18.00, 0.01, time.Time{}) // InputTokens/OutputTokens left at zero value
	if _, err := o.Optimize(r, liveExcept()); err != nil {
		t.Fatalf("Optimize: %v", err)
	}
	sr := rec.Records()[0]
	if sr.InputTokens != 0 || sr.OutputTokens != 0 {
		t.Errorf("InputTokens/OutputTokens = %d/%d, want 0/0 (a request with no measured tokens must record zero honestly, never fabricate a placeholder)", sr.InputTokens, sr.OutputTokens)
	}
}

// TestOptimize_SavingsRecorder_FailoverAlsoRecordsRealInputOutputTokens
// proves the token-forwarding fix applies on BOTH of recordSavings' call
// sites inside Optimize — the direct-selection branch (proven above) AND the
// floor-preserving-failover branch — mirroring
// TestOptimize_SavingsRecorder_FailoverTagsTheFinalBilledTier's own pattern
// for the pre-existing Tag field.
func TestOptimize_SavingsRecorder_FailoverAlsoRecordsRealInputOutputTokens(t *testing.T) {
	c := ladder(t)
	if err := c.RegisterAlternative(t5AliasCheap, t6Native); err != nil {
		t.Fatalf("RegisterAlternative: %v", err)
	}
	o := newOptimizer(t, c)
	rec := telemetry.NewSavingsRecorder()
	o.SetSavingsRecorder(rec)

	r := savingsReqWithTokens("", t5AliasCheap, true, 18.00, 18.00, time.Time{}, 500_000, 250_000)
	got, err := o.Optimize(r, liveExcept(t5AliasCheap))
	if err != nil {
		t.Fatalf("Optimize: %v", err)
	}
	if got.Tier.Name != t6Native || !got.FailedOver {
		t.Fatalf("expected failover to %q, got tier=%q failedOver=%v", t6Native, got.Tier.Name, got.FailedOver)
	}

	sr := rec.Records()[0]
	if sr.InputTokens != 500_000 || sr.OutputTokens != 250_000 {
		t.Errorf("InputTokens/OutputTokens = %d/%d, want 500000/250000 (the failover branch must ALSO forward the real per-channel counts)", sr.InputTokens, sr.OutputTokens)
	}
}

// TestOptimizeCached_CacheHit_RecordsRealInputOutputTokens proves the SECOND
// construction site — OptimizeCached's cache-hit branch (cache.go) — also
// forwards req.InputTokens/req.OutputTokens verbatim, mirroring
// TestOptimizeCached_CacheHit_EmitsFullBaselineSavedRecord's own pattern for
// the pre-existing BaselineCost/OptimizedCost fields. A cache hit skips
// Optimize entirely, so this proof can ONLY come from cache.go's own wiring.
func TestOptimizeCached_CacheHit_RecordsRealInputOutputTokens(t *testing.T) {
	o := newOptimizer(t, ladder(t))
	o.SetCache(cache.New())
	rec := telemetry.NewSavingsRecorder()
	o.SetSavingsRecorder(rec)

	r := savingsReqWithTokens(t1LocalMicro, "", true, 18.00, 0, time.Time{}, 999_000, 111_000)
	live := liveExcept()
	execute := func(d Decision) (string, time.Duration, error) { return "resp-" + d.Tier.Name, 0, nil }

	if _, _, hit, err := o.OptimizeCached("tok1", r, live, execute); err != nil || hit {
		t.Fatalf("1st call: hit=%v err=%v, want hit=false err=nil (fresh key must miss)", hit, err)
	}
	if n := rec.Len(); n != 1 {
		t.Fatalf("after miss: SavingsRecorder.Len() = %d, want 1", n)
	}

	if _, _, hit, err := o.OptimizeCached("tok1", r, live, execute); err != nil || !hit {
		t.Fatalf("2nd call: hit=%v err=%v, want hit=true err=nil (identical key must hit)", hit, err)
	}
	if n := rec.Len(); n != 2 {
		t.Fatalf("after hit: SavingsRecorder.Len() = %d, want 2", n)
	}

	hitRec := rec.Records()[1]
	if hitRec.Tag != "cache_hit" {
		t.Errorf("Tag = %q, want %q", hitRec.Tag, "cache_hit")
	}
	if hitRec.InputTokens != 999_000 || hitRec.OutputTokens != 111_000 {
		t.Errorf("InputTokens/OutputTokens = %d/%d, want 999000/111000 (the cache-hit branch must ALSO forward the real per-channel counts, not just Optimize's own routing path)", hitRec.InputTokens, hitRec.OutputTokens)
	}
}

// TestOptimize_AutoBaseline_SavingsRecord_AlsoCarriesInputOutputTokens proves
// the fix composes cleanly with the pre-existing AutoBaseline feature: when
// AutoBaseline is true, the SAME real InputTokens/OutputTokens ComputeCost
// already consumes to self-derive BaselineCost (auto_baseline_test.go) are
// ALSO now carried on the emitted record's own token fields — the record
// documents the exact measurements the engine priced, not just the resulting
// dollar figure.
func TestOptimize_AutoBaseline_SavingsRecord_AlsoCarriesInputOutputTokens(t *testing.T) {
	o := newOptimizer(t, ladder(t))
	rec := telemetry.NewSavingsRecorder()
	o.SetSavingsRecorder(rec)

	r := autoBaselineReq(t1LocalMicro, true, 1_000_000, 2_000_000, 0.01)
	if _, err := o.Optimize(r, liveExcept()); err != nil {
		t.Fatalf("Optimize: %v", err)
	}
	sr := rec.Records()[0]
	if sr.InputTokens != 1_000_000 || sr.OutputTokens != 2_000_000 {
		t.Errorf("InputTokens/OutputTokens = %d/%d, want 1000000/2000000 (the AutoBaseline-priced tokens must ALSO land on the record itself)", sr.InputTokens, sr.OutputTokens)
	}
}

// TestOptimize_SavingsRecorder_InputOutputTokens_ConcurrentCallsNeverRace
// drives N goroutines through Optimize concurrently, each with DISTINCT
// per-channel token counts, proving (a) no data race under -race and (b)
// every emitted record carries EXACTLY the token counts of the request that
// produced it — no cross-goroutine mixing (§11.4.50 determinism), the
// token-field analogue of TestOptimize_SavingsRecorder_ConcurrentCallsNeverRace.
func TestOptimize_SavingsRecorder_InputOutputTokens_ConcurrentCallsNeverRace(t *testing.T) {
	o := newOptimizer(t, ladder(t))
	rec := telemetry.NewSavingsRecorder()
	o.SetSavingsRecorder(rec)

	const n = 50
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			r := savingsReqWithTokens(t1LocalMicro, "", true, 18.00, float64(i), time.Time{}, int64(i*1000), int64(i*2000))
			if _, err := o.Optimize(r, liveExcept()); err != nil {
				t.Errorf("Optimize(%d): %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	if got := rec.Len(); got != n {
		t.Fatalf("SavingsRecorder.Len() = %d, want %d (a lost or merged record indicates a race)", got, n)
	}
	for _, sr := range rec.Records() {
		i := int(sr.OptimizedCost) // OptimizedCost was set to float64(i), uniquely identifying the goroutine
		wantIn, wantOut := int64(i*1000), int64(i*2000)
		if sr.InputTokens != wantIn || sr.OutputTokens != wantOut {
			t.Fatalf("record for i=%d: InputTokens/OutputTokens = %d/%d, want %d/%d (cross-goroutine mixing detected)", i, sr.InputTokens, sr.OutputTokens, wantIn, wantOut)
		}
	}
}
