package telemetry

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

// ---- ComputeCost: the shared pricing formula (WS1 real $ computation) ----

// TestComputeCostKnownValues locks ComputeCost to a hand-computed formula:
// USD = tokens/1e6 * pricePerMTok, summed across input+output. It FAILs if the
// formula drifts (e.g. divides by 1e3 instead of 1e6, or swaps in/out rates).
func TestComputeCostKnownValues(t *testing.T) {
	tests := []struct {
		name                            string
		in, out                         int64
		pricePerMTokIn, pricePerMTokOut float64
		want                            float64
	}{
		{"zero tokens zero cost", 0, 0, 15, 75, 0},
		{"free tier (0 price) is 0 regardless of tokens", 1_000_000, 500_000, 0, 0, 0},
		{"1M in only", 1_000_000, 0, 3, 15, 3},
		{"1M out only", 0, 1_000_000, 3, 15, 15},
		{"native-class tier: 100k in + 20k out @ $15/$75 per M", 100_000, 20_000, 15, 75, 1.5 + 1.5},
		{"cheap tier: 100k in + 20k out @ $3/$15 per M", 100_000, 20_000, 3, 15, 0.3 + 0.3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ComputeCost(tc.in, tc.out, tc.pricePerMTokIn, tc.pricePerMTokOut)
			if !approxEq(got, tc.want) {
				t.Fatalf("ComputeCost(%d,%d,%v,%v) = %v, want %v", tc.in, tc.out, tc.pricePerMTokIn, tc.pricePerMTokOut, got, tc.want)
			}
		})
	}
}

// ---- SavingsRecord.Savings() ----

func TestSavingsRecordDelta(t *testing.T) {
	r := SavingsRecord{BaselineCost: 3.0, OptimizedCost: 0.6}
	if got, want := r.Savings(), 2.4; !approxEq(got, want) {
		t.Fatalf("Savings() = %v, want %v", got, want)
	}
}

// TestExactRoutingSavings reproduces the operative example from the task
// ("this request routed to T2 instead of T6-native saved N tokens / $X") with
// REAL request metadata: 100,000 input + 20,000 output tokens, priced against
// a $15/$75-per-M "native" baseline tier vs a $3/$15-per-M chosen tier. The
// exact $ savings MUST be 2.4 -- not a fabricated or rounded value.
func TestExactRoutingSavings(t *testing.T) {
	const inTok, outTok = int64(100_000), int64(20_000)
	baseline := ComputeCost(inTok, outTok, 15, 75) // native-class tier
	chosen := ComputeCost(inTok, outTok, 3, 15)    // cheaper tier the router picked

	rec := SavingsRecord{
		Tag:           "T2",
		InputTokens:   inTok,
		OutputTokens:  outTok,
		BaselineCost:  baseline,
		OptimizedCost: chosen,
		At:            time.Unix(0, 0).UTC(),
	}
	if !approxEq(rec.BaselineCost, 3.0) {
		t.Fatalf("baseline cost = %v, want 3.0", rec.BaselineCost)
	}
	if !approxEq(rec.OptimizedCost, 0.6) {
		t.Fatalf("optimized cost = %v, want 0.6", rec.OptimizedCost)
	}
	if got, want := rec.Savings(), 2.4; !approxEq(got, want) {
		t.Fatalf("Savings() = %v, want %v (exact routing-savings delta)", got, want)
	}
}

// TestCacheHitSavesFullBaselineCost proves the second operative example
// ("cache HIT saved the full request cost"): OptimizedCost is exactly 0 (no
// tier was ever invoked) so Savings() equals BaselineCost exactly -- the full
// cost was avoided, never partially credited.
func TestCacheHitSavesFullBaselineCost(t *testing.T) {
	baseline := ComputeCost(250_000, 4_000, 15, 75)
	rec := SavingsRecord{Tag: "cache_hit", InputTokens: 250_000, OutputTokens: 4_000, BaselineCost: baseline, OptimizedCost: 0}
	if got, want := rec.Savings(), baseline; !approxEq(got, want) {
		t.Fatalf("cache-hit Savings() = %v, want full baseline cost %v", got, want)
	}
	if rec.Savings() <= 0 {
		t.Fatalf("cache-hit Savings() = %v, want > 0 (a real cache hit on a priced baseline must show positive savings)", rec.Savings())
	}
}

// TestNoOptimizationYieldsZeroSavingsNeverFabricated is the required negative
// control: when the chosen path costs exactly the same as the baseline (no
// optimization actually happened), Savings() MUST be exactly zero -- never a
// fabricated positive number (anti-bluff: a savings engine that always shows
// a "win" regardless of input is itself a bluff).
func TestNoOptimizationYieldsZeroSavingsNeverFabricated(t *testing.T) {
	cost := ComputeCost(50_000, 10_000, 3, 15)
	rec := SavingsRecord{Tag: "no_optimization", InputTokens: 50_000, OutputTokens: 10_000, BaselineCost: cost, OptimizedCost: cost}
	if got := rec.Savings(); got != 0 {
		t.Fatalf("Savings() = %v, want exactly 0 (no optimization occurred)", got)
	}
}

// TestNegativeSavingsSurfacedHonestly proves a chosen path that costs MORE
// than the baseline (a genuine regression) reports a NEGATIVE savings value,
// never clamped to zero or hidden (§11.4.6 -- report the real number).
func TestNegativeSavingsSurfacedHonestly(t *testing.T) {
	baseline := ComputeCost(10_000, 1_000, 3, 15)
	optimized := ComputeCost(10_000, 1_000, 15, 75) // "optimizer" picked a MORE expensive tier
	rec := SavingsRecord{BaselineCost: baseline, OptimizedCost: optimized}
	if got := rec.Savings(); got >= 0 {
		t.Fatalf("Savings() = %v, want < 0 (regression must be surfaced honestly, never hidden)", got)
	}
}

// ---- SavingsRecorder: validation ----

func TestSavingsRecorderRejectsNegativeCost(t *testing.T) {
	tests := []struct {
		name string
		r    SavingsRecord
	}{
		{"negative baseline", SavingsRecord{BaselineCost: -1, OptimizedCost: 0}},
		{"negative optimized", SavingsRecord{BaselineCost: 1, OptimizedCost: -1}},
		{"both negative", SavingsRecord{BaselineCost: -1, OptimizedCost: -1}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sr := NewSavingsRecorder()
			err := sr.Record(tc.r)
			if !errors.Is(err, ErrNegativeCost) {
				t.Fatalf("Record err = %v, want errors.Is ErrNegativeCost", err)
			}
			if got := sr.Len(); got != 0 {
				t.Fatalf("Len after rejected record = %d, want 0 (§11.4.1 no half-state)", got)
			}
		})
	}
}

// ---- SavingsRecorder: aggregation math ----

func srec(tag string, in, out int64, baseline, optimized float64) SavingsRecord {
	return SavingsRecord{Tag: tag, InputTokens: in, OutputTokens: out, BaselineCost: baseline, OptimizedCost: optimized, At: time.Unix(0, 0).UTC()}
}

func mustRecordSavings(t *testing.T, sr *SavingsRecorder, rec SavingsRecord) {
	t.Helper()
	if err := sr.Record(rec); err != nil {
		t.Fatalf("Record(%+v) unexpected err: %v", rec, err)
	}
}

// TestSavingsAggregatePerTagMath hand-computes every aggregation metric for a
// tag with three records and a tag with one, mirroring
// TestAggregatePerTagMath's token-layer proof at the $ layer.
func TestSavingsAggregatePerTagMath(t *testing.T) {
	sr := NewSavingsRecorder()
	// Tag "T2": savings 1.0, 2.0, 3.0 -> min 1, max 3, sum 6, mean 2.
	mustRecordSavings(t, sr, srec("T2", 1000, 200, 2.0, 1.0)) // savings 1.0
	mustRecordSavings(t, sr, srec("T2", 1000, 200, 3.0, 1.0)) // savings 2.0
	mustRecordSavings(t, sr, srec("T2", 1000, 200, 4.0, 1.0)) // savings 3.0
	// Tag "cache_hit": single record, full cost saved.
	mustRecordSavings(t, sr, srec("cache_hit", 500, 50, 5.0, 0)) // savings 5.0

	rep := sr.Aggregate()

	a, ok := rep.Tags["T2"]
	if !ok {
		t.Fatal("tag T2 missing from report")
	}
	if a.Count != 3 {
		t.Fatalf("tag T2 count = %d, want 3", a.Count)
	}
	if !approxEq(a.SumBaselineCost, 9.0) {
		t.Fatalf("tag T2 sum baseline cost = %v, want 9.0", a.SumBaselineCost)
	}
	if !approxEq(a.SumOptimizedCost, 3.0) {
		t.Fatalf("tag T2 sum optimized cost = %v, want 3.0", a.SumOptimizedCost)
	}
	if !approxEq(a.SumSavings, 6.0) || !approxEq(a.MinSavings, 1.0) || !approxEq(a.MaxSavings, 3.0) {
		t.Fatalf("tag T2 savings stats = %+v, want sum=6 min=1 max=3", a)
	}
	if !approxEq(a.MeanSavings, 2.0) {
		t.Fatalf("tag T2 mean savings = %v, want 2.0", a.MeanSavings)
	}
	// p95 of [1,2,3]: k=(3-1)*0.95=1.9, f=1,c=2 -> 2 + (3-2)*0.9 = 2.9.
	if !approxEq(a.P95Savings, 2.9) {
		t.Fatalf("tag T2 p95 savings = %v, want 2.9", a.P95Savings)
	}

	ch := rep.Tags["cache_hit"]
	if ch.Count != 1 || !approxEq(ch.SumSavings, 5.0) || !approxEq(ch.SumBaselineCost, 5.0) || !approxEq(ch.SumOptimizedCost, 0) {
		t.Fatalf("tag cache_hit stats = %+v, want count=1 sum savings=5 baseline=5 optimized=0", ch)
	}

	if rep.Total.Count != 4 || !approxEq(rep.Total.SumSavings, 11.0) {
		t.Fatalf("total = %+v, want count=4 sum savings=11.0", rep.Total)
	}
}

func TestSavingsEmptyRecorderAggregate(t *testing.T) {
	rep := NewSavingsRecorder().Aggregate()
	if len(rep.Tags) != 0 {
		t.Fatalf("empty recorder Tags = %v, want empty", rep.Tags)
	}
	if rep.Unaccounted != (SavingsStats{}) || rep.Total != (SavingsStats{}) {
		t.Fatalf("empty recorder unaccounted=%+v total=%+v, want zero", rep.Unaccounted, rep.Total)
	}
}

// ---- Unaccounted honesty (§11.4.6), mirrors telemetry.go's Recorder ----

func TestSavingsUnaccountedBucketCapturesUntaggedSpend(t *testing.T) {
	sr := NewSavingsRecorder()
	mustRecordSavings(t, sr, srec("A", 100, 0, 2.0, 1.0)) // 1.0 accounted
	mustRecordSavings(t, sr, srec("", 50, 0, 3.0, 1.0))   // 2.0 unaccounted (empty tag)

	rep := sr.Aggregate()
	if rep.Unaccounted.Count != 1 {
		t.Fatalf("unaccounted count = %d, want 1", rep.Unaccounted.Count)
	}
	if !approxEq(rep.Unaccounted.SumSavings, 2.0) {
		t.Fatalf("unaccounted sum savings = %v, want 2.0", rep.Unaccounted.SumSavings)
	}
	assertSavingsReconciles(t, rep)
}

func TestSavingsKnownTagsRestrictAttribution(t *testing.T) {
	sr := NewSavingsRecorder(WithSavingsKnownTags("A", "B"))
	mustRecordSavings(t, sr, srec("A", 10, 0, 2.0, 1.0)) // accounted, savings 1.0
	mustRecordSavings(t, sr, srec("C", 10, 0, 5.0, 1.0)) // unknown tag -> unaccounted, savings 4.0
	mustRecordSavings(t, sr, srec("", 10, 0, 1.0, 0.0))  // empty tag -> unaccounted, savings 1.0

	rep := sr.Aggregate()
	if _, ok := rep.Tags["C"]; ok {
		t.Fatal("unknown tag C was accounted; must fall to unaccounted")
	}
	if rep.Unaccounted.Count != 2 || !approxEq(rep.Unaccounted.SumSavings, 5.0) {
		t.Fatalf("unaccounted = %+v, want count=2 sum savings=5.0", rep.Unaccounted)
	}
	assertSavingsReconciles(t, rep)
}

// assertSavingsReconciles proves per-tag + unaccounted sums equal the grand
// total exactly -- no savings/cost figure may be lost, double-counted, or
// invented.
func assertSavingsReconciles(t *testing.T, rep SavingsReport) {
	t.Helper()
	var sumCount int
	var sumSavings, sumBaseline, sumOptimized float64
	for _, s := range rep.Tags {
		sumCount += s.Count
		sumSavings += s.SumSavings
		sumBaseline += s.SumBaselineCost
		sumOptimized += s.SumOptimizedCost
	}
	sumCount += rep.Unaccounted.Count
	sumSavings += rep.Unaccounted.SumSavings
	sumBaseline += rep.Unaccounted.SumBaselineCost
	sumOptimized += rep.Unaccounted.SumOptimizedCost
	if sumCount != rep.Total.Count {
		t.Fatalf("reconciliation broken: sum(tag counts)+unaccounted = %d, total = %d", sumCount, rep.Total.Count)
	}
	if !approxEq(sumSavings, rep.Total.SumSavings) {
		t.Fatalf("reconciliation broken: sum(tag savings)+unaccounted = %v, total = %v", sumSavings, rep.Total.SumSavings)
	}
	if !approxEq(sumBaseline, rep.Total.SumBaselineCost) {
		t.Fatalf("reconciliation broken: sum(tag baseline)+unaccounted = %v, total = %v", sumBaseline, rep.Total.SumBaselineCost)
	}
	if !approxEq(sumOptimized, rep.Total.SumOptimizedCost) {
		t.Fatalf("reconciliation broken: sum(tag optimized)+unaccounted = %v, total = %v", sumOptimized, rep.Total.SumOptimizedCost)
	}
}

// ---- Determinism (§11.4.50) ----

func TestSavingsDeterministicAcrossIterations(t *testing.T) {
	base := []SavingsRecord{
		srec("A", 10, 20, 2.0, 1.0), srec("A", 30, 40, 4.0, 1.0), srec("B", 5, 5, 1.0, 0.5),
		srec("", 7, 7, 0.7, 0.7), srec("C", 100, 900, 9.0, 1.0),
	}
	r1 := NewSavingsRecorder()
	for _, x := range base {
		mustRecordSavings(t, r1, x)
	}
	first := r1.Aggregate()
	for i := 0; i < 20; i++ {
		if got := r1.Aggregate(); !reflect.DeepEqual(got, first) {
			t.Fatalf("Aggregate iteration %d differs from first:\n got=%+v\nwant=%+v", i, got, first)
		}
	}
}

// ---- JSONL sink round-trip ----

func TestSavingsJSONLEmitRoundTripFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "savings.jsonl")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create sink: %v", err)
	}
	sr := NewSavingsRecorder(WithSavingsWriter(f))

	in := []SavingsRecord{
		{Tag: "T2", InputTokens: 100_000, OutputTokens: 20_000, BaselineCost: 3.0, OptimizedCost: 0.6, At: time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)},
		{Tag: "cache_hit", InputTokens: 250_000, OutputTokens: 4_000, BaselineCost: 5.0, OptimizedCost: 0, At: time.Date(2026, 7, 8, 12, 1, 0, 0, time.UTC)},
	}
	for _, x := range in {
		mustRecordSavings(t, sr, x)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close sink: %v", err)
	}

	got, err := os.Open(path)
	if err != nil {
		t.Fatalf("reopen sink: %v", err)
	}
	defer got.Close()

	var lines []savingsJSONLine
	sc := bufio.NewScanner(got)
	for sc.Scan() {
		var jl savingsJSONLine
		if err := json.Unmarshal(sc.Bytes(), &jl); err != nil {
			t.Fatalf("parse JSONL line %q: %v", sc.Text(), err)
		}
		lines = append(lines, jl)
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan sink: %v", err)
	}
	if len(lines) != len(in) {
		t.Fatalf("JSONL line count = %d, want %d", len(lines), len(in))
	}
	for i, want := range in {
		gl := lines[i]
		if gl.Tag != want.Tag || gl.InputTokens != want.InputTokens || gl.OutputTokens != want.OutputTokens {
			t.Fatalf("line %d = %+v, want tag/in/out from %+v", i, gl, want)
		}
		if !approxEq(gl.BaselineCost, want.BaselineCost) || !approxEq(gl.OptimizedCost, want.OptimizedCost) {
			t.Fatalf("line %d costs = %+v, want baseline=%v optimized=%v", i, gl, want.BaselineCost, want.OptimizedCost)
		}
		if !approxEq(gl.Savings, want.Savings()) {
			t.Fatalf("line %d savings = %v, want %v (derived)", i, gl.Savings, want.Savings())
		}
		ts, perr := time.Parse(time.RFC3339Nano, gl.Ts)
		if perr != nil {
			t.Fatalf("line %d ts %q not RFC3339Nano: %v", i, gl.Ts, perr)
		}
		if !ts.Equal(want.At) {
			t.Fatalf("line %d ts = %v, want %v", i, ts, want.At)
		}
	}
}

func TestSavingsSinkFailureSurfacedButRecordRetained(t *testing.T) {
	fw := &failingWriter{}
	sr := NewSavingsRecorder(WithSavingsWriter(fw))
	err := sr.Record(srec("A", 10, 20, 2.0, 1.0))
	if err == nil {
		t.Fatal("Record returned nil despite failing sink; error must be surfaced")
	}
	if sr.Len() != 1 {
		t.Fatalf("Len = %d, want 1 (spend retained in memory despite sink failure)", sr.Len())
	}
	if got := sr.Aggregate().Total.SumSavings; !approxEq(got, 1.0) {
		t.Fatalf("total savings = %v, want 1.0 (record must still be accounted)", got)
	}
}

func TestSavingsRecordsReturnsCopy(t *testing.T) {
	sr := NewSavingsRecorder()
	mustRecordSavings(t, sr, srec("A", 1, 1, 1.0, 0.5))
	got := sr.Records()
	got[0].Tag = "MUTATED"
	if again := sr.Records(); again[0].Tag != "A" {
		t.Fatalf("Records() returned a mutable view: got %q after external mutation", again[0].Tag)
	}
}

// ---- Concurrency (run under -race -count=20 per the dispatch instructions) ----

func TestSavingsConcurrentRecordRace(t *testing.T) {
	const goroutines = 32
	const perG = 50
	var buf bytes.Buffer
	sr := NewSavingsRecorder(WithSavingsWriter(&buf), WithSavingsKnownTags("A", "B"))

	var wg sync.WaitGroup
	wg.Add(goroutines)
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				_ = sr.Aggregate()
			}
		}
	}()

	tags := []string{"A", "B", "C", ""} // A,B accounted; C,"" -> unaccounted
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				tag := tags[(g+i)%len(tags)]
				_ = sr.Record(SavingsRecord{Tag: tag, InputTokens: 1, OutputTokens: 1, BaselineCost: 2.0, OptimizedCost: 1.0, At: time.Unix(int64(i), 0).UTC()})
			}
		}(g)
	}
	wg.Wait()
	close(stop)

	want := goroutines * perG
	if got := sr.Len(); got != want {
		t.Fatalf("Len = %d, want %d (records lost under concurrency)", got, want)
	}
	rep := sr.Aggregate()
	if rep.Total.Count != want {
		t.Fatalf("Total.Count = %d, want %d", rep.Total.Count, want)
	}
	if !approxEq(rep.Total.SumSavings, float64(want)*1.0) {
		t.Fatalf("Total.SumSavings = %v, want %v", rep.Total.SumSavings, float64(want)*1.0)
	}
	assertSavingsReconciles(t, rep)

	sc := bufio.NewScanner(&buf)
	nLines := 0
	for sc.Scan() {
		if len(sc.Bytes()) == 0 {
			continue
		}
		var jl savingsJSONLine
		if err := json.Unmarshal(sc.Bytes(), &jl); err != nil {
			t.Fatalf("corrupt JSONL line under concurrency: %q: %v", sc.Text(), err)
		}
		nLines++
	}
	if nLines != want {
		t.Fatalf("emitted JSONL lines = %d, want %d", nLines, want)
	}
}
