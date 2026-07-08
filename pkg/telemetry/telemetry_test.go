package telemetry

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

// ---- helpers ----

const eps = 1e-9

func approxEq(a, b float64) bool { return math.Abs(a-b) <= eps }

func mustRecord(t *testing.T, r *Recorder, rec Record) {
	t.Helper()
	if err := r.Record(rec); err != nil {
		t.Fatalf("Record(%+v) unexpected err: %v", rec, err)
	}
}

func rec(tag string, in, out int64) Record {
	return Record{Tag: tag, InputTokens: in, OutputTokens: out, At: time.Unix(0, 0).UTC()}
}

// ---- Record validation ----

func TestRecordRejectsNegativeTokens(t *testing.T) {
	tests := []struct {
		name string
		r    Record
	}{
		{"negative input", Record{Tag: "A", InputTokens: -1, OutputTokens: 10}},
		{"negative output", Record{Tag: "A", InputTokens: 10, OutputTokens: -5}},
		{"both negative", Record{Tag: "A", InputTokens: -3, OutputTokens: -3}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := New()
			err := r.Record(tc.r)
			if !errors.Is(err, ErrNegativeTokens) {
				t.Fatalf("Record err = %v, want errors.Is ErrNegativeTokens", err)
			}
			// A rejected record must not land in the log (§11.4.1 no half-state).
			if got := r.Len(); got != 0 {
				t.Fatalf("Len after rejected record = %d, want 0", got)
			}
		})
	}
}

func TestTotalTokens(t *testing.T) {
	r := Record{InputTokens: 132200, OutputTokens: 640}
	if got, want := r.TotalTokens(), int64(132840); got != want {
		t.Fatalf("TotalTokens = %d, want %d", got, want)
	}
}

// ---- Aggregation math ----

// TestAggregatePerTagMath asserts every aggregation metric against a hand-
// computed expectation. It FAILs if count / sum / min / max / mean / p95 for any
// tag is miscomputed.
func TestAggregatePerTagMath(t *testing.T) {
	r := New()
	// Tag "A": totals 30, 70, 110 (in+out).  min 30, max 110, sum 210, mean 70.
	mustRecord(t, r, rec("A", 10, 20)) // 30
	mustRecord(t, r, rec("A", 30, 40)) // 70
	mustRecord(t, r, rec("A", 60, 50)) // 110
	// Tag "B": single total 5.
	mustRecord(t, r, rec("B", 2, 3)) // 5

	rep := r.Aggregate()

	a, ok := rep.Tags["A"]
	if !ok {
		t.Fatal("tag A missing from report")
	}
	if a.Count != 3 || a.SumTokens != 210 || a.MinTokens != 30 || a.MaxTokens != 110 {
		t.Fatalf("tag A stats = %+v, want count=3 sum=210 min=30 max=110", a)
	}
	if !approxEq(a.MeanTokens, 70) {
		t.Fatalf("tag A mean = %v, want 70", a.MeanTokens)
	}
	// p95 of [30,70,110]: k=(3-1)*0.95=1.9, f=1, c=2 -> 70 + (110-70)*0.9 = 106.
	if !approxEq(a.P95Tokens, 106) {
		t.Fatalf("tag A p95 = %v, want 106", a.P95Tokens)
	}

	b := rep.Tags["B"]
	if b.Count != 1 || b.SumTokens != 5 || b.MinTokens != 5 || b.MaxTokens != 5 ||
		!approxEq(b.MeanTokens, 5) || !approxEq(b.P95Tokens, 5) {
		t.Fatalf("tag B stats = %+v, want single-record 5 across all", b)
	}

	// Total reconciles: 4 records, sum 215.
	if rep.Total.Count != 4 || rep.Total.SumTokens != 215 {
		t.Fatalf("total = %+v, want count=4 sum=215", rep.Total)
	}
}

// TestPercentileKnownDistributions locks the interpolation algorithm to the WS1
// POC. It FAILs if percentile() drifts from numpy-style linear interpolation.
func TestPercentileKnownDistributions(t *testing.T) {
	oneToHundred := make([]int64, 100)
	for i := range oneToHundred {
		oneToHundred[i] = int64(i + 1) // 1..100 already ascending
	}
	tests := []struct {
		name   string
		sorted []int64
		p      float64
		want   float64
	}{
		{"empty", nil, 95, 0},
		{"single", []int64{42}, 95, 42},
		{"two p95", []int64{10, 20}, 95, 19.5}, // 10 + 10*0.95
		{"1..100 p95", oneToHundred, 95, 95.05},
		{"1..100 p50", oneToHundred, 50, 50.5},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := percentile(tc.sorted, tc.p); !approxEq(got, tc.want) {
				t.Fatalf("percentile(%s) = %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}

func TestEmptyRecorderAggregate(t *testing.T) {
	rep := New().Aggregate()
	if len(rep.Tags) != 0 {
		t.Fatalf("empty recorder Tags = %v, want empty", rep.Tags)
	}
	if rep.Unaccounted != (TagStats{}) || rep.Total != (TagStats{}) {
		t.Fatalf("empty recorder unaccounted=%+v total=%+v, want zero", rep.Unaccounted, rep.Total)
	}
}

// ---- Unaccounted honesty (§11.4.6) ----

// TestUnaccountedBucketCapturesUntaggedSpend proves un-tagged spend lands in the
// unaccounted bucket (non-zero) and that per-tag + unaccounted reconcile to the
// grand total (no token lost). It FAILs if the unaccounted logic drops or
// misattributes untagged records.
func TestUnaccountedBucketCapturesUntaggedSpend(t *testing.T) {
	r := New()
	mustRecord(t, r, rec("A", 100, 0)) // 100 accounted
	mustRecord(t, r, rec("A", 200, 0)) // 200 accounted
	mustRecord(t, r, rec("", 50, 0))   // 50 unaccounted (empty tag)
	mustRecord(t, r, rec("", 25, 0))   // 25 unaccounted (empty tag)

	rep := r.Aggregate()

	if rep.Unaccounted.Count != 2 {
		t.Fatalf("unaccounted count = %d, want 2 (untagged records must not be dropped)", rep.Unaccounted.Count)
	}
	if rep.Unaccounted.SumTokens != 75 {
		t.Fatalf("unaccounted sum = %d, want 75", rep.Unaccounted.SumTokens)
	}
	assertReconciles(t, rep)
}

// TestUnaccountedZeroWhenAllTagged proves the bucket is empty when every record
// is attributable — the honesty signal must not fire spuriously.
func TestUnaccountedZeroWhenAllTagged(t *testing.T) {
	r := New()
	mustRecord(t, r, rec("A", 10, 0))
	mustRecord(t, r, rec("B", 20, 0))
	rep := r.Aggregate()
	if rep.Unaccounted != (TagStats{}) {
		t.Fatalf("unaccounted = %+v, want zero (all records tagged)", rep.Unaccounted)
	}
	assertReconciles(t, rep)
}

// TestKnownTagsRestrictAttribution proves the known-tag set generalises the
// honesty vector from the POC (unpriced model -> unaccounted): a record whose
// tag is not in the registered known set falls to unaccounted, alongside empty
// tags, and reconciliation still holds.
func TestKnownTagsRestrictAttribution(t *testing.T) {
	r := New(WithKnownTags("A", "B"))
	mustRecord(t, r, rec("A", 10, 0)) // accounted
	mustRecord(t, r, rec("B", 20, 0)) // accounted
	mustRecord(t, r, rec("C", 40, 0)) // unknown tag -> unaccounted
	mustRecord(t, r, rec("", 80, 0))  // empty tag  -> unaccounted

	rep := r.Aggregate()

	if _, ok := rep.Tags["C"]; ok {
		t.Fatal("unknown tag C was accounted; must fall to unaccounted")
	}
	if len(rep.Tags) != 2 {
		t.Fatalf("accounted tags = %v, want exactly {A,B}", rep.Tags)
	}
	if rep.Unaccounted.Count != 2 || rep.Unaccounted.SumTokens != 120 {
		t.Fatalf("unaccounted = %+v, want count=2 sum=120 (C=40 + untagged=80)", rep.Unaccounted)
	}
	assertReconciles(t, rep)
}

// assertReconciles is the load-bearing "never silently drop" invariant: the
// accounted-tag counts+sums PLUS the unaccounted bucket equal the grand total
// exactly. No token may be lost, double-counted, or invented.
func assertReconciles(t *testing.T, rep Report) {
	t.Helper()
	var sumCount int
	var sumTokens int64
	for _, s := range rep.Tags {
		sumCount += s.Count
		sumTokens += s.SumTokens
	}
	sumCount += rep.Unaccounted.Count
	sumTokens += rep.Unaccounted.SumTokens
	if sumCount != rep.Total.Count {
		t.Fatalf("reconciliation broken: sum(tag counts)+unaccounted = %d, total = %d", sumCount, rep.Total.Count)
	}
	if sumTokens != rep.Total.SumTokens {
		t.Fatalf("reconciliation broken: sum(tag tokens)+unaccounted = %d, total = %d", sumTokens, rep.Total.SumTokens)
	}
}

// ---- Determinism (§11.4.50) ----

// TestDeterministicAcrossIterations proves Aggregate is deterministic: repeated
// calls on the same recorder return identical Reports, AND a second recorder fed
// the SAME records in a shuffled order produces an identical Report (order
// independence). It FAILs if any metric depends on record insertion order.
func TestDeterministicAcrossIterations(t *testing.T) {
	base := []Record{
		rec("A", 10, 20), rec("A", 30, 40), rec("B", 5, 5),
		rec("", 7, 7), rec("C", 100, 900), rec("B", 1, 2), rec("", 3, 3),
	}
	r1 := New()
	for _, x := range base {
		mustRecord(t, r1, x)
	}
	first := r1.Aggregate()
	for i := 0; i < 100; i++ {
		if got := r1.Aggregate(); !reflect.DeepEqual(got, first) {
			t.Fatalf("Aggregate iteration %d differs from first:\n got=%+v\nwant=%+v", i, got, first)
		}
	}

	// Shuffle a copy and feed a fresh recorder: same multiset -> same Report.
	shuffled := make([]Record, len(base))
	copy(shuffled, base)
	rng := rand.New(rand.NewSource(1)) // fixed seed -> deterministic test (§11.4.50)
	rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	r2 := New()
	for _, x := range shuffled {
		mustRecord(t, r2, x)
	}
	if got := r2.Aggregate(); !reflect.DeepEqual(got, first) {
		t.Fatalf("shuffled-order Report differs (aggregation is order-dependent):\n got=%+v\nwant=%+v", got, first)
	}
}

// ---- JSONL sink round-trip (integration: real file writer) ----

// TestJSONLEmitRoundTripFile writes to a real temp file, reads it back, and
// asserts one well-formed JSONL line per record in acceptance order with correct
// derived total_tokens. It FAILs if the emit path drops, reorders, or mis-serialises.
func TestJSONLEmitRoundTripFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "usage.jsonl")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create sink: %v", err)
	}
	r := New(WithWriter(f))

	in := []Record{
		{Tag: "opus", InputTokens: 300, OutputTokens: 640, At: time.Date(2026, 7, 7, 22, 0, 0, 0, time.UTC)},
		{Tag: "sonnet", InputTokens: 25000, OutputTokens: 3400, At: time.Date(2026, 7, 7, 22, 3, 0, 0, time.UTC)},
		{Tag: "", InputTokens: 5, OutputTokens: 5, At: time.Date(2026, 7, 7, 22, 4, 0, 0, time.UTC)},
	}
	for _, x := range in {
		mustRecord(t, r, x)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close sink: %v", err)
	}

	got, err := os.Open(path)
	if err != nil {
		t.Fatalf("reopen sink: %v", err)
	}
	defer got.Close()

	var lines []jsonLine
	sc := bufio.NewScanner(got)
	for sc.Scan() {
		var jl jsonLine
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
		if gl.TotalTokens != want.TotalTokens() {
			t.Fatalf("line %d total_tokens = %d, want %d (derived)", i, gl.TotalTokens, want.TotalTokens())
		}
		ts, perr := time.Parse(time.RFC3339Nano, gl.Ts)
		if perr != nil {
			t.Fatalf("line %d ts %q not RFC3339Nano: %v", i, gl.Ts, perr)
		}
		if !ts.Equal(want.At) {
			t.Fatalf("line %d ts = %v, want %v (caller-supplied, no wall clock)", i, ts, want.At)
		}
	}
}

// TestSinkFailureSurfacedButRecordRetained proves a sink write failure is
// surfaced to the caller (§11.4.6) yet the spend is retained in the in-memory
// accounting — never silently dropped.
func TestSinkFailureSurfacedButRecordRetained(t *testing.T) {
	fw := &failingWriter{}
	r := New(WithWriter(fw))
	err := r.Record(rec("A", 10, 20))
	if err == nil {
		t.Fatal("Record returned nil despite failing sink; error must be surfaced")
	}
	if r.Len() != 1 {
		t.Fatalf("Len = %d, want 1 (spend retained in memory despite sink failure)", r.Len())
	}
	if got := r.Aggregate().Total.SumTokens; got != 30 {
		t.Fatalf("total tokens = %d, want 30 (record must still be accounted)", got)
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("sink down") }

func TestRecordsReturnsCopy(t *testing.T) {
	r := New()
	mustRecord(t, r, rec("A", 1, 1))
	got := r.Records()
	got[0].Tag = "MUTATED"
	if again := r.Records(); again[0].Tag != "A" {
		t.Fatalf("Records() returned a mutable view: got %q after external mutation", again[0].Tag)
	}
}

// ---- Concurrency (run under -race) ----

// TestConcurrentRecordRace records from many goroutines into one Recorder while
// another goroutine reads via Aggregate, then asserts nothing was lost and the
// grand total reconciles. Under -race it also proves the recorder is data-race
// free.
func TestConcurrentRecordRace(t *testing.T) {
	const goroutines = 32
	const perG = 50
	var buf bytes.Buffer
	// The shared bytes.Buffer is written only from Record, which serialises all
	// emits via the Recorder's writeMu, so no two goroutines touch buf at once;
	// the reader goroutine below calls Aggregate (data lock only) and never the
	// buffer. -race confirms both paths are clean.
	r := New(WithWriter(&buf), WithKnownTags("A", "B"))

	var wg sync.WaitGroup
	wg.Add(goroutines)
	// A concurrent reader to exercise the reader/writer lock split under -race.
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				_ = r.Aggregate()
			}
		}
	}()

	tags := []string{"A", "B", "C", ""} // A,B accounted; C,"" -> unaccounted
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				tag := tags[(g+i)%len(tags)]
				_ = r.Record(Record{Tag: tag, InputTokens: 1, OutputTokens: 1, At: time.Unix(int64(i), 0).UTC()})
			}
		}(g)
	}
	wg.Wait()
	close(stop)

	want := goroutines * perG
	if got := r.Len(); got != want {
		t.Fatalf("Len = %d, want %d (records lost under concurrency)", got, want)
	}
	rep := r.Aggregate()
	if rep.Total.Count != want {
		t.Fatalf("Total.Count = %d, want %d", rep.Total.Count, want)
	}
	if rep.Total.SumTokens != int64(want*2) {
		t.Fatalf("Total.SumTokens = %d, want %d", rep.Total.SumTokens, want*2)
	}
	assertReconciles(t, rep)

	// Every emitted JSONL line must be well-formed (no interleaving corruption).
	sc := bufio.NewScanner(&buf)
	nLines := 0
	for sc.Scan() {
		if len(sc.Bytes()) == 0 {
			continue
		}
		var jl jsonLine
		if err := json.Unmarshal(sc.Bytes(), &jl); err != nil {
			t.Fatalf("corrupt JSONL line under concurrency: %q: %v", sc.Text(), err)
		}
		nLines++
	}
	if nLines != want {
		t.Fatalf("emitted JSONL lines = %d, want %d", nLines, want)
	}
}
