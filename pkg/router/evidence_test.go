package router

import (
	"bufio"
	"bytes"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/vasic-digital/token_optimizer/pkg/config"
)

// TestNewEvidence_MapsDecisionFieldsExactly proves NewEvidence never re-derives
// or overrides a Decision field — it only adds the caller-supplied correlation
// data the engine cannot itself compute (req_hash, task_class, tokens, cost).
// This is the RED-first proof for the WS5 DESIGN.md §4 item 3 anti-bluff
// guarantee: "every decide() appends a JSONL line {req_hash, task_class,
// load_bearing, chosen_tier, reason, tokens, $}".
func TestNewEvidence_MapsDecisionFieldsExactly(t *testing.T) {
	d := Decision{
		Tier:        config.Tier{Name: "T6_NATIVE"},
		Reason:      ReasonNeverDowngradeFloor,
		LoadBearing: true,
		Floored:     true,
	}
	ev := NewEvidence(d, "req-abc123", "verdict", 512, 0.0123)

	if ev.ReqHash != "req-abc123" {
		t.Errorf("ReqHash = %q, want %q", ev.ReqHash, "req-abc123")
	}
	if ev.TaskClass != "verdict" {
		t.Errorf("TaskClass = %q, want %q", ev.TaskClass, "verdict")
	}
	if ev.LoadBearing != true {
		t.Errorf("LoadBearing = %v, want true (must echo Decision.LoadBearing exactly)", ev.LoadBearing)
	}
	if ev.ChosenTier != "T6_NATIVE" {
		t.Errorf("ChosenTier = %q, want %q (must echo Decision.Tier.Name exactly)", ev.ChosenTier, "T6_NATIVE")
	}
	if ev.Reason != ReasonNeverDowngradeFloor {
		t.Errorf("Reason = %q, want %q (must echo Decision.Reason exactly)", ev.Reason, ReasonNeverDowngradeFloor)
	}
	if ev.Tokens != 512 {
		t.Errorf("Tokens = %d, want 512", ev.Tokens)
	}
	if ev.Cost != 0.0123 {
		t.Errorf("Cost = %v, want 0.0123", ev.Cost)
	}
}

// TestNewEvidence_NeverInventsLoadBearingIndependently proves a non-load-bearing
// Decision produces non-load-bearing Evidence even when the caller's own
// task_class label sounds load-bearing-ish (e.g. "review_correctness") — the
// package must never re-classify from the string, only echo the Decision it
// was handed (§11.4.6 no-guessing: the field is the engine's own verdict, not
// a re-derivation from caller-supplied opaque data).
func TestNewEvidence_NeverInventsLoadBearingIndependently(t *testing.T) {
	d := Decision{
		Tier:        config.Tier{Name: "T2_LOCAL_TASK"},
		Reason:      ReasonMinAdequacy,
		LoadBearing: false,
	}
	ev := NewEvidence(d, "req-xyz", "review_correctness", 10, 0)
	if ev.LoadBearing {
		t.Fatal("LoadBearing must echo Decision.LoadBearing (false), never re-infer from TaskClass text")
	}
}

// schemaKeys is the exact WS5 DESIGN.md §4 item 3 JSONL schema:
// {req_hash, task_class, load_bearing, chosen_tier, reason, tokens, $}.
// This is the golden-schema self-validation per §11.4.107(10): the recorder
// output is decoded and checked against this closed key set, not merely
// "parses as JSON" (a permuted or partially-dropped schema would still parse).
var schemaKeys = []string{"req_hash", "task_class", "load_bearing", "chosen_tier", "reason", "tokens", "$"}

// TestRecorder_EmitsExactSchema proves every emitted line carries the exact
// WS5 DESIGN.md §4 schema key set — no field renamed, dropped, or added.
func TestRecorder_EmitsExactSchema(t *testing.T) {
	var buf bytes.Buffer
	rec, err := NewRecorder(&buf)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	d := Decision{Tier: config.Tier{Name: "T5_ALIAS_CHEAP"}, Reason: ReasonMinAdequacy, LoadBearing: false}
	ev := NewEvidence(d, "req-1", "extract_flat", 100, 0.0005)
	if err := rec.Record(ev); err != nil {
		t.Fatalf("Record: %v", err)
	}

	line := strings.TrimRight(buf.String(), "\n")
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal([]byte(line), &decoded); err != nil {
		t.Fatalf("emitted line is not valid JSON: %v (line=%q)", err, line)
	}
	if len(decoded) != len(schemaKeys) {
		t.Errorf("emitted %d keys, want exactly %d (schema drift): got %v", len(decoded), len(schemaKeys), decoded)
	}
	for _, k := range schemaKeys {
		if _, ok := decoded[k]; !ok {
			t.Errorf("missing required schema key %q in emitted line %q", k, line)
		}
	}
}

// TestRecorder_ZeroValuesNeverOmitted proves an empty/zero-value field (e.g. a
// caller that has no req_hash yet, or a $0 deterministic-tier decision) is
// still PRESENT in the emitted line — never silently dropped via json
// `omitempty`. Captured evidence that silently omits a zero-valued field looks
// identical to captured evidence that never recorded the field at all; the
// schema-completeness test above would not catch an `omitempty`-induced drop
// on this specific all-zero record, so this is a distinct, load-bearing check.
func TestRecorder_ZeroValuesNeverOmitted(t *testing.T) {
	var buf bytes.Buffer
	rec, err := NewRecorder(&buf)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	d := Decision{Tier: config.Tier{Name: "T1_LOCAL_MICRO"}, Reason: ReasonMinAdequacy, LoadBearing: false}
	ev := NewEvidence(d, "", "", 0, 0) // every caller-supplied field is zero-valued
	if err := rec.Record(ev); err != nil {
		t.Fatalf("Record: %v", err)
	}
	line := strings.TrimRight(buf.String(), "\n")
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal([]byte(line), &decoded); err != nil {
		t.Fatalf("emitted line is not valid JSON: %v", err)
	}
	for _, k := range []string{"req_hash", "task_class", "tokens", "$"} {
		if _, ok := decoded[k]; !ok {
			t.Errorf("zero-valued field %q was OMITTED from the emitted line (must always be present, never dropped) — line=%q", k, line)
		}
	}
}

// TestRecorder_ConcurrentRecordsNeverInterleave proves N goroutines calling
// Record concurrently produce exactly N well-formed, non-corrupted JSONL
// lines — no torn write, no interleaved bytes from two concurrent Marshal+
// Write calls landing in the same line.
func TestRecorder_ConcurrentRecordsNeverInterleave(t *testing.T) {
	var buf bytes.Buffer
	rec, err := NewRecorder(&buf)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}

	const n = 50
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			d := Decision{Tier: config.Tier{Name: "T4_LOCAL_MEDIUM"}, Reason: ReasonMinAdequacy, LoadBearing: false}
			ev := NewEvidence(d, "req", "code_small", int64(i), 0)
			if err := rec.Record(ev); err != nil {
				t.Errorf("Record(%d): %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	scanner := bufio.NewScanner(bytes.NewReader(buf.Bytes()))
	count := 0
	for scanner.Scan() {
		var decoded map[string]json.RawMessage
		if err := json.Unmarshal(scanner.Bytes(), &decoded); err != nil {
			t.Fatalf("line %d is corrupted/interleaved JSON: %v (line=%q)", count, err, scanner.Text())
		}
		count++
	}
	if count != n {
		t.Errorf("got %d lines, want %d (a lost or merged line indicates a race)", count, n)
	}
}

// TestNewRecorder_RejectsNilWriter proves a Recorder can never be constructed
// in a state that silently no-ops every Record call — a nil sink defeats the
// entire anti-bluff purpose of the type, so it must fail loudly at
// construction time (§11.4.6), not swallow evidence at emission time.
func TestNewRecorder_RejectsNilWriter(t *testing.T) {
	rec, err := NewRecorder(nil)
	if err == nil {
		t.Fatal("NewRecorder(nil) must return an error, not a silently-inert Recorder")
	}
	if rec != nil {
		t.Fatalf("NewRecorder(nil) returned a non-nil Recorder %#v alongside an error", rec)
	}
}

// --- Wiring: Recorder/Evidence into the router's decision path -------------
//
// Everything above this line proves evidence.go's Evidence/Recorder are a
// CORRECT standalone library. It does not prove they are WIRED: before the
// tests below existed, nothing in pkg/router ever called NewRecorder,
// NewEvidence, or Recorder.Record — evidence.go compiled and passed its own
// tests while being completely unreachable from Router.Select, the package's
// actual routing-decision function (§11.4.124 unwired-code). These tests are
// the RED-first proof (§11.4.43/§11.4.115) that SelectWithEvidence closes
// that gap: installing a Recorder on a Router and driving a decision through
// SelectWithEvidence genuinely appends a captured-evidence JSONL line for
// that decision — the WS5 DESIGN.md §4 item 3 anti-bluff guarantee.

// TestRouter_SelectWithEvidence_EmitsWiredEvidenceRecord proves the emitted
// JSONL line's fields echo BOTH the routing Decision (chosen_tier / reason /
// load_bearing) AND the caller-supplied correlation metadata (req_hash /
// task_class / tokens / $) exactly — a genuine end-to-end wire, not a
// standalone-library coincidence.
func TestRouter_SelectWithEvidence_EmitsWiredEvidenceRecord(t *testing.T) {
	r := newRouter(t, ladder(t))
	var buf bytes.Buffer
	rec, err := NewRecorder(&buf)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	r.SetEvidenceRecorder(rec)

	req := Request{MinTier: t1LocalMicro, LoadBearing: true}
	meta := EvidenceMeta{ReqHash: "req-wired-1", TaskClass: "verdict", Tokens: 777, Cost: 0.042}

	got, err := r.SelectWithEvidence(req, meta)
	if err != nil {
		t.Fatalf("SelectWithEvidence: %v", err)
	}

	// The decision itself must be IDENTICAL to bare Select's — evidence
	// wiring is instrumentation ADDED to the decision path, never a change
	// to the decision path's own logic.
	want, err := r.Select(req)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if got != want {
		t.Fatalf("SelectWithEvidence decision %+v != bare Select decision %+v — evidence wiring altered decision logic", got, want)
	}

	line := strings.TrimRight(buf.String(), "\n")
	var decodedEv Evidence
	if err := json.Unmarshal([]byte(line), &decodedEv); err != nil {
		t.Fatalf("wired emission is not valid JSON: %v (line=%q)", err, line)
	}
	if decodedEv.ReqHash != meta.ReqHash {
		t.Errorf("emitted ReqHash = %q, want %q", decodedEv.ReqHash, meta.ReqHash)
	}
	if decodedEv.TaskClass != meta.TaskClass {
		t.Errorf("emitted TaskClass = %q, want %q", decodedEv.TaskClass, meta.TaskClass)
	}
	if decodedEv.LoadBearing != got.LoadBearing {
		t.Errorf("emitted LoadBearing = %v, want %v (must echo the Decision)", decodedEv.LoadBearing, got.LoadBearing)
	}
	if decodedEv.ChosenTier != got.Tier.Name {
		t.Errorf("emitted ChosenTier = %q, want %q (must echo the Decision)", decodedEv.ChosenTier, got.Tier.Name)
	}
	if decodedEv.Reason != got.Reason {
		t.Errorf("emitted Reason = %q, want %q (must echo the Decision)", decodedEv.Reason, got.Reason)
	}
	if decodedEv.Tokens != meta.Tokens {
		t.Errorf("emitted Tokens = %d, want %d", decodedEv.Tokens, meta.Tokens)
	}
	if decodedEv.Cost != meta.Cost {
		t.Errorf("emitted Cost = %v, want %v", decodedEv.Cost, meta.Cost)
	}
}

// TestRouter_SelectWithEvidence_MultipleCallsAppendOneLineEach proves EACH
// routing decision emits its OWN evidence line (not a shared/overwritten
// record, not a batched/deduplicated one) — three decisions produce three
// JSONL lines, each individually valid and correctly ordered.
func TestRouter_SelectWithEvidence_MultipleCallsAppendOneLineEach(t *testing.T) {
	r := newRouter(t, ladder(t))
	var buf bytes.Buffer
	rec, err := NewRecorder(&buf)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	r.SetEvidenceRecorder(rec)

	reqs := []struct {
		req  Request
		meta EvidenceMeta
	}{
		{Request{MinTier: t1LocalMicro}, EvidenceMeta{ReqHash: "req-a", TaskClass: "classify", Tokens: 10}},
		{Request{MinTier: t4LocalMed}, EvidenceMeta{ReqHash: "req-b", TaskClass: "code_small", Tokens: 200}},
		{Request{LoadBearing: true}, EvidenceMeta{ReqHash: "req-c", TaskClass: "verdict", Tokens: 5}},
	}
	for _, tc := range reqs {
		if _, err := r.SelectWithEvidence(tc.req, tc.meta); err != nil {
			t.Fatalf("SelectWithEvidence(%+v): %v", tc.req, err)
		}
	}

	scanner := bufio.NewScanner(bytes.NewReader(buf.Bytes()))
	var gotHashes []string
	for scanner.Scan() {
		var ev Evidence
		if err := json.Unmarshal(scanner.Bytes(), &ev); err != nil {
			t.Fatalf("line is not valid JSON: %v (line=%q)", err, scanner.Text())
		}
		gotHashes = append(gotHashes, ev.ReqHash)
	}
	want := []string{"req-a", "req-b", "req-c"}
	if len(gotHashes) != len(want) {
		t.Fatalf("got %d evidence lines %v, want %d %v", len(gotHashes), gotHashes, len(want), want)
	}
	for i, h := range want {
		if gotHashes[i] != h {
			t.Errorf("line %d ReqHash = %q, want %q (order must match call order)", i, gotHashes[i], h)
		}
	}
}

// TestRouter_SelectWithEvidence_NilRecorderNoOp proves a Router with NO
// evidence Recorder installed behaves IDENTICALLY through SelectWithEvidence
// as through bare Select — no panic, no error, decision unchanged. Emission
// is opt-in instrumentation; its absence must never be a behavior change
// (§11.4.69).
func TestRouter_SelectWithEvidence_NilRecorderNoOp(t *testing.T) {
	r := newRouter(t, ladder(t))
	req := Request{MinTier: t2LocalTask, LoadBearing: true}

	got, err := r.SelectWithEvidence(req, EvidenceMeta{ReqHash: "unused", TaskClass: "unused", Tokens: 1, Cost: 1})
	if err != nil {
		t.Fatalf("SelectWithEvidence with no recorder installed: %v", err)
	}
	want, err := r.Select(req)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if got != want {
		t.Fatalf("SelectWithEvidence (no recorder) decision %+v != bare Select decision %+v", got, want)
	}
}

// TestRouter_Select_NeverEmitsEvidenceOnItsOwn proves installing an evidence
// Recorder does NOT retroactively change bare Select's behavior: Select
// remains the pure decision function it always was, with zero evidence
// side-effects, even when a Recorder is installed on the same Router.
// Evidence emission is exclusively a SelectWithEvidence concern.
func TestRouter_Select_NeverEmitsEvidenceOnItsOwn(t *testing.T) {
	r := newRouter(t, ladder(t))
	var buf bytes.Buffer
	rec, err := NewRecorder(&buf)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	r.SetEvidenceRecorder(rec)

	if _, err := r.Select(Request{MinTier: t1LocalMicro}); err != nil {
		t.Fatalf("Select: %v", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("bare Select wrote to the installed evidence recorder (buf=%q) — Select must stay a pure decision function", buf.String())
	}
}
