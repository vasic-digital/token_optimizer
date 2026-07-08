package pipeline

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/vasic-digital/token_optimizer/pkg/router"
)

// --- RED-first proof: Optimize is GENUINELY wired to SelectWithEvidence ----
//
// pkg/router/evidence.go's own doc (as of the WS5 R.37 landing) states the
// honest gap this file closes: "nothing in this package ever called
// NewRecorder, NewEvidence, or Recorder.Record from the routing-decision path
// (Router.Select). A Recorder installed by a consumer would therefore never
// receive a single line" UNLESS the caller drives selection through
// SelectWithEvidence directly. But pkg/pipeline.Optimizer — the engine's
// actual single request-path entry point real consumers call — held onto a
// bare `o.router.Select(req.Request)` call, so even a consumer that correctly
// discovered and wired router.SelectWithEvidence one layer down had NO way to
// reach it through the documented entry point. That is the exact "correct but
// unused" pattern §11.4.124 forbids shipping silently: evidence.go was a
// standalone-correct library, unreachable from the one path production code
// actually calls. These tests prove Optimize now reaches it.

// evReq extends the pipeline_test.go req() helper with the three
// evidence-correlation fields (TaskClass/Tokens/Cost) pipeline.Request now
// carries. Per §11.4.28 decoupling (mirroring router.EvidenceMeta's own doc),
// these fields are opaque, consumer-supplied data — the pipeline never
// infers, re-derives, or fabricates them; this helper only forwards exactly
// what the caller passes in.
func evReq(min, floor string, loadBearing bool, id, taskClass string, tokens int64, cost float64) Request {
	r := req(min, floor, loadBearing)
	r.ID = id
	r.TaskClass = taskClass
	r.Tokens = tokens
	r.Cost = cost
	return r
}

// TestOptimize_SelectWithEvidence_EmitsWiredEvidenceRecord proves that
// installing a *router.Recorder on an Optimizer and driving a decision
// through Optimize genuinely appends a captured-evidence JSONL line whose
// fields echo BOTH the real request data (req_hash from req.ID, task_class /
// tokens / $ from the request's own TaskClass/Tokens/Cost fields) AND the
// router's routing decision (chosen_tier / reason / load_bearing) — a
// real end-to-end wire reachable from the engine's documented entry point,
// not a standalone-library coincidence one layer down.
func TestOptimize_SelectWithEvidence_EmitsWiredEvidenceRecord(t *testing.T) {
	o := newOptimizer(t, ladder(t))
	var buf bytes.Buffer
	rec, err := router.NewRecorder(&buf)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	o.SetEvidenceRecorder(rec)

	r := evReq(t1LocalMicro, "", true, "req-wired-1", "verdict", 777, 0.042)
	got, err := o.Optimize(r, liveExcept())
	if err != nil {
		t.Fatalf("Optimize: %v", err)
	}
	if got.Tier.Name != t6Native {
		t.Fatalf("tier = %q, want %q", got.Tier.Name, t6Native)
	}

	line := strings.TrimRight(buf.String(), "\n")
	if line == "" {
		t.Fatal("Optimize with a recorder installed produced NO evidence line — SelectWithEvidence is still unreachable from Optimize")
	}
	var ev router.Evidence
	if err := json.Unmarshal([]byte(line), &ev); err != nil {
		t.Fatalf("emitted line is not valid JSON: %v (line=%q)", err, line)
	}
	if ev.ReqHash != "req-wired-1" {
		t.Errorf("ReqHash = %q, want %q (must come from req.ID — the request's own identifier, never invented)", ev.ReqHash, "req-wired-1")
	}
	if ev.TaskClass != "verdict" {
		t.Errorf("TaskClass = %q, want %q (must come from req.TaskClass)", ev.TaskClass, "verdict")
	}
	if ev.Tokens != 777 {
		t.Errorf("Tokens = %d, want 777 (must come from req.Tokens)", ev.Tokens)
	}
	if ev.Cost != 0.042 {
		t.Errorf("Cost = %v, want 0.042 (must come from req.Cost)", ev.Cost)
	}
	if ev.LoadBearing != got.LoadBearing {
		t.Errorf("LoadBearing = %v, want %v (must echo the router decision)", ev.LoadBearing, got.LoadBearing)
	}
	if ev.ChosenTier != t6Native {
		t.Errorf("ChosenTier = %q, want %q (must echo router.Select's decision)", ev.ChosenTier, t6Native)
	}
	if ev.Reason != router.ReasonNeverDowngradeFloor {
		t.Errorf("Reason = %q, want %q", ev.Reason, router.ReasonNeverDowngradeFloor)
	}
}

// TestOptimize_SelectWithEvidence_DescribesSelectedTierEvenOnFailover proves
// the emitted evidence describes router.Select's decision — the request's
// floor ENTITLEMENT — even when Optimize subsequently fails over to a
// different live alternative. Evidence records WHY the request was entitled
// to route where it did (the SELECTED tier), exactly mirroring
// pipeline.Decision.SelectedTier's own documented contract; it is not merely
// "whatever tier the byte ended up on" (that is pipeline.Decision.Tier).
func TestOptimize_SelectWithEvidence_DescribesSelectedTierEvenOnFailover(t *testing.T) {
	c := ladder(t)
	if err := c.RegisterAlternative(t5AliasCheap, t6Native); err != nil {
		t.Fatalf("RegisterAlternative: %v", err)
	}
	o := newOptimizer(t, c)
	var buf bytes.Buffer
	rec, err := router.NewRecorder(&buf)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	o.SetEvidenceRecorder(rec)

	r := evReq("", t5AliasCheap, true, "req-failover-1", "code_agentic", 100, 1.0)
	got, err := o.Optimize(r, liveExcept(t5AliasCheap))
	if err != nil {
		t.Fatalf("Optimize: %v", err)
	}
	if got.Tier.Name != t6Native || !got.FailedOver {
		t.Fatalf("expected failover to %q, got tier=%q failedOver=%v", t6Native, got.Tier.Name, got.FailedOver)
	}

	line := strings.TrimRight(buf.String(), "\n")
	var ev router.Evidence
	if err := json.Unmarshal([]byte(line), &ev); err != nil {
		t.Fatalf("emitted line is not valid JSON: %v (line=%q)", err, line)
	}
	if ev.ChosenTier != t5AliasCheap {
		t.Errorf("ChosenTier = %q, want %q (the router-SELECTED tier / floor entitlement, not the post-failover final Tier %q)",
			ev.ChosenTier, t5AliasCheap, got.Tier.Name)
	}
}

// TestOptimize_NoRecorderInstalled_BehaviorUnchanged proves Optimize with NO
// evidence Recorder installed behaves EXACTLY as documented pre-wiring: the
// pipeline_test.go suite already runs every existing case with no recorder
// installed and passes unmodified. This test adds the one direct assertion
// that pipeline.Request's new evidence-correlation fields (TaskClass/Tokens/
// Cost), when populated but no recorder is ever installed, have ZERO effect
// on the returned Decision — confirming the fields are pure, inert,
// opt-in-instrumentation data (§11.4.69) that cannot influence routing.
func TestOptimize_NoRecorderInstalled_BehaviorUnchanged(t *testing.T) {
	o := newOptimizer(t, ladder(t))
	withEvidence := evReq(t1LocalMicro, "", true, "req-x", "verdict", 999, 9.99)
	withoutEvidence := req(t1LocalMicro, "", true)

	got1, err := o.Optimize(withEvidence, liveExcept())
	if err != nil {
		t.Fatalf("Optimize(withEvidence): %v", err)
	}
	got2, err := o.Optimize(withoutEvidence, liveExcept())
	if err != nil {
		t.Fatalf("Optimize(withoutEvidence): %v", err)
	}
	if got1 != got2 {
		t.Fatalf("populated-but-unrecorded evidence fields changed the Decision: %+v != %+v", got1, got2)
	}
}

// TestOptimize_SelectWithEvidence_ConcurrentCallsNeverRace drives N
// goroutines through Optimize concurrently with ONE shared evidence Recorder
// installed, proving (a) no data race under -race and (b) exactly N
// well-formed, non-corrupted, non-duplicated JSONL evidence lines are
// produced. This is the pipeline-composition-layer analogue of
// router.TestRecorder_ConcurrentRecordsNeverInterleave — run at -count=20 per
// the R.37 mandate (a prior WS6 single-flight race was missed at -count=3).
func TestOptimize_SelectWithEvidence_ConcurrentCallsNeverRace(t *testing.T) {
	o := newOptimizer(t, ladder(t))
	var buf bytes.Buffer
	rec, err := router.NewRecorder(&buf)
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	o.SetEvidenceRecorder(rec)

	const n = 50
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			r := evReq(t1LocalMicro, "", true, fmt.Sprintf("req-%d", i), "verdict", int64(i), 0)
			if _, err := o.Optimize(r, liveExcept()); err != nil {
				t.Errorf("Optimize(%d): %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	scanner := bufio.NewScanner(bytes.NewReader(buf.Bytes()))
	seen := make(map[string]bool, n)
	count := 0
	for scanner.Scan() {
		var ev router.Evidence
		if err := json.Unmarshal(scanner.Bytes(), &ev); err != nil {
			t.Fatalf("line %d is corrupted/interleaved JSON: %v (line=%q)", count, err, scanner.Text())
		}
		if seen[ev.ReqHash] {
			t.Errorf("duplicate ReqHash %q emitted — a concurrent Record call double-wrote", ev.ReqHash)
		}
		seen[ev.ReqHash] = true
		count++
	}
	if count != n {
		t.Fatalf("got %d evidence lines, want %d (a lost or merged line indicates a race)", count, n)
	}
}

// erroringWriter always fails Write, simulating an evidence-sink I/O outage
// (e.g. a full disk or a closed pipe under the JSONL log file).
type erroringWriter struct{}

func (erroringWriter) Write(p []byte) (int, error) {
	return 0, fmt.Errorf("simulated evidence-sink write failure")
}

// TestOptimize_SelectWithEvidence_RecordFailureIsNeverSilentlySwallowed
// proves that when an installed Recorder's sink fails to write, Optimize
// surfaces that failure honestly (§11.4.6) rather than reporting a
// business-logic PASS while quietly losing the captured-evidence trail — the
// exact silent-swallow bluff §11.4 forbids at the evidence layer.
func TestOptimize_SelectWithEvidence_RecordFailureIsNeverSilentlySwallowed(t *testing.T) {
	o := newOptimizer(t, ladder(t))
	rec, err := router.NewRecorder(erroringWriter{})
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	o.SetEvidenceRecorder(rec)

	if _, err := o.Optimize(evReq(t1LocalMicro, "", true, "req-io-fail", "verdict", 1, 0), liveExcept()); err == nil {
		t.Fatal("Optimize succeeded silently despite the installed evidence Recorder's Write failing — the evidence-emission failure was swallowed")
	}
}
