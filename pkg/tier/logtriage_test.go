package tier

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// Golden fixtures live under testdata/logtriage/*.log. Each fixture uses the
// project's own per-check convention (`<gate-id>: <description>...
// <ANSI-colored VERDICT> (<reason>)` plus a final `Total tests: N / Passed: N
// / Failed: N / Warnings: N` summary block) — the exact convention documented
// in docs/research/tokens/ws2_deterministic_delegation/INTEGRATION.md §2.2.
//
// The expected values below were cross-validated against the real WS2 shell
// POC (docs/research/tokens/ws2_deterministic_delegation/POC/log_triage.sh)
// run against these SAME fixture bytes in this development session
// (§11.4.107(10) two-implementation agreement) — not merely hand-derived.
// This test file references only testdata inside this submodule (never a
// path outside it), keeping the engine decoupled per §11.4.28.
func TestTriageLog_GoldenFixtures(t *testing.T) {
	tests := []struct {
		fixture string
		want    LogTriageResult
	}{
		{
			fixture: "all_pass.log",
			want: LogTriageResult{
				Total: 2, Passed: 2, Failed: 0, Warnings: 0,
				MandatoryChecksPassed: true,
				FailGates:             []GateFinding{},
				WarnGates:             []GateFinding{},
			},
		},
		{
			fixture: "with_warn.log",
			want: LogTriageResult{
				Total: 3, Passed: 3, Failed: 0, Warnings: 2,
				MandatoryChecksPassed: true,
				FailGates:             []GateFinding{},
				WarnGates: []GateFinding{
					{ID: "CM-WARN-1", Reason: "some reason one"},
					{ID: "CM-WARN-2", Reason: "30/44 propagated - fleet cascade in flight"},
				},
			},
		},
		{
			fixture: "with_fail.log",
			want: LogTriageResult{
				Total: 2, Passed: 1, Failed: 1, Warnings: 0,
				MandatoryChecksPassed: false,
				FailGates: []GateFinding{
					{ID: "CM-FAIL-1", Reason: "root cause: missing config"},
				},
				WarnGates: []GateFinding{},
			},
		},
		{
			// The core anti-bluff regression guard (INTEGRATION.md §2.4): gate
			// ids/descriptions that literally CONTAIN "FAIL"/"WARN" as a
			// substring of their OWN name, but whose actual verdict token is
			// PASS, must NEVER be misclassified as FAIL/WARN entries. Only the
			// genuinely-WARN line (a real ANSI WARN token) may appear.
			fixture: "false_positive_guard.log",
			want: LogTriageResult{
				Total: 4, Passed: 4, Failed: 0, Warnings: 1,
				MandatoryChecksPassed: true,
				FailGates:             []GateFinding{},
				WarnGates: []GateFinding{
					{ID: "CM-REAL-WARN", Reason: "real warning reason"},
				},
			},
		},
		{
			// Malformed/truncated log: no summary block at all. Every count
			// MUST default to -1 (honest "not found"), never a fabricated 0.
			fixture: "missing_summary.log",
			want: LogTriageResult{
				Total: -1, Passed: -1, Failed: -1, Warnings: -1,
				MandatoryChecksPassed: false,
				FailGates:             []GateFinding{},
				WarnGates:             []GateFinding{},
			},
		},
		{
			// A WARN line with no trailing "(reason)" parens at all — Reason
			// must be the empty string, never fabricated text.
			fixture: "no_reason_paren.log",
			want: LogTriageResult{
				Total: 1, Passed: 0, Failed: 0, Warnings: 1,
				MandatoryChecksPassed: false,
				FailGates:             []GateFinding{},
				WarnGates: []GateFinding{
					{ID: "CM-NOPAREN-WARN", Reason: ""},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.fixture, func(t *testing.T) {
			path := filepath.Join("testdata", "logtriage", tt.fixture)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read fixture: %v", err)
			}
			got := TriageLog(path, data)
			tt.want.Source = path
			if got.Total != tt.want.Total || got.Passed != tt.want.Passed ||
				got.Failed != tt.want.Failed || got.Warnings != tt.want.Warnings ||
				got.MandatoryChecksPassed != tt.want.MandatoryChecksPassed {
				t.Fatalf("TriageLog(%s) summary = %+v, want %+v", tt.fixture, got, tt.want)
			}
			if !gateFindingsEqual(got.FailGates, tt.want.FailGates) {
				t.Fatalf("TriageLog(%s) FailGates = %+v, want %+v", tt.fixture, got.FailGates, tt.want.FailGates)
			}
			if !gateFindingsEqual(got.WarnGates, tt.want.WarnGates) {
				t.Fatalf("TriageLog(%s) WarnGates = %+v, want %+v", tt.fixture, got.WarnGates, tt.want.WarnGates)
			}
		})
	}
}

func gateFindingsEqual(a, b []GateFinding) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestTriageLog_NeverFabricatesFailGateFromGateNameSubstring is a dedicated,
// narrowly-scoped repeat of the false_positive_guard.log assertion so the
// specific §11.4.6/§11.4.1 anti-bluff invariant it protects (a gate whose OWN
// name mentions FAIL/WARN must never be misclassified) has its own named,
// independently-failing test — not merged invisibly into the table test.
func TestTriageLog_NeverFabricatesFailGateFromGateNameSubstring(t *testing.T) {
	path := filepath.Join("testdata", "logtriage", "false_positive_guard.log")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	got := TriageLog(path, data)
	if len(got.FailGates) != 0 {
		t.Fatalf("FailGates = %+v, want empty — gate ids containing the substring "+
			"\"FAIL\" (CM-NO-FAIL-OPEN-SKIP, CM-NO-SKIP-FAIL-OPEN) reported PASS, "+
			"not a real FAIL verdict token", got.FailGates)
	}
	for _, w := range got.WarnGates {
		if w.ID == "CM-WARN-BUDGET-CHECK" {
			t.Fatalf("WarnGates incorrectly includes %q — its own name mentions "+
				"WARN but its actual verdict token is PASS", w.ID)
		}
	}
}

// TestTriageLog_NeverErrors documents the honest contract: TriageLog parses
// best-effort and never returns an error — a missing summary line yields -1
// fields (§11.4.6 honest "not found"), never a panic or fabricated data.
func TestTriageLog_NeverErrors(t *testing.T) {
	got := TriageLog("empty", []byte(""))
	if got.Total != -1 || got.Passed != -1 || got.Failed != -1 || got.Warnings != -1 {
		t.Fatalf("TriageLog(empty) = %+v, want all -1 fields", got)
	}
	if got.MandatoryChecksPassed {
		t.Fatalf("TriageLog(empty).MandatoryChecksPassed = true, want false")
	}
	if len(got.FailGates) != 0 || len(got.WarnGates) != 0 {
		t.Fatalf("TriageLog(empty) fabricated gate findings from empty input: %+v", got)
	}
}

// --- Executor wiring (§11.4.124: genuinely wired through the tier
// framework's real dispatch, not a dead standalone) ---

// TestLogTriageExecutor_WiredThroughRegistry proves the log-triage tool is
// callable end-to-end through Registry.Dispatch under TierLogTriage — not a
// function that merely exists unreferenced in the package.
func TestLogTriageExecutor_WiredThroughRegistry(t *testing.T) {
	r := New()
	if err := RegisterLogTriage(r); err != nil {
		t.Fatalf("RegisterLogTriage: %v", err)
	}

	path := filepath.Join("testdata", "logtriage", "with_warn.log")
	resp, err := r.Dispatch(context.Background(), TierLogTriage, Request{
		Payload: LogTriageRequest{Path: path},
	})
	if err != nil {
		t.Fatalf("Dispatch(%s): %v", TierLogTriage, err)
	}
	if resp.Tier != TierLogTriage {
		t.Fatalf("resp.Tier = %q, want %q (evidence stamp)", resp.Tier, TierLogTriage)
	}
	result, ok := resp.Payload.(LogTriageResult)
	if !ok {
		t.Fatalf("resp.Payload type = %T, want LogTriageResult", resp.Payload)
	}
	if result.Total != 3 || result.Warnings != 2 || len(result.WarnGates) != 2 {
		t.Fatalf("dispatched result = %+v, want the with_warn.log golden values", result)
	}
}

// TestLogTriageExecutor_MissingFile proves a nonexistent path surfaces a real
// error through Dispatch — never a fabricated empty-but-successful result
// (§11.4.6).
func TestLogTriageExecutor_MissingFile(t *testing.T) {
	r := New()
	if err := RegisterLogTriage(r); err != nil {
		t.Fatalf("RegisterLogTriage: %v", err)
	}
	_, err := r.Dispatch(context.Background(), TierLogTriage, Request{
		Payload: LogTriageRequest{Path: filepath.Join("testdata", "logtriage", "does_not_exist.log")},
	})
	if err == nil {
		t.Fatalf("Dispatch with missing file returned nil error (fabricated success)")
	}
}

// TestLogTriageExecutor_EmptyPathRejected proves an empty Path is rejected
// with a classifiable sentinel error, never silently treated as "no log".
func TestLogTriageExecutor_EmptyPathRejected(t *testing.T) {
	r := New()
	if err := RegisterLogTriage(r); err != nil {
		t.Fatalf("RegisterLogTriage: %v", err)
	}
	_, err := r.Dispatch(context.Background(), TierLogTriage, Request{Payload: LogTriageRequest{}})
	if !errors.Is(err, ErrLogTriagePathEmpty) {
		t.Fatalf("err = %v, want errors.Is ErrLogTriagePathEmpty", err)
	}
}

// TestLogTriageExecutor_WrongPayloadType proves a non-LogTriageRequest
// payload is rejected with a classifiable sentinel error rather than a panic
// or a silent zero-value triage.
func TestLogTriageExecutor_WrongPayloadType(t *testing.T) {
	r := New()
	if err := RegisterLogTriage(r); err != nil {
		t.Fatalf("RegisterLogTriage: %v", err)
	}
	_, err := r.Dispatch(context.Background(), TierLogTriage, Request{Payload: "not-a-request"})
	if !errors.Is(err, ErrLogTriagePayloadType) {
		t.Fatalf("err = %v, want errors.Is ErrLogTriagePayloadType", err)
	}
}

// TestLogTriageExecutor_ConcurrentDispatch proves the executor is safe for
// concurrent use, matching the Executor interface's documented contract
// ("Implementations MUST be safe for concurrent use — one registered
// Executor serves the whole context fleet"). Run with `go test -race`.
func TestLogTriageExecutor_ConcurrentDispatch(t *testing.T) {
	r := New()
	if err := RegisterLogTriage(r); err != nil {
		t.Fatalf("RegisterLogTriage: %v", err)
	}
	path := filepath.Join("testdata", "logtriage", "with_fail.log")

	const n = 50
	var wg sync.WaitGroup
	errs := make([]error, n)
	results := make([]LogTriageResult, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, err := r.Dispatch(context.Background(), TierLogTriage, Request{
				Payload: LogTriageRequest{Path: path},
			})
			errs[i] = err
			if err == nil {
				results[i] = resp.Payload.(LogTriageResult)
			}
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
		if results[i].Total != 2 || results[i].Failed != 1 || len(results[i].FailGates) != 1 {
			t.Fatalf("goroutine %d: result = %+v, want the with_fail.log golden values", i, results[i])
		}
	}
}
