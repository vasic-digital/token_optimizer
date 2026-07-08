package tier

// Deterministic-delegation tool: log_triage (WS2/ATM-661, D4 task class).
//
// This file is the Go port of the WS2 proof-of-concept
// docs/research/tokens/ws2_deterministic_delegation/POC/log_triage.sh — the
// exact classification logic, ported faithfully rather than reimplemented
// from memory. It deterministically triages a
// pre_build_verification.sh / meta_test_false_positive_proof.sh-style sweep
// log (this project's harnesses print
// "<gate-id>: <desc>... <ANSI-colored VERDICT> (<reason>)" per-check lines
// plus a final "Total tests: N / Passed: N / Failed: N / Warnings: N"
// summary) into a compact structured verdict-input — WITHOUT any model call.
//
// # Anti-bluff discriminator (§11.4.6/§11.4.107(10))
//
// A naive substring match on "FAIL"/"WARN" false-positives on gate
// identifiers/descriptions that merely MENTION those words as part of their
// own name (e.g. a gate literally named CM-NO-FAIL-OPEN-SKIP that itself
// reports PASS). This was a genuine finding surfaced by building the shell
// POC (INTEGRATION.md §2.4), not a hypothetical — the exact class of bluff
// §11.4/§1.1 exists to catch in a deterministic tool, not only a model one.
// The fix (also present in the shell POC): match ONLY the harness's own
// ANSI-colored verdict token (an escape sequence directly followed by WARN
// or FAIL, directly followed by another escape sequence) attached to the
// per-check line, NEVER a bare substring anywhere in the line. See
// TestTriageLog_NeverFabricatesFailGateFromGateNameSubstring for the
// permanent regression guard.
//
// # Decoupling (§11.4.28)
//
// This file has ZERO dependency on any path outside this submodule — no
// project-specific gate names, no reference to the parent repo's docs/ tree.
// Its cross-validation against the shell POC (byte-identical classifications
// on 6 golden fixtures under testdata/logtriage/) was performed once during
// development by running the real POC script against these SAME fixture
// files and diffing JSON output; that one-time proof is documented in the
// commit message rather than executed as a permanent go test dependency on
// bash/the parent tree, so this engine stays fully reusable by any consumer.
//
// # Honesty guarantee (§11.4.6)
//
// TriageLog never fabricates a verdict of its own — it only extracts what
// the harness already asserted. A missing summary line yields -1 (honest
// "not found"), never a guessed 0. Judgment (was the sweep as a whole
// release-ready, does a WARN need escalation) stays strictly the caller's
// job, fed by this tool's structured output rather than raw log bytes.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// TierLogTriage is the stable tier name this tool registers under — the
// exact task-class name illustrated in
// docs/research/tokens/ws2_deterministic_delegation/INTEGRATION.md §3.3 for
// constitution/actions/subagent_tiering.yaml's `classes:` entry.
const TierLogTriage = "log_triage"

// Log-triage executor errors. Sentinel values so callers can classify with
// errors.Is (mirrors the registry.go convention).
var (
	// ErrLogTriagePayloadType is returned when Request.Payload is not a
	// LogTriageRequest — a wrong-type payload MUST fail loudly, never
	// silently produce a zero-value triage (§11.4.6).
	ErrLogTriagePayloadType = errors.New("tier: log_triage executor requires a LogTriageRequest payload")
	// ErrLogTriagePathEmpty is returned when LogTriageRequest.Path is empty.
	ErrLogTriagePathEmpty = errors.New("tier: log_triage requires a non-empty Path")
)

// LogTriageRequest is the Payload a caller sets on a Request dispatched to
// the TierLogTriage tier. Path identifies the sweep log to triage.
type LogTriageRequest struct {
	// Path is the sweep-log file to read and triage. Required.
	Path string
}

// GateFinding is one {"id":"...","reason":"..."} entry the harness already
// asserted for a WARN or FAIL verdict line — the gate identifier (the text
// before the first ": " on the line) and the parenthesized reason at the end
// of the line, if any (empty string if the line carries no "(reason)").
type GateFinding struct {
	ID     string
	Reason string
}

// LogTriageResult mirrors log_triage.sh's JSON verdict-input exactly (field
// names differ only in Go capitalization; see MarshalJSON-compatible field
// tags below is unnecessary — callers needing JSON can encode this struct
// directly, its zero-value-free FailGates/WarnGates slices already encode as
// `[]` rather than `null`).
type LogTriageResult struct {
	// Source is the log path or caller-supplied identifier this result was
	// produced from — carried through unchanged, never inferred.
	Source string
	// Total, Passed, Failed, Warnings are the harness's own declared summary
	// counts. -1 means "not found in this log" (honest absence, §11.4.6) —
	// never a fabricated 0.
	Total, Passed, Failed, Warnings int
	// MandatoryChecksPassed is true iff the log contains the harness's own
	// literal "ALL MANDATORY CHECKS PASSED" marker line.
	MandatoryChecksPassed bool
	// FailGates and WarnGates are every gate whose line carried the
	// harness's own ANSI-colored FAIL / WARN verdict token, in the order
	// they appear in the log. Never nil — an empty result is []GateFinding{}
	// so JSON encoding (if a caller wants it) emits `[]`, not `null`.
	FailGates []GateFinding
	WarnGates []GateFinding
}

const mandatoryPassedMarker = "ALL MANDATORY CHECKS PASSED"

// ansiRE strips every ANSI SGR escape sequence, mirroring the shell POC's
// `sed -E 's/\x1b\[[0-9;]*m//g'`.
var ansiRE = regexp.MustCompile("\x1b\\[[0-9;]*m")

// Summary-line extractors, mirroring the shell POC's
// `grep -m1 -oE '^\s*<Label>:\s*[0-9]+'` against the ANSI-stripped text, run
// in per-line ("(?m)") mode so `^` anchors at the start of every log line.
var (
	totalRE    = regexp.MustCompile(`(?m)^\s*Total tests:\s*([0-9]+)`)
	passedRE   = regexp.MustCompile(`(?m)^\s*Passed:\s*([0-9]+)`)
	failedRE   = regexp.MustCompile(`(?m)^\s*Failed:\s*([0-9]+)`)
	warningsRE = regexp.MustCompile(`(?m)^\s*Warnings:\s*([0-9]+)`)
)

// reasonRE extracts the parenthesized reason at the very end of a
// (already-ANSI-stripped, already-leading-trimmed) per-check line, mirroring
// the shell POC's awk `match(rest, /\(([^)]*)\)[[:space:]]*$/)`.
var reasonRE = regexp.MustCompile(`\(([^)]*)\)[ \t\r]*$`)

// verdictTokenRE builds the raw-ANSI-token matcher for one verdict word
// (WARN or FAIL), mirroring the shell POC's
// `${ESC}\[[0-9;]+m${word}${ESC}\[[0-9;]*m` pattern: an escape sequence
// carrying at least one digit/semicolon, then the literal verdict word, then
// another escape sequence (which may be the bare reset `\x1b[m`, hence the
// trailing class is `*` not `+`). Matching is deliberately performed against
// the RAW (not-yet-ANSI-stripped) line — the escape-sequence adjacency IS the
// discriminator that makes this immune to a gate's own name mentioning the
// word (see the package doc comment's anti-bluff discriminator note).
func verdictTokenRE(word string) *regexp.Regexp {
	return regexp.MustCompile("\x1b\\[[0-9;]+m" + regexp.QuoteMeta(word) + "\x1b\\[[0-9;]*m")
}

var (
	warnTokenRE = verdictTokenRE("WARN")
	failTokenRE = verdictTokenRE("FAIL")
)

// wsCutset is the POSIX [[:space:]] class the shell POC's leading-trim
// (`sed -E 's/^[[:space:]]*//'`) uses, applied per-line via strings.TrimLeft.
const wsCutset = " \t\n\v\f\r"

// extractFirstInt returns the first captured integer re finds in clean, or -1
// if re does not match — the honest "not found" default (§11.4.6), never a
// fabricated 0.
func extractFirstInt(clean string, re *regexp.Regexp) int {
	m := re.FindStringSubmatch(clean)
	if m == nil {
		return -1
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		return -1
	}
	return n
}

// extractVerdictLines walks raw (the ORIGINAL, ANSI-intact log text) line by
// line and returns one GateFinding per line carrying tokenRE's raw-ANSI
// verdict token, in file order. Never returns nil (an empty result is
// []GateFinding{}).
func extractVerdictLines(raw string, tokenRE *regexp.Regexp) []GateFinding {
	findings := []GateFinding{}
	for _, line := range strings.Split(raw, "\n") {
		if !tokenRE.MatchString(line) {
			continue
		}
		clean := strings.TrimLeft(ansiRE.ReplaceAllString(line, ""), wsCutset)

		id := clean
		if idx := strings.Index(clean, ": "); idx >= 0 {
			id = clean[:idx]
		}

		reason := ""
		if m := reasonRE.FindStringSubmatch(clean); m != nil {
			reason = m[1]
		}

		findings = append(findings, GateFinding{ID: id, Reason: reason})
	}
	return findings
}

// TriageLog deterministically parses a sweep log's own already-asserted
// PASS/FAIL/WARN verdicts into a compact structured verdict-input, exactly
// reproducing docs/research/tokens/ws2_deterministic_delegation/POC/log_triage.sh's
// classification logic. source is carried through unchanged into
// Result.Source (typically the file path, but callers may pass any
// identifier). TriageLog never returns an error — parsing is best-effort and
// total; a malformed or truncated log yields -1 summary fields and empty gate
// slices rather than a panic or a fabricated result (§11.4.6).
func TriageLog(source string, data []byte) LogTriageResult {
	raw := string(data)
	clean := ansiRE.ReplaceAllString(raw, "")

	return LogTriageResult{
		Source:                source,
		Total:                 extractFirstInt(clean, totalRE),
		Passed:                extractFirstInt(clean, passedRE),
		Failed:                extractFirstInt(clean, failedRE),
		Warnings:              extractFirstInt(clean, warningsRE),
		MandatoryChecksPassed: strings.Contains(clean, mandatoryPassedMarker),
		FailGates:             extractVerdictLines(raw, failTokenRE),
		WarnGates:             extractVerdictLines(raw, warnTokenRE),
	}
}

// logTriageExecute is the pkg/tier.Executor for TierLogTriage: it reads the
// file named by the request's LogTriageRequest.Path and returns its
// TriageLog result as the Response.Payload. A wrong-type payload, an empty
// Path, or a read failure surfaces a real, classifiable error — never a
// fabricated empty-but-successful result (§11.4.6).
func logTriageExecute(_ context.Context, req Request) (Response, error) {
	ltr, ok := req.Payload.(LogTriageRequest)
	if !ok {
		return Response{}, fmt.Errorf("%w: got %T", ErrLogTriagePayloadType, req.Payload)
	}
	if ltr.Path == "" {
		return Response{}, ErrLogTriagePathEmpty
	}
	data, err := os.ReadFile(ltr.Path)
	if err != nil {
		return Response{}, fmt.Errorf("tier: log_triage: read %q: %w", ltr.Path, err)
	}
	return Response{Payload: TriageLog(ltr.Path, data)}, nil
}

// NewLogTriageExecutor returns the Executor for the log_triage deterministic
// tool. Safe for concurrent use — it holds no mutable state (§11.4.85
// concurrent-contention posture).
func NewLogTriageExecutor() Executor {
	return ExecutorFunc(logTriageExecute)
}

// RegisterLogTriage registers the log_triage deterministic tool's Executor
// under TierLogTriage on r, so it is dispatchable through r.Dispatch exactly
// like any other tier — a caller resolves the task class to "mechanical" per
// constitution/actions/subagent_tiering.yaml (per
// docs/research/tokens/ws2_deterministic_delegation/INTEGRATION.md §3.3),
// then dispatches to TierLogTriage instead of a subagent/model call for this
// task class. Returns Register's error verbatim (e.g. ErrDuplicateExecutor
// if TierLogTriage is already registered on r).
func RegisterLogTriage(r *Registry) error {
	return r.Register(TierLogTriage, NewLogTriageExecutor())
}
