package router

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// This file wires the WS5 captured-evidence emission that
// docs/research/tokens/ws5_alias_routing/DESIGN.md §4 item 3 specifies as an
// anti-bluff guarantee ("every decide() appends a JSONL line {req_hash,
// task_class, load_bearing, chosen_tier, reason, tokens, $} ... A PASS
// without this line is a §11.4 bluff") and which
// docs/research/tokens/ws5_alias_routing/INTEGRATION.md §5 follow-up #3
// records as an explicit honest gap: "Wire the JSONL evidence emission (§2
// step 6) — currently only specified, not coded." This closes that gap.
//
// Decoupling (§11.4.28): Request/Decision carry no task_class vocabulary —
// that classification belongs to the consumer, not this engine. Evidence
// therefore accepts the consumer-supplied req_hash/task_class/tokens/cost
// alongside the engine-produced Decision; the router package never invents,
// re-derives, or guesses any of those four values (§11.4.6).

// ErrNilEvidenceWriter is returned by NewRecorder when handed a nil sink. A
// Recorder with no writer would silently discard every Record call, which
// defeats the entire anti-bluff purpose of captured-evidence emission — so
// construction fails loudly instead (§11.4.6), rather than emission silently
// no-op'ing later.
var ErrNilEvidenceWriter = errors.New("router: evidence writer must be non-nil")

// Evidence is one routing-decision captured-evidence record, matching the
// exact JSONL schema in DESIGN.md §4 item 3. Every field uses an explicit
// (non-`omitempty`) json tag: a zero-valued field (an empty req_hash, a $0
// deterministic-tier decision) is still emitted, never silently dropped —
// captured evidence that omits a zero value is indistinguishable from
// captured evidence that never recorded the field, which is itself a bluff
// surface (§11.4.6).
type Evidence struct {
	// ReqHash is the consumer's own request-correlation identifier or hash.
	// Opaque to this package — never inspected, never generated here.
	ReqHash string `json:"req_hash"`
	// TaskClass is the consumer's own task-classification label (e.g.
	// "verdict", "extract_flat", "code_small"). Opaque to this package: the
	// engine hardcodes no task_class vocabulary (§11.4.28).
	TaskClass string `json:"task_class"`
	// LoadBearing echoes Decision.LoadBearing exactly — this package never
	// re-derives it from TaskClass or any other caller-supplied string.
	LoadBearing bool `json:"load_bearing"`
	// ChosenTier echoes Decision.Tier.Name exactly.
	ChosenTier string `json:"chosen_tier"`
	// Reason echoes Decision.Reason exactly (one of the Reason* constants).
	Reason string `json:"reason"`
	// Tokens is the consumer-supplied total token count for the turn this
	// decision routed. This package prices and counts nothing itself — token
	// accounting is pkg/telemetry's job (WS1); Evidence only carries the
	// figure through to the routing evidence trail per the DESIGN.md schema.
	Tokens int64 `json:"tokens"`
	// Cost is the consumer-supplied USD cost for the turn, from the
	// consumer's own price table (DESIGN.md §5 — "PRICE-TABLE-DRIVEN, never
	// a hardcoded cost guess"). The JSON key is literally "$" per the
	// DESIGN.md §4 schema.
	Cost float64 `json:"$"`
}

// NewEvidence builds an Evidence record from a routing Decision plus the four
// consumer-supplied correlation fields the engine does not itself compute.
// It never overrides or re-derives a Decision field — LoadBearing, ChosenTier,
// and Reason are copied verbatim from d.
func NewEvidence(d Decision, reqHash, taskClass string, tokens int64, cost float64) Evidence {
	return Evidence{
		ReqHash:     reqHash,
		TaskClass:   taskClass,
		LoadBearing: d.LoadBearing,
		ChosenTier:  d.Tier.Name,
		Reason:      d.Reason,
		Tokens:      tokens,
		Cost:        cost,
	}
}

// Recorder is a thread-safe, append-only JSONL evidence sink for routing
// decisions. It carries no in-memory log and performs no aggregation — token/
// cost aggregation is pkg/telemetry's job (WS1); Recorder's sole
// responsibility is ordered, concurrency-safe JSONL emission of Evidence
// lines, per DESIGN.md §4 item 3.
type Recorder struct {
	mu sync.Mutex
	w  interface {
		Write(p []byte) (n int, err error)
	}
}

// NewRecorder returns a Recorder writing JSONL evidence lines to w. w must be
// non-nil; NewRecorder returns ErrNilEvidenceWriter otherwise rather than
// constructing a Recorder that would silently discard every Record call.
func NewRecorder(w interface {
	Write(p []byte) (n int, err error)
}) (*Recorder, error) {
	if w == nil {
		return nil, ErrNilEvidenceWriter
	}
	return &Recorder{w: w}, nil
}

// Record appends one Evidence line to the sink as a single JSON object
// followed by a newline. Concurrent calls are serialized under mu so line
// order matches call order and no two concurrent Marshal+Write calls can
// interleave their bytes into one corrupted line.
func (rec *Recorder) Record(e Evidence) error {
	b, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("router: marshal evidence: %w", err)
	}
	b = append(b, '\n')

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if _, err := rec.w.Write(b); err != nil {
		return fmt.Errorf("router: write evidence: %w", err)
	}
	return nil
}

// --- Wiring into the router's decision path ---------------------------------
//
// Everything above this line is a correct, fully-tested, but STANDALONE
// library: nothing in this package ever called NewRecorder, NewEvidence, or
// Recorder.Record from the routing-decision path (Router.Select). A Recorder
// installed by a consumer would therefore never receive a single line —
// captured-evidence emission was specified (DESIGN.md §4 item 3) and unit-
// tested in isolation, but unreachable from the engine's actual decision
// function (§11.4.124 unwired-code). SetEvidenceRecorder + SelectWithEvidence
// close that gap: they are the ONLY path by which a Decision produced by this
// package can reach a Recorder.

// EvidenceMeta carries the four consumer-supplied correlation fields
// NewEvidence needs alongside a routing Decision — the request-correlation
// hash, the consumer's task-classification label, the turn's token count, and
// its USD cost. They are opaque to this package exactly like Request.ID
// (§11.4.28 decoupling): SelectWithEvidence never inspects them for tier
// selection, never re-derives one from another, and never invents a value
// when a field is left zero (§11.4.6) — it only forwards them verbatim into
// the emitted Evidence record via NewEvidence.
type EvidenceMeta struct {
	// ReqHash is the consumer's own request-correlation identifier or hash.
	ReqHash string
	// TaskClass is the consumer's own task-classification label.
	TaskClass string
	// Tokens is the consumer-supplied total token count for the turn.
	Tokens int64
	// Cost is the consumer-supplied USD cost for the turn.
	Cost float64
}

// SetEvidenceRecorder installs rec as this Router's evidence sink: every
// subsequent SelectWithEvidence call additionally emits one Evidence JSONL
// record for its Decision via rec.Record. Passing nil disables emission.
//
// Installing a Recorder never changes Select's behavior — Select ignores
// evidence entirely, in every configuration — and a Router on which
// SetEvidenceRecorder is never called behaves exactly as one built before
// this wiring existed: SelectWithEvidence still works, it just never emits
// (see the nil-check in SelectWithEvidence). This is the "optional, nil-safe,
// no behavior change when unset" contract the WS5 evidence wiring requires.
func (r *Router) SetEvidenceRecorder(rec *Recorder) {
	r.evidence = rec
}

// SelectWithEvidence wraps Select with routing-evidence emission, closing the
// WS5 DESIGN.md §4 item 3 anti-bluff guarantee: "every decide() appends a
// JSONL line {req_hash, task_class, load_bearing, chosen_tier, reason,
// tokens, $} ... A PASS without this line is a §11.4 bluff."
//
// It calls Select exactly as-is — the decision LOGIC is completely unchanged,
// SelectWithEvidence adds nothing to and removes nothing from tier selection
// — and, IF an evidence Recorder is installed via SetEvidenceRecorder,
// additionally emits ONE Evidence record built from the resulting Decision
// plus meta's four consumer-supplied correlation fields (§11.4.28: this
// package cannot itself compute req_hash/task_class/tokens/cost, so it never
// invents them; that is exactly why they arrive as an explicit parameter
// rather than living on Request — see EvidenceMeta and evidence.go's package
// doc).
//
// When no Recorder is installed, SelectWithEvidence is behaviourally
// IDENTICAL to calling Select directly (meta is ignored entirely) — running
// with no recorder is a fully supported, zero-overhead, zero-behavior-change
// configuration (§11.4.69's opt-in-instrumentation contract).
//
// A Select error is returned verbatim with no evidence-emission attempt —
// there is no Decision to record. A subsequent Record failure (evidence-sink
// I/O error) is wrapped and returned alongside the now-valid Decision: the
// caller gets both the successful routing decision AND an honest signal that
// its captured-evidence trail is incomplete for this call. Silently
// swallowing a Record failure here would recreate the exact "PASS without the
// evidence line" bluff this wiring exists to prevent (§11.4.6).
func (r *Router) SelectWithEvidence(req Request, meta EvidenceMeta) (Decision, error) {
	d, err := r.Select(req)
	if err != nil {
		return d, err
	}
	if r.evidence == nil {
		return d, nil
	}
	ev := NewEvidence(d, meta.ReqHash, meta.TaskClass, meta.Tokens, meta.Cost)
	if recErr := r.evidence.Record(ev); recErr != nil {
		return d, fmt.Errorf("router: emit routing evidence: %w", recErr)
	}
	return d, nil
}
