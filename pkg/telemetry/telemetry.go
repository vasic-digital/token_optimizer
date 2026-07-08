// Package telemetry is the WS1 R0 token-usage accounting spine of the
// token_optimizer engine.
//
// It records per-turn LLM token usage as an append-only event log, optionally
// mirrored to a caller-injected JSONL sink, and reports per-tag aggregation
// (count, sum, min, max, mean, p95 of per-record total tokens) plus an explicit
// unaccounted bucket for spend that cannot be attributed to a known tag.
//
// Decoupling (§11.4.28): the package ships ZERO project constants. The consumer
// supplies every record field — the model/tier tag, the input and output token
// counts, and the event timestamp — and, if it wants attribution-checking,
// registers the set of known tags at construction. No wall clock is read inside
// the package: the timestamp is always caller-supplied, so aggregation is fully
// deterministic and testable (§11.4.50).
//
// Honesty (§11.4.6): a record whose tag is empty — or, when a known-tag set is
// registered, whose tag is not among the known tags — is NEVER dropped. Its
// tokens are counted in the unaccounted bucket AND in the grand total, so the
// per-tag sums plus the unaccounted sum always reconcile to the total. Spend is
// surfaced, never silently zeroed or misattributed. This mirrors the WS1 R0 POC
// convention (an unpriced model is reported as unaccounted, never assigned a
// guessed price) generalised from pricing to tag attribution.
package telemetry

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"
)

// ErrNegativeTokens is returned by Record when a record declares a negative
// input or output token count. Token counts are physical measurements and can
// never be negative; a negative value is a caller bug, surfaced rather than
// silently clamped (§11.4.1). It is a sentinel so callers can classify it with
// errors.Is.
var ErrNegativeTokens = errors.New("telemetry: token counts must be non-negative")

// Record is one per-turn token-usage event. Every field is caller-supplied; the
// engine treats Tag as opaque (it never inspects it for a magic value) and never
// reads a wall clock of its own — At is passed in so aggregation is deterministic
// (§11.4.50).
type Record struct {
	// Tag is the consumer-chosen attribution key (e.g. a model id, a tier name,
	// a track/alias label). An empty Tag marks spend the consumer could not
	// attribute; such a record is accounted in the unaccounted bucket, never
	// dropped.
	Tag string
	// InputTokens is the prompt/input token count for the turn. Must be >= 0.
	InputTokens int64
	// OutputTokens is the completion/output token count for the turn. Must be >= 0.
	OutputTokens int64
	// At is the caller-supplied event timestamp. The package never substitutes
	// time.Now(); a zero At is emitted verbatim (an honest "no timestamp
	// reported"), keeping the package clock-free and deterministic.
	At time.Time
}

// TotalTokens is the record's total token spend for the turn.
func (r Record) TotalTokens() int64 { return r.InputTokens + r.OutputTokens }

// jsonLine is the JSONL wire form of a Record: the caller fields plus the
// derived total, with the timestamp as RFC3339Nano UTC. It is a separate type so
// the derived total_tokens is always present in the stream (a consumer tailing
// the JSONL never has to recompute it) and so the wire schema is explicit and
// stable.
type jsonLine struct {
	Ts           string `json:"ts"`
	Tag          string `json:"tag"`
	InputTokens  int64  `json:"input_tokens"`
	OutputTokens int64  `json:"output_tokens"`
	TotalTokens  int64  `json:"total_tokens"`
}

func (r Record) marshalLine() ([]byte, error) {
	b, err := json.Marshal(jsonLine{
		Ts:           r.At.UTC().Format(time.RFC3339Nano),
		Tag:          r.Tag,
		InputTokens:  r.InputTokens,
		OutputTokens: r.OutputTokens,
		TotalTokens:  r.TotalTokens(),
	})
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// Recorder is a thread-safe, append-only token-usage accumulator. It is safe for
// concurrent use by multiple goroutines: the shared request path across the
// context fleet records into one Recorder while readers call Aggregate. The zero
// value is not usable; construct with New.
type Recorder struct {
	// mu guards the in-memory event log. Aggregate takes it for reading only, so
	// a slow JSONL sink never blocks a reader.
	mu  sync.RWMutex
	log []Record

	// writeMu serializes JSONL emission so the sink's line order matches the
	// order records were accepted. It is held across the (possibly blocking)
	// io.Writer call, but NEVER together with mu's data-critical section — the
	// append under mu completes and mu is released before the write — so a slow
	// or blocking sink can never stall a reader (no blocking I/O under the data
	// lock). Only Record acquires writeMu, and it never acquires mu while holding
	// writeMu except for the brief non-blocking append, so no lock-order cycle
	// exists with Aggregate (which takes only mu).
	writeMu sync.Mutex
	w       io.Writer

	// known, when non-empty, restricts accounted tags: a record whose tag is not
	// in this set is treated as unaccounted. When empty, every non-empty tag is
	// accounted (permissive default). It is populated only in New (before the
	// Recorder is shared) and never mutated afterwards, so it is read-safe
	// without a lock.
	known map[string]struct{}
}

// Option configures a Recorder at construction.
type Option func(*Recorder)

// WithWriter installs an append-only JSONL sink. Each accepted Record is written
// as one JSON object followed by a newline, in acceptance order. A nil writer is
// ignored (the Recorder keeps only its in-memory log).
func WithWriter(w io.Writer) Option {
	return func(r *Recorder) { r.w = w }
}

// WithKnownTags registers the set of tags the consumer considers attributable.
// When at least one is registered, a record whose tag is not among them is
// counted in the unaccounted bucket. When none are registered, every non-empty
// tag is accounted. Empty tag strings are ignored here (an empty tag is always
// unaccounted by definition).
func WithKnownTags(tags ...string) Option {
	return func(r *Recorder) {
		for _, t := range tags {
			if t == "" {
				continue
			}
			r.known[t] = struct{}{}
		}
	}
}

// New returns a ready Recorder configured by opts.
func New(opts ...Option) *Recorder {
	r := &Recorder{known: make(map[string]struct{})}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Record accepts one usage event: it appends the event to the in-memory log and,
// if a JSONL sink was installed, emits it. It returns ErrNegativeTokens for an
// invalid record (nothing is recorded in that case), or a wrapped sink error if
// the JSONL write fails. On a sink-write failure the event IS retained in the
// in-memory log — the spend happened, so the failure is surfaced to the caller
// rather than silently dropped from the accounting (§11.4.6).
func (r *Recorder) Record(rec Record) error {
	if rec.InputTokens < 0 || rec.OutputTokens < 0 {
		return fmt.Errorf("%w: tag=%q in=%d out=%d", ErrNegativeTokens, rec.Tag, rec.InputTokens, rec.OutputTokens)
	}

	// Serialize whole-record acceptance so the in-memory append order and the
	// JSONL emit order agree. The data lock (mu) is held only for the append and
	// released before the (possibly blocking) sink write, so readers are never
	// blocked on I/O.
	r.writeMu.Lock()
	defer r.writeMu.Unlock()

	r.mu.Lock()
	r.log = append(r.log, rec)
	r.mu.Unlock()

	if r.w != nil {
		line, err := rec.marshalLine()
		if err != nil {
			return fmt.Errorf("telemetry: marshal usage line: %w", err)
		}
		if _, err := r.w.Write(line); err != nil {
			return fmt.Errorf("telemetry: emit usage line: %w", err)
		}
	}
	return nil
}

// Len returns the number of records accepted so far.
func (r *Recorder) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.log)
}

// Records returns a copy of the in-memory event log in acceptance order.
// Mutating the returned slice does not affect the Recorder.
func (r *Recorder) Records() []Record {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Record, len(r.log))
	copy(out, r.log)
	return out
}

// TagStats is the distribution of per-record total tokens for one bucket. Every
// metric is computed over the TotalTokens() of the records in the bucket. An
// empty bucket has Count 0 and all metrics 0.
type TagStats struct {
	Count      int
	SumTokens  int64
	MinTokens  int64
	MaxTokens  int64
	MeanTokens float64
	P95Tokens  float64
}

// Report is the aggregated view produced by Aggregate.
type Report struct {
	// Tags holds one TagStats per accounted tag actually seen.
	Tags map[string]TagStats
	// Unaccounted holds the stats for records whose tag was empty or (when a
	// known-tag set is registered) not among the known tags. Never dropped.
	Unaccounted TagStats
	// Total holds the stats over every accepted record. Count and SumTokens
	// reconcile exactly with the accounted tags plus Unaccounted:
	//
	//	sum(Tags[*].Count)     + Unaccounted.Count     == Total.Count
	//	sum(Tags[*].SumTokens) + Unaccounted.SumTokens == Total.SumTokens
	Total TagStats
}

// Aggregate computes the per-tag, unaccounted, and total token distributions
// over every record accepted so far. It is deterministic (§11.4.50): the same
// multiset of records always yields an identical Report regardless of the order
// in which the records were recorded, and repeated calls return identical
// values.
func (r *Recorder) Aggregate() Report {
	r.mu.RLock()
	// Build one fresh total-token slice per bucket under the read lock, then
	// release it before the pure, CPU-only stats computation so the lock is held
	// for the minimum time and Aggregate never mutates shared state.
	perTag := make(map[string][]int64)
	var unaccounted []int64
	all := make([]int64, 0, len(r.log))
	for _, rec := range r.log {
		tot := rec.TotalTokens()
		all = append(all, tot)
		if r.isAccounted(rec.Tag) {
			perTag[rec.Tag] = append(perTag[rec.Tag], tot)
		} else {
			unaccounted = append(unaccounted, tot)
		}
	}
	r.mu.RUnlock()

	rep := Report{Tags: make(map[string]TagStats, len(perTag))}
	for tag, vals := range perTag {
		rep.Tags[tag] = statsOf(vals)
	}
	rep.Unaccounted = statsOf(unaccounted)
	rep.Total = statsOf(all)
	return rep
}

// isAccounted reports whether a tag is attributable. An empty tag is never
// accounted. When a known-tag set is registered, only tags in it are accounted;
// otherwise every non-empty tag is accounted.
func (r *Recorder) isAccounted(tag string) bool {
	if tag == "" {
		return false
	}
	if len(r.known) == 0 {
		return true
	}
	_, ok := r.known[tag]
	return ok
}

// statsOf computes the token distribution statistics for one bucket. An empty
// slice yields the zero TagStats. It sorts vals in place; the caller passes a
// freshly-built slice it does not reuse.
func statsOf(vals []int64) TagStats {
	if len(vals) == 0 {
		return TagStats{}
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
	var sum int64
	for _, v := range vals {
		sum += v
	}
	return TagStats{
		Count:      len(vals),
		SumTokens:  sum,
		MinTokens:  vals[0],
		MaxTokens:  vals[len(vals)-1],
		MeanTokens: float64(sum) / float64(len(vals)),
		P95Tokens:  percentile(vals, 95),
	}
}

// percentile returns the linear-interpolated p-th percentile of an
// ALREADY-SORTED ascending slice, matching the WS1 R0 POC (numpy-style linear
// interpolation) so the Go engine reports the same p95 the POC established. An
// empty slice yields 0.
func percentile(sorted []int64, p float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if n == 1 {
		return float64(sorted[0])
	}
	k := float64(n-1) * (p / 100.0)
	f := int(k)
	c := f + 1
	if c > n-1 {
		c = n - 1
	}
	if f == c {
		return float64(sorted[f])
	}
	return float64(sorted[f]) + (float64(sorted[c])-float64(sorted[f]))*(k-float64(f))
}
