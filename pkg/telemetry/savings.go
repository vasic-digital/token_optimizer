// WS1 usage-forensic accounting increment (ATM-660).
//
// telemetry.go proves what was spent (per-tag token distribution). This file
// proves what was SAVED: the baseline-vs-optimized $ delta a routing decision
// or a cache hit produced for one real request, computed from real request
// metadata the caller measured -- never a fabricated or hardcoded number.
//
// Design lineage: docs/research/tokens/ws1_token_waste_baseline/INTEGRATION.md
// establishes the usage-telemetry schema (input/output tokens, per-record
// attribution) that the Python POC (POC/usage_telemetry.py) ingests and
// reports on; this file is the Go-engine increment the README's "Residual
// per-WS gaps" row names as the WS1 real-usage-emission follow-up, scoped to
// the accounting math a caller needs to prove a savings claim once it has
// measured token counts and looked up tier prices -- it does not re-implement
// the transcript-tailing ingest path (a separate, already-tracked follow-up).
//
// Decoupling (§11.4.28): this file, like telemetry.go, ships ZERO project
// constants. It does not import pkg/config or know what a "tier" is -- the
// caller supplies BaselineCost and OptimizedCost directly (typically computed
// via ComputeCost against whatever price table the caller's own config.Tier
// registry holds). This keeps the WS1 accounting math usable by ANY caller,
// including ones that never wire pkg/config at all.
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

// ErrNegativeCost is returned by SavingsRecorder.Record when a record declares
// a negative BaselineCost or OptimizedCost. A negative $ figure is a caller
// bug (a price table or token count fed in wrong), surfaced rather than
// silently clamped or dropped (§11.4.1).
var ErrNegativeCost = errors.New("telemetry: costs must be non-negative")

// ComputeCost is the single formula every caller uses to price a measured
// token count against a tier's per-million-token rates (e.g.
// config.Tier.PricePerMTokIn / PricePerMTokOut). Centralising it here means a
// baseline cost and an optimized cost are ALWAYS computed by the identical
// formula -- two independently hand-rolled cost calculations could silently
// drift and produce a savings number that is a bluff rather than a real
// delta. USD = inputTokens/1e6*pricePerMTokIn + outputTokens/1e6*pricePerMTokOut.
// A price of 0 (a free/local/deterministic tier) always yields 0 regardless of
// token count.
func ComputeCost(inputTokens, outputTokens int64, pricePerMTokIn, pricePerMTokOut float64) float64 {
	const perMillion = 1e6
	return float64(inputTokens)/perMillion*pricePerMTokIn + float64(outputTokens)/perMillion*pricePerMTokOut
}

// SavingsRecord is one WS1 usage-forensic accounting event: the measured
// token counts for a single real request, plus the two $ figures needed to
// prove an optimization's savings claim.
//
//   - BaselineCost is what this EXACT request (same token counts) would have
//     cost on the project's un-optimized baseline path -- e.g. the native /
//     heaviest tier the router would have used with no optimizer present.
//   - OptimizedCost is what the request ACTUALLY cost: the chosen (typically
//     cheaper) tier's price for the same tokens, or exactly 0.0 on a cache
//     HIT, because no tier was ever invoked and the full baseline cost was
//     avoided.
//
// Every field is caller-supplied (§11.4.28): this package prices nothing
// itself. At is caller-supplied and never a wall-clock read, so aggregation
// stays deterministic (§11.4.50), matching Record's contract in telemetry.go.
type SavingsRecord struct {
	// Tag is the consumer-chosen attribution key (e.g. the chosen tier name,
	// a track/alias label, or "cache_hit"). An empty Tag marks a record the
	// consumer could not attribute; it is accounted in the unaccounted
	// bucket, never dropped -- identical semantics to Record.Tag.
	Tag string
	// InputTokens is the prompt/input token count for the turn. Recorded for
	// evidence + cross-reference with the token-layer Recorder; not used in
	// the $ computation itself (BaselineCost/OptimizedCost already encode it).
	InputTokens int64
	// OutputTokens is the completion/output token count for the turn.
	OutputTokens int64
	// BaselineCost is the $ this request would have cost WITHOUT
	// optimization. Must be >= 0.
	BaselineCost float64
	// OptimizedCost is the $ this request ACTUALLY cost. Must be >= 0. Zero
	// means the full baseline cost was avoided (e.g. a cache hit).
	OptimizedCost float64
	// At is the caller-supplied event timestamp.
	At time.Time
}

// Savings is the $ this record's decision saved relative to the baseline:
// BaselineCost - OptimizedCost. It is intentionally NOT clamped at zero --
// a chosen path that cost MORE than the baseline (a real regression) reports
// a genuine negative value so it is never hidden (§11.4.6).
func (r SavingsRecord) Savings() float64 { return r.BaselineCost - r.OptimizedCost }

// savingsJSONLine is the JSONL wire form of a SavingsRecord: the caller
// fields plus the derived savings, matching jsonLine's shape/contract in
// telemetry.go (a tailing consumer never has to recompute the derived field).
type savingsJSONLine struct {
	Ts            string  `json:"ts"`
	Tag           string  `json:"tag"`
	InputTokens   int64   `json:"input_tokens"`
	OutputTokens  int64   `json:"output_tokens"`
	BaselineCost  float64 `json:"baseline_cost"`
	OptimizedCost float64 `json:"optimized_cost"`
	Savings       float64 `json:"savings"`
}

func (r SavingsRecord) marshalSavingsLine() ([]byte, error) {
	b, err := json.Marshal(savingsJSONLine{
		Ts:            r.At.UTC().Format(time.RFC3339Nano),
		Tag:           r.Tag,
		InputTokens:   r.InputTokens,
		OutputTokens:  r.OutputTokens,
		BaselineCost:  r.BaselineCost,
		OptimizedCost: r.OptimizedCost,
		Savings:       r.Savings(),
	})
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// SavingsRecorder is a thread-safe, append-only $-savings accumulator. It
// mirrors Recorder's concurrency contract exactly: safe for concurrent
// Record + Aggregate calls, a slow JSONL sink never blocks a reader, and the
// zero value is not usable -- construct with NewSavingsRecorder.
type SavingsRecorder struct {
	mu  sync.RWMutex
	log []SavingsRecord

	writeMu sync.Mutex
	w       io.Writer

	known map[string]struct{}
}

// SavingsOption configures a SavingsRecorder at construction.
type SavingsOption func(*SavingsRecorder)

// WithSavingsWriter installs an append-only JSONL sink for savings records,
// identical in contract to telemetry.go's WithWriter.
func WithSavingsWriter(w io.Writer) SavingsOption {
	return func(r *SavingsRecorder) { r.w = w }
}

// WithSavingsKnownTags registers the set of tags the consumer considers
// attributable, identical in contract to telemetry.go's WithKnownTags.
func WithSavingsKnownTags(tags ...string) SavingsOption {
	return func(r *SavingsRecorder) {
		for _, t := range tags {
			if t == "" {
				continue
			}
			r.known[t] = struct{}{}
		}
	}
}

// NewSavingsRecorder returns a ready SavingsRecorder configured by opts.
func NewSavingsRecorder(opts ...SavingsOption) *SavingsRecorder {
	r := &SavingsRecorder{known: make(map[string]struct{})}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Record accepts one savings event. It returns ErrNegativeCost for an invalid
// record (nothing is recorded in that case), or a wrapped sink error if the
// JSONL write fails -- with the event retained in the in-memory accounting on
// a sink failure, matching Recorder.Record's honesty contract (§11.4.6).
func (r *SavingsRecorder) Record(rec SavingsRecord) error {
	if rec.BaselineCost < 0 || rec.OptimizedCost < 0 {
		return fmt.Errorf("%w: tag=%q baseline=%v optimized=%v", ErrNegativeCost, rec.Tag, rec.BaselineCost, rec.OptimizedCost)
	}

	r.writeMu.Lock()
	defer r.writeMu.Unlock()

	r.mu.Lock()
	r.log = append(r.log, rec)
	r.mu.Unlock()

	if r.w != nil {
		line, err := rec.marshalSavingsLine()
		if err != nil {
			return fmt.Errorf("telemetry: marshal savings line: %w", err)
		}
		if _, err := r.w.Write(line); err != nil {
			return fmt.Errorf("telemetry: emit savings line: %w", err)
		}
	}
	return nil
}

// Len returns the number of records accepted so far.
func (r *SavingsRecorder) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.log)
}

// Records returns a copy of the in-memory event log in acceptance order.
// Mutating the returned slice does not affect the SavingsRecorder.
func (r *SavingsRecorder) Records() []SavingsRecord {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]SavingsRecord, len(r.log))
	copy(out, r.log)
	return out
}

// SavingsStats is the $ distribution for one bucket (a tag, the unaccounted
// bucket, or the grand total).
type SavingsStats struct {
	Count int
	// SumBaselineCost / SumOptimizedCost are the raw $ sums the savings
	// figures are derived from -- kept alongside SumSavings so a reader can
	// audit the underlying spend, not just the delta.
	SumBaselineCost  float64
	SumOptimizedCost float64
	SumSavings       float64
	MinSavings       float64
	MaxSavings       float64
	MeanSavings      float64
	P95Savings       float64
}

// SavingsReport is the aggregated view produced by Aggregate.
type SavingsReport struct {
	// Tags holds one SavingsStats per accounted tag actually seen.
	Tags map[string]SavingsStats
	// Unaccounted holds the stats for records whose tag was empty or (when a
	// known-tag set is registered) not among the known tags. Never dropped.
	Unaccounted SavingsStats
	// Total holds the stats over every accepted record. Every summed field
	// reconciles exactly with the accounted tags plus Unaccounted.
	Total SavingsStats
}

// Aggregate computes the per-tag, unaccounted, and total $-savings
// distributions over every record accepted so far. Deterministic (§11.4.50):
// the same multiset of records always yields an identical Report regardless
// of insertion order, and repeated calls return identical values.
func (r *SavingsRecorder) Aggregate() SavingsReport {
	r.mu.RLock()
	perTag := make(map[string][]SavingsRecord)
	var unaccounted []SavingsRecord
	all := make([]SavingsRecord, 0, len(r.log))
	for _, rec := range r.log {
		all = append(all, rec)
		if r.isAccountedTag(rec.Tag) {
			perTag[rec.Tag] = append(perTag[rec.Tag], rec)
		} else {
			unaccounted = append(unaccounted, rec)
		}
	}
	r.mu.RUnlock()

	rep := SavingsReport{Tags: make(map[string]SavingsStats, len(perTag))}
	for tag, recs := range perTag {
		rep.Tags[tag] = savingsStatsOf(recs)
	}
	rep.Unaccounted = savingsStatsOf(unaccounted)
	rep.Total = savingsStatsOf(all)
	return rep
}

// isAccountedTag mirrors Recorder.isAccounted exactly.
func (r *SavingsRecorder) isAccountedTag(tag string) bool {
	if tag == "" {
		return false
	}
	if len(r.known) == 0 {
		return true
	}
	_, ok := r.known[tag]
	return ok
}

// savingsStatsOf computes the $ distribution statistics for one bucket. An
// empty slice yields the zero SavingsStats.
func savingsStatsOf(recs []SavingsRecord) SavingsStats {
	if len(recs) == 0 {
		return SavingsStats{}
	}
	savings := make([]float64, len(recs))
	var sumBaseline, sumOptimized, sumSavings float64
	for i, rec := range recs {
		s := rec.Savings()
		savings[i] = s
		sumBaseline += rec.BaselineCost
		sumOptimized += rec.OptimizedCost
		sumSavings += s
	}
	sort.Float64s(savings)
	return SavingsStats{
		Count:            len(recs),
		SumBaselineCost:  sumBaseline,
		SumOptimizedCost: sumOptimized,
		SumSavings:       sumSavings,
		MinSavings:       savings[0],
		MaxSavings:       savings[len(savings)-1],
		MeanSavings:      sumSavings / float64(len(recs)),
		P95Savings:       percentileF64(savings, 95),
	}
}

// percentileF64 is percentile's float64 twin: the identical numpy-style
// linear-interpolation algorithm applied to an ALREADY-SORTED ascending
// slice of $ values instead of token counts. Kept as a literal mirror (rather
// than a generic) so the two are trivially diffable against each other and
// against the WS1 POC's percentile convention. An empty slice yields 0.
func percentileF64(sorted []float64, p float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if n == 1 {
		return sorted[0]
	}
	k := float64(n-1) * (p / 100.0)
	f := int(k)
	c := f + 1
	if c > n-1 {
		c = n - 1
	}
	if f == c {
		return sorted[f]
	}
	return sorted[f] + (sorted[c]-sorted[f])*(k-float64(f))
}
