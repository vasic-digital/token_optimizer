// Package pipeline (this file): the OPTIONAL cache-first composition of
// Optimize with a caller-supplied downstream execute step — the
// pkg/cache -> pkg/router -> failover request flow
// docs/research/tokens/ws6_caching_sync/DESIGN.md §1 specifies for a
// model-completion request ("L1 exact-result ... miss -> ... CALL -> store
// in L1/L2").
//
// THE HONEST FINDING THIS FILE RECORDS (§11.4.6 — determined by reading
// DESIGN.md §1 and this package's own pipeline.go doc comment BEFORE writing
// any code, not guessed). Optimize's documented, tested contract (pipeline.go:
// "binds tier selection ... into a single request-path DECISION") is a
// ROUTING decision — a config.Tier plus evidence metadata. It never calls a
// tier and never produces the actual model response DESIGN.md §1's L1 stage
// caches ("L1 hit -> RET: return cached response (0 tokens)"). Consulting a
// response cache INSIDE Optimize's own body would therefore require Optimize
// to ALSO own tier dispatch (pkg/tier), wire encoding (pkg/wire), and
// telemetry (pkg/telemetry) — the full orchestration this repository's
// README documents as Optimize's EVENTUAL scope ("binding cache -> router ->
// wire -> tier -> telemetry") but which pipeline.go does not yet implement
// (today it composes ONLY config+router, per its own doc comment). Silently
// growing Optimize into that full orchestrator to satisfy one cache-wiring
// task would be exactly the "forced wrong-layer wire" this task warns
// against, and would touch pkg/tier/pkg/wire/pkg/telemetry far outside a
// single tightly-scoped change (§11.4.20).
//
// A SECOND, independent reason the cache must NOT be consulted inside
// Optimize's own routing logic: a Decision's correctness depends on the
// `live` predicate's state AT CALL TIME — Optimize's failover branch exists
// precisely because a tier's liveness changes moment to moment. Caching a
// Decision would silently keep routing every subsequent identical request to
// a tier that was live when the entry was written but may be DOWN now,
// defeating the never-downgrade-preserving failover this whole package
// exists to provide. That would be a correctness regression dressed up as an
// optimization (§11.4.6) — never acceptable regardless of how the task is
// phrased.
//
// THE CORRECTLY-SCOPED FIX (per this task's own escape clause: "the correct
// wiring may be a thin CachedOptimize/execution wrapper — implement to the
// design, honestly"). OptimizeCached composes the OPTIONAL *cache.Cache
// installed via SetCache with the EXISTING, UNMODIFIED Optimize and a
// caller-supplied ExecuteFunc — the caller's own dispatch of the routed
// Decision to a tier (pkg/tier) and back — matching the `live` predicate's
// existing decoupling contract exactly (§11.4.28): this package hardcodes no
// dispatch mechanics, no key schema (cacheKey is caller-derived, exactly as
// cache.ArtifactKey leaves file-hashing to its caller), and no cache
// technology (whatever *cache.Cache the caller constructs, including one
// backed by an L2 Store or a CrossProcessLock). It caches ONLY the final,
// already-executed response for an exact request identity — on every cache
// MISS it re-runs the real, unmodified Optimize (fresh liveness, fresh
// floor-preserving failover) exactly as if no cache existed.
package pipeline

import (
	"errors"
	"fmt"
	"time"

	"github.com/vasic-digital/token_optimizer/pkg/cache"
	"github.com/vasic-digital/token_optimizer/pkg/telemetry"
)

// ErrNilExecute is returned by OptimizeCached when handed a nil execute
// function. Without this guard a cache miss would nil-pointer-dereference
// instead of surfacing a caller bug (§11.4.1 — a crash is not an honest error
// path), mirroring ErrNilLiveness's own precedent in pipeline.go.
var ErrNilExecute = errors.New("pipeline: execute must be non-nil")

// ExecuteFunc performs the caller-owned downstream work for a routing
// Decision — dispatching the request to the chosen tier (pkg/tier), decoding
// its response, whatever the caller's own stack requires — and returns the
// value to cache, the TTL to apply (mirroring cache.ComputeFunc's own
// contract exactly: non-positive means never expire), and an error. An error
// is NEVER cached, matching cache.ComputeFunc: OptimizeCached's next
// identical-key call retries for real (§11.4.1 — a poisoned "successful"
// cache entry born from a failed computation is a PASS-bluff at the
// correctness layer).
type ExecuteFunc func(Decision) (value string, ttl time.Duration, err error)

// SetCache installs an OPTIONAL response cache consulted by OptimizeCached
// before routing. Passing nil disables it — the SAME nil-safe,
// no-behavior-change-when-unset contract SetEvidenceRecorder already
// provides: an Optimizer that never calls SetCache, or calls it with nil,
// has OptimizeCached ALWAYS run Optimize then execute, with zero effect from
// this file (see TestOptimizeCached_NoCacheInstalled_AlwaysRoutesAndExecutes).
//
// Plain Optimize is completely unaffected by SetCache in every case: it
// never reads o.cache, so installing (or not installing) a cache changes
// nothing about calling Optimize directly.
func (o *Optimizer) SetCache(c *cache.Cache) {
	o.cache = c
}

// OptimizeCached is the cache-first composition of Optimize
// (config-driven tier selection + floor-preserving failover) and a
// caller-supplied execute step, per DESIGN.md §1's request flow for a
// model-completion request: check the cache FIRST, keyed by cacheKey (a
// caller-derived request identity — this package hardcodes no key schema,
// matching cache.ArtifactKey's own decoupling contract). A HIT returns the
// cached response WITHOUT calling Optimize or execute AT ALL for this call —
// see this file's top-of-file doc comment for why the routing Decision
// itself is never what gets cached. A MISS runs the real, unmodified
// Optimize (fresh liveness, fresh floor-preserving failover — pipeline.go,
// unchanged by this file) and, only if that succeeds, calls execute(decision)
// to obtain the actual response; the result is then cached under cacheKey
// through the cache package's own GetOrCompute, so concurrent OptimizeCached
// calls for the SAME cacheKey coalesce into exactly one Optimize+execute pair
// — the same single-flight stampede guard GetOrCompute already provides (see
// pkg/cache/singleflight.go).
//
// hit reports whether THIS call's routing+execute were skipped (a real cache
// hit, or this call losing an in-flight single-flight race to a concurrent
// identical request — both are a genuine "downstream compute was not run for
// this call" per cache.GetOrCompute's own documented contract). decision is
// the Decision Optimize computed on a miss; on a hit, or on any error before
// Optimize itself returns a Decision, it is the zero value — there is
// nothing to report, since the whole point of a hit is that routing never
// ran.
//
// If no cache was ever installed (SetCache never called, or called with a
// nil Cache), OptimizeCached ALWAYS calls Optimize then execute — see
// TestOptimizeCached_NoCacheInstalled_AlwaysRoutesAndExecutes for the proof
// this degrades to exactly the same effect as composing Optimize+execute by
// hand, with zero cache-layer behavior (cacheKey is not even inspected in
// this branch, matching the nil-safe contract).
func (o *Optimizer) OptimizeCached(cacheKey string, req Request, live func(string) bool, execute ExecuteFunc) (string, Decision, bool, error) {
	if execute == nil {
		return "", Decision{}, false, ErrNilExecute
	}

	if o.cache == nil {
		d, err := o.Optimize(req, live)
		if err != nil {
			return "", Decision{}, false, err
		}
		v, _, err := execute(d)
		if err != nil {
			return "", d, false, err
		}
		return v, d, false, nil
	}

	var computedDecision Decision
	computed := false
	v, err := o.cache.GetOrCompute(cacheKey, func() (string, time.Duration, error) {
		computed = true
		// On a MISS, Optimize runs for real — including this Optimizer's own
		// WS1 savings wiring (pipeline.go's recordSavings), which already
		// emits the routing-decision SavingsRecord for this call. Recording
		// AGAIN here would double-count a single real decision; per the
		// "COMPOSES config and router; duplicates neither" philosophy this
		// whole package follows, the miss path's savings accounting is
		// Optimize's job alone.
		d, oErr := o.Optimize(req, live)
		if oErr != nil {
			return "", 0, oErr
		}
		computedDecision = d
		val, ttl, eErr := execute(d)
		if eErr != nil {
			return "", 0, eErr
		}
		return val, ttl, nil
	})
	if err != nil {
		return "", computedDecision, false, err
	}
	hit := !computed

	// A real cache HIT (or this call losing an in-flight single-flight race
	// to a concurrent identical request — GetOrCompute's own documented
	// contract treats both as "downstream compute was not run for this
	// call") means routing NEVER ran for this call, so Optimize's own wiring
	// above never fired. This IS the "cache hit -> full baseline cost
	// avoided" case pkg/telemetry/savings.go's own SavingsRecord.OptimizedCost
	// doc names explicitly ("Zero means the full baseline cost was avoided
	// (e.g. a cache hit)"): OptimizedCost is exactly 0 (no tier was ever
	// invoked for this call) and BaselineCost is resolved via
	// resolveBaselineCost — the caller-supplied Request.BaselineCost this
	// task's own decoupling contract originally established, UNLESS
	// Request.AutoBaseline is set, in which case it is self-computed from
	// the strongest registered tier (see pipeline.go's Request.AutoBaseline
	// doc comment). Either way it is the SAME resolution Optimize's own
	// routing-path wiring (recordSavings) applies — never re-derived or
	// guessed differently here.
	if hit && o.savings != nil {
		// resolveBaselineCost applies the IDENTICAL AutoBaseline contract as
		// Optimize's own recordSavings — a cache hit must ALSO self-compute
		// when asked, never silently keep the caller-supplied req.BaselineCost
		// as a stale fallback (§11.4.6).
		baseline, bErr := o.resolveBaselineCost(req)
		if bErr != nil {
			return v, computedDecision, hit, fmt.Errorf("pipeline: resolve auto-computed baseline for cache-hit savings record: %w", bErr)
		}
		if recErr := o.savings.Record(telemetry.SavingsRecord{
			Tag:           "cache_hit",
			BaselineCost:  baseline,
			OptimizedCost: 0,
			At:            req.At,
		}); recErr != nil {
			return v, computedDecision, hit, fmt.Errorf("pipeline: emit cache-hit savings record: %w", recErr)
		}
	}
	return v, computedDecision, hit, nil
}
