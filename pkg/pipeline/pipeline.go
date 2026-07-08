// Package pipeline is the composition point of the token_optimizer engine: it
// binds tier selection (pkg/router) and the decoupling registry (pkg/config)
// into a single request-path decision, and — critically — preserves the
// never-downgrade floor ACROSS FAILOVER.
//
// The router selects the cheapest adequate tier while honoring the floor, and
// router.Failover resolves the first LIVE tier in a primary's alternative
// chain. But router.Failover walks alternatives by liveness ONLY: during a
// primary outage it can hand a load-bearing request an alternative weaker than
// the tier the floor entitled it to, silently violating the engine's headline
// never-downgrade guarantee. The pipeline closes that gap. On a selected-tier
// outage it walks the alternative chain re-applying the floor — an alternative
// that is a forbidden downgrade from the selected tier is refused for a
// load-bearing request, and if no live alternative satisfies the floor the
// pipeline returns an honest error rather than routing below the floor.
//
// The pipeline COMPOSES config and router; it duplicates neither. Selection and
// floor resolution stay in router.Select; the forbidden-downgrade test stays in
// config.IsForbiddenDowngrade (the single point the floor is enforced, so it
// cannot be bypassed); the alternative topology stays in config.Alternatives.
// The pipeline adds only the floor-preserving failover the router deliberately
// left to its composer. It hardcodes no tier name, endpoint, price, threshold,
// or health logic — every project-specific datum flows through *config.Config
// and the consumer's liveness predicate (§11.4.28).
package pipeline

import (
	"errors"
	"fmt"
	"time"

	"github.com/vasic-digital/token_optimizer/pkg/cache"
	"github.com/vasic-digital/token_optimizer/pkg/config"
	"github.com/vasic-digital/token_optimizer/pkg/router"
	"github.com/vasic-digital/token_optimizer/pkg/telemetry"
)

// Pipeline errors are sentinel values so callers classify failures with
// errors.Is.
var (
	// ErrNilConfig is returned by New when handed a nil *config.Config.
	ErrNilConfig = errors.New("pipeline: config must be non-nil")
	// ErrNilLiveness is returned by Optimize when handed a nil liveness
	// predicate: the pipeline cannot detect a selected-tier outage or walk the
	// failover chain without one, and MUST NOT assume every tier is live
	// (§11.4.6).
	ErrNilLiveness = errors.New("pipeline: liveness predicate must be non-nil")
	// ErrNoFloorSafeLiveTier is returned when the selected tier is down and no
	// live alternative satisfies the never-downgrade floor. It is the honest
	// failure that REPLACES a silent downgrade: for a load-bearing request the
	// pipeline refuses a weaker-than-floor alternative rather than quietly
	// violating the never-downgrade guarantee (§11.4.6).
	ErrNoFloorSafeLiveTier = errors.New("pipeline: no live alternative satisfies the never-downgrade floor")
	// ErrNoBaselineTier is returned when Request.AutoBaseline is true but the
	// Optimizer's config has ZERO registered tiers to resolve a baseline price
	// from. An honest failure (§11.4.6): the pipeline never silently falls
	// back to the caller-supplied BaselineCost (which may be a deliberate "no
	// baseline reported" zero, see BaselineCost's own doc comment) nor
	// fabricates a baseline number when there is no tier to price one
	// against. In practice this is only reachable via OptimizeCached's
	// cache-hit path against an Optimizer whose config never had any tier
	// registered — Optimize's own routing path can never reach it, because
	// router.Select itself already fails with router.ErrNoTiers before any
	// savings accounting runs whenever zero tiers are registered.
	ErrNoBaselineTier = errors.New("pipeline: no registered tier to resolve an auto-computed baseline from")
)

// Request is the pipeline's request signal set. It embeds router.Request so the
// same decoupled MinTier / FloorTier / LoadBearing signals drive both selection
// and floor-preserving failover; the pipeline introduces no new routing
// vocabulary of its own.
//
// TaskClass, Tokens, and Cost are the evidence-correlation fields
// router.EvidenceMeta needs whenever an evidence Recorder is installed via
// SetEvidenceRecorder (see evidence emission below). They mirror
// router.EvidenceMeta's own decoupling contract exactly (§11.4.28): opaque to
// this package, consumer-supplied, never inferred, re-derived, or fabricated
// from LoadBearing, MinTier, or any other field (§11.4.6). A caller that never
// installs a Recorder may leave them at their zero value with zero effect on
// routing — Optimize's decision logic never reads them.
type Request struct {
	router.Request

	// TaskClass is the consumer's own task-classification label (e.g.
	// "verdict", "code_small"), forwarded verbatim into the emitted
	// evidence record's task_class field.
	TaskClass string
	// Tokens is the consumer-supplied total token count for the turn this
	// request routes, forwarded verbatim into the emitted evidence record's
	// tokens field. The pipeline counts nothing itself — token accounting is
	// pkg/telemetry's job (WS1).
	Tokens int64
	// Cost is the consumer-supplied USD cost for the turn, from the
	// consumer's own price table, forwarded verbatim into the emitted
	// evidence record's "$" field. The pipeline never computes or guesses a
	// cost. It ALSO doubles, unmodified, as the WS1 savings wiring's
	// telemetry.SavingsRecord.OptimizedCost (see SetSavingsRecorder below) —
	// it already documents itself as exactly "what the request ACTUALLY
	// cost", the same quantity SavingsRecord.OptimizedCost requires.
	Cost float64

	// BaselineCost is the WS1 savings-accounting counterpart to Cost: the
	// consumer-supplied USD cost this EXACT request would have incurred on
	// the project's un-optimized baseline path (e.g. the native / heaviest
	// tier the router would have used with no optimizer present), computed
	// by the consumer's own price table — typically via
	// telemetry.ComputeCost against a pkg/config.Tier the consumer considers
	// its baseline. Forwarded verbatim into the emitted
	// telemetry.SavingsRecord.BaselineCost field (see SetSavingsRecorder).
	//
	// Decoupling (§11.4.28) + honesty (§11.4.6): this field exists because
	// NEITHER router.Decision NOR pipeline.Decision expose a "baseline tier"
	// this package could itself resolve a price from — Optimize only ever
	// returns the tier it actually chose, never "the tier that would have
	// been used absent optimization". Rather than GUESS a baseline (e.g. by
	// assuming the strongest registered tier is always the pre-optimizer
	// default, which is a project-specific policy this decoupled engine has
	// no basis to assume — see pkg/config's own "ships ZERO project
	// constants" contract), this package asks the caller for the baseline
	// cost directly, exactly mirroring Cost's own established
	// caller-supplied-opaque-data contract. A zero value is emitted
	// verbatim (an honest "no baseline reported"), never a fabricated
	// number.
	BaselineCost float64

	// At is the consumer-supplied event timestamp, forwarded verbatim into
	// the emitted telemetry.SavingsRecord.At field. This package never
	// reads a wall clock of its own for savings accounting — At is passed
	// in so aggregation stays deterministic (§11.4.50), mirroring
	// telemetry.Record.At's and telemetry.SavingsRecord.At's own documented
	// contract exactly: a zero At is emitted verbatim, never substituted
	// with time.Now().
	At time.Time

	// AutoBaseline is the WS1 follow-up (ATM-660 continuation) closing the
	// limitation BaselineCost's own doc comment recorded: when true, the
	// pipeline SELF-COMPUTES the emitted SavingsRecord.BaselineCost instead
	// of requiring the caller to supply it, by resolving the STRONGEST
	// registered tier from the Optimizer's *config.Config — tiers[len-1] in
	// the cheapest-first ordering config.Tiers() returns (Priority
	// ascending, ties on Name) — and pricing InputTokens/OutputTokens
	// against it via telemetry.ComputeCost.
	//
	// This is NOT a newly invented, project-specific assumption (§11.4.6):
	// "the strongest registered tier" is the IDENTICAL definition
	// pkg/router/loadbearing.go's resolveFloor already uses for a
	// load-bearing request's implicit never-downgrade floor (absent an
	// explicit FloorTier), and BaselineCost's own doc comment already names
	// "the native / heaviest tier the router would have used with no
	// optimizer present" as exactly what an un-optimized baseline path
	// means — the same quantity, resolved the same way, in one
	// already-established engine-owned place.
	//
	// When false (the zero value — the default), this field has ZERO
	// effect: BaselineCost is forwarded verbatim exactly as it was before
	// this field existed (nil-safe / additive, no behavior change when
	// unused). When true, the self-computed value REPLACES BaselineCost for
	// the SavingsRecord this call emits — BaselineCost itself is ignored
	// for that call, never consulted as a secondary fallback (one
	// deterministic source of truth per call, §11.4.50).
	//
	// Self-computation requires at least one tier registered on the
	// Optimizer's config; see ErrNoBaselineTier for the honest failure mode
	// when none is.
	AutoBaseline bool

	// InputTokens / OutputTokens are the caller's own REAL measured
	// per-channel token counts for this exact request/turn. They exist
	// because telemetry.ComputeCost prices input and output tokens
	// SEPARATELY, and this Request's pre-existing Tokens field is a single
	// COMBINED total that cannot be split into input/output without
	// guessing a ratio — forbidden by §11.4.6 (see savings_wiring_test.go's
	// own "THE HONEST FINDING" comment, which first recorded this exact
	// constraint). These two fields never guess a split: they carry
	// whatever real per-channel counts the caller already measured.
	//
	// Consulted for AutoBaseline's self-derived BaselineCost when
	// AutoBaseline is true (with zero effect whatsoever when false, matching
	// Tokens'/Cost's own opt-in, zero-value-is-inert contract for THAT
	// purpose) — AND, independently of AutoBaseline, forwarded VERBATIM into
	// every emitted telemetry.SavingsRecord.InputTokens/OutputTokens (see
	// recordSavings below + cache.go's cache-hit branch), closing the data-
	// completeness gap SavingsRecord's own pre-existing (until now unwired)
	// InputTokens/OutputTokens fields left open: the emitted forensic
	// record now always carries the request's real measured counts (zero
	// when the caller never measured any — an honest zero, never
	// fabricated), whether or not the caller also opted into AutoBaseline
	// pricing.
	InputTokens  int64
	OutputTokens int64
}

// Optimizer is the engine's single request-path entry point. It is constructed
// from a consumer-populated *config.Config and is safe for concurrent use — the
// context fleet shares one Optimizer over one Config.
type Optimizer struct {
	cfg    *config.Config
	router *router.Router

	// cache is the OPTIONAL WS6 response cache installed via SetCache
	// (cache.go). It is nil unless the consumer explicitly installs one, and
	// plain Optimize NEVER reads it — installing (or not installing) a cache
	// has zero effect on Optimize itself. Only OptimizeCached consults it. See
	// cache.go for the full design rationale (why the cache-first
	// short-circuit lives in a separate composing method rather than inside
	// Optimize's own body).
	cache *cache.Cache

	// savings is the OPTIONAL WS1 $-savings sink installed via
	// SetSavingsRecorder. It is nil unless the consumer explicitly installs
	// one; every Optimize / OptimizeCached call behaves IDENTICALLY to
	// before this field existed when it is unset — the exact nil-safe,
	// no-behavior-change-when-unset contract cache and evidence already
	// established (see SetCache / SetEvidenceRecorder).
	savings *telemetry.SavingsRecorder
}

// New returns an Optimizer bound to cfg. It returns ErrNilConfig if cfg is nil,
// mirroring router.New so a misconfigured startup fails loudly rather than
// routing against nothing.
func New(cfg *config.Config) (*Optimizer, error) {
	if cfg == nil {
		return nil, ErrNilConfig
	}
	r, err := router.New(cfg)
	if err != nil {
		return nil, err
	}
	return &Optimizer{cfg: cfg, router: r}, nil
}

// SetEvidenceRecorder installs rec as this Optimizer's routing-evidence sink:
// every subsequent Optimize call additionally emits one router.Evidence JSONL
// record for the underlying router.Select decision, via the exact
// nil-safe/no-behavior-change-when-unset contract router.Router's own
// SetEvidenceRecorder provides (pkg/router/evidence.go). Passing nil disables
// emission.
//
// The pipeline never constructs its own Recorder, its own Evidence type, or
// its own emission path — it delegates entirely to the *router.Router it
// already composes, so the WS5 DESIGN.md §4 item 3 anti-bluff guarantee
// ("every decide() appends a JSONL line ... a PASS without this line is a
// §11.4 bluff") closes at exactly the layer that calls router.Select: Optimize
// itself, the engine's actual single request-path entry point (§11.4.124 —
// evidence.go's Recorder/Evidence were a correct, unit-tested, but
// UNREACHABLE standalone library from here before this wiring existed).
func (o *Optimizer) SetEvidenceRecorder(rec *router.Recorder) {
	o.router.SetEvidenceRecorder(rec)
}

// SetSavingsRecorder installs rec as this Optimizer's WS1 $-savings sink:
// every subsequent Optimize call that produces a Decision (either directly
// selected or floor-preserving-failed-over) additionally emits ONE
// telemetry.SavingsRecord built from REAL data — the tier the request was
// FINALLY sent to (Decision.Tier.Name, what actually happened, never the
// pre-failover entitlement) plus the caller-measured Request.BaselineCost /
// Request.Cost / Request.At (see their doc comments above for the exact
// decoupling contract each one carries). Passing nil disables emission.
//
// This closes the EXACT §11.4.124 "correct but unreachable" gap
// pkg/telemetry/savings.go's own WS1 R.37 review flagged: ComputeCost,
// SavingsRecord, and SavingsRecorder were a correct, unit-tested, but
// STANDALONE library before this wiring existed — nothing in this engine's
// actual decision path (Optimize, the single request-path entry point real
// consumers call) ever constructed or recorded a SavingsRecord. SetCache and
// SetEvidenceRecorder are the established precedent for exactly this pattern
// (an optional sink an Optimizer composes without owning); this method adds
// the third.
//
// Installing a SavingsRecorder never changes Optimize's ROUTING behavior —
// the returned Decision is byte-for-byte identical in every configuration
// (see TestOptimize_NoSavingsRecorderInstalled_NilSafeNoEmit) — it only
// additionally records the $-savings evidence for the decision that was
// already made.
func (o *Optimizer) SetSavingsRecorder(rec *telemetry.SavingsRecorder) {
	o.savings = rec
}

// recordSavings emits ONE telemetry.SavingsRecord for a just-produced,
// successful Decision when a SavingsRecorder is installed. It is a pure
// no-op (nil, nil) when o.savings is unset — the nil-safe,
// no-behavior-change-when-unset contract every optional sink on this
// Optimizer shares. tag is the tier the request was FINALLY billed against
// (the caller passes Decision.Tier.Name, never SelectedTier — the $ ledger
// must reflect what actually happened, not the pre-failover entitlement).
//
// InputTokens/OutputTokens are forwarded VERBATIM from req into the emitted
// record (never derived, never re-split from the combined Tokens field,
// §11.4.6) — closing the WS1 data-completeness gap the R.37 review flagged:
// SavingsRecord.InputTokens/OutputTokens existed but were left at their Go
// zero value by every construction site until this wiring. A request that
// never measured any tokens honestly records zero; it is never fabricated.
//
// A Record failure (e.g. ErrNegativeCost from a caller price-table bug, or a
// sink I/O error) is returned to the caller rather than silently swallowed —
// matching SelectWithEvidence's own established precedent for the WS5
// evidence-emission failure case (§11.4.6: a fabricated PASS while quietly
// losing the $-savings trail is the exact bluff this wiring exists to
// prevent).
func (o *Optimizer) recordSavings(req Request, tag string) error {
	if o.savings == nil {
		return nil
	}
	baseline, err := o.resolveBaselineCost(req)
	if err != nil {
		return err
	}
	return o.savings.Record(telemetry.SavingsRecord{
		Tag:           tag,
		InputTokens:   req.InputTokens,
		OutputTokens:  req.OutputTokens,
		BaselineCost:  baseline,
		OptimizedCost: req.Cost,
		At:            req.At,
	})
}

// resolveBaselineCost returns the $ figure this Request's emitted
// SavingsRecord.BaselineCost should carry: req.BaselineCost UNCHANGED when
// req.AutoBaseline is false (the pre-existing, unmodified contract — nil-safe
// / zero behavior change when unused, §11.4.6), or a value SELF-COMPUTED from
// the strongest registered tier when req.AutoBaseline is true. See
// Request.AutoBaseline's doc comment for the full rationale (why "the
// strongest tier" is not a guess, and why InputTokens/OutputTokens exist
// instead of reusing the combined Tokens field).
//
// It returns ErrNoBaselineTier when AutoBaseline is true but the config has
// no registered tiers — an honest failure rather than a silent fallback to
// req.BaselineCost or a fabricated zero.
func (o *Optimizer) resolveBaselineCost(req Request) (float64, error) {
	if !req.AutoBaseline {
		return req.BaselineCost, nil
	}
	tiers := o.cfg.Tiers() // cheapest-first, deterministic (Priority asc, then Name)
	if len(tiers) == 0 {
		return 0, ErrNoBaselineTier
	}
	strongest := tiers[len(tiers)-1]
	return telemetry.ComputeCost(req.InputTokens, req.OutputTokens, strongest.PricePerMTokIn, strongest.PricePerMTokOut), nil
}

// Optimize resolves the tier a request will actually be sent to. It first
// selects the cheapest adequate tier while honoring the never-downgrade floor
// (router.Select). If that tier is live it is used directly. If it is down the
// pipeline fails over to the first live alternative that is NOT a forbidden
// downgrade from the selected tier — preserving the floor across failover — and
// returns ErrNoFloorSafeLiveTier if none qualifies rather than silently routing
// below the floor.
//
// live is the CONSUMER's liveness predicate (an endpoint health probe, a
// circuit-breaker state, a cached ping) — the same one production uses; the
// pipeline hardcodes no health logic (§11.4.28). A nil live returns
// ErrNilLiveness. A selection error from router.Select (unknown tier, no
// registered tiers) is propagated verbatim so the caller classifies it with
// errors.Is exactly as router.Select intends.
func (o *Optimizer) Optimize(req Request, live func(name string) bool) (Decision, error) {
	if live == nil {
		return Decision{}, ErrNilLiveness
	}

	// SelectWithEvidence's decision LOGIC is byte-for-byte identical to bare
	// Select's in every configuration (see pkg/router/evidence.go's own
	// no-behavior-change-when-unset contract) — this call adds nothing to and
	// removes nothing from tier selection. The only behavioural difference is
	// OPT-IN: when a Recorder is installed via SetEvidenceRecorder, this call
	// additionally emits ONE router.Evidence JSONL record built from meta's
	// three consumer-supplied correlation fields (ReqHash from req.ID —
	// already-real request data, never invented here — plus req.TaskClass /
	// req.Tokens / req.Cost, forwarded verbatim per §11.4.6/§11.4.28). meta is
	// used unconditionally: constructing it is free (four field reads) and
	// SelectWithEvidence itself is the nil-recorder no-op path when no
	// Recorder was ever installed.
	meta := router.EvidenceMeta{
		ReqHash:   req.ID,
		TaskClass: req.TaskClass,
		Tokens:    req.Tokens,
		Cost:      req.Cost,
	}
	sel, err := o.router.SelectWithEvidence(req.Request, meta)
	if err != nil {
		return Decision{}, err
	}

	// Selected tier is live: use it directly. Its floor was already honored by
	// router.Select, so no re-application is needed here.
	if live(sel.Tier.Name) {
		d := Decision{
			Tier:         sel.Tier,
			SelectedTier: sel.Tier,
			Reason:       ReasonSelected,
			LoadBearing:  req.LoadBearing,
			Floored:      sel.Floored,
			FailedOver:   false,
		}
		if err := o.recordSavings(req, d.Tier.Name); err != nil {
			return d, fmt.Errorf("pipeline: emit savings record: %w", err)
		}
		return d, nil
	}

	// Selected tier is down. Fail over WHILE PRESERVING THE FLOOR: the selected
	// tier is the request's entitlement, so any alternative that is a forbidden
	// downgrade from it is refused for a load-bearing request. This is the
	// floor-preserving variant of router.Failover the design deliberately left
	// to the pipeline — router.Failover walks by liveness only and would return
	// the first live (possibly weaker) alternative, silently breaching the
	// never-downgrade guarantee.
	for _, altName := range o.cfg.Alternatives(sel.Tier.Name) {
		altTier, ok := o.cfg.Tier(altName)
		if !ok {
			// config guarantees registered alternatives; skip defensively rather
			// than trust an unregistered name (§11.4.6).
			continue
		}
		if o.cfg.IsForbiddenDowngrade(sel.Tier, altTier, req.LoadBearing) {
			// Weaker than the floor — never hand it to a load-bearing request.
			continue
		}
		if !live(altName) {
			continue
		}
		d := Decision{
			Tier:         altTier,
			SelectedTier: sel.Tier,
			Reason:       ReasonFailoverPreservedFloor,
			LoadBearing:  req.LoadBearing,
			Floored:      sel.Floored,
			FailedOver:   true,
		}
		if err := o.recordSavings(req, d.Tier.Name); err != nil {
			return d, fmt.Errorf("pipeline: emit savings record: %w", err)
		}
		return d, nil
	}

	// The selected tier is down and no live alternative satisfies the floor:
	// return the honest error instead of routing below the floor (§11.4.6).
	return Decision{}, fmt.Errorf("%w: selected tier %q down", ErrNoFloorSafeLiveTier, sel.Tier.Name)
}
