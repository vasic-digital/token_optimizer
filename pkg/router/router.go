// Package router selects the cheapest adequate completion tier for a request
// while enforcing the never-downgrade floor.
//
// The router is fully decoupled from any project. It operates only over the
// tiers, alternatives, and NeverDowngrade predicate that a consumer registered
// in a *config.Config at startup. It hardcodes no tier name, endpoint, price,
// task class, threshold, or region — every project-specific datum flows through
// the config surface (see the decoupling contract in pkg/config). Two consumers
// with completely different tier ladders share this exact selection logic.
//
// Selection is deterministic (§11.4.50): tiers are consulted in the stable
// cheapest-first order config.Tiers provides (Priority ascending, ties broken
// on Name), and the never-downgrade decision is delegated to the single
// config.IsForbiddenDowngrade point so the floor can never be silently bypassed.
package router

import (
	"errors"
	"fmt"

	"github.com/vasic-digital/token_optimizer/pkg/config"
)

// Router errors. They are sentinel values so callers can classify failures with
// errors.Is. Unknown-tier references reuse config.ErrUnknownTier so a consumer
// classifies "you named a tier that does not exist" the same way everywhere.
var (
	// ErrNilConfig is returned by New when handed a nil *config.Config.
	ErrNilConfig = errors.New("router: config must be non-nil")
	// ErrNoTiers is returned by Select when the config has no registered tiers.
	ErrNoTiers = errors.New("router: no tiers registered")
	// ErrNoSelectableTier is returned by Select when the active floor predicate
	// forbids every candidate at or above the request's minimum adequacy. It is
	// an honest failure (§11.4.6) — the router never falls back to a silently
	// wrong tier.
	ErrNoSelectableTier = errors.New("router: no tier satisfies the never-downgrade floor")
)

// Selection reasons. They are stable, greppable captured-evidence tokens
// (§11.4.69) recorded on every Decision — never free-form prose.
const (
	// ReasonMinAdequacy means the request's minimum-adequacy tier was itself
	// selected: no floor forced the selection higher.
	ReasonMinAdequacy = "min_adequacy"
	// ReasonNeverDowngradeFloor means the never-downgrade floor pinned the
	// selection strictly above the request's minimum-adequacy tier.
	ReasonNeverDowngradeFloor = "never_downgrade_floor"
)

// Request is a decoupled routing signal set. The consumer maps its own task
// classes, context sizes, capability flags, and difficulty estimates onto these
// fields (the WS5 TASKCLASS_BASE table plus clamps live in the CONSUMER, not the
// engine); the router hardcodes none of that vocabulary.
type Request struct {
	// ID is an optional opaque request identifier for telemetry correlation.
	// The router never interprets it.
	ID string
	// MinTier is the stable name of the minimum-adequacy tier the consumer has
	// already computed for this request's signals. The router never selects a
	// tier weaker (cheaper, earlier in cheapest-first order) than this one. An
	// empty MinTier means "no minimum" — the cheapest registered tier is
	// adequate.
	MinTier string
	// FloorTier is the stable name of an explicit never-downgrade floor: the
	// router refuses any candidate the config predicate deems a forbidden
	// downgrade relative to this tier. An empty FloorTier with LoadBearing true
	// makes the strongest registered tier the implicit floor.
	FloorTier string
	// LoadBearing marks a request that must not be downgraded. It is passed
	// verbatim to config.IsForbiddenDowngrade, and, absent an explicit
	// FloorTier, promotes the strongest registered tier to the floor.
	LoadBearing bool
}

// Decision is the result of Select: the chosen tier plus captured-evidence
// metadata (§11.4.69) proving why it was chosen.
type Decision struct {
	// Tier is the selected completion tier.
	Tier config.Tier
	// Reason is one of the Reason* constants.
	Reason string
	// LoadBearing echoes the request's load-bearing flag.
	LoadBearing bool
	// Floored is true when the never-downgrade floor pinned the selection
	// strictly above the request's minimum-adequacy tier.
	Floored bool
}

// Router selects tiers over a consumer-populated *config.Config.
type Router struct {
	cfg *config.Config
	// evidence is the optional routing-evidence sink SelectWithEvidence
	// writes to (evidence.go). It is nil by default: a Router constructed by
	// New never emits evidence and behaves exactly as it did before evidence
	// wiring existed. See SetEvidenceRecorder + SelectWithEvidence.
	evidence *Recorder
}

// New returns a Router bound to cfg. It returns ErrNilConfig if cfg is nil so a
// misconfigured startup fails loudly rather than routing against nothing.
func New(cfg *config.Config) (*Router, error) {
	if cfg == nil {
		return nil, ErrNilConfig
	}
	return &Router{cfg: cfg}, nil
}

// Select returns the cheapest tier that is (a) at least as strong as the
// request's minimum-adequacy tier AND (b) not a forbidden downgrade from the
// active floor. It walks the registered tiers in config.Tiers cheapest-first
// order and returns the first candidate satisfying both constraints.
//
// The never-downgrade floor is HARD: a request pinned to a floor never resolves
// to a tier below it. When the config's default predicate is in force, this
// pins every load-bearing request to the strongest tier (the WS5 hard floor);
// when a consumer installs a stricter predicate via config.SetNeverDowngrade,
// the router honors it because the forbidden-downgrade test is delegated to the
// single config.IsForbiddenDowngrade point.
func (r *Router) Select(req Request) (Decision, error) {
	tiers := r.cfg.Tiers() // cheapest-first, deterministic (Priority asc, then Name)
	if len(tiers) == 0 {
		return Decision{}, ErrNoTiers
	}

	index := make(map[string]int, len(tiers))
	for i, t := range tiers {
		index[t.Name] = i
	}

	minIdx := 0
	if req.MinTier != "" {
		i, ok := index[req.MinTier]
		if !ok {
			return Decision{}, fmt.Errorf("%w: min tier %q", config.ErrUnknownTier, req.MinTier)
		}
		minIdx = i
	}

	floor, floorSet, err := r.resolveFloor(req, tiers, index)
	if err != nil {
		return Decision{}, err
	}

	// Walk from the minimum-adequacy tier upward (never below it), skipping any
	// candidate the floor forbids, and return the first that survives.
	for i := minIdx; i < len(tiers); i++ {
		cand := tiers[i]
		if floorSet && r.cfg.IsForbiddenDowngrade(floor, cand, req.LoadBearing) {
			continue
		}
		reason := ReasonMinAdequacy
		floored := false
		if i > minIdx {
			reason = ReasonNeverDowngradeFloor
			floored = true
		}
		return Decision{
			Tier:        cand,
			Reason:      reason,
			LoadBearing: req.LoadBearing,
			Floored:     floored,
		}, nil
	}

	return Decision{}, ErrNoSelectableTier
}
