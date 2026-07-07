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

	"github.com/vasic-digital/token_optimizer/pkg/config"
	"github.com/vasic-digital/token_optimizer/pkg/router"
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
)

// Request is the pipeline's request signal set. It embeds router.Request so the
// same decoupled MinTier / FloorTier / LoadBearing signals drive both selection
// and floor-preserving failover; the pipeline introduces no new routing
// vocabulary of its own.
type Request struct {
	router.Request
}

// Optimizer is the engine's single request-path entry point. It is constructed
// from a consumer-populated *config.Config and is safe for concurrent use — the
// context fleet shares one Optimizer over one Config.
type Optimizer struct {
	cfg    *config.Config
	router *router.Router
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

	sel, err := o.router.Select(req.Request)
	if err != nil {
		return Decision{}, err
	}

	// Selected tier is live: use it directly. Its floor was already honored by
	// router.Select, so no re-application is needed here.
	if live(sel.Tier.Name) {
		return Decision{
			Tier:         sel.Tier,
			SelectedTier: sel.Tier,
			Reason:       ReasonSelected,
			LoadBearing:  req.LoadBearing,
			Floored:      sel.Floored,
			FailedOver:   false,
		}, nil
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
		return Decision{
			Tier:         altTier,
			SelectedTier: sel.Tier,
			Reason:       ReasonFailoverPreservedFloor,
			LoadBearing:  req.LoadBearing,
			Floored:      sel.Floored,
			FailedOver:   true,
		}, nil
	}

	// The selected tier is down and no live alternative satisfies the floor:
	// return the honest error instead of routing below the floor (§11.4.6).
	return Decision{}, fmt.Errorf("%w: selected tier %q down", ErrNoFloorSafeLiveTier, sel.Tier.Name)
}
