package router

import (
	"errors"
	"fmt"

	"github.com/vasic-digital/token_optimizer/pkg/config"
)

// ErrNoLiveTier is returned by Failover when neither the primary tier nor any
// registered alternative is reported live.
var ErrNoLiveTier = errors.New("router: no live tier in the failover chain")

// Failover-decision reasons (captured evidence, §11.4.69).
const (
	// ReasonPrimaryLive means the primary tier was live and returned unchanged.
	ReasonPrimaryLive = "primary_live"
	// ReasonFailoverAlternative means a registered alternative was returned
	// because the primary was down.
	ReasonFailoverAlternative = "failover_alternative"
)

// Failover resolves the first live tier in a primary's registered fallback
// chain. It consults the primary first, then each registered alternative (in
// config.RegisterAlternative order), returning the first for which live reports
// true.
//
// The liveness decision is the CONSUMER's — live is the same predicate the
// consumer uses in production (an endpoint health probe, a circuit-breaker
// state, a cached ping result). The router hardcodes no health logic, so it
// stays decoupled (§11.4.28); it only knows the alternative topology the
// consumer registered.
//
// It returns config.ErrUnknownTier if primary is not registered, an error if
// live is nil, and ErrNoLiveTier if the whole chain is down.
func (r *Router) Failover(primary string, live func(name string) bool) (config.Tier, string, error) {
	if live == nil {
		return config.Tier{}, "", errors.New("router: liveness predicate must be non-nil")
	}
	primaryTier, ok := r.cfg.Tier(primary)
	if !ok {
		return config.Tier{}, "", fmt.Errorf("%w: %q", config.ErrUnknownTier, primary)
	}
	if live(primary) {
		return primaryTier, ReasonPrimaryLive, nil
	}
	for _, alt := range r.cfg.Alternatives(primary) {
		altTier, ok := r.cfg.Tier(alt)
		if !ok {
			// config guarantees alternatives are registered; skip defensively
			// rather than trust an unregistered name (§11.4.6).
			continue
		}
		if live(alt) {
			return altTier, ReasonFailoverAlternative, nil
		}
	}
	return config.Tier{}, "", ErrNoLiveTier
}
