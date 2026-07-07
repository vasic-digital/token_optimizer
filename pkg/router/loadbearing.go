package router

import (
	"fmt"

	"github.com/vasic-digital/token_optimizer/pkg/config"
)

// resolveFloor determines the never-downgrade floor for a request.
//
// Precedence (§11.4.6 — no guessing, every branch is explicit):
//
//  1. An explicit Request.FloorTier is the floor. It MUST name a registered
//     tier; an unknown name fails loudly with config.ErrUnknownTier rather than
//     silently disabling the floor.
//  2. Otherwise, a load-bearing request defaults its floor to the STRONGEST
//     registered tier — the last entry in the cheapest-first ordering. This is
//     the decoupled expression of the WS5 hard floor ("a must-not-downgrade
//     request routes to the strongest available tier") with no tier name baked
//     into the engine.
//  3. Otherwise there is no floor: the minimum-adequacy tier is selected as-is.
//
// The returned bool reports whether a floor is in force. tiers is the
// cheapest-first slice from config.Tiers and index maps tier name to its
// position within it.
func (r *Router) resolveFloor(req Request, tiers []config.Tier, index map[string]int) (config.Tier, bool, error) {
	if req.FloorTier != "" {
		i, ok := index[req.FloorTier]
		if !ok {
			return config.Tier{}, false, fmt.Errorf("%w: floor tier %q", config.ErrUnknownTier, req.FloorTier)
		}
		return tiers[i], true, nil
	}
	if req.LoadBearing {
		// Strongest registered tier = last in cheapest-first order.
		return tiers[len(tiers)-1], true, nil
	}
	return config.Tier{}, false, nil
}
