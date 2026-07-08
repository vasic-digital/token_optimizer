package tier

import (
	"context"
	"errors"
	"fmt"
)

// Chain-dispatch errors. Sentinel values for errors.Is classification.
var (
	// ErrEmptyChain is returned by DispatchChain when the tier order is empty:
	// there is nothing to try, and returning a fabricated success would be a
	// bluff (§11.4.6).
	ErrEmptyChain = errors.New("tier: dispatch chain must be non-empty")
	// ErrChainExhausted is returned by DispatchChain when every tier in the
	// order failed (unregistered or its executor errored). It wraps the joined
	// per-tier causes (errors.Join) so the caller can errors.Is each one.
	ErrChainExhausted = errors.New("tier: no tier in the dispatch chain succeeded")
)

// DispatchChain tries the executors for the tiers in order and returns the
// response of the FIRST that succeeds. It is the consumer-explicit fallback
// path: the caller supplies the exact ordered tier list, so the framework
// performs NO auto-downgrade and NO reordering — it walks order as given and
// stops at the first success.
//
// The never-downgrade floor (§11.4.111 / pkg/config's NeverDowngrade) is the
// CALLER's responsibility: the caller MUST have already filtered and ordered the
// chain so it contains no forbidden downgrade for the request (exactly as
// pipeline.Optimize builds its floor-safe alternative list). This package holds
// no floor policy and never drops below one on its own; it simply honors the
// order it is given. Passing an unfiltered chain is a caller error, not a
// silently-tolerated downgrade.
//
// For each tier in order: a tier with no registered executor contributes an
// ErrNoExecutorForTier cause and the walk continues; a tier whose executor
// errors contributes that error and the walk continues; a tier whose executor
// succeeds ends the walk and its response is returned. If the order is empty,
// ErrEmptyChain is returned. If every tier fails, ErrChainExhausted is returned
// wrapping every per-tier cause via errors.Join — nothing is swallowed and no
// response is fabricated (§11.4.6).
func (r *Registry) DispatchChain(ctx context.Context, order []string, req Request) (Response, error) {
	if len(order) == 0 {
		return Response{}, ErrEmptyChain
	}
	causes := make([]error, 0, len(order))
	for _, tier := range order {
		resp, err := r.Dispatch(ctx, tier, req)
		if err != nil {
			causes = append(causes, err)
			continue
		}
		return resp, nil
	}
	return Response{}, fmt.Errorf("%w: %w", ErrChainExhausted, errors.Join(causes...))
}
