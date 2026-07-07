package pipeline

import "github.com/vasic-digital/token_optimizer/pkg/config"

// Decision reasons are stable, greppable captured-evidence tokens (§11.4.69)
// recorded on every Decision — never free-form prose. A telemetry sink can
// filter on them to distinguish a clean selection from a floor-preserving
// failover.
const (
	// ReasonSelected means the router-selected tier was live and used
	// directly: no failover was needed.
	ReasonSelected = "selected"
	// ReasonFailoverPreservedFloor means the selected tier was down and the
	// pipeline failed over to a live alternative that is NOT a forbidden
	// downgrade from the selected tier — the never-downgrade floor was
	// preserved across the failover.
	ReasonFailoverPreservedFloor = "failover_floor_preserved"
)

// Decision is the pipeline's end-to-end result: the tier the request will
// actually be sent to, plus captured-evidence metadata (§11.4.69) proving how
// the never-downgrade floor was honored on selection AND, when a failover
// occurred, preserved across it.
type Decision struct {
	// Tier is the tier the request will be sent to (the selected tier when it
	// is live, or the floor-satisfying live alternative when it is down).
	Tier config.Tier
	// SelectedTier is the tier router.Select chose while honoring the
	// never-downgrade floor. When FailedOver is false it equals Tier; when
	// FailedOver is true it is the floor the failover target had to satisfy —
	// the evidence that failover did not route below the entitlement.
	SelectedTier config.Tier
	// Reason is one of the Reason* constants.
	Reason string
	// LoadBearing echoes the request's load-bearing flag.
	LoadBearing bool
	// Floored echoes router.Decision.Floored: the never-downgrade floor pinned
	// the selection strictly above the request's minimum-adequacy tier.
	Floored bool
	// FailedOver is true when the selected tier was down and the request was
	// routed to a floor-satisfying live alternative instead.
	FailedOver bool
}
