package pipeline

import (
	"errors"
	"testing"

	"github.com/vasic-digital/token_optimizer/pkg/config"
	"github.com/vasic-digital/token_optimizer/pkg/router"
)

// Tier names used as TEST DATA only. They mirror the WS5 alias-routing ladder
// (cheapest-first) so the never-downgrade intent is reproduced faithfully. None
// appear in the shipped engine — a real consumer registers its own ladder at
// startup (§11.4.28 decoupling).
const (
	t0Cache      = "T0_CACHE"        // 0.00
	t1LocalMicro = "T1_LOCAL_MICRO"  // 0.01
	t2LocalTask  = "T2_LOCAL_TASK"   // 0.10
	t3LocalEmbed = "T3_LOCAL_EMBED"  // 0.15
	t4LocalMed   = "T4_LOCAL_MEDIUM" // 0.30
	t5AliasCheap = "T5_ALIAS_CHEAP"  // 2.00
	t6Native     = "T6_NATIVE"       // 18.00
)

// ladder builds a config whose tiers have strictly increasing combined price
// along the cheapest-first Priority order, so the price-based
// DefaultNeverDowngrade predicate and the Priority-based ordering agree.
// Registered out of order to prove nothing relies on registration order.
func ladder(t *testing.T) *config.Config {
	t.Helper()
	c := config.New()
	tiers := []config.Tier{
		{Name: t6Native, Priority: 6, PricePerMTokIn: 3, PricePerMTokOut: 15},        // 18.00
		{Name: t2LocalTask, Priority: 2, PricePerMTokIn: 0.10},                       // 0.10
		{Name: t0Cache, Priority: 0},                                                 // 0.00
		{Name: t5AliasCheap, Priority: 5, PricePerMTokIn: 0.5, PricePerMTokOut: 1.5}, // 2.00
		{Name: t1LocalMicro, Priority: 1, PricePerMTokIn: 0.01},                      // 0.01
		{Name: t4LocalMed, Priority: 4, PricePerMTokIn: 0.30},                        // 0.30
		{Name: t3LocalEmbed, Priority: 3, PricePerMTokIn: 0.15},                      // 0.15
	}
	for _, tr := range tiers {
		if err := c.RegisterTier(tr); err != nil {
			t.Fatalf("register %q: %v", tr.Name, err)
		}
	}
	return c
}

func newOptimizer(t *testing.T, c *config.Config) *Optimizer {
	t.Helper()
	o, err := New(c)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return o
}

// req is a small helper to build a pipeline.Request from the embedded
// router.Request fields.
func req(min, floor string, loadBearing bool) Request {
	return Request{router.Request{MinTier: min, FloorTier: floor, LoadBearing: loadBearing}}
}

// liveExcept returns a liveness predicate reporting every tier live except the
// named ones (the down set).
func liveExcept(down ...string) func(string) bool {
	set := make(map[string]bool, len(down))
	for _, n := range down {
		set[n] = true
	}
	return func(name string) bool { return !set[name] }
}

// TestOptimizeHappyPathHonorsFloor: when the selected tier is live, Optimize
// returns exactly what router.Select chose (the floor already honored) with
// reason ReasonSelected and no failover.
func TestOptimizeHappyPathHonorsFloor(t *testing.T) {
	o := newOptimizer(t, ladder(t))

	tests := []struct {
		name        string
		req         Request
		wantTier    string
		wantFloored bool
	}{
		{"non-load-bearing base -> min", req(t2LocalTask, "", false), t2LocalTask, false},
		{"non-load-bearing no-min -> cheapest", req("", "", false), t0Cache, false},
		{"load-bearing classify -> strongest T6", req(t1LocalMicro, "", true), t6Native, true},
		{"load-bearing no-min -> strongest T6", req("", "", true), t6Native, true},
		{"load-bearing min already strongest -> T6 not floored", req(t6Native, "", true), t6Native, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := o.Optimize(tc.req, liveExcept()) // everything live
			if err != nil {
				t.Fatalf("Optimize(%+v) = err %v, want nil", tc.req, err)
			}
			if got.Tier.Name != tc.wantTier {
				t.Fatalf("tier = %q, want %q", got.Tier.Name, tc.wantTier)
			}
			if got.Reason != ReasonSelected {
				t.Fatalf("reason = %q, want %q", got.Reason, ReasonSelected)
			}
			if got.FailedOver {
				t.Fatalf("FailedOver = true, want false (selected tier was live)")
			}
			if got.Floored != tc.wantFloored {
				t.Fatalf("Floored = %v, want %v", got.Floored, tc.wantFloored)
			}
			if got.SelectedTier.Name != got.Tier.Name {
				t.Fatalf("SelectedTier %q != Tier %q on non-failover path", got.SelectedTier.Name, got.Tier.Name)
			}
		})
	}
}

// TestOptimizeLoadBearingFailoverPreservesFloor is THE load-bearing-failover
// guarantee this whole module exists to enforce (the W13 design-observation):
// router.Failover walks alternatives by liveness ONLY, so during a primary
// outage it could hand a load-bearing request a WEAKER alternative. The pipeline
// re-applies the never-downgrade floor across failover: a live-but-weaker
// alternative is REFUSED (honest error), a floor-satisfying live alternative IS
// selected. Every sub-case FAILS if the floor re-application is removed.
func TestOptimizeLoadBearingFailoverPreservesFloor(t *testing.T) {
	// Explicit FloorTier=T5, load-bearing -> Select pins to T5 (the W13 minor
	// gap: an explicit floor overrides the implicit strongest-promotion). T5's
	// failover chain is [T4 (weaker, forbidden), T6 (stronger, allowed)] in
	// registration order, so T4 is always visited BEFORE T6 — proving the floor
	// filter, not ordering luck, is what refuses the weaker tier.
	build := func(t *testing.T, alts ...string) *Optimizer {
		c := ladder(t)
		if len(alts) > 0 {
			if err := c.RegisterAlternative(t5AliasCheap, alts...); err != nil {
				t.Fatalf("RegisterAlternative: %v", err)
			}
		}
		return newOptimizer(t, c)
	}
	lbFloorT5 := req(t1LocalMicro, t5AliasCheap, true) // selects T5

	tests := []struct {
		name     string
		alts     []string
		down     []string
		wantTier string // "" => expect refusal
		wantErr  error  // non-nil => expect this error
	}{
		{
			name:    "primary down, only weaker alt live -> REFUSE (no silent downgrade)",
			alts:    []string{t4LocalMed},
			down:    []string{t5AliasCheap},
			wantErr: ErrNoFloorSafeLiveTier,
		},
		{
			name:     "primary down, weaker+stronger both live -> pick stronger, skip weaker",
			alts:     []string{t4LocalMed, t6Native},
			down:     []string{t5AliasCheap},
			wantTier: t6Native,
		},
		{
			name:    "primary down, weaker live but stronger DOWN -> REFUSE",
			alts:    []string{t4LocalMed, t6Native},
			down:    []string{t5AliasCheap, t6Native},
			wantErr: ErrNoFloorSafeLiveTier,
		},
		{
			name:    "primary down, whole chain down -> REFUSE",
			alts:    []string{t4LocalMed, t6Native},
			down:    []string{t5AliasCheap, t4LocalMed, t6Native},
			wantErr: ErrNoFloorSafeLiveTier,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			o := build(t, tc.alts...)
			got, err := o.Optimize(lbFloorT5, liveExcept(tc.down...))
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("Optimize err = %v, want errors.Is %v", err, tc.wantErr)
				}
				if got.Tier.Name != "" {
					t.Fatalf("returned tier %q on refusal, want zero Decision (no silent downgrade below the floor)", got.Tier.Name)
				}
				return
			}
			if err != nil {
				t.Fatalf("Optimize err = %v, want nil", err)
			}
			if got.Tier.Name != tc.wantTier {
				t.Fatalf("tier = %q, want %q", got.Tier.Name, tc.wantTier)
			}
			if !got.FailedOver {
				t.Fatalf("FailedOver = false, want true (primary was down)")
			}
			if got.Reason != ReasonFailoverPreservedFloor {
				t.Fatalf("reason = %q, want %q", got.Reason, ReasonFailoverPreservedFloor)
			}
			if got.SelectedTier.Name != t5AliasCheap {
				t.Fatalf("SelectedTier = %q, want %q (the floor the alternative had to satisfy)", got.SelectedTier.Name, t5AliasCheap)
			}
		})
	}
}

// TestOptimizeEqualPriceAlternativeAllowed proves the floor boundary is a
// STRICT downgrade (cheaper), not "not-strictly-stronger": an alternative of
// EQUAL combined price to the selected tier is NOT a forbidden downgrade and is
// accepted on failover for a load-bearing request.
func TestOptimizeEqualPriceAlternativeAllowed(t *testing.T) {
	c := config.New()
	// T5 and T5B have identical combined price (2.00); T5B is a distinct region.
	for _, tr := range []config.Tier{
		{Name: t5AliasCheap, Priority: 5, PricePerMTokIn: 0.5, PricePerMTokOut: 1.5},      // 2.00
		{Name: "T5B_ALIAS_EQUAL", Priority: 7, PricePerMTokIn: 1.0, PricePerMTokOut: 1.0}, // 2.00
	} {
		if err := c.RegisterTier(tr); err != nil {
			t.Fatalf("register %q: %v", tr.Name, err)
		}
	}
	if err := c.RegisterAlternative(t5AliasCheap, "T5B_ALIAS_EQUAL"); err != nil {
		t.Fatalf("RegisterAlternative: %v", err)
	}
	o := newOptimizer(t, c)

	got, err := o.Optimize(req("", t5AliasCheap, true), liveExcept(t5AliasCheap))
	if err != nil {
		t.Fatalf("Optimize err = %v, want nil (equal-price alt is not a downgrade)", err)
	}
	if got.Tier.Name != "T5B_ALIAS_EQUAL" {
		t.Fatalf("tier = %q, want equal-price alternative T5B_ALIAS_EQUAL", got.Tier.Name)
	}
	if !got.FailedOver {
		t.Fatal("FailedOver = false, want true")
	}
}

// TestOptimizeNonLoadBearingFailoverAcceptsCheaper proves the floor does NOT
// restrict non-load-bearing failover: a cheaper live alternative IS accepted,
// exactly as router.Failover would — the pipeline changes nothing for
// non-load-bearing traffic.
func TestOptimizeNonLoadBearingFailoverAcceptsCheaper(t *testing.T) {
	c := ladder(t)
	if err := c.RegisterAlternative(t5AliasCheap, t4LocalMed); err != nil {
		t.Fatalf("RegisterAlternative: %v", err)
	}
	o := newOptimizer(t, c)

	// Non-load-bearing selected at T5 (explicit MinTier), T5 down, cheaper T4 live.
	got, err := o.Optimize(req(t5AliasCheap, "", false), liveExcept(t5AliasCheap))
	if err != nil {
		t.Fatalf("Optimize err = %v, want nil", err)
	}
	if got.Tier.Name != t4LocalMed {
		t.Fatalf("tier = %q, want %q (cheaper alt allowed for non-load-bearing)", got.Tier.Name, t4LocalMed)
	}
	if !got.FailedOver {
		t.Fatal("FailedOver = false, want true")
	}
}

// TestOptimizeExplicitWeakerFloorOverridesImplicitStrongest exercises the W13
// minor gap directly on the happy path: a load-bearing request with an explicit
// FloorTier weaker than the strongest tier is pinned to that explicit floor
// (T5), NOT promoted to the implicit strongest (T6). The pipeline defers this to
// router.Select — it composes, it does not re-derive the floor.
func TestOptimizeExplicitWeakerFloorOverridesImplicitStrongest(t *testing.T) {
	o := newOptimizer(t, ladder(t))
	got, err := o.Optimize(req(t1LocalMicro, t5AliasCheap, true), liveExcept())
	if err != nil {
		t.Fatalf("Optimize err = %v, want nil", err)
	}
	if got.Tier.Name != t5AliasCheap {
		t.Fatalf("tier = %q, want %q (explicit floor overrides implicit strongest-promotion)", got.Tier.Name, t5AliasCheap)
	}
	if !got.Floored {
		t.Fatal("Floored = false, want true (pinned above MinTier T1)")
	}
}

// TestOptimizeNilConfig / TestOptimizeNilLiveness: loud construction/argument failures.
func TestOptimizeNilConfig(t *testing.T) {
	if _, err := New(nil); !errors.Is(err, ErrNilConfig) {
		t.Fatalf("New(nil) err = %v, want ErrNilConfig", err)
	}
}

func TestOptimizeNilLiveness(t *testing.T) {
	o := newOptimizer(t, ladder(t))
	if _, err := o.Optimize(req("", "", false), nil); !errors.Is(err, ErrNilLiveness) {
		t.Fatalf("Optimize(nil live) err = %v, want ErrNilLiveness", err)
	}
}

// TestOptimizeSelectErrorPropagates: a selection error (unknown tier, no tiers)
// is propagated verbatim so callers classify it exactly as router.Select does.
func TestOptimizeSelectErrorPropagates(t *testing.T) {
	o := newOptimizer(t, ladder(t))
	if _, err := o.Optimize(req("T-BOGUS", "", false), liveExcept()); !errors.Is(err, config.ErrUnknownTier) {
		t.Fatalf("Optimize(unknown min) err = %v, want config.ErrUnknownTier", err)
	}
	empty := newOptimizer(t, config.New())
	if _, err := empty.Optimize(req("", "", false), liveExcept()); !errors.Is(err, router.ErrNoTiers) {
		t.Fatalf("Optimize(no tiers) err = %v, want router.ErrNoTiers", err)
	}
}

// TestOptimizeDeterministic: identical inputs yield identical decisions across
// many invocations (§11.4.50), on both the direct-select and failover paths.
func TestOptimizeDeterministic(t *testing.T) {
	c := ladder(t)
	if err := c.RegisterAlternative(t5AliasCheap, t4LocalMed, t6Native); err != nil {
		t.Fatalf("RegisterAlternative: %v", err)
	}
	o := newOptimizer(t, c)

	cases := []struct {
		req  Request
		live func(string) bool
	}{
		{req(t2LocalTask, "", false), liveExcept()},                       // direct select
		{req("", "", true), liveExcept()},                                 // direct select, load-bearing
		{req(t1LocalMicro, t5AliasCheap, true), liveExcept(t5AliasCheap)}, // failover to T6
	}
	for _, tc := range cases {
		first, err := o.Optimize(tc.req, tc.live)
		if err != nil {
			t.Fatalf("Optimize(%+v): %v", tc.req, err)
		}
		for i := 0; i < 100; i++ {
			got, err := o.Optimize(tc.req, tc.live)
			if err != nil {
				t.Fatalf("Optimize(%+v) iter %d: %v", tc.req, i, err)
			}
			if got != first {
				t.Fatalf("Optimize(%+v) non-deterministic at iter %d: %+v != %+v", tc.req, i, got, first)
			}
		}
	}
}
