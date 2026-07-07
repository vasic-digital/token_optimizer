package router

import (
	"errors"
	"testing"

	"github.com/vasic-digital/token_optimizer/pkg/config"
)

// Tier names used as TEST DATA only. They mirror the WS5 alias-routing ladder
// (ws5_alias_routing/POC/decide.sh: T0_CACHE .. T6_NATIVE, cheapest-first) so
// the truth-table intent is reproduced faithfully. None of these names appear
// in the shipped engine — they are registered here exactly as a real consumer
// would register its own ladder at startup (§11.4.28 decoupling).
const (
	t0Cache      = "T0_CACHE"
	t1LocalMicro = "T1_LOCAL_MICRO"
	t2LocalTask  = "T2_LOCAL_TASK"
	t3LocalEmbed = "T3_LOCAL_EMBED"
	t4LocalMed   = "T4_LOCAL_MEDIUM"
	t5AliasCheap = "T5_ALIAS_CHEAP"
	t6Native     = "T6_NATIVE"
)

// ladder builds a config whose registered tiers have strictly increasing
// combined price along the cheapest-first Priority order, so the price-based
// DefaultNeverDowngrade predicate and the Priority-based cheapest-first ordering
// agree. Registered out of order to prove Select relies on config.Tiers'
// deterministic ordering, not registration order.
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

func newRouter(t *testing.T, c *config.Config) *Router {
	t.Helper()
	r, err := New(c)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return r
}

// TestSelectTruthTable reproduces the WS5 truth-table intent (decide.sh +
// truth_table_test.sh) against the decoupled router:
//
//	A. non-load-bearing: the consumer-computed minimum-adequacy tier is selected.
//	B. clamp simulation: when the consumer raises MinTier (its clamp result), the
//	   router honors the raised floor.
//	C. THE HARD FLOOR: a load-bearing request ALWAYS resolves to the strongest
//	   tier regardless of MinTier (the WS5 §C invariant), whether via the implicit
//	   load-bearing floor or an explicit FloorTier.
func TestSelectTruthTable(t *testing.T) {
	r := newRouter(t, ladder(t))

	tests := []struct {
		name        string
		req         Request
		wantTier    string
		wantReason  string
		wantFloored bool
	}{
		// --- A. non-load-bearing base-tier selection (DESIGN.md §2 TASKCLASS_BASE) ---
		{"classify base -> T1", Request{MinTier: t1LocalMicro}, t1LocalMicro, ReasonMinAdequacy, false},
		{"extract_flat base -> T2", Request{MinTier: t2LocalTask}, t2LocalTask, ReasonMinAdequacy, false},
		{"embed base -> T3", Request{MinTier: t3LocalEmbed}, t3LocalEmbed, ReasonMinAdequacy, false},
		{"code_small base -> T4", Request{MinTier: t4LocalMed}, t4LocalMed, ReasonMinAdequacy, false},
		{"code_agentic base -> T5", Request{MinTier: t5AliasCheap}, t5AliasCheap, ReasonMinAdequacy, false},
		{"no min -> cheapest T0", Request{}, t0Cache, ReasonMinAdequacy, false},

		// --- B. clamp simulation: consumer clamped MinTier upward -> router honors it ---
		{"classify + long-ctx clamp -> consumer raised MinTier to T5", Request{MinTier: t5AliasCheap}, t5AliasCheap, ReasonMinAdequacy, false},
		{"extract + difficulty clamp -> consumer raised MinTier to T4", Request{MinTier: t4LocalMed}, t4LocalMed, ReasonMinAdequacy, false},

		// --- C. THE HARD FLOOR: load-bearing ALWAYS -> strongest tier, regardless of MinTier ---
		{"load-bearing + classify -> T6 (never T1)", Request{MinTier: t1LocalMicro, LoadBearing: true}, t6Native, ReasonNeverDowngradeFloor, true},
		{"load-bearing + extract_flat -> T6 (never T2)", Request{MinTier: t2LocalTask, LoadBearing: true}, t6Native, ReasonNeverDowngradeFloor, true},
		{"load-bearing + code_small -> T6 (never T4)", Request{MinTier: t4LocalMed, LoadBearing: true}, t6Native, ReasonNeverDowngradeFloor, true},
		{"load-bearing + no min -> T6 (never T0)", Request{LoadBearing: true}, t6Native, ReasonNeverDowngradeFloor, true},
		{"load-bearing + explicit floor T6 -> T6", Request{MinTier: t1LocalMicro, FloorTier: t6Native, LoadBearing: true}, t6Native, ReasonNeverDowngradeFloor, true},
		{"load-bearing + min already at strongest -> T6, not floored", Request{MinTier: t6Native, LoadBearing: true}, t6Native, ReasonMinAdequacy, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := r.Select(tc.req)
			if err != nil {
				t.Fatalf("Select(%+v) = err %v, want nil", tc.req, err)
			}
			if got.Tier.Name != tc.wantTier {
				t.Fatalf("Select(%+v) tier = %q, want %q", tc.req, got.Tier.Name, tc.wantTier)
			}
			if got.Reason != tc.wantReason {
				t.Fatalf("Select(%+v) reason = %q, want %q", tc.req, got.Reason, tc.wantReason)
			}
			if got.Floored != tc.wantFloored {
				t.Fatalf("Select(%+v) floored = %v, want %v", tc.req, got.Floored, tc.wantFloored)
			}
			if got.LoadBearing != tc.req.LoadBearing {
				t.Fatalf("Select(%+v) loadBearing = %v, want %v", tc.req, got.LoadBearing, tc.req.LoadBearing)
			}
		})
	}
}

// TestSelectNeverDowngradeFloorPinsAboveMin is the load-bearing-floor guarantee:
// a request whose minimum-adequacy tier is the cheapest tier, but which carries
// an explicit stronger floor, MUST be pinned to the floor — it never routes
// below it. This is the never-downgrade boundary the whole engine exists to
// protect (§11.4.6).
func TestSelectNeverDowngradeFloorPinsAboveMin(t *testing.T) {
	r := newRouter(t, ladder(t))

	req := Request{MinTier: t0Cache, FloorTier: t5AliasCheap, LoadBearing: true}
	got, err := r.Select(req)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if got.Tier.Name != t5AliasCheap {
		t.Fatalf("floor not honored: selected %q, want %q (must never route below the floor)", got.Tier.Name, t5AliasCheap)
	}
	if !got.Floored {
		t.Fatal("Floored = false, want true (selection was pinned above the minimum-adequacy tier)")
	}
	if got.Reason != ReasonNeverDowngradeFloor {
		t.Fatalf("reason = %q, want %q", got.Reason, ReasonNeverDowngradeFloor)
	}
}

// TestSelectFloorBelowMinTierMinDominates verifies the floor never WEAKENS the
// selection: when the explicit floor is cheaper than the minimum-adequacy tier,
// the minimum still wins.
func TestSelectFloorBelowMinTierMinDominates(t *testing.T) {
	r := newRouter(t, ladder(t))

	got, err := r.Select(Request{MinTier: t4LocalMed, FloorTier: t1LocalMicro})
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if got.Tier.Name != t4LocalMed {
		t.Fatalf("selected %q, want %q (min-adequacy must dominate a weaker floor)", got.Tier.Name, t4LocalMed)
	}
	if got.Floored {
		t.Fatal("Floored = true, want false (min already at/above the floor)")
	}
}

// TestSelectCustomNeverDowngradePredicate proves the router delegates the
// forbidden-downgrade decision to config.IsForbiddenDowngrade rather than
// hardcoding a price rule: a consumer policy that forbids ALL downgrades (even
// non-load-bearing) pins a non-load-bearing request to its explicit floor.
func TestSelectCustomNeverDowngradePredicate(t *testing.T) {
	c := ladder(t)
	c.SetNeverDowngrade(func(current, candidate config.Tier, _ bool) bool {
		return candidate.CombinedPrice() < current.CombinedPrice()
	})
	r := newRouter(t, c)

	// Non-load-bearing, but the custom predicate forbids downgrades below T5.
	got, err := r.Select(Request{MinTier: t0Cache, FloorTier: t5AliasCheap, LoadBearing: false})
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if got.Tier.Name != t5AliasCheap {
		t.Fatalf("custom predicate not consulted: selected %q, want %q", got.Tier.Name, t5AliasCheap)
	}
}

// TestSelectUnknownTierFailsLoud reproduces WS5 §D: an unknown tier reference
// fails loudly (config.ErrUnknownTier) and never silently defaults to a tier.
func TestSelectUnknownTierFailsLoud(t *testing.T) {
	r := newRouter(t, ladder(t))

	tests := []struct {
		name string
		req  Request
	}{
		{"unknown min tier", Request{MinTier: "T-BOGUS"}},
		{"unknown floor tier", Request{FloorTier: "T-GHOST"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := r.Select(tc.req)
			if !errors.Is(err, config.ErrUnknownTier) {
				t.Fatalf("Select(%+v) err = %v, want errors.Is config.ErrUnknownTier", tc.req, err)
			}
			if got.Tier.Name != "" {
				t.Fatalf("Select(%+v) returned tier %q on error, want zero Decision (no silent default)", tc.req, got.Tier.Name)
			}
		})
	}
}

// TestSelectNoTiers: an empty config fails loudly with ErrNoTiers.
func TestSelectNoTiers(t *testing.T) {
	r := newRouter(t, config.New())
	if _, err := r.Select(Request{}); !errors.Is(err, ErrNoTiers) {
		t.Fatalf("Select on empty config err = %v, want ErrNoTiers", err)
	}
}

// TestSelectDeterministic: identical requests yield identical decisions across
// many invocations (§11.4.50), including through the deterministic config.Tiers
// ordering.
func TestSelectDeterministic(t *testing.T) {
	r := newRouter(t, ladder(t))
	reqs := []Request{
		{MinTier: t2LocalTask},
		{MinTier: t1LocalMicro, LoadBearing: true},
		{MinTier: t0Cache, FloorTier: t5AliasCheap, LoadBearing: true},
		{},
	}
	for _, req := range reqs {
		first, err := r.Select(req)
		if err != nil {
			t.Fatalf("Select(%+v): %v", req, err)
		}
		for i := 0; i < 100; i++ {
			got, err := r.Select(req)
			if err != nil {
				t.Fatalf("Select(%+v) iter %d: %v", req, i, err)
			}
			if got != first {
				t.Fatalf("Select(%+v) non-deterministic at iter %d: %+v != %+v", req, i, got, first)
			}
		}
	}
}

func TestNewNilConfig(t *testing.T) {
	if _, err := New(nil); !errors.Is(err, ErrNilConfig) {
		t.Fatalf("New(nil) err = %v, want ErrNilConfig", err)
	}
}

// TestFailover exercises the decoupled failover chain over config.Alternatives.
func TestFailover(t *testing.T) {
	c := ladder(t)
	// T5 is primary; fall back to T4 then T6 (registration order preserved).
	if err := c.RegisterAlternative(t5AliasCheap, t4LocalMed, t6Native); err != nil {
		t.Fatalf("RegisterAlternative: %v", err)
	}
	r := newRouter(t, c)

	tests := []struct {
		name       string
		primary    string
		down       map[string]bool // names reported NOT live
		wantTier   string
		wantReason string
		wantErr    error
	}{
		{"primary live", t5AliasCheap, nil, t5AliasCheap, ReasonPrimaryLive, nil},
		{"primary down -> first live alt T4", t5AliasCheap, map[string]bool{t5AliasCheap: true}, t4LocalMed, ReasonFailoverAlternative, nil},
		{"primary+first-alt down -> T6", t5AliasCheap, map[string]bool{t5AliasCheap: true, t4LocalMed: true}, t6Native, ReasonFailoverAlternative, nil},
		{"whole chain down", t5AliasCheap, map[string]bool{t5AliasCheap: true, t4LocalMed: true, t6Native: true}, "", "", ErrNoLiveTier},
		{"unknown primary", "T-NOPE", nil, "", "", config.ErrUnknownTier},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			live := func(name string) bool { return !tc.down[name] }
			gotTier, gotReason, err := r.Failover(tc.primary, live)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("Failover err = %v, want errors.Is %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Failover err = %v, want nil", err)
			}
			if gotTier.Name != tc.wantTier {
				t.Fatalf("Failover tier = %q, want %q", gotTier.Name, tc.wantTier)
			}
			if gotReason != tc.wantReason {
				t.Fatalf("Failover reason = %q, want %q", gotReason, tc.wantReason)
			}
		})
	}
}

func TestFailoverNilPredicate(t *testing.T) {
	r := newRouter(t, ladder(t))
	if _, _, err := r.Failover(t5AliasCheap, nil); err == nil {
		t.Fatal("Failover with nil liveness predicate = nil err, want non-nil")
	}
}
