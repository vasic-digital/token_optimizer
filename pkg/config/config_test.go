package config

import (
	"errors"
	"sync"
	"testing"
)

func TestRegisterTier(t *testing.T) {
	tests := []struct {
		name    string
		tier    Tier
		wantErr error
	}{
		{"valid free deterministic", Tier{Name: "T-DET", Deterministic: true}, nil},
		{"valid priced", Tier{Name: "T-NATIVE", PricePerMTokIn: 3, PricePerMTokOut: 15}, nil},
		{"empty name", Tier{Name: ""}, ErrEmptyTierName},
		{"negative input price", Tier{Name: "bad-in", PricePerMTokIn: -1}, ErrNegativePrice},
		{"negative output price", Tier{Name: "bad-out", PricePerMTokOut: -0.5}, ErrNegativePrice},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := New()
			err := c.RegisterTier(tc.tier)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("RegisterTier(%+v) = %v, want nil", tc.tier, err)
				}
				got, ok := c.Tier(tc.tier.Name)
				if !ok {
					t.Fatalf("tier %q not retrievable after registration", tc.tier.Name)
				}
				if got != tc.tier {
					t.Fatalf("retrieved tier = %+v, want %+v", got, tc.tier)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("RegisterTier(%+v) err = %v, want errors.Is %v", tc.tier, err, tc.wantErr)
			}
		})
	}
}

func TestRegisterTierDuplicate(t *testing.T) {
	c := New()
	if err := c.RegisterTier(Tier{Name: "T-LOCAL"}); err != nil {
		t.Fatalf("first register: %v", err)
	}
	err := c.RegisterTier(Tier{Name: "T-LOCAL", Endpoint: "http://other"})
	if !errors.Is(err, ErrDuplicateTier) {
		t.Fatalf("duplicate register err = %v, want ErrDuplicateTier", err)
	}
	// The original registration must be preserved unchanged.
	got, _ := c.Tier("T-LOCAL")
	if got.Endpoint != "" {
		t.Fatalf("duplicate register mutated stored tier: endpoint = %q, want empty", got.Endpoint)
	}
}

func TestTiersOrderedByPriorityThenName(t *testing.T) {
	c := New()
	// Register out of order, with a Priority tie between "T-A" and "T-B".
	for _, tr := range []Tier{
		{Name: "T-NATIVE", Priority: 30},
		{Name: "T-B", Priority: 10},
		{Name: "T-A", Priority: 10},
		{Name: "T-DET", Priority: 0, Deterministic: true},
	} {
		if err := c.RegisterTier(tr); err != nil {
			t.Fatalf("register %q: %v", tr.Name, err)
		}
	}
	want := []string{"T-DET", "T-A", "T-B", "T-NATIVE"}
	got := c.Tiers()
	if len(got) != len(want) {
		t.Fatalf("Tiers() len = %d, want %d", len(got), len(want))
	}
	for i, name := range want {
		if got[i].Name != name {
			t.Fatalf("Tiers()[%d] = %q, want %q (full order %v)", i, got[i].Name, name, names(got))
		}
	}
}

func TestTiersReturnsCopy(t *testing.T) {
	c := New()
	_ = c.RegisterTier(Tier{Name: "T-DET"})
	got := c.Tiers()
	got[0].Name = "MUTATED"
	if again := c.Tiers(); again[0].Name != "T-DET" {
		t.Fatalf("Tiers() returned a mutable view: got %q after external mutation", again[0].Name)
	}
}

func TestRegisterAlternative(t *testing.T) {
	c := New()
	for _, n := range []string{"T-PRIMARY", "T-ALT1", "T-ALT2"} {
		if err := c.RegisterTier(Tier{Name: n}); err != nil {
			t.Fatalf("register %q: %v", n, err)
		}
	}
	tests := []struct {
		name    string
		primary string
		alts    []string
		wantErr error
		want    []string
	}{
		{"single alt", "T-PRIMARY", []string{"T-ALT1"}, nil, []string{"T-ALT1"}},
		{"accumulate + dedup", "T-PRIMARY", []string{"T-ALT1", "T-ALT2"}, nil, []string{"T-ALT1", "T-ALT2"}},
		{"unknown primary", "T-NOPE", []string{"T-ALT1"}, ErrUnknownTier, []string{"T-ALT1", "T-ALT2"}},
		{"unknown alternative", "T-PRIMARY", []string{"T-GHOST"}, ErrUnknownTier, []string{"T-ALT1", "T-ALT2"}},
		{"self alternative", "T-PRIMARY", []string{"T-PRIMARY"}, ErrSelfAlternative, []string{"T-ALT1", "T-ALT2"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := c.RegisterAlternative(tc.primary, tc.alts...)
			if tc.wantErr == nil && err != nil {
				t.Fatalf("RegisterAlternative err = %v, want nil", err)
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("RegisterAlternative err = %v, want errors.Is %v", err, tc.wantErr)
			}
			got := c.Alternatives("T-PRIMARY")
			if !equalStrings(got, tc.want) {
				t.Fatalf("Alternatives after %q = %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}

// TestRegisterAlternativeAtomicOnInvalid proves RegisterAlternative is atomic:
// when a later element in the same call is invalid, NO earlier valid element
// may land (§11.4.1 no-half-applied-state, mirroring RegisterTier's
// validate-before-mutate contract). The pre-fix code appended each valid alt
// to the stored set BEFORE it could hit the invalid one and return the error,
// leaving a partial set despite the failure. This test FAILED against that
// buggy code and PASSES after the atomic pre-validate-then-append fix
// (§11.4.115 RED→GREEN). Each sub-case uses a fresh Config so the "no partial
// state landed" assertion is unambiguous.
func TestRegisterAlternativeAtomicOnInvalid(t *testing.T) {
	tests := []struct {
		name    string
		alts    []string
		wantErr error
	}{
		{"valid then unknown ghost", []string{"T-ALT1", "T-GHOST"}, ErrUnknownTier},
		{"valid then self", []string{"T-ALT1", "T-PRIMARY"}, ErrSelfAlternative},
		{"two valid then unknown", []string{"T-ALT1", "T-ALT2", "T-GHOST"}, ErrUnknownTier},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := New()
			for _, n := range []string{"T-PRIMARY", "T-ALT1", "T-ALT2"} {
				if err := c.RegisterTier(Tier{Name: n}); err != nil {
					t.Fatalf("register %q: %v", n, err)
				}
			}
			err := c.RegisterAlternative("T-PRIMARY", tc.alts...)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("RegisterAlternative(%v) err = %v, want errors.Is %v", tc.alts, err, tc.wantErr)
			}
			// Atomic all-or-nothing: on error, NO earlier valid element may
			// have been appended to the stored set.
			if got := c.Alternatives("T-PRIMARY"); len(got) != 0 {
				t.Fatalf("partial state landed despite error: Alternatives = %v, want empty (atomic all-or-nothing)", got)
			}
		})
	}
}

func TestThresholds(t *testing.T) {
	c := New()
	if _, ok := c.Threshold("semantic_cosine_floor"); ok {
		t.Fatal("unset threshold reported present")
	}
	c.SetThreshold("semantic_cosine_floor", 0.86)
	got, ok := c.Threshold("semantic_cosine_floor")
	if !ok || got != 0.86 {
		t.Fatalf("Threshold = (%v, %v), want (0.86, true)", got, ok)
	}
	// Overwrite is allowed.
	c.SetThreshold("semantic_cosine_floor", 0.9)
	if got, _ := c.Threshold("semantic_cosine_floor"); got != 0.9 {
		t.Fatalf("Threshold after overwrite = %v, want 0.9", got)
	}
}

func TestNeverDowngradeDefault(t *testing.T) {
	strong := Tier{Name: "strong", PricePerMTokIn: 3, PricePerMTokOut: 15} // combined 18
	weak := Tier{Name: "weak", PricePerMTokIn: 0.2, PricePerMTokOut: 0.6}  // combined 0.8
	tests := []struct {
		name        string
		current     Tier
		candidate   Tier
		loadBearing bool
		want        bool
	}{
		{"load-bearing down to cheaper is forbidden", strong, weak, true, true},
		{"load-bearing up to pricier is allowed", weak, strong, true, false},
		{"load-bearing same price allowed", strong, strong, true, false},
		{"non-load-bearing down to cheaper allowed", strong, weak, false, false},
	}
	c := New() // seeded with DefaultNeverDowngrade
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := c.IsForbiddenDowngrade(tc.current, tc.candidate, tc.loadBearing); got != tc.want {
				t.Fatalf("IsForbiddenDowngrade = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestNeverDowngradeOverrideAndNilRestore(t *testing.T) {
	strong := Tier{Name: "strong", PricePerMTokIn: 10}
	weak := Tier{Name: "weak", PricePerMTokIn: 1}
	c := New()

	// Consumer policy: forbid ALL downgrades regardless of load-bearing flag.
	c.SetNeverDowngrade(func(cur, cand Tier, _ bool) bool {
		return cand.CombinedPrice() < cur.CombinedPrice()
	})
	if !c.IsForbiddenDowngrade(strong, weak, false) {
		t.Fatal("override predicate not consulted for non-load-bearing downgrade")
	}

	// nil restores the default (which allows non-load-bearing downgrades).
	c.SetNeverDowngrade(nil)
	if c.IsForbiddenDowngrade(strong, weak, false) {
		t.Fatal("nil did not restore DefaultNeverDowngrade")
	}
}

func TestConcurrentRegisterAndRead(t *testing.T) {
	c := New()
	const n = 64
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			_ = c.RegisterTier(Tier{Name: "T-" + itoa(i), Priority: i})
			_ = c.Tiers()
			_, _ = c.Tier("T-" + itoa(i))
		}(i)
	}
	wg.Wait()
	if got := len(c.Tiers()); got != n {
		t.Fatalf("after %d concurrent registrations, Tiers() len = %d", n, got)
	}
}

// --- small local helpers (kept dependency-free) ---

func names(ts []Tier) []string {
	out := make([]string, len(ts))
	for i, t := range ts {
		out[i] = t.Name
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}
