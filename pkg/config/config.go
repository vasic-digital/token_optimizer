// Package config is the decoupling surface of the token_optimizer engine.
//
// The engine ships ZERO project constants. A consumer registers every
// project-specific datum — completion tiers, per-tier pricing, routing
// thresholds, tier alternatives, and the load-bearing NeverDowngrade
// predicate — at runtime through a *Config value. No other engine package
// hardcodes a tier name, an endpoint, a price, or a threshold; they all read
// them from the *Config the consumer wired at startup.
//
// This keeps the engine fully reusable by any project (see the repository
// decoupling contract in README.md): the request path is identical whether
// the module is vendored, referenced, or reused — only the runtime
// registration differs.
package config

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

// Registration errors returned by Config methods. They are sentinel values so
// callers can classify failures with errors.Is.
var (
	// ErrEmptyTierName is returned when RegisterTier is given a tier whose
	// Name is empty.
	ErrEmptyTierName = errors.New("config: tier name must be non-empty")
	// ErrDuplicateTier is returned when RegisterTier is given a tier whose
	// Name is already registered.
	ErrDuplicateTier = errors.New("config: tier already registered")
	// ErrNegativePrice is returned when a tier declares a negative price.
	ErrNegativePrice = errors.New("config: tier price must be non-negative")
	// ErrUnknownTier is returned when an operation references a tier name
	// that was never registered.
	ErrUnknownTier = errors.New("config: unknown tier")
	// ErrSelfAlternative is returned when a tier is registered as its own
	// alternative.
	ErrSelfAlternative = errors.New("config: tier cannot be its own alternative")
)

// Tier describes one completion tier the consumer wires at startup. The engine
// treats a Tier as opaque data — it never inspects Name for a magic value.
type Tier struct {
	// Name is the consumer-chosen stable identifier (e.g. "T-DET",
	// "T-LOCAL-8B", "T-NATIVE"). Resolution is always by this stable name,
	// never by registration order (§11.4.111).
	Name string
	// Endpoint is the OpenAI-compatible completion endpoint, or "" for a
	// deterministic / shell-out tier that has no HTTP endpoint.
	Endpoint string
	// Priority orders tiers for selection; LOWER is preferred (a cheaper or
	// more-deterministic tier is tried first). Ties break on Name for
	// deterministic ordering (§11.4.50).
	Priority int
	// PricePerMTokIn is USD per million input tokens. 0 means free — the
	// expected value for local llama.cpp and deterministic tiers.
	PricePerMTokIn float64
	// PricePerMTokOut is USD per million output tokens.
	PricePerMTokOut float64
	// Deterministic marks a T-DET-class tier that shells out to a registered
	// utility rather than calling a model endpoint.
	Deterministic bool
}

// CombinedPrice is the tier's per-million-token cost summed across the input
// and output channels. It is the scalar the default downgrade predicate
// compares tiers on.
func (t Tier) CombinedPrice() float64 { return t.PricePerMTokIn + t.PricePerMTokOut }

// NeverDowngrade is the load-bearing routing floor predicate. It reports
// whether routing a request from the currently-selected tier down to a
// candidate tier is a FORBIDDEN downgrade. The router refuses any candidate
// for which this returns true. The consumer may install its own policy via
// Config.SetNeverDowngrade; when none is installed the engine uses
// DefaultNeverDowngrade.
type NeverDowngrade func(current, candidate Tier, loadBearing bool) bool

// DefaultNeverDowngrade is the engine's generic, project-agnostic floor: for a
// load-bearing request, moving to a strictly cheaper tier is a forbidden
// downgrade. Non-load-bearing requests are never restricted. It contains no
// project constant — only the universal "cheaper == weaker for a critical
// request" heuristic — so consumers may keep it or replace it wholesale.
func DefaultNeverDowngrade(current, candidate Tier, loadBearing bool) bool {
	if !loadBearing {
		return false
	}
	return candidate.CombinedPrice() < current.CombinedPrice()
}

// Config is the runtime registry the consumer populates at startup. It is safe
// for concurrent use by multiple goroutines — the request path across the
// context fleet shares one *Config.
type Config struct {
	mu           sync.RWMutex
	tiers        map[string]Tier
	alternatives map[string][]string
	thresholds   map[string]float64
	neverDown    NeverDowngrade
}

// New returns an empty Config seeded with DefaultNeverDowngrade. The consumer
// registers tiers, alternatives, and thresholds before handing the Config to
// the engine.
func New() *Config {
	return &Config{
		tiers:        make(map[string]Tier),
		alternatives: make(map[string][]string),
		thresholds:   make(map[string]float64),
		neverDown:    DefaultNeverDowngrade,
	}
}

// RegisterTier records a completion tier. It returns ErrEmptyTierName,
// ErrNegativePrice, or ErrDuplicateTier (wrapped with the tier name) if the
// tier is invalid or already registered.
func (c *Config) RegisterTier(t Tier) error {
	if t.Name == "" {
		return ErrEmptyTierName
	}
	if t.PricePerMTokIn < 0 || t.PricePerMTokOut < 0 {
		return fmt.Errorf("%w: %q", ErrNegativePrice, t.Name)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.tiers[t.Name]; exists {
		return fmt.Errorf("%w: %q", ErrDuplicateTier, t.Name)
	}
	c.tiers[t.Name] = t
	return nil
}

// Tier returns the registered tier by its stable name and whether it exists.
func (c *Config) Tier(name string) (Tier, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	t, ok := c.tiers[name]
	return t, ok
}

// Tiers returns all registered tiers ordered by Priority ascending, breaking
// ties on Name so the order is deterministic (§11.4.50). The returned slice is
// a copy; mutating it does not affect the Config.
func (c *Config) Tiers() []Tier {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]Tier, 0, len(c.tiers))
	for _, t := range c.tiers {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Priority != out[j].Priority {
			return out[i].Priority < out[j].Priority
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// RegisterAlternative records one or more fallback tiers for a primary tier
// (used by the failover path when the primary endpoint is down). Both the
// primary and each alternative MUST already be registered. It returns
// ErrUnknownTier if any name is unregistered, or ErrSelfAlternative if a tier
// is listed as its own alternative. Alternatives accumulate across calls and
// are de-duplicated preserving first-seen order.
func (c *Config) RegisterAlternative(primary string, alts ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.tiers[primary]; !ok {
		return fmt.Errorf("%w: primary %q", ErrUnknownTier, primary)
	}
	seen := make(map[string]struct{}, len(c.alternatives[primary]))
	for _, a := range c.alternatives[primary] {
		seen[a] = struct{}{}
	}
	for _, a := range alts {
		if a == primary {
			return fmt.Errorf("%w: %q", ErrSelfAlternative, primary)
		}
		if _, ok := c.tiers[a]; !ok {
			return fmt.Errorf("%w: alternative %q", ErrUnknownTier, a)
		}
		if _, dup := seen[a]; dup {
			continue
		}
		seen[a] = struct{}{}
		c.alternatives[primary] = append(c.alternatives[primary], a)
	}
	return nil
}

// Alternatives returns the registered fallback tier names for a primary tier,
// in registration order. It returns a copy; nil if none are registered.
func (c *Config) Alternatives(primary string) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	src := c.alternatives[primary]
	if len(src) == 0 {
		return nil
	}
	out := make([]string, len(src))
	copy(out, src)
	return out
}

// SetThreshold records a named routing threshold (e.g. a semantic-cache cosine
// floor, a max-cost-per-request ceiling). Keys are consumer-defined; the
// engine reads them by name and never assumes a specific key exists.
func (c *Config) SetThreshold(key string, value float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.thresholds[key] = value
}

// Threshold returns a named threshold and whether it was set.
func (c *Config) Threshold(key string) (float64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.thresholds[key]
	return v, ok
}

// SetNeverDowngrade installs the consumer's load-bearing floor predicate. A nil
// argument restores DefaultNeverDowngrade rather than disabling the floor, so
// the engine always has a predicate to consult.
func (c *Config) SetNeverDowngrade(fn NeverDowngrade) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if fn == nil {
		fn = DefaultNeverDowngrade
	}
	c.neverDown = fn
}

// IsForbiddenDowngrade reports whether routing from current to candidate is a
// forbidden downgrade under the active predicate. It is the single point the
// router consults so the floor cannot be bypassed.
func (c *Config) IsForbiddenDowngrade(current, candidate Tier, loadBearing bool) bool {
	c.mu.RLock()
	fn := c.neverDown
	c.mu.RUnlock()
	return fn(current, candidate, loadBearing)
}
