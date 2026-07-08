// Package cache is the WS6 multi-level result cache of the token_optimizer
// engine: an in-memory L1 layer over an optional, consumer-injected L2 backing
// store, with TTL expiry and correct invalidation semantics.
//
// Decoupling (§11.4.28): the package ships ZERO project constants. It hardcodes
// no key schema, no endpoint, no domain vocabulary, and no storage technology.
// The L2 store is an interface the consumer implements (sqlite, Redis, a file
// directory, an in-memory map for tests); the cache treats the persisted bytes
// as opaque. A cache with no store is a valid L1-only cache. Two consumers with
// completely different backing stores share this exact caching logic.
//
// Determinism (§11.4.50): the cache never bakes a wall clock un-testably. Every
// time-dependent decision — TTL expiry and invalidation ordering — reads the
// injected Clock, so a test drives time deterministically. The default Clock is
// time.Now for production; every deterministic path injects its own.
//
// Honesty (§11.4.6): a value that is expired, or that was written at or before
// the key's most recent invalidation, is NEVER served. It is reported as an
// honest miss so the caller recomputes, rather than being silently returned as a
// stale hit. Invalidation does not merely delete — it records a tombstone so an
// L2 value that survives a best-effort delete (a leaky store, an
// eventually-consistent backend, or a concurrent writer that re-inserted a stale
// value) is still not served. This mirrors the WS6 caching-POC L1 TTL / L3
// no-stale-serve intent.
package cache

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrEmptyKey is returned by Get, Set, and Invalidate when handed an empty key.
// A cache keyed on "" would collide every unkeyed request into one slot; the
// empty key is a caller bug, surfaced rather than silently accepted (§11.4.1).
// It is a sentinel so callers can classify it with errors.Is.
var ErrEmptyKey = errors.New("cache: key must be non-empty")

// Clock returns the current time. It is injected so TTL expiry and invalidation
// ordering are deterministic under test (§11.4.50). The default is time.Now.
type Clock func() time.Time

// Store is the optional L2 backing store the consumer injects. The consumer
// implements it over whatever technology it prefers; the cache serialises its
// own internal entry into the opaque []byte the Store persists, so the Store
// never has to understand TTL, tombstones, or timestamps — it is a dumb byte
// key/value with a delete.
//
// An implementation MUST be safe for concurrent use: the cache calls it from
// multiple goroutines and never holds its own lock across a Store call, so a
// slow store never stalls an unrelated Get.
type Store interface {
	// Get returns the stored bytes for key and whether the key was present. A
	// non-nil error means the store itself failed (surfaced to the caller, never
	// swallowed into a false miss).
	Get(key string) (value []byte, found bool, err error)
	// Set stores value under key, overwriting any previous value.
	Set(key string, value []byte) error
	// Delete removes key. Deleting an absent key is not an error. A delete that
	// is best-effort or a no-op does not compromise correctness — the cache's
	// tombstone independently prevents a stale serve.
	Delete(key string) error
}

// Option configures a Cache at construction.
type Option func(*Cache)

// WithStore installs the L2 backing store. A nil store is ignored, leaving an
// L1-only cache.
func WithStore(s Store) Option {
	return func(c *Cache) {
		if s != nil {
			c.store = s
		}
	}
}

// WithClock installs the time source. A nil clock is ignored, leaving the
// default time.Now.
func WithClock(clk Clock) Option {
	return func(c *Cache) {
		if clk != nil {
			c.clock = clk
		}
	}
}

// WithDefaultTTL sets the time-to-live applied by Set. A non-positive duration
// (the default) means entries stored via Set never TTL-expire; use SetWithTTL
// for a per-entry override.
func WithDefaultTTL(d time.Duration) Option {
	return func(c *Cache) { c.defaultTTL = d }
}

// entry is one cached value plus the metadata the cache reasons over. StoredAt
// is when the value was written (compared against a key's tombstone so an
// invalidation is honoured even if the value physically survives in L2).
// ExpiresAt zero means the entry never TTL-expires.
type entry struct {
	Value     string
	StoredAt  time.Time
	ExpiresAt time.Time
}

// wire is the L2 serialisation of an entry. Times are UnixNano ints so the
// format is compact and instant-preserving; ExpiresAt 0 encodes "no expiry".
type wire struct {
	V string `json:"v"`
	S int64  `json:"s"`
	E int64  `json:"e"`
}

func encode(e entry) ([]byte, error) {
	w := wire{V: e.Value, S: e.StoredAt.UnixNano()}
	if !e.ExpiresAt.IsZero() {
		w.E = e.ExpiresAt.UnixNano()
	}
	return json.Marshal(w)
}

func decode(raw []byte) (entry, error) {
	var w wire
	if err := json.Unmarshal(raw, &w); err != nil {
		return entry{}, err
	}
	e := entry{Value: w.V, StoredAt: time.Unix(0, w.S)}
	if w.E != 0 {
		e.ExpiresAt = time.Unix(0, w.E)
	}
	return e, nil
}

// Cache is a thread-safe multi-level cache: an in-memory L1 map over an optional
// injected L2 Store. It is safe for concurrent use by multiple goroutines — the
// shared request path across the context fleet reads and writes one Cache. The
// zero value is not usable; construct with New.
type Cache struct {
	// mu guards l1 and tombstones. It is NEVER held across a Store call, so a
	// slow or blocking L2 store can never stall an unrelated Get (no blocking I/O
	// under the data lock).
	mu         sync.Mutex
	l1         map[string]entry
	tombstones map[string]time.Time

	store      Store
	clock      Clock
	defaultTTL time.Duration
}

// New returns a ready Cache configured by opts. With no WithStore option it is a
// thread-safe L1-only cache; with WithStore it fronts the injected L2.
func New(opts ...Option) *Cache {
	c := &Cache{
		l1:         make(map[string]entry),
		tombstones: make(map[string]time.Time),
		clock:      time.Now,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// validLocked reports whether entry e for key is servable at time now: not
// TTL-expired AND written strictly after the key's most recent invalidation.
// Must be called with c.mu held.
func (c *Cache) validLocked(key string, e entry, now time.Time) bool {
	if !e.ExpiresAt.IsZero() && !now.Before(e.ExpiresAt) {
		return false // now >= ExpiresAt: expired
	}
	if tomb, ok := c.tombstones[key]; ok && !e.StoredAt.After(tomb) {
		return false // stored at or before the last invalidation: tombstoned
	}
	return true
}

// Get returns the cached value for key and whether it was a hit. It consults L1
// first, then falls through to the L2 store (when one is injected), promoting an
// L2 hit into L1. An expired or tombstoned value is never served — it is an
// honest miss (hit == false, err == nil). A non-nil error means the L2 store
// itself failed; it is surfaced, never swallowed into a false miss.
func (c *Cache) Get(key string) (value string, hit bool, err error) {
	if key == "" {
		return "", false, ErrEmptyKey
	}
	now := c.clock()

	// L1 under the lock; lazily evict a stale entry so it cannot accumulate.
	c.mu.Lock()
	if e, ok := c.l1[key]; ok {
		if c.validLocked(key, e, now) {
			c.mu.Unlock()
			return e.Value, true, nil
		}
		delete(c.l1, key)
	}
	c.mu.Unlock()

	if c.store == nil {
		return "", false, nil
	}

	// L2 outside the lock (the store call may block on I/O).
	raw, found, sErr := c.store.Get(key)
	if sErr != nil {
		return "", false, fmt.Errorf("cache: L2 get %q: %w", key, sErr)
	}
	if !found {
		return "", false, nil
	}
	e, dErr := decode(raw)
	if dErr != nil {
		return "", false, fmt.Errorf("cache: L2 decode %q: %w", key, dErr)
	}

	// Re-validate AND promote under the lock: a tombstone or fresh Set may have
	// landed while the store call was in flight, so the decision is made against
	// current state, never the state at read time.
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.validLocked(key, e, now) {
		return "", false, nil // stale/expired/tombstoned L2 value: never served
	}
	c.l1[key] = e
	return e.Value, true, nil
}

// Set stores value under key using the cache's default TTL. See SetWithTTL.
func (c *Cache) Set(key, value string) error {
	return c.SetWithTTL(key, value, c.defaultTTL)
}

// SetWithTTL stores value under key with an explicit time-to-live. A
// non-positive ttl means the entry never TTL-expires. The value is written to L1
// and, when a store is injected, to L2. Writing a fresh value clears any prior
// tombstone for the key, so re-populating an invalidated key with a genuinely new
// value serves that new value.
func (c *Cache) SetWithTTL(key, value string, ttl time.Duration) error {
	if key == "" {
		return ErrEmptyKey
	}
	now := c.clock()
	e := entry{Value: value, StoredAt: now}
	if ttl > 0 {
		e.ExpiresAt = now.Add(ttl)
	}

	c.mu.Lock()
	c.l1[key] = e
	delete(c.tombstones, key)
	c.mu.Unlock()

	if c.store != nil {
		raw, err := encode(e)
		if err != nil {
			return fmt.Errorf("cache: L2 encode %q: %w", key, err)
		}
		if err := c.store.Set(key, raw); err != nil {
			return fmt.Errorf("cache: L2 set %q: %w", key, err)
		}
	}
	return nil
}
