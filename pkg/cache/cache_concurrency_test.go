package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// hasTombstone reports whether the cache currently holds a tombstone for key
// (white-box; reads the internal map under the cache lock).
func hasTombstone(c *Cache, key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.tombstones[key]
	return ok
}

// hookStore is an in-memory Store that blocks the FIRST Get of a chosen key
// exactly once, AFTER it has read the current value, so a test can interleave a
// concurrent Set and/or a clock advance in the window between the L2 read and the
// cache's promotion write-back (the CACHE-IMP-1 and MINOR-3 straddle window). The
// value captured before the block is the value returned, modelling a Get that
// read the pre-Set state from L2. Every method is concurrency-safe.
type hookStore struct {
	mu       sync.Mutex
	m        map[string][]byte
	blockKey string
	reached  chan struct{} // closed once, when the blocked Get is entered
	release  chan struct{} // the test closes this to let the blocked Get return
	fired    bool
}

func newHookStore() *hookStore {
	return &hookStore{
		m:       make(map[string][]byte),
		reached: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (s *hookStore) Get(key string) ([]byte, bool, error) {
	s.mu.Lock()
	v, ok := s.m[key]
	block := key == s.blockKey && !s.fired
	if block {
		s.fired = true
	}
	var cp []byte
	if ok {
		cp = append([]byte(nil), v...)
	}
	s.mu.Unlock()
	if block {
		close(s.reached)
		<-s.release
	}
	return cp, ok, nil
}

func (s *hookStore) Set(key string, value []byte) error {
	s.mu.Lock()
	s.m[key] = append([]byte(nil), value...)
	s.mu.Unlock()
	return nil
}

func (s *hookStore) Delete(key string) error {
	s.mu.Lock()
	delete(s.m, key)
	s.mu.Unlock()
	return nil
}

// put plants an entry directly into the store (white-box, via the package encode)
// with a caller-chosen StoredAt and ExpiresAt (zero ExpiresAt means no expiry).
func (s *hookStore) put(t *testing.T, key, value string, storedAt, expiresAt time.Time) {
	t.Helper()
	raw, err := encode(entry{Value: value, StoredAt: storedAt, ExpiresAt: expiresAt})
	if err != nil {
		t.Fatalf("hookStore.put encode: %v", err)
	}
	s.mu.Lock()
	s.m[key] = raw
	s.mu.Unlock()
}

// TestGet_ConcurrentSetDuringL2Promotion_NoStaleWriteback reproduces CACHE-IMP-1:
// a Get whose L2 read straddles a concurrent Set(k, v-NEW) must NOT resurrect the
// older L2-read value over the fresh L1 entry. On the unconditional write-back it
// FAILs (a later Get serves the stale v-old); with the freshness guard it PASSes.
func TestGet_ConcurrentSetDuringL2Promotion_NoStaleWriteback(t *testing.T) {
	fc := newFakeClock(baseTime)
	store := newHookStore()
	store.blockKey = "k"
	// L2 holds the OLD value stamped at baseTime; L1 is cold so Get falls to L2.
	store.put(t, "k", "v-old", baseTime, time.Time{})

	c := New(WithStore(store), WithClock(fc.Now))

	type res struct {
		v   string
		hit bool
		err error
	}
	resCh := make(chan res, 1)
	go func() {
		v, hit, err := c.Get("k")
		resCh <- res{v, hit, err}
	}()

	<-store.reached // the Get has read v-old from L2 and is parked before promotion

	// A newer value is written while the Get straddles the read/promote window.
	fc.Advance(time.Second) // clock now baseTime+1s, so v-NEW.StoredAt > v-old.StoredAt
	if err := c.Set("k", "v-NEW"); err != nil {
		t.Fatalf("concurrent Set: %v", err)
	}

	close(store.release) // let the parked Get finish its promotion decision
	if r := <-resCh; r.err != nil {
		t.Fatalf("straddling Get err: %v", r.err)
	}

	// The regression is whether the straddling Get left v-old in L1 over v-NEW.
	// A subsequent Get MUST observe the fresh value, never the resurrected stale one.
	got, hit, err := c.Get("k")
	if err != nil {
		t.Fatalf("post Get err: %v", err)
	}
	if !hit {
		t.Fatalf("post Get: miss, want hit v-NEW")
	}
	if got != "v-NEW" {
		t.Fatalf("stale write-back regression (CACHE-IMP-1): post Get = %q, want v-NEW", got)
	}
}

// TestGet_L2EntryExpiringDuringStoreCall_NotServed reproduces MINOR-3: an L2 entry
// that TTL-expires DURING the (blocking) store call must be judged against the
// clock at promotion time, not the clock at Get entry. On the read-start-now code
// it FAILs (serves the just-expired value); re-reading the clock makes it PASS.
func TestGet_L2EntryExpiringDuringStoreCall_NotServed(t *testing.T) {
	fc := newFakeClock(baseTime)
	store := newHookStore()
	store.blockKey = "k"
	// Entry is valid at Get entry; it expires at baseTime+100ms.
	store.put(t, "k", "v", baseTime, baseTime.Add(100*time.Millisecond))

	c := New(WithStore(store), WithClock(fc.Now))

	type res struct {
		v   string
		hit bool
		err error
	}
	resCh := make(chan res, 1)
	go func() {
		v, hit, err := c.Get("k")
		resCh <- res{v, hit, err}
	}()

	<-store.reached                    // Get has read the (still-valid) entry, parked before promotion
	fc.Advance(200 * time.Millisecond) // the entry is now past its TTL
	close(store.release)

	r := <-resCh
	if r.err != nil {
		t.Fatalf("Get err: %v", r.err)
	}
	if r.hit {
		t.Fatalf("served an entry that expired during the store call (MINOR-3): got %q, want miss", r.v)
	}
}

// TestTombstoneTTL_DefaultOff_RetainsTombstoneForever proves the default (no
// WithTombstoneTTL) never prunes a tombstone, so the unconditional no-stale-serve
// guarantee is preserved for keys invalidated and never re-Set. A leaky store
// keeps the invalidated value in L2; only the retained tombstone prevents serving
// it. Advancing time and driving other invalidations must NOT prune it.
func TestTombstoneTTL_DefaultOff_RetainsTombstoneForever(t *testing.T) {
	fc := newFakeClock(baseTime)
	store := newMemStore()
	store.leaky = true
	c := New(WithClock(fc.Now), WithStore(store)) // tombstoneTTL off => retain forever

	if err := c.Set("k", "v"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	fc.Advance(time.Millisecond)
	if err := c.Invalidate("k"); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}
	fc.Advance(1000 * time.Hour) // far beyond any plausible retention window
	for i := 0; i < 5; i++ {
		if err := c.Invalidate(fmt.Sprintf("other%d", i)); err != nil {
			t.Fatalf("Invalidate other: %v", err)
		}
	}
	if !hasTombstone(c, "k") {
		t.Fatalf("default (tombstoneTTL off) pruned the tombstone; it must be retained forever")
	}
	mustMiss(t, c, "k") // the leaky survivor is still not served
}

// TestTombstoneTTL_WithinWindow_StillGuards proves that enabling the option does
// NOT prune a tombstone while it is still inside the retention window: within the
// window the tombstone remains and continues to prevent a stale serve.
func TestTombstoneTTL_WithinWindow_StillGuards(t *testing.T) {
	fc := newFakeClock(baseTime)
	store := newMemStore()
	store.leaky = true
	const retention = time.Hour
	c := New(WithClock(fc.Now), WithStore(store), WithTombstoneTTL(retention))

	if err := c.SetWithTTL("k", "v", 2*time.Hour); err != nil { // long-lived leaked entry
		t.Fatalf("SetWithTTL: %v", err)
	}
	fc.Advance(time.Millisecond)
	if err := c.Invalidate("k"); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}
	fc.Advance(30 * time.Minute) // still inside the 1h retention window
	if err := c.Invalidate("other"); err != nil {
		t.Fatalf("Invalidate other: %v", err) // would fire a sweep, but "k" is too young to prune
	}
	if !hasTombstone(c, "k") {
		t.Fatalf("tombstone pruned inside the retention window; that would reopen a stale serve")
	}
	mustMiss(t, c, "k") // tombstone still guards the surviving (not-yet-expired) L2 value
}

// TestTombstoneTTL_BeyondWindow_PrunesAndStaysSafeUnderContract proves the memory
// bound AND its safety: once a tombstone is older than the retention window it is
// pruned (map stays bounded), and under the documented contract (retention >= max
// entry lifetime) the value the pruned tombstone would have rejected is itself
// already TTL-expired, so validLocked still refuses to serve it. No stale serve.
func TestTombstoneTTL_BeyondWindow_PrunesAndStaysSafeUnderContract(t *testing.T) {
	fc := newFakeClock(baseTime)
	store := newMemStore()
	store.leaky = true
	const retention = time.Hour // retention >= max entry lifetime (the contract)
	c := New(WithClock(fc.Now), WithStore(store), WithTombstoneTTL(retention))

	// Leaked entry lifetime (30m) <= retention window (1h): contract satisfied.
	if err := c.SetWithTTL("k", "v", 30*time.Minute); err != nil {
		t.Fatalf("SetWithTTL: %v", err)
	}
	fc.Advance(time.Millisecond)
	if err := c.Invalidate("k"); err != nil {
		t.Fatalf("Invalidate k: %v", err)
	}
	if !hasTombstone(c, "k") {
		t.Fatalf("precondition: tombstone should exist immediately after Invalidate")
	}

	// Advance beyond BOTH the entry TTL (30m) and the retention window (1h); a
	// later invalidation of another key fires the throttled sweep.
	fc.Advance(90 * time.Minute)
	if err := c.Invalidate("other"); err != nil {
		t.Fatalf("Invalidate other: %v", err)
	}
	if hasTombstone(c, "k") {
		t.Fatalf("tombstone older than the retention window was not pruned (unbounded growth)")
	}
	// Safety under the contract: the leaked value expired (90m > 30m TTL), so it is
	// rejected on ExpiresAt grounds even with the tombstone gone.
	mustMiss(t, c, "k")
}
