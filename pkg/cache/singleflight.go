package cache

import (
	"errors"
	"time"
)

// ErrNilCompute is returned by GetOrCompute when handed a nil compute
// function. Without this guard the miss path would nil-pointer-dereference
// instead of surfacing a caller bug (§11.4.1 — a crash is not an honest
// error path).
var ErrNilCompute = errors.New("cache: GetOrCompute requires a non-nil compute func")

// ComputeFunc computes the value for a cache miss inside GetOrCompute. It
// returns the value to cache, the TTL to apply (non-positive means never
// expire, matching SetWithTTL), and an error. An error is NEVER cached —
// every concurrent caller waiting on this computation receives it, and the
// key remains genuinely uncached so the next call retries for real (§11.4.1:
// a poisoned "successful" cache entry born from a failed computation is a
// PASS-bluff at the correctness layer).
type ComputeFunc func() (value string, ttl time.Duration, err error)

// sfCall is one in-flight computation shared by every concurrent GetOrCompute
// caller for the same key. done is closed exactly once, by the winner, after
// value/err are set — every other field is safe to read only after <-done.
type sfCall struct {
	done  chan struct{}
	value string
	err   error
}

// GetOrCompute returns the cached value for key, computing it via compute on
// a miss. This is the WS6 "synchronization" primitive the caching design
// specifies but the framework did not yet implement — see
// docs/research/tokens/ws6_caching_sync/DESIGN.md §1's request-flow diagram
// ("store in L1/L2 (single-flight lock)") and §4's invalidation-trigger
// matrix row "Concurrent hot-key expiry (N contexts) -> single-flight lock".
//
// Single-flight (stampede guard). Concurrent GetOrCompute calls for the SAME
// key coalesce: the first caller to miss (the winner) runs compute exactly
// once; every other concurrent caller for that key (a loser) blocks on the
// winner's result instead of redundantly recomputing an expensive value N
// times for what is, from the cache's point of view, one logical miss.
//
// Correctness under a concurrent Invalidate (§11.4.6 honesty — no bluff).
// compute may run for a non-trivial duration, during which the data it is
// reading can genuinely change and the caller (or another goroutine) can call
// Invalidate(key) to say so. GetOrCompute snapshots the key's tombstone
// state BEFORE compute runs and compares it against the tombstone state
// immediately AFTER compute returns. If they differ, an invalidation landed
// while compute was in flight — the computed value reflects data that is now
// known-superseded. In that case the in-flight caller still receives the
// value (it was the correct answer to ITS OWN request, asked before the
// invalidation), but the result is NOT written into the cache: a subsequent
// Get MUST be an honest miss, never a resurrected stale value. This is
// validLocked's existing StoredAt-after-tombstone rule applied at write time
// instead of read time, and mirrors the CACHE-IMP-1 freshness-guard precedent
// already established for Get's own L2-promotion race window.
//
// GetOrCompute does not hold c.mu across the compute call (compute may block
// on network/tool I/O), matching the rest of the package's no-blocking-under-
// the-data-lock discipline.
func (c *Cache) GetOrCompute(key string, compute ComputeFunc) (string, error) {
	if key == "" {
		return "", ErrEmptyKey
	}
	if compute == nil {
		return "", ErrNilCompute
	}

	if v, hit, err := c.Get(key); err != nil {
		return "", err
	} else if hit {
		return v, nil
	}

	c.sfMu.Lock()
	if call, inflight := c.sf[key]; inflight {
		c.sfMu.Unlock()
		<-call.done
		return call.value, call.err
	}
	// Double-check the cache under sfMu before registering as the winner. The
	// fast-path c.Get above runs WITHOUT sfMu, so a single-flight winner can
	// cache the value AND release its slot in the window between this caller's
	// Get-miss and its acquiring sfMu here. Without this re-check, such a
	// late-arriving caller finds no in-flight call, registers a second slot,
	// and starts a SECOND redundant compute for a value that is already
	// cached — the exact stampede this primitive exists to prevent. (A -race
	// -count=20 stress reproduces the double-compute; -count=3 missed it —
	// §11.4.50 deterministic-consistency.) Lock order is consistently
	// sfMu→c.mu here; no path takes c.mu then sfMu, so this cannot deadlock.
	if v, hit, gerr := c.Get(key); gerr == nil && hit {
		c.sfMu.Unlock()
		return v, nil
	}
	call := &sfCall{done: make(chan struct{})}
	c.sf[key] = call
	c.sfMu.Unlock()

	// computeGuarded runs the WS6 cross-process guard (crossprocess.go) when
	// one is configured, then either way arrives at the tombstone-snapshot +
	// compute + write-back sequence via runComputeAndWriteBack. With no
	// CrossProcessLock configured (c.xlock == nil, the default) this is
	// EXACTLY the original in-process-only sequence — see
	// runComputeAndWriteBack for the unchanged logic.
	value, err := c.computeGuarded(key, compute)

	if err != nil {
		call.err = err
		close(call.done)
		// The winner's slot is removed ONLY after the result is available
		// (done closed): removing it any earlier would open a window where a
		// brand-new caller sees no in-flight call for this key AND no cached
		// value either, so it would start a SECOND, fully redundant compute —
		// exactly the stampede this function exists to prevent. Every caller
		// that finds the slot (even one that arrives between close(done) and
		// this delete) reads an already-closed channel and returns instantly.
		c.sfMu.Lock()
		delete(c.sf, key)
		c.sfMu.Unlock()
		return "", err
	}

	call.value = value
	close(call.done)

	// See the error-path comment above: the slot is released only now, after
	// the result is both cached (when not superseded) and broadcast via
	// done, so no window exists in which a concurrent caller could observe
	// neither an in-flight call nor a cached value for this key.
	c.sfMu.Lock()
	delete(c.sf, key)
	c.sfMu.Unlock()

	return value, nil
}

// computeGuarded runs compute for the in-process single-flight winner,
// optionally coordinated with a cross-process lock (WithCrossProcessLock).
//
// With no CrossProcessLock configured, this immediately delegates to
// runComputeAndWriteBack — the original, unmodified in-process-only
// behaviour.
//
// With a CrossProcessLock configured, it first tries to become the
// cross-process winner too:
//   - Lock acquired (ok): this process is BOTH the in-process AND the
//     cross-process winner. It runs runComputeAndWriteBack normally (which
//     writes the result to the shared Store) and releases the cross-process
//     lock afterward, regardless of outcome (defer).
//   - Lock not acquired, OR the lock backend itself errored (ok == false):
//     another process is very likely already computing this key. Rather
//     than compute redundantly, this process waits (pollForResult) for that
//     winner to publish its result to the shared Store. If a result shows up
//     within the wait budget, it is returned WITHOUT ever calling compute —
//     the cross-process single-flight success case. If nothing shows up in
//     time (winner still running, no shared Store configured, or the
//     winner's process died mid-computation), this process falls back to
//     computing directly via runComputeAndWriteBack. This fallback is the
//     honest degrade path required when no shared Store makes the winner's
//     result observable (§11.4.6): the guard can only ever REDUCE redundant
//     computation, it never risks a hang or a fabricated result.
func (c *Cache) computeGuarded(key string, compute ComputeFunc) (string, error) {
	if c.xlock == nil {
		return c.runComputeAndWriteBack(key, compute)
	}

	unlock, ok, lockErr := c.xlock.TryLock(key)
	if lockErr == nil && ok {
		defer func() { _ = unlock() }()
		return c.runComputeAndWriteBack(key, compute)
	}

	if v, found := c.pollForResult(key); found {
		return v, nil
	}
	return c.runComputeAndWriteBack(key, compute)
}

// runComputeAndWriteBack is the tombstone-snapshot + compute + write-back
// sequence GetOrCompute has always run for its in-process winner, extracted
// unchanged so computeGuarded can wrap it with the optional cross-process
// guard above. See GetOrCompute's doc comment for the full correctness
// argument (in particular the "Correctness under a concurrent Invalidate"
// section) — nothing about that argument changes here.
func (c *Cache) runComputeAndWriteBack(key string, compute ComputeFunc) (string, error) {
	// Snapshot the invalidation marker BEFORE compute runs.
	c.mu.Lock()
	genBefore, hadTomb := c.tombstones[key]
	c.mu.Unlock()

	value, ttl, err := compute()
	if err != nil {
		return "", err
	}

	c.mu.Lock()
	genNow, hasTombNow := c.tombstones[key]
	c.mu.Unlock()
	supersededByInvalidate := hasTombNow && (!hadTomb || genNow.After(genBefore))

	if !supersededByInvalidate {
		// Best-effort write-back: compute succeeded and produced the caller's
		// answer regardless, so a write-back failure (e.g. L2 store error) is
		// surfaced to nobody here — it is not this caller's request that
		// failed. A future Get will simply miss and re-trigger computation,
		// which is the correct honest fallback (§11.4.1).
		_ = c.SetWithTTL(key, value, ttl)
	}

	return value, nil
}

// pollForResult waits for a cross-process winner to publish key's result to
// the shared Store, polling c.Get (which checks L1 then the injected Store)
// at c.xpoll intervals for up to c.xtimeout, falling back to the package
// defaults for either bound left at its zero value. It returns found == true
// the moment a servable value appears; found == false once the wait budget
// elapses with none.
//
// This deliberately uses REAL wall-clock time (time.Now/time.Sleep), NOT
// c.clock() — c.clock() exists so TTL/invalidation decisions (a pure
// function of stored timestamps) are deterministic under an injected fake
// clock (§11.4.50). Waiting for a genuinely concurrent, externally-owned
// OTHER PROCESS to finish is not such a decision: no fake clock can make an
// external process finish sooner, so using c.clock() here would either (a)
// do nothing useful in production (time.Now IS c.clock() there) or (b), far
// worse, spin forever in any test that injects a fixed/fake clock — the
// fake time never advances, the deadline is never reached, and a follower
// that never finds a result loops until the test's own timeout kills it.
// That exact bug was caught live by this package's own test suite (an
// earlier draft used c.clock() here and hung the whole `go test`
// invocation) — the authoritative reason this function is pinned to real
// time regardless of what Clock the Cache was constructed with.
func (c *Cache) pollForResult(key string) (string, bool) {
	poll := c.xpoll
	if poll <= 0 {
		poll = defaultCrossProcessPoll
	}
	timeout := c.xtimeout
	if timeout <= 0 {
		timeout = defaultCrossProcessTimeout
	}

	deadline := time.Now().Add(timeout)
	for {
		if v, hit, err := c.Get(key); err == nil && hit {
			return v, true
		}
		if !time.Now().Before(deadline) {
			return "", false
		}
		time.Sleep(poll)
	}
}
