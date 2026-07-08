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
	call := &sfCall{done: make(chan struct{})}
	c.sf[key] = call
	c.sfMu.Unlock()

	// Snapshot the invalidation marker BEFORE compute runs (winner-only path).
	c.mu.Lock()
	genBefore, hadTomb := c.tombstones[key]
	c.mu.Unlock()

	value, ttl, err := compute()

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
