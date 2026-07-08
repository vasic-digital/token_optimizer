package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"time"
)

// CrossProcessLock is an optional, consumer-injected mutual-exclusion guard
// that extends GetOrCompute's in-process single-flight stampede guard
// (singleflight.go's sfMu/sf registry) ACROSS OS PROCESSES sharing the same
// L2 Store. The in-process registry coalesces N goroutines in ONE process
// missing the SAME key at once; CrossProcessLock coalesces N processes doing
// the same thing — e.g. this project's own multi-track fleet
// (/mnt/track1../mnt/trackN), each running its own long-lived agent process
// against one shared cache Store (docs/research/tokens/ws6_caching_sync/
// DESIGN.md §1's request-flow diagram: "store in L1/L2 (single-flight
// lock)"; §4's invalidation-trigger matrix row "Concurrent hot-key expiry
// (N contexts) -> single-flight lock"). Without this guard, N processes each
// missing one expensive key at once each independently pay the full compute
// cost — the cross-process analogue of the stampede §1 GetOrCompute already
// prevents within a single process.
//
// Decoupling (§11.4.28): the package hardcodes no locking technology, the
// same discipline cache.go already applies to L2 storage. The consumer
// supplies the mechanism: a flock-based FileLock (this package ships a
// reference implementation, see filelock_unix.go, matching the WS6 design's
// explicit "flock on key-named lockfiles under a cache-scratch dir"
// recommendation), a Redis SETNX/Redlock, a database advisory lock, or any
// other cross-process mutual-exclusion primitive whose scope spans every
// process sharing the Store. A Cache with no CrossProcessLock configured is
// unaffected — this is a strictly additive option (see WithCrossProcessLock).
type CrossProcessLock interface {
	// TryLock attempts to acquire an exclusive, cross-process lock scoped to
	// key, without blocking. ok == false means another process currently
	// holds it — this is the EXPECTED contention outcome under load, not an
	// error. err is reserved for the locking backend itself failing (e.g.
	// the lock directory is unwritable, a Redis connection dropped).
	// GetOrCompute treats a non-nil err identically to ok == false: it
	// degrades to waiting for the cross-process winner's result and, absent
	// one, computing directly (§11.4.1 — a hiccup in this OPTIONAL guard
	// must never escalate into a failed request for the caller's actual
	// computation; the guard can only ever REDUCE redundant work, never add
	// a new failure mode to callers who use it).
	//
	// unlock releases the lock and MUST be called exactly once whenever
	// ok == true, regardless of what the caller does next — including when
	// the guarded computation itself returns an error. unlock is nil
	// whenever ok == false or err != nil (there is nothing to release).
	TryLock(key string) (unlock func() error, ok bool, err error)
}

// Cross-process follower wait defaults. When WithCrossProcessLock is
// configured but WithCrossProcessWait was not called (or was called with a
// non-positive value for one of its arguments), a goroutine that loses the
// cross-process race for a key polls the shared Store at defaultCrossProcessPoll
// intervals, for up to defaultCrossProcessTimeout, waiting for the winning
// process to publish its result — see computeGuarded in singleflight.go.
// These are deliberately conservative (small poll granularity, seconds-scale
// budget): the wait is real wall-clock time spent waiting on a genuinely
// concurrent OTHER PROCESS this Cache instance cannot observe any other way,
// so — unlike the cache's TTL/invalidation logic — it cannot be virtualised
// through the injected Clock (§11.4.50's determinism guarantee covers
// decisions that are a pure function of stored timestamps; waiting for an
// external process to finish is not one of those decisions).
const (
	defaultCrossProcessPoll    = 25 * time.Millisecond
	defaultCrossProcessTimeout = 5 * time.Second
)

// WithCrossProcessLock installs the cross-process single-flight guard on a
// Cache constructed via New. A nil lock is ignored, leaving GetOrCompute's
// existing in-process-only guard exactly as it was — this option is purely
// additive and never required.
func WithCrossProcessLock(l CrossProcessLock) Option {
	return func(c *Cache) {
		if l != nil {
			c.xlock = l
		}
	}
}

// WithCrossProcessWait overrides the poll interval and wait budget a
// follower goroutine uses while waiting for a cross-process winner to
// publish its result (see WithCrossProcessLock and defaultCrossProcessPoll /
// defaultCrossProcessTimeout above). A non-positive poll or timeout is
// ignored, leaving the corresponding default in effect. Has no observable
// effect unless WithCrossProcessLock is also configured on the same Cache.
func WithCrossProcessWait(poll, timeout time.Duration) Option {
	return func(c *Cache) {
		if poll > 0 {
			c.xpoll = poll
		}
		if timeout > 0 {
			c.xtimeout = timeout
		}
	}
}

// hashFilename maps an opaque cache key to a filesystem-safe filename via a
// content hash, shared by the FileLock (filelock_unix.go) and FileStore
// (filestore.go) reference implementations. Going through a hash — rather
// than using key as a literal path component — is required because the
// package assumes no key schema (§11.4.28): a key may contain path
// separators, NUL bytes, or any other byte sequence a caller's own key
// scheme produces (see cache.go's L1/L2 key schema comment).
func hashFilename(key, ext string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:]) + ext
}
