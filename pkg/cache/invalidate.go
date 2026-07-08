package cache

import "fmt"

// Invalidate removes key from every layer and records a tombstone so no stale
// value survives the operation.
//
// It performs three actions, in order:
//
//  1. deletes the L1 in-memory entry, so an immediate re-Get cannot serve the
//     just-invalidated value from memory;
//  2. records a tombstone at the current clock time, so a value that physically
//     remains in L2 — because the store's Delete is best-effort or a no-op,
//     because the backend is eventually-consistent, or because a concurrent
//     writer re-inserted a value written at or before this moment — is treated
//     as invalid and NOT served (validLocked's StoredAt-after-tombstone test);
//  3. best-effort deletes the L2 entry. If the store's Delete errors it is
//     surfaced, but correctness does not depend on it succeeding: the tombstone
//     from step 2 already prevents a stale serve.
//
// This is the load-bearing distinction between "delete" and "invalidate": a
// plain delete that only removes the row would silently serve a stale value the
// instant a concurrent or lagging writer put it back; the tombstone makes the
// invalidation authoritative regardless of what the L2 store does (§11.4.6).
//
// The tombstone is cleared by a subsequent Set of the same key (writing a
// genuinely new value supersedes the invalidation), so a key that is repeatedly
// set-then-invalidated does not accumulate tombstones.
func (c *Cache) Invalidate(key string) error {
	if key == "" {
		return ErrEmptyKey
	}
	now := c.clock()

	c.mu.Lock()
	delete(c.l1, key)
	c.tombstones[key] = now
	c.mu.Unlock()

	if c.store != nil {
		if err := c.store.Delete(key); err != nil {
			return fmt.Errorf("cache: L2 delete %q: %w", key, err)
		}
	}
	return nil
}
