//go:build !unix

package cache

import "fmt"

// FileLock is the flock-based CrossProcessLock reference implementation on
// unix platforms (see filelock_unix.go). flock(2) has no direct portable
// equivalent on this platform, so NewFileLock here returns an honest error
// rather than silently emulating a weaker (or absent) guarantee (§11.4.6).
// A consumer targeting this platform must supply its own CrossProcessLock
// implementation (e.g. a database advisory lock or a Redis-based one) —
// WithCrossProcessLock accepts any type satisfying the interface.
type FileLock struct{}

// NewFileLock always fails on this platform. See the file-level doc comment
// above for why.
func NewFileLock(dir string) (*FileLock, error) {
	return nil, fmt.Errorf("cache: FileLock is unix-only (flock(2) has no portable non-POSIX equivalent); requested dir=%q", dir)
}

// TryLock is unreachable in practice, since NewFileLock always fails first —
// it exists only so *FileLock satisfies CrossProcessLock on every platform,
// and honestly refuses rather than silently pretending to lock if ever
// called directly on a zero-value FileLock{}.
func (fl *FileLock) TryLock(key string) (unlock func() error, ok bool, err error) {
	return nil, false, fmt.Errorf("cache: FileLock is unix-only on this platform")
}
