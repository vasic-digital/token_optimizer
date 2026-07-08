//go:build unix

package cache

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// FileLock is the reference CrossProcessLock implementation the WS6 caching
// design specifies verbatim: an advisory flock(2) on a per-key lock file
// under a caller-supplied directory
// (docs/research/tokens/ws6_caching_sync/DESIGN.md §5 — "Single-flight via
// flock on key-named lockfiles under a cache-scratch dir; matches the
// project's existing lock idiom; §11.4.180 stale-lock reap applies").
//
// flock is held by the kernel against the OPEN FILE DESCRIPTION, so a
// process that crashes (or is killed) while holding a lock has it released
// automatically by the kernel the moment its file descriptors are torn down
// — there is no stale lock FILE requiring a PID-liveness reap the way the
// project's own script-level `.commit_all.lock`/`.push_all.lock` idiom does
// (§11.4.180 is about THAT class of lock; flock's kernel-owned semantics
// make it structurally immune to the "dead holder never released it"
// failure mode those locks must defend against).
//
// Unix-only (this file's build tag): flock(2) is a POSIX primitive with no
// direct Windows equivalent — Windows' LockFileEx is mandatory (not
// advisory) whole/byte-range locking with different failure semantics, not
// a drop-in substitute. Rather than silently emulating a weaker guarantee
// on non-unix platforms, NewFileLock on those platforms returns an honest
// error (filelock_other.go) so a consumer discovers the gap at
// construction time, never as a silent correctness hole (§11.4.6).
type FileLock struct {
	dir string

	// heldMu serialises this PROCESS's own bookkeeping of which lock files it
	// currently has open. It plays no role in cross-process exclusion — that
	// guarantee comes entirely from the kernel-level flock(2) call below —
	// it only protects the held map from concurrent TryLock/unlock calls
	// made by different goroutines within this same process.
	heldMu sync.Mutex
	held   map[string]*os.File
}

// NewFileLock returns a FileLock whose per-key lock files live under dir.
// dir is created (including any missing parents) if it does not already
// exist. dir MUST be the SAME path across every process sharing the guarded
// Cache — that shared path is what makes the resulting lock cross-process.
func NewFileLock(dir string) (*FileLock, error) {
	if dir == "" {
		return nil, fmt.Errorf("cache: FileLock directory must be non-empty")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("cache: FileLock mkdir %q: %w", dir, err)
	}
	return &FileLock{dir: dir, held: make(map[string]*os.File)}, nil
}

func (fl *FileLock) path(key string) string {
	return filepath.Join(fl.dir, hashFilename(key, ".lock"))
}

// TryLock implements CrossProcessLock via a non-blocking exclusive flock on
// key's lock file. See the CrossProcessLock doc comment (crossprocess.go)
// for the ok/err contract this method fulfils.
func (fl *FileLock) TryLock(key string) (unlock func() error, ok bool, err error) {
	if key == "" {
		return nil, false, ErrEmptyKey
	}
	path := fl.path(key)

	f, oerr := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if oerr != nil {
		return nil, false, fmt.Errorf("cache: FileLock open %q: %w", path, oerr)
	}

	if ferr := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); ferr != nil {
		_ = f.Close()
		if ferr == syscall.EWOULDBLOCK {
			// Held by another process right now — the expected contention
			// outcome, not a failure of the lock mechanism itself.
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("cache: FileLock flock %q: %w", path, ferr)
	}

	fl.heldMu.Lock()
	fl.held[path] = f
	fl.heldMu.Unlock()

	return func() error {
		fl.heldMu.Lock()
		delete(fl.held, path)
		fl.heldMu.Unlock()
		// Explicit LOCK_UN before Close makes the release observable to a
		// waiting process as promptly as the kernel allows, rather than
		// relying solely on the implicit release that occurs when the last
		// file descriptor referencing this open file description is closed.
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		return f.Close()
	}, true, nil
}
