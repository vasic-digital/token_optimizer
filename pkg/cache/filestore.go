package cache

import (
	"fmt"
	"os"
	"path/filepath"
)

// FileStore is a minimal, dependency-free, cross-platform reference
// implementation of the Store interface: one file per key under a
// caller-supplied directory, written atomically (temp file + rename, so a
// concurrent reader never observes a partially-written value).
//
// It exists so a genuinely shared, cross-process-VISIBLE L2 backing store is
// available out of the box for tests and simple deployments — in
// particular, it is what makes FileLock's cross-process guarantee
// independently verifiable by a real multi-process test (see
// crossprocess_multiprocess_test.go): a CrossProcessLock alone only bounds
// concurrent COMPUTATION, it says nothing about whether a process that lost
// the race can observe the winner's RESULT, which requires an actual shared
// Store. FileStore does not preempt the WS6 caching design's deferred
// production storage-technology decision (Redis vs SQLite, see
// docs/research/tokens/ws6_caching_sync/DESIGN.md §5) — it is the smallest
// honest thing that makes the cross-process story testable and usable today.
type FileStore struct {
	dir string
}

// NewFileStore returns a FileStore whose per-key files live under dir. dir
// is created (including any missing parents) if it does not already exist.
// dir MUST be the SAME path across every process meant to share this store.
func NewFileStore(dir string) (*FileStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("cache: FileStore directory must be non-empty")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("cache: FileStore mkdir %q: %w", dir, err)
	}
	return &FileStore{dir: dir}, nil
}

func (fs *FileStore) path(key string) string {
	return filepath.Join(fs.dir, hashFilename(key, ".json"))
}

// Get implements Store.
func (fs *FileStore) Get(key string) ([]byte, bool, error) {
	b, err := os.ReadFile(fs.path(key))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("cache: FileStore read %q: %w", key, err)
	}
	return b, true, nil
}

// Set implements Store via an atomic temp-file-then-rename write, so a
// concurrent Get on the same key (in this process or another one sharing
// dir) never observes a truncated or partially-written file.
func (fs *FileStore) Set(key string, value []byte) error {
	dst := fs.path(key)

	tmp, err := os.CreateTemp(fs.dir, "."+filepath.Base(dst)+".tmp-*")
	if err != nil {
		return fmt.Errorf("cache: FileStore tempfile for %q: %w", key, err)
	}
	tmpName := tmp.Name()

	if _, werr := tmp.Write(value); werr != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("cache: FileStore write %q: %w", key, werr)
	}
	if cerr := tmp.Close(); cerr != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("cache: FileStore close %q: %w", key, cerr)
	}
	if rerr := os.Rename(tmpName, dst); rerr != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("cache: FileStore rename %q: %w", key, rerr)
	}
	return nil
}

// Delete implements Store. Deleting an absent key is not an error, matching
// the Store interface contract.
func (fs *FileStore) Delete(key string) error {
	err := os.Remove(fs.path(key))
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("cache: FileStore delete %q: %w", key, err)
	}
	return nil
}
