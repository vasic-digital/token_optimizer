//go:build unix

package cache

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// This file provides the REAL multi-OS-process proof the WS6 cross-process
// single-flight guard requires: it re-executes the compiled test binary
// itself as N independent child processes (the standard Go
// "helper-process" pattern — see the stdlib's os/exec_test.go for the
// original idiom), each contending on the SAME cache key against a SHARED,
// on-disk FileLock directory and FileStore directory. This is
// deliberately NOT goroutines-in-one-process: goroutines share memory and
// prove nothing about the flock(2) syscall's cross-process semantics, which
// is the entire point of this increment.
//
// Env-var protocol between the parent test and each child (kept minimal and
// explicit rather than reusing any project-specific config surface —
// §11.4.28 decoupling: this is test-only wiring, not part of the package's
// public API):
//
//	TOKOPT_XP_HELPER=1                 marks this process as a helper (see TestMain)
//	TOKOPT_XP_LOCKDIR=<path>           shared FileLock directory
//	TOKOPT_XP_STOREDIR=<path>          shared FileStore directory
//	TOKOPT_XP_COUNTERFILE=<path>       every REAL invocation of compute() appends its PID here
//	TOKOPT_XP_KEY=<string>             the cache key every child contends on
//	TOKOPT_XP_COMPUTE_SLEEP_MS=<int>   how long the winner's compute() sleeps,
//	                                   widening the contention window so losers
//	                                   are very likely still waiting when it starts

const (
	xpHelperEnv    = "TOKOPT_XP_HELPER"
	xpLockDirEnv   = "TOKOPT_XP_LOCKDIR"
	xpStoreDirEnv  = "TOKOPT_XP_STOREDIR"
	xpCounterEnv   = "TOKOPT_XP_COUNTERFILE"
	xpKeyEnv       = "TOKOPT_XP_KEY"
	xpSleepMsEnv   = "TOKOPT_XP_COMPUTE_SLEEP_MS"
	xpComputedTag  = "computed-by-real-process"
	xpChildTimeout = 10 * time.Second
)

// TestMain intercepts helper-process re-invocations of this same compiled
// test binary before the normal `go test` machinery ever runs: when
// TOKOPT_XP_HELPER=1 is set, this process does ONLY the cross-process
// GetOrCompute call described by the env vars above, prints its outcome to
// stdout, and exits — it never calls m.Run(), so it never behaves like a
// test binary at all from the parent's point of view (matching the
// long-standing Go stdlib helper-process idiom).
func TestMain(m *testing.M) {
	if os.Getenv(xpHelperEnv) == "1" {
		os.Exit(runCrossProcessHelper())
	}
	os.Exit(m.Run())
}

// runCrossProcessHelper is the entire body of one "OS process" contender in
// TestGetOrCompute_CrossProcessLock_RealMultiProcess_ExactlyOneComputes
// below. It is intentionally NOT a *testing.T-based test itself — it is a
// plain program that happens to live in a _test.go file so it can reuse the
// package's unexported types (Cache, CrossProcessLock, etc.) directly.
func runCrossProcessHelper() int {
	lockDir := os.Getenv(xpLockDirEnv)
	storeDir := os.Getenv(xpStoreDirEnv)
	counterFile := os.Getenv(xpCounterEnv)
	key := os.Getenv(xpKeyEnv)
	sleepMs, _ := strconv.Atoi(os.Getenv(xpSleepMsEnv))

	lock, err := NewFileLock(lockDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "helper: NewFileLock: %v\n", err)
		return 1
	}
	store, err := NewFileStore(storeDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "helper: NewFileStore: %v\n", err)
		return 1
	}

	c := New(
		WithStore(store),
		WithCrossProcessLock(lock),
		WithCrossProcessWait(10*time.Millisecond, 8*time.Second),
	)

	compute := func() (string, time.Duration, error) {
		// This line runs IFF this OS process is the one that actually
		// performs the expensive computation — recorded as ground truth,
		// independent of (and cross-checked against) the value every
		// process ultimately returns. Appending a single short line with
		// O_APPEND is atomic against other O_APPEND writers to the same
		// file for local filesystems (the same guarantee syslog-style
		// appenders rely on), so concurrent writers here — which would
		// only happen if the cross-process guard has a bug — can never
		// corrupt each other's lines into one, which would hide a defect.
		appendCounterLine(counterFile, os.Getpid())
		if sleepMs > 0 {
			time.Sleep(time.Duration(sleepMs) * time.Millisecond)
		}
		return xpComputedTag, 0, nil
	}

	value, gerr := c.GetOrCompute(key, compute)
	if gerr != nil {
		fmt.Fprintf(os.Stderr, "helper: GetOrCompute: %v\n", gerr)
		return 1
	}
	// The parent reads this exact line from the child's captured stdout.
	fmt.Fprintf(os.Stdout, "VALUE:%s\n", value)
	return 0
}

// appendCounterLine records that this process's compute() genuinely ran.
func appendCounterLine(path string, pid int) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		// A failure to record is itself a test-infrastructure defect, not
		// a product defect — but there is no *testing.T in this helper
		// process to report it through, so fail loudly on stderr, which
		// the parent test also captures and surfaces on any child failure.
		fmt.Fprintf(os.Stderr, "helper: append counter line: %v\n", err)
		return
	}
	defer f.Close()
	fmt.Fprintf(f, "%d\n", pid)
}

// spawnHelper launches ONE child copy of this test binary configured as a
// cross-process contender, returning the started command so the caller can
// Wait() on it and inspect captured output.
func spawnHelper(t *testing.T, lockDir, storeDir, counterFile, key string, computeSleepMs int) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(),
		xpHelperEnv+"=1",
		xpLockDirEnv+"="+lockDir,
		xpStoreDirEnv+"="+storeDir,
		xpCounterEnv+"="+counterFile,
		xpKeyEnv+"="+key,
		xpSleepMsEnv+"="+strconv.Itoa(computeSleepMs),
	)
	return cmd
}

// TestGetOrCompute_CrossProcessLock_RealMultiProcess_ExactlyOneComputes is
// the §11.4.85-mandated REAL multi-process stress proof: N genuinely
// separate OS processes (not goroutines) contend on the identical cache key
// via a real flock(2)-based FileLock plus a real on-disk FileStore shared
// only through the filesystem — exactly the deployment shape the WS6 design
// describes for the project's own multi-track (/mnt/trackN) fleet. It
// asserts, from ground-truth evidence external to the cache's own return
// values (the counter file), that compute() genuinely executed in EXACTLY
// ONE of the N processes, and that every process nonetheless returned the
// identical, correct value.
func TestGetOrCompute_CrossProcessLock_RealMultiProcess_ExactlyOneComputes(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns real OS processes; skipped under -short")
	}

	dir := t.TempDir()
	lockDir := filepath.Join(dir, "locks")
	storeDir := filepath.Join(dir, "store")
	counterFile := filepath.Join(dir, "compute_counter.log")
	const (
		nProcesses     = 5
		key            = "shared-cross-process-key"
		computeSleepMs = 200 // widen the contention window well beyond process-spawn jitter
	)

	type result struct {
		idx    int
		stdout string
		stderr string
		err    error
	}

	cmds := make([]*exec.Cmd, nProcesses)
	outs := make([]*strings.Builder, nProcesses)
	errs := make([]*strings.Builder, nProcesses)
	for i := 0; i < nProcesses; i++ {
		cmds[i] = spawnHelper(t, lockDir, storeDir, counterFile, key, computeSleepMs)
		outs[i] = &strings.Builder{}
		errs[i] = &strings.Builder{}
		cmds[i].Stdout = outs[i]
		cmds[i].Stderr = errs[i]
	}

	// Start every child as close together as the OS scheduler allows —
	// genuine, uncoordinated concurrency across real processes, which is
	// what makes a subsequent finding of "exactly one compute" meaningful
	// rather than trivially true because they ran one after another.
	for i, cmd := range cmds {
		if err := cmd.Start(); err != nil {
			t.Fatalf("Start child %d: %v", i, err)
		}
	}

	results := make(chan result, nProcesses)
	for i, cmd := range cmds {
		go func(i int, cmd *exec.Cmd) {
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			select {
			case err := <-done:
				results <- result{idx: i, stdout: outs[i].String(), stderr: errs[i].String(), err: err}
			case <-time.After(xpChildTimeout):
				_ = cmd.Process.Kill()
				results <- result{idx: i, err: fmt.Errorf("child %d did not exit within %v", i, xpChildTimeout)}
			}
		}(i, cmd)
	}

	values := make([]string, nProcesses)
	for n := 0; n < nProcesses; n++ {
		r := <-results
		if r.err != nil {
			t.Fatalf("child %d failed: %v\nstdout=%q\nstderr=%q", r.idx, r.err, r.stdout, r.stderr)
		}
		v, ok := parseHelperValue(r.stdout)
		if !ok {
			t.Fatalf("child %d: could not parse VALUE: from stdout=%q stderr=%q", r.idx, r.stdout, r.stderr)
		}
		values[r.idx] = v
	}

	// Ground-truth check #1: every process returned the SAME, correct
	// value — proving the losers genuinely retrieved the WINNER's result
	// via the shared store rather than fabricating or mismatching it.
	for i, v := range values {
		if v != xpComputedTag {
			t.Errorf("child %d returned value %q, want %q (the shared, computed value)", i, v, xpComputedTag)
		}
	}

	// Ground-truth check #2 — the load-bearing assertion: exactly ONE
	// process's compute() closure genuinely ran, evidenced by the counter
	// file having exactly one PID line. This is independent of check #1: a
	// buggy guard could still coincidentally cache the right value while
	// having let multiple processes race into compute (e.g. if the
	// underlying data source is a pure function of static input) — the
	// counter file is what actually catches a stampede.
	raw, err := os.ReadFile(counterFile)
	if err != nil {
		t.Fatalf("read counter file %q: %v", counterFile, err)
	}
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	nonEmpty := lines[:0]
	for _, l := range lines {
		if strings.TrimSpace(l) != "" {
			nonEmpty = append(nonEmpty, l)
		}
	}
	if len(nonEmpty) != 1 {
		t.Fatalf("compute() ran %d times across %d real OS processes contending on one key, want exactly 1 (counter file contents: %q)",
			len(nonEmpty), nProcesses, string(raw))
	}
}

func parseHelperValue(stdout string) (string, bool) {
	for _, line := range strings.Split(stdout, "\n") {
		if v, found := strings.CutPrefix(line, "VALUE:"); found {
			return strings.TrimSpace(v), true
		}
	}
	return "", false
}
