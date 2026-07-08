package cache

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeXLock is a controllable, in-process CrossProcessLock test double.
// Per §11.4.27 mocks/fakes are permitted ONLY in unit tests — this file is
// exactly that: it exercises computeGuarded's decision logic in isolation
// from any real OS-level locking primitive. The genuine cross-process claim
// (multiple real OS processes, real flock, real shared on-disk Store) is
// proven separately and for real in crossprocess_multiprocess_test.go.
type fakeXLock struct {
	mu sync.Mutex

	// grant, when true, makes the NEXT TryLock call for any key succeed;
	// held is then true until the returned unlock is invoked. When grant is
	// false, TryLock reports contention (ok=false, err=nil) unless failErr
	// is set, in which case it reports that error instead.
	grant   bool
	failErr error

	held        map[string]bool
	tryLockN    int32 // total TryLock invocations across all keys
	unlockN     int32 // total unlock invocations
	unlockOrder []string
}

func newFakeXLock() *fakeXLock {
	return &fakeXLock{held: make(map[string]bool)}
}

func (f *fakeXLock) TryLock(key string) (func() error, bool, error) {
	atomic.AddInt32(&f.tryLockN, 1)

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.failErr != nil {
		return nil, false, f.failErr
	}
	if !f.grant || f.held[key] {
		return nil, false, nil
	}
	f.held[key] = true
	return func() error {
		atomic.AddInt32(&f.unlockN, 1)
		f.mu.Lock()
		delete(f.held, key)
		f.unlockOrder = append(f.unlockOrder, key)
		f.mu.Unlock()
		return nil
	}, true, nil
}

// TestGetOrCompute_CrossProcessLock_WinnerComputesAndUnlocks proves the
// happy path: when this process wins the cross-process lock (grant=true),
// GetOrCompute computes exactly as it always has, THEN releases the lock
// exactly once. A CrossProcessLock configured this way must never leave the
// lock held after GetOrCompute returns — a leaked lock would deadlock every
// future contender for the same key.
func TestGetOrCompute_CrossProcessLock_WinnerComputesAndUnlocks(t *testing.T) {
	fc := newFakeClock(baseTime)
	xl := newFakeXLock()
	xl.grant = true
	c := New(WithClock(fc.Now), WithCrossProcessLock(xl))

	var calls int32
	compute := func() (string, time.Duration, error) {
		atomic.AddInt32(&calls, 1)
		return "winner-value", 0, nil
	}

	got, err := c.GetOrCompute("k", compute)
	if err != nil {
		t.Fatalf("GetOrCompute err: %v", err)
	}
	if got != "winner-value" {
		t.Fatalf("GetOrCompute = %q, want winner-value", got)
	}
	if n := atomic.LoadInt32(&calls); n != 1 {
		t.Fatalf("compute called %d times, want exactly 1", n)
	}
	if n := atomic.LoadInt32(&xl.tryLockN); n != 1 {
		t.Fatalf("TryLock called %d times, want exactly 1", n)
	}
	if n := atomic.LoadInt32(&xl.unlockN); n != 1 {
		t.Fatalf("unlock called %d times, want exactly 1 (a leaked lock deadlocks every future contender)", n)
	}

	// The value must also be genuinely cached — a subsequent Get is a hit
	// with no further compute.
	if v, hit, gerr := c.Get("k"); gerr != nil || !hit || v != "winner-value" {
		t.Fatalf("post-compute Get = (%q, %v, %v), want (winner-value, true, nil)", v, hit, gerr)
	}
}

// TestGetOrCompute_CrossProcessLock_ComputeErrorStillUnlocks proves the lock
// is released even when the guarded compute fails — an error path must not
// be a way to leak the cross-process lock forever.
func TestGetOrCompute_CrossProcessLock_ComputeErrorStillUnlocks(t *testing.T) {
	fc := newFakeClock(baseTime)
	xl := newFakeXLock()
	xl.grant = true
	c := New(WithClock(fc.Now), WithCrossProcessLock(xl))

	wantErr := errors.New("boom")
	compute := func() (string, time.Duration, error) {
		return "", 0, wantErr
	}

	_, err := c.GetOrCompute("k", compute)
	if !errors.Is(err, wantErr) {
		t.Fatalf("GetOrCompute err = %v, want %v", err, wantErr)
	}
	if n := atomic.LoadInt32(&xl.unlockN); n != 1 {
		t.Fatalf("unlock called %d times after a compute error, want exactly 1", n)
	}
}

// TestGetOrCompute_CrossProcessLock_LoserFindsWinnerResultInSharedStore
// proves the cross-process FOLLOWER path end to end using two independent
// Cache instances (simulating two OS processes) sharing ONE Store — the
// follower loses the (fake) cross-process lock race, polls, and returns the
// winner's value WITHOUT ever invoking its own compute. This is the
// in-process-fake analogue of the real multi-OS-process proof in
// crossprocess_multiprocess_test.go.
func TestGetOrCompute_CrossProcessLock_LoserFindsWinnerResultInSharedStore(t *testing.T) {
	shared := newMemStore()
	fc := newFakeClock(baseTime)

	// "Process A" (the winner): grants itself the lock and writes to the
	// shared store the moment it computes.
	winnerLock := newFakeXLock()
	winnerLock.grant = true
	winner := New(WithClock(fc.Now), WithStore(shared), WithCrossProcessLock(winnerLock))

	// "Process B" (the loser): its lock ALWAYS reports contention (another
	// process holds it), so it must fall through to polling the shared
	// Store rather than computing itself.
	loserLock := newFakeXLock() // grant stays false: every TryLock call reports contention
	loser := New(WithClock(fc.Now), WithStore(shared),
		WithCrossProcessLock(loserLock), WithCrossProcessWait(time.Millisecond, 2*time.Second))

	var winnerCalls, loserCalls int32
	winnerCompute := func() (string, time.Duration, error) {
		atomic.AddInt32(&winnerCalls, 1)
		return "shared-value", 0, nil
	}
	loserCompute := func() (string, time.Duration, error) {
		atomic.AddInt32(&loserCalls, 1)
		return "SHOULD-NEVER-BE-RETURNED", 0, nil
	}

	// Run the loser's GetOrCompute in the background FIRST so it genuinely
	// starts polling an empty store before the winner ever writes to it —
	// exercising the real "not yet found, keep polling" loop rather than
	// finding the value on the very first Get.
	loserDone := make(chan struct{})
	var loserVal string
	var loserErr error
	go func() {
		loserVal, loserErr = loser.GetOrCompute("k", loserCompute)
		close(loserDone)
	}()

	time.Sleep(20 * time.Millisecond) // let the loser observe at least one empty poll

	winVal, winErr := winner.GetOrCompute("k", winnerCompute)
	if winErr != nil {
		t.Fatalf("winner GetOrCompute err: %v", winErr)
	}
	if winVal != "shared-value" {
		t.Fatalf("winner GetOrCompute = %q, want shared-value", winVal)
	}

	select {
	case <-loserDone:
	case <-time.After(3 * time.Second):
		t.Fatal("loser GetOrCompute did not return within 3s — it should have found the winner's value via the shared store")
	}

	if loserErr != nil {
		t.Fatalf("loser GetOrCompute err: %v", loserErr)
	}
	if loserVal != "shared-value" {
		t.Fatalf("loser GetOrCompute = %q, want shared-value (the WINNER's result, found via the shared store)", loserVal)
	}
	if n := atomic.LoadInt32(&winnerCalls); n != 1 {
		t.Fatalf("winner compute called %d times, want exactly 1", n)
	}
	if n := atomic.LoadInt32(&loserCalls); n != 0 {
		t.Fatalf("loser compute called %d times, want exactly 0 (this is the whole point of the cross-process guard)", n)
	}
}

// TestGetOrCompute_CrossProcessLock_TimeoutFallsBackToComputing proves the
// honest degrade path: when nobody ever publishes a result within the wait
// budget (no shared Store, or the winner is simply never going to finish in
// this scenario), the loser MUST fall back to computing itself rather than
// hanging forever or fabricating a value. This is the §11.4.6 "never bluff,
// never hang" requirement for the case where the precondition (a shared
// Store making the winner's result observable) does not hold.
func TestGetOrCompute_CrossProcessLock_TimeoutFallsBackToComputing(t *testing.T) {
	fc := newFakeClock(baseTime)
	xl := newFakeXLock() // grant stays false: permanent (simulated) contention
	// No WithStore: even if a "winner" existed, nothing could ever be
	// observed cross-process, so the fallback path is the ONLY correct
	// outcome here.
	c := New(WithClock(fc.Now), WithCrossProcessLock(xl),
		WithCrossProcessWait(time.Millisecond, 20*time.Millisecond))

	var calls int32
	compute := func() (string, time.Duration, error) {
		atomic.AddInt32(&calls, 1)
		return "fallback-value", 0, nil
	}

	start := time.Now()
	got, err := c.GetOrCompute("k", compute)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("GetOrCompute err: %v", err)
	}
	if got != "fallback-value" {
		t.Fatalf("GetOrCompute = %q, want fallback-value", got)
	}
	if n := atomic.LoadInt32(&calls); n != 1 {
		t.Fatalf("compute called %d times, want exactly 1 (the honest fallback, not zero — a hang or a fabricated value would both be worse)", n)
	}
	if elapsed < 20*time.Millisecond {
		t.Fatalf("GetOrCompute returned after %v, want >= the configured 20ms wait budget (it must have genuinely waited before falling back)", elapsed)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("GetOrCompute took %v — the fallback must not hang far beyond the configured wait budget", elapsed)
	}
}

// TestGetOrCompute_CrossProcessLock_BackendErrorDegradesGracefully proves
// that a TryLock backend ERROR (e.g. an unwritable lock directory) is
// treated exactly like ordinary contention — never escalated into a failed
// GetOrCompute call for the caller's actual request (§11.4.1: an optional
// guard's own infra failure must not become the caller's failure).
func TestGetOrCompute_CrossProcessLock_BackendErrorDegradesGracefully(t *testing.T) {
	fc := newFakeClock(baseTime)
	xl := newFakeXLock()
	xl.failErr = errors.New("lock backend unavailable")
	c := New(WithClock(fc.Now), WithCrossProcessLock(xl),
		WithCrossProcessWait(time.Millisecond, 10*time.Millisecond))

	var calls int32
	compute := func() (string, time.Duration, error) {
		atomic.AddInt32(&calls, 1)
		return "computed-despite-lock-error", 0, nil
	}

	got, err := c.GetOrCompute("k", compute)
	if err != nil {
		t.Fatalf("GetOrCompute err = %v, want nil (a lock-backend error must never fail the caller's request)", err)
	}
	if got != "computed-despite-lock-error" {
		t.Fatalf("GetOrCompute = %q, want computed-despite-lock-error", got)
	}
	if n := atomic.LoadInt32(&calls); n != 1 {
		t.Fatalf("compute called %d times, want exactly 1", n)
	}
}

// TestGetOrCompute_NoCrossProcessLock_UnchangedBehaviour is a guard-rail
// proving the additive-only contract: a Cache with NO CrossProcessLock
// configured (the default, exercised by every pre-existing GetOrCompute
// test in singleflight_test.go) computes exactly once on a miss, exactly as
// before this feature existed.
func TestGetOrCompute_NoCrossProcessLock_UnchangedBehaviour(t *testing.T) {
	fc := newFakeClock(baseTime)
	c := New(WithClock(fc.Now))

	var calls int32
	compute := func() (string, time.Duration, error) {
		atomic.AddInt32(&calls, 1)
		return "v", 0, nil
	}

	got, err := c.GetOrCompute("k", compute)
	if err != nil {
		t.Fatalf("GetOrCompute err: %v", err)
	}
	if got != "v" || atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("GetOrCompute = (%q, calls=%d), want (v, 1)", got, calls)
	}
}
