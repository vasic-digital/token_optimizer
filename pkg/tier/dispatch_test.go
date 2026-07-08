package tier

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// register is a small test helper: registers an executor that returns the given
// response and error under name.
func registerExec(t *testing.T, r *Registry, name string, resp Response, err error) {
	t.Helper()
	if e := r.Register(name, ExecutorFunc(func(_ context.Context, req Request) (Response, error) {
		if err != nil {
			return Response{}, err
		}
		out := resp
		if out.Payload == nil {
			out.Payload = req.Payload
		}
		return out, nil
	})); e != nil {
		t.Fatalf("register %q: %v", name, e)
	}
}

func TestDispatchChain_FirstSuccessWins_OrderRespected(t *testing.T) {
	r := New()
	registerExec(t, r, "T-DET", Response{Payload: "det"}, nil)
	registerExec(t, r, "T-LOCAL", Response{Payload: "local"}, nil)
	registerExec(t, r, "T-NATIVE", Response{Payload: "native"}, nil)

	// All three live: the FIRST in the given order must win, proving no
	// reordering / no auto-downgrade — the framework honors the caller's order.
	resp, err := r.DispatchChain(context.Background(), []string{"T-LOCAL", "T-DET", "T-NATIVE"}, Request{})
	if err != nil {
		t.Fatalf("DispatchChain: %v", err)
	}
	if resp.Tier != "T-LOCAL" || resp.Payload != "local" {
		t.Fatalf("resp = %+v, want first-in-order T-LOCAL/local", resp)
	}
}

func TestDispatchChain_FallsThroughFailingToSucceeding(t *testing.T) {
	boom := errors.New("primary down")
	r := New()
	registerExec(t, r, "T-PRIMARY", Response{}, boom) // registered but failing
	registerExec(t, r, "T-BACKUP", Response{Payload: "backup"}, nil)

	resp, err := r.DispatchChain(context.Background(), []string{"T-PRIMARY", "T-BACKUP"}, Request{})
	if err != nil {
		t.Fatalf("DispatchChain: %v", err)
	}
	if resp.Tier != "T-BACKUP" || resp.Payload != "backup" {
		t.Fatalf("resp = %+v, want fallback to T-BACKUP", resp)
	}
}

func TestDispatchChain_UnregisteredEntrySkippedThenSucceeds(t *testing.T) {
	r := New()
	registerExec(t, r, "T-BACKUP", Response{Payload: "backup"}, nil)

	// First entry has no executor: it contributes ErrNoExecutorForTier and the
	// walk continues to the registered one.
	resp, err := r.DispatchChain(context.Background(), []string{"T-MISSING", "T-BACKUP"}, Request{})
	if err != nil {
		t.Fatalf("DispatchChain: %v", err)
	}
	if resp.Tier != "T-BACKUP" {
		t.Fatalf("resp.Tier = %q, want T-BACKUP", resp.Tier)
	}
}

// TestDispatchChain_AllFail_ExhaustedNeverFabricated is the chain honesty test:
// when every entry fails (one unregistered, one erroring) DispatchChain MUST
// return ErrChainExhausted wrapping BOTH causes — never a fabricated success.
//
// Negation verification: a bluffing DispatchChain that returned a synthesised
// Response with nil error would trip the `err == nil` t.Fatalf below and FAIL
// the test; a chain that swallowed the causes would fail the two errors.Is
// assertions.
func TestDispatchChain_AllFail_ExhaustedNeverFabricated(t *testing.T) {
	boom := errors.New("only tier down")
	r := New()
	registerExec(t, r, "T-DOWN", Response{}, boom)

	resp, err := r.DispatchChain(context.Background(), []string{"T-MISSING", "T-DOWN"}, Request{})
	if err == nil {
		t.Fatalf("all-fail DispatchChain returned nil error (fabricated success)")
	}
	if !errors.Is(err, ErrChainExhausted) {
		t.Fatalf("err = %v, want errors.Is ErrChainExhausted", err)
	}
	if !errors.Is(err, ErrNoExecutorForTier) {
		t.Fatalf("err = %v, want it to wrap the unregistered-tier cause", err)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want it to wrap the executor error cause", err)
	}
	if resp.Payload != nil {
		t.Fatalf("resp.Payload = %v, want nil on exhausted chain", resp.Payload)
	}
}

func TestDispatchChain_EmptyOrder_HonestError(t *testing.T) {
	r := New()
	resp, err := r.DispatchChain(context.Background(), nil, Request{})
	if !errors.Is(err, ErrEmptyChain) {
		t.Fatalf("err = %v, want errors.Is ErrEmptyChain", err)
	}
	if resp.Payload != nil {
		t.Fatalf("resp.Payload = %v, want nil for empty chain", resp.Payload)
	}
}

// TestDispatchChain_StopsAtFirstSuccess_NoFurtherInvocation proves the walk stops
// at the first success and never invokes later executors (no wasted downstream
// calls, no side effects past the winner).
func TestDispatchChain_StopsAtFirstSuccess_NoFurtherInvocation(t *testing.T) {
	r := New()
	var laterCalls int
	registerExec(t, r, "T-WIN", Response{Payload: "win"}, nil)
	if err := r.Register("T-LATER", ExecutorFunc(func(context.Context, Request) (Response, error) {
		laterCalls++
		return Response{Payload: "later"}, nil
	})); err != nil {
		t.Fatalf("register later: %v", err)
	}

	if _, err := r.DispatchChain(context.Background(), []string{"T-WIN", "T-LATER"}, Request{}); err != nil {
		t.Fatalf("DispatchChain: %v", err)
	}
	if laterCalls != 0 {
		t.Fatalf("later executor invoked %d times after an earlier success, want 0", laterCalls)
	}
}

// exampleUsage keeps the godoc-style composition honest and compiled: a consumer
// registers its own executors (here trivial stand-ins) and dispatches by the
// tier name pkg/router selected.
func Example() {
	r := New()
	_ = r.Register("T-DET", ExecutorFunc(func(_ context.Context, req Request) (Response, error) {
		return Response{Payload: "deterministic:" + fmt.Sprint(req.Payload)}, nil
	}))
	resp, err := r.Dispatch(context.Background(), "T-DET", Request{Payload: "triage"})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Printf("%s -> %v\n", resp.Tier, resp.Payload)
	// Output: T-DET -> deterministic:triage
}
