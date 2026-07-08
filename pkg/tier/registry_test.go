package tier

import (
	"context"
	"errors"
	"testing"
)

// recordingExecutor is a test executor that records the request it received and
// returns a caller-chosen response and error. It stands in for the
// consumer-supplied real executors (shell-out / local model / alias / native);
// per §11.4.27 mocks like this belong only in unit tests, and pkg/tier has no
// real backend of its own to exercise.
type recordingExecutor struct {
	name     string  // an identity the test can assert routed correctly
	gotReq   Request // the request Execute was handed
	gotCtxOK bool    // whether the passed context was the one the test sent
	wantCtx  context.Context
	resp     Response
	err      error
	calls    int
}

func (e *recordingExecutor) Execute(ctx context.Context, req Request) (Response, error) {
	e.calls++
	e.gotReq = req
	e.gotCtxOK = ctx == e.wantCtx
	if e.err != nil {
		return Response{}, e.err
	}
	return e.resp, nil
}

func TestRegister_Validation(t *testing.T) {
	valid := ExecutorFunc(func(context.Context, Request) (Response, error) { return Response{}, nil })

	tests := []struct {
		name    string
		tier    string
		exec    Executor
		wantErr error
	}{
		{name: "valid", tier: "T-DET", exec: valid, wantErr: nil},
		{name: "empty tier name", tier: "", exec: valid, wantErr: ErrEmptyTierName},
		{name: "nil interface executor", tier: "T-X", exec: nil, wantErr: ErrNilExecutor},
		{name: "typed-nil ExecutorFunc", tier: "T-Y", exec: ExecutorFunc(nil), wantErr: ErrNilExecutor},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := New()
			err := r.Register(tt.tier, tt.exec)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Register(%q) err = %v, want errors.Is %v", tt.tier, err, tt.wantErr)
			}
		})
	}
}

func TestRegister_DuplicateRejected(t *testing.T) {
	r := New()
	exec := ExecutorFunc(func(context.Context, Request) (Response, error) { return Response{}, nil })
	if err := r.Register("T-DET", exec); err != nil {
		t.Fatalf("first Register: %v", err)
	}
	err := r.Register("T-DET", exec)
	if !errors.Is(err, ErrDuplicateExecutor) {
		t.Fatalf("duplicate Register err = %v, want errors.Is ErrDuplicateExecutor", err)
	}
}

// TestDispatch_RoutesToCorrectExecutor proves Dispatch invokes exactly the
// executor registered under the requested tier and no other. It fails if routing
// picks the wrong executor or invokes more than one.
func TestDispatch_RoutesToCorrectExecutor(t *testing.T) {
	ctx := context.Background()
	det := &recordingExecutor{name: "det", wantCtx: ctx, resp: Response{Payload: "det-out"}}
	loc := &recordingExecutor{name: "loc", wantCtx: ctx, resp: Response{Payload: "loc-out"}}

	r := New()
	if err := r.Register("T-DET", det); err != nil {
		t.Fatalf("register det: %v", err)
	}
	if err := r.Register("T-LOCAL", loc); err != nil {
		t.Fatalf("register loc: %v", err)
	}

	cases := []struct {
		tier    string
		want    *recordingExecutor
		other   *recordingExecutor
		wantOut string
	}{
		{"T-DET", det, loc, "det-out"},
		{"T-LOCAL", loc, det, "loc-out"},
	}
	for _, c := range cases {
		t.Run(c.tier, func(t *testing.T) {
			c.want.calls, c.other.calls = 0, 0
			resp, err := r.Dispatch(ctx, c.tier, Request{Payload: "in"})
			if err != nil {
				t.Fatalf("Dispatch(%q): %v", c.tier, err)
			}
			if c.want.calls != 1 {
				t.Fatalf("target executor called %d times, want 1", c.want.calls)
			}
			if c.other.calls != 0 {
				t.Fatalf("wrong executor called %d times, want 0 (misrouted)", c.other.calls)
			}
			if resp.Payload != c.wantOut {
				t.Fatalf("resp.Payload = %v, want %v", resp.Payload, c.wantOut)
			}
			if resp.Tier != c.tier {
				t.Fatalf("resp.Tier = %q, want %q (evidence stamp)", resp.Tier, c.tier)
			}
			// The executor must have been handed the routing tier + the caller's
			// context + the caller's payload.
			if c.want.gotReq.Tier != c.tier {
				t.Fatalf("executor got req.Tier %q, want %q", c.want.gotReq.Tier, c.tier)
			}
			if c.want.gotReq.Payload != "in" {
				t.Fatalf("executor got req.Payload %v, want %q", c.want.gotReq.Payload, "in")
			}
			if !c.want.gotCtxOK {
				t.Fatalf("executor did not receive the caller's context")
			}
		})
	}
}

// TestDispatch_UnregisteredTier_HonestErrorNeverFabricated is the load-bearing
// honesty test (§11.4.6). Dispatching to a tier with no executor MUST return an
// explicit ErrNoExecutorForTier with the ZERO response — never a silent success
// or a fabricated payload.
//
// Negation verification: if Dispatch silently fabricated a response for a
// missing tier (e.g. `return Response{}, nil`), the `err == nil` branch below
// would fire t.Fatalf and FAIL the test; and if it returned a non-zero
// Response.Payload, the payload-is-nil assertion would FAIL it. So the test
// cannot pass on a bluffing Dispatch — it catches the negation of the honesty
// guarantee.
func TestDispatch_UnregisteredTier_HonestErrorNeverFabricated(t *testing.T) {
	r := New()
	// One unrelated executor is registered to prove the miss is per-tier, not
	// "registry is empty".
	if err := r.Register("T-DET", ExecutorFunc(func(context.Context, Request) (Response, error) {
		return Response{Payload: "should-not-run"}, nil
	})); err != nil {
		t.Fatalf("register: %v", err)
	}

	resp, err := r.Dispatch(context.Background(), "T-MISSING", Request{Payload: "in"})
	if err == nil {
		t.Fatalf("Dispatch to unregistered tier returned nil error (fabricated success) — honesty guarantee broken")
	}
	if !errors.Is(err, ErrNoExecutorForTier) {
		t.Fatalf("err = %v, want errors.Is ErrNoExecutorForTier", err)
	}
	if resp.Payload != nil {
		t.Fatalf("resp.Payload = %v, want nil (no fabricated response)", resp.Payload)
	}
	if resp.Tier != "" {
		t.Fatalf("resp.Tier = %q, want empty (no fabricated evidence stamp)", resp.Tier)
	}
}

// TestDispatch_ExecutorError_SurfacedNotSwallowed proves a registered executor's
// error is returned to the caller verbatim (errors.Is-classifiable), with the
// zero response — never masked as a success.
func TestDispatch_ExecutorError_SurfacedNotSwallowed(t *testing.T) {
	boom := errors.New("backend exploded")
	r := New()
	if err := r.Register("T-LOCAL", ExecutorFunc(func(context.Context, Request) (Response, error) {
		return Response{Payload: "leaked-on-error"}, boom
	})); err != nil {
		t.Fatalf("register: %v", err)
	}

	resp, err := r.Dispatch(context.Background(), "T-LOCAL", Request{})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want errors.Is boom", err)
	}
	if resp.Payload != nil {
		t.Fatalf("resp.Payload = %v, want nil on error (must not leak partial response)", resp.Payload)
	}
}

func TestExecutorAccessorAndTiers(t *testing.T) {
	r := New()
	names := []string{"T-NATIVE", "T-DET", "T-LOCAL", "T-ALIAS"}
	for _, n := range names {
		if err := r.Register(n, ExecutorFunc(func(context.Context, Request) (Response, error) {
			return Response{}, nil
		})); err != nil {
			t.Fatalf("register %q: %v", n, err)
		}
	}
	if _, ok := r.Executor("T-DET"); !ok {
		t.Fatalf("Executor(T-DET) not found after Register")
	}
	if _, ok := r.Executor("T-NONE"); ok {
		t.Fatalf("Executor(T-NONE) reported present for an unregistered tier")
	}
	got := r.Tiers()
	want := []string{"T-ALIAS", "T-DET", "T-LOCAL", "T-NATIVE"} // sorted ascending, deterministic
	if len(got) != len(want) {
		t.Fatalf("Tiers() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Tiers()[%d] = %q, want %q (non-deterministic order)", i, got[i], want[i])
		}
	}
}

// TestDispatch_Deterministic proves repeated dispatch of the same request routes
// identically. Combined with `go test -count=3` this exercises §11.4.50
// deterministic consistency.
func TestDispatch_Deterministic(t *testing.T) {
	r := New()
	if err := r.Register("T-DET", ExecutorFunc(func(_ context.Context, req Request) (Response, error) {
		return Response{Payload: req.Payload}, nil
	})); err != nil {
		t.Fatalf("register: %v", err)
	}
	for i := 0; i < 50; i++ {
		resp, err := r.Dispatch(context.Background(), "T-DET", Request{Payload: "x"})
		if err != nil {
			t.Fatalf("iter %d: %v", i, err)
		}
		if resp.Payload != "x" || resp.Tier != "T-DET" {
			t.Fatalf("iter %d: resp = %+v, want {T-DET x}", i, resp)
		}
	}
}
