package tier

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

// TestRegistry_ConcurrentRegisterAndDispatch_RaceSafe drives Register and
// Dispatch from many goroutines at once. Run under `go test -race` it proves the
// registry's RWMutex protects the executor map: concurrent registration of
// distinct tiers and concurrent dispatch to already-registered tiers must not
// race, deadlock, or panic. A missing lock would trip the race detector here.
func TestRegistry_ConcurrentRegisterAndDispatch_RaceSafe(t *testing.T) {
	r := New()

	const preRegistered = 32
	for i := 0; i < preRegistered; i++ {
		name := fmt.Sprintf("pre-%d", i)
		if err := r.Register(name, ExecutorFunc(func(_ context.Context, req Request) (Response, error) {
			return Response{Payload: req.Payload}, nil
		})); err != nil {
			t.Fatalf("pre-register %q: %v", name, err)
		}
	}

	ctx := context.Background()
	var wg sync.WaitGroup

	// Writers: each registers a distinct new tier (no key collision, so no
	// duplicate error expected).
	const writers = 64
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("new-%d", i)
			if err := r.Register(name, ExecutorFunc(func(_ context.Context, req Request) (Response, error) {
				return Response{Payload: req.Payload}, nil
			})); err != nil {
				t.Errorf("concurrent Register(%q): %v", name, err)
			}
		}(i)
	}

	// Readers: each dispatches to a pre-registered tier repeatedly.
	const readers = 64
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("pre-%d", i%preRegistered)
			for j := 0; j < 32; j++ {
				resp, err := r.Dispatch(ctx, name, Request{Payload: j})
				if err != nil {
					t.Errorf("concurrent Dispatch(%q): %v", name, err)
					return
				}
				if resp.Tier != name || resp.Payload != j {
					t.Errorf("concurrent Dispatch(%q) resp = %+v", name, resp)
					return
				}
			}
		}(i)
	}

	// Miss-readers: each dispatches to a never-registered tier and must get the
	// honest error even under contention (no fabricated success mid-race).
	const missReaders = 32
	for i := 0; i < missReaders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("never-%d", i)
			_, err := r.Dispatch(ctx, name, Request{})
			if !errors.Is(err, ErrNoExecutorForTier) {
				t.Errorf("concurrent Dispatch(%q) err = %v, want ErrNoExecutorForTier", name, err)
			}
		}(i)
	}

	wg.Wait()

	// Every writer's tier must now be registered and dispatchable.
	for i := 0; i < writers; i++ {
		name := fmt.Sprintf("new-%d", i)
		if _, ok := r.Executor(name); !ok {
			t.Fatalf("writer tier %q missing after concurrent registration", name)
		}
	}
}
