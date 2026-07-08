// Package tier is the tier-execution adapter framework of the token_optimizer
// engine: a thread-safe registry that maps a stable tier identifier (the
// config.Tier.Name that pkg/router selected) to a consumer-registered Executor,
// plus a Dispatch that routes a request to the executor for a given tier.
//
// # Decoupling (§11.4.28)
//
// The ACTUAL tier executors are ALL consumer-supplied and live in the consumer,
// never here: a deterministic tier shells out to a registered utility, a local
// tier calls a llama.cpp OpenAI-compatible endpoint, an alias tier issues a
// routed HTTP call, a native tier calls a vendor SDK. This package implements
// NONE of them and imports NONE of them. It provides only the framework — the
// registry, the Executor contract, and the routing — so the exact same dispatch
// logic serves any project's tier ladder. It ships ZERO project constants and
// imports nothing from the engine or any project; only the standard library.
//
// # Honesty guarantee (§11.4.6)
//
// Dispatch never fabricates a response. Routing to a tier that has no registered
// executor returns an explicit ErrNoExecutorForTier — never a silent success or
// a synthesised Response. An executor's own error is surfaced to the caller,
// never swallowed.
//
// # Resolve-by-name (§11.4.111)
//
// Executors are keyed by the tier's stable name, never by registration order.
package tier

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
)

// Registration and dispatch errors. They are sentinel values so callers can
// classify failures with errors.Is.
var (
	// ErrEmptyTierName is returned by Register when the tier name is empty.
	ErrEmptyTierName = errors.New("tier: tier name must be non-empty")
	// ErrNilExecutor is returned by Register when the executor is nil. A nil
	// executor could never run, and silently accepting it would turn a later
	// Dispatch into a nil-deref instead of an honest registration failure.
	ErrNilExecutor = errors.New("tier: executor must be non-nil")
	// ErrDuplicateExecutor is returned by Register (wrapped with the tier name)
	// when an executor is already registered for that tier. Registration is
	// explicit: overwriting is refused so a second Register cannot silently
	// shadow the first.
	ErrDuplicateExecutor = errors.New("tier: executor already registered")
	// ErrNoExecutorForTier is returned by Dispatch (wrapped with the tier name)
	// when no executor is registered for the requested tier. It is the
	// load-bearing honesty guarantee: an unrouted tier fails loudly rather than
	// returning a fabricated success (§11.4.6).
	ErrNoExecutorForTier = errors.New("tier: no executor registered for tier")
)

// Request is the opaque tier-dispatch request. The engine never inspects
// Payload — it is consumer data (a prompt, a messages array, a completion
// request, a shell-out spec). Tier records which registered tier the request is
// being dispatched to; Dispatch stamps it from its routing argument so an
// executor registered under several names can branch on the tier it is serving.
type Request struct {
	// Tier is the stable tier identifier this request is dispatched to. Dispatch
	// sets it from its tier argument before invoking the executor; any value the
	// caller sets is overwritten by the routing key so there is a single source
	// of truth.
	Tier string
	// Payload is opaque consumer data. The framework passes it through unchanged;
	// only the consumer's Executor interprets it.
	Payload any
}

// Response is the opaque tier-dispatch response. The engine never inspects
// Payload. Tier echoes the tier that produced the response — captured evidence
// (§11.4.69) of which executor actually ran, set by Dispatch on success.
type Response struct {
	// Tier is the tier whose executor produced this response.
	Tier string
	// Payload is the executor's opaque result. The framework passes it through
	// unchanged.
	Payload any
}

// Executor is the consumer-supplied contract for one tier. Execute runs the
// request against the tier's backend (a shell-out, a local model endpoint, a
// routed HTTP call, a vendor SDK) and returns its response or an error. The
// context carries cancellation and deadlines from the caller; a well-behaved
// executor honors it. Implementations MUST be safe for concurrent use — one
// registered Executor serves the whole context fleet.
type Executor interface {
	Execute(ctx context.Context, req Request) (Response, error)
}

// ExecutorFunc adapts a plain function to the Executor interface, mirroring
// http.HandlerFunc, so a consumer can register a closure without declaring a
// type.
type ExecutorFunc func(ctx context.Context, req Request) (Response, error)

// Execute calls f(ctx, req), satisfying Executor.
func (f ExecutorFunc) Execute(ctx context.Context, req Request) (Response, error) {
	return f(ctx, req)
}

// Registry maps stable tier names to consumer-registered executors. It is safe
// for concurrent use by multiple goroutines — the request path across the
// context fleet shares one Registry.
//
// A Registry MUST be constructed with New(); the zero value's nil map makes
// Register panic, matching the constructor-required contract of config.Config.
type Registry struct {
	mu        sync.RWMutex
	executors map[string]Executor
}

// New returns an empty Registry. The consumer registers one executor per tier
// before handing the Registry to the request path.
func New() *Registry {
	return &Registry{executors: make(map[string]Executor)}
}

// Register records the executor for a tier. It returns ErrEmptyTierName if name
// is empty, ErrNilExecutor if exec is nil, or ErrDuplicateExecutor (wrapped with
// the tier name) if an executor is already registered for that tier.
func (r *Registry) Register(name string, exec Executor) error {
	if name == "" {
		return ErrEmptyTierName
	}
	// A nil interface value OR a typed-nil ExecutorFunc are both unusable.
	if exec == nil {
		return ErrNilExecutor
	}
	if f, ok := exec.(ExecutorFunc); ok && f == nil {
		return ErrNilExecutor
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.executors[name]; exists {
		return fmt.Errorf("%w: %q", ErrDuplicateExecutor, name)
	}
	r.executors[name] = exec
	return nil
}

// Executor returns the executor registered for a tier and whether one exists.
// Resolution is by stable name (§11.4.111).
func (r *Registry) Executor(name string) (Executor, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.executors[name]
	return e, ok
}

// Tiers returns the registered tier names sorted ascending, so introspection is
// deterministic (§11.4.50). The returned slice is a copy.
func (r *Registry) Tiers() []string {
	r.mu.RLock()
	names := make([]string, 0, len(r.executors))
	for name := range r.executors {
		names = append(names, name)
	}
	r.mu.RUnlock()
	sort.Strings(names)
	return names
}

// Dispatch routes req to the executor registered for tier and returns its
// response. It looks the executor up by the stable tier name, stamps that name
// onto the request it hands the executor, invokes Execute, and on success stamps
// the tier onto the returned Response as captured evidence of which executor ran.
//
// If no executor is registered for tier, Dispatch returns the zero Response and
// ErrNoExecutorForTier (wrapped with the tier name) — never a fabricated success
// (§11.4.6). If the executor returns an error, Dispatch surfaces it unchanged
// (with the zero Response) rather than swallowing it.
func (r *Registry) Dispatch(ctx context.Context, tier string, req Request) (Response, error) {
	r.mu.RLock()
	exec, ok := r.executors[tier]
	r.mu.RUnlock()
	if !ok {
		return Response{}, fmt.Errorf("%w: %q", ErrNoExecutorForTier, tier)
	}
	req.Tier = tier
	resp, err := exec.Execute(ctx, req)
	if err != nil {
		return Response{}, err
	}
	resp.Tier = tier
	return resp, nil
}
