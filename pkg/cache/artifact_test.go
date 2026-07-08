package cache

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestArtifactKey_DeterministicAcrossRepeatedCalls proves ArtifactKey is a
// pure, deterministic function of its inputs: many calls over an IDENTICAL
// invocation (same tool, argv, git head, and input-path->content-hash map)
// must all derive the IDENTICAL key. Go's map iteration order is
// runtime-randomised per range, so this specifically exercises the
// path-sorting discipline ArtifactKey's doc comment claims — without it,
// the derived key would differ across calls purely due to iteration-order
// noise, an honest §11.4.111 stable-identity violation this test would
// catch.
func TestArtifactKey_DeterministicAcrossRepeatedCalls(t *testing.T) {
	inputs := map[string]string{
		"pkg/cache/cache.go":        "sha256:aaa",
		"pkg/cache/singleflight.go": "sha256:bbb",
		"go.mod":                    "sha256:ccc",
	}

	first := ArtifactKey("grep", []string{"-rn", "TODO", "."}, "deadbeef", inputs)
	if first == "" {
		t.Fatalf("ArtifactKey returned empty key")
	}
	for i := 0; i < 20; i++ {
		got := ArtifactKey("grep", []string{"-rn", "TODO", "."}, "deadbeef", inputs)
		if got != first {
			t.Fatalf("iteration %d: ArtifactKey = %q, want %q (map-order instability)", i, got, first)
		}
	}
}

// TestArtifactKey_ToolNameChange_DifferentKey proves the tool_name field
// participates in the derived identity: changing ONLY the tool name (argv,
// git head, and inputs held fixed) must change the key.
func TestArtifactKey_ToolNameChange_DifferentKey(t *testing.T) {
	argv := []string{"-rn", "TODO", "."}
	inputs := map[string]string{"a.go": "h1"}

	k1 := ArtifactKey("grep", argv, "head1", inputs)
	k2 := ArtifactKey("ripgrep", argv, "head1", inputs)
	if k1 == k2 {
		t.Fatalf("ArtifactKey identical across different tool names: %q", k1)
	}
}

// TestArtifactKey_ArgvChange_DifferentKey proves argv participates in the
// derived identity, including argv COUNT (not merely content) and ORDER —
// a different argument list for the same tool is a genuinely different
// invocation.
func TestArtifactKey_ArgvChange_DifferentKey(t *testing.T) {
	inputs := map[string]string{"a.go": "h1"}

	base := ArtifactKey("grep", []string{"-rn", "TODO"}, "head1", inputs)

	extraArg := ArtifactKey("grep", []string{"-rn", "TODO", "extra"}, "head1", inputs)
	if base == extraArg {
		t.Fatalf("ArtifactKey unchanged when an argv element was appended")
	}

	reordered := ArtifactKey("grep", []string{"TODO", "-rn"}, "head1", inputs)
	if base == reordered {
		t.Fatalf("ArtifactKey unchanged when argv order was swapped")
	}

	// Ambiguous-boundary regression guard: concatenating "ab"+"c" must not
	// collide with "a"+"bc" now that every field is length-prefixed.
	k1 := ArtifactKey("t", []string{"ab", "c"}, "h", nil)
	k2 := ArtifactKey("t", []string{"a", "bc"}, "h", nil)
	if k1 == k2 {
		t.Fatalf("ArtifactKey collided across an ambiguous argv concatenation boundary")
	}
}

// TestArtifactKey_GitHeadChange_DifferentKey proves git_head participates in
// the derived identity — the same tool+argv+inputs run against a different
// commit is a genuinely different artifact (e.g. the tool's own behaviour,
// or code it inspects beyond the declared inputs, may differ per commit).
func TestArtifactKey_GitHeadChange_DifferentKey(t *testing.T) {
	inputs := map[string]string{"a.go": "h1"}
	k1 := ArtifactKey("grep", []string{"-rn", "TODO"}, "deadbeef", inputs)
	k2 := ArtifactKey("grep", []string{"-rn", "TODO"}, "cafef00d", inputs)
	if k1 == k2 {
		t.Fatalf("ArtifactKey identical across different git_head values: %q", k1)
	}
}

// TestArtifactKey_DependencyFileEdited_KeyChangesAndMisses is the DESIGN's
// mandated §6 anti-bluff proof, restated for the pure key-derivation layer:
// "edit a dependency file -> assert the L3 key changes and the entry
// misses" (RED->GREEN per §11.4.115). This proves the KEY side: an input
// file's PATH staying the same while its CONTENT (and therefore its
// content hash) changes — the exact shape of "someone edited
// pkg/cache/cache.go" — MUST change the derived key. The
// GetOrComputeArtifact-level test below
// (TestGetOrComputeArtifact_DependencyEdit_ForcesRecomputeNotStaleServe)
// proves the consequence: a cache lookup under the new key is a genuine
// miss, never a resurrected stale hit.
func TestArtifactKey_DependencyFileEdited_KeyChangesAndMisses(t *testing.T) {
	before := map[string]string{
		"pkg/cache/cache.go": "sha256:before-edit",
		"go.mod":             "sha256:unchanged",
	}
	after := map[string]string{
		"pkg/cache/cache.go": "sha256:after-edit", // same path, edited content
		"go.mod":             "sha256:unchanged",
	}

	kBefore := ArtifactKey("pre_build_verification", []string{"--full"}, "deadbeef", before)
	kAfter := ArtifactKey("pre_build_verification", []string{"--full"}, "deadbeef", after)
	if kBefore == kAfter {
		t.Fatalf("ArtifactKey unchanged after editing a declared dependency file's content (stale-serve risk): %q", kBefore)
	}
}

// TestArtifactKey_InputPathAddedOrRemoved_DifferentKey proves the SET of
// declared input paths participates in the identity — a tool run that
// additionally depends on (or stops depending on) a file is a different
// invocation even if every other field is unchanged.
func TestArtifactKey_InputPathAddedOrRemoved_DifferentKey(t *testing.T) {
	base := map[string]string{"a.go": "h1"}
	withExtra := map[string]string{"a.go": "h1", "b.go": "h2"}

	k1 := ArtifactKey("grep", []string{"-rn"}, "head", base)
	k2 := ArtifactKey("grep", []string{"-rn"}, "head", withExtra)
	if k1 == k2 {
		t.Fatalf("ArtifactKey identical after adding a declared input path")
	}
}

// TestArtifactKey_EmptyInputsAndArgv_NoPanicAndDeterministic proves the
// boundary condition (§11.4.85: empty/off-by-one input): a tool invocation
// with no arguments and no declared inputs is a valid, well-defined key —
// it must not panic and must remain deterministic across calls.
func TestArtifactKey_EmptyInputsAndArgv_NoPanicAndDeterministic(t *testing.T) {
	k1 := ArtifactKey("noop", nil, "", nil)
	k2 := ArtifactKey("noop", []string{}, "", map[string]string{})
	if k1 != k2 {
		t.Fatalf("ArtifactKey(nil,...) = %q != ArtifactKey(empty-slice/map,...) = %q, want identical", k1, k2)
	}
	if k1 == "" {
		t.Fatalf("ArtifactKey returned empty string for a well-defined empty invocation")
	}
}

// TestArtifactKeyWeb_EtagChange_DifferentKey proves the web variant's etag
// field participates in the identity — the DESIGN's "web variant =
// sha256(url ‖ etag)".
func TestArtifactKeyWeb_EtagChange_DifferentKey(t *testing.T) {
	k1 := ArtifactKeyWeb("https://example.com/doc", "etag-v1")
	k2 := ArtifactKeyWeb("https://example.com/doc", "etag-v2")
	if k1 == k2 {
		t.Fatalf("ArtifactKeyWeb identical across different etags: %q", k1)
	}
	// Deterministic + no ambiguous boundary: differently-split url/etag with
	// the same concatenation must not collide.
	k3 := ArtifactKeyWeb("https://example.com/docX", "")
	k4 := ArtifactKeyWeb("https://example.com/doc", "X")
	if k3 == k4 {
		t.Fatalf("ArtifactKeyWeb collided across an ambiguous url/etag concatenation boundary")
	}
}

// TestGetOrComputeArtifact_HitAvoidsRecompute is the HIT/MISS
// compute-counter proof: identical inputs -> identical key -> a cache HIT
// that reuses the artifact WITHOUT recomputing it (a ground-truth
// compute-counter, matching the WS6 cross-process single-flight test's
// proof style).
func TestGetOrComputeArtifact_HitAvoidsRecompute(t *testing.T) {
	fc := newFakeClock(baseTime)
	c := New(WithClock(fc.Now))

	inputs := map[string]string{"pkg/cache/cache.go": "sha256:v1"}
	var calls int32
	compute := func() (string, time.Duration, error) {
		atomic.AddInt32(&calls, 1)
		return "artifact-result-v1", 0, nil
	}

	got1, err := c.GetOrComputeArtifact("go-vet", []string{"./..."}, "deadbeef", inputs, compute)
	if err != nil {
		t.Fatalf("first GetOrComputeArtifact err: %v", err)
	}
	if got1 != "artifact-result-v1" {
		t.Fatalf("first GetOrComputeArtifact = %q, want artifact-result-v1", got1)
	}
	if n := atomic.LoadInt32(&calls); n != 1 {
		t.Fatalf("compute called %d times on first (miss) call, want exactly 1", n)
	}

	got2, err := c.GetOrComputeArtifact("go-vet", []string{"./..."}, "deadbeef", inputs, compute)
	if err != nil {
		t.Fatalf("second GetOrComputeArtifact err: %v", err)
	}
	if got2 != "artifact-result-v1" {
		t.Fatalf("second (HIT) GetOrComputeArtifact = %q, want reused artifact-result-v1", got2)
	}
	if n := atomic.LoadInt32(&calls); n != 1 {
		t.Fatalf("compute called %d times after a repeat identical invocation, want still 1 (HIT must not recompute)", n)
	}
}

// TestGetOrComputeArtifact_DependencyEdit_ForcesRecomputeNotStaleServe is
// the end-to-end RED->GREEN no-stale-serve proof the DESIGN mandates
// (§6: "edit a dependency file -> assert the L3 key changes and the entry
// misses", per §11.4.115): (1) compute + cache an artifact for input set
// v1 (compute count -> 1); (2) an IDENTICAL repeat is a HIT (compute count
// stays 1 — proves the cache genuinely reuses); (3) simulating "someone
// edited pkg/cache/cache.go" by changing ONLY that path's content hash
// forces a MISS and a fresh compute (compute count -> 2) producing a
// DIFFERENT result — proving the stale v1 artifact is never served for the
// now-different input content.
func TestGetOrComputeArtifact_DependencyEdit_ForcesRecomputeNotStaleServe(t *testing.T) {
	fc := newFakeClock(baseTime)
	c := New(WithClock(fc.Now))

	inputsV1 := map[string]string{
		"pkg/cache/cache.go": "sha256:before-edit",
		"go.mod":             "sha256:unchanged",
	}
	var calls int32
	makeCompute := func(result string) ComputeFunc {
		return func() (string, time.Duration, error) {
			atomic.AddInt32(&calls, 1)
			return result, 0, nil
		}
	}

	got1, err := c.GetOrComputeArtifact("pre_build_verification", []string{"--full"}, "deadbeef", inputsV1, makeCompute("result-v1"))
	if err != nil {
		t.Fatalf("v1 compute err: %v", err)
	}
	if got1 != "result-v1" {
		t.Fatalf("v1 result = %q, want result-v1", got1)
	}
	if n := atomic.LoadInt32(&calls); n != 1 {
		t.Fatalf("compute called %d times for the first v1 invocation, want 1", n)
	}

	// Repeat with the SAME (unedited) inputs: must HIT, not recompute.
	gotRepeat, err := c.GetOrComputeArtifact("pre_build_verification", []string{"--full"}, "deadbeef", inputsV1, makeCompute("SHOULD-NOT-BE-USED"))
	if err != nil {
		t.Fatalf("repeat v1 compute err: %v", err)
	}
	if gotRepeat != "result-v1" {
		t.Fatalf("repeat v1 result = %q, want reused result-v1 (stale HIT check)", gotRepeat)
	}
	if n := atomic.LoadInt32(&calls); n != 1 {
		t.Fatalf("compute called %d times after an identical repeat, want still 1", n)
	}

	// Simulate a dependency-file edit: same path, changed content hash.
	inputsV2 := map[string]string{
		"pkg/cache/cache.go": "sha256:after-edit",
		"go.mod":             "sha256:unchanged",
	}
	keyV1 := ArtifactKey("pre_build_verification", []string{"--full"}, "deadbeef", inputsV1)
	keyV2 := ArtifactKey("pre_build_verification", []string{"--full"}, "deadbeef", inputsV2)
	if keyV1 == keyV2 {
		t.Fatalf("L3 key unchanged after a declared dependency's content changed — no-stale-serve guarantee broken")
	}

	got2, err := c.GetOrComputeArtifact("pre_build_verification", []string{"--full"}, "deadbeef", inputsV2, makeCompute("result-v2"))
	if err != nil {
		t.Fatalf("v2 compute err: %v", err)
	}
	if got2 != "result-v2" {
		t.Fatalf("v2 (post-edit) result = %q, want FRESH result-v2, not a stale v1 artifact", got2)
	}
	if n := atomic.LoadInt32(&calls); n != 2 {
		t.Fatalf("compute called %d times total, want exactly 2 (v1 once, v2 once after the edit forced a genuine miss)", n)
	}

	// The stale v1 entry must remain independently retrievable under its OWN
	// key — editing a dependency does not corrupt or evict unrelated
	// entries, it only prevents the EDITED identity from serving stale data.
	gotV1Again, err := c.GetOrComputeArtifact("pre_build_verification", []string{"--full"}, "deadbeef", inputsV1, makeCompute("SHOULD-NOT-BE-USED-2"))
	if err != nil {
		t.Fatalf("v1-again compute err: %v", err)
	}
	if gotV1Again != "result-v1" {
		t.Fatalf("v1-again result = %q, want result-v1 still cached under its own unchanged key", gotV1Again)
	}
	if n := atomic.LoadInt32(&calls); n != 2 {
		t.Fatalf("compute called %d times after re-checking the untouched v1 identity, want still 2", n)
	}
}

// TestGetOrComputeArtifact_DifferentArgv_IndependentEntries proves two
// invocations differing only in argv are cached INDEPENDENTLY (no key
// collision cross-contaminating results).
func TestGetOrComputeArtifact_DifferentArgv_IndependentEntries(t *testing.T) {
	fc := newFakeClock(baseTime)
	c := New(WithClock(fc.Now))
	inputs := map[string]string{"a.go": "h1"}

	gotA, err := c.GetOrComputeArtifact("grep", []string{"TODO"}, "head", inputs, func() (string, time.Duration, error) {
		return "result-TODO", 0, nil
	})
	if err != nil {
		t.Fatalf("argv A err: %v", err)
	}
	gotB, err := c.GetOrComputeArtifact("grep", []string{"FIXME"}, "head", inputs, func() (string, time.Duration, error) {
		return "result-FIXME", 0, nil
	})
	if err != nil {
		t.Fatalf("argv B err: %v", err)
	}
	if gotA != "result-TODO" || gotB != "result-FIXME" {
		t.Fatalf("cross-contaminated results: A=%q B=%q", gotA, gotB)
	}

	// Re-fetching A must still return A's own result (proves no collision
	// clobbered A's entry when B was stored).
	gotAAgain, err := c.GetOrComputeArtifact("grep", []string{"TODO"}, "head", inputs, func() (string, time.Duration, error) {
		return "SHOULD-NOT-BE-USED", 0, nil
	})
	if err != nil {
		t.Fatalf("argv A re-fetch err: %v", err)
	}
	if gotAAgain != "result-TODO" {
		t.Fatalf("argv A re-fetch = %q, want result-TODO (independent entry)", gotAAgain)
	}
}

// TestInvalidateArtifact_ThenMiss proves the "evict" half of the
// lookup/store/evict triad: an artifact cached via GetOrComputeArtifact and
// then explicitly evicted via InvalidateArtifact forces the next identical
// invocation to genuinely recompute rather than serve the evicted value.
func TestInvalidateArtifact_ThenMiss(t *testing.T) {
	fc := newFakeClock(baseTime)
	c := New(WithClock(fc.Now))
	inputs := map[string]string{"a.go": "h1"}
	argv := []string{"-rn", "TODO"}

	var calls int32
	compute := func() (string, time.Duration, error) {
		n := atomic.AddInt32(&calls, 1)
		return fmt.Sprintf("result-%d", n), 0, nil
	}

	got1, err := c.GetOrComputeArtifact("grep", argv, "head", inputs, compute)
	if err != nil {
		t.Fatalf("first compute err: %v", err)
	}
	if got1 != "result-1" {
		t.Fatalf("first result = %q, want result-1", got1)
	}

	if err := c.InvalidateArtifact("grep", argv, "head", inputs); err != nil {
		t.Fatalf("InvalidateArtifact err: %v", err)
	}

	// Direct key-level miss check, mirroring cache_test.go's mustMiss style.
	mustMiss(t, c, ArtifactKey("grep", argv, "head", inputs))

	got2, err := c.GetOrComputeArtifact("grep", argv, "head", inputs, compute)
	if err != nil {
		t.Fatalf("post-invalidate compute err: %v", err)
	}
	if got2 != "result-2" {
		t.Fatalf("post-invalidate result = %q, want FRESH result-2 (eviction must force genuine recompute)", got2)
	}
	if n := atomic.LoadInt32(&calls); n != 2 {
		t.Fatalf("compute called %d times total, want exactly 2 (before and after eviction)", n)
	}
}

// TestGetOrComputeArtifact_ConcurrentIdenticalInvocations_SingleFlight is
// the §11.4.85 stress test (N >= 10 parallel invocations, run under `go
// test -race`) proving GetOrComputeArtifact inherits GetOrCompute's
// in-process single-flight stampede guard unmodified: N goroutines racing
// on the IDENTICAL tool invocation must trigger exactly ONE compute.
func TestGetOrComputeArtifact_ConcurrentIdenticalInvocations_SingleFlight(t *testing.T) {
	fc := newFakeClock(baseTime)
	c := New(WithClock(fc.Now))
	inputs := map[string]string{"a.go": "h1", "b.go": "h2"}
	argv := []string{"--full"}

	const n = 25
	var calls int32
	entered := make(chan struct{}, n)
	release := make(chan struct{})
	compute := func() (string, time.Duration, error) {
		atomic.AddInt32(&calls, 1)
		entered <- struct{}{}
		<-release
		return "single-flight-artifact", 0, nil
	}

	var wg sync.WaitGroup
	results := make([]string, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, err := c.GetOrComputeArtifact("pre_build_verification", argv, "deadbeef", inputs, compute)
			results[i] = v
			errs[i] = err
		}(i)
	}

	<-entered
	close(release)
	wg.Wait()

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("compute invoked %d times for %d concurrent identical artifact invocations, want exactly 1", got, n)
	}
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("caller %d err: %v", i, errs[i])
		}
		if results[i] != "single-flight-artifact" {
			t.Fatalf("caller %d result = %q, want single-flight-artifact", i, results[i])
		}
	}
}
