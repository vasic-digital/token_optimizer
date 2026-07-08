// Package cache (this file): the WS6 L3 artifact cache — a content-
// addressed cache of expensive derived artifacts (tool invocations: a
// CodeGraph query, a grep sweep, a pre-build gate run, a fetched web
// resource) keyed by a stable hash of everything the artifact's
// correctness actually depends on, per
// docs/research/tokens/ws6_caching_sync/DESIGN.md §3:
//
//	L3 artifact key = sha256( tool_name ‖ argv ‖ git_head ‖
//	                          { path: content_hash for path in input_files } )
//	web variant     = sha256( url ‖ etag )
//
// L3 is layered ENTIRELY on the existing Cache/Store/GetOrCompute/Invalidate
// machinery this package already ships (cache.go, singleflight.go,
// invalidate.go) — this file adds NO new storage, eviction, or
// synchronization logic. It is additive/opt-in exactly like
// WithCrossProcessLock (crossprocess.go): a Cache that never calls the
// functions in this file behaves exactly as it did before this file
// existed. What distinguishes "L3" from the L1 exact-result cache the rest
// of the package already serves is purely the KEY SCHEMA computed here —
// content-addressed by the artifact's inputs rather than by
// (model, messages, params) — and, at the consumer/router layer (outside
// this decoupled package, §11.4.28), the choice to route tool-call misses
// through a Cache instance dedicated to this schema.
//
// Stable-name resolution (§11.4.111): every key this file derives is the
// artifact's CONTENT IDENTITY. It is built exclusively from values the
// cached artifact's correctness depends on — never a mutable/ordinal
// handle such as a PID, a temp directory, a wall-clock timestamp, or a
// /mnt/trackN partition number. Two invocations with byte-identical
// tool_name+argv+git_head+input-content therefore always derive the
// IDENTICAL key, regardless of which host/track/process/session computed
// it — the DESIGN's "scope discipline" point 6: "track-independent -> pure
// content hash -> SHARED across /mnt/trackN; NEVER key on trackN as an
// opaque partition (blocks legit share; §11.4.111)". Conversely, changing
// ANY declared input — even one whose PATH is unchanged but whose CONTENT
// changed, e.g. a dependency file was edited — changes the derived key, so
// the next lookup is an honest MISS rather than a resurrected stale hit
// (the DESIGN's §6 "No-stale-serve proof").
package cache

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"io"
	"sort"
)

// ArtifactKey computes the WS6 L3 artifact-cache key for a tool invocation:
// sha256(tool_name ‖ argv ‖ git_head ‖ {path: content_hash for path in
// inputs}), per DESIGN.md §3.
//
// inputs maps each input file's path to ITS OWN content hash (typically a
// hex sha256 the caller already computed for the file's current bytes).
// This function does not read the filesystem itself — computing content
// hashes is the caller's concern, keeping ArtifactKey a pure,
// side-effect-free function that is trivially unit-testable in isolation
// (§11.4.28 decoupling: this package hardcodes no hashing-of-files
// mechanism, exactly as it hardcodes no Store technology).
//
// Determinism (§11.4.50 / §11.4.111): paths are sorted before hashing, so
// Go's runtime-randomised map iteration order can NEVER change the derived
// key for the same logical input set — calling ArtifactKey many times over
// maps with identical content always yields the identical key. Every field
// (tool_name, each argv element, git_head, each path, each content hash) is
// length-prefixed before being written into the hash, so no ambiguous
// concatenation boundary can alias two logically-distinct invocations onto
// one key (e.g. tool_name "ab" with argv ["c"] cannot collide with
// tool_name "a" with argv ["bc"] — both length and content are hashed).
//
// A change to toolName, any argv element or its count, gitHead, the set of
// input paths, or any input's content hash produces a DIFFERENT key —
// there is no field this function accepts that does not participate in the
// derived identity, so no invocation-affecting change can silently reuse a
// stale entry.
func ArtifactKey(toolName string, argv []string, gitHead string, inputs map[string]string) string {
	h := sha256.New()
	writeField(h, []byte(toolName))
	writeUint64(h, uint64(len(argv)))
	for _, a := range argv {
		writeField(h, []byte(a))
	}
	writeField(h, []byte(gitHead))

	paths := make([]string, 0, len(inputs))
	for p := range inputs {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	writeUint64(h, uint64(len(paths)))
	for _, p := range paths {
		writeField(h, []byte(p))
		writeField(h, []byte(inputs[p]))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// ArtifactKeyWeb computes the WS6 L3 "web variant" artifact key for a
// fetched URL: sha256(url ‖ etag), per DESIGN.md §3 ("web variant =
// sha256(url ‖ etag), TTL = staleness_class(host), §11.4.99"). The caller
// applies its own §11.4.99 staleness-class TTL when storing under this key
// (via SetWithTTL / the ttl returned from a ComputeFunc) — this function
// only derives the identity, matching ArtifactKey's pure, side-effect-free
// contract.
//
// A change to either url or etag (the resource's own content-version
// marker, e.g. an HTTP ETag or Last-Modified surrogate) produces a
// DIFFERENT key, so a resource that changed upstream is an honest miss.
func ArtifactKeyWeb(url, etag string) string {
	h := sha256.New()
	writeField(h, []byte(url))
	writeField(h, []byte(etag))
	return hex.EncodeToString(h.Sum(nil))
}

// writeField writes b into w preceded by its own length (an 8-byte
// big-endian count), so concatenating fields of differing lengths can never
// produce an ambiguous byte stream two distinct logical inputs could share.
// w is always a sha256 hash.Hash in this file's usage, whose Write never
// returns an error (per the hash.Hash contract), so the error is safely
// ignored here — the same discipline crossprocess.go's hashFilename applies
// to sha256.Sum256's non-fallible Write internally.
func writeField(w io.Writer, b []byte) {
	writeUint64(w, uint64(len(b)))
	_, _ = w.Write(b)
}

func writeUint64(w io.Writer, n uint64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], n)
	_, _ = w.Write(buf[:])
}

// GetOrComputeArtifact is the WS6 L3 entry point. It derives the
// content-addressed ArtifactKey for the given tool invocation and delegates
// to GetOrCompute — so it inherits, unmodified, EVERY correctness guarantee
// GetOrCompute already provides: the in-process single-flight stampede
// guard (singleflight.go), the optional cross-process guard
// (crossprocess.go, when the Cache was constructed with
// WithCrossProcessLock), and the invalidate-during-compute
// not-cached-if-superseded guarantee. No new synchronization primitive is
// introduced by this file.
//
// A repeated call with IDENTICAL toolName+argv+gitHead+inputs content is
// served from cache — a genuine artifact reuse, compute is NOT invoked
// again. A call where ANY of those differ (including only a content-hash
// change for a path whose NAME is unchanged, e.g. a dependency file was
// edited) derives a DIFFERENT key and is therefore an honest miss: compute
// runs again and the new result is stored under the new key, never
// silently conflated with the stale entry (the DESIGN's §6 "No-stale-serve
// proof").
//
// Distinguishing this "L3" call from an ordinary L1 GetOrCompute call is
// purely the caller's choice of key schema (this function's) and, at the
// router layer, the caller's choice of WHICH Cache instance/Store to route
// tool-call misses through — assembling L1 vs L3 as separate Cache
// instances (per DESIGN.md §1's request-flow: "is this a tool call? -> L3
// artifact cache") is a consumer/router-layer concern outside this
// decoupled package (§11.4.28); a single Cache instance MAY also be shared
// across both key schemas since content-addressed keys and the L1 exact-
// result keys never collide in practice (both are hex sha256 digests over
// disjoint input alphabets), but nothing in this package assumes that.
func (c *Cache) GetOrComputeArtifact(toolName string, argv []string, gitHead string, inputs map[string]string, compute ComputeFunc) (string, error) {
	return c.GetOrCompute(ArtifactKey(toolName, argv, gitHead, inputs), compute)
}

// InvalidateArtifact evicts the L3 entry for the given tool invocation. It
// derives the same ArtifactKey GetOrComputeArtifact would derive for an
// IDENTICAL invocation and delegates to Invalidate, so it inherits
// Invalidate's tombstone-based no-stale-serve guarantee (invalidate.go)
// unmodified: an immediate re-lookup with the same identity is an honest
// miss even if the injected L2 Store's own Delete is best-effort or a
// no-op.
//
// This is the explicit "evict" counterpart to GetOrComputeArtifact's
// "lookup/store" — most L3 entries never need explicit eviction (a content
// change already derives a fresh key on its own, per ArtifactKey's doc
// comment), but this is available for a caller that needs to force-evict a
// known-bad artifact (e.g. a tool run that produced a result later proven
// wrong for reasons the content hash could not capture, such as a change
// to the tool's OWN version when gitHead/argv/inputs happen to be
// unchanged — a caller for whom that matters includes the tool's own
// version in argv or gitHead so it participates in the key; this method
// remains available as a manual fallback regardless).
func (c *Cache) InvalidateArtifact(toolName string, argv []string, gitHead string, inputs map[string]string) error {
	return c.Invalidate(ArtifactKey(toolName, argv, gitHead, inputs))
}
