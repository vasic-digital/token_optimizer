package brotli

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/vasic-digital/token_optimizer/pkg/transport"
)

// representativePayloads mirrors the shape of the WS7 wire-format corpus
// (docs/research/tokens/ws8_transport/POC/brotli/payloads/*.json) WITHOUT a
// file-path dependency on the parent repo tree (§11.4.28 decoupling — this
// submodule's tests must be hermetic and runnable standalone). Each entry is a
// realistic Claude tool-call / work-item JSON body of non-trivial size so the
// compression-ratio assertions exercise more than a toy string.
func representativePayloads() map[string][]byte {
	toolResultRows := make([]string, 0, 40)
	for i := 0; i < 40; i++ {
		toolResultRows = append(toolResultRows, fmt.Sprintf(
			`{"device_id":"D%02d","serial":"998fd36615e9%04d","status":"online","last_seen":"2026-07-08T09:3%d:00Z","capabilities":["audio_output","video_display","bluetooth_a2dp"]}`,
			i, i, i%10))
	}
	toolResultJSON := []byte(`{"tool_use_id":"toolu_01ABCDEF","content":[{"type":"text","text":"` +
		strings.ReplaceAll(strings.Join(toolResultRows, ","), `"`, `\"`) + `"}]}`)

	workItemRows := make([]string, 0, 60)
	for i := 0; i < 60; i++ {
		workItemRows = append(workItemRows, fmt.Sprintf(
			`{"atm_id":"ATM-%03d","type":"Task","status":"In progress","title":"WS8 transport increment #%d","composes_with":["§11.4.28","§11.4.43","§11.4.107"]}`,
			600+i, i))
	}
	workItemsJSON := []byte(`{"items":[` + strings.Join(workItemRows, ",") + `]}`)

	llmRequestJSON := []byte(`{"model":"claude-sonnet-5","max_tokens":4096,"messages":[{"role":"user","content":"` +
		strings.Repeat("Investigate the WS8 transport/compression follow-up and prove losslessness. ", 30) +
		`"}],"tools":[{"name":"bash","description":"Run a shell command"}]}`)

	return map[string][]byte{
		"tool_result_devices": toolResultJSON,
		"workitems_array":     workItemsJSON,
		"llm_request":         llmRequestJSON,
	}
}

// TestCompressorRoundTrip proves compress->decompress reconstructs bytes
// byte-identical to the input across a spread of body shapes, INCLUDING the
// mandatory edge cases: empty, large, and already-compressed (fed back through
// brotli itself, which must still round-trip even though it cannot shrink
// further). NEGATION: if Compress/Decompress silently dropped, reordered, or
// truncated a byte, the bytes.Equal assertion FAILs — this is the load-bearing
// lossless guarantee pkg/transport requires of every injected Compressor.
func TestCompressorRoundTrip(t *testing.T) {
	c := NewDefault()

	already := mustCompressOnce(t, c, bytes.Repeat([]byte("pre-compressed-once-"), 500))

	bodies := map[string][]byte{
		"empty":            {},
		"nil":              nil,
		"single-byte":      {0x00},
		"ascii":            []byte("the quick brown fox jumps over the lazy dog"),
		"highly-compress":  bytes.Repeat([]byte("repeat-block-"), 8192),
		"binary-allbytes":  allByteValues(),
		"utf8":             []byte(strings.Repeat("токен-оптимизатор ", 512)),
		"embedded-nul-nl":  []byte("a\x00b\nc\rd\te"),
		"already-brotli":   already,
		"large-4mb-random": pseudoRandomBytes(4 << 20),
	}
	for name, payload := range representativePayloads() {
		bodies["representative_"+name] = payload
	}

	for name, body := range bodies {
		compressed, err := c.Compress(body)
		if err != nil {
			t.Fatalf("%s: Compress: unexpected error: %v", name, err)
		}
		got, err := c.Decompress(compressed)
		if err != nil {
			t.Fatalf("%s: Decompress: unexpected error: %v", name, err)
		}
		if !bytes.Equal(got, body) {
			t.Fatalf("%s round-trip: got %d bytes, want %d bytes (LOSSY)", name, len(got), len(body))
		}
	}
}

// TestEncodingToken pins the stable content-coding token to the IANA-registered
// value "br" (HTTP Content Coding Registry, RFC 7932) — the exact token
// negotiate.go's TestDecompressMatchingPath already anticipates ("br" currently
// asserts ErrUnknownEncoding because no brotli Compressor is registered there).
// NEGATION: if the token drifted, real HTTP peers (browsers, CDNs, this
// project's own cache-sync design in ws8_transport/DESIGN.md §2.3) would never
// recognise the coding this package produces.
func TestEncodingToken(t *testing.T) {
	if got := NewDefault().Encoding(); got != "br" {
		t.Fatalf("Encoding() = %q, want %q", got, "br")
	}
}

// TestDeterministic proves compressing the same body twice at the same quality
// yields byte-identical output (§11.4.50), mirroring pkg/transport's
// TestGzipDeflateDeterministic for the built-ins. NEGATION: if the brotli
// writer embedded any per-call non-deterministic state, the two outputs would
// differ and this FAILs.
func TestDeterministic(t *testing.T) {
	c := NewDefault()
	body := bytes.Repeat([]byte("determinism-"), 4000)
	a, err := c.Compress(body)
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.Compress(body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatalf("brotli.Compress not deterministic: %d vs %d bytes differ", len(a), len(b))
	}
}

// TestNewValidatesQuality proves the constructor rejects out-of-range quality
// (brotli's own documented range is BestSpeed(0)..BestCompression(11)) rather
// than silently clamping or panicking deep inside the encoder.
func TestNewValidatesQuality(t *testing.T) {
	if _, err := New(-1); !errors.Is(err, ErrInvalidQuality) {
		t.Errorf("quality -1: want ErrInvalidQuality, got %v", err)
	}
	if _, err := New(12); !errors.Is(err, ErrInvalidQuality) {
		t.Errorf("quality 12: want ErrInvalidQuality, got %v", err)
	}
	for _, q := range []int{0, 5, 6, 11} {
		if _, err := New(q); err != nil {
			t.Errorf("quality %d: unexpected error: %v", q, err)
		}
	}
}

// TestCompressionRatioOnRepresentativePayloads reports the REAL brotli(q5) /
// brotli(q11) / gzip ratio on realistic tool-call and work-item JSON bodies
// (the class of payload ws8_transport/ANALYSIS.md measured) and asserts the
// non-bluff floor: for compressible JSON, brotli MUST actually shrink the
// payload (never regress to identity-or-larger) and MUST beat naive
// non-compression. Numbers are captured via t.Logf — never fabricated, always
// the output of a real Compress() call on the payload printed alongside it.
func TestCompressionRatioOnRepresentativePayloads(t *testing.T) {
	q5, err := New(5)
	if err != nil {
		t.Fatal(err)
	}
	q11, err := New(11)
	if err != nil {
		t.Fatal(err)
	}

	for name, payload := range representativePayloads() {
		br5, err := q5.Compress(payload)
		if err != nil {
			t.Fatalf("%s: q5 Compress: %v", name, err)
		}
		br11, err := q11.Compress(payload)
		if err != nil {
			t.Fatalf("%s: q11 Compress: %v", name, err)
		}
		back5, err := q5.Decompress(br5)
		if err != nil || !bytes.Equal(back5, payload) {
			t.Fatalf("%s: q5 round-trip broke (err=%v)", name, err)
		}
		back11, err := q11.Decompress(br11)
		if err != nil || !bytes.Equal(back11, payload) {
			t.Fatalf("%s: q11 round-trip broke (err=%v)", name, err)
		}

		ratio5 := 100 * (1 - float64(len(br5))/float64(len(payload)))
		ratio11 := 100 * (1 - float64(len(br11))/float64(len(payload)))
		t.Logf("%-24s raw=%6d br(q5)=%6d save=%5.1f%%  br(q11)=%6d save=%5.1f%%",
			name, len(payload), len(br5), ratio5, len(br11), ratio11)

		if len(br5) >= len(payload) {
			t.Errorf("%s: brotli(q5) did not shrink a compressible JSON payload: %d >= %d", name, len(br5), len(payload))
		}
		if len(br11) >= len(payload) {
			t.Errorf("%s: brotli(q11) did not shrink a compressible JSON payload: %d >= %d", name, len(br11), len(payload))
		}
	}
}

// TestConcurrentUse proves one Compressor value is safe for concurrent
// Compress + Decompress from many goroutines (run with -race), mirroring
// pkg/transport's TestNegotiatorConcurrentUse. NEGATION: if Compressor held any
// per-call shared mutable state, -race would report a data race or a
// cross-goroutine round-trip mismatch would surface intermittently.
func TestConcurrentUse(t *testing.T) {
	c := NewDefault()
	const workers = 16
	const iters = 50

	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			body := bytes.Repeat([]byte(fmt.Sprintf("w%02d-", id)), 300+id)
			for i := 0; i < iters; i++ {
				compressed, err := c.Compress(body)
				if err != nil {
					errs <- fmt.Errorf("worker %d compress: %w", id, err)
					return
				}
				got, err := c.Decompress(compressed)
				if err != nil {
					errs <- fmt.Errorf("worker %d decompress: %w", id, err)
					return
				}
				if !bytes.Equal(got, body) {
					errs <- fmt.Errorf("worker %d: round trip mismatch", id)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestWiresIntoNegotiator proves this Compressor is a real, drop-in
// transport.Compressor — the exact extension point pkg/transport/negotiate.go
// documents ("a consumer that also has a brotli ... compressor builds its
// Negotiator with NewNegotiator(Identity{}, Gzip{}, Deflate{},
// brotliCompressor, ...)"). It asserts brotli is SELECTED over gzip/deflate for
// a realistic payload (proving it is genuinely wired into the real selection
// path, not a dead standalone type) and that the negotiator's own Decompress
// reconstructs the body via the matching path.
func TestWiresIntoNegotiator(t *testing.T) {
	n, err := transport.NewNegotiator(transport.Identity{}, transport.Gzip{}, transport.Deflate{}, NewDefault())
	if err != nil {
		t.Fatal(err)
	}
	for name, payload := range representativePayloads() {
		enc, err := n.Compress(payload)
		if err != nil {
			t.Fatalf("%s: Compress: %v", name, err)
		}
		if enc.Encoding != "br" {
			t.Errorf("%s: expected brotli to win selection for a compressible JSON body, negotiator chose %q (sizes=%v)",
				name, enc.Encoding, enc.Sizes())
		}
		got, err := n.Decompress(enc.Encoding, enc.Body)
		if err != nil {
			t.Fatalf("%s: Decompress: %v", name, err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("%s: negotiator round-trip broke via the %q path", name, enc.Encoding)
		}
	}
}

// TestWiredIntoHTTPRequestBody proves brotli reaches a genuine
// *http.Request through pkg/transport's real HTTP glue
// (Negotiator.CompressRequestBody), not merely the in-memory Compressor API:
// the request's Body/ContentLength/Content-Encoding are rewritten, and a
// server-side reader (simulated by directly consuming the rewritten request,
// exactly as an httptest handler would) reconstructs the original bytes.
func TestWiredIntoHTTPRequestBody(t *testing.T) {
	n, err := transport.NewNegotiator(transport.Identity{}, transport.Gzip{}, transport.Deflate{}, NewDefault())
	if err != nil {
		t.Fatal(err)
	}
	payload := representativePayloads()["workitems_array"]

	req := httptest.NewRequest(http.MethodPost, "http://example.invalid/cache", bytes.NewReader(payload))
	enc, err := n.CompressRequestBody(req)
	if err != nil {
		t.Fatal(err)
	}
	if enc.Encoding != "br" {
		t.Fatalf("expected brotli to win for this payload, negotiator chose %q", enc.Encoding)
	}
	if got := req.Header.Get("Content-Encoding"); got != "br" {
		t.Fatalf("Content-Encoding header = %q, want %q", got, "br")
	}
	if req.ContentLength != int64(len(enc.Body)) {
		t.Fatalf("ContentLength = %d, want %d", req.ContentLength, len(enc.Body))
	}

	// Consume the request exactly as a real HTTP server handler would.
	onWire, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := n.Decompress(req.Header.Get("Content-Encoding"), onWire)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, payload) {
		t.Fatal("request body round-trip through the real HTTP glue was lossy")
	}
}

// TestWiredIntoHTTPResponseBody proves the symmetric decode path
// (Negotiator.DecodeResponseBody) against a genuine *http.Response whose body
// was brotli-compressed and labelled with Content-Encoding: br.
func TestWiredIntoHTTPResponseBody(t *testing.T) {
	n, err := transport.NewNegotiator(transport.Identity{}, transport.Gzip{}, transport.Deflate{}, NewDefault())
	if err != nil {
		t.Fatal(err)
	}
	payload := representativePayloads()["tool_result_devices"]
	compressed, err := NewDefault().Compress(payload)
	if err != nil {
		t.Fatal(err)
	}
	resp := &http.Response{
		Header: http.Header{"Content-Encoding": []string{"br"}},
		Body:   io.NopCloser(bytes.NewReader(compressed)),
	}
	got, err := n.DecodeResponseBody(resp)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("response body round-trip through the real HTTP glue was lossy")
	}
}

// --- helpers -----------------------------------------------------------------

func mustCompressOnce(t *testing.T, c Compressor, body []byte) []byte {
	t.Helper()
	out, err := c.Compress(body)
	if err != nil {
		t.Fatalf("mustCompressOnce: %v", err)
	}
	return out
}

func allByteValues() []byte {
	b := make([]byte, 256)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

// pseudoRandomBytes returns a deterministic (seeded, not crypto) large,
// low-redundancy buffer standing in for the "large" + "incompressible" edge
// case, WITHOUT importing math/rand (keeps this test file dependency-free
// beyond the stdlib + the package under test).
func pseudoRandomBytes(n int) []byte {
	b := make([]byte, n)
	var x uint32 = 0x9E3779B9
	for i := range b {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		b[i] = byte(x)
	}
	return b
}
