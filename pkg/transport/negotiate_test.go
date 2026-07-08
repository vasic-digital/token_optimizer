package transport

import (
	"bytes"
	"errors"
	"testing"
)

// --- test doubles -----------------------------------------------------------

// lyingCompressor produces the smallest possible output (one byte) but its
// Decompress can NEVER reconstruct a non-trivial body. It is the adversary the
// lossless guard exists to defeat: without the round-trip check it would win
// every selection (size 1) and corrupt the wire.
type lyingCompressor struct{ token string }

func (l lyingCompressor) Encoding() string                { return l.token }
func (l lyingCompressor) Compress([]byte) ([]byte, error) { return []byte{0x00}, nil }
func (l lyingCompressor) Decompress([]byte) ([]byte, error) {
	return []byte("this is not the original body"), nil
}

// fixedSizeCompressor is a lossless double whose compressed size is a constant
// we control, used to force exact size ties for the deterministic tie-break.
// It stores the original inside a fixed-width frame so it truly round-trips.
type fixedSizeCompressor struct {
	token string
	width int
}

func (f fixedSizeCompressor) Encoding() string { return f.token }
func (f fixedSizeCompressor) Compress(b []byte) ([]byte, error) {
	out := make([]byte, f.width+len(b))
	copy(out[f.width:], b) // leading zero-pad of width bytes forces a fixed floor size
	return out, nil
}
func (f fixedSizeCompressor) Decompress(b []byte) ([]byte, error) {
	if len(b) < f.width {
		return nil, errors.New("short frame")
	}
	return append([]byte(nil), b[f.width:]...), nil
}

// erroringCompressor always fails Compress; it must be recorded as a failed
// candidate and never selected.
type erroringCompressor struct{ token string }

func (e erroringCompressor) Encoding() string                    { return e.token }
func (e erroringCompressor) Compress([]byte) ([]byte, error)     { return nil, errors.New("boom") }
func (e erroringCompressor) Decompress(b []byte) ([]byte, error) { return b, nil }

// --- tests ------------------------------------------------------------------

// TestSmallestLosslessChosen proves Compress returns the minimum-size result
// among the LOSSLESS candidates, over a spread of body shapes. NEGATION: the
// test recomputes the expected minimum from the candidate breakdown itself; if
// min-selection were broken (returned a larger lossless result, or a lossy one),
// either the size assertion or the round-trip assertion FAILs.
func TestSmallestLosslessChosen(t *testing.T) {
	n := Default() // identity + gzip + deflate
	bodies := map[string][]byte{
		"highly-compressible": bytes.Repeat([]byte("token-optimizer "), 2000),
		"incompressible-tiny": []byte("hi"),
		"empty":               {},
		"medium-json":         []byte(`{"a":1,"b":[1,2,3,4,5],"c":"cccccccccccccccc"}`),
	}
	for name, body := range bodies {
		enc, err := n.Compress(body)
		if err != nil {
			t.Fatalf("Compress(%s): %v", name, err)
		}
		// Independently derive the smallest lossless size + its coding.
		wantSize := -1
		wantEnc := ""
		for _, c := range enc.Candidates {
			if !c.Lossless {
				continue
			}
			if wantSize == -1 || c.Size < wantSize || (c.Size == wantSize && c.Encoding < wantEnc) {
				wantSize = c.Size
				wantEnc = c.Encoding
			}
		}
		if enc.Size != wantSize {
			t.Errorf("Compress(%s): chose size %d (%q), smallest lossless is %d (%q)",
				name, enc.Size, enc.Encoding, wantSize, wantEnc)
		}
		if enc.Encoding != wantEnc {
			t.Errorf("Compress(%s): chose %q, expected %q", name, enc.Encoding, wantEnc)
		}
		// The chosen body MUST decode back to the input via the matching path.
		got, err := n.Decompress(enc.Encoding, enc.Body)
		if err != nil {
			t.Fatalf("Decompress(%s, %q): %v", name, enc.Encoding, err)
		}
		if !bytes.Equal(got, body) {
			t.Errorf("Compress(%s) round-trip broke: %d bytes back, want %d", name, len(got), len(body))
		}
	}
}

// TestHighlyCompressibleBeatsIdentity proves a real coding wins when it helps:
// a large repetitive body must NOT ship as identity. NEGATION: if identity were
// wrongly preferred (e.g. non-strict tie handling defaulting to the raw body),
// the chosen coding would be identity and this FAILs.
func TestHighlyCompressibleBeatsIdentity(t *testing.T) {
	n := Default()
	body := bytes.Repeat([]byte("AAAA-BBBB-CCCC-"), 5000)
	enc, err := n.Compress(body)
	if err != nil {
		t.Fatal(err)
	}
	if enc.Encoding == EncodingIdentity {
		t.Fatalf("large repetitive body shipped as identity (%d bytes) instead of a real coding", enc.Size)
	}
	if enc.Size >= len(body) {
		t.Fatalf("chosen coding %q did not shrink the body: %d >= %d", enc.Encoding, enc.Size, len(body))
	}
}

// TestTinyBodyShipsIdentity proves identity wins when compression can't help:
// framing overhead makes gzip/deflate LARGER than a tiny raw body. NEGATION: if
// min-selection ignored identity, a larger gzip/deflate frame would be chosen
// and this FAILs.
func TestTinyBodyShipsIdentity(t *testing.T) {
	n := Default()
	enc, err := n.Compress([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	if enc.Encoding != EncodingIdentity {
		t.Fatalf("tiny body chose %q (%d bytes), identity (1 byte) is smaller", enc.Encoding, enc.Size)
	}
}

// TestLossyCompressorNeverSelected is the LOAD-BEARING wire-safety test. A
// lyingCompressor emits a 1-byte body (the global minimum) but cannot
// reconstruct the input. It MUST be rejected and never chosen. NEGATION: if the
// lossless round-trip guard were removed, the liar (size 1) would win every
// selection and every request would be sent with a Content-Encoding whose body
// decodes to garbage at the peer — this assertion FAILs loudly the instant that
// happens.
func TestLossyCompressorNeverSelected(t *testing.T) {
	n, err := NewNegotiator(Identity{}, Gzip{}, Deflate{}, lyingCompressor{token: "liar"})
	if err != nil {
		t.Fatal(err)
	}
	body := bytes.Repeat([]byte("real user data that must survive "), 100)
	enc, err := n.Compress(body)
	if err != nil {
		t.Fatal(err)
	}
	if enc.Encoding == "liar" {
		t.Fatalf("lossy compressor was SELECTED — wire corruption: chose %q size %d", enc.Encoding, enc.Size)
	}
	// The liar must be present in the breakdown, smallest, and flagged lossy.
	var liar *Candidate
	for i := range enc.Candidates {
		if enc.Candidates[i].Encoding == "liar" {
			liar = &enc.Candidates[i]
		}
	}
	if liar == nil {
		t.Fatal("liar candidate missing from breakdown")
	}
	if liar.Lossless {
		t.Fatal("liar wrongly flagged Lossless=true")
	}
	if liar.Size >= enc.Size {
		t.Fatalf("test precondition broken: liar (%d) is not smaller than winner %q (%d) — negation not exercised",
			liar.Size, enc.Encoding, enc.Size)
	}
	// And the winner must genuinely round-trip.
	got, err := n.Decompress(enc.Encoding, enc.Body)
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("winner %q did not round-trip (err=%v)", enc.Encoding, err)
	}
}

// TestDeterministicTieBreak proves that when two lossless codings produce the
// EXACT same size, the lexicographically smaller Encoding wins, reproducibly.
// NEGATION: if tie-break were non-deterministic (e.g. map iteration order or a
// non-strict replacement), repeated runs could pick either coding and the
// stable-winner assertion FAILs.
func TestDeterministicTieBreak(t *testing.T) {
	// aaa and zzz both frame with the same width -> identical output size,
	// larger than nothing so identity is excluded from the tie by making body
	// incompressible-random-free: we drop identity to isolate the two.
	a := fixedSizeCompressor{token: "aaa", width: 4}
	z := fixedSizeCompressor{token: "zzz", width: 4}
	n, err := NewNegotiator(a, z)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("tie")
	first, err := n.Compress(body)
	if err != nil {
		t.Fatal(err)
	}
	if first.Encoding != "aaa" {
		t.Fatalf("tie-break chose %q, want lexicographically-smallest \"aaa\"", first.Encoding)
	}
	// Reproducible across many runs.
	for i := 0; i < 50; i++ {
		enc, err := n.Compress(body)
		if err != nil {
			t.Fatal(err)
		}
		if enc.Encoding != "aaa" {
			t.Fatalf("run %d: tie-break drifted to %q", i, enc.Encoding)
		}
	}
}

// TestFailedCompressorRecordedNotSelected proves an erroring compressor is
// recorded as a failed candidate (Err set) and never chosen.
func TestFailedCompressorRecordedNotSelected(t *testing.T) {
	n, err := NewNegotiator(Identity{}, erroringCompressor{token: "broken"})
	if err != nil {
		t.Fatal(err)
	}
	enc, err := n.Compress([]byte("data"))
	if err != nil {
		t.Fatal(err)
	}
	if enc.Encoding == "broken" {
		t.Fatal("erroring compressor was selected")
	}
	var broken *Candidate
	for i := range enc.Candidates {
		if enc.Candidates[i].Encoding == "broken" {
			broken = &enc.Candidates[i]
		}
	}
	if broken == nil || broken.Err == nil {
		t.Fatalf("broken candidate not recorded with Err: %+v", broken)
	}
}

// TestNoLosslessCompressorErrors proves that when every compressor is lossy (no
// identity floor), Compress refuses to ship a corrupt payload and returns the
// sentinel with a populated breakdown. NEGATION: if the guard fell through to
// "return whatever is smallest", a lossy body would ship; instead the sentinel
// FAILs the caller safely.
func TestNoLosslessCompressorErrors(t *testing.T) {
	n, err := NewNegotiator(lyingCompressor{token: "liar1"}, lyingCompressor{token: "liar2"})
	if err != nil {
		t.Fatal(err)
	}
	enc, err := n.Compress([]byte("cannot be reconstructed by any liar"))
	if !errors.Is(err, ErrNoLosslessCompressor) {
		t.Fatalf("want ErrNoLosslessCompressor, got err=%v enc=%q", err, enc.Encoding)
	}
	if len(enc.Candidates) != 2 {
		t.Fatalf("want 2 diagnostic candidates, got %d", len(enc.Candidates))
	}
}

// TestNewNegotiatorValidation covers the constructor's guards.
func TestNewNegotiatorValidation(t *testing.T) {
	if _, err := NewNegotiator(); !errors.Is(err, ErrNoCompressors) {
		t.Errorf("no compressors: want ErrNoCompressors, got %v", err)
	}
	if _, err := NewNegotiator(lyingCompressor{token: ""}); !errors.Is(err, ErrEmptyEncoding) {
		t.Errorf("empty encoding: want ErrEmptyEncoding, got %v", err)
	}
	if _, err := NewNegotiator(Gzip{}, lyingCompressor{token: "gzip"}); !errors.Is(err, ErrDuplicateEncoding) {
		t.Errorf("duplicate encoding: want ErrDuplicateEncoding, got %v", err)
	}
}

// TestDecompressMatchingPath covers the decode path directly: matching coding,
// empty header (=> identity), and unknown coding.
func TestDecompressMatchingPath(t *testing.T) {
	n := Default()
	body := bytes.Repeat([]byte("decode-me "), 300)

	enc, err := n.Compress(body)
	if err != nil {
		t.Fatal(err)
	}
	got, err := n.Decompress(enc.Encoding, enc.Body)
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("matching decode failed: err=%v equal=%v", err, bytes.Equal(got, body))
	}

	// Empty content-encoding == identity == raw passthrough.
	raw := []byte("no coding applied")
	got, err = n.Decompress("", raw)
	if err != nil || !bytes.Equal(got, raw) {
		t.Fatalf("empty-encoding passthrough failed: err=%v", err)
	}

	// Unknown coding is a typed error, not a silent passthrough (that would
	// hand back a still-compressed body as if it were plaintext).
	if _, err := n.Decompress("br", enc.Body); !errors.Is(err, ErrUnknownEncoding) {
		t.Fatalf("unknown coding: want ErrUnknownEncoding, got %v", err)
	}
}

// TestEncodingsSorted proves the introspection accessor returns sorted tokens.
func TestEncodingsSorted(t *testing.T) {
	got := Default().Encodings()
	want := []string{"deflate", "gzip", "identity"}
	if len(got) != len(want) {
		t.Fatalf("Encodings() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Encodings() = %v, want %v", got, want)
		}
	}
}
