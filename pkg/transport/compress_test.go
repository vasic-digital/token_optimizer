package transport

import (
	"bytes"
	"strings"
	"testing"
)

// TestBuiltinCompressorsRoundTrip proves each built-in Compressor reconstructs
// the exact input for a spread of body shapes. NEGATION: if any built-in's
// Compress/Decompress pairing were wrong (dropped/added/reordered a byte), the
// bytes.Equal assertion FAILs — this is the byte-exact wire-safety contract
// every eligible compressor must meet.
func TestBuiltinCompressorsRoundTrip(t *testing.T) {
	bodies := map[string][]byte{
		"empty":           {},
		"nil":             nil,
		"single-byte":     {0x00},
		"ascii":           []byte("the quick brown fox jumps over the lazy dog"),
		"highly-compress": bytes.Repeat([]byte("repeat-block-"), 4096),
		"binary-allbytes": allByteValues(),
		"utf8":            []byte(strings.Repeat("токен-оптимизатор ", 512)),
		"embedded-nul-nl": []byte("a\x00b\nc\rd\te"),
	}
	compressors := []Compressor{Identity{}, Gzip{}, Deflate{}}

	for _, c := range compressors {
		for name, body := range bodies {
			compressed, err := c.Compress(body)
			if err != nil {
				t.Fatalf("%s.Compress(%s): unexpected error: %v", c.Encoding(), name, err)
			}
			got, err := c.Decompress(compressed)
			if err != nil {
				t.Fatalf("%s.Decompress(%s): unexpected error: %v", c.Encoding(), name, err)
			}
			if !bytes.Equal(got, body) {
				t.Fatalf("%s round-trip(%s): got %d bytes, want %d bytes (lossy!)",
					c.Encoding(), name, len(got), len(body))
			}
		}
	}
}

// TestBuiltinEncodingTokens pins the stable content-coding tokens. NEGATION: if
// a token drifted (e.g. "gzip" -> "gz"), Content-Encoding negotiation and the
// decode-path lookup would silently mismatch a real HTTP peer; this FAILs first.
func TestBuiltinEncodingTokens(t *testing.T) {
	cases := []struct {
		c    Compressor
		want string
	}{
		{Identity{}, "identity"},
		{Gzip{}, "gzip"},
		{Deflate{}, "deflate"},
	}
	for _, tc := range cases {
		if got := tc.c.Encoding(); got != tc.want {
			t.Errorf("Encoding() = %q, want %q", got, tc.want)
		}
	}
}

// TestGzipDeflateDeterministic proves the built-ins emit byte-identical output
// for the same input (no wall-clock timestamp) per §11.4.50. NEGATION: if gzip's
// ModTime were set to time.Now(), the two outputs would differ and this FAILs.
func TestGzipDeflateDeterministic(t *testing.T) {
	body := bytes.Repeat([]byte("determinism-"), 1000)
	for _, c := range []Compressor{Gzip{}, Deflate{}} {
		a, err := c.Compress(body)
		if err != nil {
			t.Fatalf("%s.Compress a: %v", c.Encoding(), err)
		}
		b, err := c.Compress(body)
		if err != nil {
			t.Fatalf("%s.Compress b: %v", c.Encoding(), err)
		}
		if !bytes.Equal(a, b) {
			t.Fatalf("%s.Compress not deterministic: %d vs %d bytes differ", c.Encoding(), len(a), len(b))
		}
	}
}

// TestIdentityDoesNotAliasCaller proves Identity returns copies, so a caller
// mutating the returned slice cannot corrupt its own input (and vice versa).
func TestIdentityDoesNotAliasCaller(t *testing.T) {
	src := []byte("mutable")
	out, _ := Identity{}.Compress(src)
	if len(out) > 0 {
		out[0] = 'X'
	}
	if string(src) != "mutable" {
		t.Fatalf("Identity.Compress aliased caller slice: src became %q", src)
	}
}

func allByteValues() []byte {
	b := make([]byte, 256)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}
