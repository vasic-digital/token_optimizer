// Package transport is the WS8 compressed HTTP transport layer of the
// token_optimizer engine.
//
// Given a request body and a set of pluggable Compressors, the package
// compresses the body with each, verifies that each compression round-trips
// losslessly, and selects the SMALLEST result among the compressors that
// round-trip. It then names the winning content-coding in the Content-Encoding
// header and provides the matching decode path. This is the WS8 idea
// generalised: cut the bytes on the wire WITHOUT ever declaring a
// Content-Encoding that does not match the body, which would corrupt the wire.
//
// Decoupling (§11.4.28): the package owns the COMPRESSION SELECTION, the
// lossless guarantee, and the correct Content-Encoding negotiation — NOT any
// particular compression algorithm and NOT any HTTP endpoint. The consumer
// supplies the *http.Client and the target URL; this package only rewrites a
// request body + its encoding headers, and decodes a response body. The only
// compressors built into this package are stdlib-backed (Identity, Gzip via
// compress/gzip, Deflate via compress/flate) so the package is usable and
// testable standalone; a stronger codec (brotli, zstd, a project-specific one)
// is an external module the consumer INJECTS as a Compressor — the engine never
// imports it. The package ships ZERO project constants and ZERO hardcoded
// endpoints.
//
// Lossless guarantee (§11.4.6, the load-bearing invariant): a compression is
// ELIGIBLE only if decompressing its output reconstructs bytes byte-identical
// to the input. A compressor that produces a smaller output but does NOT
// round-trip is REJECTED, never selected — a tiny-but-lossy body that decodes
// to the wrong bytes at the peer is exactly the wire corruption this package
// exists to prevent. If no registered compressor round-trips the body, Compress
// returns ErrNoLosslessCompressor rather than shipping a corrupt payload.
// Identity always round-trips, so a negotiator that includes it never fails.
//
// Determinism (§11.4.50): compressors are ordered by Encoding, and the smallest
// eligible result wins with a deterministic tie-break by Encoding name, so the
// same body and the same compressor set always select the same coding and emit
// byte-identical output. The stdlib gzip/flate writers used here carry no
// wall-clock timestamp (gzip ModTime is left zero), so their output is stable.
package transport

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"io"
)

// Content-coding tokens for the built-in compressors, matching the HTTP
// Content-Encoding registry values.
const (
	// EncodingIdentity is the "no transformation" coding. Per RFC 7231 it is the
	// absence of any content-coding and is never sent as a Content-Encoding
	// header value; it participates in selection as the always-lossless floor.
	EncodingIdentity = "identity"
	// EncodingGzip is the "gzip" content-coding (RFC 1952), produced by
	// compress/gzip.
	EncodingGzip = "gzip"
	// EncodingDeflate is the "deflate" content-coding, produced here by
	// compress/flate as a raw DEFLATE (RFC 1951) stream. The round-trip
	// guarantee holds because this package owns BOTH the encode and the matching
	// decode path; a consumer that must interoperate with a peer expecting the
	// zlib-framed (RFC 1950) form registers its own "deflate" Compressor via the
	// pluggable interface, and selection/decoding then use that one instead.
	EncodingDeflate = "deflate"
)

// Compressor is one pluggable content-coding. The engine treats it as opaque:
// it never inspects Encoding for a magic value and never reaches into a
// compressor's internals. Implementations MUST satisfy the round-trip contract
// — Decompress(Compress(b)) reconstructs bytes byte-identical to b — for that
// compressor to be eligible for selection; the Negotiator VERIFIES this per
// body rather than trusting it (§11.4.6), so a compressor that violates the
// contract for a given body is simply skipped for that body, never selected.
//
// Compress MUST be deterministic (§11.4.50): compressing the same bytes twice
// yields identical output. The built-in Gzip and Deflate meet this (no
// timestamp is written). Compress and Decompress MUST be safe for concurrent
// use — the built-ins allocate all per-call state fresh and hold no shared
// mutable state.
type Compressor interface {
	// Encoding is the compressor's stable content-coding token used for
	// selection, tie-breaking, the Content-Encoding header, and telemetry (e.g.
	// "gzip", "deflate", "br"). It MUST be non-empty and unique within a
	// Negotiator; resolution is always by this stable token, never by
	// registration order (§11.4.111).
	Encoding() string
	// Compress serialises body to its compressed form. A returned error makes
	// the compressor ineligible for that body (recorded as a failed candidate,
	// never selected). It MUST NOT retain or mutate body.
	Compress(body []byte) ([]byte, error)
	// Decompress reverses Compress. For an eligible compressor,
	// Decompress(Compress(b)) equals b byte-for-byte. It MUST NOT retain or
	// mutate its argument.
	Decompress(body []byte) ([]byte, error)
}

// Identity is the always-available, always-lossless floor Compressor: it copies
// the body unchanged. It is what the consumer's real compressors compete
// against — a compressor is chosen only when it round-trips the body AND its
// output is strictly smaller than the identity (raw) body, so an incompressible
// or tiny body correctly ships uncompressed. Its Encoding, "identity", is the
// absence of a content-coding and is never written to the Content-Encoding
// header (see the HTTP glue).
type Identity struct{}

// Encoding returns the stable token "identity".
func (Identity) Encoding() string { return EncodingIdentity }

// Compress returns a copy of body unchanged (never aliasing the caller's slice).
func (Identity) Compress(body []byte) ([]byte, error) { return append([]byte(nil), body...), nil }

// Decompress returns a copy of body unchanged (never aliasing the caller's slice).
func (Identity) Decompress(body []byte) ([]byte, error) { return append([]byte(nil), body...), nil }

// Gzip is the built-in gzip content-coding, backed by compress/gzip. Its output
// carries no wall-clock timestamp (the writer's ModTime is left zero), so
// compressing the same body twice yields byte-identical output (§11.4.50).
type Gzip struct{}

// Encoding returns the stable token "gzip".
func (Gzip) Encoding() string { return EncodingGzip }

// Compress returns body gzip-compressed. The writer is always closed so the
// gzip trailer (CRC + length) is flushed before the bytes are returned.
func (Gzip) Compress(body []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(body); err != nil {
		_ = w.Close()
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decompress returns the original bytes from a gzip stream produced by Compress.
func (Gzip) Decompress(body []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

// Deflate is the built-in deflate content-coding, backed by compress/flate as a
// raw DEFLATE (RFC 1951) stream. Output is deterministic (no timestamp). See
// EncodingDeflate for the RFC-1950-zlib interop note.
type Deflate struct{}

// Encoding returns the stable token "deflate".
func (Deflate) Encoding() string { return EncodingDeflate }

// Compress returns body raw-DEFLATE-compressed. The writer is always closed so
// the final block is flushed before the bytes are returned.
func (Deflate) Compress(body []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := flate.NewWriter(&buf, flate.DefaultCompression)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(body); err != nil {
		_ = w.Close()
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decompress returns the original bytes from a raw-DEFLATE stream produced by
// Compress.
func (Deflate) Decompress(body []byte) ([]byte, error) {
	r := flate.NewReader(bytes.NewReader(body))
	defer r.Close()
	return io.ReadAll(r)
}
