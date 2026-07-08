// Package brotli is the WS8 (ATM-659) brotli content-coding for the
// token_optimizer engine's pkg/transport layer.
//
// pkg/transport/compress.go deliberately ships ONLY stdlib-backed compressors
// (Identity, Gzip, Deflate) so the core package stays dependency-free and
// project-not-aware (§11.4.28 decoupling): "a stronger codec (brotli, zstd, a
// project-specific one) is an external module the consumer INJECTS as a
// Compressor — the engine never imports it." This package IS that external
// module for brotli — the exact extension point pkg/transport/negotiate.go's
// Default doc comment sketches:
//
//	n, _ := transport.NewNegotiator(transport.Identity{}, transport.Gzip{},
//	    transport.Deflate{}, brotli.NewDefault())
//
// Backed by github.com/andybalholm/brotli (the same library measured in
// docs/research/tokens/ws8_transport/POC/brotli/brotli_roundtrip.go), which
// this package never re-implements — it only adapts that library to the
// transport.Compressor contract (Encoding / Compress / Decompress) so it can
// be selected, round-trip-verified, and Content-Encoding-negotiated by
// transport.Negotiator exactly like the built-ins.
//
// Content-coding token: "br", the IANA HTTP Content Coding Registry value for
// Brotli (RFC 7932) — the same token every browser and HTTP/2+ peer sends in
// Accept-Encoding / Content-Encoding, and the token
// pkg/transport/negotiate_test.go's TestDecompressMatchingPath already
// anticipates.
//
// Lossless guarantee (§11.4.6): Decompress(Compress(b)) reconstructs b
// byte-for-byte for every b, including empty, nil, binary, and
// already-brotli-compressed bodies — proven by this package's own tests AND
// re-verified per-body at selection time by transport.Negotiator (which never
// trusts a compressor's contract without checking).
//
// Determinism (§11.4.50): compressing the same body at the same quality twice
// yields byte-identical output — the brotli writer carries no wall-clock or
// other per-call non-deterministic state.
package brotli

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	extbrotli "github.com/andybalholm/brotli"

	"github.com/vasic-digital/token_optimizer/pkg/transport"
)

// EncodingToken is the IANA-registered HTTP content-coding token for Brotli
// (RFC 7932, HTTP Content Coding Registry). It is the value this package's
// Encoding() returns and the value it expects in a Content-Encoding header for
// Decompress.
const EncodingToken = "br"

// ErrInvalidQuality is returned by New when quality is outside brotli's own
// documented range [extbrotli.BestSpeed, extbrotli.BestCompression] (0..11).
var ErrInvalidQuality = errors.New("transport/brotli: quality must be between BestSpeed (0) and BestCompression (11) inclusive")

// Compressor is a transport.Compressor backed by brotli at a fixed quality.
// The zero value is quality 0 (BestSpeed) and is valid, but callers should
// prefer New or NewDefault so the quality choice is explicit and validated.
// Compressor holds no mutable state, so a single value is safe to share and
// call concurrently: Compress/Decompress each allocate fresh writer/reader
// state per call.
type Compressor struct {
	quality int
}

// Compile-time proof this package's Compressor genuinely satisfies
// pkg/transport's real Compressor interface — the wiring pkg/transport's own
// docs describe, not a structurally-similar but disconnected type.
var _ transport.Compressor = Compressor{}

// New returns a brotli Compressor at the given quality (0..11, matching
// extbrotli.BestSpeed..extbrotli.BestCompression). It returns ErrInvalidQuality
// for any value outside that range rather than silently clamping or letting an
// invalid level surface as a deep encoder panic.
func New(quality int) (Compressor, error) {
	if quality < extbrotli.BestSpeed || quality > extbrotli.BestCompression {
		return Compressor{}, fmt.Errorf("%w: got %d", ErrInvalidQuality, quality)
	}
	return Compressor{quality: quality}, nil
}

// NewDefault returns a brotli Compressor at brotli's own recommended default
// quality (extbrotli.DefaultCompression == 6) — a reasonable general-purpose
// choice for a consumer that has not measured its own payload class. A
// consumer that HAS measured its payloads (e.g. the cache-sync design in
// docs/research/tokens/ws8_transport/DESIGN.md §2.2, which found q5 captures
// most of brotli's advantage over gzip at much lower CPU cost) should call New
// with its own chosen quality instead — this package ships no project-specific
// quality constant per §11.4.28 decoupling.
func NewDefault() Compressor {
	c, err := New(extbrotli.DefaultCompression)
	if err != nil {
		// Unreachable: extbrotli.DefaultCompression is a library constant inside
		// its own documented [BestSpeed, BestCompression] range.
		panic(fmt.Errorf("transport/brotli: NewDefault: %w", err))
	}
	return c
}

// Quality returns the brotli quality level this Compressor was constructed
// with.
func (c Compressor) Quality() int { return c.quality }

// Encoding returns the stable content-coding token "br".
func (c Compressor) Encoding() string { return EncodingToken }

// Compress returns body brotli-compressed at this Compressor's quality. The
// writer is always closed so the final block is flushed before the bytes are
// returned. Compressing the same body at the same quality always yields
// byte-identical output (§11.4.50) and MUST NOT retain or mutate body.
func (c Compressor) Compress(body []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := extbrotli.NewWriterLevel(&buf, c.quality)
	if _, err := w.Write(body); err != nil {
		_ = w.Close()
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decompress reverses Compress, reconstructing the original bytes from a
// brotli stream produced by Compress (at any quality — quality only affects
// compressed size, never decodability). It MUST NOT retain or mutate its
// argument.
func (c Compressor) Decompress(body []byte) ([]byte, error) {
	r := extbrotli.NewReader(bytes.NewReader(body))
	return io.ReadAll(r)
}
