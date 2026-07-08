package transport

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
)

// Negotiation errors returned by NewNegotiator, Compress, and Decompress. They
// are sentinel values so callers can classify failures with errors.Is.
var (
	// ErrNoCompressors is returned by NewNegotiator when it is given no
	// compressors.
	ErrNoCompressors = errors.New("transport: negotiator requires at least one compressor")
	// ErrEmptyEncoding is returned by NewNegotiator when a compressor reports an
	// empty Encoding token.
	ErrEmptyEncoding = errors.New("transport: compressor encoding must be non-empty")
	// ErrDuplicateEncoding is returned by NewNegotiator when two compressors
	// share an Encoding token (resolution is by stable token, so tokens must be
	// unique).
	ErrDuplicateEncoding = errors.New("transport: duplicate compressor encoding")
	// ErrNoLosslessCompressor is returned by Compress when NONE of the
	// registered compressors round-trips the body losslessly. The engine refuses
	// to ship a corrupt payload; the caller gets the diagnostic Candidates in the
	// Encoded result. A negotiator that includes Identity never returns this.
	ErrNoLosslessCompressor = errors.New("transport: no registered compressor round-trips the body losslessly")
	// ErrUnknownEncoding is returned by Decompress (and the response-decode glue)
	// when a Content-Encoding names a compressor the negotiator does not have.
	ErrUnknownEncoding = errors.New("transport: no registered compressor for content-encoding")
)

// Candidate is the per-compressor outcome of a Compress call. It is the captured
// evidence for why a compressor won or lost: its output Size, whether it
// round-tripped Losslessly, and any compress/decompress Err. A candidate is
// eligible to win only when Lossless is true.
type Candidate struct {
	// Encoding is the compressor's Encoding token.
	Encoding string
	// Size is the byte length of the compressor's output, or 0 if Compress
	// failed.
	Size int
	// Lossless reports whether Decompress(Compress(body)) equals body byte-for-
	// byte. Only lossless candidates are eligible for selection.
	Lossless bool
	// Err is the compress or decompress error, if any. A lossy-but-error-free
	// candidate (both calls succeeded but the decoded bytes differ) has
	// Lossless=false and Err=nil.
	Err error
}

// Encoded is the outcome of a Compress call: the chosen coding, its bytes, and
// the full per-compressor Candidate breakdown. On success Encoding is the
// winner's token, Body is its output, and Size is len(Body). On
// ErrNoLosslessCompressor the winner fields are zero but Candidates is still
// populated for diagnostics.
type Encoded struct {
	// Encoding is the winning compressor's token (empty on error).
	Encoding string
	// Body is the chosen (smallest lossless) compressed body (nil on error).
	Body []byte
	// Size is len(Body) (0 on error).
	Size int
	// Candidates is every compressor's outcome, ordered by Encoding token.
	Candidates []Candidate
}

// ContentEncoding returns the value that belongs in the Content-Encoding header
// for this result, and whether the header should be set at all. Per RFC 7231
// the "identity" coding is the absence of any transformation and MUST NOT be
// sent as a Content-Encoding value, so an identity win yields ("", false)
// meaning "omit the header"; every other coding yields (Encoding, true). This
// is the single source of truth the HTTP glue uses, so a declared
// Content-Encoding always matches the compressor that produced Body.
func (e Encoded) ContentEncoding() (value string, set bool) {
	return contentEncoding(e.Encoding)
}

// Sizes returns a map from Encoding token to the byte length of that
// compressor's output for the compressed body (0 for a compressor whose
// Compress failed). It is a convenience view over Candidates.
func (e Encoded) Sizes() map[string]int {
	m := make(map[string]int, len(e.Candidates))
	for _, c := range e.Candidates {
		m[c.Encoding] = c.Size
	}
	return m
}

// Negotiator picks the smallest lossless compression for a body from a fixed
// set of compressors and provides the matching decode path. It is safe for
// concurrent use by multiple goroutines: after construction its compressor set
// and lookup map are immutable and Compress allocates all per-call state fresh,
// so the shared request fleet can call Compress/Decompress on one Negotiator
// concurrently. Construct with NewNegotiator (or Default); the zero value is not
// usable.
type Negotiator struct {
	// encoders is the immutable, Encoding-sorted, dedup-validated compressor set.
	// It is never mutated after NewNegotiator returns, so it is read-safe without
	// a lock.
	encoders []Compressor
	// byEncoding resolves a content-coding token to its compressor for the decode
	// path. Immutable after construction (§11.4.111 resolve-by-name).
	byEncoding map[string]Compressor
}

// NewNegotiator returns a Negotiator over compressors. It validates that at
// least one compressor is given, every Encoding is non-empty, and all Encoding
// tokens are unique, and it sorts the compressors by Encoding so selection order
// (and the tie-break) is deterministic (§11.4.50). It returns ErrNoCompressors,
// ErrEmptyEncoding, or ErrDuplicateEncoding (wrapped with the offending token)
// on invalid input.
func NewNegotiator(compressors ...Compressor) (*Negotiator, error) {
	if len(compressors) == 0 {
		return nil, ErrNoCompressors
	}
	// Copy so a caller mutating its slice afterwards cannot mutate our set.
	set := make([]Compressor, len(compressors))
	copy(set, compressors)
	byEnc := make(map[string]Compressor, len(set))
	for _, c := range set {
		token := c.Encoding()
		if token == "" {
			return nil, ErrEmptyEncoding
		}
		if _, dup := byEnc[token]; dup {
			return nil, fmt.Errorf("%w: %q", ErrDuplicateEncoding, token)
		}
		byEnc[token] = c
	}
	sort.Slice(set, func(i, j int) bool { return set[i].Encoding() < set[j].Encoding() })
	return &Negotiator{encoders: set, byEncoding: byEnc}, nil
}

// Default returns a Negotiator containing the stdlib-only built-ins Identity,
// Gzip, and Deflate. It is the standalone, dependency-free entry point; a
// consumer that also has a brotli (or other) compressor builds its Negotiator
// with NewNegotiator(Identity{}, Gzip{}, Deflate{}, brotliCompressor, ...)
// instead. The error from NewNegotiator is impossible here (three fixed,
// distinct, non-empty tokens), so it is discarded.
func Default() *Negotiator {
	n, _ := NewNegotiator(Identity{}, Gzip{}, Deflate{})
	return n
}

// Encodings returns the registered content-coding tokens, sorted ascending. It
// is a read-only view for telemetry and Accept-Encoding construction.
func (n *Negotiator) Encodings() []string {
	out := make([]string, len(n.encoders))
	for i, c := range n.encoders {
		out[i] = c.Encoding()
	}
	return out
}

// Compress compresses body with every registered compressor, keeps only the
// compressors whose output round-trips losslessly (Decompress(Compress(body))
// equals body byte-for-byte), and returns the SMALLEST such result. Ties in byte
// length break on Encoding token (ascending) so the choice is deterministic
// (§11.4.50).
//
// A smaller-but-lossy result is NEVER selected: the lossless check is the hard
// floor (§11.4.6) — declaring a Content-Encoding for a body the peer cannot
// faithfully reconstruct is wire corruption. If no compressor round-trips body,
// Compress returns ErrNoLosslessCompressor with the diagnostic Candidates
// populated (a negotiator that includes Identity never hits this, since Identity
// always round-trips). A nil or empty body is valid and normally selects
// Identity, because compression framing overhead makes every real coding larger
// than an empty raw body. On success Encoded.Encoding is the winner,
// Encoded.Body its output, and Encoded.Candidates records every compressor's
// size and verdict.
func (n *Negotiator) Compress(body []byte) (Encoded, error) {
	candidates := make([]Candidate, 0, len(n.encoders))
	var (
		winIdx  = -1 // index into candidates of the current smallest lossless
		winSize int
		winBody []byte
	)

	for _, c := range n.encoders {
		cand := Candidate{Encoding: c.Encoding()}

		compressed, err := c.Compress(body)
		if err != nil {
			cand.Err = err
			candidates = append(candidates, cand)
			continue
		}
		cand.Size = len(compressed)

		lossless, rtErr := roundTrips(c, body, compressed)
		if rtErr != nil {
			cand.Err = rtErr
			candidates = append(candidates, cand)
			continue
		}
		cand.Lossless = lossless
		candidates = append(candidates, cand)
		if !lossless {
			continue
		}

		// Eligible. Keep it only if strictly smaller than the current winner.
		// Because n.encoders is sorted by Encoding ascending and we replace only
		// on a STRICT decrease, the first compressor reaching a given minimum
		// size wins — i.e. the lexicographically smallest Encoding breaks a size
		// tie (§11.4.50), without a second comparison.
		if winIdx == -1 || cand.Size < winSize {
			winIdx = len(candidates) - 1
			winSize = cand.Size
			winBody = compressed
		}
	}

	if winIdx == -1 {
		return Encoded{Candidates: candidates}, ErrNoLosslessCompressor
	}
	return Encoded{
		Encoding:   candidates[winIdx].Encoding,
		Body:       winBody,
		Size:       winSize,
		Candidates: candidates,
	}, nil
}

// Decompress reverses Compress for a body received with the given
// content-coding token. An empty token or "identity" means no transformation
// was applied and the body is returned as a copy. Otherwise the token is
// resolved to its registered compressor and its Decompress is used. Decompress
// returns ErrUnknownEncoding (wrapped with the token) when the token names a
// coding the negotiator does not have. This is the matching decode path for
// Compress: a body compressed by this negotiator and labelled with the
// resulting Content-Encoding always decodes here.
func (n *Negotiator) Decompress(encoding string, body []byte) ([]byte, error) {
	if encoding == "" {
		encoding = EncodingIdentity
	}
	if c, ok := n.byEncoding[encoding]; ok {
		return c.Decompress(body)
	}
	if encoding == EncodingIdentity {
		// Identity means "no coding"; honour it even when no Identity compressor
		// was registered, since the absence of a Content-Encoding header is a
		// valid, common case.
		return append([]byte(nil), body...), nil
	}
	return nil, fmt.Errorf("%w: %q", ErrUnknownEncoding, encoding)
}

// roundTrips reports whether decompressing c's output reconstructs body
// byte-for-byte. bytes.Equal treats nil and empty as equal, so an empty body is
// handled without a nil-vs-empty trap. A decompress error is returned to the
// caller so it can be recorded as the candidate's Err; a successful decompress
// whose bytes differ yields (false, nil) — a lossy-but-error-free compressor.
func roundTrips(c Compressor, body, compressed []byte) (bool, error) {
	decompressed, err := c.Decompress(compressed)
	if err != nil {
		return false, err
	}
	return bytes.Equal(decompressed, body), nil
}

// contentEncoding maps a chosen coding to the value that belongs in the
// Content-Encoding header and whether the header should be set. Per RFC 7231 the
// "identity" coding (and the empty token) is the absence of any transformation
// and MUST NOT be sent as a Content-Encoding value, so it maps to ("", false)
// meaning "omit the header"; every other coding maps to (enc, true).
func contentEncoding(enc string) (value string, set bool) {
	if enc == "" || enc == EncodingIdentity {
		return "", false
	}
	return enc, true
}
