package wire

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
)

// Selection errors returned by NewSelector and Select. They are sentinel values
// so callers can classify failures with errors.Is.
var (
	// ErrNoEncoders is returned by NewSelector when it is given no encoders.
	ErrNoEncoders = errors.New("wire: selector requires at least one encoder")
	// ErrEmptyEncoderName is returned by NewSelector when an encoder reports an
	// empty Name.
	ErrEmptyEncoderName = errors.New("wire: encoder name must be non-empty")
	// ErrDuplicateEncoderName is returned by NewSelector when two encoders share
	// a Name (resolution is by stable name, so names must be unique).
	ErrDuplicateEncoderName = errors.New("wire: duplicate encoder name")
	// ErrNilValue is returned by Select for a nil interface value, which has no
	// concrete type to round-trip against.
	ErrNilValue = errors.New("wire: cannot select an encoding for a nil value")
	// ErrNoLosslessEncoder is returned by Select when NONE of the registered
	// encoders round-trips the value losslessly. The engine refuses to ship a
	// corrupt payload; the caller gets the diagnostic Candidates in the Result.
	ErrNoLosslessEncoder = errors.New("wire: no registered encoder round-trips the value losslessly")
)

// Candidate is the per-encoder outcome of a Select call. It is the captured
// evidence for why an encoder won or lost: its output Size, whether it round-
// tripped Losslessly, and any encode/decode Err. A candidate is eligible to win
// only when Lossless is true.
type Candidate struct {
	// Encoder is the encoder's Name.
	Encoder string
	// Size is the byte length of the encoder's output, or 0 if Encode failed.
	Size int
	// Lossless reports whether Decode(Encode(v)) deep-equals v. Only lossless
	// candidates are eligible for selection.
	Lossless bool
	// Err is the encode or decode error, if any. A lossy-but-error-free
	// candidate (Encode+Decode succeeded but the decoded value differs) has
	// Lossless=false and Err=nil.
	Err error
}

// Result is the outcome of a Select call: the chosen encoding, which encoder
// produced it, and the full per-encoder Candidate breakdown (the byte sizes and
// lossless verdicts). On a successful selection Encoder is the winner's name,
// Bytes is its output, and Size is len(Bytes). On ErrNoLosslessEncoder the
// winner fields are zero but Candidates is still populated for diagnostics.
type Result struct {
	// Encoder is the winning encoder's Name (empty on error).
	Encoder string
	// Bytes is the chosen (smallest lossless) encoding (nil on error).
	Bytes []byte
	// Size is len(Bytes) (0 on error).
	Size int
	// Candidates is every encoder's outcome, ordered by encoder Name.
	Candidates []Candidate
}

// Sizes returns a map from encoder Name to the byte length of that encoder's
// output for the selected value (0 for an encoder whose Encode failed). It is a
// convenience view over Candidates.
func (r Result) Sizes() map[string]int {
	m := make(map[string]int, len(r.Candidates))
	for _, c := range r.Candidates {
		m[c.Encoder] = c.Size
	}
	return m
}

// Selector picks the smallest lossless encoding for a value from a fixed set of
// encoders. It is safe for concurrent use by multiple goroutines: after
// construction its encoder set is immutable and Select allocates all per-call
// state fresh, so the shared request fleet can call Select on one Selector
// concurrently. Construct with NewSelector (or Default); the zero value is not
// usable.
type Selector struct {
	// encoders is the immutable, Name-sorted, dedup-validated encoder set. It is
	// never mutated after NewSelector returns, so it is read-safe without a lock.
	encoders []Encoder
}

// NewSelector returns a Selector over encoders. It validates that at least one
// encoder is given, every Name is non-empty, and all Names are unique, and it
// sorts the encoders by Name so selection order (and the tie-break) is
// deterministic (§11.4.50). It returns ErrNoEncoders, ErrEmptyEncoderName, or
// ErrDuplicateEncoderName (wrapped with the offending name) on invalid input.
func NewSelector(encoders ...Encoder) (*Selector, error) {
	if len(encoders) == 0 {
		return nil, ErrNoEncoders
	}
	// Copy so a caller mutating its slice afterwards cannot mutate our set.
	set := make([]Encoder, len(encoders))
	copy(set, encoders)
	seen := make(map[string]struct{}, len(set))
	for _, e := range set {
		name := e.Name()
		if name == "" {
			return nil, ErrEmptyEncoderName
		}
		if _, dup := seen[name]; dup {
			return nil, fmt.Errorf("%w: %q", ErrDuplicateEncoderName, name)
		}
		seen[name] = struct{}{}
	}
	sort.Slice(set, func(i, j int) bool { return set[i].Name() < set[j].Name() })
	return &Selector{encoders: set}, nil
}

// Default returns a Selector containing only the built-in CompactJSON baseline.
// It is the standalone, dependency-free entry point; a consumer that also has a
// TOON (or other) encoder builds its Selector with NewSelector(CompactJSON{},
// toonEncoder, ...) instead. The error from NewSelector is impossible here
// (CompactJSON has a fixed non-empty name and there is exactly one encoder), so
// it is discarded.
func Default() *Selector {
	s, _ := NewSelector(CompactJSON{})
	return s
}

// Select encodes v with every registered encoder, keeps only the encoders whose
// output round-trips losslessly (Decode(Encode(v)) deep-equals v), and returns
// the SMALLEST such encoding. Ties in byte length break on encoder Name
// (ascending) so the choice is deterministic (§11.4.50).
//
// A smaller-but-lossy encoding is NEVER selected: the lossless check is the
// hard floor (§11.4.6). If no encoder round-trips v, Select returns
// ErrNoLosslessEncoder with the diagnostic Candidates populated. A nil v yields
// ErrNilValue. On success Result.Encoder is the winner, Result.Bytes its
// output, and Result.Candidates records every encoder's size and verdict.
func (s *Selector) Select(v any) (Result, error) {
	if v == nil {
		return Result{}, ErrNilValue
	}

	candidates := make([]Candidate, 0, len(s.encoders))
	var (
		winIdx  = -1 // index into candidates of the current smallest lossless
		winSize int
		winByte []byte
	)

	for _, enc := range s.encoders {
		cand := Candidate{Encoder: enc.Name()}

		encoded, err := enc.Encode(v)
		if err != nil {
			cand.Err = err
			candidates = append(candidates, cand)
			continue
		}
		cand.Size = len(encoded)

		lossless, rtErr := roundTrips(enc, v, encoded)
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
		// Because s.encoders is sorted by Name ascending and we replace only on
		// a STRICT decrease, the first encoder reaching a given minimum size
		// wins — i.e. the lexicographically smallest Name breaks a size tie
		// (§11.4.50), without a second comparison.
		if winIdx == -1 || cand.Size < winSize {
			winIdx = len(candidates) - 1
			winSize = cand.Size
			winByte = encoded
		}
	}

	if winIdx == -1 {
		return Result{Candidates: candidates}, ErrNoLosslessEncoder
	}
	return Result{
		Encoder:    candidates[winIdx].Encoder,
		Bytes:      winByte,
		Size:       winSize,
		Candidates: candidates,
	}, nil
}

// roundTrips reports whether decoding enc's output reconstructs a value
// deep-equal to v. It decodes into a fresh addressable value of v's concrete
// type (so, unlike decoding into an interface, a typed target preserves the
// original numeric and container types) and compares with reflect.DeepEqual. A
// decode error is returned to the caller so it can be recorded as the
// candidate's Err; a successful decode whose value differs yields (false, nil)
// — a lossy-but-error-free encoder.
func roundTrips(enc Encoder, v any, encoded []byte) (bool, error) {
	target := reflect.New(reflect.TypeOf(v)) // *T, zeroed
	if err := enc.Decode(encoded, target.Interface()); err != nil {
		return false, err
	}
	return reflect.DeepEqual(target.Elem().Interface(), v), nil
}
