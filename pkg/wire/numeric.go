package wire

import (
	"errors"
	"fmt"
	"math"
	"reflect"
)

// ErrNumericNotRepresentable is returned by NormalizeJSONNumbers when a numeric
// value cannot be converted to a JSON number (IEEE-754 float64) WITHOUT losing
// information: an integer whose magnitude exceeds 2^53 (beyond float64's
// exact-integer range), or a floating-point NaN / +Inf / -Inf (which the JSON
// grammar has no representation for). Normalization REFUSES such a value rather
// than silently corrupting it — the never-lossy floor (§11.4.6) applied to the
// pre-wire numeric-normalization step.
var ErrNumericNotRepresentable = errors.New("wire: numeric value not exactly representable as a JSON number (float64)")

// maxExactInt is 2^53, the largest magnitude an integer may have and still be
// EXACTLY representable as an IEEE-754 float64 (the JSON number type): every
// integer n with |n| <= 2^53 round-trips through float64 without loss, and 2^53
// itself is representable while 2^53+1 is not. It is the same boundary as
// JavaScript's Number.MAX_SAFE_INTEGER + 1. Integers beyond it are conservatively
// refused (never silently lossy) even though some are individually representable.
const maxExactInt = int64(1) << 53

// NormalizeJSONNumbers returns a copy of v with every Go-native integer converted
// to the float64 that a JSON round-trip would produce, so that a map[string]any
// body carrying int / int8..int64 / uint / uint8..uint64 values (the WS7
// "LLM-body" shape) can be encoded LOSSLESSLY by the wire Selector instead of
// being safely refused with ErrNoLosslessEncoder.
//
// Why it is needed: encoding/json decodes every JSON number into a float64 when
// the target is an interface (map[string]any). So a body holding an int does NOT
// round-trip through CompactJSON — Decode(Encode(v)) reconstructs the value with
// float64 numbers, which is NOT reflect.DeepEqual to the int-typed original.
// Select therefore (correctly) marks that encoder lossy and returns
// ErrNoLosslessEncoder rather than shipping bytes that silently drop the integer
// type. Normalizing the numbers to their JSON-canonical float64 form up front
// makes the body genuinely round-trippable, so a consumer can choose the
// lossless-encode path instead of the safe-refusal path.
//
// It recurses into map[string]any and []any (the generic JSON container shapes)
// and normalizes scalar integer + float kinds; every other value (string, bool,
// nil, typed struct, typed map/slice, json.Number, etc.) is returned unchanged —
// those are the caller's responsibility and remain protected by the Selector's
// per-value lossless verification.
//
// Never-lossy guarantee (§11.4.6): an integer whose magnitude exceeds 2^53, or a
// non-finite float (NaN / ±Inf), is REFUSED with ErrNumericNotRepresentable
// rather than converted with silent precision loss. The input is never mutated;
// a fresh map / slice is returned so the caller's original body is untouched.
//
// Determinism (§11.4.50): the transform is a pure function of v — the same input
// always yields the same normalized output (or the same error).
func NormalizeJSONNumbers(v any) (any, error) {
	if v == nil {
		return nil, nil
	}

	switch tv := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(tv))
		for k, val := range tv {
			nv, err := NormalizeJSONNumbers(val)
			if err != nil {
				return nil, fmt.Errorf("key %q: %w", k, err)
			}
			out[k] = nv
		}
		return out, nil
	case []any:
		out := make([]any, len(tv))
		for i, val := range tv {
			nv, err := NormalizeJSONNumbers(val)
			if err != nil {
				return nil, fmt.Errorf("index %d: %w", i, err)
			}
			out[i] = nv
		}
		return out, nil
	}

	// Scalars: normalize integer + float kinds via reflection; pass everything
	// else through unchanged.
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n := rv.Int()
		if n > maxExactInt || n < -maxExactInt {
			return nil, fmt.Errorf("%w: integer %d exceeds the ±2^53 exact-float64 range", ErrNumericNotRepresentable, n)
		}
		return float64(n), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		n := rv.Uint()
		if n > uint64(maxExactInt) {
			return nil, fmt.Errorf("%w: unsigned integer %d exceeds the 2^53 exact-float64 range", ErrNumericNotRepresentable, n)
		}
		return float64(n), nil
	case reflect.Float32, reflect.Float64:
		f := rv.Float()
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, fmt.Errorf("%w: non-finite float %v has no JSON representation", ErrNumericNotRepresentable, f)
		}
		return f, nil
	default:
		return v, nil
	}
}
