package wire

import (
	"errors"
	"math"
	"reflect"
	"testing"
)

// --- WS7 LLM-body shape: numeric map[string]any is never SILENTLY LOSSY -------
//
// The WS7 "LLM-body" is a map[string]any carrying request/response parameters,
// some of which are numeric (max_tokens, temperature, top_p, ...). Because
// encoding/json decodes every JSON number into a float64 when the decode target
// is an interface, an int-typed value does NOT survive a CompactJSON round-trip
// (Decode reconstructs a float64, which is not DeepEqual to the int). The
// Selector's per-value lossless verification catches this and REFUSES with
// ErrNoLosslessEncoder rather than shipping bytes that silently drop the integer
// type. These tests pin that safe-refusal (it is the guard ATM-679 protects),
// plus the losslessly-encodable float64 case, plus the normalize-then-encode
// path that makes an int-bearing body genuinely round-trippable.

// TestSelectNumericMapBodyIntRefusedNotSilentlyLossy is the GREEN regression
// guard for the already-present wire guard: an int / int64-bearing LLM-body map
// is SAFELY REFUSED (ErrNoLosslessEncoder) — never encoded to bytes that would
// silently reconstruct as float64. If the lossless verification ever regressed,
// Select would return a successful-but-lossy encoding and this test would FAIL
// (see TestNumericBodyGuardIsLoadBearing for the paired mutation proof).
func TestSelectNumericMapBodyIntRefusedNotSilentlyLossy(t *testing.T) {
	sel := Default()
	cases := []struct {
		name string
		body map[string]any
	}{
		{"int", map[string]any{"max_tokens": int(4096)}},
		{"int64", map[string]any{"max_tokens": int64(4096)}},
		{"mixed-int-and-float", map[string]any{"max_tokens": int(4096), "temperature": float64(0.7)}},
		{"nested-int", map[string]any{"opts": map[string]any{"seed": int(7)}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := sel.Select(tc.body)
			if !errors.Is(err, ErrNoLosslessEncoder) {
				t.Fatalf("Select(%#v) err = %v, want ErrNoLosslessEncoder (int-bearing body must be REFUSED, never silently lossy)", tc.body, err)
			}
			if res.Encoder != "" || res.Bytes != nil {
				t.Fatalf("on refusal want zero winner, got Encoder=%q Bytes=%q", res.Encoder, res.Bytes)
			}
			// The CompactJSON candidate MUST be recorded as not-lossless — that is
			// the captured evidence for WHY the body was refused.
			var found bool
			for _, c := range res.Candidates {
				if c.Encoder == "compact-json" {
					found = true
					if c.Lossless {
						t.Fatalf("compact-json recorded Lossless=true for an int-bearing body (round-trip actually reconstructs float64)")
					}
				}
			}
			if !found {
				t.Fatalf("compact-json not present in Candidates: %+v", res.Candidates)
			}
		})
	}
}

// TestSelectNumericMapBodyFloat64Lossless proves the complementary case: a body
// whose numbers are already float64 (JSON-canonical) DOES round-trip and is
// encoded losslessly by compact-json — the never-lossy floor accepts it.
func TestSelectNumericMapBodyFloat64Lossless(t *testing.T) {
	sel := Default()
	body := map[string]any{"temperature": float64(0.7), "top_p": float64(0.95), "n": float64(3)}
	res, err := sel.Select(body)
	if err != nil {
		t.Fatalf("Select(%#v) err = %v, want nil (float64 body round-trips losslessly)", body, err)
	}
	if res.Encoder != "compact-json" {
		t.Fatalf("Encoder = %q, want compact-json", res.Encoder)
	}
	var back map[string]any
	if err := (CompactJSON{}).Decode(res.Bytes, &back); err != nil {
		t.Fatalf("re-decode chosen bytes: %v", err)
	}
	if !reflect.DeepEqual(back, body) {
		t.Fatalf("chosen encoding did not round-trip: got %#v want %#v", back, body)
	}
}

// TestNormalizeThenSelectLossless is the ATM-679 RED->GREEN: before
// NormalizeJSONNumbers existed the ONLY safe outcome for an int-bearing body was
// refusal; the normalize path now lets a consumer choose the lossless-encode
// path. NormalizeJSONNumbers converts the ints to their JSON-canonical float64,
// after which Select succeeds AND the chosen bytes round-trip to the normalized
// body exactly (no silent precision loss — 4096 stays 4096.0, 7 stays 7.0).
func TestNormalizeThenSelectLossless(t *testing.T) {
	raw := map[string]any{
		"max_tokens":  int(4096),
		"temperature": float64(0.7),
		"opts":        map[string]any{"seed": int64(7)},
		"stop":        []any{"</s>", int(2)},
	}

	// Sanity: the RAW body is refused (the pre-normalize baseline).
	if _, err := Default().Select(raw); !errors.Is(err, ErrNoLosslessEncoder) {
		t.Fatalf("raw body Select err = %v, want ErrNoLosslessEncoder (pre-normalize baseline)", err)
	}

	norm, err := NormalizeJSONNumbers(raw)
	if err != nil {
		t.Fatalf("NormalizeJSONNumbers: %v", err)
	}

	res, err := Default().Select(norm)
	if err != nil {
		t.Fatalf("Select(normalized) err = %v, want nil (normalized body round-trips losslessly)", err)
	}
	if res.Encoder != "compact-json" {
		t.Fatalf("Encoder = %q, want compact-json", res.Encoder)
	}

	var back map[string]any
	if err := (CompactJSON{}).Decode(res.Bytes, &back); err != nil {
		t.Fatalf("re-decode chosen bytes: %v", err)
	}
	if !reflect.DeepEqual(back, norm) {
		t.Fatalf("normalized body did not round-trip: got %#v want %#v", back, norm)
	}

	// Integer VALUES are preserved as their JSON-canonical float64 — no loss.
	nm := norm.(map[string]any)
	if nm["max_tokens"] != float64(4096) {
		t.Fatalf("max_tokens = %#v, want 4096.0 (integer value preserved)", nm["max_tokens"])
	}
	if seed := nm["opts"].(map[string]any)["seed"]; seed != float64(7) {
		t.Fatalf("opts.seed = %#v, want 7.0 (nested integer value preserved)", seed)
	}
	if elem := nm["stop"].([]any)[1]; elem != float64(2) {
		t.Fatalf("stop[1] = %#v, want 2.0 (slice integer value preserved)", elem)
	}
	// The original input is NOT mutated.
	if raw["max_tokens"] != int(4096) {
		t.Fatalf("NormalizeJSONNumbers mutated the input: max_tokens = %#v, want int(4096)", raw["max_tokens"])
	}
}

// TestNormalizeRefusesNonRepresentable proves normalization NEVER silently loses
// precision: an integer beyond ±2^53, or a non-finite float, is REFUSED with
// ErrNumericNotRepresentable rather than converted with loss.
func TestNormalizeRefusesNonRepresentable(t *testing.T) {
	cases := []struct {
		name string
		body any
	}{
		{"int64 above 2^53", map[string]any{"id": int64(1)<<53 + 1}},     // 2^53+1 not exactly representable
		{"int64 below -2^53", map[string]any{"id": -(int64(1)<<53 + 1)}}, // -(2^53+1)
		{"uint64 above 2^53", map[string]any{"id": uint64(1)<<53 + 1}},
		{"uint64 max", map[string]any{"id": uint64(math.MaxUint64)}},
		{"NaN", map[string]any{"x": math.NaN()}},
		{"+Inf", map[string]any{"x": math.Inf(1)}},
		{"-Inf", map[string]any{"x": math.Inf(-1)}},
		{"nested non-representable", map[string]any{"a": map[string]any{"b": int64(1)<<53 + 5}}},
		{"slice non-representable", map[string]any{"a": []any{int64(1)<<53 + 9}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NormalizeJSONNumbers(tc.body); !errors.Is(err, ErrNumericNotRepresentable) {
				t.Fatalf("NormalizeJSONNumbers(%#v) err = %v, want ErrNumericNotRepresentable (no silent precision loss)", tc.body, err)
			}
		})
	}
}

// TestNormalizeBoundaryExactlyRepresentable proves the boundary is inclusive:
// ±2^53 IS exactly representable and MUST be accepted (only |n| > 2^53 refused).
func TestNormalizeBoundaryExactlyRepresentable(t *testing.T) {
	for _, n := range []int64{1<<53 - 1, 1 << 53, -(1 << 53), 0} {
		nv, err := NormalizeJSONNumbers(n)
		if err != nil {
			t.Fatalf("NormalizeJSONNumbers(%d) err = %v, want accepted (exactly representable)", n, err)
		}
		if nv != float64(n) {
			t.Fatalf("NormalizeJSONNumbers(%d) = %#v, want %v", n, nv, float64(n))
		}
	}
	// uint 2^53 boundary accepted.
	if nv, err := NormalizeJSONNumbers(uint64(1) << 53); err != nil || nv != float64(uint64(1)<<53) {
		t.Fatalf("NormalizeJSONNumbers(uint 2^53) = (%#v, %v), want (%v, nil)", nv, err, float64(uint64(1)<<53))
	}
}

// TestNormalizePassthrough proves non-numeric values (string / bool / nil) and
// already-float64 values pass through unchanged, and nested containers are
// walked without altering non-numeric leaves.
func TestNormalizePassthrough(t *testing.T) {
	body := map[string]any{
		"model":  "gpt-x",
		"stream": true,
		"stop":   []any{"a", "b"},
		"ratio":  float64(0.25),
		"meta":   map[string]any{"tag": "t", "flag": false},
		"absent": nil,
	}
	got, err := NormalizeJSONNumbers(body)
	if err != nil {
		t.Fatalf("NormalizeJSONNumbers: %v", err)
	}
	if !reflect.DeepEqual(got, body) {
		t.Fatalf("non-numeric body changed by normalization:\n got %#v\nwant %#v", got, body)
	}
}

// TestNormalizeNilAndNonContainer covers the trivial entry points.
func TestNormalizeNilAndNonContainer(t *testing.T) {
	if v, err := NormalizeJSONNumbers(nil); err != nil || v != nil {
		t.Fatalf("NormalizeJSONNumbers(nil) = (%#v, %v), want (nil, nil)", v, err)
	}
	if v, err := NormalizeJSONNumbers("plain-string"); err != nil || v != "plain-string" {
		t.Fatalf("NormalizeJSONNumbers(string) = (%#v, %v), want (\"plain-string\", nil)", v, err)
	}
	if v, err := NormalizeJSONNumbers(int(-5)); err != nil || v != float64(-5) {
		t.Fatalf("NormalizeJSONNumbers(int) = (%#v, %v), want (-5.0, nil)", v, err)
	}
}

// TestNumericBodyGuardIsLoadBearing is the paired-mutation proof (§1.1) living
// in the test file, mirroring TestSelectionLogicMutation. It re-implements the
// FORBIDDEN "re-encode-and-compare-bytes" round-trip check — a plausible wrong
// implementation of roundTrips that would WRONGLY accept an int-bearing body as
// lossless: CompactJSON.Encode(intBody) and CompactJSON.Encode(decoded-to-float
// body) produce IDENTICAL bytes (JSON has no int/float distinction), so a
// byte-equality round-trip check reports "lossless" and Select would ship bytes
// that silently drop the integer type. The real Select (typed-decode +
// reflect.DeepEqual) correctly refuses. This test proves the refusal is caused
// by the real guard, so if roundTrips ever regressed to byte-comparison
// TestSelectNumericMapBodyIntRefusedNotSilentlyLossy would flip PASS -> FAIL.
func TestNumericBodyGuardIsLoadBearing(t *testing.T) {
	body := map[string]any{"max_tokens": int(4096)}
	enc := CompactJSON{}

	// The BROKEN "re-encode and compare bytes" round-trip check.
	original, err := enc.Encode(body)
	if err != nil {
		t.Fatalf("Encode(body): %v", err)
	}
	var decoded map[string]any
	if err := enc.Decode(original, &decoded); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	reencoded, err := enc.Encode(decoded)
	if err != nil {
		t.Fatalf("Encode(decoded): %v", err)
	}
	brokenSaysLossless := string(original) == string(reencoded)
	if !brokenSaysLossless {
		t.Fatalf("test premise broken: re-encode(%s) = %s differs from original", original, reencoded)
	}
	// Prove the decoded body is genuinely LOSSY vs the int-typed original — the
	// byte-comparison check hid a real precision-type loss.
	if reflect.DeepEqual(decoded, body) {
		t.Fatalf("test premise broken: decoded body DeepEqual to int-typed original (expected float64 divergence)")
	}

	// The REAL Select must NOT agree with the broken policy: it refuses the body
	// the byte-comparison check would have shipped silently-lossy.
	if _, err := Default().Select(body); !errors.Is(err, ErrNoLosslessEncoder) {
		t.Fatalf("Select agreed with the broken byte-comparison policy (err=%v) — the typed-decode lossless guard is not enforced", err)
	}
}
