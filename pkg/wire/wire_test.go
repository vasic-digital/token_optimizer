package wire

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"
)

// --- Test doubles -----------------------------------------------------------
//
// mockEncoder is a fully caller-controlled Encoder used to decouple an
// encoder's OUTPUT SIZE from its round-trip CORRECTNESS, so a test can construct
// an encoder that is (a) lossless but of an arbitrary chosen size, or (b) lossy
// but tiny — the two ingredients the lossless-guarantee tests need.
type mockEncoder struct {
	name   string
	encode func(v any) ([]byte, error)
	decode func(data []byte, v any) error
}

func (m mockEncoder) Name() string                    { return m.name }
func (m mockEncoder) Encode(v any) ([]byte, error)    { return m.encode(v) }
func (m mockEncoder) Decode(data []byte, v any) error { return m.decode(data, v) }

// losslessMock returns an encoder whose output is exactly size bytes yet which
// ALWAYS round-trips value faithfully: Decode reconstructs value from its
// pre-computed correct JSON, independent of the (dummy) encoded bytes. This lets
// a test place a lossless encoder at any chosen size in the competition.
func losslessMock(t *testing.T, name string, size int, value any) mockEncoder {
	t.Helper()
	correct, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("losslessMock(%q): marshal reference value: %v", name, err)
	}
	token := bytes.Repeat([]byte("x"), size)
	return mockEncoder{
		name:   name,
		encode: func(any) ([]byte, error) { return token, nil },
		decode: func(_ []byte, target any) error { return json.Unmarshal(correct, target) },
	}
}

// lossyMock returns an encoder of the given (typically tiny) size whose Decode
// leaves the target at its zero value — so for any non-zero input the round-trip
// FAILS the deep-equal check. It is the "small but corrupt" encoder the
// never-lossy guarantee must reject.
func lossyMock(name string, size int) mockEncoder {
	token := bytes.Repeat([]byte("z"), size)
	return mockEncoder{
		name:   name,
		encode: func(any) ([]byte, error) { return token, nil },
		decode: func([]byte, any) error { return nil }, // leaves target zero -> lossy
	}
}

type sample struct {
	ID    int               `json:"id"`
	Name  string            `json:"name"`
	Tags  []string          `json:"tags"`
	Attrs map[string]string `json:"attrs"`
}

func nonZeroSample() sample {
	return sample{
		ID:    7,
		Name:  "example-payload",
		Tags:  []string{"a", "b", "c"},
		Attrs: map[string]string{"k1": "v1", "k2": "v2"},
	}
}

// --- CompactJSON round-trips a variety of JSON-faithful values --------------

func TestCompactJSONRoundTripsVariety(t *testing.T) {
	sel := Default()
	values := []struct {
		name string
		v    any
	}{
		{"string", "hello world"},
		{"bool", true},
		{"int-map", map[string]int{"one": 1, "two": 2, "three": 3}},
		{"string-map", map[string]string{"k": "v", "kk": "vv"}},
		{"string-slice", []string{"x", "y", "z"}},
		{"int-slice", []int{9, 8, 7, 6}},
		{"struct", nonZeroSample()},
		{"nested-struct-slice", []sample{nonZeroSample(), {ID: 2, Name: "n2"}}},
	}
	for _, tc := range values {
		t.Run(tc.name, func(t *testing.T) {
			res, err := sel.Select(tc.v)
			if err != nil {
				t.Fatalf("Select(%v) unexpected err: %v", tc.v, err)
			}
			if res.Encoder != "compact-json" {
				t.Fatalf("Encoder = %q, want compact-json", res.Encoder)
			}
			if res.Size != len(res.Bytes) || res.Size == 0 {
				t.Fatalf("Size=%d len(Bytes)=%d (want equal, non-zero)", res.Size, len(res.Bytes))
			}
			// Independently confirm the chosen bytes really round-trip.
			target := reflect.New(reflect.TypeOf(tc.v))
			if err := (CompactJSON{}).Decode(res.Bytes, target.Interface()); err != nil {
				t.Fatalf("re-decode chosen bytes: %v", err)
			}
			if !reflect.DeepEqual(target.Elem().Interface(), tc.v) {
				t.Fatalf("chosen encoding did not round-trip: got %#v want %#v",
					target.Elem().Interface(), tc.v)
			}
		})
	}
}

// --- Smallest lossless encoding is chosen (both directions) -----------------

func TestSelectsSmallestLossless(t *testing.T) {
	v := nonZeroSample()
	jsonSize := len(mustJSON(t, v))

	t.Run("custom encoder smaller than json wins", func(t *testing.T) {
		// A lossless encoder 10 bytes long must beat compact-json (larger) and a
		// lossless 500-byte encoder.
		small := losslessMock(t, "aaa-small", 10, v)
		big := losslessMock(t, "zzz-big", 500, v)
		sel := mustSelector(t, CompactJSON{}, small, big)
		res, err := sel.Select(v)
		if err != nil {
			t.Fatalf("Select: %v", err)
		}
		if res.Encoder != "aaa-small" {
			t.Fatalf("Encoder = %q, want aaa-small (smallest lossless); sizes=%v",
				res.Encoder, res.Sizes())
		}
		if res.Size != 10 {
			t.Fatalf("Size = %d, want 10", res.Size)
		}
	})

	t.Run("compact-json wins when all custom encoders are larger", func(t *testing.T) {
		// If min-selection were broken (e.g. it picked the max), this would pick
		// one of the 500/900-byte encoders instead of compact-json.
		big1 := losslessMock(t, "big-1", jsonSize+500, v)
		big2 := losslessMock(t, "big-2", jsonSize+900, v)
		sel := mustSelector(t, CompactJSON{}, big1, big2)
		res, err := sel.Select(v)
		if err != nil {
			t.Fatalf("Select: %v", err)
		}
		if res.Encoder != "compact-json" {
			t.Fatalf("Encoder = %q, want compact-json (smallest); sizes=%v",
				res.Encoder, res.Sizes())
		}
		if res.Size != jsonSize {
			t.Fatalf("Size = %d, want %d", res.Size, jsonSize)
		}
	})
}

// --- THE load-bearing negation: a smaller-but-lossy encoder is REJECTED -----
//
// This is the test that catches a broken lossless guarantee. A 1-byte lossy
// encoder is registered alongside compact-json (much larger, lossless). The
// lossy encoder has the SMALLEST size of all candidates, so a selector that
// picked purely by min byte length WOULD pick it. The assertion that the winner
// is compact-json therefore FAILS if the lossless filter is ever removed —
// verified by TestSelectionLogicMutation below, which re-implements the broken
// "min-size-ignoring-lossless" policy and shows it selects the lossy encoder.
func TestRejectsLossyEvenWhenSmaller(t *testing.T) {
	v := nonZeroSample()
	lossy := lossyMock("aaa-lossy-tiny", 1) // 1 byte; name sorts first
	sel := mustSelector(t, lossy, CompactJSON{})

	res, err := sel.Select(v)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if res.Encoder != "compact-json" {
		t.Fatalf("Encoder = %q, want compact-json — a smaller LOSSY encoder must NOT win", res.Encoder)
	}
	if res.Size <= 1 {
		t.Fatalf("winner Size = %d, want > 1 (the lossy 1-byte output must be rejected)", res.Size)
	}

	// Directly assert the negation's premise: the lossy candidate WAS the
	// smallest-by-size available, yet was correctly not selected.
	sizes := res.Sizes()
	if sizes["aaa-lossy-tiny"] != 1 {
		t.Fatalf("lossy candidate size = %d, want 1", sizes["aaa-lossy-tiny"])
	}
	if sizes["aaa-lossy-tiny"] >= sizes["compact-json"] {
		t.Fatalf("test premise broken: lossy(%d) not smaller than winner(%d)",
			sizes["aaa-lossy-tiny"], sizes["compact-json"])
	}
	// And that the lossy candidate is recorded as not-lossless.
	for _, c := range res.Candidates {
		if c.Encoder == "aaa-lossy-tiny" && c.Lossless {
			t.Fatalf("lossy encoder recorded Lossless=true")
		}
	}
}

// TestSelectionLogicMutation re-implements the FORBIDDEN "smallest by size,
// ignoring the lossless check" policy and proves it diverges from Select. This
// is the paired-mutation proof (§1.1) living inside the test file: it shows the
// broken policy would choose the tiny lossy encoder while Select chooses the
// lossless compact-json — so a Select that ever regressed to the broken policy
// would flip TestRejectsLossyEvenWhenSmaller from PASS to FAIL.
func TestSelectionLogicMutation(t *testing.T) {
	v := nonZeroSample()
	lossy := lossyMock("aaa-lossy-tiny", 1)
	sel := mustSelector(t, lossy, CompactJSON{})

	res, err := sel.Select(v)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}

	// Broken policy: pick the smallest candidate by size, ignoring Lossless.
	brokenWinner := ""
	brokenSize := -1
	for _, c := range res.Candidates {
		if c.Err != nil {
			continue
		}
		if brokenSize == -1 || c.Size < brokenSize {
			brokenSize = c.Size
			brokenWinner = c.Encoder
		}
	}
	if brokenWinner != "aaa-lossy-tiny" {
		t.Fatalf("expected the broken size-only policy to pick the lossy encoder, got %q", brokenWinner)
	}
	if res.Encoder == brokenWinner {
		t.Fatalf("Select agreed with the broken lossy policy (%q) — the lossless guarantee is not enforced", brokenWinner)
	}
}

// --- Deterministic tie-break by encoder Name (§11.4.50) ---------------------

func TestDeterministicTieBreakByName(t *testing.T) {
	v := nonZeroSample()
	// Two lossless encoders of IDENTICAL size, plus a larger compact-json.
	a := losslessMock(t, "aaa-tie", 5, v)
	b := losslessMock(t, "bbb-tie", 5, v)
	sel := mustSelector(t, CompactJSON{}, b, a) // register out of Name order on purpose

	const iterations = 200
	for i := 0; i < iterations; i++ {
		res, err := sel.Select(v)
		if err != nil {
			t.Fatalf("iter %d: %v", i, err)
		}
		if res.Encoder != "aaa-tie" {
			t.Fatalf("iter %d: Encoder = %q, want aaa-tie (lexicographically smallest on a size tie)", i, res.Encoder)
		}
		if res.Size != 5 {
			t.Fatalf("iter %d: Size = %d, want 5", i, res.Size)
		}
	}
}

// --- Property: winner is always lossless AND the smallest lossless ----------

func TestPropertyMinLossless(t *testing.T) {
	rng := rand.New(rand.NewSource(0xA705CE)) // fixed seed -> deterministic (§11.4.50)
	const rounds = 500
	for round := 0; round < rounds; round++ {
		v := randStringMap(rng)

		// A mix of lossless encoders (random sizes) and lossy encoders (random
		// small sizes) plus the real compact-json baseline.
		encoders := []Encoder{CompactJSON{}}
		type expect struct {
			name     string
			size     int
			lossless bool
		}
		var exps []expect
		exps = append(exps, expect{"compact-json", len(mustJSON(t, v)), true})

		nLossless := rng.Intn(4)
		for i := 0; i < nLossless; i++ {
			name := fmt.Sprintf("L%02d-%d", round, i)
			size := 1 + rng.Intn(2000)
			encoders = append(encoders, losslessMock(t, name, size, v))
			exps = append(exps, expect{name, size, true})
		}
		nLossy := rng.Intn(4)
		for i := 0; i < nLossy; i++ {
			name := fmt.Sprintf("X%02d-%d", round, i)
			size := 1 + rng.Intn(3) // tiny, to tempt a size-only selector
			encoders = append(encoders, lossyMock(name, size))
			exps = append(exps, expect{name, size, false})
		}

		sel := mustSelector(t, encoders...)
		res, err := sel.Select(v)
		if err != nil {
			t.Fatalf("round %d: Select: %v", round, err)
		}

		// Compute the expected winner independently: smallest size among lossless,
		// tie-break by name ascending.
		wantName := ""
		wantSize := -1
		for _, e := range exps {
			if !e.lossless {
				continue
			}
			if wantSize == -1 || e.size < wantSize || (e.size == wantSize && e.name < wantName) {
				wantSize = e.size
				wantName = e.name
			}
		}
		if res.Encoder != wantName {
			t.Fatalf("round %d: Encoder = %q (size %d), want %q (size %d); sizes=%v",
				round, res.Encoder, res.Size, wantName, wantSize, res.Sizes())
		}
		if res.Size != wantSize {
			t.Fatalf("round %d: Size = %d, want %d", round, res.Size, wantSize)
		}
		// Invariant: the winner is never a lossy encoder, and no lossless
		// candidate is strictly smaller than the winner.
		for _, c := range res.Candidates {
			if c.Lossless && c.Size < res.Size {
				t.Fatalf("round %d: lossless %q(%d) smaller than winner %q(%d)",
					round, c.Encoder, c.Size, res.Encoder, res.Size)
			}
		}
	}
}

// --- No lossless encoder available -> ErrNoLosslessEncoder ------------------

func TestNoLosslessEncoder(t *testing.T) {
	v := nonZeroSample()
	sel := mustSelector(t, lossyMock("only-lossy", 2))
	res, err := sel.Select(v)
	if !errors.Is(err, ErrNoLosslessEncoder) {
		t.Fatalf("err = %v, want ErrNoLosslessEncoder", err)
	}
	if res.Encoder != "" || res.Bytes != nil {
		t.Fatalf("on error want zero winner, got Encoder=%q Bytes=%v", res.Encoder, res.Bytes)
	}
	if len(res.Candidates) != 1 {
		t.Fatalf("want diagnostic Candidates populated on error, got %d", len(res.Candidates))
	}
}

// --- Nil value --------------------------------------------------------------

func TestSelectNilValue(t *testing.T) {
	_, err := Default().Select(nil)
	if !errors.Is(err, ErrNilValue) {
		t.Fatalf("Select(nil) err = %v, want ErrNilValue", err)
	}
}

// --- NewSelector validation -------------------------------------------------

func TestNewSelectorValidation(t *testing.T) {
	if _, err := NewSelector(); !errors.Is(err, ErrNoEncoders) {
		t.Fatalf("NewSelector() err = %v, want ErrNoEncoders", err)
	}
	empty := mockEncoder{name: "", encode: nil, decode: nil}
	if _, err := NewSelector(empty); !errors.Is(err, ErrEmptyEncoderName) {
		t.Fatalf("NewSelector(empty-name) err = %v, want ErrEmptyEncoderName", err)
	}
	dupA := losslessMock(t, "dup", 1, "x")
	dupB := losslessMock(t, "dup", 2, "x")
	if _, err := NewSelector(dupA, dupB); !errors.Is(err, ErrDuplicateEncoderName) {
		t.Fatalf("NewSelector(dup) err = %v, want ErrDuplicateEncoderName", err)
	}
}

// --- Encode failure is recorded, not fatal ----------------------------------

func TestEncodeFailureRecorded(t *testing.T) {
	v := nonZeroSample()
	sentinel := errors.New("boom")
	failing := mockEncoder{
		name:   "aaa-failing",
		encode: func(any) ([]byte, error) { return nil, sentinel },
		decode: func([]byte, any) error { return nil },
	}
	sel := mustSelector(t, failing, CompactJSON{})
	res, err := sel.Select(v)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if res.Encoder != "compact-json" {
		t.Fatalf("Encoder = %q, want compact-json (the failing encoder is ineligible)", res.Encoder)
	}
	found := false
	for _, c := range res.Candidates {
		if c.Encoder == "aaa-failing" {
			found = true
			if c.Lossless {
				t.Fatalf("failing encoder marked Lossless")
			}
			if !errors.Is(c.Err, sentinel) {
				t.Fatalf("failing candidate Err = %v, want sentinel", c.Err)
			}
			if c.Size != 0 {
				t.Fatalf("failing candidate Size = %d, want 0", c.Size)
			}
		}
	}
	if !found {
		t.Fatalf("failing encoder not present in Candidates")
	}
}

// --- Concurrency: Select is safe for the shared request fleet (-race) -------

func TestSelectConcurrent(t *testing.T) {
	v := nonZeroSample()
	small := losslessMock(t, "aaa-small", 4, v)
	sel := mustSelector(t, CompactJSON{}, small, losslessMock(t, "zzz-big", 1000, v))

	const goroutines = 50
	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	names := make([]string, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				res, err := sel.Select(v)
				if err != nil {
					errs[idx] = err
					return
				}
				names[idx] = res.Encoder
			}
		}(g)
	}
	wg.Wait()
	for g := 0; g < goroutines; g++ {
		if errs[g] != nil {
			t.Fatalf("goroutine %d: %v", g, errs[g])
		}
		if names[g] != "aaa-small" {
			t.Fatalf("goroutine %d: Encoder = %q, want aaa-small", g, names[g])
		}
	}
}

// --- helpers ----------------------------------------------------------------

func mustSelector(t *testing.T, encoders ...Encoder) *Selector {
	t.Helper()
	s, err := NewSelector(encoders...)
	if err != nil {
		t.Fatalf("NewSelector: %v", err)
	}
	return s
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	return b
}

func randStringMap(rng *rand.Rand) map[string]int {
	n := rng.Intn(6)
	m := make(map[string]int, n)
	for i := 0; i < n; i++ {
		m[fmt.Sprintf("k%d", rng.Intn(1000))] = rng.Intn(100000)
	}
	return m
}
