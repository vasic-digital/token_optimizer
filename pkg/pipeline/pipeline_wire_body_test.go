package pipeline

import (
	"errors"
	"reflect"
	"testing"

	"github.com/vasic-digital/token_optimizer/pkg/wire"
)

// TestOptimizePlusWireLLMBodyGracefulRefusalAndNormalize proves the realistic
// request-path COMPOSITION for the WS7 map[string]any "LLM-body" numeric shape:
// the pipeline decides the tier, then the consumer encodes the request body via
// pkg/wire. The composition MUST never crash and MUST never lose numeric
// precision silently, on BOTH the refusal path and the normalize path:
//
//	(1) a raw int-bearing body is SAFELY REFUSED by wire.Select
//	    (ErrNoLosslessEncoder) — the caller handles that refusal path WITHOUT
//	    panic, and the pipeline Decision is entirely unaffected; and
//	(2) normalizing the body first with wire.NormalizeJSONNumbers yields a
//	    losslessly-encodable body, so wire.Select succeeds and the chosen bytes
//	    round-trip to the normalized body exactly (integer values preserved as
//	    their JSON-canonical float64 — no silent precision loss).
//
// The pipeline PACKAGE stays fully decoupled from pkg/wire (§11.4.28): this is a
// TEST-SCOPE composition proving the graceful-accept / normalize contract; no
// production import edge from pipeline to wire is introduced (pkg/pipeline
// production code imports only config + router).
func TestOptimizePlusWireLLMBodyGracefulRefusalAndNormalize(t *testing.T) {
	o := newOptimizer(t, ladder(t))

	// The pipeline decides the tier for a load-bearing request. This MUST succeed
	// independently of whether the body is encodable — the pipeline does not
	// touch the body, so a wire refusal can never crash tier selection.
	dec, err := o.Optimize(req("", "", true), liveExcept())
	if err != nil {
		t.Fatalf("Optimize: %v", err)
	}
	if dec.Tier.Name == "" {
		t.Fatal("Optimize returned an empty tier")
	}

	body := map[string]any{"max_tokens": int(4096), "temperature": float64(0.7)}
	sel := wire.Default()

	// (1) Raw body: wire SAFELY REFUSES rather than shipping a silently-lossy
	//     encoding. The caller handles the refusal WITHOUT panic and the pipeline
	//     Decision is unchanged.
	if _, wErr := sel.Select(body); !errors.Is(wErr, wire.ErrNoLosslessEncoder) {
		t.Fatalf("raw int-bearing body: wire.Select err = %v, want ErrNoLosslessEncoder", wErr)
	}
	if dec.Tier.Name == "" {
		t.Fatal("pipeline Decision corrupted by the wire refusal path")
	}

	// (2) Normalize-before-wire: the int becomes its JSON-canonical float64 and
	//     the body encodes losslessly — no crash, no silent precision loss.
	norm, nErr := wire.NormalizeJSONNumbers(body)
	if nErr != nil {
		t.Fatalf("NormalizeJSONNumbers: %v", nErr)
	}
	res, sErr := sel.Select(norm)
	if sErr != nil {
		t.Fatalf("Select(normalized body): %v", sErr)
	}

	var back map[string]any
	if dErr := (wire.CompactJSON{}).Decode(res.Bytes, &back); dErr != nil {
		t.Fatalf("decode chosen bytes: %v", dErr)
	}
	if !reflect.DeepEqual(back, norm) {
		t.Fatalf("normalized body did not round-trip: got %#v want %#v", back, norm)
	}
	if back["max_tokens"] != float64(4096) {
		t.Fatalf("max_tokens = %#v, want 4096.0 (integer value preserved, no precision loss)", back["max_tokens"])
	}
	// The original body is NOT mutated by normalization.
	if body["max_tokens"] != int(4096) {
		t.Fatalf("NormalizeJSONNumbers mutated the caller's body: max_tokens = %#v, want int(4096)", body["max_tokens"])
	}
}
