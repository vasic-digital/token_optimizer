// Package wire is the WS7 lossless wire-format selection layer of the
// token_optimizer engine.
//
// Given a value and a set of pluggable Encoders, the package encodes the value
// with each, verifies that each encoding round-trips losslessly, and selects
// the SMALLEST encoding among the encoders that round-trip. This is the WS7
// "shape-routed encoder" idea generalised: choose min(candidate encodings) with
// a hard never-lossy guarantee, so the request path always ships the fewest
// bytes WITHOUT ever silently corrupting the payload.
//
// Decoupling (§11.4.28): the package owns the SELECTION and the lossless
// guarantee, NOT any particular encoding algorithm. The TOON encoder (the WS7
// flagship, measured -30.5% / -11.7% in the POC) lives in a separate own-org
// module and is INJECTED by the consumer as an Encoder — the engine never
// imports it. The only encoder built into this package is CompactJSON, a
// stdlib-only baseline so the package is usable and testable standalone; every
// other encoder (TOON, MessagePack, CBOR, a project-specific codec) is
// registered by the consumer at startup. The package ships ZERO project
// constants.
//
// Lossless guarantee (§11.4.6, the load-bearing invariant): an encoding is
// ELIGIBLE only if decoding its output reconstructs a value deep-equal to the
// input. An encoder that produces a smaller output but does NOT round-trip is
// REJECTED, never selected — a tiny-but-lossy encoding losing user data is
// exactly the failure this package exists to prevent. If no registered encoder
// round-trips the value, Select returns ErrNoLosslessEncoder rather than
// shipping a corrupt payload.
//
// Determinism (§11.4.50): encoders are ordered by Name, and the smallest
// eligible encoding wins with a deterministic tie-break by encoder Name, so the
// same value and the same encoder set always select the same encoder — the
// selection is reproducible across the shared request fleet.
package wire

import "encoding/json"

// Encoder is one pluggable wire codec. The engine treats it as opaque: it never
// inspects Name for a magic value and never reaches into an encoder's internals.
// Implementations MUST satisfy the round-trip contract for the value classes
// they claim to support — Decode(Encode(v)) reconstructs a value deep-equal to
// v — for that encoder to be eligible for selection; the Selector VERIFIES this
// per value rather than trusting it (§11.4.6), so an encoder that violates the
// contract for a given value is simply skipped for that value, never selected.
//
// Encode and Decode MUST be deterministic (§11.4.50): encoding the same value
// twice yields identical bytes. The stdlib json codec used by CompactJSON meets
// this (map keys are emitted in sorted order).
type Encoder interface {
	// Name is the encoder's stable identifier used for selection, tie-breaking,
	// and telemetry (e.g. "compact-json", "toon"). It MUST be non-empty and
	// unique within a Selector; resolution is always by this stable name, never
	// by registration order (§11.4.111).
	Name() string
	// Encode serialises v to bytes. A returned error makes the encoder
	// ineligible for that value (it is recorded as a failed candidate, never
	// selected).
	Encode(v any) ([]byte, error)
	// Decode deserialises data into the value pointed to by v (a non-nil
	// pointer), mirroring the encoding/json Unmarshal contract.
	Decode(data []byte, v any) error
}

// CompactJSON is the always-available baseline Encoder. It uses only the Go
// standard library encoding/json, which emits compact JSON (no insignificant
// whitespace) with map keys in sorted order — a deterministic, lossless codec
// for every JSON-faithful value. It is the floor the consumer's injected
// encoders (TOON, etc.) compete against: a consumer encoder is chosen only when
// it round-trips the value AND is at least as small as compact JSON.
type CompactJSON struct{}

// Name returns the stable identifier "compact-json".
func (CompactJSON) Name() string { return "compact-json" }

// Encode marshals v to compact JSON via encoding/json.
func (CompactJSON) Encode(v any) ([]byte, error) { return json.Marshal(v) }

// Decode unmarshals compact JSON into the value pointed to by v.
func (CompactJSON) Decode(data []byte, v any) error { return json.Unmarshal(data, v) }
