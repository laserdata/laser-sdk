package wire_test

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// A write names an absolute expiry or a relative lifetime, never both. The
// relative form is resolved once, at fold time, so every replica stores the
// same expiry rather than each resolving against its own clock.
func TestGivenAWriteExpiryWhenResolvedThenShouldYieldOneAbsoluteInstant(t *testing.T) {
	at := uint64(1_700_000_000_000_000)
	ttl := uint64(5_000_000)
	now := uint64(1_600_000_000_000_000)

	absolute, err := wire.ResolveExpiry(&at, nil, now)
	if err != nil || absolute == nil || *absolute != at {
		t.Errorf("an absolute expiry passes through, got %v (%v)", absolute, err)
	}

	relative, err := wire.ResolveExpiry(nil, &ttl, now)
	if err != nil || relative == nil || *relative != now+ttl {
		t.Errorf("a lifetime resolves against the mutation clock, got %v (%v)", relative, err)
	}

	if none, err := wire.ResolveExpiry(nil, nil, now); err != nil || none != nil {
		t.Errorf("a write naming neither form never expires, got %v (%v)", none, err)
	}

	if _, err := wire.ResolveExpiry(&at, &ttl, now); err == nil {
		t.Error("a write naming both forms must be refused")
	}

	// A lifetime past the end of the epoch pins there rather than wrapping to
	// an expiry in the past, which would hide the entry immediately.
	far := ^uint64(0)
	saturated, err := wire.ResolveExpiry(nil, &far, now)
	if err != nil || saturated == nil || *saturated != far {
		t.Errorf("an overlong lifetime saturates, got %v (%v)", saturated, err)
	}
}

// A mutation id keys a recorded outcome, so two spellings of one mutation
// must never record two outcomes.
func TestGivenAMutationIDWhenValidatedThenShouldRefuseAnAmbiguousSpelling(t *testing.T) {
	if err := wire.ValidateMutationID("bind-role-7"); err != nil {
		t.Errorf("a plain id validates: %v", err)
	}
	for name, id := range map[string]string{
		"empty":       "",
		"a separator": "bind_role",
		"a space":     "bind role",
		"a slash":     "bind/role",
	} {
		if err := wire.ValidateMutationID(id); err == nil {
			t.Errorf("%s must be refused", name)
		}
	}
	long := make([]byte, wire.MaxAuthzMutationIDBytes+1)
	for i := range long {
		long[i] = 'a'
	}
	if err := wire.ValidateMutationID(string(long)); err == nil {
		t.Error("an id past the cap must be refused")
	}
}

// A materialization step's operation id comes from the setup's own identity,
// so a repeated setup repeats every step under the same identity and never
// applies one twice.
func TestGivenAMaterializationStepWhenIdentifiedThenShouldRepeatForTheSameSetup(t *testing.T) {
	setup := wire.U128FromUint64(42)
	first := wire.MaterializationOperationID(setup, 1)
	if first != wire.MaterializationOperationID(setup, 1) {
		t.Error("the same setup and step must repeat their identity")
	}
	if first == wire.MaterializationOperationID(setup, 2) {
		t.Error("two steps of one setup must differ")
	}
	if first == wire.MaterializationOperationID(wire.U128FromUint64(43), 1) {
		t.Error("the same step of two setups must differ")
	}
	// Zero is reserved for "no operation", so an identity never lands there.
	if first.IsZero() {
		t.Error("an operation identity is never zero")
	}
}

// A frame is a length prefix and a payload. A reader that mistook a partial
// buffer for a short frame would hand the decoder a truncated record.
func TestGivenAFramedPayloadWhenDecodedThenShouldWaitForTheWholeFrame(t *testing.T) {
	payload := []byte("a managed request")
	framed, err := wire.FrameEncode(payload)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(framed) != 4+len(payload) {
		t.Fatalf("want a 4-byte prefix, got %d bytes for %d", len(framed), len(payload))
	}

	read, consumed, whole, err := wire.FrameDecode(framed)
	if err != nil || !whole {
		t.Fatalf("a whole frame decodes, got whole=%v (%v)", whole, err)
	}
	if string(read) != string(payload) || consumed != len(framed) {
		t.Errorf("want %q spanning %d bytes, got %q spanning %d",
			payload, len(framed), read, consumed)
	}

	// A buffer short of the prefix, and one short of the payload, are both
	// "read more" rather than a failure.
	for name, partial := range map[string][]byte{
		"a partial prefix":  framed[:2],
		"a partial payload": framed[:len(framed)-1],
	} {
		if _, _, whole, err := wire.FrameDecode(partial); err != nil || whole {
			t.Errorf("%s must ask for more bytes, got whole=%v (%v)", name, whole, err)
		}
	}

	// A prefix past the cap is a failure, not a wait: no buffer will ever
	// satisfy it, so a reader must not grow one forever.
	oversized := []byte{0xff, 0xff, 0xff, 0xff}
	if _, _, _, err := wire.FrameDecode(oversized); err == nil {
		t.Error("a frame length past the cap must be refused")
	}
}

// Two documents hold the same JSON when their numbers hold the same value,
// whatever form each was written in. A document that round-tripped through a
// JSON surface may carry a whole number as a float and is the same document,
// so comparing by form would report a state write as applied when it changed
// nothing.
func TestGivenTwoDocumentsWhenComparedThenShouldMatchNumbersByValue(t *testing.T) {
	document := func(t *testing.T, encoded []byte) wire.DynValue {
		t.Helper()
		var value wire.DynValue
		if err := wire.Unmarshal(encoded, &value); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return value
	}
	encode := func(t *testing.T, value any) []byte {
		t.Helper()
		encoded, err := wire.Marshal(value)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		return encoded
	}

	type counted struct {
		Count any `cbor:"count"`
	}
	whole := document(t, encode(t, counted{Count: uint64(7)}))
	asFloat := document(t, encode(t, counted{Count: float64(7)}))
	if !whole.Equal(asFloat) {
		t.Error("a whole number and its float form are the same document")
	}

	fractional := document(t, encode(t, counted{Count: 7.5}))
	if whole.Equal(fractional) {
		t.Error("a fractional number is a different document")
	}

	negative := document(t, encode(t, counted{Count: int64(-7)}))
	negativeFloat := document(t, encode(t, counted{Count: float64(-7)}))
	if !negative.Equal(negativeFloat) {
		t.Error("a negative whole number and its float form are the same document")
	}
	if negative.Equal(whole) {
		t.Error("a number and its negation are different documents")
	}
}
