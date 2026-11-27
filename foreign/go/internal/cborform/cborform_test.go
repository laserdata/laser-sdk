package cborform_test

import (
	"errors"
	"math"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// A band payload is exactly one CBOR item, so leftover bytes are a decode
// failure rather than a silent skip. This mirrors decode_named in
// wire/src/framing.rs.
func TestGivenTrailingBytesWhenDecodedThenShouldError(t *testing.T) {
	encoded, err := cborform.Encode(cborform.Uint(7))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if _, err := cborform.Decode(append(encoded, 0x00)); !errors.Is(err, cborform.ErrTrailing) {
		t.Fatalf("want ErrTrailing, got %v", err)
	}
}

// A hostile header may declare far more elements than the input holds. Decode
// must fail on the missing bytes rather than preallocate from the declared
// length, matching the Rust robustness test in wire/src/encoding.rs.
func TestGivenAHugeDeclaredLengthWhenDecodedThenShouldRejectWithoutExhaustingMemory(t *testing.T) {
	// array(4_294_967_295) with no elements following.
	huge := []byte{0x9a, 0xff, 0xff, 0xff, 0xff}
	if _, err := cborform.Decode(huge); err == nil {
		t.Fatal("want an error for an array header longer than the input")
	}

	// The same shape nested under a one-entry map, as the Rust test spells it.
	nested := []byte{
		0xa1,
		0x67, 'p', 'a', 'y', 'l', 'o', 'a', 'd',
		0x9a, 0xff, 0xff, 0xff, 0xff,
	}
	if _, err := cborform.Decode(nested); err == nil {
		t.Fatal("want an error for a truncated nested array")
	}
}

func TestGivenATruncatedArgumentWhenDecodedThenShouldError(t *testing.T) {
	for name, input := range map[string][]byte{
		"uint8":  {0x18},
		"uint16": {0x19, 0x01},
		"uint32": {0x1a, 0x01, 0x02},
		"uint64": {0x1b, 0x01, 0x02, 0x03},
		"empty":  {},
	} {
		if _, err := cborform.Decode(input); err == nil {
			t.Errorf("%s: want an error for a truncated argument", name)
		}
	}
}

// The band uses definite lengths throughout. Indefinite forms are rejected so
// a Go decoder never accepts a frame the Rust decoder would refuse.
func TestGivenIndefiniteLengthWhenDecodedThenShouldError(t *testing.T) {
	for name, input := range map[string][]byte{
		"bytes": {0x5f, 0xff},
		"text":  {0x7f, 0xff},
		"array": {0x9f, 0xff},
		"map":   {0xbf, 0xff},
	} {
		if _, err := cborform.Decode(input); err == nil {
			t.Errorf("%s: want an error for an indefinite length item", name)
		}
	}
}

// Map entry order carries meaning: serde writes struct fields in declaration
// order, so a decoder that sorted keys would re-encode different bytes.
func TestGivenAMapWhenRoundTrippedThenShouldPreserveEntryOrder(t *testing.T) {
	// {"b": 1, "a": 2} - deliberately not in sorted key order.
	original := []byte{0xa2, 0x61, 'b', 0x01, 0x61, 'a', 0x02}
	value, err := cborform.Decode(original)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	entries, ok := value.(cborform.Map)
	if !ok {
		t.Fatalf("want a Map, got %T", value)
	}
	if got := string(entries[0].Key.(cborform.Text)); got != "b" {
		t.Fatalf("want the first key to stay %q, got %q", "b", got)
	}
	again, err := cborform.Encode(value)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if string(again) != string(original) {
		t.Fatalf("re-encode reordered the map: % x", again)
	}
}

// Floats ride at the narrowest width that preserves the value exactly. The
// corpus proves the rule end to end, and these pin the individual decisions.
func TestGivenAFloatWhenEncodedThenShouldUseTheNarrowestExactWidth(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		value float64
		head  byte // 0xf9 half, 0xfa single, 0xfb double
	}{
		{"zero", 0, 0xf9},
		{"negative zero", math.Copysign(0, -1), 0xf9},
		{"one", 1, 0xf9},
		{"half representable", 1.5, 0xf9},
		{"half subnormal", math.Ldexp(1, -24), 0xf9},
		{"infinity", math.Inf(1), 0xf9},
		// 1 + 2^-11 needs eleven mantissa bits, one more than a half carries.
		{"needs single", 1.00048828125, 0xfa},
		{"needs double", 1.1, 0xfb},
	} {
		encoded, err := cborform.Encode(cborform.Float{Value: testCase.value, Bits: 64})
		if err != nil {
			t.Errorf("%s: encode: %v", testCase.name, err)
			continue
		}
		if encoded[0] != testCase.head {
			t.Errorf("%s: want head 0x%02x, got 0x%02x", testCase.name, testCase.head, encoded[0])
			continue
		}
		decoded, err := cborform.Decode(encoded)
		if err != nil {
			t.Errorf("%s: decode: %v", testCase.name, err)
			continue
		}
		back := decoded.(cborform.Float).Value
		if back != testCase.value && !(math.IsNaN(back) && math.IsNaN(testCase.value)) {
			t.Errorf("%s: round trip changed the value: want %v, got %v", testCase.name, testCase.value, back)
		}
		if math.Signbit(back) != math.Signbit(testCase.value) {
			t.Errorf("%s: round trip lost the sign", testCase.name)
		}
	}
}

// Integer heads use the shortest legal form, so a re-encode cannot widen a
// field and change the bytes other clients hash or compare.
func TestGivenAnIntegerWhenEncodedThenShouldUseTheShortestHead(t *testing.T) {
	for _, testCase := range []struct {
		value uint64
		want  int // encoded length in bytes
	}{
		{0, 1}, {23, 1}, {24, 2}, {255, 2}, {256, 3},
		{65535, 3}, {65536, 5}, {math.MaxUint32, 5}, {math.MaxUint32 + 1, 9},
	} {
		encoded, err := cborform.Encode(cborform.Uint(testCase.value))
		if err != nil {
			t.Fatalf("encode %d: %v", testCase.value, err)
		}
		if len(encoded) != testCase.want {
			t.Errorf("uint %d: want %d byte(s), got %d", testCase.value, testCase.want, len(encoded))
		}
	}
}
