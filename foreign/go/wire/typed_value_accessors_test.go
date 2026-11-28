package wire_test

import (
	"math"
	"strings"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenATypedValueWhenReadAsTextThenShouldAnswerOnlyAString(t *testing.T) {
	text, found := wire.StringTyped("node-7").AsStr()
	if !found || text != "node-7" {
		t.Fatalf("got %q found %v", text, found)
	}
	for _, value := range []wire.TypedValue{
		wire.NullTyped(), wire.LongTyped(7), wire.BinaryTyped([]byte("node-7")),
	} {
		if text, found := value.AsStr(); found || text != "" {
			t.Fatalf("%s answered %q found %v", value.Kind, text, found)
		}
	}
}

func TestGivenATypedValueWhenReadAsUnsignedThenShouldRefuseNegativesAndOtherKinds(t *testing.T) {
	type answer struct {
		number uint64
		found  bool
	}
	cases := []struct {
		name  string
		value wire.TypedValue
		want  answer
	}{
		{"an int", wire.IntTyped(7), answer{7, true}},
		{"a long", wire.LongTyped(math.MaxInt64), answer{math.MaxInt64, true}},
		{"zero", wire.LongTyped(0), answer{0, true}},
		{"a negative int", wire.IntTyped(-1), answer{0, false}},
		{"a negative long", wire.LongTyped(-1), answer{0, false}},
		{"a double", wire.DoubleTyped(7), answer{0, false}},
		{"a string", wire.StringTyped("7"), answer{0, false}},
		{"null", wire.NullTyped(), answer{0, false}},
	}
	for _, test := range cases {
		number, found := test.value.AsU64()
		if got := (answer{number, found}); got != test.want {
			t.Fatalf("%s: got %+v, want %+v", test.name, got, test.want)
		}
	}
}

func TestGivenAVariantFieldWhenValidatedThenShouldAcceptEveryCanonicalValueAndRefuseARequiredNull(t *testing.T) {
	variant := wire.LogicalType{Kind: wire.KindVariant}
	for _, value := range []wire.TypedValue{
		wire.NullTyped(), wire.BooleanTyped(true), wire.LongTyped(math.MaxInt64),
		wire.DoubleTyped(2.5), wire.StringTyped("text"),
	} {
		if err := value.ValidateAgainst(variant, false); err != nil {
			t.Fatalf("a variant takes any tagged value, %s: %v", value.Kind, err)
		}
	}
	if err := wire.DoubleTyped(math.NaN()).ValidateAgainst(variant, false); err == nil {
		t.Fatal("a value that is not canonical must be refused under a variant too")
	}
	err := wire.NullTyped().ValidateAgainst(variant, true)
	if err == nil || err.Error() != "required value is null" {
		t.Fatalf("got %v", err)
	}
}

func TestGivenAValueWhenValidatedAgainstItsLogicalTypeThenShouldAcceptOnlyTheMatchingShape(t *testing.T) {
	text := wire.StringType()
	long := wire.LongType()
	row := wire.StructType(
		wire.LogicalField{ID: 1, Name: "host", Required: true, FieldType: wire.StringType()},
		wire.LogicalField{ID: 2, Name: "cpu", Required: false, FieldType: wire.LongType()},
	)
	cases := []struct {
		name     string
		value    wire.TypedValue
		against  wire.LogicalType
		required bool
		refusal  string
	}{
		{"a scalar of its type", wire.LongTyped(7), long, true, ""},
		{"an optional null", wire.NullTyped(), long, false, ""},
		{"a required null", wire.NullTyped(), long, true, "required value is null"},
		{"a scalar of another type", wire.StringTyped("7"), long, true,
			"typed value does not match logical type"},
		{"a float that is not finite", wire.FloatTyped(float32(math.Inf(1))), wire.FloatType(), true,
			"floating-point value must be finite"},
		{"a decimal of its precision", wire.DecimalTyped(wire.DecimalValue{Unscaled: []byte{1}, Precision: 10, Scale: 2}),
			wire.DecimalType(10, 2), true, ""},
		{"a decimal of another scale", wire.DecimalTyped(wire.DecimalValue{Unscaled: []byte{1}, Precision: 10, Scale: 2}),
			wire.DecimalType(10, 3), true, "decimal type mismatch: value is (10, 2), schema is (10, 3)"},
		{"a time inside one day", wire.TimeTyped(1), wire.TimeType(), true, ""},
		{"a time past one day", wire.TimeTyped(wire.MicrosPerDay), wire.TimeType(), true,
			"is outside one day in microseconds"},
		{"a uuid of sixteen bytes", wire.UUIDTyped(make([]byte, 16)), wire.UUIDType(), true, ""},
		{"a short uuid", wire.UUIDTyped(make([]byte, 4)), wire.UUIDType(), true, "UUID must be 16 bytes, got 4"},
		{"a fixed value of its length", wire.FixedTyped(make([]byte, 4)), wire.FixedType(4), true, ""},
		{"a fixed value of another length", wire.FixedTyped(make([]byte, 3)), wire.FixedType(4), true,
			"fixed value is 3 bytes, expected 4"},
		{"a struct in field order", wire.StructTyped(
			wire.FieldValue{FieldID: 1, Value: wire.StringTyped("node-7")},
			wire.FieldValue{FieldID: 2, Value: wire.NullTyped()}), row, true, ""},
		{"a struct missing a field", wire.StructTyped(
			wire.FieldValue{FieldID: 1, Value: wire.StringTyped("node-7")}), row, true,
			"struct carries 1 values, schema has 2 fields"},
		{"a struct with another field id", wire.StructTyped(
			wire.FieldValue{FieldID: 1, Value: wire.StringTyped("node-7")},
			wire.FieldValue{FieldID: 3, Value: wire.LongTyped(1)}), row, true,
			"struct field id 3 appears where 2 is required"},
		{"a struct with a required null", wire.StructTyped(
			wire.FieldValue{FieldID: 1, Value: wire.NullTyped()},
			wire.FieldValue{FieldID: 2, Value: wire.LongTyped(1)}), row, true, "required value is null"},
		{"a list of its element", wire.ListTyped(wire.LongTyped(1), wire.LongTyped(2)),
			wire.ListType(1, true, long), true, ""},
		{"a list holding a required null", wire.ListTyped(wire.LongTyped(1), wire.NullTyped()),
			wire.ListType(1, true, long), true, "required value is null"},
		{"a list holding another type", wire.ListTyped(wire.StringTyped("1")),
			wire.ListType(1, true, long), true, "typed value does not match logical type"},
		{"a map in key order", wire.MapTyped(
			wire.MapEntry{Key: wire.StringTyped("a"), Value: wire.LongTyped(1)},
			wire.MapEntry{Key: wire.StringTyped("b"), Value: wire.NullTyped()}),
			wire.MapType(1, text, 2, false, long), true, ""},
		{"a map out of key order", wire.MapTyped(
			wire.MapEntry{Key: wire.StringTyped("b"), Value: wire.LongTyped(1)},
			wire.MapEntry{Key: wire.StringTyped("a"), Value: wire.LongTyped(2)}),
			wire.MapType(1, text, 2, false, long), true,
			"map entries must be strictly ordered by canonical key"},
		{"a map with a null key", wire.MapTyped(
			wire.MapEntry{Key: wire.NullTyped(), Value: wire.LongTyped(1)}),
			wire.MapType(1, text, 2, false, long), true,
			"map key must be a deterministic non-floating primitive"},
		{"a map with a key of another type", wire.MapTyped(
			wire.MapEntry{Key: wire.LongTyped(1), Value: wire.LongTyped(1)}),
			wire.MapType(1, text, 2, false, long), true, "typed value does not match logical type"},
		{"a map with a required null value", wire.MapTyped(
			wire.MapEntry{Key: wire.StringTyped("a"), Value: wire.NullTyped()}),
			wire.MapType(1, text, 2, true, long), true, "required value is null"},
	}
	for _, test := range cases {
		err := test.value.ValidateAgainst(test.against, test.required)
		switch {
		case test.refusal == "" && err != nil:
			t.Fatalf("%s: %v", test.name, err)
		case test.refusal != "" && (err == nil || !strings.Contains(err.Error(), test.refusal)):
			t.Fatalf("%s: got %v, want a refusal holding %q", test.name, err, test.refusal)
		}
	}
}
