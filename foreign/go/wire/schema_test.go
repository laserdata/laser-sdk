package wire_test

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenSchemaFixturesWhenDecodedThenShouldReEncodeByteIdentically(t *testing.T) {
	t.Run("logical_schema.bin", func(t *testing.T) {
		roundTripFixture[wire.LogicalSchema](t, "logical_schema.bin")
	})
	t.Run("logical_type_discriminants.bin", func(t *testing.T) {
		types := roundTripFixture[[]wire.LogicalType](t, "logical_type_discriminants.bin")
		if len(types) == 0 {
			t.Fatal("the discriminant fixture lists every logical type")
		}
		// Every listed type must have a canonical code, or a schema carrying it
		// could not be fingerprinted at all.
		for i, logicalType := range types {
			schema := wire.LogicalSchema{
				Schema: wire.LogicalSchemaRef{
					ID: wire.LogicalSchemaID{ID: wire.IDFromBytes([16]byte{15: 1})}, Version: 1,
				},
				Fields: []wire.LogicalField{
					{ID: 1, Name: "f", Required: true, FieldType: logicalType},
				},
			}
			if _, err := schema.ComputeFingerprint(); err != nil {
				t.Errorf("type %d (%s) cannot be fingerprinted: %v", i, logicalType.Kind, err)
			}
		}
	})
	t.Run("typed_value_discriminants.bin", func(t *testing.T) {
		values := roundTripFixture[[]wire.TypedValue](t, "typed_value_discriminants.bin")
		if len(values) == 0 {
			t.Fatal("the discriminant fixture lists every typed value")
		}
	})
}

// The fingerprint is the schema's identity: a schema that claimed one it did
// not have would let a producer and a consumer disagree about field meanings
// while both believing they agreed. Recomputing the corpus fingerprint proves
// the canonical byte layout and the digest match the Rust encoder.
func TestGivenTheCorpusSchemaWhenFingerprintedThenShouldMatchItsStoredDigest(t *testing.T) {
	schema := roundTripFixture[wire.LogicalSchema](t, "logical_schema.bin")

	computed, err := schema.ComputeFingerprint()
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	if !schema.Schema.Fingerprint.Equal(computed) {
		t.Fatalf("recomputed fingerprint differs\n stored:   %x\n computed: %x",
			schema.Schema.Fingerprint.Bytes, computed.Bytes)
	}
	if err := schema.Validate(); err != nil {
		t.Fatalf("the corpus schema must validate: %v", err)
	}
	if len(computed.Bytes) != wire.DigestBytes {
		t.Fatalf("want a %d-byte digest, got %d", wire.DigestBytes, len(computed.Bytes))
	}
}

// A schema whose stored fingerprint does not match its fields must be refused,
// and any change to the fields must change the fingerprint.
func TestGivenAMismatchedFingerprintWhenValidatedThenShouldBeRefused(t *testing.T) {
	schema := roundTripFixture[wire.LogicalSchema](t, "logical_schema.bin")

	tampered := schema
	tampered.Fields = append([]wire.LogicalField(nil), schema.Fields...)
	tampered.Fields[0].Name = tampered.Fields[0].Name + "_renamed"

	if err := tampered.Validate(); err == nil {
		t.Fatal("a renamed field must invalidate the stored fingerprint")
	}

	recomputed, err := tampered.ComputeFingerprint()
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	if schema.Schema.Fingerprint.Equal(recomputed) {
		t.Fatal("renaming a field must change the fingerprint")
	}
}

// The canonical encoding is independent of the CBOR encoding, so every part of
// a schema that carries meaning must move the digest.
func TestGivenSchemaChangesWhenFingerprintedThenShouldChangeTheDigest(t *testing.T) {
	base := wire.LogicalSchema{
		Schema: wire.LogicalSchemaRef{
			ID: wire.LogicalSchemaID{ID: wire.IDFromBytes([16]byte{15: 1})}, Version: 1,
		},
		Fields: []wire.LogicalField{
			{ID: 1, Name: "host", Required: true, FieldType: wire.StringType()},
			{ID: 2, Name: "cpu", Required: false, FieldType: wire.DoubleType()},
		},
	}
	baseline := mustFingerprint(t, base)

	doc := "the host"
	variants := map[string]func(*wire.LogicalSchema){
		"a different schema id":   func(s *wire.LogicalSchema) { s.Schema.ID = wire.LogicalSchemaID{ID: wire.IDFromBytes([16]byte{15: 2})} },
		"a different version":     func(s *wire.LogicalSchema) { s.Schema.Version = 2 },
		"a different field id":    func(s *wire.LogicalSchema) { s.Fields[0].ID = 9 },
		"a different field name":  func(s *wire.LogicalSchema) { s.Fields[0].Name = "hostname" },
		"a flipped required flag": func(s *wire.LogicalSchema) { s.Fields[0].Required = false },
		"a different field type":  func(s *wire.LogicalSchema) { s.Fields[0].FieldType = wire.LongType() },
		"an added doc":            func(s *wire.LogicalSchema) { s.Fields[0].Doc = &doc },
		"a reordered field set":   func(s *wire.LogicalSchema) { s.Fields[0], s.Fields[1] = s.Fields[1], s.Fields[0] },
		"an extra field": func(s *wire.LogicalSchema) {
			s.Fields = append(s.Fields, wire.LogicalField{ID: 3, Name: "x", FieldType: wire.IntType()})
		},
	}

	for name, mutate := range variants {
		altered := base
		altered.Fields = append([]wire.LogicalField(nil), base.Fields...)
		mutate(&altered)
		if mustFingerprint(t, altered).Equal(baseline) {
			t.Errorf("%s must change the fingerprint", name)
		}
	}
}

// A decimal's precision and scale, and a nested type's shape, are part of the
// canonical encoding too.
func TestGivenParameterizedTypesWhenFingerprintedThenShouldIncludeTheirParameters(t *testing.T) {
	withType := func(logicalType wire.LogicalType) wire.LogicalSchema {
		return wire.LogicalSchema{
			Schema: wire.LogicalSchemaRef{
				ID: wire.LogicalSchemaID{ID: wire.IDFromBytes([16]byte{15: 1})}, Version: 1,
			},
			Fields: []wire.LogicalField{
				{ID: 1, Name: "f", Required: true, FieldType: logicalType},
			},
		}
	}

	if mustFingerprint(t, withType(wire.DecimalType(10, 2))).
		Equal(mustFingerprint(t, withType(wire.DecimalType(10, 3)))) {
		t.Error("a decimal's scale must change the fingerprint")
	}
	if mustFingerprint(t, withType(wire.FixedType(16))).
		Equal(mustFingerprint(t, withType(wire.FixedType(32)))) {
		t.Error("a fixed width must change the fingerprint")
	}
	if mustFingerprint(t, withType(wire.ListType(2, true, wire.IntType()))).
		Equal(mustFingerprint(t, withType(wire.ListType(2, false, wire.IntType())))) {
		t.Error("a list's element requirement must change the fingerprint")
	}
	if mustFingerprint(t, withType(wire.MapType(2, wire.StringType(), 3, true, wire.IntType()))).
		Equal(mustFingerprint(t, withType(wire.MapType(2, wire.StringType(), 3, true, wire.LongType())))) {
		t.Error("a map's value type must change the fingerprint")
	}

	// A nested type must round-trip through CBOR as well as fingerprint.
	nested := withType(wire.MapType(2, wire.StringType(), 3, true,
		wire.ListType(4, true, wire.StructType(
			wire.LogicalField{ID: 5, Name: "inner", Required: true, FieldType: wire.UUIDType()},
		))))
	encoded, err := wire.Marshal(nested.Fields[0].FieldType)
	if err != nil {
		t.Fatalf("marshal nested: %v", err)
	}
	var back wire.LogicalType
	if err := wire.Unmarshal(encoded, &back); err != nil {
		t.Fatalf("unmarshal nested: %v", err)
	}
	again, err := wire.Marshal(back)
	if err != nil {
		t.Fatalf("re-marshal nested: %v", err)
	}
	if string(again) != string(encoded) {
		t.Fatal("a nested logical type must re-encode identically")
	}
}

// A schema must name an identity and at least one field, since a fingerprint
// over nothing would be a shared constant rather than an identity.
func TestGivenAnIncompleteSchemaWhenValidatedThenShouldBeRefused(t *testing.T) {
	valid := wire.LogicalSchemaRef{
		ID: wire.LogicalSchemaID{ID: wire.IDFromBytes([16]byte{15: 1})}, Version: 1,
	}

	if err := (wire.LogicalSchema{Schema: valid}).Validate(); err == nil {
		t.Error("a schema with no fields must be refused")
	}
	noID := wire.LogicalSchema{
		Schema: wire.LogicalSchemaRef{Version: 1},
		Fields: []wire.LogicalField{{ID: 1, Name: "f", FieldType: wire.IntType()}},
	}
	if err := noID.Validate(); err == nil {
		t.Error("a schema with no id must be refused")
	}
	noVersion := wire.LogicalSchema{
		Schema: wire.LogicalSchemaRef{ID: valid.ID},
		Fields: []wire.LogicalField{{ID: 1, Name: "f", FieldType: wire.IntType()}},
	}
	if err := noVersion.Validate(); err == nil {
		t.Error("a schema with no version must be refused")
	}

	// A reference's fingerprint width is checked too.
	if err := (wire.LogicalSchemaRef{ID: valid.ID, Version: 1}).Validate(); err == nil {
		t.Error("a reference with a short fingerprint must be refused")
	}
}

func mustFingerprint(t *testing.T, schema wire.LogicalSchema) wire.SchemaFingerprint {
	t.Helper()
	fingerprint, err := schema.ComputeFingerprint()
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	return fingerprint
}
