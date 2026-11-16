package wire_test

import (
	"strings"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func cborFilter(t *testing.T, expr wire.FilterExpr) *wire.CompiledFilter {
	t.Helper()
	compiled, err := wire.CompileFilter(wire.NewConsumerFilter(expr, wire.CodecCBOR))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return compiled
}

// The shared verdict corpus carries no CBOR payload, so the CBOR decoder's
// rules are pinned here: a filter reading a CBOR record must reach the same
// values the reference decoder builds, and refuse the same payloads.
func TestGivenACBORPayloadWhenEvaluatedThenShouldReadTheSameValues(t *testing.T) {
	limits := wire.DefaultDecodeLimits()
	compiled := cborFilter(t,
		wire.ExprPredicate("mode", wire.CmpEq, wire.StringTyped("safe")))

	// {"mode": "safe"}
	payload := []byte{0xa1, 0x64, 'm', 'o', 'd', 'e', 0x64, 's', 'a', 'f', 'e'}
	if verdict := compiled.Evaluate(
		wire.FilterRecord{Payload: payload}, limits); verdict != wire.VerdictSelected {
		t.Errorf("a matching CBOR record is %q", verdict)
	}

	// An integer field compares exactly, whatever its CBOR head width is.
	counted := cborFilter(t, wire.ExprPredicate("n", wire.CmpEq, wire.LongTyped(300)))
	// {"n": 300} with a two-byte argument
	wide := []byte{0xa1, 0x61, 'n', 0x19, 0x01, 0x2c}
	if verdict := counted.Evaluate(
		wire.FilterRecord{Payload: wide}, limits); verdict != wire.VerdictSelected {
		t.Errorf("a wide CBOR integer is %q", verdict)
	}

	// A negative integer rides as -1-n and must read back as itself.
	negative := cborFilter(t, wire.ExprPredicate("n", wire.CmpEq, wire.LongTyped(-300)))
	if verdict := negative.Evaluate(
		wire.FilterRecord{Payload: []byte{0xa1, 0x61, 'n', 0x39, 0x01, 0x2b}},
		limits); verdict != wire.VerdictSelected {
		t.Errorf("a negative CBOR integer is %q", verdict)
	}

	// A byte string compares as its octets, so an index addresses one.
	indexed := cborFilter(t, wire.ExprPredicate("b[1]", wire.CmpEq, wire.LongTyped(2)))
	if verdict := indexed.Evaluate(
		wire.FilterRecord{Payload: []byte{0xa1, 0x61, 'b', 0x43, 1, 2, 3}},
		limits); verdict != wire.VerdictSelected {
		t.Errorf("a CBOR byte string reads as octets, got %q", verdict)
	}
}

func TestGivenAnUndecodableCBORPayloadWhenEvaluatedThenShouldFaultWithItsReason(t *testing.T) {
	compiled := cborFilter(t,
		wire.ExprPredicate("mode", wire.CmpEq, wire.StringTyped("safe")))

	cases := []struct {
		name    string
		payload []byte
		limits  wire.DecodeLimits
		reason  wire.FaultReason
	}{
		{
			// A tag needs an explicit semantic profile, so one is never
			// silently unwrapped.
			name:    "a tagged payload",
			payload: []byte{0xc1, 0x01},
			limits:  wire.DefaultDecodeLimits(),
			reason:  wire.FaultMalformed,
		},
		{
			// An indefinite length has no canonical encoding.
			name:    "an indefinite length",
			payload: []byte{0xbf, 0x61, 'a', 0x01, 0xff},
			limits:  wire.DefaultDecodeLimits(),
			reason:  wire.FaultMalformed,
		},
		{
			name:    "a repeated map key",
			payload: []byte{0xa2, 0x61, 'a', 0x01, 0x61, 'a', 0x02},
			limits:  wire.DefaultDecodeLimits(),
			reason:  wire.FaultMalformed,
		},
		{
			// A field path addresses text keys, so a map keyed otherwise has
			// nothing a filter can read.
			name:    "a non-text map key",
			payload: []byte{0xa1, 0x01, 0x02},
			limits:  wire.DefaultDecodeLimits(),
			reason:  wire.FaultMalformed,
		},
		{
			name:    "trailing bytes",
			payload: []byte{0xa0, 0x00},
			limits:  wire.DefaultDecodeLimits(),
			reason:  wire.FaultMalformed,
		},
		{
			name:    "a payload above the size limit",
			payload: []byte{0xa0},
			limits:  wire.DecodeLimits{MaxPayloadBytes: 0, MaxDepth: 64},
			reason:  wire.FaultTooLarge,
		},
		{
			// [[1]] nests two levels under a one-level limit.
			name:    "a payload below the depth limit",
			payload: []byte{0x81, 0x81, 0x01},
			limits:  wire.DecodeLimits{MaxPayloadBytes: 1024, MaxDepth: 1},
			reason:  wire.FaultTooDeep,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			verdict, reason := compiled.EvaluateWithFault(
				wire.FilterRecord{Payload: testCase.payload}, testCase.limits)
			if verdict != wire.VerdictFault {
				t.Errorf("verdict is %q, want a fault", verdict)
			}
			if reason != testCase.reason {
				t.Errorf("reason is %q, want %q", reason, testCase.reason)
			}
		})
	}
}

// A filter reads only what it needs, so a caller can skip decoding headers it
// will never compare.
func TestGivenAFilterWhenAskedWhatItReadsThenShouldNameOnlyWhatItCompares(t *testing.T) {
	headerOnly, err := wire.CompileFilter(wire.NewConsumerFilter(
		wire.ExprHeaderPredicate("table", wire.CmpEq, wire.StringTyped("satellites")),
		wire.CodecHeadersOnly))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if headerOnly.HeaderNeed() != wire.NeedAllHeaders || headerOnly.ReadsPayload() {
		t.Error("a header filter needs the headers and not the payload")
	}

	payloadOnly, err := wire.CompileFilter(wire.NewConsumerFilter(
		wire.ExprPredicate("mode", wire.CmpEq, wire.StringTyped("safe")), wire.CodecJSON))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	// A payload filter still reads the content type, to keep a record in
	// another codec out of the decoder.
	if payloadOnly.HeaderNeed() != wire.NeedContentType || !payloadOnly.ReadsPayload() {
		t.Error("a payload filter needs only the content type")
	}
	if payloadOnly.ReadsHeaders() {
		t.Error("a payload filter compares no user header")
	}
}

// The policy resolution a reader applies to a fault, which decides whether the
// page stops, drops the record, or delivers it unevaluated.
func TestGivenAFaultReasonWhenResolvedThenShouldFollowItsOwningPolicy(t *testing.T) {
	filter := wire.NewConsumerFilter(
		wire.ExprPredicate("mode", wire.CmpEq, wire.StringTyped("safe")), wire.CodecJSON)
	filter.ForeignPolicy = wire.PolicyPass
	filter.MismatchPolicy = wire.PolicyReject
	filter.FaultPolicy = wire.FaultDrop
	compiled, err := wire.CompileFilter(filter)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	if got := compiled.PolicyFor(wire.FaultForeignCodec); got != wire.FaultPass {
		t.Errorf("a foreign record follows the foreign policy, got %q", got)
	}
	// A reject record policy never reaches a fault, so it resolves to drop.
	if got := compiled.PolicyFor(wire.FaultTypeMismatch); got != wire.FaultDrop {
		t.Errorf("a mismatch under reject resolves to drop, got %q", got)
	}
	// A payload broken in the filter's own codec is a decode fault, which
	// neither record policy covers.
	if _, named := compiled.RecordPolicy(wire.FaultMalformed); named {
		t.Error("a decode fault has no record policy")
	}
	if got := compiled.PolicyFor(wire.FaultMalformed); got != wire.FaultDrop {
		t.Errorf("a decode fault follows the fault policy, got %q", got)
	}
}

// A JSON payload must obey the same rules whether or not the filter reads the
// field it is in, so one SDK never selects a record another calls malformed.
func TestGivenStrictJSONRulesWhenAPayloadBreaksThemThenShouldFault(t *testing.T) {
	compiled, err := wire.CompileFilter(wire.NewConsumerFilter(
		wire.ExprPredicate("mode", wire.CmpEq, wire.StringTyped("safe")), wire.CodecJSON))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	limits := wire.DefaultDecodeLimits()

	for _, payload := range []string{
		`{"mode":"safe","unused":1e400}`,
		`{"mode":"safe","unused":"\ud800"}`,
		`{"mode":"safe"} ` + "\x00",
		`{"mode":"safe",}`,
		`{"mode":"safe"}{}`,
		`{"mode":01}`,
		"{\"mode\":\"sa\tfe\"}",
	} {
		verdict, reason := compiled.EvaluateWithFault(
			wire.FilterRecord{Payload: []byte(payload)}, limits)
		if verdict != wire.VerdictFault || reason != wire.FaultMalformed {
			t.Errorf("%q is %q/%q, want a malformed fault", payload, verdict, reason)
		}
	}

	// A deeply nested payload is refused by the byte scan, before the parser
	// recurses into it.
	deep := strings.Repeat("[", 200) + strings.Repeat("]", 200)
	verdict, reason := compiled.EvaluateWithFault(
		wire.FilterRecord{Payload: []byte(deep)}, limits)
	if verdict != wire.VerdictFault || reason != wire.FaultTooDeep {
		t.Errorf("a deep payload is %q/%q, want a too-deep fault", verdict, reason)
	}

	// Brackets inside a string are data, not structure.
	nested := `{"mode":"safe","note":"` + strings.Repeat("[", 200) + `"}`
	if got := compiled.Evaluate(
		wire.FilterRecord{Payload: []byte(nested)}, limits); got != wire.VerdictSelected {
		t.Errorf("brackets in a string are %q", got)
	}
}

// Named Avro types resolve by their full name, so a schema that refers back to
// a record declared earlier decodes rather than failing. The shared corpus has
// no recursive schema, so the rule is pinned here.
func TestGivenARecursiveAvroSchemaWhenDecodedThenShouldResolveTheNamedType(t *testing.T) {
	const schema = `{"type":"record","name":"Node","namespace":"tree","fields":[
		{"name":"value","type":"long"},
		{"name":"next","type":["null","Node"]}]}`
	schemas := []wire.SchemaDef{{ID: 1, Source: wire.AvroSchema(schema)}}

	// value = 1, next = Node{value = 2, next = null}
	payload := []byte{0x02, 0x02, 0x04, 0x00}
	headers := []wire.FilterHeader{{
		Key: wire.HeaderSchemaID, Value: wire.UintHeaderScalar(1),
	}}

	for _, testCase := range []struct {
		field string
		want  int64
	}{{"value", 1}, {"next.value", 2}} {
		filter := wire.NewConsumerFilter(
			wire.ExprPredicate(testCase.field, wire.CmpEq, wire.LongTyped(testCase.want)),
			wire.CodecAvro)
		filter.SchemaRefs = []uint32{1}
		compiled, err := wire.CompileFilterWithSchemas(filter, schemas)
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		verdict := compiled.Evaluate(
			wire.FilterRecord{Payload: payload, Headers: headers},
			wire.DefaultDecodeLimits())
		if verdict != wire.VerdictSelected {
			t.Errorf("%s should be %d, got %q", testCase.field, testCase.want, verdict)
		}
	}

	// The union's null branch leaves the nested record absent, not null-valued
	// at a deeper path.
	absent, err := wire.ExprAbsentPath("next.next.value")
	if err != nil {
		t.Fatalf("parse the path: %v", err)
	}
	filter := wire.NewConsumerFilter(absent, wire.CodecAvro)
	filter.SchemaRefs = []uint32{1}
	compiled, err := wire.CompileFilterWithSchemas(filter, schemas)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if verdict := compiled.Evaluate(
		wire.FilterRecord{Payload: payload, Headers: headers},
		wire.DefaultDecodeLimits()); verdict != wire.VerdictSelected {
		t.Errorf("a null union branch has no fields, got %q", verdict)
	}
}

// The CBOR vectors the reference decoder pins: an unsigned value above the
// signed range keeps every digit, and a byte string reads as its octets.
func TestGivenTheReferenceCBORVectorsWhenDecodedThenShouldKeepExactValues(t *testing.T) {
	// [18446744073709551615, h'00FF']
	payload := []byte{
		0x82, 0x1b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x42, 0x00, 0xff,
	}
	limits := wire.DefaultDecodeLimits()

	// An unsigned value above the signed range must stay above it, which a
	// double would not.
	above := cborFilter(t,
		wire.ExprPredicate("[0]", wire.CmpGt, wire.LongTyped(9223372036854775807)))
	if verdict := above.Evaluate(
		wire.FilterRecord{Payload: payload}, limits); verdict != wire.VerdictSelected {
		t.Errorf("an unsigned above the signed range is %q", verdict)
	}
	for index, octet := range []int64{0, 255} {
		field := "[1][" + string(rune('0'+index)) + "]"
		compiled := cborFilter(t, wire.ExprPredicate(field, wire.CmpEq,
			wire.LongTyped(octet)))
		if verdict := compiled.Evaluate(
			wire.FilterRecord{Payload: payload}, limits); verdict != wire.VerdictSelected {
			t.Errorf("%s should be %d, got %q", field, octet, verdict)
		}
	}

	// The payloads the reference decoder refuses.
	compiled := cborFilter(t,
		wire.ExprPredicate("mode", wire.CmpEq, wire.StringTyped("safe")))
	for _, malformed := range [][]byte{
		{0x61, 0xff},
		{0x9a, 1, 0, 0, 0},
		{0x3b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
	} {
		verdict, reason := compiled.EvaluateWithFault(
			wire.FilterRecord{Payload: malformed}, limits)
		if verdict != wire.VerdictFault || reason != wire.FaultMalformed {
			t.Errorf("%x is %q/%q, want a malformed fault", malformed, verdict, reason)
		}
	}
}
