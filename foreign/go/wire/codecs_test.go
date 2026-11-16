package wire_test

import (
	"math"
	"reflect"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// A body the application defines, named by json tags the way the other SDKs
// name the same fields.
type testBody struct {
	ID     uint32            `json:"id"`
	Name   string            `json:"name"`
	Score  float64           `json:"score"`
	Tags   []string          `json:"tags"`
	Counts map[string]int64  `json:"counts"`
	Blob   []byte            `json:"blob"`
	Nested *testNested       `json:"nested"`
	Labels map[string]string `json:"labels"`
}

type testNested struct {
	Flag   bool   `json:"flag"`
	Offset int64  `json:"offset"`
	Note   string `json:"note"`
}

func sampleBody() testBody {
	return testBody{
		ID: 7, Name: "alice", Score: 0.25,
		Tags:   []string{"a", "b"},
		Counts: map[string]int64{"hits": -3},
		Blob:   []byte{0, 128, 255},
		Nested: &testNested{Flag: true, Offset: -9007199254740993, Note: "λ"},
		Labels: map[string]string{"zone": "leo"},
	}
}

func bodyCodecs() []struct {
	name  string
	codec wire.Codec
	// decoder is the matching decode half.
	decoder wire.Decoder
	want    wire.ContentType
} {
	return []struct {
		name    string
		codec   wire.Codec
		decoder wire.Decoder
		want    wire.ContentType
	}{
		{"json", wire.JSONCodec, wire.JSONCodec, wire.ContentJSON},
		{"msgpack", wire.MsgpackCodec, wire.MsgpackCodec, wire.ContentMsgpack},
		{"cbor", wire.CBORCodec, wire.CBORCodec, wire.ContentCBOR},
		{"bson", wire.BSONCodec, wire.BSONCodec, wire.ContentBSON},
	}
}

// Every built-in codec round-trips a body and stamps the content type a
// consumer decodes it by.
func TestGivenABodyWhenEncodedWithEachCodecThenShouldRoundTrip(t *testing.T) {
	for _, testCase := range bodyCodecs() {
		t.Run(testCase.name, func(t *testing.T) {
			if got := testCase.codec.ContentType(); got != testCase.want {
				t.Errorf("content type is %q, want %q", got, testCase.want)
			}
			encoded, err := testCase.codec.Encode(sampleBody())
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			var decoded testBody
			if err := testCase.decoder.Decode(encoded, &decoded); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !reflect.DeepEqual(decoded, sampleBody()) {
				t.Errorf("round trip changed the body:\n got %+v\nwant %+v",
					decoded, sampleBody())
			}
		})
	}
}

// An integer past the exact double range must survive every codec, since the
// filters compare one exactly.
func TestGivenALargeIntegerWhenRoundTrippedThenShouldKeepEveryDigit(t *testing.T) {
	type counter struct {
		Value int64 `json:"value"`
	}
	for _, testCase := range bodyCodecs() {
		t.Run(testCase.name, func(t *testing.T) {
			for _, value := range []int64{
				9007199254740993, -9007199254740993, math.MaxInt64, math.MinInt64,
			} {
				encoded, err := testCase.codec.Encode(counter{Value: value})
				if err != nil {
					t.Fatalf("encode %d: %v", value, err)
				}
				var decoded counter
				if err := testCase.decoder.Decode(encoded, &decoded); err != nil {
					t.Fatalf("decode %d: %v", value, err)
				}
				if decoded.Value != value {
					t.Errorf("%d came back as %d", value, decoded.Value)
				}
			}
		})
	}
}

// A payload that is not what the codec wrote is an error, never a partial
// body: a consumer must not act on half a record.
func TestGivenABrokenPayloadWhenDecodedThenShouldReportAnError(t *testing.T) {
	var decoded testBody
	for _, testCase := range bodyCodecs() {
		t.Run(testCase.name, func(t *testing.T) {
			for _, payload := range [][]byte{nil, {0xff}, []byte("not a body")} {
				if err := testCase.decoder.Decode(payload, &decoded); err == nil {
					t.Errorf("%x must not decode", payload)
				}
			}
			// Bytes after a complete body are not part of it.
			encoded, err := testCase.codec.Encode(sampleBody())
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if err := testCase.decoder.Decode(
				append(encoded, 0x00), &decoded); err == nil {
				t.Error("trailing bytes must not decode")
			}
		})
	}
}

// BSON frames a document, so a bare scalar has no BSON body.
func TestGivenABareScalarWhenEncodedAsBSONThenShouldRefuseIt(t *testing.T) {
	if _, err := wire.BSONCodec.Encode(7); err == nil {
		t.Error("a bare integer is not a BSON document")
	}
	if _, err := wire.MsgpackCodec.Encode(7); err != nil {
		t.Errorf("MessagePack frames a bare scalar: %v", err)
	}
}

// A key-value entry decodes its value through any codec, with JSON as the
// common case.
func TestGivenAKvEntryWhenDecodedThenShouldReadItsValue(t *testing.T) {
	encoded, err := wire.MsgpackCodec.Encode([]string{"a", "b"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	entry := wire.KvEntry{Key: []byte("k"), Value: encoded}
	var names []string
	if err := entry.DecodeValueWith(wire.MsgpackCodec, &names); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(names, []string{"a", "b"}) {
		t.Errorf("want the encoded names, got %v", names)
	}

	jsonEntry := wire.KvEntry{Key: []byte("k"), Value: []byte(`{"flag":true}`)}
	var nested testNested
	if err := jsonEntry.DecodeValue(&nested); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
	if !nested.Flag {
		t.Error("the decoded value carries its field")
	}
}
