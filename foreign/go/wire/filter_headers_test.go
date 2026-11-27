package wire_test

import (
	"encoding/binary"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// headerEntry builds one native Iggy header block entry: a text key and a
// typed value, each as a kind byte, a little-endian length, and its bytes.
func headerEntry(key string, kind uint8, value []byte) []byte {
	entry := []byte{2}
	entry = binary.LittleEndian.AppendUint32(entry, uint32(len(key)))
	entry = append(entry, key...)
	entry = append(entry, kind)
	entry = binary.LittleEndian.AppendUint32(entry, uint32(len(value)))
	return append(entry, value...)
}

// A header kind this build does not name keeps its bytes rather than failing
// the record, so a newer producer's header is still compared.
func TestGivenUnknownAndKnownHeadersWhenDecodedThenShouldPreserveBoth(t *testing.T) {
	block := append(headerEntry("future", 99, []byte("opaque")),
		headerEntry("priority", 9, []byte{2})...)

	headers, ok := wire.DecodeRecordHeaders(block)
	if !ok {
		t.Fatal("a block of valid entries must decode")
	}
	if len(headers) != 2 {
		t.Fatalf("want two headers, got %d", len(headers))
	}
	if headers[0].Value.Kind != wire.ScalarRaw ||
		string(headers[0].Value.Raw) != "opaque" {
		t.Errorf("an unknown kind keeps its bytes, got %+v", headers[0].Value)
	}
	if headers[1].Value.Kind != wire.ScalarUint || headers[1].Value.Uint != 2 {
		t.Errorf("a uint8 reads as an unsigned value, got %+v", headers[1].Value)
	}
}

// Reading only the content type steps over every other entry, so a value this
// filter never compares cannot fault the record.
func TestGivenManyHeadersWhenOnlyTheContentTypeIsReadThenShouldDecodeJustThatOne(t *testing.T) {
	block := headerEntry("frostline.unit", 2, []byte("reefer"))
	block = append(block, headerEntry("agdx.ct", 9, []byte{1})...)
	block = append(block, headerEntry("agdx.ct", 9, []byte{3})...)
	// A bool value of 2 is malformed, but this filter never reads it.
	block = append(block, headerEntry("odd", 3, []byte{2})...)

	headers, ok := wire.DecodeContentTypeHeader(block)
	if !ok {
		t.Fatal("the structure parses")
	}
	if len(headers) != 1 {
		t.Fatalf("want one header, got %d", len(headers))
	}
	// The last declaration wins, as it does for every repeated key.
	if headers[0].Key != wire.HeaderContentType ||
		headers[0].Value.Kind != wire.ScalarUint || headers[0].Value.Uint != 3 {
		t.Errorf("want the last declared codec, got %+v", headers[0])
	}

	// A block with no content type declares none.
	none, ok := wire.DecodeContentTypeHeader(
		headerEntry("frostline.unit", 2, []byte("reefer")))
	if !ok || len(none) != 0 {
		t.Errorf("a block without a content type declares none, got %v %v", none, ok)
	}

	// A truncated block, and a content type whose value is the wrong width,
	// both fault.
	truncated := headerEntry("agdx.ct", 9, []byte{1})
	if _, ok := wire.DecodeContentTypeHeader(
		truncated[:len(truncated)-1]); ok {
		t.Error("a truncated block must fault")
	}
	if _, ok := wire.DecodeContentTypeHeader(
		headerEntry("agdx.ct", 9, []byte{1, 2})); ok {
		t.Error("a uint8 that is two bytes wide must fault")
	}
}

// A value that does not fit its declared kind faults the whole block: a filter
// must never compare against a value another client reads differently.
func TestGivenMalformedKnownHeadersWhenDecodedThenShouldFaultTheWholeBlock(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		kind  uint8
		value []byte
	}{
		{"a uint32 one byte wide", 11, []byte{1}},
		{"a bool that is neither", 3, []byte{2}},
		{"a 128-bit integer short of its width", 8, make([]byte, 15)},
		{"a string that is not UTF-8", 2, []byte{255}},
	} {
		block := append(headerEntry("priority", 9, []byte{2}),
			headerEntry("broken", testCase.kind, testCase.value)...)
		if _, ok := wire.DecodeRecordHeaders(block); ok {
			t.Errorf("%s must fault the block", testCase.name)
		}
	}

	// A 128-bit integer at its width compares as raw bytes.
	wide := headerEntry("trace", 13, make([]byte, 16))
	headers, ok := wire.DecodeRecordHeaders(wide)
	if !ok || len(headers) != 1 || headers[0].Value.Kind != wire.ScalarRaw {
		t.Errorf("a 128-bit integer reads as raw bytes, got %v %v", headers, ok)
	}
}

// An entry under a non-text key is stepped over: no predicate can name it, and
// consuming it keeps the rest of the block aligned.
func TestGivenANonTextKeyWhenDecodedThenShouldStepOverTheEntry(t *testing.T) {
	block := []byte{9, 1, 0, 0, 0, 7}
	block = binary.LittleEndian.AppendUint32(append(block, 9), 1)
	block = append(block, 2)
	block = append(block, headerEntry("priority", 9, []byte{2})...)

	headers, ok := wire.DecodeRecordHeaders(block)
	if !ok {
		t.Fatal("a non-text key does not fault the block")
	}
	if len(headers) != 1 || headers[0].Key != "priority" {
		t.Errorf("want only the text-keyed entry, got %+v", headers)
	}
}

// The need a compiled filter reports drives how much of the block is read.
func TestGivenAHeaderNeedWhenDecodingThenShouldReadOnlyWhatIsNeeded(t *testing.T) {
	block := headerEntry("odd", 3, []byte{2})

	if headers, ok := wire.DecodeHeadersFor(wire.NeedNoHeaders, block); !ok ||
		len(headers) != 0 {
		t.Error("a filter needing no headers reads none")
	}
	// The malformed bool is stepped over for the content type alone.
	if _, ok := wire.DecodeHeadersFor(wire.NeedContentType, block); !ok {
		t.Error("the content type need steps over other values")
	}
	if _, ok := wire.DecodeHeadersFor(wire.NeedAllHeaders, block); ok {
		t.Error("reading every header faults on the malformed bool")
	}
}
