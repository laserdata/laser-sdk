package wire_test

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// canonicalHeader mirrors the fixture's per-header capsule: the test-side kind
// classification and the exact bytes Iggy carries.
type canonicalHeader struct {
	Kind  string `cbor:"kind"`
	Bytes []byte `cbor:"bytes"`
}

// canonicalRecord is the whole record as it lands on a topic: the partition
// key, the sorted header block, and the CBOR envelope as the payload.
type canonicalRecord struct {
	PartitionKey string                     `cbor:"partition_key"`
	Headers      map[string]canonicalHeader `cbor:"headers"`
	Payload      []byte                     `cbor:"payload"`
}

// kindFor is the fixture's classification of a header value.
func kindFor(key string) string {
	switch key {
	case wire.HeaderAgentVersion:
		return "u32"
	case wire.HeaderContentType:
		return "u8"
	case wire.HeaderConversationID, wire.HeaderTargetAgentID, wire.HeaderAgentID:
		return "string"
	default:
		return "bytes"
	}
}

// The end-to-end proof for the producing path: the command envelope from the
// corpus, stamped with its headers and assembled into a record, must equal the
// canonical record fixture byte for byte. It ties the envelope encoding, the
// header block, the Iggy value encoding and the partition key together.
func TestGivenTheCanonicalEnvelopeWhenAssembledIntoARecordThenShouldMatchTheFixture(t *testing.T) {
	payload := readFixture(t, "agent_command.bin")

	var envelope wire.AgentEnvelope
	if err := wire.Unmarshal(payload, &envelope); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if err := envelope.Validate(); err != nil {
		t.Fatalf("the canonical envelope must validate: %v", err)
	}

	// The fixture stamps a targeted record, so it does not broadcast.
	block := wire.HeadersForEnvelope(&envelope, wire.ContentJSON, false)
	headers := map[string]canonicalHeader{}
	for _, entry := range block.Encode() {
		headers[entry.Key] = canonicalHeader{
			Kind:  kindFor(entry.Key),
			Bytes: entry.Value.IggyBytes(),
		}
	}

	record := canonicalRecord{
		PartitionKey: envelope.Conversation.String(),
		Headers:      headers,
		Payload:      payload,
	}

	encoded, err := wire.Marshal(record)
	if err != nil {
		t.Fatalf("encode record: %v", err)
	}
	golden := readFixture(t, "agent_record.bin")
	if string(encoded) != string(golden) {
		t.Fatalf("the assembled record differs from the canonical fixture (%d golden, %d built)%s",
			len(golden), len(encoded), describeDiff(golden, encoded))
	}
}

// The soft cap reflects on-wire size, so an oversized value or an inflated
// block is caught before publish rather than at the server.
func TestGivenAHeaderBlockWhenMeasuredThenShouldRespectTheSoftCap(t *testing.T) {
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}
	modest := wire.NewRecordHeaders(conversation).Encode()
	if !wire.WithinSoftCap(modest) {
		t.Fatal("a single conversation header must fit the soft cap")
	}

	oversized := []wire.HeaderEntry{{
		Key:   "agdx.idx.big",
		Value: wire.TextHeader(string(make([]byte, wire.HeaderValueMax+1))),
	}}
	if wire.WithinSoftCap(oversized) {
		t.Fatalf("a value past %d bytes must not fit, since Iggy frames the length as a u8",
			wire.HeaderValueMax)
	}
}

// The numeric header encodings are the substrate's, so they are pinned against
// the widths and byte order the fixture carries.
func TestGivenTypedHeaderValuesWhenEncodedThenShouldUseLittleEndianWidths(t *testing.T) {
	if got := wire.Uint32Header(1).IggyBytes(); string(got) != string([]byte{1, 0, 0, 0}) {
		t.Errorf("u32 must be 4 little-endian bytes, got % x", got)
	}
	if got := wire.Uint8Header(1).IggyBytes(); string(got) != string([]byte{1}) {
		t.Errorf("u8 must be one byte, got % x", got)
	}
	if got := wire.Uint64Header(1).IggyBytes(); len(got) != 8 || got[0] != 1 {
		t.Errorf("u64 must be 8 little-endian bytes, got % x", got)
	}
	if got := wire.TextHeader("planner").IggyBytes(); string(got) != "planner" {
		t.Errorf("text must be UTF-8, got % x", got)
	}
	if got := wire.Float64Header(1).IggyBytes(); len(got) != 8 {
		t.Errorf("f64 must be 8 bytes, got %d", len(got))
	}
}
