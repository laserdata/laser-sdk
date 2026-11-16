package wire_test

import (
	"strings"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// The id text form is the one users and consoles see, and it appears verbatim
// in the golden corpus, so the walk must reproduce it exactly.
func TestGivenTheCorpusIDTextWhenParsedThenShouldRoundTrip(t *testing.T) {
	// The conversation id carried by wire/fixtures/agent_record.bin.
	const fromFixture = "01J0Y1ZAG00000000000000002"

	parsed, err := wire.ParseID(fromFixture)
	if err != nil {
		t.Fatalf("parse %q: %v", fromFixture, err)
	}
	if got := parsed.String(); got != fromFixture {
		t.Fatalf("round trip changed the text: want %q, got %q", fromFixture, got)
	}
}

func TestGivenIDTextWhenParsedThenShouldAcceptLowerCaseAndRejectBadInput(t *testing.T) {
	const canonical = "01J0Y1ZAG00000000000000002"

	lower, err := wire.ParseID(strings.ToLower(canonical))
	if err != nil {
		t.Fatalf("lower case should parse: %v", err)
	}
	if lower.String() != canonical {
		t.Fatalf("lower case parsed to %q", lower.String())
	}

	for name, bad := range map[string]string{
		"too short":        "01J0Y1ZAG",
		"too long":         canonical + "0",
		"letter u":         "01J0Y1ZAG0000000000000000U",
		"letter i":         "01J0Y1ZAG0000000000000000I",
		"overflowing head": "81J0Y1ZAG00000000000000002",
	} {
		if _, err := wire.ParseID(bad); err == nil {
			t.Errorf("%s: want a parse error for %q", name, bad)
		}
	}
}

// Every 128-bit value must survive the five-bits-at-a-time walk in both
// directions, including the high bits that the shift helpers carry across bytes.
func TestGivenIDBytesWhenRenderedAndParsedThenShouldPreserveEveryByte(t *testing.T) {
	cases := map[string][16]byte{
		"zero":        {},
		"one":         {15: 1},
		"high bit":    {0: 0x07, 15: 0xff},
		"alternating": {0x05, 0xaa, 0x55, 0xaa, 0x55, 0xaa, 0x55, 0xaa, 0x55, 0xaa, 0x55, 0xaa, 0x55, 0xaa, 0x55, 0xaa},
		"max usable":  {0x07, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
	}
	for name, payload := range cases {
		id := wire.IDFromBytes(payload)
		text := id.String()
		if len(text) != 26 {
			t.Errorf("%s: want 26 characters, got %d", name, len(text))
			continue
		}
		back, err := wire.ParseID(text)
		if err != nil {
			t.Errorf("%s: parse %q: %v", name, text, err)
			continue
		}
		if back.Bytes() != payload {
			t.Errorf("%s: bytes changed: want % x, got % x", name, payload, back.Bytes())
		}
	}
}

// Ids ride as fixed-width 16-byte CBOR byte strings, not bignums.
func TestGivenAnIDWhenEncodedThenShouldBeSixteenByteString(t *testing.T) {
	id := wire.IDFromBytes([16]byte{0x01, 0x02, 15: 0xff})
	encoded, err := wire.Marshal(id)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if encoded[0] != 0x50 { // major type 2, length 16
		t.Fatalf("want a 16-byte byte-string head 0x50, got 0x%02x", encoded[0])
	}
	if len(encoded) != 17 {
		t.Fatalf("want 17 bytes on the wire, got %d", len(encoded))
	}

	var back wire.ID
	if err := wire.Unmarshal(encoded, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back != id {
		t.Fatalf("round trip changed the id")
	}
}

// The locator is one opaque 20-byte string so the envelope names no server.
func TestGivenALogPositionWhenEncodedThenShouldPackTwentyBigEndianBytes(t *testing.T) {
	position := wire.LogPosition{StreamID: 1, TopicID: 2, PartitionID: 3, Offset: 4}

	encoded, err := wire.Marshal(position)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if encoded[0] != 0x54 { // major type 2, length 20
		t.Fatalf("want a 20-byte byte-string head 0x54, got 0x%02x", encoded[0])
	}
	want := []byte{
		0, 0, 0, 1,
		0, 0, 0, 2,
		0, 0, 0, 3,
		0, 0, 0, 0, 0, 0, 0, 4,
	}
	if string(encoded[1:]) != string(want) {
		t.Fatalf("packing differs:\n want % x\n got  % x", want, encoded[1:])
	}

	var back wire.LogPosition
	if err := wire.Unmarshal(encoded, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back != position {
		t.Fatalf("round trip changed the locator: %+v", back)
	}
}

func TestGivenAnAgentIDWhenValidatedThenShouldEnforceTheContractBounds(t *testing.T) {
	if _, err := wire.NewAgentID(""); err == nil {
		t.Error("an empty agent id must be rejected")
	}
	if _, err := wire.NewAgentID(strings.Repeat("a", wire.MaxAgentStringBytes+1)); err == nil {
		t.Error("an over-cap agent id must be rejected")
	}
	if _, err := wire.NewAgentID("bad\tname"); err == nil {
		t.Error("a control character must be rejected")
	}
	if _, err := wire.NewAgentID(strings.Repeat("a", wire.MaxAgentStringBytes)); err != nil {
		t.Errorf("an at-cap agent id must be accepted: %v", err)
	}
	// Non-ASCII is allowed: every character but a control character is legal.
	if _, err := wire.NewAgentID("planner-日本"); err != nil {
		t.Errorf("a non-ASCII agent id must be accepted: %v", err)
	}
}

func TestGivenAnIdempotencyKeyWhenValidatedThenShouldEnforceTheCap(t *testing.T) {
	if _, err := wire.NewIdempotencyKey(""); err == nil {
		t.Error("an empty key must be rejected")
	}
	if _, err := wire.NewIdempotencyKey(strings.Repeat("k", wire.MaxIdempotencyKeyBytes+1)); err == nil {
		t.Error("an over-cap key must be rejected")
	}
	key, err := wire.NewIdempotencyKey("job-123-attempt-2")
	if err != nil {
		t.Fatalf("a natural business key must be accepted: %v", err)
	}
	if key.String() != "job-123-attempt-2" {
		t.Fatalf("key text changed: %q", key.String())
	}
}
