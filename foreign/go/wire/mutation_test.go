package wire_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// The managed envelopes wrap every state-changing request and every barriered
// read, so their bytes must match the Rust encoder exactly.
func TestGivenManagedEnvelopeFixturesWhenDecodedThenShouldReEncodeByteIdentically(t *testing.T) {
	t.Run("managed_request.bin", func(t *testing.T) {
		raw := readFixture(t, "managed_request.bin")
		var envelope wire.ManagedRequestEnvelope
		if err := wire.Unmarshal(raw, &envelope); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if err := envelope.Validate(); err != nil {
			t.Fatalf("validate: %v", err)
		}
		assertSameBytes(t, raw, mustMarshal(t, envelope))
	})

	t.Run("managed_read.bin", func(t *testing.T) {
		raw := readFixture(t, "managed_read.bin")
		var envelope wire.ManagedReadEnvelope
		if err := wire.Unmarshal(raw, &envelope); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if !envelope.LogHead {
			t.Error("the fixture asks for a log-head barrier")
		}
		if envelope.MinPosition == nil {
			t.Error("the fixture carries a minimum position")
		}
		if len(envelope.MinSessions) != 1 {
			t.Errorf("want one session position, got %d", len(envelope.MinSessions))
		}
		assertSameBytes(t, raw, mustMarshal(t, envelope))
	})

	t.Run("managed_reply.bin", func(t *testing.T) {
		raw := readFixture(t, "managed_reply.bin")
		var envelope wire.ManagedReplyEnvelope
		if err := wire.Unmarshal(raw, &envelope); err != nil {
			t.Fatalf("decode: %v", err)
		}
		// The fixture's frontier names kv and graph but not fork, so an
		// absent surface must stay absent rather than encode as null.
		if envelope.Frontier.Kv == nil || envelope.Frontier.Graph == nil {
			t.Error("want kv and graph positions in the frontier")
		}
		if envelope.Frontier.Fork != nil {
			t.Error("the fixture names no fork position")
		}
		assertSameBytes(t, raw, mustMarshal(t, envelope))
	})

	t.Run("mutation_command.bin", func(t *testing.T) {
		raw := readFixture(t, "mutation_command.bin")
		var envelope wire.MutationCommandEnvelope
		if err := wire.Unmarshal(raw, &envelope); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if err := envelope.Validate(); err != nil {
			t.Fatalf("validate: %v", err)
		}
		assertSameBytes(t, raw, mustMarshal(t, envelope))
	})

	t.Run("mutation_command_caller.bin", func(t *testing.T) {
		raw := readFixture(t, "mutation_command_caller.bin")
		var envelope wire.MutationCommandEnvelope
		if err := wire.Unmarshal(raw, &envelope); err != nil {
			t.Fatalf("decode: %v", err)
		}
		// The caller is stamped before the command enters the log and is the
		// only actor it names, so a replay attributes the write to the client
		// that asked rather than to whichever process folded it.
		if envelope.Caller == nil {
			t.Error("the caller fixture names the client that asked")
		}
		assertSameBytes(t, raw, mustMarshal(t, envelope))
	})

	t.Run("mutation_command_scoped.bin", func(t *testing.T) {
		raw := readFixture(t, "mutation_command_scoped.bin")
		var envelope wire.MutationCommandEnvelope
		if err := wire.Unmarshal(raw, &envelope); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if envelope.Scope == nil {
			t.Error("the scoped fixture carries a forwarded scope")
		}
		assertSameBytes(t, raw, mustMarshal(t, envelope))
	})
}

// The whole point of refusing unknown fields: a plain request must never
// decode as a barrier or reply frame, or a wrapped and an unwrapped request
// would be indistinguishable.
func TestGivenAPlainRequestWhenDecodedAsAManagedFrameThenShouldBeRefused(t *testing.T) {
	plainRead := readFixture(t, "managed_request.bin") // a request, not a read frame

	var read wire.ManagedReadEnvelope
	if err := wire.Unmarshal(plainRead, &read); err == nil {
		t.Error("a managed request must not decode as a barrier read frame")
	}

	var reply wire.ManagedReplyEnvelope
	if err := wire.Unmarshal(plainRead, &reply); err == nil {
		t.Error("a managed request must not decode as a reply frame")
	}

	// And the reverse: a reply frame is not a read frame.
	var crossed wire.ManagedReadEnvelope
	if err := wire.Unmarshal(readFixture(t, "managed_reply.bin"), &crossed); err == nil {
		t.Error("a reply frame must not decode as a barrier read frame")
	}
}

func TestGivenAManagedEnvelopeWhenValidatedThenShouldRefuseZeroAndWrongVersion(t *testing.T) {
	if err := (&wire.ManagedRequestEnvelope{V: wire.ManagedRequestVersion}).Validate(); err == nil {
		t.Error("a zero operation id must be refused")
	}
	if err := (&wire.ManagedRequestEnvelope{V: 99, OperationID: wire.U128FromUint64(1)}).Validate(); err == nil {
		t.Error("an unsupported version must be refused")
	}
	if err := (&wire.MutationCommandEnvelope{V: 0, OperationID: wire.U128FromUint64(1)}).Validate(); err == nil {
		t.Error("a zero mutation command version must be refused")
	}
}

// The operation identity spans the full 128-bit range, so it must survive both
// the plain-integer and the bignum form.
func TestGivenAnOperationIdentityWhenEncodedThenShouldSpanTheFullRange(t *testing.T) {
	maximum, err := wire.ParseU128("340282366920938463463374607431768211455")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	for name, value := range map[string]wire.U128{
		"small":    wire.U128FromUint64(42),
		"u64 max":  wire.U128FromUint64(^uint64(0)),
		"past u64": func() wire.U128 { v, _ := wire.ParseU128("18446744073709551616"); return v }(),
		"u128 max": maximum,
	} {
		envelope := wire.ManagedRequestEnvelope{
			V: wire.ManagedRequestVersion, OperationID: value, Payload: []byte{1},
		}
		encoded := mustMarshal(t, envelope)
		var back wire.ManagedRequestEnvelope
		if err := wire.Unmarshal(encoded, &back); err != nil {
			t.Errorf("%s: decode: %v", name, err)
			continue
		}
		if back.OperationID != value {
			t.Errorf("%s: round trip changed the identity: want %s, got %s",
				name, value.String(), back.OperationID.String())
		}
	}

	if _, err := wire.ParseU128("340282366920938463463374607431768211456"); err == nil {
		t.Error("a value past 128 bits must be refused")
	}
	if _, err := wire.ParseU128("-1"); err == nil {
		t.Error("a negative value must be refused")
	}
}

// The client keeps the later position per surface, ordered by topic generation
// and then offset, so a late reply from a topic that was since recreated never
// lowers the frontier.
func TestGivenFrontiersWhenMergedThenShouldKeepTheLaterPositionPerSurface(t *testing.T) {
	at := func(generation, offset uint64) *wire.MutationPosition {
		return &wire.MutationPosition{TopicGeneration: generation, Offset: offset}
	}
	held := wire.AppliedFrontier{Kv: at(1, 9), Fork: at(1, 4)}

	wire.MergeFrontier(&held, wire.AppliedFrontier{Kv: at(1, 3), Fork: at(2, 0), Graph: at(1, 1)})

	want := wire.AppliedFrontier{Kv: at(1, 9), Fork: at(2, 0), Graph: at(1, 1)}
	if !reflect.DeepEqual(held, want) {
		t.Fatalf("merged frontier = %s, want %s", describeFrontier(held), describeFrontier(want))
	}

	wire.MergeFrontier(&held, wire.AppliedFrontier{Fork: at(1, 500)})
	if !reflect.DeepEqual(held, want) {
		t.Fatalf("an older generation lowered the frontier: %s", describeFrontier(held))
	}
}

func TestGivenSessionPositionsWhenMergedThenShouldKeepTheLaterPerUnitAndDropAnOlderStream(t *testing.T) {
	session := func(streamGeneration uint64, topic string, offset uint64) wire.SessionPosition {
		return wire.SessionPosition{
			StreamID: 4, StreamGeneration: streamGeneration, Topic: topic,
			TopicGeneration: 1, PartitionID: 0, Offset: offset,
		}
	}
	var held wire.AppliedFrontier

	wire.MergeFrontier(&held, wire.AppliedFrontier{Sessions: []wire.SessionPosition{
		session(1, "lane", 5), session(1, "inbox", 2),
	}})
	wire.MergeFrontier(&held, wire.AppliedFrontier{Sessions: []wire.SessionPosition{session(1, "lane", 3)}})
	want := []wire.SessionPosition{session(1, "lane", 5), session(1, "inbox", 2)}
	if !reflect.DeepEqual(held.Sessions, want) {
		t.Fatalf("an older answer lowered a unit: %+v", held.Sessions)
	}

	other := session(1, "lane", 0)
	other.PartitionID = 1
	wire.MergeFrontier(&held, wire.AppliedFrontier{Sessions: []wire.SessionPosition{other}})
	if want = append(want, other); !reflect.DeepEqual(held.Sessions, want) {
		t.Fatalf("another partition is its own unit: %+v", held.Sessions)
	}

	recreatedTopic := session(1, "inbox", 0)
	recreatedTopic.TopicGeneration = 2
	wire.MergeFrontier(&held, wire.AppliedFrontier{Sessions: []wire.SessionPosition{recreatedTopic}})
	want[1] = recreatedTopic
	if !reflect.DeepEqual(held.Sessions, want) {
		t.Fatalf("a recreated topic replaces its unit in place: %+v", held.Sessions)
	}

	wire.MergeFrontier(&held, wire.AppliedFrontier{Sessions: []wire.SessionPosition{session(2, "lane", 0)}})
	if want = []wire.SessionPosition{session(2, "lane", 0)}; !reflect.DeepEqual(held.Sessions, want) {
		t.Fatalf("a recreated stream must drop the positions of the one before it: %+v", held.Sessions)
	}

	wire.MergeFrontier(&held, wire.AppliedFrontier{Sessions: []wire.SessionPosition{session(1, "lane", 99)}})
	if !reflect.DeepEqual(held.Sessions, want) {
		t.Fatalf("a late answer from the older stream must not return: %+v", held.Sessions)
	}
}

func TestGivenAReplyEnvelopeWithoutItsVersionOrReplyWhenDecodedThenShouldBeRefused(t *testing.T) {
	for name, frame := range map[string][]byte{
		"an empty map":     {0xa0},
		"a version alone":  {0xa1, 0x61, 'v', 0x01},
		"a reply alone":    {0xa1, 0x65, 'r', 'e', 'p', 'l', 'y', 0x41, 0x01},
		"a frontier alone": {0xa1, 0x68, 'f', 'r', 'o', 'n', 't', 'i', 'e', 'r', 0xa0},
	} {
		var envelope wire.ManagedReplyEnvelope
		if err := wire.Unmarshal(frame, &envelope); err == nil {
			t.Errorf("%s decoded as a reply envelope", name)
		}
	}

	var envelope wire.ManagedReplyEnvelope
	whole := []byte{0xa2, 0x61, 'v', 0x01, 0x65, 'r', 'e', 'p', 'l', 'y', 0x41, 0x07}
	if err := wire.Unmarshal(whole, &envelope); err != nil {
		t.Fatalf("an envelope without a frontier must decode: %v", err)
	}
	want := wire.ManagedReplyEnvelope{V: 1, Reply: []byte{7}}
	if !reflect.DeepEqual(envelope, want) {
		t.Fatalf("envelope = %+v, want %+v", envelope, want)
	}
}

func describeFrontier(frontier wire.AppliedFrontier) string {
	show := func(position *wire.MutationPosition) string {
		if position == nil {
			return "none"
		}
		return fmt.Sprintf("(%d,%d)", position.TopicGeneration, position.Offset)
	}
	return fmt.Sprintf("kv %s fork %s graph %s", show(frontier.Kv), show(frontier.Fork), show(frontier.Graph))
}

// Each mutation-log surface must map from its command codes, and a command
// outside the three logs must map to none.
func TestGivenACommandCodeWhenMappedThenShouldNameItsMutationSurface(t *testing.T) {
	for code, want := range map[uint32]wire.PositionSurface{
		wire.AgdxKvGetCode:          wire.SurfaceKv,
		wire.AgdxKvCasFencedCode:    wire.SurfaceKv,
		wire.AgdxForkPromoteCode:    wire.SurfaceFork,
		wire.AgdxGraphNeighborsCode: wire.SurfaceGraph,
	} {
		got, ok := wire.PositionSurfaceOfCode(code)
		if !ok || got != want {
			t.Errorf("code %d: want %q, got %q (ok=%v)", code, want, got, ok)
		}
	}
	if _, ok := wire.PositionSurfaceOfCode(wire.AgdxQueryCode); ok {
		t.Error("a query is outside the mutation logs")
	}
	if _, ok := wire.PositionSurfaceOfCode(wire.AgdxHelloCode); ok {
		t.Error("hello is outside the mutation logs")
	}
}

func mustMarshal(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := wire.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return encoded
}

func assertSameBytes(t *testing.T, want, got []byte) {
	t.Helper()
	if string(want) != string(got) {
		t.Fatalf("bytes differ (%d want, %d got)%s", len(want), len(got), describeDiff(want, got))
	}
}
