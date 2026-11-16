package wire_test

import (
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

// The client keeps the highest position per surface, and a generation change
// replaces rather than silently drops, since the old coordinate is meaningless
// once a surface is rebuilt.
func TestGivenFrontiersWhenMergedThenShouldKeepTheHighestPerSurface(t *testing.T) {
	held := wire.AppliedFrontier{
		Kv: &wire.MutationPosition{TopicGeneration: 1, Partition: 0, Offset: 10},
	}

	wire.MergeFrontier(&held, wire.AppliedFrontier{
		Kv: &wire.MutationPosition{TopicGeneration: 1, Partition: 0, Offset: 4},
	})
	if held.Kv.Offset != 10 {
		t.Fatalf("an earlier position must not lower the frontier, got %d", held.Kv.Offset)
	}

	wire.MergeFrontier(&held, wire.AppliedFrontier{
		Kv: &wire.MutationPosition{TopicGeneration: 1, Partition: 0, Offset: 21},
	})
	if held.Kv.Offset != 21 {
		t.Fatalf("a later position must raise the frontier, got %d", held.Kv.Offset)
	}

	// A new topic generation does not compare with the held one.
	wire.MergeFrontier(&held, wire.AppliedFrontier{
		Kv: &wire.MutationPosition{TopicGeneration: 2, Partition: 0, Offset: 1},
	})
	if held.Kv.TopicGeneration != 2 || held.Kv.Offset != 1 {
		t.Fatalf("a rebuilt surface must replace the coordinate, got %+v", *held.Kv)
	}

	// Session lanes merge per partition of a topic incarnation.
	lane := wire.SessionPosition{
		StreamID: 1, StreamGeneration: 5, Topic: wire.AgentSessions,
		TopicGeneration: 7, PartitionID: 0, Offset: 3,
	}
	wire.MergeFrontier(&held, wire.AppliedFrontier{Sessions: []wire.SessionPosition{lane}})
	if len(held.Sessions) != 1 {
		t.Fatalf("want one session lane, got %d", len(held.Sessions))
	}
	advanced := lane
	advanced.Offset = 9
	wire.MergeFrontier(&held, wire.AppliedFrontier{Sessions: []wire.SessionPosition{advanced}})
	if len(held.Sessions) != 1 || held.Sessions[0].Offset != 9 {
		t.Fatalf("the same lane must advance in place, got %+v", held.Sessions)
	}
	other := lane
	other.PartitionID = 1
	wire.MergeFrontier(&held, wire.AppliedFrontier{Sessions: []wire.SessionPosition{other}})
	if len(held.Sessions) != 2 {
		t.Fatalf("a different partition is its own lane, got %d", len(held.Sessions))
	}
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
