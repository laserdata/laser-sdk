package wire_test

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// Fixtures for surfaces already ported that the first pass did not assert.
// A type written without checking its own corpus bytes is only assumed
// correct, so these close that gap.
func TestGivenRemainingPortedSurfaceFixturesWhenDecodedThenShouldReEncodeByteIdentically(t *testing.T) {
	t.Run("batch_request.bin", func(t *testing.T) {
		request := roundTripFixture[wire.BatchRequest](t, "batch_request.bin")
		if err := request.Validate(); err != nil {
			t.Fatalf("the corpus batch must validate: %v", err)
		}
		if len(request.Ops) == 0 {
			t.Error("a batch carries its ops")
		}
	})
	t.Run("batch_reply.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.BatchReply](t, "batch_reply.bin")
		if len(reply.Results) == 0 {
			t.Error("a batch reply carries per-op frames")
		}
	})

	t.Run("change_record.bin", func(t *testing.T) {
		record := roundTripFixture[wire.ChangeRecord](t, "change_record.bin")
		if record.ToOffset < record.FromOffset {
			t.Error("a change window never runs backwards")
		}
	})

	t.Run("memory_item_origin.bin", func(t *testing.T) {
		record := roundTripFixture[wire.MemoryRecord](t, "memory_item_origin.bin")
		if record.Kind != wire.MemoryItem {
			t.Fatalf("want an item, got kind %d", record.Kind)
		}
		if record.Origin == nil {
			t.Error("the origin fixture points back at its source")
		}
	})
	t.Run("memory_forget_scoped.bin", func(t *testing.T) {
		record := roundTripFixture[wire.MemoryRecord](t, "memory_forget_scoped.bin")
		if record.Kind != wire.MemoryForget {
			t.Fatalf("want a tombstone, got kind %d", record.Kind)
		}
		// A scoped tombstone removes the item only when it was remembered in
		// that conversation, so the scope must survive.
		if record.Conversation == nil {
			t.Error("the scoped fixture names its conversation")
		}
	})
	t.Run("memory_feedback_scoped.bin", func(t *testing.T) {
		record := roundTripFixture[wire.MemoryRecord](t, "memory_feedback_scoped.bin")
		if record.Kind != wire.MemoryFeedback {
			t.Fatalf("want feedback, got kind %d", record.Kind)
		}
		if record.Conversation == nil {
			t.Error("the scoped fixture names its conversation")
		}
	})

	t.Run("session_ref.bin", func(t *testing.T) {
		ref := roundTripFixture[wire.SessionRef](t, "session_ref.bin")
		if ref.Stream == "" {
			t.Error("a session reference names its stream")
		}
	})
	t.Run("session_link_surface.bin", func(t *testing.T) {
		surface := roundTripFixture[wire.LinkSurface](t, "session_link_surface.bin")
		if surface == "" {
			t.Fatal("the fixture names a link surface")
		}
	})
	t.Run("session_link_relation.bin", func(t *testing.T) {
		relation := roundTripFixture[wire.LinkRelation](t, "session_link_relation.bin")
		if relation == "" {
			t.Fatal("the fixture names a link relation")
		}
	})

	t.Run("source_ref_generation.bin", func(t *testing.T) {
		source := roundTripFixture[wire.SourceRef](t, "source_ref_generation.bin")
		if source.Kind != wire.SourceMessage || source.Message == nil {
			t.Fatalf("want a message source, got kind %d", source.Kind)
		}
		// The generation proves which incarnation of the topic the offset
		// belongs to. Without it a reused topic id would look like the same
		// source.
		if source.Message.Generation == nil {
			t.Error("the generation fixture names its topic generation")
		}
	})

}

// A bare byte vector rides as an array of integers, not a byte string. The two
// are different bytes on the wire, so a field encoded with the wrong one
// produces a record the other clients decode differently.
func TestGivenAByteListWhenEncodedThenShouldRideAsIntegersNotAByteString(t *testing.T) {
	encoded, err := wire.Marshal(wire.ByteList{1, 2, 255})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// Major type 4 is an array, where major type 2 would be a byte string.
	if encoded[0]>>5 != 4 {
		t.Fatalf("want an array head, got 0x%02x", encoded[0])
	}

	var back wire.ByteList
	if err := wire.Unmarshal(encoded, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if string(back) != string([]byte{1, 2, 255}) {
		t.Fatalf("round trip changed the bytes: % x", back)
	}

	// A plain byte slice in the same position would encode differently, which
	// is exactly the divergence this type exists to prevent.
	asBytes, err := wire.Marshal([]byte{1, 2, 255})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if asBytes[0]>>5 != 2 {
		t.Fatalf("want a byte-string head for a plain slice, got 0x%02x", asBytes[0])
	}
	if string(asBytes) == string(encoded) {
		t.Fatal("the two encodings must differ")
	}
}
