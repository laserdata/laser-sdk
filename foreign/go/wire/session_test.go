package wire_test

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// The session read requests and replies, which the agent runtime reads
// through. Every request names its stream first, because the index is per
// stream.
func TestGivenSessionRequestFixturesWhenDecodedThenShouldReEncodeByteIdentically(t *testing.T) {
	t.Run("session_get.bin", func(t *testing.T) {
		roundTripFixture[wire.SessionGetRequest](t, "session_get.bin")
	})
	t.Run("session_list.bin", func(t *testing.T) {
		roundTripFixture[wire.SessionListRequest](t, "session_list.bin")
	})
	t.Run("session_events.bin", func(t *testing.T) {
		roundTripFixture[wire.SessionEventsRequest](t, "session_events.bin")
	})
	t.Run("session_state.bin", func(t *testing.T) {
		roundTripFixture[wire.SessionStateRequest](t, "session_state.bin")
	})
	t.Run("session_links.bin", func(t *testing.T) {
		roundTripFixture[wire.SessionLinksRequest](t, "session_links.bin")
	})
	t.Run("session_sources.bin", func(t *testing.T) {
		roundTripFixture[wire.SessionSourcesRequest](t, "session_sources.bin")
	})
	t.Run("session_sources_lane_only.bin", func(t *testing.T) {
		request := roundTripFixture[wire.SessionSourcesRequest](t, "session_sources_lane_only.bin")
		if !request.LaneOnly {
			t.Error("the lane-only fixture asks for the lane identity alone")
		}
	})
	t.Run("session_changes.bin", func(t *testing.T) {
		roundTripFixture[wire.SessionChangesRequest](t, "session_changes.bin")
	})
	t.Run("session_heartbeat.bin", func(t *testing.T) {
		heartbeat := roundTripFixture[wire.SessionHeartbeat](t, "session_heartbeat.bin")
		if err := heartbeat.Validate(); err != nil {
			t.Fatalf("the corpus heartbeat must validate: %v", err)
		}
	})
}

func TestGivenSessionReplyFixturesWhenDecodedThenShouldReEncodeByteIdentically(t *testing.T) {
	for name, want := range map[string]wire.SessionOutcomeKind{
		"session_reply_info.bin":    wire.SessionInfoOutcome,
		"session_reply_page.bin":    wire.SessionPageOutcome,
		"session_reply_events.bin":  wire.SessionEventsOutcome,
		"session_reply_state.bin":   wire.SessionStateOutcome,
		"session_reply_links.bin":   wire.SessionLinksOutcome,
		"session_reply_sources.bin": wire.SessionSourcesOutcome,
		"session_reply_changes.bin": wire.SessionChangesOutcome,
	} {
		t.Run(name, func(t *testing.T) {
			reply := roundTripFixture[wire.SessionReply](t, name)
			if !reply.IsOk() {
				t.Fatalf("want a successful reply, got %+v", reply.Err)
			}
			if reply.Outcome.Kind != want {
				t.Fatalf("want outcome kind %d, got %d", want, reply.Outcome.Kind)
			}
		})
	}

	t.Run("session_reply_page_rebuilding.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.SessionReply](t, "session_reply_page_rebuilding.bin")
		if !reply.IsOk() || reply.Outcome.Page == nil {
			t.Fatalf("want a page outcome, got %+v", reply)
		}
		// A rebuilding index can answer incompletely, and the flag is how a
		// caller knows not to trust the totals.
		if !reply.Outcome.Page.Rebuilding {
			t.Error("the rebuilding fixture must report it")
		}
	})

	t.Run("session_reply_error.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.SessionReply](t, "session_reply_error.bin")
		if reply.IsOk() {
			t.Fatalf("want a failure, got %+v", reply.Outcome)
		}
		if reply.Err.ResultCode().String() == "" {
			t.Error("a session failure must project onto a result code")
		}
	})
}

// A status, gap reason or state outcome a newer peer reports must pass
// through rather than fail the read, since the rest of the page is still
// usable.
func TestGivenUnknownSessionVocabularyWhenDecodedThenShouldPassThrough(t *testing.T) {
	encoded, err := wire.Marshal(wire.SessionStatus("teleporting"))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var status wire.SessionStatus
	if err := wire.Unmarshal(encoded, &status); err != nil {
		t.Fatalf("an unknown status must decode: %v", err)
	}
	if status != wire.SessionUnrecognized || status.IsNamed() {
		t.Fatalf("want the passthrough status, got %q", status)
	}

	gapEncoded, err := wire.Marshal(wire.GapReason("sunspots"))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var reason wire.GapReason
	if err := wire.Unmarshal(gapEncoded, &reason); err != nil {
		t.Fatalf("an unknown gap reason must decode: %v", err)
	}
	if reason != wire.GapUnrecognized {
		t.Fatalf("want the passthrough reason, got %q", reason)
	}

	stateEncoded, err := wire.Marshal(wire.StateOutcome("folded sideways"))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var outcome wire.StateOutcome
	if err := wire.Unmarshal(stateEncoded, &outcome); err != nil {
		t.Fatalf("an unknown state outcome must decode: %v", err)
	}
	if outcome != wire.StateUnrecognized {
		t.Fatalf("want the passthrough outcome, got %q", outcome)
	}

	// A named value must still survive unchanged.
	named, err := wire.Marshal(wire.SessionActive)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back wire.SessionStatus
	if err := wire.Unmarshal(named, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back != wire.SessionActive {
		t.Fatalf("a named status must survive, got %q", back)
	}
}

// A heartbeat is bounded, since a process could otherwise claim an unbounded
// set of session leases in one record.
func TestGivenAnOversizedHeartbeatWhenValidatedThenShouldBeRefused(t *testing.T) {
	heartbeat := wire.SessionHeartbeat{
		Process:  "worker-1",
		Stream:   "agents",
		Sessions: make([]wire.ConversationID, wire.MaxHeartbeatSessions+1),
	}
	if err := heartbeat.Validate(); err == nil {
		t.Fatal("a heartbeat past the cap must be refused")
	}
}
