package wire_test

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// Each session body type must reproduce the Rust bytes, since these records sit
// on the session lane where every client folds them.
func TestGivenSessionBodyFixturesWhenDecodedThenShouldReEncodeByteIdentically(t *testing.T) {
	roundTrip := func(t *testing.T, name string, decode func([]byte) ([]byte, error)) {
		t.Helper()
		raw := readFixture(t, name)
		again, err := decode(raw)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if string(again) != string(raw) {
			t.Fatalf("%s: re-encode differs (%d in, %d out)%s",
				name, len(raw), len(again), describeDiff(raw, again))
		}
	}

	t.Run("agent_session_start.bin", func(t *testing.T) {
		roundTrip(t, "agent_session_start.bin", func(raw []byte) ([]byte, error) {
			var start wire.SessionStart
			if err := wire.Unmarshal(raw, &start); err != nil {
				return nil, err
			}
			if err := start.Validate(); err != nil {
				return nil, err
			}
			return wire.Marshal(start)
		})
	})

	t.Run("agent_session_end.bin", func(t *testing.T) {
		roundTrip(t, "agent_session_end.bin", func(raw []byte) ([]byte, error) {
			var end wire.SessionEnd
			if err := wire.Unmarshal(raw, &end); err != nil {
				return nil, err
			}
			return wire.Marshal(end)
		})
	})

	t.Run("agent_session_transition.bin", func(t *testing.T) {
		roundTrip(t, "agent_session_transition.bin", func(raw []byte) ([]byte, error) {
			var transition wire.SessionTransition
			if err := wire.Unmarshal(raw, &transition); err != nil {
				return nil, err
			}
			return wire.Marshal(transition)
		})
	})

	t.Run("agent_error_body.bin", func(t *testing.T) {
		roundTrip(t, "agent_error_body.bin", func(raw []byte) ([]byte, error) {
			var body wire.AgentErrorBody
			if err := wire.Unmarshal(raw, &body); err != nil {
				return nil, err
			}
			return wire.Marshal(body)
		})
	})
}

// A session start whose ancestry disagrees with its envelope must be rejected:
// the body and the envelope would otherwise describe different session trees.
func TestGivenASessionStatusWhenItsBodyAncestryDisagreesThenShouldBeRejected(t *testing.T) {
	raw := readFixture(t, "agent_status_session.bin")

	var envelope wire.AgentEnvelope
	if err := wire.Unmarshal(raw, &envelope); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if err := envelope.Validate(); err != nil {
		t.Fatalf("the corpus fixture must validate: %v", err)
	}

	// Give the envelope a parent its start body does not name.
	parent := wire.ConversationID{ID: wire.IDFromBytes([16]byte{9})}
	envelope.Parent = &parent

	err := envelope.Validate()
	if err == nil {
		t.Fatal("want a rejection when the body ancestry disagrees")
	}
	var validateError *wire.ValidateError
	if !asValidateError(err, &validateError) || validateError.Field != "body" {
		t.Fatalf("want a body validation error, got %v", err)
	}
}
