package wire_test

import (
	"strings"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// validEnvelopeFixtures must pass the matrix.
var validEnvelopeFixtures = []string{
	"agent_command.bin",
	"agent_command_signed.bin",
	"agent_response.bin",
	"agent_event.bin",
	"agent_chunk.bin",
	"agent_chunk_open.bin",
	"agent_chunk_terminal.bin",
	"agent_error.bin",
	"agent_status_task.bin",
	"agent_status_card.bin",
	"agent_must_understand.bin",
	"agent_status_session.bin",
}

// invalidEnvelopeFixtures are the corpus negatives. AGENTS.md requires these to
// fail validate() identically in every port, so each one decodes cleanly and
// then must be rejected.
var invalidEnvelopeFixtures = []string{
	"agent_invalid_chunk_late_deadline.bin",
	"agent_invalid_chunk_no_sequence.bin",
	"agent_invalid_chunk_open_no_operation.bin",
	"agent_invalid_command_no_correlation.bin",
	"agent_invalid_error_last.bin",
	"agent_invalid_event_task_state.bin",
	"agent_invalid_response_channel.bin",
	"agent_invalid_status_bad_operation.bin",
	"agent_invalid_status_no_operation.bin",
	"agent_invalid_status_fact_no_deadline.bin",
}

func TestGivenValidEnvelopeFixturesWhenValidatedThenShouldBeAccepted(t *testing.T) {
	for _, name := range validEnvelopeFixtures {
		t.Run(name, func(t *testing.T) {
			var envelope wire.AgentEnvelope
			if err := wire.Unmarshal(readFixture(t, name), &envelope); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if err := envelope.Validate(); err != nil {
				t.Fatalf("a corpus-valid envelope was rejected: %v", err)
			}
		})
	}
}

// Each negative names the rule it breaks, so the test asserts the reason and
// not merely that something failed. A port that rejected these for the wrong
// reason would diverge the moment a real record exercised the same field.
func TestGivenInvalidEnvelopeFixturesWhenValidatedThenShouldBeRejectedForTheNamedRule(t *testing.T) {
	wantField := map[string]string{
		"agent_invalid_chunk_late_deadline.bin":     "deadline_micros",
		"agent_invalid_chunk_no_sequence.bin":       "sequence",
		"agent_invalid_chunk_open_no_operation.bin": "operation",
		"agent_invalid_command_no_correlation.bin":  "correlation",
		"agent_invalid_error_last.bin":              "last",
		"agent_invalid_event_task_state.bin":        "task_state",
		"agent_invalid_response_channel.bin":        "channel",
		"agent_invalid_status_bad_operation.bin":    "operation",
		"agent_invalid_status_no_operation.bin":     "operation",
		"agent_invalid_status_fact_no_deadline.bin": "deadline_micros",
	}

	for _, name := range invalidEnvelopeFixtures {
		t.Run(name, func(t *testing.T) {
			var envelope wire.AgentEnvelope
			if err := wire.Unmarshal(readFixture(t, name), &envelope); err != nil {
				t.Fatalf("a negative fixture must still decode, it is valid CBOR: %v", err)
			}
			err := envelope.Validate()
			if err == nil {
				t.Fatal("want a validation error, got none")
			}
			var validateError *wire.ValidateError
			if !asValidateError(err, &validateError) {
				t.Fatalf("want a *wire.ValidateError, got %T: %v", err, err)
			}
			if field := wantField[name]; field != "" && validateError.Field != field {
				t.Fatalf("want the error to name %q, got %q (%v)", field, validateError.Field, err)
			}
			t.Logf("rejected: %v", err)
		})
	}
}

func asValidateError(err error, target **wire.ValidateError) bool {
	if typed, ok := err.(*wire.ValidateError); ok {
		*target = typed
		return true
	}
	return false
}

// The matrix rules that no corpus fixture covers still need pinning, so a
// future edit cannot silently drop one.
func TestGivenEnvelopeRulesWhenViolatedThenShouldBeRejected(t *testing.T) {
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{1})}
	other := wire.ConversationID{ID: wire.IDFromBytes([16]byte{2})}
	record := wire.RecordID{ID: wire.IDFromBytes([16]byte{3})}
	correlation := wire.CorrelationID{ID: wire.IDFromBytes([16]byte{4})}
	source, err := wire.NewAgentID("planner")
	if err != nil {
		t.Fatalf("agent id: %v", err)
	}

	command := func() wire.AgentEnvelope {
		return wire.AgentEnvelope{
			Kind:         wire.KindCommand,
			Record:       &record,
			Conversation: conversation,
			Source:       source,
			Correlation:  &correlation,
			Body:         []byte{0x01},
		}
	}

	t.Run("root without parent", func(t *testing.T) {
		envelope := command()
		envelope.Root = &other
		assertRejected(t, &envelope, "root")
	})

	t.Run("parent equal to conversation", func(t *testing.T) {
		envelope := command()
		envelope.Parent = &conversation
		assertRejected(t, &envelope, "parent")
	})

	t.Run("usage on a command", func(t *testing.T) {
		envelope := command()
		envelope.Usage = &wire.TokenUsage{InputTokens: 1, OutputTokens: 2}
		assertRejected(t, &envelope, "usage")
	})

	t.Run("empty body", func(t *testing.T) {
		envelope := command()
		envelope.Body = nil
		assertRejected(t, &envelope, "body")
	})

	t.Run("over-cap operation", func(t *testing.T) {
		envelope := command()
		tooLong := strings.Repeat("o", wire.MaxAgentStringBytes+1)
		envelope.Operation = &tooLong
		assertRejected(t, &envelope, "operation")
	})

	t.Run("too many metadata entries", func(t *testing.T) {
		envelope := command()
		envelope.Metadata = map[string]wire.Value{}
		for i := 0; i <= wire.MaxMetadataEntries; i++ {
			envelope.Metadata[string(rune('a'+i%26))+itoa(i)] = wire.IntValue(int64(i))
		}
		assertRejected(t, &envelope, "metadata")
	})

	t.Run("over-cap metadata value", func(t *testing.T) {
		envelope := command()
		envelope.Metadata = map[string]wire.Value{
			"big": wire.StrValue(strings.Repeat("v", wire.MaxMetadataValueBytes+1)),
		}
		assertRejected(t, &envelope, "metadata value")
	})

	t.Run("ed25519 signature of the wrong length", func(t *testing.T) {
		envelope := command()
		envelope.Signature = &wire.Signature{
			Scheme: wire.SignatureSchemeEd25519,
			KeyID:  []byte{1, 2, 3},
			Bytes:  make([]byte, 64),
		}
		assertRejected(t, &envelope, "key_id")
	})

	t.Run("unknown signature scheme passes", func(t *testing.T) {
		envelope := command()
		envelope.Signature = &wire.Signature{Scheme: 99, KeyID: []byte{1}, Bytes: []byte{2}}
		if err := envelope.Validate(); err != nil {
			t.Fatalf("an unknown scheme must pass through, got %v", err)
		}
	})
}

func assertRejected(t *testing.T, envelope *wire.AgentEnvelope, wantField string) {
	t.Helper()
	err := envelope.Validate()
	if err == nil {
		t.Fatalf("want a rejection naming %q, got none", wantField)
	}
	var validateError *wire.ValidateError
	if !asValidateError(err, &validateError) {
		t.Fatalf("want a *wire.ValidateError, got %T", err)
	}
	if validateError.Field != wantField {
		t.Fatalf("want the error to name %q, got %q (%v)", wantField, validateError.Field, err)
	}
}
