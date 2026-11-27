package wire_test

import (
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// dispatchCases mirrors wire/fixtures/dispatch_cases.json: the shared
// classification corpus every client must agree on.
type dispatchCases struct {
	Agent string `json:"agent"`
	Cases []struct {
		Name     string          `json:"name"`
		Topic    string          `json:"topic"`
		Handled  json.RawMessage `json:"handled"`
		Dispatch string          `json:"dispatch"`
		Display  string          `json:"display"`
		Record   struct {
			Kind      *string `json:"kind"`
			Operation *string `json:"operation"`
			TaskState *string `json:"task_state"`
			Body      *string `json:"body"`
			Target    *string `json:"target"`
			Role      *string `json:"role"`
			Recorded  *bool   `json:"recorded"`
			Memory    *string `json:"memory"`
			Other     *string `json:"other"`
		} `json:"record"`
	} `json:"cases"`
	Generic []struct {
		Name        string  `json:"name"`
		Topic       string  `json:"topic"`
		To          *string `json:"to"`
		Cause       bool    `json:"cause"`
		Correlation bool    `json:"correlation"`
		Dispatch    string  `json:"dispatch"`
	} `json:"generic"`
	Filters []struct {
		Agent     *string `json:"agent"`
		Broadcast *bool   `json:"broadcast"`
		Digest    string  `json:"digest"`
	} `json:"filters"`
}

// The classification corpus is behaviour, not encoding: every client must
// reach the same dispatch and display verdict for the same record. A
// divergence here means a Go agent handles work a Rust agent observes, or
// ignores work a Rust agent runs.
func TestGivenTheDispatchCorpusWhenClassifiedThenShouldMatchEveryVerdict(t *testing.T) {
	var corpus dispatchCases
	if err := json.Unmarshal(readFixture(t, "dispatch_cases.json"), &corpus); err != nil {
		t.Fatalf("decode the corpus: %v", err)
	}
	if len(corpus.Cases) == 0 {
		t.Fatal("the corpus lists cases")
	}

	me, err := wire.NewAgentID(corpus.Agent)
	if err != nil {
		t.Fatalf("agent id: %v", err)
	}

	for _, testCase := range corpus.Cases {
		t.Run(testCase.Name, func(t *testing.T) {
			record, envelope := buildSessionRecord(t, testCase.Record.Kind,
				testCase.Record.Operation, testCase.Record.TaskState, testCase.Record.Body,
				testCase.Record.Target, testCase.Record.Role,
				testCase.Record.Recorded, testCase.Record.Memory, testCase.Record.Other)

			if got := string(wire.DisplayTypeOf(record, testCase.Topic)); got != testCase.Display {
				t.Errorf("display: want %q, got %q", testCase.Display, got)
			}

			// Only an envelope record has a dispatch verdict.
			if envelope == nil {
				return
			}
			handled, ok := parseHandled(t, testCase.Handled)
			if !ok {
				return
			}
			if got := string(wire.Classify(envelope, testCase.Topic, me, handled)); got != testCase.Dispatch {
				t.Errorf("dispatch: want %q, got %q", testCase.Dispatch, got)
			}
		})
	}

	// A generic record has no kind, so its verdict comes from its addressee,
	// its causal parent and its correlation alone.
	for _, generic := range corpus.Generic {
		t.Run("generic/"+generic.Name, func(t *testing.T) {
			got := wire.ClassifyGeneric(generic.To, generic.Cause, generic.Correlation,
				generic.Topic, me)
			if string(got) != generic.Dispatch {
				t.Errorf("want %q, got %q", generic.Dispatch, got)
			}
		})
	}
}

// The corpus also pins the digest of each routing filter, which checks the
// filter construction and the canonical JSON encoding together: a Go agent
// must bind the same group policy a Rust agent binds.
func TestGivenTheDispatchCorpusFiltersWhenBuiltThenShouldMatchTheirDigests(t *testing.T) {
	var corpus dispatchCases
	if err := json.Unmarshal(readFixture(t, "dispatch_cases.json"), &corpus); err != nil {
		t.Fatalf("decode the corpus: %v", err)
	}
	if len(corpus.Filters) == 0 {
		t.Fatal("the corpus pins filter digests")
	}

	for _, expected := range corpus.Filters {
		name := "broadcast"
		var filter wire.ConsumerFilter
		switch {
		case expected.Broadcast != nil && *expected.Broadcast:
			filter = wire.BroadcastFilter()
		case expected.Agent != nil:
			name = *expected.Agent
			agent, err := wire.NewAgentID(*expected.Agent)
			if err != nil {
				t.Errorf("%s: agent id: %v", name, err)
				continue
			}
			filter = wire.AddresseeFilter(agent)
		default:
			t.Errorf("a filter case names neither an agent nor broadcast")
			continue
		}

		t.Run(name, func(t *testing.T) {
			digest, err := filter.Digest()
			if err != nil {
				t.Fatalf("digest: %v", err)
			}
			if got := hex.EncodeToString(digest.Bytes); got != expected.Digest {
				t.Fatalf("digest differs\n want %s\n got  %s", expected.Digest, got)
			}
			// A routing filter reads headers only, so the payload may be in
			// any format.
			if filter.Codec != wire.CodecHeadersOnly {
				t.Errorf("want the headers-only codec, got %q", filter.Codec)
			}
		})
	}
}

// buildSessionRecord assembles the record a corpus case describes, returning
// the envelope separately when the case has one.
func buildSessionRecord(
	t *testing.T, kind, operation, taskState, body, target, role *string,
	recorded *bool, memory, other *string,
) (wire.SessionRecord, *wire.AgentEnvelope) {
	t.Helper()

	if other != nil {
		switch *other {
		case "dead_letter":
			return wire.DeadLetterRecord(), nil
		case "journal":
			return wire.JournalRecord(), nil
		case "kv_mutation":
			return wire.KvMutationRecord(), nil
		case "graph_mutation":
			return wire.GraphMutationRecord(), nil
		case "undecodable":
			return wire.UndecodableRecord(nil), nil
		default:
			t.Fatalf("unknown record kind %q", *other)
		}
	}
	if memory != nil {
		var record wire.MemoryRecord
		switch *memory {
		case "item":
			record = wire.RememberItem("m", "fact", []byte("b"))
		case "forget":
			record = wire.ForgetItem("m")
		case "feedback":
			record = wire.FeedbackForItem("m", 1)
		default:
			t.Fatalf("unknown memory record %q", *memory)
		}
		return wire.MemorySessionRecord(&record), nil
	}

	if kind == nil {
		t.Fatalf("a case names neither a kind nor another record type")
	}
	envelope := &wire.AgentEnvelope{
		Kind:         wire.AgentKind(*kind),
		Conversation: wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})},
		Operation:    operation,
	}
	if taskState != nil {
		state, err := wire.ParseTaskState(*taskState)
		if err != nil {
			t.Fatalf("task state %q: %v", *taskState, err)
		}
		envelope.TaskState = &state
	}
	if body != nil {
		// A start body names the agent and SDK. A transition body names
		// neither, and that distinction decides started versus resumed.
		switch *body {
		case "start":
			encoded, err := wire.Marshal(wire.SessionStart{
				Agent: mustAgent(t, "planner"),
				SDK:   wire.SdkInfo{Language: "go", Version: "0"},
			})
			if err != nil {
				t.Fatalf("encode a start body: %v", err)
			}
			envelope.Body = encoded
		case "transition":
			encoded, err := wire.Marshal(wire.SessionTransition{})
			if err != nil {
				t.Fatalf("encode a transition body: %v", err)
			}
			envelope.Body = encoded
		case "end":
			encoded, err := wire.Marshal(wire.SessionEnd{})
			if err != nil {
				t.Fatalf("encode an end body: %v", err)
			}
			envelope.Body = encoded
		default:
			t.Fatalf("unknown body %q", *body)
		}
	}
	if target != nil {
		addressed := mustAgent(t, *target)
		envelope.Target = &addressed
	}
	if role != nil || recorded != nil {
		envelope.Metadata = map[string]wire.Value{}
		if role != nil {
			envelope.Metadata[wire.MetadataRole] = wire.StrValue(*role)
		}
		if recorded != nil {
			envelope.Metadata[wire.MetadataRecorded] = wire.BoolValue(*recorded)
		}
	}
	return wire.EnvelopeRecord(envelope), envelope
}

func parseHandled(t *testing.T, raw json.RawMessage) (wire.HandledOperations, bool) {
	t.Helper()
	if len(raw) == 0 || string(raw) == "null" {
		return wire.HandledOperations{}, false
	}
	var word string
	if err := json.Unmarshal(raw, &word); err == nil {
		if word != "any" {
			t.Fatalf("unknown handled selector %q", word)
		}
		return wire.AnyOperation(), true
	}
	var operations []string
	if err := json.Unmarshal(raw, &operations); err != nil {
		t.Fatalf("decode handled: %v", err)
	}
	return wire.OnlyOperations(operations...), true
}

func mustAgent(t *testing.T, name string) wire.AgentID {
	t.Helper()
	agent, err := wire.NewAgentID(name)
	if err != nil {
		t.Fatalf("agent id %q: %v", name, err)
	}
	return agent
}
