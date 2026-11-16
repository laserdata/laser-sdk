package wire_test

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// The agent discovery and liveness bodies. A card that decoded wrongly would
// route work to an agent that cannot do it.
func TestGivenAgentCardFixturesWhenDecodedThenShouldReEncodeByteIdentically(t *testing.T) {
	t.Run("agent_card.bin", func(t *testing.T) {
		card := roundTripFixture[wire.AgentCard](t, "agent_card.bin")
		if card.Name == nil || card.Version == nil {
			t.Error("the fixture card names itself")
		}
		if len(card.Capabilities) != 2 {
			t.Fatalf("want two capabilities, got %d", len(card.Capabilities))
		}
		// A capability's content shape is tagged, so a codec name and a schema
		// id are never confused.
		chat := card.Capabilities[0]
		if chat.Input == nil || chat.Input.Kind != wire.ContentRefContentType ||
			chat.Input.ContentType != wire.ContentJSON {
			t.Errorf("want a JSON content type input, got %+v", chat.Input)
		}
		plan := card.Capabilities[1]
		if plan.Input == nil || plan.Input.Kind != wire.ContentRefSchemaID ||
			plan.Input.SchemaID != "reading.v1" {
			t.Errorf("want a schema id input, got %+v", plan.Input)
		}
		if chat.Health == nil || *chat.Health != wire.HealthHealthy {
			t.Error("the first capability is healthy")
		}
		if plan.Health == nil || *plan.Health != wire.HealthDegraded {
			t.Error("the second capability is degraded")
		}
		// A card with no lifetime is discovery only, and this one is liveness.
		if card.TTLMicros == nil {
			t.Error("the fixture card carries a lifetime")
		}
		if err := card.Validate(); err != nil {
			t.Errorf("the fixture must validate: %v", err)
		}
	})

	t.Run("agent_presence.bin", func(t *testing.T) {
		presence := roundTripFixture[wire.AgentPresence](t, "agent_presence.bin")
		if presence.V != wire.PresenceOpVersion {
			t.Errorf("version is %d, want %d", presence.V, wire.PresenceOpVersion)
		}
		// Presence answers where to send this agent work right now.
		if presence.Inbox == nil || *presence.Inbox == "" {
			t.Error("the fixture declares an inbox")
		}
		if err := presence.Validate(); err != nil {
			t.Errorf("the fixture must validate: %v", err)
		}
	})

	t.Run("agent_dead_letter.bin", func(t *testing.T) {
		letter := roundTripFixture[wire.AgentDeadLetter](t, "agent_dead_letter.bin")
		if letter.Reason != wire.DeadLetterRetryExhausted {
			t.Errorf("want retry exhausted, got %d", letter.Reason)
		}
		if letter.Attempts == 0 {
			t.Error("a dead letter counts its attempts")
		}
		// The payload is kept so the record can be replayed once the cause is
		// fixed.
		if len(letter.Payload) == 0 {
			t.Error("a dead letter keeps the record it gave up on")
		}
	})

	t.Run("agent_session_parking.bin", func(t *testing.T) {
		parking := roundTripFixture[wire.SessionParking](t, "agent_session_parking.bin")
		if parking.Role.IsZero() {
			t.Error("a parked record names the role that must act")
		}
		if err := parking.Validate(); err != nil {
			t.Errorf("the fixture must validate: %v", err)
		}
	})
}

// A parked record must name its source topic generation, so it stays resumable
// after a topic is recreated under the same numeric id.
func TestGivenAParkedRecordWithoutAGenerationWhenValidatedThenShouldRefuseIt(t *testing.T) {
	parking := roundTripFixture[wire.SessionParking](t, "agent_session_parking.bin")
	parking.Source.Message.Generation = nil
	if err := parking.Validate(); err == nil {
		t.Error("a parked record without a generation must be refused")
	}

	// A record parked against something that is not a log record is refused
	// too: there is nothing to resume from.
	parking.Source = wire.NewKvSource("ns", "key")
	if err := parking.Validate(); err == nil {
		t.Error("a parked record must point at a log record")
	}
}

// The context and state bodies, which are what make a model call reproducible.
func TestGivenContextFixturesWhenDecodedThenShouldReEncodeByteIdentically(t *testing.T) {
	t.Run("context_manifest.bin", func(t *testing.T) {
		manifest := roundTripFixture[wire.ContextManifest](t, "context_manifest.bin")
		if manifest.Policy == "" || manifest.PolicyVersion == "" {
			t.Error("a manifest names the policy that selected its inputs")
		}
		if len(manifest.Fragments) != 2 {
			t.Fatalf("want two fragments, got %d", len(manifest.Fragments))
		}
		if manifest.Fragments[0].Kind != wire.FragmentMessage ||
			manifest.Fragments[0].At == nil {
			t.Errorf("want a message fragment, got %+v", manifest.Fragments[0])
		}
		if manifest.Fragments[1].Kind != wire.FragmentMemory ||
			manifest.Fragments[1].ID != "item-7" {
			t.Errorf("want a memory fragment, got %+v", manifest.Fragments[1])
		}
		// The frontier is the exact source positions the manifest read
		// through, which is what makes the assembly reproducible.
		if len(manifest.Frontier) == 0 {
			t.Error("a manifest records the positions it read through")
		}
		if err := manifest.Validate(); err != nil {
			t.Errorf("the fixture must validate: %v", err)
		}
	})

	t.Run("state_delta.bin", func(t *testing.T) {
		delta := roundTripFixture[wire.StateDelta](t, "state_delta.bin")
		// The base revision makes the patch conditional, so two writers racing
		// on one document cannot both apply.
		if delta.BaseRevision == 0 || delta.OpID == "" {
			t.Error("a delta is guarded by a revision and an operation id")
		}
		if len(delta.Patch) != 1 || delta.Patch[0].Op != wire.PatchAdd {
			t.Fatalf("want one add, got %+v", delta.Patch)
		}
		if delta.Patch[0].Path != "/ready" {
			t.Errorf("want the ready path, got %q", delta.Patch[0].Path)
		}
		if err := delta.Validate(); err != nil {
			t.Errorf("the fixture must validate: %v", err)
		}
	})

	t.Run("state_snapshot.bin", func(t *testing.T) {
		snapshot := roundTripFixture[wire.StateSnapshot](t, "state_snapshot.bin")
		if snapshot.BaseRevision == 0 {
			t.Error("a snapshot names the revision it replaces")
		}
		if _, present := snapshot.Document.Field("ready"); !present {
			t.Error("the snapshot carries its document")
		}
	})
}

// Every fragment kind must survive the wire as its own variant: the kind is
// what tells a reader whether to look for a record, a memory item, or a key.
func TestGivenEveryContextFragmentWhenRoundTrippedThenShouldKeepItsVariant(t *testing.T) {
	at := wire.NewMessageSource(wire.MessageSource{Stream: 1, Topic: 2, Offset: 3})
	fragments := map[wire.FragmentKind]wire.Fragment{
		wire.FragmentMessage: wire.MessageFragment(at, 10, 40),
		wire.FragmentSummary: wire.SummaryFragment(at, 5, 20),
		wire.FragmentMemory:  wire.MemoryFragment("item-1", 4, 16),
		wire.FragmentKv:      wire.KvFragment("ns", "key", 2, 8),
		wire.FragmentState:   wire.StateFragment("doc", 7, 1, 4),
	}
	for kind, fragment := range fragments {
		encoded, err := wire.Marshal(fragment)
		if err != nil {
			t.Errorf("%s: marshal: %v", kind, err)
			continue
		}
		var back wire.Fragment
		if err := wire.Unmarshal(encoded, &back); err != nil {
			t.Errorf("%s: unmarshal: %v", kind, err)
			continue
		}
		if back.Kind != kind {
			t.Errorf("%s decoded as %s", kind, back.Kind)
			continue
		}
		again, err := wire.Marshal(back)
		if err != nil || string(again) != string(encoded) {
			t.Errorf("%s: re-encode differs", kind)
		}
	}
}
