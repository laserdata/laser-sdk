package wire_test

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// The remaining contract bodies, each decoded into its Go type and re-encoded.
//
// The structural corpus already proves these bytes survive a round trip
// through the value tree. What it cannot prove is that the Go type reads them
// correctly, so each case here names what it expects to find.

func TestGivenAgentBodyFixturesWhenDecodedThenShouldReEncodeByteIdentically(t *testing.T) {
	t.Run("agent_body_ref.bin", func(t *testing.T) {
		reference := roundTripFixture[wire.BodyRef](t, "agent_body_ref.bin")
		if reference.Reference == "" || reference.SizeBytes == 0 {
			t.Error("a body reference names where the bytes live and how many")
		}
		// The digest is what lets a reader tell it fetched the bytes the
		// producer meant.
		if len(reference.SHA256) != wire.DigestBytes {
			t.Errorf("a body reference digest is %d bytes", len(reference.SHA256))
		}
		if err := reference.Validate(); err != nil {
			t.Errorf("the fixture must validate: %v", err)
		}
	})

	t.Run("agent_signature.bin", func(t *testing.T) {
		signature := roundTripFixture[wire.Signature](t, "agent_signature.bin")
		if signature.Scheme != wire.SignatureSchemeEd25519 {
			t.Errorf("want the Ed25519 scheme, got %d", signature.Scheme)
		}
		if len(signature.KeyID) == 0 || len(signature.Bytes) == 0 {
			t.Error("a signature names its key and carries its bytes")
		}
	})

	t.Run("agent_usage_cost.bin", func(t *testing.T) {
		usage := roundTripFixture[wire.TokenUsage](t, "agent_usage_cost.bin")
		if usage.InputTokens == 0 || usage.OutputTokens == 0 {
			t.Error("usage accounts for both directions")
		}
		// Cost rides as integer micro-units, so it never loses a fraction to a
		// float.
		if usage.CostMicros == nil || *usage.CostMicros == 0 {
			t.Error("the fixture carries a cost")
		}
	})

	t.Run("agent_session_status.bin", func(t *testing.T) {
		status := roundTripFixture[wire.SessionStatus](t, "agent_session_status.bin")
		if status != wire.SessionPaused {
			t.Errorf("want a paused session, got %q", status)
		}
	})

	t.Run("key_record.bin", func(t *testing.T) {
		record := roundTripFixture[wire.KeyRecord](t, "key_record.bin")
		if record.Principal == "" || len(record.VerifyingKey) == 0 {
			t.Error("a key record names its principal and carries its key")
		}
		// A revoked key must read as revoked, since the field is what stops a
		// verifier trusting it.
		if record.Revoked {
			t.Error("the fixture key is live")
		}
		if record.ValidToMicros == nil ||
			record.ValidFromMicros >= *record.ValidToMicros {
			t.Error("a key record names a real validity window")
		}
	})
}

func TestGivenContextFoldFixturesWhenDecodedThenShouldReEncodeByteIdentically(t *testing.T) {
	t.Run("context_retrieval.bin", func(t *testing.T) {
		retrieval := roundTripFixture[wire.ContextRetrieval](t, "context_retrieval.bin")
		if retrieval.Query == nil || *retrieval.Query == "" {
			t.Error("the fixture records the query that selected the items")
		}
		if len(retrieval.Items) != 1 {
			t.Fatalf("want one item, got %d", len(retrieval.Items))
		}
		if retrieval.Items[0].ID != "item-7" || retrieval.Items[0].Score != 0.5 {
			t.Errorf("want the scored item, got %+v", retrieval.Items[0])
		}
	})

	t.Run("context_compaction.bin", func(t *testing.T) {
		compaction := roundTripFixture[wire.ContextCompaction](t, "context_compaction.bin")
		if compaction.SummaryAt.Kind != wire.SourceMessage {
			t.Error("a compaction points at the summary record it wrote")
		}
		if len(compaction.Covered) == 0 {
			t.Fatal("a compaction names the range it replaced")
		}
		// The covered range is half-open, so the end must be above the start
		// or the compaction replaced nothing.
		covered := compaction.Covered[0]
		if covered.Start >= covered.EndExclusive {
			t.Errorf("a covered range is empty: %+v", covered)
		}
	})

	t.Run("fold_snapshot.bin", func(t *testing.T) {
		snapshot := roundTripFixture[wire.FoldSnapshot](t, "fold_snapshot.bin")
		if snapshot.Stream == "" || snapshot.Fold == "" {
			t.Error("a snapshot names the stream and fold it belongs to")
		}
		if len(snapshot.AsOf) != 2 {
			t.Fatalf("want two source positions, got %d", len(snapshot.AsOf))
		}
		// The resume offset is the next one after what the fold consumed, and
		// a source the snapshot never saw resumes at zero.
		if got := snapshot.ResumeOffset(2, 20, 0); got != 42 {
			t.Errorf("resume offset is %d, want 42", got)
		}
		if got := snapshot.ResumeOffset(2, 21, 0); got != 0 {
			t.Errorf("another topic generation resumes at 0, got %d", got)
		}
		if err := snapshot.Validate(); err != nil {
			t.Errorf("the fixture must validate: %v", err)
		}
	})
}

func TestGivenDiscoveryFixturesWhenDecodedThenShouldReEncodeByteIdentically(t *testing.T) {
	t.Run("client_metadata_list.bin", func(t *testing.T) {
		page := roundTripFixture[wire.ClientMetadataList](t, "client_metadata_list.bin")
		if len(page.Clients) != 2 {
			t.Fatalf("want two connections, got %d", len(page.Clients))
		}
		// A connection that advertised nothing carries no metadata, which is
		// distinct from advertising empty bytes.
		if len(page.Clients[0].Metadata) == 0 {
			t.Error("the first connection advertised metadata")
		}
		if page.Clients[1].Metadata != nil {
			t.Error("the second connection advertised none")
		}
		if page.NextCursor == nil {
			t.Error("the fixture continues to another page")
		}
	})

	t.Run("client_metadata_query.bin", func(t *testing.T) {
		query := roundTripFixture[wire.ClientMetadataQuery](t, "client_metadata_query.bin")
		if !query.WithMetadataOnly {
			t.Error("the fixture asks only for connections that advertised")
		}
		if query.UserID == nil || query.Limit == 0 {
			t.Error("the fixture narrows by principal and bounds its page")
		}
	})

	t.Run("producer_presence.bin", func(t *testing.T) {
		presence := roundTripFixture[wire.ProducerPresence](t, "producer_presence.bin")
		if len(presence.Producers) == 0 {
			t.Fatal("the fixture reports a producer")
		}
		producer := presence.Producers[0]
		// Submitted counts what was sent and confirmed counts what the server
		// acknowledged, so the gap is what is still in flight or lost.
		if producer.ConfirmedRecords == nil ||
			*producer.ConfirmedRecords >= producer.SubmittedRecords {
			t.Error("the fixture has unconfirmed records")
		}
		if producer.Latency.Samples == 0 {
			t.Error("a producer reports its latency samples")
		}
		// A percentile with too few samples is absent rather than invented.
		if producer.Latency.P999Micros != nil {
			t.Error("the fixture has too few samples for a p999")
		}
	})

	t.Run("decode_record.bin", func(t *testing.T) {
		request := roundTripFixture[wire.DecodeRecord](t, "decode_record.bin")
		if request.ID == 0 || len(request.Payload) == 0 {
			t.Error("a decode request names a schema and carries a payload")
		}
	})
}

// The forwarded frames the streaming server hands to the managed backend. The
// identity fields are server-stamped, so a frame that decoded wrongly would
// attribute a request to the wrong principal.
func TestGivenForwardedFixturesWhenDecodedThenShouldReEncodeByteIdentically(t *testing.T) {
	t.Run("forwarded_query.bin", func(t *testing.T) {
		forwarded := roundTripFixture[wire.ForwardedQuery](t, "forwarded_query.bin")
		if forwarded.UserID == 0 {
			t.Error("a forwarded query carries the authenticated principal")
		}
		if len(forwarded.QueryEnvelope) == 0 {
			t.Error("the query envelope rides opaque and must survive")
		}
	})

	t.Run("forwarded_query_scoped.bin", func(t *testing.T) {
		forwarded := roundTripFixture[wire.ForwardedQuery](t,
			"forwarded_query_scoped.bin")
		// The scope is stamped once the caller's read authority over that
		// stream was checked, and the backend refuses a scoped query without
		// it.
		if forwarded.Scope == nil {
			t.Error("a scoped query carries the stream it was authorized for")
		}
	})

	t.Run("forwarded_command.bin", func(t *testing.T) {
		forwarded := roundTripFixture[wire.ForwardedCommand](t, "forwarded_command.bin")
		if forwarded.CommandCode == 0 {
			t.Error("a forwarded command names the code the backend dispatches on")
		}
		if len(forwarded.Payload) == 0 {
			t.Error("the request payload rides opaque and must survive")
		}
	})

	t.Run("forwarded_command_agent.bin", func(t *testing.T) {
		forwarded := roundTripFixture[wire.ForwardedCommand](t,
			"forwarded_command_agent.bin")
		if forwarded.Caller == nil || forwarded.Caller.Agent == nil {
			t.Error("this fixture names the agent its connection declared")
		}
	})

	t.Run("forwarded_command_on_behalf_of.bin", func(t *testing.T) {
		forwarded := roundTripFixture[wire.ForwardedCommand](t,
			"forwarded_command_on_behalf_of.bin")
		// The claim is recorded, never verified, so a reader treats it as
		// provenance rather than as the authenticated identity.
		if forwarded.Caller == nil || forwarded.Caller.OnBehalfOf == nil {
			t.Error("this fixture names the end user an intermediary wrote for")
		}
	})

	t.Run("forwarded_command_internal.bin", func(t *testing.T) {
		forwarded := roundTripFixture[wire.ForwardedCommand](t,
			"forwarded_command_internal.bin")
		// A request the server issues on its own names no caller, so the root
		// user, id zero, is never mistaken for one.
		if forwarded.Caller != nil {
			t.Error("a request the server issued itself names no caller")
		}
	})

	t.Run("forwarded_command_scoped.bin", func(t *testing.T) {
		forwarded := roundTripFixture[wire.ForwardedCommand](t,
			"forwarded_command_scoped.bin")
		if forwarded.Scope == nil {
			t.Error("a scoped command carries its stream identity")
		}
	})

	t.Run("forwarded_command_positioned.bin", func(t *testing.T) {
		forwarded := roundTripFixture[wire.ForwardedCommand](t,
			"forwarded_command_positioned.bin")
		// A positioned read makes the backend wait until its fold has caught
		// up, which is what turns a read-your-writes request into one.
		if !forwarded.ReadBarrier && forwarded.MinPosition == nil &&
			len(forwarded.MinSessions) == 0 {
			t.Error("a positioned command carries what the backend must have applied")
		}
	})
}
