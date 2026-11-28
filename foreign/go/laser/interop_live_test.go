//go:build integration

// The two-client interop check: one client publishes, the other consumes.
//
// This is the only test in the suite that proves the Go port reads and writes
// bytes another implementation produced. Every other test, fixture or live,
// checks this client against its own encoder, which cannot catch a field that
// both sides of one port spell the same wrong way.
//
// The Rust half is the binary under scripts/interop, driven here through
// cargo. A record carries its provenance in the headers and a JSON mirror of
// the same values in its body, so neither language holds a copy of the other's
// fixture: the body says what the publisher meant, the headers say what it
// actually wrote, and the reader compares the two with its own decoder.
//
// Run it like the rest of the live suite:
//
//	cd foreign/go && LASER_LIVE_CONNECTION_STRING='iggy://iggy:iggy@127.0.0.1:18090' \
//	  go test ./laser/ -tags integration -count=1
//
// The Rust build uses the workspace toolchain selected by rust-toolchain.toml.
package laser_test

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// buildInterop builds the Rust peer, so a later run starts at once.
func buildInterop(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	manifest := filepath.Join("..", "..", "..", "scripts", "interop", "Cargo.toml")
	build := exec.CommandContext(ctx, "cargo", "build", "--quiet", "--locked", "--manifest-path", manifest)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build the Rust interop peer: %v\n%s", err, output)
	}
}

// runInterop runs the Rust peer using the repository toolchain.
func runInterop(t *testing.T, command, stream, topic string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	manifest := filepath.Join("..", "..", "..", "scripts", "interop", "Cargo.toml")
	run := exec.CommandContext(ctx, "cargo", "run", "--quiet", "--locked", "--manifest-path", manifest,
		"--", command, stream, topic)
	run.Env = append(os.Environ(), "LASER_CONNECTION_STRING="+liveConnectionString())
	output, err := run.Output()
	if err != nil {
		if failure, ok := err.(*exec.ExitError); ok {
			t.Fatalf("Rust interop %s failed: %v\n%s", command, err, failure.Stderr)
		}
		t.Fatalf("run Rust interop %s: %v", command, err)
	}
	return string(output)
}

// interopMirror renders a provenance block into the JSON shape both clients
// agree on. It has to match mirror_of in scripts/interop/src/main.rs field for
// field, which is what makes the comparison meaningful.
func interopMirror(provenance *laser.Provenance) map[string]any {
	mirror := map[string]any{
		"conversation":        provenance.Conversation.String(),
		"parent_conversation": conversationText(provenance.ParentConversation),
		"root_conversation":   conversationText(provenance.RootConversation),
		"agent":               agentText(provenance.Agent),
		"target_agent":        agentText(provenance.TargetAgent),
		"causal_parent":       nil,
		"idempotency_key":     stringOrNil(provenance.IdempotencyKey),
		"correlation_id":      stringOrNil(provenance.CorrelationID),
		"fence_token":         uintOrNil(provenance.FenceToken),
		"input_tokens":        nil,
		"output_tokens":       nil,
		"cost_usd":            nil,
		"deadline_micros":     nil,
	}
	if provenance.CausalParent != nil {
		mirror["causal_parent"] = provenance.CausalParent.String()
	}
	if provenance.DeadlineAt != nil {
		mirror["deadline_micros"] = uint64(provenance.DeadlineAt.UnixMicro())
	}
	if provenance.Usage != nil {
		mirror["input_tokens"] = uintOrNil(provenance.Usage.InputTokens)
		mirror["output_tokens"] = uintOrNil(provenance.Usage.OutputTokens)
		if provenance.Usage.CostUSD != nil {
			mirror["cost_usd"] = *provenance.Usage.CostUSD
		}
	}
	return mirror
}

func conversationText(id *wire.ConversationID) any {
	if id == nil {
		return nil
	}
	return id.String()
}

func agentText(id *wire.AgentID) any {
	if id == nil {
		return nil
	}
	return id.String()
}

func stringOrNil(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

func uintOrNil(value *uint64) any {
	if value == nil {
		return nil
	}
	return *value
}

// normalizeJSON puts a value through JSON so both sides are compared in the
// same representation. Without it a Go uint64 and the float64 that comes back
// out of a decoded document would differ on type alone, which says nothing
// about what crossed the wire.
func normalizeJSON(t *testing.T, value any) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("encode the mirror: %v", err)
	}
	var normalized map[string]any
	if err := json.Unmarshal(encoded, &normalized); err != nil {
		t.Fatalf("decode the mirror: %v", err)
	}
	return normalized
}

// TestGivenRustPublishedRecordWhenGoConsumesThenShouldReadTheSameSpine proves
// the Go client decodes a header block a different implementation wrote.
//
// This is the direction that matters most for a new port: the Rust client is
// the reference, so a field Go reads back differently is a Go defect rather
// than a disagreement.
func TestGivenRustPublishedRecordWhenGoConsumesThenShouldReadTheSameSpine(t *testing.T) {
	client := dialLive(t)
	// The build comes before the clock starts, because a cold cargo build of
	// the Rust client runs for minutes and would otherwise spend the whole
	// operation budget before the first request.
	buildInterop(t)
	ctx := liveContext(t)

	streamName := liveScratch(t, client, ctx, "laser-interop-rust-to-go").Name()
	topicName := "records"

	// The Rust client creates the topic as it publishes, so the Go side here
	// reads a topic it did not make: one more thing that has to agree across
	// the two.
	published := runInterop(t, "publish", streamName, topicName)
	t.Logf("the Rust client published %s", published)

	topic := client.Stream(streamName).Topic(topicName)
	consumer := liveConsumer(t, topic, ctx, livePartitions(t, topic, ctx))

	record, err := consumer.Next(ctx)
	if err != nil {
		t.Fatalf("read the record the Rust client published: %v", err)
	}
	if record.Provenance == nil {
		t.Fatal("the Go client read no provenance off a record the Rust client " +
			"published with a full header block")
	}

	// The body is what the publisher said it meant, decoded by Go. The mirror
	// is what Go made of the headers. They have to agree.
	var body map[string]any
	if err := json.Unmarshal(record.Payload(), &body); err != nil {
		t.Fatalf("read the record body the Rust client wrote: %v", err)
	}
	decoded := normalizeJSON(t, interopMirror(record.Provenance))

	if !reflect.DeepEqual(decoded, normalizeJSON(t, body)) {
		t.Errorf("the Go client read the Rust client's header block differently "+
			"than the Rust client wrote it.\n  Rust wrote: %v\n  Go read:    %v",
			body, decoded)
		for key, wanted := range normalizeJSON(t, body) {
			if got := decoded[key]; !reflect.DeepEqual(got, wanted) {
				t.Errorf("  field %s: Rust wrote %#v, Go read %#v", key, wanted, got)
			}
		}
	}
}

// TestGivenGoPublishedRecordWhenRustConsumesThenShouldReadTheSameSpine proves
// the other direction: a header block this client wrote is readable by the
// reference implementation.
//
// A port that only decodes correctly is half a port. Anything Go writes that
// Rust cannot read is a record already on someone's log that no other client
// will ever understand.
func TestGivenGoPublishedRecordWhenRustConsumesThenShouldReadTheSameSpine(t *testing.T) {
	client := dialLive(t)
	// The build comes before the clock starts, because a cold cargo build of
	// the Rust client runs for minutes and would otherwise spend the whole
	// operation budget before the first request.
	buildInterop(t)
	ctx := liveContext(t)

	scratch := liveScratch(t, client, ctx, "laser-interop-go-to-rust")
	streamName, topicName := scratch.Name(), "records"
	topic := scratch.Laser().Topic(topicName)
	if err := topic.Ensure(ctx, 1, nil); err != nil {
		t.Fatalf("create a topic on %s: %v", streamName, err)
	}

	author, err := wire.NewAgentID("go-author")
	if err != nil {
		t.Fatalf("build the author id: %v", err)
	}
	target, err := wire.NewAgentID("rust-reader")
	if err != nil {
		t.Fatalf("build the target id: %v", err)
	}

	causalParent := laser.NewMessageID(0, 42)
	parent := laser.NewConversationID()
	root := laser.NewConversationID()
	inputTokens, outputTokens := uint64(1024), uint64(256)
	// Exact in binary floating point, so a mismatch means the wire and not the
	// last bit of a decimal that never round-tripped cleanly.
	costUSD := 0.015625
	deadline := time.UnixMicro(1_800_000_000_000_000).UTC()
	idempotencyKey := "interop-idempotency-key"
	correlationID := "interop-correlation-id"
	fenceToken := uint64(99)

	provenance := &laser.Provenance{
		Conversation:       laser.NewConversationID(),
		CausalParent:       &causalParent,
		ParentConversation: &parent,
		RootConversation:   &root,
		Agent:              &author,
		TargetAgent:        &target,
		Usage: &laser.LlmUsage{
			InputTokens:  &inputTokens,
			OutputTokens: &outputTokens,
			CostUSD:      &costUSD,
		},
		DeadlineAt:     &deadline,
		IdempotencyKey: &idempotencyKey,
		CorrelationID:  &correlationID,
		FenceToken:     &fenceToken,
	}

	body, err := json.Marshal(interopMirror(provenance))
	if err != nil {
		t.Fatalf("encode the mirror body: %v", err)
	}
	if _, err := topic.Publish().Payload(body).Provenance(provenance).Send(ctx); err != nil {
		t.Fatalf("publish the record for the Rust client to read: %v", err)
	}

	// The Rust tool exits non-zero when the header block it decodes disagrees
	// with the body, so a clean run is the proof. Its stdout is logged so a
	// passing run still shows what crossed the wire.
	verified := runInterop(t, "verify", streamName, topicName)
	t.Logf("the Rust client read back %s", verified)
}
