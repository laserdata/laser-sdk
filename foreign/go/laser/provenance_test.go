package laser_test

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func textPointer(value string) *string { return &value }

func uint64Pointer(value uint64) *uint64 { return &value }

// roundTripProvenance carries the spine through the exact encoding a record
// uses, so the test proves the two halves agree rather than that one of them
// runs.
func roundTripProvenance(t *testing.T, provenance *laser.Provenance) *laser.Provenance {
	t.Helper()
	entries, err := provenance.Headers()
	if err != nil {
		t.Fatalf("encode the provenance: %v", err)
	}
	message, err := iggcon.NewIggyMessage([]byte("body"), iggcon.WithUserHeaders(entries))
	if err != nil {
		t.Fatalf("build the record: %v", err)
	}
	back, err := laser.ProvenanceFrom(&message)
	if err != nil {
		t.Fatalf("decode the provenance: %v", err)
	}
	return back
}

// Every field has to survive the wire, or a reader reconstructs a different
// exchange than the one that happened.
func TestGivenAFullProvenanceWhenRoundTrippedThenShouldSurviveIntact(t *testing.T) {
	deadline := time.UnixMicro(1_700_000_000_000_000)
	agent, err := wire.NewAgentID("planner")
	if err != nil {
		t.Fatalf("agent id: %v", err)
	}
	target, err := wire.NewAgentID("executor")
	if err != nil {
		t.Fatalf("agent id: %v", err)
	}
	parent := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 2})}
	root := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 3})}
	cause := wire.RecordID{ID: wire.IDFromBytes([16]byte{15: 4})}
	cost := 0.0125

	sent := &laser.Provenance{
		Conversation:       conversation(1),
		CausalParent:       &cause,
		ParentConversation: &parent,
		RootConversation:   &root,
		Agent:              &agent,
		TargetAgent:        &target,
		Usage: &laser.LlmUsage{
			InputTokens:  uint64Pointer(120),
			OutputTokens: uint64Pointer(45),
			CostUSD:      &cost,
		},
		DeadlineAt:     &deadline,
		IdempotencyKey: textPointer("order-7"),
		CorrelationID:  textPointer("corr-9"),
		FenceToken:     uint64Pointer(42),
	}

	back := roundTripProvenance(t, sent)

	if back.Conversation != sent.Conversation {
		t.Errorf("want the conversation kept, got %v", back.Conversation)
	}
	if back.CausalParent == nil || *back.CausalParent != cause {
		t.Errorf("want the cause kept, got %v", back.CausalParent)
	}
	if back.ParentConversation == nil || *back.ParentConversation != parent {
		t.Errorf("want the parent kept, got %v", back.ParentConversation)
	}
	if back.RootConversation == nil || *back.RootConversation != root {
		t.Errorf("want the root kept, got %v", back.RootConversation)
	}
	if back.Agent == nil || *back.Agent != agent {
		t.Errorf("want the author kept, got %v", back.Agent)
	}
	if back.TargetAgent == nil || *back.TargetAgent != target {
		t.Errorf("want the addressee kept, got %v", back.TargetAgent)
	}
	if back.IdempotencyKey == nil || *back.IdempotencyKey != "order-7" {
		t.Errorf("want the dedup key kept, got %v", back.IdempotencyKey)
	}
	if back.CorrelationID == nil || *back.CorrelationID != "corr-9" {
		t.Errorf("want the correlation kept, got %v", back.CorrelationID)
	}
	if back.FenceToken == nil || *back.FenceToken != 42 {
		t.Errorf("want the fence kept, got %v", back.FenceToken)
	}
	if back.DeadlineAt == nil || !back.DeadlineAt.Equal(deadline) {
		t.Errorf("want the deadline kept, got %v", back.DeadlineAt)
	}
	if back.Usage == nil || back.Usage.InputTokens == nil || *back.Usage.InputTokens != 120 {
		t.Errorf("want the input tokens kept, got %+v", back.Usage)
	}
	if back.Usage.OutputTokens == nil || *back.Usage.OutputTokens != 45 {
		t.Errorf("want the output tokens kept, got %+v", back.Usage)
	}
	if back.Usage.CostUSD == nil || *back.Usage.CostUSD != cost {
		t.Errorf("want the cost kept, got %+v", back.Usage.CostUSD)
	}
}

// A number must ride typed rather than as its decimal text, so a reader gets
// the value back and not a string that looks like one.
func TestGivenNumericProvenanceWhenEncodedThenShouldRideTyped(t *testing.T) {
	cost := 1.5
	provenance := &laser.Provenance{
		Conversation: conversation(1),
		FenceToken:   uint64Pointer(7),
		Usage:        &laser.LlmUsage{InputTokens: uint64Pointer(9), CostUSD: &cost},
	}

	entries, err := provenance.Headers()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	kinds := map[string]iggcon.HeaderKind{}
	for _, entry := range entries {
		kinds[string(entry.Key.Value)] = entry.Value.Kind
	}
	if kinds[wire.HeaderFence] != iggcon.Uint64 {
		t.Errorf("want a numeric fence, got kind %v", kinds[wire.HeaderFence])
	}
	if kinds[wire.HeaderUsageInputTokens] != iggcon.Uint64 {
		t.Errorf("want numeric tokens, got kind %v", kinds[wire.HeaderUsageInputTokens])
	}
	if kinds[wire.HeaderCostUSD] != iggcon.Double {
		t.Errorf("want a numeric cost, got kind %v", kinds[wire.HeaderCostUSD])
	}
	if kinds[wire.HeaderConversationID] != iggcon.String {
		t.Errorf("want a textual conversation, got kind %v", kinds[wire.HeaderConversationID])
	}
}

// Without a conversation a record has no partition key and no conversation to
// belong to, so reading one back is a failure rather than an empty spine.
func TestGivenNoConversationWhenDecodedThenShouldRefuseIt(t *testing.T) {
	key, err := iggcon.NewHeaderKeyString(wire.HeaderCorrelationID)
	if err != nil {
		t.Fatalf("header key: %v", err)
	}
	entries := []iggcon.HeaderEntry{{
		Key:   key,
		Value: iggcon.HeaderValue{Kind: iggcon.String, Value: []byte("corr-1")},
	}}

	_, err = laser.ProvenanceFromHeaders(entries)

	// It is its own type, because a record with no spine and a record whose
	// spine is broken want opposite handling.
	var absent *laser.NoProvenanceError
	if !errors.As(err, &absent) {
		t.Fatalf("want a NoProvenanceError, got %T: %v", err, err)
	}
}

// Broadcast addresses every agent and is never an agent id, so it must not
// read back as an agent named "*".
func TestGivenABroadcastAddresseeWhenDecodedThenShouldNameNoTarget(t *testing.T) {
	block := wire.NewRecordHeaders(conversation(1))
	addressee := wire.BroadcastAddressee()
	block.Addressee = &addressee
	entries, err := laser.EncodeHeaderBlock(block)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	provenance, err := laser.ProvenanceFromHeaders(entries)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if provenance.TargetAgent != nil {
		t.Errorf("a broadcast names no target, got %v", provenance.TargetAgent)
	}
}

// A header outside the dictionary is foreign: an application's own key and the
// AGDX envelope stamps must pass through without breaking the decode.
func TestGivenForeignHeadersWhenDecodedThenShouldIgnoreThem(t *testing.T) {
	entries, err := (&laser.Provenance{Conversation: conversation(1)}).Headers()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	foreign, err := iggcon.NewHeaderKeyString("tenant")
	if err != nil {
		t.Fatalf("header key: %v", err)
	}
	entries = append(entries, iggcon.HeaderEntry{
		Key:   foreign,
		Value: iggcon.HeaderValue{Kind: iggcon.String, Value: []byte("acme")},
	})

	provenance, err := laser.ProvenanceFromHeaders(entries)
	if err != nil {
		t.Fatalf("a foreign header must not break the decode: %v", err)
	}
	if provenance.Conversation != conversation(1) {
		t.Error("want the conversation still read")
	}
}

// A known key carrying the wrong kind is corruption, not something to drop.
// Losing a fence silently would skip the gate and let a stale holder's replay
// through as if it were unfenced.
func TestGivenAMalformedKnownHeaderWhenDecodedThenShouldRefuseIt(t *testing.T) {
	for name, key := range map[string]string{
		"fence":    wire.HeaderFence,
		"deadline": wire.HeaderDeadline,
		"cost":     wire.HeaderCostUSD,
	} {
		t.Run(name, func(t *testing.T) {
			entries, err := (&laser.Provenance{Conversation: conversation(1)}).Headers()
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			corrupt, err := iggcon.NewHeaderKeyString(key)
			if err != nil {
				t.Fatalf("header key: %v", err)
			}
			entries = append(entries, iggcon.HeaderEntry{
				Key:   corrupt,
				Value: iggcon.HeaderValue{Kind: iggcon.String, Value: []byte("not a number")},
			})

			if _, err := laser.ProvenanceFromHeaders(entries); err == nil {
				t.Error("a known key with the wrong kind must be refused")
			}
		})
	}
}

// A non-finite cost has no agreed spelling and would be rolled up into a
// total, so it never reaches the wire.
func TestGivenANonFiniteCostWhenEncodedThenShouldRefuseIt(t *testing.T) {
	for name, cost := range map[string]float64{
		"not a number": math.NaN(),
		"infinite":     math.Inf(1),
	} {
		t.Run(name, func(t *testing.T) {
			provenance := &laser.Provenance{
				Conversation: conversation(1),
				Usage:        &laser.LlmUsage{CostUSD: &cost},
			}
			if _, err := provenance.Headers(); err == nil {
				t.Error("a non-finite cost must be refused")
			}
		})
	}
}

// The conversation is the partition key, which is what keeps one conversation
// on one partition and therefore in order.
func TestGivenProvenanceWhenPublishedThenShouldPartitionByTheConversation(t *testing.T) {
	server := &streamServer{}
	provenance := &laser.Provenance{Conversation: conversation(5)}

	if _, err := streamHandle(server, laser.Options{DefaultStream: "agents"}).
		Topic("sessions").Publish().Payload([]byte("body")).
		Provenance(provenance).Send(context.Background()); err != nil {
		t.Fatalf("publish: %v", err)
	}

	if server.sentPartitions.Kind != iggcon.MessageKey {
		t.Errorf("want key partitioning, got %v", server.sentPartitions.Kind)
	}
	if string(server.sentPartitions.Value) != provenance.PartitionKey() {
		t.Errorf("want the conversation as the key, got %q", server.sentPartitions.Value)
	}
	headers := sentHeaders(t, server)
	if headers[wire.HeaderConversationID] != provenance.PartitionKey() {
		t.Errorf("want the conversation stamped, got %q", headers[wire.HeaderConversationID])
	}
}

// An explicit key wins, for a caller routing by something other than the
// conversation.
func TestGivenAnExplicitKeyWithProvenanceWhenPublishedThenShouldKeepTheKey(t *testing.T) {
	server := &streamServer{}

	if _, err := streamHandle(server, laser.Options{DefaultStream: "agents"}).
		Topic("sessions").Publish().Payload([]byte("body")).Key("by-tenant").
		Provenance(&laser.Provenance{Conversation: conversation(5)}).
		Send(context.Background()); err != nil {
		t.Fatalf("publish: %v", err)
	}

	if string(server.sentPartitions.Value) != "by-tenant" {
		t.Errorf("want the explicit key kept, got %q", server.sentPartitions.Value)
	}
}

// There are two size rules, and a caller shortens a different thing for each,
// so a refusal has to say which one it broke.
func TestGivenOversizedHeadersWhenEncodedThenShouldNameWhichCapWasBroken(t *testing.T) {
	t.Run("one value past the value cap", func(t *testing.T) {
		provenance := &laser.Provenance{
			Conversation:   conversation(1),
			IdempotencyKey: textPointer(strings.Repeat("k", wire.HeaderValueMax+1)),
		}

		_, err := provenance.Headers()
		if err == nil {
			t.Fatal("an oversized header value must be refused")
		}
		if !strings.Contains(err.Error(), "value cap") {
			t.Errorf("want the value cap named, got %v", err)
		}
	})

	t.Run("a block past the total cap", func(t *testing.T) {
		// Every value here is inside the value cap, so only the total trips.
		atCap := strings.Repeat("v", wire.HeaderValueMax)
		agent, err := wire.NewAgentID(atCap)
		if err != nil {
			t.Fatalf("agent id: %v", err)
		}
		target, err := wire.NewAgentID(atCap)
		if err != nil {
			t.Fatalf("agent id: %v", err)
		}
		provenance := &laser.Provenance{
			Conversation:   conversation(1),
			Agent:          &agent,
			TargetAgent:    &target,
			IdempotencyKey: textPointer(atCap),
			CorrelationID:  textPointer(atCap),
		}

		_, err = provenance.Headers()
		if err == nil {
			t.Fatal("a block past the total cap must be refused")
		}
		if strings.Contains(err.Error(), "value cap") {
			t.Errorf("want the total cap named, got %v", err)
		}
		if !strings.Contains(err.Error(), "over the") {
			t.Errorf("want the total cap named, got %v", err)
		}
	})
}

// A peer can write a cost this client would never produce, so the decode
// refuses one that would be rolled up into a total as a number that is not one.
func TestGivenANonFiniteCostOnTheWireWhenDecodedThenShouldRefuseIt(t *testing.T) {
	entries, err := (&laser.Provenance{Conversation: conversation(1)}).Headers()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	costKey, err := iggcon.NewHeaderKeyString(wire.HeaderCostUSD)
	if err != nil {
		t.Fatalf("header key: %v", err)
	}
	encoded := make([]byte, 8)
	binary.LittleEndian.PutUint64(encoded, math.Float64bits(math.Inf(1)))
	entries = append(entries, iggcon.HeaderEntry{
		Key:   costKey,
		Value: iggcon.HeaderValue{Kind: iggcon.Double, Value: encoded},
	})

	if _, err := laser.ProvenanceFromHeaders(entries); err == nil {
		t.Error("a non-finite cost on the wire must be refused")
	}
}

// An empty value is not the same as an absent header: it reaches the wire as a
// key carrying nothing, which a reader cannot tell from a key that was set to
// the empty string on purpose. Omit the field instead.
func TestGivenAnEmptyHeaderValueWhenEncodedThenShouldRefuseIt(t *testing.T) {
	provenance := &laser.Provenance{
		Conversation:   conversation(1),
		IdempotencyKey: textPointer(""),
	}

	_, err := provenance.Headers()
	if err == nil {
		t.Fatal("an empty header value must be refused")
	}
	if !strings.Contains(err.Error(), wire.HeaderIdempotencyKey) {
		t.Errorf("want the header named, got %v", err)
	}
}
