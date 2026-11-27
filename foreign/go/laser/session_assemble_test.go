package laser_test

import (
	"context"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// GetStream answers a fixed stream id, which is all topicLocators needs: the
// scopeServer fixture otherwise has no stream-level state of its own.
func (server *scopeServer) GetStream(
	_ context.Context, _ iggcon.Identifier,
) (*iggcon.StreamDetails, error) {
	details := laneStream()
	details.Id = 7
	return details, nil
}

func assembleSession(t *testing.T, server *scopeServer, conversation wire.ConversationID) *laser.Session {
	t.Helper()
	return scopeHandle(server).Sessions().Open(conversation, "")
}

// A session's own history, read and bounded by a policy, turns into one
// fragment per message plus a manifest naming the policy, the token and byte
// totals, and the exact log position each fragment came from.
func TestGivenASessionsHistoryWhenAssembledThenShouldManifestEveryFragment(t *testing.T) {
	server := newScope()
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}
	server.appendRecord(t, conversation, "planner", "first")
	server.appendRecord(t, conversation, "planner", "second")
	server.appendRecord(t, conversation, "planner", "third")
	session := assembleSession(t, server, conversation)

	assembled, err := session.Assemble(context.Background(), laser.LastN(10))
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if len(assembled.Fragments) != 3 {
		t.Fatalf("want 3 fragments, got %d", len(assembled.Fragments))
	}
	if len(assembled.Manifest.Fragments) != 3 {
		t.Fatalf("want 3 manifest fragments, got %d", len(assembled.Manifest.Fragments))
	}
	if assembled.Manifest.Policy != laser.LastN(10).Name() {
		t.Errorf("want the policy's own name, got %q", assembled.Manifest.Policy)
	}
	var wantTokens, wantBytes uint64
	for _, fragment := range assembled.Fragments {
		body := fragment.Body()
		wantBytes += uint64(len(body))
		wantTokens += wire.EstimateTokens(len(body))
	}
	if assembled.Manifest.Tokens != wantTokens {
		t.Errorf("want %d tokens, got %d", wantTokens, assembled.Manifest.Tokens)
	}
	if assembled.Manifest.Bytes != wantBytes {
		t.Errorf("want %d bytes, got %d", wantBytes, assembled.Manifest.Bytes)
	}
	for _, fragment := range assembled.Manifest.Fragments {
		if fragment.Kind != wire.FragmentMessage {
			t.Errorf("want every fragment to be a message, got %v", fragment.Kind)
		}
		if fragment.At == nil || fragment.At.Message == nil {
			t.Fatal("want every fragment to name its log position")
		}
	}
}

// The frontier names the highest offset this read reached on each partition,
// not every offset read: three records on one partition manifest one
// frontier entry at the last of them.
func TestGivenMultipleRecordsOnOnePartitionWhenAssembledThenFrontierShouldNameTheHighestOffset(t *testing.T) {
	server := newScope()
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 2})}
	server.appendRecord(t, conversation, "planner", "first")
	server.appendRecord(t, conversation, "planner", "second")
	server.appendRecord(t, conversation, "planner", "third")
	session := assembleSession(t, server, conversation)

	assembled, err := session.Assemble(context.Background(), laser.LastN(10))
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if len(assembled.Manifest.Frontier) != 1 {
		t.Fatalf("want one frontier entry for the one partition read, got %d",
			len(assembled.Manifest.Frontier))
	}
	if offset := assembled.Manifest.Frontier[0].Offset; offset != 2 {
		t.Errorf("want the frontier at the highest offset read (2), got %d", offset)
	}
}

// A policy narrower than the full history is what Assemble hands the fetch
// to, so it narrows the assembled fragments the same way a direct
// ContextScope.Fetch under the same policy would.
func TestGivenANarrowingPolicyWhenAssembledThenShouldKeepOnlyWhatItSelects(t *testing.T) {
	server := newScope()
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 3})}
	server.appendRecord(t, conversation, "planner", "first")
	server.appendRecord(t, conversation, "planner", "second")
	server.appendRecord(t, conversation, "planner", "third")
	session := assembleSession(t, server, conversation)

	assembled, err := session.Assemble(context.Background(), laser.LastN(1))
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if len(assembled.Fragments) != 1 {
		t.Fatalf("want LastN(1) to keep one fragment, got %d", len(assembled.Fragments))
	}
	if string(assembled.Fragments[0].Body()) != "third" {
		t.Errorf("want the most recent record kept, got %q", assembled.Fragments[0].Body())
	}
}

// A session with nothing on its lane yet assembles to an empty, still valid,
// manifest rather than an error: a reader started before its writer is not a
// failure anywhere else in this package, and Assemble is no exception.
func TestGivenAnEmptySessionWhenAssembledThenShouldReturnAnEmptyManifest(t *testing.T) {
	server := newScope()
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 4})}
	session := assembleSession(t, server, conversation)

	assembled, err := session.Assemble(context.Background(), laser.LastN(10))
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if len(assembled.Fragments) != 0 {
		t.Errorf("want no fragments, got %d", len(assembled.Fragments))
	}
	if len(assembled.Manifest.Fragments) != 0 {
		t.Errorf("want no manifest fragments, got %d", len(assembled.Manifest.Fragments))
	}
	if assembled.Manifest.Tokens != 0 || assembled.Manifest.Bytes != 0 {
		t.Errorf("want zero tokens and bytes, got %d/%d",
			assembled.Manifest.Tokens, assembled.Manifest.Bytes)
	}
	if len(assembled.Manifest.Frontier) != 0 {
		t.Errorf("want no frontier entries, got %d", len(assembled.Manifest.Frontier))
	}
}
