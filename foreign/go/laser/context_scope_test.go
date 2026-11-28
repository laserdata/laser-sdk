package laser_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// scopeServer holds records per topic and accepts appends.
type scopeServer struct {
	*feedServer
	appended []iggcon.IggyMessage
	topics   []string
}

func newScope() *scopeServer {
	return &scopeServer{feedServer: newFeed(1)}
}

func (server *scopeServer) SendMessages(
	_ context.Context, _ iggcon.Identifier, topic iggcon.Identifier,
	_ iggcon.Partitioning, messages []iggcon.IggyMessage,
) (*iggcon.SendMessagesResponse, error) {
	named, err := topic.String()
	if err != nil {
		return nil, err
	}
	server.guard.Lock()
	defer server.guard.Unlock()
	server.appended = append(server.appended, messages...)
	server.topics = append(server.topics, named)
	return &iggcon.SendMessagesResponse{}, nil
}

// appendRecord puts one agent record of a conversation on the feed.
func (server *scopeServer) appendRecord(
	t *testing.T, conversation wire.ConversationID, author, body string,
) {
	t.Helper()
	server.appendEnvelope(t, conversation, nil, author, body)
}

// appendChild puts one agent record of a subconversation rooted at parent on the feed.
func (server *scopeServer) appendChild(
	t *testing.T, conversation, parent wire.ConversationID, author, body string,
) {
	t.Helper()
	server.appendEnvelope(t, conversation, &parent, author, body)
}

func (server *scopeServer) appendEnvelope(
	t *testing.T, conversation wire.ConversationID, parent *wire.ConversationID, author, body string,
) {
	t.Helper()
	source, err := wire.NewAgentID(author)
	if err != nil {
		t.Fatalf("agent id: %v", err)
	}
	record := wire.RecordID{ID: wire.IDFromBytes(laser.NewOperationID())}
	envelope := wire.AgentEnvelope{
		Kind: wire.KindEvent, Record: &record,
		Conversation: conversation, Source: source, Body: []byte(body),
		Parent: parent, Root: parent,
	}
	encoded, err := wire.Marshal(envelope)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	headers, err := laser.EncodeHeaderBlock(
		wire.HeadersForEnvelope(&envelope, wire.ContentJSON, true))
	if err != nil {
		t.Fatalf("headers: %v", err)
	}
	message, err := iggcon.NewIggyMessage(encoded, iggcon.WithUserHeaders(headers))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	server.guard.Lock()
	defer server.guard.Unlock()
	offset := server.floor[0] + uint64(len(server.records[0]))
	message.Header.Offset = offset
	message.Header.Timestamp = offset + 1
	server.records[0] = append(server.records[0], message)
}

func scopeHandle(server iggcon.Client) *laser.Laser {
	return laser.FromClient(server, laser.Options{DefaultStream: "agents"})
}

// The context is the log rather than a copy of it, so appending rides an
// ordinary publish keyed by the conversation.
func TestGivenAnAppendWhenWrittenThenShouldRideTheConversation(t *testing.T) {
	server := newScope()
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}

	if err := scopeHandle(server).Context(conversation).
		Append(context.Background(), wire.AgentSessions,
			[]byte("what happened")); err != nil {
		t.Fatalf("append: %v", err)
	}

	server.guard.Lock()
	defer server.guard.Unlock()
	if len(server.appended) != 1 {
		t.Fatalf("want one record, got %d", len(server.appended))
	}
	headers, err := iggcon.DeserializeHeaders(server.appended[0].UserHeaders)
	if err != nil {
		t.Fatalf("headers: %v", err)
	}
	provenance, err := laser.ProvenanceFromHeaders(headers)
	if err != nil {
		t.Fatalf("provenance: %v", err)
	}
	if provenance.Conversation != conversation {
		t.Errorf("want the conversation pinned, got %v", provenance.Conversation)
	}
	if server.topics[0] != wire.AgentSessions {
		t.Errorf("want the named topic, got %q", server.topics[0])
	}
}

// A read is bounded by turns, because an unbounded read of a long-lived
// conversation is a replay nobody asked for.
func TestGivenAHistoryWhenFetchedThenShouldBoundItByTurns(t *testing.T) {
	server := newScope()
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}
	for _, body := range []string{"first", "second", "third", "fourth"} {
		server.appendRecord(t, conversation, "planner", body)
	}

	messages, err := scopeHandle(server).Context(conversation).
		Fetch(context.Background(), []string{wire.AgentSessions}, 2, 0)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}

	if len(messages) != 2 {
		t.Fatalf("want the last two, got %d", len(messages))
	}
	// The last n, in order, so a prompt reads forwards.
	if string(messages[0].Body()) != "third" ||
		string(messages[1].Body()) != "fourth" {
		t.Errorf("want the latest turns in order, got %q then %q",
			messages[0].Body(), messages[1].Body())
	}
}

// A read is bounded by prompt size as well as by turns, because either alone
// leaves the other unbounded.
func TestGivenATokenBudgetWhenFetchedThenShouldBoundItBySizeToo(t *testing.T) {
	server := newScope()
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}
	for range 4 {
		server.appendRecord(t, conversation, "planner",
			strings.Repeat("x", 400))
	}

	messages, err := scopeHandle(server).Context(conversation).
		Fetch(context.Background(), []string{wire.AgentSessions}, 4, 110)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}

	if len(messages) == 0 || len(messages) >= 4 {
		t.Fatalf("want the budget to trim the turns, got %d", len(messages))
	}
	// A budget always keeps something, so an oversized message reaches the
	// model to be truncated there rather than vanishing before it arrives.
	if len(messages) < 1 {
		t.Error("want at least one message kept")
	}
}

// Another conversation's traffic is on the same topic, so a read keeps only
// what belongs to this one.
func TestGivenAnotherConversationOnTheTopicWhenFetchedThenShouldLeaveIt(t *testing.T) {
	server := newScope()
	mine := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}
	theirs := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 2})}
	server.appendRecord(t, mine, "planner", "mine")
	server.appendRecord(t, theirs, "planner", "theirs")
	server.appendRecord(t, mine, "planner", "also mine")

	messages, err := scopeHandle(server).Context(mine).
		Fetch(context.Background(), []string{wire.AgentSessions}, 10, 0)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}

	if len(messages) != 2 {
		t.Fatalf("want this conversation alone, got %d", len(messages))
	}
	for _, message := range messages {
		if strings.Contains(string(message.Body()), "theirs") {
			t.Error("want another conversation left alone")
		}
	}
}

// The block is the prompt-ready form: the same bounded read, joined.
func TestGivenAHistoryWhenRenderedThenShouldJoinTheBodies(t *testing.T) {
	server := newScope()
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}
	server.appendRecord(t, conversation, "planner", "first")
	server.appendRecord(t, conversation, "planner", "second")

	block, err := scopeHandle(server).Context(conversation).
		Block(context.Background(), []string{wire.AgentSessions}, 10, 0)
	if err != nil {
		t.Fatalf("block: %v", err)
	}

	if block != "first\nsecond" {
		t.Errorf("want the bodies joined in order, got %q", block)
	}
}

// Rebuilding state is folding the log, and what the state means is the
// caller's: this knows how to read in order and nothing about what it says.
func TestGivenAHistoryWhenFoldedThenShouldRebuildTheCallersState(t *testing.T) {
	server := newScope()
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}
	for _, body := range []string{"a", "b", "c"} {
		server.appendRecord(t, conversation, "planner", body)
	}

	folded, err := laser.State(context.Background(),
		scopeHandle(server).Context(conversation),
		[]string{wire.AgentSessions}, 10, 0, "",
		func(state string, message laser.ContextMessage) string {
			return state + string(message.Body())
		})
	if err != nil {
		t.Fatalf("state: %v", err)
	}

	if folded != "abc" {
		t.Errorf("want the fold in log order, got %q", folded)
	}
}

// A checkpoint says where the topics stood, so a caller folds only what
// arrived since rather than folding the same records twice.
func TestGivenACheckpointWhenResumedThenShouldReadOnlyWhatArrivedSince(t *testing.T) {
	server := newScope()
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}
	scope := scopeHandle(server).Context(conversation)
	server.appendRecord(t, conversation, "planner", "before")

	checkpoint, err := scope.Checkpoint(
		context.Background(), []string{wire.AgentSessions})
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	server.appendRecord(t, conversation, "planner", "after")

	since, err := scope.FetchSince(context.Background(), checkpoint,
		[]string{wire.AgentSessions}, laser.LastN(10))
	if err != nil {
		t.Fatalf("fetch since: %v", err)
	}

	if len(since) != 1 || string(since[0].Body()) != "after" {
		t.Fatalf("want only what arrived since, got %+v", since)
	}
	// Offsets are per topic partition, which is what the log has to resume
	// from: a conversation is a key within a partition, not a position.
	if offsets := checkpoint.Offsets[wire.AgentSessions]; len(offsets) != 1 ||
		offsets[0] != 1 {
		t.Errorf("want the partition tail, got %v", offsets)
	}

	// No checkpoint reads the lot, which is the deliberate full walk.
	all, err := scope.FetchSince(context.Background(), nil,
		[]string{wire.AgentSessions}, laser.LastN(10))
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(all) != 2 {
		t.Errorf("want the whole history, got %d", len(all))
	}
}

// This conversation's memory never repeats the conversation id, because the
// scope already knows it.
func TestGivenAScopedMemoryWhenOpenedThenShouldCarryTheConversation(t *testing.T) {
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}

	memory := scopeHandle(newScope()).Context(conversation).Memory("notes")

	scope := memory.Scope()
	if scope.Conversation == nil || *scope.Conversation != conversation {
		t.Errorf("want the conversation applied, got %v", scope.Conversation)
	}
	if memory.Namespace() != "notes" {
		t.Errorf("want the namespace kept, got %q", memory.Namespace())
	}
}

// A dependency graph is shared across conversations, because an edge holds
// whichever task asked, so the scope hands it back unnarrowed.
func TestGivenAGraphFromAScopeWhenOpenedThenShouldNotNarrowIt(t *testing.T) {
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}
	handle := scopeHandle(newScope())

	fromScope := handle.Context(conversation).Graph("deps")
	direct := handle.Graph("deps")

	if fromScope.Name() != direct.Name() {
		t.Errorf("want the same graph, got %q and %q",
			fromScope.Name(), direct.Name())
	}
}

// A scope needs a stream to read and write on.
func TestGivenNoStreamWhenAScopeReadsThenShouldRefuseIt(t *testing.T) {
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}
	scope := laser.FromClient(newScope(), laser.Options{}).Context(conversation)

	if _, err := scope.Fetch(context.Background(),
		[]string{wire.AgentSessions}, 10, 0); err == nil {
		t.Error("a read without a stream must be refused")
	}
	if _, err := scope.Checkpoint(context.Background(),
		[]string{wire.AgentSessions}); err == nil {
		t.Error("a checkpoint without a stream must be refused")
	}
	if _, err := scope.FetchSince(context.Background(), nil,
		[]string{wire.AgentSessions}, laser.LastN(1)); err == nil {
		t.Error("a resumed read without a stream must be refused")
	}
	if err := scope.Append(context.Background(),
		wire.AgentSessions, []byte("x")); err == nil {
		t.Error("an append without a stream must be refused")
	}
}

// A scope says which conversation it covers and what it reads through, so a
// caller passing one around does not have to carry both beside it.
func TestGivenAScopeWhenAskedThenShouldNameItsConversation(t *testing.T) {
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}
	handle := scopeHandle(newScope())

	scope := handle.Context(conversation)

	if scope.Conversation() != conversation {
		t.Errorf("want the conversation, got %v", scope.Conversation())
	}
	if scope.Laser() != handle {
		t.Error("want the handle it reads through")
	}
}

// A read of a topic nobody has written to yet is empty rather than an error:
// a reader started before its writer is not a failure.
func TestGivenAnUnwrittenTopicWhenFetchedThenShouldReadNothing(t *testing.T) {
	server := newScope()
	server.missing = true
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}

	messages, err := scopeHandle(server).Context(conversation).
		Fetch(context.Background(), []string{wire.AgentSessions}, 10, 0)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(messages) != 0 {
		t.Errorf("want nothing, got %d", len(messages))
	}
}

// A caller that gives up stops the read.
func TestGivenACancelledCallerWhenFetchingThenShouldStop(t *testing.T) {
	server := newScope()
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}
	server.appendRecord(t, conversation, "planner", "a")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := scopeHandle(server).Context(conversation).
		Fetch(ctx, []string{wire.AgentSessions}, 10, 0)

	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("want the cancellation, got %v", err)
	}
}
