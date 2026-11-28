package laser_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// memoryServer is a memory topic of one partition: a send appends at the next
// offset and a poll serves from the offset it asks for, which is what lets a
// test write memories and read them back the way a deployment would.
type memoryServer struct {
	clusterOfOne
	mutex       sync.Mutex
	appended    []iggcon.IggyMessage
	polls       []uint64
	topicAbsent bool
	pollFails   error
}

func (server *memoryServer) Close() error { return nil }

func (server *memoryServer) GetTopic(
	_ context.Context, _, _ iggcon.Identifier,
) (*iggcon.TopicDetails, error) {
	if server.topicAbsent {
		return nil, ierror.TopicNameNotFound{}
	}
	return &iggcon.TopicDetails{Topic: iggcon.Topic{PartitionsCount: 1}}, nil
}

func (server *memoryServer) SendMessages(
	_ context.Context, _, _ iggcon.Identifier,
	_ iggcon.Partitioning, messages []iggcon.IggyMessage,
) (*iggcon.SendMessagesResponse, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	for _, message := range messages {
		// The append time is the server's, so the fold orders a tombstone
		// against a write the way a real topic would.
		message.Header.Offset = uint64(len(server.appended))
		message.Header.Timestamp = uint64(len(server.appended) + 1)
		server.appended = append(server.appended, message)
	}
	return &iggcon.SendMessagesResponse{}, nil
}

func (server *memoryServer) PollMessages(
	_ context.Context, _, _ iggcon.Identifier, _ uint32,
	_ iggcon.Consumer, strategy iggcon.PollingStrategy, options iggcon.PollOptions,
) (*iggcon.PolledMessage, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.pollFails != nil {
		return nil, server.pollFails
	}
	server.polls = append(server.polls, strategy.Value)
	var out []iggcon.IggyMessage
	for _, message := range server.appended {
		if message.Header.Offset >= strategy.Value && uint32(len(out)) < options.Count {
			out = append(out, message)
		}
	}
	return &iggcon.PolledMessage{Messages: out}, nil
}

func (server *memoryServer) pollOffsets() []uint64 {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	return append([]uint64(nil), server.polls...)
}

func recallMemory(server *memoryServer, scope laser.MemoryScope) *laser.Memory {
	return laser.FromClient(server, laser.Options{DefaultStream: "agents"}).
		Memory("prefs").For(scope)
}

func mustRemember(t *testing.T, memory *laser.Memory, body string) laser.MemoryID {
	t.Helper()
	id, err := memory.Remember(context.Background(), laser.MemoryFact, []byte(body))
	if err != nil {
		t.Fatalf("remember %q: %v", body, err)
	}
	return id
}

func mustRecall(
	t *testing.T, memory *laser.Memory, query laser.MemoryQuery,
) []laser.MemoryItem {
	t.Helper()
	items, err := memory.Recall(context.Background(), query)
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	return items
}

// Writing to the log rather than to a store is what makes memory replayable,
// so what was remembered must come back off the topic it rode.
func TestGivenARememberedFactWhenRecalledThenShouldReadItBack(t *testing.T) {
	server := &memoryServer{}
	memory := recallMemory(server, laser.MemoryScope{User: "ana"})
	mustRemember(t, memory, "the user prefers green tea")

	items := mustRecall(t, memory, laser.MemoryQuery{})

	wantTexts(t, items, "the user prefers green tea")
	if items[0].Scope.User != "ana" {
		t.Errorf("want the owner read back, got %q", items[0].Scope.User)
	}
}

// A reader started before its writer is not a failure, so a memory topic
// nobody has written to recalls nothing rather than erroring.
func TestGivenAnAbsentTopicWhenRecalledThenShouldRecallNothing(t *testing.T) {
	server := &memoryServer{topicAbsent: true}
	memory := recallMemory(server, laser.MemoryScope{User: "ana"})

	items := mustRecall(t, memory, laser.MemoryQuery{})

	if len(items) != 0 {
		t.Errorf("want nothing, got %v", foldTexts(items))
	}
}

func TestGivenNoStreamWhenRecallingThenShouldRefuseIt(t *testing.T) {
	server := &memoryServer{}
	memory := laser.FromClient(server, laser.Options{}).Memory("prefs")

	_, err := memory.Recall(context.Background(), laser.MemoryQuery{})

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
}

// A read that failed must be reported rather than answered with a half-folded
// view, because a recall that silently drops memories is worse than one that
// fails.
func TestGivenAFailedReadWhenRecallingThenShouldReportIt(t *testing.T) {
	server := &memoryServer{pollFails: errors.New("the link dropped")}
	memory := recallMemory(server, laser.MemoryScope{User: "ana"})

	_, err := memory.Recall(context.Background(), laser.MemoryQuery{})

	if err == nil {
		t.Fatal("want the read failure reported")
	}
}

// Two users must never see each other's memories, so the owner has to ride
// the record rather than living only on the handle that wrote it.
func TestGivenTwoOwnersWhenRecalledThenShouldSeeOnlyTheirOwn(t *testing.T) {
	server := &memoryServer{}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"}).Memory("prefs")
	mustRemember(t, handle.For(laser.MemoryScope{User: "ana"}), "ana likes green tea")
	mustRemember(t, handle.For(laser.MemoryScope{User: "bo"}), "bo likes coffee")

	wantTexts(t, mustRecall(t, handle.For(laser.MemoryScope{User: "ana"}),
		laser.MemoryQuery{}), "ana likes green tea")
	wantTexts(t, mustRecall(t, handle.For(laser.MemoryScope{User: "bo"}),
		laser.MemoryQuery{}), "bo likes coffee")
}

// A conversation is the session layer, so a recall inside one must not reach
// what another session remembered, while a recall naming none sees both.
func TestGivenTwoConversationsWhenRecalledThenShouldNarrowToTheOneAsked(t *testing.T) {
	server := &memoryServer{}
	first := conversation(21)
	second := conversation(22)
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"}).Memory("prefs")
	mustRemember(t, handle.For(laser.MemoryScope{User: "ana", Conversation: &first}), "in the first")
	mustRemember(t, handle.For(laser.MemoryScope{User: "ana", Conversation: &second}), "in the second")

	wantTexts(t, mustRecall(t, handle.For(laser.MemoryScope{Conversation: &first}),
		laser.MemoryQuery{}), "in the first")
	if got := mustRecall(t, handle.For(laser.MemoryScope{User: "ana"}),
		laser.MemoryQuery{}); len(got) != 2 {
		t.Errorf("a recall naming no conversation widens, got %v", foldTexts(got))
	}
}

// The scenario the feature file turns on: a fact corrected in a later
// conversation leaves only the correction for that user everywhere.
func TestGivenACorrectedFactWhenRecalledThenShouldReturnTheCorrectionOnly(t *testing.T) {
	server := &memoryServer{}
	first := conversation(31)
	second := conversation(32)
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"}).Memory("prefs")
	inFirst := handle.For(laser.MemoryScope{User: "ana", Conversation: &first})
	inSecond := handle.For(laser.MemoryScope{User: "ana", Conversation: &second})
	old := mustRemember(t, inFirst, "the user is allergic to peanuts")

	if _, err := inSecond.Supersede(context.Background(), old,
		laser.MemoryFact, []byte("the user is allergic to shellfish")); err != nil {
		t.Fatalf("supersede: %v", err)
	}

	items := mustRecall(t, handle.For(laser.MemoryScope{User: "ana"}), laser.MemoryQuery{})
	wantTexts(t, items, "the user is allergic to shellfish")
	// The correction has to hold inside the conversation the old fact was
	// remembered in too, or a later turn there recalls it again.
	wantTexts(t, mustRecall(t, handle.For(laser.MemoryScope{Conversation: &first}),
		laser.MemoryQuery{}))
}

// A forget names the conversation its handle is scoped to, so a correction
// meant for one session does not reach another.
func TestGivenAScopedForgetWhenRecalledElsewhereThenShouldKeepTheFact(t *testing.T) {
	server := &memoryServer{}
	first := conversation(33)
	second := conversation(34)
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"}).Memory("prefs")
	inFirst := handle.For(laser.MemoryScope{User: "ana", Conversation: &first})
	old := mustRemember(t, inFirst, "remembered in the first")

	if err := handle.For(laser.MemoryScope{User: "ana", Conversation: &second}).
		Forget(context.Background(), old); err != nil {
		t.Fatalf("forget: %v", err)
	}

	wantTexts(t, mustRecall(t, inFirst, laser.MemoryQuery{}), "remembered in the first")
}

// Feedback changes what an agent is shown later without anything being
// rewritten, and it has to reach recall through the log to do so.
func TestGivenFeedbackWhenRecalledThenShouldReorderTheItems(t *testing.T) {
	server := &memoryServer{}
	memory := recallMemory(server, laser.MemoryScope{User: "ana"})
	promoted := mustRemember(t, memory, "promoted")
	mustRemember(t, memory, "newer")

	if err := memory.Improve(context.Background(), laser.Feedback{
		Target: promoted, Weight: 2,
	}); err != nil {
		t.Fatalf("improve: %v", err)
	}

	wantTexts(t, mustRecall(t, memory, laser.MemoryQuery{}), "promoted", "newer")
}

// Keyword recall keeps only what shares a word, which is the feature file's
// second recall shape.
func TestGivenAKeywordRecallWhenReadBackThenShouldKeepOnlyTheSharers(t *testing.T) {
	server := &memoryServer{}
	memory := recallMemory(server, laser.MemoryScope{User: "ana"})
	mustRemember(t, memory, "the user is allergic to peanuts")
	mustRemember(t, memory, "the user lives in Lisbon")

	items := mustRecall(t, memory, laser.MemoryQuery{
		Query: "Lisbon", Strategy: laser.RecallKeyword,
	})

	wantTexts(t, items, "the user lives in Lisbon")
}

// A held handle folds only what was appended since its last recall, because
// rescanning the whole topic each time costs a read of everything ever
// remembered.
func TestGivenASecondRecallWhenFoldedThenShouldReadOnlyWhatIsNew(t *testing.T) {
	server := &memoryServer{}
	memory := recallMemory(server, laser.MemoryScope{User: "ana"})
	mustRemember(t, memory, "first")
	mustRecall(t, memory, laser.MemoryQuery{})
	mustRemember(t, memory, "second")

	before := len(server.pollOffsets())
	items := mustRecall(t, memory, laser.MemoryQuery{})

	wantTexts(t, items, "second", "first")
	for _, offset := range server.pollOffsets()[before:] {
		if offset == 0 {
			t.Error("a second recall must resume rather than rescan from the start")
		}
	}
}

// Narrowing a handle changes which items a recall returns and not which topic
// they were read from, so the clones share one fold rather than each
// rebuilding it.
func TestGivenANarrowedHandleWhenRecallingThenShouldShareTheFold(t *testing.T) {
	server := &memoryServer{}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"}).Memory("prefs")
	mustRemember(t, handle.For(laser.MemoryScope{User: "ana"}), "remembered")
	mustRecall(t, handle.For(laser.MemoryScope{User: "ana"}), laser.MemoryQuery{})

	before := len(server.pollOffsets())
	wantTexts(t, mustRecall(t, handle.For(laser.MemoryScope{}),
		laser.MemoryQuery{}), "remembered")

	for _, offset := range server.pollOffsets()[before:] {
		if offset == 0 {
			t.Error("a narrowed handle must not fold the topic again from the start")
		}
	}
}

// The scope rides as headers because recall folds the topic back and narrows
// on them. A record naming neither conversation nor owner could only ever be
// recalled by everyone.
func TestGivenAMemoryWriteWhenStampedThenShouldCarryTheScopeAsHeaders(t *testing.T) {
	server := &memoryServer{}
	agent := agentID(t, "planner")
	session := conversation(41)
	memory := recallMemory(server, laser.MemoryScope{
		User: "ana", App: "support", Agent: &agent, Conversation: &session,
	})
	mustRemember(t, memory, "stamped")

	headers := map[string]string{}
	entries, err := iggcon.DeserializeHeaders(server.appended[0].UserHeaders)
	if err != nil {
		t.Fatalf("decode the headers: %v", err)
	}
	for _, entry := range entries {
		headers[string(entry.Key.Value)] = string(entry.Value.Value)
	}

	for key, want := range map[string]string{
		wire.HeaderMemoryNamespace: "stream:agents/prefs",
		wire.HeaderMemoryUser:      "ana",
		wire.HeaderMemoryApp:       "support",
		wire.HeaderConversationID:  session.String(),
		wire.HeaderAgentID:         agent.String(),
	} {
		if headers[key] != want {
			t.Errorf("want %s stamped %q, got %q", key, want, headers[key])
		}
	}
}

// An unset owner widens recall rather than narrowing it to the empty string,
// so there is nothing to stamp for it.
func TestGivenAnUnsetOwnerWhenStampedThenShouldLeaveTheHeaderOff(t *testing.T) {
	server := &memoryServer{}
	memory := recallMemory(server, laser.MemoryScope{})
	mustRemember(t, memory, "unowned")

	entries, err := iggcon.DeserializeHeaders(server.appended[0].UserHeaders)
	if err != nil {
		t.Fatalf("decode the headers: %v", err)
	}
	for _, entry := range entries {
		if key := string(entry.Key.Value); key == wire.HeaderMemoryUser ||
			key == wire.HeaderMemoryApp {
			t.Errorf("want no %s header, got %q", key, entry.Value.Value)
		}
	}
}

// Two namespaces share one memory topic, so a recall in one must not reach
// what the other remembered.
func TestGivenTwoNamespacesWhenRecalledThenShouldNotReachEachOther(t *testing.T) {
	server := &memoryServer{}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	mustRemember(t, handle.Memory("prefs").For(laser.MemoryScope{User: "ana"}), "a preference")
	mustRemember(t, handle.Memory("billing").For(laser.MemoryScope{User: "ana"}), "an invoice")

	items := mustRecall(t, handle.Memory("prefs").For(laser.MemoryScope{User: "ana"}),
		laser.MemoryQuery{})

	wantTexts(t, items, "a preference")
}
