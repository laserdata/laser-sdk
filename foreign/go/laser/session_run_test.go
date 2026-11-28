package laser_test

import (
	"context"
	"errors"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenWorkThatSucceedsWhenRunThenShouldEndTheSessionCompleted(t *testing.T) {
	server := &streamServer{streamExists: true}
	session := runSession(t, server)
	ran := false

	err := session.Run(context.Background(), nil, func(_ context.Context, inside *laser.Session) error {
		ran = inside.Conversation() == session.Conversation()
		return nil
	})

	if err != nil || !ran {
		t.Fatalf("want the work run inside the session, got ran=%v err=%v", ran, err)
	}
	if state := lastTaskState(t, server); state != wire.TaskCompleted {
		t.Errorf("want the session completed, got %v", state)
	}
}

func TestGivenWorkThatFailsWhenRunThenShouldEndTheSessionFailedAndReturnTheError(t *testing.T) {
	server := &streamServer{streamExists: true}
	session := runSession(t, server)
	failure := errors.New("the model refused")

	err := session.Run(context.Background(), nil, func(context.Context, *laser.Session) error {
		return failure
	})

	if !errors.Is(err, failure) {
		t.Fatalf("want the error of the work, got %v", err)
	}
	end := lastSessionEnd(t, server)
	if state := lastTaskState(t, server); state != wire.TaskFailed {
		t.Errorf("want the session failed, got %v", state)
	}
	if end.Error == nil || end.Error.Message == nil || *end.Error.Message != "the model refused" ||
		end.Error.Code != wire.ErrorInternal {
		t.Errorf("want the failure written, got %+v", end.Error)
	}
}

func TestGivenWorkThatPanicsWhenRunThenShouldEndTheSessionFailedAndPanicAgain(t *testing.T) {
	server := &streamServer{streamExists: true}
	session := runSession(t, server)

	raised := func() (raised any) {
		defer func() { raised = recover() }()
		_ = session.Run(context.Background(), nil, func(context.Context, *laser.Session) error {
			panic("the tool crashed")
		})
		return nil
	}()

	if raised != "the tool crashed" {
		t.Fatalf("want the panic raised again, got %v", raised)
	}
	end := lastSessionEnd(t, server)
	if end.Error == nil || end.Error.Message == nil || *end.Error.Message != "the tool crashed" {
		t.Fatalf("want the panic written as the failure, got %+v", end.Error)
	}
	if flag := end.Error.Detail["panic"]; flag.Kind != wire.ValueBool || !flag.Bool {
		t.Errorf("want the failure marked as a panic, got %+v", end.Error.Detail)
	}
}

func TestGivenAnEnvelopeOfThisSessionWhenAppendedThenShouldRideTheLaneAsItsSource(t *testing.T) {
	server := &streamServer{streamExists: true}
	session := runSession(t, server)
	record := wire.RecordID{ID: wire.IDFromBytes(laser.NewOperationID())}
	envelope := wire.AgentEnvelope{
		Kind: wire.KindEvent, Record: &record, Conversation: session.Conversation(),
		Source: agentID(t, "auditor"), Body: []byte("reviewed"),
	}

	receipt, err := session.Append(context.Background(), envelope)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	sent := sentEnvelope(t, server)
	if receipt.Record != record || sent.Source != agentID(t, "auditor") || string(sent.Body) != "reviewed" {
		t.Errorf("want the envelope appended as built, got %+v", sent)
	}
	if string(server.sentPartitions.Value) != session.Conversation().String() {
		t.Errorf("want the session as the key, got %q", server.sentPartitions.Value)
	}
}

func TestGivenAnEnvelopeOfAnotherSessionWhenAppendedThenShouldRefuseBeforeSending(t *testing.T) {
	server := &streamServer{streamExists: true}
	session := runSession(t, server)

	_, err := session.Append(context.Background(), wire.AgentEnvelope{
		Kind: wire.KindEvent, Conversation: laser.NewConversationID(), Source: agentID(t, "auditor"),
	})

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) || len(server.sentMessages) != 0 {
		t.Errorf("want the append refused without a send, got %v and %d records", err, len(server.sentMessages))
	}
}

func TestGivenTagsWhenASessionBeginsThenShouldWriteThemOnTheStart(t *testing.T) {
	server := &streamServer{streamExists: true}
	base := runSession(t, server).Tag("nightly")
	tagged := base.Tag("eu")

	if _, _, err := tagged.Begin(context.Background()); err != nil {
		t.Fatalf("begin: %v", err)
	}

	var start wire.SessionStart
	if err := wire.Unmarshal(sentEnvelope(t, server).Body, &start); err != nil {
		t.Fatalf("decode the start: %v", err)
	}
	if len(start.Tags) != 2 || start.Tags[0] != "nightly" || start.Tags[1] != "eu" {
		t.Errorf("want both tags in order, got %v", start.Tags)
	}
}

func TestGivenAncestryWhenASessionBeginsThenShouldCarryItOnTheStartAndTheEnvelope(t *testing.T) {
	server := &streamServer{streamExists: true}
	parent, root := laser.NewConversationID(), laser.NewConversationID()
	child := runSession(t, server).WithAncestry(parent, root)

	if _, _, err := child.Begin(context.Background()); err != nil {
		t.Fatalf("begin a child session: %v", err)
	}

	envelope := sentEnvelope(t, server)
	var start wire.SessionStart
	if err := wire.Unmarshal(envelope.Body, &start); err != nil {
		t.Fatalf("decode the start: %v", err)
	}
	if start.Parent == nil || *start.Parent != parent || start.Root == nil || *start.Root != root {
		t.Errorf("want the ancestry on the start, got parent %v root %v", start.Parent, start.Root)
	}
	if envelope.Parent == nil || *envelope.Parent != parent || envelope.Root == nil || *envelope.Root != root {
		t.Errorf("want the same ancestry on the envelope, got parent %v root %v", envelope.Parent, envelope.Root)
	}
}

func TestGivenASigningKeyWhenASessionEndsThenShouldSignItsTerminalRecord(t *testing.T) {
	server := &streamServer{streamExists: true}
	key, err := laser.SigningKeyFromSeed(make([]byte, laser.SigningSeedBytes))
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	session := runSession(t, server).SignedBy(key)

	if _, err := session.Complete(context.Background(), ""); err != nil {
		t.Fatalf("complete: %v", err)
	}

	if envelope := sentEnvelope(t, server); envelope.Signature == nil {
		t.Error("want the terminal record signed")
	}
}

func TestGivenASessionHandleWhenInspectedThenShouldReportWhatItWasOpenedWith(t *testing.T) {
	server := &streamServer{streamExists: true}
	sessions := streamHandle(server, laser.Options{DefaultStream: "agents"}).
		SessionsWith(laser.SessionConfig{Namespace: "support"})
	plain := sessions.Create("nightly")
	parent, root := laser.NewConversationID(), laser.NewConversationID()
	child := plain.As(agentID(t, "planner")).WithAncestry(parent, root)

	if _, named := plain.Agent(); named {
		t.Error("a handle that names no agent must say so")
	}
	if _, isChild := plain.Parent(); isChild {
		t.Error("a session opened without ancestry has no parent")
	}
	if _, isChild := plain.Root(); isChild {
		t.Error("a session opened without ancestry has no root")
	}
	agent, named := child.Agent()
	gotParent, hasParent := child.Parent()
	gotRoot, hasRoot := child.Root()
	if !named || agent != agentID(t, "planner") || !hasParent || gotParent != parent || !hasRoot || gotRoot != root {
		t.Errorf("want the agent and ancestry reported, got %v %v %v", agent, gotParent, gotRoot)
	}
	if child.Stream() != "agents" || child.Config().Namespace != "support" {
		t.Errorf("want the stream and configuration reported, got %q and %+v", child.Stream(), child.Config())
	}
	if child.Scope().Conversation() != child.Conversation() {
		t.Error("want the scope of this session's conversation")
	}
}

func TestGivenAnExplicitNamespaceWhenSessionMemoryIsOpenedThenShouldScopeItToTheSession(t *testing.T) {
	server := &streamServer{streamExists: true}
	session := runSession(t, server)

	scope := session.MemoryIn("notes").Scope()

	if scope.Conversation == nil || *scope.Conversation != session.Conversation() {
		t.Errorf("want the memory scoped to the session, got %+v", scope)
	}
	if session.Graph("fleet") == nil {
		t.Error("want the named graph")
	}
}

func TestGivenAnIndexedSessionWhenItsStatusIsReadThenShouldAskTheIndexForThisSession(t *testing.T) {
	server := &sessionServer{outcome: wire.SessionOutcome{
		Kind: wire.SessionInfoOutcome,
		Info: &wire.SessionInfo{Stream: "agents", Status: wire.SessionActive},
	}}
	session := sessionHandle(server, laser.Options{DefaultStream: "agents"}).
		Sessions().Open(conversation(7), "")

	info, err := session.Status(context.Background())
	if err != nil {
		t.Fatalf("status: %v", err)
	}

	var request wire.SessionGetRequest
	decodeSessionRequest(t, server, 0, &request)
	want := wire.SessionGetRequest{Stream: "agents", ID: conversation(7)}
	if request != want || server.codes[0] != wire.AgdxSessionGetCode || info.Status != wire.SessionActive {
		t.Errorf("want a get for this session answered active, got %+v code %d status %q",
			request, server.codes[0], info.Status)
	}
}

func runSession(t *testing.T, server *streamServer) *laser.Session {
	t.Helper()
	return streamHandle(server, laser.Options{DefaultStream: "agents"}).
		Sessions().Create("nightly").As(agentID(t, "planner"))
}

// lastEnvelope reads back the newest envelope that reached the server.
func lastEnvelope(t *testing.T, server *streamServer) wire.AgentEnvelope {
	t.Helper()
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if len(server.sentMessages) == 0 {
		t.Fatal("nothing was sent")
	}
	var envelope wire.AgentEnvelope
	if err := wire.Unmarshal(server.sentMessages[len(server.sentMessages)-1].Payload, &envelope); err != nil {
		t.Fatalf("decode the envelope: %v", err)
	}
	return envelope
}

func lastTaskState(t *testing.T, server *streamServer) wire.TaskState {
	t.Helper()
	envelope := lastEnvelope(t, server)
	if envelope.TaskState == nil {
		t.Fatal("the last record carries no task state")
	}
	return *envelope.TaskState
}

func lastSessionEnd(t *testing.T, server *streamServer) wire.SessionEnd {
	t.Helper()
	var end wire.SessionEnd
	if err := wire.Unmarshal(lastEnvelope(t, server).Body, &end); err != nil {
		t.Fatalf("decode the end: %v", err)
	}
	return end
}
