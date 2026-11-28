package laser_test

import (
	"context"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenAPauseThenAResumeWhenControlIsReadOutsideAHandlerThenShouldReportNoPause(t *testing.T) {
	server := newScope()
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 31})}
	putControl(t, server, conversation, wire.OperationSessionPause, "")
	putControl(t, server, conversation, wire.OperationSessionResume, "")
	session := scopeHandle(server).Sessions().Open(conversation, "").As(agentID(t, "worker"))

	pending, err := session.PendingControl(context.Background())
	if err != nil {
		t.Fatalf("pending control: %v", err)
	}

	if pending != (laser.PendingControl{}) {
		t.Errorf("want a lifted pause to leave nothing pending, got %+v", pending)
	}
}

func TestGivenACancelRequestWhenReadOutsideAHandlerThenShouldReportIt(t *testing.T) {
	server := newScope()
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 32})}
	putControl(t, server, conversation, wire.OperationSessionPause, "")
	putControl(t, server, conversation, wire.OperationSessionCancel, "")
	session := scopeHandle(server).Sessions().Open(conversation, "").As(agentID(t, "worker"))

	pending, err := session.PendingControl(context.Background())
	if err != nil {
		t.Fatalf("pending control: %v", err)
	}
	canceled, err := session.CancelRequested(context.Background())
	if err != nil {
		t.Fatalf("cancel requested: %v", err)
	}

	want := laser.PendingControl{PauseRequested: true, CancelRequested: true}
	if pending != want || !canceled {
		t.Errorf("want %+v and a cancel, got %+v and %v", want, pending, canceled)
	}
}

func TestGivenARequestForAnotherAgentWhenControlIsReadThenShouldNotApplyIt(t *testing.T) {
	server := newScope()
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 33})}
	putControl(t, server, conversation, wire.OperationSessionCancel, "reviewer")
	session := scopeHandle(server).Sessions().Open(conversation, "").As(agentID(t, "worker"))

	canceled, err := session.CancelRequested(context.Background())
	if err != nil {
		t.Fatalf("cancel requested: %v", err)
	}

	if canceled {
		t.Error("a cancel addressed to another agent is not this agent's to read")
	}
}

func TestGivenNoControlTopicWhenControlIsReadThenShouldReportNothingPending(t *testing.T) {
	server := newScope()
	server.missing = true
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 34})}
	session := scopeHandle(server).Sessions().Open(conversation, "").As(agentID(t, "worker"))

	pending, err := session.PendingControl(context.Background())
	if err != nil {
		t.Fatalf("pending control: %v", err)
	}

	if pending != (laser.PendingControl{}) {
		t.Errorf("want nothing pending without a control topic, got %+v", pending)
	}
}

func TestGivenSkippedSourceRegistrationWhenBootstrappedThenShouldPublishNoRegistration(t *testing.T) {
	capabilities := &laser.Capabilities{Managed: true, Sessions: true}
	registering, skipping := &routingServer{}, &routingServer{}
	registered := streamHandle(registering, laser.Options{DefaultStream: "agents", Capabilities: capabilities}).
		Sessions()
	skipped := streamHandle(skipping, laser.Options{DefaultStream: "agents", Capabilities: capabilities}).
		SessionsWith(laser.SessionConfig{SkipSourceRegistration: true})

	if _, err := registered.Bootstrap(context.Background(), 2, laser.ExpireAfter(time.Hour)); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	report, err := skipped.Bootstrap(context.Background(), 2, laser.ExpireAfter(time.Hour))
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	if codes := registering.managedCodes(); len(codes) == 0 {
		t.Fatal("want the default bootstrap to publish the registration")
	}
	if codes := skipping.managedCodes(); len(codes) != 0 || report.Registered {
		t.Errorf("want no registration published when skipped, got codes %v and registered=%v",
			codes, report.Registered)
	}
}

// putControl appends one control command of a session to the fake's only feed.
func putControl(
	t *testing.T, server *scopeServer, conversation wire.ConversationID, operation, target string,
) {
	t.Helper()
	record := wire.RecordID{ID: wire.IDFromBytes(laser.NewOperationID())}
	envelope := wire.AgentEnvelope{
		Kind: wire.KindCommand, Record: &record, Conversation: conversation,
		Source: agentID(t, "operator"), Operation: &operation, Body: []byte("{}"),
	}
	if target != "" {
		addressee := agentID(t, target)
		envelope.Target = &addressee
	}
	putEnvelope(t, server, envelope)
}

// putEnvelope appends one envelope at the next offset of partition 0.
func putEnvelope(t *testing.T, server *scopeServer, envelope wire.AgentEnvelope) {
	t.Helper()
	encoded, err := wire.Marshal(envelope)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	headers, err := laser.EncodeHeaderBlock(wire.HeadersForEnvelope(&envelope, wire.ContentJSON, true))
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
