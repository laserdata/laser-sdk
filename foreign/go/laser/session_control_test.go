package laser_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// controlServer records what reached agent.control and answers lane reads.
type controlServer struct {
	clusterOfOne
	mutex sync.Mutex
	// lane is what a read of agent.sessions returns.
	lane []iggcon.IggyMessage
	// sent is every record that reached the log, with the topic it rode.
	sent   []iggcon.IggyMessage
	topics []string
}

func (server *controlServer) Close() error { return nil }

func (server *controlServer) GetStream(
	_ context.Context, _ iggcon.Identifier,
) (*iggcon.StreamDetails, error) {
	return &iggcon.StreamDetails{}, nil
}

func (server *controlServer) GetTopic(
	_ context.Context, _, _ iggcon.Identifier,
) (*iggcon.TopicDetails, error) {
	return &iggcon.TopicDetails{
		Topic:      iggcon.Topic{PartitionsCount: 1},
		Partitions: []iggcon.PartitionContract{{Id: 0}},
	}, nil
}

func (server *controlServer) PollMessages(
	_ context.Context, _, _ iggcon.Identifier, _ uint32,
	_ iggcon.Consumer, _ iggcon.PollingStrategy, _ iggcon.PollOptions,
) (*iggcon.PolledMessage, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	messages := server.lane
	server.lane = nil
	return &iggcon.PolledMessage{Messages: messages}, nil
}

func (server *controlServer) SendMessages(
	_ context.Context, _, topicID iggcon.Identifier,
	_ iggcon.Partitioning, messages []iggcon.IggyMessage,
) (*iggcon.SendMessagesResponse, error) {
	name, err := topicID.String()
	if err != nil {
		return nil, err
	}
	server.mutex.Lock()
	defer server.mutex.Unlock()
	for range messages {
		server.topics = append(server.topics, name)
	}
	server.sent = append(server.sent, messages...)
	return &iggcon.SendMessagesResponse{}, nil
}

// controlled reads back the envelope that reached agent.control.
func (server *controlServer) controlled(t *testing.T) wire.AgentEnvelope {
	t.Helper()
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if len(server.sent) != 1 {
		t.Fatalf("want one record, got %d", len(server.sent))
	}
	if server.topics[0] != wire.AgentControl {
		t.Fatalf("the record rode %q, want the control topic", server.topics[0])
	}
	var envelope wire.AgentEnvelope
	if err := wire.Unmarshal(server.sent[0].Payload, &envelope); err != nil {
		t.Fatalf("decode the envelope: %v", err)
	}
	return envelope
}

// laneRecord builds one record of the session lane.
func laneRecord(
	t *testing.T, session wire.ConversationID, envelope wire.AgentEnvelope,
) iggcon.IggyMessage {
	t.Helper()
	payload, err := wire.Marshal(envelope)
	if err != nil {
		t.Fatalf("encode the envelope: %v", err)
	}
	provenance := &laser.Provenance{Conversation: session}
	if envelope.Source.String() != "" {
		source := envelope.Source
		provenance.Agent = &source
	}
	headers, err := provenance.Headers()
	if err != nil {
		t.Fatalf("encode the provenance: %v", err)
	}
	message, err := iggcon.NewIggyMessage(payload, iggcon.WithUserHeaders(headers))
	if err != nil {
		t.Fatalf("build the record: %v", err)
	}
	return message
}

func workCommand(
	t *testing.T, session wire.ConversationID, operation, target string,
) iggcon.IggyMessage {
	t.Helper()
	addressee := agentID(t, target)
	return laneRecord(t, session, wire.AgentEnvelope{
		Kind:      wire.KindCommand,
		Source:    agentID(t, "planner"),
		Target:    &addressee,
		Operation: &operation,
	})
}

func transitionRecord(
	t *testing.T, session wire.ConversationID, actor string,
) iggcon.IggyMessage {
	t.Helper()
	acting := agentID(t, actor)
	body, err := wire.Marshal(wire.SessionTransition{Actor: &acting})
	if err != nil {
		t.Fatalf("encode the transition: %v", err)
	}
	operation := wire.OperationSession
	return laneRecord(t, session, wire.AgentEnvelope{
		Kind:      wire.KindStatus,
		Source:    acting,
		Operation: &operation,
		Body:      body,
	})
}

func controlHandle(server *controlServer) *laser.Laser {
	return laser.FromClient(server, laser.Options{DefaultStream: "agents"})
}

func TestGivenAnOperatorWhenCancellingThenShouldWriteToTheControlTopic(t *testing.T) {
	server := &controlServer{}
	control := controlHandle(server).Sessions().
		Control("agents", conversation(1)).AsOperator(agentID(t, "ops"))

	if _, err := control.Cancel(context.Background()); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	envelope := server.controlled(t)
	if envelope.Kind != wire.KindCommand {
		t.Fatalf("a control request is a command, got %q", envelope.Kind)
	}
	if envelope.Operation == nil || *envelope.Operation != wire.OperationSessionCancel {
		t.Fatalf("got %v", envelope.Operation)
	}
	if envelope.Source != agentID(t, "ops") {
		t.Fatalf("the operator must be named, got %v", envelope.Source)
	}
	if envelope.Conversation != conversation(1) {
		t.Fatalf("got %v", envelope.Conversation)
	}
	// A reader folds it as a cancel, which is the whole point of writing it.
	parsed, made := laser.ControlRequestOf(&envelope, nil)
	if !made || parsed.Action() != "cancel" {
		t.Fatalf("got %+v %v", parsed, made)
	}
}

func TestGivenAnOperatorWhenResumingThenShouldWriteAResume(t *testing.T) {
	server := &controlServer{}
	control := controlHandle(server).Sessions().
		Control("agents", conversation(1)).AsOperator(agentID(t, "ops"))

	if _, err := control.Resume(context.Background()); err != nil {
		t.Fatalf("resume: %v", err)
	}
	envelope := server.controlled(t)
	parsed, made := laser.ControlRequestOf(&envelope, nil)
	if !made || parsed.Action() != "resume" {
		t.Fatalf("got %+v %v", parsed, made)
	}
}

func TestGivenNamedParticipantsWhenPausingThenShouldCarryThem(t *testing.T) {
	server := &controlServer{}
	control := controlHandle(server).Sessions().
		Control("agents", conversation(1)).
		AsOperator(agentID(t, "ops")).
		Participants(agentID(t, "executor"), agentID(t, "auditor"))

	if _, err := control.Pause(context.Background()); err != nil {
		t.Fatalf("pause: %v", err)
	}

	envelope := server.controlled(t)
	var request wire.SessionPauseRequest
	if err := wire.UnmarshalJSON(envelope.Body, &request); err != nil {
		t.Fatalf("decode the pause: %v", err)
	}
	if len(request.Participants) != 2 ||
		request.Participants[0] != agentID(t, "executor") ||
		request.Participants[1] != agentID(t, "auditor") {
		t.Fatalf("got %v", request.Participants)
	}
	// A named participant reads itself as named; anyone else still pauses.
	executor := agentID(t, "executor")
	parsed, made := laser.ControlRequestOf(&envelope, &executor)
	if !made || parsed.Action() != "pause" || !parsed.Named {
		t.Fatalf("got %+v %v", parsed, made)
	}
	stranger := agentID(t, "stranger")
	parsed, made = laser.ControlRequestOf(&envelope, &stranger)
	if !made || parsed.Named {
		t.Fatalf("got %+v %v", parsed, made)
	}
}

func TestGivenNoNamedParticipantsWhenPausingThenShouldReadThemOffTheLane(t *testing.T) {
	session := conversation(1)
	server := &controlServer{lane: []iggcon.IggyMessage{
		workCommand(t, session, "do_the_thing", "executor"),
		transitionRecord(t, session, "auditor"),
		// A model call is the session talking to something that is not an
		// agent, so it names no participant.
		workCommand(t, session, wire.OperationChat, "a-model"),
		// So is an operator's own control request.
		workCommand(t, session, wire.OperationSessionPause, "ops"),
	}}
	control := controlHandle(server).Sessions().
		Control("agents", session).AsOperator(agentID(t, "ops"))

	if _, err := control.Pause(context.Background()); err != nil {
		t.Fatalf("pause: %v", err)
	}

	envelope := server.controlled(t)
	var request wire.SessionPauseRequest
	if err := wire.UnmarshalJSON(envelope.Body, &request); err != nil {
		t.Fatalf("decode the pause: %v", err)
	}
	// Alphabetical, so two operators reading one lane write the same request.
	if len(request.Participants) != 2 ||
		request.Participants[0] != agentID(t, "auditor") ||
		request.Participants[1] != agentID(t, "executor") {
		t.Fatalf("got %v", request.Participants)
	}
}

func TestGivenAnEmptyParticipantSetWhenPausingThenShouldKeepItEmpty(t *testing.T) {
	// A caller that named nobody meant nobody, and must not have the lane
	// read behind its back.
	session := conversation(1)
	server := &controlServer{lane: []iggcon.IggyMessage{
		workCommand(t, session, "do_the_thing", "executor"),
	}}
	control := controlHandle(server).Sessions().
		Control("agents", session).AsOperator(agentID(t, "ops")).Participants()

	if _, err := control.Pause(context.Background()); err != nil {
		t.Fatalf("pause: %v", err)
	}
	envelope := server.controlled(t)
	var request wire.SessionPauseRequest
	if err := wire.UnmarshalJSON(envelope.Body, &request); err != nil {
		t.Fatalf("decode the pause: %v", err)
	}
	if len(request.Participants) != 0 {
		t.Fatalf("got %v", request.Participants)
	}
}

func TestGivenAnotherConversationsRecordsWhenReadThenShouldNotBeParticipants(t *testing.T) {
	session := conversation(1)
	server := &controlServer{lane: []iggcon.IggyMessage{
		workCommand(t, session, "do_the_thing", "executor"),
		workCommand(t, conversation(2), "do_the_thing", "stranger"),
	}}
	control := controlHandle(server).Sessions().
		Control("agents", session).AsOperator(agentID(t, "ops"))

	participants, err := control.LaneParticipants(context.Background())
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	// A topic carries many conversations, so reading one and filtering is
	// the normal path.
	if len(participants) != 1 || participants[0] != agentID(t, "executor") {
		t.Fatalf("got %v", participants)
	}
}

func TestGivenAForcedCancelWhenSentThenShouldEndTheSessionItself(t *testing.T) {
	server := &controlServer{}
	control := controlHandle(server).Sessions().
		Control("agents", conversation(1)).AsOperator(agentID(t, "ops"))

	if _, err := control.ForceCancel(context.Background()); err != nil {
		t.Fatalf("force: %v", err)
	}

	envelope := server.controlled(t)
	// A cancel asks and a force tells, so this is a terminal status rather
	// than a command waiting for an agent to agree.
	if envelope.Kind != wire.KindStatus {
		t.Fatalf("got %q", envelope.Kind)
	}
	if envelope.TaskState == nil || *envelope.TaskState != wire.TaskCanceled {
		t.Fatalf("got %v", envelope.TaskState)
	}
	var end wire.SessionEnd
	if err := wire.Unmarshal(envelope.Body, &end); err != nil {
		t.Fatalf("decode the end: %v", err)
	}
	if end.Reason == nil || *end.Reason != "forced" {
		t.Fatalf("got %v", end.Reason)
	}
	// A reader folds it as a cancel the same way it folds the command.
	parsed, made := laser.ControlRequestOf(&envelope, nil)
	if !made || parsed.Action() != "cancel" {
		t.Fatalf("got %+v %v", parsed, made)
	}
}

func TestGivenNoOperatorWhenControllingThenShouldRefuseTheRequest(t *testing.T) {
	server := &controlServer{}
	control := controlHandle(server).Sessions().Control("agents", conversation(1))

	for name, call := range map[string]func() error{
		"pause":  func() error { _, err := control.Pause(context.Background()); return err },
		"resume": func() error { _, err := control.Resume(context.Background()); return err },
		"cancel": func() error { _, err := control.Cancel(context.Background()); return err },
		"force": func() error {
			_, err := control.ForceCancel(context.Background())
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := call()
			if err == nil || !strings.Contains(err.Error(), "AsOperator") {
				t.Fatalf("got %v, want the operator named", err)
			}
		})
	}
	if len(server.sent) != 0 {
		t.Fatal("nothing must be sent")
	}
}

func TestGivenASigningKeyWhenControllingThenShouldSignTheRecord(t *testing.T) {
	server := &controlServer{}
	key, err := laser.SigningKeyFromSeed(make([]byte, laser.SigningSeedBytes))
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	control := controlHandle(server).Sessions().
		Control("agents", conversation(1)).
		AsOperator(agentID(t, "ops")).SignedBy(key)

	if _, err := control.Cancel(context.Background()); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	// A verifying reader can prove which operator sent it, which an
	// unsigned record cannot support.
	if envelope := server.controlled(t); envelope.Signature == nil {
		t.Fatal("the record must be signed")
	}
}

func TestGivenNoStreamWhenControllingThenShouldSaySo(t *testing.T) {
	server := &controlServer{}
	control := laser.FromClient(server, laser.Options{}).Sessions().
		Control("", conversation(1)).AsOperator(agentID(t, "ops"))

	if _, err := control.Cancel(context.Background()); err == nil {
		t.Fatal("a control record needs a stream")
	}
}

func TestGivenAControlHandleWhenDerivedThenShouldNotChangeTheOriginal(t *testing.T) {
	server := &controlServer{}
	base := controlHandle(server).Sessions().Control("agents", conversation(1))
	withOperator := base.AsOperator(agentID(t, "ops"))

	// The original named no operator, so it must still refuse.
	if _, err := base.Cancel(context.Background()); err == nil {
		t.Fatal("the original handle must be unchanged")
	}
	if _, err := withOperator.Cancel(context.Background()); err != nil {
		t.Fatalf("the derived handle controls: %v", err)
	}
}
