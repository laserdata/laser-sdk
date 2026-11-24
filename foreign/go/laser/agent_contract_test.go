package laser_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// contractServer is a one-partition log a contract publishes its command to
// and polls back for the reply, which is all Send's live wait needs: the
// poll is the behaviour under test, not the transport under it.
type contractServer struct {
	clusterOfOne
	mutex   sync.Mutex
	records []iggcon.IggyMessage
	noTopic bool

	// replyKind, when set, auto-answers every published command inline, the
	// way a target agent would. Empty sends no reply, for the timed-out case.
	replyKind   wire.AgentKind
	replyBody   []byte
	replySource wire.AgentID
	// repliesByTarget overrides replyKind/replyBody/replySource for a
	// specific command target (by its agent id string), for a scatter test
	// where more than one agent answers differently.
	repliesByTarget map[string]contractReply
	// silent lists command targets that are never answered, even when
	// replyKind is set, for a scatter branch that should time out among
	// others that complete.
	silent map[string]bool
}

// contractReply is one target's canned answer, overriding contractServer's
// own default reply shape.
type contractReply struct {
	kind   wire.AgentKind
	body   []byte
	source wire.AgentID
}

func (server *contractServer) Close() error                  { return nil }
func (server *contractServer) Connect(context.Context) error { return nil }

func (server *contractServer) GetTopic(
	_ context.Context, _, _ iggcon.Identifier,
) (*iggcon.TopicDetails, error) {
	if server.noTopic {
		return nil, nil
	}
	details := &iggcon.TopicDetails{}
	details.PartitionsCount = 1
	return details, nil
}

func (server *contractServer) push(message iggcon.IggyMessage) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	message.Header.Offset = uint64(len(server.records))
	server.records = append(server.records, message)
}

// SendMessages records the command and, when replyKind is set, synchronously
// answers it, so Send's poll always has something to find without a real
// agent on the other end.
func (server *contractServer) SendMessages(
	_ context.Context, _, _ iggcon.Identifier,
	_ iggcon.Partitioning, messages []iggcon.IggyMessage,
) (*iggcon.SendMessagesResponse, error) {
	for index := range messages {
		server.push(messages[index])
	}
	for _, message := range messages {
		var command wire.AgentEnvelope
		if err := wire.Unmarshal(message.Payload, &command); err != nil ||
			command.Kind != wire.KindCommand || command.Correlation == nil {
			continue
		}
		target := ""
		if command.Target != nil {
			target = command.Target.String()
		}
		if server.silent[target] {
			continue
		}
		answer := contractReply{
			kind: server.replyKind, body: server.replyBody, source: server.replySource,
		}
		if override, has := server.repliesByTarget[target]; has {
			answer = override
		}
		if answer.kind == "" {
			continue
		}
		reply := wire.AgentEnvelope{
			Kind:         answer.kind,
			Record:       &wire.RecordID{ID: wire.IDFromBytes(laser.NewOperationID())},
			Conversation: command.Conversation,
			Source:       answer.source,
			Target:       &command.Source,
			Correlation:  command.Correlation,
			Body:         answer.body,
		}
		payload, err := wire.Marshal(&reply)
		if err != nil {
			continue
		}
		headers, err := laser.EncodeHeaderBlock(
			wire.HeadersForEnvelope(&reply, wire.ContentCBOR, false))
		if err != nil {
			continue
		}
		replyMessage, err := iggcon.NewIggyMessage(payload, iggcon.WithUserHeaders(headers))
		if err != nil {
			continue
		}
		server.push(replyMessage)
	}
	return &iggcon.SendMessagesResponse{}, nil
}

func (server *contractServer) PollMessages(
	_ context.Context, _, _ iggcon.Identifier, _ uint32,
	_ iggcon.Consumer, strategy iggcon.PollingStrategy, _ iggcon.PollOptions,
) (*iggcon.PolledMessage, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	from := uint64(0)
	if strategy.Kind == iggcon.POLLING_OFFSET {
		from = strategy.Value
	}
	if from >= uint64(len(server.records)) {
		return &iggcon.PolledMessage{}, nil
	}
	return &iggcon.PolledMessage{
		Messages: append([]iggcon.IggyMessage(nil), server.records[from:]...),
	}, nil
}

func (server *contractServer) stored() []iggcon.IggyMessage {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	return append([]iggcon.IggyMessage(nil), server.records...)
}

func notConsumedBody(t *testing.T) []byte {
	t.Helper()
	body, err := wire.Marshal(wire.AgentErrorBody{
		Code: wire.ErrorDeadlineExceeded,
		Detail: map[string]wire.Value{
			laser.DeadLetterReasonDetail: wire.UintValue(uint64(wire.DeadLetterDeadlineExceeded)),
		},
	})
	if err != nil {
		t.Fatalf("encode the dead-letter body: %v", err)
	}
	return body
}

func TestGivenACompletedReplyWhenContractSentThenShouldReportCompleted(t *testing.T) {
	server := &contractServer{replyKind: wire.KindResponse, replyBody: []byte("done"), replySource: agentID(t, "worker")}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	contract, err := handle.Contract(laser.To(agentID(t, "worker")), "agent.sessions").
		From(agentID(t, "orchestrator")).
		Payload([]byte(`{"task":"run"}`)).
		Deadline(2 * time.Second).
		Send(context.Background())
	if err != nil {
		t.Fatalf("send the contract: %v", err)
	}
	if contract.State != laser.ContractCompleted {
		t.Fatalf("want completed, got %v", contract.State)
	}
	if contract.Reply == nil {
		t.Fatal("want the completing reply kept")
	}
}

func TestGivenAnErrorReplyWhenContractSentThenShouldReportFailed(t *testing.T) {
	message := "the task failed"
	body, err := wire.Marshal(wire.AgentErrorBody{Code: wire.ErrorInternal, Message: &message})
	if err != nil {
		t.Fatalf("encode the error body: %v", err)
	}
	server := &contractServer{
		replyKind: wire.KindError, replyBody: body, replySource: agentID(t, "worker"),
	}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	contract, err := handle.Contract(laser.To(agentID(t, "worker")), "agent.sessions").
		From(agentID(t, "orchestrator")).
		Payload([]byte(`{"task":"run"}`)).
		Deadline(2 * time.Second).
		Send(context.Background())
	if err != nil {
		t.Fatalf("send the contract: %v", err)
	}
	if contract.State != laser.ContractFailed {
		t.Fatalf("want failed, got %v", contract.State)
	}
}

func TestGivenADeadLetteredReplyWhenContractSentThenShouldReportNotConsumed(t *testing.T) {
	server := &contractServer{
		replyKind: wire.KindError, replyBody: notConsumedBody(t), replySource: agentID(t, "worker"),
	}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	contract, err := handle.Contract(laser.To(agentID(t, "worker")), "agent.sessions").
		From(agentID(t, "orchestrator")).
		Payload([]byte(`{"task":"run"}`)).
		ExpireIfNotConsumed(500 * time.Millisecond).
		Deadline(2 * time.Second).
		Send(context.Background())
	if err != nil {
		t.Fatalf("send the contract: %v", err)
	}
	if contract.State != laser.ContractNotConsumed {
		t.Fatalf("want not consumed, got %v", contract.State)
	}
}

func TestGivenNoReplyWhenContractSentThenShouldReportTimedOut(t *testing.T) {
	server := &contractServer{}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	contract, err := handle.Contract(laser.To(agentID(t, "worker")), "agent.sessions").
		From(agentID(t, "orchestrator")).
		Payload([]byte(`{"task":"run"}`)).
		Deadline(300 * time.Millisecond).
		Send(context.Background())
	if err != nil {
		t.Fatalf("send the contract: %v", err)
	}
	if contract.State != laser.ContractTimedOut {
		t.Fatalf("want timed out, got %v", contract.State)
	}
}

func TestGivenNoSourceWhenContractSentThenShouldRefuseBeforeResolving(t *testing.T) {
	handle := laser.FromClient(&contractServer{}, laser.Options{DefaultStream: "agents"})

	_, err := handle.Contract(laser.To(agentID(t, "worker")), "agent.sessions").
		Payload([]byte(`{"task":"run"}`)).
		Send(context.Background())

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an invalid-argument refusal, got %T: %v", err, err)
	}
}

func TestGivenABroadcastRouteWhenContractSentThenShouldRefuseIt(t *testing.T) {
	handle := laser.FromClient(&contractServer{}, laser.Options{DefaultStream: "agents"})

	_, err := handle.Contract(laser.Broadcast(), "agent.sessions").
		From(agentID(t, "orchestrator")).
		Payload([]byte(`{"task":"run"}`)).
		Send(context.Background())

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want a directed-route refusal, got %T: %v", err, err)
	}
}

func TestGivenACapabilityRouteWhenContractSentThenShouldResolveAgainstTheRegistry(t *testing.T) {
	registry := registryWith(t, time.Now(), map[string]wire.AgentCard{
		"worker": cardFor("review", nil, nil, nil, nil),
	})
	server := &contractServer{replyKind: wire.KindResponse, replyBody: []byte("done"), replySource: agentID(t, "worker")}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	contract, err := handle.Contract(laser.ToCapable("review", laser.RouteAny), "agent.sessions").
		From(agentID(t, "orchestrator")).
		Payload([]byte(`{"task":"run"}`)).
		Registry(registry).
		Deadline(2 * time.Second).
		Send(context.Background())
	if err != nil {
		t.Fatalf("send the contract: %v", err)
	}
	if contract.State != laser.ContractCompleted {
		t.Fatalf("want completed, got %v", contract.State)
	}

	stored := server.stored()
	var command wire.AgentEnvelope
	if err := wire.Unmarshal(stored[0].Payload, &command); err != nil {
		t.Fatalf("decode the command: %v", err)
	}
	if command.Target == nil || command.Target.String() != "worker" {
		t.Errorf("want the command addressed to the resolved agent, got %v", command.Target)
	}
}

func TestGivenAnAncestryWhenContractCompletesThenShouldEndTheChildCompleted(t *testing.T) {
	server := &contractServer{replyKind: wire.KindResponse, replyBody: []byte("done"), replySource: agentID(t, "worker")}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	parent, root := laser.NewConversationID(), laser.NewConversationID()

	contract, err := handle.Contract(laser.To(agentID(t, "worker")), "agent.sessions").
		From(agentID(t, "orchestrator")).
		Payload([]byte(`{"task":"run"}`)).
		Parent(parent, root).
		Deadline(2 * time.Second).
		Send(context.Background())
	if err != nil {
		t.Fatalf("send the contract: %v", err)
	}
	if contract.State != laser.ContractCompleted {
		t.Fatalf("want completed, got %v", contract.State)
	}

	stored := server.stored()
	var start wire.AgentEnvelope
	if err := wire.Unmarshal(stored[0].Payload, &start); err != nil {
		t.Fatalf("decode the start: %v", err)
	}
	if start.TaskState == nil || *start.TaskState != wire.TaskSubmitted {
		t.Errorf("want a submitted session status first, got %v", start.TaskState)
	}
	var command wire.AgentEnvelope
	if err := wire.Unmarshal(stored[1].Payload, &command); err != nil {
		t.Fatalf("decode the command: %v", err)
	}
	if command.Parent == nil || *command.Parent != parent ||
		command.Root == nil || *command.Root != root {
		t.Errorf("want the command to carry the ancestry, got parent=%v root=%v",
			command.Parent, command.Root)
	}
	ended := stored[len(stored)-1]
	var end wire.AgentEnvelope
	if err := wire.Unmarshal(ended.Payload, &end); err != nil {
		t.Fatalf("decode the end: %v", err)
	}
	if end.TaskState == nil || *end.TaskState != wire.TaskCompleted {
		t.Errorf("want the child ended completed, got %v", end.TaskState)
	}
}

func TestGivenAnAncestryWhenContractFailsThenShouldEndTheChildFailed(t *testing.T) {
	message := "the task failed"
	body, err := wire.Marshal(wire.AgentErrorBody{Code: wire.ErrorInternal, Message: &message})
	if err != nil {
		t.Fatalf("encode the error body: %v", err)
	}
	server := &contractServer{
		replyKind: wire.KindError, replyBody: body, replySource: agentID(t, "worker"),
	}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	parent, root := laser.NewConversationID(), laser.NewConversationID()

	contract, err := handle.Contract(laser.To(agentID(t, "worker")), "agent.sessions").
		From(agentID(t, "orchestrator")).
		Payload([]byte(`{"task":"run"}`)).
		Parent(parent, root).
		Deadline(2 * time.Second).
		Send(context.Background())
	if err != nil {
		t.Fatalf("send the contract: %v", err)
	}
	if contract.State != laser.ContractFailed {
		t.Fatalf("want failed, got %v", contract.State)
	}

	stored := server.stored()
	ended := stored[len(stored)-1]
	var end wire.AgentEnvelope
	if err := wire.Unmarshal(ended.Payload, &end); err != nil {
		t.Fatalf("decode the end: %v", err)
	}
	if end.TaskState == nil || *end.TaskState != wire.TaskFailed {
		t.Errorf("want the child ended failed, got %v", end.TaskState)
	}
}
