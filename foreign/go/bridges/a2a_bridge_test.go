package bridges_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/laserdata/laser-sdk/foreign/go/bridges"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// bridgeLog is a one-partition log the bridge publishes to and reads back,
// which is all a task lookup needs: the scan is the behaviour under test, not
// the transport under it.
type bridgeLog struct {
	iggcon.Client
	guard   sync.Mutex
	records []iggcon.IggyMessage
	polls   int
	noTopic bool
}

func (log *bridgeLog) Close() error                  { return nil }
func (log *bridgeLog) Connect(context.Context) error { return nil }

func (log *bridgeLog) SendBinaryRequest(
	context.Context, uint32, []byte,
) ([]byte, error) {
	return nil, ierror.InvalidCommand{}
}

func (log *bridgeLog) GetClusterMetadata(
	context.Context,
) (*iggcon.ClusterMetadata, error) {
	return &iggcon.ClusterMetadata{Nodes: []iggcon.ClusterNode{{ReplicaID: 0}}}, nil
}

func (log *bridgeLog) SendMessages(
	_ context.Context, _, _ iggcon.Identifier,
	_ iggcon.Partitioning, messages []iggcon.IggyMessage,
) (*iggcon.SendMessagesResponse, error) {
	log.guard.Lock()
	defer log.guard.Unlock()
	for index := range messages {
		message := messages[index]
		message.Header.Offset = uint64(len(log.records))
		log.records = append(log.records, message)
	}
	return &iggcon.SendMessagesResponse{}, nil
}

// GetStream answers the bridge's stream, whose creation time a bound task
// id is tied to.
func (log *bridgeLog) GetStream(
	_ context.Context, _ iggcon.Identifier,
) (*iggcon.StreamDetails, error) {
	// The session topic is listed because a session write checks the lane's
	// identity before it sends, and a stream with no agent.sessions reads as
	// a lane that is not there.
	return &iggcon.StreamDetails{
		Stream: iggcon.Stream{Id: 1, Name: "agents", CreatedAt: 1_700_000_000_000_000},
		Topics: []iggcon.Topic{{
			Id: 1, Name: wire.AgentSessions, CreatedAt: 1, PartitionsCount: 1,
		}},
	}, nil
}

func (log *bridgeLog) GetTopic(
	_ context.Context, _, _ iggcon.Identifier,
) (*iggcon.TopicDetails, error) {
	if log.noTopic {
		return nil, nil
	}
	return &iggcon.TopicDetails{Topic: iggcon.Topic{PartitionsCount: 1}}, nil
}

func (log *bridgeLog) PollMessages(
	_ context.Context, _, _ iggcon.Identifier, _ uint32,
	_ iggcon.Consumer, strategy iggcon.PollingStrategy, _ iggcon.PollOptions,
) (*iggcon.PolledMessage, error) {
	log.guard.Lock()
	defer log.guard.Unlock()
	log.polls++
	from := uint64(0)
	if strategy.Kind == iggcon.POLLING_OFFSET {
		from = strategy.Value
	}
	if from >= uint64(len(log.records)) {
		return &iggcon.PolledMessage{}, nil
	}
	return &iggcon.PolledMessage{Messages: log.records[from:]}, nil
}

// stored is every record the bridge published.
func (log *bridgeLog) stored() []iggcon.IggyMessage {
	log.guard.Lock()
	defer log.guard.Unlock()
	return append([]iggcon.IggyMessage(nil), log.records...)
}

func bridgeOver(t *testing.T, log iggcon.Client) *bridges.A2ABridge {
	t.Helper()
	handle := laser.FromClient(log, laser.Options{DefaultStream: "agents"})
	return bridges.NewA2ABridge(handle, agent(t, "edge"), "agent.sessions", "agent.sessions")
}

const sendParams = `{"message":{"parts":[{"text":"run the audit"}]}}`

func TestGivenParamsWhenSubmittedThenShouldPublishThemByteIdentical(t *testing.T) {
	log := &bridgeLog{}
	bridge := bridgeOver(t, log)

	task, err := bridge.Submit(context.Background(), []byte(sendParams))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	stored := log.stored()
	if len(stored) != 1 {
		t.Fatalf("want one command, got %d", len(stored))
	}
	var envelope wire.AgentEnvelope
	if err := wire.Unmarshal(stored[0].Payload, &envelope); err != nil {
		t.Fatalf("decode the command: %v", err)
	}
	// The foreign request rides whole, so an agent reads exactly what the
	// A2A client sent rather than this bridge's idea of it.
	if string(envelope.Body) != sendParams {
		t.Errorf("want the params byte-identical, got %q", envelope.Body)
	}
	if envelope.Kind != wire.KindCommand {
		t.Errorf("want a command, got %q", envelope.Kind)
	}
	// The correlation is derived from the task, which is what lets a later
	// lookup find the answers without holding any state.
	if envelope.Correlation == nil ||
		envelope.Correlation.String() != task.ID {
		t.Errorf("want the correlation to be the task, got %v", envelope.Correlation)
	}
	if task.Status.State != "TASK_STATE_SUBMITTED" {
		t.Errorf("want a submitted task, got %q", task.Status.State)
	}
}

func TestGivenAncestryWhenSubmittedAsAChildThenShouldWriteTheStartBeforeTheCommand(t *testing.T) {
	log := &bridgeLog{}
	bridge := bridgeOver(t, log)
	parent, root := laser.NewConversationID(), laser.NewConversationID()

	task, err := bridge.SubmitIn(context.Background(), parent, root, []byte(sendParams))
	if err != nil {
		t.Fatalf("submit in: %v", err)
	}

	stored := log.stored()
	if len(stored) != 2 {
		t.Fatalf("want a session start and a command, got %d", len(stored))
	}
	var start wire.AgentEnvelope
	if err := wire.Unmarshal(stored[0].Payload, &start); err != nil {
		t.Fatalf("decode the start: %v", err)
	}
	if start.Kind != wire.KindStatus ||
		start.TaskState == nil || *start.TaskState != wire.TaskSubmitted {
		t.Errorf("want a submitted session status, got %q state=%v", start.Kind, start.TaskState)
	}
	if start.Parent == nil || *start.Parent != parent ||
		start.Root == nil || *start.Root != root {
		t.Errorf("want the start to carry the ancestry, got parent=%v root=%v",
			start.Parent, start.Root)
	}
	var body wire.SessionStart
	if err := wire.Unmarshal(start.Body, &body); err != nil {
		t.Fatalf("decode the session start body: %v", err)
	}
	if body.Parent == nil || *body.Parent != parent ||
		body.Root == nil || *body.Root != root {
		t.Errorf("want the session start body to carry the ancestry, got %+v", body)
	}

	var command wire.AgentEnvelope
	if err := wire.Unmarshal(stored[1].Payload, &command); err != nil {
		t.Fatalf("decode the command: %v", err)
	}
	if command.Kind != wire.KindCommand {
		t.Errorf("want a command, got %q", command.Kind)
	}
	if command.Parent == nil || *command.Parent != parent ||
		command.Root == nil || *command.Root != root {
		t.Errorf("want the command to carry the ancestry, got parent=%v root=%v",
			command.Parent, command.Root)
	}
	if string(command.Body) != sendParams {
		t.Errorf("want the params byte-identical, got %q", command.Body)
	}
	if task.Status.State != "TASK_STATE_SUBMITTED" {
		t.Errorf("want a submitted task, got %q", task.Status.State)
	}
}

func TestGivenATargetWhenSubmittedInAChildThenShouldAddressOnlyThatAgent(t *testing.T) {
	log := &bridgeLog{}
	bridge := bridgeOver(t, log)
	parent, root := laser.NewConversationID(), laser.NewConversationID()
	target := agent(t, "worker")

	if _, err := bridge.SubmitInTo(
		context.Background(), target, parent, root, []byte(sendParams),
	); err != nil {
		t.Fatalf("submit in to: %v", err)
	}

	stored := log.stored()
	var command wire.AgentEnvelope
	if err := wire.Unmarshal(stored[len(stored)-1].Payload, &command); err != nil {
		t.Fatalf("decode the command: %v", err)
	}
	if command.Target == nil || *command.Target != target {
		t.Errorf("want the command addressed to the target, got %v", command.Target)
	}
}

func TestGivenNoAnswerYetWhenTheTaskIsReadThenShouldReportItWorking(t *testing.T) {
	log := &bridgeLog{}
	bridge := bridgeOver(t, log)
	task, err := bridge.Submit(context.Background(), []byte(sendParams))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	read, err := bridge.Task(context.Background(), task.ID)
	if err != nil {
		t.Fatalf("read the task: %v", err)
	}

	if read.Status.State != "TASK_STATE_WORKING" {
		t.Errorf("want a working task, got %q", read.Status.State)
	}
}

func TestGivenAnIdNoTaskAnswersToWhenReadThenShouldRefuseIt(t *testing.T) {
	bridge := bridgeOver(t, &bridgeLog{})
	absent := laser.NewConversationID().String()

	if _, err := bridge.Task(context.Background(), absent); err == nil {
		t.Fatal("an id this bridge published nothing for is unknown")
	}
}

func TestGivenAMalformedIdWhenReadThenShouldRefuseItBeforeScanning(t *testing.T) {
	log := &bridgeLog{}
	bridge := bridgeOver(t, log)

	if _, err := bridge.Task(context.Background(), "not-a-ulid"); err == nil {
		t.Fatal("a malformed id must be refused")
	}
	// Nothing was read: an id that cannot name a task is refused before the
	// scan rather than after a fruitless one.
	if log.polls != 0 {
		t.Errorf("want no reads, got %d", log.polls)
	}
}

func TestGivenATaskInProgressWhenCanceledThenShouldWriteTheTerminal(t *testing.T) {
	log := &bridgeLog{}
	bridge := bridgeOver(t, log)
	task, err := bridge.Submit(context.Background(), []byte(sendParams))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	canceled, err := bridge.Cancel(context.Background(), task.ID)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}

	if canceled.Status.State != "TASK_STATE_CANCELED" {
		t.Errorf("want a canceled task, got %q", canceled.Status.State)
	}
	stored := log.stored()
	var terminal wire.AgentEnvelope
	if err := wire.Unmarshal(stored[len(stored)-1].Payload, &terminal); err != nil {
		t.Fatalf("decode the terminal: %v", err)
	}
	if terminal.Kind != wire.KindError {
		t.Errorf("want an error terminal, got %q", terminal.Kind)
	}
	if terminal.TaskState == nil || *terminal.TaskState != wire.TaskCanceled {
		t.Errorf("want the canceled state, got %v", terminal.TaskState)
	}
}

func TestGivenATaskThatEndedWhenCanceledAgainThenShouldRefuseIt(t *testing.T) {
	log := &bridgeLog{}
	bridge := bridgeOver(t, log)
	task, err := bridge.Submit(context.Background(), []byte(sendParams))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := bridge.Cancel(context.Background(), task.ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	_, err = bridge.Cancel(context.Background(), task.ID)

	// A terminal state is a fact on the log. A second one after it would
	// make the task read differently depending on which record was seen last.
	var rejected *bridges.A2ARejectedError
	if !asError(err, &rejected) {
		t.Fatalf("want a rejection, got %T: %v", err, err)
	}
}

func TestGivenACanceledTaskWhenReadThenShouldReportItCanceled(t *testing.T) {
	log := &bridgeLog{}
	bridge := bridgeOver(t, log)
	task, err := bridge.Submit(context.Background(), []byte(sendParams))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := bridge.Cancel(context.Background(), task.ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	read, err := bridge.Task(context.Background(), task.ID)
	if err != nil {
		t.Fatalf("read the task: %v", err)
	}

	// The state came off the log rather than out of the bridge, which is
	// what makes it survive a restart.
	if read.Status.State != "TASK_STATE_CANCELED" {
		t.Errorf("want a canceled task, got %q", read.Status.State)
	}
}

func TestGivenANewA2ABridgeWhenHopsAreContinuedThenShouldExtendItsOwnPath(t *testing.T) {
	bridge, err := bridgeOver(t, &bridgeLog{}).WithBridgeHops([]string{"mcp-edge"})
	if err != nil {
		t.Fatalf("with bridge hops: %v", err)
	}
	if _, err := bridge.WithBridgeHops([]string{"edge"}); err == nil {
		t.Error("a path already holding this bridge must be refused")
	}
}

func TestGivenNoPublicURLWhenTheCardIsAskedForThenShouldRefuseIt(t *testing.T) {
	bridge := bridgeOver(t, &bridgeLog{})

	if _, err := bridge.Card(); err == nil {
		t.Fatal("a card nobody can dial is worse than none")
	}
}

func TestGivenAPublicURLWhenTheCardIsBuiltThenShouldCarryTheV1Interface(t *testing.T) {
	bridge, err := bridgeOver(t, &bridgeLog{}).WithPublicURL("https://edge.example/a2a")
	if err != nil {
		t.Fatalf("set the public URL: %v", err)
	}

	card, err := bridge.WithDescription("the audit desk").WithVersion("2.1").Card()
	if err != nil {
		t.Fatalf("card: %v", err)
	}

	if len(card.SupportedInterfaces) != 1 {
		t.Fatalf("want one interface, got %d", len(card.SupportedInterfaces))
	}
	served := card.SupportedInterfaces[0]
	if served.URL != "https://edge.example/a2a" ||
		served.ProtocolBinding != bridges.A2AJSONRPCBinding ||
		served.ProtocolVersion != bridges.A2AProtocolVersion {
		t.Errorf("want the v1.0 interface, got %+v", served)
	}
	// Streaming is never advertised: a client told to expect SSE would wait
	// for something the log never sends.
	if card.Capabilities.Streaming {
		t.Error("the card must not advertise streaming")
	}
	if card.Name != "edge" || card.Description != "the audit desk" || card.Version != "2.1" {
		t.Errorf("want the bridge's own identity, got %+v", card)
	}
}

func asError[T error](err error, target *T) bool {
	for err != nil {
		if typed, ok := err.(T); ok {
			*target = typed
			return true
		}
		unwrapped, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = unwrapped.Unwrap()
	}
	return false
}

var _ = strings.TrimSpace
var _ = json.Marshal

// rpcCall runs one JSON-RPC request against a bridge and returns the answer.
func rpcCall(t *testing.T, bridge *bridges.A2ABridge, body string) bridges.JSONRPCResponse {
	t.Helper()
	return bridge.HandleRPC(context.Background(), []byte(body))
}

func TestGivenASendMessageCallWhenDispatchedThenShouldAnswerWithTheTask(t *testing.T) {
	bridge := bridgeOver(t, &bridgeLog{})

	answer := rpcCall(t, bridge, `{"jsonrpc":"2.0","id":1,"method":"SendMessage",
		"params":{"message":{"parts":[{"text":"run the audit"}]}}}`)

	if answer.Error != nil {
		t.Fatalf("want a result, got %+v", answer.Error)
	}
	if string(answer.ID) != "1" {
		t.Errorf("want the caller's id echoed, got %s", answer.ID)
	}
	result, ok := answer.Result.(map[string]any)
	if !ok || result["task"] == nil {
		t.Fatalf("want a task in the result, got %+v", answer.Result)
	}
}

func TestGivenAnUnknownMethodWhenDispatchedThenShouldAnswerMethodNotFound(t *testing.T) {
	bridge := bridgeOver(t, &bridgeLog{})

	answer := rpcCall(t, bridge, `{"jsonrpc":"2.0","id":7,"method":"ListTasks","params":{}}`)

	// The bridge is stateless over the log, so listing is not something it
	// can serve rather than something it refuses.
	if answer.Error == nil || answer.Error.Code != bridges.CodeMethodNotFound {
		t.Fatalf("want method not found, got %+v", answer.Error)
	}
	if string(answer.ID) != "7" {
		t.Errorf("want the id echoed on a refusal too, got %s", answer.ID)
	}
}

func TestGivenAStreamingSendWhenDispatchedThenShouldSayItIsUnsupported(t *testing.T) {
	bridge := bridgeOver(t, &bridgeLog{})

	answer := rpcCall(t, bridge,
		`{"jsonrpc":"2.0","id":1,"method":"SendStreamingMessage","params":{"message":{}}}`)

	// The card advertises no streaming, so the honest answer is that this
	// bridge does not serve it, not that the method does not exist.
	if answer.Error == nil || answer.Error.Code != bridges.CodeUnsupportedOperation {
		t.Fatalf("want unsupported operation, got %+v", answer.Error)
	}
}

func TestGivenAnUnknownTaskWhenAskedForThenShouldAnswerTaskNotFound(t *testing.T) {
	bridge := bridgeOver(t, &bridgeLog{})
	absent := laser.NewConversationID().String()

	answer := rpcCall(t, bridge,
		`{"jsonrpc":"2.0","id":1,"method":"GetTask","params":{"id":"`+absent+`"}}`)

	if answer.Error == nil || answer.Error.Code != bridges.CodeTaskNotFound {
		t.Fatalf("want task not found, got %+v", answer.Error)
	}
}

func TestGivenATaskThatEndedWhenCancelIsDispatchedThenShouldAnswerNotCancelable(t *testing.T) {
	bridge := bridgeOver(t, &bridgeLog{})
	task, err := bridge.Submit(context.Background(), []byte(sendParams))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := bridge.Cancel(context.Background(), task.ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	answer := rpcCall(t, bridge,
		`{"jsonrpc":"2.0","id":1,"method":"CancelTask","params":{"id":"`+task.ID+`"}}`)

	if answer.Error == nil || answer.Error.Code != bridges.CodeTaskNotCancelable {
		t.Fatalf("want task not cancelable, got %+v", answer.Error)
	}
}

func TestGivenARequestThatIsNotAnObjectWhenDispatchedThenShouldRefuseIt(t *testing.T) {
	bridge := bridgeOver(t, &bridgeLog{})

	// A JSON array decodes into a struct as happily as an object does, so
	// the shape is checked rather than inferred from a successful decode.
	answer := rpcCall(t, bridge, `["SendMessage"]`)

	if answer.Error == nil || answer.Error.Code != bridges.CodeInvalidRequest {
		t.Fatalf("want an invalid request, got %+v", answer.Error)
	}
}

func TestGivenAMessageNamingALiveTaskWhenSentThenShouldContinueThatConversation(t *testing.T) {
	log := &bridgeLog{}
	bridge := bridgeOver(t, log)
	task, err := bridge.Submit(context.Background(), []byte(sendParams))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	answer := rpcCall(t, bridge, `{"jsonrpc":"2.0","id":2,"method":"SendMessage","params":`+
		`{"message":{"taskId":"`+task.ID+`","parts":[{"text":"and again"}]}}}`)

	if answer.Error != nil {
		t.Fatalf("want a result, got %+v", answer.Error)
	}
	// The continuation rides the task's own conversation, so the agent
	// already handling it sees the next message rather than a second task.
	stored := log.stored()
	var continued wire.AgentEnvelope
	if err := wire.Unmarshal(stored[len(stored)-1].Payload, &continued); err != nil {
		t.Fatalf("decode the continuation: %v", err)
	}
	if continued.Conversation.String() != task.ID {
		t.Errorf("want the task's conversation, got %s", continued.Conversation)
	}
}
