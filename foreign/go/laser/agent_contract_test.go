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
	// perTopic keeps each topic's own log, so a test can tell where a
	// command was addressed and await a reply somewhere else. records stays
	// the flat log every single-topic test already reads.
	perTopic map[string][]iggcon.IggyMessage
	// answerOn, when set, pushes every auto-reply to that topic instead of
	// the one the command arrived on, the way a target told to respond
	// elsewhere would.
	answerOn string
	// polledFrom is every offset each topic was polled from, so a test can
	// tell which topics a wait read and where a resumed fold started.
	polledFrom map[string][]uint64

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
	// replyKey, when set, signs every auto-answer with it, the way a real
	// target with a SigningKey configured would.
	replyKey *laser.SigningKey
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

// pushOn appends to one topic's log, and to the flat log every single-topic
// test reads. An empty topic is the flat log alone.
func (server *contractServer) pushOn(topic string, message iggcon.IggyMessage) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if topic != "" {
		if server.perTopic == nil {
			server.perTopic = map[string][]iggcon.IggyMessage{}
		}
		message.Header.Offset = uint64(len(server.perTopic[topic]))
		server.perTopic[topic] = append(server.perTopic[topic], message)
		server.records = append(server.records, message)
		return
	}
	message.Header.Offset = uint64(len(server.records))
	server.records = append(server.records, message)
}

// topicName reads a topic identifier back as the name it was built from.
func topicName(identifier iggcon.Identifier) string {
	return string(identifier.Value())
}

// SendMessages records the command and, when replyKind is set, synchronously
// answers it, so Send's poll always has something to find without a real
// agent on the other end.
func (server *contractServer) SendMessages(
	_ context.Context, _, topic iggcon.Identifier,
	_ iggcon.Partitioning, messages []iggcon.IggyMessage,
) (*iggcon.SendMessagesResponse, error) {
	sentOn := topicName(topic)
	for index := range messages {
		server.pushOn(sentOn, messages[index])
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
		if server.replyKey != nil {
			version := uint32(wire.AgentOpVersion)
			signContext := wire.SignatureContext{AgentVersion: &version}
			if code, named := wire.ContentCBOR.Code(); named {
				signContext.ContentType = &code
			}
			signature, err := server.replyKey.SignWithContext(reply, signContext)
			if err != nil {
				continue
			}
			reply.Signature = signature
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
		answerOn := sentOn
		if server.answerOn != "" {
			answerOn = server.answerOn
		}
		server.pushOn(answerOn, replyMessage)
	}
	return &iggcon.SendMessagesResponse{}, nil
}

func (server *contractServer) PollMessages(
	_ context.Context, _, topic iggcon.Identifier, _ uint32,
	_ iggcon.Consumer, strategy iggcon.PollingStrategy, _ iggcon.PollOptions,
) (*iggcon.PolledMessage, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	from := uint64(0)
	if strategy.Kind == iggcon.POLLING_OFFSET {
		from = strategy.Value
	}
	if server.polledFrom == nil {
		server.polledFrom = map[string][]uint64{}
	}
	server.polledFrom[topicName(topic)] = append(server.polledFrom[topicName(topic)], from)
	// A test that addressed topics reads the polled topic's own log. One
	// that never did reads the flat log, which is what it published to.
	log := server.records
	if server.perTopic != nil {
		log = server.perTopic[topicName(topic)]
	}
	if from >= uint64(len(log)) {
		return &iggcon.PolledMessage{}, nil
	}
	return &iggcon.PolledMessage{
		Messages: append([]iggcon.IggyMessage(nil), log[from:]...),
	}, nil
}

// storedOn is one topic's own log.
func (server *contractServer) storedOn(topic string) []iggcon.IggyMessage {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	return append([]iggcon.IggyMessage(nil), server.perTopic[topic]...)
}

// pollsOf is every offset topic was polled from.
func (server *contractServer) pollsOf(topic string) []uint64 {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	return append([]uint64(nil), server.polledFrom[topic]...)
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

	contract, err := handle.Contract(laser.To(agentID(t, "worker")), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")).
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

	contract, err := handle.Contract(laser.To(agentID(t, "worker")), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")).
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

	contract, err := handle.Contract(laser.To(agentID(t, "worker")), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")).
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

	contract, err := handle.Contract(laser.To(agentID(t, "worker")), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")).
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

	_, err := handle.Contract(laser.To(agentID(t, "worker")), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")).
		Payload([]byte(`{"task":"run"}`)).
		Send(context.Background())

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an invalid-argument refusal, got %T: %v", err, err)
	}
}

func TestGivenABroadcastRouteWhenContractSentThenShouldRefuseIt(t *testing.T) {
	handle := laser.FromClient(&contractServer{}, laser.Options{DefaultStream: "agents"})

	_, err := handle.Contract(laser.Broadcast(), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")).
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

	contract, err := handle.Contract(laser.ToCapable("review", laser.RouteAny), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")).
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

	contract, err := handle.Contract(laser.To(agentID(t, "worker")), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")).
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

	contract, err := handle.Contract(laser.To(agentID(t, "worker")), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")).
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

func TestGivenAVerifierWhenContractReplySignedByTheTargetThenShouldCompleteWithThePrincipal(t *testing.T) {
	key, err := laser.GenerateSigningKey()
	if err != nil {
		t.Fatalf("generate a signing key: %v", err)
	}
	registry := laser.NewKeyRegistry()
	registry.Enroll("worker", key.VerifyingKey())
	server := &contractServer{
		replyKind: wire.KindResponse, replyBody: []byte("done"),
		replySource: agentID(t, "worker"), replyKey: key,
	}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	contract, err := handle.Contract(laser.To(agentID(t, "worker")), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")).
		From(agentID(t, "orchestrator")).
		Payload([]byte(`{"task":"run"}`)).
		Verifier(registry).
		Deadline(2 * time.Second).
		Send(context.Background())
	if err != nil {
		t.Fatalf("send the contract: %v", err)
	}
	if contract.State != laser.ContractCompleted {
		t.Fatalf("want completed, got %v", contract.State)
	}
	if contract.Reply.VerifiedPrincipal == nil || *contract.Reply.VerifiedPrincipal != "worker" {
		t.Errorf("want the verified principal on the reply, got %v", contract.Reply.VerifiedPrincipal)
	}
}

func TestGivenAVerifierWhenContractReplyUnsignedThenShouldTimeOut(t *testing.T) {
	registry := laser.NewKeyRegistry()
	server := &contractServer{
		replyKind: wire.KindResponse, replyBody: []byte("done"), replySource: agentID(t, "worker"),
	}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	contract, err := handle.Contract(laser.To(agentID(t, "worker")), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")).
		From(agentID(t, "orchestrator")).
		Payload([]byte(`{"task":"run"}`)).
		Verifier(registry).
		Deadline(300 * time.Millisecond).
		Send(context.Background())
	if err != nil {
		t.Fatalf("send the contract: %v", err)
	}
	if contract.State != laser.ContractTimedOut {
		t.Fatalf("want timed out, since an unsigned reply must not settle a verified contract, got %v",
			contract.State)
	}
}

func TestGivenAVerifierWhenContractReplySignedByAnotherAgentThenShouldTimeOut(t *testing.T) {
	key, err := laser.GenerateSigningKey()
	if err != nil {
		t.Fatalf("generate a signing key: %v", err)
	}
	registry := laser.NewKeyRegistry()
	registry.Enroll("imposter", key.VerifyingKey())
	server := &contractServer{
		replyKind: wire.KindResponse, replyBody: []byte("done"),
		replySource: agentID(t, "worker"), replyKey: key,
	}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	contract, err := handle.Contract(laser.To(agentID(t, "worker")), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")).
		From(agentID(t, "orchestrator")).
		Payload([]byte(`{"task":"run"}`)).
		Verifier(registry).
		Deadline(300 * time.Millisecond).
		Send(context.Background())
	if err != nil {
		t.Fatalf("send the contract: %v", err)
	}
	if contract.State != laser.ContractTimedOut {
		t.Fatalf("want timed out, since the reply is signed by a different "+
			"principal than the resolved target, got %v", contract.State)
	}
}

// A route that insisted on who the agent is must not then accept an answer
// signed by whoever holds the agent's name: the expected signer becomes the
// route's authenticated principal, joining discovery to authorship.
func TestGivenAPrincipalBoundRouteWhenTheReplyIsSignedByThatPrincipalThenShouldComplete(t *testing.T) {
	key, err := laser.GenerateSigningKey()
	if err != nil {
		t.Fatalf("generate a signing key: %v", err)
	}
	keys := laser.NewKeyRegistry()
	// Enrolled under the principal's own identity, which is what the route
	// required, rather than under the agent name it happens to answer as.
	keys.Enroll("7", key.VerifyingKey())
	agents := boundRegistry(t, time.Now(), "run", map[string]*uint32{
		"worker": principalOf(7),
	})
	server := &contractServer{
		replyKind: wire.KindResponse, replyBody: []byte("done"),
		replySource: agentID(t, "worker"), replyKey: key,
	}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	contract, err := handle.Contract(
		laser.ToPrincipal(agentID(t, "worker"), laser.PrincipalID(7)), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")).
		From(agentID(t, "orchestrator")).
		Payload([]byte(`{"task":"run"}`)).
		Registry(agents).
		Verifier(keys).
		Deadline(2 * time.Second).
		Send(context.Background())
	if err != nil {
		t.Fatalf("send the contract: %v", err)
	}
	if contract.State != laser.ContractCompleted {
		t.Fatalf("want completed, got %v", contract.State)
	}
	if contract.Reply.VerifiedPrincipal == nil || *contract.Reply.VerifiedPrincipal != "7" {
		t.Errorf("want the route's principal on the reply, got %v",
			contract.Reply.VerifiedPrincipal)
	}
}

// The same contract, answered by a key enrolled under the agent's name
// instead of the required principal, does not settle: this is what proves
// the expected signer actually moved to the principal.
func TestGivenAPrincipalBoundRouteWhenTheReplyIsSignedByTheAgentNameThenShouldTimeOut(t *testing.T) {
	key, err := laser.GenerateSigningKey()
	if err != nil {
		t.Fatalf("generate a signing key: %v", err)
	}
	keys := laser.NewKeyRegistry()
	keys.Enroll("worker", key.VerifyingKey())
	agents := boundRegistry(t, time.Now(), "run", map[string]*uint32{
		"worker": principalOf(7),
	})
	server := &contractServer{
		replyKind: wire.KindResponse, replyBody: []byte("done"),
		replySource: agentID(t, "worker"), replyKey: key,
	}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	contract, err := handle.Contract(
		laser.ToPrincipal(agentID(t, "worker"), laser.PrincipalID(7)), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")).
		From(agentID(t, "orchestrator")).
		Payload([]byte(`{"task":"run"}`)).
		Registry(agents).
		Verifier(keys).
		Deadline(250 * time.Millisecond).
		Send(context.Background())
	if err != nil {
		t.Fatalf("send the contract: %v", err)
	}
	if contract.State != laser.ContractTimedOut {
		t.Fatalf("want timed out, got %v", contract.State)
	}
}
