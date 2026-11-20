package laser_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// handledThrough runs one record through a runtime and hands the handler
// context to inspect, so a test exercises the context a real handler is given
// rather than one it built itself.
func handledThrough(
	t *testing.T, server *runtimeServer, options laser.RuntimeOptions,
	record *laser.Record, act func(context.Context, *laser.AgentCtx) error,
) error {
	t.Helper()
	runtime := runtimeFor(t, server, "executor", options)
	var acted error
	handler := laser.HandlerFunc(func(
		ctx context.Context, _ *laser.AgentMessage, agent *laser.AgentCtx,
	) error {
		acted = act(ctx, agent)
		// The handler succeeds whatever the context call reported, so a
		// refusal under test is not retried and dead-lettered into noise.
		return nil
	})
	if err := runUntilDrained(t, runtime, sourceOf(record), handler); err != nil {
		t.Fatalf("run: %v", err)
	}
	return acted
}

// commandFor is a typed request addressed to the agent under test.
func commandFor(t *testing.T, offset uint64, correlation byte) *laser.Record {
	t.Helper()
	return delivered(t, 1, offset, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(correlation), []byte("question")).
			WithTarget(agentID(t, "executor"))
	})
}

// A typed request gets a typed answer: correlated, addressed back to whoever
// asked, and naming the request as its cause. Without all three the caller
// cannot tell this answer from another agent's on a shared topic.
func TestGivenATypedRequestWhenRespondedThenShouldAnswerItCorrelatedAndAddressed(t *testing.T) {
	server := newRuntimeServer()
	record := commandFor(t, 12, 4)

	err := handledThrough(t, server,
		laser.RuntimeOptions{RespondOn: wire.AgentSessions}, record,
		func(ctx context.Context, agent *laser.AgentCtx) error {
			return agent.Respond(ctx, []byte("the answer"))
		})
	if err != nil {
		t.Fatalf("respond: %v", err)
	}

	answers := envelopesOn(t, server, wire.AgentSessions)
	if len(answers) != 1 {
		t.Fatalf("want one answer, got %d", len(answers))
	}
	answer := answers[0]
	if answer.Kind != wire.KindResponse {
		t.Errorf("want a response, got %q", answer.Kind)
	}
	if string(answer.Body) != "the answer" {
		t.Errorf("want the handler's body, got %q", answer.Body)
	}
	if answer.Correlation == nil || *answer.Correlation != correlationID(4) {
		t.Error("want the request's correlation echoed, or the caller cannot pair it")
	}
	if answer.Target == nil || *answer.Target != agentID(t, "planner") {
		t.Errorf("want the answer addressed back to the asker, got %v", answer.Target)
	}
	if answer.Source != agentID(t, "executor") {
		t.Errorf("want this agent as the author, got %v", answer.Source)
	}
	if answer.CauseAt == nil || answer.CauseAt.Offset != 12 ||
		answer.CauseAt.PartitionID != 1 {
		t.Errorf("want the request's position as the cause, got %v", answer.CauseAt)
	}
}

// A handler that answers somewhere else is answering a caller that waits
// there, so the topic is the caller's to choose rather than the agent's.
func TestGivenAnotherTopicWhenRespondingThereThenShouldAnswerOnIt(t *testing.T) {
	server := newRuntimeServer()
	record := commandFor(t, 3, 5)

	err := handledThrough(t, server,
		laser.RuntimeOptions{RespondOn: wire.AgentSessions}, record,
		func(ctx context.Context, agent *laser.AgentCtx) error {
			return agent.RespondVia(ctx, wire.AgentAudit, []byte("elsewhere"))
		})
	if err != nil {
		t.Fatalf("respond: %v", err)
	}

	if len(server.on(wire.AgentSessions)) != 0 {
		t.Error("want nothing on the configured reply topic")
	}
	answers := envelopesOn(t, server, wire.AgentAudit)
	if len(answers) != 1 || string(answers[0].Body) != "elsewhere" {
		t.Fatalf("want the answer on the named topic, got %d records", len(answers))
	}
}

// Where an answer belongs is a deployment's routing decision, so an agent
// built without a reply topic refuses rather than guessing one nobody reads.
func TestGivenNoReplyTopicWhenRespondingThenShouldRefuseIt(t *testing.T) {
	server := newRuntimeServer()
	record := commandFor(t, 1, 2)

	err := handledThrough(t, server, laser.RuntimeOptions{}, record,
		func(ctx context.Context, agent *laser.AgentCtx) error {
			return agent.Respond(ctx, []byte("nowhere"))
		})

	var missing *laser.NoRespondTopicError
	if !errors.As(err, &missing) {
		t.Fatalf("want a refusal naming the missing topic, got %v", err)
	}
	if len(server.on(wire.AgentSessions)) != 0 {
		t.Error("a refused answer must not reach the log")
	}
}

// A plain request has no envelope to answer, so the reply is a plain record
// whose spine carries the pairing instead.
func TestGivenAPlainRequestWhenRespondedThenShouldReplyWithTheSpineAlone(t *testing.T) {
	server := newRuntimeServer()
	correlation := "ask-7"
	spine := &laser.Provenance{
		Conversation:  conversation(1),
		Agent:         agentPointer(t, "planner"),
		TargetAgent:   agentPointer(t, "executor"),
		CorrelationID: &correlation,
	}
	record := plainRecord(t, 1, 9, spine, []byte("question"))

	err := handledThrough(t, server,
		laser.RuntimeOptions{RespondOn: wire.AgentSessions}, record,
		func(ctx context.Context, agent *laser.AgentCtx) error {
			return agent.Respond(ctx, []byte("the answer"))
		})
	if err != nil {
		t.Fatalf("respond: %v", err)
	}

	written := server.on(wire.AgentSessions)
	if len(written) != 1 {
		t.Fatalf("want one reply, got %d", len(written))
	}
	if string(written[0].Payload) != "the answer" {
		t.Errorf("want the body sent as it stands, got %q", written[0].Payload)
	}
	reply, err := laser.ProvenanceFrom(&written[0])
	if err != nil {
		t.Fatalf("read the reply spine: %v", err)
	}
	if reply.CorrelationID == nil || *reply.CorrelationID != correlation {
		t.Errorf("want the request's correlation echoed, got %v", reply.CorrelationID)
	}
	if reply.TargetAgent == nil || *reply.TargetAgent != agentID(t, "planner") {
		t.Errorf("want the reply routed back to the asker, got %v", reply.TargetAgent)
	}
	if reply.CausalParent == nil || reply.CausalParent.Offset != 9 {
		t.Errorf("want the request named as the cause, got %v", reply.CausalParent)
	}
}

// A request that carried no correlation still gets one, taken from its own
// position, so every reader classifies the reply as a reply rather than as
// new work for somebody to pick up.
func TestGivenARequestWithoutACorrelationWhenRepliedThenShouldUseItsOwnPosition(t *testing.T) {
	server := newRuntimeServer()
	spine := &laser.Provenance{
		Conversation: conversation(1),
		Agent:        agentPointer(t, "planner"),
		TargetAgent:  agentPointer(t, "executor"),
	}
	record := plainRecord(t, 2, 40, spine, []byte("question"))

	err := handledThrough(t, server,
		laser.RuntimeOptions{RespondOn: wire.AgentSessions}, record,
		func(ctx context.Context, agent *laser.AgentCtx) error {
			return agent.Respond(ctx, []byte("the answer"))
		})
	if err != nil {
		t.Fatalf("respond: %v", err)
	}

	written := server.on(wire.AgentSessions)
	if len(written) != 1 {
		t.Fatalf("want one reply, got %d", len(written))
	}
	reply, err := laser.ProvenanceFrom(&written[0])
	if err != nil {
		t.Fatalf("read the reply spine: %v", err)
	}
	if reply.CorrelationID == nil || *reply.CorrelationID != "2:40" {
		t.Errorf("want the request's own position as the correlation, got %v",
			reply.CorrelationID)
	}
}

// The reply is its own operation, so the request's business dedup key is not
// echoed: a reply carrying it could suppress a later operation under it.
func TestGivenARequestWithAnIdempotencyKeyWhenRepliedThenShouldNotEchoIt(t *testing.T) {
	server := newRuntimeServer()
	key := "order-7"
	correlation := "ask-1"
	spine := &laser.Provenance{
		Conversation:   conversation(1),
		Agent:          agentPointer(t, "planner"),
		IdempotencyKey: &key,
		CorrelationID:  &correlation,
	}
	record := plainRecord(t, 0, 1, spine, []byte("question"))

	err := handledThrough(t, server,
		laser.RuntimeOptions{RespondOn: wire.AgentSessions}, record,
		func(ctx context.Context, agent *laser.AgentCtx) error {
			return agent.ReplyOn(ctx, wire.AgentSessions, []byte("the answer"))
		})
	if err != nil {
		t.Fatalf("reply: %v", err)
	}

	written := server.on(wire.AgentSessions)
	if len(written) != 1 {
		t.Fatalf("want one reply, got %d", len(written))
	}
	reply, err := laser.ProvenanceFrom(&written[0])
	if err != nil {
		t.Fatalf("read the reply spine: %v", err)
	}
	if reply.IdempotencyKey != nil {
		t.Errorf("a reply must not carry the request's dedup key, got %q",
			*reply.IdempotencyKey)
	}
}

// A reply stays in the conversation it answers, and keeps its place in the
// tree, so a branch three levels down can still be traced to its root.
func TestGivenARequestInAChildConversationWhenRepliedThenShouldKeepParentAndRoot(t *testing.T) {
	server := newRuntimeServer()
	parent, root := conversation(2), conversation(3)
	spine := &laser.Provenance{
		Conversation:       conversation(1),
		ParentConversation: &parent,
		RootConversation:   &root,
		Agent:              agentPointer(t, "planner"),
	}
	record := plainRecord(t, 0, 1, spine, []byte("question"))

	err := handledThrough(t, server,
		laser.RuntimeOptions{RespondOn: wire.AgentSessions}, record,
		func(ctx context.Context, agent *laser.AgentCtx) error {
			return agent.ReplyOn(ctx, wire.AgentSessions, []byte("the answer"))
		})
	if err != nil {
		t.Fatalf("reply: %v", err)
	}

	written := server.on(wire.AgentSessions)
	reply, err := laser.ProvenanceFrom(&written[0])
	if err != nil {
		t.Fatalf("read the reply spine: %v", err)
	}
	if reply.Conversation != conversation(1) {
		t.Errorf("want the reply in the conversation it answers, got %v",
			reply.Conversation)
	}
	if reply.ParentConversation == nil || *reply.ParentConversation != parent {
		t.Errorf("want the parent kept, got %v", reply.ParentConversation)
	}
	if reply.RootConversation == nil || *reply.RootConversation != root {
		t.Errorf("want the root kept, got %v", reply.RootConversation)
	}
}

// A sub-conversation is a branch of the work being handled, so it names the
// handled conversation as its parent and keeps the root of the whole tree.
func TestGivenAHandledRecordWhenSpawningASubconversationThenShouldLinkParentAndRoot(t *testing.T) {
	server := newRuntimeServer()
	root := conversation(9)
	spine := &laser.Provenance{
		Conversation:     conversation(1),
		RootConversation: &root,
		Agent:            agentPointer(t, "planner"),
	}
	record := plainRecord(t, 0, 1, spine, []byte("question"))
	var child *laser.Provenance

	err := handledThrough(t, server, laser.RuntimeOptions{}, record,
		func(_ context.Context, agent *laser.AgentCtx) error {
			spawned, err := agent.SpawnSubconversation()
			child = spawned
			return err
		})
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}

	if child.Conversation == conversation(1) {
		t.Error("a branch is its own conversation, not the one it came from")
	}
	if child.ParentConversation == nil || *child.ParentConversation != conversation(1) {
		t.Errorf("want the handled conversation as the parent, got %v",
			child.ParentConversation)
	}
	if child.RootConversation == nil || *child.RootConversation != root {
		t.Errorf("want the tree's root carried down, got %v", child.RootConversation)
	}
	if child.Agent == nil || *child.Agent != agentID(t, "executor") {
		t.Errorf("want this agent as the branch's author, got %v", child.Agent)
	}
}

// A request's answer is matched by the loop reading this agent's topic, so a
// request expecting its answer anywhere else is refused rather than left to
// wait out a reply that nothing here will ever match.
func TestGivenARequestOnAnotherTopicWhenRequestedThenShouldRefuseIt(t *testing.T) {
	server := newRuntimeServer()
	record := commandFor(t, 1, 2)

	err := handledThrough(t, server, laser.RuntimeOptions{}, record,
		func(ctx context.Context, agent *laser.AgentCtx) error {
			_, err := agent.Request(ctx, wire.AgentAudit, []byte("ask"),
				laser.RequestOptions{Timeout: 20 * time.Millisecond})
			return err
		})

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want the request refused, got %v", err)
	}
	if len(server.on(wire.AgentAudit)) != 0 {
		t.Error("a refused request must not reach the log")
	}
}

// A handler's request is answered by the loop that is already reading the
// topic, so a handler needs no reader of its own.
func TestGivenAHandlersRequestWhenAnsweredOnItsTopicThenShouldResolveItInTheLoop(t *testing.T) {
	server := newRuntimeServer()
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{})
	record := commandFor(t, 1, 2)
	asked := make(chan wire.CorrelationID, 1)
	answered := make(chan *laser.AgdxMessage, 1)

	handler := laser.HandlerFunc(func(
		ctx context.Context, _ *laser.AgentMessage, agent *laser.AgentCtx,
	) error {
		// The request is sent from the handler, and its answer is injected
		// into the hub the loop feeds, which is what a reply read off the
		// topic would do.
		go func() {
			correlation := <-asked
			answer := delivered(t, 0, 2, func(agdx *laser.Agdx) *laser.AgdxSend {
				return agdx.Respond(correlation, []byte("answered")).
					WithTarget(agentID(t, "executor"))
			})
			message, err := laser.OpenAgdx(answer)
			if err != nil {
				return
			}
			runtime.Replies().Deliver(message)
		}()
		correlation := correlationID(8)
		asked <- correlation
		reply, err := agent.Request(ctx, wire.AgentSessions, []byte("ask"),
			laser.RequestOptions{
				Correlation: correlation, Timeout: 2 * time.Second,
				Target: agentID(t, "reviewer"),
			})
		if err != nil {
			return err
		}
		answered <- reply
		return nil
	})

	if err := runUntilDrained(t, runtime, sourceOf(record), handler); err != nil {
		t.Fatalf("run: %v", err)
	}

	select {
	case reply := <-answered:
		if string(reply.Envelope.Body) != "answered" {
			t.Errorf("want the answer handed back, got %q", reply.Envelope.Body)
		}
	default:
		t.Fatal("want the handler's request answered by the loop")
	}
}

// The session a handler reads through is the handled record's own, written as
// this agent, so what it records lands on the right timeline under the right
// author.
func TestGivenAHandledRecordWhenOpeningItsSessionThenShouldLensTheHandledOne(t *testing.T) {
	server := newRuntimeServer()
	record := commandFor(t, 1, 2)
	var opened wire.ConversationID

	// What the lens writes is the only proof of who it writes as, so the
	// session records a step rather than being inspected from outside.
	if err := handledThrough(t, server, laser.RuntimeOptions{}, record,
		func(ctx context.Context, agent *laser.AgentCtx) error {
			session := agent.Session()
			opened = session.Conversation()
			_, err := session.Progress(ctx, wire.SessionTransition{})
			return err
		}); err != nil {
		t.Fatalf("record a step: %v", err)
	}

	if opened != conversation(1) {
		t.Errorf("want the handled conversation, got %v", opened)
	}
	written := envelopesOn(t, server, wire.AgentSessions)
	if len(written) != 1 {
		t.Fatalf("want the step recorded, got %d records", len(written))
	}
	if written[0].Source != agentID(t, "executor") {
		t.Errorf("want the lens writing as this agent, got %v", written[0].Source)
	}
	if written[0].Conversation != conversation(1) {
		t.Errorf("want the step on the handled session, got %v",
			written[0].Conversation)
	}
}

// A handler decides for itself when to stop, so it reads the operator
// requests the runtime observed rather than being cut off mid-step.
func TestGivenAPausedSessionWhenHandledThenShouldReportThePendingControl(t *testing.T) {
	server := newRuntimeServer()
	book := laser.NewControlBook()
	book.SetLive(true)
	book.Load([]laser.ControlRecord{{
		Conversation: conversation(1),
		At:           laser.ControlPosition{Partition: 0, Offset: 1},
		Request:      pauseRequest(t),
	}})
	record := commandFor(t, 1, 2)
	var pending laser.PendingControl
	var known bool

	if err := handledThrough(t, server, laser.RuntimeOptions{Control: book}, record,
		func(_ context.Context, agent *laser.AgentCtx) error {
			pending, known = agent.PendingControl()
			return nil
		}); err != nil {
		t.Fatalf("read the control: %v", err)
	}

	if !known {
		t.Fatal("want the folded control reported")
	}
	if !pending.PauseRequested {
		t.Error("want the pause the operator asked for")
	}
}

// Where the handled record sits is what a typed reply names as its cause, so
// a reader of the answer can go and read what it answered.
func TestGivenAHandledRecordWhenAskedWhereItSatThenShouldLocateItInTheTopic(t *testing.T) {
	server := newRuntimeServer()
	record := commandFor(t, 12, 2)
	var at wire.LogPosition

	if err := handledThrough(t, server, laser.RuntimeOptions{}, record,
		func(_ context.Context, agent *laser.AgentCtx) error {
			at = agent.RequestAt()
			return nil
		}); err != nil {
		t.Fatalf("locate the record: %v", err)
	}

	if at.StreamID != 7 || at.TopicID != 3 {
		t.Errorf("want the topic located, got stream %d topic %d",
			at.StreamID, at.TopicID)
	}
	if at.PartitionID != 1 || at.Offset != 12 {
		t.Errorf("want the record located within it, got partition %d offset %d",
			at.PartitionID, at.Offset)
	}
}

// An answer and a branch both name their author, so an agent without an
// identity refuses rather than publishing a record signed by nobody.
func TestGivenNoIdentityWhenAnsweringOrBranchingThenShouldRefuseBoth(t *testing.T) {
	server := newRuntimeServer()
	runtime := runtimeFor(t, server, "", laser.RuntimeOptions{
		RespondOn: wire.AgentSessions,
	})
	record := commandFor(t, 1, 2)
	var answered, branched error
	handler := laser.HandlerFunc(func(
		ctx context.Context, _ *laser.AgentMessage, agent *laser.AgentCtx,
	) error {
		answered = agent.Respond(ctx, []byte("from nobody"))
		_, branched = agent.SpawnSubconversation()
		return nil
	})

	if err := runUntilDrained(t, runtime, sourceOf(record), handler); err != nil {
		t.Fatalf("run: %v", err)
	}

	var invalid *laser.InvalidError
	if !errors.As(answered, &invalid) {
		t.Errorf("want the answer refused, got %v", answered)
	}
	if !errors.As(branched, &invalid) {
		t.Errorf("want the branch refused, got %v", branched)
	}
	if len(server.on(wire.AgentSessions)) != 0 {
		t.Error("a record signed by nobody must not reach the log")
	}
}

// A handler that is not answering anything sends under a provenance it built,
// so nothing of the handled record is wired in behind its back.
func TestGivenAProvenanceAHandlerBuiltWhenSendingThenShouldUseItAsItStands(t *testing.T) {
	server := newRuntimeServer()
	record := commandFor(t, 1, 2)
	own := conversation(4)

	err := handledThrough(t, server, laser.RuntimeOptions{}, record,
		func(ctx context.Context, agent *laser.AgentCtx) error {
			return agent.Send(ctx, wire.AgentAudit, []byte("noted"),
				&laser.Provenance{
					Conversation: own, Agent: agentPointer(t, "executor")})
		})
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	written := server.on(wire.AgentAudit)
	if len(written) != 1 {
		t.Fatalf("want one record, got %d", len(written))
	}
	spine, err := laser.ProvenanceFrom(&written[0])
	if err != nil {
		t.Fatalf("read the spine: %v", err)
	}
	if spine.Conversation != own {
		t.Errorf("want the handler's own conversation, got %v", spine.Conversation)
	}
	if spine.CausalParent != nil {
		t.Error("a plain send names no cause, or it would read as an answer")
	}
}

// An input request is answered on the topic its waiting caller reads, under
// the correlation that caller is paused on.
func TestGivenAnInputRequestWhenAnsweredThenShouldCarryTheCorrelationItPauses(t *testing.T) {
	server := newRuntimeServer()
	record := delivered(t, 0, 6, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(7), []byte("approve?")).
			WithTarget(agentID(t, "executor")).
			WithOperation(wire.OperationInputRequest)
	})

	err := handledThrough(t, server,
		laser.RuntimeOptions{Operations: []string{wire.OperationInputRequest}},
		record, func(ctx context.Context, agent *laser.AgentCtx) error {
			return agent.RespondInput(ctx, wire.AgentAudit, []byte("approved"))
		})
	if err != nil {
		t.Fatalf("answer the input request: %v", err)
	}

	answers := envelopesOn(t, server, wire.AgentAudit)
	if len(answers) != 1 {
		t.Fatalf("want one answer, got %d", len(answers))
	}
	if answers[0].Kind != wire.KindResponse {
		t.Errorf("want a response, got %q", answers[0].Kind)
	}
	if answers[0].Correlation == nil || *answers[0].Correlation != correlationID(7) {
		t.Error("want the correlation the caller is paused on")
	}
	if answers[0].Target == nil || *answers[0].Target != agentID(t, "planner") {
		t.Errorf("want the answer addressed to the asker, got %v", answers[0].Target)
	}
}

// A record that is not a correlated agent message cannot be answered as an
// input request, so it is refused rather than sent as a plain record that
// nothing correlates and nobody waits for.
func TestGivenAPlainRecordWhenAnsweredAsAnInputRequestThenShouldRefuseIt(t *testing.T) {
	server := newRuntimeServer()
	spine := &laser.Provenance{
		Conversation: conversation(1), Agent: agentPointer(t, "planner"),
	}
	record := plainRecord(t, 0, 1, spine, []byte("question"))

	err := handledThrough(t, server, laser.RuntimeOptions{}, record,
		func(ctx context.Context, agent *laser.AgentCtx) error {
			return agent.RespondInput(ctx, wire.AgentAudit, []byte("approved"))
		})

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want the answer refused, got %v", err)
	}
	if len(server.on(wire.AgentAudit)) != 0 {
		t.Error("a refused answer must not reach the log")
	}
}

// An approval gate marks the session as waiting, asks the approver, and holds
// the handler until the approver decides.
func TestGivenAnApproverWhenGatedThenShouldAskAndWaitForTheDecision(t *testing.T) {
	server := newRuntimeServer()
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{})
	record := commandFor(t, 1, 2)
	var grant *laser.ApprovalGrant
	var gateErr error

	handler := laser.HandlerFunc(func(
		ctx context.Context, _ *laser.AgentMessage, agent *laser.AgentCtx,
	) error {
		go approve(t, server, runtime, wire.KindResponse, []byte("go ahead"))
		grant, gateErr = agent.ApprovalGate(ctx, agentID(t, "reviewer"),
			[]byte("may I?"), 2*time.Second)
		return nil
	})

	if err := runUntilDrained(t, runtime, sourceOf(record), handler); err != nil {
		t.Fatalf("run: %v", err)
	}

	if gateErr != nil {
		t.Fatalf("gate: %v", gateErr)
	}
	if grant.Approver() != agentID(t, "reviewer") {
		t.Errorf("want the named approver, got %v", grant.Approver())
	}
	if string(grant.Decision()) != "go ahead" {
		t.Errorf("want the approver's own words, got %q", grant.Decision())
	}
	asked := envelopesOn(t, server, wire.AgentSessions)
	if len(asked) != 2 {
		t.Fatalf("want a waiting status then the prompt, got %d", len(asked))
	}
	if asked[0].Kind != wire.KindStatus || asked[0].TaskState == nil ||
		*asked[0].TaskState != wire.TaskInputRequired {
		t.Error("want the session marked as waiting for input first")
	}
	if asked[1].Kind != wire.KindCommand || asked[1].Operation == nil ||
		*asked[1].Operation != wire.OperationInputRequest {
		t.Error("want the prompt sent as an input request")
	}
	if asked[1].Target == nil || *asked[1].Target != agentID(t, "reviewer") {
		t.Errorf("want the prompt addressed to the approver, got %v", asked[1].Target)
	}
	if asked[0].Correlation == nil || asked[1].Correlation == nil ||
		*asked[0].Correlation != *asked[1].Correlation {
		t.Error("want the status and the prompt on one correlation")
	}
	if asked[1].DeadlineMicros == nil {
		t.Error("want the prompt to expire, or an approver that never answers holds forever")
	}
}

// A refusal is a decision, and it is not a grant: turning one into a grant
// would let the work go ahead on the strength of the approver saying no.
func TestGivenARefusingApproverWhenGatedThenShouldRefuseRatherThanGrant(t *testing.T) {
	server := newRuntimeServer()
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{})
	record := commandFor(t, 1, 2)
	var grant *laser.ApprovalGrant
	var gateErr error

	reason := "not this quarter"
	refusal, err := wire.Marshal(wire.AgentErrorBody{
		Code: wire.ErrorUnauthorized, Message: &reason})
	if err != nil {
		t.Fatalf("encode the refusal: %v", err)
	}
	handler := laser.HandlerFunc(func(
		ctx context.Context, _ *laser.AgentMessage, agent *laser.AgentCtx,
	) error {
		go approve(t, server, runtime, wire.KindError, refusal)
		grant, gateErr = agent.ApprovalGate(ctx, agentID(t, "reviewer"),
			[]byte("may I?"), 2*time.Second)
		return nil
	})

	if err := runUntilDrained(t, runtime, sourceOf(record), handler); err != nil {
		t.Fatalf("run: %v", err)
	}

	if grant != nil {
		t.Fatal("a refusal must not mint a grant")
	}
	var refused *laser.ApprovalRefusedError
	if !errors.As(gateErr, &refused) {
		t.Fatalf("want a refusal, got %v", gateErr)
	}
	if refused.Reason != reason {
		t.Errorf("want the approver's own reason, got %q", refused.Reason)
	}
	if laser.RetryableError(gateErr) {
		t.Error("asking again will not change the answer")
	}
}

// Only the named approver decides, so a reply on this correlation from
// anybody else is a fault rather than a decision.
func TestGivenAReplyFromAnotherAgentWhenGatedThenShouldNotTakeItAsTheDecision(t *testing.T) {
	server := newRuntimeServer()
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{})
	record := commandFor(t, 1, 2)
	var grant *laser.ApprovalGrant
	var gateErr error

	handler := laser.HandlerFunc(func(
		ctx context.Context, _ *laser.AgentMessage, agent *laser.AgentCtx,
	) error {
		go approveAs(t, server, runtime, "impostor", wire.KindResponse,
			[]byte("go ahead"))
		grant, gateErr = agent.ApprovalGate(ctx, agentID(t, "reviewer"),
			[]byte("may I?"), 2*time.Second)
		return nil
	})

	if err := runUntilDrained(t, runtime, sourceOf(record), handler); err != nil {
		t.Fatalf("run: %v", err)
	}

	if grant != nil {
		t.Fatal("an agent nobody asked must not decide an approval")
	}
	if gateErr == nil {
		t.Fatal("want the wrong author reported")
	}
}

// An approval gate must name who decides, or anybody could.
func TestGivenNoApproverWhenAHandlerGatesThenShouldRefuseIt(t *testing.T) {
	server := newRuntimeServer()
	record := commandFor(t, 1, 2)

	err := handledThrough(t, server, laser.RuntimeOptions{}, record,
		func(ctx context.Context, agent *laser.AgentCtx) error {
			_, err := agent.ApprovalGate(ctx, wire.AgentID{}, []byte("may I?"),
				time.Second)
			return err
		})

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want the gate refused, got %v", err)
	}
	if len(server.on(wire.AgentSessions)) != 0 {
		t.Error("a refused gate must not ask anybody")
	}
}

// approve answers the prompt the gate under test sent, as the approver it
// named.
func approve(
	t *testing.T, server *runtimeServer, runtime *laser.AgentRuntime,
	kind wire.AgentKind, body []byte,
) {
	t.Helper()
	approveAs(t, server, runtime, "reviewer", kind, body)
}

// approveAs answers the gate's prompt as a named agent, so a test can show
// that only the approver decides.
func approveAs(
	t *testing.T, server *runtimeServer, runtime *laser.AgentRuntime,
	author string, kind wire.AgentKind, body []byte,
) {
	t.Helper()
	correlation, found := awaitPrompt(server)
	if !found {
		return
	}
	answer := delivered(t, 0, 2, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Respond(correlation, body).WithTarget(agentID(t, "executor"))
	})
	message, err := laser.OpenAgdx(answer)
	if err != nil {
		return
	}
	// The author and kind are rewritten here rather than produced, because a
	// test needs to answer as somebody else and to refuse as well as agree.
	message.Envelope.Source = agentID(t, author)
	message.Envelope.Kind = kind
	message.Envelope.Body = body
	runtime.Replies().Deliver(message)
}

// awaitPrompt waits for the gate to send its input request and reads the
// correlation the gate is waiting on.
func awaitPrompt(server *runtimeServer) (wire.CorrelationID, bool) {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, message := range server.on(wire.AgentSessions) {
			var envelope wire.AgentEnvelope
			if err := wire.Unmarshal(message.Payload, &envelope); err != nil {
				continue
			}
			if envelope.Kind == wire.KindCommand && envelope.Operation != nil &&
				*envelope.Operation == wire.OperationInputRequest &&
				envelope.Correlation != nil {
				return *envelope.Correlation, true
			}
		}
		time.Sleep(time.Millisecond)
	}
	return wire.CorrelationID{}, false
}

// plainRecord builds a record produced by a plain agent send, which carries a
// spine and no envelope.
func plainRecord(
	t *testing.T, partition uint32, offset uint64,
	spine *laser.Provenance, payload []byte,
) *laser.Record {
	t.Helper()
	server := &streamServer{}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	if _, err := handle.SendAgent(
		context.Background(), wire.AgentSessions, payload, spine); err != nil {
		t.Fatalf("build a record: %v", err)
	}
	return openDelivered(t, server.sent()[0], partition, offset)
}

// pauseRequest is an operator's pause of the session under test.
func pauseRequest(t *testing.T) laser.ControlRequest {
	t.Helper()
	operation := wire.OperationSessionPause
	target := agentID(t, "executor")
	request, made := laser.ControlRequestOf(&wire.AgentEnvelope{
		Kind: wire.KindCommand, Operation: &operation,
		Source: agentID(t, "operator"), Target: &target,
		Conversation: conversation(1),
	}, &target)
	if !made {
		t.Fatal("this envelope must read as a pause request")
	}
	return request
}
