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

// gating is a handler that asks for one approval and records what the gate
// answered, so a test can tell a handler that parked from one that ran
// through and from one that was replayed with a decision.
type gating struct {
	mutex    sync.Mutex
	approver wire.AgentID
	timeout  time.Duration
	// passes is each time the handler ran, by the body it saw.
	passes []string
	// grants is the decision each completed gate returned.
	grants []string
	// failures is what each gate refused with.
	failures []error
	// gateOnly limits the gate to the bodies named, so a test can send work
	// that does not itself ask for an approval.
	gateOnly map[string]bool
}

func (handler *gating) Handle(
	ctx context.Context, message *laser.AgentMessage, agent *laser.AgentCtx,
) error {
	body := string(message.Envelope.Body)
	handler.mutex.Lock()
	handler.passes = append(handler.passes, body)
	gated := handler.gateOnly == nil || handler.gateOnly[body]
	handler.mutex.Unlock()
	if !gated {
		return nil
	}
	grant, err := agent.ApprovalGate(ctx, handler.approver, []byte("may I?"), handler.timeout)
	handler.mutex.Lock()
	defer handler.mutex.Unlock()
	if err != nil {
		handler.failures = append(handler.failures, err)
		return err
	}
	handler.grants = append(handler.grants, string(grant.Decision()))
	return nil
}

func (handler *gating) ran() []string {
	handler.mutex.Lock()
	defer handler.mutex.Unlock()
	return append([]string(nil), handler.passes...)
}

func (handler *gating) decided() []string {
	handler.mutex.Lock()
	defer handler.mutex.Unlock()
	return append([]string(nil), handler.grants...)
}

func (handler *gating) refused() []error {
	handler.mutex.Lock()
	defer handler.mutex.Unlock()
	return append([]error(nil), handler.failures...)
}

// approvalDecision is the approver's reply to a parked gate: correlated to
// the record the gate ran on, and addressed back to the agent that asked.
func approvalDecision(
	t *testing.T, runtime *laser.AgentRuntime, work *laser.Record,
	offset uint64, kind wire.AgentKind, body string,
) *laser.Record {
	t.Helper()
	correlation := laser.ApprovalCorrelationForTest(
		runtime.SourcePositionForTest(work))
	// Authored by the approver: a decision is only a decision from the agent
	// that was asked, which the gate checks on the way back out.
	server := &streamServer{}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	agdx := handle.Agdx(agentID(t, "boss"), conversation(1))
	send := agdx.Respond(correlation, []byte(body))
	if kind == wire.KindError {
		send = agdx.Fail(correlation, wire.AgentErrorBody{
			Message: stringOf("not on my watch")})
	}
	if _, err := send.WithTarget(agentID(t, "executor")).
		Send(context.Background()); err != nil {
		t.Fatalf("build the decision: %v", err)
	}
	return openDelivered(t, server.sent()[0], 0, offset)
}

func stringOf(value string) *string { return &value }

// promptsOn reads back the input requests a runtime published.
func promptsOn(t *testing.T, server *runtimeServer, topic string) []wire.AgentEnvelope {
	t.Helper()
	var prompts []wire.AgentEnvelope
	for _, message := range server.on(topic) {
		var envelope wire.AgentEnvelope
		if err := wire.Unmarshal(message.Payload, &envelope); err != nil {
			continue
		}
		if envelope.Operation != nil && *envelope.Operation == wire.OperationInputRequest {
			prompts = append(prompts, envelope)
		}
	}
	return prompts
}

// liveServer is a deployment with a control lane that says nothing, so the
// pause gate is live but no session is paused. Approval parking has to work
// there: it is not an operator's pause.
//
// Any work the test will want replayed has to be readable back from the log,
// because a replay re-reads the record from its source rather than keeping
// the decoded message: the parking is durable and may outlive the process
// that made it.
func liveServer(t *testing.T, readable ...*laser.Record) *runtimeServer {
	t.Helper()
	server := newRuntimeServer()
	server.partitions = 1
	server.createdAt = parkedGeneration
	// A control record for another operation, so the book reads the lane and
	// knows the session is not paused rather than not knowing at all.
	server.seedHistory(wire.AgentControl, 0, 0,
		controlRecord(t, wire.OperationSessionResume, 0))
	if len(readable) > 0 {
		messages := make([]iggcon.IggyMessage, 0, len(readable))
		for _, record := range readable {
			messages = append(messages, record.Message)
		}
		server.seedHistory(wire.AgentSessions, 0, 0, messages...)
		server.aliasNumeric(3, wire.AgentSessions)
	}
	return server
}

// The point of parking an approval: the handler does not sit holding the
// consumer while a human decides. It returns, the record is durably held,
// and the offset commits.
func TestGivenAnApprovalGateWhenTheRuntimeCanParkThenShouldParkRatherThanWait(t *testing.T) {
	server := liveServer(t)
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{EnforceControl: true})
	work := workRecord(t, 41, "risky")
	source := sourceOf(work)
	handler := &gating{approver: agentID(t, "boss"), timeout: time.Minute}

	if err := runUntilDrained(t, runtime, source, handler); err != nil {
		t.Fatalf("run: %v", err)
	}

	if ran := handler.ran(); len(ran) != 1 {
		t.Fatalf("want the handler to have run once, got %v", ran)
	}
	if decided := handler.decided(); len(decided) != 0 {
		t.Fatalf("want no decision yet, got %v", decided)
	}
	// Parked, and under the prompt rather than under a control request.
	held := parkedOn(t, server, wire.OperationSessionParked)
	if len(held) != 1 {
		t.Fatalf("want the record parked on the session lane, got %d", len(held))
	}
	if held[0].Request.TopicID == 0 && held[0].Request.StreamID == 0 {
		t.Error("want the parking to name where the prompt landed, got a bare position")
	}
	if prompts := promptsOn(t, server, wire.AgentSessions); len(prompts) != 1 {
		t.Fatalf("want one input request published, got %d", len(prompts))
	}
	// Committed: the work is durable on the lane, so re-reading the source
	// would park it twice.
	if commits := source.commits(); len(commits) != 1 || commits[0] != 41 {
		t.Errorf("want the parked record committed, got %v", commits)
	}
}

// The decision is what makes parking worth doing: it has to find the record
// and run the handler again with the grant.
func TestGivenAParkedApprovalWhenDecidedThenShouldReplayTheHandlerWithTheGrant(t *testing.T) {
	work := workRecord(t, 41, "risky")
	server := liveServer(t, work)
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{EnforceControl: true})
	handler := &gating{approver: agentID(t, "boss"), timeout: time.Minute}
	// Two passes, because that is the shape of the thing: the record parks,
	// and the decision arrives afterwards. It also means the correlation is
	// derived from the source ids as the running runtime resolved them,
	// which is what the parked record carries.
	if err := runUntilDrained(t, runtime, sourceOf(work), handler); err != nil {
		t.Fatalf("the parking run: %v", err)
	}
	decision := approvalDecision(t, runtime, work, 42, wire.KindResponse, "go ahead")
	if err := runUntilDrained(t, runtime, sourceOf(decision), handler); err != nil {
		t.Fatalf("the decision run: %v", err)
	}

	if decided := handler.decided(); len(decided) != 1 || decided[0] != "go ahead" {
		t.Fatalf("want the replayed gate to answer with the decision, got %v", decided)
	}
	// Twice: once to the gate, once with the answer. The handler's own doc
	// says the part before the gate runs again, so this is the contract.
	if ran := handler.ran(); len(ran) != 2 {
		t.Fatalf("want the handler replayed, got %v", ran)
	}
	// Written off the lane, so a reader folding it sees nothing still held.
	if unparked := parkedOn(t, server, wire.OperationSessionUnparked); len(unparked) != 1 {
		t.Fatalf("want the record written off the lane, got %d", len(unparked))
	}
}

// A refusal is a decision too, and it must not read as a grant.
func TestGivenAParkedApprovalWhenRefusedThenShouldReplayWithTheRefusal(t *testing.T) {
	work := workRecord(t, 41, "risky")
	server := liveServer(t, work)
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{EnforceControl: true})
	handler := &gating{approver: agentID(t, "boss"), timeout: time.Minute}
	if err := runUntilDrained(t, runtime, sourceOf(work), handler); err != nil {
		t.Fatalf("the parking run: %v", err)
	}
	decision := approvalDecision(t, runtime, work, 42, wire.KindError, "")
	_ = runUntilDrained(t, runtime, sourceOf(decision), handler)

	if decided := handler.decided(); len(decided) != 0 {
		t.Fatalf("want a refusal not to grant anything, got %v", decided)
	}
	var refused *laser.ApprovalRefusedError
	failures := handler.refused()
	if len(failures) == 0 || !errors.As(failures[len(failures)-1], &refused) {
		t.Fatalf("want the replayed gate refused, got %v", failures)
	}
}

// A reply nobody parked on must not be mistaken for a decision, or any
// correlated traffic on the topic would start replaying records.
func TestGivenAnUnrelatedReplyWhenReadThenShouldNotReplayAnything(t *testing.T) {
	server := liveServer(t)
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{EnforceControl: true})
	work := workRecord(t, 41, "risky")
	handler := &gating{approver: agentID(t, "boss"), timeout: time.Minute}
	if err := runUntilDrained(t, runtime, sourceOf(work), handler); err != nil {
		t.Fatalf("the parking run: %v", err)
	}
	// A properly tagged approval correlation, derived from the resolved ids
	// exactly as a real one is -- but for a record nothing parked at. The
	// companion test above proves this same construction does replay when
	// the position matches, so a pass here is the position being checked
	// rather than the reply being unrecognizable.
	stray := approvalDecision(t, runtime, workRecord(t, 99, "other"), 42,
		wire.KindResponse, "go ahead")
	if err := runUntilDrained(t, runtime, sourceOf(stray), handler); err != nil {
		t.Fatalf("the stray run: %v", err)
	}

	if ran := handler.ran(); len(ran) != 1 {
		t.Fatalf("want the handler not replayed by a stray reply, got %v", ran)
	}
	if decided := handler.decided(); len(decided) != 0 {
		t.Fatalf("want no decision, got %v", decided)
	}
}

// The session's later work belongs behind the approval, or the decision
// would resume a session whose newer records had already run past it.
func TestGivenWorkArrivingWhileAnApprovalIsHeldThenShouldHoldItBehind(t *testing.T) {
	server := liveServer(t)
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{EnforceControl: true})
	work := workRecord(t, 41, "risky")
	later := workRecord(t, 42, "later")
	source := sourceOf(work, later)
	handler := &gating{
		approver: agentID(t, "boss"), timeout: time.Minute,
		gateOnly: map[string]bool{"risky": true},
	}

	if err := runUntilDrained(t, runtime, source, handler); err != nil {
		t.Fatalf("run: %v", err)
	}

	for _, body := range handler.ran() {
		if body == "later" {
			t.Fatal("want the later record held behind the approval, it was handled")
		}
	}
	if held := parkedOn(t, server, wire.OperationSessionParked); len(held) != 2 {
		t.Fatalf("want both records held, got %d", len(held))
	}
}

// Without a runtime that can park, the gate has to keep waiting in place:
// that is what every caller outside this runtime still relies on.
func TestGivenNoPauseRuntimeWhenGatingThenShouldWaitInPlaceInstead(t *testing.T) {
	server := liveServer(t)
	// EnforceControl off, so there is no pause gate and nothing can park.
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{})
	source := sourceOf(workRecord(t, 41, "risky"))
	// Short, so the waiting gate gives up rather than hanging the test.
	handler := &gating{approver: agentID(t, "boss"), timeout: 300 * time.Millisecond}

	_ = runUntilDrained(t, runtime, source, handler)

	if held := parkedOn(t, server, wire.OperationSessionParked); len(held) != 0 {
		t.Fatalf("want nothing parked without a pause runtime, got %d", len(held))
	}
	// It waited and timed out rather than deferring.
	failures := handler.refused()
	if len(failures) == 0 {
		t.Fatal("want the waiting gate to have given up, it returned nothing")
	}
	var deferred *laser.PolicyDeferredError
	if errors.As(failures[0], &deferred) {
		t.Fatal("want a waiting gate, it deferred instead")
	}
}

// A record parked on an approval must not be parked twice when the source
// redelivers it before the decision lands.
func TestGivenARecordParkedOnAnApprovalWhenRedeliveredThenShouldNotParkItTwice(t *testing.T) {
	server := liveServer(t)
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{EnforceControl: true})
	work := workRecord(t, 41, "risky")
	source := sourceOf(work, workRecord(t, 41, "risky"))
	handler := &gating{approver: agentID(t, "boss"), timeout: time.Minute}

	if err := runUntilDrained(t, runtime, source, handler); err != nil {
		t.Fatalf("run: %v", err)
	}

	if held := parkedOn(t, server, wire.OperationSessionParked); len(held) != 1 {
		t.Fatalf("want the record held once, got %d", len(held))
	}
	if prompts := promptsOn(t, server, wire.AgentSessions); len(prompts) != 1 {
		t.Fatalf("want the approver prompted once, got %d", len(prompts))
	}
}

// A decision addressed to another agent is that agent's to act on. Acting on
// it here would hand one agent's work forward on an approval somebody else
// was granted, on a topic several roles share.
//
// What stops it is the runtime's own addressing filter, which skips a record
// aimed elsewhere before any of the approval machinery sees it; resumeApproval
// checks the addressee again, but on this path that check is defence in depth
// rather than the thing under test. This pins the behaviour end to end, which
// is what a caller relies on either way.
func TestGivenADecisionForAnotherAgentWhenReadThenShouldNotResumeMine(t *testing.T) {
	work := workRecord(t, 41, "risky")
	server := liveServer(t, work)
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{EnforceControl: true})
	handler := &gating{approver: agentID(t, "boss"), timeout: time.Minute}
	if err := runUntilDrained(t, runtime, sourceOf(work), handler); err != nil {
		t.Fatalf("the parking run: %v", err)
	}
	// The same correlation this runtime is waiting on -- so only the
	// addressee separates it from the decision that does resume.
	correlation := laser.ApprovalCorrelationForTest(
		runtime.SourcePositionForTest(work))
	build := &streamServer{}
	handle := laser.FromClient(build, laser.Options{DefaultStream: "agents"})
	if _, err := handle.Agdx(agentID(t, "boss"), conversation(1)).
		Respond(correlation, []byte("go ahead")).
		WithTarget(agentID(t, "somebody-else")).
		Send(context.Background()); err != nil {
		t.Fatalf("build the decision: %v", err)
	}
	elsewhere := openDelivered(t, build.sent()[0], 0, 42)

	if err := runUntilDrained(t, runtime, sourceOf(elsewhere), handler); err != nil {
		t.Fatalf("the decision run: %v", err)
	}

	if decided := handler.decided(); len(decided) != 0 {
		t.Fatalf("want another agent's decision to resume nothing, got %v", decided)
	}
	if ran := handler.ran(); len(ran) != 1 {
		t.Fatalf("want the handler not replayed, got %v", ran)
	}
}
