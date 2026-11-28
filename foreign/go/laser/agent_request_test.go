package laser_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenASerialRuntimeWhenAHandlerRequestsAndTheReplyLandsOnItsOwnTopicThenShouldReturnTheReply(t *testing.T) {
	server := newWorkflowServer()
	server.replyKind = wire.KindResponse
	server.replyBody = []byte("disk full")
	server.replySource = agentID(t, "specialist")
	runtime := requestingRuntime(t, server)
	var guard sync.Mutex
	var answers [][]byte
	handler := laser.HandlerFunc(func(
		ctx context.Context, _ *laser.AgentMessage, agent *laser.AgentCtx,
	) error {
		reply, err := agent.Request(ctx, wire.AgentSessions, []byte("diagnose"), laser.RequestOptions{
			Target: agentID(t, "specialist"), ReplyTopic: wire.AgentSessions, Timeout: 2 * time.Second,
		})
		if err != nil {
			return err
		}
		guard.Lock()
		answers = append(answers, reply.Envelope.Body)
		guard.Unlock()
		return nil
	})

	if err := runUntilDrained(t, runtime, sourceOf(workRecord(t, 41, "triage")), handler); err != nil {
		t.Fatalf("run: %v", err)
	}

	guard.Lock()
	defer guard.Unlock()
	if len(answers) != 1 || string(answers[0]) != "disk full" {
		t.Fatalf("want the specialist's answer returned to the handler once, got %q", answers)
	}
}

func TestGivenARequestOnAnotherTopicWhenAHandlerRequestsThenShouldAwaitTheReplyOnThatTopic(t *testing.T) {
	server := newWorkflowServer()
	server.replyKind = wire.KindResponse
	server.replyBody = []byte("logged")
	server.replySource = agentID(t, "auditor")
	runtime := requestingRuntime(t, server)
	var guard sync.Mutex
	var answer []byte
	handler := laser.HandlerFunc(func(
		ctx context.Context, _ *laser.AgentMessage, agent *laser.AgentCtx,
	) error {
		reply, err := agent.Request(ctx, wire.AgentAudit, []byte("ask"), laser.RequestOptions{Timeout: 2 * time.Second})
		if err != nil {
			return err
		}
		guard.Lock()
		answer = reply.Envelope.Body
		guard.Unlock()
		return nil
	})

	if err := runUntilDrained(t, runtime, sourceOf(workRecord(t, 41, "triage")), handler); err != nil {
		t.Fatalf("run: %v", err)
	}

	guard.Lock()
	defer guard.Unlock()
	if string(answer) != "logged" {
		t.Fatalf("want the answer read on the other topic, got %q", answer)
	}
	if len(server.on(wire.AgentAudit)) == 0 {
		t.Error("want the request published on the topic it named")
	}
}

func TestGivenASubconversationWhenAHandlerRequestsUnderItThenShouldSendAndAwaitInTheChild(t *testing.T) {
	server := newWorkflowServer()
	server.replyKind = wire.KindResponse
	server.replyBody = []byte("done")
	server.replySource = agentID(t, "specialist")
	runtime := requestingRuntime(t, server)
	var guard sync.Mutex
	var child *laser.Provenance
	var answered bool
	handler := laser.HandlerFunc(func(
		ctx context.Context, _ *laser.AgentMessage, agent *laser.AgentCtx,
	) error {
		branch, err := agent.SpawnSubconversation()
		if err != nil {
			return err
		}
		reply, err := agent.Request(ctx, wire.AgentSessions, []byte("diagnose"), laser.RequestOptions{
			Target: agentID(t, "specialist"), Timeout: 2 * time.Second, Provenance: branch,
		})
		if err != nil {
			return err
		}
		guard.Lock()
		child, answered = branch, reply != nil
		guard.Unlock()
		return nil
	})

	if err := runUntilDrained(t, runtime, sourceOf(workRecord(t, 41, "triage")), handler); err != nil {
		t.Fatalf("run: %v", err)
	}

	guard.Lock()
	defer guard.Unlock()
	if !answered {
		t.Fatal("want the request answered in the subconversation")
	}
	var command wire.AgentEnvelope
	for _, record := range server.on(wire.AgentSessions) {
		var envelope wire.AgentEnvelope
		if err := wire.Unmarshal(record.Payload, &envelope); err == nil && envelope.Kind == wire.KindCommand {
			command = envelope
		}
	}
	if command.Conversation != child.Conversation || command.Parent == nil ||
		*command.Parent != *child.ParentConversation || command.Root == nil || *command.Root != *child.RootConversation {
		t.Errorf("want the command in the child conversation with its ancestry, got %+v", command)
	}
}

func TestGivenASerialRuntimeWhenAHandlerFansOutThenShouldGatherTheBranchReply(t *testing.T) {
	server := newWorkflowServer()
	server.replyKind = wire.KindResponse
	server.replyBody = []byte("reviewed")
	server.replySource = agentID(t, "reviewer")
	runtime := requestingRuntime(t, server)
	registry := registryWith(t, time.Now(), map[string]wire.AgentCard{
		"reviewer": cardFor("review", nil, nil, nil, nil),
	})
	var guard sync.Mutex
	var gathered *laser.Gather
	handler := laser.HandlerFunc(func(
		ctx context.Context, _ *laser.AgentMessage, agent *laser.AgentCtx,
	) error {
		result, err := agent.FanOut(ctx, laser.FanOutRequest{
			Skill: "review", Routing: laser.RouteAny, Registry: registry,
			Payload: []byte("check this"), Policy: laser.GatherAll(), Deadline: 2 * time.Second,
		})
		guard.Lock()
		gathered = result
		guard.Unlock()
		return err
	})

	if err := runUntilDrained(t, runtime, sourceOf(workRecord(t, 41, "triage")), handler); err != nil {
		t.Fatalf("run: %v", err)
	}

	guard.Lock()
	defer guard.Unlock()
	if gathered == nil || !gathered.Satisfied || len(gathered.OK) != 1 {
		t.Fatalf("want the one branch answered while the loop is held, got %+v", gathered)
	}
}

func TestGivenASerialRuntimeWhenAHandlerWaitsAtAnApprovalGateThenShouldTakeTheApproversDecision(t *testing.T) {
	server := newWorkflowServer()
	server.replyKind = wire.KindResponse
	server.replyBody = []byte("approved")
	server.replySource = agentID(t, "boss")
	runtime := requestingRuntime(t, server)
	var guard sync.Mutex
	var granted bool
	handler := laser.HandlerFunc(func(
		ctx context.Context, _ *laser.AgentMessage, agent *laser.AgentCtx,
	) error {
		grant, err := agent.ApprovalGate(ctx, agentID(t, "boss"), []byte("ship it?"), 2*time.Second)
		guard.Lock()
		granted = grant != nil
		guard.Unlock()
		return err
	})

	if err := runUntilDrained(t, runtime, sourceOf(workRecord(t, 41, "risky")), handler); err != nil {
		t.Fatalf("run: %v", err)
	}

	guard.Lock()
	defer guard.Unlock()
	if !granted {
		t.Fatal("want the decision read while the loop is held by the gated handler")
	}
}

func TestGivenAProvenanceWhenARequesterRequestsThenShouldSendUnderItsConversationAndAncestry(t *testing.T) {
	server := newWorkflowServer()
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	parent, root, child := laser.NewConversationID(), laser.NewConversationID(), laser.NewConversationID()
	branch := &laser.Provenance{Conversation: child, ParentConversation: &parent, RootConversation: &root}

	_, err := handle.Requester(agentID(t, "planner"), wire.AgentSessions).Request(
		context.Background(), laser.NewConversationID(), []byte("ask"),
		laser.RequestOptions{Timeout: 20 * time.Millisecond, Provenance: branch})

	var timedOut *laser.TimeoutError
	if !errors.As(err, &timedOut) {
		t.Fatalf("want the unanswered request to time out, got %v", err)
	}
	records := server.on(wire.AgentSessions)
	if len(records) != 1 {
		t.Fatalf("want one command on the log, got %d", len(records))
	}
	var command wire.AgentEnvelope
	if err := wire.Unmarshal(records[0].Payload, &command); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if command.Conversation != child || command.Parent == nil || *command.Parent != parent ||
		command.Root == nil || *command.Root != root {
		t.Errorf("want the command under the provenance's conversation and ancestry, got %+v", command)
	}
}

func requestingRuntime(t *testing.T, server *workflowServer) *laser.AgentRuntime {
	t.Helper()
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	runtime, err := handle.AgentRuntime(
		handle.Agent(agentID(t, "executor"), laser.AgentOptions{}),
		laser.RuntimeOptions{Topic: wire.AgentSessions})
	if err != nil {
		t.Fatalf("open the runtime: %v", err)
	}
	return runtime
}
