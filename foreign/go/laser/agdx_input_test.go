package laser_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenAResponderWhenInputIsRequestedThenShouldMarkTheTaskAndReturnTheAnswer(t *testing.T) {
	server := newWorkflowServer()
	server.replyKind = wire.KindResponse
	server.replyBody = []byte("approved")
	server.replySource = agentID(t, "reviewer")
	conversation := laser.NewConversationID()
	producer := laser.FromClient(server, laser.Options{DefaultStream: "agents"}).
		Agdx(agentID(t, "planner"), conversation)

	answer, err := producer.RequestInput(context.Background(), wire.AgentSessions, []byte("ship it?"), 2*time.Second)
	if err != nil {
		t.Fatalf("request input: %v", err)
	}

	if string(answer) != "approved" {
		t.Errorf("want the responder's body, got %q", answer)
	}
	status, prompt := inputExchange(t, server)
	if status.Kind != wire.KindStatus || status.TaskState == nil || *status.TaskState != wire.TaskInputRequired {
		t.Errorf("want an input-required task status first, got %+v", status)
	}
	if prompt.Kind != wire.KindCommand || prompt.Operation == nil ||
		*prompt.Operation != wire.OperationInputRequest || string(prompt.Body) != "ship it?" {
		t.Errorf("want the prompt as an input request, got %+v", prompt)
	}
	if prompt.Target != nil {
		t.Errorf("want the prompt addressed to every agent, got %v", prompt.Target)
	}
	if status.Correlation == nil || prompt.Correlation == nil || *status.Correlation != *prompt.Correlation {
		t.Error("want the status and the prompt under one interrupt correlation")
	}
}

func TestGivenARejectingResponderWhenInputIsRequestedThenShouldReportTheRejection(t *testing.T) {
	server := newWorkflowServer()
	reason := "not this week"
	body, err := wire.Marshal(wire.AgentErrorBody{Code: wire.ErrorUnauthorized, Message: &reason})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	server.replyKind = wire.KindError
	server.replyBody = body
	server.replySource = agentID(t, "reviewer")
	producer := laser.FromClient(server, laser.Options{DefaultStream: "agents"}).
		Agdx(agentID(t, "planner"), laser.NewConversationID())

	_, err = producer.RequestInput(context.Background(), wire.AgentSessions, []byte("ship it?"), 2*time.Second)

	var rejected *laser.InputRejectedError
	if !errors.As(err, &rejected) || rejected.Reason != "not this week" {
		t.Errorf("want the rejection with its reason, got %v", err)
	}
}

func TestGivenANamedApproverWhenAnotherAgentAnswersFirstThenShouldIgnoreItAndTakeTheApproversAnswer(t *testing.T) {
	server := newWorkflowServer()
	server.replyKind = wire.KindResponse
	server.replyBody = []byte("approved")
	server.earlierSources = []wire.AgentID{agentID(t, "intruder")}
	server.replySource = agentID(t, "approver")
	producer := laser.FromClient(server, laser.Options{DefaultStream: "agents"}).
		Agdx(agentID(t, "planner"), laser.NewConversationID())

	answer, err := producer.RequestInputFrom(
		context.Background(), agentID(t, "approver"), wire.AgentSessions, []byte("ship it?"), 2*time.Second)

	if err != nil || string(answer) != "approved" {
		t.Fatalf("want the approver's answer after the other agent's was ignored, got %q, %v", answer, err)
	}
	_, prompt := inputExchange(t, server)
	if prompt.Target == nil || *prompt.Target != agentID(t, "approver") {
		t.Errorf("want the prompt addressed to the approver, got %v", prompt.Target)
	}
}

func TestGivenANamedApproverWhenOnlyAnotherAgentAnswersThenShouldKeepWaitingUntilTheTimeout(t *testing.T) {
	server := newWorkflowServer()
	server.replyKind = wire.KindResponse
	server.replyBody = []byte("approved")
	server.replySource = agentID(t, "intruder")
	producer := laser.FromClient(server, laser.Options{DefaultStream: "agents"}).
		Agdx(agentID(t, "planner"), laser.NewConversationID())

	_, err := producer.RequestInputFrom(
		context.Background(), agentID(t, "approver"), wire.AgentSessions, []byte("ship it?"), 100*time.Millisecond)

	var timedOut *laser.TimeoutError
	if !errors.As(err, &timedOut) {
		t.Fatalf("want the wait to go on until its timeout, got %v", err)
	}
}

func TestGivenNobodyAnsweringWhenInputIsRequestedThenShouldTimeOut(t *testing.T) {
	server := newWorkflowServer()
	producer := laser.FromClient(server, laser.Options{DefaultStream: "agents"}).
		Agdx(agentID(t, "planner"), laser.NewConversationID())

	_, err := producer.RequestInput(context.Background(), wire.AgentSessions, []byte("ship it?"), 50*time.Millisecond)

	var timedOut *laser.TimeoutError
	if !errors.As(err, &timedOut) {
		t.Errorf("want a timeout, got %v", err)
	}
}

// inputExchange reads back the status and the prompt an input request wrote.
func inputExchange(t *testing.T, server *workflowServer) (status, prompt wire.AgentEnvelope) {
	t.Helper()
	records := server.on(wire.AgentSessions)
	if len(records) < 2 {
		t.Fatalf("want a status and a prompt on the lane, got %d records", len(records))
	}
	if err := wire.Unmarshal(records[0].Payload, &status); err != nil {
		t.Fatalf("decode the status: %v", err)
	}
	if err := wire.Unmarshal(records[1].Payload, &prompt); err != nil {
		t.Fatalf("decode the prompt: %v", err)
	}
	return status, prompt
}
