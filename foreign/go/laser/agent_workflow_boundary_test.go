package laser_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenACancelRequestOnTheControlTopicWhenAWorkflowRunsThenShouldStopAsCancelled(t *testing.T) {
	server := newWorkflowServer()
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	runID := requestCancel(t, handle)
	bodies := 0

	_, err := handle.Workflow(agentID(t, "pipeline"), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")).
		RunID(runID).
		StepLocal("never", func(context.Context, map[string][]byte) ([]byte, error) {
			bodies++
			return nil, nil
		}).
		Run(context.Background())

	var cancelled *laser.CancelledError
	if !errors.As(err, &cancelled) || cancelled.Run != runID.String() || err.Error() != "run cancelled: "+runID.String() {
		t.Fatalf("want the run cancelled by its id, got %T: %v", err, err)
	}
	if code := laser.ResultCode(err); code != wire.ResultCancelled {
		t.Errorf("want the cancelled result code, got %v", code)
	}
	if bodies != 0 {
		t.Errorf("a cancelled run must not start a step, ran %d", bodies)
	}
	if ended := lastRunTaskState(t, server); ended != wire.TaskCanceled {
		t.Errorf("want the run session ended as canceled, got %q", ended)
	}
}

func TestGivenACancelledRunWhenTheNextStepStartsThenShouldStopBeforeItsBody(t *testing.T) {
	server := newWorkflowServer()
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	runID := requestCancel(t, handle)
	bodies := 0

	_, err := laser.RunWith(context.Background(),
		handle.Workflow(agentID(t, "pipeline"), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")).RunID(runID),
		func(ctx context.Context, run *laser.WorkflowRun) (int, error) {
			return laser.RunStep(ctx, run, "never", func(context.Context) (int, error) {
				bodies++
				return 1, nil
			})
		})

	var cancelled *laser.CancelledError
	if !errors.As(err, &cancelled) || bodies != 0 {
		t.Fatalf("want the run cancelled before the body, got %v after %d bodies", err, bodies)
	}
	if ended := lastRunTaskState(t, server); ended != wire.TaskCanceled {
		t.Errorf("want the run session ended as canceled, got %q", ended)
	}
}

func TestGivenAWallClockBudgetTooSpentToDispatchWhenAStepIsDueThenShouldStopOverBudget(t *testing.T) {
	server := newWorkflowServer()
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	bodies := 0

	_, err := handle.Workflow(agentID(t, "pipeline"), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")).
		Budget(laser.WorkflowBudget{}.WithWallClock(50*time.Millisecond)).
		StepLocal("never", func(context.Context, map[string][]byte) ([]byte, error) {
			bodies++
			return nil, nil
		}).
		Run(context.Background())

	var budget *laser.BudgetExceededError
	if !errors.As(err, &budget) || budget.Breach.Ceiling != 50_000 || bodies != 0 {
		t.Fatalf("want the wall-clock budget refused before the step, got %v after %d bodies", err, bodies)
	}
}

func TestGivenATokenBudgetWhenAWorkflowRunsThenShouldStartItsSessionUnderThatBudget(t *testing.T) {
	server := newWorkflowServer()
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	_, err := handle.Workflow(agentID(t, "pipeline"), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")).
		Budget(laser.WorkflowBudget{}.WithTokens(500)).
		StepLocal("only", noopStep).
		Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	records := server.on("agent.sessions")
	if len(records) == 0 {
		t.Fatal("want a session start record on agent.sessions")
	}
	var envelope wire.AgentEnvelope
	var start wire.SessionStart
	if err := wire.Unmarshal(records[0].Payload, &envelope); err != nil {
		t.Fatalf("decode the start record: %v", err)
	}
	if err := wire.Unmarshal(envelope.Body, &start); err != nil {
		t.Fatalf("decode the start body: %v", err)
	}
	if start.Budget == nil || start.Budget.Tokens == nil || *start.Budget.Tokens != 500 || start.Budget.CostMicros != nil {
		t.Fatalf("want the run session started under 500 tokens, got %+v", start.Budget)
	}
}

// requestCancel writes an operator's cancel request for a run that has not started.
func requestCancel(t *testing.T, handle *laser.Laser) wire.ConversationID {
	t.Helper()
	runID := laser.NewConversationID()
	if _, err := handle.Sessions().Control("agents", runID).AsOperator(agentID(t, "operator")).
		Cancel(context.Background()); err != nil {
		t.Fatalf("request the cancel: %v", err)
	}
	return runID
}

// lastRunTaskState is the task state of the last record on the session lane.
func lastRunTaskState(t *testing.T, server *workflowServer) wire.TaskState {
	t.Helper()
	records := server.on("agent.sessions")
	if len(records) == 0 {
		t.Fatal("want records on agent.sessions")
	}
	var envelope wire.AgentEnvelope
	if err := wire.Unmarshal(records[len(records)-1].Payload, &envelope); err != nil {
		t.Fatalf("decode the last session record: %v", err)
	}
	if envelope.TaskState == nil {
		t.Fatal("want a task state on the last session record")
	}
	return *envelope.TaskState
}
