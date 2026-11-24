package laser_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenTwoCapableAgentsWhenScatteredThenShouldGatherBothCompletions(t *testing.T) {
	server := &contractServer{
		repliesByTarget: map[string]contractReply{
			"worker-a": {kind: wire.KindResponse, body: []byte("a"), source: agentID(t, "worker-a")},
			"worker-b": {kind: wire.KindResponse, body: []byte("b"), source: agentID(t, "worker-b")},
		},
	}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	registry := registryWith(t, time.Now(), map[string]wire.AgentCard{
		"worker-a": cardFor("review", nil, nil, nil, nil),
		"worker-b": cardFor("review", nil, nil, nil, nil),
	})

	report, err := handle.ScatterReport(context.Background(), laser.ScatterRequest{
		Skill: "review", Routing: laser.RouteAny, Registry: registry,
		From: agentID(t, "orchestrator"), Topic: "agent.sessions",
		Payload: []byte(`{"task":"run"}`), Policy: laser.GatherAll(),
		Deadline: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("scatter: %v", err)
	}
	if len(report.Outcomes) != 2 {
		t.Fatalf("want two outcomes, got %d", len(report.Outcomes))
	}
	if len(report.Completed()) != 2 {
		t.Fatalf("want both branches completed, got %d", len(report.Completed()))
	}
	if !report.Satisfied {
		t.Error("want GatherAll satisfied when every branch completed")
	}
	if len(report.Cancelled) != 0 {
		t.Errorf("want nothing cancelled, got %v", report.Cancelled)
	}
}

func TestGivenAQuorumWhenScatteredThenShouldStopAfterEnoughSuccessesAndCancelTheRest(t *testing.T) {
	server := &contractServer{
		repliesByTarget: map[string]contractReply{
			"worker-a": {kind: wire.KindResponse, body: []byte("a"), source: agentID(t, "worker-a")},
			"worker-b": {kind: wire.KindResponse, body: []byte("b"), source: agentID(t, "worker-b")},
		},
		silent: map[string]bool{"worker-c": true},
	}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	registry := registryWith(t, time.Now(), map[string]wire.AgentCard{
		"worker-a": cardFor("review", nil, nil, nil, nil),
		"worker-b": cardFor("review", nil, nil, nil, nil),
		"worker-c": cardFor("review", nil, nil, nil, nil),
	})

	report, err := handle.ScatterReport(context.Background(), laser.ScatterRequest{
		Skill: "review", Routing: laser.RouteAny, Registry: registry,
		From: agentID(t, "orchestrator"), Topic: "agent.sessions",
		Payload: []byte(`{"task":"run"}`), Policy: laser.GatherQuorum(2),
		Deadline: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("scatter: %v", err)
	}
	if len(report.Completed()) != 2 {
		t.Fatalf("want two completions, got %d", len(report.Completed()))
	}
	if !report.Satisfied {
		t.Error("want the quorum satisfied")
	}
	if len(report.Cancelled) != 1 || report.Cancelled[0].String() != "worker-c" {
		t.Errorf("want the silent branch cancelled, got %v", report.Cancelled)
	}
}

func TestGivenBestEffortWhenScatteredThenShouldTakeWhateverLandedByTheDeadline(t *testing.T) {
	server := &contractServer{
		repliesByTarget: map[string]contractReply{
			"worker-a": {kind: wire.KindResponse, body: []byte("a"), source: agentID(t, "worker-a")},
		},
		silent: map[string]bool{"worker-b": true},
	}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	registry := registryWith(t, time.Now(), map[string]wire.AgentCard{
		"worker-a": cardFor("review", nil, nil, nil, nil),
		"worker-b": cardFor("review", nil, nil, nil, nil),
	})

	report, err := handle.ScatterReport(context.Background(), laser.ScatterRequest{
		Skill: "review", Routing: laser.RouteAny, Registry: registry,
		From: agentID(t, "orchestrator"), Topic: "agent.sessions",
		Payload: []byte(`{"task":"run"}`), Policy: laser.GatherBestEffort(),
		Deadline: 300 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("scatter: %v", err)
	}
	if len(report.Completed()) != 1 {
		t.Fatalf("want one completion, got %d", len(report.Completed()))
	}
	if !report.Satisfied {
		t.Error("want best-effort satisfied once anything landed")
	}
	if len(report.Cancelled) != 1 || report.Cancelled[0].String() != "worker-b" {
		t.Errorf("want the silent branch cancelled, got %v", report.Cancelled)
	}
}

func TestGivenAMixOfOutcomesWhenScatteredThenShouldReportEachAgentsOwnOutcome(t *testing.T) {
	message := "the task failed"
	body, err := wire.Marshal(wire.AgentErrorBody{Code: wire.ErrorInternal, Message: &message})
	if err != nil {
		t.Fatalf("encode the error body: %v", err)
	}
	server := &contractServer{
		repliesByTarget: map[string]contractReply{
			"worker-a": {kind: wire.KindResponse, body: []byte("a"), source: agentID(t, "worker-a")},
			"worker-b": {kind: wire.KindError, body: body, source: agentID(t, "worker-b")},
		},
	}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	registry := registryWith(t, time.Now(), map[string]wire.AgentCard{
		"worker-a": cardFor("review", nil, nil, nil, nil),
		"worker-b": cardFor("review", nil, nil, nil, nil),
	})

	report, err := handle.ScatterReport(context.Background(), laser.ScatterRequest{
		Skill: "review", Routing: laser.RouteAny, Registry: registry,
		From: agentID(t, "orchestrator"), Topic: "agent.sessions",
		Payload: []byte(`{"task":"run"}`), Policy: laser.GatherAll(),
		Deadline: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("scatter: %v", err)
	}
	if len(report.Completed()) != 1 {
		t.Fatalf("want one completion, got %d", len(report.Completed()))
	}
	if len(report.Failures()) != 0 {
		t.Fatalf("want no infrastructure failures, got %d", len(report.Failures()))
	}
	var failedOutcome *laser.ScatterOutcome
	for index := range report.Outcomes {
		outcome := &report.Outcomes[index]
		if outcome.Agent.String() == "worker-b" {
			failedOutcome = outcome
		}
	}
	if failedOutcome == nil {
		t.Fatal("want worker-b's outcome present")
	}
	if failedOutcome.Err != nil || failedOutcome.Contract.State != laser.ContractFailed {
		t.Errorf("want worker-b's branch reported as a failed contract, got err=%v state=%v",
			failedOutcome.Err, failedOutcome.Contract)
	}
	// GatherAll is satisfied only when every branch completed, not merely
	// finished: one of these two failed, so the policy was not met even
	// though nothing timed out or was cancelled.
	if report.Satisfied {
		t.Error("want GatherAll unsatisfied when one branch failed rather than completed")
	}
}

func TestGivenNoCapableAgentWhenScatteredThenShouldRefuse(t *testing.T) {
	handle := laser.FromClient(&contractServer{}, laser.Options{DefaultStream: "agents"})

	_, err := handle.ScatterReport(context.Background(), laser.ScatterRequest{
		Skill: "review", Registry: laser.NewAgentRegistry(),
		From: agentID(t, "orchestrator"), Topic: "agent.sessions",
		Payload: []byte(`{"task":"run"}`), Policy: laser.GatherAll(),
	})

	var noCapable *laser.NoCapableAgentError
	if !errors.As(err, &noCapable) {
		t.Fatalf("want a no-capable-agent refusal, got %T: %v", err, err)
	}
}

func TestGivenAZeroQuorumWhenScatteredThenShouldRefuseBeforeAnyRead(t *testing.T) {
	handle := laser.FromClient(&contractServer{}, laser.Options{DefaultStream: "agents"})
	registry := registryWith(t, time.Now(), map[string]wire.AgentCard{
		"worker": cardFor("review", nil, nil, nil, nil),
	})

	_, err := handle.ScatterReport(context.Background(), laser.ScatterRequest{
		Skill: "review", Registry: registry,
		From: agentID(t, "orchestrator"), Topic: "agent.sessions",
		Payload: []byte(`{"task":"run"}`), Policy: laser.GatherQuorum(0),
	})

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an invalid-argument refusal, got %T: %v", err, err)
	}
}

func TestGivenACompletedScatterWhenScatterCollectsThenShouldReturnOnlyCompletedBodies(t *testing.T) {
	message := "the task failed"
	body, err := wire.Marshal(wire.AgentErrorBody{Code: wire.ErrorInternal, Message: &message})
	if err != nil {
		t.Fatalf("encode the error body: %v", err)
	}
	server := &contractServer{
		repliesByTarget: map[string]contractReply{
			"worker-a": {kind: wire.KindResponse, body: []byte("a"), source: agentID(t, "worker-a")},
			"worker-b": {kind: wire.KindError, body: body, source: agentID(t, "worker-b")},
		},
	}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	registry := registryWith(t, time.Now(), map[string]wire.AgentCard{
		"worker-a": cardFor("review", nil, nil, nil, nil),
		"worker-b": cardFor("review", nil, nil, nil, nil),
	})

	replies, err := handle.Scatter(context.Background(), laser.ScatterRequest{
		Skill: "review", Routing: laser.RouteAny, Registry: registry,
		From: agentID(t, "orchestrator"), Topic: "agent.sessions",
		Payload: []byte(`{"task":"run"}`), Policy: laser.GatherAll(),
		Deadline: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("scatter: %v", err)
	}
	if len(replies) != 1 || string(replies[0]) != "a" {
		t.Fatalf("want only the completed branch's body, got %v", replies)
	}
}
