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

	report, err := handle.ScatterReport(context.Background(), laser.ScatterRequest{Inbox: &laser.InboxRoute{Kind: laser.InboxFixed, Topic: wire.AgentSessions},
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

	report, err := handle.ScatterReport(context.Background(), laser.ScatterRequest{Inbox: &laser.InboxRoute{Kind: laser.InboxFixed, Topic: wire.AgentSessions},
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

	report, err := handle.ScatterReport(context.Background(), laser.ScatterRequest{Inbox: &laser.InboxRoute{Kind: laser.InboxFixed, Topic: wire.AgentSessions},
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

	report, err := handle.ScatterReport(context.Background(), laser.ScatterRequest{Inbox: &laser.InboxRoute{Kind: laser.InboxFixed, Topic: wire.AgentSessions},
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

	_, err := handle.ScatterReport(context.Background(), laser.ScatterRequest{Inbox: &laser.InboxRoute{Kind: laser.InboxFixed, Topic: wire.AgentSessions},
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

	_, err := handle.ScatterReport(context.Background(), laser.ScatterRequest{Inbox: &laser.InboxRoute{Kind: laser.InboxFixed, Topic: wire.AgentSessions},
		Skill: "review", Registry: registry,
		From: agentID(t, "orchestrator"), Topic: "agent.sessions",
		Payload: []byte(`{"task":"run"}`), Policy: laser.GatherQuorum(0),
	})

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an invalid-argument refusal, got %T: %v", err, err)
	}
}

func TestGivenAVerifierWhenScatteredThenShouldOnlyCompleteBranchesSignedByTheirOwnTarget(t *testing.T) {
	key, err := laser.GenerateSigningKey()
	if err != nil {
		t.Fatalf("generate a signing key: %v", err)
	}
	keys := laser.NewKeyRegistry()
	// Enrolled as worker-a only: worker-b's branch expects a reply signed as
	// "worker-b", so the same key answering on its behalf still fails that
	// branch's own ExpectedSigner check.
	keys.Enroll("worker-a", key.VerifyingKey())
	server := &contractServer{
		repliesByTarget: map[string]contractReply{
			"worker-a": {kind: wire.KindResponse, body: []byte("a"), source: agentID(t, "worker-a")},
			"worker-b": {kind: wire.KindResponse, body: []byte("b"), source: agentID(t, "worker-b")},
		},
		replyKey: key,
	}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	registry := registryWith(t, time.Now(), map[string]wire.AgentCard{
		"worker-a": cardFor("review", nil, nil, nil, nil),
		"worker-b": cardFor("review", nil, nil, nil, nil),
	})

	report, err := handle.ScatterReport(context.Background(), laser.ScatterRequest{Inbox: &laser.InboxRoute{Kind: laser.InboxFixed, Topic: wire.AgentSessions},
		Skill: "review", Routing: laser.RouteAny, Registry: registry,
		From: agentID(t, "orchestrator"), Topic: "agent.sessions",
		Payload: []byte(`{"task":"run"}`), Policy: laser.GatherBestEffort(),
		Deadline: 300 * time.Millisecond, Verifier: keys,
	})
	if err != nil {
		t.Fatalf("scatter: %v", err)
	}
	completed := report.Completed()
	if len(completed) != 1 || completed[0].Agent.String() != "worker-a" {
		t.Fatalf("want only worker-a's branch (signed as its own expected principal) to complete, got %+v",
			completed)
	}
	if completed[0].Contract.Reply.VerifiedPrincipal == nil ||
		*completed[0].Contract.Reply.VerifiedPrincipal != "worker-a" {
		t.Errorf("want the verified principal on the completed branch's reply, got %v",
			completed[0].Contract.Reply.VerifiedPrincipal)
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

	replies, err := handle.Scatter(context.Background(), laser.ScatterRequest{Inbox: &laser.InboxRoute{Kind: laser.InboxFixed, Topic: wire.AgentSessions},
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

// A scatter can reach agents that share no topic: each branch's command goes
// to that agent's own inbox, while every answer comes back to the topic the
// caller named and watches.
func TestGivenAnAdvertisedInboxWhenScatteredThenShouldAddressEachAgentsOwnTopic(t *testing.T) {
	now := time.Now()
	registry, _ := seedPresence(t, []wire.ClientMetadata{
		advertised(t, "alpha", "alpha-inbox", principalOf(1)),
		advertised(t, "beta", "beta-inbox", principalOf(1)),
	})
	registry.Observe(agentID(t, "alpha"), cardFor("settle", nil, nil, nil, nil), now)
	registry.Observe(agentID(t, "beta"), cardFor("settle", nil, nil, nil, nil), now)
	server := &contractServer{
		replyKind: wire.KindResponse, replyBody: []byte("done"),
		replySource: agentID(t, "alpha"),
		answerOn:    "agent.sessions",
	}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	advertisedRoute := laser.AdvertisedInbox()
	report, err := handle.ScatterReport(context.Background(), laser.ScatterRequest{
		Skill:    "settle",
		Registry: registry,
		From:     agentID(t, "orchestrator"),
		Topic:    "agent.sessions",
		Payload:  []byte(`{"task":"settle"}`),
		Policy:   laser.GatherAll(),
		Inbox:    &advertisedRoute,
		Deadline: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("scatter: %v", err)
	}
	if len(report.Completed()) != 2 {
		t.Fatalf("want both branches completed, got %d of %d outcomes",
			len(report.Completed()), len(report.Outcomes))
	}
	for _, topic := range []string{"alpha-inbox", "beta-inbox"} {
		if len(server.storedOn(topic)) == 0 {
			t.Errorf("want a command addressed to %s", topic)
		}
	}
}
