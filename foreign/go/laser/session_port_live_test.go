//go:build integration

package laser_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenALiveSessionWhenTravellingToACheckpointThenShouldReproduceItAndCatchUp(t *testing.T) {
	ctx := liveContext(t)
	handle := liveScratch(t, dialLive(t), ctx, "laser-session-travel").Laser()
	if err := handle.Bootstrap(ctx, 1, laser.ExpireAfter(time.Hour)); err != nil {
		t.Fatalf("bootstrap the stream: %v", err)
	}
	session := handle.Sessions().Start().As(agentID(t, "planner")).Tag("live")
	_, lease, err := session.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	lane, err := session.Agdx()
	if err != nil {
		t.Fatalf("lane: %v", err)
	}
	if _, err := lane.Emit([]byte("first")).Send(ctx); err != nil {
		t.Fatalf("emit: %v", err)
	}
	checkpoint, err := session.Checkpoint(ctx)
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	err = session.Run(ctx, lease, func(ctx context.Context, inside *laser.Session) error {
		_, err := lane.Emit([]byte("second")).Send(ctx)
		return err
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	count := func(total int, _ laser.SessionTurn) int { return total + 1 }
	before, err := laser.SessionStateAt(ctx, session, checkpoint, 0, count)
	if err != nil {
		t.Fatalf("state at: %v", err)
	}
	after, err := session.TurnsSince(ctx, checkpoint)
	if err != nil {
		t.Fatalf("turns since: %v", err)
	}
	// The start and the first event precede the checkpoint. The second event and the completion follow it.
	if before != 2 || len(after) != 2 || after[0].Text() != "second" ||
		after[1].Display != wire.DisplaySessionCompleted {
		t.Fatalf("want 2 records before and the event then the completion after, got %d and %+v", before, after)
	}
	ended, err := session.EndedAs(ctx)
	if err != nil || ended != wire.SessionCompleted {
		t.Fatalf("want the run to end the session completed, got %q, %v", ended, err)
	}
}

func TestGivenALiveParentSessionWhenAChildBeginsWithItsAncestryThenShouldWriteTheStart(t *testing.T) {
	ctx := liveContext(t)
	handle := liveScratch(t, dialLive(t), ctx, "laser-session-child").Laser()
	if err := handle.Bootstrap(ctx, 1, laser.ExpireAfter(time.Hour)); err != nil {
		t.Fatalf("bootstrap the stream: %v", err)
	}
	parent := handle.Sessions().Start().As(agentID(t, "planner"))
	_, parentLease, err := parent.Begin(ctx)
	if err != nil {
		t.Fatalf("begin the parent: %v", err)
	}
	defer parentLease.Release()

	child := handle.Sessions().Create("research").As(agentID(t, "researcher")).
		WithAncestry(parent.Conversation(), parent.Conversation())
	_, childLease, err := child.Begin(ctx)
	if err != nil {
		t.Fatalf("begin the child: %v", err)
	}
	defer childLease.Release()

	checkpoint, err := child.Checkpoint(ctx)
	if err != nil {
		t.Fatalf("checkpoint the child: %v", err)
	}
	turns, err := child.TurnsAt(ctx, checkpoint)
	if err != nil {
		t.Fatalf("read the child: %v", err)
	}
	if len(turns) != 1 || turns[0].Display != wire.DisplaySessionStarted {
		t.Fatalf("want the child's start on the lane, got %+v", turns)
	}
}

func TestGivenADeclaredAgentWhenSpawnedWithoutATopicThenShouldServeWorkFromItsDeclaredTopic(t *testing.T) {
	ctx := liveContext(t)
	handle := liveScratch(t, dialLive(t), ctx, "laser-declared-spawn").Laser()
	worker, controller := agentID(t, "worker"), agentID(t, "controller")
	sessions := handle.SessionsWith(laser.SessionConfig{
		Layout: laser.PerAgentTopicLayout(map[wire.AgentID]string{worker: "worker.inbox"}),
	})
	if _, err := sessions.Bootstrap(ctx, 1, laser.ExpireAfter(time.Hour)); err != nil {
		t.Fatalf("bootstrap the layout: %v", err)
	}
	spawned, err := handle.SpawnAgent(ctx, worker, laser.SpawnOptions{
		RuntimeOptions: laser.RuntimeOptions{RespondOn: wire.AgentSessions, Sessions: sessions},
	}, laser.HandlerFunc(func(ctx context.Context, message *laser.AgentMessage, agent *laser.AgentCtx) error {
		if message.Envelope == nil || message.Envelope.Kind != wire.KindCommand {
			return nil
		}
		return agent.Respond(ctx, []byte("served"))
	}))
	if err != nil {
		t.Fatalf("spawn the worker: %v", err)
	}
	stopLiveAgent(t, spawned)
	if err := spawned.Ready(ctx); err != nil {
		t.Fatalf("wait for the worker: %v", err)
	}

	contract, err := handle.Contract(laser.To(worker), wire.AgentSessions).
		InboxRoute(laser.FixedInbox(wire.AgentSessions)).
		From(controller).Payload([]byte("review this")).Deadline(10 * time.Second).Send(ctx)
	if err != nil {
		t.Fatalf("contract: %v", err)
	}

	// Under the layout a directed command leaves the lane, so only a worker reading its declared topic answers.
	if contract.State != laser.ContractCompleted || string(contract.Reply.Envelope.Body) != "served" {
		t.Fatalf("want the work served from the declared topic, got %v with %+v", contract.State, contract.Reply)
	}
}

func TestGivenALiveApproverWhenInputIsRequestedThenShouldReturnItsAnswerOrItsRejection(t *testing.T) {
	ctx := liveContext(t)
	handle := liveScratch(t, dialLive(t), ctx, "laser-request-input").Laser()
	if err := handle.Bootstrap(ctx, 1, laser.ExpireAfter(time.Hour)); err != nil {
		t.Fatalf("bootstrap the stream: %v", err)
	}
	approver, planner := agentID(t, "approver"), agentID(t, "planner")
	spawned, err := handle.SpawnAgent(ctx, approver, laser.SpawnOptions{
		RuntimeOptions: laser.RuntimeOptions{
			RespondOn: wire.AgentSessions, Operations: []string{wire.OperationInputRequest},
		},
	}, laser.HandlerFunc(func(ctx context.Context, message *laser.AgentMessage, agent *laser.AgentCtx) error {
		if string(message.Body()) == "delete it?" {
			return &laser.InvalidError{Reason: "not allowed"}
		}
		return agent.RespondInput(ctx, wire.AgentSessions, []byte("approved"))
	}))
	if err != nil {
		t.Fatalf("spawn the approver: %v", err)
	}
	stopLiveAgent(t, spawned)
	if err := spawned.Ready(ctx); err != nil {
		t.Fatalf("wait for the approver: %v", err)
	}
	producer := handle.Agdx(planner, laser.NewConversationID())

	answer, err := producer.RequestInputFrom(ctx, approver, wire.AgentSessions, []byte("ship it?"), 10*time.Second)
	if err != nil {
		t.Fatalf("request input: %v", err)
	}
	_, refusal := producer.RequestInputFrom(ctx, approver, wire.AgentSessions, []byte("delete it?"), 10*time.Second)

	var rejected *laser.InputRejectedError
	if string(answer) != "approved" || !errors.As(refusal, &rejected) {
		t.Fatalf("want an approval then a rejection, got %q and %v", answer, refusal)
	}
}

func TestGivenALiveSessionWhenAnOperatorPausesThenCancelsThenShouldReadBothOutsideAHandler(t *testing.T) {
	ctx := liveContext(t)
	handle := liveScratch(t, dialLive(t), ctx, "laser-session-control").Laser()
	sessions := handle.Sessions()
	if _, err := sessions.Bootstrap(ctx, 1, laser.ExpireAfter(time.Hour)); err != nil {
		t.Fatalf("bootstrap the stream: %v", err)
	}
	worker := agentID(t, "worker")
	session := sessions.Start().As(worker).Tag("live")
	_, lease, err := session.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer lease.Release()
	control := sessions.Control(sessions.Stream(), session.Conversation()).
		AsOperator(agentID(t, "ops")).Participants(worker)

	if pending, err := session.PendingControl(ctx); err != nil || pending != (laser.PendingControl{}) {
		t.Fatalf("want nothing pending before an operator acts, got %+v, %v", pending, err)
	}
	if _, err := control.Pause(ctx); err != nil {
		t.Fatalf("pause: %v", err)
	}
	paused, err := session.PendingControl(ctx)
	if err != nil {
		t.Fatalf("pending control: %v", err)
	}
	if _, err := control.Cancel(ctx); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	canceled, err := session.CancelRequested(ctx)
	if err != nil {
		t.Fatalf("cancel requested: %v", err)
	}

	if !paused.PauseRequested || paused.CancelRequested || !canceled {
		t.Fatalf("want the pause then the cancel read, got %+v and %v", paused, canceled)
	}
	if !handle.Capabilities().Sessions {
		return
	}
	// The index folds the lane after the write, so the status is read until it shows the session.
	deadline := time.Now().Add(20 * time.Second)
	for {
		info, err := session.Status(ctx)
		if err == nil && info != nil && info.ID == session.Conversation() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the session index never showed the session: %+v, %v", info, err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("the session index never showed the session: %v", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func stopLiveAgent(t *testing.T, spawned *laser.AgentHandle) {
	t.Helper()
	t.Cleanup(func() {
		stop, cancel := context.WithTimeout(context.Background(), liveOperationTimeout)
		defer cancel()
		if err := spawned.Shutdown(stop); err != nil {
			t.Errorf("stop the agent: %v", err)
		}
	})
}
