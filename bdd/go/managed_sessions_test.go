package bdd_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	bdd "github.com/laserdata/laser-sdk/bdd/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// TestGivenTheSharedManagedSessionScenariosWhenRunThenShouldHold runs the
// managed session index feature: a deployment that folds every registered
// stream into a session index, read without scanning the log.
func TestGivenTheSharedManagedSessionScenariosWhenRunThenShouldHold(t *testing.T) {
	runFeature(t, "managed_sessions.feature", 5*time.Minute, managedSessionStep)
}

// managedSessionFlow is the session a scenario ran and the agents in it.
type managedSessionFlow struct {
	writer *laser.Laser

	lease *laser.SessionLease
	watch *laser.SessionWatch

	session        *laser.Session
	label          string
	sessions       *laser.Sessions
	config         laser.SessionConfig
	partitions     uint32
	byLabel        map[string]*laser.Session
	retained       *laser.Session
	retainedState  *laser.SessionState
	retainedSource wire.SourceRef
	firstPage      *wire.SessionPage
	remembered     laser.MemoryID
	second         *laser.Sessions
	secondSession  *laser.Session
	workflowRun    wire.ConversationID
	worker         *managedWorker
	children       []wire.ConversationID
}

func managedSessionFlowOf(state *world) *managedSessionFlow {
	if existing, found := state.get("managed-sessions"); found {
		return existing.(*managedSessionFlow)
	}
	flow := &managedSessionFlow{byLabel: make(map[string]*laser.Session)}
	state.put("managed-sessions", flow)
	return flow
}

func managedSessionStep(ctx context.Context, state *world, step bdd.Step) error {
	handled, err := background(ctx, state, step)
	if handled {
		return err
	}
	flow := managedSessionFlowOf(state)
	text := step.Text
	arguments := argumentsOf(step)

	switch {
	case strings.HasPrefix(text, "a managed session stream in the") || strings.HasPrefix(text, "a managed session stream whose agents heartbeat"):
		return managedStreamInLayout(ctx, state, text)

	case strings.Contains(text, "runs the session") &&
		strings.Contains(text, "with one command answered by"):
		if len(arguments) != 3 {
			return fmt.Errorf("want an agent, a label and an answering agent, got %v",
				arguments)
		}
		return runAnsweredSession(ctx, state, flow, arguments[0], arguments[1], arguments[2])

	case strings.HasPrefix(text, "the indexed session") &&
		strings.Contains(text, "is completed by"):
		events, named := after(text, "with ")
		if len(arguments) != 2 || !named {
			return fmt.Errorf("want a label, an agent and an event count in %q", text)
		}
		return indexedSessionCompleted(ctx, state, flow, arguments[0], arguments[1], events)

	case strings.HasPrefix(text, "the stream counts") &&
		strings.Contains(text, "completed session"):
		wanted, named := after(text, "the stream counts ")
		if !named {
			return fmt.Errorf("want a session count in %q", text)
		}
		return streamCountsCompleted(ctx, state, wanted)

	case text == "the client has seen session positions for the stream":
		return seenSessionPositions(state)

	default:
		return managedSessionIndexStep(ctx, state, flow, text, arguments)
	}
}

// managedStreamInLayout opens a fresh stream and declares the layout the
// scenario names, then bootstraps it so the layout's topics exist.
//
// Every layout has to yield the same indexed row, which is the point of the
// outline: where the records physically go must not change what a reader
// sees.
func managedStreamInLayout(ctx context.Context, state *world, text string) error {
	if err := state.freshStream(ctx); err != nil {
		return err
	}
	planner, err := bridgeAgent("planner")
	if err != nil {
		return err
	}
	worker, err := bridgeAgent("worker")
	if err != nil {
		return err
	}
	var layout laser.SessionLayout
	switch {
	case strings.Contains(text, "per-agent topic"):
		layout = laser.PerAgentTopicLayout(map[wire.AgentID]string{
			planner: "agent.planner", worker: "agent.worker",
		})
	case strings.Contains(text, "declared partitions"):
		layout = laser.PerAgentPartitionLayout(map[wire.AgentID]uint32{
			planner: 0, worker: 1,
		})
	case strings.Contains(text, "single partition"):
		layout = laser.SinglePartitionLayout()
	default:
		layout = laser.SharedLayout()
	}
	flow := managedSessionFlowOf(state)
	flow.config = laser.SessionConfig{Layout: layout}
	if seconds, ok := after(text, "heartbeat every "); ok {
		flow.config.Heartbeat = time.Duration(seconds) * time.Second
	}
	sessions := state.client.SessionsWith(flow.config)
	partitions := uint32(2)
	if layout.Kind() == laser.LayoutSinglePartition {
		partitions = 1
	}
	// Sessions.Bootstrap, not the plain Laser.Bootstrap: the plain one
	// creates the topics but never registers the stream as a session
	// source, so every read through the managed session index answers
	// "stream is not registered for sessions". The shared harness step was
	// fixed for exactly this once before; this feature has its own setup
	// and kept the plain call, which only a managed deployment can show --
	// open Iggy has no index to be unregistered with.
	flow.sessions, flow.partitions = sessions, partitions
	_, err = sessions.Bootstrap(ctx, partitions, laser.ExpireAfter(24*time.Hour))
	return err
}

// runAnsweredSession runs one session end to end: it begins, sends one
// command the other agent answers, and completes.
//
// Four events is what the scenario expects, which is the start, the command,
// the answer and the completion.
func runAnsweredSession(
	ctx context.Context, state *world, flow *managedSessionFlow,
	agent, label, answering string,
) error {
	source, err := bridgeAgent(agent)
	if err != nil {
		return err
	}
	target, err := bridgeAgent(answering)
	if err != nil {
		return err
	}
	session := flow.sessions.Create(label).As(source)
	_, lease, err := session.Begin(ctx)
	if err != nil {
		return err
	}
	if lease != nil {
		defer lease.Release()
	}
	flow.session, flow.label = session, label

	lane, err := session.Agdx()
	if err != nil {
		return err
	}
	correlation := wire.CorrelationID{ID: wire.IDFromBytes(laser.NewOperationID())}
	if _, err := lane.Command(correlation, []byte("do it")).
		WithTarget(target).Send(ctx); err != nil {
		return err
	}
	// The answer is written as the target rather than run through a
	// consumer: the scenario is about what the index folds, not about an
	// agent loop reading the command.
	answer := state.client.AgdxOn(state.stream, wire.AgentSessions,
		target, session.Conversation())
	if _, err := answer.Respond(correlation, []byte("done")).
		WithTarget(source).Send(ctx); err != nil {
		return err
	}
	_, err = session.Complete(ctx, "")
	return err
}

// indexedSessionCompleted reads the session back from the index and checks
// the row the fold built.
func indexedSessionCompleted(
	ctx context.Context, state *world, flow *managedSessionFlow,
	label, agent string, events uint64,
) error {
	if flow.session == nil || flow.label != label {
		return fmt.Errorf("the scenario ran no session %q", label)
	}
	expected, err := bridgeAgent(agent)
	if err != nil {
		return err
	}
	// The fold is asynchronous, so the read waits for the row rather than
	// racing it.
	deadline := time.Now().Add(30 * time.Second)
	var info *wire.SessionInfo
	for time.Now().Before(deadline) {
		info, err = state.client.Sessions().Get(ctx, flow.session.Conversation())
		// A session the index has not folded yet is refused, not answered
		// empty, so "not found" is a reason to keep waiting rather than to
		// give up. Returning on it defeated the wait entirely: against a
		// real managed deployment the first poll always lands before the
		// fold, so this read never got a second attempt.
		if err != nil && !sessionNotYetFolded(err) {
			return err
		}
		if err == nil && info != nil &&
			info.Status == wire.SessionCompleted && info.Events >= events {
			break
		}
		info = nil
		time.Sleep(200 * time.Millisecond)
	}
	if info == nil {
		return fmt.Errorf("the index holds no session %q", label)
	}
	if info.Status != wire.SessionCompleted {
		return fmt.Errorf("want session %q completed, got %v", label, info.Status)
	}
	if info.Agent == nil || *info.Agent != expected {
		return fmt.Errorf("want session %q completed by %q, got %v", label, agent, info.Agent)
	}
	if info.Events != events {
		return fmt.Errorf("want session %q to hold %d events, got %d",
			label, events, info.Events)
	}
	return nil
}

// streamCountsCompleted asserts how many completed sessions the stream's
// index holds.
func streamCountsCompleted(ctx context.Context, state *world, wanted uint64) error {
	page, err := state.client.Sessions().List().
		Status(wire.SessionCompleted).
		Fetch(ctx)
	if err != nil {
		return err
	}
	if uint64(len(page.Items)) != wanted {
		return fmt.Errorf("want %d completed sessions, got %d", wanted, len(page.Items))
	}
	return nil
}

// seenSessionPositions asserts the session reads gave the client the
// positions the fold applied, which is what makes a later read keep up with
// this client's own writes.
func seenSessionPositions(state *world) error {
	if len(state.client.SeenPositions().Sessions) == 0 {
		return fmt.Errorf("want the client to have seen session positions")
	}
	return nil
}

// sessionNotYetFolded reports a session read the index refused because it has
// not folded the session yet, which a wait should treat as "not yet" rather
// than as a failure.
func sessionNotYetFolded(err error) bool {
	var refusal *laser.SessionError
	if !errors.As(err, &refusal) {
		return false
	}
	return refusal.Failure.Kind == wire.SessionNotFound ||
		refusal.Failure.Kind == wire.SessionNotRegistered
}
