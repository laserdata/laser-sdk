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
//
// This runner covers the layout and index-read core. The rest of the feature
// is planned rather than implemented: each entry below names what that
// scenario needs that this runner does not yet set up, and the harness
// reports those as skips naming the reason. A scenario absent from the plan
// and missing a step implementation still fails, so nothing here can pass
// vacuously. Shrinking the plan is the work.
func TestGivenTheSharedManagedSessionScenariosWhenRunThenShouldHold(t *testing.T) {
	runFeaturePlanned(t, "managed_sessions.feature", 5*time.Minute, managedSessionStep,
		map[string]string{
			"A2A and MCP children roll up under their root": "" +
				"needs the A2A and MCP bridges wired into the session tree read",
			"A fan-out branch rolls up under its root": "" +
				"needs a fan-out branch read back through the session tree",
			"A resumed workflow run opens no phantom child": "" +
				"needs the session tree read of a workflow run",
			"A resumed workflow run never runs a completed step again": "" +
				"needs a counting agent spawned against the managed stream",
			"Untargeted low-altitude work reaches a filter-bound agent": "" +
				"needs a filter-bound consumer group on the managed stream",
			"A recalled memory item links to the session": "" +
				"needs the memory-to-session link read",
			"State history keeps old and new digests": "" +
				"needs the state history read",
			"A state write right after the session starts is accepted": "" +
				"needs the state write path against the index",
			"A graph node re-observed by a second session links to both": "" +
				"needs the graph element's session links read",
			"A key-value write inside a session appears under the session lens": "" +
				"needs the session-scoped key-value lens read",
			"A linked write right after bootstrap keeps its link": "" +
				"needs the linked-write path against the index",
			"A session reference naming another stream is refused at the edge": "" +
				"needs the edge refusal of a cross-stream session reference",
			"Heartbeats keep a quiet session live and never appear in its events": "" +
				"needs a held lease heartbeating against the index",
			"The change feed holds one row per fold batch and only its stream's sessions": "" +
				"needs the session change feed read",
			"Every read answers a finished session from the index": "" +
				"needs every index read shape asserted against one finished session",
			"A session list walk keeps its first page's bound while a session starts": "" +
				"needs a paged list walk with a session starting mid-walk",
			"The runtime ends a session over its budget instead of handling its work": "" +
				"needs the budget gate wired to a runtime on the managed stream",
			// These three are Scenario Outlines: ParseFeature expands each
			// Examples row into its own scenario, named "<title> [<row
			// values, in column order>]" (gherkin.go's (*outline).expand), not
			// the bare title.
			//
			// All 14 skipped for want of a LaneGuard-equivalent. **That is no
			// longer the reason**: the client capability shipped on
			// 2026-11-27 as laser/session_lane.go (laneGuard, laneIdentity,
			// the public laser.StaleLaneError), checked on every session
			// lifecycle and state write. What is still missing is this
			// runner's own steps -- retaining handles, changing the source's
			// partition count or topic/stream generation, removing and
			// re-registering the source -- none of which are written yet.
			// Writing them is the remaining work; the client is ready for
			// them.
			"Retained handles refuse changed lane identity before writes [shared, partition count]": "" +
				"the client's lane guard exists; this runner has no steps for it yet (see the note above)",
			"Retained handles refuse changed lane identity before writes [shared, topic generation]": "" +
				"the client's lane guard exists; this runner has no steps for it yet (see the note above)",
			"Retained handles refuse changed lane identity before writes [per-agent topic, partition count]": "" +
				"the client's lane guard exists; this runner has no steps for it yet (see the note above)",
			"Retained handles refuse changed lane identity before writes [per-agent topic, topic generation]": "" +
				"the client's lane guard exists; this runner has no steps for it yet (see the note above)",
			"Retained handles refuse changed lane identity before writes [declared partitions, partition count]": "" +
				"the client's lane guard exists; this runner has no steps for it yet (see the note above)",
			"Retained handles refuse changed lane identity before writes [declared partitions, topic generation]": "" +
				"the client's lane guard exists; this runner has no steps for it yet (see the note above)",
			"Retained handles refuse changed lane identity before writes [single partition, partition count]": "" +
				"the client's lane guard exists; this runner has no steps for it yet (see the note above)",
			"Retained handles refuse changed lane identity before writes [single partition, topic generation]": "" +
				"the client's lane guard exists; this runner has no steps for it yet (see the note above)",
			"Retained handles refuse a recreated stream before writes [shared]": "" +
				"the client's lane guard exists; this runner has no steps for it yet (see the note above)",
			"Retained handles refuse a recreated stream before writes [per-agent topic]": "" +
				"the client's lane guard exists; this runner has no steps for it yet (see the note above)",
			"Retained handles refuse a recreated stream before writes [declared partitions]": "" +
				"the client's lane guard exists; this runner has no steps for it yet (see the note above)",
			"Retained handles refuse a recreated stream before writes [single partition]": "" +
				"the client's lane guard exists; this runner has no steps for it yet (see the note above)",
			"Exact record reads refuse a recreated source with reused addresses [topic generation]": "" +
				"the client's lane guard exists; this runner has no steps for it yet (see the note above)",
			"Exact record reads refuse a recreated source with reused addresses [stream generation]": "" +
				"the client's lane guard exists; this runner has no steps for it yet (see the note above)",
			"A heartbeat of a deleted stream's lease never counts in its replacement": "" +
				"needs a lease held across a stream delete and recreate",
			"A native lane writer needs no aggregate session read grant": "" +
				"needs a restricted-grant client",
		})
}

// managedSessionFlow is the session a scenario ran and the agents in it.
type managedSessionFlow struct {
	session *laser.Session
	label   string
}

func managedSessionFlowOf(state *world) *managedSessionFlow {
	if existing, found := state.get("managed-sessions"); found {
		return existing.(*managedSessionFlow)
	}
	flow := &managedSessionFlow{}
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
	case strings.HasPrefix(text, "a managed session stream in the"):
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
		return fmt.Errorf("this runner does not implement the step")
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
	sessions := state.client.SessionsWith(laser.SessionConfig{Layout: layout})
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
	session := state.client.Sessions().Create(label).As(source)
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
