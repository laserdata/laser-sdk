// Two agents exchanging work on one conversation, with the gates a running
// agent applies.
//
// The gates are the interesting part. A record that expired, one fenced out by
// a newer holder, and one already handled all reach the same consumer, and
// each is turned away for its own reason before any handler sees it.
package main

import (
	"context"
	"fmt"
	"time"

	examples "github.com/laserdata/laser-sdk/examples/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func main() { examples.Run("agent", run) }

func run(ctx context.Context, client *laser.Laser) error {
	planner, err := wire.NewAgentID("planner")
	if err != nil {
		return err
	}
	executor, err := wire.NewAgentID("executor")
	if err != nil {
		return err
	}

	examples.Phase("bootstrap the agent topology")
	// Sessions.Bootstrap, not the plain Laser.Bootstrap: this example opens a
	// session, so the stream also needs registering as a session source on a
	// deployment that indexes them (a no-op, Registered: false, on one that
	// does not -- plain Apache Iggy here). Skipping that step still creates
	// working topics, but a managed deployment's own session-index reads
	// (and its Console) have nothing to serve for a session on a stream that
	// was never registered.
	bootstrapped, err := client.Sessions().Bootstrap(ctx, examples.Partitions, laser.ExpireAfter(time.Hour))
	if err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}
	fmt.Printf("stream and agent topics ready, registered as a session source: %v\n",
		bootstrapped.Registered)

	examples.Phase("open a session")
	session := client.Sessions().Create("nightly-report").As(planner)
	fmt.Printf("session %s\n", session.Conversation())
	_, lease, err := session.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer lease.Release()

	examples.Phase("the planner asks the executor to do something")
	agdx := client.Agdx(planner, session.Conversation())
	correlation := wire.CorrelationID{ID: wire.IDFromBytes(laser.NewOperationID())}
	receipt, err := agdx.Command(correlation, []byte(`{"task":"summarise"}`)).
		WithTarget(executor).
		WithDeadline(time.Now().Add(30 * time.Second)).
		Send(ctx)
	if err != nil {
		return fmt.Errorf("command: %w", err)
	}
	fmt.Printf("command %s addressed to %s\n", receipt.Record, executor)

	examples.Phase("the executor answers, naming what it answered")
	answer := client.Agdx(executor, session.Conversation())
	if _, err := answer.Respond(correlation, []byte(`{"summary":"all quiet"}`)).
		WithTarget(planner).
		WithCause(receipt.Record, nil).
		Send(ctx); err != nil {
		return fmt.Errorf("respond: %w", err)
	}
	fmt.Println("answered, with the command named as its cause")

	examples.Phase("the gates a running agent applies")
	gates := client.Agent(executor, laser.AgentOptions{})
	fmt.Println(describe("a record addressed to this agent",
		gates.DecideMessage(message(session.Conversation(), &executor, nil, nil))))
	fmt.Println(describe("a record for somebody else",
		gates.DecideMessage(message(session.Conversation(), &planner, nil, nil))))

	// A deadline in the past is work nobody is waiting for any more.
	expired := time.Now().Add(-time.Minute)
	fmt.Println(describe("a record past its deadline",
		gates.DecideMessage(message(session.Conversation(), &executor, &expired, nil))))

	// A holder that lost its lease and came back with stale work.
	ahead, behind := uint64(5), uint64(4)
	gates.DecideMessage(message(session.Conversation(), &executor, nil, &ahead))
	fmt.Println(describe("a record fenced out by a newer holder",
		gates.DecideMessage(message(session.Conversation(), &executor, nil, &behind))))

	examples.Phase("close the session")
	if _, err := session.Complete(ctx, "nothing to report"); err != nil {
		return fmt.Errorf("complete: %w", err)
	}
	fmt.Println("session closed")
	return nil
}

// message builds a record the way one arrives off a topic, so the gates are
// exercised on the shape they really see.
func message(
	conversation wire.ConversationID, target *wire.AgentID,
	deadline *time.Time, fence *uint64,
) *laser.AgdxMessage {
	return &laser.AgdxMessage{
		Envelope: wire.AgentEnvelope{Kind: wire.KindCommand, Target: target},
		Record: &laser.Record{Provenance: &laser.Provenance{
			Conversation: conversation,
			DeadlineAt:   deadline,
			FenceToken:   fence,
		}},
	}
}

func describe(what string, decision laser.Decision) string {
	if decision.Detail == "" {
		return fmt.Sprintf("  %-38s %s", what, decision.Verdict)
	}
	return fmt.Sprintf("  %-38s %s: %s", what, decision.Verdict, decision.Detail)
}
