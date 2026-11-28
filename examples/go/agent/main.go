// Agents that discover each other by capability, take directed work with a
// deadline, and reply, all over the log. The runtime needs no managed backend.
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	examples "github.com/laserdata/laser-sdk/examples/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

const (
	capability = "resolve-ticket"
	deadline   = 60 * time.Second
)

func main() { examples.RunExample("agent", run) }

func run(ctx context.Context, client *laser.Laser) (err error) {
	// The agent topics (the shared session topic and its satellites) must exist
	// before an agent's consumer group joins one.
	if err := client.Bootstrap(ctx, examples.Partitions, laser.ExpireAfter(examples.SessionRetention)); err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}

	examples.Phase("spawn a handler, then hand it a deadline-bounded task")
	triage, err := wire.NewAgentID("triage")
	if err != nil {
		return err
	}
	handle, err := client.SpawnAgent(ctx, triage, laser.SpawnOptions{
		RuntimeOptions: laser.RuntimeOptions{
			Topic:     wire.AgentSessions,
			RespondOn: wire.AgentSessions,
			// A Working status on pickup tells a contract caller the command
			// was consumed. Redelivery after a crash comes from commit-after-success.
			AckOnPickup: true,
		},
		// The advertised capability is what makes this agent addressable by
		// what it can do, not by the name it happens to run under.
		Capabilities: []wire.CapabilityDescriptor{{SkillID: capability}},
	}, laser.HandlerFunc(pickUp))
	if err != nil {
		return fmt.Errorf("spawn triage: %w", err)
	}
	defer func() { err = errors.Join(err, handle.Shutdown(context.WithoutCancel(ctx))) }()
	if err := handle.Ready(ctx); err != nil {
		return fmt.Errorf("triage ready: %w", err)
	}

	// A contract is a directed task with a deadline and a real answer: consumed,
	// completed, failed, or timed out. Routed by capability, not by name.
	orchestrator, err := wire.NewAgentID("orchestrator")
	if err != nil {
		return err
	}
	contract, err := client.Contract(laser.ToCapable(capability, laser.RouteAny), wire.AgentSessions).
		From(orchestrator).
		Payload([]byte("ticket #42 is stuck")).
		InboxRoute(laser.FixedInbox(wire.AgentSessions)).
		Deadline(deadline).
		Send(ctx)
	if err != nil {
		return fmt.Errorf("contract: %w", err)
	}
	if contract.State != laser.ContractCompleted {
		fmt.Printf("  contract ended without a reply: %s\n", contract.State)
		return nil
	}
	fmt.Printf("  contract completed: %s\n", contract.Reply.Envelope.Body)
	return nil
}

func pickUp(ctx context.Context, message *laser.AgentMessage, agent *laser.AgentCtx) error {
	fmt.Printf("  triage picked up %q\n", message.Body())
	return agent.Respond(ctx, []byte("on it"))
}
