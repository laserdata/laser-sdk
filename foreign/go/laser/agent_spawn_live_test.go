//go:build integration

package laser_test

import (
	"context"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenALiveAgentThatShutDownWhenASecondJoinsItsGroupThenShouldHandTheWorkToTheSecond(t *testing.T) {
	ctx := liveContext(t)
	handle := liveScratch(t, dialLive(t), ctx, "laser-spawn-restart").Laser()
	if err := handle.Bootstrap(ctx, 1, laser.ExpireAfter(time.Hour)); err != nil {
		t.Fatalf("bootstrap the stream: %v", err)
	}
	worker, controller := agentID(t, "worker"), agentID(t, "controller")
	answering := func(body string) laser.AgentHandler {
		return laser.HandlerFunc(func(ctx context.Context, message *laser.AgentMessage, agent *laser.AgentCtx) error {
			if message.Envelope == nil || message.Envelope.Kind != wire.KindCommand {
				return nil
			}
			return agent.Respond(ctx, []byte(body))
		})
	}
	options := laser.SpawnOptions{
		RuntimeOptions: laser.RuntimeOptions{RespondOn: wire.AgentSessions, Group: "workers"},
	}
	first, err := handle.SpawnAgent(ctx, worker, options, answering("first"))
	if err != nil {
		t.Fatalf("spawn the first: %v", err)
	}
	if err := first.Ready(ctx); err != nil {
		t.Fatalf("wait for the first: %v", err)
	}
	if err := first.Shutdown(ctx); err != nil {
		t.Fatalf("shut the first down: %v", err)
	}
	second, err := handle.SpawnAgent(ctx, worker, options, answering("second"))
	if err != nil {
		t.Fatalf("spawn the second: %v", err)
	}
	stopLiveAgent(t, second)
	if err := second.Ready(ctx); err != nil {
		t.Fatalf("wait for the second: %v", err)
	}

	contract, err := handle.Contract(laser.To(worker), wire.AgentSessions).
		InboxRoute(laser.FixedInbox(wire.AgentSessions)).
		From(controller).Payload([]byte("work")).Deadline(15 * time.Second).Send(ctx)

	if err != nil {
		t.Fatalf("contract: %v", err)
	}
	if contract.State != laser.ContractCompleted || string(contract.Reply.Envelope.Body) != "second" {
		t.Fatalf("want the second member to hold the partition and answer, got %v with %+v", contract.State, contract.Reply)
	}
}
