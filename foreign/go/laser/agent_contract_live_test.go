//go:build integration

package laser_test

import (
	"context"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// A spawned agent is reachable through the routes a caller gets by default,
// on a server that keeps no live presence: the card names the inbox.
func TestGivenASpawnedAgentWhenContractsUseTheDefaultRouteThenShouldReachItsOwnInbox(t *testing.T) {
	ctx := liveContext(t)
	client := dialLive(t)
	handle := liveScratch(t, client, ctx, "laser-default-route").Laser()
	if err := handle.Bootstrap(ctx, 1, laser.ExpireAfter(time.Hour)); err != nil {
		t.Fatalf("bootstrap the stream: %v", err)
	}
	if err := handle.Topic("worker-inbox").Ensure(ctx, 1, nil); err != nil {
		t.Fatalf("create the worker inbox: %v", err)
	}
	worker, controller := agentID(t, "worker"), agentID(t, "controller")
	spawned, err := handle.SpawnAgent(ctx, worker, laser.SpawnOptions{
		RuntimeOptions: laser.RuntimeOptions{Topic: "worker-inbox", RespondOn: wire.AgentSessions},
		Capabilities:   []wire.CapabilityDescriptor{{SkillID: "review"}},
	}, laser.HandlerFunc(func(ctx context.Context, message *laser.AgentMessage, agent *laser.AgentCtx) error {
		if message.Envelope == nil || message.Envelope.Kind != wire.KindCommand {
			return nil
		}
		return agent.Respond(ctx, []byte("reviewed"))
	}))
	if err != nil {
		t.Fatalf("spawn the worker: %v", err)
	}
	t.Cleanup(func() {
		stop, cancel := context.WithTimeout(context.Background(), liveOperationTimeout)
		defer cancel()
		if err := spawned.Shutdown(stop); err != nil {
			t.Errorf("stop the worker: %v", err)
		}
	})
	if err := spawned.Ready(ctx); err != nil {
		t.Fatalf("wait for the worker: %v", err)
	}

	for name, route := range map[string]laser.Router{
		"directed": laser.To(worker),
		"capable":  laser.ToCapable("review", laser.RouteAny),
	} {
		contract, err := handle.Contract(route, wire.AgentSessions).
			From(controller).Payload([]byte("review this")).Deadline(10 * time.Second).Send(ctx)
		if err != nil {
			t.Fatalf("%s contract: %v", name, err)
		}
		if contract.State != laser.ContractCompleted || string(contract.Reply.Envelope.Body) != "reviewed" {
			t.Fatalf("%s contract settled %v with %+v", name, contract.State, contract.Reply)
		}
	}

	registry := laser.NewAgentRegistry()
	cursor, err := handle.Topic(wire.AgentRegistry).Replay()
	if err != nil {
		t.Fatalf("open the registry topic: %v", err)
	}
	if err := registry.Refresh(ctx, cursor); err != nil {
		t.Fatalf("fold the registry topic: %v", err)
	}
	report, err := handle.ScatterReport(ctx, laser.ScatterRequest{
		Skill: "review", Registry: registry, From: controller, Topic: wire.AgentSessions,
		Payload: []byte("check this"), Policy: laser.GatherAll(), Deadline: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("scatter: %v", err)
	}
	if !report.Satisfied || len(report.Completed()) != 1 {
		t.Fatalf("scatter outcomes: %+v", report.Outcomes)
	}
}
