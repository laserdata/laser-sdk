//go:build integration

package laser_test

import (
	"context"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenTwoLiveAgentsWhenOneRequestsTheOtherFromItsHandlerThenShouldAnswerWithItsReply(t *testing.T) {
	ctx := liveContext(t)
	handle := liveScratch(t, dialLive(t), ctx, "laser-handler-request").Laser()
	if err := handle.Bootstrap(ctx, 1, laser.ExpireAfter(time.Hour)); err != nil {
		t.Fatalf("bootstrap the stream: %v", err)
	}
	triage, specialist, controller := agentID(t, "triage"), agentID(t, "specialist"), agentID(t, "controller")
	answering := laser.HandlerFunc(func(ctx context.Context, message *laser.AgentMessage, agent *laser.AgentCtx) error {
		if message.Envelope == nil || message.Envelope.Kind != wire.KindCommand {
			return nil
		}
		return agent.Respond(ctx, []byte("rotate the logs"))
	})
	asking := laser.HandlerFunc(func(ctx context.Context, message *laser.AgentMessage, agent *laser.AgentCtx) error {
		if message.Envelope == nil || message.Envelope.Kind != wire.KindCommand {
			return nil
		}
		branch, err := agent.SpawnSubconversation()
		if err != nil {
			return err
		}
		reply, err := agent.Request(ctx, wire.AgentSessions, []byte("disk alert"), laser.RequestOptions{
			Target: specialist, ReplyTopic: wire.AgentSessions, Timeout: 10 * time.Second, Provenance: branch,
		})
		if err != nil {
			return err
		}
		return agent.Respond(ctx, append([]byte("diagnosis: "), reply.Envelope.Body...))
	})
	for agent, handler := range map[wire.AgentID]laser.AgentHandler{specialist: answering, triage: asking} {
		spawned, err := handle.SpawnAgent(ctx, agent, laser.SpawnOptions{
			RuntimeOptions: laser.RuntimeOptions{RespondOn: wire.AgentSessions},
		}, handler)
		if err != nil {
			t.Fatalf("spawn %s: %v", agent, err)
		}
		stopLiveAgent(t, spawned)
		if err := spawned.Ready(ctx); err != nil {
			t.Fatalf("wait for %s: %v", agent, err)
		}
	}

	contract, err := handle.Contract(laser.To(triage), wire.AgentSessions).
		InboxRoute(laser.FixedInbox(wire.AgentSessions)).
		From(controller).Payload([]byte("node-7 is slow")).Deadline(20 * time.Second).Send(ctx)

	if err != nil {
		t.Fatalf("contract: %v", err)
	}
	if contract.State != laser.ContractCompleted || string(contract.Reply.Envelope.Body) != "diagnosis: rotate the logs" {
		t.Fatalf("want triage to answer with the specialist's reply, got %v with %+v", contract.State, contract.Reply)
	}
}
