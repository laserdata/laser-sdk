package laser_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// commandTopics reads back which topics carried a command, so a test can
// prove where the work was actually addressed rather than only that it
// completed.
func commandTopics(t *testing.T, server *contractServer, topics ...string) map[string]int {
	t.Helper()
	counts := map[string]int{}
	for _, topic := range topics {
		for _, message := range server.storedOn(topic) {
			var envelope wire.AgentEnvelope
			if err := wire.Unmarshal(message.Payload, &envelope); err != nil {
				continue
			}
			if envelope.Kind == wire.KindCommand {
				counts[topic]++
			}
		}
	}
	return counts
}

// The point of an advertised route: the work goes to the topic that agent
// says it is consuming, not to a shared name the SDK picked.
func TestGivenAnAdvertisedInboxWhenContractSentThenShouldAddressTheAgentsOwnTopic(t *testing.T) {
	registry, _ := seedPresence(t, []wire.ClientMetadata{
		advertised(t, "worker", "worker-inbox", principalOf(1)),
	})
	server := &contractServer{
		replyKind: wire.KindResponse, replyBody: []byte("done"),
		replySource: agentID(t, "worker"),
	}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	contract, err := handle.Contract(laser.To(agentID(t, "worker")), "agent.sessions").
		From(agentID(t, "orchestrator")).
		Payload([]byte(`{"task":"run"}`)).
		Registry(registry).
		InboxRoute(laser.AdvertisedInbox()).
		Deadline(2 * time.Second).
		Send(context.Background())
	if err != nil {
		t.Fatalf("send the contract: %v", err)
	}
	if contract.State != laser.ContractCompleted {
		t.Fatalf("want completed, got %v", contract.State)
	}
	counts := commandTopics(t, server, "worker-inbox", "agent.sessions")
	if counts["worker-inbox"] != 1 {
		t.Errorf("want the command on the advertised inbox, got %d there", counts["worker-inbox"])
	}
	if counts["agent.sessions"] != 0 {
		t.Errorf("the command should not have gone to the named topic, got %d",
			counts["agent.sessions"])
	}
}

// An agent that advertises nothing has nowhere to receive work, and
// inventing a destination would deliver it somewhere nobody consumes.
func TestGivenNoAdvertisedInboxWhenContractSentThenShouldRefuseRatherThanGuess(t *testing.T) {
	registry, _ := seedPresence(t, nil)
	server := &contractServer{
		replyKind: wire.KindResponse, replyBody: []byte("done"),
		replySource: agentID(t, "worker"),
	}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	_, err := handle.Contract(laser.To(agentID(t, "worker")), "agent.sessions").
		From(agentID(t, "orchestrator")).
		Payload([]byte(`{"task":"run"}`)).
		Registry(registry).
		InboxRoute(laser.AdvertisedInbox()).
		Deadline(time.Second).
		Send(context.Background())
	var noInbox *laser.NoInboxError
	if err == nil {
		t.Fatal("expected an agent with no advertised inbox to be refused")
	}
	if !errors.As(err, &noInbox) {
		t.Fatalf("expected *NoInboxError, got %T: %v", err, err)
	}
	if noInbox.Agent != agentID(t, "worker") {
		t.Errorf("expected the unreachable agent named, got %q", noInbox.Agent)
	}
	if len(server.stored()) != 0 {
		t.Error("nothing should have been published for an unaddressable agent")
	}
}

// A capable agent with nowhere to receive work must not win the pick and
// then fail the send: the unreachable candidate is skipped while resolving.
func TestGivenACapableAgentWithNoInboxWhenResolvedThenShouldPickAReachableOne(t *testing.T) {
	now := time.Now()
	registry, _ := seedPresence(t, []wire.ClientMetadata{
		// "reachable" advertises an inbox; "silent" advertises presence
		// with no inbox at all, so it is capable but unaddressable.
		advertised(t, "reachable", "reachable-inbox", principalOf(1)),
		advertised(t, "silent", "", principalOf(1)),
	})
	registry.Observe(agentID(t, "reachable"), cardFor("run", nil, nil, nil, nil), now)
	registry.Observe(agentID(t, "silent"), cardFor("run", nil, nil, nil, nil), now)

	targets, err := laser.ToCapable("run", laser.RouteAny).
		ResolveRoutable(registry, laser.AdvertisedInbox(), now)
	if err != nil {
		t.Fatalf("resolve a routable capability: %v", err)
	}
	if len(targets) != 1 || targets[0] != agentID(t, "reachable") {
		t.Fatalf("want only the addressable agent, got %v", targets)
	}

	// With no agent reachable at all, the skill reads as unserved rather
	// than resolving to somewhere the work cannot land.
	bare, _ := seedPresence(t, nil)
	bare.Observe(agentID(t, "silent"), cardFor("run", nil, nil, nil, nil), now)
	_, err = laser.ToCapable("run", laser.RouteAny).
		ResolveRoutable(bare, laser.AdvertisedInbox(), now)
	var missing *laser.NoCapableAgentError
	if !errors.As(err, &missing) {
		t.Fatalf("expected *NoCapableAgentError, got %T: %v", err, err)
	}
}

// A directed route is not filtered by reachability: the caller named that
// agent, so an unreachable one is reported as having no inbox rather than
// silently becoming "no capable agent".
func TestGivenADirectedRouteToAnUnreachableAgentThenShouldStillNameIt(t *testing.T) {
	now := time.Now()
	registry, _ := seedPresence(t, nil)

	targets, err := laser.To(agentID(t, "worker")).
		ResolveRoutable(registry, laser.AdvertisedInbox(), now)
	if err != nil {
		t.Fatalf("a directed route should resolve regardless of its inbox: %v", err)
	}
	if len(targets) != 1 || targets[0] != agentID(t, "worker") {
		t.Fatalf("want the named agent, got %v", targets)
	}
}

// The command goes to the agent's inbox while the answer lands where the
// caller already watches.
func TestGivenAReplyTopicWhenContractSentThenShouldAwaitTheAnswerThere(t *testing.T) {
	registry, _ := seedPresence(t, []wire.ClientMetadata{
		advertised(t, "worker", "worker-inbox", principalOf(1)),
	})
	server := &contractServer{
		replyKind: wire.KindResponse, replyBody: []byte("done"),
		replySource: agentID(t, "worker"),
		answerOn:    "orchestrator-replies",
	}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	contract, err := handle.Contract(laser.To(agentID(t, "worker")), "agent.sessions").
		From(agentID(t, "orchestrator")).
		Payload([]byte(`{"task":"run"}`)).
		Registry(registry).
		InboxRoute(laser.AdvertisedInbox()).
		ReplyOn("orchestrator-replies").
		Deadline(2 * time.Second).
		Send(context.Background())
	if err != nil {
		t.Fatalf("send the contract: %v", err)
	}
	if contract.State != laser.ContractCompleted {
		t.Fatalf("want completed, got %v", contract.State)
	}
	counts := commandTopics(t, server, "worker-inbox")
	if counts["worker-inbox"] != 1 {
		t.Errorf("want the command on the advertised inbox, got %d", counts["worker-inbox"])
	}
	if len(server.storedOn("orchestrator-replies")) != 1 {
		t.Errorf("want the answer on the reply topic, got %d records",
			len(server.storedOn("orchestrator-replies")))
	}
}

// An answer that lands somewhere the contract is not watching never settles
// it, which is what proves the reply topic is really being honored.
func TestGivenAReplyTopicWhenTheAnswerLandsElsewhereThenShouldTimeOut(t *testing.T) {
	server := &contractServer{
		replyKind: wire.KindResponse, replyBody: []byte("done"),
		replySource: agentID(t, "worker"),
		answerOn:    "somewhere-else",
	}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	contract, err := handle.Contract(laser.To(agentID(t, "worker")), "agent.sessions").
		From(agentID(t, "orchestrator")).
		Payload([]byte(`{"task":"run"}`)).
		ReplyOn("orchestrator-replies").
		Deadline(250 * time.Millisecond).
		Send(context.Background())
	if err != nil {
		t.Fatalf("send the contract: %v", err)
	}
	if contract.State != laser.ContractTimedOut {
		t.Fatalf("want timed out, got %v", contract.State)
	}
}

// The default must not move: every existing caller addresses the topic it
// named at Contract, for both the command and the answer.
func TestGivenNoInboxRouteWhenContractSentThenShouldKeepUsingTheNamedTopic(t *testing.T) {
	server := &contractServer{
		replyKind: wire.KindResponse, replyBody: []byte("done"),
		replySource: agentID(t, "worker"),
	}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	contract, err := handle.Contract(laser.To(agentID(t, "worker")), "agent.sessions").
		From(agentID(t, "orchestrator")).
		Payload([]byte(`{"task":"run"}`)).
		Deadline(2 * time.Second).
		Send(context.Background())
	if err != nil {
		t.Fatalf("send the contract: %v", err)
	}
	if contract.State != laser.ContractCompleted {
		t.Fatalf("want completed, got %v", contract.State)
	}
	if counts := commandTopics(t, server, "agent.sessions"); counts["agent.sessions"] != 1 {
		t.Errorf("want the command on the named topic, got %d", counts["agent.sessions"])
	}
}

// A fixed route with no topic is a caller mistake, not a licence to pick a
// destination.
func TestGivenAFixedInboxWithNoTopicWhenResolvedThenShouldRefuse(t *testing.T) {
	if _, err := laser.FixedInbox("").Resolve(agentID(t, "worker"), "advertised"); err == nil {
		t.Fatal("expected a fixed route with no topic to be refused")
	}
	// A fixed route ignores what the agent advertises, which is what makes
	// it the escape hatch.
	topic, err := laser.FixedInbox("shared").Resolve(agentID(t, "worker"), "advertised")
	if err != nil {
		t.Fatalf("resolve a fixed route: %v", err)
	}
	if topic != "shared" {
		t.Errorf("want the fixed topic honored ahead of presence, got %q", topic)
	}
}
