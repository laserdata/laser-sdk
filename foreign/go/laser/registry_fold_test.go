package laser_test

import (
	"context"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenAnOperatorsQuarantineWhenFoldedThenShouldTakeOutTheNamedAgentNotTheOperator(t *testing.T) {
	now := time.Now()
	registry := foldRegistry(t,
		registryFact(t, "ops", wire.OperationQuarantine, "worker", now, now.Add(5*time.Minute)))

	if !registry.Quarantined(agentID(t, "worker")) {
		t.Error("want the agent the fact names taken out of rotation")
	}
	if registry.Quarantined(agentID(t, "ops")) {
		t.Error("the operator that wrote the fact must stay in rotation")
	}
}

func TestGivenAnUnquarantineFactWhenFoldedThenShouldReturnTheNamedAgent(t *testing.T) {
	now := time.Now()
	registry := foldRegistry(t,
		registryFact(t, "ops", wire.OperationQuarantine, "worker", now, now.Add(5*time.Minute)),
		registryFact(t, "ops", wire.OperationUnquarantine, "worker", now.Add(time.Second), now.Add(5*time.Minute)))

	if registry.Quarantined(agentID(t, "worker")) {
		t.Error("want the lifted agent back in rotation")
	}
}

func TestGivenAFactWhoseBodyNamesNoAgentWhenFoldedThenShouldIgnoreIt(t *testing.T) {
	now := time.Now()
	registry := foldRegistry(t,
		registryFact(t, "ops", wire.OperationQuarantine, "{}", now, now.Add(5*time.Minute)))

	if registry.Quarantined(agentID(t, "ops")) {
		t.Error("a fact that names no agent must take nobody out")
	}
}

func TestGivenAReplayedQuarantineAfterItWasLiftedWhenFoldedThenShouldNotApplyItAgain(t *testing.T) {
	now := time.Now()
	quarantine := registryFact(t, "ops", wire.OperationQuarantine, "worker", now, now.Add(5*time.Minute))
	replay := quarantine
	replay.Header.Timestamp = uint64(now.Add(2 * time.Second).UnixMicro())
	registry := foldRegistry(t,
		quarantine,
		registryFact(t, "ops", wire.OperationUnquarantine, "worker", now.Add(time.Second), now.Add(5*time.Minute)),
		replay)

	if registry.Quarantined(agentID(t, "worker")) {
		t.Error("a verbatim replay of a folded fact must not quarantine the agent again")
	}
}

func TestGivenAFactValidLongerThanTheBoundWhenFoldedThenShouldIgnoreIt(t *testing.T) {
	now := time.Now()
	registry := foldRegistry(t,
		registryFact(t, "ops", wire.OperationQuarantine, "worker", now, now.Add(48*time.Hour)))

	if registry.Quarantined(agentID(t, "worker")) {
		t.Error("a fact valid for longer than the replay window must not fold")
	}
}

func TestGivenAnOperatorWhenQuarantiningThenShouldAppendTheFactTheRegistryFolds(t *testing.T) {
	server := newWorkflowServer()
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	before := time.Now()

	if err := handle.Quarantine(context.Background(), agentID(t, "ops"), agentID(t, "worker")); err != nil {
		t.Fatalf("quarantine: %v", err)
	}
	if err := handle.Unquarantine(context.Background(), agentID(t, "ops"), agentID(t, "worker")); err != nil {
		t.Fatalf("unquarantine: %v", err)
	}

	facts := server.on(wire.AgentRegistry)
	if len(facts) != 2 {
		t.Fatalf("want two facts on the registry topic, got %d", len(facts))
	}
	for index, operation := range []string{wire.OperationQuarantine, wire.OperationUnquarantine} {
		var envelope wire.AgentEnvelope
		if err := wire.Unmarshal(facts[index].Payload, &envelope); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if envelope.Kind != wire.KindStatus || envelope.Operation == nil || *envelope.Operation != operation ||
			envelope.Source != agentID(t, "ops") || string(envelope.Body) != "worker" {
			t.Errorf("want a %s fact by the operator naming the agent, got %+v", operation, envelope)
		}
		if envelope.DeadlineMicros == nil {
			t.Fatalf("a registry fact carries its expiry")
		}
		validity := time.UnixMicro(int64(*envelope.DeadlineMicros)).Sub(before)
		if validity < 4*time.Minute || validity > 6*time.Minute {
			t.Errorf("want a five minute validity, got %v", validity)
		}
	}
}

func TestGivenASigningKeyWhenQuarantiningThenShouldSignTheFact(t *testing.T) {
	server := newWorkflowServer()
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	key, err := laser.SigningKeyFromSeed(make([]byte, laser.SigningSeedBytes))
	if err != nil {
		t.Fatalf("key: %v", err)
	}

	if err := handle.QuarantineSigned(
		context.Background(), agentID(t, "ops"), agentID(t, "worker"), key); err != nil {
		t.Fatalf("quarantine: %v", err)
	}
	if err := handle.UnquarantineSigned(
		context.Background(), agentID(t, "ops"), agentID(t, "worker"), key); err != nil {
		t.Fatalf("unquarantine: %v", err)
	}

	for _, fact := range server.on(wire.AgentRegistry) {
		var envelope wire.AgentEnvelope
		if err := wire.Unmarshal(fact.Payload, &envelope); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if envelope.Signature == nil {
			t.Error("want every fact signed")
		}
	}
}

// registryFact builds one quarantine or unquarantine record as an operator appends it.
func registryFact(
	t *testing.T, operator, operation, body string, appended, until time.Time,
) iggcon.IggyMessage {
	t.Helper()
	server := &streamServer{}
	agdx := streamHandle(server, laser.Options{DefaultStream: "agents"}).
		Agdx(agentID(t, operator), conversation(1))
	send := agdx.Quarantine(until, []byte(body))
	if operation == wire.OperationUnquarantine {
		send = agdx.Unquarantine(until, []byte(body))
	}
	if _, err := send.Send(context.Background()); err != nil {
		t.Fatalf("publish the %s fact: %v", operation, err)
	}
	message := server.sentMessages[0]
	message.Header.Timestamp = uint64(appended.UnixMicro())
	return message
}

// foldRegistry folds registry records, in order, into a fresh registry.
func foldRegistry(t *testing.T, records ...iggcon.IggyMessage) *laser.AgentRegistry {
	t.Helper()
	for index := range records {
		records[index].Header.Offset = uint64(index)
	}
	server := &cursorServer{partitions: 1, byPartition: map[uint32][]iggcon.IggyMessage{0: records}}
	cursor, err := cursorHandle(server).Topic(wire.AgentRegistry).Replay()
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	registry := laser.NewAgentRegistry()
	if err := registry.Refresh(context.Background(), cursor); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	return registry
}
