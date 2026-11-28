package laser_test

import (
	"context"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenAVerifyingRegistryWhenFactsArriveThenShouldFoldOnlyOperatorSignedOnes(t *testing.T) {
	now := time.Now()
	until := now.Add(5 * time.Minute)
	operatorKey, agentKey := seededKey(t, 71), seededKey(t, 72)
	keys := laser.NewKeyRegistry()
	keys.EnrollOperator("operator", operatorKey.VerifyingKey())
	keys.Enroll("agent-signer", agentKey.VerifyingKey())

	registry := foldVerifiedRegistry(t, keys,
		signedRegistryFact(t, wire.OperationQuarantine, "q-unsigned", now, until, nil),
		signedRegistryFact(t, wire.OperationQuarantine, "q-agent-kind", now, until, agentKey),
		signedRegistryFact(t, wire.OperationQuarantine, "q-operator", now, until, operatorKey))

	if !registry.Quarantined(agentID(t, "q-operator")) {
		t.Error("want the operator-signed fact folded")
	}
	if registry.Quarantined(agentID(t, "q-unsigned")) {
		t.Error("an unsigned fact must not fold in a verifying registry")
	}
	if registry.Quarantined(agentID(t, "q-agent-kind")) {
		t.Error("a fact signed by an agent key must not fold")
	}
}

func TestGivenASignedUnquarantinePastItsDeadlineWhenFoldedThenShouldKeepTheQuarantine(t *testing.T) {
	now := time.Now()
	operatorKey := seededKey(t, 73)
	keys := laser.NewKeyRegistry()
	keys.EnrollOperator("operator", operatorKey.VerifyingKey())

	registry := foldVerifiedRegistry(t, keys,
		signedRegistryFact(t, wire.OperationQuarantine, "q-target", now, now.Add(5*time.Minute), operatorKey),
		signedRegistryFact(t, wire.OperationUnquarantine, "q-target", now.Add(time.Second), time.UnixMicro(1), operatorKey),
		signedRegistryFact(t, wire.OperationQuarantine, "q-sentinel", now.Add(2*time.Second), now.Add(5*time.Minute), operatorKey))

	if !registry.Quarantined(agentID(t, "q-sentinel")) {
		t.Fatal("want the sentinel fact folded")
	}
	if !registry.Quarantined(agentID(t, "q-target")) {
		t.Error("a late unquarantine lifted the quarantine")
	}
}

func TestGivenARevokedOperatorKeyWhenItsFactArrivesThenShouldRefuseIt(t *testing.T) {
	now := time.Now()
	until := now.Add(5 * time.Minute)
	revokedKey, liveKey := seededKey(t, 74), seededKey(t, 75)
	keys := laser.NewKeyRegistry()
	keys.EnrollRecord(laser.OperatorKey("operator", revokedKey.VerifyingKey()).Revoke())
	keys.EnrollOperator("operator", liveKey.VerifyingKey())

	registry := foldVerifiedRegistry(t, keys,
		signedRegistryFact(t, wire.OperationQuarantine, "q-after-revocation", now, until, revokedKey),
		signedRegistryFact(t, wire.OperationQuarantine, "q-revocation-sentinel", now, until, liveKey))

	if !registry.Quarantined(agentID(t, "q-revocation-sentinel")) {
		t.Fatal("want the fact signed by the live key folded")
	}
	if registry.Quarantined(agentID(t, "q-after-revocation")) {
		t.Error("a fact signed by a revoked key must not fold")
	}
}

func TestGivenAnUnsignedCopyOfAFactWhenTheSignedOneFollowsThenShouldStillFoldTheSignedOne(t *testing.T) {
	now := time.Now()
	until := now.Add(5 * time.Minute)
	operatorKey := seededKey(t, 76)
	keys := laser.NewKeyRegistry()
	keys.EnrollOperator("operator", operatorKey.VerifyingKey())
	signed := signedRegistryFact(t, wire.OperationQuarantine, "q-operator", now, until, operatorKey)
	var envelope wire.AgentEnvelope
	if err := wire.Unmarshal(signed.Payload, &envelope); err != nil {
		t.Fatalf("decode: %v", err)
	}
	envelope.Signature = nil
	stripped, err := wire.Marshal(envelope)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	unsigned := signed
	unsigned.Payload = stripped
	unsigned.Header.PayloadLength = uint32(len(stripped))

	registry := foldVerifiedRegistry(t, keys, unsigned, signed)

	if !registry.Quarantined(agentID(t, "q-operator")) {
		t.Error("a refused copy must not use up the record id of the signed fact")
	}
}

func seededKey(t *testing.T, fill byte) *laser.SigningKey {
	t.Helper()
	seed := make([]byte, laser.SigningSeedBytes)
	for index := range seed {
		seed[index] = fill
	}
	key, err := laser.SigningKeyFromSeed(seed)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	return key
}

// signedRegistryFact builds one registry fact by "operator", signed when key is set.
func signedRegistryFact(
	t *testing.T, operation, agent string, appended, until time.Time, key *laser.SigningKey,
) iggcon.IggyMessage {
	t.Helper()
	server := &streamServer{}
	agdx := streamHandle(server, laser.Options{DefaultStream: "agents"}).
		Agdx(agentID(t, "operator"), conversation(1))
	send := agdx.Quarantine(until, []byte(agent))
	if operation == wire.OperationUnquarantine {
		send = agdx.Unquarantine(until, []byte(agent))
	}
	if key != nil {
		send = send.Signed(key)
	}
	if _, err := send.Send(context.Background()); err != nil {
		t.Fatalf("publish the %s fact: %v", operation, err)
	}
	message := server.sentMessages[0]
	message.Header.Timestamp = uint64(appended.UnixMicro())
	return message
}

// foldVerifiedRegistry folds registry records, in order, into a registry that verifies operators.
func foldVerifiedRegistry(
	t *testing.T, keys *laser.KeyRegistry, records ...iggcon.IggyMessage,
) *laser.AgentRegistry {
	t.Helper()
	for index := range records {
		records[index].Header.Offset = uint64(index)
	}
	server := &cursorServer{partitions: 1, byPartition: map[uint32][]iggcon.IggyMessage{0: records}}
	cursor, err := cursorHandle(server).Topic(wire.AgentRegistry).Replay()
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	registry := laser.NewAgentRegistry().Verifier(keys)
	if err := registry.Refresh(context.Background(), cursor); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	return registry
}
