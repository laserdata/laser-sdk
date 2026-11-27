package laser_test

import (
	"context"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// verifying records every message a handler receives, principal included, so
// a test can check what the consumer actually handed the handler rather than
// only whether it ran at all.
type verifying struct {
	principals []*string
}

func (handler *verifying) Handle(
	_ context.Context, message *laser.AgentMessage, _ *laser.AgentCtx,
) error {
	handler.principals = append(handler.principals, message.VerifiedPrincipal)
	return nil
}

// signedRecords builds the same signed command twice (once per offset), the
// way a replayed record reaching the log a second time would: the identical
// envelope bytes, including its record id, at a different position.
func signedRecords(
	t *testing.T, build func(*laser.Agdx) *laser.AgdxSend, offsets ...uint64,
) []*laser.Record {
	t.Helper()
	server := &streamServer{}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	send := build(handle.Agdx(agentID(t, "planner"), conversation(1)))
	if _, err := send.Send(context.Background()); err != nil {
		t.Fatalf("build a record: %v", err)
	}
	message := server.sent()[0]
	records := make([]*laser.Record, len(offsets))
	for index, offset := range offsets {
		records[index] = openDelivered(t, message, 0, offset)
	}
	return records
}

func TestGivenAnUnsignedCommandOnAVerifiedAgentWhenRunThenShouldDeadLetterIt(t *testing.T) {
	key, err := laser.GenerateSigningKey()
	if err != nil {
		t.Fatalf("generate a signing key: %v", err)
	}
	registry := laser.NewKeyRegistry()
	registry.Enroll("producer-1", key.VerifyingKey())
	server := newRuntimeServer()
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{Verifier: registry})
	record := delivered(t, 0, 1, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(1), []byte("do it")).
			WithTarget(agentID(t, "executor"))
	})
	handler := &verifying{}

	if err := runUntilDrained(t, runtime, sourceOf(record), handler); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(handler.principals) != 0 {
		t.Fatalf("want the handler never reached, got %d deliveries", len(handler.principals))
	}
	kept := capsules(t, server)
	if len(kept) != 1 {
		t.Fatalf("want one dead letter, got %d", len(kept))
	}
	if kept[0].Reason != wire.DeadLetterRejected {
		t.Errorf("want DeadLetterRejected, got %v", kept[0].Reason)
	}
}

func TestGivenASignedCommandOnAVerifiedAgentWhenRunThenShouldHandleItWithThePrincipal(t *testing.T) {
	key, err := laser.GenerateSigningKey()
	if err != nil {
		t.Fatalf("generate a signing key: %v", err)
	}
	registry := laser.NewKeyRegistry()
	registry.Enroll("producer-1", key.VerifyingKey())
	server := newRuntimeServer()
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{Verifier: registry})
	record := delivered(t, 0, 1, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(1), []byte("do it")).
			WithTarget(agentID(t, "executor")).
			Signed(key)
	})
	handler := &verifying{}

	if err := runUntilDrained(t, runtime, sourceOf(record), handler); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(handler.principals) != 1 {
		t.Fatalf("want exactly one delivery, got %d", len(handler.principals))
	}
	if handler.principals[0] == nil || *handler.principals[0] != "producer-1" {
		t.Errorf("want the enrolled principal on the message, got %v", handler.principals[0])
	}
	if kept := capsules(t, server); len(kept) != 0 {
		t.Errorf("a verified command is not poison, got %d dead letters", len(kept))
	}
}

func TestGivenAReplayedSignedRecordWhenRunThenShouldDropItWithoutHandlingOrDeadLettering(t *testing.T) {
	key, err := laser.GenerateSigningKey()
	if err != nil {
		t.Fatalf("generate a signing key: %v", err)
	}
	registry := laser.NewKeyRegistry()
	registry.Enroll("producer-1", key.VerifyingKey())
	server := newRuntimeServer()
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{Verifier: registry})
	records := signedRecords(t, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(1), []byte("do it")).
			WithTarget(agentID(t, "executor")).
			Signed(key)
	}, 1, 2)
	handler := &verifying{}

	if err := runUntilDrained(t, runtime, sourceOf(records...), handler); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(handler.principals) != 1 {
		t.Fatalf("want the replay dropped rather than handled twice, got %d deliveries",
			len(handler.principals))
	}
	if kept := capsules(t, server); len(kept) != 0 {
		t.Errorf("a replay is not poison, so nothing is dead-lettered, got %d", len(kept))
	}
}

func TestGivenASigningKeyWhenRespondingThenShouldSignTheAnswer(t *testing.T) {
	key, err := laser.GenerateSigningKey()
	if err != nil {
		t.Fatalf("generate a signing key: %v", err)
	}
	registry := laser.NewKeyRegistry()
	registry.Enroll("executor", key.VerifyingKey())
	server := newRuntimeServer()
	record := commandFor(t, 12, 4)

	respondErr := handledThrough(t, server,
		laser.RuntimeOptions{RespondOn: wire.AgentSessions, SigningKey: key}, record,
		func(ctx context.Context, agent *laser.AgentCtx) error {
			return agent.Respond(ctx, []byte("the answer"))
		})
	if respondErr != nil {
		t.Fatalf("respond: %v", respondErr)
	}

	answers := envelopesOn(t, server, wire.AgentSessions)
	if len(answers) != 1 {
		t.Fatalf("want one answer, got %d", len(answers))
	}
	answer := answers[0]
	if answer.Signature == nil {
		t.Fatal("want the answer signed")
	}
	principal, err := registry.Verify(answer)
	if err != nil {
		t.Fatalf("verify the signed answer: %v", err)
	}
	if principal != "executor" {
		t.Errorf("want the responding agent's own principal, got %q", principal)
	}
}
