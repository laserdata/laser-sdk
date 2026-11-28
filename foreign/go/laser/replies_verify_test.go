package laser_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// signedAnswer builds a Response-kind envelope the way source would really
// send it, with real headers and a real signature bound to them when key is set,
// so a verifier has a genuine record to check rather than a hand-built
// envelope with no signing context at all. cause, when set, must be applied
// before signing: it rides inside the signed envelope, so naming it after the
// fact would invalidate the signature rather than merely narrow the match.
func signedAnswer(
	t *testing.T, source, target wire.AgentID, session wire.ConversationID,
	correlation wire.CorrelationID, cause *wire.RecordID, key *laser.SigningKey,
) *laser.AgdxMessage {
	t.Helper()
	server := &streamServer{}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	send := handle.Agdx(source, session).Respond(correlation, []byte("the answer")).
		WithTarget(target)
	if cause != nil {
		send = send.WithCause(*cause, nil)
	}
	if key != nil {
		send = send.Signed(key)
	}
	if _, err := send.Send(context.Background()); err != nil {
		t.Fatalf("build the reply: %v", err)
	}
	record := openDelivered(t, server.sent()[0], 0, 1)
	message, err := laser.OpenAgdx(record)
	if err != nil {
		t.Fatalf("open the reply: %v", err)
	}
	return message
}

func TestGivenAVerifiedHubWithAnUnsignedReplyWhenDeliveredThenShouldLeaveItAlone(t *testing.T) {
	hub := laser.NewVerifiedReplyHub(laser.NewKeyRegistry())
	requester := agentID(t, "planner")
	ticket, err := hub.Subscribe(correlationID(1), laser.ExpectedReply{
		Session: conversation(1), Requester: requester,
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	message := signedAnswer(
		t, agentID(t, "executor"), requester, conversation(1), correlationID(1), nil, nil)
	if hub.Deliver(message) {
		t.Fatal("an unsigned reply must not settle a verified wait")
	}

	_, err = ticket.Await(context.Background(), 50*time.Millisecond)
	var timeout *laser.TimeoutError
	if !errors.As(err, &timeout) {
		t.Fatalf("want the wait still open for a reply that never verified, got %v", err)
	}
}

func TestGivenAVerifiedHubWithASignedReplyWhenDeliveredThenShouldWakeItsWaiterWithThePrincipal(t *testing.T) {
	key, err := laser.GenerateSigningKey()
	if err != nil {
		t.Fatalf("generate a signing key: %v", err)
	}
	registry := laser.NewKeyRegistry()
	registry.Enroll("executor", key.VerifyingKey())
	hub := laser.NewVerifiedReplyHub(registry)
	requester := agentID(t, "planner")
	ticket, err := hub.Subscribe(correlationID(1), laser.ExpectedReply{
		Session: conversation(1), Requester: requester, ExpectedSigner: "executor",
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	message := signedAnswer(
		t, agentID(t, "executor"), requester, conversation(1), correlationID(1), nil, key)
	if !hub.Deliver(message) {
		t.Fatal("want a correctly signed, correctly addressed reply taken")
	}

	answer, err := ticket.Await(context.Background(), time.Second)
	if err != nil {
		t.Fatalf("await: %v", err)
	}
	if answer.VerifiedPrincipal == nil || *answer.VerifiedPrincipal != "executor" {
		t.Errorf("want the verified principal carried on the answer, got %v", answer.VerifiedPrincipal)
	}
}

func TestGivenAVerifiedHubWithAWrongSignerReplyWhenDeliveredThenShouldLeaveItAlone(t *testing.T) {
	key, err := laser.GenerateSigningKey()
	if err != nil {
		t.Fatalf("generate a signing key: %v", err)
	}
	registry := laser.NewKeyRegistry()
	registry.Enroll("imposter", key.VerifyingKey())
	hub := laser.NewVerifiedReplyHub(registry)
	requester := agentID(t, "planner")
	ticket, err := hub.Subscribe(correlationID(1), laser.ExpectedReply{
		Session: conversation(1), Requester: requester, ExpectedSigner: "executor",
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Validly signed and enrolled, but as a different agent than the one the
	// waiter is pinned to: a wrong-signer reply, not an unsigned one.
	message := signedAnswer(
		t, agentID(t, "imposter"), requester, conversation(1), correlationID(1), nil, key)
	if hub.Deliver(message) {
		t.Fatal("a reply signed by the wrong principal must not settle the wait")
	}

	_, err = ticket.Await(context.Background(), 50*time.Millisecond)
	var timeout *laser.TimeoutError
	if !errors.As(err, &timeout) {
		t.Fatalf("want the wait still open for the wrong signer, got %v", err)
	}
}
