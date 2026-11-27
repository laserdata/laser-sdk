package laser_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// A directed request armed with a verifier only settles on a reply signed by
// the agent it was sent to, and carries that principal back on the answer.
func TestGivenAVerifierWhenRequestAnsweredBySignedTargetThenShouldReturnTheReplyWithThePrincipal(t *testing.T) {
	key, err := laser.GenerateSigningKey()
	if err != nil {
		t.Fatalf("generate a signing key: %v", err)
	}
	registry := laser.NewKeyRegistry()
	registry.Enroll("executor", key.VerifyingKey())
	server := &streamServer{}
	requester := streamHandle(server, laser.Options{DefaultStream: "agents"}).
		Requester(agentID(t, "planner"), wire.AgentSessions).
		Verifier(registry)
	session := conversation(1)

	go func() {
		for range 200 {
			time.Sleep(5 * time.Millisecond)
			sent := server.sent()
			if len(sent) == 0 {
				continue
			}
			command, err := laser.OpenAgdx(openDelivered(t, sent[0], 0, 0))
			if err != nil {
				continue
			}
			reply := signedAnswer(t, agentID(t, "executor"), agentID(t, "planner"),
				session, *command.Envelope.Correlation, command.Envelope.Record, key)
			requester.Hub().Deliver(reply)
			return
		}
	}()

	answer, err := requester.Request(context.Background(), session,
		[]byte(`{"ask":"something"}`), laser.RequestOptions{
			Target: agentID(t, "executor"), Timeout: 3 * time.Second,
		})
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if answer.VerifiedPrincipal == nil || *answer.VerifiedPrincipal != "executor" {
		t.Errorf("want the verified principal on the answer, got %v", answer.VerifiedPrincipal)
	}
}

// A reply validly signed by someone other than the addressed target must not
// settle a verified request: the request keeps waiting, which is what a
// waiter pinned to one principal degrades to rather than a hard failure, so a
// genuine late answer from the right agent can still settle it.
func TestGivenAVerifierWhenRequestAnsweredBySomeoneElseThenShouldKeepWaiting(t *testing.T) {
	key, err := laser.GenerateSigningKey()
	if err != nil {
		t.Fatalf("generate a signing key: %v", err)
	}
	registry := laser.NewKeyRegistry()
	registry.Enroll("imposter", key.VerifyingKey())
	server := &streamServer{}
	requester := streamHandle(server, laser.Options{DefaultStream: "agents"}).
		Requester(agentID(t, "planner"), wire.AgentSessions).
		Verifier(registry)
	session := conversation(1)

	go func() {
		for range 200 {
			time.Sleep(5 * time.Millisecond)
			sent := server.sent()
			if len(sent) == 0 {
				continue
			}
			command, err := laser.OpenAgdx(openDelivered(t, sent[0], 0, 0))
			if err != nil {
				continue
			}
			reply := signedAnswer(t, agentID(t, "imposter"), agentID(t, "planner"),
				session, *command.Envelope.Correlation, command.Envelope.Record, key)
			if requester.Hub().Deliver(reply) {
				t.Error("an answer from the wrong principal must not be taken")
			}
			return
		}
	}()

	_, err = requester.Request(context.Background(), session, []byte("x"),
		laser.RequestOptions{Target: agentID(t, "executor"), Timeout: 150 * time.Millisecond})

	var timeout *laser.TimeoutError
	if !errors.As(err, &timeout) {
		t.Fatalf("want a timeout, since no reply from the addressed target ever landed, got %v", err)
	}
}
