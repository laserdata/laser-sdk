package laser_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// A request is only finished when an answer comes back, so the hub and the
// send have to be wired to each other rather than left for a caller to join.
func TestGivenARequestWhenAnsweredThenShouldReturnTheReply(t *testing.T) {
	server := &streamServer{}
	handle := streamHandle(server, laser.Options{DefaultStream: "agents"})
	requester := handle.Requester(agentID(t, "planner"), wire.AgentSessions)
	session := conversation(1)

	// The answer arrives once the request is on the wire, which is what a
	// reader of the reply topic would feed in.
	go func() {
		for range 200 {
			time.Sleep(5 * time.Millisecond)
			delivered := server.sent()
			if len(delivered) == 0 {
				continue
			}
			sent, err := laser.OpenRecord(delivered[0])
			if err != nil {
				continue
			}
			command, err := laser.OpenAgdx(sent)
			if err != nil {
				continue
			}
			requester.Hub().Deliver(answerTo(t, command, session))
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
	if string(answer.Envelope.Body) != "the answer" {
		t.Errorf("want the answer, got %q", answer.Envelope.Body)
	}
	if requester.Hub().Waiting() != 0 {
		t.Error("a settled request must release its wait")
	}
}

// answerTo builds the reply a target would send, naming the command it
// answers.
func answerTo(
	t *testing.T, command *laser.AgdxMessage, session wire.ConversationID,
) *laser.AgdxMessage {
	t.Helper()
	source := command.Envelope.Source
	return &laser.AgdxMessage{
		Envelope: wire.AgentEnvelope{
			Kind:        wire.KindResponse,
			Correlation: command.Envelope.Correlation,
			Cause:       command.Envelope.Record,
			Target:      &source,
			Body:        []byte("the answer"),
		},
		Record: &laser.Record{Provenance: &laser.Provenance{Conversation: session}},
	}
}

// A caller that has stopped waiting does not want the work done either, so the
// request carries its own timeout as the deadline.
func TestGivenARequestWhenSentThenShouldStampItsDeadline(t *testing.T) {
	server := &streamServer{}
	requester := streamHandle(server, laser.Options{DefaultStream: "agents"}).
		Requester(agentID(t, "planner"), wire.AgentSessions)

	_, err := requester.Request(context.Background(), conversation(1), []byte("x"),
		laser.RequestOptions{Timeout: 80 * time.Millisecond})

	var timeout *laser.TimeoutError
	if !errors.As(err, &timeout) {
		t.Fatalf("want a timeout, got %T: %v", err, err)
	}
	record, openErr := laser.OpenRecord(server.sentMessages[0])
	if openErr != nil {
		t.Fatalf("open: %v", openErr)
	}
	if record.Provenance == nil || record.Provenance.DeadlineAt == nil {
		t.Fatal("want the request to carry a deadline")
	}
	if record.Provenance.DeadlineAt.After(time.Now().Add(time.Minute)) {
		t.Error("the deadline must follow the caller's own wait")
	}
}

// A request addressed to nobody is for whichever member picks it up, which
// every record on a shared topic must say.
func TestGivenNoTargetWhenRequestingThenShouldBroadcast(t *testing.T) {
	server := &streamServer{}
	requester := streamHandle(server, laser.Options{DefaultStream: "agents"}).
		Requester(agentID(t, "planner"), wire.AgentSessions)

	_, _ = requester.Request(context.Background(), conversation(1), []byte("x"),
		laser.RequestOptions{Timeout: 50 * time.Millisecond})

	headers := sentHeaders(t, server)
	if headers[wire.HeaderTargetAgentID] != wire.Broadcast {
		t.Errorf("want a broadcast, got %q", headers[wire.HeaderTargetAgentID])
	}
}

// A request that never left must not hold its correlation, or a retry on the
// same one is refused.
func TestGivenARequestThatCannotBeSentWhenTriedThenShouldReleaseItsWait(t *testing.T) {
	server := &streamServer{}
	// No default stream, so the send is refused before it reaches the socket.
	requester := streamHandle(server, laser.Options{}).
		Requester(agentID(t, "planner"), wire.AgentSessions)

	if _, err := requester.Request(context.Background(), conversation(1),
		[]byte("x"), laser.RequestOptions{}); err == nil {
		t.Fatal("a request with nowhere to go must fail")
	}
	if requester.Hub().Waiting() != 0 {
		t.Error("a request that never left must not hold a correlation")
	}
}

// A reply belonging to another exchange on the same correlation must not
// settle this one, which is what narrowing to the sent command is for.
func TestGivenAReplyToAnotherCommandWhenDeliveredThenShouldNotSettleTheRequest(t *testing.T) {
	server := &streamServer{}
	requester := streamHandle(server, laser.Options{DefaultStream: "agents"}).
		Requester(agentID(t, "planner"), wire.AgentSessions)
	session := conversation(1)

	go func() {
		for range 200 {
			time.Sleep(5 * time.Millisecond)
			delivered := server.sent()
			if len(delivered) == 0 {
				continue
			}
			sent, err := laser.OpenRecord(delivered[0])
			if err != nil {
				continue
			}
			command, err := laser.OpenAgdx(sent)
			if err != nil {
				continue
			}
			// An answer to a different command, on this correlation.
			other := answerTo(t, command, session)
			elsewhere := wire.RecordID{ID: wire.IDFromBytes([16]byte{15: 99})}
			other.Envelope.Cause = &elsewhere
			if requester.Hub().Deliver(other) {
				t.Error("a reply to another command must not be taken")
			}
			return
		}
	}()

	_, err := requester.Request(context.Background(), session, []byte("x"),
		laser.RequestOptions{Timeout: 300 * time.Millisecond})

	var timeout *laser.TimeoutError
	if !errors.As(err, &timeout) {
		t.Fatalf("want the request to still be waiting, got %T: %v", err, err)
	}
}

// A record the gates turned away is kept rather than dropped, so the work can
// be replayed once the cause is fixed.
func TestGivenARefusedRecordWhenDeadLetteredThenShouldKeepTheWork(t *testing.T) {
	server := &streamServer{}
	handle := streamHandle(server, laser.Options{DefaultStream: "agents"})
	message, err := iggcon.NewIggyMessage([]byte("the work"))
	if err != nil {
		t.Fatalf("build a record: %v", err)
	}
	message.Header.Offset = 17
	record := &laser.Record{
		Message:     message,
		PartitionID: 2,
		Provenance:  &laser.Provenance{Conversation: conversation(5)},
	}
	decision := laser.Decision{
		Verdict: laser.DeadLetter,
		Reason:  wire.DeadLetterDeadlineExceeded,
		Detail:  "it expired",
	}

	if err := handle.DeadLetter(context.Background(), record, decision, 3); err != nil {
		t.Fatalf("dead letter: %v", err)
	}

	var capsule wire.AgentDeadLetter
	if err := wire.Unmarshal(server.sentMessages[0].Payload, &capsule); err != nil {
		t.Fatalf("decode the capsule: %v", err)
	}
	if string(capsule.Payload) != "the work" {
		t.Errorf("want the work kept, got %q", capsule.Payload)
	}
	if capsule.Source.Offset != 17 || capsule.Source.PartitionID != 2 {
		t.Errorf("want the original named, got %+v", capsule.Source)
	}
	if capsule.Reason != wire.DeadLetterDeadlineExceeded || capsule.Attempts != 3 {
		t.Errorf("want the reason and attempts, got %+v", capsule)
	}
	// One conversation's dead letters stay in the order they failed.
	if string(server.sentPartitions.Value) != conversation(5).String() {
		t.Errorf("want the conversation as the key, got %q", server.sentPartitions.Value)
	}
}

func TestGivenNoStreamWhenDeadLetteringThenShouldRefuseIt(t *testing.T) {
	handle := streamHandle(&streamServer{}, laser.Options{})

	err := handle.DeadLetter(context.Background(), &laser.Record{}, laser.Decision{}, 1)

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "DefaultStream") {
		t.Errorf("want the remedy named, got %v", err)
	}
}
