package laser_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// reply builds an answer on one correlation, as a target would send it.
func reply(
	t *testing.T, kind wire.AgentKind, session wire.ConversationID,
	correlation wire.CorrelationID, target *wire.AgentID,
) *laser.AgdxMessage {
	t.Helper()
	record := &laser.Record{Provenance: &laser.Provenance{Conversation: session}}
	return &laser.AgdxMessage{
		Envelope: wire.AgentEnvelope{
			Kind:        kind,
			Correlation: &correlation,
			Target:      target,
		},
		Record: record,
	}
}

func working(
	t *testing.T, session wire.ConversationID, correlation wire.CorrelationID,
) *laser.AgdxMessage {
	t.Helper()
	operation := wire.OperationTask
	state := wire.TaskWorking
	record := &laser.Record{Provenance: &laser.Provenance{Conversation: session}}
	return &laser.AgdxMessage{
		Envelope: wire.AgentEnvelope{
			Kind:        wire.KindStatus,
			Correlation: &correlation,
			Operation:   &operation,
			TaskState:   &state,
		},
		Record: record,
	}
}

func TestGivenAReplyWhenDeliveredThenShouldWakeItsWaiter(t *testing.T) {
	hub := laser.NewReplyHub()
	requester := agentID(t, "planner")
	ticket, err := hub.Subscribe(correlationID(1), laser.ExpectedReply{
		Session: conversation(1), Requester: requester,
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if !hub.Deliver(reply(t, wire.KindResponse, conversation(1), correlationID(1), &requester)) {
		t.Fatal("want the reply taken")
	}

	answer, err := ticket.Await(context.Background(), time.Second)
	if err != nil {
		t.Fatalf("await: %v", err)
	}
	if answer.Envelope.Kind != wire.KindResponse {
		t.Errorf("want the response, got %q", answer.Envelope.Kind)
	}
	if hub.Waiting() != 0 {
		t.Error("a settled wait must be released")
	}
}

// A failure is an answer too: a caller waiting on a request has to be woken by
// one, or it waits out its deadline for a reply that already came.
func TestGivenAFailureWhenDeliveredThenShouldAlsoAnswer(t *testing.T) {
	hub := laser.NewReplyHub()
	ticket, err := hub.Subscribe(correlationID(1), laser.ExpectedReply{
		Session: conversation(1),
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	hub.Deliver(reply(t, wire.KindError, conversation(1), correlationID(1), nil))

	answer, err := ticket.Await(context.Background(), time.Second)
	if err != nil {
		t.Fatalf("await: %v", err)
	}
	if answer.Envelope.Kind != wire.KindError {
		t.Errorf("want the failure, got %q", answer.Envelope.Kind)
	}
}

// Two conversations can reuse one correlation, so a correlation alone must not
// wake a waiter: it would act on somebody else's answer.
func TestGivenAReplyInAnotherSessionWhenDeliveredThenShouldNotWakeTheWaiter(t *testing.T) {
	hub := laser.NewReplyHub()
	ticket, err := hub.Subscribe(correlationID(1), laser.ExpectedReply{
		Session: conversation(1),
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if hub.Deliver(reply(t, wire.KindResponse, conversation(2), correlationID(1), nil)) {
		t.Error("a reply from another session must not be taken")
	}

	_, err = ticket.Await(context.Background(), 50*time.Millisecond)
	var timeout *laser.TimeoutError
	if !errors.As(err, &timeout) {
		t.Fatalf("want a timeout, got %T: %v", err, err)
	}
}

// A shared reply topic carries every agent's answers, so a waiter that knows
// who it is will not take one addressed elsewhere.
func TestGivenAReplyForAnotherAgentWhenDeliveredThenShouldNotWakeTheWaiter(t *testing.T) {
	hub := laser.NewReplyHub()
	someoneElse := agentID(t, "auditor")
	if _, err := hub.Subscribe(correlationID(1), laser.ExpectedReply{
		Session: conversation(1), Requester: agentID(t, "planner"),
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if hub.Deliver(reply(t, wire.KindResponse,
		conversation(1), correlationID(1), &someoneElse)) {
		t.Error("another agent's reply must not be taken")
	}
	// An unaddressed reply could be for anyone, so a waiter that knows who it
	// is will not take that either.
	if hub.Deliver(reply(t, wire.KindResponse, conversation(1), correlationID(1), nil)) {
		t.Error("an unaddressed reply must not satisfy a named waiter")
	}
}

// A sole consumer of a private reply topic takes whatever arrives, since
// nothing else is writing to it.
func TestGivenAnUnnamedWaiterWhenAnyReplyArrivesThenShouldTakeIt(t *testing.T) {
	hub := laser.NewReplyHub()
	if _, err := hub.Subscribe(correlationID(1), laser.ExpectedReply{
		Session: conversation(1),
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if !hub.Deliver(reply(t, wire.KindResponse, conversation(1), correlationID(1), nil)) {
		t.Error("a waiter that does not care must take any reply")
	}
}

// A command or an event on the same correlation is somebody else's traffic in
// the same exchange, not the answer.
func TestGivenANonAnswerOnTheCorrelationWhenDeliveredThenShouldBeLeftAlone(t *testing.T) {
	hub := laser.NewReplyHub()
	if _, err := hub.Subscribe(correlationID(1), laser.ExpectedReply{
		Session: conversation(1),
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	for _, kind := range []wire.AgentKind{wire.KindCommand, wire.KindEvent, wire.KindChunk} {
		if hub.Deliver(reply(t, kind, conversation(1), correlationID(1), nil)) {
			t.Errorf("a %s is not an answer", kind)
		}
	}
}

// A reply answering a different command is a different exchange, even on one
// correlation.
func TestGivenAReplyToAnotherCommandWhenDeliveredThenShouldNotWakeTheWaiter(t *testing.T) {
	hub := laser.NewReplyHub()
	mine := wire.RecordID{ID: wire.IDFromBytes([16]byte{15: 1})}
	theirs := wire.RecordID{ID: wire.IDFromBytes([16]byte{15: 2})}
	if _, err := hub.Subscribe(correlationID(1), laser.ExpectedReply{
		Session: conversation(1), Command: &mine,
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	message := reply(t, wire.KindResponse, conversation(1), correlationID(1), nil)
	message.Envelope.Cause = &theirs
	if hub.Deliver(message) {
		t.Error("a reply to another command must not be taken")
	}

	message.Envelope.Cause = &mine
	if !hub.Deliver(message) {
		t.Error("the reply to this command must be taken")
	}
}

// The deadline is for a reply that never comes, not for a handler that is
// slow, so an acknowledgment extends the wait rather than ending it.
func TestGivenAnAcknowledgmentWhenWaitingThenShouldExtendTheDeadline(t *testing.T) {
	hub := laser.NewReplyHub()
	ticket, err := hub.Subscribe(correlationID(1), laser.ExpectedReply{
		Session: conversation(1),
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Keep saying "working" for longer than the deadline, then answer.
	go func() {
		for range 4 {
			time.Sleep(40 * time.Millisecond)
			hub.Deliver(working(t, conversation(1), correlationID(1)))
		}
		hub.Deliver(reply(t, wire.KindResponse, conversation(1), correlationID(1), nil))
	}()

	answer, err := ticket.Await(context.Background(), 100*time.Millisecond)
	if err != nil {
		t.Fatalf("a working target must not time out: %v", err)
	}
	if answer.Envelope.Kind != wire.KindResponse {
		t.Errorf("want the answer, got %q", answer.Envelope.Kind)
	}
}

// A silent target is a different thing from a slow one, and only the silent
// one times out.
func TestGivenASilentTargetWhenWaitingThenShouldTimeOut(t *testing.T) {
	hub := laser.NewReplyHub()
	ticket, err := hub.Subscribe(correlationID(1), laser.ExpectedReply{
		Session: conversation(1),
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	started := time.Now()
	_, err = ticket.Await(context.Background(), 60*time.Millisecond)

	var timeout *laser.TimeoutError
	if !errors.As(err, &timeout) {
		t.Fatalf("want a TimeoutError, got %T: %v", err, err)
	}
	if !timeout.Timeout() {
		t.Error("a timeout must report itself as one, so a caller can retry on it")
	}
	if timeout.Correlation != correlationID(1) {
		t.Error("a caller running many requests needs to know which gave up")
	}
	if time.Since(started) > 3*time.Second {
		t.Error("want the deadline honored")
	}
	if hub.Waiting() != 0 {
		t.Error("a given-up wait must be released")
	}
}

// An acknowledgment tells a caller the request was picked up, so a slow
// handler is waited out rather than retried into a duplicate.
func TestGivenAnAcknowledgmentWhenCheckedThenShouldReportTheTargetIsWorking(t *testing.T) {
	hub := laser.NewReplyHub()
	ticket, err := hub.Subscribe(correlationID(1), laser.ExpectedReply{
		Session: conversation(1),
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer ticket.Close()

	if ticket.Acknowledged() {
		t.Error("nothing has acknowledged yet")
	}
	hub.Deliver(working(t, conversation(1), correlationID(1)))
	if !ticket.Acknowledged() {
		t.Error("want the acknowledgment seen")
	}
}

// The reply cannot exist until the request is sent, so a hub that only started
// listening afterwards could miss an answer that arrived in between.
func TestGivenTwoWaitsOnOneCorrelationWhenSubscribedThenShouldRefuseTheSecond(t *testing.T) {
	hub := laser.NewReplyHub()
	first, err := hub.Subscribe(correlationID(1), laser.ExpectedReply{
		Session: conversation(1),
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if _, err := hub.Subscribe(correlationID(1), laser.ExpectedReply{
		Session: conversation(1),
	}); err == nil {
		t.Error("two waits on one correlation must not both be registered")
	}

	// Releasing the first makes the correlation free again.
	first.Close()
	if _, err := hub.Subscribe(correlationID(1), laser.ExpectedReply{
		Session: conversation(1),
	}); err != nil {
		t.Errorf("a released correlation must be reusable: %v", err)
	}
}

// A message nobody is waiting for belongs to another exchange and is some
// other reader's business.
func TestGivenAnUnawaitedReplyWhenDeliveredThenShouldBeLeftAlone(t *testing.T) {
	hub := laser.NewReplyHub()

	if hub.Deliver(reply(t, wire.KindResponse, conversation(1), correlationID(9), nil)) {
		t.Error("a reply nobody waits for must be left alone")
	}
	// A message carrying no correlation belongs to no exchange at all.
	message := reply(t, wire.KindResponse, conversation(1), correlationID(1), nil)
	message.Envelope.Correlation = nil
	if hub.Deliver(message) {
		t.Error("a message with no correlation answers nothing")
	}
}

// A second answer is a duplicate of a settled exchange and must not overwrite
// the first.
func TestGivenASecondAnswerWhenDeliveredThenShouldKeepTheFirst(t *testing.T) {
	hub := laser.NewReplyHub()
	ticket, err := hub.Subscribe(correlationID(1), laser.ExpectedReply{
		Session: conversation(1),
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	first := reply(t, wire.KindResponse, conversation(1), correlationID(1), nil)
	first.Envelope.Body = []byte("first")
	second := reply(t, wire.KindResponse, conversation(1), correlationID(1), nil)
	second.Envelope.Body = []byte("second")

	hub.Deliver(first)
	if hub.Deliver(second) {
		t.Error("a settled exchange must not take a second answer")
	}

	answer, err := ticket.Await(context.Background(), time.Second)
	if err != nil {
		t.Fatalf("await: %v", err)
	}
	if string(answer.Envelope.Body) != "first" {
		t.Errorf("want the first answer kept, got %q", answer.Envelope.Body)
	}
}

func TestGivenACancelledContextWhenWaitingThenShouldStopWaiting(t *testing.T) {
	hub := laser.NewReplyHub()
	ticket, err := hub.Subscribe(correlationID(1), laser.ExpectedReply{
		Session: conversation(1),
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := ticket.Await(ctx, time.Minute); !errors.Is(err, context.Canceled) {
		t.Errorf("want the cancellation, got %v", err)
	}
}

// A hub serves many waiters at once, so its bookkeeping runs under the race
// detector.
func TestGivenConcurrentWaitersWhenServedThenShouldMatchEachToItsOwn(t *testing.T) {
	hub := laser.NewReplyHub()
	var waiting sync.WaitGroup

	for index := range 16 {
		correlation := correlationID(byte(index))
		ticket, err := hub.Subscribe(correlation, laser.ExpectedReply{
			Session: conversation(byte(index)),
		})
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}
		waiting.Add(1)
		go func() {
			defer waiting.Done()
			answer, err := ticket.Await(context.Background(), 5*time.Second)
			if err != nil {
				t.Errorf("await: %v", err)
				return
			}
			// Each waiter must get its own answer, not another's.
			if *answer.Envelope.Correlation != correlation {
				t.Errorf("want %v, got %v", correlation, answer.Envelope.Correlation)
			}
		}()
	}

	for index := range 16 {
		go hub.Deliver(reply(t, wire.KindResponse,
			conversation(byte(index)), correlationID(byte(index)), nil))
	}
	waiting.Wait()

	if hub.Waiting() != 0 {
		t.Errorf("every wait must be released, %d left", hub.Waiting())
	}
}
