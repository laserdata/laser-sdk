package lasertest_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/laser/lasertest"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenATestMessageWhenBuiltThenShouldCarryItsPayloadAndProvenance(t *testing.T) {
	provenance := &laser.Provenance{Conversation: laser.NewConversationID()}
	payload := []byte("hello")

	message := lasertest.AgentMessage(payload, provenance)

	want := &laser.AgentMessage{Record: &laser.Record{
		Message:    iggcon.IggyMessage{Payload: []byte("hello")},
		Provenance: provenance,
	}}
	if !reflect.DeepEqual(message, want) {
		t.Fatalf("got %+v, want %+v", message, want)
	}
	if message.Envelope != nil || message.ID() != laser.NewMessageID(0, 0) {
		t.Fatalf("a test message is a plain record at the start of partition 0, got %+v", message)
	}
	payload[0] = 'j'
	if string(message.Body()) != "hello" {
		t.Fatal("the message must not alias the caller's payload")
	}
}

func TestGivenATestContextWhenAHandlerReadsItThenShouldSeeTheMessageAndTheAgent(t *testing.T) {
	client := laser.FromClient(nil, laser.Options{})
	worker := mustAgent(t, "worker")
	message := lasertest.AgentMessage([]byte("task"), &laser.Provenance{Conversation: laser.NewConversationID()})

	agentCtx, err := lasertest.AgentCtx(client, message, lasertest.AgentCtxOptions{Agent: worker})
	if err != nil {
		t.Fatalf("context: %v", err)
	}

	var seen []byte
	handler := func(_ context.Context, agent *laser.AgentCtx) error {
		seen = agent.Message().Body()
		return nil
	}
	if err := handler(context.Background(), agentCtx); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if string(seen) != "task" || agentCtx.Identity() != worker || agentCtx.Laser() != client {
		t.Fatalf("got body %q, agent %s", seen, agentCtx.Identity())
	}
	if agentCtx.Message() != message {
		t.Fatal("the context must hand over the message it was built with")
	}
}

func TestGivenATestContextWithoutAReplyTopicWhenRespondingThenShouldRefuseIt(t *testing.T) {
	client := laser.FromClient(nil, laser.Options{})
	message := lasertest.AgentMessage([]byte("task"), &laser.Provenance{Conversation: laser.NewConversationID()})
	agentCtx, err := lasertest.AgentCtx(client, message, lasertest.AgentCtxOptions{Agent: mustAgent(t, "worker")})
	if err != nil {
		t.Fatalf("context: %v", err)
	}

	err = agentCtx.Respond(context.Background(), []byte("done"))

	if _, refused := err.(*laser.NoRespondTopicError); !refused {
		t.Fatalf("got %v, want the missing reply topic refusal", err)
	}
}

func TestGivenATestClockWhenAdvancedThenShouldReportTheSetTime(t *testing.T) {
	start := time.UnixMicro(1_000)
	clock := lasertest.NewTestClock(start)
	if !clock.Now().Equal(start) {
		t.Fatalf("got %v", clock.Now())
	}
	clock.Advance(500 * time.Microsecond)
	clock.Advance(2 * time.Millisecond)
	if got := clock.Now().UnixMicro(); got != 3_500 {
		t.Fatalf("got %d", got)
	}
	clock.Set(time.UnixMicro(42))
	if got := clock.Now().UnixMicro(); got != 42 {
		t.Fatalf("got %d", got)
	}
}

func TestGivenATestClockWhenItPassesADeadlineThenShouldDeadLetterTheRecord(t *testing.T) {
	clock := lasertest.NewTestClock(time.UnixMicro(1_000_000))
	deadline := time.UnixMicro(2_000_000)
	agent := laser.FromClient(nil, laser.Options{}).
		Agent(mustAgent(t, "worker"), laser.AgentOptions{Now: clock.Now})
	record := func() *laser.Record {
		return lasertest.AgentMessage([]byte("task"),
			&laser.Provenance{Conversation: laser.NewConversationID(), DeadlineAt: &deadline}).Record
	}

	if verdict := agent.Decide(record()).Verdict; verdict != laser.Deliver {
		t.Fatalf("before the deadline: got %v", verdict)
	}
	clock.Advance(2 * time.Second)
	decision := agent.Decide(record())
	if decision.Verdict != laser.DeadLetter || decision.Reason != wire.DeadLetterDeadlineExceeded {
		t.Fatalf("after the deadline: got %+v", decision)
	}
}

func mustAgent(t *testing.T, name string) wire.AgentID {
	t.Helper()
	agent, err := wire.NewAgentID(name)
	if err != nil {
		t.Fatalf("agent id %q: %v", name, err)
	}
	return agent
}
