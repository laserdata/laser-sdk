package laser_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// The point of the cache: ten concurrent contracts on one reply topic read
// it once between them rather than opening ten polls over the same records.
func TestGivenConcurrentContractsOnOneTopicWhenSentThenShouldShareOneDispatcher(t *testing.T) {
	server := &contractServer{
		replyKind: wire.KindResponse, replyBody: []byte("done"),
		replySource: agentID(t, "worker"),
	}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	defer handle.Close()

	var group sync.WaitGroup
	states := make([]laser.ContractState, 4)
	for index := range states {
		group.Add(1)
		go func(slot int) {
			defer group.Done()
			contract, err := handle.Contract(laser.To(agentID(t, "worker")), "agent.sessions").
				From(agentID(t, "orchestrator")).
				Payload([]byte(`{"task":"run"}`)).
				Deadline(5 * time.Second).
				Send(context.Background())
			if err != nil {
				t.Errorf("branch %d: %v", slot, err)
				return
			}
			states[slot] = contract.State
		}(index)
	}
	group.Wait()

	for index, state := range states {
		if state != laser.ContractCompleted {
			t.Errorf("branch %d: want completed, got %v", index, state)
		}
	}
	// The count of pollers started, not the count currently held: a cache
	// that never hit would re-open under the same key and still leave one
	// entry behind, so only the opened count can tell the two apart.
	if opened := laser.ReplyDispatchersOpenedForTest(handle); opened != 1 {
		t.Errorf("want one poller started for one reply topic, got %d", opened)
	}
}

// Two reply topics are two exchanges, so they get a reader each: the cache
// is per topic, not one reader for the whole connection.
func TestGivenTwoReplyTopicsWhenContractsSentThenShouldOpenADispatcherEach(t *testing.T) {
	server := &contractServer{
		replyKind: wire.KindResponse, replyBody: []byte("done"),
		replySource: agentID(t, "worker"),
	}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	defer handle.Close()

	for _, topic := range []string{"replies-a", "replies-b"} {
		contract, err := handle.Contract(laser.To(agentID(t, "worker")), topic).
			From(agentID(t, "orchestrator")).
			Payload([]byte(`{"task":"run"}`)).
			Deadline(5 * time.Second).
			Send(context.Background())
		if err != nil {
			t.Fatalf("contract on %s: %v", topic, err)
		}
		if contract.State != laser.ContractCompleted {
			t.Fatalf("contract on %s: want completed, got %v", topic, contract.State)
		}
	}
	if opened := laser.ReplyDispatchersOpenedForTest(handle); opened != 2 {
		t.Errorf("want a poller started per reply topic, got %d", opened)
	}
}

// A dispatcher outlives every waiter, so something has to end it. Go has no
// destructor, which makes Close the only thing that can.
func TestGivenAnOpenDispatcherWhenTheHandleIsClosedThenShouldStopItsGoroutine(t *testing.T) {
	server := &contractServer{
		replyKind: wire.KindResponse, replyBody: []byte("done"),
		replySource: agentID(t, "worker"),
	}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	if _, err := handle.Contract(laser.To(agentID(t, "worker")), "agent.sessions").
		From(agentID(t, "orchestrator")).
		Payload([]byte(`{"task":"run"}`)).
		Deadline(5 * time.Second).
		Send(context.Background()); err != nil {
		t.Fatalf("send the contract: %v", err)
	}
	done := laser.ReplyDispatcherDoneForTest(handle)
	if len(done) != 1 {
		t.Fatalf("want one dispatcher open before closing, got %d", len(done))
	}

	if err := handle.Close(); err != nil {
		t.Fatalf("close the handle: %v", err)
	}
	for index, stopped := range done {
		select {
		case <-stopped:
		case <-time.After(5 * time.Second):
			t.Errorf("dispatcher %d is still running after Close", index)
		}
	}
	if open := laser.ReplyDispatchersForTest(handle); open != 0 {
		t.Errorf("want no dispatchers held after Close, got %d", open)
	}
}

// A contract that armed its own verifier keeps the private poll: the hub's
// verifier is per dispatcher and shared by every waiter on that topic, so
// one caller's key registry must not silently become every caller's.
func TestGivenAVerifiedContractWhenSentThenShouldNotJoinTheSharedDispatcher(t *testing.T) {
	key, err := laser.GenerateSigningKey()
	if err != nil {
		t.Fatalf("generate a signing key: %v", err)
	}
	keys := laser.NewKeyRegistry()
	keys.Enroll("worker", key.VerifyingKey())
	server := &contractServer{
		replyKind: wire.KindResponse, replyBody: []byte("done"),
		replySource: agentID(t, "worker"), replyKey: key,
	}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	defer handle.Close()

	contract, err := handle.Contract(laser.To(agentID(t, "worker")), "agent.sessions").
		From(agentID(t, "orchestrator")).
		Payload([]byte(`{"task":"run"}`)).
		Verifier(keys).
		Deadline(2 * time.Second).
		Send(context.Background())
	if err != nil {
		t.Fatalf("send the verified contract: %v", err)
	}
	if contract.State != laser.ContractCompleted {
		t.Fatalf("want completed, got %v", contract.State)
	}
	if opened := laser.ReplyDispatchersOpenedForTest(handle); opened != 0 {
		t.Errorf("a verified contract should not start a shared poller, got %d", opened)
	}
}

// The wait is registered before the command is sent, so an answer that
// lands between the two is taken rather than missed.
func TestGivenAnAnswerThatLandsBeforeTheWaitBeginsThenShouldStillBeTaken(t *testing.T) {
	// contractServer answers inside SendMessages, synchronously, so the
	// reply is already on the topic before Send returns. Nothing in the
	// contract has started waiting at that point: only a subscription taken
	// before the send can catch it.
	server := &contractServer{
		replyKind: wire.KindResponse, replyBody: []byte("done"),
		replySource: agentID(t, "worker"),
	}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	defer handle.Close()

	contract, err := handle.Contract(laser.To(agentID(t, "worker")), "agent.sessions").
		From(agentID(t, "orchestrator")).
		Payload([]byte(`{"task":"run"}`)).
		Deadline(5 * time.Second).
		Send(context.Background())
	if err != nil {
		t.Fatalf("send the contract: %v", err)
	}
	if contract.State != laser.ContractCompleted {
		t.Fatalf("want the already-landed answer taken, got %v", contract.State)
	}
}
