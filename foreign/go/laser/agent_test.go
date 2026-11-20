package laser_test

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func boolPointer(value bool) *bool { return &value }

// recordWith builds a record carrying one spine, for exercising the gates.
func recordWith(provenance *laser.Provenance) *laser.Record {
	return &laser.Record{Provenance: provenance}
}

// A holder that lost its lease and came back with stale work is the case the
// fence exists for: its effect must not be applied.
func TestGivenAStaleFenceWhenGatedThenShouldRefuseIt(t *testing.T) {
	gate := laser.NewFenceGate()
	task := conversation(1)
	now := time.Now()

	if !gate.Accept(task, uint64Pointer(5), now) {
		t.Fatal("the first fence must be accepted")
	}
	if gate.Accept(task, uint64Pointer(4), now) {
		t.Error("a fence behind the high water must be refused")
	}
	// The same holder's legitimate retry carries the same fence, which dedup
	// then handles rather than the gate.
	if !gate.Accept(task, uint64Pointer(5), now) {
		t.Error("an equal fence is the same holder retrying and must be accepted")
	}
	if !gate.Accept(task, uint64Pointer(6), now) {
		t.Error("a fence ahead of the high water must be accepted")
	}
	if high, seen := gate.HighWater(task); !seen || high != 6 {
		t.Errorf("want the high water advanced to 6, got %d (%v)", high, seen)
	}
}

// The gate refuses stale holders. It does not require everything to be fenced,
// so unfenced work passes.
func TestGivenNoFenceWhenGatedThenShouldPass(t *testing.T) {
	gate := laser.NewFenceGate()

	if !gate.Accept(conversation(1), nil, time.Now()) {
		t.Error("a record carrying no fence is not fenced work")
	}
	if _, seen := gate.HighWater(conversation(1)); seen {
		t.Error("an unfenced record must not create a gate entry")
	}
}

// Two tasks have independent fences, or one task's progress would lock
// another out.
func TestGivenTwoTasksWhenGatedThenShouldKeepSeparateHighWaters(t *testing.T) {
	gate := laser.NewFenceGate()
	now := time.Now()

	gate.Accept(conversation(1), uint64Pointer(9), now)
	if !gate.Accept(conversation(2), uint64Pointer(1), now) {
		t.Error("another task's fence must not be gated by the first")
	}
}

// A redelivery must not apply an operation twice, which is what the window is
// for.
func TestGivenARepeatedKeyWhenDeduplicatedThenShouldRefuseTheSecond(t *testing.T) {
	dedup := laser.NewDeduplicator(0)

	if !dedup.Accept("order-7") {
		t.Fatal("the first sighting must be accepted")
	}
	if dedup.Accept("order-7") {
		t.Error("a repeat must be refused")
	}
	if !dedup.Accept("order-8") {
		t.Error("a different key must be accepted")
	}
}

// A record with no key has nothing to compare against, so it always passes.
func TestGivenNoDedupKeyWhenDeduplicatedThenShouldPass(t *testing.T) {
	dedup := laser.NewDeduplicator(0)

	if !dedup.Accept("") || !dedup.Accept("") {
		t.Error("a record with no key must always pass")
	}
	if dedup.Remembered() != 0 {
		t.Error("an empty key must not be remembered")
	}
}

// The window is bounded by itself rather than by the traffic through it, or a
// long-lived consumer grows without limit.
func TestGivenMoreKeysThanTheWindowWhenDeduplicatedThenShouldForgetTheOldest(t *testing.T) {
	dedup := laser.NewDeduplicator(3)

	for _, key := range []string{"a", "b", "c", "d"} {
		dedup.Accept(key)
	}

	if dedup.Remembered() != 3 {
		t.Errorf("want the window bounded at 3, got %d", dedup.Remembered())
	}
	// The oldest left, so it is seen as new again. That is the cost of a
	// bounded window and why dedup is best-effort.
	if !dedup.Accept("a") {
		t.Error("the oldest key must have left the window")
	}
	if dedup.Accept("d") {
		t.Error("a recent key must still be remembered")
	}
}

// One producer must not be able to suppress or replay another's key, so the
// key is scoped to the agent that wrote it.
func TestGivenTwoAgentsSharingAKeyWhenScopedThenShouldNotCollide(t *testing.T) {
	first := laser.DedupKey(&laser.Provenance{
		Conversation:   conversation(1),
		Agent:          agentPointer(t, "planner"),
		IdempotencyKey: textPointer("order-7"),
	})
	second := laser.DedupKey(&laser.Provenance{
		Conversation:   conversation(1),
		Agent:          agentPointer(t, "executor"),
		IdempotencyKey: textPointer("order-7"),
	})

	if first == second {
		t.Error("two agents using one key must not collide")
	}
	if !strings.Contains(first, "order-7") || !strings.Contains(first, "planner") {
		t.Errorf("want the key scoped to its agent, got %q", first)
	}

	unscoped := laser.DedupKey(&laser.Provenance{
		Conversation:   conversation(1),
		IdempotencyKey: textPointer("order-7"),
	})
	if unscoped != "order-7" {
		t.Errorf("a record with no agent carries the key alone, got %q", unscoped)
	}
	if laser.DedupKey(&laser.Provenance{Conversation: conversation(1)}) != "" {
		t.Error("a record with no key has none")
	}
	if laser.DedupKey(nil) != "" {
		t.Error("a record with no spine has no key")
	}
}

func agentPointer(t *testing.T, name string) *wire.AgentID {
	t.Helper()
	agent := agentID(t, name)
	return &agent
}

// A record past its deadline is one nobody is waiting for, so it is
// dead-lettered rather than worked on.
func TestGivenAnExpiredRecordWhenDecidedThenShouldDeadLetterIt(t *testing.T) {
	past := time.Now().Add(-time.Minute)
	agent := laser.FromClient(&streamServer{}, laser.Options{}).
		Agent(agentID(t, "planner"), laser.AgentOptions{})

	decision := agent.Decide(recordWith(&laser.Provenance{
		Conversation: conversation(1), DeadlineAt: &past,
	}))

	if decision.Verdict != laser.DeadLetter {
		t.Fatalf("want a dead letter, got %s", decision.Verdict)
	}
	if decision.Reason != wire.DeadLetterDeadlineExceeded {
		t.Errorf("want the deadline reason, got %d", decision.Reason)
	}
}

// Expiry is checked before dedup, because burning a key on a record that can
// never be handled would suppress a later record that could be.
func TestGivenAnExpiredRecordWhenDecidedThenShouldNotSpendItsDedupKey(t *testing.T) {
	past := time.Now().Add(-time.Minute)
	dedup := laser.NewDeduplicator(0)
	agent := laser.FromClient(&streamServer{}, laser.Options{}).
		Agent(agentID(t, "planner"), laser.AgentOptions{Dedup: dedup})

	agent.Decide(recordWith(&laser.Provenance{
		Conversation:   conversation(1),
		DeadlineAt:     &past,
		IdempotencyKey: textPointer("order-7"),
	}))

	if dedup.Remembered() != 0 {
		t.Error("a record that can never be handled must not spend its key")
	}
	// The same operation, arriving in time, is still handled.
	live := agent.Decide(recordWith(&laser.Provenance{
		Conversation:   conversation(1),
		IdempotencyKey: textPointer("order-7"),
	}))
	if live.Verdict != laser.Deliver {
		t.Errorf("want the live record delivered, got %s", live.Verdict)
	}
}

// A stale holder's replay must not advance anything, so the fence is checked
// before the key is spent. It is skipped rather than kept: the legitimate
// holder's own record does the work, so there is nothing here to redrive.
func TestGivenAFencedOutRecordWhenDecidedThenShouldSkipIt(t *testing.T) {
	dedup := laser.NewDeduplicator(0)
	agent := laser.FromClient(&streamServer{}, laser.Options{}).
		Agent(agentID(t, "planner"), laser.AgentOptions{Dedup: dedup})

	ahead := agent.Decide(recordWith(&laser.Provenance{
		Conversation: conversation(1), FenceToken: uint64Pointer(5),
	}))
	if ahead.Verdict != laser.Deliver {
		t.Fatalf("want the first record delivered, got %s", ahead.Verdict)
	}

	behind := agent.Decide(recordWith(&laser.Provenance{
		Conversation:   conversation(1),
		FenceToken:     uint64Pointer(4),
		IdempotencyKey: textPointer("order-7"),
	}))

	if behind.Verdict != laser.Skip {
		t.Fatalf("want the replay skipped, got %s", behind.Verdict)
	}
	if dedup.Remembered() != 0 {
		t.Error("a fenced-out record must not spend its key either")
	}
}

// A record whose operation was already handled must not be applied twice.
func TestGivenAnAlreadyHandledRecordWhenDecidedThenShouldSkipIt(t *testing.T) {
	agent := laser.FromClient(&streamServer{}, laser.Options{}).
		Agent(agentID(t, "planner"), laser.AgentOptions{})
	spine := func() *laser.Provenance {
		return &laser.Provenance{
			Conversation:   conversation(1),
			Agent:          agentPointer(t, "planner"),
			IdempotencyKey: textPointer("order-7"),
		}
	}

	if verdict := agent.Decide(recordWith(spine())).Verdict; verdict != laser.Deliver {
		t.Fatalf("want the first delivered, got %s", verdict)
	}
	decision := agent.Decide(recordWith(spine()))
	if decision.Verdict != laser.Skip {
		t.Errorf("want the repeat skipped, got %s", decision.Verdict)
	}
}

// A message asking for a feature this build does not implement must not be
// mis-handled, which is the whole point of the bitset.
func TestGivenAnUnmetRequirementWhenDecidedThenShouldDeadLetterIt(t *testing.T) {
	agent := laser.FromClient(&streamServer{}, laser.Options{}).
		Agent(agentID(t, "planner"), laser.AgentOptions{Understood: wire.FeatureNone})

	decision := agent.DecideMessage(&laser.AgdxMessage{
		Envelope: wire.AgentEnvelope{MustUnderstand: 1 << 3},
		Record:   recordWith(&laser.Provenance{Conversation: conversation(1)}),
	})

	if decision.Verdict != laser.DeadLetter {
		t.Fatalf("want a dead letter, got %s", decision.Verdict)
	}
	if decision.Reason != wire.DeadLetterRejected {
		t.Errorf("want the rejected reason, got %d", decision.Reason)
	}
}

// Work addressed to another agent is not this agent's to do, and skipping it
// is not a failure.
func TestGivenAMessageForAnotherAgentWhenDecidedThenShouldSkipIt(t *testing.T) {
	agent := laser.FromClient(&streamServer{}, laser.Options{}).
		Agent(agentID(t, "planner"), laser.AgentOptions{})
	target := agentID(t, "executor")

	decision := agent.DecideMessage(&laser.AgdxMessage{
		Envelope: wire.AgentEnvelope{Target: &target},
		Record:   recordWith(&laser.Provenance{Conversation: conversation(1)}),
	})

	if decision.Verdict != laser.Skip {
		t.Errorf("want it skipped, got %s", decision.Verdict)
	}
}

// Most records on a shared session topic are broadcasts, so they are delivered
// by default and an agent that wants only addressed work opts out.
func TestGivenABroadcastWhenDecidedThenShouldFollowTheAgentsChoice(t *testing.T) {
	broadcast := &laser.AgdxMessage{
		Envelope: wire.AgentEnvelope{},
		Record:   recordWith(&laser.Provenance{Conversation: conversation(1)}),
	}

	listening := laser.FromClient(&streamServer{}, laser.Options{}).
		Agent(agentID(t, "planner"), laser.AgentOptions{})
	if listening.DecideMessage(broadcast).Verdict != laser.Deliver {
		t.Error("a broadcast reaches an agent that accepts them")
	}

	addressedOnly := laser.FromClient(&streamServer{}, laser.Options{}).
		Agent(agentID(t, "planner"),
			laser.AgentOptions{AcceptBroadcast: boolPointer(false)})
	if addressedOnly.DecideMessage(broadcast).Verdict != laser.Skip {
		t.Error("an agent that wants addressed work only must skip a broadcast")
	}
}

// A dead letter keeps the payload so the work can be replayed once the cause
// is fixed, and names where the original sat.
func TestGivenADeadLetterWhenCapsulatedThenShouldKeepThePayloadAndSource(t *testing.T) {
	record := &laser.Record{Provenance: &laser.Provenance{Conversation: conversation(1)}}
	record.Message.Payload = []byte("the work")
	source := wire.LogPosition{StreamID: 1, TopicID: 2, PartitionID: 3, Offset: 4}
	decision := laser.Decision{
		Verdict: laser.DeadLetter,
		Reason:  wire.DeadLetterDeadlineExceeded,
		Detail:  "it expired",
	}

	capsule := laser.DeadLetterCapsule(source, record, decision, 3)

	if string(capsule.Payload) != "the work" {
		t.Errorf("want the work kept, got %q", capsule.Payload)
	}
	if capsule.Source != source {
		t.Errorf("want the source named, got %+v", capsule.Source)
	}
	if capsule.Reason != wire.DeadLetterDeadlineExceeded || capsule.Attempts != 3 {
		t.Errorf("want the reason and attempts, got %+v", capsule)
	}
	if capsule.Detail == nil || *capsule.Detail != "it expired" {
		t.Errorf("want the detail, got %v", capsule.Detail)
	}
}

// The gates are shared across a consumer's goroutines, so they are exercised
// under the race detector.
func TestGivenConcurrentDecisionsWhenRunThenShouldGateSafely(t *testing.T) {
	agent := laser.FromClient(&streamServer{}, laser.Options{}).
		Agent(agentID(t, "planner"), laser.AgentOptions{})

	var waiting sync.WaitGroup
	for worker := range 16 {
		waiting.Add(1)
		go func(worker int) {
			defer waiting.Done()
			for round := range 32 {
				agent.Decide(recordWith(&laser.Provenance{
					Conversation: conversation(byte(worker)),
					FenceToken:   uint64Pointer(uint64(round)),
				}))
			}
		}(worker)
	}
	waiting.Wait()

	if agent.Fence().Tracked() == 0 {
		t.Error("want the gate to have tracked the tasks")
	}
}
