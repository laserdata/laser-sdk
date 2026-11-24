package laser_test

import (
	"context"
	"errors"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func idempotencyKey(t *testing.T, value string) wire.IdempotencyKey {
	t.Helper()
	key, err := wire.NewIdempotencyKey(value)
	if err != nil {
		t.Fatalf("idempotency key: %v", err)
	}
	return key
}

// warmDedupRuntime opens a runtime with WarmDedup on against group, its dedup
// window capped to capacity so a test can prove the depth bound without
// seeding thousands of records.
func warmDedupRuntime(
	t *testing.T, server *runtimeServer, group string, capacity int, policy laser.WarmDedupPolicy,
) *laser.AgentRuntime {
	t.Helper()
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	agent := handle.Agent(agentID(t, "executor"), laser.AgentOptions{
		Dedup: laser.NewDeduplicator(capacity),
	})
	runtime, err := handle.AgentRuntime(agent, laser.RuntimeOptions{
		Topic: wire.AgentSessions, Group: group,
		WarmDedup: true, WarmDedupPolicy: policy,
	})
	if err != nil {
		t.Fatalf("open the runtime: %v", err)
	}
	return runtime
}

func TestGivenWarmDedupOnWithNoGroupWhenBuiltThenShouldRefuse(t *testing.T) {
	server := newRuntimeServer()
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	agent := handle.Agent(agentID(t, "executor"), laser.AgentOptions{})
	_, err := handle.AgentRuntime(agent, laser.RuntimeOptions{
		Topic: wire.AgentSessions, WarmDedup: true,
	})
	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want a refusal naming the missing group, got %v", err)
	}
}

func TestGivenWarmDedupOffWhenBuiltThenShouldNotRequireGroup(t *testing.T) {
	server := newRuntimeServer()
	// runtimeFor itself fails the test if AgentRuntime refuses this, which is
	// exactly what proves the zero-value WarmDedup needs no Group.
	runtimeFor(t, server, "executor", laser.RuntimeOptions{})
}

// Three records sit on the group's partition, but the group has only
// committed through the middle one: the last is unconsumed and must never be
// read, and the first is addressed to a different agent and must never be
// seeded even though it was read.
func TestGivenCommittedHistoryWhenWarmDedupRunsThenShouldSeedOnlyWhatIsCommittedAndAddressedHere(t *testing.T) {
	server := newRuntimeServer()
	server.partitions = 1

	kept := idempotencyKey(t, "kept")
	elsewhere := idempotencyKey(t, "elsewhere")
	tooNew := idempotencyKey(t, "too-new")

	addressedHere := delivered(t, 0, 0, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(1), []byte("work")).
			WithTarget(agentID(t, "executor")).WithIdempotencyKey(kept)
	})
	addressedElsewhere := delivered(t, 0, 1, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(2), []byte("work")).
			WithTarget(agentID(t, "someone-else")).WithIdempotencyKey(elsewhere)
	})
	unconsumed := delivered(t, 0, 2, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(3), []byte("work")).
			WithTarget(agentID(t, "executor")).WithIdempotencyKey(tooNew)
	})
	server.seedHistory(wire.AgentSessions, 0, 1,
		addressedHere.Message, addressedElsewhere.Message, unconsumed.Message)

	runtime := warmDedupRuntime(t, server, "executor-group", 10, laser.WarmDedupUnexpired)
	if err := laser.WarmUpDedupForTest(runtime, context.Background()); err != nil {
		t.Fatalf("warm up: %v", err)
	}

	dedup := laser.RuntimeAgentForTest(runtime).Dedup()
	if dedup.Accept(laser.DedupKey(addressedHere.Provenance)) {
		t.Error("want the committed, addressed-here record already seeded")
	}
	if !dedup.Accept(laser.DedupKey(addressedElsewhere.Provenance)) {
		t.Error("want a record addressed to another agent left out")
	}
	if !dedup.Accept(laser.DedupKey(unconsumed.Provenance)) {
		t.Error("want the unconsumed record, past the stored offset, left out")
	}
}

// The dedup window's own capacity bounds how much history is read per
// partition: with room for two keys, only the two most recent committed
// records refill it.
func TestGivenMoreHistoryThanTheWindowsCapacityWhenWarmDedupRunsThenShouldKeepOnlyTheMostRecent(t *testing.T) {
	server := newRuntimeServer()
	server.partitions = 1

	oldest := idempotencyKey(t, "oldest")
	middle := idempotencyKey(t, "middle")
	newest := idempotencyKey(t, "newest")

	first := delivered(t, 0, 0, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(1), []byte("work")).
			WithTarget(agentID(t, "executor")).WithIdempotencyKey(oldest)
	})
	second := delivered(t, 0, 1, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(2), []byte("work")).
			WithTarget(agentID(t, "executor")).WithIdempotencyKey(middle)
	})
	third := delivered(t, 0, 2, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(3), []byte("work")).
			WithTarget(agentID(t, "executor")).WithIdempotencyKey(newest)
	})
	server.seedHistory(wire.AgentSessions, 0, 2, first.Message, second.Message, third.Message)

	runtime := warmDedupRuntime(t, server, "executor-group", 2, laser.WarmDedupUnexpired)
	if err := laser.WarmUpDedupForTest(runtime, context.Background()); err != nil {
		t.Fatalf("warm up: %v", err)
	}

	// Checked in this order deliberately: Accept only mutates the window on a
	// key it has not already seen, so probing the two that should already be
	// there first reads them without side effects, leaving the oldest's probe
	// (which does insert, and at this capacity would evict one of them) for
	// last.
	dedup := laser.RuntimeAgentForTest(runtime).Dedup()
	if dedup.Accept(laser.DedupKey(second.Provenance)) {
		t.Error("want the middle record already seeded")
	}
	if dedup.Accept(laser.DedupKey(third.Provenance)) {
		t.Error("want the newest record already seeded")
	}
	if !dedup.Accept(laser.DedupKey(first.Provenance)) {
		t.Error("want the oldest record, past the window's capacity, left out")
	}
}

// A record whose deadline passed, but that no dead letter actually names as
// dropped, is not proof the copy was ever abandoned: DeadLetters still
// refills it, unlike Unexpired, which would leave it out on the deadline
// alone.
func TestGivenAPastDeadlineWithNoMatchingDeadLetterWhenWarmDedupRunsUnderDeadLettersThenShouldStillRefillIt(t *testing.T) {
	server := newRuntimeServer()
	server.partitions = 1
	past := time.Now().Add(-time.Hour)
	key := idempotencyKey(t, "expired-but-not-dead-lettered")

	record := delivered(t, 0, 0, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(1), []byte("work")).
			WithTarget(agentID(t, "executor")).WithIdempotencyKey(key).WithDeadline(past)
	})
	record.Message.Header.Timestamp = uint64(past.Add(-time.Minute).UnixMicro())
	server.seedHistory(wire.AgentSessions, 0, 0, record.Message)

	runtime := warmDedupRuntime(t, server, "executor-group", 10, laser.WarmDedupDeadLetters)
	if err := laser.WarmUpDedupForTest(runtime, context.Background()); err != nil {
		t.Fatalf("warm up: %v", err)
	}

	dedup := laser.RuntimeAgentForTest(runtime).Dedup()
	if dedup.Accept(laser.DedupKey(record.Provenance)) {
		t.Error("want DeadLetters to refill a record no dead letter names as dropped")
	}
}

// The same expired record, but this time a dead letter on the stream's
// agent.dlq topic names its exact source: DeadLetters reads that and leaves
// it out, same as Unexpired would from the deadline alone.
func TestGivenAPastDeadlineWithAMatchingDeadLetterWhenWarmDedupRunsUnderDeadLettersThenShouldLeaveItOut(t *testing.T) {
	server := newRuntimeServer()
	server.partitions = 1
	past := time.Now().Add(-time.Hour)
	key := idempotencyKey(t, "expired-and-dead-lettered")

	record := delivered(t, 0, 0, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(1), []byte("work")).
			WithTarget(agentID(t, "executor")).WithIdempotencyKey(key).WithDeadline(past)
	})
	record.Message.Header.Timestamp = uint64(past.Add(-time.Minute).UnixMicro())
	server.seedHistory(wire.AgentSessions, 0, 0, record.Message)

	// GetStream/GetTopic on this fake always answer stream id 7, topic id 3,
	// whatever is asked, which is what resolveSource will read for the main
	// topic too: the dead letter's source has to name the same pair to match.
	letter := wire.AgentDeadLetter{
		Source: wire.LogPosition{StreamID: 7, TopicID: 3, PartitionID: 0, Offset: 0},
		Reason: wire.DeadLetterDeadlineExceeded,
	}
	payload, err := wire.Marshal(letter)
	if err != nil {
		t.Fatalf("encode the dead letter: %v", err)
	}
	dlqMessage, err := iggcon.NewIggyMessage(payload)
	if err != nil {
		t.Fatalf("build the dead letter record: %v", err)
	}
	dlqMessage.Header.Offset = 0
	dlqMessage.Header.Timestamp = uint64(past.UnixMicro())
	server.seedHistory(wire.AgentDLQ, 0, 0, dlqMessage)

	runtime := warmDedupRuntime(t, server, "executor-group", 10, laser.WarmDedupDeadLetters)
	if err := laser.WarmUpDedupForTest(runtime, context.Background()); err != nil {
		t.Fatalf("warm up: %v", err)
	}

	dedup := laser.RuntimeAgentForTest(runtime).Dedup()
	if !dedup.Accept(laser.DedupKey(record.Provenance)) {
		t.Error("want DeadLetters to leave out a record its own dead letter names as dropped")
	}
}
