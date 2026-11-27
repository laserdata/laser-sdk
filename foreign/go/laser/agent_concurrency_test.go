package laser_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// gatedHandler blocks on gate before reporting a record whose body matches
// held, and reports every other record at once, in the order its Handle is
// called. It is how these tests prove two partitions ran concurrently rather
// than one after another: a lane blocked on gate must not stop another lane's
// record from being seen first.
type gatedHandler struct {
	held string
	gate chan struct{}

	mutex sync.Mutex
	seen  []string
}

func (handler *gatedHandler) Handle(
	_ context.Context, message *laser.AgentMessage, _ *laser.AgentCtx,
) error {
	body := string(message.Body())
	if body == handler.held {
		<-handler.gate
	}
	handler.mutex.Lock()
	handler.seen = append(handler.seen, body)
	handler.mutex.Unlock()
	return nil
}

func (handler *gatedHandler) snapshot() []string {
	handler.mutex.Lock()
	defer handler.mutex.Unlock()
	return append([]string(nil), handler.seen...)
}

func perPartitionRuntime(t *testing.T, server *runtimeServer, maxPartitions int) *laser.AgentRuntime {
	t.Helper()
	return runtimeFor(t, server, "executor", laser.RuntimeOptions{
		Concurrency: laser.SerialPerPartitionConcurrency(maxPartitions),
	})
}

func targetedRecord(t *testing.T, partition uint32, offset uint64, body string) *laser.Record {
	t.Helper()
	return delivered(t, partition, offset, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(byte(offset)+1), []byte(body)).
			WithTarget(agentID(t, "executor"))
	})
}

func TestGivenConcurrencyIsUnsetWhenRunThenShouldStillBeSerial(t *testing.T) {
	server := newRuntimeServer()
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{})
	record := targetedRecord(t, 0, 0, "do it")
	source := sourceOf(record)
	handler := &handling{}

	if err := runUntilDrained(t, runtime, source, handler); err != nil {
		t.Fatalf("run: %v", err)
	}
	if seen := handler.seen(); len(seen) != 1 || seen[0] != "do it" {
		t.Errorf("want the record handled under the zero-value policy, got %v", seen)
	}
}

// Two records on two different partitions: the second must be seen before
// the first is released, which only holds if the two partitions' lanes ran
// concurrently rather than the second waiting behind the first.
func TestGivenTwoPartitionsWhenRunUnderSerialPerPartitionThenShouldHandleThemConcurrently(t *testing.T) {
	server := newRuntimeServer()
	runtime := perPartitionRuntime(t, server, 2)
	first := targetedRecord(t, 0, 0, "first")
	second := targetedRecord(t, 1, 0, "second")
	source := sourceOf(first, second)
	handler := &gatedHandler{held: "first", gate: make(chan struct{})}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx, source, handler) }()

	deadline := time.After(2 * time.Second)
	for {
		seen := handler.snapshot()
		if len(seen) >= 1 {
			if seen[0] != "second" {
				t.Fatalf("want \"second\" handled while \"first\" is still gated, got %v", seen)
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("\"second\" was never handled while \"first\" was gated")
		case <-time.After(5 * time.Millisecond):
		}
	}

	close(handler.gate)
	select {
	case <-source.drained:
	case <-time.After(2 * time.Second):
		t.Fatal("the runtime never read both records")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
	if seen := handler.snapshot(); len(seen) != 2 || seen[1] != "first" {
		t.Errorf("want both records handled, \"first\" last, got %v", seen)
	}
}

// The same two partitions, but capped at one concurrent lane: now "second"
// must wait for "first" to be released, because there is no second lane to
// run it on until the first one retires.
func TestGivenMaxPartitionsOneWhenRunThenShouldSerializeAcrossPartitionsToo(t *testing.T) {
	server := newRuntimeServer()
	runtime := perPartitionRuntime(t, server, 1)
	first := targetedRecord(t, 0, 0, "first")
	second := targetedRecord(t, 1, 0, "second")
	source := sourceOf(first, second)
	handler := &gatedHandler{held: "first", gate: make(chan struct{})}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx, source, handler) }()

	// "second" must not appear while "first" is still gated: give the runtime
	// a fair chance to (wrongly) handle it concurrently before asserting it
	// did not.
	time.Sleep(200 * time.Millisecond)
	if seen := handler.snapshot(); len(seen) != 0 {
		t.Fatalf("want nothing handled while the one lane is gated on \"first\", got %v", seen)
	}

	close(handler.gate)
	select {
	case <-source.drained:
	case <-time.After(2 * time.Second):
		t.Fatal("the runtime never read both records")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
	if seen := handler.snapshot(); len(seen) != 2 || seen[0] != "first" || seen[1] != "second" {
		t.Errorf("want \"first\" then \"second\", got %v", seen)
	}
}

// Within one partition, SerialPerPartition must still process records
// strictly in the order they were queued: concurrency is across partitions
// only.
func TestGivenOnePartitionWithManyRecordsWhenRunUnderSerialPerPartitionThenShouldKeepItsOwnOrder(t *testing.T) {
	server := newRuntimeServer()
	runtime := perPartitionRuntime(t, server, 4)
	records := []*laser.Record{
		targetedRecord(t, 0, 0, "a"),
		targetedRecord(t, 0, 1, "b"),
		targetedRecord(t, 0, 2, "c"),
	}
	source := sourceOf(records...)
	handler := &handling{}

	if err := runUntilDrained(t, runtime, source, handler); err != nil {
		t.Fatalf("run: %v", err)
	}
	if seen := handler.seen(); len(seen) != 3 || seen[0] != "a" || seen[1] != "b" || seen[2] != "c" {
		t.Errorf("want a, b, c in order, got %v", seen)
	}
	if commits := source.commits(); len(commits) != 3 || commits[0] != 0 || commits[1] != 1 || commits[2] != 2 {
		t.Errorf("want commits in order 0, 1, 2, got %v", commits)
	}
}

// A handler failure whose dead letter cannot be written must stop the whole
// run, the same contract Serial gives for runtime.deliver's error (see
// TestGivenAnUnwritableDeadLetterWhenRunThenShouldLeaveTheRecordUncommitted):
// an ordinary refused record is kept via a dead letter and the run carries
// on, so only a dead letter that itself cannot be published makes deliver
// return a fatal error.
func TestGivenAFatalHandlerFailureOnOneLaneWhenRunThenShouldStopTheWholeRun(t *testing.T) {
	server := newRuntimeServer()
	server.refuse, server.refuseAlways = wire.AgentDLQ, true
	runtime := perPartitionRuntime(t, server, 4)
	poison := targetedRecord(t, 0, 0, "poison")
	source := sourceOf(poison)
	handler := &handling{fail: &laser.InvalidError{Reason: "refused"}}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx, source, handler) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a record whose dead letter could not be kept must end the run")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the run never stopped after its one lane's dead letter failed to publish")
	}
}

// A kept record (one the handler fails on without ending the run, i.e. the
// gate around a retried dispatch is exercised elsewhere) still lets Shutdown
// finish cleanly through an AgentHandle built over a per-partition runtime.
func TestGivenAPerPartitionAgentWhenShutdownThenShouldStopCleanly(t *testing.T) {
	server := newRuntimeServer()
	runtime := perPartitionRuntime(t, server, 2)
	source := sourceOf()
	handle := laser.StartAgentHandleForTest(agentID(t, "executor"), runtime, source, &handling{})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := handle.Ready(ctx); err != nil {
		t.Fatalf("ready: %v", err)
	}
	if err := handle.Shutdown(ctx); err != nil {
		t.Fatalf("want a clean shutdown, got %v", err)
	}
}

func TestGivenMaxPartitionsBelowOneWhenBuiltThenShouldClampToOne(t *testing.T) {
	server := newRuntimeServer()
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{
		Concurrency: laser.SerialPerPartitionConcurrency(0),
	})
	// Two partitions, capped (after clamping) at one lane: proven the same
	// way TestGivenMaxPartitionsOneWhenRunThenShouldSerializeAcrossPartitionsToo
	// proves an explicit 1, since 0 must behave identically.
	first := targetedRecord(t, 0, 0, "first")
	second := targetedRecord(t, 1, 0, "second")
	source := sourceOf(first, second)
	handler := &gatedHandler{held: "first", gate: make(chan struct{})}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx, source, handler) }()

	time.Sleep(200 * time.Millisecond)
	if seen := handler.snapshot(); len(seen) != 0 {
		t.Fatalf("want 0 clamped to 1 lane, nothing handled yet, got %v", seen)
	}
	close(handler.gate)
	select {
	case <-source.drained:
	case <-time.After(2 * time.Second):
		t.Fatal("the runtime never read both records")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
}
