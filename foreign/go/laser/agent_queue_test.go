package laser_test

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

func TestGivenABackedUpPartitionWhenAnotherHasWorkThenShouldNotHoldItBack(t *testing.T) {
	server := newRuntimeServer()
	runtime := perPartitionRuntime(t, server, 2)
	source := sourceOf(
		targetedRecord(t, 0, 0, "slow"), targetedRecord(t, 0, 1, "queued"),
		targetedRecord(t, 0, 2, "queued again"), targetedRecord(t, 1, 3, "other partition"),
	)
	handler := &gatedHandler{held: "slow", gate: make(chan struct{})}
	stop, done := runInBackground(runtime, source, handler)

	waitUntilSeen(t, handler, "other partition")
	if seen := handler.snapshot(); slices.Contains(seen, "queued") {
		t.Errorf("want the slow partition kept in order, got %v", seen)
	}
	close(handler.gate)
	waitUntilSeen(t, handler, "queued again")
	stop()
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestGivenARecordBoundOfOneWhenALaneIsBusyThenShouldStopReadingUntilItFinishes(t *testing.T) {
	server := newRuntimeServer()
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{
		Concurrency: laser.SerialPerPartitionConcurrency(2), MaxQueuedRecords: 1,
	})
	source := sourceOf(
		targetedRecord(t, 0, 0, "slow"), targetedRecord(t, 1, 1, "other partition"),
		targetedRecord(t, 1, 2, "last"),
	)
	reads := make(chan int, 8)
	source.beforeNext = func(index int) { reads <- index }
	handler := &gatedHandler{held: "slow", gate: make(chan struct{})}
	stop, done := runInBackground(runtime, source, handler)

	// The second record is read and then waits for room, so the third is never asked for.
	for range 2 {
		select {
		case <-reads:
		case <-time.After(5 * time.Second):
			t.Fatal("the runtime did not read up to the bound")
		}
	}
	select {
	case index := <-reads:
		t.Fatalf("want reading stopped at the bound, got record %d read", index)
	case <-time.After(50 * time.Millisecond):
	}
	if seen := handler.snapshot(); len(seen) != 0 {
		t.Errorf("want nothing handled past the bound, got %v", seen)
	}
	close(handler.gate)
	waitUntilSeen(t, handler, "last")
	stop()
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestGivenARecordLargerThanTheByteBoundWhenRunThenShouldStillHandleIt(t *testing.T) {
	server := newRuntimeServer()
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{
		Concurrency: laser.SerialPerPartitionConcurrency(2), MaxQueuedBytes: 8,
	})
	large := strings.Repeat("x", 4096)
	source := sourceOf(targetedRecord(t, 0, 0, large), targetedRecord(t, 1, 1, "small"))
	handler := &gatedHandler{}

	if err := runUntilDrained(t, runtime, source, handler); err != nil {
		t.Fatalf("run: %v", err)
	}

	seen := handler.snapshot()
	if len(seen) != 2 || !slices.Contains(seen, large) || !slices.Contains(seen, "small") {
		t.Errorf("want both records handled, got %d", len(seen))
	}
	if commits := source.commits(); len(commits) != 2 {
		t.Errorf("want both records committed, got %v", commits)
	}
}

// runInBackground runs a runtime until stop is called and reports how it ended.
func runInBackground(
	runtime *laser.AgentRuntime, source *queuedSource, handler laser.AgentHandler,
) (stop context.CancelFunc, done chan error) {
	ctx, cancel := context.WithCancel(context.Background())
	done = make(chan error, 1)
	go func() { done <- runtime.Run(ctx, source, handler) }()
	return cancel, done
}

// waitUntilSeen waits for a handler to report one body.
func waitUntilSeen(t *testing.T, handler *gatedHandler, body string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !slices.Contains(handler.snapshot(), body) {
		if time.Now().After(deadline) {
			t.Fatalf("the handler never saw %q, got %v", body, handler.snapshot())
		}
		time.Sleep(time.Millisecond)
	}
}
