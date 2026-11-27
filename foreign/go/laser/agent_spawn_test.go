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

func lifecycleRuntime(t *testing.T) *laser.AgentRuntime {
	t.Helper()
	return runtimeFor(t, newRuntimeServer(), "worker", laser.RuntimeOptions{})
}

func TestGivenAHandleWhenTheLoopStartsThenShouldReportReady(t *testing.T) {
	runtime := lifecycleRuntime(t)
	source := sourceOf()
	handle := laser.StartAgentHandleForTest(agentID(t, "worker"), runtime, source, &handling{})
	defer func() { _ = handle.Shutdown(context.Background()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := handle.Ready(ctx); err != nil {
		t.Fatalf("ready: %v", err)
	}
	if handle.Identity().String() != "worker" {
		t.Errorf("want the spawned identity, got %q", handle.Identity())
	}
}

func TestGivenARunningAgentWhenShutdownThenShouldStopAndJoinCleanly(t *testing.T) {
	runtime := lifecycleRuntime(t)
	source := sourceOf()
	handle := laser.StartAgentHandleForTest(agentID(t, "worker"), runtime, source, &handling{})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := handle.Ready(ctx); err != nil {
		t.Fatalf("ready: %v", err)
	}
	if err := handle.Shutdown(ctx); err != nil {
		t.Fatalf("want a clean shutdown, got %v", err)
	}
}

func TestGivenAStoppedAgentWhenReadyIsCalledAgainThenShouldReportTheSameOutcome(t *testing.T) {
	runtime := lifecycleRuntime(t)
	source := sourceOf()
	handle := laser.StartAgentHandleForTest(agentID(t, "worker"), runtime, source, &handling{})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := handle.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	// Ready is idempotent: a caller that calls it after the agent already
	// stopped sees the same outcome Join would, not a hang or a panic.
	if err := handle.Ready(ctx); err != nil {
		t.Fatalf("want ready to report the clean stop, got %v", err)
	}
}

func TestGivenAHandlerThatKeepsItsRecordWhenShutdownThenShouldStillStopCleanly(t *testing.T) {
	server := newRuntimeServer()
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{})
	record := delivered(t, 0, 7, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(1), []byte("do it")).
			WithTarget(agentID(t, "executor"))
	})
	source := sourceOf(record)
	refusing := &handling{fail: &laser.InvalidError{Reason: "refused"}}
	handle := laser.StartAgentHandleForTest(agentID(t, "executor"), runtime, source, refusing)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for refusing.tries() == 0 {
		select {
		case <-ctx.Done():
			t.Fatal("the handler was never invoked")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if err := handle.Shutdown(ctx); err != nil {
		t.Fatalf("a kept record still stops the run cleanly, got %v", err)
	}
}

// failingSource reports err from its first Next call, as a consumer group
// that could not be joined, or whose very first poll failed outright, would.
type failingSource struct{ err error }

func (source *failingSource) Next(context.Context) (*laser.Record, error) {
	return nil, source.err
}

func (source *failingSource) Commit(context.Context, iggcon.Checkpoint) error {
	return nil
}

// A source whose very first read fails races Ready against the goroutine
// actually reaching that read: Ready may see only that the loop started, not
// that it then failed. Join has no such race, since it only ever reads the
// run's final result.
//
// The failure must be one RetryableError reports false for: a plain error
// retries inside the consumer's own failure budget rather than ending the
// run, which is the right thing for a transient read and the wrong shape for
// this test.
func TestGivenASourceThatFailsAtOnceWhenJoinedThenShouldReportItsError(t *testing.T) {
	runtime := lifecycleRuntime(t)
	source := &failingSource{err: &laser.InvalidError{Reason: "the group could not be joined"}}
	handle := laser.StartAgentHandleForTest(agentID(t, "worker"), runtime, source, &handling{})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var invalid *laser.InvalidError
	if err := handle.Join(ctx); !errors.As(err, &invalid) {
		t.Fatalf("want Join to report the run's own error, got %v", err)
	}
	// Ready is safe to call after the agent already stopped: it reports the
	// same outcome rather than hanging on a loop that will never start.
	if err := handle.Ready(ctx); !errors.As(err, &invalid) {
		t.Fatalf("want Ready to report the same error once it is settled, got %v", err)
	}
}

func TestGivenAnAbortWhenCalledThenShouldNotBlockTheCaller(t *testing.T) {
	runtime := lifecycleRuntime(t)
	source := sourceOf()
	handle := laser.StartAgentHandleForTest(agentID(t, "worker"), runtime, source, &handling{})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := handle.Ready(ctx); err != nil {
		t.Fatalf("ready: %v", err)
	}
	done := make(chan struct{})
	go func() {
		handle.Abort()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Abort must return without waiting for the agent to stop")
	}
	if err := handle.Join(ctx); err != nil {
		t.Fatalf("want a clean stop after abort, got %v", err)
	}
}

func TestGivenNoIdentityWhenSpawnedThenShouldRefuseBeforeJoiningAnything(t *testing.T) {
	handle := laser.FromClient(newRuntimeServer(), laser.Options{DefaultStream: "agents"})
	_, err := handle.SpawnAgent(context.Background(), wire.AgentID{}, laser.SpawnOptions{
		RuntimeOptions: laser.RuntimeOptions{Topic: wire.AgentSessions},
	}, &handling{})
	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an invalid refusal for a zero identity, got %v", err)
	}
}

func TestGivenNoHandlerWhenSpawnedThenShouldRefuseBeforeJoiningAnything(t *testing.T) {
	handle := laser.FromClient(newRuntimeServer(), laser.Options{DefaultStream: "agents"})
	_, err := handle.SpawnAgent(context.Background(), agentID(t, "worker"), laser.SpawnOptions{
		RuntimeOptions: laser.RuntimeOptions{Topic: wire.AgentSessions},
	}, nil)
	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an invalid refusal for no handler, got %v", err)
	}
}

func TestGivenNoTopicWhenSpawnedThenShouldRefuseBeforeJoiningAnything(t *testing.T) {
	handle := laser.FromClient(newRuntimeServer(), laser.Options{DefaultStream: "agents"})
	_, err := handle.SpawnAgent(
		context.Background(), agentID(t, "worker"), laser.SpawnOptions{}, &handling{})
	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an invalid refusal for no topic, got %v", err)
	}
}

func TestGivenNoDefaultStreamWhenSpawnedThenShouldRefuseBeforeJoiningAnything(t *testing.T) {
	handle := laser.FromClient(newRuntimeServer(), laser.Options{})
	_, err := handle.SpawnAgent(context.Background(), agentID(t, "worker"), laser.SpawnOptions{
		RuntimeOptions: laser.RuntimeOptions{Topic: wire.AgentSessions},
	}, &handling{})
	if err == nil {
		t.Fatal("want a refusal for no default stream and none named")
	}
}
