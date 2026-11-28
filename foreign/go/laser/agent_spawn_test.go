package laser_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
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

func TestGivenAnIdleSourceWhenAwaitingReadyThenShouldWaitForStartup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := &blockedStartupServer{runtimeServer: newRuntimeServer(), resume: make(chan struct{})}
		runtime := runtimeFor(t, server, "worker", laser.RuntimeOptions{})
		source := sourceOf()
		handle := laser.StartAgentHandleForTest(agentID(t, "worker"), runtime, source, &handling{})
		defer func() { _ = handle.Shutdown(context.Background()) }()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		ready := make(chan error, 1)
		go func() { ready <- handle.Ready(ctx) }()

		synctest.Wait()
		select {
		case err := <-ready:
			t.Fatalf("Ready returned during source lookup: %v", err)
		default:
		}

		close(server.resume)
		if err := <-ready; err != nil {
			t.Fatalf("Ready on an idle source after startup: %v", err)
		}
		synctest.Wait()
		select {
		case <-source.drained:
		default:
			t.Fatal("the ready agent never polled its source")
		}
	})
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

func TestGivenARunningAgentWhenShutdownThenShouldCloseItsSourceBeforeReportingDone(t *testing.T) {
	runtime := lifecycleRuntime(t)
	source := &closingSource{queuedSource: sourceOf()}
	handle := laser.StartAgentHandleForTest(agentID(t, "worker"), runtime, source, &handling{})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := handle.Ready(ctx); err != nil {
		t.Fatalf("ready: %v", err)
	}

	if err := handle.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	if closed := source.closed.Load(); closed != 1 {
		t.Errorf("want the source closed once so its group membership is left, got %d", closed)
	}
}

func TestGivenARunningAgentWhenAbortedThenShouldCloseItsSource(t *testing.T) {
	runtime := lifecycleRuntime(t)
	source := &closingSource{queuedSource: sourceOf()}
	handle := laser.StartAgentHandleForTest(agentID(t, "worker"), runtime, source, &handling{})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := handle.Ready(ctx); err != nil {
		t.Fatalf("ready: %v", err)
	}

	handle.Abort()
	if err := handle.Join(ctx); err != nil {
		t.Fatalf("join: %v", err)
	}

	if closed := source.closed.Load(); closed != 1 {
		t.Errorf("want the source closed after an abort, got %d", closed)
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
	// A stop that was asked for is honoured only after readiness is reported.
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

// An agent whose first read fails was polling, so Ready reports it ready and
// Join reports the failure.
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
	if err := handle.Ready(ctx); err != nil {
		t.Fatalf("want an agent that reached its first read reported ready, got %v", err)
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

func TestGivenNoDefaultStreamWhenSpawnedThenShouldRefuseBeforeJoiningAnything(t *testing.T) {
	handle := laser.FromClient(newRuntimeServer(), laser.Options{})
	_, err := handle.SpawnAgent(context.Background(), agentID(t, "worker"), laser.SpawnOptions{
		RuntimeOptions: laser.RuntimeOptions{Topic: wire.AgentSessions},
	}, &handling{})
	if err == nil {
		t.Fatal("want a refusal for no default stream and none named")
	}
}

type blockedStartupServer struct {
	*runtimeServer
	resume chan struct{}
}

func (server *blockedStartupServer) GetStream(ctx context.Context, id iggcon.Identifier) (*iggcon.StreamDetails, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-server.resume:
		return server.runtimeServer.GetStream(ctx, id)
	}
}

// closingSource is a source that counts its closes.
type closingSource struct {
	*queuedSource
	closed atomic.Int32
}

func (source *closingSource) Close(context.Context) error {
	source.closed.Add(1)
	return nil
}
