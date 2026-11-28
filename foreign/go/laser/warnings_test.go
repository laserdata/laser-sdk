package laser_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenACompensationThatFailsToBuildWhenAWorkflowRollsBackThenShouldWarnAndGoOn(t *testing.T) {
	warnings := captureWarnings(t)
	handle := laser.FromClient(newWorkflowServer(), laser.Options{DefaultStream: "agents"})
	undone := false

	_, err := handle.Workflow(agentID(t, "billing"), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")).
		StepLocal("reserve", func(context.Context, map[string][]byte) ([]byte, error) {
			return []byte("reserved"), nil
		}).CompensateWith(func(context.Context, map[string][]byte) ([]byte, error) {
		undone = true
		return nil, nil
	}).
		StepLocal("charge", func(context.Context, map[string][]byte) ([]byte, error) {
			return []byte("charged"), nil
		}).After("reserve").CompensateWith(func(context.Context, map[string][]byte) ([]byte, error) {
		return nil, errors.New("refund refused")
	}).
		StepLocal("ship", func(context.Context, map[string][]byte) ([]byte, error) {
			return nil, errors.New("ship failed")
		}).After("charge").
		Run(context.Background())

	if err == nil || !strings.Contains(err.Error(), "ship failed") {
		t.Fatalf("want the ship step's own error, got %v", err)
	}
	if !undone {
		t.Error("want the rollback to go on to the earlier step")
	}
	want := "Building the compensation of workflow step charge failed"
	if got := warnings.messages(); len(got) != 1 || got[0] != want {
		t.Fatalf("want one warning %q, got %v", want, got)
	}
}

func TestGivenACancelledRunWhenAStepFailsThenShouldStopWithoutCompensating(t *testing.T) {
	warnings := captureWarnings(t)
	handle := laser.FromClient(newWorkflowServer(), laser.Options{DefaultStream: "agents"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	undone := false

	_, err := handle.Workflow(agentID(t, "billing"), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")).
		StepLocal("reserve", func(context.Context, map[string][]byte) ([]byte, error) {
			return []byte("reserved"), nil
		}).CompensateWith(func(context.Context, map[string][]byte) ([]byte, error) {
		undone = true
		return nil, nil
	}).
		StepLocal("charge", func(step context.Context, _ map[string][]byte) ([]byte, error) {
			cancel()
			<-step.Done()
			return nil, step.Err()
		}).After("reserve").
		Run(ctx)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want the cancellation reported, got %v", err)
	}
	if undone {
		t.Error("want a cancelled run to stop without compensating")
	}
	if got := warnings.messages(); len(got) != 0 {
		t.Errorf("want no compensation warning from a cancelled run, got %v", got)
	}
}

func TestGivenAMemoryWriteThatFailsWhenAHandlerSucceedsThenShouldWarnAndKeepTheTurnHandled(t *testing.T) {
	warnings := captureWarnings(t)
	server := newRuntimeServer()
	server.refuse, server.refuseAlways = wire.AgentMemory, true
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	handled := 0
	handler := laser.WithMemory(laser.HandlerFunc(func(
		context.Context, *laser.AgentMessage, *laser.AgentCtx,
	) error {
		handled++
		return nil
	}), handle.Memory("agent-mem")).AutoRemember(laser.MemoryMessage)
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{})

	if err := runUntilDrained(t, runtime, sourceOf(workRecord(t, 41, "a turn")), handler); err != nil {
		t.Fatalf("want a failed memory write not to fail the run, got %v", err)
	}

	if handled != 1 {
		t.Errorf("want the turn handled once and not delivered again, got %d", handled)
	}
	got := warnings.messages()
	if len(got) != 1 || !strings.HasPrefix(got[0], "Memory handler could not remember the turn of conversation ") {
		t.Fatalf("want one warning about the turn not remembered, got %v", got)
	}
}

// warningLog collects what the client logs at warn level.
type warningLog struct {
	mutex  sync.Mutex
	logged []string
}

// captureWarnings swaps the default logger for the length of the test.
func captureWarnings(t *testing.T) *warningLog {
	t.Helper()
	collected := &warningLog{}
	previous := slog.Default()
	slog.SetDefault(slog.New(collected))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return collected
}

func (log *warningLog) messages() []string {
	log.mutex.Lock()
	defer log.mutex.Unlock()
	return append([]string(nil), log.logged...)
}

func (log *warningLog) Enabled(_ context.Context, level slog.Level) bool {
	return level >= slog.LevelWarn
}

func (log *warningLog) Handle(_ context.Context, record slog.Record) error {
	log.mutex.Lock()
	defer log.mutex.Unlock()
	log.logged = append(log.logged, record.Message)
	return nil
}

func (log *warningLog) WithAttrs([]slog.Attr) slog.Handler { return log }

func (log *warningLog) WithGroup(string) slog.Handler { return log }
