package laser

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// noopGovernor never governs anything in these tests: only the wiring that
// picks it (or doesn't) is under test here, never its own Decide.
type noopGovernor struct{}

func (noopGovernor) Decide(context.Context, *GovernedAction) (*ActionDecision, error) {
	return &ActionDecision{}, nil
}

func TestGivenARunThatEndedCleanlyBeforePollingWhenReadyIsAwaitedThenShouldReportTheStopEveryTime(t *testing.T) {
	handle := &AgentHandle{cancel: func() {}, ready: make(chan struct{}), done: make(chan struct{})}
	close(handle.done)
	ctx := context.Background()

	for _, wait := range []struct {
		name string
		call func(context.Context) error
	}{{"ready", handle.Ready}, {"ready again", handle.Ready}, {"join", handle.Join}, {"shutdown", handle.Shutdown}} {
		err := wait.call(ctx)

		var config *ConfigError
		if !errors.As(err, &config) || config.Reason != "agent stopped before it became ready" {
			t.Errorf("%s: want the agent reported stopped before it became ready, got %v", wait.name, err)
		}
	}
}

func TestGivenARunThatEndedCleanlyBeforePollingWhenOnlyJoinedThenShouldReportTheCleanStop(t *testing.T) {
	handle := &AgentHandle{cancel: func() {}, ready: make(chan struct{}), done: make(chan struct{})}
	close(handle.done)

	if err := handle.Join(context.Background()); err != nil {
		t.Fatalf("want a join without a failed ready to report the run's own result, got %v", err)
	}
}

func TestGivenARunThatFailedBeforePollingWhenReadyIsAwaitedThenShouldReturnItsFailure(t *testing.T) {
	failure := &TransportError{Reason: "presence conflict"}
	handle := &AgentHandle{cancel: func() {}, ready: make(chan struct{}), done: make(chan struct{}), result: failure}
	close(handle.done)

	if err := handle.Ready(context.Background()); !errors.Is(err, failure) {
		t.Fatalf("want the failure the run stopped with, got %v", err)
	}
}

func TestGivenAnAgentThatWasPollingWhenItStoppedThenShouldStillReportReady(t *testing.T) {
	handle := &AgentHandle{cancel: func() {}, ready: make(chan struct{}), done: make(chan struct{}),
		result: &TransportError{Reason: "read failed later"}}
	close(handle.ready)
	close(handle.done)

	if err := handle.Ready(context.Background()); err != nil {
		t.Fatalf("want an agent that was polling reported ready, got %v", err)
	}
}

func TestGivenNoGovernorWhenSpawningThenShouldLeaveTheHandleUngoverned(t *testing.T) {
	laser := isolatedLaser()
	scoped := spawnGoverned(laser, SpawnOptions{})
	if scoped != laser {
		t.Fatal("expected the same handle back when no governor is set")
	}
	if _, governed := scoped.Governor(); governed {
		t.Fatal("expected no governor on an ungoverned spawn")
	}
}

func TestGivenAGovernorWhenSpawningThenShouldScopeTheHandleUnderIt(t *testing.T) {
	laser := isolatedLaser()
	scoped := spawnGoverned(laser, SpawnOptions{Governor: noopGovernor{}, GovernorMode: GovernorEnforce})
	if scoped == laser {
		t.Fatal("expected a distinct, governed handle")
	}
	mode, governed := scoped.Governor()
	if !governed || mode != GovernorEnforce {
		t.Fatalf("want governed under GovernorEnforce, got governed=%v mode=%v", governed, mode)
	}
	if _, governed := laser.Governor(); governed {
		t.Fatal("the original handle must stay ungoverned")
	}
}

func TestGivenNoRetentionWhenSpawningGovernedThenShouldDefaultIt(t *testing.T) {
	laser := isolatedLaser()
	scoped := spawnGoverned(laser, SpawnOptions{Governor: noopGovernor{}})
	if got, want := scoped.governor.retention, DefaultGovernorRetention(); got != want {
		t.Fatalf("want the default retention %+v, got %+v", want, got)
	}
}

func TestGivenAnExplicitRetentionWhenSpawningGovernedThenShouldUseIt(t *testing.T) {
	laser := isolatedLaser()
	retention := GovernorRetention{Capacity: 7, IdleTTL: time.Minute}
	scoped := spawnGoverned(laser, SpawnOptions{Governor: noopGovernor{}, GovernorRetention: &retention})
	if scoped.governor.retention != retention {
		t.Fatalf("want the caller's retention %+v, got %+v", retention, scoped.governor.retention)
	}
}

func TestGivenNoTopicWhenSpawningThenShouldReadTheSessionLane(t *testing.T) {
	laser := isolatedLaser()
	laser.defaultStream = "agents"

	topic := spawnTopic(laser, presenceAgentID(t, "worker"), RuntimeOptions{})

	if topic != wire.AgentSessions {
		t.Fatalf("want the session lane, got %q", topic)
	}
}

func TestGivenADeclaredAgentWhenSpawningOnTheLaneThenShouldReadItsDeclaredTopic(t *testing.T) {
	laser := isolatedLaser()
	laser.defaultStream = "agents"
	worker := presenceAgentID(t, "worker")
	laser.declareLayout("agents", PerAgentTopicLayout(map[wire.AgentID]string{worker: "worker.inbox"}))

	unnamed := spawnTopic(laser, worker, RuntimeOptions{})
	lane := spawnTopic(laser, worker, RuntimeOptions{Topic: wire.AgentSessions})
	undeclared := spawnTopic(laser, presenceAgentID(t, "auditor"), RuntimeOptions{})

	if unnamed != "worker.inbox" || lane != "worker.inbox" {
		t.Errorf("want the declared topic in place of the lane, got %q and %q", unnamed, lane)
	}
	if undeclared != wire.AgentSessions {
		t.Errorf("want an undeclared agent on the lane, got %q", undeclared)
	}
}

func TestGivenAnExplicitTopicWhenSpawningThenShouldKeepIt(t *testing.T) {
	laser := isolatedLaser()
	laser.defaultStream = "agents"
	worker := presenceAgentID(t, "worker")
	laser.declareLayout("agents", PerAgentTopicLayout(map[wire.AgentID]string{worker: "worker.inbox"}))

	topic := spawnTopic(laser, worker, RuntimeOptions{Topic: "reviews"})

	if topic != "reviews" {
		t.Fatalf("want the named topic kept, got %q", topic)
	}
}

func TestGivenADeclaredLayoutOnAnotherStreamWhenSpawningThereThenShouldReadThatStreamsTopic(t *testing.T) {
	laser := isolatedLaser()
	laser.defaultStream = "agents"
	worker := presenceAgentID(t, "worker")
	laser.declareLayout("billing", PerAgentTopicLayout(map[wire.AgentID]string{worker: "worker.inbox"}))

	here := spawnTopic(laser, worker, RuntimeOptions{})
	there := spawnTopic(laser, worker, RuntimeOptions{Stream: "billing"})

	if here != wire.AgentSessions || there != "worker.inbox" {
		t.Errorf("want the layout of the stream the agent runs on, got %q and %q", here, there)
	}
}
