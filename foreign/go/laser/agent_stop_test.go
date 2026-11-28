package laser_test

import (
	"context"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenAStopDuringStartupWhenRunThenShouldEndCleanly(t *testing.T) {
	handle := laser.FromClient(newScope(), laser.Options{DefaultStream: "agents"})
	runtime, err := handle.AgentRuntime(
		handle.Agent(agentID(t, "executor"), laser.AgentOptions{}),
		laser.RuntimeOptions{Topic: wire.AgentSessions, EnforceControl: true})
	if err != nil {
		t.Fatalf("open the runtime: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := runtime.Run(ctx, sourceOf(), &handling{}); err != nil {
		t.Errorf("want a stop during startup to end the run cleanly, got %v", err)
	}
}
