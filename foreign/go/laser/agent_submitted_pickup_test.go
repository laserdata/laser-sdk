package laser_test

import (
	"context"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// pickupRuntime opens a runtime the same way runtimeFor does, but also hands
// back the Laser handle it was built from, which a pickup test needs to
// inspect Leases().Held() against the exact lease registry the runtime
// itself would acquire into.
func pickupRuntime(t *testing.T, server *runtimeServer) (*laser.Laser, *laser.AgentRuntime) {
	t.Helper()
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	agent := handle.Agent(agentID(t, "executor"), laser.AgentOptions{})
	runtime, err := handle.AgentRuntime(agent, laser.RuntimeOptions{Topic: wire.AgentSessions})
	if err != nil {
		t.Fatalf("open the runtime: %v", err)
	}
	return handle, runtime
}

func submittedCommand(t *testing.T, offset uint64) *laser.Record {
	t.Helper()
	return delivered(t, 0, offset, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(byte(offset)+1), []byte("work")).
			WithTarget(agentID(t, "executor")).
			WithMetadata(wire.MetadataSubmitted, wire.BoolValue(true))
	})
}

// A submitted session's first command marks the session working, on
// agent.sessions, and holds a lease for exactly the handler's own run.
func TestGivenASubmittedCommandWhenHandledThenShouldMarkTheSessionWorkingAndTakeALease(t *testing.T) {
	server := newRuntimeServer()
	handle, runtime := pickupRuntime(t, server)
	record := submittedCommand(t, 0)
	source := sourceOf(record)

	heldDuringHandle := -1
	handler := laser.HandlerFunc(func(
		_ context.Context, _ *laser.AgentMessage, _ *laser.AgentCtx,
	) error {
		heldDuringHandle = handle.Leases().Held()
		return nil
	})

	if err := runUntilDrained(t, runtime, source, handler); err != nil {
		t.Fatalf("run: %v", err)
	}
	if heldDuringHandle != 1 {
		t.Errorf("want one lease held while the handler ran, got %d", heldDuringHandle)
	}
	if held := handle.Leases().Held(); held != 0 {
		t.Errorf("want the lease released once the handler returned, got %d still held", held)
	}
	if sessions := server.on(wire.AgentSessions); len(sessions) != 1 {
		t.Errorf("want one status record marking the session working, got %d", len(sessions))
	}
}

// An ordinary command, with no submitted metadata, never marks a session
// working or takes a lease: pickup is specific to the one path that needs it.
func TestGivenAnOrdinaryCommandWhenHandledThenShouldNotMarkAnySessionOrTakeALease(t *testing.T) {
	server := newRuntimeServer()
	handle, runtime := pickupRuntime(t, server)
	record := delivered(t, 0, 0, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(1), []byte("work")).
			WithTarget(agentID(t, "executor"))
	})
	source := sourceOf(record)

	heldDuringHandle := -1
	handler := laser.HandlerFunc(func(
		_ context.Context, _ *laser.AgentMessage, _ *laser.AgentCtx,
	) error {
		heldDuringHandle = handle.Leases().Held()
		return nil
	})

	if err := runUntilDrained(t, runtime, source, handler); err != nil {
		t.Fatalf("run: %v", err)
	}
	if heldDuringHandle != 0 {
		t.Errorf("want no lease held for an unsubmitted command, got %d", heldDuringHandle)
	}
	if sessions := server.on(wire.AgentSessions); len(sessions) != 0 {
		t.Errorf("want no status record for an unsubmitted command, got %d", len(sessions))
	}
}

// A pickup that cannot publish its own status is not fatal to the record: the
// command survived every gate, so the handler still runs and the record still
// commits.
func TestGivenAPickupThatCannotPublishWhenHandledThenShouldStillHandleTheRecord(t *testing.T) {
	server := newRuntimeServer()
	server.refuse, server.refuseAlways = wire.AgentSessions, true
	handle, runtime := pickupRuntime(t, server)
	record := submittedCommand(t, 0)
	source := sourceOf(record)

	heldDuringHandle := -1
	handled := false
	handler := laser.HandlerFunc(func(
		_ context.Context, _ *laser.AgentMessage, _ *laser.AgentCtx,
	) error {
		heldDuringHandle = handle.Leases().Held()
		handled = true
		return nil
	})

	if err := runUntilDrained(t, runtime, source, handler); err != nil {
		t.Fatalf("want a refused pickup status to not fail the run, got %v", err)
	}
	if !handled {
		t.Fatal("want the handler to run despite the refused pickup status")
	}
	if heldDuringHandle != 0 {
		t.Errorf("want no lease taken when the pickup status could not publish, got %d", heldDuringHandle)
	}
	if commits := source.commits(); len(commits) != 1 {
		t.Errorf("want the record committed despite the refused pickup status, got %v", commits)
	}
}
