package laser_test

import (
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// An operator sees the role stop even when no work arrives for the session,
// because a pause that names the role is acknowledged on its own schedule.
func TestGivenAPauseNamingTheRoleAndNoWorkWhenReconciledThenShouldAcknowledgeIt(t *testing.T) {
	server := pausedServer(t,
		controlRequest(t, wire.OperationSessionPause, `{"participants":["executor"]}`, 0))
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{EnforceControl: true})

	acknowledged := func() bool { return len(acknowledgementsOn(t, server)) > 0 }
	if err := runSource(t, runtime, sourceOf(), &handling{}, acknowledged); err != nil {
		t.Fatalf("run: %v", err)
	}
	if states := acknowledgementsOn(t, server); len(states) != 1 || states[0] != wire.TaskPaused {
		t.Fatalf("want one paused acknowledgment, got %v", states)
	}
}

// A pause that does not name the role is not this role's to acknowledge until
// it takes part, so two reconcile passes write nothing.
func TestGivenAPauseNotNamingTheRoleAndNoWorkWhenReconciledThenShouldStaySilent(t *testing.T) {
	server := pausedServer(t,
		controlRequest(t, wire.OperationSessionPause, `{"participants":["reviewer"]}`, 0))
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{EnforceControl: true})

	started := time.Now()
	twoPasses := func() bool { return time.Since(started) > 1200*time.Millisecond }
	if err := runSource(t, runtime, sourceOf(), &handling{}, twoPasses); err != nil {
		t.Fatalf("run: %v", err)
	}
	if states := acknowledgementsOn(t, server); len(states) != 0 {
		t.Fatalf("want no acknowledgment from a role the pause does not name, got %v", states)
	}
}

// A cancel that reaches a paused session ends it canceled as this role, once,
// and its held work stays unprocessed.
func TestGivenAPausedSessionWhenCanceledThenShouldEndItCanceledAndKeepItsWorkHeld(t *testing.T) {
	server := pausedServer(t,
		controlRequest(t, wire.OperationSessionPause, `{"participants":["executor"]}`, 0),
		controlRequest(t, wire.OperationSessionCancel, `{}`, 1))
	held := workRecord(t, 41, "held work")
	server.seedHistory(wire.AgentSessions, 0, 0, held.Message)
	server.aliasNumeric(3, wire.AgentSessions)
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{EnforceControl: true})
	handler := &handling{}
	source := sourceOf(held)

	started := time.Time{}
	settled := func() bool {
		if !drainedOf(source)() || canceledOn(t, server) == 0 {
			return false
		}
		if started.IsZero() {
			started = time.Now()
		}
		// Two more passes, so a second ending would have been written by now.
		return time.Since(started) > 1200*time.Millisecond
	}
	if err := runSource(t, runtime, source, handler, settled); err != nil {
		t.Fatalf("run: %v", err)
	}
	if canceled := canceledOn(t, server); canceled != 1 {
		t.Fatalf("want the session ended canceled once, got %d", canceled)
	}
	if seen := handler.seen(); len(seen) != 0 {
		t.Fatalf("want a canceled session's held work left unprocessed, got %v", seen)
	}
	if parked := parkedOn(t, server, wire.OperationSessionParked); len(parked) != 1 {
		t.Fatalf("want the record still parked, got %d", len(parked))
	}
}

// A control history longer than the startup read cannot prove that nothing
// was paused, so the book says there may be held work to look for.
func TestGivenAControlHistoryLongerThanTheStartupReadWhenFollowedThenShouldNotClaimNoPauseHistory(t *testing.T) {
	server := pausedServer(t, controlRecord(t, wire.OperationSessionPause, 0))
	filler := make([]iggcon.IggyMessage, laser.MaxDrainRecords)
	for index := range filler {
		filler[index] = workRecord(t, uint64(index)+1, "not a control request").Message
	}
	server.seedHistory(wire.AgentControl, 0, 0, filler...)
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{EnforceControl: true})

	live := func() bool { return runtime.Control().IsLive() }
	if err := runSource(t, runtime, sourceOf(), &handling{}, live); err != nil {
		t.Fatalf("run: %v", err)
	}
	if pending, known := runtime.Control().Pending(conversation(1)); !known || pending.PauseRequested {
		t.Fatalf("want the pause outside the read window not folded, got %+v, %v", pending, known)
	}
	if !runtime.Control().HasPauseHistory() {
		t.Fatal("want a truncated read to report that a pause may exist")
	}
}

// A recreated agent.control starts every session's control history again, so
// a pause read from the old topic stops holding the session's work.
func TestGivenARecreatedControlTopicWhenFollowedThenShouldStartTheBookAgain(t *testing.T) {
	laser.SetControlTopicCheckForTest(t, 50*time.Millisecond)
	server := pausedServer(t, controlRecord(t, wire.OperationSessionPause, 0))
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{EnforceControl: true})
	source := sourceOf(workRecord(t, 41, "after the recreation"))
	source.beforeNext = func(index int) {
		if index != 0 {
			return
		}
		server.recreate(wire.AgentControl)
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if pending, known := runtime.Control().Pending(conversation(1)); known && !pending.PauseRequested {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Error("the follower never noticed the recreated control topic")
	}
	handler := &handling{}

	if err := runUntilDrained(t, runtime, source, handler); err != nil {
		t.Fatalf("run: %v", err)
	}
	if seen := handler.seen(); len(seen) != 1 {
		t.Fatalf("want the work handled once the old pause is gone, got %v", seen)
	}
}

// controlRequest builds one operator request of the session under test with
// the body given, ready to seed onto agent.control.
func controlRequest(t *testing.T, operation, body string, offset uint64) iggcon.IggyMessage {
	t.Helper()
	record := delivered(t, 0, offset, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(9), []byte(body)).
			WithOperation(operation).
			ContentType(wire.ContentJSON)
	})
	return record.Message
}

// canceledOn counts the canceled session endings a role wrote on the lane.
func canceledOn(t *testing.T, server *runtimeServer) int {
	t.Helper()
	canceled := 0
	for _, message := range server.on(wire.AgentSessions) {
		var envelope wire.AgentEnvelope
		if err := wire.Unmarshal(message.Payload, &envelope); err != nil {
			continue
		}
		if envelope.Kind == wire.KindStatus && envelope.TaskState != nil &&
			*envelope.TaskState == wire.TaskCanceled {
			canceled++
		}
	}
	return canceled
}
