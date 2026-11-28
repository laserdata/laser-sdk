package laser_test

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenFailOnDeadLetterWhenARecordIsDeadLetteredThenShouldFailItsSession(t *testing.T) {
	server := newRuntimeServer()
	sessions := laser.FromClient(server, laser.Options{DefaultStream: "agents"}).
		SessionsWith(laser.SessionConfig{FailOnDeadLetter: true})
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{Sessions: sessions})
	record := refusedWork(t)
	handler := &handling{fail: &laser.InvalidError{Reason: "the body names no order"}}

	if err := runUntilDrained(t, runtime, sourceOf(record), handler); err != nil {
		t.Fatalf("run: %v", err)
	}

	if kept := capsules(t, server); len(kept) != 1 {
		t.Fatalf("want the record dead-lettered, got %d capsules", len(kept))
	}
	ended := sessionEndings(t, server)
	if len(ended) != 1 {
		t.Fatalf("want the session failed once, got %d terminal records", len(ended))
	}
	failure := ended[0].Error
	if failure == nil || failure.Code != wire.ErrorInternal || failure.Retryable || failure.Message == nil {
		t.Fatalf("want a final internal failure, got %+v", failure)
	}
	want := "a record of this session was dead-lettered (Rejected): the body names no order"
	if *failure.Message != want {
		t.Errorf("want %q, got %q", want, *failure.Message)
	}
	if reason := failure.Detail[laser.DeadLetterReasonDetail]; wholeNumber(reason) != uint64(wire.DeadLetterRejected) {
		t.Errorf("want the dead-letter reason carried, got %+v", failure.Detail)
	}
	if source := failure.Detail["source"]; source.Kind != wire.ValueStr || source.Str == "" {
		t.Errorf("want the poison record's position carried, got %+v", failure.Detail)
	}
}

func TestGivenTheDefaultSessionConfigWhenARecordIsDeadLetteredThenShouldLeaveItsSessionOpen(t *testing.T) {
	server := newRuntimeServer()
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{})
	handler := &handling{fail: &laser.InvalidError{Reason: "the body names no order"}}

	if err := runUntilDrained(t, runtime, sourceOf(refusedWork(t)), handler); err != nil {
		t.Fatalf("run: %v", err)
	}

	if kept := capsules(t, server); len(kept) != 1 {
		t.Fatalf("want the record dead-lettered, got %d capsules", len(kept))
	}
	if ended := sessionEndings(t, server); len(ended) != 0 {
		t.Errorf("want a dead letter to end no session by default, got %d terminal records", len(ended))
	}
}

func refusedWork(t *testing.T) *laser.Record {
	t.Helper()
	return delivered(t, 0, 1, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(2), []byte("work")).WithTarget(agentID(t, "executor"))
	})
}

// sessionEndings reads back the failed session endings written on the lane.
func sessionEndings(t *testing.T, server *runtimeServer) []wire.SessionEnd {
	t.Helper()
	var endings []wire.SessionEnd
	for _, message := range server.on(wire.AgentSessions) {
		var envelope wire.AgentEnvelope
		if err := wire.Unmarshal(message.Payload, &envelope); err != nil {
			continue
		}
		if envelope.Kind != wire.KindStatus || envelope.Operation == nil ||
			*envelope.Operation != wire.OperationSession || envelope.TaskState == nil ||
			*envelope.TaskState != wire.TaskFailed {
			continue
		}
		var end wire.SessionEnd
		if err := wire.Unmarshal(envelope.Body, &end); err != nil {
			t.Fatalf("decode the ending: %v", err)
		}
		endings = append(endings, end)
	}
	return endings
}
