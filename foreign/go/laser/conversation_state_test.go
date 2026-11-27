package laser_test

import (
	"context"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// Rebuilding state by folding the log is event sourcing: the fold belongs to
// the caller, and the replay's job is to hand over the history in order.
func TestGivenALoggedConversationWhenReplayedThenShouldFoldItInOrder(t *testing.T) {
	server := &contractServer{}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	conversation := conversation(4)
	for _, body := range []string{"one", "two", "three"} {
		if _, err := handle.AgdxOn("agents", wire.AgentSessions,
			agentID(t, "planner"), conversation).
			Emit([]byte(body)).
			Send(context.Background()); err != nil {
			t.Fatalf("append %q: %v", body, err)
		}
	}

	joined, err := laser.LoadConversationState(
		context.Background(), handle, conversation,
		[]string{wire.AgentSessions}, laser.FullReplay(), "",
		func(state string, message laser.ContextMessage) string {
			if state != "" {
				state += "|"
			}
			return state + string(message.Body())
		})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if joined != "one|two|three" {
		t.Errorf("want the history folded in order, got %q", joined)
	}
}

// A bounded replay folds only the newest messages, for a caller rebuilding
// recent state rather than all of it.
func TestGivenALastNBoundWhenReplayedThenShouldFoldOnlyTheNewest(t *testing.T) {
	server := &contractServer{}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	conversation := conversation(5)
	for _, body := range []string{"one", "two", "three"} {
		if _, err := handle.AgdxOn("agents", wire.AgentSessions,
			agentID(t, "planner"), conversation).
			Emit([]byte(body)).
			Send(context.Background()); err != nil {
			t.Fatalf("append %q: %v", body, err)
		}
	}

	count, err := laser.LoadConversationState(
		context.Background(), handle, conversation,
		[]string{wire.AgentSessions}, laser.LastReplay(2), 0,
		func(state int, _ laser.ContextMessage) int { return state + 1 })
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if count != 2 {
		t.Errorf("want the two newest messages folded, got %d", count)
	}
}

// Reproducing a moment means stopping where the checkpoint stopped, however
// much arrived afterwards.
func TestGivenACheckpointWhenReplayedUpToItThenShouldIgnoreWhatCameAfter(t *testing.T) {
	server := &contractServer{}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	conversation := conversation(6)
	appendOne := func(body string) {
		t.Helper()
		if _, err := handle.AgdxOn("agents", wire.AgentSessions,
			agentID(t, "planner"), conversation).
			Emit([]byte(body)).
			Send(context.Background()); err != nil {
			t.Fatalf("append %q: %v", body, err)
		}
	}
	appendOne("before")
	checkpoint, err := handle.Context(conversation).
		Checkpoint(context.Background(), []string{wire.AgentSessions})
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	appendOne("after")

	upTo, err := laser.LoadConversationState(
		context.Background(), handle, conversation,
		[]string{wire.AgentSessions}, laser.ReplayUntil(checkpoint), "",
		func(state string, message laser.ContextMessage) string {
			return state + string(message.Body())
		})
	if err != nil {
		t.Fatalf("replay up to the checkpoint: %v", err)
	}
	if upTo != "before" {
		t.Errorf("want only what the checkpoint had seen, got %q", upTo)
	}

	// The complement: resuming after it sees only what arrived since.
	since, err := laser.LoadConversationState(
		context.Background(), handle, conversation,
		[]string{wire.AgentSessions}, laser.ReplayAfter(checkpoint), "",
		func(state string, message laser.ContextMessage) string {
			return state + string(message.Body())
		})
	if err != nil {
		t.Fatalf("replay after the checkpoint: %v", err)
	}
	if since != "after" {
		t.Errorf("want only what arrived since, got %q", since)
	}
}

// A bound that needs a checkpoint and was given none is a caller mistake,
// not a silent full replay.
func TestGivenACheckpointBoundWithNoCheckpointWhenReplayedThenShouldRefuse(t *testing.T) {
	server := &contractServer{}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	for _, bound := range []laser.ReplayBound{
		laser.ReplayAfter(nil), laser.ReplayUntil(nil),
	} {
		_, err := laser.LoadConversationState(
			context.Background(), handle, conversation(7),
			[]string{wire.AgentSessions}, bound, 0,
			func(state int, _ laser.ContextMessage) int { return state })
		if err == nil {
			t.Errorf("want a checkpoint bound with no checkpoint refused, kind %v", bound.Kind)
		}
	}
}
