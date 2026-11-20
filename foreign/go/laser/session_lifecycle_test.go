package laser_test

import (
	"context"
	"errors"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// endedSession opens a labeled session whose lane already holds a start and
// the terminal state given, which is the shape Begin has to read before it
// decides whether it may open the session again.
func endedSession(
	t *testing.T, server *stateServer, ended wire.TaskState,
) *laser.Session {
	t.Helper()
	sessions := submitSessions(t, server)
	session := sessions.Create("nightly").As(agentID(t, "planner"))
	server.session = session.Conversation()
	lifecycleRecord(t, server, session.Conversation(), wire.TaskWorking,
		&wire.SessionStart{Agent: agentID(t, "planner")})
	lifecycleRecord(t, server, session.Conversation(), ended, nil)
	return session
}

func TestGivenAnEndedSessionWhenBegunAgainThenShouldRefuseIt(t *testing.T) {
	server := &stateServer{}
	session := endedSession(t, server, wire.TaskCompleted)
	before := server.written()

	_, err := session.Begin(context.Background())
	var ended *laser.SessionEndedError
	if !errors.As(err, &ended) {
		t.Fatalf("want a SessionEndedError, got %T: %v", err, err)
	}
	if ended.Status != wire.SessionCompleted {
		t.Fatalf("ended as %q, want %q", ended.Status, wire.SessionCompleted)
	}
	// A refused begin must write nothing, or the lane holds a second start
	// under an id a reader has already seen close.
	if server.written() != before {
		t.Fatalf("a refused begin wrote %d records", server.written()-before)
	}
}

func TestGivenAnEndedSessionWhenResumedThenShouldBeginAgain(t *testing.T) {
	server := &stateServer{}
	session := endedSession(t, server, wire.TaskCompleted)
	before := server.written()

	if _, err := session.Resume().Begin(context.Background()); err != nil {
		t.Fatalf("a completed session resumes on request: %v", err)
	}
	if server.written() != before+1 {
		t.Fatalf("a resumed begin wrote %d records, want one",
			server.written()-before)
	}
}

func TestGivenACanceledSessionWhenResumedThenShouldStillRefuseIt(t *testing.T) {
	server := &stateServer{}
	session := endedSession(t, server, wire.TaskCanceled)

	_, err := session.Resume().Begin(context.Background())
	var ended *laser.SessionEndedError
	if !errors.As(err, &ended) {
		t.Fatalf("want a SessionEndedError, got %T: %v", err, err)
	}
	// Cancellation is a decision somebody made about the work, so resuming
	// would quietly overturn it. This is the one state resume does not open.
	if ended.Status != wire.SessionCanceled {
		t.Fatalf("ended as %q, want %q", ended.Status, wire.SessionCanceled)
	}
}

func TestGivenAFreshSessionWhenBegunThenShouldNotReadTheLane(t *testing.T) {
	server := &stateServer{}
	sessions := submitSessions(t, server)
	session := sessions.Start().As(agentID(t, "planner"))

	if _, err := session.Begin(context.Background()); err != nil {
		t.Fatalf("begin: %v", err)
	}
	// A conversation minted here cannot have ended, so paying for a lane read
	// on every one-shot session would be waste.
	if reads := server.reads(); reads != 0 {
		t.Fatalf("a fresh begin read the lane %d times, want none", reads)
	}
}

func TestGivenAnEndedSessionWhenEndedAgainTheSameWayThenShouldRepeatTheRecord(t *testing.T) {
	server := &stateServer{}
	sessions := submitSessions(t, server)
	session := sessions.Start().As(agentID(t, "planner"))

	first, err := session.Complete(context.Background(), "done")
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	second, err := session.Complete(context.Background(), "done again")
	if err != nil {
		t.Fatalf("ending a session twice the same way is not a failure: %v", err)
	}
	// The same record id, so a reader reconciles one ending rather than two.
	// The reason of the first call wins, because that is the ending the
	// session actually settled on.
	if first.Record != second.Record {
		t.Fatalf("a repeated end minted a new record %v, want %v",
			second.Record, first.Record)
	}
}

func TestGivenAnEndedSessionWhenEndedADifferentWayThenShouldRefuseIt(t *testing.T) {
	server := &stateServer{}
	sessions := submitSessions(t, server)
	session := sessions.Start().As(agentID(t, "planner"))

	if _, err := session.Complete(context.Background(), "done"); err != nil {
		t.Fatalf("complete: %v", err)
	}
	written := server.written()

	_, err := session.End(context.Background(), wire.TaskCanceled, wire.SessionEnd{})
	if err == nil {
		t.Fatal("a session that completed did not then cancel")
	}
	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("reported %T, want *laser.InvalidError", err)
	}
	if server.written() != written {
		t.Fatal("a refused end must write nothing to the lane")
	}
}

func TestGivenADerivedHandleWhenEndingADifferentWayThenShouldRefuseIt(t *testing.T) {
	server := &stateServer{}
	sessions := submitSessions(t, server)
	session := sessions.Start().As(agentID(t, "planner"))

	if _, err := session.Complete(context.Background(), "done"); err != nil {
		t.Fatalf("complete: %v", err)
	}
	// The latch is shared with every handle derived from this one, or a
	// caller could contradict an ending simply by taking another handle.
	derived := session.As(agentID(t, "planner"))
	if _, err := derived.End(
		context.Background(), wire.TaskFailed, wire.SessionEnd{}); err == nil {
		t.Fatal("a derived handle must see the same latched ending")
	}
}

func TestGivenChangedStateWhenCompletingThenShouldSnapshotFirst(t *testing.T) {
	server := &stateServer{}
	sessions := submitSessions(t, server)
	session := sessions.Start().As(agentID(t, "planner"))
	server.session = session.Conversation()

	if _, err := session.State().Set(
		context.Background(), "step", text("collect")); err != nil {
		t.Fatalf("set: %v", err)
	}
	before := server.written()
	if _, err := session.Complete(context.Background(), "done"); err != nil {
		t.Fatalf("complete: %v", err)
	}
	// A snapshot and then the terminal record, so a reader of a completed
	// session starts from the final document rather than folding the deltas.
	if wrote := server.written() - before; wrote != 2 {
		t.Fatalf("completing wrote %d records, want a snapshot and an ending", wrote)
	}
}

func TestGivenUnchangedStateWhenCompletingThenShouldWriteOnlyTheEnding(t *testing.T) {
	server := &stateServer{}
	sessions := submitSessions(t, server)
	session := sessions.Start().As(agentID(t, "planner"))
	server.session = session.Conversation()

	before := server.written()
	if _, err := session.Complete(context.Background(), "done"); err != nil {
		t.Fatalf("complete: %v", err)
	}
	// Nothing changed, so a snapshot would checkpoint an empty document and
	// cost a record for nothing.
	if wrote := server.written() - before; wrote != 1 {
		t.Fatalf("completing wrote %d records, want only the ending", wrote)
	}
}

func TestGivenChangedStateWhenFailingThenShouldNotSnapshot(t *testing.T) {
	server := &stateServer{}
	sessions := submitSessions(t, server)
	session := sessions.Start().As(agentID(t, "planner"))
	server.session = session.Conversation()

	if _, err := session.State().Set(
		context.Background(), "step", text("collect")); err != nil {
		t.Fatalf("set: %v", err)
	}
	before := server.written()
	message := "it broke"
	if _, err := session.Fail(context.Background(), wire.AgentErrorBody{
		Code: wire.ErrorInvalidRequest, Message: &message}); err != nil {
		t.Fatalf("fail: %v", err)
	}
	// A session that failed stopped partway, so its last consistent document
	// is whatever it managed to write. Checkpointing here would present a
	// half-finished document as a settled one.
	if wrote := server.written() - before; wrote != 1 {
		t.Fatalf("failing wrote %d records, want only the ending", wrote)
	}
}

func TestGivenAStaleHandleWhenCompletingThenShouldEndWithoutOverwriting(t *testing.T) {
	server := &stateServer{}
	session := stateSession(t, server)
	if _, err := session.State().Set(
		context.Background(), "mine", text("a")); err != nil {
		t.Fatalf("set: %v", err)
	}
	// Another writer moved the session on, so this handle's document is no
	// longer the session's.
	rivalDelta(t, server, 1, "01ARZ3NDEKTSV4RRFFQ69G5FAV", nil,
		wire.AddOperation("/theirs", text("b")))
	before := server.written()

	if _, err := session.Complete(context.Background(), "done"); err != nil {
		t.Fatalf("a stale document must not stop the session ending: %v", err)
	}
	// Only the ending. Checkpointing here would write this handle's document
	// whole and silently undo the other writer's change, and ending the
	// session is what the caller actually asked for.
	if wrote := server.written() - before; wrote != 1 {
		t.Fatalf("completing wrote %d records, want only the ending", wrote)
	}
	view, err := session.State().Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if value, _ := field(t, view, "theirs"); value != "b" {
		t.Fatalf("the other writer's change was lost, got %q", value)
	}
}
