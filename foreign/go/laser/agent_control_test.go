package laser_test

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func controlSession(t *testing.T, text string) wire.ConversationID {
	t.Helper()
	id, err := wire.ParseID(text)
	if err != nil {
		t.Fatalf("conversation %q: %v", text, err)
	}
	return wire.ConversationID{ID: id}
}

// controlEnvelope is a control record as it rides agent.control.
func controlEnvelope(
	t *testing.T, kind wire.AgentKind, operation string,
	target *wire.AgentID, body []byte,
) *wire.AgentEnvelope {
	t.Helper()
	envelope := &wire.AgentEnvelope{Kind: kind, Target: target, Body: body}
	if operation != "" {
		envelope.Operation = &operation
	}
	return envelope
}

func pauseBody(t *testing.T, participants ...string) []byte {
	t.Helper()
	request := wire.SessionPauseRequest{}
	for _, name := range participants {
		request.Participants = append(request.Participants, agentID(t, name))
	}
	encoded, err := wire.Marshal(request)
	if err != nil {
		t.Fatalf("encode the pause: %v", err)
	}
	return encoded
}

func at(partition uint32, offset uint64) laser.ControlPosition {
	return laser.ControlPosition{Partition: partition, Offset: offset}
}

func request(t *testing.T, operation string) laser.ControlRequest {
	t.Helper()
	parsed, made := laser.ControlRequestOf(
		controlEnvelope(t, wire.KindCommand, operation, nil, nil), nil)
	if !made {
		t.Fatalf("%s is a control request", operation)
	}
	return parsed
}

func TestGivenAControlEnvelopeWhenReadThenShouldNameItsRequest(t *testing.T) {
	canceled := wire.TaskCanceled
	running := wire.TaskWorking
	for _, probe := range []struct {
		name     string
		envelope *wire.AgentEnvelope
		action   string
		made     bool
	}{
		{"a pause command",
			controlEnvelope(t, wire.KindCommand, wire.OperationSessionPause, nil, nil),
			"pause", true},
		{"a resume command",
			controlEnvelope(t, wire.KindCommand, wire.OperationSessionResume, nil, nil),
			"resume", true},
		{"a cancel command",
			controlEnvelope(t, wire.KindCommand, wire.OperationSessionCancel, nil, nil),
			"cancel", true},
		{"a forced cancel",
			&wire.AgentEnvelope{Kind: wire.KindStatus, TaskState: &canceled},
			"cancel", true},
		{"an operation this client does not name",
			controlEnvelope(t, wire.KindCommand, "session_reheat", nil, nil), "", false},
		{"a command with no operation",
			controlEnvelope(t, wire.KindCommand, "", nil, nil), "", false},
		{"a status that carries no state",
			&wire.AgentEnvelope{Kind: wire.KindStatus}, "", false},
		{"a status that is not a cancel",
			&wire.AgentEnvelope{Kind: wire.KindStatus, TaskState: &running}, "", false},
		{"an ordinary event",
			controlEnvelope(t, wire.KindEvent, wire.OperationSessionPause, nil, nil),
			"", false},
	} {
		t.Run(probe.name, func(t *testing.T) {
			parsed, made := laser.ControlRequestOf(probe.envelope, nil)
			if made != probe.made {
				t.Fatalf("got %v, want %v", made, probe.made)
			}
			if made && parsed.Action() != probe.action {
				t.Fatalf("got %q, want %q", parsed.Action(), probe.action)
			}
		})
	}
	if _, made := laser.ControlRequestOf(nil, nil); made {
		t.Fatal("no envelope is no request")
	}
}

func TestGivenARequestForAnotherAgentWhenReadThenShouldNotBeMine(t *testing.T) {
	other := agentID(t, "executor")
	me := agentID(t, "planner")
	envelope := controlEnvelope(
		t, wire.KindCommand, wire.OperationSessionCancel, &other, nil)

	if _, made := laser.ControlRequestOf(envelope, &me); made {
		t.Fatal("a request addressed to another agent is not mine to read")
	}
	// A reader that is nobody in particular, such as an operator console,
	// reads every request.
	if _, made := laser.ControlRequestOf(envelope, nil); !made {
		t.Fatal("an unnamed reader reads every request")
	}
	// Addressed to me is mine, and names me.
	parsed, made := laser.ControlRequestOf(
		controlEnvelope(t, wire.KindCommand, wire.OperationSessionCancel, &me, nil), &me)
	if !made || !parsed.Named {
		t.Fatalf("got %+v %v", parsed, made)
	}
}

func TestGivenAPauseWhenReadThenShouldNameOnlyItsParticipants(t *testing.T) {
	me := agentID(t, "planner")
	listed := controlEnvelope(t, wire.KindCommand, wire.OperationSessionPause,
		nil, pauseBody(t, "executor", "planner"))
	parsed, made := laser.ControlRequestOf(listed, &me)
	if !made || !parsed.Named {
		t.Fatalf("a listed participant is named, got %+v %v", parsed, made)
	}

	// A pause applies to every agent it reaches. The participant list only
	// decides whose acknowledgment it waits for, so an agent it does not
	// list still pauses.
	unlisted := controlEnvelope(t, wire.KindCommand, wire.OperationSessionPause,
		nil, pauseBody(t, "executor"))
	parsed, made = laser.ControlRequestOf(unlisted, &me)
	if !made || parsed.Named {
		t.Fatalf("an unlisted agent still pauses but is not named, got %+v %v",
			parsed, made)
	}
	// A body that is not a pause request names nobody rather than failing:
	// the pause still applies.
	parsed, made = laser.ControlRequestOf(controlEnvelope(
		t, wire.KindCommand, wire.OperationSessionPause, nil, []byte{0xff}), &me)
	if !made || parsed.Named {
		t.Fatalf("got %+v %v", parsed, made)
	}
}

func TestGivenControlRecordsWhenFoldedThenShouldReportWhatIsInForce(t *testing.T) {
	session := controlSession(t, "01BX5ZZKBKACTAV9WEVGEMMVRZ")
	route := uint32(0)
	folded, last := laser.FoldControl([]laser.ControlRecord{
		{Conversation: session, At: at(0, 1), Request: request(t, wire.OperationSessionPause)},
		{Conversation: session, At: at(0, 2), Request: request(t, wire.OperationSessionResume)},
	}, &route)
	if folded.PauseRequested || folded.CancelRequested || folded.ControlConflict {
		t.Fatalf("a resume lifts its pause, got %+v", folded)
	}
	if last == nil || last.Offset != 2 {
		t.Fatalf("got %+v", last)
	}

	folded, _ = laser.FoldControl([]laser.ControlRecord{
		{Conversation: session, At: at(0, 1), Request: request(t, wire.OperationSessionResume)},
		{Conversation: session, At: at(0, 2), Request: request(t, wire.OperationSessionPause)},
	}, &route)
	if !folded.PauseRequested {
		t.Fatalf("a pause after a resume is in force, got %+v", folded)
	}

	// A cancel is terminal: nothing lifts it.
	folded, _ = laser.FoldControl([]laser.ControlRecord{
		{Conversation: session, At: at(0, 1), Request: request(t, wire.OperationSessionCancel)},
		{Conversation: session, At: at(0, 2), Request: request(t, wire.OperationSessionResume)},
	}, &route)
	if !folded.CancelRequested {
		t.Fatalf("a cancel is not lifted, got %+v", folded)
	}
}

func TestGivenAStaleRecordWhenFoldedThenShouldNotApplyIt(t *testing.T) {
	session := controlSession(t, "01BX5ZZKBKACTAV9WEVGEMMVRZ")
	route := uint32(0)
	// Offsets order records within one partition, so a replayed earlier
	// record must not undo a later one.
	folded, last := laser.FoldControl([]laser.ControlRecord{
		{Conversation: session, At: at(0, 5), Request: request(t, wire.OperationSessionPause)},
		{Conversation: session, At: at(0, 2), Request: request(t, wire.OperationSessionResume)},
	}, &route)
	if !folded.PauseRequested {
		t.Fatalf("an earlier record must not undo a later one, got %+v", folded)
	}
	if last == nil || last.Offset != 5 {
		t.Fatalf("got %+v", last)
	}
}

func TestGivenARecordOffTheControlRouteWhenFoldedThenShouldFlagIt(t *testing.T) {
	session := controlSession(t, "01BX5ZZKBKACTAV9WEVGEMMVRZ")
	route := uint32(0)
	folded, last := laser.FoldControl([]laser.ControlRecord{
		{Conversation: session, At: at(1, 1), Request: request(t, wire.OperationSessionCancel)},
	}, &route)
	// Applying it would order a record against records it shares no
	// partition with, so it is flagged and never applied.
	if folded.CancelRequested {
		t.Fatalf("a record off the route must not apply, got %+v", folded)
	}
	if !folded.ControlConflict {
		t.Fatalf("a record off the route must be flagged, got %+v", folded)
	}
	if last != nil {
		t.Fatalf("got %+v", last)
	}
	// Without a known route every partition is on it.
	folded, _ = laser.FoldControl([]laser.ControlRecord{
		{Conversation: session, At: at(1, 1), Request: request(t, wire.OperationSessionCancel)},
	}, nil)
	if !folded.CancelRequested || folded.ControlConflict {
		t.Fatalf("got %+v", folded)
	}
}

func TestGivenAConversationWhenRoutedThenShouldMatchTheServersPartition(t *testing.T) {
	session := controlSession(t, "01BX5ZZKBKACTAV9WEVGEMMVRZ")
	route, known := laser.ControlPartition(session, 8)
	if !known || route >= 8 {
		t.Fatalf("got %d %v", route, known)
	}
	// The same rule the managed backend applies: the partition the
	// conversation key hashes to.
	want := wire.XXHash32([]byte(session.String()), 0) % 8
	if route != want {
		t.Fatalf("got %d, want %d", route, want)
	}
	if _, known := laser.ControlPartition(session, 0); known {
		t.Fatal("an unknown partition count routes nothing")
	}
}

func TestGivenABookThatNobodyFollowsWhenReadThenShouldSayItCannotAnswer(t *testing.T) {
	session := controlSession(t, "01BX5ZZKBKACTAV9WEVGEMMVRZ")
	book := laser.NewControlBook()
	book.Load([]laser.ControlRecord{
		{Conversation: session, At: at(0, 1), Request: request(t, wire.OperationSessionPause)},
	})

	// Without a follower the book is a snapshot of an unknown age, so it
	// does not answer at all rather than answering stale.
	if _, known := book.Pending(session); known {
		t.Fatal("a book nobody follows cannot answer")
	}
	book.SetLive(true)
	flags, known := book.Pending(session)
	if !known || !flags.PauseRequested {
		t.Fatalf("got %+v %v", flags, known)
	}
}

func TestGivenALoadedBookWhenASessionIsUnknownThenShouldReportNoRequests(t *testing.T) {
	book := laser.NewControlBook()
	book.SetLive(true)
	unknown := controlSession(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")

	// Before a load, a session with no entry has a history nobody read.
	if _, known := book.Pending(unknown); known {
		t.Fatal("an unloaded book cannot answer for a session it never saw")
	}
	book.Load(nil)
	flags, known := book.Pending(unknown)
	if !known || flags != (laser.PendingControl{}) {
		t.Fatalf("got %+v %v", flags, known)
	}
}

func TestGivenALiveRequestBeforeTheFoldWhenInstalledThenShouldReplayIt(t *testing.T) {
	session := controlSession(t, "01BX5ZZKBKACTAV9WEVGEMMVRZ")
	book := laser.NewControlBook()
	book.SetLive(true)
	// The follower saw a resume while the session's history was still being
	// read. Applying it to nothing would report a resume as though no pause
	// preceded it.
	book.Observe(laser.ControlRecord{
		Conversation: session, At: at(0, 9),
		Request: request(t, wire.OperationSessionResume)})
	if _, known := book.Pending(session); known {
		t.Fatal("a session whose history is unread cannot be answered")
	}

	route := uint32(0)
	flags := book.Install(session, []laser.ControlRecord{
		{Conversation: session, At: at(0, 1), Request: request(t, wire.OperationSessionPause)},
	}, &route)
	if flags.PauseRequested {
		t.Fatalf("the live resume must replay on top of the fold, got %+v", flags)
	}
	folded, known := book.Pending(session)
	if !known || folded.PauseRequested {
		t.Fatalf("got %+v %v", folded, known)
	}
}

func TestGivenAFollowedSessionWhenInstalledThenShouldKeepWhatTheFollowerHas(t *testing.T) {
	session := controlSession(t, "01BX5ZZKBKACTAV9WEVGEMMVRZ")
	book := laser.NewControlBook()
	book.Load(nil)
	book.SetLive(true)
	book.Observe(laser.ControlRecord{
		Conversation: session, At: at(0, 9),
		Request: request(t, wire.OperationSessionPause)})

	// A fold read from the log is older than what a live follower has seen,
	// so installing it must not roll the session back.
	route := uint32(0)
	flags := book.Install(session, []laser.ControlRecord{
		{Conversation: session, At: at(0, 1), Request: request(t, wire.OperationSessionResume)},
	}, &route)
	if !flags.PauseRequested {
		t.Fatalf("the follower's state must stand, got %+v", flags)
	}
}

func TestGivenARequestOffTheRouteWhenObservedThenShouldFlagAndNeverApply(t *testing.T) {
	session := controlSession(t, "01BX5ZZKBKACTAV9WEVGEMMVRZ")
	route, known := laser.ControlPartition(session, 2)
	if !known {
		t.Fatal("two partitions route somewhere")
	}
	book := laser.NewControlBook()
	book.SetPartitions(2)
	book.Load([]laser.ControlRecord{
		{Conversation: session, At: at(route, 1),
			Request: request(t, wire.OperationSessionPause)},
		{Conversation: session, At: at(1-route, 2),
			Request: request(t, wire.OperationSessionCancel)},
	})
	book.SetLive(true)
	book.Observe(laser.ControlRecord{
		Conversation: session, At: at(1-route, 3),
		Request: request(t, wire.OperationSessionResume)})

	flags, found := book.Pending(session)
	if !found {
		t.Fatal("the session is folded")
	}
	if !flags.PauseRequested {
		t.Fatalf("the routed pause applies, got %+v", flags)
	}
	if flags.CancelRequested {
		t.Fatalf("a cancel off the route must not apply, got %+v", flags)
	}
	if !flags.ControlConflict {
		t.Fatalf("a request off the route must be flagged, got %+v", flags)
	}
}

func TestGivenPausedSessionsWhenAskedThenShouldListThem(t *testing.T) {
	first := controlSession(t, "01BX5ZZKBKACTAV9WEVGEMMVRZ")
	second := controlSession(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	third := controlSession(t, "01ARZ3NDEKTSV4RRFFQ69G5FAW")
	book := laser.NewControlBook()
	if book.HasPauseHistory() {
		t.Fatal("an empty book has no pause history")
	}
	book.Load([]laser.ControlRecord{
		{Conversation: first, At: at(0, 1), Request: request(t, wire.OperationSessionPause)},
		{Conversation: second, At: at(0, 2), Request: request(t, wire.OperationSessionResume)},
		{Conversation: third, At: at(0, 3), Request: request(t, wire.OperationSessionCancel)},
	})

	if !book.HasPauseHistory() {
		t.Fatal("a paused session is pause history")
	}
	// A cancel is not held work. A book holding only cancelled sessions has
	// nothing paused to go looking for.
	cancelled := laser.NewControlBook()
	cancelled.Load([]laser.ControlRecord{
		{Conversation: third, At: at(0, 1),
			Request: request(t, wire.OperationSessionCancel)},
	})
	if cancelled.HasPauseHistory() {
		t.Fatal("a cancelled session is not pause history")
	}
	// A resumed session stays listed: its held work still has to be looked
	// for. A cancel alone is not pause history.
	listed := map[wire.ConversationID]bool{}
	for _, session := range book.PausedOrResumed() {
		listed[session] = true
	}
	if !listed[first] || !listed[second] || listed[third] || len(listed) != 2 {
		t.Fatalf("got %v", book.PausedOrResumed())
	}
}

func TestGivenARecreatedControlTopicWhenResetThenShouldForgetEverything(t *testing.T) {
	session := controlSession(t, "01BX5ZZKBKACTAV9WEVGEMMVRZ")
	book := laser.NewControlBook()
	book.SetLive(true)
	book.Load([]laser.ControlRecord{
		{Conversation: session, At: at(0, 1), Request: request(t, wire.OperationSessionPause)},
	})
	if flags, known := book.Pending(session); !known || !flags.PauseRequested {
		t.Fatalf("got %+v %v", flags, known)
	}

	// A recreated topic's history starts again, so the old offsets name
	// nothing and the old state is not about this log.
	book.Reset()
	if _, known := book.Pending(session); known {
		t.Fatal("a reset book has read nothing")
	}
	if book.HasPauseHistory() {
		t.Fatal("a reset book holds no sessions")
	}
	if !book.IsLive() {
		t.Fatal("a reset does not stop the follower")
	}
}

func TestGivenACancelCarryingAPauseBodyWhenReadThenShouldNotNameAParticipant(t *testing.T) {
	// The participant list belongs to a pause. A cancel that happens to
	// carry a body shaped like one names nobody: only a pause waits for
	// acknowledgments.
	me := agentID(t, "planner")
	parsed, made := laser.ControlRequestOf(controlEnvelope(
		t, wire.KindCommand, wire.OperationSessionCancel, nil,
		pauseBody(t, "planner")), &me)
	if !made {
		t.Fatal("a cancel is a control request")
	}
	if parsed.Named {
		t.Fatalf("only a pause names its participants, got %+v", parsed)
	}
}

func TestGivenRecordsOnTwoPartitionsWhenFoldedThenShouldTakeTheLaterOneAsNewer(t *testing.T) {
	// With no known route every partition is on it, so a session's records
	// can span two. Offsets order records only within one partition, so a
	// record on another partition is taken as newer rather than compared
	// against an offset it shares no sequence with.
	session := controlSession(t, "01BX5ZZKBKACTAV9WEVGEMMVRZ")
	folded, last := laser.FoldControl([]laser.ControlRecord{
		{Conversation: session, At: at(0, 5),
			Request: request(t, wire.OperationSessionPause)},
		{Conversation: session, At: at(1, 2),
			Request: request(t, wire.OperationSessionResume)},
	}, nil)
	if folded.PauseRequested {
		t.Fatalf("the record on the other partition must apply, got %+v", folded)
	}
	if last == nil || last.Partition != 1 || last.Offset != 2 {
		t.Fatalf("got %+v", last)
	}
}
