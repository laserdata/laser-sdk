package laser_test

import (
	"context"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// controlRecord builds one operator control request for the session every
// delivered() record belongs to, ready to seed onto agent.control.
func controlRecord(t *testing.T, operation string, offset uint64) iggcon.IggyMessage {
	t.Helper()
	record := delivered(t, 0, offset, func(agdx *laser.Agdx) *laser.AgdxSend {
		// A pause request's body is the JSON every client writes it as, so
		// an operator with nothing but a log viewer can read the topic.
		return agdx.Command(correlationID(9), []byte(`{"participants":[]}`)).
			WithOperation(operation).
			ContentType(wire.ContentJSON)
	})
	return record.Message
}

// workRecord builds one piece of work addressed to the agent under test.
func workRecord(t *testing.T, offset uint64, body string) *laser.Record {
	t.Helper()
	return delivered(t, 0, offset, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(2), []byte(body)).
			WithTarget(agentID(t, "executor"))
	})
}

// parkedOn reads back the parking records a runtime wrote to agent.sessions.
func parkedOn(t *testing.T, server *runtimeServer, operation string) []wire.SessionParking {
	t.Helper()
	var held []wire.SessionParking
	for _, message := range server.on(wire.AgentSessions) {
		var envelope wire.AgentEnvelope
		if err := wire.Unmarshal(message.Payload, &envelope); err != nil {
			continue
		}
		if envelope.Operation == nil || *envelope.Operation != operation {
			continue
		}
		var parking wire.SessionParking
		if err := wire.Unmarshal(envelope.Body, &parking); err != nil {
			t.Fatalf("decode the parking body: %v", err)
		}
		held = append(held, parking)
	}
	return held
}

// pausedServer is a deployment whose agent.control already carries a pause
// for the session under test.
func pausedServer(t *testing.T, control ...iggcon.IggyMessage) *runtimeServer {
	t.Helper()
	server := newRuntimeServer()
	server.partitions = 1
	// A non-zero generation, so a parking that names one is really checked
	// against it rather than matching a zero value by accident.
	server.createdAt = parkedGeneration
	server.seedHistory(wire.AgentControl, 0, 0, control...)
	return server
}

// The whole point of the gate: an operator's pause stops the work reaching
// the handler at all, rather than asking the handler to notice.
func TestGivenAPausedSessionWhenWorkArrivesThenShouldParkItWithoutHandlingIt(t *testing.T) {
	server := pausedServer(t, controlRecord(t, wire.OperationSessionPause, 0))
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{EnforceControl: true})
	source := sourceOf(workRecord(t, 41, "do it"))
	handler := &handling{}

	if err := runUntilDrained(t, runtime, source, handler); err != nil {
		t.Fatalf("run: %v", err)
	}

	if seen := handler.seen(); len(seen) != 0 {
		t.Errorf("want a paused session's work held back from the handler, got %v", seen)
	}
	held := parkedOn(t, server, wire.OperationSessionParked)
	if len(held) != 1 {
		t.Fatalf("want the record parked on the session lane, got %d", len(held))
	}
	if held[0].Role != agentID(t, "executor") {
		t.Errorf("want the parking to name the role holding it, got %q", held[0].Role)
	}
	if held[0].Source.Message == nil || held[0].Source.Message.Offset != 41 {
		t.Errorf("want the parking to name the source offset, got %+v", held[0].Source.Message)
	}
	if held[0].Source.Message.Generation == nil {
		t.Error("want the parking to name its source topic generation")
	}
	// Committed, not left behind: the work is durable on the lane now, so
	// re-reading it from the source would park it twice.
	if commits := source.commits(); len(commits) != 1 || commits[0] != 41 {
		t.Errorf("want the parked record committed, got %v", commits)
	}
}

// Parking is per source position, so a record redelivered while the session
// is still paused is already held rather than held twice.
func TestGivenAParkedRecordWhenRedeliveredThenShouldNotParkItTwice(t *testing.T) {
	server := pausedServer(t, controlRecord(t, wire.OperationSessionPause, 0))
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{EnforceControl: true})
	source := sourceOf(workRecord(t, 41, "do it"), workRecord(t, 41, "do it"))
	handler := &handling{}

	if err := runUntilDrained(t, runtime, source, handler); err != nil {
		t.Fatalf("run: %v", err)
	}
	if held := parkedOn(t, server, wire.OperationSessionParked); len(held) != 1 {
		t.Errorf("want one parking for one source position, got %d", len(held))
	}
}

// A session nobody paused runs exactly as before, which is what keeps the
// gate from being a tax on every other session.
func TestGivenAnUnpausedSessionWhenWorkArrivesThenShouldHandleItNormally(t *testing.T) {
	server := pausedServer(t)
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{EnforceControl: true})
	source := sourceOf(workRecord(t, 41, "do it"))
	handler := &handling{}

	if err := runUntilDrained(t, runtime, source, handler); err != nil {
		t.Fatalf("run: %v", err)
	}
	if seen := handler.seen(); len(seen) != 1 || seen[0] != "do it" {
		t.Errorf("want an unpaused session's work handled, got %v", seen)
	}
	if held := parkedOn(t, server, wire.OperationSessionParked); len(held) != 0 {
		t.Errorf("want nothing parked for an unpaused session, got %d", len(held))
	}
}

// The gate is opt-in, so a runtime that did not ask for it behaves exactly
// as it always has even against a paused session.
func TestGivenEnforcementOffWhenTheSessionIsPausedThenShouldStillHandleIt(t *testing.T) {
	server := pausedServer(t, controlRecord(t, wire.OperationSessionPause, 0))
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{})
	source := sourceOf(workRecord(t, 41, "do it"))
	handler := &handling{}

	if err := runUntilDrained(t, runtime, source, handler); err != nil {
		t.Fatalf("run: %v", err)
	}
	if seen := handler.seen(); len(seen) != 1 {
		t.Errorf("want the cooperative path unchanged, got %v", seen)
	}
	if held := parkedOn(t, server, wire.OperationSessionParked); len(held) != 0 {
		t.Errorf("want nothing parked with enforcement off, got %d", len(held))
	}
}

// A resume hands the held work back before the record that found the
// session resumed, so the session continues where it left off.
//
// The resume lands *between* the two records rather than before the run, so
// the first record is genuinely parked and the second genuinely finds a
// resumed session. Seeding both up front would prove nothing: the first
// record would simply have been handled.
func TestGivenAResumedSessionWhenWorkArrivesThenShouldHandleTheHeldRecordFirst(t *testing.T) {
	server := pausedServer(t, controlRecord(t, wire.OperationSessionPause, 0))
	held := workRecord(t, 41, "held work")
	// The held record is read back from its source to be replayed, so it
	// has to be there: the runtime consumes agent.sessions in these tests.
	server.seedHistory(wire.AgentSessions, 0, 0, held.Message)
	// A parking names its source topic by numeric id, which is what the
	// replay reads back through.
	server.aliasNumeric(3, wire.AgentSessions)

	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{EnforceControl: true})
	handler := &handling{}
	source := sourceOf(held, workRecord(t, 42, "new work"))
	source.beforeNext = func(index int) {
		if index != 1 {
			return
		}
		// The operator resumes after the first record was parked, and the
		// test waits for the runtime's own follower to read it rather than
		// writing to the book directly, so the follower is under test too.
		server.seedHistory(wire.AgentControl, 0, 0,
			controlRecord(t, wire.OperationSessionPause, 0),
			controlRecord(t, wire.OperationSessionResume, 1))
		waitForResume(t, runtime, conversation(1))
	}

	if err := runUntilDrained(t, runtime, source, handler); err != nil {
		t.Fatalf("run: %v", err)
	}
	seen := handler.seen()
	if len(seen) != 2 {
		t.Fatalf("want the held record and the new one handled, got %v", seen)
	}
	if seen[0] != "held work" || seen[1] != "new work" {
		t.Errorf("want the held record handled first, got %v", seen)
	}
	// The held record is written off the lane, so a reader folding it sees
	// the work handled rather than still held.
	if unparked := parkedOn(t, server, wire.OperationSessionUnparked); len(unparked) != 1 {
		t.Errorf("want the held record written off the lane, got %d", len(unparked))
	}
}

// waitForResume blocks until the runtime's control follower has read the
// resume, so a test never races the poll interval.
func waitForResume(t *testing.T, runtime *laser.AgentRuntime, session wire.ConversationID) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if pending, known := runtime.Control().Pending(session); known && !pending.PauseRequested {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the control follower never read the resume")
}

// Session.Parked folds the lane, so an operator sees exactly what is still
// being held and by whom.
func TestGivenParkedAndUnparkedRecordsWhenFoldedThenShouldListOnlyWhatIsStillHeld(t *testing.T) {
	first := parkingFor(t, "executor", 41)
	second := parkingFor(t, "executor", 42)
	envelopes := []wire.AgentEnvelope{
		parkedEnvelope(t, wire.OperationSessionParked, first),
		parkedEnvelope(t, wire.OperationSessionParked, second),
		parkedEnvelope(t, wire.OperationSessionUnparked, first),
	}

	parked := laser.FoldParkedForTest(envelopes)
	if len(parked.Records) != 1 {
		t.Fatalf("want only the record still held, got %d", len(parked.Records))
	}
	if parked.Records[0].Source.Message.Offset != 42 {
		t.Errorf("want the unhandled record listed, got offset %d",
			parked.Records[0].Source.Message.Offset)
	}
	if !parked.Complete {
		t.Error("want a clean fold reported complete")
	}
}

// Two roles holding the same record are two pieces of held work: one role's
// resume must not clear the other's.
func TestGivenTwoRolesHoldingOneRecordWhenOneResumesThenShouldKeepTheOther(t *testing.T) {
	mine := parkingFor(t, "executor", 41)
	theirs := parkingFor(t, "reviewer", 41)
	envelopes := []wire.AgentEnvelope{
		parkedEnvelope(t, wire.OperationSessionParked, mine),
		parkedEnvelope(t, wire.OperationSessionParked, theirs),
		parkedEnvelope(t, wire.OperationSessionUnparked, mine),
	}

	parked := laser.FoldParkedForTest(envelopes)
	if len(parked.Records) != 1 {
		t.Fatalf("want the other role's hold kept, got %d", len(parked.Records))
	}
	if parked.Records[0].Role != agentID(t, "reviewer") {
		t.Errorf("want the role that has not resumed, got %q", parked.Records[0].Role)
	}
}

// A parking record that will not decode is a hole in the account, so the
// fold says so rather than reporting a short list as the whole truth.
func TestGivenAnUndecodableParkingWhenFoldedThenShouldReportItIncomplete(t *testing.T) {
	operation := wire.OperationSessionParked
	envelopes := []wire.AgentEnvelope{{
		Operation: &operation,
		Body:      []byte("not a parking"),
	}}

	parked := laser.FoldParkedForTest(envelopes)
	if parked.Complete {
		t.Error("want an undecodable parking reported as an incomplete account")
	}
}

// parkingFor builds one parking record of role over a source offset.
// parkedGeneration is the topic generation the pause tests' deployment
// reports, and so the one a valid parking has to name.
const parkedGeneration uint64 = 99

func parkingFor(t *testing.T, role string, offset uint64) wire.SessionParking {
	t.Helper()
	generation := parkedGeneration
	return wire.SessionParking{
		Source: wire.SourceRef{
			Kind: wire.SourceMessage,
			Message: &wire.MessageSource{
				Stream: 7, Topic: 3, Partition: 0, Offset: offset,
				Generation: &generation,
			},
		},
		Request: wire.LogPosition{StreamID: 7, TopicID: 4},
		Role:    agentID(t, role),
	}
}

// parkedEnvelope wraps a parking as the lane record that carries it.
func parkedEnvelope(
	t *testing.T, operation string, parking wire.SessionParking,
) wire.AgentEnvelope {
	t.Helper()
	body, err := wire.Marshal(parking)
	if err != nil {
		t.Fatalf("encode the parking: %v", err)
	}
	named := operation
	return wire.AgentEnvelope{Operation: &named, Body: body}
}

// A parking is durable, so work held by a process that then restarted is
// still on the lane. A fresh runtime has to find it, or the work stays
// listed as held forever with nothing to hand it back.
func TestGivenHeldWorkFromAnEarlierProcessWhenResumedThenShouldRecoverAndReplayIt(t *testing.T) {
	server := pausedServer(t,
		controlRecord(t, wire.OperationSessionPause, 0),
		controlRecord(t, wire.OperationSessionResume, 1),
	)
	// The earlier process parked this record and stopped: the parking is on
	// the lane, the work is still readable at its source, and nothing in
	// this process has ever seen either.
	held := workRecord(t, 41, "held work")
	parking := parkingFor(t, "executor", 41)
	server.seedHistory(wire.AgentSessions, 0, 0,
		held.Message,
		parkingLaneRecord(t, wire.OperationSessionParked, parking),
	)
	server.aliasNumeric(3, wire.AgentSessions)

	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{EnforceControl: true})
	handler := &handling{}
	// One new record for the same session is what brings the runtime to
	// this session at all. The held work must go first.
	source := sourceOf(workRecord(t, 42, "new work"))

	if err := runUntilDrained(t, runtime, source, handler); err != nil {
		t.Fatalf("run: %v", err)
	}
	seen := handler.seen()
	if len(seen) != 2 {
		t.Fatalf("want the recovered record and the new one handled, got %v", seen)
	}
	if seen[0] != "held work" || seen[1] != "new work" {
		t.Errorf("want the recovered record handled first, got %v", seen)
	}
	if unparked := parkedOn(t, server, wire.OperationSessionUnparked); len(unparked) != 1 {
		t.Errorf("want the recovered record written off the lane, got %d", len(unparked))
	}
}

// Another role's held work is that role's to replay: taking it on would
// hand one agent's work to another.
func TestGivenAnotherRolesHeldWorkWhenRecoveringThenShouldLeaveItAlone(t *testing.T) {
	server := pausedServer(t,
		controlRecord(t, wire.OperationSessionPause, 0),
		controlRecord(t, wire.OperationSessionResume, 1),
	)
	held := workRecord(t, 41, "someone else's work")
	server.seedHistory(wire.AgentSessions, 0, 0,
		held.Message,
		parkingLaneRecord(t, wire.OperationSessionParked, parkingFor(t, "reviewer", 41)),
	)
	server.aliasNumeric(3, wire.AgentSessions)

	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{EnforceControl: true})
	handler := &handling{}
	source := sourceOf(workRecord(t, 42, "new work"))

	if err := runUntilDrained(t, runtime, source, handler); err != nil {
		t.Fatalf("run: %v", err)
	}
	seen := handler.seen()
	if len(seen) != 1 || seen[0] != "new work" {
		t.Errorf("want only this role's own work handled, got %v", seen)
	}
}

// A record this role already wrote off the lane is done, whichever order
// the parked and unparked records are read back in.
func TestGivenRecoveredWorkAlreadyUnparkedWhenResumedThenShouldNotReplayIt(t *testing.T) {
	server := pausedServer(t,
		controlRecord(t, wire.OperationSessionPause, 0),
		controlRecord(t, wire.OperationSessionResume, 1),
	)
	held := workRecord(t, 41, "already done")
	parking := parkingFor(t, "executor", 41)
	server.seedHistory(wire.AgentSessions, 0, 0,
		held.Message,
		parkingLaneRecord(t, wire.OperationSessionParked, parking),
		parkingLaneRecord(t, wire.OperationSessionUnparked, parking),
	)
	server.aliasNumeric(3, wire.AgentSessions)

	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{EnforceControl: true})
	handler := &handling{}
	source := sourceOf(workRecord(t, 42, "new work"))

	if err := runUntilDrained(t, runtime, source, handler); err != nil {
		t.Fatalf("run: %v", err)
	}
	if seen := handler.seen(); len(seen) != 1 || seen[0] != "new work" {
		t.Errorf("want the already-handled record left alone, got %v", seen)
	}
}

// parkingLaneRecord builds one parked or unparked lane record, as an earlier
// process would have written it.
func parkingLaneRecord(
	t *testing.T, operation string, parking wire.SessionParking,
) iggcon.IggyMessage {
	t.Helper()
	body, err := wire.Marshal(parking)
	if err != nil {
		t.Fatalf("encode the parking: %v", err)
	}
	record := delivered(t, 0, 0, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Emit(body).
			WithOperation(operation).
			ContentType(wire.ContentCBOR)
	})
	return record.Message
}

// A resume has to reach a session even when no further work arrives for it,
// or an operator's resume silently does nothing until the next record.
func TestGivenHeldWorkAndNoNewRecordsWhenResumedThenShouldStillHandItBack(t *testing.T) {
	server := pausedServer(t, controlRecord(t, wire.OperationSessionPause, 0))
	held := workRecord(t, 41, "held work")
	server.seedHistory(wire.AgentSessions, 0, 0,
		held.Message,
		parkingLaneRecord(t, wire.OperationSessionParked, parkingFor(t, "executor", 41)),
	)
	server.aliasNumeric(3, wire.AgentSessions)

	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{EnforceControl: true})
	handler := &handling{}
	// Nothing new for this session: only the reconcile can hand the held
	// record back.
	source := sourceOf()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx, source, handler) }()

	// The operator resumes after the run started.
	server.seedHistory(wire.AgentControl, 0, 0,
		controlRecord(t, wire.OperationSessionPause, 0),
		controlRecord(t, wire.OperationSessionResume, 1),
	)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if len(handler.seen()) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done

	seen := handler.seen()
	if len(seen) != 1 || seen[0] != "held work" {
		t.Fatalf("want the held record handed back without new work, got %v", seen)
	}
	if unparked := parkedOn(t, server, wire.OperationSessionUnparked); len(unparked) != 1 {
		t.Errorf("want it written off the lane, got %d", len(unparked))
	}
}

// A member that starts reading a partition it did not read before may now
// be responsible for work another member parked, so it re-reads the lane
// rather than deciding as though it held nothing there.
func TestGivenANewPartitionWhenReadThenShouldRebuildHeldWorkAgain(t *testing.T) {
	server := pausedServer(t,
		controlRecord(t, wire.OperationSessionPause, 0),
		controlRecord(t, wire.OperationSessionResume, 1),
	)
	server.partitions = 2
	// The work another member parked, on the partition this one is about to
	// start reading.
	held := workRecord(t, 41, "another member's held work")
	server.seedHistory(wire.AgentSessions, 0, 0, held.Message)
	server.aliasNumeric(3, wire.AgentSessions)

	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{EnforceControl: true})
	handler := &handling{}
	// The first record settles this member's starting assignment. The
	// parking only appears on the lane afterwards, so only a re-read
	// triggered by the second record's new partition can find it.
	first := workRecord(t, 10, "own work")
	second := workRecord(t, 11, "own work again")
	second.PartitionID = 1
	source := sourceOf(first, second)
	source.beforeNext = func(index int) {
		if index != 1 {
			return
		}
		server.seedHistory(wire.AgentSessions, 0, 0,
			held.Message,
			parkingLaneRecord(t, wire.OperationSessionParked, parkingFor(t, "executor", 41)),
		)
	}

	if err := runUntilDrained(t, runtime, source, handler); err != nil {
		t.Fatalf("run: %v", err)
	}
	seen := handler.seen()
	found := false
	for _, body := range seen {
		if body == "another member's held work" {
			found = true
		}
	}
	if !found {
		t.Errorf("want the re-read to find the other member's held work, got %v", seen)
	}
}

// acknowledgementsOn reads back the session-status records a role wrote to
// acknowledge control requests, with the state each reported.
func acknowledgementsOn(t *testing.T, server *runtimeServer) []wire.TaskState {
	t.Helper()
	var states []wire.TaskState
	for _, message := range server.on(wire.AgentSessions) {
		var envelope wire.AgentEnvelope
		if err := wire.Unmarshal(message.Payload, &envelope); err != nil {
			continue
		}
		if envelope.Kind != wire.KindStatus || envelope.Operation == nil ||
			*envelope.Operation != wire.OperationSession || envelope.TaskState == nil {
			continue
		}
		var transition wire.SessionTransition
		if err := wire.Unmarshal(envelope.Body, &transition); err != nil {
			continue
		}
		if transition.Acknowledges == nil {
			continue
		}
		states = append(states, *envelope.TaskState)
	}
	return states
}

// An operator needs to see which agents actually stopped, not only that a
// pause was asked for, so a role acknowledges the request on the lane.
func TestGivenAPausedSessionWhenWorkArrivesThenShouldAcknowledgeTheRequest(t *testing.T) {
	server := pausedServer(t, controlRecord(t, wire.OperationSessionPause, 0))
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{EnforceControl: true})
	// Two records of the same paused session: the pause is acknowledged
	// once, not once per record.
	source := sourceOf(workRecord(t, 41, "do it"), workRecord(t, 42, "again"))

	if err := runUntilDrained(t, runtime, source, &handling{}); err != nil {
		t.Fatalf("run: %v", err)
	}
	states := acknowledgementsOn(t, server)
	if len(states) != 1 || states[0] != wire.TaskPaused {
		t.Fatalf("want one paused acknowledgment, got %v", states)
	}
}

// A role that acknowledged a pause says it is working again when the resume
// lifts it. One that never stopped says nothing: it was never a participant.
func TestGivenAResumeWhenTheRoleHadStoppedThenShouldAcknowledgeWorkingAgain(t *testing.T) {
	server := pausedServer(t, controlRecord(t, wire.OperationSessionPause, 0))
	held := workRecord(t, 41, "held work")
	server.seedHistory(wire.AgentSessions, 0, 0, held.Message)
	server.aliasNumeric(3, wire.AgentSessions)

	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{EnforceControl: true})
	source := sourceOf(held, workRecord(t, 42, "new work"))
	source.beforeNext = func(index int) {
		if index != 1 {
			return
		}
		server.seedHistory(wire.AgentControl, 0, 0,
			controlRecord(t, wire.OperationSessionPause, 0),
			controlRecord(t, wire.OperationSessionResume, 1))
		waitForResume(t, runtime, conversation(1))
	}

	if err := runUntilDrained(t, runtime, source, &handling{}); err != nil {
		t.Fatalf("run: %v", err)
	}
	states := acknowledgementsOn(t, server)
	if len(states) != 2 {
		t.Fatalf("want a paused and a working acknowledgment, got %v", states)
	}
	if states[0] != wire.TaskPaused || states[1] != wire.TaskWorking {
		t.Errorf("want paused then working, got %v", states)
	}
}

// A session nobody paused is acknowledged by nobody.
func TestGivenAnUnpausedSessionWhenHandledThenShouldAcknowledgeNothing(t *testing.T) {
	server := pausedServer(t)
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{EnforceControl: true})

	if err := runUntilDrained(
		t, runtime, sourceOf(workRecord(t, 41, "do it")), &handling{}); err != nil {
		t.Fatalf("run: %v", err)
	}
	if states := acknowledgementsOn(t, server); len(states) != 0 {
		t.Errorf("want no acknowledgment for an unpaused session, got %v", states)
	}
}
