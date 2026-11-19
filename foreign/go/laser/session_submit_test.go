package laser_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// submitSessions opens the session factory a submission is built from.
func submitSessions(t *testing.T, server *stateServer) *laser.Sessions {
	t.Helper()
	server.session = conversation(1)
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	return handle.Sessions()
}

// lifecycleRecord puts one lifecycle status on the lane.
func lifecycleRecord(
	t *testing.T, server *stateServer, session wire.ConversationID,
	state wire.TaskState, start *wire.SessionStart,
) {
	t.Helper()
	var body []byte
	if start != nil {
		encoded, err := wire.Marshal(*start)
		if err != nil {
			t.Fatalf("encode the start: %v", err)
		}
		body = encoded
	}
	operation := wire.OperationSession
	message := laneRecord(t, session, wire.AgentEnvelope{
		Kind: wire.KindStatus, Source: agentID(t, "worker"),
		Operation: &operation, TaskState: &state, Body: body,
	})
	server.mutex.Lock()
	defer server.mutex.Unlock()
	message.Header.Offset = uint64(len(server.lane))
	server.lane = append(server.lane, message)
}

func TestGivenASubmissionWhenSentThenShouldWriteTheStatusThenTheCommand(t *testing.T) {
	server := &stateServer{}
	sessions := submitSessions(t, server)

	submitted, err := sessions.Submit(agentID(t, "worker"), []byte("do the thing")).
		From(agentID(t, "planner")).Send(context.Background())
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	written := recorded(t, server)
	if len(written) != 2 {
		t.Fatalf("want a status and a command, got %d", len(written))
	}
	status, command := written[0], written[1]
	// The status comes first, so a session never has work on it before it
	// exists on the log.
	if status.Kind != wire.KindStatus || status.TaskState == nil ||
		*status.TaskState != wire.TaskSubmitted {
		t.Fatalf("got %q %v", status.Kind, status.TaskState)
	}
	if command.Kind != wire.KindCommand {
		t.Fatalf("got %q", command.Kind)
	}
	if command.Target == nil || *command.Target != agentID(t, "worker") {
		t.Fatalf("got %v", command.Target)
	}
	if command.Operation == nil || *command.Operation != wire.OperationInvokeAgent {
		t.Fatalf("got %v", command.Operation)
	}
	if string(command.Body) != "do the thing" {
		t.Fatalf("got %q", command.Body)
	}
	if value, carried := metadata(t, command, wire.MetadataSubmitted); !carried ||
		!value.Bool {
		t.Fatalf("got %+v %v", value, carried)
	}
	if command.Correlation == nil || *command.Correlation != submitted.Correlation {
		t.Fatalf("got %v", command.Correlation)
	}
	// Both records ride the session the submission named, so a caller that
	// took the id before sending is watching the right one.
	if status.Conversation != submitted.Session ||
		command.Conversation != submitted.Session {
		t.Fatalf("got %v and %v", status.Conversation, command.Conversation)
	}
	var start wire.SessionStart
	if err := wire.Unmarshal(status.Body, &start); err != nil {
		t.Fatalf("decode the start: %v", err)
	}
	if start.Agent != agentID(t, "worker") {
		t.Fatalf("got %v", start.Agent)
	}
	if start.IdleTimeoutMicros == nil ||
		*start.IdleTimeoutMicros != uint64(laser.DefaultSessionIdleTimeout.Microseconds()) {
		t.Fatalf("got %v", start.IdleTimeoutMicros)
	}
	if start.SDK.Language != "go" {
		t.Fatalf("got %+v", start.SDK)
	}
}

func TestGivenASubmissionWhenAskedForItsIDThenShouldNameWhatItWillWrite(t *testing.T) {
	server := &stateServer{}
	sessions := submitSessions(t, server)
	builder := sessions.Submit(agentID(t, "worker"), []byte("x")).
		From(agentID(t, "planner"))

	// A caller takes the id before sending so it can watch the session it is
	// about to create. Asking twice must name the same one.
	first, second := builder.ID(), builder.ID()
	if first != second {
		t.Fatalf("got %v then %v", first, second)
	}
	submitted, err := builder.Send(context.Background())
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if submitted.Session != first {
		t.Fatalf("sent to %v, named %v", submitted.Session, first)
	}
}

func TestGivenALabelWhenSubmittedThenShouldDeriveTheSameSession(t *testing.T) {
	server := &stateServer{}
	sessions := submitSessions(t, server)

	first := sessions.Submit(agentID(t, "worker"), []byte("x")).
		From(agentID(t, "planner")).Label("nightly").Namespace("jobs")
	second := sessions.Submit(agentID(t, "worker"), []byte("y")).
		From(agentID(t, "planner")).Label("nightly").Namespace("jobs")

	// The same label reaches the same session, which is what lets a job that
	// runs nightly resume rather than start a new one each time.
	if first.ID() != second.ID() {
		t.Fatalf("got %v and %v", first.ID(), second.ID())
	}
	other := sessions.Submit(agentID(t, "worker"), []byte("x")).
		From(agentID(t, "planner")).Label("nightly").Namespace("other")
	if other.ID() == first.ID() {
		t.Fatal("another namespace is another session")
	}
	if _, err := first.Send(context.Background()); err != nil {
		t.Fatalf("submit: %v", err)
	}
	var start wire.SessionStart
	if err := wire.Unmarshal(recorded(t, server)[0].Body, &start); err != nil {
		t.Fatalf("decode the start: %v", err)
	}
	// Both ride the start record, so a reader can tell which job a session
	// belongs to without recomputing the derivation.
	if start.Label == nil || *start.Label != "nightly" {
		t.Fatalf("got %v", start.Label)
	}
	if start.Namespace == nil || *start.Namespace != "jobs" {
		t.Fatalf("got %v", start.Namespace)
	}
	unlabeled := sessions.Submit(agentID(t, "worker"), []byte("x")).
		From(agentID(t, "planner"))
	if unlabeled.ID() == first.ID() {
		t.Fatal("an unlabeled submission is a fresh session")
	}
}

func TestGivenACanceledLabeledSessionWhenSubmittedThenShouldRefuseIt(t *testing.T) {
	server := &stateServer{}
	sessions := submitSessions(t, server)
	builder := sessions.Submit(agentID(t, "worker"), []byte("x")).
		From(agentID(t, "planner")).Label("nightly")
	server.session = builder.ID()
	lifecycleRecord(t, server, builder.ID(), wire.TaskSubmitted,
		&wire.SessionStart{Agent: agentID(t, "worker")})
	lifecycleRecord(t, server, builder.ID(), wire.TaskCanceled, nil)

	_, err := builder.Send(context.Background())
	var ended *laser.SessionEndedError
	if !asEndedError(err, &ended) {
		t.Fatalf("want a SessionEndedError, got %T: %v", err, err)
	}
	// A cancel is a decision somebody made, so resuming under the same label
	// would undo it quietly.
	if ended.Status != wire.SessionCanceled {
		t.Fatalf("got %q", ended.Status)
	}
	if server.written() != 2 {
		t.Fatal("nothing must be written")
	}
}

func TestGivenACompletedLabeledSessionWhenSubmittedThenShouldResumeIt(t *testing.T) {
	server := &stateServer{}
	sessions := submitSessions(t, server)
	builder := sessions.Submit(agentID(t, "worker"), []byte("x")).
		From(agentID(t, "planner")).Label("nightly")
	server.session = builder.ID()
	lifecycleRecord(t, server, builder.ID(), wire.TaskSubmitted,
		&wire.SessionStart{Agent: agentID(t, "worker")})
	lifecycleRecord(t, server, builder.ID(), wire.TaskCompleted, nil)

	// A completed session is finished work, not a refused one, so the next
	// run of the same job picks the label back up.
	if _, err := builder.Send(context.Background()); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if server.written() != 4 {
		t.Fatalf("got %d records", server.written())
	}
}

func TestGivenAForcedCancelWhenSubmittedThenShouldRefuseTheLabel(t *testing.T) {
	server := &stateServer{}
	sessions := submitSessions(t, server)
	builder := sessions.Submit(agentID(t, "worker"), []byte("x")).
		From(agentID(t, "planner")).Label("nightly")
	server.session = builder.ID()
	lifecycleRecord(t, server, builder.ID(), wire.TaskSubmitted,
		&wire.SessionStart{Agent: agentID(t, "worker")})
	// An operator forced the cancel on the control topic. The session's
	// agent is gone, so there is no lane record saying it ended.
	operation := wire.OperationSession
	state := wire.TaskCanceled
	message := laneRecord(t, builder.ID(), wire.AgentEnvelope{
		Kind: wire.KindStatus, Source: agentID(t, "ops"),
		Operation: &operation, TaskState: &state,
	})
	server.mutex.Lock()
	server.control = append(server.control, message)
	server.mutex.Unlock()

	_, err := builder.Send(context.Background())
	var ended *laser.SessionEndedError
	if !asEndedError(err, &ended) {
		t.Fatalf("want a SessionEndedError, got %T: %v", err, err)
	}
}

func TestGivenAnotherSessionsForcedCancelThenShouldNotEndThisOne(t *testing.T) {
	server := &stateServer{}
	sessions := submitSessions(t, server)
	builder := sessions.Submit(agentID(t, "worker"), []byte("x")).
		From(agentID(t, "planner")).Label("nightly")
	server.session = builder.ID()
	lifecycleRecord(t, server, builder.ID(), wire.TaskSubmitted,
		&wire.SessionStart{Agent: agentID(t, "worker")})
	// The control topic carries every session's requests, so another
	// session's cancel sits beside this one's and must not end it.
	operation := wire.OperationSession
	state := wire.TaskCanceled
	message := laneRecord(t, conversation(99), wire.AgentEnvelope{
		Kind: wire.KindStatus, Source: agentID(t, "ops"),
		Operation: &operation, TaskState: &state,
	})
	server.mutex.Lock()
	server.control = append(server.control, message)
	server.mutex.Unlock()

	if _, err := builder.Send(context.Background()); err != nil {
		t.Fatalf("another session's cancel must not end this one: %v", err)
	}
}

func TestGivenAControlStatusThatIsNotACancelThenShouldNotEndTheSession(t *testing.T) {
	server := &stateServer{}
	sessions := submitSessions(t, server)
	builder := sessions.Submit(agentID(t, "worker"), []byte("x")).
		From(agentID(t, "planner")).Label("nightly")
	server.session = builder.ID()
	lifecycleRecord(t, server, builder.ID(), wire.TaskSubmitted,
		&wire.SessionStart{Agent: agentID(t, "worker")})
	// A status on the control topic that is not a cancel says nothing about
	// whether the session ended. Reading any status as a forced cancel
	// would refuse a label over an operator's progress note.
	operation := wire.OperationSession
	state := wire.TaskWorking
	message := laneRecord(t, builder.ID(), wire.AgentEnvelope{
		Kind: wire.KindStatus, Source: agentID(t, "ops"),
		Operation: &operation, TaskState: &state,
	})
	server.mutex.Lock()
	server.control = append(server.control, message)
	server.mutex.Unlock()

	if _, err := builder.Send(context.Background()); err != nil {
		t.Fatalf("a status that is not a cancel must not end the session: %v", err)
	}
}

func TestGivenACommandThatCannotBePublishedThenShouldEndTheSession(t *testing.T) {
	server := &stateServer{}
	sessions := submitSessions(t, server)

	// The status landed and the command did not. A session that still reads
	// as submitted with no work in it is worse than one that visibly failed.
	server.mutex.Lock()
	server.refuseAt = 2
	server.mutex.Unlock()

	_, err := sessions.Submit(agentID(t, "worker"), []byte("x")).
		From(agentID(t, "planner")).Send(context.Background())
	if err == nil {
		t.Fatal("the publish failure must be reported")
	}

	written := recorded(t, server)
	if len(written) != 2 {
		t.Fatalf("want the status and the failure, got %d", len(written))
	}
	failure := written[1]
	if failure.TaskState == nil || *failure.TaskState != wire.TaskFailed {
		t.Fatalf("got %v", failure.TaskState)
	}
	var end wire.SessionEnd
	if err := wire.Unmarshal(failure.Body, &end); err != nil {
		t.Fatalf("decode the end: %v", err)
	}
	if end.Error == nil || end.Error.Message == nil ||
		!strings.Contains(*end.Error.Message, "not published") {
		t.Fatalf("got %+v", end.Error)
	}
}

func TestGivenAnIncompleteSubmissionWhenSentThenShouldRefuseIt(t *testing.T) {
	server := &stateServer{}
	sessions := submitSessions(t, server)

	_, err := sessions.Submit(agentID(t, "worker"), []byte("x")).
		Send(context.Background())
	if err == nil || !strings.Contains(err.Error(), "From(submitter)") {
		t.Fatalf("got %v, want the submitter named", err)
	}
	_, err = sessions.Submit(wire.AgentID{}, []byte("x")).
		From(agentID(t, "planner")).Send(context.Background())
	if err == nil || !strings.Contains(err.Error(), "hands work to") {
		t.Fatalf("got %v, want the agent named", err)
	}
	if server.written() != 0 {
		t.Fatal("nothing must be written")
	}
}

func TestGivenANegativeIdleTimeoutWhenSubmittedThenShouldRefuseIt(t *testing.T) {
	// Zero takes the default, but a negative one is a caller mistake that
	// would ride the wire as an enormous unsigned timeout.
	server := &stateServer{}
	server.session = conversation(1)
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	sessions := handle.SessionsWith(laser.SessionConfig{IdleTimeout: -time.Second})

	_, err := sessions.Submit(agentID(t, "worker"), []byte("x")).
		From(agentID(t, "planner")).Send(context.Background())
	if err == nil || !strings.Contains(err.Error(), "idle timeout") {
		t.Fatalf("got %v", err)
	}
	if server.written() != 0 {
		t.Fatal("nothing must be written")
	}
}

func TestGivenAStartTheContractRefusesWhenSubmittedThenShouldNotWriteIt(t *testing.T) {
	server := &stateServer{}
	sessions := submitSessions(t, server)

	// A label with a control character in it is one the contract refuses, so
	// it is caught here rather than by the server after the record landed.
	_, err := sessions.Submit(agentID(t, "worker"), []byte("x")).
		From(agentID(t, "planner")).Label("night\x00ly").Send(context.Background())
	// The encoder would refuse it too, but as a codec failure. A label the
	// caller chose is an invalid argument, and a caller matching on that
	// class should find it there.
	var invalid *laser.InvalidError
	if !asInvalidError(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
	if !strings.Contains(invalid.Reason, "label") {
		t.Fatalf("got %q", invalid.Reason)
	}
	if server.written() != 0 {
		t.Fatal("nothing must be written")
	}
}

func TestGivenAnUnlabeledSubmissionWhenSentThenShouldNotReadTheLane(t *testing.T) {
	server := &stateServer{}
	sessions := submitSessions(t, server)

	// A fresh session has no history, so reading one would be a round trip
	// per submission for an answer that cannot be anything but empty.
	if _, err := sessions.Submit(agentID(t, "worker"), []byte("x")).
		From(agentID(t, "planner")).Send(context.Background()); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if reads := server.reads(); reads != 0 {
		t.Fatalf("an unlabeled submission read the lane %d times", reads)
	}
}

func TestGivenTwoUnlabeledSubmissionsThenShouldReachDifferentSessions(t *testing.T) {
	server := &stateServer{}
	sessions := submitSessions(t, server)

	first := sessions.Submit(agentID(t, "worker"), []byte("x")).
		From(agentID(t, "planner"))
	second := sessions.Submit(agentID(t, "worker"), []byte("y")).
		From(agentID(t, "planner"))
	// Two pieces of work nobody named are two sessions. Deriving both from
	// the absent label would pile them into one.
	if first.ID() == second.ID() {
		t.Fatalf("both submissions named %v", first.ID())
	}
}

func TestGivenASubmissionBuilderWhenDerivedThenShouldNotChangeTheOriginal(t *testing.T) {
	server := &stateServer{}
	sessions := submitSessions(t, server)
	base := sessions.Submit(agentID(t, "worker"), []byte("x"))

	tagged := base.From(agentID(t, "planner")).Tag("first").Tag("second")
	if _, err := base.Send(context.Background()); err == nil {
		t.Fatal("the original named no submitter")
	}
	if _, err := tagged.Send(context.Background()); err != nil {
		t.Fatalf("submit: %v", err)
	}
	var start wire.SessionStart
	if err := wire.Unmarshal(recorded(t, server)[0].Body, &start); err != nil {
		t.Fatalf("decode the start: %v", err)
	}
	if len(start.Tags) != 2 || start.Tags[0] != "first" || start.Tags[1] != "second" {
		t.Fatalf("got %v", start.Tags)
	}
}

func TestGivenOneTaggedBuilderWhenBranchedThenShouldNotShareItsTags(t *testing.T) {
	server := &stateServer{}
	sessions := submitSessions(t, server)
	// Three tags leaves the slice behind them with room to spare, so an
	// append in place would land in it rather than allocating.
	shared := sessions.Submit(agentID(t, "worker"), []byte("x")).
		From(agentID(t, "planner")).
		Tag("a").Tag("b").Tag("shared")

	// Two branches of one builder each add their own tag. Appending in
	// place would have the second write into the first's slice.
	first := shared.Tag("first")
	second := shared.Tag("second")
	for index, builder := range []*laser.SubmitBuilder{first, second} {
		if _, err := builder.Send(context.Background()); err != nil {
			t.Fatalf("submit %d: %v", index, err)
		}
	}
	written := recorded(t, server)
	for index, want := range []string{"first", "second"} {
		var start wire.SessionStart
		if err := wire.Unmarshal(written[index*2].Body, &start); err != nil {
			t.Fatalf("decode start %d: %v", index, err)
		}
		if len(start.Tags) != 4 || start.Tags[2] != "shared" ||
			start.Tags[3] != want {
			t.Fatalf("branch %d carries %v", index, start.Tags)
		}
	}
}

func TestGivenABudgetAndTagsWhenSubmittedThenShouldRecordThem(t *testing.T) {
	server := &stateServer{}
	sessions := submitSessions(t, server)
	tokens := uint64(5000)

	if _, err := sessions.Submit(agentID(t, "worker"), []byte("x")).
		From(agentID(t, "planner")).
		Budget(wire.Budget{Tokens: &tokens}).
		Operation("summarize").
		Tag("nightly").Send(context.Background()); err != nil {
		t.Fatalf("submit: %v", err)
	}

	written := recorded(t, server)
	var start wire.SessionStart
	if err := wire.Unmarshal(written[0].Body, &start); err != nil {
		t.Fatalf("decode the start: %v", err)
	}
	if start.Budget == nil || start.Budget.Tokens == nil || *start.Budget.Tokens != 5000 {
		t.Fatalf("got %+v", start.Budget)
	}
	if len(start.Tags) != 1 || start.Tags[0] != "nightly" {
		t.Fatalf("got %v", start.Tags)
	}
	if written[1].Operation == nil || *written[1].Operation != "summarize" {
		t.Fatalf("got %v", written[1].Operation)
	}
}

func TestGivenALaneWhenFoldedThenShouldReportTheLifecycle(t *testing.T) {
	tokens := uint64(10)
	resumed := uint64(20)
	start := func(budget *uint64) wire.SessionStart {
		opened := wire.SessionStart{Agent: agentID(t, "worker")}
		if budget != nil {
			opened.Budget = &wire.Budget{Tokens: budget}
		}
		return opened
	}
	for _, probe := range []struct {
		name    string
		states  []wire.TaskState
		starts  []*wire.SessionStart
		started bool
		ended   wire.SessionStatus
		budget  *uint64
	}{
		{"an open session", []wire.TaskState{wire.TaskSubmitted},
			[]*wire.SessionStart{ptrStart(start(&tokens))}, true, "", &tokens},
		{"a completed session",
			[]wire.TaskState{wire.TaskSubmitted, wire.TaskCompleted},
			[]*wire.SessionStart{ptrStart(start(&tokens)), nil},
			true, wire.SessionCompleted, &tokens},
		{"a failed session",
			[]wire.TaskState{wire.TaskSubmitted, wire.TaskRejected},
			[]*wire.SessionStart{ptrStart(start(nil)), nil},
			true, wire.SessionFailed, nil},
		{"a resumed session",
			[]wire.TaskState{wire.TaskSubmitted, wire.TaskCompleted, wire.TaskWorking},
			[]*wire.SessionStart{ptrStart(start(&tokens)), nil, ptrStart(start(&resumed))},
			true, "", &resumed},
		{"a canceled session is not resumed",
			[]wire.TaskState{wire.TaskSubmitted, wire.TaskCanceled, wire.TaskWorking},
			[]*wire.SessionStart{ptrStart(start(&tokens)), nil, ptrStart(start(&resumed))},
			true, wire.SessionCanceled, &tokens},
		{"the first end stands",
			[]wire.TaskState{wire.TaskSubmitted, wire.TaskCompleted, wire.TaskFailed},
			[]*wire.SessionStart{ptrStart(start(nil)), nil, nil},
			true, wire.SessionCompleted, nil},
	} {
		t.Run(probe.name, func(t *testing.T) {
			var envelopes []wire.AgentEnvelope
			for index, state := range probe.states {
				operation := wire.OperationSession
				taskState := state
				envelope := wire.AgentEnvelope{
					Kind: wire.KindStatus, Operation: &operation, TaskState: &taskState,
				}
				if probe.starts[index] != nil {
					body, err := wire.Marshal(*probe.starts[index])
					if err != nil {
						t.Fatalf("encode: %v", err)
					}
					envelope.Body = body
				}
				envelopes = append(envelopes, envelope)
			}
			lifecycle := laser.FoldLaneLifecycle(envelopes)
			if lifecycle.Started != probe.started || lifecycle.Ended != probe.ended {
				t.Fatalf("got %+v", lifecycle)
			}
			switch {
			case probe.budget == nil && lifecycle.Budget != nil:
				t.Fatalf("got %+v", lifecycle.Budget)
			case probe.budget != nil:
				if lifecycle.Budget == nil || lifecycle.Budget.Tokens == nil ||
					*lifecycle.Budget.Tokens != *probe.budget {
					t.Fatalf("got %+v", lifecycle.Budget)
				}
			}
		})
	}
}

func TestGivenRecordsThatAreNotLifecycleWhenFoldedThenShouldIgnoreThem(t *testing.T) {
	operation := wire.OperationSession
	other := wire.OperationChat
	state := wire.TaskCompleted
	lifecycle := laser.FoldLaneLifecycle([]wire.AgentEnvelope{
		// A command, not a status.
		{Kind: wire.KindCommand, Operation: &operation, TaskState: &state},
		// A status of another operation.
		{Kind: wire.KindStatus, Operation: &other, TaskState: &state},
		// A session status with no state at all.
		{Kind: wire.KindStatus, Operation: &operation},
		// A start whose body is not one.
		{Kind: wire.KindStatus, Operation: &operation,
			TaskState: taskStatePointer(wire.TaskSubmitted), Body: []byte{0xff}},
	})
	if lifecycle.Started || lifecycle.Ended != "" || lifecycle.Budget != nil {
		t.Fatalf("got %+v", lifecycle)
	}
}

func ptrStart(start wire.SessionStart) *wire.SessionStart { return &start }

func taskStatePointer(state wire.TaskState) *wire.TaskState { return &state }

// asInvalidError narrows an error to the invalid-argument failure under it.
func asInvalidError(err error, target **laser.InvalidError) bool {
	for err != nil {
		if invalid, ok := err.(*laser.InvalidError); ok {
			*target = invalid
			return true
		}
		unwrapped, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = unwrapped.Unwrap()
	}
	return false
}

// asEndedError narrows an error to the session-ended failure under it.
func asEndedError(err error, target **laser.SessionEndedError) bool {
	for err != nil {
		if ended, ok := err.(*laser.SessionEndedError); ok {
			*target = ended
			return true
		}
		unwrapped, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = unwrapped.Unwrap()
	}
	return false
}
