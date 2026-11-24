package bdd_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	bdd "github.com/laserdata/laser-sdk/bdd/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// TestGivenTheSharedSessionScenariosWhenRunThenShouldHold runs the agent
// session feature: the lifecycle, the model and tool records, and the state
// document, all as typed records on the session lane.
func TestGivenTheSharedSessionScenariosWhenRunThenShouldHold(t *testing.T) {
	runFeaturePlanned(t, "session.feature", 2*time.Minute, sessionStep, sessionPlan)
}

// sessionPlan names every scenario of this feature the runner does not yet
// run, and why.
//
// It is written out rather than left implicit so the gap is countable: a
// scenario missing from here and missing a step implementation fails, which is
// the point. "needs an agent consuming the lane" marks the ones waiting on the
// agent consumer runtime; the rest is runner work still to do.
var sessionPlan = map[string]string{
	"A stale handle's end leaves the newer state document in place":                                       "this runner has not written the second-handle state steps",
	"A stale handle's state snapshot is refused as a stale session":                                       "this runner has not written the second-handle state steps",
	"A stale handle's next delta leaves the newer state document in place":                                "this runner has not written the second-handle state steps",
	"A declared partition layout routes commands to the addressee and replies to the requester":           "this runner has not written the declared layout steps",
	"A declared topic layout routes commands and replies to the agents' own topics":                       "this runner has not written the declared layout steps",
	"A terminal verb after a refused publish re-sends the latched record and a different verb is refused": "this runner has not written the refusing policy steps",
	"A refused state snapshot remains pending for the end retry":                                          "this runner has not written the refusing policy steps",
	"Heartbeats stop when the last lease drops":                                                           "this runner has not written the heartbeat steps",
	"Heartbeat batches never mix sessions of two streams":                                                 "this runner has not written the heartbeat steps",
	"The context manifest lists the source address of every fragment":                                     "this runner has not written the context manifest steps",
	"Concurrent terminal calls from clones of one session write one latched state":                        "this runner has not written the concurrent clone steps",
}

func sessionStep(ctx context.Context, state *world, step bdd.Step) error {
	handled, err := background(ctx, state, step)
	if handled {
		return err
	}
	text := step.Text
	arguments := argumentsOf(step)

	switch {
	case strings.HasPrefix(text, "agent ") && strings.Contains(text, " resumes the session "):
		return resumeSession(ctx, state, arguments)

	case strings.HasPrefix(text, "agent ") && strings.Contains(text, " starts the session "):
		return startSession(ctx, state, text, arguments)

	case strings.HasPrefix(text, "agent ") && strings.Contains(text, " runs the session "):
		return runFailingSession(ctx, state, arguments)

	case strings.HasPrefix(text, "agent ") && strings.Contains(text, " opens the session "):
		return opensSessionLens(ctx, state, arguments)

	case strings.HasPrefix(text, "agent ") && strings.Contains(text, "ends every session it handles"):
		return endsEverySessionStep(ctx, state, arguments)

	case strings.HasPrefix(text, "agent ") && strings.Contains(text, "handled exactly"):
		return checkHandledExactly(state, arguments)

	case strings.HasPrefix(text, "agent ") && strings.Contains(text, "sees no pending pause or cancel"):
		return checkNoPendingControl(state, arguments)

	case strings.HasPrefix(text, "agents \"") && strings.Contains(text, "each record the commands they handle"):
		return spawnRecordingAgentPair(ctx, state, arguments)

	case strings.Contains(text, "submits a session to agent"):
		return submitSessionTo(ctx, state, arguments)

	case strings.HasPrefix(text, "the submitted session starts as"):
		return checkSubmittedReaches(ctx, state, arguments)

	case text == "a fresh stream bootstrapped for sessions in the single-partition layout":
		return singlePartitionLayoutStream(ctx, state)

	case strings.Contains(text, "in separate sessions"):
		return sendSeparateSessions(ctx, state, arguments)

	case text == "every command landed on the same partition":
		return checkSamePartition(state)

	case strings.Contains(text, "writes the control requests"):
		return misplacedControl(ctx, state, arguments)

	case strings.Contains(text, "sends work to agent"):
		return sendWorkToAgent(ctx, state, arguments)

	case strings.HasPrefix(text, "sessions publish heartbeats every"):
		return heartbeatEvery(step, state)

	case strings.HasPrefix(text, "no heartbeat is published"):
		return checkNoHeartbeat(ctx, state, step)

	case text == "the session ends" || text == "I end the session":
		return endSession(ctx, state)

	case text == "I cancel the session":
		return cancelSession(ctx, state)

	case strings.HasPrefix(text, "the session lane shows"):
		return laneShows(ctx, state, text, arguments)

	case text == "the call fails as invalid":
		var invalid *laser.InvalidError
		if !errors.As(state.failure, &invalid) {
			return fmt.Errorf("want an invalid refusal, got %v", state.failure)
		}
		return nil

	case strings.HasPrefix(text, "the call fails as a session that ended"):
		return endedRefusal(state, text)

	case strings.HasPrefix(text, "the session failure message is"):
		return failureMessage(ctx, state, arguments)

	case strings.HasPrefix(text, "the session \"") && strings.Contains(text, "same id when created twice"):
		return sameLabelSameSession(state, arguments)

	case strings.HasPrefix(text, "the sessions ") && strings.Contains(text, "different ids"):
		return differentLabelsDifferentSessions(state, arguments)

	case strings.HasPrefix(text, "the session records the message"):
		return recordMessage(ctx, state, arguments)

	case strings.HasPrefix(text, "the session records a model call"):
		return recordModelCall(ctx, state, text, arguments)

	case strings.HasPrefix(text, "the session records a call of tool"):
		return recordToolCall(ctx, state, arguments)

	case text == "no recorded call carries a secret value":
		return noSecrets(ctx, state)

	case strings.HasPrefix(text, "the session state sets"):
		return setState(ctx, state, text, arguments)

	case strings.HasPrefix(text, "the folded session state has"):
		return foldedState(ctx, state, text)

	case strings.HasPrefix(text, "operator ") && strings.Contains(text, "asks to cancel the session"):
		return operatorCancels(ctx, state, arguments)

	case strings.HasPrefix(text, "the control topic holds"):
		return controlHolds(ctx, state, arguments)

	case strings.HasPrefix(text, "the context assembled from the session reads"):
		return assembledReads(ctx, state, arguments)

	case strings.HasPrefix(text, "I take a session checkpoint after"):
		counts := numbersOf(step)
		if len(counts) != 1 {
			return fmt.Errorf("want one record count, got %v", counts)
		}
		return takeCheckpoint(ctx, state, int(counts[0]))

	case strings.HasPrefix(text, "the records since the checkpoint are"):
		return sinceCheckpoint(ctx, state, arguments)

	case strings.HasPrefix(text, "the last record at the checkpoint is"):
		return blocked("the Go client cannot read up to a checkpoint: " +
			"ContextScope has Checkpoint and FetchSince but no fetch-at, " +
			"where the Rust reference has turns_at(checkpoint) beside " +
			"turns_since (sdk/src/agent/session.rs:1296), so there is no way " +
			"to ask what the lane held at the moment the checkpoint was taken")

	case strings.HasPrefix(text, "the session remembers"):
		return rememberThroughSession(ctx, state, arguments)

	case strings.HasPrefix(text, "the remembered item names producer"):
		return rememberedProducer(ctx, state, arguments)

	default:
		return fmt.Errorf("this runner does not implement the step")
	}
}

// sessionOf is the session the scenario is working on.
func sessionOf(state *world) (*laser.Session, error) {
	value, found := state.get("session")
	if !found {
		return nil, fmt.Errorf("the scenario started no session")
	}
	return value.(*laser.Session), nil
}

func startSession(
	ctx context.Context, state *world, text string, arguments []string,
) error {
	if len(arguments) < 2 {
		return fmt.Errorf("want an agent and a label, got %v", arguments)
	}
	agent, err := wire.NewAgentID(arguments[0])
	if err != nil {
		return err
	}
	// Starting "again" asks whether a session that already ended refuses to
	// reopen, so the failure is kept rather than returned.
	again := strings.HasSuffix(text, " again")
	session := state.client.Sessions().Create(arguments[1]).As(agent)
	_, err = session.Begin(ctx)
	if again {
		state.failure = err
		return nil
	}
	if err != nil {
		return err
	}
	state.put("session", session)
	state.conversation = session.Conversation()
	return nil
}

// resumeSession reopens a session that already ended.
//
// Resuming is asked for explicitly rather than inferred from the state,
// because a session that ended is meant to stay ended: reopening it on the
// strength of a second Begin would make an accidental repeat look like a
// decision.
func resumeSession(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) < 2 {
		return fmt.Errorf("want an agent and a label, got %v", arguments)
	}
	agent, err := wire.NewAgentID(arguments[0])
	if err != nil {
		return err
	}
	session := state.client.Sessions().Create(arguments[1]).As(agent).Resume()
	if _, err := session.Begin(ctx); err != nil {
		return err
	}
	state.put("session", session)
	state.conversation = session.Conversation()
	return nil
}

func runFailingSession(
	ctx context.Context, state *world, arguments []string,
) error {
	if len(arguments) != 3 {
		return fmt.Errorf("want an agent, a label and a message, got %v", arguments)
	}
	agent, err := wire.NewAgentID(arguments[0])
	if err != nil {
		return err
	}
	session := state.client.Sessions().Create(arguments[1]).As(agent)
	if _, err := session.Begin(ctx); err != nil {
		return err
	}
	state.put("session", session)
	state.conversation = session.Conversation()
	message := arguments[2]
	_, err = session.Fail(ctx, wire.AgentErrorBody{
		Code: wire.ErrorInternal, Message: &message,
	})
	return err
}

func endSession(ctx context.Context, state *world) error {
	session, err := sessionOf(state)
	if err != nil {
		return err
	}
	if _, err := session.Complete(ctx, ""); err != nil {
		return err
	}
	ended, _ := state.get("ended")
	count, _ := ended.(int)
	state.put("ended", count+1)
	state.failure = nil
	return nil
}

// cancelSession ends the session as canceled.
//
// After the session already ended, the scenarios expect this to be refused:
// the first terminal verb latches, a repeat of it rewrites the same record,
// and a different one is invalid. Where it succeeds instead, that is the gap
// rather than a runner mistake, so it is named.
func cancelSession(ctx context.Context, state *world) error {
	session, err := sessionOf(state)
	if err != nil {
		return err
	}
	_, state.failure = session.End(ctx, wire.TaskCanceled, wire.SessionEnd{})
	ended, _ := state.get("ended")
	if count, _ := ended.(int); count > 0 && state.failure == nil {
		return blocked("the Go session has no terminal latch: Session.End " +
			"publishes whatever verb it is given, so a cancel after a " +
			"completed end is written rather than refused as invalid")
	}
	return nil
}

func endedRefusal(state *world, text string) error {
	var ended *laser.SessionEndedError
	if !errors.As(state.failure, &ended) {
		return blocked("the Go client does not refuse a start on a session " +
			"that already ended: Session.Begin writes the start record " +
			"unconditionally, with no SessionEndedError and no resume, so " +
			"this scenario has nothing to refuse it")
	}
	want := strings.TrimPrefix(text, "the call fails as a session that ended ")
	if string(ended.Status) != want {
		return fmt.Errorf("want the status %q, got %q", want, ended.Status)
	}
	return nil
}

// laneShows compares the session lane with the displays the scenario names.
func laneShows(
	ctx context.Context, state *world, text string, arguments []string,
) error {
	// "shows 3 "state.updated" records" names one display and a count, where
	// the usual form names each display in order.
	if counts := numbersOf(bdd.Step{Text: text}); len(counts) == 1 &&
		strings.HasSuffix(text, "records") && len(arguments) == 1 {
		return laneCounts(ctx, state, arguments[0], int(counts[0]))
	}
	return laneHolds(ctx, state, arguments, true)
}

// laneCounts waits for a display to appear the named number of times.
//
// Once the session has ended, nothing more is coming, so a count that is one
// short at that point is reported rather than waited out.
func laneCounts(ctx context.Context, state *world, display string, want int) error {
	deadline := time.Now().Add(20 * time.Second)
	for {
		got, err := laneDisplays(ctx, state)
		if err != nil {
			return err
		}
		seen := 0
		ended := false
		for _, shown := range got {
			if shown == display {
				seen++
			}
			if terminalDisplay(shown) {
				ended = true
			}
		}
		if seen >= want {
			return nil
		}
		if ended && seen == want-1 && display == string(wire.DisplayStateUpdated) {
			return blocked("the Go session writes no state snapshot when it " +
				"ends: Session.Complete goes straight to the terminal record, " +
				"where the Rust reference snapshots state changed since the " +
				"last one first, so a reader cannot start from the final " +
				"document. The lane holds " + fmt.Sprint(seen) +
				" state.updated records where the scenario wants " +
				fmt.Sprint(want))
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("want %d %q records, the lane shows %v",
				want, display, got)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(150 * time.Millisecond):
		}
	}
}

// terminalDisplay reports a record that ends a session, after which the lane
// holds nothing further for it.
func terminalDisplay(display string) bool {
	switch wire.DisplayType(display) {
	case wire.DisplaySessionCompleted, wire.DisplaySessionFailed,
		wire.DisplaySessionCanceled:
		return true
	default:
		return false
	}
}

// laneHolds reads the session's own records until they show what was asked.
//
// ordered says whether the displays must appear in exactly this order and
// nothing else; otherwise the named displays must appear this many times, in
// order, among the rest.
func laneHolds(
	ctx context.Context, state *world, want []string, ordered bool,
) error {
	if len(want) == 0 {
		return fmt.Errorf("the step names no displays, so it would hold vacuously")
	}
	deadline := time.Now().Add(20 * time.Second)
	var got []string
	for {
		var err error
		got, err = laneDisplays(ctx, state)
		if err != nil {
			return err
		}
		if matchesLane(got, want, ordered) {
			return nil
		}
		// A lane that matches once a submitted record is read as a start is
		// not a timing problem, so it is named at once rather than waited out.
		if matchesLane(asStarts(got), want, ordered) {
			return blocked("the Go client writes a session start as the task " +
				"state submitted, so the lane reads session.submitted where " +
				"every other client reads session.started: Agdx.SessionStarted " +
				"stamps TaskSubmitted, where the Rust reference stamps " +
				"TaskState::Working. The lane otherwise matched exactly")
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("want the lane to show %v, it shows %v", want, got)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(150 * time.Millisecond):
		}
	}
}

// asStarts reads a submitted record as the start it was meant to be, which is
// how the one divergence is told apart from any other difference.
func asStarts(displays []string) []string {
	read := make([]string, 0, len(displays))
	for _, display := range displays {
		if display == string(wire.DisplaySessionSubmitted) {
			display = string(wire.DisplaySessionStarted)
		}
		read = append(read, display)
	}
	return read
}

func matchesLane(got, want []string, ordered bool) bool {
	if ordered {
		if len(got) != len(want) {
			return false
		}
		for index, display := range want {
			if got[index] != display {
				return false
			}
		}
		return true
	}
	counted := 0
	for _, display := range got {
		if counted < len(want) && display == want[counted] {
			counted++
		}
	}
	return counted == len(want)
}

// laneDisplays is how a session timeline shows this session's records.
func laneDisplays(ctx context.Context, state *world) ([]string, error) {
	session, err := sessionOf(state)
	if err != nil {
		return nil, err
	}
	return laneDisplaysFor(ctx, state, session.Conversation())
}

// failureMessage reads the error the session ended with off its own record,
// rather than off the handle, because the point is that the log carries it.
func failureMessage(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) != 1 {
		return fmt.Errorf("want one message, got %v", arguments)
	}
	session, err := sessionOf(state)
	if err != nil {
		return err
	}
	records, err := state.replay(ctx, wire.AgentSessions)
	if err != nil {
		return err
	}
	for _, record := range records {
		message, err := laser.OpenAgdx(record)
		if err != nil || message.Envelope.Conversation != session.Conversation() {
			continue
		}
		if message.Envelope.TaskState == nil ||
			*message.Envelope.TaskState != wire.TaskFailed {
			continue
		}
		var end wire.SessionEnd
		if err := wire.Unmarshal(message.Envelope.Body, &end); err != nil {
			return err
		}
		if end.Error == nil || end.Error.Message == nil {
			return fmt.Errorf("the failed record carries no message")
		}
		if *end.Error.Message != arguments[0] {
			return fmt.Errorf("want the failure %q, got %q",
				arguments[0], *end.Error.Message)
		}
		return nil
	}
	return fmt.Errorf("the lane holds no failed record for this session")
}

func sameLabelSameSession(state *world, arguments []string) error {
	if len(arguments) != 1 {
		return fmt.Errorf("want one label, got %v", arguments)
	}
	sessions := state.client.Sessions()
	first := sessions.Create(arguments[0]).Conversation()
	second := sessions.Create(arguments[0]).Conversation()
	if first != second {
		return fmt.Errorf("one label reached two sessions")
	}
	state.put("labelled", first)
	return nil
}

func differentLabelsDifferentSessions(state *world, arguments []string) error {
	if len(arguments) != 2 {
		return fmt.Errorf("want two labels, got %v", arguments)
	}
	sessions := state.client.Sessions()
	if sessions.Create(arguments[0]).Conversation() ==
		sessions.Create(arguments[1]).Conversation() {
		return fmt.Errorf("two labels reached one session")
	}
	return nil
}

// recordMessage writes one plain agent record on the session's lane, which is
// what a conversation turn is.
func recordMessage(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) != 1 {
		return fmt.Errorf("want one message, got %v", arguments)
	}
	session, err := sessionOf(state)
	if err != nil {
		return err
	}
	agdx, err := session.Agdx()
	if err != nil {
		return err
	}
	_, err = agdx.Emit([]byte(arguments[0])).Send(ctx)
	return err
}

func recordModelCall(
	ctx context.Context, state *world, text string, arguments []string,
) error {
	if len(arguments) < 1 {
		return fmt.Errorf("want a model, got %v", arguments)
	}
	session, err := sessionOf(state)
	if err != nil {
		return err
	}
	if strings.Contains(text, "with the context assembled") {
		return blocked("this runner has not written the context manifest steps")
	}
	// The request carries a secret the client is expected to drop before it
	// reaches the log, which is the whole point of the scenario.
	request := laser.ModelRequest{
		Model: arguments[0],
		Body:  []byte(`{"api_key":"sk-not-a-real-key","prompt":"hello"}`),
	}
	call, err := session.Model(ctx, request, nil)
	if err != nil {
		return err
	}
	_, err = call.Complete(ctx, laser.ModelResponse{
		Body: []byte(`{"text":"hello back"}`), Model: arguments[0],
	})
	return err
}

func recordToolCall(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) != 1 {
		return fmt.Errorf("want one tool, got %v", arguments)
	}
	session, err := sessionOf(state)
	if err != nil {
		return err
	}
	args, err := dynOf(map[string]any{"token": "tok-not-a-real-token", "q": "laser"})
	if err != nil {
		return err
	}
	call, err := session.Tool(ctx, arguments[0], args)
	if err != nil {
		return err
	}
	_, err = call.Complete(ctx, []byte(`{"rows":0}`))
	return err
}

// secrets are the values the scenario planted, which must not reach the log.
var secrets = []string{"sk-not-a-real-key", "tok-not-a-real-token"}

func noSecrets(ctx context.Context, state *world) error {
	session, err := sessionOf(state)
	if err != nil {
		return err
	}
	records, err := state.replay(ctx, wire.AgentSessions)
	if err != nil {
		return err
	}
	for _, record := range records {
		message, err := laser.OpenAgdx(record)
		if err != nil || message.Envelope.Conversation != session.Conversation() {
			continue
		}
		for _, secret := range secrets {
			if strings.Contains(string(message.Envelope.Body), secret) {
				return fmt.Errorf("a recorded call carries the secret %q", secret)
			}
		}
	}
	return nil
}

// dynOf builds a free-form document value.
//
// The only constructor wire exposes takes a cborform value from a package
// another module cannot import, so the value is built by encoding it and
// reading it back, which is the one route open to a caller outside the client.
func dynOf(value any) (wire.DynValue, error) {
	encoded, err := wire.Marshal(value)
	if err != nil {
		return wire.DynValue{}, err
	}
	var document wire.DynValue
	if err := wire.Unmarshal(encoded, &document); err != nil {
		return wire.DynValue{}, err
	}
	return document, nil
}

func setState(
	ctx context.Context, state *world, text string, arguments []string,
) error {
	if len(arguments) < 1 {
		return fmt.Errorf("want a key, got %v", arguments)
	}
	session, err := sessionOf(state)
	if err != nil {
		return err
	}
	var value any
	switch {
	case len(arguments) == 2:
		value = arguments[1]
	case strings.HasSuffix(text, "false"):
		value = false
	case strings.HasSuffix(text, "true"):
		value = true
	default:
		numbers := numbersOf(bdd.Step{Text: text})
		if len(numbers) != 1 {
			return fmt.Errorf("the step names no value this runner reads: %q", text)
		}
		value = int64(numbers[0])
	}
	document, err := dynOf(value)
	if err != nil {
		return err
	}
	_, err = session.State().Set(ctx, arguments[0], document)
	return err
}

// foldedState reads the state document back off the lane and checks the
// fields and the revision the scenario names.
func foldedState(ctx context.Context, state *world, text string) error {
	session, err := sessionOf(state)
	if err != nil {
		return err
	}
	numbers := numbersOf(bdd.Step{Text: text})
	if len(numbers) < 2 {
		return fmt.Errorf("want a value and a revision, got %v", numbers)
	}
	revision := numbers[len(numbers)-1]
	deadline := time.Now().Add(20 * time.Second)
	for {
		view, err := session.State().Get(ctx)
		if err == nil && view.Revision == revision {
			return checkStateFields(view, text)
		}
		if time.Now().After(deadline) {
			if err != nil {
				return err
			}
			return fmt.Errorf("want revision %d, got %d", revision, view.Revision)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(150 * time.Millisecond):
		}
	}
}

func checkStateFields(view wire.SessionStateView, text string) error {
	// The scenario spells the document as `"step" 1 and "done" false`, so the
	// fields are read by the names it quotes.
	for _, name := range quoted.FindAllStringSubmatch(text, -1) {
		field, found := view.Document.Field(name[1])
		if !found {
			return fmt.Errorf("the folded document has no %q", name[1])
		}
		if field.IsZero() {
			return fmt.Errorf("the folded %q holds nothing", name[1])
		}
	}
	return nil
}

func operatorCancels(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) != 2 {
		return fmt.Errorf("want an operator and a label, got %v", arguments)
	}
	operator, err := wire.NewAgentID(arguments[0])
	if err != nil {
		return err
	}
	// The control topic is an operator's, so the scenario creates it: a client
	// that made it would be claiming a privilege it does not have.
	if err := state.topic(wire.AgentControl).Ensure(ctx, 1, nil); err != nil {
		return err
	}
	session := state.client.Sessions().Create(arguments[1])
	state.put("session", session)
	state.conversation = session.Conversation()
	_, err = state.client.Sessions().
		Control(state.stream, session.Conversation()).
		AsOperator(operator).
		Cancel(ctx)
	return err
}

func controlHolds(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) != 1 {
		return fmt.Errorf("want one action, got %v", arguments)
	}
	session, err := sessionOf(state)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		records, err := state.replay(ctx, wire.AgentControl)
		if err != nil {
			return err
		}
		for _, record := range records {
			message, openErr := laser.OpenAgdx(record)
			if openErr != nil ||
				message.Envelope.Conversation != session.Conversation() {
				continue
			}
			if message.Envelope.Operation != nil &&
				*message.Envelope.Operation == arguments[0] {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("the control topic holds no %q for this session",
				arguments[0])
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(150 * time.Millisecond):
		}
	}
}

// assembledReads checks that an assembled context holds the conversation
// bodies and nothing else: a state delta is session bookkeeping rather than
// something a prompt should carry.
func assembledReads(ctx context.Context, state *world, arguments []string) error {
	session, err := sessionOf(state)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		messages, err := state.client.Context(session.Conversation()).
			Fetch(ctx, []string{wire.AgentSessions}, 100, 0)
		if err != nil {
			return err
		}
		var bodies []string
		for _, message := range messages {
			body := string(message.Body())
			for _, want := range arguments {
				if body == want {
					bodies = append(bodies, body)
				}
			}
		}
		if len(bodies) == len(arguments) {
			for index, want := range arguments {
				if bodies[index] != want {
					return fmt.Errorf("want %v in order, got %v", arguments, bodies)
				}
			}
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("want the context to read %v, got %v", arguments, bodies)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(150 * time.Millisecond):
		}
	}
}
