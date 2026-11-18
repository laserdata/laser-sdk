package bridges_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/bridges"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func conversation(marker byte) wire.ConversationID {
	return wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: marker})}
}

func correlation(marker byte) wire.CorrelationID {
	return wire.CorrelationID{ID: wire.IDFromBytes([16]byte{15: marker})}
}

func record(marker byte) wire.RecordID {
	return wire.RecordID{ID: wire.IDFromBytes([16]byte{15: marker})}
}

func agent(t *testing.T, name string) wire.AgentID {
	t.Helper()
	id, err := wire.NewAgentID(name)
	if err != nil {
		t.Fatalf("agent id: %v", err)
	}
	return id
}

func textPointer(value string) *string                  { return &value }
func statePointer(value wire.TaskState) *wire.TaskState { return &value }
func uint64Pointer(value uint64) *uint64                { return &value }

// Every contract state has an A2A spelling, and the pairing must survive a
// round trip or a bridge reports one state as another.
func TestGivenEveryTaskStateWhenMappedThenShouldRoundTripThroughA2A(t *testing.T) {
	for _, state := range []wire.TaskState{
		wire.TaskSubmitted, wire.TaskWorking, wire.TaskInputRequired,
		wire.TaskCompleted, wire.TaskCanceled, wire.TaskFailed,
		wire.TaskRejected, wire.TaskAuthRequired, wire.TaskUnknown,
	} {
		word := bridges.A2AState(state)
		back, err := bridges.TaskStateFromA2A(word)
		if err != nil {
			t.Fatalf("%v: %v", state, err)
		}
		if back != state {
			t.Errorf("%v mapped to %q and back to %v", state, word, back)
		}
	}
}

// A paused task has not stopped, and A2A has no paused state, so reporting
// anything terminal would tell a caller the work ended when it has not.
func TestGivenAPausedTaskWhenMappedThenShouldReadAsWorking(t *testing.T) {
	if got := bridges.A2AState(wire.TaskPaused); got != "TASK_STATE_WORKING" {
		t.Errorf("want a paused task to read as working, got %q", got)
	}
}

// A bridge that invented a state would report something the deployment never
// said.
func TestGivenAnUnrecognisedStateWhenMappedThenShouldReadAsUnspecified(t *testing.T) {
	if got := bridges.A2AState(wire.TaskState(99)); got != "TASK_STATE_UNSPECIFIED" {
		t.Errorf("want unspecified, got %q", got)
	}
}

// An edge sending a word outside the vocabulary is speaking a protocol this
// bridge does not implement, and guessing would act on work it never
// understood.
func TestGivenAnUnknownA2AWordWhenReadThenShouldRefuseIt(t *testing.T) {
	if _, err := bridges.TaskStateFromA2A("TASK_STATE_TELEPORTING"); err == nil {
		t.Error("an unknown A2A state must be refused")
	}
}

// An answer that arrived is either the work finishing or the work failing, and
// a bridge must say which.
func TestGivenAnAnswerWithNoStateWhenReadThenShouldInferItFromTheKind(t *testing.T) {
	answer := &wire.AgentEnvelope{Kind: wire.KindResponse}
	if bridges.AnswerState(answer) != wire.TaskCompleted {
		t.Error("a response with no state is the work finishing")
	}
	failure := &wire.AgentEnvelope{Kind: wire.KindError}
	if bridges.AnswerState(failure) != wire.TaskFailed {
		t.Error("a failure with no state is the work failing")
	}
	// An envelope that names its state is believed over the inference.
	named := &wire.AgentEnvelope{
		Kind: wire.KindResponse, TaskState: statePointer(wire.TaskCanceled),
	}
	if bridges.AnswerState(named) != wire.TaskCanceled {
		t.Error("a named state must be believed")
	}
}

// A caller ties an artifact back to the record that produced it, so the
// artifact is named by that record.
func TestGivenAnAnsweringEnvelopeWhenRenderedThenShouldCarryItsArtifact(t *testing.T) {
	answered := record(7)
	task := bridges.TaskFromEnvelope("task-1", "ctx-1", &wire.AgentEnvelope{
		Kind:   wire.KindResponse,
		Record: &answered,
		Body:   []byte("the answer"),
	})

	if task.ID != "task-1" || task.ContextID != "ctx-1" {
		t.Errorf("want the task addressed, got %+v", task)
	}
	if task.Status.State != "TASK_STATE_COMPLETED" {
		t.Errorf("want a completed task, got %q", task.Status.State)
	}
	if len(task.Artifacts) != 1 {
		t.Fatalf("want one artifact, got %d", len(task.Artifacts))
	}
	if task.Artifacts[0].ArtifactID != answered.String() {
		t.Errorf("want the artifact named by its record, got %q",
			task.Artifacts[0].ArtifactID)
	}
	if len(task.Artifacts[0].Parts) != 1 ||
		task.Artifacts[0].Parts[0].Text != "the answer" {
		t.Errorf("want the body as the part, got %+v", task.Artifacts[0].Parts)
	}
}

// An empty body is no artifact rather than an empty one, which would render as
// a blank result.
func TestGivenAnEmptyAnswerWhenRenderedThenShouldCarryNoArtifact(t *testing.T) {
	task := bridges.TaskFromEnvelope("task-1", "ctx-1",
		&wire.AgentEnvelope{Kind: wire.KindResponse})

	if len(task.Artifacts) != 0 {
		t.Errorf("want no artifact, got %+v", task.Artifacts)
	}
}

// A caller that groups several tasks under one context expects them to stay
// grouped, so a named context wins over the conversation.
func TestGivenParamsNamingAContextWhenReadThenShouldKeepThatGrouping(t *testing.T) {
	params := []byte(`{"message":{"contextId":"thread-9","parts":[]}}`)

	if got := bridges.ContextIDOf(params, conversation(1)); got != "thread-9" {
		t.Errorf("want the named context, got %q", got)
	}
	// With none named the task's own conversation is the context, so a task
	// still belongs somewhere.
	plain := []byte(`{"message":{"parts":[]}}`)
	if got := bridges.ContextIDOf(plain, conversation(1)); got != conversation(1).String() {
		t.Errorf("want the conversation as the context, got %q", got)
	}
	// Params that do not parse must not crash a bridge, and still group the
	// task under its conversation.
	if got := bridges.ContextIDOf([]byte("not json"), conversation(1)); got !=
		conversation(1).String() {
		t.Errorf("want a fallback context, got %q", got)
	}
}

// A caller that split a message across parts meant all of it.
func TestGivenAMessageInSeveralPartsWhenReadThenShouldJoinThem(t *testing.T) {
	params := []byte(`{"message":{"parts":[{"text":"hello "},{"text":"world"}]}}`)

	if got := bridges.TextFromParams(params); got != "hello world" {
		t.Errorf("want the parts joined, got %q", got)
	}
	if got := bridges.TextFromParams([]byte("not json")); got != "" {
		t.Errorf("want nothing from unparseable params, got %q", got)
	}
}

// An edge that cannot tell a failure from an answer will show an error to a
// user as though it were the result.
func TestGivenAFailureWhenRenderedAsAToolResultThenShouldBeMarkedAsOne(t *testing.T) {
	failed := bridges.ToolResultFromEnvelope(&wire.AgentEnvelope{
		Kind: wire.KindError, Body: []byte("the tool exploded"),
	})
	if !failed.IsError {
		t.Error("want the failure marked")
	}
	if len(failed.Content) != 1 || failed.Content[0].Text != "the tool exploded" {
		t.Errorf("want the body as content, got %+v", failed.Content)
	}

	answered := bridges.ToolResultFromEnvelope(&wire.AgentEnvelope{
		Kind: wire.KindResponse, Body: []byte("42"),
	})
	if answered.IsError {
		t.Error("an answer is not a failure")
	}
}

// A content block holding nothing renders as a blank message, so an empty
// body is no content at all.
func TestGivenAnEmptyToolAnswerWhenRenderedThenShouldCarryNoContent(t *testing.T) {
	result := bridges.ToolResultFromEnvelope(&wire.AgentEnvelope{Kind: wire.KindResponse})

	if len(result.Content) != 0 {
		t.Errorf("want no content, got %+v", result.Content)
	}
	// A successful result says so explicitly rather than leaving a reader to
	// infer it from an absent field.
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(encoded), `"isError":false`) {
		t.Errorf("want isError written explicitly, got %s", encoded)
	}
}

// AG-UI addresses an exchange by thread and run, and an event naming an empty
// run would group everything under one.
func TestGivenAnEnvelopeWhenAddressedThenShouldAlwaysNameARun(t *testing.T) {
	correlated := correlation(2)
	thread, run := bridges.ThreadAndRun(&wire.AgentEnvelope{
		Conversation: conversation(1), Correlation: &correlated,
	})
	if thread != conversation(1).String() || run != correlated.String() {
		t.Errorf("want the correlation as the run, got %q / %q", thread, run)
	}

	identified := record(3)
	_, run = bridges.ThreadAndRun(&wire.AgentEnvelope{
		Conversation: conversation(1), Record: &identified,
	})
	if run != identified.String() {
		t.Errorf("want the record as the run, got %q", run)
	}

	_, run = bridges.ThreadAndRun(&wire.AgentEnvelope{Conversation: conversation(1)})
	if run == "" {
		t.Error("an event must always name a run")
	}
}

// Submitted opens a run and a terminal state closes it, which is how a front
// end knows when to stop showing it as live.
func TestGivenATaskLifecycleWhenRenderedThenShouldOpenAndCloseTheRun(t *testing.T) {
	operation := wire.OperationTask
	correlated := correlation(2)
	build := func(state wire.TaskState) *wire.AgentEnvelope {
		return &wire.AgentEnvelope{
			Kind:         wire.KindStatus,
			Conversation: conversation(1),
			Correlation:  &correlated,
			Operation:    &operation,
			TaskState:    &state,
		}
	}

	opened := bridges.EventsFor(build(wire.TaskSubmitted), bridges.ChunkChat)
	if len(opened) != 1 || opened[0].Type != bridges.RunStarted {
		t.Errorf("want the run opened, got %+v", opened)
	}
	closed := bridges.EventsFor(build(wire.TaskCompleted), bridges.ChunkChat)
	if len(closed) != 1 || closed[0].Type != bridges.RunFinished {
		t.Errorf("want the run closed, got %+v", closed)
	}
	// Progress is already shown through the message stream, so it emits
	// nothing of its own.
	if got := bridges.EventsFor(build(wire.TaskWorking), bridges.ChunkChat); len(got) != 0 {
		t.Errorf("want nothing for progress, got %+v", got)
	}
}

// A front end must show what went wrong rather than a run that simply stopped.
func TestGivenAFailedTaskWhenRenderedThenShouldCloseAsAnError(t *testing.T) {
	operation := wire.OperationTask
	state := wire.TaskFailed
	envelope := &wire.AgentEnvelope{
		Kind:         wire.KindStatus,
		Conversation: conversation(1),
		Operation:    &operation,
		TaskState:    &state,
		Metadata:     map[string]wire.Value{"detail": wire.StrValue("the model refused")},
	}

	events := bridges.EventsFor(envelope, bridges.ChunkChat)
	if len(events) != 1 || events[0].Type != bridges.RunError {
		t.Fatalf("want an error, got %+v", events)
	}
	if events[0].Message != "the model refused" {
		t.Errorf("want the detail shown, got %q", events[0].Message)
	}
}

// A number or a list would render as Go's spelling of it, which is not a
// message anyone wrote.
func TestGivenANonTextualDetailWhenRenderedThenShouldFallBackToAPlainMessage(t *testing.T) {
	operation := wire.OperationTask
	state := wire.TaskFailed
	envelope := &wire.AgentEnvelope{
		Kind:         wire.KindStatus,
		Conversation: conversation(1),
		Operation:    &operation,
		TaskState:    &state,
		Metadata:     map[string]wire.Value{"detail": wire.IntValue(7)},
	}

	events := bridges.EventsFor(envelope, bridges.ChunkChat)
	if len(events) != 1 || events[0].Message != "task failed" {
		t.Errorf("want a plain message, got %+v", events)
	}
}

// A tool result is a different event from an ordinary answer, so a front end
// can attach it to the call it belongs to.
func TestGivenAToolAnswerWhenRenderedThenShouldBeAToolResult(t *testing.T) {
	correlated := correlation(2)
	answered := record(3)
	events := bridges.EventsFor(&wire.AgentEnvelope{
		Kind:         wire.KindResponse,
		Conversation: conversation(1),
		Correlation:  &correlated,
		Record:       &answered,
		Tool:         textPointer("search"),
		Body:         []byte("results"),
	}, bridges.ChunkChat)

	if len(events) != 1 || events[0].Type != bridges.ToolCallResult {
		t.Fatalf("want a tool result, got %+v", events)
	}
	if events[0].ToolCallID != correlated.String() {
		t.Errorf("want the call named, got %q", events[0].ToolCallID)
	}
	if events[0].Content != "results" {
		t.Errorf("want the body as content, got %q", events[0].Content)
	}
}

// A front end opens and closes exactly one message per channel however many
// chunks arrive between.
func TestGivenAChunkStreamWhenRenderedThenShouldOpenAndCloseOneMessage(t *testing.T) {
	channel := wire.ChannelID{ID: wire.IDFromBytes([16]byte{15: 5})}
	stream := bridges.NewAgUiStream()
	operation := wire.OperationChat

	chunk := func(sequence uint64, body string, last bool) *wire.AgentEnvelope {
		envelope := &wire.AgentEnvelope{
			Kind:         wire.KindChunk,
			Conversation: conversation(1),
			Channel:      &channel,
			Sequence:     uint64Pointer(sequence),
			Body:         []byte(body),
			Last:         last,
		}
		if sequence == 0 {
			envelope.Operation = &operation
		}
		return envelope
	}

	var kinds []bridges.AgUiEventKind
	for _, envelope := range []*wire.AgentEnvelope{
		chunk(0, "Hel", false), chunk(1, "lo", false), chunk(2, "", true),
	} {
		for _, event := range stream.Push(envelope) {
			kinds = append(kinds, event.Type)
		}
	}

	want := []bridges.AgUiEventKind{
		bridges.TextMessageStart, bridges.TextMessageContent,
		bridges.TextMessageContent, bridges.TextMessageEnd,
	}
	if len(kinds) != len(want) {
		t.Fatalf("want %v, got %v", want, kinds)
	}
	for index := range want {
		if kinds[index] != want[index] {
			t.Errorf("at %d want %s, got %s", index, want[index], kinds[index])
		}
	}
	// A long-running stream must not grow a map entry per finished channel.
	if stream.Tracked() != 0 {
		t.Errorf("want the finished channel forgotten, got %d", stream.Tracked())
	}
}

// The purpose is declared only on the opening chunk, so a later chunk of a
// known channel must reuse it rather than falling back to chat.
func TestGivenAReasoningStreamWhenContinuedThenShouldRememberItsKind(t *testing.T) {
	channel := wire.ChannelID{ID: wire.IDFromBytes([16]byte{15: 6})}
	stream := bridges.NewAgUiStream()
	operation := wire.OperationReasoning

	opening := &wire.AgentEnvelope{
		Kind: wire.KindChunk, Conversation: conversation(1), Channel: &channel,
		Sequence: uint64Pointer(0), Operation: &operation, Body: []byte("thinking"),
	}
	continuing := &wire.AgentEnvelope{
		Kind: wire.KindChunk, Conversation: conversation(1), Channel: &channel,
		Sequence: uint64Pointer(1), Body: []byte(" more"),
	}

	stream.Push(opening)
	events := stream.Push(continuing)

	if len(events) != 1 || events[0].Type != bridges.ReasoningMessageContent {
		t.Errorf("want the channel's kind remembered, got %+v", events)
	}
}

// A tool-argument stream names the tool it is building a call for.
func TestGivenAToolArgsStreamWhenOpenedThenShouldNameItsTool(t *testing.T) {
	channel := wire.ChannelID{ID: wire.IDFromBytes([16]byte{15: 7})}
	operation := wire.OperationToolArgs
	events := bridges.NewAgUiStream().Push(&wire.AgentEnvelope{
		Kind: wire.KindChunk, Conversation: conversation(1), Channel: &channel,
		Sequence: uint64Pointer(0), Operation: &operation,
		Tool: textPointer("search"), Body: []byte(`{"q":`),
	})

	if len(events) != 2 {
		t.Fatalf("want a start and a fragment, got %+v", events)
	}
	if events[0].Type != bridges.ToolCallStart || events[0].ToolName != "search" {
		t.Errorf("want the tool named, got %+v", events[0])
	}
	if events[1].Type != bridges.ToolCallArgs || events[1].ToolCallID != channel.String() {
		t.Errorf("want the fragment on its call, got %+v", events[1])
	}
}

// The protocol spells a text fragment and a state patch alike, so each must
// reach the wire under that one name.
func TestGivenEitherDeltaWhenEncodedThenShouldUseTheProtocolName(t *testing.T) {
	text, err := json.Marshal(bridges.AgUiEvent{
		Type: bridges.TextMessageContent, MessageID: "m-1", Delta: "hello",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(text), `"delta":"hello"`) {
		t.Errorf("want the text fragment under delta, got %s", text)
	}

	patch, err := json.Marshal(bridges.AgUiEvent{
		Type: bridges.StateDelta, Patch: json.RawMessage(`[{"op":"add"}]`),
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(patch), `"delta":[{"op":"add"}]`) {
		t.Errorf("want the patch under delta, got %s", patch)
	}

	// And read back to the field its kind means.
	var back bridges.AgUiEvent
	if err := json.Unmarshal(patch, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if string(back.Patch) != `[{"op":"add"}]` {
		t.Errorf("want the patch read back, got %q", back.Patch)
	}
	if back.Delta != "" {
		t.Errorf("a state delta carries no text fragment, got %q", back.Delta)
	}
}

// A front end mirroring a broken snapshot would show a state the session never
// had.
func TestGivenAnUndecodableStateRecordWhenRenderedThenShouldEmitNothing(t *testing.T) {
	operation := wire.OperationStateSnapshot
	events := bridges.EventsFor(&wire.AgentEnvelope{
		Kind:         wire.KindEvent,
		Conversation: conversation(1),
		Operation:    &operation,
		Body:         []byte("not a snapshot"),
	}, bridges.ChunkChat)

	if len(events) != 0 {
		t.Errorf("want nothing emitted, got %+v", events)
	}
}
