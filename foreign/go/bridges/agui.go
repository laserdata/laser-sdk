package bridges

import (
	"encoding/json"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// AgUiEventKind is the event name an AG-UI consumer switches on.
type AgUiEventKind string

const (
	RunStarted              AgUiEventKind = "RUN_STARTED"
	RunFinished             AgUiEventKind = "RUN_FINISHED"
	RunError                AgUiEventKind = "RUN_ERROR"
	TextMessageStart        AgUiEventKind = "TEXT_MESSAGE_START"
	TextMessageContent      AgUiEventKind = "TEXT_MESSAGE_CONTENT"
	TextMessageEnd          AgUiEventKind = "TEXT_MESSAGE_END"
	ReasoningMessageStart   AgUiEventKind = "REASONING_MESSAGE_START"
	ReasoningMessageContent AgUiEventKind = "REASONING_MESSAGE_CONTENT"
	ReasoningMessageEnd     AgUiEventKind = "REASONING_MESSAGE_END"
	ToolCallStart           AgUiEventKind = "TOOL_CALL_START"
	ToolCallArgs            AgUiEventKind = "TOOL_CALL_ARGS"
	ToolCallEnd             AgUiEventKind = "TOOL_CALL_END"
	ToolCallResult          AgUiEventKind = "TOOL_CALL_RESULT"
	StateSnapshot           AgUiEventKind = "STATE_SNAPSHOT"
	StateDelta              AgUiEventKind = "STATE_DELTA"
)

// AgUiEvent is one event the front end receives.
//
// It is one struct with optional fields rather than a union, because the shape
// a consumer reads is JSON where absent fields are simply missing, and a
// union would add a Go-only distinction the protocol does not have.
type AgUiEvent struct {
	Type       AgUiEventKind   `json:"type"`
	ThreadID   string          `json:"threadId,omitempty"`
	RunID      string          `json:"runId,omitempty"`
	MessageID  string          `json:"messageId,omitempty"`
	Role       string          `json:"role,omitempty"`
	Delta      string          `json:"delta,omitempty"`
	ToolCallID string          `json:"toolCallId,omitempty"`
	ToolName   string          `json:"toolCallName,omitempty"`
	Content    string          `json:"content,omitempty"`
	Message    string          `json:"message,omitempty"`
	Snapshot   json.RawMessage `json:"snapshot,omitempty"`
	// Patch is a state delta. The protocol spells both this and a text
	// fragment "delta", and they never ride one event, so MarshalJSON writes
	// whichever is set rather than the struct carrying one ambiguous field.
	Patch json.RawMessage `json:"-"`
}

// MarshalJSON writes the event the way the protocol spells it.
//
// A text fragment and a state patch share the name "delta" in AG-UI. Keeping
// them as separate Go fields is what lets each be typed, and this is where the
// two are put back onto one name.
func (event AgUiEvent) MarshalJSON() ([]byte, error) {
	type plain AgUiEvent
	encoded, err := json.Marshal(plain(event))
	if err != nil {
		return nil, err
	}
	if event.Patch == nil {
		return encoded, nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		return nil, err
	}
	fields["delta"] = event.Patch
	return json.Marshal(fields)
}

// UnmarshalJSON reads an event back, routing "delta" to the field its shape
// means.
//
// The two are told apart by what the value is rather than by the event name: a
// text fragment is a JSON string and a patch is not, so a reader does not have
// to trust the name to route the value correctly.
func (event *AgUiEvent) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	delta, carried := fields["delta"]
	if carried && !isJSONString(delta) {
		// The plain decode would refuse a non-string here, so the patch is
		// lifted out before it runs and put back afterwards.
		delete(fields, "delta")
		rest, err := json.Marshal(fields)
		if err != nil {
			return err
		}
		data = rest
	}

	type plain AgUiEvent
	var read plain
	if err := json.Unmarshal(data, &read); err != nil {
		return err
	}
	*event = AgUiEvent(read)
	if carried && !isJSONString(delta) {
		event.Patch = delta
	}
	return nil
}

// isJSONString reports whether a raw value is a JSON string, which is what
// separates a text fragment from a state patch under one name.
func isJSONString(raw json.RawMessage) bool {
	for _, character := range raw {
		switch character {
		case ' ', '\t', '\n', '\r':
			continue
		case '"':
			return true
		default:
			return false
		}
	}
	return false
}

// ChunkKind is what a stream of chunks carries. It is declared on the opening
// chunk, so a reader tracks it per channel rather than re-reading it from
// every chunk.
type ChunkKind int

const (
	ChunkChat ChunkKind = iota
	ChunkReasoning
	ChunkToolArgs
)

// ChunkKindOf reads the purpose an opening chunk declares.
func ChunkKindOf(envelope *wire.AgentEnvelope) ChunkKind {
	if envelope.Operation == nil {
		return ChunkChat
	}
	switch *envelope.Operation {
	case wire.OperationReasoning:
		return ChunkReasoning
	case wire.OperationToolArgs:
		return ChunkToolArgs
	default:
		return ChunkChat
	}
}

// ThreadAndRun is how AG-UI addresses an exchange: the conversation is the
// thread and the correlation is the run.
//
// A record with no correlation falls back to its own id, and then to the
// thread, so an event always names a run rather than an empty string a front
// end would group everything under.
func ThreadAndRun(envelope *wire.AgentEnvelope) (string, string) {
	thread := envelope.Conversation.String()
	switch {
	case envelope.Correlation != nil:
		return thread, envelope.Correlation.String()
	case envelope.Record != nil:
		return thread, envelope.Record.String()
	default:
		return thread, thread
	}
}

// EventsFor renders one envelope as the AG-UI events it represents.
//
// A chunk needs the kind its channel was opened with, which only the caller
// threading the stream knows, so it is passed in rather than guessed per
// chunk.
func EventsFor(envelope *wire.AgentEnvelope, chunk ChunkKind) []AgUiEvent {
	if envelope.Kind == wire.KindChunk {
		return chunkEvents(envelope, chunk)
	}
	thread, run := ThreadAndRun(envelope)

	switch {
	case envelope.Kind == wire.KindStatus && operationIs(envelope, wire.OperationTask):
		return taskEvents(envelope, thread, run)

	// A response or failure naming a tool is that tool's result, which is a
	// different event from an ordinary answer.
	case (envelope.Kind == wire.KindResponse || envelope.Kind == wire.KindError) &&
		envelope.Tool != nil:
		messageID := run
		if envelope.Record != nil {
			messageID = envelope.Record.String()
		}
		return []AgUiEvent{{
			Type:       ToolCallResult,
			MessageID:  messageID,
			ToolCallID: run,
			Content:    string(envelope.Body),
		}}

	case envelope.Kind == wire.KindError:
		return []AgUiEvent{{Type: RunError, Message: string(envelope.Body)}}

	case envelope.Kind == wire.KindEvent:
		return stateEvents(envelope)
	}
	return nil
}

// taskEvents renders a task lifecycle update: submitted opens the run and a
// terminal state closes it.
func taskEvents(envelope *wire.AgentEnvelope, thread, run string) []AgUiEvent {
	if envelope.TaskState == nil {
		return nil
	}
	state := *envelope.TaskState
	switch {
	case state == wire.TaskSubmitted:
		return []AgUiEvent{{Type: RunStarted, ThreadID: thread, RunID: run}}

	// A failure closes the run as an error rather than a finish, so a front
	// end shows what went wrong instead of a run that simply stopped.
	case state == wire.TaskFailed || state == wire.TaskRejected:
		message := "task failed"
		// Only a textual detail is shown. A number or a list would render as
		// Go's spelling of it, which is not a message anyone wrote.
		if detail, found := envelope.Metadata["detail"]; found &&
			detail.Kind == wire.ValueStr && detail.Str != "" {
			message = detail.Str
		}
		return []AgUiEvent{{Type: RunError, Message: message}}

	case state.IsTerminal():
		return []AgUiEvent{{Type: RunFinished, ThreadID: thread, RunID: run}}
	}
	// A non-terminal state is progress the front end already shows through the
	// message stream, so it emits nothing of its own.
	return nil
}

// stateEvents renders the session state documents a front end mirrors.
func stateEvents(envelope *wire.AgentEnvelope) []AgUiEvent {
	if envelope.Operation == nil {
		return nil
	}
	switch *envelope.Operation {
	case wire.OperationStateSnapshot:
		var snapshot wire.StateSnapshot
		if err := wire.Unmarshal(envelope.Body, &snapshot); err != nil {
			// A state record that does not decode is dropped rather than sent
			// half-read: a front end mirroring a broken snapshot would show a
			// state the session never had.
			return nil
		}
		document, err := json.Marshal(snapshot.Document)
		if err != nil {
			return nil
		}
		return []AgUiEvent{{Type: StateSnapshot, Snapshot: document}}

	case wire.OperationStateDelta:
		var delta wire.StateDelta
		if err := wire.Unmarshal(envelope.Body, &delta); err != nil {
			return nil
		}
		patch, err := json.Marshal(delta.Patch)
		if err != nil {
			return nil
		}
		return []AgUiEvent{{Type: StateDelta, Patch: patch}}
	}
	return nil
}

// chunkEvents renders one chunk as its message family.
//
// The opening chunk starts the message and the terminal one ends it, so a
// front end opens and closes exactly one message per channel however many
// chunks arrive between.
func chunkEvents(envelope *wire.AgentEnvelope, kind ChunkKind) []AgUiEvent {
	id := ""
	if envelope.Channel != nil {
		id = envelope.Channel.String()
	}
	opening := envelope.Sequence != nil && *envelope.Sequence == 0
	body := string(envelope.Body)

	var start, content, end AgUiEventKind
	switch kind {
	case ChunkReasoning:
		start, content, end = ReasoningMessageStart, ReasoningMessageContent, ReasoningMessageEnd
	case ChunkToolArgs:
		start, content, end = ToolCallStart, ToolCallArgs, ToolCallEnd
	default:
		start, content, end = TextMessageStart, TextMessageContent, TextMessageEnd
	}

	var events []AgUiEvent
	if opening {
		opened := AgUiEvent{Type: start, MessageID: id}
		if kind == ChunkToolArgs {
			opened.MessageID = ""
			opened.ToolCallID = id
			if envelope.Tool != nil {
				opened.ToolName = *envelope.Tool
			}
		} else {
			opened.Role = "assistant"
		}
		events = append(events, opened)
	}
	if body != "" {
		piece := AgUiEvent{Type: content, Delta: body}
		if kind == ChunkToolArgs {
			piece.ToolCallID = id
		} else {
			piece.MessageID = id
		}
		events = append(events, piece)
	}
	if envelope.Last {
		closed := AgUiEvent{Type: end}
		if kind == ChunkToolArgs {
			closed.ToolCallID = id
		} else {
			closed.MessageID = id
		}
		events = append(events, closed)
	}
	return events
}

func operationIs(envelope *wire.AgentEnvelope, operation string) bool {
	return envelope.Operation != nil && *envelope.Operation == operation
}

// AgUiStream threads a sequence of envelopes into AG-UI events.
//
// A chunk's kind is declared only on the opening chunk, so the stream
// remembers it per channel: a later chunk of a known channel reuses it rather
// than being rendered as chat by default.
type AgUiStream struct {
	channels map[string]ChunkKind
}

// NewAgUiStream opens a stream with no channels seen yet.
func NewAgUiStream() *AgUiStream {
	return &AgUiStream{channels: map[string]ChunkKind{}}
}

// Push renders one envelope, remembering what its channel carries.
func (stream *AgUiStream) Push(envelope *wire.AgentEnvelope) []AgUiEvent {
	if envelope.Kind != wire.KindChunk {
		return EventsFor(envelope, ChunkChat)
	}
	id := ""
	if envelope.Channel != nil {
		id = envelope.Channel.String()
	}
	kind := ChunkChat
	if envelope.Sequence != nil && *envelope.Sequence == 0 {
		kind = ChunkKindOf(envelope)
		stream.channels[id] = kind
	} else if known, seen := stream.channels[id]; seen {
		kind = known
	}
	events := EventsFor(envelope, kind)
	if envelope.Last {
		// The channel is finished, so it is forgotten: a long-running stream
		// must not grow a map entry per channel forever.
		delete(stream.channels, id)
	}
	return events
}

// Tracked is how many open channels the stream is following.
func (stream *AgUiStream) Tracked() int { return len(stream.channels) }
