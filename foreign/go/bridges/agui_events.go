package bridges

import (
	"context"
	"math"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// AgUiEvents reads conversation's whole retained history on topic and frames
// it into the AG-UI events a client verifier accepts: chat and reasoning chunk
// streams become TEXT_MESSAGE_*/REASONING_MESSAGE_* runs, a task lifecycle or
// a chunk stream with nothing wrapping it still opens and closes its own run,
// state events become STATE_SNAPSHOT/STATE_DELTA, and an error terminal
// becomes RUN_ERROR.
//
// EventsFor and AgUiStream render one envelope at a time and know nothing of
// runs; this is the one reader that threads a conversation's records through
// them and adds the run framing. Log-native and replayable, over Apache Iggy
// rather than SSE.
func AgUiEvents(
	ctx context.Context, client *laser.Laser,
	conversation wire.ConversationID, topic string,
) ([]AgUiEvent, error) {
	messages, err := client.Context(conversation).
		FetchWith(ctx, []string{topic}, laser.LastN(math.MaxInt))
	if err != nil {
		return nil, err
	}
	channels := map[string]ChunkKind{}
	framer := &runFramer{}
	for _, message := range messages {
		envelope := message.Envelope
		if envelope == nil {
			continue
		}
		thread, run := ThreadAndRun(envelope)
		var rendered []AgUiEvent
		if envelope.Kind == wire.KindChunk {
			id := ""
			if envelope.Channel != nil {
				id = envelope.Channel.String()
			}
			kind := ChunkChat
			if envelope.Sequence != nil && *envelope.Sequence == 0 {
				kind = ChunkKindOf(envelope)
				channels[id] = kind
			} else if known, seen := channels[id]; seen {
				kind = known
			}
			rendered = chunkEvents(envelope, kind)
		} else {
			rendered = EventsFor(envelope, ChunkChat)
		}
		for _, event := range rendered {
			framer.push(event, thread, run)
		}
	}
	return framer.finish(), nil
}

// runFramer places rendered events into AG-UI runs: an event outside a run
// opens one, a span whose opening fell outside the read opens before its
// content, and a run closes its open spans, in reverse, before it finishes.
type runFramer struct {
	events []AgUiEvent
	run    *openRun
}

type openRun struct {
	thread string
	run    string
	spans  []openSpan
}

type openSpan struct {
	kind spanKind
	id   string
}

// spanKind is the message family a start/content/end triple belongs to.
type spanKind int

const (
	spanText spanKind = iota
	spanReasoning
	spanToolCall
)

// spanStep is where one event sits within its span.
type spanStep int

const (
	spanOpen spanStep = iota
	spanContent
	spanClose
)

func (framer *runFramer) push(event AgUiEvent, thread, run string) {
	switch event.Type {
	case RunStarted:
		framer.close()
		framer.run = &openRun{thread: event.ThreadID, run: event.RunID}
		framer.events = append(framer.events, event)
		return
	case RunFinished:
		framer.close()
		return
	case RunError:
		framer.run = nil
		framer.events = append(framer.events, event)
		return
	}
	if framer.run == nil {
		framer.events = append(framer.events,
			AgUiEvent{Type: RunStarted, ThreadID: thread, RunID: run})
		framer.run = &openRun{thread: thread, run: run}
	}
	if kind, id, step, tracked := spanOf(event); tracked {
		open := framer.run.indexOf(kind, id)
		switch {
		case step == spanOpen && open >= 0:
			// A duplicate start for a span already open: nothing to add.
			return
		case step == spanOpen:
			framer.run.spans = append(framer.run.spans, openSpan{kind: kind, id: id})
		case open < 0:
			// Content or a close with no recorded open: the stream was joined
			// mid-flight, so the opening is synthesized before it.
			framer.events = append(framer.events, openingOf(kind, id))
			if step == spanContent {
				framer.run.spans = append(framer.run.spans, openSpan{kind: kind, id: id})
			}
		case step == spanClose:
			framer.run.spans = append(framer.run.spans[:open], framer.run.spans[open+1:]...)
		}
	}
	framer.events = append(framer.events, event)
}

func (run *openRun) indexOf(kind spanKind, id string) int {
	for index, held := range run.spans {
		if held.kind == kind && held.id == id {
			return index
		}
	}
	return -1
}

func (framer *runFramer) finish() []AgUiEvent {
	framer.close()
	return framer.events
}

// close ends the open run, closing its still-open spans in reverse order
// before the run itself finishes, whether an explicit terminal closed it or
// the read simply ended with it still open.
func (framer *runFramer) close() {
	run := framer.run
	if run == nil {
		return
	}
	framer.run = nil
	for index := len(run.spans) - 1; index >= 0; index-- {
		framer.events = append(framer.events, closingOf(run.spans[index].kind, run.spans[index].id))
	}
	framer.events = append(framer.events, AgUiEvent{Type: RunFinished, ThreadID: run.thread, RunID: run.run})
}

// spanOf reports the span one event belongs to and where it sits in it. An
// event outside every message family tracks no span.
func spanOf(event AgUiEvent) (kind spanKind, id string, step spanStep, tracked bool) {
	switch event.Type {
	case TextMessageStart:
		return spanText, event.MessageID, spanOpen, true
	case TextMessageContent:
		return spanText, event.MessageID, spanContent, true
	case TextMessageEnd:
		return spanText, event.MessageID, spanClose, true
	case ReasoningMessageStart:
		return spanReasoning, event.MessageID, spanOpen, true
	case ReasoningMessageContent:
		return spanReasoning, event.MessageID, spanContent, true
	case ReasoningMessageEnd:
		return spanReasoning, event.MessageID, spanClose, true
	case ToolCallStart:
		return spanToolCall, event.ToolCallID, spanOpen, true
	case ToolCallArgs:
		return spanToolCall, event.ToolCallID, spanContent, true
	case ToolCallEnd:
		return spanToolCall, event.ToolCallID, spanClose, true
	default:
		return 0, "", 0, false
	}
}

func openingOf(kind spanKind, id string) AgUiEvent {
	switch kind {
	case spanText:
		return AgUiEvent{Type: TextMessageStart, MessageID: id, Role: "assistant"}
	case spanReasoning:
		return AgUiEvent{Type: ReasoningMessageStart, MessageID: id, Role: "reasoning"}
	default:
		return AgUiEvent{Type: ToolCallStart, ToolCallID: id}
	}
}

func closingOf(kind spanKind, id string) AgUiEvent {
	switch kind {
	case spanText:
		return AgUiEvent{Type: TextMessageEnd, MessageID: id}
	case spanReasoning:
		return AgUiEvent{Type: ReasoningMessageEnd, MessageID: id}
	default:
		return AgUiEvent{Type: ToolCallEnd, ToolCallID: id}
	}
}
