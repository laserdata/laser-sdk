package laser_test

import (
	"context"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// streamChunk is one chunk of a channel at a sequence.
func streamChunk(
	t *testing.T, channel wire.ChannelID, sequence uint64, body string, last bool,
) wire.AgentEnvelope {
	t.Helper()
	source, err := wire.NewAgentID("planner")
	if err != nil {
		t.Fatalf("agent id: %v", err)
	}
	at := sequence
	record := wire.RecordID{ID: wire.IDFromBytes(laser.NewOperationID())}
	return wire.AgentEnvelope{
		Kind:         wire.KindChunk,
		Record:       &record,
		Conversation: wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})},
		Source:       source,
		Channel:      &channel,
		Sequence:     &at,
		Last:         last,
		Body:         []byte(body),
	}
}

// Chunks apply in order from zero, each once, and the last one ends the
// stream.
func TestGivenChunksInOrderWhenAssembledThenShouldReplayTheStream(t *testing.T) {
	channel := wire.ChannelID{ID: wire.IDFromBytes([16]byte{15: 9})}
	assembler := laser.NewChunkAssembler()

	var events []laser.StreamEvent
	events = append(events, assembler.Feed(streamChunk(t, channel, 0, "the ", false))...)
	events = append(events, assembler.Feed(streamChunk(t, channel, 1, "answer", true))...)

	if len(events) != 3 {
		t.Fatalf("want two bodies and a terminal, got %d", len(events))
	}
	if events[0].Kind != laser.StreamBody || string(events[0].Payload) != "the " {
		t.Errorf("want the first body, got %+v", events[0])
	}
	if events[2].Kind != laser.StreamFinished || events[2].Synthetic {
		t.Errorf("want a real terminal, got %+v", events[2])
	}
	if !assembler.Finished() {
		t.Error("a stream that ended is finished")
	}
}

// A consumer is at-least-once, so a redelivered chunk is dropped and counted
// rather than applied twice.
func TestGivenARedeliveredChunkWhenAssembledThenShouldDropIt(t *testing.T) {
	channel := wire.ChannelID{ID: wire.IDFromBytes([16]byte{15: 9})}
	assembler := laser.NewChunkAssembler()

	assembler.Feed(streamChunk(t, channel, 0, "once", false))
	again := assembler.Feed(streamChunk(t, channel, 0, "once", false))

	if len(again) != 0 {
		t.Errorf("want the duplicate dropped, got %+v", again)
	}
	if assembler.DuplicatesDropped() != 1 {
		t.Errorf("want it counted, got %d", assembler.DuplicatesDropped())
	}
}

// The log is in order within a channel, so a missing sequence is one the
// producer never published rather than one that is late.
func TestGivenAGapWhenAssembledThenShouldEndTheStream(t *testing.T) {
	channel := wire.ChannelID{ID: wire.IDFromBytes([16]byte{15: 9})}
	assembler := laser.NewChunkAssembler()

	assembler.Feed(streamChunk(t, channel, 0, "the ", false))
	events := assembler.Feed(streamChunk(t, channel, 2, "end", false))

	if len(events) != 1 || events[0].Kind != laser.StreamFinished {
		t.Fatalf("want the stream ended, got %+v", events)
	}
	if events[0].FinishReason == nil ||
		*events[0].FinishReason != laser.FinishReasonGap {
		t.Errorf("want the gap named, got %v", events[0].FinishReason)
	}
	// Synthetic: the log never carried this terminal, the reader made it.
	if !events[0].Synthetic {
		t.Error("a reader-local ending is synthetic")
	}
}

// The first terminal wins, so everything after one is dropped and counted.
func TestGivenChunksAfterATerminalWhenAssembledThenShouldDropThem(t *testing.T) {
	channel := wire.ChannelID{ID: wire.IDFromBytes([16]byte{15: 9})}
	assembler := laser.NewChunkAssembler()

	assembler.Feed(streamChunk(t, channel, 0, "done", true))
	late := assembler.Feed(streamChunk(t, channel, 1, "too late", false))

	if len(late) != 0 {
		t.Errorf("want the late chunk dropped, got %+v", late)
	}
	if assembler.LateDropped() != 1 {
		t.Errorf("want it counted, got %d", assembler.LateDropped())
	}
	// A second terminal is late too.
	if abandoned := assembler.Abandon(); abandoned != nil {
		t.Error("a stream that ended cannot be abandoned")
	}
}

// An error terminal ends the stream as a failure, carrying the body a caller
// decodes the reason from.
func TestGivenAnErrorTerminalWhenAssembledThenShouldFailTheStream(t *testing.T) {
	channel := wire.ChannelID{ID: wire.IDFromBytes([16]byte{15: 9})}
	assembler := laser.NewChunkAssembler()
	failure := streamChunk(t, channel, 0, "", false)
	failure.Kind = wire.KindError
	failure.Sequence = nil
	failure.Body = []byte("the encoded reason")

	events := assembler.Feed(failure)

	if len(events) != 1 || events[0].Kind != laser.StreamFailed {
		t.Fatalf("want a failure, got %+v", events)
	}
	if string(events[0].Payload) != "the encoded reason" {
		t.Errorf("want the reason carried, got %q", events[0].Payload)
	}
	if !assembler.Finished() {
		t.Error("a failed stream has ended")
	}
}

// A message that is not part of the stream neither advances nor ends it.
func TestGivenAnUnrelatedKindWhenAssembledThenShouldIgnoreIt(t *testing.T) {
	channel := wire.ChannelID{ID: wire.IDFromBytes([16]byte{15: 9})}
	assembler := laser.NewChunkAssembler()

	for _, kind := range []wire.AgentKind{
		wire.KindCommand, wire.KindResponse, wire.KindEvent, wire.KindStatus,
	} {
		envelope := streamChunk(t, channel, 0, "body", false)
		envelope.Kind = kind
		if events := assembler.Feed(envelope); len(events) != 0 {
			t.Errorf("%s: want it ignored, got %+v", kind, events)
		}
	}
	if assembler.Finished() {
		t.Error("nothing ended the stream")
	}
	// The stream is still at its first chunk, so chunk zero still applies.
	if events := assembler.Feed(
		streamChunk(t, channel, 0, "first", false)); len(events) != 1 {
		t.Errorf("want the first chunk still to apply, got %+v", events)
	}
}

// A chunk with no sequence has no place in the stream, so it never advances
// it.
func TestGivenAChunkWithNoSequenceWhenAssembledThenShouldIgnoreIt(t *testing.T) {
	channel := wire.ChannelID{ID: wire.IDFromBytes([16]byte{15: 9})}
	assembler := laser.NewChunkAssembler()
	malformed := streamChunk(t, channel, 0, "body", false)
	malformed.Sequence = nil

	events := assembler.Feed(malformed)

	if len(events) != 0 {
		t.Errorf("want it ignored, got %+v", events)
	}
	if assembler.Finished() {
		t.Error("a malformed chunk does not end the stream")
	}
}

// A terminal chunk that also carries a body yields both, in order, so the
// last bytes are not lost to the ending.
func TestGivenATerminalWithABodyWhenAssembledThenShouldYieldBoth(t *testing.T) {
	channel := wire.ChannelID{ID: wire.IDFromBytes([16]byte{15: 9})}
	assembler := laser.NewChunkAssembler()
	reason := "stop"
	last := streamChunk(t, channel, 0, "the last bytes", true)
	last.FinishReason = &reason

	events := assembler.Feed(last)

	if len(events) != 2 {
		t.Fatalf("want the body then the terminal, got %+v", events)
	}
	if events[0].Kind != laser.StreamBody ||
		string(events[0].Payload) != "the last bytes" {
		t.Errorf("want the body first, got %+v", events[0])
	}
	if events[1].FinishReason == nil || *events[1].FinishReason != reason {
		t.Errorf("want the producer's reason, got %v", events[1].FinishReason)
	}
}

// An empty terminal chunk ends the stream without an empty body beside it.
func TestGivenAnEmptyTerminalWhenAssembledThenShouldYieldOnlyTheEnding(t *testing.T) {
	channel := wire.ChannelID{ID: wire.IDFromBytes([16]byte{15: 9})}
	assembler := laser.NewChunkAssembler()

	events := assembler.Feed(streamChunk(t, channel, 0, "", true))

	if len(events) != 1 || events[0].Kind != laser.StreamFinished {
		t.Fatalf("want only the ending, got %+v", events)
	}
}

// A stream whose producer never ended it is ended by the reader, once the
// bound the opening chunk declared has passed.
func TestGivenAnUnfinishedStreamWhenAbandonedThenShouldEndItSynthetically(t *testing.T) {
	channel := wire.ChannelID{ID: wire.IDFromBytes([16]byte{15: 9})}
	assembler := laser.NewChunkAssembler()
	assembler.Feed(streamChunk(t, channel, 0, "started", false))

	abandoned := assembler.Abandon()

	if abandoned == nil || abandoned.Kind != laser.StreamFinished {
		t.Fatalf("want an ending, got %+v", abandoned)
	}
	if abandoned.FinishReason == nil ||
		*abandoned.FinishReason != laser.FinishReasonAbandoned {
		t.Errorf("want the abandonment named, got %v", abandoned.FinishReason)
	}
	if !abandoned.Synthetic {
		t.Error("the log never carried this ending")
	}
}

// Offset replay does what a server-sent event stream cannot: a finished
// stream reconstructs after the fact with no transcript store anywhere.
func TestGivenAFinishedStreamOnTheLogWhenReassembledThenShouldReplayIt(t *testing.T) {
	channel := wire.ChannelID{ID: wire.IDFromBytes([16]byte{15: 9})}
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}
	server := newFeed(1)
	// Published out of order, as two partitions of one stream would arrive.
	for _, chunk := range []wire.AgentEnvelope{
		streamChunk(t, channel, 1, "answer", true),
		streamChunk(t, channel, 0, "the ", false),
	} {
		encoded, err := wire.Marshal(chunk)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		server.append(0, encoded)
	}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	events, err := handle.ReassembleChannel(
		context.Background(), conversation, "agent.sessions", channel)
	if err != nil {
		t.Fatalf("reassemble: %v", err)
	}

	if len(events) != 3 {
		t.Fatalf("want both bodies and the terminal, got %d", len(events))
	}
	// Ordered by sequence, so it reassembles deterministically whichever way
	// the stream was keyed.
	if string(events[0].Payload) != "the " || string(events[1].Payload) != "answer" {
		t.Errorf("want the bodies in sequence order, got %q then %q",
			events[0].Payload, events[1].Payload)
	}
	if events[2].Kind != laser.StreamFinished {
		t.Errorf("want the terminal last, got %+v", events[2])
	}
}

// Another conversation's or another channel's chunks are not part of this
// stream, so they are left where they are.
func TestGivenOtherStreamsOnTheTopicWhenReassembledThenShouldIgnoreThem(t *testing.T) {
	mine := wire.ChannelID{ID: wire.IDFromBytes([16]byte{15: 9})}
	theirs := wire.ChannelID{ID: wire.IDFromBytes([16]byte{15: 8})}
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}
	server := newFeed(1)
	elsewhere := streamChunk(t, theirs, 0, "not mine", true)
	otherConversation := streamChunk(t, mine, 0, "not here", true)
	otherConversation.Conversation = wire.ConversationID{
		ID: wire.IDFromBytes([16]byte{15: 2})}
	for _, chunk := range []wire.AgentEnvelope{
		elsewhere, otherConversation, streamChunk(t, mine, 0, "mine", true),
	} {
		encoded, err := wire.Marshal(chunk)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		server.append(0, encoded)
	}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	events, err := handle.ReassembleChannel(
		context.Background(), conversation, "agent.sessions", mine)
	if err != nil {
		t.Fatalf("reassemble: %v", err)
	}

	if len(events) != 2 {
		t.Fatalf("want this stream alone, got %d", len(events))
	}
	if string(events[0].Payload) != "mine" {
		t.Errorf("want this channel's body, got %q", events[0].Payload)
	}
}

// A stream whose producer died is ended by the reader once the bound its
// opening chunk declared has passed.
func TestGivenAnAbandonedStreamOnTheLogWhenReassembledThenShouldEndIt(t *testing.T) {
	channel := wire.ChannelID{ID: wire.IDFromBytes([16]byte{15: 9})}
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}
	server := newFeed(1)
	opening := streamChunk(t, channel, 0, "started", false)
	past := uint64(time.Now().Add(-time.Hour).UnixMicro())
	opening.DeadlineMicros = &past
	encoded, err := wire.Marshal(opening)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	server.append(0, encoded)
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	events, err := handle.ReassembleChannel(
		context.Background(), conversation, "agent.sessions", channel)
	if err != nil {
		t.Fatalf("reassemble: %v", err)
	}

	if len(events) != 2 {
		t.Fatalf("want the body and a synthetic ending, got %d", len(events))
	}
	if events[1].FinishReason == nil ||
		*events[1].FinishReason != laser.FinishReasonAbandoned {
		t.Errorf("want the abandonment, got %+v", events[1])
	}
}

// A stream still inside its bound is not abandoned: its producer may yet
// finish it.
func TestGivenAStreamInsideItsBoundWhenReassembledThenShouldLeaveItOpen(t *testing.T) {
	channel := wire.ChannelID{ID: wire.IDFromBytes([16]byte{15: 9})}
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}
	server := newFeed(1)
	opening := streamChunk(t, channel, 0, "started", false)
	ahead := uint64(time.Now().Add(time.Hour).UnixMicro())
	opening.DeadlineMicros = &ahead
	encoded, err := wire.Marshal(opening)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	server.append(0, encoded)
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	events, err := handle.ReassembleChannel(
		context.Background(), conversation, "agent.sessions", channel)
	if err != nil {
		t.Fatalf("reassemble: %v", err)
	}

	if len(events) != 1 || events[0].Kind != laser.StreamBody {
		t.Fatalf("want the body alone, got %+v", events)
	}
}

// A chunk stream nobody wrote reassembles to nothing rather than failing.
func TestGivenNoChunksWhenReassembledThenShouldReturnNothing(t *testing.T) {
	channel := wire.ChannelID{ID: wire.IDFromBytes([16]byte{15: 9})}
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}
	handle := laser.FromClient(newFeed(1),
		laser.Options{DefaultStream: "agents"})

	events, err := handle.ReassembleChannel(
		context.Background(), conversation, "agent.sessions", channel)
	if err != nil {
		t.Fatalf("reassemble: %v", err)
	}
	if len(events) != 0 {
		t.Errorf("want nothing, got %d", len(events))
	}
}

// A record on the topic that is not an agent envelope at all is stepped over
// rather than failing the replay.
func TestGivenOtherTrafficOnTheTopicWhenReassembledThenShouldStepOverIt(t *testing.T) {
	channel := wire.ChannelID{ID: wire.IDFromBytes([16]byte{15: 9})}
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}
	server := newFeed(1)
	server.append(0, []byte("not an envelope"))
	encoded, err := wire.Marshal(streamChunk(t, channel, 0, "mine", true))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	server.append(0, encoded)
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	events, err := handle.ReassembleChannel(
		context.Background(), conversation, "agent.sessions", channel)
	if err != nil {
		t.Fatalf("reassemble: %v", err)
	}

	if len(events) != 2 {
		t.Fatalf("want the stream past the noise, got %d", len(events))
	}
}

// appendTo keeps the feed fake usable as a plain topic for these tests.
var _ iggcon.Client = (*feedServer)(nil)
