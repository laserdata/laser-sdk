package laser

import (
	"bytes"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// The synthetic finish reasons. They are reader-local and never published:
// the log keeps the raw truth, and a live view diverges from it only in this
// one direction, to say a stream ended that the producer never ended.
const (
	// FinishReasonAbandoned is synthesized when the stream's own deadline
	// passed with no further chunk.
	FinishReasonAbandoned = "abandoned"
	// FinishReasonGap is synthesized when a sequence was skipped. The log is
	// in order within a channel, so a gap proves the producer never published
	// it rather than that it is late.
	FinishReasonGap = "gap"
)

// StreamEventKind discriminates one reassembled occurrence.
type StreamEventKind string

const (
	// StreamBody is the next body bytes of the stream.
	StreamBody StreamEventKind = "body"
	// StreamFinished is the stream ending.
	StreamFinished StreamEventKind = "finished"
	// StreamFailed is the stream ending on an error terminal.
	StreamFailed StreamEventKind = "failed"
)

// StreamEvent is one reassembled stream occurrence, in order.
type StreamEvent struct {
	Kind StreamEventKind
	// Sequence is where a body sat in the stream.
	Sequence uint64
	// Payload is a body's bytes, or a failure's encoded error body.
	Payload []byte
	// FinishReason is why a finished stream ended, when it said.
	FinishReason *string
	// Usage is what the step cost, when the terminal carried it.
	Usage *wire.TokenUsage
	// Synthetic marks a reader-local ending, a gap or an abandonment, that
	// never reached the log.
	Synthetic bool
}

// ChunkAssembler reassembles one channel's chunks into ordered events.
//
// It is pure and clock-free: a caller that wants a deadline enforced wires
// the timer and calls Abandon. Keeping the clock out means the same assembler
// reassembles a live stream and replays a finished one from the log.
type ChunkAssembler struct {
	nextSequence      uint64
	finished          bool
	duplicatesDropped uint64
	lateDropped       uint64
}

// NewChunkAssembler opens an assembler for one channel.
func NewChunkAssembler() *ChunkAssembler { return &ChunkAssembler{} }

// Feed applies one envelope of this channel and returns the events it
// produces: none, one, or a body and its terminal for a non-empty last chunk.
//
// Reassembly is mechanical. Chunks apply in sequence order from zero, each
// once. A redelivered chunk drops and counts, because a consumer is
// at-least-once. A gap ends the stream, because the log is in order within a
// channel and a missing sequence is one the producer never published.
// Everything after a terminal drops and counts: the first terminal wins.
func (assembler *ChunkAssembler) Feed(envelope wire.AgentEnvelope) []StreamEvent {
	switch envelope.Kind {
	case wire.KindChunk:
		return assembler.feedChunk(envelope)
	case wire.KindError:
		if assembler.finished {
			assembler.lateDropped++
			return nil
		}
		assembler.finished = true
		return []StreamEvent{{
			Kind: StreamFailed, Payload: bytes.Clone(envelope.Body),
		}}
	default:
		// A command, response, event or status is not part of the stream, so
		// it neither advances nor ends it.
		return nil
	}
}

// Abandon synthesizes the reader-local ending for a stream whose deadline
// passed with no further chunk, nil when it already ended.
func (assembler *ChunkAssembler) Abandon() *StreamEvent {
	if assembler.finished {
		return nil
	}
	assembler.finished = true
	reason := FinishReasonAbandoned
	return &StreamEvent{
		Kind: StreamFinished, FinishReason: &reason, Synthetic: true,
	}
}

// Finished reports a terminal, real or synthetic, having been seen.
func (assembler *ChunkAssembler) Finished() bool { return assembler.finished }

// DuplicatesDropped is how many redelivered chunks were dropped.
func (assembler *ChunkAssembler) DuplicatesDropped() uint64 {
	return assembler.duplicatesDropped
}

// LateDropped is how many chunks and terminals arrived after the stream
// ended.
func (assembler *ChunkAssembler) LateDropped() uint64 { return assembler.lateDropped }

func (assembler *ChunkAssembler) feedChunk(envelope wire.AgentEnvelope) []StreamEvent {
	if envelope.Sequence == nil {
		// A chunk with no sequence has no place in the stream. The contract
		// refuses one upstream, and a malformed one never advances it.
		return nil
	}
	sequence := *envelope.Sequence
	switch {
	case assembler.finished:
		assembler.lateDropped++
		return nil
	case sequence < assembler.nextSequence:
		assembler.duplicatesDropped++
		return nil
	case sequence > assembler.nextSequence:
		assembler.finished = true
		reason := FinishReasonGap
		return []StreamEvent{{
			Kind: StreamFinished, FinishReason: &reason, Synthetic: true,
		}}
	}
	assembler.nextSequence++
	events := make([]StreamEvent, 0, 2)
	if len(envelope.Body) > 0 {
		events = append(events, StreamEvent{
			Kind: StreamBody, Sequence: sequence, Payload: bytes.Clone(envelope.Body),
		})
	}
	if envelope.Last {
		assembler.finished = true
		events = append(events, StreamEvent{
			Kind: StreamFinished, FinishReason: envelope.FinishReason,
			Usage: envelope.Usage,
		})
	}
	return events
}
