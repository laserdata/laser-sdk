package laser

import (
	"context"
	"fmt"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

const (
	// DefaultChunkFlushBytes is the suggested body size at which a producer flushes a chunk.
	DefaultChunkFlushBytes = 512
	// DefaultChunkLinger is the suggested longest wait before a producer flushes a chunk.
	DefaultChunkLinger = 20 * time.Millisecond
	// DefaultChunkAbandon is how long after its opening a chunk stream runs before readers abandon it.
	DefaultChunkAbandon = 10 * time.Minute
)

// AgdxStream writes one chunk stream past the governor: sequences from zero, the purpose and the abandonment bound on the opening chunk, one terminal.
type AgdxStream struct {
	agdx        *Agdx
	correlation wire.CorrelationID
	channel     wire.ChannelID
	purpose     string
	sequence    uint64
	deadline    time.Time
	target      *wire.AgentID
	contentType wire.ContentType
	buffer      *chunkBuffer
	closed      bool
}

// Stream opens a chunk stream writer under correlation on a fresh channel. The purpose is chat, reasoning or tool_args.
func (agdx *Agdx) Stream(correlation wire.CorrelationID, purpose string) *AgdxStream {
	return &AgdxStream{
		agdx:        agdx,
		correlation: correlation,
		channel:     wire.ChannelID{ID: wire.IDFromBytes(NewOperationID())},
		purpose:     purpose,
		deadline:    time.Now().Add(DefaultChunkAbandon),
		contentType: wire.ContentRaw,
	}
}

// Channel is the stream's channel id.
func (stream *AgdxStream) Channel() wire.ChannelID { return stream.channel }

// WithDeadline sets when readers abandon the stream. It rides the opening chunk, so set it before the first Write.
func (stream *AgdxStream) WithDeadline(deadline time.Time) *AgdxStream {
	stream.deadline = deadline
	return stream
}

// WithTarget narrows delivery to one agent within the shared topic.
func (stream *AgdxStream) WithTarget(target wire.AgentID) *AgdxStream {
	stream.target = &target
	return stream
}

// ContentType names how the chunk bodies are encoded. The default is raw.
func (stream *AgdxStream) ContentType(contentType wire.ContentType) *AgdxStream {
	stream.contentType = contentType
	return stream
}

// Buffered holds writes and appends them as one batch once maxChunks are held or a Write finds linger passed since the first held chunk.
func (stream *AgdxStream) Buffered(maxChunks int, linger time.Duration) *AgdxStream {
	stream.buffer = &chunkBuffer{maxChunks: max(maxChunks, 1), linger: linger}
	return stream
}

// Write publishes the next chunk. A failed publish keeps the sequence, so writing the body again continues the stream without a gap.
func (stream *AgdxStream) Write(ctx context.Context, body []byte) error {
	if stream.closed {
		return &InvalidError{Reason: "an AGDX stream is already terminal"}
	}
	if len(body) > MaxChunkBodyBytes {
		return &InvalidError{Reason: fmt.Sprintf(
			"chunk body is %dB, exceeds cap %dB", len(body), MaxChunkBodyBytes)}
	}
	chunk := stream.chunk(body)
	if stream.buffer == nil {
		if _, err := chunk.Send(ctx); err != nil {
			return err
		}
		stream.sequence++
		return nil
	}
	message, err := chunk.prepare(ctx)
	if err != nil {
		return err
	}
	stream.sequence++
	if stream.buffer.hold(message) {
		return stream.Flush(ctx)
	}
	return nil
}

// Flush appends every held chunk as one batch. A failed append keeps them for the next Flush.
func (stream *AgdxStream) Flush(ctx context.Context) error {
	if stream.buffer == nil || len(stream.buffer.messages) == 0 {
		return nil
	}
	if _, err := stream.chunk(nil).appendMessages(ctx, stream.buffer.messages); err != nil {
		return err
	}
	stream.buffer.take()
	return nil
}

// Finish publishes the terminal chunk with the reason the stream ended and its usage. A buffered stream appends its held chunks with it.
func (stream *AgdxStream) Finish(ctx context.Context, finishReason string, usage *wire.TokenUsage) error {
	if stream.closed {
		return &InvalidError{Reason: "an AGDX stream is already terminal"}
	}
	stream.closed = true
	terminal := stream.chunk(nil).Last().WithFinishReason(finishReason)
	if usage != nil {
		terminal.WithUsage(*usage)
	}
	return stream.sendTerminal(ctx, terminal)
}

// Fail ends the stream with an error record carrying this channel and the next sequence.
func (stream *AgdxStream) Fail(ctx context.Context, failure wire.AgentErrorBody) error {
	if stream.closed {
		return &InvalidError{Reason: "an AGDX stream is already terminal"}
	}
	stream.closed = true
	terminal := stream.agdx.Fail(stream.correlation, failure)
	terminal.ungoverned = true
	sequence := stream.sequence
	terminal.envelope.Channel, terminal.envelope.Sequence = &stream.channel, &sequence
	if stream.target != nil {
		terminal.WithTarget(*stream.target)
	}
	return stream.sendTerminal(ctx, terminal)
}

func (stream *AgdxStream) sendTerminal(ctx context.Context, terminal *AgdxSend) error {
	if stream.buffer == nil {
		_, err := terminal.Send(ctx)
		return err
	}
	message, err := terminal.prepare(ctx)
	if err != nil {
		return err
	}
	_, err = terminal.appendMessages(ctx, append(stream.buffer.take(), message))
	return err
}

// chunk is the next chunk of the stream, unsent.
func (stream *AgdxStream) chunk(body []byte) *AgdxSend {
	chunk := stream.agdx.Chunk(stream.correlation, stream.channel, stream.sequence, body).
		ContentType(stream.contentType)
	chunk.ungoverned = true
	if stream.sequence == 0 {
		chunk.WithOperation(stream.purpose).WithDeadline(stream.deadline)
	}
	if stream.target != nil {
		chunk.WithTarget(*stream.target)
	}
	return chunk
}

// chunkBuffer holds prepared chunks until a bound trips. Sequences are assigned at Write.
type chunkBuffer struct {
	maxChunks int
	linger    time.Duration
	firstAt   time.Time
	messages  []iggcon.IggyMessage
}

// hold keeps one prepared chunk and reports whether a bound tripped.
func (buffer *chunkBuffer) hold(message iggcon.IggyMessage) bool {
	if buffer.firstAt.IsZero() {
		buffer.firstAt = time.Now()
	}
	buffer.messages = append(buffer.messages, message)
	return len(buffer.messages) >= buffer.maxChunks || time.Since(buffer.firstAt) >= buffer.linger
}

func (buffer *chunkBuffer) take() []iggcon.IggyMessage {
	held := buffer.messages
	buffer.messages, buffer.firstAt = nil, time.Time{}
	return held
}
