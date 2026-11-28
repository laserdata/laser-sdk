package laser_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenAChunkStreamWhenWrittenAndFinishedThenShouldReassembleInOrder(t *testing.T) {
	log := &chunkLog{streamServer: &streamServer{}}
	stream := chunkAgdx(t, log).Stream(correlationID(2), wire.OperationChat)
	ctx := context.Background()

	for _, body := range []string{"Hel", "lo"} {
		if err := stream.Write(ctx, []byte(body)); err != nil {
			t.Fatalf("write %q: %v", body, err)
		}
	}
	if err := stream.Finish(ctx, "stop", nil); err != nil {
		t.Fatalf("finish: %v", err)
	}

	if sizes := log.batchSizes(); !reflect.DeepEqual(sizes, []int{1, 1, 1}) {
		t.Fatalf("an unbuffered stream publishes each write, got batches %v", sizes)
	}
	assembler := laser.NewChunkAssembler()
	var events []laser.StreamEvent
	for _, envelope := range log.envelopes(t) {
		events = append(events, assembler.Feed(envelope)...)
	}
	stop := "stop"
	want := []laser.StreamEvent{
		{Kind: laser.StreamBody, Sequence: 0, Payload: []byte("Hel")},
		{Kind: laser.StreamBody, Sequence: 1, Payload: []byte("lo")},
		{Kind: laser.StreamFinished, FinishReason: &stop},
	}
	if !reflect.DeepEqual(events, want) {
		t.Errorf("want %+v, got %+v", want, events)
	}
}

func TestGivenABufferedChunkStreamWhenFinishedThenShouldAppendChunksAndTerminalOnce(t *testing.T) {
	log := &chunkLog{streamServer: &streamServer{}}
	target := agentID(t, "target-agent")
	stream := chunkAgdx(t, log).Stream(correlationID(7), wire.OperationChat).
		WithTarget(target).Buffered(10, 10*time.Second)
	ctx := context.Background()

	for _, body := range []string{"one", "two"} {
		if err := stream.Write(ctx, []byte(body)); err != nil {
			t.Fatalf("write %q: %v", body, err)
		}
	}
	held := log.batchSizes()
	usage := wire.TokenUsage{InputTokens: 2, OutputTokens: 3}
	if err := stream.Finish(ctx, "stop", &usage); err != nil {
		t.Fatalf("finish: %v", err)
	}

	if len(held) != 0 {
		t.Fatalf("buffered chunks wait for the terminal, got batches %v", held)
	}
	if sizes := log.batchSizes(); !reflect.DeepEqual(sizes, []int{3}) {
		t.Fatalf("want one batch of three, got %v", sizes)
	}
	envelopes := log.envelopes(t)
	for index, envelope := range envelopes {
		if envelope.Sequence == nil || *envelope.Sequence != uint64(index) {
			t.Errorf("record %d carries sequence %v", index, envelope.Sequence)
		}
		if envelope.Channel == nil || *envelope.Channel != stream.Channel() {
			t.Errorf("record %d carries channel %v", index, envelope.Channel)
		}
		if envelope.Target == nil || *envelope.Target != target {
			t.Errorf("record %d carries target %v", index, envelope.Target)
		}
	}
	if operation := envelopes[0].Operation; operation == nil || *operation != wire.OperationChat {
		t.Errorf("the opening chunk declares the purpose, got %v", operation)
	}
	if envelopes[1].Operation != nil {
		t.Errorf("only the opening chunk declares the purpose, got %v", *envelopes[1].Operation)
	}
	terminal := envelopes[2]
	if !terminal.Last || terminal.FinishReason == nil || *terminal.FinishReason != "stop" {
		t.Errorf("want a terminal that stopped, got %+v", terminal)
	}
	if terminal.Usage == nil || terminal.Usage.OutputTokens != 3 {
		t.Errorf("want the stream usage on the terminal, got %+v", terminal.Usage)
	}
	var invalid *laser.InvalidError
	if err := stream.Write(ctx, nil); !errors.As(err, &invalid) {
		t.Errorf("a terminal stream refuses a write, got %T: %v", err, err)
	}
}

func TestGivenAChunkPublishFailureWhenWritingThenShouldKeepTheSequenceForTheRetry(t *testing.T) {
	log := &chunkLog{streamServer: &streamServer{}, failures: 1}
	stream := chunkAgdx(t, log).Stream(correlationID(9), wire.OperationChat)
	ctx := context.Background()

	failed := stream.Write(ctx, []byte("t"))
	if err := stream.Write(ctx, []byte("t")); err != nil {
		t.Fatalf("the retry: %v", err)
	}
	if err := stream.Finish(ctx, "stop", nil); err != nil {
		t.Fatalf("finish: %v", err)
	}

	if failed == nil {
		t.Fatal("the first publish must fail")
	}
	envelopes := log.envelopes(t)
	if len(envelopes) != 2 || *envelopes[0].Sequence != 0 || *envelopes[1].Sequence != 1 {
		t.Fatalf("the unsent chunk keeps its sequence, got %+v", envelopes)
	}
	if envelopes[0].DeadlineMicros == nil {
		t.Error("the opening chunk is bounded")
	}
}

func TestGivenABufferedFlushFailureWhenFlushedAgainThenShouldSendTheHeldChunks(t *testing.T) {
	log := &chunkLog{streamServer: &streamServer{}, failures: 1}
	stream := chunkAgdx(t, log).Stream(correlationID(9), wire.OperationChat).Buffered(1, time.Minute)
	ctx := context.Background()

	failed := stream.Write(ctx, []byte("t"))
	flushed := stream.Flush(ctx)
	again := stream.Flush(ctx)

	if failed == nil || flushed != nil || again != nil {
		t.Fatalf("want a failed write, then a flush and a no-op, got %v, %v, %v", failed, flushed, again)
	}
	envelopes := log.envelopes(t)
	if len(envelopes) != 1 || *envelopes[0].Sequence != 0 || string(envelopes[0].Body) != "t" {
		t.Fatalf("the unsent chunk stays buffered for the next flush, got %+v", envelopes)
	}
}

func TestGivenABufferedChunkStreamWhenItsBoundsTripThenShouldFlushAsOneBatch(t *testing.T) {
	log := &chunkLog{streamServer: &streamServer{}}
	agdx := chunkAgdx(t, log)
	ctx := context.Background()
	counted := agdx.Stream(correlationID(3), wire.OperationChat).Buffered(2, time.Minute)
	lingering := agdx.Stream(correlationID(4), wire.OperationChat).Buffered(10, 0)

	for _, body := range []string{"a", "b", "c"} {
		if err := counted.Write(ctx, []byte(body)); err != nil {
			t.Fatalf("write %q: %v", body, err)
		}
	}
	afterCount := log.batchSizes()
	if err := lingering.Write(ctx, []byte("d")); err != nil {
		t.Fatalf("write under an elapsed linger: %v", err)
	}

	if !reflect.DeepEqual(afterCount, []int{2}) {
		t.Errorf("two chunks fill the buffer and the third waits, got %v", afterCount)
	}
	if sizes := log.batchSizes(); !reflect.DeepEqual(sizes, []int{2, 1}) {
		t.Errorf("an elapsed linger flushes at the write, got %v", sizes)
	}
}

func TestGivenANewChunkStreamWhenOpenedThenShouldCarryTheDefaultAbandonmentBound(t *testing.T) {
	log := &chunkLog{streamServer: &streamServer{}}
	opened := time.Now()
	stream := chunkAgdx(t, log).Stream(correlationID(8), wire.OperationChat)

	if err := stream.Write(context.Background(), []byte("t")); err != nil {
		t.Fatalf("write: %v", err)
	}

	bound := log.envelopes(t)[0].DeadlineMicros
	if bound == nil || *bound < uint64(opened.Add(laser.DefaultChunkAbandon).UnixMicro()) {
		t.Errorf("want a bound at least %v after opening, got %v", laser.DefaultChunkAbandon, bound)
	}
}

func TestGivenAnAbsoluteDeadlineWhenSetOnAChunkStreamThenShouldCarryItUnchanged(t *testing.T) {
	log := &chunkLog{streamServer: &streamServer{}}
	deadline := time.UnixMicro(1_717_171_777_000_000)
	stream := chunkAgdx(t, log).Stream(correlationID(8), wire.OperationChat).WithDeadline(deadline)
	ctx := context.Background()

	for range 2 {
		if err := stream.Write(ctx, []byte("t")); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	envelopes := log.envelopes(t)
	if bound := envelopes[0].DeadlineMicros; bound == nil || *bound != 1_717_171_777_000_000 {
		t.Errorf("want the deadline unchanged on the opening chunk, got %v", bound)
	}
	if envelopes[1].DeadlineMicros != nil {
		t.Errorf("only the opening chunk carries the bound, got %d", *envelopes[1].DeadlineMicros)
	}
}

func TestGivenAnOversizedChunkWhenWrittenThenShouldRejectBeforeTransport(t *testing.T) {
	log := &chunkLog{streamServer: &streamServer{}}
	stream := chunkAgdx(t, log).Stream(correlationID(7), wire.OperationChat).Buffered(4, time.Minute)

	err := stream.Write(context.Background(), make([]byte, laser.MaxChunkBodyBytes+1))
	flushed := stream.Flush(context.Background())

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
	if flushed != nil || len(log.batchSizes()) != 0 {
		t.Errorf("a refused chunk is neither sent nor buffered, got %v and %v", flushed, log.batchSizes())
	}
}

func TestGivenAFailedChunkStreamWhenTerminatedThenShouldEndOnAnErrorCarryingTheChannelAndNextSequence(t *testing.T) {
	log := &chunkLog{streamServer: &streamServer{}}
	stream := chunkAgdx(t, log).Stream(correlationID(5), wire.OperationChat).Buffered(10, time.Minute)
	ctx := context.Background()
	message := "the model stopped answering"
	failure := wire.AgentErrorBody{Code: wire.ErrorToolFailure, Message: &message, Retryable: true}

	if err := stream.Write(ctx, []byte("par")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := stream.Fail(ctx, failure); err != nil {
		t.Fatalf("fail: %v", err)
	}

	if sizes := log.batchSizes(); !reflect.DeepEqual(sizes, []int{2}) {
		t.Fatalf("the held chunk and the terminal append together, got %v", sizes)
	}
	terminal := log.envelopes(t)[1]
	if terminal.Kind != wire.KindError || terminal.Record == nil {
		t.Fatalf("want an error terminal with its own record, got %+v", terminal)
	}
	if terminal.Channel == nil || *terminal.Channel != stream.Channel() || terminal.Sequence == nil || *terminal.Sequence != 1 {
		t.Errorf("want the channel and the next sequence, got %v and %v", terminal.Channel, terminal.Sequence)
	}
	var decoded wire.AgentErrorBody
	if err := wire.Unmarshal(terminal.Body, &decoded); err != nil || !reflect.DeepEqual(decoded, failure) {
		t.Errorf("want the failure body %+v, got %+v and %v", failure, decoded, err)
	}
	if got := contentTypeOf(t, log.batches[0][1]); got != wire.ContentCBOR {
		t.Errorf("an error terminal is CBOR, got %q", got)
	}
	if got := contentTypeOf(t, log.batches[0][0]); got != wire.ContentRaw {
		t.Errorf("chunk bodies default to raw, got %q", got)
	}
	var invalid *laser.InvalidError
	if err := stream.Finish(ctx, "stop", nil); !errors.As(err, &invalid) {
		t.Errorf("a terminal stream refuses a second terminal, got %T: %v", err, err)
	}
}

func TestGivenAGovernorThatBlocksWhenAChunkStreamWritesThenShouldPublishWithoutAskingIt(t *testing.T) {
	log := &chunkLog{streamServer: &streamServer{}}
	governor := &scriptedGovernor{decision: laser.Block("no chunks")}
	handle := laser.FromClient(log, laser.Options{DefaultStream: "agents"}).
		WithGovernor(governor, laser.GovernorEnforce)
	agdx := handle.Agdx(agentID(t, "worker"), conversation(1))
	stream := agdx.Stream(correlationID(6), wire.OperationChat)
	ctx := context.Background()

	_, governed := agdx.Chunk(correlationID(6), stream.Channel(), 0, []byte("t")).
		WithOperation(wire.OperationChat).Send(ctx)
	asked := len(governor.asked())
	recorded := len(log.batchSizes())
	written := stream.Write(ctx, []byte("t"))
	finished := stream.Finish(ctx, "stop", nil)

	var blocked *laser.PolicyBlockedError
	if !errors.As(governed, &blocked) || asked != 1 {
		t.Fatalf("a plain chunk send is governed, got %T: %v after %d decisions", governed, governed, asked)
	}
	if written != nil || finished != nil {
		t.Fatalf("the stream writer bypasses the governor, got %v and %v", written, finished)
	}
	if total := len(governor.asked()); total != asked {
		t.Errorf("the writer must not ask the governor, got %d more decisions", total-asked)
	}
	if sizes := log.batchSizes()[recorded:]; !reflect.DeepEqual(sizes, []int{1, 1}) {
		t.Errorf("want the chunk and its terminal on the log, got %v", sizes)
	}
}

func TestGivenAChunkStreamContentTypeWhenSetThenShouldStampEveryChunk(t *testing.T) {
	log := &chunkLog{streamServer: &streamServer{}}
	stream := chunkAgdx(t, log).Stream(correlationID(4), wire.OperationToolArgs).ContentType(wire.ContentJSON)

	if err := stream.Write(context.Background(), []byte(`{"city":`)); err != nil {
		t.Fatalf("write: %v", err)
	}

	if got := contentTypeOf(t, log.batches[0][0]); got != wire.ContentJSON {
		t.Errorf("want JSON chunk bodies, got %q", got)
	}
}

// chunkLog records every publish as its own batch and fails the first ones on request.
type chunkLog struct {
	*streamServer
	guard    sync.Mutex
	batches  [][]iggcon.IggyMessage
	failures int
}

func (log *chunkLog) SendMessages(
	_ context.Context, _, _ iggcon.Identifier,
	_ iggcon.Partitioning, messages []iggcon.IggyMessage,
) (*iggcon.SendMessagesResponse, error) {
	log.guard.Lock()
	defer log.guard.Unlock()
	if log.failures > 0 {
		log.failures--
		return nil, errors.New("link down")
	}
	log.batches = append(log.batches, append([]iggcon.IggyMessage(nil), messages...))
	return &iggcon.SendMessagesResponse{}, nil
}

func (log *chunkLog) batchSizes() []int {
	log.guard.Lock()
	defer log.guard.Unlock()
	sizes := make([]int, 0, len(log.batches))
	for _, batch := range log.batches {
		sizes = append(sizes, len(batch))
	}
	return sizes
}

func (log *chunkLog) envelopes(t *testing.T) []wire.AgentEnvelope {
	t.Helper()
	log.guard.Lock()
	defer log.guard.Unlock()
	var envelopes []wire.AgentEnvelope
	for _, batch := range log.batches {
		for _, message := range batch {
			var envelope wire.AgentEnvelope
			if err := wire.Unmarshal(message.Payload, &envelope); err != nil {
				t.Fatalf("decode the envelope: %v", err)
			}
			envelopes = append(envelopes, envelope)
		}
	}
	return envelopes
}

func chunkAgdx(t *testing.T, log *chunkLog) *laser.Agdx {
	t.Helper()
	return laser.FromClient(log, laser.Options{DefaultStream: "agents"}).
		Agdx(agentID(t, "worker"), conversation(1))
}

func contentTypeOf(t *testing.T, message iggcon.IggyMessage) wire.ContentType {
	t.Helper()
	entries, err := iggcon.DeserializeHeaders(message.UserHeaders)
	if err != nil {
		t.Fatalf("decode the user headers: %v", err)
	}
	for _, entry := range entries {
		if string(entry.Key.Value) != wire.HeaderContentType || len(entry.Value.Value) != 1 {
			continue
		}
		contentType, named := wire.ContentTypeFromCode(entry.Value.Value[0])
		if !named {
			t.Fatalf("unknown content type code %d", entry.Value.Value[0])
		}
		return contentType
	}
	t.Fatal("the record carries no content type")
	return ""
}
