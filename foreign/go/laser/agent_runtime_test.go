package laser_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// runtimeServer keeps every record that reached it, under the topic it landed
// on, which is what a test asserting both a dead letter and the answer it
// triggered needs. streamServer keeps only the last send.
type runtimeServer struct {
	clusterOfOne
	mutex sync.Mutex
	sent  map[string][]iggcon.IggyMessage
	// refuse fails the first send to this topic, so a test can prove the
	// runtime retries a publish rather than losing the capsule.
	refuse       string
	refuseAlways bool
	refused      int
}

func newRuntimeServer() *runtimeServer {
	return &runtimeServer{sent: map[string][]iggcon.IggyMessage{}}
}

func (server *runtimeServer) Close() error { return nil }

func (server *runtimeServer) GetStream(
	_ context.Context, _ iggcon.Identifier,
) (*iggcon.StreamDetails, error) {
	details := &iggcon.StreamDetails{}
	details.Id = 7
	return details, nil
}

func (server *runtimeServer) GetTopic(
	_ context.Context, _, _ iggcon.Identifier,
) (*iggcon.TopicDetails, error) {
	details := &iggcon.TopicDetails{}
	details.Id = 3
	return details, nil
}

func (server *runtimeServer) SendMessages(
	_ context.Context, _, topicID iggcon.Identifier,
	_ iggcon.Partitioning, messages []iggcon.IggyMessage,
) (*iggcon.SendMessagesResponse, error) {
	name, _ := topicID.String()
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if name == server.refuse && (server.refuseAlways || server.refused == 0) {
		server.refused++
		return nil, &refusedPublish{retryable: !server.refuseAlways}
	}
	server.sent[name] = append(server.sent[name], messages...)
	return &iggcon.SendMessagesResponse{}, nil
}

// on is what reached one topic.
func (server *runtimeServer) on(topic string) []iggcon.IggyMessage {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	return append([]iggcon.IggyMessage(nil), server.sent[topic]...)
}

// refusedPublish is a send the server turned away. It says for itself whether
// a repeat could fix it, which is what decides whether the runtime tries again
// and therefore how long a test waits to see the outcome.
type refusedPublish struct{ retryable bool }

func (e *refusedPublish) Error() string   { return "the topic refuses this record" }
func (e *refusedPublish) Retryable() bool { return e.retryable }

// step is one thing a source hands over: a record, or a failure instead.
type step struct {
	record *laser.Record
	err    error
}

// queuedSource hands a runtime a fixed list of records and remembers what it
// committed, so the loop can be driven without a server.
//
// Once the list runs out it blocks until the context ends, which is what a
// live topic does, and signals that every record before it was settled: the
// runtime only asks for the next record after committing the last.
type queuedSource struct {
	mutex     sync.Mutex
	steps     []step
	index     int
	committed []uint64
	commitErr error
	drained   chan struct{}
	once      sync.Once
}

func sourceOf(records ...*laser.Record) *queuedSource {
	steps := make([]step, 0, len(records))
	for _, record := range records {
		steps = append(steps, step{record: record})
	}
	return &queuedSource{steps: steps, drained: make(chan struct{})}
}

func (source *queuedSource) Next(ctx context.Context) (*laser.Record, error) {
	source.mutex.Lock()
	if source.index < len(source.steps) {
		next := source.steps[source.index]
		source.index++
		source.mutex.Unlock()
		return next.record, next.err
	}
	source.mutex.Unlock()
	source.once.Do(func() { close(source.drained) })
	<-ctx.Done()
	return nil, ctx.Err()
}

func (source *queuedSource) Commit(_ context.Context, checkpoint iggcon.Checkpoint) error {
	if source.commitErr != nil {
		return source.commitErr
	}
	source.mutex.Lock()
	defer source.mutex.Unlock()
	source.committed = append(source.committed, checkpoint.Offset())
	return nil
}

func (source *queuedSource) commits() []uint64 {
	source.mutex.Lock()
	defer source.mutex.Unlock()
	return append([]uint64(nil), source.committed...)
}

// runUntilDrained drives the runtime over every queued record and then stops
// it, returning what the run reported.
func runUntilDrained(
	t *testing.T, runtime *laser.AgentRuntime,
	source *queuedSource, handler laser.AgentHandler,
) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx, source, handler) }()

	select {
	case err := <-done:
		// The run ended on its own, which a record it could not settle does.
		return err
	case <-source.drained:
	case <-time.After(5 * time.Second):
		t.Fatal("the runtime did not read every record")
	}
	cancel()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("the runtime did not stop")
		return nil
	}
}

// runtimeFor opens a runtime for one agent over a recording server.
func runtimeFor(
	t *testing.T, server *runtimeServer, name string, options laser.RuntimeOptions,
) *laser.AgentRuntime {
	t.Helper()
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	var identity wire.AgentID
	if name != "" {
		identity = agentID(t, name)
	}
	agent := handle.Agent(identity, laser.AgentOptions{})
	if options.Topic == "" {
		options.Topic = wire.AgentSessions
	}
	runtime, err := handle.AgentRuntime(agent, options)
	if err != nil {
		t.Fatalf("open the runtime: %v", err)
	}
	return runtime
}

// delivered builds the record one agent send would have landed as, at a
// position the test names.
func delivered(
	t *testing.T, partition uint32, offset uint64, build func(*laser.Agdx) *laser.AgdxSend,
) *laser.Record {
	t.Helper()
	server := &streamServer{}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	send := build(handle.Agdx(agentID(t, "planner"), conversation(1)))
	if _, err := send.Send(context.Background()); err != nil {
		t.Fatalf("build a record: %v", err)
	}
	return openDelivered(t, server.sent()[0], partition, offset)
}

// openDelivered reads a built record back as a consumer would hand it over.
func openDelivered(
	t *testing.T, message iggcon.IggyMessage, partition uint32, offset uint64,
) *laser.Record {
	t.Helper()
	message.Header.Offset = offset
	record, err := laser.OpenRecord(message)
	if err != nil {
		t.Fatalf("open the record: %v", err)
	}
	record.PartitionID = partition
	record.EndsBatch = true
	checkpoint, made := (&iggcon.PolledMessage{
		PartitionId: partition, Messages: []iggcon.IggyMessage{message},
	}).Checkpoint()
	if !made {
		t.Fatal("a delivered record has a checkpoint")
	}
	record.Checkpoint = checkpoint
	return record
}

// capsules reads back the dead letters that reached the server.
func capsules(t *testing.T, server *runtimeServer) []wire.AgentDeadLetter {
	t.Helper()
	var read []wire.AgentDeadLetter
	for _, message := range server.on(wire.AgentDLQ) {
		var capsule wire.AgentDeadLetter
		if err := wire.Unmarshal(message.Payload, &capsule); err != nil {
			t.Fatalf("decode the dead letter: %v", err)
		}
		read = append(read, capsule)
	}
	return read
}

// stripSpineHeaders leaves a record carrying only the envelope stamps that
// wire/fixtures/header_block_envelope.json pins, which is what an agent
// record produced by the Rust and TypeScript clients carries.
func stripSpineHeaders(t *testing.T, record *laser.Record) {
	t.Helper()
	entries, err := iggcon.DeserializeHeaders(record.Message.UserHeaders)
	if err != nil {
		t.Fatalf("read the headers: %v", err)
	}
	kept := make([]iggcon.HeaderEntry, 0, len(entries))
	for _, entry := range entries {
		switch string(entry.Key.Value) {
		case wire.HeaderAgentVersion, wire.HeaderContentType,
			wire.HeaderConversationID, wire.HeaderAgentID,
			wire.HeaderTargetAgentID, wire.HeaderParentConversation,
			wire.HeaderRootConversation:
			kept = append(kept, entry)
		}
	}
	rebuilt, err := iggcon.NewIggyMessage(record.Message.Payload,
		iggcon.WithUserHeaders(kept))
	if err != nil {
		t.Fatalf("rebuild the record: %v", err)
	}
	rebuilt.Header.Offset = record.Message.Header.Offset
	record.Message = rebuilt
	provenance, err := laser.ProvenanceFrom(&rebuilt)
	if err != nil {
		t.Fatalf("read the stripped spine: %v", err)
	}
	record.Provenance = provenance
}

// wholeNumber reads a scalar the contract wrote as a count.
//
// A small unsigned number comes back off the wire as a signed one, because
// that is the narrower encoding for it, so a reader that insisted on the kind
// it was written as would read nothing.
func wholeNumber(value wire.Value) uint64 {
	if value.Kind == wire.ValueInt && value.Int >= 0 {
		return uint64(value.Int)
	}
	return value.Uint
}

// envelopesOn reads back the agent messages that reached one topic.
func envelopesOn(t *testing.T, server *runtimeServer, topic string) []wire.AgentEnvelope {
	t.Helper()
	var read []wire.AgentEnvelope
	for _, message := range server.on(topic) {
		var envelope wire.AgentEnvelope
		if err := wire.Unmarshal(message.Payload, &envelope); err != nil {
			t.Fatalf("decode the envelope: %v", err)
		}
		read = append(read, envelope)
	}
	return read
}

// handling collects what reached a handler, so a test asserts on the work
// rather than on a side effect of it.
type handling struct {
	mutex    sync.Mutex
	bodies   []string
	fail     error
	attempts int
	panics   bool
}

func (record *handling) Handle(
	_ context.Context, message *laser.AgentMessage, _ *laser.AgentCtx,
) error {
	record.mutex.Lock()
	record.attempts++
	record.bodies = append(record.bodies, string(message.Body()))
	failure, panicking := record.fail, record.panics
	record.mutex.Unlock()
	if panicking {
		panic("the handler is broken")
	}
	return failure
}

func (record *handling) seen() []string {
	record.mutex.Lock()
	defer record.mutex.Unlock()
	return append([]string(nil), record.bodies...)
}

func (record *handling) tries() int {
	record.mutex.Lock()
	defer record.mutex.Unlock()
	return record.attempts
}

// A command addressed to this agent is its work, and its position is stored
// once the handler is done with it.
func TestGivenACommandForThisAgentWhenRunThenShouldHandleItAndCommitIt(t *testing.T) {
	server := newRuntimeServer()
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{})
	record := delivered(t, 2, 41, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(2), []byte("do it")).
			WithTarget(agentID(t, "executor"))
	})
	source := sourceOf(record)
	handler := &handling{}

	if err := runUntilDrained(t, runtime, source, handler); err != nil {
		t.Fatalf("run: %v", err)
	}

	if seen := handler.seen(); len(seen) != 1 || seen[0] != "do it" {
		t.Errorf("want the command body handled, got %v", seen)
	}
	if commits := source.commits(); len(commits) != 1 || commits[0] != 41 {
		t.Errorf("want the handled record committed, got %v", commits)
	}
}

// Iggy delivers a record to one member of a group, so a record addressed to
// another agent is skipped rather than handled here: the fix for two
// identities in one group is one group per agent, not this client quietly
// doing another agent's work.
func TestGivenACommandForAnotherAgentWhenRunThenShouldSkipItAndCommitIt(t *testing.T) {
	server := newRuntimeServer()
	var skipped []wire.Dispatch
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{
		Observer: laser.RuntimeObserver{
			OnSkip: func(_ *laser.AgentMessage, dispatch wire.Dispatch) {
				skipped = append(skipped, dispatch)
			},
		},
	})
	record := delivered(t, 0, 5, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(2), []byte("not yours")).
			WithTarget(agentID(t, "reviewer"))
	})
	source := sourceOf(record)
	handler := &handling{}

	if err := runUntilDrained(t, runtime, source, handler); err != nil {
		t.Fatalf("run: %v", err)
	}

	if seen := handler.seen(); len(seen) != 0 {
		t.Errorf("want another agent's work left alone, got %v", seen)
	}
	if len(skipped) != 1 || skipped[0] != wire.DispatchForeign {
		t.Errorf("want the record reported as another agent's, got %v", skipped)
	}
	if commits := source.commits(); len(commits) != 1 {
		t.Errorf("want the skipped record committed, got %v", commits)
	}
}

// A reply is nobody's work. It belongs to whoever asked, so it is offered to
// the hub the loop serves rather than handed to the handler.
func TestGivenAReplyWhenRunThenShouldOfferItToTheHubRatherThanTheHandler(t *testing.T) {
	server := newRuntimeServer()
	runtime := runtimeFor(t, server, "planner", laser.RuntimeOptions{})
	ticket, err := runtime.Replies().Subscribe(correlationID(9),
		laser.ExpectedReply{Session: conversation(1), Requester: agentID(t, "planner")})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	record := delivered(t, 0, 3, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Respond(correlationID(9), []byte("answered")).
			WithTarget(agentID(t, "planner"))
	})
	source := sourceOf(record)
	handler := &handling{}

	if err := runUntilDrained(t, runtime, source, handler); err != nil {
		t.Fatalf("run: %v", err)
	}

	if seen := handler.seen(); len(seen) != 0 {
		t.Errorf("want the reply kept from the handler, got %v", seen)
	}
	answer, err := ticket.Await(context.Background(), time.Second)
	if err != nil {
		t.Fatalf("the waiter must be woken by the reply: %v", err)
	}
	if string(answer.Envelope.Body) != "answered" {
		t.Errorf("want the answer handed to the waiter, got %q", answer.Envelope.Body)
	}
}

// An agent record's spine comes off its envelope, so a deadline the envelope
// names is read even when no header carries one.
func TestGivenAnEnvelopeDeadlineWithoutAHeaderWhenRunThenShouldReadItOffTheEnvelope(t *testing.T) {
	server := newRuntimeServer()
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{})
	record := delivered(t, 1, 8, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(2), []byte("too late")).
			WithTarget(agentID(t, "executor")).
			WithDeadline(time.Now().Add(-time.Minute))
	})
	// The spine headers are stripped, leaving the envelope stamps and the
	// envelope itself as the only place the deadline is written. That is what
	// an agent record produced by the other clients carries.
	stripSpineHeaders(t, record)
	source := sourceOf(record)
	handler := &handling{}

	if err := runUntilDrained(t, runtime, source, handler); err != nil {
		t.Fatalf("run: %v", err)
	}

	if seen := handler.seen(); len(seen) != 0 {
		t.Errorf("want the expired command kept from the handler, got %v", seen)
	}
	kept := capsules(t, server)
	if len(kept) != 1 {
		t.Fatalf("want the expired command kept, got %d dead letters", len(kept))
	}
	if kept[0].Reason != wire.DeadLetterDeadlineExceeded {
		t.Errorf("want the deadline reason, got %d", kept[0].Reason)
	}
}

// A record nobody is waiting for is kept rather than worked on, and its
// sender is told at once instead of waiting out a deadline that has already
// passed.
func TestGivenAnExpiredCommandWhenRunThenShouldKeepItAndAnswerItsSender(t *testing.T) {
	server := newRuntimeServer()
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{
		RespondOn: wire.AgentSessions,
	})
	record := delivered(t, 1, 12, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(4), []byte("stale")).
			WithTarget(agentID(t, "executor")).
			WithDeadline(time.Now().Add(-time.Second))
	})
	source := sourceOf(record)

	if err := runUntilDrained(t, runtime, source, &handling{}); err != nil {
		t.Fatalf("run: %v", err)
	}

	kept := capsules(t, server)
	if len(kept) != 1 {
		t.Fatalf("want one dead letter, got %d", len(kept))
	}
	if string(kept[0].Payload) == "" {
		t.Error("a dead letter keeps the original payload so the work can be replayed")
	}
	if kept[0].Source.StreamID != 7 || kept[0].Source.TopicID != 3 ||
		kept[0].Source.PartitionID != 1 || kept[0].Source.Offset != 12 {
		t.Errorf("want the poison record located, got %+v", kept[0].Source)
	}
	answers := envelopesOn(t, server, wire.AgentSessions)
	if len(answers) != 1 {
		t.Fatalf("want the sender answered, got %d records", len(answers))
	}
	if answers[0].Kind != wire.KindError {
		t.Errorf("want a failure, got %q", answers[0].Kind)
	}
	if answers[0].Correlation == nil || *answers[0].Correlation != correlationID(4) {
		t.Error("want the answer to carry the correlation its caller waits on")
	}
	var failure wire.AgentErrorBody
	if err := wire.Unmarshal(answers[0].Body, &failure); err != nil {
		t.Fatalf("decode the failure: %v", err)
	}
	if failure.Code != wire.ErrorDeadlineExceeded {
		t.Errorf("want the deadline code, got %d", failure.Code)
	}
	if reason, named := failure.Detail[laser.DeadLetterReasonDetail]; !named ||
		wholeNumber(reason) != uint64(wire.DeadLetterDeadlineExceeded) {
		t.Error("want the dead-letter reason in the detail, which the code alone does not say")
	}
	if commits := source.commits(); len(commits) != 1 {
		t.Errorf("want the kept record committed, got %v", commits)
	}
}

// A handler failure that a repeat may survive is retried, and the record is
// only kept once the budget is spent.
func TestGivenARetryableFailureWhenRunThenShouldRetryThenKeepTheRecord(t *testing.T) {
	server := newRuntimeServer()
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{
		Retry: laser.RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond},
	})
	record := delivered(t, 0, 1, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(2), []byte("work")).
			WithTarget(agentID(t, "executor"))
	})
	handler := &handling{fail: errors.New("the downstream is busy")}

	if err := runUntilDrained(t, runtime, sourceOf(record), handler); err != nil {
		t.Fatalf("run: %v", err)
	}

	if tries := handler.tries(); tries != 3 {
		t.Errorf("want every attempt spent, got %d", tries)
	}
	kept := capsules(t, server)
	if len(kept) != 1 {
		t.Fatalf("want the exhausted record kept, got %d", len(kept))
	}
	if kept[0].Reason != wire.DeadLetterRetryExhausted {
		t.Errorf("want the exhausted reason, got %d", kept[0].Reason)
	}
	if kept[0].Attempts != 3 {
		t.Errorf("want the attempts it cost recorded, got %d", kept[0].Attempts)
	}
}

// A handler that knows a repeat cannot help says so, and the record is kept
// at once rather than failing the same way four more times.
func TestGivenARefusedRecordWhenRunThenShouldKeepItWithoutRetrying(t *testing.T) {
	server := newRuntimeServer()
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{})
	record := delivered(t, 0, 1, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(2), []byte("work")).
			WithTarget(agentID(t, "executor"))
	})
	handler := &handling{fail: &laser.InvalidError{Reason: "the body names no order"}}

	if err := runUntilDrained(t, runtime, sourceOf(record), handler); err != nil {
		t.Fatalf("run: %v", err)
	}

	if tries := handler.tries(); tries != 1 {
		t.Errorf("want one attempt, got %d", tries)
	}
	kept := capsules(t, server)
	if len(kept) != 1 || kept[0].Reason != wire.DeadLetterRejected {
		t.Fatalf("want one rejected record, got %+v", kept)
	}
}

// A panicking handler is a bug in that handler, so its record is kept and the
// loop goes on rather than taking the process down.
func TestGivenAPanickingHandlerWhenRunThenShouldKeepTheRecordAndCarryOn(t *testing.T) {
	server := newRuntimeServer()
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{})
	first := delivered(t, 0, 1, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(2), []byte("boom")).
			WithTarget(agentID(t, "executor"))
	})
	second := delivered(t, 0, 2, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(3), []byte("after")).
			WithTarget(agentID(t, "executor"))
	})
	handler := &handling{panics: true}
	source := sourceOf(first, second)

	if err := runUntilDrained(t, runtime, source, handler); err != nil {
		t.Fatalf("run: %v", err)
	}

	if kept := capsules(t, server); len(kept) != 2 {
		t.Errorf("want both records kept, got %d", len(kept))
	}
	if commits := source.commits(); len(commits) != 2 {
		t.Errorf("want the loop to carry on past the panic, got %v", commits)
	}
}

// A dead letter that could not be published leaves its record uncommitted, so
// the work is read again rather than lost to a topic nobody wrote it to.
func TestGivenAnUnwritableDeadLetterWhenRunThenShouldLeaveTheRecordUncommitted(t *testing.T) {
	server := newRuntimeServer()
	server.refuse, server.refuseAlways = wire.AgentDLQ, true
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{})
	record := delivered(t, 0, 1, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(2), []byte("work")).
			WithTarget(agentID(t, "executor"))
	})
	source := sourceOf(record)
	handler := &handling{fail: &laser.InvalidError{Reason: "no"}}

	err := runUntilDrained(t, runtime, source, handler)

	if err == nil {
		t.Fatal("a record that could not be kept must end the run")
	}
	if commits := source.commits(); len(commits) != 0 {
		t.Errorf("want nothing committed, got %v", commits)
	}
}

// A brief outage must not lose a capsule, so the publish is retried in place.
func TestGivenAMomentarilyUnwritableDeadLetterWhenRunThenShouldRetryThePublish(t *testing.T) {
	server := newRuntimeServer()
	// The first capsule publish fails in a way a resend fixes, and the
	// second lands.
	server.refuse = wire.AgentDLQ
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{
		Retry: laser.RetryPolicy{MaxAttempts: 1, BaseDelay: time.Millisecond},
	})
	record := delivered(t, 0, 1, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(2), []byte("work")).
			WithTarget(agentID(t, "executor"))
	})
	source := sourceOf(record)

	err := runUntilDrained(t, runtime, source,
		&handling{fail: errors.New("the model is unreachable")})

	if err != nil {
		t.Fatalf("a retried publish must not end the run: %v", err)
	}
	if kept := capsules(t, server); len(kept) != 1 {
		t.Errorf("want the capsule written on the retry, got %d", len(kept))
	}
}

// A record the dedup window already saw must not be applied twice.
func TestGivenADuplicateWhenRunThenShouldSkipItAndCommitIt(t *testing.T) {
	server := newRuntimeServer()
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{})
	key, err := wire.NewIdempotencyKey("order-7")
	if err != nil {
		t.Fatalf("idempotency key: %v", err)
	}
	build := func(offset uint64) *laser.Record {
		return delivered(t, 0, offset, func(agdx *laser.Agdx) *laser.AgdxSend {
			return agdx.Command(correlationID(2), []byte("charge")).
				WithTarget(agentID(t, "executor")).WithIdempotencyKey(key)
		})
	}
	source := sourceOf(build(1), build(2))
	handler := &handling{}

	if err := runUntilDrained(t, runtime, source, handler); err != nil {
		t.Fatalf("run: %v", err)
	}

	if tries := handler.tries(); tries != 1 {
		t.Errorf("want the operation applied once, got %d", tries)
	}
	if commits := source.commits(); len(commits) != 2 {
		t.Errorf("want both records committed, got %v", commits)
	}
}

// A record whose spine does not decode cannot be routed or gated, so its
// bytes are kept and its position is committed: reading it again would only
// fail to decode again.
func TestGivenAnUndecodableRecordWhenRunThenShouldKeepItsBytesAndCommitIt(t *testing.T) {
	server := newRuntimeServer()
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{})
	message, err := iggcon.NewIggyMessage([]byte("raw bytes"))
	if err != nil {
		t.Fatalf("build a record: %v", err)
	}
	message.Header.Offset = 4
	checkpoint, _ := (&iggcon.PolledMessage{
		PartitionId: 1, Messages: []iggcon.IggyMessage{message},
	}).Checkpoint()
	source := &queuedSource{
		steps: []step{{err: &laser.UndecodableRecord{
			PartitionID: 1, Message: message, Checkpoint: checkpoint,
			Cause: &laser.CodecError{Reason: "read the record's headers"},
		}}},
		drained: make(chan struct{}),
	}

	if err := runUntilDrained(t, runtime, source, &handling{}); err != nil {
		t.Fatalf("run: %v", err)
	}

	kept := capsules(t, server)
	if len(kept) != 1 {
		t.Fatalf("want the bytes kept, got %d dead letters", len(kept))
	}
	if string(kept[0].Payload) != "raw bytes" {
		t.Errorf("want the payload kept verbatim, got %q", kept[0].Payload)
	}
	if kept[0].Reason != wire.DeadLetterDecodeFailed {
		t.Errorf("want the decode reason, got %d", kept[0].Reason)
	}
	if commits := source.commits(); len(commits) != 1 || commits[0] != 4 {
		t.Errorf("want the undecodable record committed, got %v", commits)
	}
}

// A caller cannot tell a command that was consumed from one that expired
// unconsumed unless the agent says it has it, so the acknowledgment goes out
// before the handler runs rather than after.
func TestGivenAPickupAcknowledgementWhenRunThenShouldReportWorkingBeforeTheHandler(t *testing.T) {
	server := newRuntimeServer()
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{
		RespondOn: wire.AgentSessions, AckOnPickup: true,
	})
	record := delivered(t, 0, 1, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(6), []byte("slow work")).
			WithTarget(agentID(t, "executor"))
	})
	var acknowledgedFirst bool
	handler := laser.HandlerFunc(func(
		_ context.Context, _ *laser.AgentMessage, _ *laser.AgentCtx,
	) error {
		acknowledgedFirst = len(server.on(wire.AgentSessions)) == 1
		return nil
	})

	if err := runUntilDrained(t, runtime, sourceOf(record), handler); err != nil {
		t.Fatalf("run: %v", err)
	}

	if !acknowledgedFirst {
		t.Error("want the pickup acknowledged before the handler runs")
	}
	sent := envelopesOn(t, server, wire.AgentSessions)
	if len(sent) != 1 {
		t.Fatalf("want one acknowledgment, got %d", len(sent))
	}
	if sent[0].TaskState == nil || *sent[0].TaskState != wire.TaskWorking {
		t.Errorf("want a working status, got %v", sent[0].TaskState)
	}
	if sent[0].Correlation == nil || *sent[0].Correlation != correlationID(6) {
		t.Error("want the acknowledgment on the caller's own correlation")
	}
}

// A runtime that acknowledges a pickup must know where to send it, so the
// combination is refused when it is built rather than failing per record.
func TestGivenNoReplyTopicWhenAcknowledgingPickupsThenShouldRefuseTheRuntime(t *testing.T) {
	handle := laser.FromClient(newRuntimeServer(), laser.Options{DefaultStream: "agents"})
	agent := handle.Agent(agentID(t, "executor"), laser.AgentOptions{})

	_, err := handle.AgentRuntime(agent, laser.RuntimeOptions{
		Topic: wire.AgentSessions, AckOnPickup: true,
	})

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want the runtime refused, got %v", err)
	}
}

// Middleware is where authorization sits, so a refusal keeps the record from
// the handler entirely rather than letting it run and undoing it after.
func TestGivenAMiddlewareRefusalWhenRunThenShouldKeepTheRecordFromTheHandler(t *testing.T) {
	server := newRuntimeServer()
	gate := &refusingMiddleware{refusal: &laser.InvalidError{Reason: "not permitted"}}
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{
		Middleware: []laser.AgentMiddleware{gate},
	})
	record := delivered(t, 0, 1, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(2), []byte("work")).
			WithTarget(agentID(t, "executor"))
	})
	handler := &handling{}

	if err := runUntilDrained(t, runtime, sourceOf(record), handler); err != nil {
		t.Fatalf("run: %v", err)
	}

	if tries := handler.tries(); tries != 0 {
		t.Errorf("want the handler never reached, got %d attempts", tries)
	}
	kept := capsules(t, server)
	if len(kept) != 1 || kept[0].Reason != wire.DeadLetterRejected {
		t.Fatalf("want one rejected record, got %+v", kept)
	}
}

// Middleware counts what the handler did, so it sees every attempt's outcome
// and its number rather than only the last one.
func TestGivenARetriedRecordWhenRunThenShouldReportEveryAttemptToMiddleware(t *testing.T) {
	server := newRuntimeServer()
	gate := &refusingMiddleware{}
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{
		Middleware: []laser.AgentMiddleware{gate},
		Retry:      laser.RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond},
	})
	record := delivered(t, 0, 1, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(2), []byte("work")).
			WithTarget(agentID(t, "executor"))
	})

	if err := runUntilDrained(t, runtime, sourceOf(record),
		&handling{fail: errors.New("busy")}); err != nil {
		t.Fatalf("run: %v", err)
	}

	if reported := gate.attempts(); len(reported) != 3 ||
		reported[0] != 1 || reported[2] != 3 {
		t.Errorf("want every attempt reported one-based, got %v", reported)
	}
}

// An operation this handler does not serve is somebody else's work, so it is
// skipped rather than handled: a model call an agent recorded for its own
// timeline must never become its own work.
func TestGivenAnUnservedOperationWhenRunThenShouldSkipIt(t *testing.T) {
	server := newRuntimeServer()
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{
		Operations: []string{wire.OperationExecuteTool},
	})
	record := delivered(t, 0, 1, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(2), []byte("generate")).
			WithTarget(agentID(t, "executor")).
			WithOperation(wire.OperationChat)
	})
	source := sourceOf(record)
	handler := &handling{}

	if err := runUntilDrained(t, runtime, source, handler); err != nil {
		t.Fatalf("run: %v", err)
	}

	if tries := handler.tries(); tries != 0 {
		t.Errorf("want an unserved operation left alone, got %d attempts", tries)
	}
	if commits := source.commits(); len(commits) != 1 {
		t.Errorf("want the skipped record committed, got %v", commits)
	}
}

// A stopping runtime lets the record in its hands finish rather than cutting
// a handler off mid-step.
func TestGivenACancelledContextWhenRunningThenShouldFinishTheRecordInHand(t *testing.T) {
	server := newRuntimeServer()
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{})
	record := delivered(t, 0, 1, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(2), []byte("long")).
			WithTarget(agentID(t, "executor"))
	})
	source := sourceOf(record)
	started := make(chan struct{})
	finished := make(chan struct{})
	handler := laser.HandlerFunc(func(
		ctx context.Context, _ *laser.AgentMessage, _ *laser.AgentCtx,
	) error {
		close(started)
		// The handler goes on working past the stop, which is what the
		// grace is for.
		time.Sleep(20 * time.Millisecond)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		close(finished)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx, source, handler) }()
	<-started
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("a graceful stop is not a failure: %v", err)
	}
	select {
	case <-finished:
	default:
		t.Error("want the record in hand finished rather than cut off")
	}
	if commits := source.commits(); len(commits) != 1 {
		t.Errorf("want the finished record committed, got %v", commits)
	}
}

// refusingMiddleware refuses before the handler and counts what it saw after.
type refusingMiddleware struct {
	mutex   sync.Mutex
	refusal error
	seen    []uint32
}

func (gate *refusingMiddleware) BeforeHandle(
	_ context.Context, _ *laser.AgentMessage,
) error {
	return gate.refusal
}

func (gate *refusingMiddleware) AfterHandle(
	_ context.Context, _ *laser.AgentMessage, _ error, attempt uint32,
) {
	gate.mutex.Lock()
	defer gate.mutex.Unlock()
	gate.seen = append(gate.seen, attempt)
}

func (gate *refusingMiddleware) attempts() []uint32 {
	gate.mutex.Lock()
	defer gate.mutex.Unlock()
	return append([]uint32(nil), gate.seen...)
}

// A retryable failure is one a repeat may survive, and a refusal is not, which
// is what decides whether a record is retried or kept at once.
func TestGivenAFailureWhenClassifiedThenShouldRetryOnlyWhatARepeatMaySurvive(t *testing.T) {
	cases := []struct {
		name    string
		failure error
		retry   bool
	}{
		{"a bare handler failure", errors.New("busy"), true},
		{"a refusal", &laser.InvalidError{Reason: "no"}, false},
		{"a body that will not decode", &laser.CodecError{Reason: "no"}, false},
		{"a surface nobody serves", &laser.UnsupportedError{Surface: "queries"}, false},
		{"a feature this build lacks", &laser.UnderstoodError{Unmet: 4}, false},
		{"a cancelled context", context.Canceled, false},
		{"a dropped link", &laser.TransportError{Reason: "publish"}, true},
	}
	for _, testCase := range cases {
		if laser.RetryableError(testCase.failure) != testCase.retry {
			t.Errorf("%s: want retryable %v", testCase.name, testCase.retry)
		}
	}
	if laser.RetryableError(nil) {
		t.Error("there is nothing to retry when nothing failed")
	}
}

// The backoff doubles per attempt and is capped, so a long-lived retry loop
// cannot overflow its delay into a negative one.
func TestGivenRepeatedAttemptsWhenBackingOffThenShouldDoubleWithoutOverflowing(t *testing.T) {
	policy := laser.RetryPolicy{MaxAttempts: 5, BaseDelay: 10 * time.Millisecond}

	if delay := policy.DelayFor(0); delay != 10*time.Millisecond {
		t.Errorf("want the base delay first, got %s", delay)
	}
	if delay := policy.DelayFor(3); delay != 80*time.Millisecond {
		t.Errorf("want the delay doubled per attempt, got %s", delay)
	}
	if delay := policy.DelayFor(4096); delay <= 0 {
		t.Errorf("want a capped positive delay, got %s", delay)
	}
}

// The natural reading of the gates is deadline first, and it is wrong. A
// record that is both stale and expired is a replay of work a newer fence
// already settled, so there is nothing to keep: dead-lettering it would fill
// the topic with copies of work that already happened.
//
// This fails if the two gates are ever reordered back.
func TestGivenARecordBothStaleAndExpiredWhenDecidedThenShouldSkipItRatherThanKeepIt(t *testing.T) {
	agent := laser.FromClient(&streamServer{}, laser.Options{}).
		Agent(agentID(t, "executor"), laser.AgentOptions{})
	past := time.Now().Add(-time.Minute)

	ahead := agent.Decide(recordWith(&laser.Provenance{
		Conversation: conversation(1), FenceToken: uint64Pointer(5),
	}))
	if ahead.Verdict != laser.Deliver {
		t.Fatalf("want the holder's own record delivered, got %s", ahead.Verdict)
	}

	stale := agent.Decide(recordWith(&laser.Provenance{
		Conversation: conversation(1),
		FenceToken:   uint64Pointer(4),
		DeadlineAt:   &past,
	}))

	if stale.Verdict != laser.Skip {
		t.Fatalf("want the stale replay skipped rather than kept, got %s", stale.Verdict)
	}
	// An expired record that is not stale is still kept, so the skip above is
	// the fence answering first rather than the deadline gate being gone.
	expired := agent.Decide(recordWith(&laser.Provenance{
		Conversation: conversation(2), DeadlineAt: &past,
	}))
	if expired.Verdict != laser.DeadLetter ||
		expired.Reason != wire.DeadLetterDeadlineExceeded {
		t.Errorf("want an expired record kept, got %s", expired.Verdict)
	}
}

// A record whose spine did not decode is refused with the record attached, so
// a runtime can keep its bytes. A caller matching on the reason underneath
// must still find it, or every existing reader of that refusal breaks in
// silence.
func TestGivenAnUndecodableRecordWhenRefusedThenShouldStillMatchItsCause(t *testing.T) {
	cause := &laser.CodecError{Reason: "read the record's headers"}
	refusal := error(&laser.UndecodableRecord{Cause: cause})

	var codec *laser.CodecError
	if !errors.As(refusal, &codec) {
		t.Fatal("want the cause still matchable through the refusal")
	}
	if codec != cause {
		t.Error("want the original cause, not a copy of it")
	}
	if !errors.Is(refusal, error(cause)) {
		t.Error("want the refusal to report the cause it wraps")
	}
	var unreadable *laser.UndecodableRecord
	if !errors.As(refusal, &unreadable) {
		t.Fatal("want the refusal itself matchable")
	}
}

// budgetRuntime opens a runtime over a steerable session index.
func budgetRuntime(
	t *testing.T, server *budgetServer, name string,
) *laser.AgentRuntime {
	t.Helper()
	server.session = conversation(1)
	capabilities := laser.Capabilities{Managed: true, Sessions: true}
	handle := laser.FromClient(server, laser.Options{
		DefaultStream: "agents", Capabilities: &capabilities,
	})
	agent := handle.Agent(agentID(t, name), laser.AgentOptions{})
	runtime, err := handle.AgentRuntime(agent, laser.RuntimeOptions{
		Topic: wire.AgentSessions, Budget: laser.NewBudgetGate(),
	})
	if err != nil {
		t.Fatalf("open the runtime: %v", err)
	}
	return runtime
}

// A session over its budget stops spending, so its work is kept from the
// handler rather than left to run up a bill nobody authorized.
func TestGivenASessionOverItsBudgetWhenRunThenShouldKeepItsWorkFromTheHandler(t *testing.T) {
	server := &budgetServer{}
	runtime := budgetRuntime(t, server, "executor")
	server.answerIndex(&wire.SessionInfo{
		Status: wire.SessionActive, OverBudget: true,
		TokensIn: 110, Budget: &wire.Budget{Tokens: uintOf(100)},
	})
	record := delivered(t, 0, 1, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(2), []byte("spend more")).
			WithTarget(agentID(t, "executor"))
	})
	source := sourceOf(record)
	handler := &handling{}

	if err := runUntilDrained(t, runtime, source, handler); err != nil {
		t.Fatalf("run: %v", err)
	}

	if tries := handler.tries(); tries != 0 {
		t.Errorf("want the work kept from the handler, got %d attempts", tries)
	}
	if commits := source.commits(); len(commits) != 1 {
		t.Errorf("want the record committed, got %v", commits)
	}
}

// Records of one poll share a verdict, so the index is read once for the
// batch rather than once per record, and read again for the next batch.
func TestGivenRecordsOfOneBatchWhenRunThenShouldReadTheBudgetIndexOncePerBatch(t *testing.T) {
	server := &budgetServer{}
	runtime := budgetRuntime(t, server, "executor")
	server.answerIndex(&wire.SessionInfo{Status: wire.SessionActive})
	build := func(offset uint64, endsBatch bool) *laser.Record {
		record := delivered(t, 0, offset, func(agdx *laser.Agdx) *laser.AgdxSend {
			return agdx.Command(correlationID(byte(offset)), []byte("work")).
				WithTarget(agentID(t, "executor"))
		})
		record.EndsBatch = endsBatch
		return record
	}
	source := sourceOf(build(1, false), build(2, true), build(3, true))

	if err := runUntilDrained(t, runtime, source, &handling{}); err != nil {
		t.Fatalf("run: %v", err)
	}

	if reads := server.indexed(); reads != 2 {
		t.Errorf("want one index read per batch, got %d", reads)
	}
}

// A capsule must not be dropped for the very deadline that killed the record
// it carries, so the deadline is cleared on the way out.
func TestGivenAnExpiredRecordWhenKeptThenShouldClearTheDeadlineOnTheCapsule(t *testing.T) {
	server := newRuntimeServer()
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{})
	record := delivered(t, 0, 1, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(2), []byte("stale")).
			WithTarget(agentID(t, "executor")).
			WithDeadline(time.Now().Add(-time.Second))
	})

	if err := runUntilDrained(t, runtime, sourceOf(record), &handling{}); err != nil {
		t.Fatalf("run: %v", err)
	}

	written := server.on(wire.AgentDLQ)
	if len(written) != 1 {
		t.Fatalf("want one capsule, got %d", len(written))
	}
	spine, err := laser.ProvenanceFrom(&written[0])
	if err != nil {
		t.Fatalf("read the capsule spine: %v", err)
	}
	if spine.DeadlineAt != nil {
		t.Error("a capsule carrying a passed deadline would be dropped by its own reader")
	}
	if spine.Conversation != conversation(1) {
		t.Errorf("want the capsule on the record's own session, got %v", spine.Conversation)
	}
	if spine.CausalParent == nil || spine.CausalParent.Offset != 1 {
		t.Errorf("want the capsule pointed at the poison record, got %v", spine.CausalParent)
	}
}

// A fenced effect carries its fence in the envelope, so a stale holder's
// replay is refused there the same way a header-borne one is.
func TestGivenAStaleFenceInTheEnvelopeWhenRunThenShouldSkipTheReplay(t *testing.T) {
	server := newRuntimeServer()
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{})
	fenced := func(offset uint64, fence uint64) *laser.Record {
		return delivered(t, 0, offset, func(agdx *laser.Agdx) *laser.AgdxSend {
			return agdx.Command(correlationID(byte(offset)), []byte("apply")).
				WithTarget(agentID(t, "executor")).
				WithMetadata(wire.HeaderFence, wire.UintValue(fence))
		})
	}
	source := sourceOf(fenced(1, 5), fenced(2, 4))
	handler := &handling{}

	if err := runUntilDrained(t, runtime, source, handler); err != nil {
		t.Fatalf("run: %v", err)
	}

	if tries := handler.tries(); tries != 1 {
		t.Errorf("want the stale holder's replay refused, got %d attempts", tries)
	}
	if kept := capsules(t, server); len(kept) != 0 {
		t.Errorf("a replay is not poison, so nothing is kept, got %d", len(kept))
	}
	if commits := source.commits(); len(commits) != 2 {
		t.Errorf("want both records committed, got %v", commits)
	}
}

// A dead letter belongs on its session's timeline, so the conversation is
// read off the header even when the rest of the spine did not decode.
func TestGivenAnUndecodableRecordNamingItsSessionWhenKeptThenShouldStayOnIt(t *testing.T) {
	server := newRuntimeServer()
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{})
	headers, err := (&laser.Provenance{Conversation: conversation(5)}).Headers()
	if err != nil {
		t.Fatalf("encode the spine: %v", err)
	}
	fence, err := iggcon.NewHeaderKeyString(wire.HeaderFence)
	if err != nil {
		t.Fatalf("header key: %v", err)
	}
	headers = append(headers, iggcon.HeaderEntry{
		Key:   fence,
		Value: iggcon.HeaderValue{Kind: iggcon.String, Value: []byte("not a number")},
	})
	message, err := iggcon.NewIggyMessage([]byte("body"), iggcon.WithUserHeaders(headers))
	if err != nil {
		t.Fatalf("build a record: %v", err)
	}
	if _, err := laser.OpenRecord(message); err == nil {
		t.Fatal("this record must not decode, or the test proves nothing")
	}
	checkpoint, _ := (&iggcon.PolledMessage{
		PartitionId: 0, Messages: []iggcon.IggyMessage{message},
	}).Checkpoint()
	source := &queuedSource{
		steps: []step{{err: &laser.UndecodableRecord{
			Message: message, Checkpoint: checkpoint,
			Cause: &laser.CodecError{Reason: "the fence is not a number"},
		}}},
		drained: make(chan struct{}),
	}

	if err := runUntilDrained(t, runtime, source, &handling{}); err != nil {
		t.Fatalf("run: %v", err)
	}

	written := server.on(wire.AgentDLQ)
	if len(written) != 1 {
		t.Fatalf("want one capsule, got %d", len(written))
	}
	spine, err := laser.ProvenanceFrom(&written[0])
	if err != nil {
		t.Fatalf("read the capsule spine: %v", err)
	}
	if spine.Conversation != conversation(5) {
		t.Errorf("want the capsule on the record's own session, got %v", spine.Conversation)
	}
}

// An agent told to take only work addressed to it by name leaves a broadcast
// alone, which is what a deployment that fans one topic out by hand wants.
func TestGivenABroadcastWhenTheAgentWantsNamedWorkThenShouldLeaveItAlone(t *testing.T) {
	server := newRuntimeServer()
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	agent := handle.Agent(agentID(t, "executor"), laser.AgentOptions{
		AcceptBroadcast: boolPointer(false),
	})
	runtime, err := handle.AgentRuntime(agent, laser.RuntimeOptions{
		Topic: wire.AgentSessions,
	})
	if err != nil {
		t.Fatalf("open the runtime: %v", err)
	}
	record := delivered(t, 0, 1, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(2), []byte("anyone")).Broadcast()
	})
	source := sourceOf(record)
	handler := &handling{}

	if err := runUntilDrained(t, runtime, source, handler); err != nil {
		t.Fatalf("run: %v", err)
	}

	if tries := handler.tries(); tries != 0 {
		t.Errorf("want the broadcast left alone, got %d attempts", tries)
	}
	if commits := source.commits(); len(commits) != 1 {
		t.Errorf("want the skipped record committed, got %v", commits)
	}
}

// A reply addressed to another agent is that agent's answer. A waiter here
// that would take any reply must not be woken by it.
func TestGivenAnotherAgentsReplyWhenRunThenShouldNotWakeAWaiterHere(t *testing.T) {
	server := newRuntimeServer()
	runtime := runtimeFor(t, server, "planner", laser.RuntimeOptions{})
	ticket, err := runtime.Replies().Subscribe(correlationID(9),
		laser.ExpectedReply{Session: conversation(1)})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	record := delivered(t, 0, 3, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Respond(correlationID(9), []byte("not yours")).
			WithTarget(agentID(t, "reviewer"))
	})

	if err := runUntilDrained(t, runtime, sourceOf(record), &handling{}); err != nil {
		t.Fatalf("run: %v", err)
	}

	if _, err := ticket.Await(context.Background(), 20*time.Millisecond); err == nil {
		t.Error("another agent's reply must not answer a waiter here")
	}
}
