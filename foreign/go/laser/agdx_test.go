package laser_test

import (
	"context"
	"errors"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func agentID(t *testing.T, name string) wire.AgentID {
	t.Helper()
	agent, err := wire.NewAgentID(name)
	if err != nil {
		t.Fatalf("agent id %q: %v", name, err)
	}
	return agent
}

func correlationID(marker byte) wire.CorrelationID {
	return wire.CorrelationID{ID: wire.IDFromBytes([16]byte{15: marker})}
}

// sentEnvelope reads back the agent envelope that reached the server.
func sentEnvelope(t *testing.T, server *streamServer) wire.AgentEnvelope {
	t.Helper()
	if len(server.sentMessages) == 0 {
		t.Fatal("nothing was sent")
	}
	var envelope wire.AgentEnvelope
	if err := wire.Unmarshal(server.sentMessages[0].Payload, &envelope); err != nil {
		t.Fatalf("decode the envelope: %v", err)
	}
	return envelope
}

func agdxHandle(server *streamServer) *laser.Laser {
	return streamHandle(server, laser.Options{DefaultStream: "agents"})
}

// The conversation is the partition key, so one exchange stays ordered.
func TestGivenAnAgentCommandWhenSentThenShouldPartitionByTheConversation(t *testing.T) {
	server := &streamServer{}
	agdx := agdxHandle(server).Agdx(agentID(t, "planner"), conversation(1))

	receipt, err := agdx.Command(correlationID(2), []byte(`{"do":"it"}`)).
		WithTarget(agentID(t, "executor")).Send(context.Background())
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	if string(server.sentPartitions.Value) != conversation(1).String() {
		t.Errorf("want the conversation as the key, got %q", server.sentPartitions.Value)
	}
	envelope := sentEnvelope(t, server)
	if envelope.Kind != wire.KindCommand {
		t.Errorf("want a command, got %q", envelope.Kind)
	}
	if envelope.Source != agentID(t, "planner") {
		t.Errorf("want the author, got %v", envelope.Source)
	}
	if envelope.Target == nil || *envelope.Target != agentID(t, "executor") {
		t.Errorf("want the addressee, got %v", envelope.Target)
	}
	if envelope.Correlation == nil || *envelope.Correlation != correlationID(2) {
		t.Errorf("want the correlation, got %v", envelope.Correlation)
	}
	if receipt.Record.IsZero() {
		t.Error("a receipt must name where the message landed")
	}
	if envelope.Record == nil || *envelope.Record != receipt.Record {
		t.Error("the receipt and the envelope must name the same record")
	}
}

// A reader needs the routing and causality to decide anything, so the whole
// envelope is the body rather than only its content.
func TestGivenAnAgentMessageWhenSentThenShouldCarryTheWholeEnvelopeAsTheBody(t *testing.T) {
	server := &streamServer{}
	agdx := agdxHandle(server).Agdx(agentID(t, "planner"), conversation(1))

	if _, err := agdx.Emit([]byte("content")).Send(context.Background()); err != nil {
		t.Fatalf("send: %v", err)
	}

	envelope := sentEnvelope(t, server)
	if string(envelope.Body) != "content" {
		t.Errorf("want the content inside the envelope, got %q", envelope.Body)
	}
}

// The envelope stamps and the provenance spine ride one header block, so a
// reader routes and audits without decoding the body.
func TestGivenAnAgentMessageWhenSentThenShouldCarryBothHeaderBlocks(t *testing.T) {
	server := &streamServer{}
	agdx := agdxHandle(server).Agdx(agentID(t, "planner"), conversation(1))

	if _, err := agdx.Command(correlationID(2), []byte("x")).
		WithTarget(agentID(t, "executor")).Send(context.Background()); err != nil {
		t.Fatalf("send: %v", err)
	}

	headers := sentHeaders(t, server)
	if headers[wire.HeaderConversationID] != conversation(1).String() {
		t.Errorf("want the conversation stamped, got %q", headers[wire.HeaderConversationID])
	}
	if headers[wire.HeaderAgentID] != "planner" {
		t.Errorf("want the author stamped, got %q", headers[wire.HeaderAgentID])
	}
	if headers[wire.HeaderTargetAgentID] != "executor" {
		t.Errorf("want the addressee stamped, got %q", headers[wire.HeaderTargetAgentID])
	}
	if headers[wire.HeaderCorrelationID] == "" {
		t.Error("want the correlation stamped for reply pairing")
	}
	if headers[wire.HeaderAgentVersion] == "" {
		t.Error("want the envelope version stamped")
	}
}

// Every record on a shared session topic names an addressee, so one addressed
// to nobody in particular broadcasts.
func TestGivenABroadcastWhenSentThenShouldAddressEveryAgent(t *testing.T) {
	server := &streamServer{}
	agdx := agdxHandle(server).Agdx(agentID(t, "planner"), conversation(1))

	if _, err := agdx.Emit([]byte("x")).Broadcast().Send(context.Background()); err != nil {
		t.Fatalf("send: %v", err)
	}

	headers := sentHeaders(t, server)
	if headers[wire.HeaderTargetAgentID] != wire.Broadcast {
		t.Errorf("want the broadcast addressee, got %q", headers[wire.HeaderTargetAgentID])
	}
	envelope := sentEnvelope(t, server)
	if envelope.Target != nil {
		t.Errorf("a broadcast names no target in the envelope, got %v", envelope.Target)
	}
}

// A chunk is identified by its channel and sequence, which is why it is the
// one kind that carries no record id.
func TestGivenAChunkWhenSentThenShouldBeIdentifiedByChannelAndSequence(t *testing.T) {
	server := &streamServer{}
	agdx := agdxHandle(server).Agdx(agentID(t, "planner"), conversation(1))
	channel := wire.ChannelID{ID: wire.IDFromBytes([16]byte{15: 5})}

	if _, err := agdx.Chunk(correlationID(2), channel, 3, []byte("piece")).
		Send(context.Background()); err != nil {
		t.Fatalf("send: %v", err)
	}

	envelope := sentEnvelope(t, server)
	if envelope.Kind != wire.KindChunk {
		t.Errorf("want a chunk, got %q", envelope.Kind)
	}
	if envelope.Record != nil {
		t.Error("a chunk carries no record id")
	}
	if envelope.Channel == nil || *envelope.Channel != channel {
		t.Errorf("want the channel, got %v", envelope.Channel)
	}
	if envelope.Sequence == nil || *envelope.Sequence != 3 {
		t.Errorf("want the sequence, got %v", envelope.Sequence)
	}
}

// A status is discriminated by its operation, and the contract requires a
// correlation and a state on a task status, so that shape has its own
// constructor rather than a chain a caller can leave half built.
func TestGivenATaskStatusWhenSentThenShouldCarryItsCorrelationAndState(t *testing.T) {
	server := &streamServer{}
	agdx := agdxHandle(server).Agdx(agentID(t, "planner"), conversation(1))

	if _, err := agdx.TaskStatus(correlationID(2), wire.TaskWorking).
		Send(context.Background()); err != nil {
		t.Fatalf("send: %v", err)
	}

	envelope := sentEnvelope(t, server)
	if envelope.Kind != wire.KindStatus {
		t.Errorf("want a status, got %q", envelope.Kind)
	}
	if envelope.Operation == nil || *envelope.Operation != wire.OperationTask {
		t.Errorf("want the task operation, got %v", envelope.Operation)
	}
	if envelope.Correlation == nil || *envelope.Correlation != correlationID(2) {
		t.Errorf("want the correlation, got %v", envelope.Correlation)
	}
	if envelope.TaskState == nil || *envelope.TaskState != wire.TaskWorking {
		t.Errorf("want the state, got %v", envelope.TaskState)
	}
}

// A session status belongs to the session rather than to one exchange, so it
// carries a state but names no correlation.
func TestGivenASessionProgressWhenSentThenShouldCarryItsStateAlone(t *testing.T) {
	server := &streamServer{}
	agdx := agdxHandle(server).Agdx(agentID(t, "planner"), conversation(1))

	if _, err := agdx.SessionProgressed(wire.SessionTransition{}).
		Send(context.Background()); err != nil {
		t.Fatalf("send: %v", err)
	}

	envelope := sentEnvelope(t, server)
	if envelope.Operation == nil || *envelope.Operation != wire.OperationSession {
		t.Errorf("want the session operation, got %v", envelope.Operation)
	}
	if envelope.TaskState == nil {
		t.Error("want the state carried")
	}
	if envelope.Correlation != nil {
		t.Errorf("a session status names no correlation, got %v", envelope.Correlation)
	}
	// A working session is not finished, so it is not terminal.
	if envelope.Last {
		t.Error("a non-terminal state must not mark the session finished")
	}
}

// The contract requires the terminal flag and the state to agree, so deriving
// the flag is the only way the two cannot drift apart.
func TestGivenATerminalSessionStateWhenSentThenShouldMarkItLast(t *testing.T) {
	server := &streamServer{}
	agdx := agdxHandle(server).Agdx(agentID(t, "planner"), conversation(1))

	if _, err := agdx.SessionEnded(wire.TaskCompleted, wire.SessionEnd{}).
		Send(context.Background()); err != nil {
		t.Fatalf("send: %v", err)
	}

	envelope := sentEnvelope(t, server)
	if !envelope.Last {
		t.Error("a terminal state must mark the session finished")
	}
}

// A session that is still running has not ended, so a non-terminal state is
// refused by the call that would otherwise set the flag wrong.
func TestGivenANonTerminalStateWhenEndingASessionThenShouldRefuseIt(t *testing.T) {
	server := &streamServer{}
	agdx := agdxHandle(server).Agdx(agentID(t, "planner"), conversation(1))

	_, err := agdx.SessionEnded(wire.TaskWorking, wire.SessionEnd{}).
		Send(context.Background())

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
	if len(server.calls) != 0 {
		t.Error("a refused status must not reach the socket")
	}
}

// Starting a session carries what it was started with, and the ancestry in the
// body has to match the envelope's or the two disagree about the same tree.
func TestGivenASessionStartWhenSentThenShouldCarryItsOwnRecord(t *testing.T) {
	server := &streamServer{}
	agdx := agdxHandle(server).Agdx(agentID(t, "planner"), conversation(1))
	label := "nightly-run"

	if _, err := agdx.SessionStarted(wire.SessionStart{
		Label: &label,
		Agent: agentID(t, "planner"),
		SDK:   wire.SdkInfo{Language: "go", Version: "0.1.0"},
	}).Send(context.Background()); err != nil {
		t.Fatalf("send: %v", err)
	}

	envelope := sentEnvelope(t, server)
	if envelope.TaskState == nil || *envelope.TaskState != wire.TaskSubmitted {
		t.Errorf("want a submitted session, got %v", envelope.TaskState)
	}
	var start wire.SessionStart
	if err := wire.Unmarshal(envelope.Body, &start); err != nil {
		t.Fatalf("decode the session start: %v", err)
	}
	if start.Label == nil || *start.Label != "nightly-run" {
		t.Errorf("want the label carried, got %v", start.Label)
	}
	if envelope.Last {
		t.Error("a session that just started has not finished")
	}
}

// The remaining operations carry no state, which is why they use the plain
// constructor.
func TestGivenAProgressStatusWhenSentThenShouldNeedNeither(t *testing.T) {
	server := &streamServer{}
	agdx := agdxHandle(server).Agdx(agentID(t, "planner"), conversation(1))

	if _, err := agdx.Status(wire.OperationProgress).Send(context.Background()); err != nil {
		t.Fatalf("send: %v", err)
	}

	envelope := sentEnvelope(t, server)
	if envelope.Operation == nil || *envelope.Operation != wire.OperationProgress {
		t.Errorf("want the progress operation, got %v", envelope.Operation)
	}
}

// Cost rides the envelope in integer micro-units and the header in whole
// currency, so the two spell one amount differently and must be converted
// rather than copied.
func TestGivenUsageWhenSentThenShouldSpellCostForEachCarrier(t *testing.T) {
	server := &streamServer{}
	agdx := agdxHandle(server).Agdx(agentID(t, "planner"), conversation(1))
	cost := uint64(12_500)

	if _, err := agdx.Emit([]byte("x")).WithUsage(wire.TokenUsage{
		InputTokens: 120, OutputTokens: 45, CostMicros: &cost,
	}).Send(context.Background()); err != nil {
		t.Fatalf("send: %v", err)
	}

	envelope := sentEnvelope(t, server)
	if envelope.Usage == nil || envelope.Usage.CostMicros == nil ||
		*envelope.Usage.CostMicros != 12_500 {
		t.Errorf("want the micro-units in the envelope, got %+v", envelope.Usage)
	}
	record, err := laser.OpenRecord(server.sentMessages[0])
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if record.Provenance == nil || record.Provenance.Usage == nil ||
		record.Provenance.Usage.CostUSD == nil {
		t.Fatalf("want the cost on the headers, got %+v", record.Provenance)
	}
	if *record.Provenance.Usage.CostUSD != 0.0125 {
		t.Errorf("want whole currency on the header, got %v",
			*record.Provenance.Usage.CostUSD)
	}
	if record.Provenance.Usage.InputTokens == nil ||
		*record.Provenance.Usage.InputTokens != 120 {
		t.Errorf("want the tokens on the headers, got %+v", record.Provenance.Usage)
	}
}

// A sent message has to be readable by the same client, or two Laser peers
// cannot talk to each other.
func TestGivenASentMessageWhenReadBackThenShouldOpenAsTheSameExchange(t *testing.T) {
	server := &streamServer{}
	agdx := agdxHandle(server).Agdx(agentID(t, "planner"), conversation(1)).
		WithAncestry(conversation(2), conversation(3))
	deadline := time.UnixMicro(1_700_000_000_000_000)

	if _, err := agdx.Command(correlationID(4), []byte(`{"do":"it"}`)).
		WithTarget(agentID(t, "executor")).
		WithDeadline(deadline).
		WithTool("search").
		Send(context.Background()); err != nil {
		t.Fatalf("send: %v", err)
	}

	record, err := laser.OpenRecord(server.sentMessages[0])
	if err != nil {
		t.Fatalf("open the record: %v", err)
	}
	message, err := laser.OpenAgdx(record)
	if err != nil {
		t.Fatalf("open the envelope: %v", err)
	}

	if message.Envelope.Kind != wire.KindCommand {
		t.Errorf("want a command, got %q", message.Envelope.Kind)
	}
	if message.Envelope.Parent == nil || *message.Envelope.Parent != conversation(2) {
		t.Errorf("want the parent, got %v", message.Envelope.Parent)
	}
	if message.Envelope.Root == nil || *message.Envelope.Root != conversation(3) {
		t.Errorf("want the root, got %v", message.Envelope.Root)
	}
	if message.Envelope.Tool == nil || *message.Envelope.Tool != "search" {
		t.Errorf("want the tool, got %v", message.Envelope.Tool)
	}
	if record.Provenance == nil || record.Provenance.DeadlineAt == nil ||
		!record.Provenance.DeadlineAt.Equal(deadline) {
		t.Errorf("want the deadline on the headers, got %+v", record.Provenance)
	}
	if !message.AddressedTo(agentID(t, "executor")) {
		t.Error("want the message addressed to its target")
	}
	if message.AddressedTo(agentID(t, "someone-else")) {
		t.Error("a targeted message is not addressed to another agent")
	}
}

// A message addressed to nobody is for whoever is reading.
func TestGivenAnUntargetedMessageWhenCheckedThenShouldBeForAnyone(t *testing.T) {
	message := &laser.AgdxMessage{Envelope: wire.AgentEnvelope{}}
	if !message.AddressedTo(agentID(t, "anyone")) {
		t.Error("an untargeted message is addressed to whoever reads it")
	}
}

// A record that is not an agent message would decode into an empty envelope,
// which would route as a command from nobody.
func TestGivenANonAgentRecordWhenOpenedThenShouldRefuseIt(t *testing.T) {
	server := &streamServer{}

	if _, err := agdxHandle(server).Topic(wire.AgentSessions).Publish().
		Payload([]byte("not an envelope")).Key("k").Send(context.Background()); err != nil {
		t.Fatalf("publish: %v", err)
	}
	record, err := laser.OpenRecord(server.sentMessages[0])
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if _, err := laser.OpenAgdx(record); err == nil {
		t.Error("a record that is not an agent message must be refused")
	}
}

// A must-understand bit a receiver does not implement means the message must
// not be processed, which is the whole point of the bitset.
func TestGivenAnUnmetRequirementWhenCheckedThenShouldReportItNotUnderstood(t *testing.T) {
	message := &laser.AgdxMessage{
		Envelope: wire.AgentEnvelope{MustUnderstand: 1 << 3},
	}

	if message.Understood(wire.FeatureNone) {
		t.Error("a receiver that implements nothing must not claim to understand")
	}
	if !message.Understood(1 << 3) {
		t.Error("a receiver that implements the bit understands the message")
	}

	open := &laser.AgdxMessage{Envelope: wire.AgentEnvelope{}}
	if !open.Understood(wire.FeatureNone) {
		t.Error("the open-world default is understood by everyone")
	}
}

// An envelope that fails the contract must not reach the log, where every
// reader would then have to cope with it.
func TestGivenAnInvalidEnvelopeWhenSentThenShouldRefuseBeforeSending(t *testing.T) {
	server := &streamServer{}
	agdx := agdxHandle(server).Agdx(agentID(t, "planner"), conversation(1))

	// The operation is a closed vocabulary, so one outside it is refused.
	_, err := agdx.Status("not-an-operation").Send(context.Background())

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
	if len(server.calls) != 0 {
		t.Error("an invalid envelope must not reach the socket")
	}
}

func TestGivenNoStreamWhenSendingAnAgentMessageThenShouldRefuseIt(t *testing.T) {
	server := &streamServer{}
	agdx := streamHandle(server, laser.Options{}).
		Agdx(agentID(t, "planner"), conversation(1))

	_, err := agdx.Emit([]byte("x")).Send(context.Background())

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
}

// The two header blocks overlap on the conversation and the agent ids. A key
// written twice wastes bytes against the soft cap and leaves a reader to guess
// which copy is authoritative, so the block carries each key once.
func TestGivenBothHeaderBlocksWhenMergedThenShouldCarryEachKeyOnce(t *testing.T) {
	server := &streamServer{}
	agdx := agdxHandle(server).Agdx(agentID(t, "planner"), conversation(1))

	if _, err := agdx.Command(correlationID(2), []byte("x")).
		WithTarget(agentID(t, "executor")).Send(context.Background()); err != nil {
		t.Fatalf("send: %v", err)
	}

	entries, err := iggcon.DeserializeHeaders(server.sentMessages[0].UserHeaders)
	if err != nil {
		t.Fatalf("decode the headers: %v", err)
	}
	seen := map[string]int{}
	for _, entry := range entries {
		seen[string(entry.Key.Value)]++
	}
	for key, count := range seen {
		if count > 1 {
			t.Errorf("header %q appears %d times", key, count)
		}
	}
	// The overlap is real, so this would pass vacuously without it.
	if seen[wire.HeaderConversationID] == 0 || seen[wire.HeaderAgentID] == 0 {
		t.Error("the blocks must actually overlap for this to mean anything")
	}
}

// A payload that decodes as an envelope but fails the contract would route as
// a message nobody can act on, so it is refused as firmly as one that does not
// decode at all.
func TestGivenAnEnvelopeThatFailsTheContractWhenOpenedThenShouldRefuseIt(t *testing.T) {
	// A command with no correlation decodes cleanly and is still unusable:
	// nothing could pair a reply with it.
	payload, err := wire.Marshal(wire.AgentEnvelope{
		Kind:         wire.KindCommand,
		Record:       &wire.RecordID{ID: wire.IDFromBytes([16]byte{15: 1})},
		Conversation: conversation(1),
		Source:       agentID(t, "planner"),
		Body:         []byte("x"),
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	message, err := iggcon.NewIggyMessage(payload)
	if err != nil {
		t.Fatalf("build a record: %v", err)
	}
	record, err := laser.OpenRecord(message)
	if err != nil {
		t.Fatalf("open the record: %v", err)
	}

	_, err = laser.OpenAgdx(record)

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
}

// A reply names where the record it answers sits on the log, in the one form
// every client spells it: partition and offset, not a record id.
func TestGivenARepliedCauseWhenSentThenShouldStampTheLogPosition(t *testing.T) {
	server := &streamServer{}
	agdx := agdxHandle(server).Agdx(agentID(t, "planner"), conversation(1))
	cause := wire.RecordID{ID: wire.IDFromBytes([16]byte{15: 9})}
	at := wire.LogPosition{StreamID: 2, TopicID: 3, PartitionID: 4, Offset: 77}

	if _, err := agdx.Respond(correlationID(2), []byte("answer")).
		WithTarget(agentID(t, "executor")).
		WithCause(cause, &at).Send(context.Background()); err != nil {
		t.Fatalf("send: %v", err)
	}

	headers := sentHeaders(t, server)
	if got := headers[wire.HeaderCausalParent]; got != "4:77" {
		t.Fatalf("the cause header is %q, want the log position", got)
	}
	// The record id stays in the envelope, where the contract puts it. The
	// header carries the position because that is what a reader needs to go
	// and read the record the reply answers.
	envelope := sentEnvelope(t, server)
	if envelope.Cause == nil || *envelope.Cause != cause {
		t.Fatalf("the envelope must keep the record id, got %v", envelope.Cause)
	}
}

// A reply with no known position stamps no cause header rather than one a
// reader cannot resolve.
func TestGivenACauseWithNoPositionWhenSentThenShouldStampNoCauseHeader(t *testing.T) {
	server := &streamServer{}
	agdx := agdxHandle(server).Agdx(agentID(t, "planner"), conversation(1))
	cause := wire.RecordID{ID: wire.IDFromBytes([16]byte{15: 9})}

	if _, err := agdx.Respond(correlationID(2), []byte("answer")).
		WithTarget(agentID(t, "executor")).
		WithCause(cause, nil).Send(context.Background()); err != nil {
		t.Fatalf("send: %v", err)
	}

	if got, stamped := sentHeaders(t, server)[wire.HeaderCausalParent]; stamped {
		t.Fatalf("want no cause header, got %q", got)
	}
}
