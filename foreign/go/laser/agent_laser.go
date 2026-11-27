package laser

import (
	"bytes"
	"context"
	"sort"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// SendAgent appends a payload to an agent topic, stamping a provenance the
// caller built and keying the partition by its conversation.
//
// It is the low-altitude form. Application code usually reads better through
// Agdx, which derives the provenance from the envelope so the two cannot
// disagree about who sent what to whom.
func (laser *Laser) SendAgent(
	ctx context.Context, topic string, payload []byte, provenance *Provenance,
) (*iggcon.SendMessagesResponse, error) {
	return laser.SendAgentAs(ctx, ActionSend, topic, payload, provenance)
}

// SendAgentAs is SendAgent with the governed kind named by the caller, so a
// request or a memory write reports what it is to a policy rather than
// reading as a plain send.
func (laser *Laser) SendAgentAs(
	ctx context.Context, kind ActionKind, topic string,
	payload []byte, provenance *Provenance,
) (*iggcon.SendMessagesResponse, error) {
	if laser.defaultStream == "" {
		return nil, errNoStream()
	}
	if provenance == nil {
		return nil, &NoProvenanceError{}
	}
	headers, err := provenance.Headers()
	if err != nil {
		return nil, err
	}
	headers, err = stampBroadcastAddressee(headers, topic)
	if err != nil {
		return nil, err
	}
	conversation := provenance.Conversation
	action := &GovernedAction{
		Kind: kind, Stream: laser.defaultStream, Topic: topic,
		Conversation: &conversation, Payload: payload,
	}
	if provenance.Agent != nil {
		action.Source = provenance.Agent.String()
	}
	if provenance.TargetAgent != nil {
		action.Target = provenance.TargetAgent.String()
	}
	if provenance.CorrelationID != nil {
		action.Correlation = *provenance.CorrelationID
	}
	governed, err := laser.govern(ctx, action)
	if err != nil {
		return nil, err
	}
	if governed != nil {
		payload = governed
	}
	message, err := iggcon.NewIggyMessage(payload, iggcon.WithUserHeaders(headers))
	if err != nil {
		return nil, &InvalidError{Reason: "build the agent record: " + err.Error()}
	}
	// A declared per-agent topic layout moves a record addressed to a
	// declared agent off the lane onto that agent's topic. A record with no
	// envelope carries no kind, so its addressee alone routes it.
	destination := topic
	if moved := laser.AgentDestination(
		topic, "", provenance.TargetAgent); moved != "" {
		destination = moved
	}
	return laser.Stream(laser.defaultStream).Topic(destination).sendMessages(
		ctx, []iggcon.IggyMessage{message}, provenance.PartitionKey(), true)
}

// stampBroadcastAddressee names every agent on a record that targets none.
//
// Every record on a shared session topic carries an addressee, so a role
// group bound to the addressee filter still receives one nobody addressed. A
// record on any other topic keeps the caller's headers verbatim.
func stampBroadcastAddressee(
	headers []iggcon.HeaderEntry, topic string,
) ([]iggcon.HeaderEntry, error) {
	if topic != wire.AgentSessions && topic != wire.AgentControl {
		return headers, nil
	}
	key, err := iggcon.NewHeaderKeyString(wire.HeaderTargetAgentID)
	if err != nil {
		return nil, &InvalidError{Reason: "addressee header: " + err.Error()}
	}
	if hasHeader(headers, key) {
		return headers, nil
	}
	return append(headers, iggcon.HeaderEntry{
		Key: key,
		Value: iggcon.HeaderValue{
			Kind: iggcon.String, Value: []byte(wire.BroadcastAddressee().String()),
		},
	}), nil
}

// hasHeader reports a header block already naming a key. A key is a kind and
// bytes rather than a comparable value, so it is matched rather than compared.
func hasHeader(headers []iggcon.HeaderEntry, key iggcon.HeaderKey) bool {
	for _, entry := range headers {
		if entry.Key.Kind == key.Kind && bytes.Equal(entry.Key.Value, key.Value) {
			return true
		}
	}
	return false
}

// SpawnSubconversation derives the provenance of a child conversation.
//
// The parent and the root are both carried: the parent is who spawned this
// one, and the root is the conversation the whole tree belongs to. Without
// the root, a reconstruction three levels down would have to walk every
// parent to find out what the work was for.
func (laser *Laser) SpawnSubconversation(
	parent *Provenance, author wire.AgentID,
) *Provenance {
	root := parent.Conversation
	if parent.RootConversation != nil {
		root = *parent.RootConversation
	}
	child := wire.ConversationID{ID: wire.IDFromBytes(NewOperationID())}
	spawned := parent.Conversation
	return &Provenance{
		Conversation:       child,
		ParentConversation: &spawned,
		RootConversation:   &root,
		Agent:              &author,
	}
}

// RedriveDeadLetter republishes a dead-lettered record so a fixed handler
// reprocesses it.
//
// The original is read back at the position the capsule names and its payload
// is replayed verbatim to that stream and topic, keyed by its conversation so
// the copy stays ordered with the rest of it.
//
// The capsule names its own source, and any writer that can append to the
// dead-letter topic can forge one, so the redrive is confined to this
// connection's own stream: a capsule pointing anywhere else is refused rather
// than copied into a stream the caller did not choose.
func (laser *Laser) RedriveDeadLetter(
	ctx context.Context, capsule wire.AgentDeadLetter,
) error {
	if laser.defaultStream == "" {
		return errNoStream()
	}
	position := capsule.Source
	client, err := laser.connected()
	if err != nil {
		return err
	}
	streamID, err := iggcon.NewIdentifier(position.StreamID)
	if err != nil {
		return &InvalidError{Reason: "the dead letter's stream id: " + err.Error()}
	}
	topicID, err := iggcon.NewIdentifier(position.TopicID)
	if err != nil {
		return &InvalidError{Reason: "the dead letter's topic id: " + err.Error()}
	}
	streamName, err := laser.namedStream(ctx, client, streamID, position.StreamID)
	if err != nil {
		return err
	}
	if streamName != laser.defaultStream {
		return &InvalidError{Reason: "the dead letter names stream " +
			streamName + ", outside this connection's stream " +
			laser.defaultStream}
	}
	topicName, err := laser.namedTopic(ctx, client, streamID, topicID, position.TopicID)
	if err != nil {
		return err
	}
	original, err := laser.readAt(ctx, client, streamID, topicID, position)
	if err != nil {
		return err
	}
	headers, err := iggcon.DeserializeHeaders(original.UserHeaders)
	if err != nil {
		return &CodecError{Reason: "read the dead letter's headers", Cause: err}
	}
	// The consumer's deduplicator saw the original key before the record
	// dead-lettered, so a byte-verbatim redrive would be dropped as a
	// duplicate. The copy is re-keyed by the source position: deterministic,
	// so redriving one capsule twice still deduplicates, while the payload
	// itself stays verbatim.
	headers, err = rekeyIdempotency(headers, position)
	if err != nil {
		return err
	}
	payload, err := redrivenPayload(original.Payload, headers, position)
	if err != nil {
		return err
	}
	message, err := iggcon.NewIggyMessage(payload, iggcon.WithUserHeaders(headers))
	if err != nil {
		return &InvalidError{Reason: "build the redriven record: " + err.Error()}
	}
	key := ""
	if provenance, err := ProvenanceFromHeaders(headers); err == nil && provenance != nil {
		key = provenance.PartitionKey()
	}
	_, err = laser.Stream(streamName).Topic(topicName).sendMessages(
		ctx, []iggcon.IggyMessage{message}, key, key != "")
	return err
}

// namedStream is a stream's name, refused when the id names nothing.
func (laser *Laser) namedStream(
	ctx context.Context, client iggcon.Client, streamID iggcon.Identifier, id uint32,
) (string, error) {
	details, err := client.GetStream(ctx, streamID)
	if err != nil && !isMissingResource(err) {
		return "", &TransportError{Reason: "read the stream", Cause: err}
	}
	if details == nil {
		return "", &InvalidError{Reason: "the dead letter's source stream " +
			itoa(int(id)) + " no longer exists"}
	}
	return details.Name, nil
}

// namedTopic is a topic's name, refused when the id names nothing.
func (laser *Laser) namedTopic(
	ctx context.Context, client iggcon.Client,
	streamID, topicID iggcon.Identifier, id uint32,
) (string, error) {
	details, err := client.GetTopic(ctx, streamID, topicID)
	if err != nil && !isMissingResource(err) {
		return "", &TransportError{Reason: "read the topic", Cause: err}
	}
	if details == nil {
		return "", &InvalidError{Reason: "the dead letter's source topic " +
			itoa(int(id)) + " no longer exists"}
	}
	return details.Name, nil
}

// readAt reads back the one record a position names.
func (laser *Laser) readAt(
	ctx context.Context, client iggcon.Client,
	streamID, topicID iggcon.Identifier, position wire.LogPosition,
) (iggcon.IggyMessage, error) {
	options := iggcon.DefaultPollOptions()
	options.Count = 1
	reader, err := iggcon.NewIdentifier("laser-redrive")
	if err != nil {
		return iggcon.IggyMessage{}, &InvalidError{Reason: err.Error()}
	}
	polled, err := client.PollMessages(ctx, streamID, topicID, position.PartitionID,
		iggcon.NewSingleConsumer(reader),
		iggcon.OffsetPollingStrategy(position.Offset), options)
	if err != nil {
		return iggcon.IggyMessage{}, &TransportError{
			Reason: "read the dead-lettered record", Cause: err}
	}
	if polled != nil {
		for _, message := range polled.Messages {
			if message.Header.Offset == position.Offset {
				return message, nil
			}
		}
	}
	// Retention removed it, so there is nothing to redrive. Saying so is
	// better than republishing whatever happens to sit there now.
	return iggcon.IggyMessage{}, &InvalidError{
		Reason: "the dead-lettered record at offset " +
			itoa(int(position.Offset)) + " is no longer on the log"}
}

// rekeyIdempotency rewrites the idempotency header so a redriven copy is not
// dropped by a consumer that still remembers the original.
func rekeyIdempotency(
	headers []iggcon.HeaderEntry, position wire.LogPosition,
) ([]iggcon.HeaderEntry, error) {
	key, err := iggcon.NewHeaderKeyString(wire.HeaderIdempotencyKey)
	if err != nil {
		return nil, &InvalidError{Reason: "idempotency header: " + err.Error()}
	}
	for index, entry := range headers {
		if entry.Key.Kind != key.Kind || !bytes.Equal(entry.Key.Value, key.Value) {
			continue
		}
		rewritten := redriveKey(string(entry.Value.Value), position)
		headers[index].Value = iggcon.HeaderValue{
			Kind: iggcon.String, Value: []byte(rewritten),
		}
		return headers, nil
	}
	return headers, nil
}

// redriveKey is the idempotency key a redriven copy carries: the original
// plus the position it came from, so two redrives of one capsule agree and a
// redrive of another does not.
func redriveKey(original string, position wire.LogPosition) string {
	return original + "/redrive/" + itoa(int(position.PartitionID)) +
		"-" + itoa(int(position.Offset))
}

// redrivenPayload is the body a redrive republishes.
//
// Dedup and routing of an agent record read the envelope rather than the
// headers, so an agent body is re-keyed there too. A signed one is refused:
// the signature covers the record id and the idempotency key, so a redrive
// would publish a record whose signature no longer holds. One past its
// deadline is refused as well, since it would only dead-letter again.
func redrivenPayload(
	payload []byte, headers []iggcon.HeaderEntry, position wire.LogPosition,
) ([]byte, error) {
	if !carriesAgentVersion(headers) {
		return bytes.Clone(payload), nil
	}
	var envelope wire.AgentEnvelope
	if err := wire.Unmarshal(payload, &envelope); err != nil {
		// A body that does not decode has no key to re-key, so it is replayed
		// byte for byte.
		return bytes.Clone(payload), nil
	}
	if envelope.Signature != nil {
		return nil, &InvalidError{Reason: "a signed agent dead letter cannot " +
			"be redriven, because its signature covers the record id and the " +
			"idempotency key a redrive has to change"}
	}
	if envelope.DeadlineMicros != nil &&
		*envelope.DeadlineMicros <= uint64(time.Now().UnixMicro()) {
		return nil, &InvalidError{
			Reason: "the dead-lettered agent record is past its deadline"}
	}
	if envelope.Record != nil {
		minted := wire.RecordID{ID: wire.IDFromBytes(NewOperationID())}
		envelope.Record = &minted
	}
	if envelope.IdempotencyKey != nil {
		rewritten, err := wire.NewIdempotencyKey(
			redriveKey(envelope.IdempotencyKey.String(), position))
		if err != nil {
			return nil, &InvalidError{
				Reason: "the redriven idempotency key: " + err.Error()}
		}
		envelope.IdempotencyKey = &rewritten
	}
	if err := envelope.Validate(); err != nil {
		return nil, &InvalidError{Reason: err.Error()}
	}
	encoded, err := wire.Marshal(envelope)
	if err != nil {
		return nil, &CodecError{
			Reason: "encode the redriven agent record", Cause: err}
	}
	return encoded, nil
}

// carriesAgentVersion reports a record stamped as an agent envelope.
func carriesAgentVersion(headers []iggcon.HeaderEntry) bool {
	key, err := iggcon.NewHeaderKeyString(wire.HeaderAgentVersion)
	if err != nil {
		return false
	}
	return hasHeader(headers, key)
}

// ReassembleChannel rebuilds a chunk stream from the log.
//
// Every retained record of the conversation on the topic is read, the chunks
// of the channel are taken in sequence order, and they are replayed through
// an assembler into the ordered events. This is what offset replay can do that
// a server-sent event stream cannot: a finished stream reconstructs
// deterministically after the fact, with no transcript store anywhere.
//
// A stream whose producer never ended it, past the bound its opening chunk
// declared, ends with a synthetic abandonment.
func (laser *Laser) ReassembleChannel(
	ctx context.Context, conversation wire.ConversationID,
	topic string, channel wire.ChannelID,
) ([]StreamEvent, error) {
	cursor, err := laser.Stream(laser.defaultStream).Topic(topic).
		ReplayAs("laser-reassemble")
	if err != nil {
		return nil, err
	}
	var chunks []wire.AgentEnvelope
	if err := cursor.Each(ctx, func(record *Record) bool {
		var envelope wire.AgentEnvelope
		if wire.Unmarshal(record.Payload(), &envelope) != nil {
			return true
		}
		if envelope.Conversation != conversation {
			return true
		}
		matches := envelope.Channel != nil && *envelope.Channel == channel
		if matches && (envelope.Kind == wire.KindChunk || envelope.Kind == wire.KindError) {
			chunks = append(chunks, envelope)
		}
		return true
	}); err != nil {
		return nil, err
	}
	// Within one partition the server delivers in publish order, but a stream
	// can be keyed onto several, so ordering by sequence reassembles
	// deterministically whichever way it was keyed.
	sortBySequence(chunks)
	// The opening chunk's bound is what ends a stream its producer never did.
	var abandonAt *uint64
	if len(chunks) > 0 && chunks[0].Sequence != nil && *chunks[0].Sequence == 0 {
		abandonAt = chunks[0].DeadlineMicros
	}
	assembler := NewChunkAssembler()
	events := make([]StreamEvent, 0, len(chunks)+1)
	for _, chunk := range chunks {
		events = append(events, assembler.Feed(chunk)...)
	}
	if abandonAt != nil && uint64(time.Now().UnixMicro()) > *abandonAt {
		if abandoned := assembler.Abandon(); abandoned != nil {
			events = append(events, *abandoned)
		}
	}
	return events, nil
}

// sortBySequence orders chunks by the sequence they declare, leaving one that
// declares none at the end: it has no place in the stream, and the assembler
// drops it.
func sortBySequence(chunks []wire.AgentEnvelope) {
	sort.SliceStable(chunks, func(first, second int) bool {
		left, right := chunks[first], chunks[second]
		switch {
		case left.Sequence == nil:
			return false
		case right.Sequence == nil:
			return true
		default:
			return *left.Sequence < *right.Sequence
		}
	})
}

// ConsumptionStatusKind is whether a consumer has committed past a position.
type ConsumptionStatusKind string

const (
	// NotYetConsumed means the consumer's stored offset is still behind.
	NotYetConsumed ConsumptionStatusKind = "not_yet_consumed"
	// Consumed means it has committed past the record.
	Consumed ConsumptionStatusKind = "consumed"
	// Skipped means an agent's group committed past the record without
	// handling it, because the record was not work for that agent.
	Skipped ConsumptionStatusKind = "skipped"
)

// ConsumptionStatus is how far a consumer has got relative to one record.
type ConsumptionStatus struct {
	Kind ConsumptionStatusKind
	// BehindBy is how many records short the consumer is.
	BehindBy uint64
	// Committed is its stored offset, and Head the partition head when the
	// question was asked.
	Committed uint64
	Head      uint64
	// Dispatch says why a skipped record was not work for the agent.
	Dispatch wire.Dispatch
}

// Consumed reports whether a consumer has committed past a record's position.
//
// A consumer that has never committed reads as not yet consumed. The server
// answers the same empty result both for a consumer that genuinely never
// committed and for a topic read it could not authorize or resolve, so a
// caller using this to decide a reassignment needs verified read access:
// otherwise a misconfiguration reads as "not consumed" and drives a
// reassignment nobody needed.
func (laser *Laser) Consumed(
	ctx context.Context, consumer iggcon.Consumer, at wire.LogPosition,
) (*ConsumptionStatus, error) {
	client, err := laser.connected()
	if err != nil {
		return nil, err
	}
	partition := at.PartitionID
	streamID, err := iggcon.NewIdentifier(at.StreamID)
	if err != nil {
		return nil, &InvalidError{Reason: "the stream id: " + err.Error()}
	}
	topicID, err := iggcon.NewIdentifier(at.TopicID)
	if err != nil {
		return nil, &InvalidError{Reason: "the topic id: " + err.Error()}
	}
	stored, err := client.GetConsumerOffset(ctx, consumer, streamID, topicID, &partition)
	if err != nil {
		if isMissingResource(err) {
			return &ConsumptionStatus{
				Kind: NotYetConsumed, BehindBy: at.Offset + 1,
			}, nil
		}
		return nil, &TransportError{Reason: "read the consumer offset", Cause: err}
	}
	if stored == nil {
		return &ConsumptionStatus{
			Kind: NotYetConsumed, BehindBy: at.Offset + 1,
		}, nil
	}
	if stored.StoredOffset < at.Offset {
		return &ConsumptionStatus{
			Kind:     NotYetConsumed,
			BehindBy: at.Offset - stored.StoredOffset,
		}, nil
	}
	return &ConsumptionStatus{
		Kind:      Consumed,
		Committed: stored.StoredOffset,
		Head:      stored.CurrentOffset,
	}, nil
}
