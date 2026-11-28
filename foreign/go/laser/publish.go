package laser

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// PublishRequest is an unsent record. Nothing reaches the log until Send.
//
// A builder error is carried rather than returned at each step, so a chain
// reads as one expression and still reports the first thing that was wrong.
type PublishRequest struct {
	topic   *Topic
	payload []byte
	headers []iggcon.HeaderEntry
	key     string
	hasKey  bool
	err     error
}

// Publish begins a record on this topic.
func (topic *Topic) Publish() *PublishRequest {
	return &PublishRequest{topic: topic}
}

func (request *PublishRequest) fail(err error) *PublishRequest {
	if request.err == nil {
		request.err = err
	}
	return request
}

// Payload sets the opaque body.
func (request *PublishRequest) Payload(payload []byte) *PublishRequest {
	request.payload = payload
	return request
}

// JSON encodes a value as JSON and sends that as the body, stamping the
// content type so a reader knows how to decode it without being told.
func (request *PublishRequest) JSON(value any) *PublishRequest {
	encoded, err := json.Marshal(value)
	if err != nil {
		return request.fail(&CodecError{Reason: "encode the payload as JSON", Cause: err})
	}
	return request.RawBytes(encoded, wire.ContentJSON)
}

// CBOR encodes a value as the contract's CBOR and sends that as the body,
// stamping the content type.
func (request *PublishRequest) CBOR(value any) *PublishRequest {
	encoded, err := wire.Marshal(value)
	if err != nil {
		return request.fail(&CodecError{Reason: "encode the payload as CBOR", Cause: err})
	}
	return request.RawBytes(encoded, wire.ContentCBOR)
}

// EncodeWith writes a body through a caller-supplied codec.
func (request *PublishRequest) EncodeWith(codec wire.Codec, value any) *PublishRequest {
	if codec == nil {
		return request.fail(&InvalidError{Reason: "missing payload codec"})
	}
	encoded, err := codec.Encode(value)
	compiled, registered := codec.(*CompiledSchema)
	if err != nil {
		if registered {
			return request.fail(err)
		}
		return request.fail(&CodecError{Reason: "encode the record body", Cause: err})
	}
	request.RawBytes(encoded, codec.ContentType())
	if registered {
		request.SchemaID(compiled.id)
	}
	return request
}

// Msgpack encodes a MessagePack record.
func (request *PublishRequest) Msgpack(value any) *PublishRequest {
	return request.EncodeWith(wire.MsgpackCodec, value)
}

// BSON encodes a BSON document.
func (request *PublishRequest) BSON(value any) *PublishRequest {
	return request.EncodeWith(wire.BSONCodec, value)
}

// RawBytes sets the encoded body and its content type.
func (request *PublishRequest) RawBytes(payload []byte, kind wire.ContentType) *PublishRequest {
	request.payload = payload
	return request.ContentType(kind)
}

// SchemaID stamps the registered writer schema id on this record.
func (request *PublishRequest) SchemaID(id uint32) *PublishRequest {
	return request.setHeader(wire.HeaderSchemaID, iggcon.HeaderValue{Kind: iggcon.Uint32, Value: binary.LittleEndian.AppendUint32(nil, id)})
}

// Avro encodes a value and stamps the compiled schema's id.
func (request *PublishRequest) Avro(schema *CompiledSchema, value any) *PublishRequest {
	if schema == nil {
		return request.fail(&InvalidError{Reason: "Avro publish requires a compiled schema"})
	}
	encoded, err := schema.EncodeAvro(value)
	if err != nil {
		return request.fail(err)
	}
	return request.RawBytes(encoded, wire.ContentAvro).SchemaID(schema.id)
}

// Header adds one string user header.
func (request *PublishRequest) Header(name, value string) *PublishRequest {
	if reservedMetadata(name) {
		return request.fail(&InvalidError{Reason: "metadata header " + name + " is reserved"})
	}
	return request.textHeader(name, value)
}

func (request *PublishRequest) setHeader(name string, value iggcon.HeaderValue) *PublishRequest {
	key, err := iggcon.NewHeaderKeyString(name)
	if err != nil {
		return request.fail(&InvalidError{Reason: "header name " + name + ": " + err.Error()})
	}
	for i, entry := range request.headers {
		if entry.Key.Kind == key.Kind && bytes.Equal(entry.Key.Value, key.Value) {
			request.headers[i].Value = value
			return request
		}
	}
	request.headers = append(request.headers, iggcon.HeaderEntry{Key: key, Value: value})
	return request
}

// Provenance stamps the agentic spine onto the record and partitions by its
// conversation, which is what keeps one conversation ordered.
//
// An explicit Key still wins, for the rare case where a caller routes by
// something other than the conversation.
func (request *PublishRequest) Provenance(provenance *Provenance) *PublishRequest {
	entries, err := provenance.Headers()
	if err != nil {
		return request.fail(err)
	}
	request.headers = append(request.headers, entries...)
	if !request.hasKey {
		request.key, request.hasKey = provenance.PartitionKey(), true
	}
	return request
}

// Key sets the partition key.
//
// Records sharing a key land on one partition, which is the only ordering
// Iggy gives: per partition. Without a key the producer balances across
// partitions and no order is promised between records.
func (request *PublishRequest) Key(key string) *PublishRequest {
	request.key, request.hasKey = key, true
	return request
}

// Send appends the record and returns the server's confirmations.
func (request *PublishRequest) Send(ctx context.Context) (*iggcon.SendMessagesResponse, error) {
	message, err := request.message()
	if err != nil {
		return nil, err
	}
	return request.topic.governedSend(ctx, []iggcon.IggyMessage{message},
		request.key, request.hasKey)
}

func (request *PublishRequest) message() (iggcon.IggyMessage, error) {
	if request.err != nil {
		return iggcon.IggyMessage{}, request.err
	}
	if err := validatePublishHeaders(request.headers); err != nil {
		return iggcon.IggyMessage{}, err
	}
	message, err := iggcon.NewIggyMessage(request.payload,
		iggcon.WithUserHeaders(request.headers))
	if err != nil {
		return iggcon.IggyMessage{}, &InvalidError{Reason: "build the record: " + err.Error()}
	}
	return message, nil
}

// Send appends one record with explicit headers, the path with nothing built
// on top of it.
func (topic *Topic) Send(
	ctx context.Context, payload []byte, headers []iggcon.HeaderEntry, key string,
) (*iggcon.SendMessagesResponse, error) {
	message, err := iggcon.NewIggyMessage(payload, iggcon.WithUserHeaders(headers))
	if err != nil {
		return nil, &InvalidError{Reason: "build the record: " + err.Error()}
	}
	return topic.governedSend(ctx, []iggcon.IggyMessage{message}, key, key != "")
}

// Batch appends many pre-built records in one call, all sharing one
// partitioning. An empty batch is a cheap no-op that touches no socket.
func (topic *Topic) Batch(
	ctx context.Context, messages []iggcon.IggyMessage, key string,
) (*iggcon.SendMessagesResponse, error) {
	if len(messages) == 0 {
		return &iggcon.SendMessagesResponse{}, nil
	}
	return topic.governedSend(ctx, messages, key, key != "")
}

// governedSend runs the governor over every record before it reaches the log.
//
// It sits between the public publish verbs and sendMessages rather than
// inside sendMessages, so the paths that carry their own governed kind (an
// AGDX verb, a memory write) and the evidence writes themselves reach the log
// without being decided twice.
func (topic *Topic) governedSend(
	ctx context.Context, messages []iggcon.IggyMessage, key string, hasKey bool,
) (*iggcon.SendMessagesResponse, error) {
	if err := topic.laser.governMessages(
		ctx, topic.stream, topic.name, messages); err != nil {
		return nil, err
	}
	return topic.sendMessages(ctx, messages, key, hasKey)
}

// sendMessages is the one place a record reaches the log, so the partitioning
// rule and the identifier handling live here rather than at each caller.
func (topic *Topic) sendMessages(
	ctx context.Context, messages []iggcon.IggyMessage, key string, hasKey bool,
) (*iggcon.SendMessagesResponse, error) {
	if topic.stream == "" {
		return nil, errNoStream()
	}
	if _, err := topic.laser.connected(); err != nil {
		return nil, err
	}
	partitioning, err := partitioningFor(key, hasKey)
	if err != nil {
		return nil, err
	}
	return topic.sendResolved(ctx, messages, partitioning)
}

// sendOnPartition appends to one named partition rather than keying by a
// value, for a declared per-agent partition layout.
func (topic *Topic) sendOnPartition(
	ctx context.Context, messages []iggcon.IggyMessage, partition uint32,
) (*iggcon.SendMessagesResponse, error) {
	if topic.stream == "" {
		return nil, errNoStream()
	}
	if _, err := topic.laser.connected(); err != nil {
		return nil, err
	}
	return topic.sendResolved(ctx, messages, iggcon.PartitionId(partition))
}

// publishPartitions is how many partitions a topic created by a publish
// gets, matching what the reference client's producer asks for.
const publishPartitions uint32 = 1

// sendResolved is the one place a record reaches the log once its
// partitioning is settled.
func (topic *Topic) sendResolved(
	ctx context.Context, messages []iggcon.IggyMessage, partitioning iggcon.Partitioning,
) (*iggcon.SendMessagesResponse, error) {
	streamID, topicID, err := topic.identifiers()
	if err != nil {
		return nil, err
	}
	if topic.encryption != nil {
		// Sealed here rather than at each publish verb, so every path to the
		// log goes over encrypted: a verb that sealed its own records would
		// leave the next one to remember.
		messages, err = topic.encryption.seal(messages)
		if err != nil {
			return nil, err
		}
	}
	response, err := topic.publishOnce(ctx, streamID, topicID, partitioning, messages)
	if err == nil || !isMissingResource(err) {
		return response, err
	}
	// A publish creates the topic it names, which is what the reference
	// client's producer does, so a first publish needs no bootstrap before
	// it. Creating the topic is attempted rather than reasoned about: when
	// it is the stream that is missing, the create fails too and the
	// original publish error is returned, which already names what was not
	// found.
	if created := topic.createTopicForPublish(ctx); created != nil {
		return nil, err
	}
	return topic.publishOnce(ctx, streamID, topicID, partitioning, messages)
}

// publishOnce sends one batch, recovering a lost link but not a missing
// topic.
//
// Publishing is retried on a lost or stalled link under the shared recovery
// schedule, because a dropped connection is the one publish failure a resend
// reliably fixes.
func (topic *Topic) publishOnce(
	ctx context.Context, streamID, topicID iggcon.Identifier,
	partitioning iggcon.Partitioning, messages []iggcon.IggyMessage,
) (*iggcon.SendMessagesResponse, error) {
	return runWithRecovery(ctx, topic.laser.publishSchedule(),
		func(attempt context.Context) (*iggcon.SendMessagesResponse, error) {
			live, err := topic.laser.connected()
			if err != nil {
				return nil, err
			}
			response, err := live.SendMessages(
				attempt, streamID, topicID, partitioning, messages)
			topic.laser.reportLink(err)
			if err != nil {
				return nil, &TransportError{Reason: "publish", Cause: err}
			}
			return response, nil
		},
		topic.laser.reconnect)
}

// createTopicForPublish creates the topic a publish named, with one
// partition and whatever retention the server defaults to.
//
// The stream is never created here. A governed identity has no stream-wide
// create right, and a stream is provisioned with its deployment, so publishing
// into a stream that does not exist is a mistake to report rather than
// something to paper over.
func (topic *Topic) createTopicForPublish(ctx context.Context) error {
	client, err := topic.laser.connected()
	if err != nil {
		return err
	}
	streamID, topicID, err := topic.identifiers()
	if err != nil {
		return err
	}
	partitions := publishPartitions
	if _, err := client.CreateTopic(ctx, streamID, topic.name,
		&iggcon.TopicOptions{PartitionsCount: &partitions}); err != nil {
		// A concurrent publish may have created it first, which is what both
		// callers wanted.
		if _, probe := client.GetTopic(ctx, streamID, topicID); probe == nil {
			return nil
		}
		return &TransportError{Reason: "create the topic", Cause: err}
	}
	return nil
}

// partitioningFor turns a key into the partitioning the server reads.
//
// An empty key is not a key: it would hash every record onto one partition
// while reading as "no key given", so it is refused rather than silently
// serializing a topic.
func partitioningFor(key string, hasKey bool) (iggcon.Partitioning, error) {
	if !hasKey {
		return iggcon.None(), nil
	}
	if key == "" {
		return iggcon.Partitioning{}, &InvalidError{
			Reason: "a partition key must not be empty: omit the key to balance instead"}
	}
	partitioning, err := iggcon.EntityIdString(key)
	if err != nil {
		return iggcon.Partitioning{}, &InvalidError{Reason: "partition key: " + err.Error()}
	}
	return partitioning, nil
}
