package laser

import (
	"context"
	"encoding/json"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// PublishRequest is an unsent record. Nothing reaches the log until Send.
//
// A builder error is carried rather than returned at each step, so a chain
// reads as one expression and still reports the first thing that was wrong.
type PublishRequest struct {
	topic    *Topic
	payload  []byte
	headers  []iggcon.HeaderEntry
	key      string
	hasKey   bool
	messages []iggcon.IggyMessage
	err      error
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
	request.payload = encoded
	return request.Header(wire.HeaderContentType, string(wire.ContentJSON))
}

// CBOR encodes a value as the contract's CBOR and sends that as the body,
// stamping the content type.
func (request *PublishRequest) CBOR(value any) *PublishRequest {
	encoded, err := wire.Marshal(value)
	if err != nil {
		return request.fail(&CodecError{Reason: "encode the payload as CBOR", Cause: err})
	}
	request.payload = encoded
	return request.Header(wire.HeaderContentType, string(wire.ContentCBOR))
}

// Header adds one string user header.
func (request *PublishRequest) Header(name, value string) *PublishRequest {
	key, err := iggcon.NewHeaderKeyString(name)
	if err != nil {
		return request.fail(&InvalidError{Reason: "header name " + name + ": " + err.Error()})
	}
	request.headers = append(request.headers, iggcon.HeaderEntry{
		Key:   key,
		Value: iggcon.HeaderValue{Kind: iggcon.String, Value: []byte(value)},
	})
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
	if request.err != nil {
		return nil, request.err
	}
	message, err := iggcon.NewIggyMessage(request.payload,
		iggcon.WithUserHeaders(request.headers))
	if err != nil {
		return nil, &InvalidError{Reason: "build the record: " + err.Error()}
	}
	return request.topic.governedSend(ctx, []iggcon.IggyMessage{message},
		request.key, request.hasKey)
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

// sendResolved is the one place a record reaches the log once its
// partitioning is settled.
func (topic *Topic) sendResolved(
	ctx context.Context, messages []iggcon.IggyMessage, partitioning iggcon.Partitioning,
) (*iggcon.SendMessagesResponse, error) {
	streamID, topicID, err := topic.identifiers()
	if err != nil {
		return nil, err
	}
	// Publishing is retried on a lost or stalled link under the shared
	// recovery schedule, because a dropped connection is the one publish
	// failure a resend reliably fixes.
	return runWithRecovery(ctx, topic.laser.publishSchedule(),
		func(attempt context.Context) (*iggcon.SendMessagesResponse, error) {
			live, err := topic.laser.connected()
			if err != nil {
				return nil, err
			}
			response, err := live.SendMessages(
				attempt, streamID, topicID, partitioning, messages)
			if err != nil {
				return nil, &TransportError{Reason: "publish", Cause: err}
			}
			return response, nil
		},
		topic.laser.reconnect)
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
