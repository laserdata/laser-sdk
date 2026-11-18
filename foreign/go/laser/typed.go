package laser

import (
	"context"
	"encoding/json"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// Codec encodes and decodes a record body. The two built in are JSON and the
// contract's CBOR, and a caller supplies its own for anything else.
type Codec interface {
	// ContentType is stamped on each record so a reader knows how to decode it
	// without being told out of band.
	ContentType() wire.ContentType
	Encode(value any) ([]byte, error)
	Decode(payload []byte, into any) error
}

// JSONCodec reads and writes JSON bodies.
type JSONCodec struct{}

func (JSONCodec) ContentType() wire.ContentType { return wire.ContentJSON }

func (JSONCodec) Encode(value any) ([]byte, error) { return json.Marshal(value) }

func (JSONCodec) Decode(payload []byte, into any) error { return json.Unmarshal(payload, into) }

// CBORCodec reads and writes the contract's named-field CBOR, which is the
// same encoding every Laser client uses for contract types.
type CBORCodec struct{}

func (CBORCodec) ContentType() wire.ContentType { return wire.ContentCBOR }

func (CBORCodec) Encode(value any) ([]byte, error) { return wire.Marshal(value) }

func (CBORCodec) Decode(payload []byte, into any) error { return wire.Unmarshal(payload, into) }

// TypedTopic publishes and reads one body type through one codec.
//
// It is a view over a topic rather than a separate connection, so a caller can
// hold several of them over one topic for different body types.
type TypedTopic[T any] struct {
	topic *Topic
	codec Codec
}

// TypedJSON views a topic as carrying JSON bodies of one type.
func TypedJSON[T any](topic *Topic) *TypedTopic[T] {
	return &TypedTopic[T]{topic: topic, codec: JSONCodec{}}
}

// TypedCBOR views a topic as carrying CBOR bodies of one type.
func TypedCBOR[T any](topic *Topic) *TypedTopic[T] {
	return &TypedTopic[T]{topic: topic, codec: CBORCodec{}}
}

// TypedWith views a topic through a caller-supplied codec.
func TypedWith[T any](topic *Topic, codec Codec) *TypedTopic[T] {
	return &TypedTopic[T]{topic: topic, codec: codec}
}

// Topic is the topic underneath, for anything the typed view does not cover.
func (typed *TypedTopic[T]) Topic() *Topic { return typed.topic }

// Publish begins a record carrying an encoded body, with the content type
// already stamped.
func (typed *TypedTopic[T]) Publish(body T) *PublishRequest {
	request := typed.topic.Publish()
	encoded, err := typed.codec.Encode(body)
	if err != nil {
		return request.fail(&CodecError{Reason: "encode the record body", Cause: err})
	}
	return request.Payload(encoded).
		Header(wire.HeaderContentType, string(typed.codec.ContentType()))
}

// TypedRecord is a record whose body decoded.
type TypedRecord[T any] struct {
	// Record is the record the body came from, with its provenance and
	// position.
	Record *Record
	Body   T
}

// TypedDecodeError reports a record whose body did not decode, naming where it
// sat so a reader can skip it and carry on rather than stopping the stream.
//
// One unreadable record in a topic is not a reason to stop reading the rest:
// it is usually one bad writer, not a broken topic.
type TypedDecodeError struct {
	PartitionID uint32
	Offset      uint64
	Cause       error
}

func (e *TypedDecodeError) Error() string {
	return "decode the record at partition " + itoa(int(e.PartitionID)) +
		" offset " + itoa(int(e.Offset)) + ": " + e.Cause.Error()
}

func (e *TypedDecodeError) Unwrap() error { return e.Cause }

// Decode reads one record's body.
func (typed *TypedTopic[T]) Decode(record *Record) (*TypedRecord[T], error) {
	var body T
	if err := typed.codec.Decode(record.Payload(), &body); err != nil {
		return nil, &TypedDecodeError{
			PartitionID: record.PartitionID,
			Offset:      record.Offset(),
			Cause:       err,
		}
	}
	return &TypedRecord[T]{Record: record, Body: body}, nil
}

// Records drains the topic and calls visit once per decoded record.
//
// A record that does not decode is reported through onError and the read goes
// on, because one bad writer should not stop a reader from seeing the rest.
// Passing nil for onError skips such records silently. visit returning false
// stops the read.
func (typed *TypedTopic[T]) Records(
	ctx context.Context, cursor *Cursor,
	visit func(*TypedRecord[T]) bool,
	onError func(*TypedDecodeError),
) error {
	return cursor.Each(ctx, func(record *Record) bool {
		decoded, err := typed.Decode(record)
		if err != nil {
			var failure *TypedDecodeError
			if onError != nil && asTypedDecodeError(err, &failure) {
				onError(failure)
			}
			return true
		}
		return visit(decoded)
	})
}

// Replay opens a cursor over the underlying topic, for use with Records.
func (typed *TypedTopic[T]) Replay() (*Cursor, error) { return typed.topic.Replay() }

func asTypedDecodeError(err error, target **TypedDecodeError) bool {
	failure, ok := err.(*TypedDecodeError)
	if ok {
		*target = failure
	}
	return ok
}
