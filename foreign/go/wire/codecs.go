package wire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// Codec is an encoding strategy for a typed body.
//
// Four first-party codecs ship here, all self-describing so a projector can
// index their fields without a schema: JSON, MessagePack, CBOR and BSON. For a
// schema-first format such as Avro or Protobuf, for Arrow, or for your own
// framing, implement this interface on your own type. The codec advertises its
// ContentType so a consumer knows how to decode what it reads.
type Codec interface {
	// ContentType is the wire-format tag stamped on the agdx.ct header.
	ContentType() ContentType
	// Encode writes the bytes that ride the Iggy payload.
	Encode(value any) ([]byte, error)
}

// Decoder is the decode half of a codec.
//
// It is separate from Codec because a body is encoded from whatever the caller
// holds and decoded into a destination the caller supplies, so a custom codec
// may implement only the half it needs.
type Decoder interface {
	// Decode reads bytes a matching Codec wrote into the value target points
	// at, which must be a non-nil pointer.
	Decode(payload []byte, target any) error
}

// The built-in codecs. Each one is stateless, so a single value is safe to
// share across goroutines.
var (
	// JSONCodec is the built-in JSON codec.
	JSONCodec jsonCodec
	// MsgpackCodec is the built-in MessagePack codec. Structs ride as maps
	// keyed by field name, so a body round-trips with a JSON-shaped consumer.
	MsgpackCodec msgpackCodec
	// CBORCodec is the built-in CBOR codec, self-describing like JSON.
	CBORCodec cborCodec
	// BSONCodec is the built-in BSON codec. The top-level body must be a
	// document, which is a struct or a map.
	BSONCodec bsonCodec
)

// Each built-in codec satisfies both halves.
var (
	_ Codec   = JSONCodec
	_ Decoder = JSONCodec
	_ Codec   = MsgpackCodec
	_ Decoder = MsgpackCodec
	_ Codec   = CBORCodec
	_ Decoder = CBORCodec
	_ Codec   = BSONCodec
	_ Decoder = BSONCodec
)

type jsonCodec struct{}

func (jsonCodec) ContentType() ContentType { return ContentJSON }

func (jsonCodec) Encode(value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("wire: encode JSON payload: %w", err)
	}
	return encoded, nil
}

func (jsonCodec) Decode(payload []byte, target any) error {
	source := bytes.NewReader(payload)
	decoder := json.NewDecoder(source)
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("wire: decode JSON payload: %w", err)
	}
	// A payload is exactly one value, so anything after it is not this body.
	// What the decoder read ahead into its buffer comes first, then whatever
	// is left unread.
	rest, err := io.ReadAll(io.MultiReader(decoder.Buffered(), source))
	if err != nil {
		return fmt.Errorf("wire: decode JSON payload: %w", err)
	}
	if len(bytes.TrimSpace(rest)) != 0 {
		return fmt.Errorf("wire: decode JSON payload: %d trailing bytes",
			len(bytes.TrimSpace(rest)))
	}
	return nil
}

type cborCodec struct{}

func (cborCodec) ContentType() ContentType { return ContentCBOR }

func (cborCodec) Encode(value any) ([]byte, error) {
	encoded, err := Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("wire: encode CBOR payload: %w", err)
	}
	return encoded, nil
}

func (cborCodec) Decode(payload []byte, target any) error {
	if err := Unmarshal(payload, target); err != nil {
		return fmt.Errorf("wire: decode CBOR payload: %w", err)
	}
	return nil
}

type msgpackCodec struct{}

func (msgpackCodec) ContentType() ContentType { return ContentMsgpack }

func (msgpackCodec) Encode(value any) ([]byte, error) {
	tree, err := toValueOf(value)
	if err != nil {
		return nil, fmt.Errorf("wire: encode msgpack payload: %w", err)
	}
	encoded, err := marshalMsgpack(tree)
	if err != nil {
		return nil, fmt.Errorf("wire: encode msgpack payload: %w", err)
	}
	return encoded, nil
}

func (msgpackCodec) Decode(payload []byte, target any) error {
	tree, err := unmarshalMsgpack(payload)
	if err != nil {
		return fmt.Errorf("wire: decode msgpack payload: %w", err)
	}
	if err := fromValueInto(tree, target); err != nil {
		return fmt.Errorf("wire: decode msgpack payload: %w", err)
	}
	return nil
}

type bsonCodec struct{}

func (bsonCodec) ContentType() ContentType { return ContentBSON }

func (bsonCodec) Encode(value any) ([]byte, error) {
	tree, err := toValueOf(value)
	if err != nil {
		return nil, fmt.Errorf("wire: encode BSON payload: %w", err)
	}
	encoded, err := marshalBSON(tree)
	if err != nil {
		return nil, fmt.Errorf("wire: encode BSON payload: %w", err)
	}
	return encoded, nil
}

func (bsonCodec) Decode(payload []byte, target any) error {
	tree, err := unmarshalBSON(payload)
	if err != nil {
		return fmt.Errorf("wire: decode BSON payload: %w", err)
	}
	if err := fromValueInto(tree, target); err != nil {
		return fmt.Errorf("wire: decode BSON payload: %w", err)
	}
	return nil
}

// DecodeValue reads a key-value entry's value as JSON into target.
//
// It is the common case of DecodeValueWith.
func (entry KvEntry) DecodeValue(target any) error {
	return entry.DecodeValueWith(JSONCodec, target)
}

// DecodeValueWith reads a key-value entry's value with any decoder.
func (entry KvEntry) DecodeValueWith(decoder Decoder, target any) error {
	return decoder.Decode(entry.Value, target)
}
