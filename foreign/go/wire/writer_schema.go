package wire

import (
	"fmt"
	"sort"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// RecordDecodeLimits are the bounds a stored record decodes under: the
// projector's payload cap and the shared parse depth.
var RecordDecodeLimits = DecodeLimits{
	MaxPayloadBytes: MaxProjectorPayloadBytes,
	MaxDepth:        MaxFilterParseDepth,
}

// WriterSchemaKind discriminates a compiled writer schema's codec.
type WriterSchemaKind uint8

const (
	// WriterSchemaAvro decodes an Avro datum.
	WriterSchemaAvro WriterSchemaKind = iota
	// WriterSchemaProtobuf decodes a Protobuf message.
	WriterSchemaProtobuf
)

// WriterSchema is a compiled Avro or Protobuf writer schema, decoding a stored
// record body into the one shared value model.
//
// It is the projector's decoder: the same decode a filter runs on an
// `agdx.sid`-stamped record, exposed so a console or a tool can read a
// schema-first payload without re-implementing the codecs.
type WriterSchema struct {
	kind     WriterSchemaKind
	avro     *AvroWriterSchema
	protobuf protoreflect.MessageDescriptor
}

// AvroWriterSchema is an Avro writer schema compiled without its logical
// annotations, so decoded values stay physical.
//
// Logical annotations describe producer interpretation. Keeping values
// physical is what makes one record decode identically in every client and on
// the server.
type AvroWriterSchema struct {
	schema *avroSchema
	names  *avroNames
}

// CompileWriterSchema compiles an Avro or Protobuf source within the shared
// schema bounds.
//
// A JSON Schema validates payloads rather than describing how they were
// written, so it has no writer decoder and is refused here.
func CompileWriterSchema(source SchemaSource) (*WriterSchema, error) {
	switch source.Kind {
	case SchemaAvro:
		compiled, err := CompileAvroWriterSchema(source.Schema)
		if err != nil {
			return nil, err
		}
		return &WriterSchema{kind: WriterSchemaAvro, avro: compiled}, nil
	case SchemaProtobuf:
		descriptor, err := CompileProtobufMessage(source.DescriptorSet, source.MessageType)
		if err != nil {
			return nil, err
		}
		return &WriterSchema{kind: WriterSchemaProtobuf, protobuf: descriptor}, nil
	case SchemaJSON:
		return nil, &InvalidError{
			Reason: "a JSON Schema validates payloads and has no writer decoder",
		}
	default:
		return nil, &InvalidError{Reason: "unknown schema source kind"}
	}
}

// Kind is the codec this schema decodes.
func (schema *WriterSchema) Kind() WriterSchemaKind { return schema.kind }

// Avro is the compiled Avro schema, and false for a Protobuf one.
func (schema *WriterSchema) Avro() (*AvroWriterSchema, bool) {
	return schema.avro, schema.kind == WriterSchemaAvro
}

// Protobuf is the resolved message descriptor, and false for an Avro schema.
func (schema *WriterSchema) Protobuf() (protoreflect.MessageDescriptor, bool) {
	return schema.protobuf, schema.kind == WriterSchemaProtobuf
}

// ContentType is the content type a record written under this schema carries.
func (schema *WriterSchema) ContentType() ContentType {
	if schema.kind == WriterSchemaAvro {
		return ContentAvro
	}
	return ContentProtobuf
}

// Decode reads exactly one body within the limits into the shared model.
//
// The second result names the fault when the body does not decode, and is
// empty on success.
func (schema *WriterSchema) Decode(
	payload []byte, limits DecodeLimits,
) (DecodedValue, FaultReason) {
	if len(payload) > limits.MaxPayloadBytes {
		return DecodedValue{}, FaultTooLarge
	}
	switch schema.kind {
	case WriterSchemaAvro:
		return schema.avro.Decode(payload, limits)
	default:
		value, reason := decodeProtobufPayload(payload, schema.protobuf, limits)
		return DecodedValue{value: value}, reason
	}
}

// CompileAvroWriterSchema compiles Avro schema text within the shared schema
// bounds, stripping the logical annotations so values stay physical.
func CompileAvroWriterSchema(source string) (*AvroWriterSchema, error) {
	parsed, names, err := compileAvroWriterSchema(source)
	if err != nil {
		return nil, &InvalidError{Reason: fmt.Sprintf("Avro schema does not compile: %v", err)}
	}
	return &AvroWriterSchema{schema: parsed, names: names}, nil
}

// Decode reads exactly one datum within the limits into the shared model.
func (schema *AvroWriterSchema) Decode(
	payload []byte, limits DecodeLimits,
) (DecodedValue, FaultReason) {
	if len(payload) > limits.MaxPayloadBytes {
		return DecodedValue{}, FaultTooLarge
	}
	value, reason := decodeAvroPayload(payload, schema.schema, schema.names, limits)
	return DecodedValue{value: value}, reason
}

// CompileProtobufMessage resolves a Protobuf message type in a descriptor set
// within the shared schema byte cap.
func CompileProtobufMessage(
	descriptorSet []byte, messageType string,
) (protoreflect.MessageDescriptor, error) {
	if len(descriptorSet) > MaxFilterSchemaBytes {
		return nil, &InvalidError{Reason: fmt.Sprintf(
			"Protobuf descriptor set exceeds %d bytes", MaxFilterSchemaBytes)}
	}
	descriptor, err := compileProtobufMessage(descriptorSet, messageType)
	if err != nil {
		return nil, &InvalidError{Reason: err.Error()}
	}
	return descriptor, nil
}

// DecodedValue is one decoded record body in the shared model: the JSON shapes
// every client agrees on, with integers kept exact.
//
// Numbers keep the text they were written as, because a 64-bit integer does
// not survive a float and a timestamp coercion reads an epoch from an integer
// while leaving a double undecidable.
type DecodedValue struct {
	value payloadValue
}

// IsNull reports the null value.
func (decoded DecodedValue) IsNull() bool { return decoded.value.kind == payloadNull }

// Bool reads the value as a boolean. The second result is false for any other
// shape.
func (decoded DecodedValue) Bool() (bool, bool) {
	return decoded.value.flag, decoded.value.kind == payloadBool
}

// Number reads the value as the text it was written as, exactly. The second
// result is false for any other shape.
func (decoded DecodedValue) Number() (string, bool) {
	return decoded.value.number, decoded.value.kind == payloadNumber
}

// Text reads the value as a string. The second result is false for any other
// shape.
func (decoded DecodedValue) Text() (string, bool) {
	return decoded.value.text, decoded.value.kind == payloadText
}

// Items reads the value as an array. The second result is false for any other
// shape.
func (decoded DecodedValue) Items() ([]DecodedValue, bool) {
	if decoded.value.kind != payloadArray {
		return nil, false
	}
	items := make([]DecodedValue, len(decoded.value.array))
	for index, item := range decoded.value.array {
		items[index] = DecodedValue{value: item}
	}
	return items, true
}

// Field reads one entry of an object value. The second result is false when
// the value is not an object or does not hold the field.
func (decoded DecodedValue) Field(name string) (DecodedValue, bool) {
	if decoded.value.kind != payloadObject {
		return DecodedValue{}, false
	}
	field, present := decoded.value.object[name]
	return DecodedValue{value: field}, present
}

// Fields are an object value's field names in sorted order, and false for any
// other shape.
func (decoded DecodedValue) Fields() ([]string, bool) {
	if decoded.value.kind != payloadObject {
		return nil, false
	}
	names := make([]string, 0, len(decoded.value.object))
	for name := range decoded.value.object {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, true
}

// JSON renders the value as canonical JSON, with object keys in sorted order
// and every number in the text it was written as.
//
// Two decoded bodies are equal exactly when their renderings are, which is how
// a reader compares a decode against an expected value.
func (decoded DecodedValue) JSON() string {
	var out strings.Builder
	writeDecodedJSON(&out, decoded.value)
	return out.String()
}

// String renders the value as canonical JSON.
func (decoded DecodedValue) String() string { return decoded.JSON() }

func writeDecodedJSON(out *strings.Builder, value payloadValue) {
	switch value.kind {
	case payloadNull:
		out.WriteString("null")
	case payloadBool:
		if value.flag {
			out.WriteString("true")
		} else {
			out.WriteString("false")
		}
	case payloadNumber:
		out.WriteString(value.number)
	case payloadText:
		writeJSONString(out, value.text)
	case payloadArray:
		out.WriteByte('[')
		for index, item := range value.array {
			if index > 0 {
				out.WriteByte(',')
			}
			writeDecodedJSON(out, item)
		}
		out.WriteByte(']')
	case payloadObject:
		names := make([]string, 0, len(value.object))
		for name := range value.object {
			names = append(names, name)
		}
		sort.Strings(names)
		out.WriteByte('{')
		for index, name := range names {
			if index > 0 {
				out.WriteByte(',')
			}
			writeJSONString(out, name)
			out.WriteByte(':')
			writeDecodedJSON(out, value.object[name])
		}
		out.WriteByte('}')
	}
}

// writeJSONString renders a string with the escapes serde_json emits, so one
// rendering compares byte for byte against another client's.
func writeJSONString(out *strings.Builder, text string) {
	out.WriteByte('"')
	for _, r := range text {
		switch r {
		case '"':
			out.WriteString(`\"`)
		case '\\':
			out.WriteString(`\\`)
		case '\n':
			out.WriteString(`\n`)
		case '\r':
			out.WriteString(`\r`)
		case '\t':
			out.WriteString(`\t`)
		case '\b':
			out.WriteString(`\b`)
		case '\f':
			out.WriteString(`\f`)
		default:
			if r < 0x20 {
				fmt.Fprintf(out, `\u%04x`, r)
			} else {
				out.WriteRune(r)
			}
		}
	}
	out.WriteByte('"')
}

// DecodeJSONPayload reads a JSON body within the limits into the shared model,
// the third codec a projector reads.
func DecodeJSONPayload(payload []byte, limits DecodeLimits) (DecodedValue, FaultReason) {
	value, reason := decodeJSONPayload(payload, limits)
	return DecodedValue{value: value}, reason
}
