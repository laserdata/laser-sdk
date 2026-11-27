package wire

import (
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// PayloadDecoders are the schema decoders a filter prepares before records
// enter the evaluation loop, so a schema is parsed once per filter rather than
// once per record.
//
// They hold no mutable state, so one compiled filter may be evaluated
// concurrently.
type PayloadDecoders struct {
	avro     map[uint32]*avroWriterSchema
	protobuf map[uint32]protoreflect.MessageDescriptor
}

type avroWriterSchema struct {
	schema *avroSchema
	names  *avroNames
}

// CompilePayloadDecoders prepares the writer schemas a filter names.
//
// Every id the filter lists must be supplied, and its source must match the
// filter's codec, so a record can never select a schema the filter was not
// compiled against.
func CompilePayloadDecoders(
	filter ConsumerFilter, schemas []SchemaDef,
) (*PayloadDecoders, error) {
	decoders := &PayloadDecoders{
		avro:     map[uint32]*avroWriterSchema{},
		protobuf: map[uint32]protoreflect.MessageDescriptor{},
	}
	for _, id := range filter.SchemaRefs {
		schema, found := findSchemaDef(schemas, id)
		if !found {
			return nil, &InvalidError{Reason: fmt.Sprintf(
				"writer schema %d was not supplied", id)}
		}
		switch {
		case schema.Source.Kind == SchemaAvro && filter.Codec == CodecAvro:
			parsed, names, err := compileAvroWriterSchema(schema.Source.Schema)
			if err != nil {
				return nil, &InvalidError{Reason: fmt.Sprintf(
					"Avro schema %d: %v", id, err)}
			}
			decoders.avro[id] = &avroWriterSchema{schema: parsed, names: names}
		case schema.Source.Kind == SchemaProtobuf && filter.Codec == CodecProtobuf:
			descriptor, err := compileProtobufMessage(
				schema.Source.DescriptorSet, schema.Source.MessageType)
			if err != nil {
				return nil, &InvalidError{Reason: fmt.Sprintf(
					"Protobuf schema %d: %v", id, err)}
			}
			decoders.protobuf[id] = descriptor
		default:
			return nil, &InvalidError{Reason: fmt.Sprintf(
				"schema %d does not match codec %s", id, filter.Codec)}
		}
	}
	return decoders, nil
}

func findSchemaDef(schemas []SchemaDef, id uint32) (SchemaDef, bool) {
	for _, schema := range schemas {
		if schema.ID == id {
			return schema, true
		}
	}
	return SchemaDef{}, false
}

func compileAvroWriterSchema(text string) (*avroSchema, *avroNames, error) {
	names := &avroNames{byName: map[string]*avroSchema{}}
	document, reason := decodeJSONPayload([]byte(text), DecodeLimits{
		MaxPayloadBytes: MaxFilterSchemaBytes, MaxDepth: MaxFilterSchemaDepth,
	})
	if reason != "" {
		return nil, nil, fmt.Errorf("schema JSON is %s", reason)
	}
	parsed, err := parseAvroNode(document, "", names, 0)
	if err != nil {
		return nil, nil, err
	}
	return parsed, names, nil
}

// compileProtobufMessage resolves one message type out of a descriptor set.
func compileProtobufMessage(
	descriptorSet []byte, messageType string,
) (protoreflect.MessageDescriptor, error) {
	if len(descriptorSet) > MaxFilterSchemaBytes {
		return nil, fmt.Errorf("descriptor exceeds %d bytes", MaxFilterSchemaBytes)
	}
	var files descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(descriptorSet, &files); err != nil {
		return nil, err
	}
	for _, file := range files.GetFile() {
		normalizeMapEntryNames(file)
	}
	registry, err := protodesc.NewFiles(&files)
	if err != nil {
		return nil, err
	}
	name := protoreflect.FullName(strings.TrimPrefix(messageType, "."))
	found, err := registry.FindDescriptorByName(name)
	if err != nil {
		if err == protoregistry.NotFound {
			return nil, fmt.Errorf("no message %s", messageType)
		}
		return nil, err
	}
	message, ok := found.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("%s is not a message", messageType)
	}
	return message, nil
}

// decodeSchemaPayload reads one record against the writer schema its header
// selects.
func (decoders *PayloadDecoders) decodeSchemaPayload(
	codec FilterCodec, record FilterRecord, limits DecodeLimits,
) (payloadValue, FaultReason) {
	id, found := schemaIDOf(record)
	if !found {
		return payloadValue{}, FaultMissingSchema
	}
	switch codec {
	case CodecAvro:
		writer, allowed := decoders.avro[id]
		if !allowed {
			return payloadValue{}, decoders.unallowedReason(id)
		}
		return decodeAvroPayload(record.Payload, writer.schema, writer.names, limits)
	case CodecProtobuf:
		descriptor, allowed := decoders.protobuf[id]
		if !allowed {
			return payloadValue{}, decoders.unallowedReason(id)
		}
		return decodeProtobufPayload(record.Payload, descriptor, limits)
	default:
		return payloadValue{}, FaultMalformed
	}
}

// unallowedReason tells a schema this filter does not permit apart from one it
// permits under another codec.
func (decoders *PayloadDecoders) unallowedReason(id uint32) FaultReason {
	if _, known := decoders.avro[id]; known {
		return FaultSchemaMismatch
	}
	if _, known := decoders.protobuf[id]; known {
		return FaultSchemaMismatch
	}
	return FaultSchemaNotAllowed
}

// schemaIDOf reads the writer-schema id a record stamps. The last entry with
// the key wins, as it does for every other header.
func schemaIDOf(record FilterRecord) (uint32, bool) {
	for index := len(record.Headers) - 1; index >= 0; index-- {
		header := record.Headers[index]
		if header.Key != HeaderSchemaID {
			continue
		}
		if header.Value.Kind != ScalarUint || header.Value.Uint > 0xffff_ffff {
			return 0, false
		}
		return uint32(header.Value.Uint), true
	}
	return 0, false
}

// decodeProtobufPayload reads one Protobuf message against its descriptor.
//
// The bytes are walked for structure first, so nesting and value count are
// bounded before the decoder allocates a message tree.
func decodeProtobufPayload(
	payload []byte, descriptor protoreflect.MessageDescriptor, limits DecodeLimits,
) (payloadValue, FaultReason) {
	budget := limits.MaxPayloadBytes
	if budget < 1 {
		budget = 1
	}
	scan := protobufScan{maxDepth: limits.parseDepth(), remaining: budget}
	if reason := scan.message(payload, descriptor, 0); reason != "" {
		return payloadValue{}, reason
	}

	message := dynamicpb.NewMessage(descriptor)
	if err := proto.Unmarshal(payload, message); err != nil {
		return payloadValue{}, FaultMalformed
	}
	return protobufMessageValue(message)
}

// protobufScan bounds a message's nesting and value count without building it.
type protobufScan struct {
	maxDepth  int
	remaining int
}

func (scan *protobufScan) message(
	bytes []byte, descriptor protoreflect.MessageDescriptor, depth int,
) FaultReason {
	if depth > scan.maxDepth {
		return FaultTooDeep
	}
	if scan.remaining == 0 {
		return FaultTooLarge
	}
	scan.remaining--

	for len(bytes) > 0 {
		key, rest, ok := protobufVarint(bytes)
		if !ok {
			return FaultMalformed
		}
		bytes = rest
		field := key >> 3
		if field == 0 || field > 0x1fff_ffff {
			return FaultMalformed
		}
		switch key & 7 {
		case 0:
			if _, rest, ok = protobufVarint(bytes); !ok {
				return FaultMalformed
			}
			bytes = rest
		case 1:
			if len(bytes) < 8 {
				return FaultMalformed
			}
			bytes = bytes[8:]
		case 2:
			length, rest, ok := protobufVarint(bytes)
			if !ok || length > uint64(len(rest)) {
				return FaultMalformed
			}
			data, remainder := rest[:length], rest[length:]
			bytes = remainder
			// Only a nested message adds nesting. Every other length-delimited
			// field is a leaf run of bytes.
			if nested := nestedMessageOf(descriptor, int32(field)); nested != nil {
				if reason := scan.message(data, nested, depth+1); reason != "" {
					return reason
				}
			}
		case 5:
			if len(bytes) < 4 {
				return FaultMalformed
			}
			bytes = bytes[4:]
		default:
			return FaultMalformed
		}
	}
	return ""
}

func nestedMessageOf(
	descriptor protoreflect.MessageDescriptor, number int32,
) protoreflect.MessageDescriptor {
	field := descriptor.Fields().ByNumber(protoreflect.FieldNumber(number))
	if field == nil || field.Kind() != protoreflect.MessageKind &&
		field.Kind() != protoreflect.GroupKind {
		return nil
	}
	return field.Message()
}

func protobufVarint(bytes []byte) (uint64, []byte, bool) {
	result := uint64(0)
	for shift := 0; shift < 70; shift += 7 {
		if len(bytes) == 0 {
			return 0, nil, false
		}
		octet := bytes[0]
		bytes = bytes[1:]
		if shift == 63 && octet > 1 {
			return 0, nil, false
		}
		result |= uint64(octet&127) << shift
		if octet < 128 {
			return result, bytes, true
		}
	}
	return 0, nil, false
}

// protobufMessageValue reads a decoded message as the value model.
//
// A field without presence tracking, which is a proto3 scalar or any repeated
// field, is always part of the message, at its default when the bytes omit it.
// So a zero counter compares equal to zero and a presence test on a repeated
// field holds, exactly as they do for JSON and Avro.
func protobufMessageValue(message protoreflect.Message) (payloadValue, FaultReason) {
	descriptor := message.Descriptor()
	fields := make(map[string]payloadValue, descriptor.Fields().Len())
	for index := 0; index < descriptor.Fields().Len(); index++ {
		field := descriptor.Fields().Get(index)
		set := message.Has(field)
		if field.Cardinality() == protoreflect.Required && !set {
			return payloadValue{}, FaultMalformed
		}
		if !set && field.HasPresence() {
			continue
		}
		value, reason := protobufFieldValue(message.Get(field), field)
		if reason != "" {
			return payloadValue{}, reason
		}
		fields[string(field.Name())] = value
	}
	return payloadValue{kind: payloadObject, object: fields}, ""
}

func protobufFieldValue(
	value protoreflect.Value, field protoreflect.FieldDescriptor,
) (payloadValue, FaultReason) {
	switch {
	case field.IsMap():
		return protobufMapValue(value.Map(), field)
	case field.IsList():
		list := value.List()
		items := make([]payloadValue, list.Len())
		for index := 0; index < list.Len(); index++ {
			item, reason := protobufScalarValue(list.Get(index), field)
			if reason != "" {
				return payloadValue{}, reason
			}
			items[index] = item
		}
		return payloadValue{kind: payloadArray, array: items}, ""
	default:
		return protobufScalarValue(value, field)
	}
}

func protobufMapValue(
	entries protoreflect.Map, field protoreflect.FieldDescriptor,
) (payloadValue, FaultReason) {
	values := field.MapValue()
	fields := make(map[string]payloadValue, entries.Len())
	var failure FaultReason
	entries.Range(func(key protoreflect.MapKey, value protoreflect.Value) bool {
		item, reason := protobufScalarValue(value, values)
		if reason != "" {
			failure = reason
			return false
		}
		// A map key is addressed by its text, whatever its declared type.
		fields[key.String()] = item
		return true
	})
	if failure != "" {
		return payloadValue{}, failure
	}
	return payloadValue{kind: payloadObject, object: fields}, ""
}

func protobufScalarValue(
	value protoreflect.Value, field protoreflect.FieldDescriptor,
) (payloadValue, FaultReason) {
	switch field.Kind() {
	case protoreflect.BoolKind:
		return payloadValue{kind: payloadBool, flag: value.Bool()}, ""
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return payloadValue{
			kind: payloadNumber, number: strconv.FormatInt(value.Int(), 10),
		}, ""
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return payloadValue{
			kind: payloadNumber, number: strconv.FormatUint(value.Uint(), 10),
		}, ""
	case protoreflect.EnumKind:
		return payloadValue{
			kind: payloadNumber, number: strconv.FormatInt(int64(value.Enum()), 10),
		}, ""
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return finiteNumber(value.Float())
	case protoreflect.StringKind:
		return payloadValue{kind: payloadText, text: value.String()}, ""
	case protoreflect.BytesKind:
		return octetArray(value.Bytes()), ""
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return protobufMessageValue(value.Message())
	default:
		return payloadValue{}, FaultMalformed
	}
}

// normalizeMapEntryNames renames the synthetic message a map field is built
// from to the name Go's descriptor resolver expects.
//
// A map entry type is generated, never written by hand, and its name is not
// observable in a decoded value: only its key and value are. Other
// implementations accept whatever name a descriptor carries, so renaming it
// here lets the Go client read exactly the descriptors the server accepts.
func normalizeMapEntryNames(file *descriptorpb.FileDescriptorProto) {
	for _, message := range file.GetMessageType() {
		normalizeMapEntriesIn(message, file.GetPackage())
	}
}

func normalizeMapEntriesIn(message *descriptorpb.DescriptorProto, scope string) {
	enclosing := message.GetName()
	if scope != "" {
		enclosing = scope + "." + enclosing
	}
	nested := make(map[string]*descriptorpb.DescriptorProto, len(message.GetNestedType()))
	taken := make(map[string]bool, len(message.GetNestedType()))
	for _, child := range message.GetNestedType() {
		nested[child.GetName()] = child
		taken[child.GetName()] = true
	}

	for _, field := range message.GetField() {
		if field.GetLabel() != descriptorpb.FieldDescriptorProto_LABEL_REPEATED ||
			field.GetType() != descriptorpb.FieldDescriptorProto_TYPE_MESSAGE {
			continue
		}
		declared := field.GetTypeName()
		child, found := nested[lastNameSegment(declared)]
		if !found || !child.GetOptions().GetMapEntry() {
			continue
		}
		expected := mapEntryName(field.GetName())
		if child.GetName() == expected || taken[expected] {
			continue
		}
		delete(taken, child.GetName())
		child.Name = proto.String(expected)
		taken[expected] = true
		field.TypeName = proto.String("." + enclosing + "." + expected)
	}

	for _, child := range message.GetNestedType() {
		normalizeMapEntriesIn(child, enclosing)
	}
}

// mapEntryName is the generated name of a map field's entry type: the field
// name in camel case with an Entry suffix.
func mapEntryName(field string) string {
	var name strings.Builder
	upper := true
	for _, character := range field {
		if character == '_' {
			upper = true
			continue
		}
		if upper && character >= 'a' && character <= 'z' {
			character -= 'a' - 'A'
		}
		upper = false
		name.WriteRune(character)
	}
	return name.String() + "Entry"
}

func lastNameSegment(name string) string {
	if dot := strings.LastIndex(name, "."); dot >= 0 {
		return name[dot+1:]
	}
	return name
}
