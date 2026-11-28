package wire

import (
	"encoding/base64"
	"fmt"
	"slices"
	"strconv"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// EncodeProtobufJSON writes one Protobuf message from the JSON shape returned by Decode.
// Bytes fields accept integer arrays and base64 text, and enum fields accept numbers and value names.
func EncodeProtobufJSON(descriptor protoreflect.MessageDescriptor, body []byte) ([]byte, error) {
	value, fault := decodeJSONPayload(body, RecordDecodeLimits)
	if fault != "" {
		return nil, fmt.Errorf("Protobuf input: %s", fault)
	}
	message, err := protobufMessageOf(descriptor, value, 0)
	if err != nil {
		return nil, err
	}
	out, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("Protobuf message encode failed: %w", err)
	}
	if len(out) > RecordDecodeLimits.MaxPayloadBytes {
		return nil, fmt.Errorf("Protobuf message exceeds record limits")
	}
	return out, nil
}

func protobufMessageOf(
	descriptor protoreflect.MessageDescriptor, value payloadValue, depth int,
) (*dynamicpb.Message, error) {
	if value.kind != payloadObject {
		return nil, fmt.Errorf("a Protobuf message requires an object")
	}
	if depth > RecordDecodeLimits.parseDepth() {
		return nil, fmt.Errorf("Protobuf message nests too deeply")
	}
	message := dynamicpb.NewMessage(descriptor)
	names := make([]string, 0, len(value.object))
	for name := range value.object {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		field := descriptor.Fields().ByName(protoreflect.Name(name))
		if field == nil {
			field = descriptor.Fields().ByJSONName(name)
		}
		item := value.object[name]
		// A key the schema does not declare and a null are left out, as an unset field.
		if field == nil || item.isNull() {
			continue
		}
		if err := setProtobufField(message, field, item, depth); err != nil {
			return nil, fmt.Errorf("field `%s`: %w", name, err)
		}
	}
	return message, nil
}

func setProtobufField(
	message *dynamicpb.Message, field protoreflect.FieldDescriptor, value payloadValue, depth int,
) error {
	switch {
	case field.IsMap():
		if value.kind != payloadObject {
			return fmt.Errorf("a map field requires an object")
		}
		entries := message.Mutable(field).Map()
		for text, item := range value.object {
			key, err := protobufMapKey(field.MapKey(), text)
			if err != nil {
				return err
			}
			scalar, err := protobufScalarOf(field.MapValue(), item, depth)
			if err != nil {
				return fmt.Errorf("key `%s`: %w", text, err)
			}
			entries.Set(key, scalar)
		}
		return nil
	case field.IsList():
		if value.kind != payloadArray {
			return fmt.Errorf("a repeated field requires an array")
		}
		list := message.Mutable(field).List()
		for index, item := range value.array {
			scalar, err := protobufScalarOf(field, item, depth)
			if err != nil {
				return fmt.Errorf("index %d: %w", index, err)
			}
			list.Append(scalar)
		}
		return nil
	default:
		scalar, err := protobufScalarOf(field, value, depth)
		if err != nil {
			return err
		}
		message.Set(field, scalar)
		return nil
	}
}

func protobufScalarOf(
	field protoreflect.FieldDescriptor, value payloadValue, depth int,
) (protoreflect.Value, error) {
	switch field.Kind() {
	case protoreflect.BoolKind:
		if value.kind != payloadBool {
			return protoreflect.Value{}, fmt.Errorf("expects a boolean")
		}
		return protoreflect.ValueOfBool(value.flag), nil
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		number, err := protobufInteger(value, 32)
		return protoreflect.ValueOfInt32(int32(number)), err
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		number, err := protobufInteger(value, 64)
		return protoreflect.ValueOfInt64(number), err
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		number, err := protobufUnsigned(value, 32)
		return protoreflect.ValueOfUint32(uint32(number)), err
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		number, err := protobufUnsigned(value, 64)
		return protoreflect.ValueOfUint64(number), err
	case protoreflect.FloatKind:
		number, err := protobufFloat(value, 32)
		return protoreflect.ValueOfFloat32(float32(number)), err
	case protoreflect.DoubleKind:
		number, err := protobufFloat(value, 64)
		return protoreflect.ValueOfFloat64(number), err
	case protoreflect.StringKind:
		if value.kind != payloadText {
			return protoreflect.Value{}, fmt.Errorf("expects text")
		}
		return protoreflect.ValueOfString(value.text), nil
	case protoreflect.BytesKind:
		raw, err := protobufBytes(value)
		return protoreflect.ValueOfBytes(raw), err
	case protoreflect.EnumKind:
		return protobufEnum(field, value)
	case protoreflect.MessageKind, protoreflect.GroupKind:
		nested, err := protobufMessageOf(field.Message(), value, depth+1)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfMessage(nested), nil
	default:
		return protoreflect.Value{}, fmt.Errorf("unsupported Protobuf field kind")
	}
}

func protobufInteger(value payloadValue, bits int) (int64, error) {
	if value.kind != payloadNumber {
		return 0, fmt.Errorf("expects an integer")
	}
	number, err := strconv.ParseInt(value.number, 10, bits)
	if err != nil {
		return 0, fmt.Errorf("expects a %d-bit integer, got %s", bits, value.number)
	}
	return number, nil
}

func protobufUnsigned(value payloadValue, bits int) (uint64, error) {
	if value.kind != payloadNumber {
		return 0, fmt.Errorf("expects an unsigned integer")
	}
	number, err := strconv.ParseUint(value.number, 10, bits)
	if err != nil {
		return 0, fmt.Errorf("expects a %d-bit unsigned integer, got %s", bits, value.number)
	}
	return number, nil
}

func protobufFloat(value payloadValue, bits int) (float64, error) {
	if value.kind != payloadNumber {
		return 0, fmt.Errorf("expects a number")
	}
	number, err := strconv.ParseFloat(value.number, bits)
	if err != nil {
		return 0, fmt.Errorf("expects a %d-bit float, got %s", bits, value.number)
	}
	return number, nil
}

func protobufBytes(value payloadValue) ([]byte, error) {
	if value.kind == payloadText {
		raw, err := base64.StdEncoding.DecodeString(value.text)
		if err != nil {
			return nil, fmt.Errorf("expects base64 text or a byte array")
		}
		return raw, nil
	}
	if value.kind != payloadArray {
		return nil, fmt.Errorf("expects base64 text or a byte array")
	}
	raw := make([]byte, len(value.array))
	for index, item := range value.array {
		octet, err := strconv.ParseUint(item.number, 10, 8)
		if item.kind != payloadNumber || err != nil {
			return nil, fmt.Errorf("invalid byte at index %d", index)
		}
		raw[index] = byte(octet)
	}
	return raw, nil
}

func protobufEnum(field protoreflect.FieldDescriptor, value payloadValue) (protoreflect.Value, error) {
	if value.kind == payloadText {
		named := field.Enum().Values().ByName(protoreflect.Name(value.text))
		if named == nil {
			return protoreflect.Value{}, fmt.Errorf("enum has no value named %s", value.text)
		}
		return protoreflect.ValueOfEnum(named.Number()), nil
	}
	number, err := protobufInteger(value, 32)
	return protoreflect.ValueOfEnum(protoreflect.EnumNumber(number)), err
}

func protobufMapKey(key protoreflect.FieldDescriptor, text string) (protoreflect.MapKey, error) {
	switch key.Kind() {
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(text).MapKey(), nil
	case protoreflect.BoolKind:
		flag, err := strconv.ParseBool(text)
		if err != nil {
			return protoreflect.MapKey{}, fmt.Errorf("map key `%s` is not a boolean", text)
		}
		return protoreflect.ValueOfBool(flag).MapKey(), nil
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		number, err := strconv.ParseInt(text, 10, 32)
		if err != nil {
			return protoreflect.MapKey{}, fmt.Errorf("map key `%s` is not a 32-bit integer", text)
		}
		return protoreflect.ValueOfInt32(int32(number)).MapKey(), nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		number, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return protoreflect.MapKey{}, fmt.Errorf("map key `%s` is not a 64-bit integer", text)
		}
		return protoreflect.ValueOfInt64(number).MapKey(), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		number, err := strconv.ParseUint(text, 10, 32)
		if err != nil {
			return protoreflect.MapKey{}, fmt.Errorf("map key `%s` is not a 32-bit unsigned integer", text)
		}
		return protoreflect.ValueOfUint32(uint32(number)).MapKey(), nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		number, err := strconv.ParseUint(text, 10, 64)
		if err != nil {
			return protoreflect.MapKey{}, fmt.Errorf("map key `%s` is not a 64-bit unsigned integer", text)
		}
		return protoreflect.ValueOfUint64(number).MapKey(), nil
	default:
		return protoreflect.MapKey{}, fmt.Errorf("unsupported Protobuf map key kind")
	}
}
