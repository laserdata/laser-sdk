package wire

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"strconv"
)

// EncodeJSON writes one Avro datum from the JSON shape returned by Decode.
// Byte and fixed fields accept integer arrays and Go's JSON encoding of []byte.
func (schema *AvroWriterSchema) EncodeJSON(body []byte) ([]byte, error) {
	value, fault := decodeJSONPayload(body, RecordDecodeLimits)
	if fault != "" {
		return nil, fmt.Errorf("Avro input: %s", fault)
	}
	writer := avroEncoder{names: schema.names, remaining: RecordDecodeLimits.MaxPayloadBytes}
	out, err := writer.append(nil, schema.schema, value, 0, false)
	if err == nil && len(out) > RecordDecodeLimits.MaxPayloadBytes {
		return nil, fmt.Errorf("Avro datum exceeds record limits")
	}
	return out, err
}

type avroEncoder struct {
	names     *avroNames
	remaining int
}

func (w *avroEncoder) append(dst []byte, schema *avroSchema, value payloadValue, depth int, fromDefault bool) ([]byte, error) {
	if depth > RecordDecodeLimits.parseDepth() || w.remaining == 0 || len(dst) > RecordDecodeLimits.MaxPayloadBytes {
		return nil, fmt.Errorf("Avro datum exceeds record limits")
	}
	w.remaining--
	schema, ok := w.names.resolve(schema)
	if !ok {
		return nil, fmt.Errorf("unknown Avro type")
	}
	switch schema.kind {
	case avroNull:
		if value.kind == payloadNull {
			return dst, nil
		}
	case avroBoolean:
		if value.kind == payloadBool {
			if value.flag {
				return append(dst, 1), nil
			}
			return append(dst, 0), nil
		}
	case avroInt, avroLong:
		if value.kind == payloadNumber {
			n, err := strconv.ParseInt(value.number, 10, 64)
			if err == nil && (schema.kind == avroLong || (math.MinInt32 <= n && n <= math.MaxInt32)) {
				return binary.AppendVarint(dst, n), nil
			}
		}
	case avroFloat, avroDouble:
		if value.kind == payloadNumber {
			n, err := strconv.ParseFloat(value.number, 64)
			if err == nil {
				if schema.kind == avroDouble {
					return binary.LittleEndian.AppendUint64(dst, math.Float64bits(n)), nil
				}
				if math.Abs(n) <= math.MaxFloat32 {
					return binary.LittleEndian.AppendUint32(dst, math.Float32bits(float32(n))), nil
				}
			}
		}
	case avroString:
		if value.kind == payloadText {
			return append(binary.AppendVarint(dst, int64(len(value.text))), value.text...), nil
		}
	case avroEnum:
		if value.kind == payloadText {
			if i := slices.Index(schema.symbols, value.text); i >= 0 {
				return binary.AppendVarint(dst, int64(i)), nil
			}
		}
	case avroBytes, avroFixed:
		cost := len(value.array) + len(value.text)
		if cost > w.remaining {
			return nil, fmt.Errorf("Avro bytes exceed record limits")
		}
		w.remaining -= cost
		raw, err := avroByteValue(value, fromDefault)
		if err != nil {
			return nil, err
		}
		if schema.kind == avroBytes {
			dst = binary.AppendVarint(dst, int64(len(raw)))
		} else if len(raw) != schema.size {
			return nil, fmt.Errorf("Avro fixed needs %d bytes", schema.size)
		}
		return append(dst, raw...), nil
	case avroRecord:
		if value.kind != payloadObject {
			break
		}
		for _, field := range schema.fields {
			child, ok := value.object[field.name]
			useDefault := fromDefault || !ok
			if !ok {
				if field.fallback == nil {
					return nil, fmt.Errorf("missing Avro field %s", field.name)
				}
				child = *field.fallback
			}
			var err error
			dst, err = w.append(dst, field.schema, child, depth+1, useDefault)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", field.name, err)
			}
		}
		return dst, nil
	case avroUnion:
		// A text value belongs to the string branch, also when a bytes branch comes first.
		if value.kind == payloadText && !fromDefault {
			for index, branch := range schema.variants {
				if resolved, ok := w.names.resolve(branch); ok && resolved.kind == avroString {
					return w.append(binary.AppendVarint(dst, int64(index)), branch, value, depth+1, fromDefault)
				}
			}
		}
		for index, branch := range schema.variants {
			out, err := w.append(binary.AppendVarint(dst, int64(index)), branch, value, depth+1, fromDefault)
			if err == nil {
				return out, nil
			}
		}
	case avroArray:
		if value.kind != payloadArray {
			break
		}
		if len(value.array) > 0 {
			dst = binary.AppendVarint(dst, int64(len(value.array)))
		}
		for _, child := range value.array {
			var err error
			dst, err = w.append(dst, schema.items, child, depth+1, fromDefault)
			if err != nil {
				return nil, err
			}
		}
		return append(dst, 0), nil
	case avroMap:
		if value.kind != payloadObject {
			break
		}
		if len(value.object) > 0 {
			dst = binary.AppendVarint(dst, int64(len(value.object)))
		}
		keys := make([]string, 0, len(value.object))
		for k := range value.object {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		for _, k := range keys {
			dst = append(binary.AppendVarint(dst, int64(len(k))), k...)
			var err error
			dst, err = w.append(dst, schema.items, value.object[k], depth+1, fromDefault)
			if err != nil {
				return nil, err
			}
		}
		return append(dst, 0), nil
	}
	return nil, fmt.Errorf("value does not match the Avro type")
}

func avroByteValue(value payloadValue, fromDefault bool) ([]byte, error) {
	if fromDefault && value.kind == payloadText {
		raw := make([]byte, 0, len(value.text))
		for _, c := range value.text {
			if c > 255 {
				return nil, fmt.Errorf("Avro bytes default contains a non-byte character")
			}
			raw = append(raw, byte(c))
		}
		return raw, nil
	}
	if value.kind == payloadText {
		return base64.StdEncoding.DecodeString(value.text)
	}
	if value.kind != payloadArray {
		return nil, fmt.Errorf("Avro bytes require a byte array")
	}
	raw := make([]byte, len(value.array))
	for i, item := range value.array {
		v, err := strconv.ParseUint(item.number, 10, 8)
		if item.kind != payloadNumber || err != nil {
			return nil, fmt.Errorf("invalid Avro byte at index %d", i)
		}
		raw[i] = byte(v)
	}
	return raw, nil
}
