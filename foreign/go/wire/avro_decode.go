package wire

import (
	"encoding/binary"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"
)

// avroReader walks an Avro payload against its writer schema.
//
// Nesting and collection sizes are bounded as the payload is read, because an
// Avro array of nulls can name a huge number of values in a few bytes.
type avroReader struct {
	bytes    []byte
	names    *avroNames
	maxDepth int
	// remaining bounds how many values one payload may hold at all.
	remaining int
	// invalidText records a string that is not UTF-8. The reference decoder
	// reports it only after the whole payload is walked, so a structural fault
	// further on still wins.
	invalidText bool
}

func (reader *avroReader) take(count int) ([]byte, bool) {
	if count < 0 || count > len(reader.bytes) {
		return nil, false
	}
	head := reader.bytes[:count]
	reader.bytes = reader.bytes[count:]
	return head, true
}

// varint reads one unsigned base-128 integer.
func (reader *avroReader) varint() (uint64, bool) {
	result := uint64(0)
	for shift := 0; shift < 70; shift += 7 {
		octet, ok := reader.take(1)
		if !ok {
			return 0, false
		}
		if shift == 63 && octet[0] > 1 {
			return 0, false
		}
		result |= uint64(octet[0]&127) << shift
		if octet[0] < 128 {
			return result, true
		}
	}
	return 0, false
}

// long reads one zigzag-encoded signed integer.
func (reader *avroReader) long() (int64, bool) {
	raw, ok := reader.varint()
	if !ok {
		return 0, false
	}
	return int64(raw>>1) ^ -int64(raw&1), true
}

// sized reads one length-prefixed byte run.
func (reader *avroReader) sized() ([]byte, bool) {
	length, ok := reader.long()
	if !ok || length < 0 || length > int64(len(reader.bytes)) {
		return nil, false
	}
	return reader.take(int(length))
}

// visit charges one value against the depth and count bounds.
func (reader *avroReader) visit(depth int) FaultReason {
	if depth > reader.maxDepth {
		return FaultTooDeep
	}
	if reader.remaining == 0 {
		return FaultTooLarge
	}
	reader.remaining--
	return ""
}

// decodeAvroPayload reads one Avro record against its writer schema.
func decodeAvroPayload(
	payload []byte, schema *avroSchema, names *avroNames, limits DecodeLimits,
) (payloadValue, FaultReason) {
	budget := limits.MaxPayloadBytes
	if budget < 1 {
		budget = 1
	}
	reader := &avroReader{
		bytes: payload, names: names,
		maxDepth: limits.parseDepth(), remaining: budget,
	}
	value, reason := reader.value(schema, 0)
	if reason != "" {
		return payloadValue{}, reason
	}
	if len(reader.bytes) != 0 || reader.invalidText {
		return payloadValue{}, FaultMalformed
	}
	return value, ""
}

func (reader *avroReader) value(schema *avroSchema, depth int) (payloadValue, FaultReason) {
	if reason := reader.visit(depth); reason != "" {
		return payloadValue{}, reason
	}
	// A reference reads as the type it names, at the same depth, since it adds
	// no nesting of its own.
	for schema.kind == avroRef {
		target, declared := reader.names.resolve(schema)
		if !declared {
			return payloadValue{}, FaultMalformed
		}
		schema = target
	}

	switch schema.kind {
	case avroNull:
		return payloadValue{kind: payloadNull}, ""
	case avroBoolean:
		octet, ok := reader.take(1)
		if !ok || octet[0] > 1 {
			return payloadValue{}, FaultMalformed
		}
		return payloadValue{kind: payloadBool, flag: octet[0] == 1}, ""
	case avroInt, avroLong:
		number, ok := reader.long()
		if !ok {
			return payloadValue{}, FaultMalformed
		}
		return payloadValue{
			kind: payloadNumber, number: strconv.FormatInt(number, 10),
		}, ""
	case avroFloat:
		raw, ok := reader.take(4)
		if !ok {
			return payloadValue{}, FaultMalformed
		}
		single := math.Float32frombits(binary.LittleEndian.Uint32(raw))
		return finiteNumber(float64(single))
	case avroDouble:
		raw, ok := reader.take(8)
		if !ok {
			return payloadValue{}, FaultMalformed
		}
		return finiteNumber(math.Float64frombits(binary.LittleEndian.Uint64(raw)))
	case avroBytes:
		raw, ok := reader.sized()
		if !ok {
			return payloadValue{}, FaultMalformed
		}
		return octetArray(raw), ""
	case avroFixed:
		raw, ok := reader.take(schema.size)
		if !ok {
			return payloadValue{}, FaultMalformed
		}
		return octetArray(raw), ""
	case avroString:
		raw, ok := reader.sized()
		if !ok {
			return payloadValue{}, FaultMalformed
		}
		if !utf8.Valid(raw) {
			reader.invalidText = true
		}
		return payloadValue{kind: payloadText, text: string(raw)}, ""
	case avroEnum:
		index, ok := reader.long()
		if !ok || index < 0 || index >= int64(len(schema.symbols)) {
			return payloadValue{}, FaultMalformed
		}
		return payloadValue{kind: payloadText, text: schema.symbols[index]}, ""
	case avroUnion:
		index, ok := reader.long()
		if !ok || index < 0 || index >= int64(len(schema.variants)) {
			return payloadValue{}, FaultMalformed
		}
		// A union reads as its branch, with no wrapper, so a filter addresses
		// the value directly.
		return reader.value(schema.variants[index], depth+1)
	case avroRecord:
		fields := make(map[string]payloadValue, len(schema.fields))
		for _, field := range schema.fields {
			value, reason := reader.value(field.schema, depth+1)
			if reason != "" {
				return payloadValue{}, reason
			}
			fields[field.name] = value
		}
		return payloadValue{kind: payloadObject, object: fields}, ""
	case avroArray:
		return reader.blocks(schema.items, depth, false)
	case avroMap:
		return reader.blocks(schema.items, depth, true)
	default:
		return payloadValue{}, FaultMalformed
	}
}

func finiteNumber(value float64) (payloadValue, FaultReason) {
	if math.IsInf(value, 0) || math.IsNaN(value) {
		return payloadValue{}, FaultMalformed
	}
	return payloadValue{
		kind:    payloadNumber,
		number:  formatFloat(value),
		isFloat: true,
	}, ""
}

// formatFloat renders a float the way the contract's reference encoder does:
// the shortest form that reads back exactly, always recognizable as a float.
//
// A float with no fractional part keeps a trailing ".0", and an exponent is
// written bare, so a whole-valued double never renders as the integer it is
// not and one client's rendering compares byte for byte against another's.
func formatFloat(value float64) string {
	text := strconv.FormatFloat(value, 'g', -1, 64)
	exponent := strings.IndexAny(text, "eE")
	if exponent < 0 {
		if !strings.Contains(text, ".") {
			return text + ".0"
		}
		return text
	}
	mantissa, power := text[:exponent], text[exponent+1:]
	negative := strings.HasPrefix(power, "-")
	power = strings.TrimLeft(strings.TrimLeft(power, "+-"), "0")
	if power == "" {
		power = "0"
	}
	if negative {
		power = "-" + power
	}
	return mantissa + "e" + power
}

// octetArray renders a byte run as the array of its octets, which is how every
// SDK compares one.
func octetArray(raw []byte) payloadValue {
	items := make([]payloadValue, len(raw))
	for index, octet := range raw {
		items[index] = payloadValue{
			kind: payloadNumber, number: strconv.FormatUint(uint64(octet), 10),
		}
	}
	return payloadValue{kind: payloadArray, array: items}
}

// blocks reads the block framing an array or a map is written in. A negative
// count names the items and the byte size of the block that holds them.
func (reader *avroReader) blocks(
	item *avroSchema, depth int, keyed bool,
) (payloadValue, FaultReason) {
	items := []payloadValue{}
	fields := map[string]payloadValue{}
	for {
		count, ok := reader.long()
		if !ok {
			return payloadValue{}, FaultMalformed
		}
		if count == 0 {
			if keyed {
				return payloadValue{kind: payloadObject, object: fields}, ""
			}
			return payloadValue{kind: payloadArray, array: items}, ""
		}
		blockEnd := -1
		if count < 0 {
			size, ok := reader.long()
			if !ok || size < 0 || size > int64(len(reader.bytes)) {
				return payloadValue{}, FaultMalformed
			}
			blockEnd = len(reader.bytes) - int(size)
			if count == math.MinInt64 {
				return payloadValue{}, FaultTooLarge
			}
			count = -count
		}
		if count > int64(reader.remaining) {
			return payloadValue{}, FaultTooLarge
		}
		for index := int64(0); index < count; index++ {
			var key string
			if keyed {
				raw, ok := reader.sized()
				if !ok {
					return payloadValue{}, FaultMalformed
				}
				if !utf8.Valid(raw) {
					reader.invalidText = true
				}
				key = string(raw)
			}
			value, reason := reader.value(item, depth+1)
			if reason != "" {
				return payloadValue{}, reason
			}
			if keyed {
				fields[key] = value
			} else {
				items = append(items, value)
			}
		}
		if blockEnd >= 0 && len(reader.bytes) != blockEnd {
			return payloadValue{}, FaultMalformed
		}
	}
}
