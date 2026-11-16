package wire

import (
	"encoding/binary"
	"math"
)

// Native Iggy header kinds, as they ride in a record's header block.
const (
	headerKindString  = 2
	headerKindBool    = 3
	headerKindInt8    = 4
	headerKindInt16   = 5
	headerKindInt32   = 6
	headerKindInt64   = 7
	headerKindInt128  = 8
	headerKindUint8   = 9
	headerKindUint16  = 10
	headerKindUint32  = 11
	headerKindUint64  = 12
	headerKindUint128 = 13
	headerKindFloat32 = 14
	headerKindFloat64 = 15
)

// headerValueBytes is the width a 128-bit header value must have.
const headerValueBytes = 16

// DecodeRecordHeaders reads a record's native Iggy header block.
//
// An entry under a non-text key is stepped over undecoded, since no predicate
// can name it. A malformed value under a text key faults the whole record, so
// a filter never compares against a value another client would read
// differently. Entries keep their order, and the evaluator reads the last
// entry of a repeated key.
//
// The second result is false when the block's structure does not parse, which
// the caller reports as a malformed record.
func DecodeRecordHeaders(block []byte) ([]FilterHeader, bool) {
	headers := []FilterHeader{}
	for len(block) > 0 {
		keyKind, key, rest, ok := headerField(block)
		if !ok {
			return nil, false
		}
		valueKind, value, rest, ok := headerField(rest)
		if !ok {
			return nil, false
		}
		block = rest
		if keyKind != headerKindString {
			continue
		}
		if !isValidUTF8Bytes(key) {
			return nil, false
		}
		scalar, ok := headerScalarFromBytes(valueKind, value)
		if !ok {
			return nil, false
		}
		headers = append(headers, FilterHeader{Key: string(key), Value: scalar})
	}
	return headers, true
}

// DecodeContentTypeHeader reads only the content-type header.
//
// Every entry is stepped over, so a block whose structure does not parse still
// faults the record, but no other key or value is decoded or checked. A filter
// that compares no user header pays only this.
func DecodeContentTypeHeader(block []byte) ([]FilterHeader, bool) {
	found := []FilterHeader{}
	for len(block) > 0 {
		keyKind, key, rest, ok := headerField(block)
		if !ok {
			return nil, false
		}
		valueKind, value, rest, ok := headerField(rest)
		if !ok {
			return nil, false
		}
		block = rest
		if keyKind != headerKindString || string(key) != HeaderContentType {
			continue
		}
		scalar, ok := headerScalarFromBytes(valueKind, value)
		if !ok {
			return nil, false
		}
		// Only an integer names a codec. Any other value declares nothing, and
		// replaces whatever an earlier entry declared.
		switch scalar.Kind {
		case ScalarUint, ScalarInt:
			found = []FilterHeader{{Key: HeaderContentType, Value: scalar}}
		default:
			found = []FilterHeader{}
		}
	}
	return found, true
}

// DecodeHeadersFor reads the headers a filter needs before evaluating:
// nothing, only the content type, or every entry.
func DecodeHeadersFor(need HeaderNeed, block []byte) ([]FilterHeader, bool) {
	switch need {
	case NeedNoHeaders:
		return nil, true
	case NeedContentType:
		return DecodeContentTypeHeader(block)
	default:
		return DecodeRecordHeaders(block)
	}
}

// headerField reads one kind-length-value field, returning what is left of
// the block.
func headerField(block []byte) (uint8, []byte, []byte, bool) {
	if len(block) < 5 {
		return 0, nil, nil, false
	}
	kind := block[0]
	length := int(binary.LittleEndian.Uint32(block[1:5]))
	// A zero kind names nothing, and a field is one to 255 bytes wide.
	if kind == 0 || length < 1 || length > 255 || len(block) < 5+length {
		return 0, nil, nil, false
	}
	return kind, block[5 : 5+length], block[5+length:], true
}

// headerScalarFromBytes reads one typed header value.
//
// A kind this build does not name keeps its bytes, so a newer producer's
// header is compared as raw rather than failing the record.
func headerScalarFromBytes(kind uint8, value []byte) (HeaderScalar, bool) {
	switch kind {
	case headerKindString:
		if !isValidUTF8Bytes(value) {
			return HeaderScalar{}, false
		}
		return StringHeaderScalar(string(value)), true
	case headerKindBool:
		if len(value) != 1 || value[0] > 1 {
			return HeaderScalar{}, false
		}
		return BoolHeaderScalar(value[0] == 1), true
	case headerKindInt8, headerKindInt16, headerKindInt32, headerKindInt64:
		number, ok := littleEndianSigned(kind, value)
		if !ok {
			return HeaderScalar{}, false
		}
		return IntHeaderScalar(number), true
	case headerKindUint8, headerKindUint16, headerKindUint32, headerKindUint64:
		number, ok := littleEndianUnsigned(kind, value)
		if !ok {
			return HeaderScalar{}, false
		}
		return UintHeaderScalar(number), true
	case headerKindFloat32:
		if len(value) != 4 {
			return HeaderScalar{}, false
		}
		return FloatHeaderScalar(float64(
			math.Float32frombits(binary.LittleEndian.Uint32(value)))), true
	case headerKindFloat64:
		if len(value) != 8 {
			return HeaderScalar{}, false
		}
		return FloatHeaderScalar(
			math.Float64frombits(binary.LittleEndian.Uint64(value))), true
	case headerKindInt128, headerKindUint128:
		// A 128-bit integer compares as raw bytes, but only at its width.
		if len(value) != headerValueBytes {
			return HeaderScalar{}, false
		}
		return RawHeaderScalar(value), true
	default:
		return RawHeaderScalar(value), true
	}
}

func littleEndianSigned(kind uint8, value []byte) (int64, bool) {
	switch kind {
	case headerKindInt8:
		if len(value) != 1 {
			return 0, false
		}
		return int64(int8(value[0])), true
	case headerKindInt16:
		if len(value) != 2 {
			return 0, false
		}
		return int64(int16(binary.LittleEndian.Uint16(value))), true
	case headerKindInt32:
		if len(value) != 4 {
			return 0, false
		}
		return int64(int32(binary.LittleEndian.Uint32(value))), true
	default:
		if len(value) != 8 {
			return 0, false
		}
		return int64(binary.LittleEndian.Uint64(value)), true
	}
}

func littleEndianUnsigned(kind uint8, value []byte) (uint64, bool) {
	switch kind {
	case headerKindUint8:
		if len(value) != 1 {
			return 0, false
		}
		return uint64(value[0]), true
	case headerKindUint16:
		if len(value) != 2 {
			return 0, false
		}
		return uint64(binary.LittleEndian.Uint16(value)), true
	case headerKindUint32:
		if len(value) != 4 {
			return 0, false
		}
		return uint64(binary.LittleEndian.Uint32(value)), true
	default:
		if len(value) != 8 {
			return 0, false
		}
		return binary.LittleEndian.Uint64(value), true
	}
}
