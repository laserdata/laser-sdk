package wire

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// BSON element types, as the specification numbers them.
const (
	bsonDouble   = 0x01
	bsonString   = 0x02
	bsonDocument = 0x03
	bsonArray    = 0x04
	bsonBinary   = 0x05
	bsonBool     = 0x08
	bsonNull     = 0x0a
	bsonInt32    = 0x10
	bsonInt64    = 0x12
	// bsonGenericBinary is the subtype a plain byte run carries.
	bsonGenericBinary = 0x00
)

// marshalBSON writes a value tree as a BSON document.
//
// The top-level body must be a document, which is a struct or a map: BSON has
// no framing for a bare scalar.
func marshalBSON(item cborform.Value) ([]byte, error) {
	fields, ok := item.(cborform.Map)
	if !ok {
		return nil, fmt.Errorf("wire: a BSON body must be a document, got %T", item)
	}
	return appendBSONDocument(nil, fields)
}

func appendBSONDocument(out []byte, fields cborform.Map) ([]byte, error) {
	start := len(out)
	out = append(out, 0, 0, 0, 0) // the length is written once it is known
	for _, entry := range fields {
		key, ok := entry.Key.(cborform.Text)
		if !ok {
			return nil, fmt.Errorf("wire: a BSON field name must be text, got %T", entry.Key)
		}
		var err error
		if out, err = appendBSONElement(out, string(key), entry.Value); err != nil {
			return nil, err
		}
	}
	out = append(out, 0)
	return finishBSONDocument(out, start)
}

// finishBSONDocument writes the length a document is prefixed with.
func finishBSONDocument(out []byte, start int) ([]byte, error) {
	length := len(out) - start
	if length > math.MaxInt32 {
		return nil, fmt.Errorf("wire: a BSON document of %d bytes is out of range", length)
	}
	binary.LittleEndian.PutUint32(out[start:start+4], uint32(length))
	return out, nil
}

func appendBSONElement(out []byte, name string, value cborform.Value) ([]byte, error) {
	// A field name rides as a C string, so it cannot hold a null byte.
	if bytes.IndexByte([]byte(name), 0) >= 0 {
		return nil, fmt.Errorf("wire: the BSON field name %q holds a null byte", name)
	}
	appendName := func(kind byte) []byte {
		out = append(out, kind)
		out = append(out, name...)
		return append(out, 0)
	}

	switch typed := value.(type) {
	case cborform.Null, cborform.Undefined:
		return appendName(bsonNull), nil
	case cborform.Bool:
		out = appendName(bsonBool)
		if typed {
			return append(out, 1), nil
		}
		return append(out, 0), nil
	case cborform.Uint:
		if uint64(typed) > math.MaxInt64 {
			return nil, fmt.Errorf("wire: %d is above the BSON integer range", uint64(typed))
		}
		return appendBSONInteger(out, name, int64(typed)), nil
	case cborform.NegInt:
		if uint64(typed) >= 1<<63 {
			return nil, fmt.Errorf("wire: a value below the BSON integer range")
		}
		return appendBSONInteger(out, name, -1-int64(typed)), nil
	case cborform.Float:
		out = appendName(bsonDouble)
		return binary.LittleEndian.AppendUint64(out, math.Float64bits(typed.Value)), nil
	case cborform.Text:
		out = appendName(bsonString)
		out = binary.LittleEndian.AppendUint32(out, uint32(len(typed)+1))
		out = append(out, typed...)
		return append(out, 0), nil
	case cborform.Bytes:
		out = appendName(bsonBinary)
		out = binary.LittleEndian.AppendUint32(out, uint32(len(typed)))
		out = append(out, bsonGenericBinary)
		return append(out, typed...), nil
	case cborform.Array:
		// An array is a document whose field names are its indices.
		out = appendName(bsonArray)
		entries := make(cborform.Map, len(typed))
		for index, element := range typed {
			entries[index] = cborform.Entry{
				Key: cborform.Text(strconv.Itoa(index)), Value: element,
			}
		}
		return appendBSONDocument(out, entries)
	case cborform.Map:
		out = appendName(bsonDocument)
		return appendBSONDocument(out, typed)
	case cborform.Tag:
		return appendBSONElement(out, name, typed.Content)
	default:
		return nil, fmt.Errorf("wire: %T has no BSON form", value)
	}
}

// appendBSONInteger writes an integer in the narrowest of the two widths BSON
// names.
func appendBSONInteger(out []byte, name string, value int64) []byte {
	kind := byte(bsonInt64)
	if value >= math.MinInt32 && value <= math.MaxInt32 {
		kind = bsonInt32
	}
	out = append(out, kind)
	out = append(out, name...)
	out = append(out, 0)
	if kind == bsonInt32 {
		return binary.LittleEndian.AppendUint32(out, uint32(int32(value)))
	}
	return binary.LittleEndian.AppendUint64(out, uint64(value))
}

// unmarshalBSON reads one BSON document, refusing trailing bytes.
func unmarshalBSON(payload []byte) (cborform.Value, error) {
	reader := &bsonReader{bytes: payload}
	document, err := reader.document(0)
	if err != nil {
		return nil, err
	}
	if len(reader.bytes) != 0 {
		return nil, fmt.Errorf("wire: %d trailing BSON bytes", len(reader.bytes))
	}
	return document, nil
}

type bsonReader struct{ bytes []byte }

// maxBSONDepth bounds how deep one body may nest.
const maxBSONDepth = 1024

func (reader *bsonReader) take(count int) ([]byte, error) {
	if count < 0 || count > len(reader.bytes) {
		return nil, fmt.Errorf("wire: BSON payload ends mid-value")
	}
	head := reader.bytes[:count]
	reader.bytes = reader.bytes[count:]
	return head, nil
}

func (reader *bsonReader) int32() (int32, error) {
	raw, err := reader.take(4)
	if err != nil {
		return 0, err
	}
	return int32(binary.LittleEndian.Uint32(raw)), nil
}

// cstring reads a null-terminated field name.
func (reader *bsonReader) cstring() (string, error) {
	end := bytes.IndexByte(reader.bytes, 0)
	if end < 0 {
		return "", fmt.Errorf("wire: a BSON field name is not terminated")
	}
	name := string(reader.bytes[:end])
	reader.bytes = reader.bytes[end+1:]
	return name, nil
}

func (reader *bsonReader) document(depth int) (cborform.Map, error) {
	if depth > maxBSONDepth {
		return nil, fmt.Errorf("wire: BSON nests deeper than %d", maxBSONDepth)
	}
	length, err := reader.int32()
	if err != nil {
		return nil, err
	}
	// The length counts itself and the terminator.
	if length < 5 || int(length)-4 > len(reader.bytes) {
		return nil, fmt.Errorf("wire: a BSON document names %d bytes", length)
	}
	body, err := reader.take(int(length) - 4)
	if err != nil {
		return nil, err
	}
	if body[len(body)-1] != 0 {
		return nil, fmt.Errorf("wire: a BSON document is not terminated")
	}
	inner := &bsonReader{bytes: body[:len(body)-1]}

	fields := cborform.Map{}
	for len(inner.bytes) > 0 {
		kind, err := inner.take(1)
		if err != nil {
			return nil, err
		}
		name, err := inner.cstring()
		if err != nil {
			return nil, err
		}
		value, err := inner.element(kind[0], depth)
		if err != nil {
			return nil, err
		}
		fields = append(fields, cborform.Entry{Key: cborform.Text(name), Value: value})
	}
	return fields, nil
}

func (reader *bsonReader) element(kind byte, depth int) (cborform.Value, error) {
	switch kind {
	case bsonNull:
		return cborform.Null{}, nil
	case bsonBool:
		raw, err := reader.take(1)
		if err != nil {
			return nil, err
		}
		if raw[0] > 1 {
			return nil, fmt.Errorf("wire: a BSON boolean is 0x%02x", raw[0])
		}
		return cborform.Bool(raw[0] == 1), nil
	case bsonDouble:
		raw, err := reader.take(8)
		if err != nil {
			return nil, err
		}
		return cborform.Float{
			Value: math.Float64frombits(binary.LittleEndian.Uint64(raw)), Bits: 64,
		}, nil
	case bsonInt32:
		value, err := reader.int32()
		if err != nil {
			return nil, err
		}
		return integerValue(int64(value)), nil
	case bsonInt64:
		raw, err := reader.take(8)
		if err != nil {
			return nil, err
		}
		return integerValue(int64(binary.LittleEndian.Uint64(raw))), nil
	case bsonString:
		length, err := reader.int32()
		if err != nil {
			return nil, err
		}
		if length < 1 {
			return nil, fmt.Errorf("wire: a BSON string names %d bytes", length)
		}
		raw, err := reader.take(int(length))
		if err != nil {
			return nil, err
		}
		if raw[len(raw)-1] != 0 {
			return nil, fmt.Errorf("wire: a BSON string is not terminated")
		}
		return cborform.Text(raw[:len(raw)-1]), nil
	case bsonBinary:
		length, err := reader.int32()
		if err != nil {
			return nil, err
		}
		if _, err := reader.take(1); err != nil { // the subtype
			return nil, err
		}
		raw, err := reader.take(int(length))
		if err != nil {
			return nil, err
		}
		return cborform.Bytes(append([]byte{}, raw...)), nil
	case bsonDocument:
		return reader.document(depth + 1)
	case bsonArray:
		fields, err := reader.document(depth + 1)
		if err != nil {
			return nil, err
		}
		items := make(cborform.Array, len(fields))
		for index, entry := range fields {
			items[index] = entry.Value
		}
		return items, nil
	default:
		return nil, fmt.Errorf("wire: BSON element type 0x%02x is not supported", kind)
	}
}

// integerValue is the value tree's form of a signed integer.
func integerValue(value int64) cborform.Value {
	if value < 0 {
		return cborform.NegInt(^uint64(value))
	}
	return cborform.Uint(value)
}
