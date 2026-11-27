package wire

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// MessagePack rides the same value tree the CBOR encoder builds, so a body's
// fields are named and ordered identically whichever codec carries it.

// marshalMsgpack writes a value tree as MessagePack, always in the shortest
// form a value fits.
func marshalMsgpack(item cborform.Value) ([]byte, error) {
	return appendMsgpack(nil, item)
}

func appendMsgpack(out []byte, item cborform.Value) ([]byte, error) {
	switch typed := item.(type) {
	case cborform.Null, cborform.Undefined:
		return append(out, 0xc0), nil
	case cborform.Bool:
		if typed {
			return append(out, 0xc3), nil
		}
		return append(out, 0xc2), nil
	case cborform.Uint:
		return appendMsgpackUint(out, uint64(typed)), nil
	case cborform.NegInt:
		if uint64(typed) >= 1<<63 {
			return nil, fmt.Errorf("wire: %d is below the MessagePack integer range", typed)
		}
		return appendMsgpackInt(out, -1-int64(typed)), nil
	case cborform.Float:
		// A value that round-trips through single precision rides as one.
		if single := float32(typed.Value); float64(single) == typed.Value {
			out = append(out, 0xca)
			return binary.BigEndian.AppendUint32(out, math.Float32bits(single)), nil
		}
		out = append(out, 0xcb)
		return binary.BigEndian.AppendUint64(out, math.Float64bits(typed.Value)), nil
	case cborform.Text:
		out = appendMsgpackHead(out, len(typed), 0xa0, 0xd9, 0xda, 0xdb)
		return append(out, typed...), nil
	case cborform.Bytes:
		out = appendMsgpackHead(out, len(typed), 0, 0xc4, 0xc5, 0xc6)
		return append(out, typed...), nil
	case cborform.Array:
		out = appendMsgpackHead(out, len(typed), 0x90, 0, 0xdc, 0xdd)
		for _, element := range typed {
			var err error
			if out, err = appendMsgpack(out, element); err != nil {
				return nil, err
			}
		}
		return out, nil
	case cborform.Map:
		out = appendMsgpackHead(out, len(typed), 0x80, 0, 0xde, 0xdf)
		for _, entry := range typed {
			var err error
			if out, err = appendMsgpack(out, entry.Key); err != nil {
				return nil, err
			}
			if out, err = appendMsgpack(out, entry.Value); err != nil {
				return nil, err
			}
		}
		return out, nil
	case cborform.Tag:
		// A tag has no MessagePack spelling, so the value rides without it.
		return appendMsgpack(out, typed.Content)
	default:
		return nil, fmt.Errorf("wire: %T has no MessagePack form", item)
	}
}

// appendMsgpackHead writes a length-prefixed head in the narrowest form the
// length fits. A zero code means that width has no fixed form for this type.
func appendMsgpackHead(out []byte, length int, fixed, eight, sixteen, wide byte) []byte {
	switch {
	case fixed != 0 && length < fixedMsgpackLimit(fixed):
		return append(out, fixed|byte(length))
	case eight != 0 && length <= math.MaxUint8:
		return append(out, eight, byte(length))
	case length <= math.MaxUint16:
		return binary.BigEndian.AppendUint16(append(out, sixteen), uint16(length))
	default:
		return binary.BigEndian.AppendUint32(append(out, wide), uint32(length))
	}
}

// fixedMsgpackLimit is how many values the fixed form of a head can count.
func fixedMsgpackLimit(fixed byte) int {
	if fixed == 0xa0 {
		return 32 // a fixed string counts five bits
	}
	return 16 // a fixed array or map counts four
}

func appendMsgpackUint(out []byte, value uint64) []byte {
	switch {
	case value < 0x80:
		return append(out, byte(value))
	case value <= math.MaxUint8:
		return append(out, 0xcc, byte(value))
	case value <= math.MaxUint16:
		return binary.BigEndian.AppendUint16(append(out, 0xcd), uint16(value))
	case value <= math.MaxUint32:
		return binary.BigEndian.AppendUint32(append(out, 0xce), uint32(value))
	default:
		return binary.BigEndian.AppendUint64(append(out, 0xcf), value)
	}
}

func appendMsgpackInt(out []byte, value int64) []byte {
	if value >= 0 {
		return appendMsgpackUint(out, uint64(value))
	}
	switch {
	case value >= -32:
		return append(out, byte(value))
	case value >= math.MinInt8:
		return append(out, 0xd0, byte(int8(value)))
	case value >= math.MinInt16:
		return binary.BigEndian.AppendUint16(append(out, 0xd1), uint16(int16(value)))
	case value >= math.MinInt32:
		return binary.BigEndian.AppendUint32(append(out, 0xd2), uint32(int32(value)))
	default:
		return binary.BigEndian.AppendUint64(append(out, 0xd3), uint64(value))
	}
}

// unmarshalMsgpack reads one MessagePack value, refusing trailing bytes.
func unmarshalMsgpack(payload []byte) (cborform.Value, error) {
	reader := &msgpackReader{bytes: payload}
	value, err := reader.value(0)
	if err != nil {
		return nil, err
	}
	if len(reader.bytes) != 0 {
		return nil, fmt.Errorf("wire: %d trailing MessagePack bytes", len(reader.bytes))
	}
	return value, nil
}

type msgpackReader struct{ bytes []byte }

// maxMsgpackDepth bounds how deep one body may nest, which keeps a hostile
// payload from recursing without bound.
const maxMsgpackDepth = 1024

func (reader *msgpackReader) take(count int) ([]byte, error) {
	if count < 0 || count > len(reader.bytes) {
		return nil, fmt.Errorf("wire: MessagePack payload ends mid-value")
	}
	head := reader.bytes[:count]
	reader.bytes = reader.bytes[count:]
	return head, nil
}

func (reader *msgpackReader) length(width int) (int, error) {
	raw, err := reader.take(width)
	if err != nil {
		return 0, err
	}
	switch width {
	case 1:
		return int(raw[0]), nil
	case 2:
		return int(binary.BigEndian.Uint16(raw)), nil
	default:
		size := binary.BigEndian.Uint32(raw)
		if uint64(size) > uint64(math.MaxInt32) {
			return 0, fmt.Errorf("wire: MessagePack length %d is out of range", size)
		}
		return int(size), nil
	}
}

func (reader *msgpackReader) value(depth int) (cborform.Value, error) {
	if depth > maxMsgpackDepth {
		return nil, fmt.Errorf("wire: MessagePack nests deeper than %d", maxMsgpackDepth)
	}
	head, err := reader.take(1)
	if err != nil {
		return nil, err
	}
	marker := head[0]
	switch {
	case marker < 0x80:
		return cborform.Uint(marker), nil
	case marker >= 0xe0:
		return cborform.NegInt(^uint64(int64(int8(marker)))), nil
	case marker >= 0x80 && marker <= 0x8f:
		return reader.mapOf(int(marker&0x0f), depth)
	case marker >= 0x90 && marker <= 0x9f:
		return reader.arrayOf(int(marker&0x0f), depth)
	case marker >= 0xa0 && marker <= 0xbf:
		return reader.textOf(int(marker & 0x1f))
	}
	switch marker {
	case 0xc0:
		return cborform.Null{}, nil
	case 0xc2:
		return cborform.Bool(false), nil
	case 0xc3:
		return cborform.Bool(true), nil
	case 0xc4, 0xc5, 0xc6:
		return reader.bytesOf(1 << (marker - 0xc4))
	case 0xca:
		raw, err := reader.take(4)
		if err != nil {
			return nil, err
		}
		single := math.Float32frombits(binary.BigEndian.Uint32(raw))
		return cborform.Float{Value: float64(single), Bits: 32}, nil
	case 0xcb:
		raw, err := reader.take(8)
		if err != nil {
			return nil, err
		}
		return cborform.Float{
			Value: math.Float64frombits(binary.BigEndian.Uint64(raw)), Bits: 64,
		}, nil
	case 0xcc, 0xcd, 0xce, 0xcf:
		return reader.unsignedOf(1 << (marker - 0xcc))
	case 0xd0, 0xd1, 0xd2, 0xd3:
		return reader.signedOf(1 << (marker - 0xd0))
	case 0xd9, 0xda, 0xdb:
		return reader.textOfWidth(1 << (marker - 0xd9))
	case 0xdc, 0xdd:
		count, err := reader.length(2 << (marker - 0xdc))
		if err != nil {
			return nil, err
		}
		return reader.arrayOf(count, depth)
	case 0xde, 0xdf:
		count, err := reader.length(2 << (marker - 0xde))
		if err != nil {
			return nil, err
		}
		return reader.mapOf(count, depth)
	default:
		// An extension type carries an application framing no body codec
		// writes, so it never decodes here.
		return nil, fmt.Errorf("wire: MessagePack marker 0x%02x is not supported", marker)
	}
}

func (reader *msgpackReader) unsignedOf(width int) (cborform.Value, error) {
	raw, err := reader.take(width)
	if err != nil {
		return nil, err
	}
	value := uint64(0)
	for _, octet := range raw {
		value = value<<8 | uint64(octet)
	}
	return cborform.Uint(value), nil
}

func (reader *msgpackReader) signedOf(width int) (cborform.Value, error) {
	raw, err := reader.take(width)
	if err != nil {
		return nil, err
	}
	value := int64(int8(raw[0]))
	for _, octet := range raw[1:] {
		value = value<<8 | int64(octet)
	}
	if value < 0 {
		return cborform.NegInt(^uint64(value)), nil
	}
	return cborform.Uint(value), nil
}

func (reader *msgpackReader) textOfWidth(width int) (cborform.Value, error) {
	length, err := reader.length(width)
	if err != nil {
		return nil, err
	}
	return reader.textOf(length)
}

func (reader *msgpackReader) textOf(length int) (cborform.Value, error) {
	raw, err := reader.take(length)
	if err != nil {
		return nil, err
	}
	return cborform.Text(raw), nil
}

func (reader *msgpackReader) bytesOf(width int) (cborform.Value, error) {
	length, err := reader.length(width)
	if err != nil {
		return nil, err
	}
	raw, err := reader.take(length)
	if err != nil {
		return nil, err
	}
	return cborform.Bytes(append([]byte{}, raw...)), nil
}

func (reader *msgpackReader) arrayOf(count, depth int) (cborform.Value, error) {
	// A count beyond what is left cannot be filled, so it is refused before
	// the slice is sized to it.
	if count > len(reader.bytes) {
		return nil, fmt.Errorf("wire: MessagePack array of %d is longer than its payload", count)
	}
	items := make(cborform.Array, 0, count)
	for index := 0; index < count; index++ {
		element, err := reader.value(depth + 1)
		if err != nil {
			return nil, err
		}
		items = append(items, element)
	}
	return items, nil
}

func (reader *msgpackReader) mapOf(count, depth int) (cborform.Value, error) {
	if count > len(reader.bytes) {
		return nil, fmt.Errorf("wire: MessagePack map of %d is longer than its payload", count)
	}
	entries := make(cborform.Map, 0, count)
	for index := 0; index < count; index++ {
		key, err := reader.value(depth + 1)
		if err != nil {
			return nil, err
		}
		value, err := reader.value(depth + 1)
		if err != nil {
			return nil, err
		}
		entries = append(entries, cborform.Entry{Key: key, Value: value})
	}
	return entries, nil
}
