// Package cborform is a structural CBOR reader and writer used to pin the
// canonical byte layout of the LaserData wire band.
//
// The band encodes every frame as named-field CBOR produced by ciborium
// (wire/src/framing.rs). A Go port only interoperates if it reproduces those
// bytes exactly, so this package decodes a fixture into an order-preserving
// tree and re-emits it. Re-emission chooses the shortest legal head for every
// length and integer rather than copying the input's head, so a byte-identical
// round trip is evidence about the encoding rules, not a memcpy.
//
// Map key order is preserved because serde writes struct fields in declaration
// order, and sorting keys here would hide a mismatch.
package cborform

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// Major types, per RFC 8949 section 3.1.
const (
	majorUint   = 0
	majorNegInt = 1
	majorBytes  = 2
	majorText   = 3
	majorArray  = 4
	majorMap    = 5
	majorTag    = 6
	majorSimple = 7
)

const indefiniteAI = 31

// Value is one decoded CBOR data item.
type Value interface{ isValue() }

// Uint is major type 0.
type Uint uint64

// NegInt is major type 1, holding the raw encoded argument n for the value
// -1-n, so the full 64-bit negative range survives a round trip.
type NegInt uint64

// Bytes is major type 2.
type Bytes []byte

// Textual is a value whose binary and human-readable spellings differ.
//
// A wire id rides as bytes in the binary encoding and as its text form in a
// human-readable one. An integer map key rides as an integer in the binary
// encoding and as a string in JSON, which has no other kind of key. Holding
// both spellings here lets one value tree serve both encodings, so the field
// order and the skip rules can never drift between them.
type Textual struct {
	// Binary is what the binary encoding writes.
	Binary Value
	// Text is what a human-readable encoding writes, always as a string.
	Text string
}

// Text is major type 3.
type Text string

// Array is major type 4.
type Array []Value

// Entry is one key/value pair of a Map, keeping its position.
type Entry struct {
	Key   Value
	Value Value
}

// Map is major type 5 with its original entry order intact.
type Map []Entry

// Tag is major type 6.
type Tag struct {
	Number  uint64
	Content Value
}

// Bool, Null and Undefined are the simple values the band uses.
type Bool bool

// Null is the CBOR null simple value.
type Null struct{}

// Undefined is the CBOR undefined simple value.
type Undefined struct{}

// Simple is any other simple value, by its raw code.
type Simple uint8

// Float is a floating point value together with the width it was encoded at,
// because shortening a float is not always value-preserving and the band's
// encoder picks the width.
type Float struct {
	Value float64
	Bits  int // 16, 32 or 64
}

func (Uint) isValue()      {}
func (NegInt) isValue()    {}
func (Bytes) isValue()     {}
func (Textual) isValue()   {}
func (Text) isValue()      {}
func (Array) isValue()     {}
func (Map) isValue()       {}
func (Tag) isValue()       {}
func (Bool) isValue()      {}
func (Null) isValue()      {}
func (Undefined) isValue() {}
func (Simple) isValue()    {}
func (Float) isValue()     {}

// ErrTrailing reports bytes left over after one complete data item. A band
// payload is exactly one item, matching decode_named in wire/src/framing.rs.
var ErrTrailing = errors.New("cborform: trailing bytes after the CBOR value")

// Decode reads exactly one data item from input and rejects trailing bytes.
func Decode(input []byte) (Value, error) {
	value, rest, err := decodeItem(input)
	if err != nil {
		return nil, err
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("%w: %d byte(s)", ErrTrailing, len(rest))
	}
	return value, nil
}

// head reads the initial byte and its argument.
func head(input []byte) (major byte, ai byte, arg uint64, rest []byte, err error) {
	if len(input) == 0 {
		return 0, 0, 0, nil, errors.New("cborform: unexpected end of input")
	}
	initial := input[0]
	major = initial >> 5
	ai = initial & 0x1f
	rest = input[1:]

	switch {
	case ai < 24:
		return major, ai, uint64(ai), rest, nil
	case ai == 24:
		if len(rest) < 1 {
			return 0, 0, 0, nil, errors.New("cborform: truncated uint8 argument")
		}
		return major, ai, uint64(rest[0]), rest[1:], nil
	case ai == 25:
		if len(rest) < 2 {
			return 0, 0, 0, nil, errors.New("cborform: truncated uint16 argument")
		}
		return major, ai, uint64(binary.BigEndian.Uint16(rest)), rest[2:], nil
	case ai == 26:
		if len(rest) < 4 {
			return 0, 0, 0, nil, errors.New("cborform: truncated uint32 argument")
		}
		return major, ai, uint64(binary.BigEndian.Uint32(rest)), rest[4:], nil
	case ai == 27:
		if len(rest) < 8 {
			return 0, 0, 0, nil, errors.New("cborform: truncated uint64 argument")
		}
		return major, ai, binary.BigEndian.Uint64(rest), rest[8:], nil
	case ai == indefiniteAI:
		return major, ai, 0, rest, nil
	default:
		return 0, 0, 0, nil, fmt.Errorf("cborform: reserved additional information %d", ai)
	}
}

func decodeItem(input []byte) (Value, []byte, error) {
	major, ai, arg, rest, err := head(input)
	if err != nil {
		return nil, nil, err
	}

	switch major {
	case majorUint:
		if ai == indefiniteAI {
			return nil, nil, errors.New("cborform: indefinite length is not valid for an integer")
		}
		return Uint(arg), rest, nil

	case majorNegInt:
		if ai == indefiniteAI {
			return nil, nil, errors.New("cborform: indefinite length is not valid for an integer")
		}
		return NegInt(arg), rest, nil

	case majorBytes, majorText:
		if ai == indefiniteAI {
			return nil, nil, errors.New("cborform: indefinite length strings are not used by the band")
		}
		if uint64(len(rest)) < arg {
			return nil, nil, fmt.Errorf("cborform: string declares %d byte(s), %d remain", arg, len(rest))
		}
		payload := rest[:arg]
		rest = rest[arg:]
		if major == majorBytes {
			out := make(Bytes, len(payload))
			copy(out, payload)
			return out, rest, nil
		}
		return Text(payload), rest, nil

	case majorArray:
		if ai == indefiniteAI {
			return nil, nil, errors.New("cborform: indefinite length arrays are not used by the band")
		}
		// A hostile header can declare far more elements than follow, so grow
		// on demand rather than preallocating from the declared count.
		items := Array{}
		for i := uint64(0); i < arg; i++ {
			var item Value
			item, rest, err = decodeItem(rest)
			if err != nil {
				return nil, nil, fmt.Errorf("array element %d: %w", i, err)
			}
			items = append(items, item)
		}
		return items, rest, nil

	case majorMap:
		if ai == indefiniteAI {
			return nil, nil, errors.New("cborform: indefinite length maps are not used by the band")
		}
		entries := Map{}
		for i := uint64(0); i < arg; i++ {
			var key, value Value
			key, rest, err = decodeItem(rest)
			if err != nil {
				return nil, nil, fmt.Errorf("map key %d: %w", i, err)
			}
			value, rest, err = decodeItem(rest)
			if err != nil {
				return nil, nil, fmt.Errorf("map value %d: %w", i, err)
			}
			entries = append(entries, Entry{Key: key, Value: value})
		}
		return entries, rest, nil

	case majorTag:
		if ai == indefiniteAI {
			return nil, nil, errors.New("cborform: indefinite length is not valid for a tag")
		}
		content, rest, err := decodeItem(rest)
		if err != nil {
			return nil, nil, fmt.Errorf("tag %d content: %w", arg, err)
		}
		return Tag{Number: arg, Content: content}, rest, nil

	case majorSimple:
		switch ai {
		case 20:
			return Bool(false), rest, nil
		case 21:
			return Bool(true), rest, nil
		case 22:
			return Null{}, rest, nil
		case 23:
			return Undefined{}, rest, nil
		case 25:
			return Float{Value: float64(math.Float32frombits(halfToSingleBits(uint16(arg)))), Bits: 16}, rest, nil
		case 26:
			return Float{Value: float64(math.Float32frombits(uint32(arg))), Bits: 32}, rest, nil
		case 27:
			return Float{Value: math.Float64frombits(arg), Bits: 64}, rest, nil
		case indefiniteAI:
			return nil, nil, errors.New("cborform: unexpected break")
		default:
			return Simple(arg), rest, nil
		}

	default:
		return nil, nil, fmt.Errorf("cborform: unhandled major type %d", major)
	}
}

// halfToSingleBits widens an IEEE 754 half to the bit pattern of a single.
func halfToSingleBits(half uint16) uint32 {
	sign := uint32(half&0x8000) << 16
	exponent := uint32(half>>10) & 0x1f
	mantissa := uint32(half & 0x03ff)

	switch exponent {
	case 0:
		if mantissa == 0 {
			return sign // signed zero
		}
		// Subnormal half: renormalise into a single's exponent range.
		exponent = 127 - 15 + 1
		for mantissa&0x0400 == 0 {
			mantissa <<= 1
			exponent--
		}
		mantissa &= 0x03ff
		return sign | exponent<<23 | mantissa<<13
	case 0x1f:
		return sign | 0xff<<23 | mantissa<<13 // inf / NaN
	default:
		return sign | (exponent+127-15)<<23 | mantissa<<13
	}
}

// Encode writes value back out, choosing the shortest legal head for every
// integer and length.
func Encode(value Value) ([]byte, error) {
	var out []byte
	out, err := encodeInto(out, value)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// appendHead writes major type and argument in the shortest legal form.
func appendHead(dst []byte, major byte, arg uint64) []byte {
	base := major << 5
	switch {
	case arg < 24:
		return append(dst, base|byte(arg))
	case arg <= math.MaxUint8:
		return append(dst, base|24, byte(arg))
	case arg <= math.MaxUint16:
		dst = append(dst, base|25)
		return binary.BigEndian.AppendUint16(dst, uint16(arg))
	case arg <= math.MaxUint32:
		dst = append(dst, base|26)
		return binary.BigEndian.AppendUint32(dst, uint32(arg))
	default:
		dst = append(dst, base|27)
		return binary.BigEndian.AppendUint64(dst, arg)
	}
}

func encodeInto(dst []byte, value Value) ([]byte, error) {
	switch v := value.(type) {
	case Uint:
		return appendHead(dst, majorUint, uint64(v)), nil
	case NegInt:
		return appendHead(dst, majorNegInt, uint64(v)), nil
	case Bytes:
		dst = appendHead(dst, majorBytes, uint64(len(v)))
		return append(dst, v...), nil
	case Textual:
		// The binary encoding writes the binary spelling. The text is for a
		// human-readable encoding only.
		return encodeInto(dst, v.Binary)
	case Text:
		dst = appendHead(dst, majorText, uint64(len(v)))
		return append(dst, v...), nil
	case Array:
		dst = appendHead(dst, majorArray, uint64(len(v)))
		var err error
		for i, item := range v {
			if dst, err = encodeInto(dst, item); err != nil {
				return nil, fmt.Errorf("array element %d: %w", i, err)
			}
		}
		return dst, nil
	case Map:
		dst = appendHead(dst, majorMap, uint64(len(v)))
		var err error
		for i, entry := range v {
			if dst, err = encodeInto(dst, entry.Key); err != nil {
				return nil, fmt.Errorf("map key %d: %w", i, err)
			}
			if dst, err = encodeInto(dst, entry.Value); err != nil {
				return nil, fmt.Errorf("map value %d: %w", i, err)
			}
		}
		return dst, nil
	case Tag:
		dst = appendHead(dst, majorTag, v.Number)
		return encodeInto(dst, v.Content)
	case Bool:
		if v {
			return append(dst, majorSimple<<5|21), nil
		}
		return append(dst, majorSimple<<5|20), nil
	case Null:
		return append(dst, majorSimple<<5|22), nil
	case Undefined:
		return append(dst, majorSimple<<5|23), nil
	case Simple:
		if v < 24 {
			return append(dst, majorSimple<<5|byte(v)), nil
		}
		return append(dst, majorSimple<<5|24, byte(v)), nil
	case Float:
		// The band encodes a float at the narrowest width that preserves its
		// value exactly, so the width is derived from the value rather than
		// copied from the input.
		if half, ok := float64ToHalfBits(v.Value); ok {
			dst = append(dst, majorSimple<<5|25)
			return binary.BigEndian.AppendUint16(dst, half), nil
		}
		if single := float32(v.Value); float64(single) == v.Value || math.IsNaN(v.Value) {
			dst = append(dst, majorSimple<<5|26)
			return binary.BigEndian.AppendUint32(dst, math.Float32bits(single)), nil
		}
		dst = append(dst, majorSimple<<5|27)
		return binary.BigEndian.AppendUint64(dst, math.Float64bits(v.Value)), nil
	default:
		return nil, fmt.Errorf("cborform: unhandled value %T", value)
	}
}

// float64ToHalfBits returns the IEEE 754 half encoding of value when the half
// represents it exactly, matching the narrowest-width rule the band encodes
// floats under. The second result reports whether the half is exact.
func float64ToHalfBits(value float64) (uint16, bool) {
	single := float32(value)
	if float64(single) != value && !math.IsNaN(value) {
		return 0, false
	}
	bits := math.Float32bits(single)
	sign := uint16(bits>>16) & 0x8000
	biased := (bits >> 23) & 0xff
	mantissa := bits & 0x7fffff

	// Infinity and NaN keep the exponent pattern. A NaN payload survives only
	// when its significant bits fit in the half's ten.
	if biased == 0xff {
		if mantissa == 0 {
			return sign | 0x7c00, true
		}
		payload := uint16(mantissa >> 13)
		if uint32(payload)<<13 != mantissa || payload == 0 {
			return 0, false
		}
		return sign | 0x7c00 | payload, true
	}

	if bits<<1 == 0 {
		return sign, true // signed zero
	}

	exponent := int32(biased) - 127
	switch {
	case exponent >= -14 && exponent <= 15:
		if mantissa&0x1fff != 0 {
			return 0, false // the low mantissa bits would be lost
		}
		return sign | uint16((exponent+15)<<10) | uint16(mantissa>>13), true
	case exponent >= -24 && exponent < -14:
		// Subnormal half: restore the implicit leading one, then shift.
		full := mantissa | 0x800000
		shift := uint32(-exponent - 1)
		if full&((1<<shift)-1) != 0 {
			return 0, false
		}
		return sign | uint16(full>>shift), true
	default:
		return 0, false
	}
}
