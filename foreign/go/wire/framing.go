package wire

import (
	"encoding/binary"
	"fmt"
	"unicode/utf8"
)

// MaxScanDepth bounds how deep a scanned value may nest, so a hostile payload
// cannot exhaust the stack before a server has authorized the request.
const MaxScanDepth = 64

// The CBOR major types, as the scanner reads them off a head byte.
const (
	majorUint uint8 = iota
	majorNegInt
	majorBytes
	majorText
	majorArray
	majorMap
	majorTag
	majorSimple
)

// breakByte closes an indefinite-length string, array or map.
const breakByte = 0xff

// nullByte is the whole encoding of a CBOR null.
const nullByte = 0xf6

// NamedFields holds the top-level fields of one named-field CBOR map, each
// value left encoded in place.
//
// One pass over the map finds every key and the bytes of its value and copies
// nothing: a byte string is stepped over by its length. A reader then decodes
// only the small values it needs and leaves a large one alone. Keys must be
// definite text and unique, nesting is bounded, and nothing may follow the
// map, so a malformed request is refused rather than read in part.
type NamedFields struct {
	names  []string
	values [][]byte
}

// DecodeNamedFields scans the fields of payload.
func DecodeNamedFields(payload []byte) (NamedFields, error) {
	return DecodeNamedFieldsWithin(payload, -1)
}

// DecodeNamedFieldsWithin scans the fields of payload, refusing a map of more
// than maxFields fields before it scans past them, so a server can bound the
// work one request costs before it authorizes it.
//
// A negative maxFields leaves the count unbounded.
func DecodeNamedFieldsWithin(payload []byte, maxFields int) (NamedFields, error) {
	tooMany := func() error {
		return fmt.Errorf("wire: a named-field map holds more than %d fields", maxFields)
	}
	bounded := maxFields >= 0

	scan := &scanner{bytes: payload}
	major, argument, indefinite, err := scan.head()
	if err != nil {
		return NamedFields{}, err
	}
	if major != majorMap {
		return NamedFields{}, fmt.Errorf("wire: a named-field value is a map")
	}

	var fields NamedFields
	if indefinite {
		for {
			atBreak, err := scan.atBreak()
			if err != nil {
				return NamedFields{}, err
			}
			if atBreak {
				break
			}
			if bounded && len(fields.names) == maxFields {
				return NamedFields{}, tooMany()
			}
			name, value, err := scan.entry()
			if err != nil {
				return NamedFields{}, err
			}
			fields.names = append(fields.names, name)
			fields.values = append(fields.values, value)
		}
		scan.at++
	} else {
		if argument > uint64(len(payload)) || (bounded && argument > uint64(maxFields)) {
			return NamedFields{}, tooMany()
		}
		for i := uint64(0); i < argument; i++ {
			name, value, err := scan.entry()
			if err != nil {
				return NamedFields{}, err
			}
			fields.names = append(fields.names, name)
			fields.values = append(fields.values, value)
		}
	}
	if scan.at != len(payload) {
		return NamedFields{}, fmt.Errorf(
			"wire: %d trailing byte(s) after the CBOR value", len(payload)-scan.at)
	}

	seen := make(map[string]bool, len(fields.names))
	for _, name := range fields.names {
		if seen[name] {
			return NamedFields{}, fmt.Errorf("wire: duplicate field %q", name)
		}
		seen[name] = true
	}
	return fields, nil
}

// Len is how many fields the map held.
func (fields NamedFields) Len() int { return len(fields.names) }

// Names are the field names in encounter order.
func (fields NamedFields) Names() []string { return fields.names }

// Get is the encoded value of key, and false when the map does not hold it.
func (fields NamedFields) Get(key string) ([]byte, bool) {
	for index, name := range fields.names {
		if name == key {
			return fields.values[index], true
		}
	}
	return nil, false
}

// Text is the text value of key, borrowed from the payload.
//
// The second result is false when the field is absent or null, and the error
// is non-nil when it holds anything but text.
func (fields NamedFields) Text(key string) (string, bool, error) {
	value, present := fields.Get(key)
	if !present {
		return "", false, nil
	}
	if len(value) == 1 && value[0] == nullByte {
		return "", false, nil
	}
	scan := &scanner{bytes: value}
	major, length, indefinite, err := scan.head()
	if err != nil {
		return "", false, err
	}
	if major != majorText || indefinite {
		return "", false, fmt.Errorf("wire: field %q is not text", key)
	}
	text, err := scan.take(length)
	if err != nil {
		return "", false, err
	}
	if !utf8.Valid(text) {
		return "", false, fmt.Errorf("wire: field %q is not UTF-8", key)
	}
	return string(text), true, nil
}

// Value decodes the value of key on its own. The second result is false when
// the field is absent.
func (fields NamedFields) Value(key string, target any) (bool, error) {
	value, present := fields.Get(key)
	if !present {
		return false, nil
	}
	return true, Unmarshal(value, target)
}

// scanner walks a CBOR payload without decoding it.
type scanner struct {
	bytes []byte
	at    int
}

// entry reads one map entry: a definite text key and the bytes of its value.
func (scan *scanner) entry() (string, []byte, error) {
	major, length, indefinite, err := scan.head()
	if err != nil {
		return "", nil, err
	}
	if major != majorText || indefinite {
		return "", nil, fmt.Errorf("wire: a named-field key is definite text")
	}
	raw, err := scan.take(length)
	if err != nil {
		return "", nil, err
	}
	if !utf8.Valid(raw) {
		return "", nil, fmt.Errorf("wire: a named-field key is UTF-8")
	}
	start := scan.at
	if err := scan.skip(0); err != nil {
		return "", nil, err
	}
	return string(raw), scan.bytes[start:scan.at], nil
}

// head reads one CBOR head, reporting the major type, the argument, and
// whether the argument was the indefinite marker.
func (scan *scanner) head() (uint8, uint64, bool, error) {
	if scan.at >= len(scan.bytes) {
		return 0, 0, false, errTruncatedScan
	}
	initial := scan.bytes[scan.at]
	scan.at++
	major := initial >> 5
	switch info := initial & 0x1f; {
	case info <= 23:
		return major, uint64(info), false, nil
	case info == 24:
		raw, err := scan.take(1)
		if err != nil {
			return 0, 0, false, err
		}
		return major, uint64(raw[0]), false, nil
	case info == 25:
		raw, err := scan.take(2)
		if err != nil {
			return 0, 0, false, err
		}
		return major, uint64(binary.BigEndian.Uint16(raw)), false, nil
	case info == 26:
		raw, err := scan.take(4)
		if err != nil {
			return 0, 0, false, err
		}
		return major, uint64(binary.BigEndian.Uint32(raw)), false, nil
	case info == 27:
		raw, err := scan.take(8)
		if err != nil {
			return 0, 0, false, err
		}
		return major, binary.BigEndian.Uint64(raw), false, nil
	case info == 31:
		return major, 0, true, nil
	default:
		return 0, 0, false, fmt.Errorf("wire: reserved CBOR additional information")
	}
}

// skip steps over one whole value without decoding it.
func (scan *scanner) skip(depth int) error {
	if depth > MaxScanDepth {
		return fmt.Errorf("wire: CBOR nesting exceeds %d", MaxScanDepth)
	}
	major, argument, indefinite, err := scan.head()
	if err != nil {
		return err
	}
	switch {
	case (major == majorBytes || major == majorText) && !indefinite:
		_, err := scan.take(argument)
		return err

	case (major == majorBytes || major == majorText) && indefinite:
		for {
			atBreak, err := scan.atBreak()
			if err != nil {
				return err
			}
			if atBreak {
				break
			}
			chunkMajor, length, chunkIndefinite, err := scan.head()
			if err != nil {
				return err
			}
			if chunkIndefinite {
				return fmt.Errorf("wire: an indefinite string chunk is definite")
			}
			if chunkMajor != major {
				return fmt.Errorf("wire: an indefinite string chunk has its string's type")
			}
			if _, err := scan.take(length); err != nil {
				return err
			}
		}
		scan.at++
		return nil

	case major == majorArray && !indefinite:
		for i := uint64(0); i < argument; i++ {
			if err := scan.skip(depth + 1); err != nil {
				return err
			}
		}
		return nil

	case major == majorMap && !indefinite:
		for i := uint64(0); i < argument; i++ {
			if err := scan.skip(depth + 1); err != nil {
				return err
			}
			if err := scan.skip(depth + 1); err != nil {
				return err
			}
		}
		return nil

	case (major == majorArray || major == majorMap) && indefinite:
		items := 0
		for {
			atBreak, err := scan.atBreak()
			if err != nil {
				return err
			}
			if atBreak {
				break
			}
			if err := scan.skip(depth + 1); err != nil {
				return err
			}
			items++
		}
		if major == majorMap && items%2 == 1 {
			return fmt.Errorf("wire: an indefinite map holds whole pairs")
		}
		scan.at++
		return nil

	case major == majorTag && !indefinite:
		return scan.skip(depth + 1)

	case indefinite:
		return fmt.Errorf("wire: only strings, arrays and maps are indefinite")

	default:
		// An unsigned, negative or simple value is wholly in its head.
		return nil
	}
}

// atBreak reports whether the next byte closes an indefinite-length value.
func (scan *scanner) atBreak() (bool, error) {
	if scan.at >= len(scan.bytes) {
		return false, errTruncatedScan
	}
	return scan.bytes[scan.at] == breakByte, nil
}

// take consumes the next length bytes.
func (scan *scanner) take(length uint64) ([]byte, error) {
	if length > uint64(len(scan.bytes)-scan.at) {
		return nil, errTruncatedScan
	}
	start := scan.at
	scan.at += int(length)
	return scan.bytes[start:scan.at], nil
}

var errTruncatedScan = fmt.Errorf("wire: the CBOR value is truncated")

// EncodeNamedFramed encodes value as named-field CBOR behind its
// little-endian u32 length prefix, the framing the managed command sockets
// carry.
//
// It refuses a payload above maxBytes or above the shared frame cap,
// whichever is smaller, so an oversized request fails here rather than at the
// far end of the socket.
func EncodeNamedFramed(value any, maxBytes int) ([]byte, error) {
	payload, err := Marshal(value)
	if err != nil {
		return nil, err
	}
	cap := maxBytes
	if MaxFrameBytes < cap {
		cap = MaxFrameBytes
	}
	if len(payload) > cap {
		return nil, fmt.Errorf(
			"wire: payload is %dB, exceeds frame cap %dB", len(payload), cap)
	}
	framed := make([]byte, 4, 4+len(payload))
	binary.LittleEndian.PutUint32(framed, uint32(len(payload)))
	return append(framed, payload...), nil
}

// FrameEncode prefixes a payload with its little-endian u32 length, the
// framing the managed command sockets carry.
//
// It is the raw form, for a caller that already holds encoded bytes.
// EncodeNamedFramed encodes a value and frames it in one pass.
func FrameEncode(payload []byte) ([]byte, error) {
	if len(payload) > MaxFrameBytes {
		return nil, fmt.Errorf(
			"wire: payload is %dB, exceeds frame cap %dB", len(payload), MaxFrameBytes)
	}
	framed := make([]byte, 4, 4+len(payload))
	binary.LittleEndian.PutUint32(framed, uint32(len(payload)))
	return append(framed, payload...), nil
}

// FrameDecode reads one frame from the front of a buffer.
//
// The second result is how many bytes of the buffer the frame spanned, and
// the third is false when the buffer does not yet hold a whole frame, so a
// reader fills it further rather than treating a short read as an error.
func FrameDecode(buffer []byte) ([]byte, int, bool, error) {
	if len(buffer) < 4 {
		return nil, 0, false, nil
	}
	length := int(binary.LittleEndian.Uint32(buffer[:4]))
	if length > MaxFrameBytes {
		return nil, 0, false, fmt.Errorf(
			"wire: frame length %dB exceeds cap %dB", length, MaxFrameBytes)
	}
	if len(buffer) < 4+length {
		return nil, 0, false, nil
	}
	return buffer[4 : 4+length], 4 + length, true, nil
}
