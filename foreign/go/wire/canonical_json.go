package wire

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// Canonical JSON for the surfaces whose identity is a digest over their JSON
// encoding rather than their CBOR encoding.
//
// A consumer filter's digest is taken over serde_json's output, so Go must
// reproduce those bytes exactly. Go's encoding/json cannot: it renders a byte
// slice as base64 where serde renders an array of integers, and it escapes
// HTML characters that serde leaves alone. This writer walks the same
// structural tree the CBOR encoder builds, so field order, the skip rules and
// the value shapes are shared between the two encodings by construction.

// MarshalCanonicalJSON encodes a value as the compact JSON its digest covers.
func MarshalCanonicalJSON(value any) ([]byte, error) {
	item, err := toValueOf(value)
	if err != nil {
		return nil, err
	}
	var out []byte
	out, err = appendCanonicalJSON(out, item)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func appendCanonicalJSON(dst []byte, item cborform.Value) ([]byte, error) {
	var err error
	switch typed := item.(type) {
	case cborform.Null:
		return append(dst, "null"...), nil
	case cborform.Undefined:
		return append(dst, "null"...), nil
	case cborform.Bool:
		if typed {
			return append(dst, "true"...), nil
		}
		return append(dst, "false"...), nil
	case cborform.Uint:
		return strconv.AppendUint(dst, uint64(typed), 10), nil
	case cborform.NegInt:
		// The argument n encodes the value -1-n, which stays inside i64 for
		// every value the contract carries.
		return strconv.AppendInt(dst, -1-int64(typed), 10), nil
	case cborform.Float:
		return appendCanonicalFloat(dst, typed.Value)
	case cborform.Text:
		return appendCanonicalString(dst, string(typed)), nil
	case cborform.Textual:
		// A wire id and a numeric map key both carry their text spelling on a
		// human-readable surface.
		return appendCanonicalString(dst, typed.Text), nil
	case cborform.Bytes:
		// serde renders a byte string as an array of integers on the JSON
		// surface, since JSON has no byte string.
		dst = append(dst, '[')
		for i, b := range typed {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = strconv.AppendUint(dst, uint64(b), 10)
		}
		return append(dst, ']'), nil
	case cborform.Array:
		dst = append(dst, '[')
		for i, element := range typed {
			if i > 0 {
				dst = append(dst, ',')
			}
			if dst, err = appendCanonicalJSON(dst, element); err != nil {
				return nil, fmt.Errorf("element %d: %w", i, err)
			}
		}
		return append(dst, ']'), nil
	case cborform.Map:
		dst = append(dst, '{')
		for i, entry := range typed {
			if i > 0 {
				dst = append(dst, ',')
			}
			key, ok := jsonObjectKey(entry.Key)
			if !ok {
				return nil, fmt.Errorf(
					"wire: a JSON object key must be text, got %T", entry.Key)
			}
			dst = appendCanonicalString(dst, key)
			dst = append(dst, ':')
			if dst, err = appendCanonicalJSON(dst, entry.Value); err != nil {
				return nil, fmt.Errorf("field %q: %w", key, err)
			}
		}
		return append(dst, '}'), nil
	case cborform.Tag:
		return appendCanonicalJSON(dst, typed.Content)
	default:
		return nil, fmt.Errorf("wire: cannot encode %T as canonical JSON", item)
	}
}

// appendCanonicalString writes a JSON string, escaping only what the format
// requires. HTML characters are left alone, matching serde rather than Go's
// encoding/json.
func appendCanonicalString(dst []byte, value string) []byte {
	dst = append(dst, '"')
	for _, character := range value {
		switch character {
		case '"':
			dst = append(dst, '\\', '"')
		case '\\':
			dst = append(dst, '\\', '\\')
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = append(dst, '\\', 't')
		case '\b':
			dst = append(dst, '\\', 'b')
		case '\f':
			dst = append(dst, '\\', 'f')
		default:
			if character < 0x20 {
				dst = append(dst, '\\', 'u', '0', '0',
					lowerHex[character>>4], lowerHex[character&0x0f])
				continue
			}
			if character == utf8.RuneError {
				dst = append(dst, "\\ufffd"...)
				continue
			}
			dst = utf8.AppendRune(dst, character)
		}
	}
	return append(dst, '"')
}

const lowerHex = "0123456789abcdef"

// appendCanonicalFloat writes a float the way serde does: a finite value keeps
// a decimal point or an exponent so it never reads back as an integer.
//
// Only finite values are encodable, since JSON has no infinity or NaN and
// serde refuses them too.
func appendCanonicalFloat(dst []byte, value float64) ([]byte, error) {
	if math.IsInf(value, 0) || math.IsNaN(value) {
		return nil, fmt.Errorf("wire: JSON cannot carry a non-finite float")
	}
	shortest := strconv.FormatFloat(value, 'g', -1, 64)
	if !strings.ContainsAny(shortest, ".eE") {
		shortest += ".0"
	}
	return append(dst, shortest...), nil
}

// jsonObjectKey is the string a map key is spelled with on a human-readable
// surface. A numeric key carries that spelling alongside its number.
func jsonObjectKey(key cborform.Value) (string, bool) {
	switch typed := key.(type) {
	case cborform.Text:
		return string(typed), true
	case cborform.Textual:
		return typed.Text, true
	default:
		return "", false
	}
}
