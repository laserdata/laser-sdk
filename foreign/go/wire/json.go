package wire

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// JSON for the managed backend's HTTP surface.
//
// The bodies are the same contract types the binary band carries, so they are
// written through the same structural tree the CBOR encoder builds: the field
// order, the skip rules, and the variant tagging are shared by construction
// rather than restated in a second set of struct tags that could drift.
//
// Go's own encoding/json cannot stand in. It renders a byte slice as base64
// where the contract renders an array of integers, it escapes HTML characters
// the contract leaves alone, and it has no way to spell a wire id as its
// canonical text on one surface and its bytes on the other.

// jsonIndent is the two-space indent the contract's pretty encoding uses.
const jsonIndent = "  "

// MarshalJSON encodes a value as compact JSON.
func MarshalJSON(value any) ([]byte, error) {
	return MarshalCanonicalJSON(value)
}

// MarshalJSONIndent encodes a value as the indented JSON the HTTP surface
// serves and the shared fixtures hold.
func MarshalJSONIndent(value any) ([]byte, error) {
	item, err := toValueOf(value)
	if err != nil {
		return nil, err
	}
	return appendIndentedJSON(nil, item, 0)
}

func appendIndentedJSON(dst []byte, item cborform.Value, depth int) ([]byte, error) {
	var err error
	switch typed := item.(type) {
	case cborform.Array:
		if len(typed) == 0 {
			return append(dst, "[]"...), nil
		}
		dst = append(dst, '[')
		for index, element := range typed {
			if index > 0 {
				dst = append(dst, ',')
			}
			dst = appendJSONNewline(dst, depth+1)
			if dst, err = appendIndentedJSON(dst, element, depth+1); err != nil {
				return nil, fmt.Errorf("element %d: %w", index, err)
			}
		}
		dst = appendJSONNewline(dst, depth)
		return append(dst, ']'), nil
	case cborform.Map:
		if len(typed) == 0 {
			return append(dst, "{}"...), nil
		}
		dst = append(dst, '{')
		for index, entry := range typed {
			if index > 0 {
				dst = append(dst, ',')
			}
			dst = appendJSONNewline(dst, depth+1)
			key, ok := jsonObjectKey(entry.Key)
			if !ok {
				return nil, fmt.Errorf(
					"wire: a JSON object key must be text, got %T", entry.Key)
			}
			dst = appendCanonicalString(dst, key)
			dst = append(dst, ": "...)
			if dst, err = appendIndentedJSON(dst, entry.Value, depth+1); err != nil {
				return nil, fmt.Errorf("field %q: %w", key, err)
			}
		}
		dst = appendJSONNewline(dst, depth)
		return append(dst, '}'), nil
	case cborform.Bytes:
		// A byte string is an array of integers here, so it indents like any
		// other array.
		items := make(cborform.Array, len(typed))
		for index, octet := range typed {
			items[index] = cborform.Uint(octet)
		}
		return appendIndentedJSON(dst, items, depth)
	default:
		return appendCanonicalJSON(dst, item)
	}
}

func appendJSONNewline(dst []byte, depth int) []byte {
	dst = append(dst, '\n')
	return append(dst, strings.Repeat(jsonIndent, depth)...)
}

// UnmarshalJSON reads JSON into the value target points at, which must be a
// non-nil pointer.
func UnmarshalJSON(data []byte, target any) error {
	reader := &jsonTreeReader{data: data}
	reader.space()
	item, err := reader.value(0)
	if err != nil {
		return err
	}
	reader.space()
	if reader.at != len(data) {
		return fmt.Errorf("wire: %d trailing JSON bytes", len(data)-reader.at)
	}
	return fromValueInto(item, target)
}

// jsonTreeReader parses JSON into the structural tree the decoders read.
type jsonTreeReader struct {
	data []byte
	at   int
}

// maxJSONTreeDepth bounds how deep one body may nest.
const maxJSONTreeDepth = 256

func (reader *jsonTreeReader) space() {
	for reader.at < len(reader.data) {
		switch reader.data[reader.at] {
		case ' ', '\t', '\n', '\r':
			reader.at++
		default:
			return
		}
	}
}

func (reader *jsonTreeReader) value(depth int) (cborform.Value, error) {
	if depth > maxJSONTreeDepth {
		return nil, fmt.Errorf("wire: JSON nests deeper than %d", maxJSONTreeDepth)
	}
	reader.space()
	if reader.at >= len(reader.data) {
		return nil, fmt.Errorf("wire: JSON ends where a value was expected")
	}
	switch leader := reader.data[reader.at]; {
	case leader == '{':
		return reader.object(depth)
	case leader == '[':
		return reader.array(depth)
	case leader == '"':
		text, err := reader.text()
		return cborform.Text(text), err
	case leader == 't':
		return cborform.Bool(true), reader.literal("true")
	case leader == 'f':
		return cborform.Bool(false), reader.literal("false")
	case leader == 'n':
		return cborform.Null{}, reader.literal("null")
	default:
		return reader.number()
	}
}

func (reader *jsonTreeReader) literal(word string) error {
	if reader.at+len(word) > len(reader.data) ||
		string(reader.data[reader.at:reader.at+len(word)]) != word {
		return fmt.Errorf("wire: JSON expected %q", word)
	}
	reader.at += len(word)
	return nil
}

func (reader *jsonTreeReader) object(depth int) (cborform.Value, error) {
	reader.at++ // the opening brace
	entries := cborform.Map{}
	reader.space()
	if reader.at < len(reader.data) && reader.data[reader.at] == '}' {
		reader.at++
		return entries, nil
	}
	for {
		reader.space()
		if reader.at >= len(reader.data) || reader.data[reader.at] != '"' {
			return nil, fmt.Errorf("wire: a JSON object key must be a string")
		}
		key, err := reader.text()
		if err != nil {
			return nil, err
		}
		reader.space()
		if reader.at >= len(reader.data) || reader.data[reader.at] != ':' {
			return nil, fmt.Errorf("wire: a JSON object field needs a colon")
		}
		reader.at++
		value, err := reader.value(depth + 1)
		if err != nil {
			return nil, err
		}
		entries = append(entries, cborform.Entry{Key: cborform.Text(key), Value: value})
		reader.space()
		if reader.at >= len(reader.data) {
			return nil, fmt.Errorf("wire: a JSON object is not closed")
		}
		switch reader.data[reader.at] {
		case ',':
			reader.at++
		case '}':
			reader.at++
			return entries, nil
		default:
			return nil, fmt.Errorf("wire: a JSON object needs a comma or a brace")
		}
	}
}

func (reader *jsonTreeReader) array(depth int) (cborform.Value, error) {
	reader.at++ // the opening bracket
	items := cborform.Array{}
	reader.space()
	if reader.at < len(reader.data) && reader.data[reader.at] == ']' {
		reader.at++
		return items, nil
	}
	for {
		element, err := reader.value(depth + 1)
		if err != nil {
			return nil, err
		}
		items = append(items, element)
		reader.space()
		if reader.at >= len(reader.data) {
			return nil, fmt.Errorf("wire: a JSON array is not closed")
		}
		switch reader.data[reader.at] {
		case ',':
			reader.at++
		case ']':
			reader.at++
			return items, nil
		default:
			return nil, fmt.Errorf("wire: a JSON array needs a comma or a bracket")
		}
	}
}

// text reads one JSON string, resolving its escapes.
func (reader *jsonTreeReader) text() (string, error) {
	scanner := jsonScanner{data: reader.data, at: reader.at}
	value, ok := scanner.string()
	if !ok {
		return "", fmt.Errorf("wire: a JSON string is malformed")
	}
	reader.at = scanner.at
	return value, nil
}

// number reads one JSON number, keeping an integer exact.
func (reader *jsonTreeReader) number() (cborform.Value, error) {
	scanner := jsonScanner{data: reader.data, at: reader.at}
	value, ok := scanner.number()
	if !ok {
		return nil, fmt.Errorf("wire: a JSON number is malformed")
	}
	reader.at = scanner.at
	if !value.isFloat {
		if unsigned, err := strconv.ParseUint(value.number, 10, 64); err == nil {
			return cborform.Uint(unsigned), nil
		}
		if signed, err := strconv.ParseInt(value.number, 10, 64); err == nil && signed < 0 {
			return cborform.NegInt(^uint64(signed)), nil
		}
	}
	float, err := strconv.ParseFloat(value.number, 64)
	if err != nil {
		return nil, fmt.Errorf("wire: a JSON number is out of range")
	}
	return cborform.Float{Value: float, Bits: 64}, nil
}
