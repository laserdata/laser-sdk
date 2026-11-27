package wire

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// PathSegmentKind discriminates a path segment.
type PathSegmentKind uint8

const (
	// SegmentKey selects an object key.
	SegmentKey PathSegmentKind = iota
	// SegmentIndex selects an array element.
	SegmentIndex
)

// PathSegment is one step of a field path.
type PathSegment struct {
	Kind  PathSegmentKind
	Key   string
	Index uint32
}

// KeySegment selects an object key.
func KeySegment(key string) PathSegment {
	return PathSegment{Kind: SegmentKey, Key: key}
}

// IndexSegment selects an array element.
func IndexSegment(index uint32) PathSegment {
	return PathSegment{Kind: SegmentIndex, Index: index}
}

// FieldPath is a validated location inside a decoded payload.
//
// The text form separates object keys with a dot and selects array elements
// with brackets, so "after.ground_stations[0]" reads the first ground station.
// A literal dot, bracket or backslash inside a key is escaped with a backslash.
// A digit-only key stays a key, since only brackets select an index.
//
// There is exactly one text form per path, which is why the rendered string is
// also the canonical encoding the filter digest covers.
type FieldPath struct {
	segments []PathSegment
}

// NewFieldPath builds a path from explicit segments.
func NewFieldPath(segments ...PathSegment) FieldPath {
	return FieldPath{segments: segments}
}

// Segments are the path's steps.
func (path FieldPath) Segments() []PathSegment { return path.segments }

// IsZero reports an unset path.
func (path FieldPath) IsZero() bool { return len(path.segments) == 0 }

// String renders the canonical text form.
func (path FieldPath) String() string {
	var out strings.Builder
	for position, segment := range path.segments {
		switch segment.Kind {
		case SegmentKey:
			if position > 0 {
				out.WriteByte('.')
			}
			for _, character := range segment.Key {
				switch character {
				case '.', '[', ']', '\\':
					out.WriteByte('\\')
				}
				out.WriteRune(character)
			}
		case SegmentIndex:
			out.WriteByte('[')
			out.WriteString(strconv.FormatUint(uint64(segment.Index), 10))
			out.WriteByte(']')
		}
	}
	return out.String()
}

// ParseFieldPath reads the text form.
//
// It rejects empty keys, unterminated escapes or brackets, non-canonical
// indexes, and paths over the segment or byte caps.
func ParseFieldPath(text string) (FieldPath, error) {
	if text == "" {
		return FieldPath{}, &InvalidError{Reason: "field path must not be empty"}
	}
	if len(text) > MaxFilterPathBytes {
		return FieldPath{}, &InvalidError{Reason: fmt.Sprintf(
			"field path is %dB, exceeds cap %dB", len(text), MaxFilterPathBytes)}
	}

	parser := &pathParser{text: text}
	segments, err := parser.segments()
	if err != nil {
		return FieldPath{}, err
	}
	if len(segments) > MaxFilterPathSegments {
		return FieldPath{}, &InvalidError{Reason: fmt.Sprintf(
			"field path has %d segments, exceeds cap %d", len(segments), MaxFilterPathSegments)}
	}
	return FieldPath{segments: segments}, nil
}

type pathParser struct {
	text     string
	position int
}

func (parser *pathParser) segments() ([]PathSegment, error) {
	var segments []PathSegment

	first, err := parser.leadingSegment()
	if err != nil {
		return nil, err
	}
	segments = append(segments, first)

	for parser.position < len(parser.text) {
		switch parser.text[parser.position] {
		case '.':
			parser.position++
			key, err := parser.key()
			if err != nil {
				return nil, err
			}
			segments = append(segments, key)
		case '[':
			index, err := parser.index()
			if err != nil {
				return nil, err
			}
			segments = append(segments, index)
		default:
			return nil, parser.invalid("expected `.` or `[` after a segment")
		}
	}
	return segments, nil
}

func (parser *pathParser) leadingSegment() (PathSegment, error) {
	if parser.position < len(parser.text) && parser.text[parser.position] == '[' {
		return parser.index()
	}
	return parser.key()
}

func (parser *pathParser) key() (PathSegment, error) {
	var key strings.Builder
scan:
	for parser.position < len(parser.text) {
		character := parser.text[parser.position]
		switch character {
		case '.', '[':
			break scan // the key ends here
		case ']':
			return PathSegment{}, parser.invalid("unescaped `]` inside a key")
		case '\\':
			parser.position++
			if parser.position >= len(parser.text) {
				return PathSegment{}, parser.invalid("`\\` must escape `.`, `[`, `]`, or `\\`")
			}
			escaped := parser.text[parser.position]
			switch escaped {
			case '.', '[', ']', '\\':
				key.WriteByte(escaped)
				parser.position++
			default:
				return PathSegment{}, parser.invalid("`\\` must escape `.`, `[`, `]`, or `\\`")
			}
		default:
			decoded, width := utf8.DecodeRuneInString(parser.text[parser.position:])
			key.WriteRune(decoded)
			parser.position += width
		}
	}
	if key.Len() == 0 {
		return PathSegment{}, parser.invalid("empty key")
	}
	return KeySegment(key.String()), nil
}

func (parser *pathParser) index() (PathSegment, error) {
	parser.position++ // the opening bracket
	start := parser.position
	for parser.position < len(parser.text) && parser.text[parser.position] >= '0' &&
		parser.text[parser.position] <= '9' {
		parser.position++
	}
	digits := parser.text[start:parser.position]
	if parser.position >= len(parser.text) || parser.text[parser.position] != ']' {
		return PathSegment{}, parser.invalid("an index must be digits closed by `]`")
	}
	parser.position++ // the closing bracket

	// A canonical decimal: no leading zero unless the index is zero itself.
	if digits == "" || (len(digits) > 1 && digits[0] == '0') {
		return PathSegment{}, parser.invalid("an index must be a canonical decimal number")
	}
	index, err := strconv.ParseUint(digits, 10, 32)
	if err != nil {
		return PathSegment{}, parser.invalid("an index must fit in 32 bits")
	}
	return IndexSegment(uint32(index)), nil
}

func (parser *pathParser) invalid(reason string) error {
	return &InvalidError{Reason: fmt.Sprintf(
		"field path `%s` is invalid at byte %d: %s", parser.text, parser.position, reason)}
}

// MarshalCBOR writes the canonical text form.
func (path FieldPath) MarshalCBOR() (cborform.Value, error) {
	return cborform.Text(path.String()), nil
}

// UnmarshalCBOR reads and validates the text form.
func (path *FieldPath) UnmarshalCBOR(item cborform.Value) error {
	text, ok := item.(cborform.Text)
	if !ok {
		return fmt.Errorf("wire: cannot decode %T as a field path", item)
	}
	parsed, err := ParseFieldPath(string(text))
	if err != nil {
		return err
	}
	*path = parsed
	return nil
}
