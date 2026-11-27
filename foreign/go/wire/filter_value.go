package wire

import (
	"math"
	"strconv"
	"unicode/utf16"
	"unicode/utf8"
)

// DecodeLimits bounds decoding one payload. They are checked before the
// payload is parsed, so a hostile record cannot make the decoder do the work
// the limits exist to prevent.
type DecodeLimits struct {
	MaxPayloadBytes int
	MaxDepth        int
}

// DefaultDecodeLimits are the limits a reader applies when it names none.
func DefaultDecodeLimits() DecodeLimits {
	return DecodeLimits{MaxPayloadBytes: 1024 * 1024, MaxDepth: 64}
}

// parseDepth is the depth the decoders enforce.
func (limits DecodeLimits) parseDepth() int {
	if limits.MaxDepth < MaxFilterParseDepth {
		return limits.MaxDepth
	}
	return MaxFilterParseDepth
}

// payloadValue is one decoded payload node.
//
// Numbers keep their written text rather than becoming floats, because the
// contract compares integers exactly: a double cannot tell 9007199254740993
// from 9007199254740992, and a filter that rounded would select the wrong
// records.
type payloadValue struct {
	kind   payloadKind
	flag   bool
	number string
	// isFloat records that the number was written as a floating-point value.
	// An integral one is still not an integer: a timestamp coercion reads an
	// epoch from an integer and leaves a double undecidable, so the written
	// form has to survive decoding.
	isFloat bool
	text    string
	array   []payloadValue
	object  map[string]payloadValue
}

type payloadKind uint8

const (
	payloadNull payloadKind = iota
	payloadBool
	payloadNumber
	payloadText
	payloadArray
	payloadObject
)

func (value payloadValue) isNull() bool { return value.kind == payloadNull }

// resolvePath walks a field path into a decoded payload. A segment that does
// not match the value's shape leaves the field missing.
func resolvePath(value payloadValue, path FieldPath) (payloadValue, bool) {
	current := value
	for _, segment := range path.Segments() {
		switch {
		case segment.Kind == SegmentKey && current.kind == payloadObject:
			next, present := current.object[segment.Key]
			if !present {
				return payloadValue{}, false
			}
			current = next
		case segment.Kind == SegmentIndex && current.kind == payloadArray:
			if int(segment.Index) >= len(current.array) {
				return payloadValue{}, false
			}
			current = current.array[segment.Index]
		default:
			return payloadValue{}, false
		}
	}
	return current, true
}

// decodeJSONPayload decodes a JSON payload within the limits.
//
// The depth is checked by a scan over the raw bytes before any value is built,
// so a hostile payload cannot make the parser recurse or allocate past the
// limit. Values the filter never reads obey the same JSON rules as the ones it
// does, so a number out of range or a lone surrogate anywhere is a fault
// rather than a field that quietly reads as missing.
func decodeJSONPayload(payload []byte, limits DecodeLimits) (payloadValue, FaultReason) {
	if len(payload) > limits.MaxPayloadBytes {
		return payloadValue{}, FaultTooLarge
	}
	if !jsonDepthWithin(payload, limits.parseDepth()) {
		return payloadValue{}, FaultTooDeep
	}
	scanner := jsonScanner{data: payload}
	value, ok := scanner.value()
	if !ok {
		return payloadValue{}, FaultMalformed
	}
	scanner.space()
	if scanner.at != len(payload) {
		return payloadValue{}, FaultMalformed
	}
	return value, ""
}

// jsonDepthWithin reports whether the raw bytes stay within max open brackets.
// Brackets inside string bodies are data, not structure.
func jsonDepthWithin(payload []byte, maxDepth int) bool {
	depth := 0
	for index := 0; index < len(payload); index++ {
		switch payload[index] {
		case '"':
			index++
			for index < len(payload) && payload[index] != '"' {
				if payload[index] == '\\' {
					index++
				}
				index++
			}
		case '[', '{':
			depth++
			if depth > maxDepth {
				return false
			}
		case ']', '}':
			if depth > 0 {
				depth--
			}
		}
	}
	return true
}

// jsonScanner parses one JSON document under the rules the Rust evaluator's
// parser applies: no lone surrogates, no unescaped control characters, no
// number outside the double range, and no trailing content.
type jsonScanner struct {
	data []byte
	at   int
}

func (scanner *jsonScanner) space() {
	for scanner.at < len(scanner.data) {
		switch scanner.data[scanner.at] {
		case ' ', '\t', '\n', '\r':
			scanner.at++
		default:
			return
		}
	}
}

func (scanner *jsonScanner) literal(word string, value payloadValue) (payloadValue, bool) {
	if scanner.at+len(word) > len(scanner.data) ||
		string(scanner.data[scanner.at:scanner.at+len(word)]) != word {
		return payloadValue{}, false
	}
	scanner.at += len(word)
	return value, true
}

func (scanner *jsonScanner) value() (payloadValue, bool) {
	scanner.space()
	if scanner.at >= len(scanner.data) {
		return payloadValue{}, false
	}
	switch leader := scanner.data[scanner.at]; leader {
	case 'n':
		return scanner.literal("null", payloadValue{kind: payloadNull})
	case 't':
		return scanner.literal("true", payloadValue{kind: payloadBool, flag: true})
	case 'f':
		return scanner.literal("false", payloadValue{kind: payloadBool})
	case '"':
		text, ok := scanner.string()
		return payloadValue{kind: payloadText, text: text}, ok
	case '[':
		return scanner.array()
	case '{':
		return scanner.object()
	default:
		if leader == '-' || (leader >= '0' && leader <= '9') {
			return scanner.number()
		}
		return payloadValue{}, false
	}
}

func (scanner *jsonScanner) array() (payloadValue, bool) {
	scanner.at++ // the opening bracket
	items := []payloadValue{}
	scanner.space()
	if scanner.at < len(scanner.data) && scanner.data[scanner.at] == ']' {
		scanner.at++
		return payloadValue{kind: payloadArray, array: items}, true
	}
	for {
		item, ok := scanner.value()
		if !ok {
			return payloadValue{}, false
		}
		items = append(items, item)
		scanner.space()
		if scanner.at >= len(scanner.data) {
			return payloadValue{}, false
		}
		switch scanner.data[scanner.at] {
		case ',':
			scanner.at++
		case ']':
			scanner.at++
			return payloadValue{kind: payloadArray, array: items}, true
		default:
			return payloadValue{}, false
		}
	}
}

// A repeated key keeps its last value, which is what the Rust and TypeScript
// evaluators read.
func (scanner *jsonScanner) object() (payloadValue, bool) {
	scanner.at++ // the opening brace
	fields := map[string]payloadValue{}
	scanner.space()
	if scanner.at < len(scanner.data) && scanner.data[scanner.at] == '}' {
		scanner.at++
		return payloadValue{kind: payloadObject, object: fields}, true
	}
	for {
		scanner.space()
		if scanner.at >= len(scanner.data) || scanner.data[scanner.at] != '"' {
			return payloadValue{}, false
		}
		key, ok := scanner.string()
		if !ok {
			return payloadValue{}, false
		}
		scanner.space()
		if scanner.at >= len(scanner.data) || scanner.data[scanner.at] != ':' {
			return payloadValue{}, false
		}
		scanner.at++
		value, ok := scanner.value()
		if !ok {
			return payloadValue{}, false
		}
		fields[key] = value
		scanner.space()
		if scanner.at >= len(scanner.data) {
			return payloadValue{}, false
		}
		switch scanner.data[scanner.at] {
		case ',':
			scanner.at++
		case '}':
			scanner.at++
			return payloadValue{kind: payloadObject, object: fields}, true
		default:
			return payloadValue{}, false
		}
	}
}

func (scanner *jsonScanner) string() (string, bool) {
	scanner.at++ // the opening quote
	var builder []byte
	for {
		if scanner.at >= len(scanner.data) {
			return "", false
		}
		character := scanner.data[scanner.at]
		switch {
		case character == '"':
			scanner.at++
			return string(builder), true
		case character < 0x20:
			// An unescaped control character is not valid JSON.
			return "", false
		case character == '\\':
			scanner.at++
			escaped, ok := scanner.escape()
			if !ok {
				return "", false
			}
			builder = append(builder, escaped...)
		case character < utf8.RuneSelf:
			builder = append(builder, character)
			scanner.at++
		default:
			decoded, size := utf8.DecodeRune(scanner.data[scanner.at:])
			if decoded == utf8.RuneError && size <= 1 {
				// The payload is not valid UTF-8.
				return "", false
			}
			builder = append(builder, scanner.data[scanner.at:scanner.at+size]...)
			scanner.at += size
		}
	}
}

// escape reads one escape sequence, including a surrogate pair. A lone
// surrogate is refused rather than replaced, so a payload that is invalid for
// one SDK is invalid for all of them.
func (scanner *jsonScanner) escape() ([]byte, bool) {
	if scanner.at >= len(scanner.data) {
		return nil, false
	}
	character := scanner.data[scanner.at]
	scanner.at++
	switch character {
	case '"':
		return []byte{'"'}, true
	case '\\':
		return []byte{'\\'}, true
	case '/':
		return []byte{'/'}, true
	case 'b':
		return []byte{'\b'}, true
	case 'f':
		return []byte{'\f'}, true
	case 'n':
		return []byte{'\n'}, true
	case 'r':
		return []byte{'\r'}, true
	case 't':
		return []byte{'\t'}, true
	case 'u':
		first, ok := scanner.hex4()
		if !ok {
			return nil, false
		}
		if !utf16.IsSurrogate(rune(first)) {
			return []byte(string(rune(first))), true
		}
		// A leading surrogate must be followed by its trailing half.
		if scanner.at+1 >= len(scanner.data) ||
			scanner.data[scanner.at] != '\\' || scanner.data[scanner.at+1] != 'u' {
			return nil, false
		}
		scanner.at += 2
		second, ok := scanner.hex4()
		if !ok {
			return nil, false
		}
		paired := utf16.DecodeRune(rune(first), rune(second))
		if paired == utf8.RuneError {
			return nil, false
		}
		return []byte(string(paired)), true
	default:
		return nil, false
	}
}

func (scanner *jsonScanner) hex4() (uint16, bool) {
	if scanner.at+4 > len(scanner.data) {
		return 0, false
	}
	value := uint16(0)
	for _, digit := range scanner.data[scanner.at : scanner.at+4] {
		switch {
		case digit >= '0' && digit <= '9':
			value = value<<4 | uint16(digit-'0')
		case digit >= 'a' && digit <= 'f':
			value = value<<4 | uint16(digit-'a'+10)
		case digit >= 'A' && digit <= 'F':
			value = value<<4 | uint16(digit-'A'+10)
		default:
			return 0, false
		}
	}
	scanner.at += 4
	return value, true
}

// number reads one JSON number. A value that does not fit a 64-bit integer
// becomes a double, and one that overflows the double range is malformed, the
// same boundary the Rust parser draws.
func (scanner *jsonScanner) number() (payloadValue, bool) {
	start := scanner.at
	if scanner.at < len(scanner.data) && scanner.data[scanner.at] == '-' {
		scanner.at++
	}
	if !scanner.digits(true) {
		return payloadValue{}, false
	}
	if scanner.at < len(scanner.data) && scanner.data[scanner.at] == '.' {
		scanner.at++
		if !scanner.digits(false) {
			return payloadValue{}, false
		}
	}
	if scanner.at < len(scanner.data) &&
		(scanner.data[scanner.at] == 'e' || scanner.data[scanner.at] == 'E') {
		scanner.at++
		if scanner.at < len(scanner.data) &&
			(scanner.data[scanner.at] == '+' || scanner.data[scanner.at] == '-') {
			scanner.at++
		}
		if !scanner.digits(false) {
			return payloadValue{}, false
		}
	}
	text := string(scanner.data[start:scanner.at])
	if _, err := strconv.ParseInt(text, 10, 64); err == nil {
		return payloadValue{kind: payloadNumber, number: text}, true
	}
	if _, err := strconv.ParseUint(text, 10, 64); err == nil {
		return payloadValue{kind: payloadNumber, number: text}, true
	}
	// Anything else is a double, and a double out of range is not a number.
	float, err := strconv.ParseFloat(text, 64)
	if err != nil && !isRangeError(err) {
		return payloadValue{}, false
	}
	if math.IsInf(float, 0) || math.IsNaN(float) {
		return payloadValue{}, false
	}
	return payloadValue{kind: payloadNumber, number: text, isFloat: true}, true
}

func isRangeError(err error) bool {
	numeric, ok := err.(*strconv.NumError)
	return ok && numeric.Err == strconv.ErrRange
}

// digits reads one run of digits. JSON forbids a leading zero before another
// digit in the integer part.
func (scanner *jsonScanner) digits(leading bool) bool {
	start := scanner.at
	for scanner.at < len(scanner.data) &&
		scanner.data[scanner.at] >= '0' && scanner.data[scanner.at] <= '9' {
		scanner.at++
	}
	if scanner.at == start {
		return false
	}
	if leading && scanner.data[start] == '0' && scanner.at-start > 1 {
		return false
	}
	return true
}
