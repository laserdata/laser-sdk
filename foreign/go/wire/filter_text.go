package wire

import (
	"fmt"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// TextMatch is how a text predicate compares a value to its pattern.
type TextMatch string

const (
	// TextEquals matches the whole value.
	TextEquals TextMatch = "equals"
	// TextPrefix matches a value that starts with the pattern.
	TextPrefix TextMatch = "prefix"
	// TextSuffix matches a value that ends with the pattern.
	TextSuffix TextMatch = "suffix"
	// TextContains matches the pattern anywhere in the value.
	TextContains TextMatch = "contains"
	// TextGlob matches the whole value against a glob, where a star is any run
	// of characters, a question mark is one character, and a backslash escapes
	// the next character.
	TextGlob TextMatch = "glob"
	// TextRegex matches a regular expression anywhere in the value, run in
	// time linear in the value. Lookaround, backreferences and inline flags
	// are refused. The character classes are Unicode-aware, and
	// case-insensitive matching uses simple case folding where the other
	// kinds lowercase both sides.
	TextRegex TextMatch = "regex"
)

// IsKnown reports whether this build names the match kind.
func (match TextMatch) IsKnown() bool {
	switch match {
	case TextEquals, TextPrefix, TextSuffix, TextContains, TextGlob, TextRegex:
		return true
	default:
		return false
	}
}

// MarshalCBOR writes the snake_case name.
func (match TextMatch) MarshalCBOR() (cborform.Value, error) {
	if !match.IsKnown() {
		return nil, fmt.Errorf("wire: unknown text match %q", string(match))
	}
	return cborform.Text(string(match)), nil
}

// UnmarshalCBOR reads the snake_case name.
func (match *TextMatch) UnmarshalCBOR(item cborform.Value) error {
	text, ok := item.(cborform.Text)
	if !ok {
		return fmt.Errorf("wire: cannot decode %T as a text match", item)
	}
	candidate := TextMatch(text)
	if !candidate.IsKnown() {
		return fmt.Errorf("wire: unknown text match %q", string(text))
	}
	*match = candidate
	return nil
}

// TextPredicate is a text match on one payload field or user header.
//
// The field is a field path for a payload match and an exact header key for a
// header match, which is why the two ride different expression variants over
// the same shape.
type TextPredicate struct {
	Field           string    `cbor:"field" json:"field"`
	Kind            TextMatch `cbor:"kind" json:"kind"`
	Pattern         string    `cbor:"pattern" json:"pattern"`
	CaseInsensitive bool      `cbor:"case_insensitive" json:"case_insensitive"`
}

// TimestampFormat is how a coerced timestamp literal is read.
type TimestampFormat string

const (
	// FormatRFC3339 is RFC 3339 text with an explicit Z or offset. Fractions
	// up to nanoseconds are accepted when the digits past microseconds are
	// zero.
	FormatRFC3339 TimestampFormat = "rfc3339"
	// FormatEpochSeconds is whole seconds since the Unix epoch, as a number or
	// an integer string.
	FormatEpochSeconds TimestampFormat = "epoch_seconds"
	// FormatEpochMillis is whole milliseconds since the Unix epoch.
	FormatEpochMillis TimestampFormat = "epoch_millis"
	// FormatEpochMicros is whole microseconds since the Unix epoch.
	FormatEpochMicros TimestampFormat = "epoch_micros"
)

// IsKnown reports whether this build names the format.
func (format TimestampFormat) IsKnown() bool {
	switch format {
	case FormatRFC3339, FormatEpochSeconds, FormatEpochMillis, FormatEpochMicros:
		return true
	default:
		return false
	}
}

// UnmarshalCBOR reads the value, refusing one outside the closed vocabulary.
func (value *TimestampFormat) UnmarshalCBOR(item cborform.Value) error {
	return decodeClosedEnum(item, value, "timestamp format", FormatRFC3339, FormatEpochSeconds, FormatEpochMillis, FormatEpochMicros)
}
