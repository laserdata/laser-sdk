package wire

import (
	"strconv"
	"strings"
)

// The coercion constants.
const (
	microsPerSecond  int64 = 1_000_000
	microsPerMilli   int64 = 1_000
	secondsPerDay    int64 = 86_400
	maxDecimalDigits       = 512
)

// MicrosFromText is the instant a text value names under this format, in
// microseconds since the Unix epoch.
//
// The second result is false when the text does not parse exactly: a value
// that does not parse compares as unknown rather than failing the record.
func (format TimestampFormat) MicrosFromText(text string) (int64, bool) {
	if format == FormatRFC3339 {
		return rfc3339Micros(text)
	}
	value, ok := parseDecimalInteger(text)
	if !ok {
		return 0, false
	}
	return format.MicrosFromInteger(value)
}

// MicrosFromInteger is the instant an integer value names under this format.
// The second result is false for the text-only RFC 3339 format and on
// overflow.
func (format TimestampFormat) MicrosFromInteger(value int64) (int64, bool) {
	switch format {
	case FormatEpochSeconds:
		return mulChecked(value, microsPerSecond)
	case FormatEpochMillis:
		return mulChecked(value, microsPerMilli)
	case FormatEpochMicros:
		return value, true
	default:
		return 0, false
	}
}

// mulChecked multiplies without wrapping, reporting false on overflow.
func mulChecked(left, right int64) (int64, bool) {
	if left == 0 || right == 0 {
		return 0, true
	}
	product := left * right
	if product/right != left {
		return 0, false
	}
	return product, true
}

func addChecked(left, right int64) (int64, bool) {
	sum := left + right
	if (right > 0 && sum < left) || (right < 0 && sum > left) {
		return 0, false
	}
	return sum, true
}

// parseDecimalInteger reads an optionally signed run of digits.
func parseDecimalInteger(text string) (int64, bool) {
	digits := strings.TrimLeft(text, "+-")
	if len(text)-len(digits) > 1 || digits == "" {
		return 0, false
	}
	for i := 0; i < len(digits); i++ {
		if digits[i] < '0' || digits[i] > '9' {
			return 0, false
		}
	}
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0, false
	}
	return value, true
}

// ExactDecimal is a decimal number compared without floating-point rounding.
//
// It is built from decimal text, an integer, or the shortest exact rendering
// of a finite float. Leading and trailing zeros are normalised away, so 1.50
// equals 1.5 and negative zero equals zero.
type ExactDecimal struct {
	negative bool
	integer  string
	fraction string
}

// ParseExactDecimal reads an optionally signed decimal with no exponent and no
// surrounding whitespace.
func ParseExactDecimal(text string) (ExactDecimal, bool) {
	if text == "" {
		return ExactDecimal{}, false
	}
	negative := false
	unsigned := text
	switch text[0] {
	case '-':
		negative, unsigned = true, text[1:]
	case '+':
		unsigned = text[1:]
	}

	integer, fraction, hadPoint := strings.Cut(unsigned, ".")
	if integer == "" && fraction == "" {
		return ExactDecimal{}, false
	}
	if len(integer)+len(fraction) > maxDecimalDigits {
		return ExactDecimal{}, false
	}
	if hadPoint && fraction == "" {
		return ExactDecimal{}, false
	}
	for _, part := range []string{integer, fraction} {
		for i := 0; i < len(part); i++ {
			if part[i] < '0' || part[i] > '9' {
				return ExactDecimal{}, false
			}
		}
	}
	return normalizedDecimal(negative, integer, fraction), true
}

// DecimalFromInteger builds an exact decimal from a signed integer.
func DecimalFromInteger(value int64) ExactDecimal {
	// The magnitude is built through uint64 because negating the minimum
	// signed value would overflow back to itself.
	if value < 0 {
		return decimalFromDigits(true, strconv.FormatUint(uint64(-(value+1))+1, 10))
	}
	return decimalFromDigits(false, strconv.FormatUint(uint64(value), 10))
}

// DecimalFromUnsigned builds an exact decimal from an unsigned integer, so a
// value above the signed range keeps every digit.
func DecimalFromUnsigned(value uint64) ExactDecimal {
	return decimalFromDigits(false, strconv.FormatUint(value, 10))
}

func decimalFromDigits(negative bool, digits string) ExactDecimal {
	return normalizedDecimal(negative, digits, "")
}

// DecimalFromFloat is the shortest decimal that round-trips a finite float, so
// a JSON number compares at the precision it was written with. The second
// result is false for infinities and NaN.
func DecimalFromFloat(value float64) (ExactDecimal, bool) {
	rendered := strconv.FormatFloat(value, 'f', -1, 64)
	if strings.ContainsAny(rendered, "nNiI") {
		return ExactDecimal{}, false
	}
	return ParseExactDecimal(rendered)
}

func normalizedDecimal(negative bool, integer, fraction string) ExactDecimal {
	integer = strings.TrimLeft(integer, "0")
	fraction = strings.TrimRight(fraction, "0")
	// Negative zero is zero.
	if integer == "" && fraction == "" {
		negative = false
	}
	return ExactDecimal{negative: negative, integer: integer, fraction: fraction}
}

// String renders the normalised decimal.
func (decimal ExactDecimal) String() string {
	var out strings.Builder
	if decimal.negative {
		out.WriteByte('-')
	}
	if decimal.integer == "" {
		out.WriteByte('0')
	} else {
		out.WriteString(decimal.integer)
	}
	if decimal.fraction != "" {
		out.WriteByte('.')
		out.WriteString(decimal.fraction)
	}
	return out.String()
}

// Compare orders two exact decimals: negative when the receiver is smaller,
// zero when equal, positive when larger.
func (decimal ExactDecimal) Compare(other ExactDecimal) int {
	switch {
	case !decimal.negative && other.negative:
		return 1
	case decimal.negative && !other.negative:
		return -1
	case !decimal.negative:
		return decimal.compareMagnitude(other)
	default:
		// Both negative: the larger magnitude is the smaller number.
		return other.compareMagnitude(decimal)
	}
}

// compareMagnitude orders the magnitudes. A longer integer part is always
// larger once leading zeros are gone, so length precedes lexical order.
func (decimal ExactDecimal) compareMagnitude(other ExactDecimal) int {
	if len(decimal.integer) != len(other.integer) {
		return compareInt(len(decimal.integer), len(other.integer))
	}
	if decimal.integer != other.integer {
		return strings.Compare(decimal.integer, other.integer)
	}
	return strings.Compare(decimal.fraction, other.fraction)
}

func compareInt(left, right int) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

// Equal reports whether two decimals name the same number.
func (decimal ExactDecimal) Equal(other ExactDecimal) bool {
	return decimal == other
}

// rfc3339Micros parses an RFC 3339 instant into microseconds since the epoch.
//
// It is written out rather than taken from the standard library so the
// accepted shapes match the contract exactly: an explicit offset is required,
// and fractions past microseconds are accepted only when the extra digits are
// zero, so no value is ever silently rounded.
func rfc3339Micros(text string) (int64, bool) {
	bytes := []byte(text)
	if len(bytes) < 20 {
		return 0, false
	}

	year, ok := digitsAt(bytes, 0, 4)
	if !ok {
		return 0, false
	}
	month, ok := digitsAt(bytes, 5, 2)
	if !ok {
		return 0, false
	}
	day, ok := digitsAt(bytes, 8, 2)
	if !ok {
		return 0, false
	}
	hour, ok := digitsAt(bytes, 11, 2)
	if !ok {
		return 0, false
	}
	minute, ok := digitsAt(bytes, 14, 2)
	if !ok {
		return 0, false
	}
	second, ok := digitsAt(bytes, 17, 2)
	if !ok {
		return 0, false
	}

	if bytes[4] != '-' || bytes[7] != '-' ||
		(bytes[10] != 'T' && bytes[10] != 't') ||
		bytes[13] != ':' || bytes[16] != ':' {
		return 0, false
	}
	if month < 1 || month > 12 || day == 0 || day > daysInMonth(year, month) ||
		hour > 23 || minute > 59 || second > 59 {
		return 0, false
	}

	position := 19
	var fractionMicros int64
	if position < len(bytes) && bytes[position] == '.' {
		position++
		start := position
		for position < len(bytes) && bytes[position] >= '0' && bytes[position] <= '9' {
			position++
		}
		fraction := text[start:position]
		if fraction == "" || len(fraction) > 9 {
			return 0, false
		}
		keptLen := min(len(fraction), 6)
		kept, dropped := fraction[:keptLen], fraction[keptLen:]
		// Digits past microseconds are accepted only when they are zero, so
		// an instant is never silently rounded.
		for i := 0; i < len(dropped); i++ {
			if dropped[i] != '0' {
				return 0, false
			}
		}
		scale := int64(1)
		for i := 0; i < 6-len(kept); i++ {
			scale *= 10
		}
		parsed, err := strconv.ParseInt(kept, 10, 64)
		if err != nil {
			return 0, false
		}
		fractionMicros = parsed * scale
	}

	if position >= len(bytes) {
		return 0, false
	}
	var offsetSeconds int64
	switch marker := bytes[position]; {
	case (marker == 'Z' || marker == 'z') && position+1 == len(bytes):
		offsetSeconds = 0
	case (marker == '+' || marker == '-') && position+6 == len(bytes):
		offsetHour, ok := digitsAt(bytes, position+1, 2)
		if !ok {
			return 0, false
		}
		offsetMinute, ok := digitsAt(bytes, position+4, 2)
		if !ok {
			return 0, false
		}
		if bytes[position+3] != ':' || offsetHour > 23 || offsetMinute > 59 {
			return 0, false
		}
		magnitude := offsetHour*3600 + offsetMinute*60
		if marker == '-' {
			magnitude = -magnitude
		}
		offsetSeconds = magnitude
	default:
		return 0, false
	}

	days := daysFromCivil(year, month, day)
	seconds, ok := mulChecked(days, secondsPerDay)
	if !ok {
		return 0, false
	}
	seconds, ok = addChecked(seconds, hour*3600+minute*60+second)
	if !ok {
		return 0, false
	}
	seconds, ok = addChecked(seconds, -offsetSeconds)
	if !ok {
		return 0, false
	}
	micros, ok := mulChecked(seconds, microsPerSecond)
	if !ok {
		return 0, false
	}
	return addChecked(micros, fractionMicros)
}

func digitsAt(bytes []byte, start, length int) (int64, bool) {
	if start+length > len(bytes) {
		return 0, false
	}
	var value int64
	for _, b := range bytes[start : start+length] {
		if b < '0' || b > '9' {
			return 0, false
		}
		value = value*10 + int64(b-'0')
	}
	return value, true
}

func isLeapYear(year int64) bool {
	return (year%4 == 0 && year%100 != 0) || year%400 == 0
}

func daysInMonth(year, month int64) int64 {
	switch month {
	case 1, 3, 5, 7, 8, 10, 12:
		return 31
	case 4, 6, 9, 11:
		return 30
	case 2:
		if isLeapYear(year) {
			return 29
		}
		return 28
	default:
		return 0
	}
}

// daysFromCivil is the days between a civil date and the Unix epoch, by the
// standard era algorithm, so no time-zone database is needed.
func daysFromCivil(year, month, day int64) int64 {
	if month <= 2 {
		year--
	}
	era := year
	if year < 0 {
		era = year - 399
	}
	era /= 400
	yearOfEra := year - era*400

	shiftedMonth := month + 9
	if month > 2 {
		shiftedMonth = month - 3
	}
	dayOfYear := (153*shiftedMonth+2)/5 + day - 1
	dayOfEra := yearOfEra*365 + yearOfEra/4 - yearOfEra/100 + dayOfYear
	return era*146_097 + dayOfEra - 719_468
}
