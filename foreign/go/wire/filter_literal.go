package wire

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"
)

// literalKind discriminates a compiled filter literal.
type literalKind uint8

const (
	litNull literalKind = iota
	litBool
	litInt
	litFloat
	litText
	litList
)

// literal is one filter literal prepared for comparison.
type literal struct {
	kind    literalKind
	flag    bool
	integer wideInt
	float   float64
	text    string
	list    []literal
}

// String renders the literal for an explanation label.
func (value literal) String() string {
	switch value.kind {
	case litNull:
		return "null"
	case litBool:
		return strconv.FormatBool(value.flag)
	case litInt:
		return value.integer.String()
	case litFloat:
		// The full decimal form, never an exponent, so every SDK prints one
		// literal the same way.
		return strconv.FormatFloat(value.float, 'f', -1, 64)
	case litText:
		return strconv.Quote(value.text)
	default:
		parts := make([]string, len(value.list))
		for index, item := range value.list {
			parts[index] = item.String()
		}
		return "[" + strings.Join(parts, ", ") + "]"
	}
}

func (value wideInt) String() string {
	digits := strconv.FormatUint(value.magnitude, 10)
	if value.negative {
		return "-" + digits
	}
	return digits
}

func compileLiteral(value TypedValue) (literal, error) {
	switch value.Kind {
	case TypedNull:
		return literal{kind: litNull}, nil
	case TypedBoolean:
		return literal{kind: litBool, flag: value.Boolean}, nil
	case TypedInt:
		return literal{kind: litInt, integer: intFromSigned(int64(value.Int))}, nil
	case TypedLong:
		return literal{kind: litInt, integer: intFromSigned(value.Long)}, nil
	case TypedDouble:
		return literal{kind: litFloat, float: value.Double}, nil
	case TypedString:
		return literal{kind: litText, text: value.Str}, nil
	case TypedList:
		items := make([]literal, len(value.List))
		for index, item := range value.List {
			compiled, err := compileLiteral(item)
			if err != nil {
				return literal{}, err
			}
			items[index] = compiled
		}
		return literal{kind: litList, list: items}, nil
	default:
		return literal{}, &InvalidError{Reason: fmt.Sprintf(
			"unsupported filter literal %s", value.DiagnosticText())}
	}
}

// coercedLiteral is a literal already read under its coercion, so a record's
// field is coerced once per comparison rather than the literal reparsed.
type coercedLiteral struct {
	isList  bool
	instant *int64
	decimal *ExactDecimal
	list    []coercedLiteral
}

func (value coercedLiteral) String() string {
	switch {
	case value.instant != nil:
		return strconv.FormatInt(*value.instant, 10) + "us"
	case value.decimal != nil:
		return value.decimal.String()
	default:
		parts := make([]string, len(value.list))
		for index, item := range value.list {
			parts[index] = item.String()
		}
		return "[" + strings.Join(parts, ", ") + "]"
	}
}

func compileCoercedLiteral(coerce Coerce, value TypedValue) (coercedLiteral, error) {
	if value.Kind == TypedList {
		items := make([]coercedLiteral, len(value.List))
		for index, item := range value.List {
			compiled, err := compileCoercedLiteral(coerce, item)
			if err != nil {
				return coercedLiteral{}, err
			}
			items[index] = compiled
		}
		return coercedLiteral{isList: true, list: items}, nil
	}
	failure := &InvalidError{Reason: fmt.Sprintf(
		"literal %s does not parse under its coercion", value.DiagnosticText())}

	switch coerce.Kind {
	case CoerceTimestamp:
		var micros int64
		var ok bool
		switch value.Kind {
		case TypedString:
			micros, ok = coerce.Format.MicrosFromText(value.Str)
		case TypedInt:
			micros, ok = coerce.Format.MicrosFromInteger(int64(value.Int))
		case TypedLong:
			micros, ok = coerce.Format.MicrosFromInteger(value.Long)
		}
		if !ok {
			return coercedLiteral{}, failure
		}
		return coercedLiteral{instant: &micros}, nil
	case CoerceNumber:
		var decimal ExactDecimal
		var ok bool
		switch value.Kind {
		case TypedString:
			decimal, ok = ParseExactDecimal(value.Str)
		case TypedInt:
			decimal, ok = DecimalFromInteger(int64(value.Int)), true
		case TypedLong:
			decimal, ok = DecimalFromInteger(value.Long), true
		}
		if !ok {
			return coercedLiteral{}, failure
		}
		return coercedLiteral{decimal: &decimal}, nil
	default:
		return coercedLiteral{}, failure
	}
}

// coercedValue is a record field read under a coercion.
type coercedValue struct {
	instant int64
	decimal ExactDecimal
	isTime  bool
}

// coerceValue reads one field under a coercion. A value that does not parse
// compares as unknown rather than failing the record.
func coerceValue(coerce Coerce, field payloadValue) (coercedValue, bool) {
	switch coerce.Kind {
	case CoerceTimestamp:
		if field.kind == payloadText {
			micros, ok := coerce.Format.MicrosFromText(field.text)
			return coercedValue{instant: micros, isTime: true}, ok
		}
		// RFC 3339 is a text format, so a number never names an instant in it.
		if coerce.Format == FormatRFC3339 || field.kind != payloadNumber {
			return coercedValue{}, false
		}
		// A double never names an epoch, however integral it looks.
		if field.isFloat {
			return coercedValue{}, false
		}
		integer, ok := integerOf(field.number)
		if !ok {
			return coercedValue{}, false
		}
		signed, fits := integer.signed()
		if !fits {
			return coercedValue{}, false
		}
		micros, ok := coerce.Format.MicrosFromInteger(signed)
		return coercedValue{instant: micros, isTime: true}, ok
	case CoerceNumber:
		switch field.kind {
		case payloadText:
			decimal, ok := ParseExactDecimal(field.text)
			return coercedValue{decimal: decimal}, ok
		case payloadNumber:
			if integer, ok := integerOf(field.number); ok && !field.isFloat {
				return coercedValue{decimal: integer.decimal()}, true
			}
			decimal, ok := DecimalFromFloat(floatOf(field.number))
			return coercedValue{decimal: decimal}, ok
		}
	}
	return coercedValue{}, false
}

// floatOf reads a number's text as a double. A value past the double range
// has already been refused by the decoder, so it saturates here.
func floatOf(number string) float64 {
	value, _ := strconv.ParseFloat(number, 64)
	return value
}

// integerOf reads a number's text as an exact 64-bit integer, signed or
// unsigned, and reports false when it is neither.
func integerOf(number string) (wideInt, bool) {
	if signed, err := strconv.ParseInt(number, 10, 64); err == nil {
		return intFromSigned(signed), true
	}
	if unsigned, err := strconv.ParseUint(number, 10, 64); err == nil {
		return intFromUnsigned(unsigned), true
	}
	return wideInt{}, false
}

// signed is the value as an int64, and false when it is above the signed range.
func (value wideInt) signed() (int64, bool) {
	switch {
	case value.negative && value.magnitude <= 1<<63:
		return -int64(value.magnitude-1) - 1, true
	case !value.negative && value.magnitude <= math.MaxInt64:
		return int64(value.magnitude), true
	default:
		return 0, false
	}
}

func (value wideInt) decimal() ExactDecimal {
	return decimalFromDigits(value.negative, strconv.FormatUint(value.magnitude, 10))
}

// compareCoerced compares a coerced field against a coerced literal. Values
// read under different coercions never compare.
func compareCoerced(field coercedValue, op CmpOp, value coercedLiteral) Truth {
	ordering := func(item coercedLiteral) (int, bool) {
		switch {
		case field.isTime && item.instant != nil:
			switch {
			case field.instant < *item.instant:
				return -1, true
			case field.instant > *item.instant:
				return 1, true
			default:
				return 0, true
			}
		case !field.isTime && item.decimal != nil:
			return field.decimal.Compare(*item.decimal), true
		default:
			return 0, false
		}
	}
	if op == CmpIn && value.isList {
		for _, item := range value.list {
			if order, ok := ordering(item); ok && order == 0 {
				return TruthMatch
			}
		}
		return TruthNoMatch
	}
	order, ok := ordering(value)
	if !ok {
		return TruthUnknown
	}
	return orderingTruth(op, order)
}

// orderingTruth reads an ordering under a comparison operator. The operators
// that are not orderings leave the comparison unknown.
func orderingTruth(op CmpOp, order int) Truth {
	switch op {
	case CmpEq:
		return truthOf(order == 0)
	case CmpNe:
		return truthOf(order != 0)
	case CmpLt:
		return truthOf(order < 0)
	case CmpLte:
		return truthOf(order <= 0)
	case CmpGt:
		return truthOf(order > 0)
	case CmpGte:
		return truthOf(order >= 0)
	default:
		return TruthUnknown
	}
}

// compareScalar is the three-valued truth of one scalar comparison.
//
// Null is special: it compares only against a null literal, and any other
// value is unequal to null rather than incomparable with it.
func compareScalar(value scalar, field payloadValue, op CmpOp, want literal) Truth {
	switch {
	case value.kind == scalarNull && want.kind == litNull:
		return truthOf(op == CmpEq)
	case value.kind == scalarNull:
		return TruthUnknown
	case want.kind == litNull:
		return truthOf(op == CmpNe)
	}

	switch op {
	case CmpEq:
		equal, known := scalarEqual(value, want)
		if !known {
			return TruthUnknown
		}
		return truthOf(equal)
	case CmpNe:
		equal, known := scalarEqual(value, want)
		if !known {
			return TruthUnknown
		}
		return truthOf(!equal)
	case CmpLt, CmpLte, CmpGt, CmpGte:
		order, known := scalarCompare(value, want)
		if !known {
			return TruthUnknown
		}
		return orderingTruth(op, order)
	case CmpIn:
		if want.kind != litList {
			return TruthUnknown
		}
		unknown := false
		for _, item := range want.list {
			equal, known := scalarEqual(value, item)
			switch {
			case known && equal:
				return TruthMatch
			case !known:
				unknown = true
			}
		}
		if unknown {
			return TruthUnknown
		}
		return TruthNoMatch
	case CmpContains:
		switch {
		case value.kind == scalarText && want.kind == litText:
			return truthOf(strings.Contains(value.text, want.text))
		case value.kind == scalarComposite && field.kind == payloadArray:
			// An array contains a value when one of its elements equals it.
			for _, element := range field.array {
				if equal, known := scalarEqual(scalarOf(element), want); known && equal {
					return TruthMatch
				}
			}
			return TruthNoMatch
		default:
			return TruthUnknown
		}
	case CmpPrefix:
		if value.kind == scalarText && want.kind == litText {
			return truthOf(strings.HasPrefix(value.text, want.text))
		}
		return TruthUnknown
	default:
		return TruthUnknown
	}
}

// scalarEqual reports equality, and false for a pair with no shared ordering.
func scalarEqual(value scalar, want literal) (bool, bool) {
	switch {
	case value.kind == scalarBool && want.kind == litBool:
		return value.flag == want.flag, true
	case value.kind == scalarText && want.kind == litText:
		return value.text == want.text, true
	default:
		order, known := scalarCompare(value, want)
		return order == 0, known
	}
}

// scalarCompare orders a scalar against a literal. An integer that no double
// represents exactly has no ordering against a double, which is undecidable
// rather than a type mismatch.
func scalarCompare(value scalar, want literal) (int, bool) {
	switch {
	case value.kind == scalarInt && want.kind == litInt:
		return value.integer.compare(want.integer), true
	case value.kind == scalarFloat && want.kind == litFloat:
		return compareFloats(value.float, want.float)
	case value.kind == scalarInt && want.kind == litFloat:
		exact, ok := value.integer.exactDouble()
		if !ok {
			return 0, false
		}
		return compareFloats(exact, want.float)
	case value.kind == scalarFloat && want.kind == litInt:
		exact, ok := want.integer.exactDouble()
		if !ok {
			return 0, false
		}
		return compareFloats(value.float, exact)
	case value.kind == scalarText && want.kind == litText:
		// Text orders by its bytes, so every SDK sorts the same way whatever
		// its native string collation is.
		return strings.Compare(value.text, want.text), true
	default:
		return 0, false
	}
}

func compareFloats(left, right float64) (int, bool) {
	switch {
	case math.IsNaN(left) || math.IsNaN(right):
		return 0, false
	case left < right:
		return -1, true
	case left > right:
		return 1, true
	default:
		return 0, true
	}
}

// comparableKinds reports whether the value's kind can be compared with the
// literal's kind at all.
//
// A comparison that stays unknown between comparable kinds, such as an integer
// beyond the exact double range against a double, is undecidable rather than a
// type mismatch. A null value is never a mismatch.
func comparableKinds(value scalar, field payloadValue, op CmpOp, want literal) bool {
	switch {
	case value.kind == scalarNull || want.kind == litNull:
		return true
	case value.kind == scalarText && want.kind == litText:
		return true
	case value.kind == scalarBool && want.kind == litBool:
		return true
	case (value.kind == scalarInt || value.kind == scalarFloat) &&
		(want.kind == litInt || want.kind == litFloat):
		return true
	case want.kind == litList:
		for _, item := range want.list {
			if comparableKinds(value, field, op, item) {
				return true
			}
		}
		return false
	case value.kind == scalarComposite:
		return op == CmpContains && field.kind == payloadArray
	default:
		return false
	}
}

func isValidUTF8Bytes(value []byte) bool { return utf8.Valid(value) }
