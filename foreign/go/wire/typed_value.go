package wire

import (
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// TypedValueKind discriminates a TypedValue.
type TypedValueKind string

// The typed-value kinds, in their wire spelling. TypedValue is adjacently
// tagged, so a value rides as {"kind": <name>, "value": <payload>} and a unit
// variant carries no value entry.
const (
	TypedNull              TypedValueKind = "null"
	TypedBoolean           TypedValueKind = "boolean"
	TypedInt               TypedValueKind = "int"
	TypedLong              TypedValueKind = "long"
	TypedFloat             TypedValueKind = "float"
	TypedDouble            TypedValueKind = "double"
	TypedDecimal           TypedValueKind = "decimal"
	TypedDate              TypedValueKind = "date"
	TypedTimeMicros        TypedValueKind = "time_micros"
	TypedTimestampMicros   TypedValueKind = "timestamp_micros"
	TypedTimestampTzMicros TypedValueKind = "timestamp_tz_micros"
	TypedString            TypedValueKind = "string"
	TypedUUID              TypedValueKind = "uuid"
	TypedFixed             TypedValueKind = "fixed"
	TypedBinary            TypedValueKind = "binary"
	TypedStruct            TypedValueKind = "struct"
	TypedList              TypedValueKind = "list"
	TypedMap               TypedValueKind = "map"
)

// DecimalValue is an exact decimal as its unscaled big-endian bytes plus
// precision and scale.
type DecimalValue struct {
	Unscaled  []byte `cbor:"unscaled"`
	Precision uint8  `cbor:"precision"`
	Scale     uint8  `cbor:"scale"`
}

// UUIDBytes is the width of a UUID payload.
const UUIDBytes = 16

// FieldValue is one field of a struct value, addressed by its schema field id.
type FieldValue struct {
	FieldID uint32     `cbor:"field_id"`
	Value   TypedValue `cbor:"value"`
}

// MapEntry is one key and value of a map value.
type MapEntry struct {
	Key   TypedValue `cbor:"key"`
	Value TypedValue `cbor:"value"`
}

// TypedValue is the tagged value contract the query predicates and results use.
//
// Unlike the untagged Value of the graph and older document surfaces, this
// carries its type, so a consumer never has to infer one from the encoding.
type TypedValue struct {
	Kind      TypedValueKind
	Boolean   bool
	Int       int32
	Long      int64
	Float     float32
	Double    float64
	Decimal   *DecimalValue
	Date      int32
	Micros    int64
	Str       string
	Bytes     []byte
	Fields    []FieldValue
	List      []TypedValue
	MapValues []MapEntry
}

// The TypedValue constructors.
func NullTyped() TypedValue            { return TypedValue{Kind: TypedNull} }
func BooleanTyped(v bool) TypedValue   { return TypedValue{Kind: TypedBoolean, Boolean: v} }
func IntTyped(v int32) TypedValue      { return TypedValue{Kind: TypedInt, Int: v} }
func LongTyped(v int64) TypedValue     { return TypedValue{Kind: TypedLong, Long: v} }
func FloatTyped(v float32) TypedValue  { return TypedValue{Kind: TypedFloat, Float: v} }
func DoubleTyped(v float64) TypedValue { return TypedValue{Kind: TypedDouble, Double: v} }
func StringTyped(v string) TypedValue  { return TypedValue{Kind: TypedString, Str: v} }
func DateTyped(days int32) TypedValue  { return TypedValue{Kind: TypedDate, Date: days} }
func DecimalTyped(v DecimalValue) TypedValue {
	return TypedValue{Kind: TypedDecimal, Decimal: &v}
}

// TimeTyped, TimestampTyped and TimestampTzTyped carry microsecond instants.
func TimeTyped(micros int64) TypedValue { return TypedValue{Kind: TypedTimeMicros, Micros: micros} }
func TimestampTyped(micros int64) TypedValue {
	return TypedValue{Kind: TypedTimestampMicros, Micros: micros}
}
func TimestampTzTyped(micros int64) TypedValue {
	return TypedValue{Kind: TypedTimestampTzMicros, Micros: micros}
}

// UUIDTyped carries a 16-byte UUID.
func UUIDTyped(payload []byte) TypedValue { return TypedValue{Kind: TypedUUID, Bytes: payload} }

// FixedTyped and BinaryTyped carry opaque bytes.
func FixedTyped(payload []byte) TypedValue  { return TypedValue{Kind: TypedFixed, Bytes: payload} }
func BinaryTyped(payload []byte) TypedValue { return TypedValue{Kind: TypedBinary, Bytes: payload} }

// StructTyped, ListTyped and MapTyped carry the composite values.
func StructTyped(fields ...FieldValue) TypedValue {
	return TypedValue{Kind: TypedStruct, Fields: fields}
}
func ListTyped(items ...TypedValue) TypedValue {
	return TypedValue{Kind: TypedList, List: items}
}
func MapTyped(entries ...MapEntry) TypedValue {
	return TypedValue{Kind: TypedMap, MapValues: entries}
}

// The adjacent tag keys.
const (
	typedTagKey     = "kind"
	typedContentKey = "value"
)

// payload encodes the variant's content, or nil for a unit variant that
// carries no value entry.
func (value TypedValue) payload() (cborform.Value, error) {
	switch value.Kind {
	case TypedNull:
		return nil, nil
	case TypedBoolean:
		return cborform.Bool(value.Boolean), nil
	case TypedInt:
		return signedValue(int64(value.Int)), nil
	case TypedLong:
		return signedValue(value.Long), nil
	case TypedFloat:
		return cborform.Float{Value: float64(value.Float), Bits: 64}, nil
	case TypedDouble:
		return cborform.Float{Value: value.Double, Bits: 64}, nil
	case TypedDecimal:
		if value.Decimal == nil {
			return nil, fmt.Errorf("wire: a decimal value carries no payload")
		}
		return toValueOf(*value.Decimal)
	case TypedDate:
		return signedValue(int64(value.Date)), nil
	case TypedTimeMicros, TypedTimestampMicros, TypedTimestampTzMicros:
		return signedValue(value.Micros), nil
	case TypedString:
		return cborform.Text(value.Str), nil
	case TypedUUID, TypedFixed, TypedBinary:
		return cborform.Bytes(value.Bytes), nil
	case TypedStruct:
		return toValueOf(value.Fields)
	case TypedList:
		return toValueOf(value.List)
	case TypedMap:
		return toValueOf(value.MapValues)
	default:
		return nil, fmt.Errorf("wire: unknown typed value kind %q", string(value.Kind))
	}
}

// signedValue encodes a signed integer in the CBOR major type its sign selects.
func signedValue(number int64) cborform.Value {
	if number < 0 {
		return cborform.NegInt(uint64(-1 - number))
	}
	return cborform.Uint(uint64(number))
}

// MarshalCBOR writes the adjacently tagged value.
func (value TypedValue) MarshalCBOR() (cborform.Value, error) {
	payload, err := value.payload()
	if err != nil {
		return nil, err
	}
	entries := cborform.Map{{
		Key:   cborform.Text(typedTagKey),
		Value: cborform.Text(string(value.Kind)),
	}}
	if payload != nil {
		entries = append(entries, cborform.Entry{
			Key:   cborform.Text(typedContentKey),
			Value: payload,
		})
	}
	return entries, nil
}

// UnmarshalCBOR reads the adjacently tagged value.
func (value *TypedValue) UnmarshalCBOR(item cborform.Value) error {
	entries, ok := item.(cborform.Map)
	if !ok {
		return fmt.Errorf("wire: cannot decode %T as a typed value", item)
	}

	var kind TypedValueKind
	var payload cborform.Value
	var sawKind bool
	for _, entry := range entries {
		key, ok := entry.Key.(cborform.Text)
		if !ok {
			continue
		}
		switch string(key) {
		case typedTagKey:
			name, ok := entry.Value.(cborform.Text)
			if !ok {
				return fmt.Errorf("wire: a typed value kind must be text, got %T", entry.Value)
			}
			kind, sawKind = TypedValueKind(name), true
		case typedContentKey:
			payload = entry.Value
		}
	}
	if !sawKind {
		return fmt.Errorf("wire: a typed value names no kind")
	}

	decoded := TypedValue{Kind: kind}
	need := func() error {
		if payload == nil {
			return fmt.Errorf("wire: typed value %q carries no value", string(kind))
		}
		return nil
	}

	switch kind {
	case TypedNull:
		// A unit variant carries no value entry.
	case TypedBoolean:
		if err := need(); err != nil {
			return err
		}
		flag, ok := payload.(cborform.Bool)
		if !ok {
			return fmt.Errorf("wire: a boolean value must be a bool, got %T", payload)
		}
		decoded.Boolean = bool(flag)
	case TypedInt, TypedDate:
		if err := need(); err != nil {
			return err
		}
		number, err := readSigned(payload)
		if err != nil {
			return err
		}
		if kind == TypedInt {
			decoded.Int = int32(number)
		} else {
			decoded.Date = int32(number)
		}
	case TypedLong:
		if err := need(); err != nil {
			return err
		}
		number, err := readSigned(payload)
		if err != nil {
			return err
		}
		decoded.Long = number
	case TypedTimeMicros, TypedTimestampMicros, TypedTimestampTzMicros:
		if err := need(); err != nil {
			return err
		}
		number, err := readSigned(payload)
		if err != nil {
			return err
		}
		decoded.Micros = number
	case TypedFloat, TypedDouble:
		if err := need(); err != nil {
			return err
		}
		number, ok := payload.(cborform.Float)
		if !ok {
			return fmt.Errorf("wire: a floating point value must be a float, got %T", payload)
		}
		if kind == TypedFloat {
			decoded.Float = float32(number.Value)
		} else {
			decoded.Double = number.Value
		}
	case TypedDecimal:
		if err := need(); err != nil {
			return err
		}
		var decimal DecimalValue
		if err := fromValueInto(payload, &decimal); err != nil {
			return err
		}
		decoded.Decimal = &decimal
	case TypedString:
		if err := need(); err != nil {
			return err
		}
		text, ok := payload.(cborform.Text)
		if !ok {
			return fmt.Errorf("wire: a string value must be text, got %T", payload)
		}
		decoded.Str = string(text)
	case TypedUUID, TypedFixed, TypedBinary:
		if err := need(); err != nil {
			return err
		}
		if err := fromValueInto(payload, &decoded.Bytes); err != nil {
			return err
		}
	case TypedStruct:
		if err := need(); err != nil {
			return err
		}
		if err := fromValueInto(payload, &decoded.Fields); err != nil {
			return err
		}
	case TypedList:
		if err := need(); err != nil {
			return err
		}
		if err := fromValueInto(payload, &decoded.List); err != nil {
			return err
		}
	case TypedMap:
		if err := need(); err != nil {
			return err
		}
		if err := fromValueInto(payload, &decoded.MapValues); err != nil {
			return err
		}
	default:
		return fmt.Errorf("wire: unknown typed value kind %q", string(kind))
	}

	*value = decoded
	return nil
}

// readSigned reads either CBOR integer major type as a signed value.
func readSigned(payload cborform.Value) (int64, error) {
	switch number := payload.(type) {
	case cborform.Uint:
		return int64(number), nil
	case cborform.NegInt:
		return -1 - int64(number), nil
	default:
		return 0, fmt.Errorf("wire: an integer value must be an integer, got %T", payload)
	}
}

// DiagnosticText renders the value for an error message. It is for people
// reading a rejection, not for the wire.
func (value TypedValue) DiagnosticText() string {
	switch value.Kind {
	case TypedNull:
		return "null"
	case TypedBoolean:
		return strconv.FormatBool(value.Boolean)
	case TypedInt:
		return strconv.FormatInt(int64(value.Int), 10)
	case TypedLong:
		return strconv.FormatInt(value.Long, 10)
	case TypedFloat:
		return strconv.FormatFloat(float64(value.Float), 'f', -1, 32)
	case TypedDouble:
		return strconv.FormatFloat(value.Double, 'f', -1, 64)
	case TypedDate:
		return strconv.FormatInt(int64(value.Date), 10)
	case TypedTimeMicros, TypedTimestampMicros, TypedTimestampTzMicros:
		return strconv.FormatInt(value.Micros, 10)
	case TypedString:
		return value.Str
	case TypedUUID:
		return formatUUID(value.Bytes)
	case TypedDecimal:
		if value.Decimal == nil {
			return "0x scale 0"
		}
		return fmt.Sprintf("0x%s scale %d",
			hex.EncodeToString(value.Decimal.Unscaled), value.Decimal.Scale)
	case TypedFixed, TypedBinary:
		return "0x" + hex.EncodeToString(value.Bytes)
	default:
		encoded, err := MarshalCanonicalJSON(value)
		if err != nil {
			return "<invalid typed value>"
		}
		return string(encoded)
	}
}

// formatUUID renders 16 bytes in the hyphenated 8-4-4-4-12 form.
func formatUUID(value []byte) string {
	if len(value) != UUIDBytes {
		return "0x" + hex.EncodeToString(value)
	}
	text := hex.EncodeToString(value)
	return text[0:8] + "-" + text[8:12] + "-" + text[12:16] + "-" +
		text[16:20] + "-" + text[20:32]
}
