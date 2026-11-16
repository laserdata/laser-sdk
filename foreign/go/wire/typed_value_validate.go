package wire

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
)

// A typed value's canonical rules.
//
// Two clients must agree byte for byte on the same value, so the forms that
// could be written two ways are refused rather than normalized: a negative
// zero, a non-minimal decimal, an unordered map. Normalizing would hide the
// disagreement; refusing surfaces it where the value was built.

// Validate checks that a value is canonical, so the same value encodes the
// same way in every client.
func (value TypedValue) Validate() error { return validateTypedValue(value, 1) }

func validateTypedValue(value TypedValue, depth int) error {
	if depth > MaxLogicalSchemaDepth {
		return &InvalidError{Reason: fmt.Sprintf(
			"logical schema depth %d exceeds cap %d", depth, MaxLogicalSchemaDepth)}
	}

	switch value.Kind {
	case TypedNull, TypedBoolean, TypedInt, TypedLong, TypedDate,
		TypedTimestampMicros, TypedTimestampTzMicros:
		return nil

	case TypedString:
		if len(value.Str) > MaxValueBytes {
			return &InvalidError{Reason: fmt.Sprintf(
				"string value is %d bytes, exceeds cap %d", len(value.Str), MaxValueBytes)}
		}
		return nil

	case TypedFloat:
		return validateCanonicalFloat(float64(value.Float))
	case TypedDouble:
		return validateCanonicalFloat(value.Double)

	case TypedDecimal:
		if value.Decimal == nil {
			return &InvalidError{Reason: "a decimal value carries no decimal"}
		}
		return value.Decimal.Validate()

	case TypedTimeMicros:
		if value.Micros < 0 || value.Micros >= MicrosPerDay {
			return &InvalidError{Reason: fmt.Sprintf(
				"time value %d is outside one day in microseconds", value.Micros)}
		}
		return nil

	case TypedUUID:
		if len(value.Bytes) != UuidValueBytes {
			return &InvalidError{Reason: fmt.Sprintf(
				"UUID must be %d bytes, got %d", UuidValueBytes, len(value.Bytes))}
		}
		return nil

	case TypedFixed:
		if uint32(len(value.Bytes)) > MaxFixedBytes {
			return &InvalidError{Reason: fmt.Sprintf(
				"binary value is %d bytes, exceeds cap %d", len(value.Bytes), MaxFixedBytes)}
		}
		return nil

	case TypedBinary:
		if len(value.Bytes) > MaxValueBytes {
			return &InvalidError{Reason: fmt.Sprintf(
				"binary value is %d bytes, exceeds cap %d", len(value.Bytes), MaxValueBytes)}
		}
		return nil

	case TypedStruct:
		if len(value.Fields) > MaxLogicalSchemaFields {
			return &InvalidError{Reason: fmt.Sprintf(
				"struct carries %d fields, exceeds cap %d",
				len(value.Fields), MaxLogicalSchemaFields)}
		}
		// Strictly increasing ids put the fields in one order, so the same
		// struct cannot encode two ways.
		previous := uint32(0)
		for _, field := range value.Fields {
			if field.FieldID == 0 || field.FieldID <= previous {
				return &InvalidError{Reason: "struct field values must have " +
					"strictly increasing positive field IDs"}
			}
			previous = field.FieldID
			if err := validateTypedValue(field.Value, depth+1); err != nil {
				return err
			}
		}
		return nil

	case TypedList:
		if len(value.List) > MaxLogicalSchemaFields {
			return &InvalidError{Reason: fmt.Sprintf(
				"list carries %d values, exceeds cap %d",
				len(value.List), MaxLogicalSchemaFields)}
		}
		for _, item := range value.List {
			if err := validateTypedValue(item, depth+1); err != nil {
				return err
			}
		}
		return nil

	case TypedMap:
		if len(value.MapValues) > MaxLogicalSchemaFields {
			return &InvalidError{Reason: fmt.Sprintf(
				"map carries %d entries, exceeds cap %d",
				len(value.MapValues), MaxLogicalSchemaFields)}
		}
		var previous []byte
		for _, entry := range value.MapValues {
			if err := validateTypedValue(entry.Key, depth+1); err != nil {
				return err
			}
			if err := validateTypedValue(entry.Value, depth+1); err != nil {
				return err
			}
			key, err := canonicalMapKey(entry.Key)
			if err != nil {
				return err
			}
			if previous != nil && bytes.Compare(previous, key) >= 0 {
				return &InvalidError{
					Reason: "map entries must be strictly ordered by canonical key",
				}
			}
			previous = key
		}
		return nil

	default:
		return &InvalidError{Reason: fmt.Sprintf(
			"unknown typed value kind %q", string(value.Kind))}
	}
}

// validateCanonicalFloat refuses the values that have no single spelling: a
// non-finite one has no encoding, and a negative zero is a second spelling of
// zero, so one value would compare as two.
func validateCanonicalFloat(value float64) error {
	if math.IsInf(value, 0) || math.IsNaN(value) {
		return &InvalidError{Reason: "floating-point value must be finite"}
	}
	if value == 0 && math.Signbit(value) {
		return &InvalidError{
			Reason: "negative zero is not a canonical floating-point value",
		}
	}
	return nil
}

// Validate checks a decimal's precision, scale and minimal two's-complement
// unscaled bytes.
func (decimal DecimalValue) Validate() error {
	if decimal.Precision == 0 || decimal.Precision > MaxDecimalPrecision ||
		decimal.Scale > decimal.Precision {
		return &InvalidError{Reason: fmt.Sprintf(
			"decimal precision and scale (%d, %d) are invalid",
			decimal.Precision, decimal.Scale)}
	}
	if len(decimal.Unscaled) == 0 || len(decimal.Unscaled) > 16 {
		return &InvalidError{Reason: fmt.Sprintf(
			"decimal unscaled value must contain 1..=16 bytes, got %d",
			len(decimal.Unscaled))}
	}
	// A leading byte that the next byte's sign bit already implies is
	// redundant, so one number would have several encodings.
	if len(decimal.Unscaled) > 1 {
		first, second := decimal.Unscaled[0], decimal.Unscaled[1]
		if (first == 0 && second&0x80 == 0) || (first == 0xff && second&0x80 != 0) {
			return &InvalidError{
				Reason: "decimal unscaled bytes are not minimal two's complement",
			}
		}
	}
	return nil
}

// UuidValueBytes is how many bytes a UUID value carries.
const UuidValueBytes = 16

// canonicalMapKey renders a map key as the bytes its ordering compares.
//
// Each kind carries a distinct leading tag, so two keys of different kinds can
// never collide, and the floating and composite kinds are refused because
// their ordering is not deterministic across clients.
func canonicalMapKey(value TypedValue) ([]byte, error) {
	switch value.Kind {
	case TypedBoolean:
		flag := byte(0)
		if value.Boolean {
			flag = 1
		}
		return []byte{0, flag}, nil
	case TypedInt:
		return append([]byte{1}, bigEndianInt32(value.Int)...), nil
	case TypedLong:
		return append([]byte{2}, bigEndianInt64(value.Long)...), nil
	case TypedDecimal:
		if value.Decimal == nil {
			return nil, &InvalidError{Reason: "a decimal key carries no decimal"}
		}
		key := []byte{3, value.Decimal.Precision, value.Decimal.Scale}
		return append(key, value.Decimal.Unscaled...), nil
	case TypedDate:
		return append([]byte{4}, bigEndianInt32(value.Date)...), nil
	case TypedTimeMicros:
		return append([]byte{5}, bigEndianInt64(value.Micros)...), nil
	case TypedTimestampMicros:
		return append([]byte{6}, bigEndianInt64(value.Micros)...), nil
	case TypedTimestampTzMicros:
		return append([]byte{7}, bigEndianInt64(value.Micros)...), nil
	case TypedString:
		return append([]byte{8}, value.Str...), nil
	case TypedUUID:
		return append([]byte{9}, value.Bytes...), nil
	case TypedFixed:
		return append([]byte{10}, value.Bytes...), nil
	case TypedBinary:
		return append([]byte{11}, value.Bytes...), nil
	default:
		return nil, &InvalidError{
			Reason: "map key must be a deterministic non-floating primitive",
		}
	}
}

func bigEndianInt32(value int32) []byte {
	return binary.BigEndian.AppendUint32(nil, uint32(value))
}

func bigEndianInt64(value int64) []byte {
	return binary.BigEndian.AppendUint64(nil, uint64(value))
}
