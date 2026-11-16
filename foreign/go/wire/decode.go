package wire

import (
	"fmt"
	"math"
	"reflect"
	"strconv"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

var unmarshalerType = reflect.TypeOf((*Unmarshaler)(nil)).Elem()

// StrictFields is implemented by the frames that refuse unknown fields, the Go
// form of serde's deny_unknown_fields.
//
// The managed envelopes rely on it: refusing unknown fields is what keeps a
// plain request from ever decoding as a barrier or reply frame, so a wrapped
// and an unwrapped request can never be confused.
type StrictFields interface {
	DeniesUnknownFields() bool
}

var strictFieldsType = reflect.TypeOf((*StrictFields)(nil)).Elem()

// RequiredFields is implemented by the frames that refuse a missing field,
// the Go form of a serde field carrying no default.
//
// Most of the contract's fields may be absent, so the decoder fills a zero
// value rather than failing a record a newer or older peer wrote. A few
// cannot: a session change page without its series would read as series zero
// and let a follower continue across a rebuild it should have noticed.
type RequiredFields interface {
	RequiredFieldNames() []string
}

var requiredFieldsType = reflect.TypeOf((*RequiredFields)(nil)).Elem()

// requiredFieldNames reports the fields this struct type refuses to go
// without.
func requiredFieldNames(structValue reflect.Value) []string {
	if structValue.Type().Implements(requiredFieldsType) {
		return structValue.Interface().(RequiredFields).RequiredFieldNames()
	}
	if structValue.CanAddr() {
		if pointer := structValue.Addr(); pointer.Type().Implements(requiredFieldsType) {
			return pointer.Interface().(RequiredFields).RequiredFieldNames()
		}
	}
	return nil
}

// deniesUnknownFields reports whether this struct type refuses unknown fields.
func deniesUnknownFields(structValue reflect.Value) bool {
	if structValue.Type().Implements(strictFieldsType) {
		return structValue.Interface().(StrictFields).DeniesUnknownFields()
	}
	if structValue.CanAddr() {
		if pointer := structValue.Addr(); pointer.Type().Implements(strictFieldsType) {
			return pointer.Interface().(StrictFields).DeniesUnknownFields()
		}
	}
	return false
}

// fromValue writes one decoded CBOR item into target.
//
// Unknown map keys are ignored rather than rejected, matching serde's default
// for the contract's structs: a newer peer may send a field this build has not
// learned, and dropping the record would break forward compatibility.
func fromValue(item cborform.Value, target reflect.Value) error {
	if target.CanAddr() {
		if pointer := target.Addr(); pointer.Type().Implements(unmarshalerType) {
			return pointer.Interface().(Unmarshaler).UnmarshalCBOR(item)
		}
	}

	// An absent optional stays at its zero value.
	if _, isNull := item.(cborform.Null); isNull {
		target.Set(reflect.Zero(target.Type()))
		return nil
	}
	if _, isUndefined := item.(cborform.Undefined); isUndefined {
		target.Set(reflect.Zero(target.Type()))
		return nil
	}

	if target.Kind() == reflect.Pointer {
		if target.IsNil() {
			target.Set(reflect.New(target.Type().Elem()))
		}
		return fromValue(item, target.Elem())
	}

	switch typed := item.(type) {
	case cborform.Bool:
		if target.Kind() != reflect.Bool {
			return typeError(item, target)
		}
		target.SetBool(bool(typed))
		return nil

	case cborform.Uint:
		return setInteger(uint64(typed), false, target)

	case cborform.NegInt:
		return setInteger(uint64(typed), true, target)

	case cborform.Float:
		switch target.Kind() {
		case reflect.Float32, reflect.Float64:
			target.SetFloat(typed.Value)
			return nil
		default:
			return typeError(item, target)
		}

	case cborform.Text:
		if target.Kind() != reflect.String {
			return typeError(item, target)
		}
		target.SetString(string(typed))
		return nil

	case cborform.Bytes:
		return setBytes(typed, target)

	case cborform.Array:
		return setArray(typed, target)

	case cborform.Map:
		return setMap(typed, target)

	case cborform.Tag:
		return fromValue(typed.Content, target)

	default:
		return fmt.Errorf("wire: cannot decode %T into %s", item, target.Type())
	}
}

func typeError(item cborform.Value, target reflect.Value) error {
	return fmt.Errorf("wire: cannot decode %T into %s", item, target.Type())
}

func setInteger(argument uint64, negative bool, target reflect.Value) error {
	switch target.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		var signed int64
		if negative {
			if argument > math.MaxInt64 {
				return fmt.Errorf("wire: negative integer overflows %s", target.Type())
			}
			signed = -1 - int64(argument)
		} else {
			if argument > math.MaxInt64 {
				return fmt.Errorf("wire: integer overflows %s", target.Type())
			}
			signed = int64(argument)
		}
		if target.OverflowInt(signed) {
			return fmt.Errorf("wire: %d overflows %s", signed, target.Type())
		}
		target.SetInt(signed)
		return nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if negative {
			return fmt.Errorf("wire: a negative integer cannot decode into %s", target.Type())
		}
		if target.OverflowUint(argument) {
			return fmt.Errorf("wire: %d overflows %s", argument, target.Type())
		}
		target.SetUint(argument)
		return nil

	case reflect.Float32, reflect.Float64:
		if negative {
			target.SetFloat(-1 - float64(argument))
		} else {
			target.SetFloat(float64(argument))
		}
		return nil

	default:
		return fmt.Errorf("wire: cannot decode an integer into %s", target.Type())
	}
}

func setBytes(payload cborform.Bytes, target reflect.Value) error {
	switch target.Kind() {
	case reflect.Slice:
		if target.Type().Elem().Kind() != reflect.Uint8 {
			return typeError(payload, target)
		}
		out := make([]byte, len(payload))
		copy(out, payload)
		target.SetBytes(out)
		return nil
	case reflect.Array:
		if target.Type().Elem().Kind() != reflect.Uint8 {
			return typeError(payload, target)
		}
		if target.Len() != len(payload) {
			return fmt.Errorf("wire: %s wants %d byte(s), got %d", target.Type(), target.Len(), len(payload))
		}
		reflect.Copy(target, reflect.ValueOf([]byte(payload)))
		return nil
	default:
		return typeError(payload, target)
	}
}

func setArray(items cborform.Array, target reflect.Value) error {
	// The contract's byte fields are dual-format: a CBOR byte string on the
	// binary band, an array of integers on the JSON surface. Accept both so a
	// value crossing surfaces decodes either way.
	if target.Kind() == reflect.Slice && isRawByte(target.Type().Elem()) {
		payload := make([]byte, 0, len(items))
		for i, item := range items {
			number, ok := item.(cborform.Uint)
			if !ok || number > math.MaxUint8 {
				return fmt.Errorf("wire: byte array element %d is not a byte", i)
			}
			payload = append(payload, byte(number))
		}
		target.SetBytes(payload)
		return nil
	}

	switch target.Kind() {
	case reflect.Slice:
		created := reflect.MakeSlice(target.Type(), len(items), len(items))
		for i, item := range items {
			if err := fromValue(item, created.Index(i)); err != nil {
				return fmt.Errorf("element %d: %w", i, err)
			}
		}
		target.Set(created)
		return nil
	case reflect.Array:
		if target.Len() != len(items) {
			return fmt.Errorf("wire: %s wants %d element(s), got %d", target.Type(), target.Len(), len(items))
		}
		for i, item := range items {
			if err := fromValue(item, target.Index(i)); err != nil {
				return fmt.Errorf("element %d: %w", i, err)
			}
		}
		return nil
	default:
		return fmt.Errorf("wire: cannot decode an array into %s", target.Type())
	}
}

func setMap(entries cborform.Map, target reflect.Value) error {
	switch target.Kind() {
	case reflect.Struct:
		byName := make(map[string]int, target.NumField())
		for _, structField := range fieldsOf(target.Type()) {
			byName[structField.name] = structField.index
		}
		strict := deniesUnknownFields(target)
		required := requiredFieldNames(target)
		var seen map[string]bool
		if len(required) != 0 {
			seen = make(map[string]bool, len(required))
		}
		for _, entry := range entries {
			key, ok := entry.Key.(cborform.Text)
			if !ok {
				if strict {
					return fmt.Errorf("wire: %s refuses a non-text field key", target.Type())
				}
				continue // a non-text key cannot name a field
			}
			index, known := byName[string(key)]
			if !known {
				if strict {
					return fmt.Errorf("wire: %s refuses the unknown field %q", target.Type(), key)
				}
				continue // forward compatibility: ignore unknown fields
			}
			if seen != nil {
				seen[string(key)] = true
			}
			if err := fromValue(entry.Value, target.Field(index)); err != nil {
				return fmt.Errorf("field %q: %w", key, err)
			}
		}
		for _, name := range required {
			if !seen[name] {
				return fmt.Errorf("wire: %s requires the field %q", target.Type(), name)
			}
		}
		return nil

	case reflect.Map:
		created := reflect.MakeMapWithSize(target.Type(), len(entries))
		keyType := target.Type().Key()
		valueType := target.Type().Elem()
		for _, entry := range entries {
			key := reflect.New(keyType).Elem()
			if err := fromMapKey(entry.Key, key); err != nil {
				return fmt.Errorf("map key: %w", err)
			}
			value := reflect.New(valueType).Elem()
			if err := fromValue(entry.Value, value); err != nil {
				return fmt.Errorf("map value: %w", err)
			}
			created.SetMapIndex(key, value)
		}
		target.Set(created)
		return nil

	default:
		return fmt.Errorf("wire: cannot decode a map into %s", target.Type())
	}
}

// fromMapKey decodes one map key, accepting the decimal string a
// human-readable encoding spells a numeric key with.
func fromMapKey(item cborform.Value, target reflect.Value) error {
	text, isText := item.(cborform.Text)
	if !isText {
		return fromValue(item, target)
	}
	switch target.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		number, err := strconv.ParseInt(string(text), 10, 64)
		if err != nil || target.OverflowInt(number) {
			return fmt.Errorf("wire: %q is not a %s map key", text, target.Type())
		}
		target.SetInt(number)
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		number, err := strconv.ParseUint(string(text), 10, 64)
		if err != nil || target.OverflowUint(number) {
			return fmt.Errorf("wire: %q is not a %s map key", text, target.Type())
		}
		target.SetUint(number)
		return nil
	default:
		return fromValue(item, target)
	}
}
