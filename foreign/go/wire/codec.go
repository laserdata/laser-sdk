// Package wire is the Go port of the laser-wire contract: the data types,
// codes, limits and validation shared by every LaserData client.
//
// The band encodes each frame as named-field CBOR. This file maps Go values
// onto that encoding through the byte-exact emitter in internal/cborform, so
// every type in the package inherits the canonical layout the Rust crate
// produces rather than re-deriving it per struct.
package wire

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// Marshaler is implemented by types that encode themselves, such as the wire
// ids (16 big-endian bytes) and the string-valued enums.
type Marshaler interface {
	MarshalCBOR() (cborform.Value, error)
}

// Unmarshaler is the decoding half of Marshaler.
type Unmarshaler interface {
	UnmarshalCBOR(cborform.Value) error
}

// Marshal encodes value as named-field CBOR, the band's canonical encoding.
func Marshal(value any) ([]byte, error) {
	item, err := toValue(reflect.ValueOf(value))
	if err != nil {
		return nil, err
	}
	return cborform.Encode(item)
}

// Unmarshal decodes named-field CBOR into target, which must be a pointer.
// Trailing bytes are rejected: a band payload is exactly one CBOR item.
func Unmarshal(data []byte, target any) error {
	item, err := cborform.Decode(data)
	if err != nil {
		return err
	}
	pointer := reflect.ValueOf(target)
	if pointer.Kind() != reflect.Pointer || pointer.IsNil() {
		return fmt.Errorf("wire: Unmarshal needs a non-nil pointer, got %T", target)
	}
	return fromValue(item, pointer.Elem())
}

// field describes one struct field's wire shape, resolved once per type.
type field struct {
	index     int
	name      string
	omitEmpty bool
}

var fieldCache sync.Map // reflect.Type -> []field

// fieldsOf resolves the encoded fields of a struct type in declaration order,
// which is the order serde writes them and therefore the order the bytes must
// carry.
func fieldsOf(structType reflect.Type) []field {
	if cached, ok := fieldCache.Load(structType); ok {
		return cached.([]field)
	}
	var resolved []field
	for i := 0; i < structType.NumField(); i++ {
		structField := structType.Field(i)
		if structField.PkgPath != "" {
			continue // unexported
		}
		tag := structField.Tag.Get("cbor")
		if tag == "" {
			// A body the application defines carries json tags, which name its
			// fields the same way the other SDKs name them.
			tag = structField.Tag.Get("json")
		}
		if tag == "-" {
			continue
		}
		name, options, _ := strings.Cut(tag, ",")
		if name == "" {
			name = structField.Name
		}
		resolved = append(resolved, field{
			index:     i,
			name:      name,
			omitEmpty: strings.Contains(options, "omitempty"),
		})
	}
	fieldCache.Store(structType, resolved)
	return resolved
}

// DefaultValued is implemented by a type whose skip rule is "equal to the
// default" rather than "empty".
//
// Several contract fields skip on a named default rather than a zero value:
// a record policy skips when it rejects, an edge direction when it points out,
// a consistency level when it is eventual. Those defaults are non-empty
// strings, so the structural emptiness test cannot see them, and a field set
// explicitly to its default would otherwise be written where Rust omits it.
type DefaultValued interface {
	IsDefaultValue() bool
}

var defaultValuedType = reflect.TypeOf((*DefaultValued)(nil)).Elem()

// isEmpty reports whether a field marked omitempty should be skipped.
//
// It covers every skip_serializing_if predicate the contract uses:
// Option::is_none (a nil pointer), is_false, is_empty, the is_zero family, and
// the named-default predicates through DefaultValued. Only fields the Rust
// type actually skips carry the tag, so a counted field such as
// TokenUsage::input_tokens still encodes its zero.
func isEmpty(value reflect.Value) bool {
	if value.IsValid() && value.Type().Implements(defaultValuedType) {
		return value.Interface().(DefaultValued).IsDefaultValue()
	}
	switch value.Kind() {
	case reflect.Pointer, reflect.Interface:
		return value.IsNil()
	case reflect.Bool:
		return !value.Bool()
	case reflect.Slice, reflect.Map, reflect.String:
		return value.Len() == 0
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return value.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return value.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return value.Float() == 0
	default:
		return false
	}
}

var marshalerType = reflect.TypeOf((*Marshaler)(nil)).Elem()

// isRawByte reports an element type that rides as a raw byte of a byte string
// rather than as its own value.
//
// A u8-backed contract type carries its own codec, so a vector of them is an
// array of encoded values. Only a plain byte collapses to a byte string.
func isRawByte(element reflect.Type) bool {
	if element.Kind() != reflect.Uint8 {
		return false
	}
	return !element.Implements(marshalerType) &&
		!reflect.PointerTo(element).Implements(unmarshalerType)
}

func toValue(value reflect.Value) (cborform.Value, error) {
	if !value.IsValid() {
		return cborform.Null{}, nil
	}

	// A type that encodes itself wins over the structural rules, so ids and
	// enums keep their exact wire form.
	if value.Type().Implements(marshalerType) {
		return value.Interface().(Marshaler).MarshalCBOR()
	}
	if value.Kind() != reflect.Pointer && value.CanAddr() {
		if pointer := value.Addr(); pointer.Type().Implements(marshalerType) {
			return pointer.Interface().(Marshaler).MarshalCBOR()
		}
	}

	switch value.Kind() {
	case reflect.Pointer:
		if value.IsNil() {
			return cborform.Null{}, nil
		}
		return toValue(value.Elem())

	case reflect.Interface:
		if value.IsNil() {
			return cborform.Null{}, nil
		}
		return toValue(value.Elem())

	case reflect.Bool:
		return cborform.Bool(value.Bool()), nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n := value.Int()
		if n < 0 {
			return cborform.NegInt(uint64(-1 - n)), nil
		}
		return cborform.Uint(uint64(n)), nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return cborform.Uint(value.Uint()), nil

	case reflect.Float32, reflect.Float64:
		return cborform.Float{Value: value.Float(), Bits: 64}, nil

	case reflect.String:
		return cborform.Text(value.String()), nil

	case reflect.Slice:
		if isRawByte(value.Type().Elem()) {
			if value.IsNil() {
				return cborform.Null{}, nil
			}
			return cborform.Bytes(value.Bytes()), nil
		}
		fallthrough

	case reflect.Array:
		if value.Kind() == reflect.Array && isRawByte(value.Type().Elem()) {
			raw := make([]byte, value.Len())
			reflect.Copy(reflect.ValueOf(raw), value)
			return cborform.Bytes(raw), nil
		}
		items := make(cborform.Array, 0, value.Len())
		for i := 0; i < value.Len(); i++ {
			item, err := toValue(value.Index(i))
			if err != nil {
				return nil, fmt.Errorf("element %d: %w", i, err)
			}
			items = append(items, item)
		}
		return items, nil

	case reflect.Map:
		if value.IsNil() {
			return cborform.Null{}, nil
		}
		// Rust writes its wire maps from ordered collections, so keys are
		// emitted in sorted order to keep the bytes reproducible.
		keys := value.MapKeys()
		sort.Slice(keys, func(a, b int) bool { return mapKeyLess(keys[a], keys[b]) })
		entries := make(cborform.Map, 0, len(keys))
		for _, key := range keys {
			encodedKey, err := toMapKey(key)
			if err != nil {
				return nil, fmt.Errorf("map key: %w", err)
			}
			encodedValue, err := toValue(value.MapIndex(key))
			if err != nil {
				return nil, fmt.Errorf("map value: %w", err)
			}
			entries = append(entries, cborform.Entry{Key: encodedKey, Value: encodedValue})
		}
		return entries, nil

	case reflect.Struct:
		entries := cborform.Map{}
		for _, structField := range fieldsOf(value.Type()) {
			fieldValue := value.Field(structField.index)
			if structField.omitEmpty && isEmpty(fieldValue) {
				continue
			}
			encoded, err := toValue(fieldValue)
			if err != nil {
				return nil, fmt.Errorf("field %q: %w", structField.name, err)
			}
			entries = append(entries, cborform.Entry{
				Key:   cborform.Text(structField.name),
				Value: encoded,
			})
		}
		return entries, nil

	default:
		return nil, fmt.Errorf("wire: cannot encode %s", value.Kind())
	}
}

// toValueOf encodes any Go value, for the variant helpers that build a payload
// from a struct.
func toValueOf(value any) (cborform.Value, error) {
	return toValue(reflect.ValueOf(value))
}

// fromValueInto decodes one already-parsed item into target, for the variant
// helpers that read a payload back into a struct.
func fromValueInto(item cborform.Value, target any) error {
	pointer := reflect.ValueOf(target)
	if pointer.Kind() != reflect.Pointer || pointer.IsNil() {
		return fmt.Errorf("wire: decoding a variant payload needs a non-nil pointer, got %T", target)
	}
	return fromValue(item, pointer.Elem())
}

func mapKeyLess(a, b reflect.Value) bool {
	switch a.Kind() {
	case reflect.String:
		return a.String() < b.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return a.Int() < b.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return a.Uint() < b.Uint()
	default:
		return fmt.Sprint(a.Interface()) < fmt.Sprint(b.Interface())
	}
}

// toMapKey encodes one map key.
//
// A numeric key rides as a number in the binary encoding and as its decimal
// string in a human-readable one, because JSON has no other kind of key.
func toMapKey(key reflect.Value) (cborform.Value, error) {
	encoded, err := toValue(key)
	if err != nil {
		return nil, err
	}
	switch encoded.(type) {
	case cborform.Uint, cborform.NegInt:
		return cborform.Textual{
			Binary: encoded, Text: fmt.Sprint(key.Interface()),
		}, nil
	default:
		return encoded, nil
	}
}
