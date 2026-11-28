package wire

import (
	"math"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// DynValue is an arbitrary document value: the Go stand-in for the contract's
// free-form JSON values, such as a session's folded state document.
//
// It holds the decoded structure directly, so a document round-trips through
// the wire unchanged whatever shape it has, without the port having to model
// every application's schema.
type DynValue struct {
	item cborform.Value
}

// NewDynValue wraps an already-decoded value.
func NewDynValue(item cborform.Value) DynValue { return DynValue{item: item} }

// DynValueOf turns a Go value into a document, as Marshal encodes it.
func DynValueOf(value any) (DynValue, error) {
	encoded, err := Marshal(value)
	if err != nil {
		return DynValue{}, err
	}
	var document DynValue
	if err := Unmarshal(encoded, &document); err != nil {
		return DynValue{}, err
	}
	return document, nil
}

// IsZero reports an absent document.
func (value DynValue) IsZero() bool { return value.item == nil }

// Item is the decoded structure.
func (value DynValue) Item() cborform.Value { return value.item }

// Text reads the value as a string. The second result is false for any other
// shape.
func (value DynValue) Text() (string, bool) {
	text, ok := value.item.(cborform.Text)
	return string(text), ok
}

// Field reads one entry of an object value.
func (value DynValue) Field(name string) (DynValue, bool) {
	entries, ok := value.item.(cborform.Map)
	if !ok {
		return DynValue{}, false
	}
	for _, entry := range entries {
		if key, ok := entry.Key.(cborform.Text); ok && string(key) == name {
			return DynValue{item: entry.Value}, true
		}
	}
	return DynValue{}, false
}

// MarshalCBOR writes the held structure, or null when absent.
func (value DynValue) MarshalCBOR() (cborform.Value, error) {
	if value.item == nil {
		return cborform.Null{}, nil
	}
	return value.item, nil
}

// UnmarshalCBOR keeps the decoded structure as it arrived.
func (value *DynValue) UnmarshalCBOR(item cborform.Value) error {
	value.item = item
	return nil
}

// Clone is a deep copy, so a patched document never aliases the one it was
// built from.
func (value DynValue) Clone() DynValue {
	return DynValue{item: cloneValue(value.item)}
}

func cloneValue(item cborform.Value) cborform.Value {
	switch typed := item.(type) {
	case cborform.Array:
		items := make(cborform.Array, len(typed))
		for index, element := range typed {
			items[index] = cloneValue(element)
		}
		return items
	case cborform.Map:
		entries := make(cborform.Map, len(typed))
		for index, entry := range typed {
			entries[index] = cborform.Entry{
				Key: cloneValue(entry.Key), Value: cloneValue(entry.Value),
			}
		}
		return entries
	case cborform.Bytes:
		return cborform.Bytes(append([]byte{}, typed...))
	default:
		// Every other node is immutable.
		return item
	}
}

// Equal reports whether two values hold the same document.
//
// Objects compare by their fields rather than their order, because a document
// that round-tripped through another SDK may name its fields in another order
// and still be the same document.
func (value DynValue) Equal(other DynValue) bool {
	return equalValues(value.item, other.item)
}

func equalValues(left, right cborform.Value) bool {
	switch typedLeft := left.(type) {
	case cborform.Array:
		typedRight, ok := right.(cborform.Array)
		if !ok || len(typedLeft) != len(typedRight) {
			return false
		}
		for index := range typedLeft {
			if !equalValues(typedLeft[index], typedRight[index]) {
				return false
			}
		}
		return true
	case cborform.Map:
		typedRight, ok := right.(cborform.Map)
		if !ok || len(typedLeft) != len(typedRight) {
			return false
		}
		for _, entry := range typedLeft {
			key, ok := entry.Key.(cborform.Text)
			if !ok {
				return false
			}
			found, present := DynValue{item: right}.Field(string(key))
			if !present || !equalValues(entry.Value, found.item) {
				return false
			}
		}
		return true
	case cborform.Bytes:
		typedRight, ok := right.(cborform.Bytes)
		if !ok || len(typedLeft) != len(typedRight) {
			return false
		}
		for index := range typedLeft {
			if typedLeft[index] != typedRight[index] {
				return false
			}
		}
		return true
	default:
		if number, comparable := equalNumbers(left, right); comparable {
			return number
		}
		return left == right
	}
}

// equalNumbers compares two numeric values by value rather than by form.
//
// A document that round-tripped through a JSON surface may carry a whole
// number as a float, and it is the same document: comparing by form would
// report a state write as applied when it changed nothing. The second result
// is false when either side is not a number, so the caller falls back to
// identity.
func equalNumbers(left, right cborform.Value) (bool, bool) {
	leftInteger, leftIsInteger := cborIntegerOf(left)
	rightInteger, rightIsInteger := cborIntegerOf(right)
	leftFloat, leftIsFloat := left.(cborform.Float)
	rightFloat, rightIsFloat := right.(cborform.Float)

	switch {
	case leftIsInteger && rightIsInteger:
		return leftInteger == rightInteger, true
	case leftIsInteger && rightIsFloat:
		return wholeFloatEquals(rightFloat.Value, leftInteger), true
	case leftIsFloat && rightIsInteger:
		return wholeFloatEquals(leftFloat.Value, rightInteger), true
	case leftIsFloat && rightIsFloat:
		return leftFloat.Value == rightFloat.Value, true
	default:
		return false, false
	}
}

// cborIntegerOf reads a CBOR integer as a signed value. The second result
// is false for anything else, including a float.
func cborIntegerOf(item cborform.Value) (int64, bool) {
	switch typed := item.(type) {
	case cborform.Uint:
		if typed > math.MaxInt64 {
			return 0, false
		}
		return int64(typed), true
	case cborform.NegInt:
		if typed > math.MaxInt64 {
			return 0, false
		}
		return -1 - int64(typed), true
	default:
		return 0, false
	}
}

// wholeFloatEquals reports a float that is exactly the integer, which is the
// only way the two forms name one value.
func wholeFloatEquals(value float64, integer int64) bool {
	return value == math.Trunc(value) && value == float64(integer) &&
		int64(value) == integer
}
