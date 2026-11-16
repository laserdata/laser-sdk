package wire

import (
	"fmt"
	"math"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// ValueKind discriminates a Value.
type ValueKind uint8

// The Value variants, in the order the untagged decode tries them.
const (
	ValueNull ValueKind = iota
	ValueStr
	ValueInt
	ValueUint
	ValueFloat
	ValueBool
	ValueList
)

// Value is a scalar used by the graph, metadata, and the older non-document
// surfaces. Query predicates and results use the tagged TypedValue contract.
//
// It is untagged on the wire, riding as a bare scalar. Variant order matters
// for the untagged decode: a small or negative integer stays an Int, and a
// value past the signed maximum falls through to Uint rather than becoming a
// lossy Float.
type Value struct {
	Kind  ValueKind
	Str   string
	Int   int64
	Uint  uint64
	Float float64
	Bool  bool
	List  []Value
}

// The Value constructors.
func NullValue() Value                { return Value{Kind: ValueNull} }
func StrValue(value string) Value     { return Value{Kind: ValueStr, Str: value} }
func IntValue(value int64) Value      { return Value{Kind: ValueInt, Int: value} }
func UintValue(value uint64) Value    { return Value{Kind: ValueUint, Uint: value} }
func FloatValue(value float64) Value  { return Value{Kind: ValueFloat, Float: value} }
func BoolValue(value bool) Value      { return Value{Kind: ValueBool, Bool: value} }
func ListValue(values ...Value) Value { return Value{Kind: ValueList, List: values} }

// MarshalCBOR writes the bare scalar, with no variant tag.
func (value Value) MarshalCBOR() (cborform.Value, error) {
	switch value.Kind {
	case ValueNull:
		return cborform.Null{}, nil
	case ValueStr:
		return cborform.Text(value.Str), nil
	case ValueInt:
		if value.Int < 0 {
			return cborform.NegInt(uint64(-1 - value.Int)), nil
		}
		return cborform.Uint(uint64(value.Int)), nil
	case ValueUint:
		return cborform.Uint(value.Uint), nil
	case ValueFloat:
		return cborform.Float{Value: value.Float, Bits: 64}, nil
	case ValueBool:
		return cborform.Bool(value.Bool), nil
	case ValueList:
		items := make(cborform.Array, 0, len(value.List))
		for i, element := range value.List {
			encoded, err := element.MarshalCBOR()
			if err != nil {
				return nil, fmt.Errorf("list element %d: %w", i, err)
			}
			items = append(items, encoded)
		}
		return items, nil
	default:
		return nil, fmt.Errorf("wire: unknown value kind %d", value.Kind)
	}
}

// UnmarshalCBOR reads a bare scalar.
func (value *Value) UnmarshalCBOR(item cborform.Value) error {
	switch typed := item.(type) {
	case cborform.Null, cborform.Undefined:
		*value = NullValue()
		return nil
	case cborform.Text:
		*value = StrValue(string(typed))
		return nil
	case cborform.Uint:
		// A value that fits the signed range stays an Int. Anything past it
		// falls through to Uint rather than losing precision as a Float.
		if uint64(typed) <= math.MaxInt64 {
			*value = IntValue(int64(typed))
		} else {
			*value = UintValue(uint64(typed))
		}
		return nil
	case cborform.NegInt:
		if uint64(typed) > math.MaxInt64 {
			return fmt.Errorf("wire: negative value %d underflows an i64", typed)
		}
		*value = IntValue(-1 - int64(typed))
		return nil
	case cborform.Float:
		*value = FloatValue(typed.Value)
		return nil
	case cborform.Bool:
		*value = BoolValue(bool(typed))
		return nil
	case cborform.Array:
		items := make([]Value, len(typed))
		for i, element := range typed {
			if err := items[i].UnmarshalCBOR(element); err != nil {
				return fmt.Errorf("list element %d: %w", i, err)
			}
		}
		*value = Value{Kind: ValueList, List: items}
		return nil
	default:
		return fmt.Errorf("wire: cannot decode %T as a value", item)
	}
}

// size is the approximate scalar size used by the metadata caps: text by
// length, scalars by their widest encoding, lists by the sum of their elements.
func (value Value) size() int {
	switch value.Kind {
	case ValueStr:
		return len(value.Str)
	case ValueList:
		total := 0
		for _, element := range value.List {
			total += 1 + element.size()
		}
		return total
	default:
		return 9
	}
}
