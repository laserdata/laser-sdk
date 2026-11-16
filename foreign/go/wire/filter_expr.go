package wire

import (
	"fmt"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// CmpOp is a comparison operator for a filter leaf.
type CmpOp string

const (
	CmpEq       CmpOp = "eq"
	CmpNe       CmpOp = "ne"
	CmpLt       CmpOp = "lt"
	CmpLte      CmpOp = "lte"
	CmpGt       CmpOp = "gt"
	CmpGte      CmpOp = "gte"
	CmpIn       CmpOp = "in"
	CmpContains CmpOp = "contains"
	CmpPrefix   CmpOp = "prefix"
)

// IsKnown reports whether this build names the operator.
func (op CmpOp) IsKnown() bool {
	switch op {
	case CmpEq, CmpNe, CmpLt, CmpLte, CmpGt, CmpGte, CmpIn, CmpContains, CmpPrefix:
		return true
	default:
		return false
	}
}

// MarshalCBOR writes the snake_case name.
func (op CmpOp) MarshalCBOR() (cborform.Value, error) {
	if !op.IsKnown() {
		return nil, fmt.Errorf("wire: unknown comparison operator %q", string(op))
	}
	return cborform.Text(string(op)), nil
}

// UnmarshalCBOR reads the snake_case name.
func (op *CmpOp) UnmarshalCBOR(item cborform.Value) error {
	text, ok := item.(cborform.Text)
	if !ok {
		return fmt.Errorf("wire: cannot decode %T as a comparison operator", item)
	}
	candidate := CmpOp(text)
	if !candidate.IsKnown() {
		return fmt.Errorf("wire: unknown comparison operator %q", string(text))
	}
	*op = candidate
	return nil
}

// Dir is a sort direction.
type Dir string

const (
	// DirAsc is the default direction.
	DirAsc  Dir = "asc"
	DirDesc Dir = "desc"
)

// Sort is an order-by clause.
type Sort struct {
	Field string `cbor:"field"`
	Dir   Dir    `cbor:"dir"`
}

// Consistency is how fresh a managed read must be.
//
// The order is meaningful, running eventual then read-your-writes, so a
// capability check is "wanted is at most served".
type Consistency string

const (
	// ConsistencyEventual serves from the index as-is, whatever the projector
	// has applied so far. The default and cheapest.
	ConsistencyEventual Consistency = "eventual"
	// ConsistencyReadYourWrites waits until the projector has applied the
	// source log up to its head, so a query issued after a publish sees that
	// write. Bounded: a projector that cannot catch up within the managed
	// deadline returns stale rather than downgrading to a stale read.
	ConsistencyReadYourWrites Consistency = "read_your_writes"
)

// Rank orders the levels so a stronger level compares greater.
func (consistency Consistency) Rank() int {
	switch consistency {
	case ConsistencyReadYourWrites:
		return 1
	default:
		return 0
	}
}

// IsEventual reports the default level, which is omitted on the wire.
func (consistency Consistency) IsEventual() bool {
	return consistency == ConsistencyEventual || consistency == ""
}

// Predicate is a filter leaf: a field, a comparison operator and a value.
type Predicate struct {
	Field string     `cbor:"field"`
	Op    CmpOp      `cbor:"op"`
	Value TypedValue `cbor:"value"`
}

// FilterKind discriminates a predicate tree node.
type FilterKind uint8

const (
	// FilterAll is an n-ary AND.
	FilterAll FilterKind = iota
	// FilterAny is an n-ary OR.
	FilterAny
	// FilterNot negates one filter.
	FilterNot
	// FilterPred is a single comparison leaf.
	FilterPred
)

// Filter is a predicate tree, shared by the query and graph surfaces so there
// is one filter grammar across both.
type Filter struct {
	Kind     FilterKind
	Children []Filter
	Negated  *Filter
	Pred     *Predicate
}

// AllOf is the AND of its filters.
func AllOf(filters ...Filter) Filter {
	return Filter{Kind: FilterAll, Children: filters}
}

// AnyOf is the OR of its filters.
func AnyOf(filters ...Filter) Filter {
	return Filter{Kind: FilterAny, Children: filters}
}

// Negate negates a filter.
func Negate(filter Filter) Filter {
	return Filter{Kind: FilterNot, Negated: &filter}
}

// Pred is a single comparison leaf.
func Pred(field string, op CmpOp, value TypedValue) Filter {
	return Filter{Kind: FilterPred, Pred: &Predicate{Field: field, Op: op, Value: value}}
}

// The externally tagged, snake_case filter variant names.
const (
	filterAllVariant  = "all"
	filterAnyVariant  = "any"
	filterNotVariant  = "not"
	filterPredVariant = "pred"
)

// MarshalCBOR writes the externally tagged filter.
func (filter Filter) MarshalCBOR() (cborform.Value, error) {
	switch filter.Kind {
	case FilterAll, FilterAny:
		name := filterAllVariant
		if filter.Kind == FilterAny {
			name = filterAnyVariant
		}
		children := make(cborform.Array, 0, len(filter.Children))
		for i, child := range filter.Children {
			encoded, err := child.MarshalCBOR()
			if err != nil {
				return nil, fmt.Errorf("filter child %d: %w", i, err)
			}
			children = append(children, encoded)
		}
		return taggedVariant(name, children), nil
	case FilterNot:
		if filter.Negated == nil {
			return nil, fmt.Errorf("wire: a negation carries no filter")
		}
		inner, err := filter.Negated.MarshalCBOR()
		if err != nil {
			return nil, err
		}
		return taggedVariant(filterNotVariant, inner), nil
	case FilterPred:
		if filter.Pred == nil {
			return nil, fmt.Errorf("wire: a predicate leaf carries no predicate")
		}
		return marshalVariantStruct(filterPredVariant, *filter.Pred)
	default:
		return nil, fmt.Errorf("wire: unknown filter kind %d", filter.Kind)
	}
}

// UnmarshalCBOR reads the externally tagged filter.
func (filter *Filter) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	if err := requirePayload("Filter", name, payload); err != nil {
		return err
	}

	switch name {
	case filterAllVariant, filterAnyVariant:
		items, ok := payload.(cborform.Array)
		if !ok {
			return fmt.Errorf("wire: %q carries a list of filters, got %T", name, payload)
		}
		children := make([]Filter, len(items))
		for i, child := range items {
			if err := children[i].UnmarshalCBOR(child); err != nil {
				return fmt.Errorf("filter child %d: %w", i, err)
			}
		}
		kind := FilterAll
		if name == filterAnyVariant {
			kind = FilterAny
		}
		*filter = Filter{Kind: kind, Children: children}
		return nil

	case filterNotVariant:
		var inner Filter
		if err := inner.UnmarshalCBOR(payload); err != nil {
			return err
		}
		*filter = Filter{Kind: FilterNot, Negated: &inner}
		return nil

	case filterPredVariant:
		var predicate Predicate
		if err := fromValueInto(payload, &predicate); err != nil {
			return err
		}
		*filter = Filter{Kind: FilterPred, Pred: &predicate}
		return nil

	default:
		return unknownVariant("Filter", name)
	}
}

// IsDefaultValue reports the default consistency level, omitted on the wire.
func (consistency Consistency) IsDefaultValue() bool { return consistency.IsEventual() }

// UnmarshalCBOR reads the value, refusing one outside the closed vocabulary.
func (value *Consistency) UnmarshalCBOR(item cborform.Value) error {
	return decodeClosedEnum(item, value, "consistency", ConsistencyEventual, ConsistencyReadYourWrites)
}

// UnmarshalCBOR reads the value, refusing one outside the closed vocabulary.
func (value *Dir) UnmarshalCBOR(item cborform.Value) error {
	return decodeClosedEnum(item, value, "dir", DirAsc, DirDesc)
}
