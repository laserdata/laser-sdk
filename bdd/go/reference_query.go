package bdd

import (
	"sort"
	"strconv"
	"strings"
)

// Reference query engine: the executable specification of the query
// semantics every LaserData SDK and the managed backend must reproduce.
// Pure and transport-free, the same contract bdd/python/reference.py's
// QueryEngine and the Rust BDD crate's own engine implement. This port must
// return the same answers for the same inputs.

// MaxPageSize is a full page when a query sets no limit.
const MaxPageSize = 1000

// QueryRow is one indexed row: string fields, as a materialized read model
// stores them.
type QueryRow map[string]string

// QueryPredicate narrows the rows a query returns.
type QueryPredicate struct {
	Field string
	// Op is one of gt, gte, lt, lte, eq, ne, contains, prefix.
	Op    string
	Value string
}

// QueryOrder is one sort key and its direction.
type QueryOrder struct {
	Field string
	// Desc sorts highest first.
	Desc bool
}

// QueryAggregate groups the matched rows and folds each group.
type QueryAggregate struct {
	GroupBy []string
	// Func is the fold, today only "count".
	Func  string
	Alias string
}

// QueryPage is what a query answers: the page, and how much it was taken
// from.
type QueryPage struct {
	Rows    []QueryRow
	Total   int
	Limit   int
	Offset  int
	HasMore bool
}

// QueryEngine is an in-memory materialized index.
type QueryEngine struct {
	indexes map[string][]QueryRow
}

// NewQueryEngine opens an empty engine.
func NewQueryEngine() *QueryEngine {
	return &QueryEngine{indexes: map[string][]QueryRow{}}
}

// Insert appends one row to an index.
func (engine *QueryEngine) Insert(index string, row QueryRow) {
	copied := make(QueryRow, len(row))
	for field, value := range row {
		copied[field] = value
	}
	engine.indexes[index] = append(engine.indexes[index], copied)
}

// QueryRequest is one query against an index.
type QueryRequest struct {
	Index     string
	Predicate *QueryPredicate
	Order     []QueryOrder
	Limit     int
	Offset    int
	Aggregate *QueryAggregate
}

// Execute runs a query and answers its page.
func (engine *QueryEngine) Execute(request QueryRequest) QueryPage {
	rows, known := engine.indexes[request.Index]
	if !known {
		return QueryPage{Limit: request.Limit, Offset: request.Offset}
	}
	matched := make([]QueryRow, 0, len(rows))
	for _, row := range rows {
		if matches(row, request.Predicate) {
			matched = append(matched, row)
		}
	}
	if request.Aggregate != nil {
		return aggregate(matched, *request.Aggregate)
	}
	// Applied last key first, so the first key named is the one that
	// decides, which is what a stable multi-key sort means.
	for index := len(request.Order) - 1; index >= 0; index-- {
		order := request.Order[index]
		sort.SliceStable(matched, func(left, right int) bool {
			less := lessBySortKey(matched[left], matched[right], order.Field)
			if order.Desc {
				return lessBySortKey(matched[right], matched[left], order.Field)
			}
			return less
		})
	}
	total := len(matched)
	limit := request.Limit
	if limit == 0 {
		limit = MaxPageSize
	}
	page := []QueryRow{}
	if request.Offset < total {
		end := min(request.Offset+limit, total)
		page = matched[request.Offset:end]
	}
	return QueryPage{
		Rows:    page,
		Total:   total,
		Limit:   limit,
		Offset:  request.Offset,
		HasMore: request.Offset+len(page) < total,
	}
}

// matches reports whether a row satisfies the predicate. A row without the
// field never matches: an absent field is not an empty one.
func matches(row QueryRow, predicate *QueryPredicate) bool {
	if predicate == nil {
		return true
	}
	value, held := row[predicate.Field]
	if !held {
		return false
	}
	return compare(value, predicate.Op, predicate.Value)
}

// compare is numeric when the bound is a number, and otherwise lexical,
// matching every other client's reference engine.
func compare(field, op, value string) bool {
	switch op {
	case "contains":
		return strings.Contains(field, value)
	case "prefix":
		return strings.HasPrefix(field, value)
	}
	left, leftNumber := asNumber(field)
	right, rightNumber := asNumber(value)
	if leftNumber && rightNumber {
		switch op {
		case "gt":
			return left > right
		case "gte":
			return left >= right
		case "lt":
			return left < right
		case "lte":
			return left <= right
		case "eq":
			return left == right
		case "ne":
			return left != right
		}
		return false
	}
	switch op {
	case "gt":
		return field > value
	case "gte":
		return field >= value
	case "lt":
		return field < value
	case "lte":
		return field <= value
	case "eq":
		return field == value
	case "ne":
		return field != value
	}
	return false
}

// lessBySortKey orders numerically when the field parses as a number, so
// "30" sorts below "550", not lexically above it.
//
// Numbers sort before non-numbers, so one unparseable row cannot scatter the
// rest.
func lessBySortKey(left, right QueryRow, field string) bool {
	leftValue, leftNumber := asNumber(left[field])
	rightValue, rightNumber := asNumber(right[field])
	switch {
	case leftNumber && rightNumber:
		return leftValue < rightValue
	case leftNumber:
		return true
	case rightNumber:
		return false
	default:
		return left[field] < right[field]
	}
}

// asNumber reads a field as a number, reporting whether it is one.
func asNumber(value string) (float64, bool) {
	number, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, false
	}
	return number, true
}

// aggregate folds the matched rows into one row per group, groups ordered by
// their key so two runs answer the same way.
func aggregate(matched []QueryRow, request QueryAggregate) QueryPage {
	groups := map[string][]QueryRow{}
	var order []string
	for _, row := range matched {
		key := groupKey(row, request.GroupBy)
		if _, seen := groups[key]; !seen {
			order = append(order, key)
		}
		groups[key] = append(groups[key], row)
	}
	sort.Strings(order)
	rows := make([]QueryRow, 0, len(order))
	for _, key := range order {
		members := groups[key]
		folded := QueryRow{}
		for _, field := range request.GroupBy {
			folded[field] = members[0][field]
		}
		folded[request.Alias] = aggregateValue(members, request.Func)
		rows = append(rows, folded)
	}
	return QueryPage{
		Rows:  rows,
		Total: len(rows),
		Limit: max(len(rows), 1),
	}
}

// groupKey joins a row's group fields into one comparable key.
func groupKey(row QueryRow, fields []string) string {
	parts := make([]string, 0, len(fields))
	for _, field := range fields {
		parts = append(parts, row[field])
	}
	return strings.Join(parts, "\x00")
}

// aggregateValue folds one group. Count is the only fold the contract names.
func aggregateValue(members []QueryRow, function string) string {
	if function == "count" {
		return strconv.Itoa(len(members))
	}
	return ""
}
