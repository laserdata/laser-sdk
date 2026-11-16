package wire

import (
	"fmt"
	"math"
	"strings"
	"unicode"
)

// The query surface's validation rules.
//
// A query is refused here rather than by the server, so a malformed one fails
// where the mistake was made instead of after a round trip. Every rule mirrors
// the contract's, because a client that accepted what the server refuses would
// report the failure a step too late, and one that refused what the server
// accepts would deny a legal query outright.

// validateQueryName is the shared rule for a field, index or alias name:
// present, within the cap, and free of surrounding whitespace or control
// characters, any of which would change which column a name selects.
func validateQueryName(label, value string) error {
	if value == "" || len(value) > MaxQueryNameBytes {
		return &InvalidError{Reason: fmt.Sprintf(
			"%s must contain 1..=%d bytes", label, MaxQueryNameBytes)}
	}
	if strings.TrimSpace(value) != value || hasControl(value) {
		return &InvalidError{Reason: fmt.Sprintf(
			"%s contains surrounding whitespace or a control character", label)}
	}
	return nil
}

// Validate checks a query against every bound the contract names.
func (query Query) Validate() error {
	if query.ExecutionID.IsZero() {
		return &InvalidError{Reason: "query execution id must be nonzero"}
	}
	if query.DeadlineMicros == 0 {
		return &InvalidError{Reason: "query deadline must be nonzero"}
	}
	if query.Target.Kind != TargetOperational {
		return &InvalidError{Reason: fmt.Sprintf(
			"unknown query target %q", string(query.Target.Kind))}
	}
	if err := validateQueryName("index", query.Target.Index); err != nil {
		return err
	}
	if query.Page.Limit == 0 || query.Page.Limit > MaxPageSize {
		return &InvalidError{Reason: fmt.Sprintf(
			"query limit %d is outside 1..=%d", query.Page.Limit, MaxPageSize)}
	}
	for label, count := range map[string]int{
		"exact matches":   len(query.ByKey),
		"sort fields":     len(query.Order),
		"selected fields": len(query.Select.Fields),
	} {
		if count > MaxQueryFields {
			return &InvalidError{Reason: fmt.Sprintf(
				"query %s count %d exceeds cap %d", label, count, MaxQueryFields)}
		}
	}
	if query.Page.Offset != nil && *query.Page.Offset > MaxQueryOffset {
		return &InvalidError{Reason: fmt.Sprintf(
			"query offset %d exceeds cap %d", *query.Page.Offset, MaxQueryOffset)}
	}
	for _, match := range query.ByKey {
		if err := validateQueryName("key-match field", match.Field); err != nil {
			return err
		}
		if err := match.Value.Validate(); err != nil {
			return err
		}
	}
	if query.MessageType != nil {
		if err := validateQueryName("message type", *query.MessageType); err != nil {
			return err
		}
	}
	if query.TimeRange != nil && query.TimeRange.Start > query.TimeRange.End {
		return &InvalidError{Reason: "query time range start must not be after " +
			"its end, and both bounds are inclusive"}
	}
	for _, sort := range query.Order {
		if err := validateQueryName("sort field", sort.Field); err != nil {
			return err
		}
	}
	for _, field := range query.Select.Fields {
		if err := validateQueryName("selected field", field); err != nil {
			return err
		}
	}
	if query.Text != nil {
		if err := query.Text.Validate(); err != nil {
			return err
		}
	}
	if query.Vector != nil {
		if err := query.Vector.Validate(); err != nil {
			return err
		}
	}
	if err := validateQueryFilter(query.Filter); err != nil {
		return err
	}
	if err := validateQueryFilter(query.Having); err != nil {
		return err
	}
	if query.Having != nil && query.Aggregate == nil {
		return &InvalidError{Reason: "query having filter requires an aggregate"}
	}
	if query.Aggregate != nil {
		if err := query.Aggregate.Validate(); err != nil {
			return err
		}
		if len(query.Select.Fields) != 0 || query.Select.Payload {
			return &InvalidError{Reason: "aggregate queries cannot request row selection"}
		}
	}

	// The four result shapes are mutually exclusive: each orders and bounds
	// its rows its own way, so two at once has no defined answer.
	shapes := 0
	for _, set := range []bool{
		query.Text != nil, query.Vector != nil, query.Aggregate != nil, query.Distinct,
	} {
		if set {
			shapes++
		}
	}
	if shapes > 1 {
		return &InvalidError{
			Reason: "a query takes at most one of text, vector, aggregate and distinct",
		}
	}
	if query.Text != nil && (len(query.Order) != 0 || query.Page.WantTotal) {
		return &InvalidError{
			Reason: "a text query ranks by relevance and takes no order or total",
		}
	}
	if query.Vector != nil {
		offset := query.Page.Offset != nil && *query.Page.Offset > 0
		if len(query.Order) != 0 || query.Page.WantTotal || offset {
			return &InvalidError{Reason: "a vector query returns its nearest rows " +
				"and takes no order, offset or total"}
		}
	}
	if query.RawSQL != nil {
		if err := query.RawSQL.Validate(); err != nil {
			return err
		}
		if query.hasStructuredExpression() {
			return &InvalidError{
				Reason: "raw SQL cannot be combined with the structured query expression",
			}
		}
	}
	return nil
}

// hasStructuredExpression reports whether the query names any part of the
// structured form, which raw SQL replaces rather than extends.
func (query Query) hasStructuredExpression() bool {
	return len(query.ByKey) != 0 || query.MessageType != nil ||
		query.TimeRange != nil || query.Filter != nil || query.Vector != nil ||
		query.Text != nil || len(query.Order) != 0 || query.Aggregate != nil ||
		query.Having != nil || query.Distinct ||
		len(query.Select.Fields) != 0 || query.Select.Payload
}

// maxQueryFilterDepth bounds a filter tree, so a hostile one cannot exhaust
// the stack before the server has authorized the request.
const maxQueryFilterDepth = 64

// validateQueryFilter bounds a filter tree's size and depth and checks each
// leaf, counting nodes across the whole tree rather than per branch.
func validateQueryFilter(filter *Filter) error {
	if filter == nil {
		return nil
	}
	count := 0
	return visitQueryFilter(*filter, 0, &count)
}

func visitQueryFilter(filter Filter, depth int, count *int) error {
	*count++
	if *count > MaxQueryPredicates {
		return &InvalidError{Reason: fmt.Sprintf(
			"query filter node count exceeds cap %d", MaxQueryPredicates)}
	}
	if depth > maxQueryFilterDepth {
		return &InvalidError{Reason: fmt.Sprintf(
			"query filter depth exceeds cap %d", maxQueryFilterDepth)}
	}
	switch filter.Kind {
	case FilterAll, FilterAny:
		if len(filter.Children) == 0 {
			return &InvalidError{Reason: "query filter conjunction must not be empty"}
		}
		for _, child := range filter.Children {
			if err := visitQueryFilter(child, depth+1, count); err != nil {
				return err
			}
		}
		return nil
	case FilterNot:
		if filter.Negated == nil {
			return &InvalidError{Reason: "query filter negation must hold a filter"}
		}
		return visitQueryFilter(*filter.Negated, depth+1, count)
	default:
		if filter.Pred == nil {
			return &InvalidError{Reason: "query filter leaf must hold a predicate"}
		}
		if err := validateQueryName("filter field", filter.Pred.Field); err != nil {
			return err
		}
		return filter.Pred.Value.Validate()
	}
}

// Validate checks a raw SQL statement and its positional parameters.
func (sql RawSql) Validate() error {
	if strings.TrimSpace(sql.SQL) == "" || len(sql.SQL) > MaxRawSQLBytes {
		return &InvalidError{Reason: fmt.Sprintf(
			"raw SQL must contain 1..=%d bytes", MaxRawSQLBytes)}
	}
	// Newlines and tabs are how a statement is written, so only the other
	// control characters are refused.
	for _, r := range sql.SQL {
		if unicode.IsControl(r) && r != '\n' && r != '\r' && r != '\t' {
			return &InvalidError{Reason: "raw SQL contains a control character"}
		}
	}
	if len(sql.Params) > MaxQueryParameters {
		return &InvalidError{Reason: fmt.Sprintf(
			"raw SQL has %d parameters, exceeds cap %d",
			len(sql.Params), MaxQueryParameters)}
	}
	for _, param := range sql.Params {
		if err := param.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// Validate checks an aggregation's grouping, aliases and function arguments.
func (aggregate Aggregate) Validate() error {
	if len(aggregate.Funcs) == 0 || len(aggregate.Funcs) > MaxQueryParameters {
		return &InvalidError{Reason: fmt.Sprintf(
			"aggregate function count must be in 1..=%d", MaxQueryParameters)}
	}
	for _, field := range aggregate.GroupBy {
		if err := validateQueryName("group field", field); err != nil {
			return err
		}
	}
	if len(aggregate.GroupBy) > MaxQueryFields {
		return &InvalidError{Reason: fmt.Sprintf(
			"aggregate group field count exceeds cap %d", MaxQueryFields)}
	}
	// An alias names a column of the result, so two calls cannot share one.
	aliases := make(map[string]bool, len(aggregate.Funcs))
	for _, call := range aggregate.Funcs {
		if err := validateQueryName("aggregate alias", call.Alias); err != nil {
			return err
		}
		if aliases[call.Alias] {
			return &InvalidError{Reason: fmt.Sprintf(
				"aggregate repeats alias `%s`", call.Alias)}
		}
		aliases[call.Alias] = true
		if call.Field != nil {
			if err := validateQueryName("aggregate field", *call.Field); err != nil {
				return err
			}
		}
		if err := call.validateArguments(); err != nil {
			return err
		}
	}
	return nil
}

// validateArguments checks that a call's field and argument match the shape
// its function takes.
func (call AggCall) validateArguments() error {
	switch {
	case call.Func == AggCount && call.Arg == nil:
		return nil
	case call.Func == AggPercentile:
		fraction := call.Field != nil && call.Arg != nil &&
			!math.IsInf(*call.Arg, 0) && !math.IsNaN(*call.Arg) &&
			*call.Arg >= 0 && *call.Arg <= 1
		if fraction {
			return nil
		}
		return &InvalidError{
			Reason: "percentile requires a field and a finite fraction in 0..=1",
		}
	case call.Field != nil && call.Arg == nil:
		return nil
	default:
		return &InvalidError{
			Reason: "aggregate field and argument do not match the selected function",
		}
	}
}

// Validate checks a lexical relevance search.
func (text TextQuery) Validate() error {
	if strings.TrimSpace(text.Query) == "" || len(text.Query) > MaxTextQueryBytes {
		return &InvalidError{Reason: fmt.Sprintf(
			"text query is %dB, expected 1..=%dB", len(text.Query), MaxTextQueryBytes)}
	}
	if hasControl(text.Query) {
		return &InvalidError{Reason: "text query contains a control character"}
	}
	if text.Field != nil {
		field := *text.Field
		if field == "" || len(field) > MaxQueryNameBytes || hasControl(field) {
			return &InvalidError{Reason: "text query field is invalid"}
		}
	}
	return nil
}

// Validate checks a nearest-neighbour search.
func (vector VectorQuery) Validate() error {
	if vector.Field == "" || len(vector.Field) > MaxQueryNameBytes ||
		hasControl(vector.Field) {
		return &InvalidError{Reason: "vector field is invalid"}
	}
	if len(vector.Embedding) == 0 || len(vector.Embedding) > MaxVectorDimensions {
		return &InvalidError{Reason: fmt.Sprintf(
			"vector dimensions must be in 1..=%d", MaxVectorDimensions)}
	}
	for _, value := range vector.Embedding {
		// A negative zero is a second spelling of zero, so it would make one
		// embedding two distinct byte sequences.
		negativeZero := value == 0 && math.Signbit(float64(value))
		if math.IsInf(float64(value), 0) || math.IsNaN(float64(value)) || negativeZero {
			return &InvalidError{Reason: "vector values must be finite canonical floats"}
		}
	}
	if vector.TopK == 0 || vector.TopK > MaxPageSize {
		return &InvalidError{Reason: fmt.Sprintf(
			"vector top_k %d is outside 1..=%d", vector.TopK, MaxPageSize)}
	}
	return nil
}

// Validate checks the evidence a reply carries about how it was served.
//
// A context whose delivered consistency was weaker than the one requested
// would have a caller trust a read that never met its level, so the pair is
// checked here rather than assumed.
func (context QueryContext) Validate() error {
	if context.ExecutionID.IsZero() {
		return &InvalidError{Reason: "query context execution id must be nonzero"}
	}
	if err := validateQueryName("query engine name", context.Engine.Name); err != nil {
		return err
	}
	if err := validateQueryName(
		"query engine version", context.Engine.Version); err != nil {
		return err
	}
	if context.DeliveredConsistency.Rank() < context.RequestedConsistency.Rank() {
		return &InvalidError{
			Reason: "delivered consistency is weaker than requested consistency",
		}
	}
	if context.ResolvedTarget.Kind != ResolvedOperational {
		return &InvalidError{Reason: fmt.Sprintf(
			"unknown resolved query target %q", string(context.ResolvedTarget.Kind))}
	}
	index, named := context.ResolvedTarget.Index()
	if !named {
		return &InvalidError{Reason: "a resolved operational target carries no index"}
	}
	if err := validateQueryName("resolved index", index); err != nil {
		return err
	}
	// The three identify which backend, at which generation and which
	// configuration, actually answered. A reply missing any of them cannot be
	// attributed, so a stale one could not be told from a fresh one.
	operational := context.ResolvedTarget.Operational
	if operational.BackendResourceID.IsZero() || operational.BackendGeneration == 0 ||
		operational.RuntimeConfigurationRevision == 0 {
		return &InvalidError{
			Reason: "operational query context is missing backend evidence",
		}
	}
	return nil
}
