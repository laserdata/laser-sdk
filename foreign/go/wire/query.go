package wire

import (
	"fmt"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// QueryExecutionID is a query's execution boundary: the identity a page,
// cancel or status request addresses.
type QueryExecutionID struct{ ID }

// SqlDialect is the SQL grammar the selected query backend accepts.
type SqlDialect string

const (
	DialectPostgres SqlDialect = "postgres"
	DialectMySQL    SqlDialect = "my_sql"
	DialectSQLite   SqlDialect = "sqlite"
)

// QueryTargetKind discriminates what a query reads.
type QueryTargetKind string

const (
	// TargetOperational reads one materialized index.
	TargetOperational QueryTargetKind = "operational"
)

// QueryTarget is what a query reads. It is internally tagged, so the kind sits
// alongside the variant's own fields rather than nesting them.
type QueryTarget struct {
	Kind QueryTargetKind
	// Index names the materialized index an operational query reads.
	Index string
}

// OperationalTarget reads one materialized index.
func OperationalTarget(index string) QueryTarget {
	return QueryTarget{Kind: TargetOperational, Index: index}
}

type operationalTargetFields struct {
	Index string `cbor:"index"`
}

// internalTagKey is the key an internally tagged enum names its variant with.
const internalTagKey = "kind"

// marshalInternallyTagged prefixes a struct's fields with the variant tag.
func marshalInternallyTagged(kind string, fields any) (cborform.Value, error) {
	payload, err := toValueOf(fields)
	if err != nil {
		return nil, err
	}
	entries, ok := payload.(cborform.Map)
	if !ok {
		return nil, fmt.Errorf("wire: an internally tagged variant needs named fields")
	}
	tagged := make(cborform.Map, 0, len(entries)+1)
	tagged = append(tagged, cborform.Entry{
		Key:   cborform.Text(internalTagKey),
		Value: cborform.Text(kind),
	})
	return append(tagged, entries...), nil
}

// readInternalTag reads the variant name of an internally tagged value,
// returning the whole map so the caller can decode the remaining fields.
func readInternalTag(item cborform.Value) (string, cborform.Map, error) {
	entries, ok := item.(cborform.Map)
	if !ok {
		return "", nil, fmt.Errorf("wire: an internally tagged value is a map, got %T", item)
	}
	for _, entry := range entries {
		key, ok := entry.Key.(cborform.Text)
		if !ok || string(key) != internalTagKey {
			continue
		}
		name, ok := entry.Value.(cborform.Text)
		if !ok {
			return "", nil, fmt.Errorf("wire: a variant tag must be text, got %T", entry.Value)
		}
		return string(name), entries, nil
	}
	return "", nil, fmt.Errorf("wire: an internally tagged value names no kind")
}

// MarshalCBOR writes the internally tagged target.
func (target QueryTarget) MarshalCBOR() (cborform.Value, error) {
	switch target.Kind {
	case TargetOperational:
		return marshalInternallyTagged(string(TargetOperational),
			operationalTargetFields{Index: target.Index})
	default:
		return nil, fmt.Errorf("wire: unknown query target %q", string(target.Kind))
	}
}

// UnmarshalCBOR reads the internally tagged target.
func (target *QueryTarget) UnmarshalCBOR(item cborform.Value) error {
	kind, entries, err := readInternalTag(item)
	if err != nil {
		return err
	}
	switch QueryTargetKind(kind) {
	case TargetOperational:
		var fields operationalTargetFields
		if err := fromValueInto(entries, &fields); err != nil {
			return err
		}
		*target = QueryTarget{Kind: TargetOperational, Index: fields.Index}
		return nil
	default:
		return fmt.Errorf("wire: unknown query target %q", kind)
	}
}

// KeyMatch is one exact-match constraint: the indexed field must equal a value.
type KeyMatch struct {
	Field string     `cbor:"field"`
	Value TypedValue `cbor:"value"`
}

// TextQuery is a lexical relevance search: the text to match and, optionally,
// the one indexed field to match it in.
//
// It is capability-gated: a backend without a lexical index answers
// unsupported rather than approximating with a contains.
type TextQuery struct {
	Field *string `cbor:"field,omitempty"`
	Query string  `cbor:"query"`
}

// VectorQuery is a nearest-neighbour search.
type VectorQuery struct {
	Field     string    `cbor:"field"`
	Embedding []float32 `cbor:"embedding"`
	TopK      uint32    `cbor:"top_k"`
}

// AggFunc is an aggregate function. Percentile and StdDev are backend-gated.
type AggFunc string

const (
	AggCount         AggFunc = "count"
	AggCountDistinct AggFunc = "count_distinct"
	AggSum           AggFunc = "sum"
	AggAvg           AggFunc = "avg"
	AggMin           AggFunc = "min"
	AggMax           AggFunc = "max"
	AggPercentile    AggFunc = "percentile"
	AggStdDev        AggFunc = "std_dev"
)

// AggCall is one aggregate. Field is absent only for a count, and Arg is the
// fraction for a percentile. Alias names the output field in each result row.
type AggCall struct {
	Func  AggFunc  `cbor:"func"`
	Field *string  `cbor:"field,omitempty"`
	Arg   *float64 `cbor:"arg,omitempty"`
	Alias string   `cbor:"alias"`
}

// Window is a tumbling window over a timestamp field.
type Window struct {
	Field       string `cbor:"field"`
	EveryMicros uint64 `cbor:"every_micros"`
}

// Aggregate is a grouped aggregation carrying one or more calls, so one query
// can return several aggregates grouped by the same keys.
type Aggregate struct {
	GroupBy []string  `cbor:"group_by,omitempty"`
	Funcs   []AggCall `cbor:"funcs"`
	Window  *Window   `cbor:"window,omitempty"`
}

// Select is which columns and payload a query returns. Empty fields select
// every indexed field.
type Select struct {
	Fields []string `cbor:"fields,omitempty"`
	// Payload returns the opaque payload bytes alongside the indexed fields.
	// It carries no skip rule, so false is written explicitly.
	Payload bool `cbor:"payload"`
}

// RawSql is the raw-SQL escape hatch, scoped to the query's target. The server
// validates both the parsed statement and the resulting scan plan against it.
type RawSql struct {
	Dialect SqlDialect   `cbor:"dialect"`
	SQL     string       `cbor:"sql"`
	Params  []TypedValue `cbor:"params,omitempty"`
}

// TimeRange is an inclusive instant range, riding the wire as a two-element
// array the way a Rust tuple does.
type TimeRange struct {
	Start uint64
	End   uint64
}

// MarshalCBOR writes the pair as a two-element array.
func (span TimeRange) MarshalCBOR() (cborform.Value, error) {
	return cborform.Array{cborform.Uint(span.Start), cborform.Uint(span.End)}, nil
}

// UnmarshalCBOR reads the two-element array.
func (span *TimeRange) UnmarshalCBOR(item cborform.Value) error {
	pair, ok := item.(cborform.Array)
	if !ok || len(pair) != 2 {
		return fmt.Errorf("wire: a time range is a two-element array, got %T", item)
	}
	start, startOK := pair[0].(cborform.Uint)
	end, endOK := pair[1].(cborform.Uint)
	if !startOK || !endOK {
		return fmt.Errorf("wire: a time range holds two instants")
	}
	*span = TimeRange{Start: uint64(start), End: uint64(end)}
	return nil
}

// QueryPageRequest is the pagination a query asks for.
type QueryPageRequest struct {
	Limit  uint32  `cbor:"limit"`
	Offset *uint64 `cbor:"offset,omitempty"`
	// WantTotal runs a real count over the filter, which is unbounded work on
	// a large index.
	WantTotal bool `cbor:"want_total,omitempty"`
}

// DefaultQueryPageRequest is the page a query asks for when it names none.
func DefaultQueryPageRequest() QueryPageRequest {
	offset := uint64(0)
	return QueryPageRequest{Limit: 50, Offset: &offset}
}

// Query is a query against a materialized index.
type Query struct {
	ExecutionID QueryExecutionID `cbor:"execution_id"`
	Target      QueryTarget      `cbor:"target"`
	// DeadlineMicros is absolute. A server never extends it while queueing or
	// paging.
	DeadlineMicros uint64     `cbor:"deadline_micros"`
	ByKey          []KeyMatch `cbor:"by_key,omitempty"`
	MessageType    *string    `cbor:"message_type,omitempty"`
	// TimeRange bounds are both inclusive.
	TimeRange *TimeRange `cbor:"time_range,omitempty"`
	// Filter is the predicate tree. Absent with no sugar is an unfiltered scan.
	Filter *Filter      `cbor:"filter,omitempty"`
	Vector *VectorQuery `cbor:"vector,omitempty"`
	// Text is additive like Vector. An unaware server would silently drop it,
	// so a client refuses an unadvertised text search before sending.
	Text  *TextQuery       `cbor:"text,omitempty"`
	Order []Sort           `cbor:"order,omitempty"`
	Page  QueryPageRequest `cbor:"page"`
	// Aggregate is analytics, mutually exclusive with row selection.
	Aggregate *Aggregate `cbor:"aggregate,omitempty"`
	// Having filters aggregate output, referencing an alias or group key. It
	// is meaningful only with an aggregate.
	Having   *Filter `cbor:"having,omitempty"`
	Distinct bool    `cbor:"distinct,omitempty"`
	Select   Select  `cbor:"select"`
	// Fork resolves against a fork's copy-on-write view instead of the trunk.
	Fork   *string `cbor:"fork,omitempty"`
	RawSQL *RawSql `cbor:"raw_sql,omitempty"`
	// Consistency is absent on the wire at the default level.
	Consistency Consistency `cbor:"consistency,omitempty"`
}

// NewQuery creates a query for an explicit target and execution boundary.
func NewQuery(executionID QueryExecutionID, target QueryTarget, deadlineMicros uint64) Query {
	return Query{
		ExecutionID:    executionID,
		Target:         target,
		DeadlineMicros: deadlineMicros,
		Page:           DefaultQueryPageRequest(),
		Consistency:    ConsistencyEventual,
	}
}

// OperationalQuery creates a query for one materialized index.
func OperationalQuery(executionID QueryExecutionID, index string, deadlineMicros uint64) Query {
	return NewQuery(executionID, OperationalTarget(index), deadlineMicros)
}

// Page is the pagination a query result reports.
//
// The default reply costs one page: the server fetches one row past the limit
// and answers HasMore exactly from that probe row, never counting the rest.
// Total is present only when the request asked for it, so a caller that never
// asked cannot misread a page-bounded number as a count, because there is no
// number at all.
type Page struct {
	Offset *uint64 `cbor:"offset,omitempty"`
	// Limit is the effective limit applied after validation against the cap.
	Limit uint32 `cbor:"limit"`
	// Total is an exact match count, present only when requested.
	Total *uint64 `cbor:"total,omitempty"`
	// HasMore is always exact and always free.
	HasMore    bool    `cbor:"has_more"`
	NextCursor *string `cbor:"next_cursor,omitempty"`
}

// AtLeast is the rows known to exist so far: the position one past this page's
// last row, a cheap display lower bound paired with HasMore for a trailing plus.
func (page Page) AtLeast(rowsOnPage int) (uint64, bool) {
	if page.Offset == nil {
		return 0, false
	}
	return *page.Offset + uint64(rowsOnPage), true
}

// TotalPages is the pages implied by an exact total, when one was requested.
// Page math is undefined without a page size, so a zero limit reports false.
func (page Page) TotalPages() (uint64, bool) {
	if page.Total == nil || page.Limit == 0 {
		return 0, false
	}
	return (*page.Total + uint64(page.Limit) - 1) / uint64(page.Limit), true
}

// Validate checks a result page against the shared bounds.
func (page Page) Validate() error {
	if page.Limit == 0 || page.Limit > MaxPageSize {
		return &InvalidError{Reason: fmt.Sprintf(
			"query result page limit exceeds cap %d", MaxPageSize)}
	}
	return nil
}

// QueryEnvelope is the versioned wrapper around a query. Workers and clients
// use it, and application code does not.
type QueryEnvelope struct {
	V     uint32 `cbor:"v"`
	Query Query  `cbor:"query"`
}

// NewQueryEnvelope wraps a query at the current op version.
func NewQueryEnvelope(query Query) QueryEnvelope {
	return QueryEnvelope{V: QueryOpVersion, Query: query}
}

// Validate checks the envelope version.
func (envelope QueryEnvelope) Validate() error {
	if envelope.V != QueryOpVersion {
		return &InvalidError{Reason: fmt.Sprintf(
			"query version must be %d, got %d", QueryOpVersion, envelope.V)}
	}
	return nil
}

// QueryErrorCode is the stable machine-readable classification of a query
// failure.
type QueryErrorCode string

const (
	QueryCodeUnsupported      QueryErrorCode = "unsupported"
	QueryCodeInvalid          QueryErrorCode = "invalid"
	QueryCodeUnauthorized     QueryErrorCode = "unauthorized"
	QueryCodeIndexNotFound    QueryErrorCode = "index_not_found"
	QueryCodeForkNotFound     QueryErrorCode = "fork_not_found"
	QueryCodeSchemaNotFound   QueryErrorCode = "schema_not_found"
	QueryCodeBackend          QueryErrorCode = "backend"
	QueryCodeUnavailable      QueryErrorCode = "unavailable"
	QueryCodeTooLarge         QueryErrorCode = "too_large"
	QueryCodeVersion          QueryErrorCode = "version"
	QueryCodeStale            QueryErrorCode = "stale"
	QueryCodeDeadlineExceeded QueryErrorCode = "deadline_exceeded"
	QueryCodeStaleGeneration  QueryErrorCode = "stale_generation"
)

// ConsistencyGate is how far a projector has applied against where a read
// needs it to be.
//
// It is what turns a consistency level into a decision: a read that needs more
// than eventual consistency is refused with both offsets, so the caller can
// retry while the projector catches up rather than silently reading stale
// data.
//
// It is a decision this port makes locally, never a shape that crosses the
// wire, so it carries no field tags: the contract declares it without a
// serializer.
type ConsistencyGate struct {
	// Applied is the projector's applied offset for the queried source.
	Applied uint64
	// Required is the offset the read must reach before serving, which is the
	// source log head.
	Required uint64
}

// NewConsistencyGate names a projector's progress against a head.
func NewConsistencyGate(applied, required uint64) ConsistencyGate {
	return ConsistencyGate{Applied: applied, Required: required}
}

// IsCaughtUp reports whether the projector has reached the required offset.
func (gate ConsistencyGate) IsCaughtUp() bool { return gate.Applied >= gate.Required }

// Check enforces a consistency level for one named source.
//
// Eventual always passes. Any stronger level passes only once the projector
// has caught up, and otherwise fails with the offsets so the caller can retry.
func (gate ConsistencyGate) Check(level Consistency, what string) error {
	if level.IsEventual() || gate.IsCaughtUp() {
		return nil
	}
	return &QueryError{
		Kind: QueryStale, What: what,
		Applied: gate.Applied, Required: gate.Required,
	}
}

// UnmarshalCBOR reads the value, refusing one outside the closed vocabulary.
func (value *AggFunc) UnmarshalCBOR(item cborform.Value) error {
	return decodeClosedEnum(item, value, "agg func", AggCount, AggCountDistinct, AggSum, AggAvg, AggMin, AggMax, AggPercentile, AggStdDev)
}

// UnmarshalCBOR reads the value, refusing one outside the closed vocabulary.
func (value *QueryErrorCode) UnmarshalCBOR(item cborform.Value) error {
	return decodeClosedEnum(item, value, "query error code", QueryCodeUnsupported, QueryCodeInvalid, QueryCodeUnauthorized, QueryCodeIndexNotFound, QueryCodeForkNotFound, QueryCodeSchemaNotFound, QueryCodeBackend, QueryCodeUnavailable, QueryCodeTooLarge, QueryCodeVersion, QueryCodeStale, QueryCodeDeadlineExceeded, QueryCodeStaleGeneration)
}

// UnmarshalCBOR reads the value, refusing one outside the closed vocabulary.
func (value *SqlDialect) UnmarshalCBOR(item cborform.Value) error {
	return decodeClosedEnum(item, value, "sql dialect", DialectPostgres, DialectMySQL, DialectSQLite)
}
