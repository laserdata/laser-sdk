package laser

import (
	"context"
	"fmt"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// DefaultQueryDeadline bounds a query that names none and is the longest one
// may ask for. The deadline is absolute on the wire, and a server never
// extends it while queueing or paging, so it bounds the whole execution rather
// than one hop.
const DefaultQueryDeadline = QueryDeadline

// QueryRequest builds a query against one materialized index. Nothing is sent
// until Fetch.
//
// A builder error is carried rather than returned at each step, so a chain
// reads as one expression and still reports the first thing that was wrong.
type QueryRequest struct {
	laser            *Laser
	query            wire.Query
	deadline         time.Duration
	absoluteDeadline uint64
	maxRows          *uint64
	window           *wire.Window
	filters          []wire.Filter
	having           []wire.Filter
	aggs             []wire.AggCall
	groupBy          []string
	err              error
}

// Query begins a query against a materialized index, named as this handle
// scopes resources.
func (laser *Laser) Query(index string) *QueryRequest {
	return laser.QueryTarget(wire.OperationalTarget(index))
}

// QueryTarget begins a query against an explicit target. An operational index
// is sent as this handle names it.
func (laser *Laser) QueryTarget(target wire.QueryTarget) *QueryRequest {
	if target.Kind == wire.TargetOperational {
		target = wire.OperationalTarget(laser.ResourceName(target.Index))
	}
	return &QueryRequest{
		laser:    laser,
		query:    wire.NewQuery(wire.QueryExecutionID{ID: wire.IDFromBytes(NewOperationID())}, target, 0),
		deadline: DefaultQueryDeadline,
	}
}

func (request *QueryRequest) fail(err error) *QueryRequest {
	if request.err == nil {
		request.err = err
	}
	return request
}

// Deadline bounds each call and each page of a walk, from the moment it is
// sent. One past QueryDeadline fails the call before anything is sent.
func (request *QueryRequest) Deadline(deadline time.Duration) *QueryRequest {
	if deadline <= 0 {
		return request.fail(&InvalidError{Reason: "a query deadline must be positive"})
	}
	request.deadline = deadline
	request.absoluteDeadline = 0
	return request
}

// DeadlineAtMicros fixes one absolute deadline, in epoch microseconds, for
// every later call and page.
func (request *QueryRequest) DeadlineAtMicros(micros uint64) *QueryRequest {
	if micros == 0 {
		return request.fail(&InvalidError{Reason: "query deadline must be nonzero"})
	}
	request.absoluteDeadline = micros
	return request
}

// WhereEq adds an exact-match constraint on an indexed field.
func (request *QueryRequest) WhereEq(field string, value wire.TypedValue) *QueryRequest {
	request.query.ByKey = append(request.query.ByKey, wire.KeyMatch{Field: field, Value: value})
	return request
}

// Conversation selects rows produced by the specified conversation.
func (request *QueryRequest) Conversation(id wire.ConversationID) *QueryRequest {
	return request.WhereEq(wire.ConversationField, wire.StringTyped(id.String()))
}

// Predicate adds a structured predicate tree to the query.
func (request *QueryRequest) Predicate(filter wire.Filter) *QueryRequest {
	request.filters = append(request.filters, filter)
	return request
}

// Nearest searches the default embedding field.
func (request *QueryRequest) Nearest(embedding []float32, topK uint32) *QueryRequest {
	return request.NearestIn(wire.VectorField, embedding, topK)
}

// NearestIn selects the nearest vectors in field.
func (request *QueryRequest) NearestIn(field string, embedding []float32, topK uint32) *QueryRequest {
	request.query.Vector = &wire.VectorQuery{Field: field, Embedding: embedding, TopK: topK}
	return request
}

// Window groups an aggregate by intervals of the given timestamp field.
func (request *QueryRequest) Window(field string, every time.Duration) *QueryRequest {
	if every.Microseconds() <= 0 {
		return request.fail(&InvalidError{Reason: "a query window must be at least one microsecond"})
	}
	request.window = &wire.Window{Field: field, EveryMicros: uint64(every.Microseconds())}
	return request
}

// Where adds an equality predicate, the common case.
func (request *QueryRequest) Where(field string, value wire.TypedValue) *QueryRequest {
	return request.Filter(field, wire.CmpEq, value)
}

// Filter adds one comparison. Every predicate added composes with AND, which
// is what a reader expects from a chain of narrowing conditions. Use AnyOf for
// a disjunction.
func (request *QueryRequest) Filter(
	field string, op wire.CmpOp, value wire.TypedValue,
) *QueryRequest {
	if !op.IsKnown() {
		return request.fail(&InvalidError{Reason: "unknown comparison operator " + string(op)})
	}
	request.filters = append(request.filters, wire.Pred(field, op, value))
	return request
}

// AnyOf adds a disjunction, which composes with AND against the rest.
func (request *QueryRequest) AnyOf(filters ...wire.Filter) *QueryRequest {
	request.filters = append(request.filters, wire.AnyOf(filters...))
	return request
}

// MessageType narrows to records of one declared type.
func (request *QueryRequest) MessageType(value string) *QueryRequest {
	request.query.MessageType = &value
	return request
}

// TimeRange bounds the query to an inclusive instant range, in epoch
// microseconds.
func (request *QueryRequest) TimeRange(start, end uint64) *QueryRequest {
	if end < start {
		return request.fail(&InvalidError{Reason: "a query time range must not end before it starts"})
	}
	request.query.TimeRange = &wire.TimeRange{Start: start, End: end}
	return request
}

// Fork resolves the query against a fork's copy-on-write view instead of the
// trunk.
func (request *QueryRequest) Fork(forkID string) *QueryRequest {
	scoped := request.laser.ResourceName(forkID)
	request.query.Fork = &scoped
	return request
}

// OrderAsc sorts ascending by field, after any earlier sort key.
func (request *QueryRequest) OrderAsc(field string) *QueryRequest {
	request.query.Order = append(request.query.Order, wire.Sort{Field: field, Dir: wire.DirAsc})
	return request
}

// OrderDesc sorts descending by field, after any earlier sort key.
func (request *QueryRequest) OrderDesc(field string) *QueryRequest {
	request.query.Order = append(request.query.Order, wire.Sort{Field: field, Dir: wire.DirDesc})
	return request
}

// Limit bounds the rows one page returns.
func (request *QueryRequest) Limit(limit uint32) *QueryRequest {
	request.query.Page.Limit = limit
	return request
}

// Offset skips rows before the page.
func (request *QueryRequest) Offset(offset uint64) *QueryRequest {
	request.query.Page.Offset = &offset
	return request
}

// WithTotal asks for an exact match count. It runs a real count over the
// filter, which is unbounded work on a large index, so a page-bounded caller
// should leave it off.
func (request *QueryRequest) WithTotal() *QueryRequest {
	request.query.Page.WantTotal = true
	return request
}

// WithPayload returns the opaque payload bytes alongside the indexed fields.
func (request *QueryRequest) WithPayload() *QueryRequest {
	request.query.Select.Payload = true
	return request
}

// SelectFields narrows the returned columns. Naming none returns every indexed
// field.
func (request *QueryRequest) SelectFields(fields ...string) *QueryRequest {
	request.query.Select.Fields = fields
	return request
}

// Distinct removes duplicate rows.
func (request *QueryRequest) Distinct() *QueryRequest {
	request.query.Distinct = true
	return request
}

// Consistency asks for a read-consistency level.
//
// A level the deployment has not advertised is refused before sending, because
// the field is additive: a backend that does not implement it would drop it
// and serve an eventual read that looks successful. Failing is the only way
// not to silently downgrade.
func (request *QueryRequest) Consistency(level wire.Consistency) *QueryRequest {
	request.query.Consistency = level
	return request
}

// ReadYourWrites asks for a read that sees this client's own writes.
func (request *QueryRequest) ReadYourWrites() *QueryRequest {
	return request.Consistency(wire.ConsistencyReadYourWrites)
}

// Text adds a lexical relevance search over every indexed text field.
//
// It is additive like Consistency, so an unadvertised search is refused before
// sending rather than dropped by an unaware backend, which would answer the
// unfiltered query: a wider answer than was asked for, reported as success.
func (request *QueryRequest) Text(query string) *QueryRequest {
	request.query.Text = &wire.TextQuery{Query: query}
	return request
}

// TextIn is Text narrowed to one field.
func (request *QueryRequest) TextIn(field, query string) *QueryRequest {
	request.query.Text = &wire.TextQuery{Field: &field, Query: query}
	return request
}

// GroupBy groups an aggregation by these fields.
func (request *QueryRequest) GroupBy(fields ...string) *QueryRequest {
	request.groupBy = fields
	return request
}

// Count adds a row count, aliased as "count".
func (request *QueryRequest) Count() *QueryRequest {
	return request.AggAs(wire.AggCount, "", "count")
}

// Sum, Avg, Min and Max add the named aggregate over field, aliased after the
// function.
func (request *QueryRequest) Sum(field string) *QueryRequest {
	return request.AggAs(wire.AggSum, field, "sum")
}

func (request *QueryRequest) Avg(field string) *QueryRequest {
	return request.AggAs(wire.AggAvg, field, "avg")
}

func (request *QueryRequest) Min(field string) *QueryRequest {
	return request.AggAs(wire.AggMin, field, "min")
}

func (request *QueryRequest) Max(field string) *QueryRequest {
	return request.AggAs(wire.AggMax, field, "max")
}

// CountDistinct counts distinct values of field.
func (request *QueryRequest) CountDistinct(field string) *QueryRequest {
	return request.AggAs(wire.AggCountDistinct, field, "count_distinct")
}

// StdDev adds the standard deviation of field.
func (request *QueryRequest) StdDev(field string) *QueryRequest {
	return request.AggAs(wire.AggStdDev, field, "stddev")
}

// Percentile adds the value at fraction of field, where fraction runs from 0
// to 1 inclusive.
func (request *QueryRequest) Percentile(field string, fraction float64) *QueryRequest {
	if fraction < 0 || fraction > 1 {
		return request.fail(&InvalidError{
			Reason: "a percentile fraction must run from 0 to 1"})
	}
	call := wire.AggCall{Func: wire.AggPercentile, Field: &field, Arg: &fraction,
		Alias: "percentile"}
	request.aggs = append(request.aggs, call)
	return request
}

// AggAs adds one aggregate under a caller-chosen alias, which is how a query
// carries two aggregates of the same function.
func (request *QueryRequest) AggAs(
	function wire.AggFunc, field, alias string,
) *QueryRequest {
	call := wire.AggCall{Func: function, Alias: alias}
	if field != "" {
		call.Field = &field
	}
	request.aggs = append(request.aggs, call)
	return request
}

// Having filters aggregate output by an alias or a group key. It is meaningful
// only alongside an aggregate.
func (request *QueryRequest) Having(
	alias string, op wire.CmpOp, value wire.TypedValue,
) *QueryRequest {
	if !op.IsKnown() {
		return request.fail(&InvalidError{Reason: "unknown comparison operator " + string(op)})
	}
	request.having = append(request.having, wire.Pred(alias, op, value))
	return request
}

// RawSQL sends a statement in one dialect, scoped to the query's target. The
// server validates both the parsed statement and the resulting scan plan
// against that target.
func (request *QueryRequest) RawSQL(
	dialect wire.SqlDialect, sql string, params ...wire.TypedValue,
) *QueryRequest {
	request.query.RawSQL = &wire.RawSql{Dialect: dialect, SQL: sql, Params: params}
	return request
}

// build finishes the query, folding the collected predicates and aggregates in
// and stamping the execution boundary.
func (request *QueryRequest) build() (wire.Query, error) {
	if request.err != nil {
		return wire.Query{}, request.err
	}
	query := request.query
	// The deadline is absolute on the wire, so it is resolved at send time
	// rather than when the builder was created: a request held before sending
	// would otherwise arrive already expired.
	query.DeadlineMicros = request.absoluteDeadline
	if query.DeadlineMicros == 0 {
		query.DeadlineMicros = uint64(time.Now().Add(request.deadline).UnixMicro())
	}
	switch len(request.filters) {
	case 0:
	case 1:
		query.Filter = &request.filters[0]
	default:
		combined := wire.AllOf(request.filters...)
		query.Filter = &combined
	}
	if len(request.aggs) > 0 || len(request.groupBy) > 0 || request.window != nil {
		query.Aggregate = &wire.Aggregate{GroupBy: request.groupBy, Funcs: request.aggs, Window: request.window}
	}
	switch len(request.having) {
	case 0:
	case 1:
		query.Having = &request.having[0]
	default:
		combined := wire.AllOf(request.having...)
		query.Having = &combined
	}
	if err := query.Validate(); err != nil {
		return wire.Query{}, &InvalidError{Reason: err.Error()}
	}
	return query, nil
}

// ExecutionID is the identity the next request sends and its reply echoes
// back. Every call and every page of a walk takes a fresh one.
func (request *QueryRequest) ExecutionID() wire.QueryExecutionID { return request.query.ExecutionID }

// rotate moves the builder to a fresh identity for its next call.
func (request *QueryRequest) rotate() {
	request.query.ExecutionID = wire.QueryExecutionID{ID: wire.IDFromBytes(NewOperationID())}
}

// take hands out the builder as it stands for one call or walk, and moves the
// builder itself to a fresh identity.
func (request *QueryRequest) take() QueryRequest {
	taken := *request
	request.rotate()
	return taken
}

// IntoQuery builds and validates the wire request without sending it.
func (request *QueryRequest) IntoQuery() (wire.Query, error) { return request.build() }

// Fetch runs the query and returns one page of rows.
func (request *QueryRequest) Fetch(ctx context.Context) (*wire.QueryResult, error) {
	query, err := request.build()
	if err != nil {
		return nil, err
	}
	request.rotate()
	return request.laser.ExecuteQuery(ctx, query)
}

// ExecuteQuery runs a query that was built directly. Most callers want the
// builder instead.
//
// The reply is awaited QueryReplyGrace past the query's deadline and no
// longer. A wait that runs out does not prove the query stopped.
func (laser *Laser) ExecuteQuery(
	ctx context.Context, query wire.Query,
) (*wire.QueryResult, error) {
	if err := query.Validate(); err != nil {
		return nil, &InvalidError{Reason: err.Error()}
	}
	if err := laser.requireQuery(query); err != nil {
		return nil, err
	}
	remaining := time.Duration(0)
	if now := uint64(time.Now().UnixMicro()); query.DeadlineMicros > now {
		if query.DeadlineMicros-now > uint64(QueryDeadline.Microseconds()) {
			return nil, &InvalidError{Reason: fmt.Sprintf(
				"the query deadline is %ds away, the longest allowed is %ds",
				(query.DeadlineMicros-now)/1_000_000, int(QueryDeadline.Seconds()))}
		}
		remaining = time.Duration(query.DeadlineMicros-now) * time.Microsecond
	}
	payload, err := wire.Marshal(wire.NewQueryEnvelope(query))
	if err != nil {
		return nil, &CodecError{Reason: "encode query", Cause: err}
	}
	// A query every node refused walks the roster again until the query's own
	// deadline, so the last refusal comes back before the reply wait ends.
	deadline := time.Now().Add(remaining)
	bounded, cancel := context.WithDeadline(ctx, deadline.Add(QueryReplyGrace))
	defer cancel()
	raw, err := laser.sendManagedWalking(bounded, wire.AgdxQueryCode, payload, walkingUntil(deadline))
	if err != nil {
		if ctx.Err() == nil && bounded.Err() != nil {
			return nil, &StageTimeoutError{Stage: "the query reply by the query deadline"}
		}
		return nil, err
	}
	var reply wire.QueryReply
	if err := decodeManagedReply(raw, &reply, "decode query reply"); err != nil {
		return nil, err
	}
	if !reply.IsOk() {
		if reply.Err == nil {
			return nil, &CodecError{Reason: "query reply carried neither a result nor an error"}
		}
		return nil, &QueryError{Failure: *reply.Err}
	}
	// A row of the wrong width would silently misalign values against field
	// names, which reads as data rather than as a fault.
	if err := reply.Result.Validate(); err != nil {
		return nil, &CodecError{Reason: "query result: " + err.Error()}
	}
	if reply.Result.Context.ExecutionID != query.ExecutionID {
		return nil, &ProtocolError{Reason: "query reply execution id does not match the request"}
	}
	return reply.Result, nil
}

// requireQuery refuses a query this deployment cannot answer as asked.
//
// Three of these are fail-not-downgrade checks on additive fields. A backend
// that does not implement one drops it and answers the query without it, which
// looks like success while being a wider or staler answer than was asked for.
// Refusing locally is the only way to tell the caller.
func (laser *Laser) requireQuery(query wire.Query) error {
	if !laser.Capabilities().Managed || !laser.Capabilities().Query.Available {
		return &UnsupportedError{Surface: "the query surface"}
	}
	if err := laser.requireOpVersion("query", wire.QueryOpVersion,
		func(versions wire.OpVersions) uint32 { return versions.Query }); err != nil {
		return err
	}
	if !laser.Capabilities().ServesConsistency(query.Consistency) {
		return &UnsupportedError{
			Surface: "consistency level " + string(query.Consistency) + ", which"}
	}
	if query.Text != nil && !laser.Capabilities().Query.Keyword {
		return &UnsupportedError{Surface: "lexical relevance search, which"}
	}
	if query.RawSQL != nil && !laser.servesDialect(query.RawSQL.Dialect) {
		return &UnsupportedError{
			Surface: "raw SQL in the " + string(query.RawSQL.Dialect) + " dialect, which"}
	}
	return nil
}

// servesDialect reports whether an enabled backend announced raw SQL in this
// dialect.
func (laser *Laser) servesDialect(dialect wire.SqlDialect) bool {
	for _, backend := range laser.Capabilities().Backends {
		if backend.DesiredState != wire.BackendEnabledState || backend.Query == nil || !backend.Query.RawSQL {
			continue
		}
		for _, served := range backend.Query.Dialects {
			if served == dialect {
				return true
			}
		}
	}
	return false
}

// QueryError reports a query the server refused, carrying the contract's own
// classification so a caller can tell a retryable condition from a fault.
type QueryError struct{ Failure wire.QueryError }

func (e *QueryError) Error() string { return "query: " + e.Failure.Error() }

// Kind is the contract's classification of the failure.
func (e *QueryError) Kind() wire.QueryErrorKind { return e.Failure.Kind }
