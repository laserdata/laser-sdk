package laser

import (
	"context"
	"iter"
	"slices"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// QueryRow binds the values returned by a query to the fields of their page.
type QueryRow struct {
	Fields []wire.LogicalField
	wire.Row
}

// QueryRowsFromPage binds every row of one page to the page's fields.
func QueryRowsFromPage(fields []wire.LogicalField, rows []wire.Row) []QueryRow {
	bound := make([]QueryRow, len(rows))
	for index, row := range rows {
		bound[index] = QueryRow{Fields: fields, Row: row}
	}
	return bound
}

// FieldIndex is the position of a column in the page's fields.
func (row QueryRow) FieldIndex(name string) (int, bool) {
	index := slices.IndexFunc(row.Fields, func(field wire.LogicalField) bool { return field.Name == name })
	return max(index, 0), index >= 0
}

// Value looks up a typed value by its column name.
func (row QueryRow) Value(name string) (wire.TypedValue, bool) {
	index, found := row.FieldIndex(name)
	if !found || index >= len(row.Values) {
		return wire.TypedValue{}, false
	}
	return row.Values[index], true
}

// ValueText reads a column as its diagnostic text.
func (row QueryRow) ValueText(name string) (string, bool) {
	value, found := row.Value(name)
	if !found {
		return "", false
	}
	return value.DiagnosticText(), true
}

// ValueI64 reads an int or long column.
func (row QueryRow) ValueI64(name string) (int64, bool) {
	value, _ := row.Value(name)
	return value.AsI64()
}

// ValueU64 reads an int or long column that is not negative.
func (row QueryRow) ValueU64(name string) (uint64, bool) {
	value, _ := row.Value(name)
	return value.AsU64()
}

// Decode decodes a query row's original payload with the selected codec.
func (row QueryRow) Decode(codec Codec, into any) error {
	value, ok := row.Value(wire.OriginalPayloadFieldName)
	if !ok || value.Kind == wire.TypedNull {
		return &CodecError{Reason: "query row has no original payload"}
	}
	if value.Kind != wire.TypedBinary {
		return &CodecError{Reason: "query original payload field is not binary"}
	}
	if codec == nil {
		return &InvalidError{Reason: "a query payload decoder is required"}
	}
	if err := codec.Decode(value.Bytes, into); err != nil {
		return &CodecError{Reason: "decode query row payload", Cause: err}
	}
	return nil
}

// QueryRows reads pages on demand. It is used by one goroutine at a time.
type QueryRows struct {
	request       QueryRequest
	remaining     uint64
	page          *wire.QueryResult
	index         int
	finished      bool
	first         []wire.QueryFrontier
	frontierMoved bool
}

// MaxRows caps the number of rows returned by Rows.
func (request *QueryRequest) MaxRows(n uint64) *QueryRequest {
	request.maxRows = &n
	return request
}

// Rows opens a bounded walk. MaxRows must be set explicitly.
func (request *QueryRequest) Rows() (*QueryRows, error) {
	if request.err != nil {
		return nil, request.err
	}
	if request.maxRows == nil {
		return nil, &InvalidError{Reason: "Rows requires an explicit MaxRows limit"}
	}
	return &QueryRows{request: request.take(), remaining: *request.maxRows}, nil
}

// FrontierMoved reports whether later pages were served at a different position.
func (rows *QueryRows) FrontierMoved() bool { return rows.frontierMoved }

// Next returns nil at the end of the walk.
func (rows *QueryRows) Next(ctx context.Context) (*QueryRow, error) {
	if rows.remaining == 0 {
		return nil, nil
	}
	if rows.page == nil || rows.index == len(rows.page.Rows) {
		if rows.finished {
			return nil, nil
		}
		page, err := rows.request.Fetch(ctx)
		if err != nil {
			return nil, err
		}
		if rows.page == nil {
			rows.first = page.Context.Frontier
		} else if !rows.frontierMoved && !sameQueryFrontier(rows.first, page.Context.Frontier) {
			rows.frontierMoved = true
		}
		rows.page, rows.index = page, 0
		rows.finished = len(page.Rows) == 0 || !page.Page.HasMore || len(rows.request.aggs) > 0 || len(rows.request.groupBy) > 0 || rows.request.window != nil || rows.request.query.Vector != nil
		if len(page.Rows) == 0 {
			return nil, nil
		}
		next := uint64(len(page.Rows))
		if previous := rows.request.query.Page.Offset; previous != nil {
			next += *previous
		}
		rows.request.Offset(next)
	}
	row := &QueryRow{Fields: rows.page.Fields, Row: rows.page.Rows[rows.index]}
	rows.index++
	rows.remaining--
	return row, nil
}

// All iterates the remaining rows, returning the first error to the caller.
func (rows *QueryRows) All(ctx context.Context) iter.Seq2[*QueryRow, error] {
	return func(yield func(*QueryRow, error) bool) {
		for {
			row, err := rows.Next(ctx)
			if err != nil {
				yield(nil, err)
				return
			}
			if row == nil || !yield(row, nil) {
				return
			}
		}
	}
}

// QueryRowSet contains every row and the consistency observation for the walk.
type QueryRowSet struct {
	Rows          []QueryRow
	FrontierMoved bool
}

// FetchAll explicitly collects all matching rows across pages.
func (request *QueryRequest) FetchAll(ctx context.Context) (*QueryRowSet, error) {
	rows := QueryRows{request: request.take(), remaining: ^uint64(0)}
	result := &QueryRowSet{}
	for row, err := range rows.All(ctx) {
		if err != nil {
			return nil, err
		}
		result.Rows = append(result.Rows, *row)
	}
	result.FrontierMoved = rows.FrontierMoved()
	return result, nil
}

func sameQueryFrontier(a, b []wire.QueryFrontier) bool {
	if len(a) != len(b) {
		return false
	}
	equal := func(x, y wire.QueryFrontier) bool {
		g, h := x.TopicCreatedAtMicros, y.TopicCreatedAtMicros
		x.TopicCreatedAtMicros, y.TopicCreatedAtMicros = nil, nil
		return x == y && (g == h || (g != nil && h != nil && *g == *h))
	}
	if slices.EqualFunc(a, b, equal) {
		return true
	}
	for _, position := range b {
		if !slices.ContainsFunc(a, func(other wire.QueryFrontier) bool { return equal(position, other) }) {
			return false
		}
	}
	return true
}

// FetchOne decodes one matching payload into the supplied destination. A missing row returns false.
func (request *QueryRequest) FetchOne(ctx context.Context, codec Codec, into any) (bool, error) {
	page := request.take()
	result, err := page.WithPayload().Limit(1).Fetch(ctx)
	if err != nil || len(result.Rows) == 0 {
		return false, err
	}
	row := QueryRow{Fields: result.Fields, Row: result.Rows[0]}
	if err := row.Decode(codec, into); err != nil {
		return false, err
	}
	return true, nil
}

// TypedQueryRequest reads query results as values of T using their original payloads.
type TypedQueryRequest[T any] struct {
	request QueryRequest
	codec   Codec
}

// TypedQuery selects the decoder for the original payload column. Use JSONCodec for JSON records.
func TypedQuery[T any](request *QueryRequest, codec Codec) *TypedQueryRequest[T] {
	typed := request.take()
	typed.WithPayload()
	if codec == nil {
		typed.fail(&InvalidError{Reason: "a query payload decoder is required"})
	}
	return &TypedQueryRequest[T]{request: typed, codec: codec}
}

// Fetch reads and decodes a single page.
func (query *TypedQueryRequest[T]) Fetch(ctx context.Context) ([]T, error) {
	page, err := query.request.Fetch(ctx)
	if err != nil {
		return nil, err
	}
	values := make([]T, len(page.Rows))
	for i, record := range page.Rows {
		row := QueryRow{Fields: page.Fields, Row: record}
		if err := row.Decode(query.codec, &values[i]); err != nil {
			return nil, err
		}
	}
	return values, nil
}

// FetchOne decodes the first matching row. A missing row returns nil.
func (query *TypedQueryRequest[T]) FetchOne(ctx context.Context) (*T, error) {
	var value T
	ok, err := query.request.FetchOne(ctx, query.codec, &value)
	if err != nil || !ok {
		return nil, err
	}
	return &value, nil
}

// TypedQueryRows decodes a bounded query walk one row at a time.
type TypedQueryRows[T any] struct {
	rows  *QueryRows
	codec Codec
}

// Rows opens a walk bounded by the underlying query's MaxRows.
func (query *TypedQueryRequest[T]) Rows() (*TypedQueryRows[T], error) {
	rows, err := query.request.Rows()
	if err != nil {
		return nil, err
	}
	return &TypedQueryRows[T]{rows: rows, codec: query.codec}, nil
}

// FrontierMoved reports whether the data changed between pages.
func (rows *TypedQueryRows[T]) FrontierMoved() bool { return rows.rows.FrontierMoved() }

// Next returns a decoded payload, or nil when the walk is complete.
func (rows *TypedQueryRows[T]) Next(ctx context.Context) (*T, error) {
	row, err := rows.rows.Next(ctx)
	if err != nil || row == nil {
		return nil, err
	}
	var value T
	if err := row.Decode(rows.codec, &value); err != nil {
		return nil, err
	}
	return &value, nil
}

// FetchAll decodes all matching rows across pages.
func (query *TypedQueryRequest[T]) FetchAll(ctx context.Context) ([]T, error) {
	unbounded := query.request.take()
	rows, err := unbounded.MaxRows(^uint64(0)).Rows()
	if err != nil {
		return nil, err
	}
	decoded := TypedQueryRows[T]{rows: rows, codec: query.codec}
	values := []T{}
	for {
		value, err := decoded.Next(ctx)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return values, nil
		}
		values = append(values, *value)
	}
}
