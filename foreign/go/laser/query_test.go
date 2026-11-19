package laser_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

type queryServer struct {
	clusterOfOne
	reply    wire.QueryReply
	codes    []uint32
	requests [][]byte
}

func (server *queryServer) Close() error { return nil }

func (server *queryServer) SendBinaryRequest(
	_ context.Context, code uint32, payload []byte,
) ([]byte, error) {
	server.codes = append(server.codes, code)
	server.requests = append(server.requests, payload)
	return wire.Marshal(server.reply)
}

// resultContext is what a server stamps on every reply: which index actually
// answered, and at which backend generation.
func resultContext() wire.QueryContext {
	return wire.QueryContext{
		Engine: wire.QueryEngine{Name: "embedded", Version: "1.0.0"},
		ResolvedTarget: wire.NewResolvedOperationalTarget("events",
			wire.BackendResourceID{ID: wire.IDFromBytes([16]byte{15: 1})}, 1, 1),
		RequestedConsistency: wire.ConsistencyEventual,
		DeliveredConsistency: wire.ConsistencyEventual,
	}
}

// okResult is the smallest well-formed result: a query reply must carry a
// schema, and every row has to match its width.
func okResult(rows ...wire.Row) wire.QueryReply {
	return wire.OkQueryReply(wire.QueryResult{
		Fields:  []wire.LogicalField{{Name: "id", FieldType: wire.LogicalType{Kind: wire.KindLong}}},
		Rows:    rows,
		Page:    wire.Page{Limit: 50},
		Context: resultContext(),
	})
}

func queryCapabilities() *laser.Capabilities {
	versions := wire.NewOpVersions(
		wire.QueryOpVersion, wire.ControlOpVersion, wire.KvOpVersion, wire.ForkOpVersion)
	capabilities := laser.OpenCapabilities()
	capabilities.Managed = true
	capabilities.Hello = laser.HelloAnswered
	capabilities.Query = laser.QueryCaps{Available: true, Consistency: wire.ConsistencyEventual}
	capabilities.Versions = &versions
	return &capabilities
}

func queryHandle(server iggcon.Client, options laser.Options) *laser.Laser {
	if options.Capabilities == nil {
		options.Capabilities = queryCapabilities()
	}
	return laser.FromClient(server, options)
}

func sentQuery(t *testing.T, server *queryServer, index int) wire.Query {
	t.Helper()
	var envelope wire.QueryEnvelope
	if err := wire.Unmarshal(server.requests[index], &envelope); err != nil {
		t.Fatalf("decode query %d: %v", index, err)
	}
	return envelope.Query
}

// A chain of narrowing conditions is what a reader expects to compose with
// AND, so several predicates must arrive as one conjunction rather than the
// last one winning.
func TestGivenSeveralPredicatesWhenBuiltThenShouldComposeWithAnd(t *testing.T) {
	server := &queryServer{reply: okResult()}

	if _, err := queryHandle(server, laser.Options{}).Query("events").
		Where("kind", wire.StringTyped("click")).
		Filter("count", wire.CmpGte, wire.LongTyped(5)).
		Fetch(context.Background()); err != nil {
		t.Fatalf("fetch: %v", err)
	}

	query := sentQuery(t, server, 0)
	if query.Filter == nil || query.Filter.Kind != wire.FilterAll {
		t.Fatalf("want a conjunction, got %+v", query.Filter)
	}
	if len(query.Filter.Children) != 2 {
		t.Errorf("want both predicates, got %d", len(query.Filter.Children))
	}
}

// One predicate is sent as the leaf itself, not wrapped in a conjunction of
// one, so the common query stays the shape the server reads most often.
func TestGivenOnePredicateWhenBuiltThenShouldSendTheLeafAlone(t *testing.T) {
	server := &queryServer{reply: okResult()}

	if _, err := queryHandle(server, laser.Options{}).Query("events").
		Where("kind", wire.StringTyped("click")).
		Fetch(context.Background()); err != nil {
		t.Fatalf("fetch: %v", err)
	}

	query := sentQuery(t, server, 0)
	if query.Filter == nil || query.Filter.Kind != wire.FilterPred {
		t.Fatalf("want a bare predicate, got %+v", query.Filter)
	}
}

func TestGivenADefaultStreamWhenQueryingThenShouldScopeTheIndex(t *testing.T) {
	server := &queryServer{reply: okResult()}

	if _, err := queryHandle(server, laser.Options{DefaultStream: "agents"}).Query("events").
		Fetch(context.Background()); err != nil {
		t.Fatalf("fetch: %v", err)
	}

	query := sentQuery(t, server, 0)
	if query.Target.Index != "stream:agents/events" {
		t.Errorf("want the scoped index, got %q", query.Target.Index)
	}
	if server.codes[0] != wire.AgdxQueryCode {
		t.Errorf("want the query code, got %d", server.codes[0])
	}
}

// The deadline is absolute on the wire, so it has to be resolved when the
// request is sent. Resolving it when the builder was created would let a
// request held for a while arrive already expired.
func TestGivenADeadlineWhenSentThenShouldBeResolvedAtSendTime(t *testing.T) {
	server := &queryServer{reply: okResult()}
	request := queryHandle(server, laser.Options{}).Query("events").Deadline(time.Minute)

	before := time.Now()
	if _, err := request.Fetch(context.Background()); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	after := time.Now()

	query := sentQuery(t, server, 0)
	deadline := time.UnixMicro(int64(query.DeadlineMicros))
	// The window has to exclude "now": a deadline that was not offset at all
	// still sits between the two timestamps, so only a lower bound a whole
	// minute out separates the two implementations.
	if deadline.Before(before.Add(time.Minute)) {
		t.Errorf("want a deadline at least a minute out, got %v past %v", deadline, before)
	}
	if deadline.After(after.Add(time.Minute)) {
		t.Errorf("want a deadline no more than a minute out, got %v past %v", deadline, after)
	}
}

// Every execution needs its own identity, or a server keeping an outcome under
// it would answer a second query with the first one's result.
func TestGivenTwoQueriesWhenSentThenShouldCarryDistinctExecutionIdentities(t *testing.T) {
	server := &queryServer{reply: okResult()}
	handle := queryHandle(server, laser.Options{})

	for range 2 {
		if _, err := handle.Query("events").Fetch(context.Background()); err != nil {
			t.Fatalf("fetch: %v", err)
		}
	}

	first, second := sentQuery(t, server, 0), sentQuery(t, server, 1)
	if first.ExecutionID.IsZero() {
		t.Error("a query must name its execution")
	}
	if first.ExecutionID == second.ExecutionID {
		t.Error("two executions must not share an identity")
	}
}

// Consistency, text search and raw SQL are additive fields. A backend that
// does not implement one drops it and answers without it, which looks like
// success while being a wider or staler answer than was asked for.
func TestGivenAnUnadvertisedAdditiveFieldWhenSentThenShouldRefuseRatherThanDowngrade(t *testing.T) {
	cases := map[string]func(*laser.QueryRequest) *laser.QueryRequest{
		"read-your-writes": func(r *laser.QueryRequest) *laser.QueryRequest {
			return r.ReadYourWrites()
		},
		"lexical search": func(r *laser.QueryRequest) *laser.QueryRequest {
			return r.Text("hello")
		},
		"raw SQL": func(r *laser.QueryRequest) *laser.QueryRequest {
			return r.RawSQL(wire.DialectPostgres, "select 1")
		},
	}
	for name, narrow := range cases {
		t.Run(name, func(t *testing.T) {
			server := &queryServer{reply: okResult()}
			_, err := narrow(queryHandle(server, laser.Options{}).Query("events")).
				Fetch(context.Background())

			var unsupported *laser.UnsupportedError
			if !errors.As(err, &unsupported) {
				t.Fatalf("want an UnsupportedError, got %T: %v", err, err)
			}
			if len(server.codes) != 0 {
				t.Error("an unserved field must not reach the socket")
			}
		})
	}
}

// Each of those is served once the deployment announces it.
func TestGivenAnAdvertisedAdditiveFieldWhenSentThenShouldCarryIt(t *testing.T) {
	capabilities := queryCapabilities()
	capabilities.Query.Keyword = true
	capabilities.Query.Consistency = wire.ConsistencyReadYourWrites
	server := &queryServer{reply: okResult()}

	if _, err := queryHandle(server, laser.Options{Capabilities: capabilities}).
		Query("events").ReadYourWrites().TextIn("body", "hello").
		Fetch(context.Background()); err != nil {
		t.Fatalf("fetch: %v", err)
	}

	query := sentQuery(t, server, 0)
	if query.Consistency != wire.ConsistencyReadYourWrites {
		t.Errorf("want the level carried, got %q", query.Consistency)
	}
	if query.Text == nil || query.Text.Field == nil || *query.Text.Field != "body" {
		t.Errorf("want the field search carried, got %+v", query.Text)
	}
}

// Raw SQL needs a ready backend that announced both the escape hatch and the
// dialect, because the server validates the statement against the target.
func TestGivenRawSQLWhenADialectIsAnnouncedThenShouldSendIt(t *testing.T) {
	capabilities := queryCapabilities()
	backend := wire.NewBackendDescriptor(
		wire.BackendResourceID{ID: wire.IDFromBytes([16]byte{15: 1})},
		wire.BackendOperational, "Embedded",
		wire.BackendImplementation{Kind: "embedded", Version: "1.0.0"}, 1, 1)
	backend.Readiness = wire.BackendReadiness{Ready: true, ObservedAtMicros: 1}
	backend.Query = &wire.QueryCapabilities{
		Dialects:     []wire.SqlDialect{wire.DialectPostgres},
		Consistency:  []wire.Consistency{wire.ConsistencyEventual},
		LogicalTypes: []wire.LogicalTypeKind{wire.KindLong},
		RawSQL:       true,
	}
	capabilities.Backends = []wire.BackendDescriptor{backend}
	server := &queryServer{reply: okResult()}

	if _, err := queryHandle(server, laser.Options{Capabilities: capabilities}).
		Query("events").RawSQL(wire.DialectPostgres, "select 1").
		Fetch(context.Background()); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	query := sentQuery(t, server, 0)
	if query.RawSQL == nil || query.RawSQL.SQL != "select 1" {
		t.Errorf("want the statement carried, got %+v", query.RawSQL)
	}

	// A dialect the backend did not announce is still refused.
	other := &queryServer{reply: okResult()}
	_, err := queryHandle(other, laser.Options{Capabilities: capabilities}).
		Query("events").RawSQL(wire.DialectSQLite, "select 1").Fetch(context.Background())
	if err == nil || len(other.codes) != 0 {
		t.Errorf("an unannounced dialect must be refused, got %v", err)
	}
}

func TestGivenAnAggregationWhenBuiltThenShouldCarryItsCallsAndGrouping(t *testing.T) {
	server := &queryServer{reply: okResult()}

	if _, err := queryHandle(server, laser.Options{}).Query("events").
		GroupBy("kind").Count().Sum("amount").
		Having("count", wire.CmpGt, wire.LongTyped(10)).
		Fetch(context.Background()); err != nil {
		t.Fatalf("fetch: %v", err)
	}

	query := sentQuery(t, server, 0)
	if query.Aggregate == nil || len(query.Aggregate.Funcs) != 2 {
		t.Fatalf("want two aggregate calls, got %+v", query.Aggregate)
	}
	if len(query.Aggregate.GroupBy) != 1 || query.Aggregate.GroupBy[0] != "kind" {
		t.Errorf("want the grouping carried, got %+v", query.Aggregate.GroupBy)
	}
	if query.Having == nil {
		t.Error("want the having filter carried")
	}
}

// Two aggregates of one function need distinct aliases, which is what the
// explicit form is for.
func TestGivenTwoAggregatesOfOneFunctionWhenAliasedThenShouldCarryBoth(t *testing.T) {
	server := &queryServer{reply: okResult()}

	if _, err := queryHandle(server, laser.Options{}).Query("events").
		AggAs(wire.AggMax, "latency", "worst").
		AggAs(wire.AggMax, "amount", "largest").
		Fetch(context.Background()); err != nil {
		t.Fatalf("fetch: %v", err)
	}

	query := sentQuery(t, server, 0)
	aliases := map[string]bool{}
	for _, call := range query.Aggregate.Funcs {
		aliases[call.Alias] = true
	}
	if !aliases["worst"] || !aliases["largest"] {
		t.Errorf("want both aliases, got %+v", aliases)
	}
}

func TestGivenAnInvalidBuilderStepWhenFetchedThenShouldRefuseBeforeSending(t *testing.T) {
	cases := map[string]func(*laser.QueryRequest) *laser.QueryRequest{
		"a deadline that is not positive": func(r *laser.QueryRequest) *laser.QueryRequest {
			return r.Deadline(0)
		},
		"a range that ends before it starts": func(r *laser.QueryRequest) *laser.QueryRequest {
			return r.TimeRange(100, 50)
		},
		"a percentile outside its range": func(r *laser.QueryRequest) *laser.QueryRequest {
			return r.Percentile("latency", 1.5)
		},
		"an unknown operator": func(r *laser.QueryRequest) *laser.QueryRequest {
			return r.Filter("kind", wire.CmpOp("approximately"), wire.LongTyped(1))
		},
	}
	for name, broken := range cases {
		t.Run(name, func(t *testing.T) {
			server := &queryServer{reply: okResult()}
			_, err := broken(queryHandle(server, laser.Options{}).Query("events")).
				Fetch(context.Background())

			var invalid *laser.InvalidError
			if !errors.As(err, &invalid) {
				t.Fatalf("want an InvalidError, got %T: %v", err, err)
			}
			if len(server.codes) != 0 {
				t.Error("a refused query must not reach the socket")
			}
		})
	}
}

// A row of the wrong width would silently misalign values against field names,
// which reads as data rather than as a fault.
func TestGivenAResultWhoseRowsDoNotMatchTheSchemaWhenReadThenShouldRefuseIt(t *testing.T) {
	server := &queryServer{reply: wire.OkQueryReply(wire.QueryResult{
		Fields:  []wire.LogicalField{{Name: "id", FieldType: wire.LogicalType{Kind: wire.KindLong}}},
		Rows:    []wire.Row{{Values: []wire.TypedValue{wire.LongTyped(1), wire.LongTyped(2)}}},
		Page:    wire.Page{Limit: 50},
		Context: resultContext(),
	})}

	_, err := queryHandle(server, laser.Options{}).Query("events").Fetch(context.Background())

	var codec *laser.CodecError
	if !errors.As(err, &codec) {
		t.Fatalf("want a CodecError, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "schema") {
		t.Errorf("want the mismatch named, got %v", err)
	}
}

func TestGivenAQueryFailureWhenReportedThenShouldCarryItsKind(t *testing.T) {
	server := &queryServer{reply: wire.ErrQueryReply(
		wire.QueryError{Kind: wire.QueryIndexNotFound, Message: "no such index"})}

	_, err := queryHandle(server, laser.Options{}).Query("events").Fetch(context.Background())

	var failure *laser.QueryError
	if !errors.As(err, &failure) {
		t.Fatalf("want a QueryError, got %T: %v", err, err)
	}
	if failure.Kind() != wire.QueryIndexNotFound {
		t.Errorf("want the index-not-found kind, got %d", failure.Kind())
	}
}

func TestGivenNoQuerySurfaceWhenCalledThenShouldRefuseIt(t *testing.T) {
	capabilities := queryCapabilities()
	capabilities.Query.Available = false
	server := &queryServer{}

	_, err := queryHandle(server, laser.Options{Capabilities: capabilities}).
		Query("events").Fetch(context.Background())

	var unsupported *laser.UnsupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("want an UnsupportedError, got %T: %v", err, err)
	}
}

// A page-bounded caller should not pay for an unbounded count, so the request
// asks for one only when told to.
func TestGivenAPageWhenRequestedThenShouldCarryOnlyWhatWasAsked(t *testing.T) {
	plain := &queryServer{reply: okResult()}
	if _, err := queryHandle(plain, laser.Options{}).Query("events").
		Limit(10).Offset(20).Fetch(context.Background()); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	query := sentQuery(t, plain, 0)
	if query.Page.Limit != 10 || query.Page.Offset == nil || *query.Page.Offset != 20 {
		t.Errorf("want the page carried, got %+v", query.Page)
	}
	if query.Page.WantTotal {
		t.Error("an unbounded count must be opt-in")
	}

	counted := &queryServer{reply: okResult()}
	if _, err := queryHandle(counted, laser.Options{}).Query("events").
		WithTotal().Fetch(context.Background()); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if !sentQuery(t, counted, 0).Page.WantTotal {
		t.Error("want the count requested")
	}
}
