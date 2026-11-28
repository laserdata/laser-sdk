package laser_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// queryServer answers every query with reply, or with the next of pages, and
// echoes the request's execution identity as a server does.
type queryServer struct {
	clusterOfOne
	reply    wire.QueryReply
	pages    []wire.QueryReply
	answerAs *wire.QueryExecutionID
	codes    []uint32
	requests [][]byte
}

func (server *queryServer) Close() error { return nil }

func (server *queryServer) SendBinaryRequest(
	_ context.Context, code uint32, payload []byte,
) ([]byte, error) {
	server.codes = append(server.codes, code)
	server.requests = append(server.requests, payload)
	reply := server.reply
	if len(server.pages) > 0 {
		reply = server.pages[min(len(server.requests), len(server.pages))-1]
	}
	if reply.Result != nil {
		var envelope wire.QueryEnvelope
		if err := wire.Unmarshal(payload, &envelope); err != nil {
			return nil, err
		}
		result := *reply.Result
		result.Context.ExecutionID = envelope.Query.ExecutionID
		if server.answerAs != nil {
			result.Context.ExecutionID = *server.answerAs
		}
		reply.Result = &result
	}
	return wire.Marshal(reply)
}

// silentServer never answers a query, so only the caller's own bound ends the wait.
type silentServer struct{ clusterOfOne }

func (*silentServer) Close() error { return nil }

func (*silentServer) SendBinaryRequest(ctx context.Context, _ uint32, _ []byte) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func sentExecutions(t *testing.T, server *queryServer) []wire.QueryExecutionID {
	t.Helper()
	sent := make([]wire.QueryExecutionID, len(server.requests))
	for index := range server.requests {
		sent[index] = sentQuery(t, server, index).ExecutionID
	}
	return sent
}

func payloadResult(hasMore bool, names ...string) wire.QueryReply {
	rows := make([]wire.Row, len(names))
	for index, name := range names {
		rows[index] = wire.Row{Values: []wire.TypedValue{wire.BinaryTyped([]byte(`{"name":"` + name + `"}`))}}
	}
	reply := okResult(rows...)
	reply.Result.Fields = []wire.LogicalField{{
		Name: wire.OriginalPayloadFieldName, FieldType: wire.LogicalType{Kind: wire.KindBinary},
	}}
	reply.Result.Page.HasMore = hasMore
	return reply
}

type namedItem struct {
	Name string `json:"name"`
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
	context := resultContext()
	context.RowCount = uint64(len(rows))
	return wire.OkQueryReply(wire.QueryResult{
		Fields:  []wire.LogicalField{{Name: "id", FieldType: wire.LogicalType{Kind: wire.KindLong}}},
		Rows:    rows,
		Page:    wire.Page{Limit: 50},
		Context: context,
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
	request := queryHandle(server, laser.Options{}).Query("events").Deadline(10 * time.Second)

	before := time.Now()
	if _, err := request.Fetch(context.Background()); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	after := time.Now()

	query := sentQuery(t, server, 0)
	deadline := time.UnixMicro(int64(query.DeadlineMicros))
	// The wire value truncates to microseconds, so the lower bound does too.
	if deadline.Before(before.Truncate(time.Microsecond).Add(10 * time.Second)) {
		t.Errorf("want a deadline at least ten seconds out, got %v past %v", deadline, before)
	}
	if deadline.After(after.Add(10 * time.Second)) {
		t.Errorf("want a deadline no more than ten seconds out, got %v past %v", deadline, after)
	}
}

func TestGivenNoDeadlineWhenSentThenShouldAllowTheSharedQueryBudget(t *testing.T) {
	server := &queryServer{reply: okResult()}

	before := time.Now()
	if _, err := queryHandle(server, laser.Options{}).Query("events").Fetch(context.Background()); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	after := time.Now()

	deadline := time.UnixMicro(int64(sentQuery(t, server, 0).DeadlineMicros))
	if deadline.Before(before.Truncate(time.Microsecond).Add(laser.QueryDeadline)) ||
		deadline.After(after.Add(laser.QueryDeadline)) {
		t.Errorf("want a deadline %s out, got %v from %v", laser.QueryDeadline, deadline, before)
	}
	if laser.DefaultQueryDeadline != laser.QueryDeadline {
		t.Errorf("the default deadline is %s, the shared budget is %s", laser.DefaultQueryDeadline, laser.QueryDeadline)
	}
}

func TestGivenADeadlinePastTheQueryBudgetWhenFetchedThenShouldRefuseBeforeSending(t *testing.T) {
	hour := uint64(time.Now().Add(time.Hour).UnixMicro())
	for name, build := range map[string]func(*laser.QueryRequest) *laser.QueryRequest{
		"a relative deadline": func(request *laser.QueryRequest) *laser.QueryRequest {
			return request.Deadline(laser.QueryDeadline + time.Second)
		},
		"an absolute deadline": func(request *laser.QueryRequest) *laser.QueryRequest { return request.DeadlineAtMicros(hour) },
	} {
		server := &queryServer{reply: okResult()}

		_, err := build(queryHandle(server, laser.Options{}).Query("events")).Fetch(context.Background())

		var invalid *laser.InvalidError
		if !errors.As(err, &invalid) || len(server.codes) != 0 {
			t.Errorf("%s: want an InvalidError before any send, got %T %v after %d sends", name, err, err, len(server.codes))
		}
	}
}

func TestGivenAnAbsoluteDeadlineWhenFetchedAgainThenShouldCarryItOnEveryCall(t *testing.T) {
	server := &queryServer{reply: okResult()}
	at := uint64(time.Now().Add(5 * time.Second).UnixMicro())
	request := queryHandle(server, laser.Options{}).Query("events").DeadlineAtMicros(at)

	for range 2 {
		if _, err := request.Fetch(context.Background()); err != nil {
			t.Fatalf("fetch: %v", err)
		}
	}

	if first, second := sentQuery(t, server, 0).DeadlineMicros, sentQuery(t, server, 1).DeadlineMicros; first != at || second != at {
		t.Fatalf("want %d on both calls, got %d and %d", at, first, second)
	}
}

func TestGivenNoReplyWhenTheQueryDeadlinePassesThenShouldStopWaitingAfterTheGrace(t *testing.T) {
	request := queryHandle(&silentServer{}, laser.Options{}).Query("events").
		DeadlineAtMicros(uint64(time.Now().Add(50 * time.Millisecond).UnixMicro()))

	started := time.Now()
	_, err := request.Fetch(context.Background())
	waited := time.Since(started)

	var timeout interface{ Timeout() bool }
	if !errors.As(err, &timeout) || !timeout.Timeout() {
		t.Fatalf("want a timeout, got %T: %v", err, err)
	}
	if waited < laser.QueryReplyGrace || waited > laser.QueryReplyGrace+2*time.Second {
		t.Fatalf("want the wait to end %s past the deadline, waited %s", laser.QueryReplyGrace, waited)
	}
}

func TestGivenOneBuilderWhenRunAgainThenShouldCarryAFreshExecutionIdentityEachTime(t *testing.T) {
	server := &queryServer{reply: okResult()}
	request := queryHandle(server, laser.Options{}).Query("events")
	announced := request.ExecutionID()
	ctx := context.Background()

	for range 2 {
		if _, err := request.Fetch(ctx); err != nil {
			t.Fatalf("fetch: %v", err)
		}
		if _, err := request.FetchOne(ctx, laser.JSONCodec{}, &namedItem{}); err != nil {
			t.Fatalf("fetch one: %v", err)
		}
		rows, err := request.MaxRows(1).Rows()
		if err != nil {
			t.Fatalf("rows: %v", err)
		}
		if _, err := rows.Next(ctx); err != nil {
			t.Fatalf("next: %v", err)
		}
		if _, err := request.FetchAll(ctx); err != nil {
			t.Fatalf("fetch all: %v", err)
		}
	}

	sent := sentExecutions(t, server)
	distinct := make(map[wire.QueryExecutionID]bool, len(sent))
	for _, id := range sent {
		distinct[id] = true
	}
	if len(sent) != 8 || len(distinct) != len(sent) || distinct[wire.QueryExecutionID{}] {
		t.Fatalf("want eight distinct nonzero identities, got %d distinct of %d", len(distinct), len(sent))
	}
	if sent[0] != announced {
		t.Errorf("the first call sent %s, the builder announced %s", sent[0], announced)
	}
	if next := request.ExecutionID(); distinct[next] {
		t.Errorf("the next identity %s was already sent", next)
	}
}

func TestGivenAReplyForAnotherExecutionWhenReadThenShouldRefuseIt(t *testing.T) {
	other := wire.QueryExecutionID{ID: wire.IDFromBytes([16]byte{15: 9})}
	server := &queryServer{reply: okResult(), answerAs: &other}

	_, err := queryHandle(server, laser.Options{}).Query("events").Fetch(context.Background())

	var protocol *laser.ProtocolError
	if !errors.As(err, &protocol) || !strings.Contains(err.Error(), "execution id") {
		t.Fatalf("want a ProtocolError naming the execution id, got %T: %v", err, err)
	}
}

func TestGivenAReplyWithoutAnExecutionIdentityWhenReadThenShouldRefuseIt(t *testing.T) {
	server := &queryServer{reply: okResult(), answerAs: &wire.QueryExecutionID{}}

	_, err := queryHandle(server, laser.Options{}).Query("events").Fetch(context.Background())

	var codec *laser.CodecError
	if !errors.As(err, &codec) || !strings.Contains(err.Error(), "execution id must be nonzero") {
		t.Fatalf("want a CodecError naming the zero execution id, got %T: %v", err, err)
	}
}

func TestGivenAnInvalidQueryWhenExecutedDirectlyThenShouldRefuseBeforeSending(t *testing.T) {
	server := &queryServer{reply: okResult()}
	unnamed := wire.NewQuery(wire.QueryExecutionID{}, wire.OperationalTarget("events"),
		uint64(time.Now().Add(time.Second).UnixMicro()))

	_, err := queryHandle(server, laser.Options{}).ExecuteQuery(context.Background(), unnamed)

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) || len(server.codes) != 0 {
		t.Fatalf("want an InvalidError before any send, got %T %v after %d sends", err, err, len(server.codes))
	}
}

func TestGivenKeyMatchesAndAPredicateTreeWhenBuiltThenShouldCarryThemAsGiven(t *testing.T) {
	server := &queryServer{reply: okResult()}
	tree := wire.AnyOf(
		wire.Pred("kind", wire.CmpEq, wire.StringTyped("click")),
		wire.Pred("count", wire.CmpGte, wire.LongTyped(5)))

	if _, err := queryHandle(server, laser.Options{}).Query("events").
		WhereEq("host", wire.StringTyped("node-1")).
		WhereEq("zone", wire.LongTyped(3)).
		Predicate(tree).
		Fetch(context.Background()); err != nil {
		t.Fatalf("fetch: %v", err)
	}

	query := sentQuery(t, server, 0)
	wantKeys := []wire.KeyMatch{
		{Field: "host", Value: wire.StringTyped("node-1")}, {Field: "zone", Value: wire.LongTyped(3)},
	}
	if !reflect.DeepEqual(query.ByKey, wantKeys) {
		t.Errorf("key matches = %+v, want %+v", query.ByKey, wantKeys)
	}
	if query.Filter == nil || !reflect.DeepEqual(*query.Filter, tree) {
		t.Errorf("filter = %+v, want %+v", query.Filter, tree)
	}
}

func TestGivenAVectorSearchWhenBuiltThenShouldNameItsFieldEmbeddingAndCount(t *testing.T) {
	embedding := []float32{0.5, 0.25}
	for field, build := range map[string]func(*laser.QueryRequest) *laser.QueryRequest{
		wire.VectorField: func(request *laser.QueryRequest) *laser.QueryRequest { return request.Nearest(embedding, 3) },
		"summary_vector": func(request *laser.QueryRequest) *laser.QueryRequest {
			return request.NearestIn("summary_vector", embedding, 3)
		},
	} {
		server := &queryServer{reply: okResult()}

		if _, err := build(queryHandle(server, laser.Options{}).Query("events")).Fetch(context.Background()); err != nil {
			t.Fatalf("%s: fetch: %v", field, err)
		}

		want := &wire.VectorQuery{Field: field, Embedding: embedding, TopK: 3}
		if got := sentQuery(t, server, 0).Vector; !reflect.DeepEqual(got, want) {
			t.Errorf("vector = %+v, want %+v", got, want)
		}
	}
}

func TestGivenAWindowWhenBuiltThenShouldGroupTheAggregateByItsInterval(t *testing.T) {
	server := &queryServer{reply: okResult()}

	if _, err := queryHandle(server, laser.Options{}).Query("events").
		Count().Window("occurred_at", time.Minute).Fetch(context.Background()); err != nil {
		t.Fatalf("fetch: %v", err)
	}

	want := &wire.Aggregate{
		Funcs:  []wire.AggCall{{Func: wire.AggCount, Alias: "count"}},
		Window: &wire.Window{Field: "occurred_at", EveryMicros: 60_000_000},
	}
	if got := sentQuery(t, server, 0).Aggregate; !reflect.DeepEqual(got, want) {
		t.Errorf("aggregate = %+v, want %+v", got, want)
	}

	_, err := queryHandle(server, laser.Options{}).Query("events").
		Count().Window("occurred_at", 0).Fetch(context.Background())
	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) || len(server.codes) != 1 {
		t.Errorf("want an empty window refused before sending, got %T %v", err, err)
	}
}

func TestGivenAStandardDeviationWhenBuiltThenShouldUseTheSharedAlias(t *testing.T) {
	server := &queryServer{reply: okResult()}

	if _, err := queryHandle(server, laser.Options{}).Query("events").
		StdDev("latency").Fetch(context.Background()); err != nil {
		t.Fatalf("fetch: %v", err)
	}

	field := "latency"
	want := &wire.Aggregate{Funcs: []wire.AggCall{{Func: wire.AggStdDev, Field: &field, Alias: "stddev"}}}
	if got := sentQuery(t, server, 0).Aggregate; !reflect.DeepEqual(got, want) {
		t.Errorf("aggregate = %+v, want %+v", got, want)
	}
}

func TestGivenSeveralPagesWhenFetchingAllThenShouldCollectEveryRowUnderFreshIdentities(t *testing.T) {
	first := okResult(wire.Row{Values: []wire.TypedValue{wire.LongTyped(1)}}, wire.Row{Values: []wire.TypedValue{wire.LongTyped(2)}})
	first.Result.Page.HasMore = true
	last := okResult(wire.Row{Values: []wire.TypedValue{wire.LongTyped(3)}})
	server := &queryServer{pages: []wire.QueryReply{first, last}}

	set, err := queryHandle(server, laser.Options{}).Query("events").FetchAll(context.Background())
	if err != nil {
		t.Fatalf("fetch all: %v", err)
	}

	collected := make([]int64, len(set.Rows))
	for index, row := range set.Rows {
		collected[index], _ = row.ValueI64("id")
	}
	if !reflect.DeepEqual(collected, []int64{1, 2, 3}) || set.FrontierMoved {
		t.Fatalf("rows = %v moved = %v, want 1 2 3 at one frontier", collected, set.FrontierMoved)
	}
	sent := sentExecutions(t, server)
	if len(sent) != 2 || sent[0] == sent[1] {
		t.Fatalf("want two pages under two identities, got %v", sent)
	}
	if offset := sentQuery(t, server, 1).Page.Offset; offset == nil || *offset != 2 {
		t.Fatalf("the second page must start after the first two rows, got %v", offset)
	}
}

func TestGivenATypedQueryWhenFetchedThenShouldDecodeEveryPayloadOfThePage(t *testing.T) {
	server := &queryServer{reply: payloadResult(false, "planner", "critic")}
	query := laser.TypedQuery[namedItem](queryHandle(server, laser.Options{}).Query("events"), laser.JSONCodec{})

	items, err := query.Fetch(context.Background())
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}

	if want := []namedItem{{Name: "planner"}, {Name: "critic"}}; !reflect.DeepEqual(items, want) {
		t.Fatalf("items = %+v, want %+v", items, want)
	}
	if !sentQuery(t, server, 0).Select.Payload {
		t.Error("a typed fetch must ask for the payload")
	}
}

func TestGivenATypedQueryWhenWalkedOrCollectedThenShouldDecodeAcrossPages(t *testing.T) {
	pages := []wire.QueryReply{payloadResult(true, "planner", "critic"), payloadResult(false, "writer")}
	want := []namedItem{{Name: "planner"}, {Name: "critic"}, {Name: "writer"}}

	server := &queryServer{pages: pages}
	all, err := laser.TypedQuery[namedItem](queryHandle(server, laser.Options{}).Query("events"), laser.JSONCodec{}).
		FetchAll(context.Background())
	if err != nil || !reflect.DeepEqual(all, want) {
		t.Fatalf("fetch all = %+v, %v, want %+v", all, err, want)
	}

	server = &queryServer{pages: pages}
	rows, err := laser.TypedQuery[namedItem](
		queryHandle(server, laser.Options{}).Query("events").MaxRows(3), laser.JSONCodec{}).Rows()
	if err != nil {
		t.Fatalf("rows: %v", err)
	}
	var walked []namedItem
	for {
		item, err := rows.Next(context.Background())
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if item == nil {
			break
		}
		walked = append(walked, *item)
	}
	if !reflect.DeepEqual(walked, want) || rows.FrontierMoved() || len(server.requests) != 2 {
		t.Fatalf("walk = %+v over %d pages, want %+v over 2", walked, len(server.requests), want)
	}
}

func TestGivenAQueryRowWhenReadByNameThenShouldAnswerItsPositionTextAndIntegers(t *testing.T) {
	row := laser.QueryRow{
		Fields: []wire.LogicalField{{Name: "host"}, {Name: "cpu"}, {Name: "delta"}, {Name: "load"}},
		Row: wire.Row{Values: []wire.TypedValue{
			wire.StringTyped("node-1"), wire.IntTyped(42), wire.LongTyped(-7), wire.DoubleTyped(1.5),
		}},
	}

	type read struct {
		index  int
		text   string
		signed int64
		whole  uint64
		found  [4]bool
	}
	observe := func(name string) read {
		var seen read
		seen.index, seen.found[0] = row.FieldIndex(name)
		seen.text, seen.found[1] = row.ValueText(name)
		seen.signed, seen.found[2] = row.ValueI64(name)
		seen.whole, seen.found[3] = row.ValueU64(name)
		return seen
	}
	for name, want := range map[string]read{
		"host":    {index: 0, text: "node-1", found: [4]bool{true, true, false, false}},
		"cpu":     {index: 1, text: "42", signed: 42, whole: 42, found: [4]bool{true, true, true, true}},
		"delta":   {index: 2, text: "-7", signed: -7, found: [4]bool{true, true, true, false}},
		"load":    {index: 3, text: "1.5", found: [4]bool{true, true, false, false}},
		"missing": {},
	} {
		if got := observe(name); got != want {
			t.Errorf("%s read as %+v, want %+v", name, got, want)
		}
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
	backend.DesiredState = wire.BackendEnabledState
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

func TestGivenMoreQueryPagesWhenWalkingThenShouldRespectTheCapAndFrontier(t *testing.T) {
	first := okResult(wire.Row{Values: []wire.TypedValue{wire.LongTyped(1)}})
	first.Result.Page.HasMore = true
	first.Result.Context.Frontier = []wire.QueryFrontier{{Topic: "events", AppliedOffset: 1}}
	server := &queryServer{reply: first}
	request := queryHandle(server, laser.Options{}).Query("events").Offset(4)
	if _, err := request.Rows(); err == nil {
		t.Fatal("accepted unbounded Rows")
	}
	rows, err := request.MaxRows(2).Rows()
	if err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		row, err := rows.Next(t.Context())
		if err != nil || row == nil {
			t.Fatalf("page %d: %v, %v", i, row, err)
		}
		value, ok := row.Value("id")
		if !ok || value.Long != 1 {
			t.Fatalf("row lost its fields: %+v", row)
		}
		server.reply.Result.Context.Frontier[0].AppliedOffset++
		var envelope wire.QueryEnvelope
		if err := wire.Unmarshal(server.requests[i], &envelope); err != nil {
			t.Fatal(err)
		}
		if envelope.Query.Page.Offset == nil || *envelope.Query.Page.Offset != uint64(4+i) {
			t.Fatal("wrong page offset")
		}
	}
	if row, err := rows.Next(t.Context()); err != nil || row != nil || !rows.FrontierMoved() || len(server.requests) != 2 {
		t.Fatal("walk failed to stop at its cap or report a moved frontier")
	}
}

func TestGivenAnOriginalPayloadWhenFetchingTypedThenShouldDecodeIntoTheRequestedType(t *testing.T) {
	reply := okResult(wire.Row{Values: []wire.TypedValue{wire.BinaryTyped([]byte(`{"name":"planner"}`))}})
	reply.Result.Fields = []wire.LogicalField{{Name: wire.OriginalPayloadFieldName, FieldType: wire.LogicalType{Kind: wire.KindBinary}}}
	server := &queryServer{reply: reply}
	request := queryHandle(server, laser.Options{}).Query("events")
	type item struct {
		Name string `json:"name"`
	}
	query := laser.TypedQuery[item](request, laser.JSONCodec{})
	one, err := query.FetchOne(t.Context())
	if err != nil || one == nil || one.Name != "planner" {
		t.Fatalf("one: %+v, %v", one, err)
	}
	var asked wire.QueryEnvelope
	if err := wire.Unmarshal(server.requests[0], &asked); err != nil {
		t.Fatal(err)
	}
	if !asked.Query.Select.Payload || asked.Query.Page.Limit != 1 || asked.Query.ExecutionID == request.ExecutionID() {
		t.Fatalf("wrong typed query: %+v", asked.Query)
	}
	original, err := request.IntoQuery()
	if err != nil || original.Select.Payload || original.Page.Limit == 1 {
		t.Fatal("typed fetch mutated its source builder")
	}
	server.reply.Result.Rows = nil
	server.reply.Result.Context.RowCount = 0
	one, err = query.FetchOne(t.Context())
	if one != nil || err != nil {
		t.Fatalf("absent row: %+v, %v", one, err)
	}
}
