package bdd_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	bdd "github.com/laserdata/laser-sdk/bdd/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// TestGivenTheSharedQueryManagedScenariosWhenRunThenShouldHold runs the
// managed query feature: two thousand records published through the client,
// materialized by the backend, and read back through the client's own query
// builder at a size that spans many pages.
//
// The reference-engine feature (query.feature) pins the same semantics with
// no server; this one proves the builder and the backend agree with it.
func TestGivenTheSharedQueryManagedScenariosWhenRunThenShouldHold(t *testing.T) {
	runFeature(t, "query_managed.feature", 5*time.Minute, queryManagedStep)
}

// materializeCatchUp bounds how long the scenario waits for the backend to
// catch up with what was published.
const materializeCatchUp = 2 * time.Minute

// managedQueryFlow is the index under test and the last result.
type managedQueryFlow struct {
	index  string
	result *wire.QueryResult
	// walked is every latency_ms an offset walk returned, in page order.
	walked []int64
	// frontierMoved is what a whole read reported about its pages, nil
	// until one ran.
	frontierMoved *bool
}

func managedQueryFlowOf(state *world) *managedQueryFlow {
	if existing, found := state.get("query-managed"); found {
		return existing.(*managedQueryFlow)
	}
	flow := &managedQueryFlow{}
	state.put("query-managed", flow)
	return flow
}

func queryManagedStep(ctx context.Context, state *world, step bdd.Step) error {
	handled, err := background(ctx, state, step)
	if handled {
		return err
	}
	flow := managedQueryFlowOf(state)
	text := step.Text
	arguments := argumentsOf(step)

	switch {
	case strings.Contains(text, "api-call records published and materialized into"):
		count, named := after(text, "")
		if len(arguments) != 1 || !named {
			return fmt.Errorf("want a record count and an index in %q", text)
		}
		return seedMaterialized(ctx, state, flow, arguments[0], int(count))

	case strings.Contains(text, "is queried for latency_ms greater than"):
		bound, named := after(text, "greater than ")
		if len(arguments) != 1 || !named {
			return fmt.Errorf("want an index and a bound in %q", text)
		}
		return runManagedQuery(ctx, state, flow, func(request *laser.QueryRequest) *laser.QueryRequest {
			return request.Filter("latency_ms", wire.CmpGt,
				wire.LongTyped(int64(bound))).WithTotal()
		})

	case strings.Contains(text, "is queried ordered by latency_ms descending"):
		limit, named := after(text, "with limit ")
		if len(arguments) != 1 || !named {
			return fmt.Errorf("want an index and a limit in %q", text)
		}
		return runManagedQuery(ctx, state, flow, func(request *laser.QueryRequest) *laser.QueryRequest {
			return request.OrderDesc("latency_ms").Limit(uint32(limit))
		})

	case strings.Contains(text, "is queried with limit"):
		limit, named := after(text, "with limit ")
		if len(arguments) != 1 || !named {
			return fmt.Errorf("want an index and a limit in %q", text)
		}
		return runManagedQuery(ctx, state, flow, func(request *laser.QueryRequest) *laser.QueryRequest {
			return request.Limit(uint32(limit)).WithTotal()
		})

	case strings.Contains(text, "is counted grouped by"):
		field := strings.TrimSpace(
			text[strings.Index(text, "grouped by ")+len("grouped by "):])
		if len(arguments) != 1 {
			return fmt.Errorf("want one index, got %v", arguments)
		}
		return runManagedQuery(ctx, state, flow, func(request *laser.QueryRequest) *laser.QueryRequest {
			return request.GroupBy(field).Count()
		})

	case strings.Contains(text, "is walked in pages of"):
		size, named := after(text, "in pages of ")
		if len(arguments) != 1 || !named {
			return fmt.Errorf("want an index and a page size in %q", text)
		}
		return walkPages(ctx, state, flow, arguments[0], int(size))

	case strings.Contains(text, "is read whole in pages of"):
		size, named := after(text, "in pages of ")
		if len(arguments) != 1 || !named {
			return fmt.Errorf("want an index and a page size in %q", text)
		}
		return readWhole(ctx, state, flow, arguments[0], int(size))

	case strings.HasPrefix(text, "the materialized total is"):
		wanted, named := after(text, "total is ")
		if !named {
			return fmt.Errorf("want a total in %q", text)
		}
		return managedTotalIs(flow, wanted)

	case strings.HasPrefix(text, "every materialized row has latency_ms greater than"):
		bound, named := after(text, "greater than ")
		if !named {
			return fmt.Errorf("want a bound in %q", text)
		}
		return everyManagedRowAbove(flow, int64(bound))

	case strings.HasPrefix(text, "the materialized latency_ms values are"):
		if len(arguments) != 1 {
			return fmt.Errorf("want the expected values, got %v", arguments)
		}
		return managedValuesInOrder(flow, list(arguments[0]))

	case strings.HasPrefix(text, "the materialized page holds"):
		wanted, named := after(text, "page holds ")
		if !named {
			return fmt.Errorf("want a row count in %q", text)
		}
		if got := len(flow.result.Rows); got != int(wanted) {
			return fmt.Errorf("want %d rows on the page, got %d", wanted, got)
		}
		return nil

	case strings.HasPrefix(text, "the materialized group"):
		count, named := after(text, "has count ")
		if len(arguments) != 1 || !named {
			return fmt.Errorf("want a group and a count in %q", text)
		}
		return managedGroupCount(flow, arguments[0], int64(count))

	case strings.HasPrefix(text, "the walk returns every latency_ms"):
		from, starts := after(text, "from ")
		to, named := after(text, " to ")
		if !starts || !named {
			return fmt.Errorf("want a range in %q", text)
		}
		return walkCoveredRange(flow, int64(from), int64(to))

	case text == "the whole read reports its frontier unmoved":
		return frontierUnmoved(flow)

	default:
		return fmt.Errorf("this runner does not implement the step")
	}
}

// seedMaterialized publishes the records the feature describes and waits for
// the backend to materialize them.
//
// The shape is every client's: status "500" on every fourth record and "200"
// otherwise, latency_ms counting up, so the totals the scenarios assert are
// the same everywhere.
func seedMaterialized(
	ctx context.Context, state *world, flow *managedQueryFlow, index string, count int,
) error {
	flow.index = index
	topic := state.client.Stream(state.stream).Topic(index)
	if err := topic.Ensure(ctx, 1, nil); err != nil {
		return err
	}
	projection := wire.Projection{
		ID:          wire.ProjectionID(index),
		Name:        index,
		Version:     1,
		ContentType: wire.ContentJSON,
		Extraction: wire.IndexSchema{
			Fields: []wire.IndexField{
				wire.TypedIndexField("status", "/status", wire.FieldText),
				wire.TypedIndexField("latency_ms", "/latency_ms", wire.FieldInt),
			},
		},
	}
	materialization, err := topic.Materialize().Typed(projection).Apply(ctx)
	if err != nil {
		return err
	}
	batch := topic.PublishBatch()
	for latency := 0; latency < count; latency++ {
		status := "200"
		if latency%4 == 0 {
			status = "500"
		}
		batch.AddJSON(map[string]any{"status": status, "latency_ms": latency})
	}
	if _, err := batch.Send(ctx); err != nil {
		return err
	}
	wait, err := materialization.Wait(ctx, wire.ReadinessCaughtUp, materializeCatchUp)
	if err != nil {
		return err
	}
	if wait == nil || wait.Kind != laser.WaitReady {
		return fmt.Errorf("want the materialization caught up, got %+v", wait)
	}
	return nil
}

// runManagedQuery runs one query through the client's builder and keeps the
// result for the Then steps.
func runManagedQuery(
	ctx context.Context, state *world, flow *managedQueryFlow,
	build func(*laser.QueryRequest) *laser.QueryRequest,
) error {
	result, err := build(state.client.Query(flow.index)).Fetch(ctx)
	if err != nil {
		return err
	}
	flow.result = result
	return nil
}

// walkPages reads the whole index in pages, collecting every latency_ms, so
// a later step can check every row came back exactly once.
func walkPages(
	ctx context.Context, state *world, flow *managedQueryFlow, index string, size int,
) error {
	flow.index = index
	flow.walked = nil
	for offset := uint64(0); ; offset += uint64(size) {
		result, err := state.client.Query(index).
			OrderAsc("latency_ms").
			Limit(uint32(size)).
			Offset(offset).
			Fetch(ctx)
		if err != nil {
			return err
		}
		if len(result.Rows) == 0 {
			flow.result = result
			return nil
		}
		for index := range result.Rows {
			value, err := rowInt(result, &result.Rows[index], "latency_ms")
			if err != nil {
				return err
			}
			flow.walked = append(flow.walked, value)
		}
		flow.result = result
	}
}

// rowInt reads one integer field of a row by name.
func rowInt(result *wire.QueryResult, row *wire.Row, field string) (int64, error) {
	for index, declared := range result.Fields {
		if declared.Name != field {
			continue
		}
		if index >= len(row.Values) {
			return 0, fmt.Errorf("row is narrower than the result schema")
		}
		switch value := row.Values[index]; value.Kind {
		case wire.TypedLong, wire.TypedTimestampMicros:
			return value.Long, nil
		case wire.TypedInt:
			return int64(value.Int), nil
		case wire.TypedString:
			return strconv.ParseInt(value.Str, 10, 64)
		default:
			return 0, fmt.Errorf("field %q is not a number: %+v", field, value)
		}
	}
	return 0, fmt.Errorf("the result has no field %q", field)
}

// rowText reads one text field of a row by name.
func rowText(result *wire.QueryResult, row *wire.Row, field string) (string, error) {
	for index, declared := range result.Fields {
		if declared.Name != field {
			continue
		}
		if index >= len(row.Values) {
			return "", fmt.Errorf("row is narrower than the result schema")
		}
		switch value := row.Values[index]; value.Kind {
		case wire.TypedString:
			return value.Str, nil
		case wire.TypedLong:
			return strconv.FormatInt(value.Long, 10), nil
		case wire.TypedInt:
			return strconv.FormatInt(int64(value.Int), 10), nil
		default:
			return "", fmt.Errorf("field %q holds no readable value", field)
		}
	}
	return "", fmt.Errorf("the result has no field %q", field)
}

func managedTotalIs(flow *managedQueryFlow, wanted uint64) error {
	if flow.result == nil {
		return fmt.Errorf("the scenario ran no query")
	}
	if flow.result.Page.Total == nil {
		return fmt.Errorf("want a total, the query asked for none")
	}
	if *flow.result.Page.Total != wanted {
		return fmt.Errorf("want the total %d, got %d", wanted, *flow.result.Page.Total)
	}
	return nil
}

func everyManagedRowAbove(flow *managedQueryFlow, bound int64) error {
	for index := range flow.result.Rows {
		value, err := rowInt(flow.result, &flow.result.Rows[index], "latency_ms")
		if err != nil {
			return err
		}
		if value <= bound {
			return fmt.Errorf("want every row above %d, got %d", bound, value)
		}
	}
	return nil
}

func managedValuesInOrder(flow *managedQueryFlow, expected []string) error {
	got := make([]string, 0, len(flow.result.Rows))
	for index := range flow.result.Rows {
		value, err := rowInt(flow.result, &flow.result.Rows[index], "latency_ms")
		if err != nil {
			return err
		}
		got = append(got, strconv.FormatInt(value, 10))
	}
	if !equalStrings(got, expected) {
		return fmt.Errorf("want the values %v in order, got %v", expected, got)
	}
	return nil
}

func managedGroupCount(flow *managedQueryFlow, group string, wanted int64) error {
	for index := range flow.result.Rows {
		row := &flow.result.Rows[index]
		status, err := rowText(flow.result, row, "status")
		if err != nil {
			return err
		}
		if status != group {
			continue
		}
		count, err := groupCountOf(flow.result, row)
		if err != nil {
			return err
		}
		if count != wanted {
			return fmt.Errorf("want group %q count %d, got %d", group, wanted, count)
		}
		return nil
	}
	return fmt.Errorf("want a group %q in the result", group)
}

// groupCountOf reads the count a grouped result carries, whatever the
// backend named the aggregate column.
func groupCountOf(result *wire.QueryResult, row *wire.Row) (int64, error) {
	for index, declared := range result.Fields {
		if declared.Name == "status" || index >= len(row.Values) {
			continue
		}
		switch value := row.Values[index]; value.Kind {
		case wire.TypedLong:
			return value.Long, nil
		case wire.TypedInt:
			return int64(value.Int), nil
		}
	}
	return 0, fmt.Errorf("the grouped result carries no count")
}

// walkCoveredRange asserts the walk returned every value of the range once.
func walkCoveredRange(flow *managedQueryFlow, from, to int64) error {
	seen := make(map[int64]int, len(flow.walked))
	for _, value := range flow.walked {
		seen[value]++
	}
	for value := from; value <= to; value++ {
		switch seen[value] {
		case 1:
		case 0:
			return fmt.Errorf("the walk never returned latency_ms %d", value)
		default:
			return fmt.Errorf("the walk returned latency_ms %d %d times", value, seen[value])
		}
	}
	if len(seen) != int(to-from+1) {
		return fmt.Errorf("want %d distinct values, got %d", to-from+1, len(seen))
	}
	return nil
}

// readWhole collects every page of the index in one call, which also reports
// whether the pages were served at one position.
func readWhole(
	ctx context.Context, state *world, flow *managedQueryFlow, index string, size int,
) error {
	flow.index = index
	flow.walked = nil
	whole, err := state.client.Query(index).OrderAsc("latency_ms").Limit(uint32(size)).FetchAll(ctx)
	if err != nil {
		return err
	}
	for _, row := range whole.Rows {
		latency, numeric := row.ValueI64("latency_ms")
		if !numeric {
			return fmt.Errorf("latency_ms is not a number in %+v", row.Values)
		}
		flow.walked = append(flow.walked, latency)
	}
	flow.frontierMoved = &whole.FrontierMoved
	return nil
}

// frontierUnmoved asserts the whole read reported one frontier for every page.
func frontierUnmoved(flow *managedQueryFlow) error {
	if flow.frontierMoved == nil {
		return fmt.Errorf("the scenario read no index whole")
	}
	if *flow.frontierMoved {
		return fmt.Errorf("want the frontier unmoved, the whole read reports it moved")
	}
	return nil
}
