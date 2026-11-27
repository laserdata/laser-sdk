package bdd_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	bdd "github.com/laserdata/laser-sdk/bdd/go"
)

// TestGivenTheSharedQueryScenariosWhenRunThenShouldHold runs the query
// semantics feature against this module's reference engine.
//
// A local feature: the scenarios describe the semantics every client and the
// managed backend must reproduce, and prove neither an SDK nor a backend.
// The SDK builders run the same queries against a deployment in
// query_managed.feature.
func TestGivenTheSharedQueryScenariosWhenRunThenShouldHold(t *testing.T) {
	runLocalFeature(t, "query.feature", time.Minute, queryStep)
}

// queryBench is the scenario's engine, the index it seeded, and its last
// page.
type queryBench struct {
	engine *bdd.QueryEngine
	index  string
	page   bdd.QueryPage
}

func queryBenchOf(state *world) *queryBench {
	if existing, found := state.get("query"); found {
		return existing.(*queryBench)
	}
	bench := &queryBench{engine: bdd.NewQueryEngine()}
	state.put("query", bench)
	return bench
}

func queryStep(_ context.Context, state *world, step bdd.Step) error {
	bench := queryBenchOf(state)
	text := step.Text
	arguments := argumentsOf(step)

	switch {
	case strings.HasPrefix(text, "a query index") &&
		strings.Contains(text, "sample api-call rows"):
		if len(arguments) != 1 {
			return fmt.Errorf("want one index name, got %v", arguments)
		}
		return seedSampleRows(bench, arguments[0])

	case strings.HasPrefix(text, "I query") && strings.Contains(text, "greater than"):
		bound, named := after(text, "greater than ")
		if len(arguments) != 1 || !named {
			return fmt.Errorf("want an index and a bound in %q", text)
		}
		bench.page = bench.engine.Execute(bdd.QueryRequest{
			Index: arguments[0],
			Predicate: &bdd.QueryPredicate{
				Field: "latency_ms", Op: "gt", Value: strconv.FormatUint(bound, 10),
			},
		})
		return nil

	case strings.HasPrefix(text, "I query") && strings.Contains(text, "ordered by"):
		if len(arguments) != 1 {
			return fmt.Errorf("want one index name, got %v", arguments)
		}
		bench.page = bench.engine.Execute(bdd.QueryRequest{
			Index: arguments[0],
			Order: []bdd.QueryOrder{{
				Field: "latency_ms", Desc: strings.Contains(text, "descending"),
			}},
		})
		return nil

	case strings.HasPrefix(text, "I query") && strings.Contains(text, "with limit"):
		limit, named := after(text, "with limit ")
		if len(arguments) != 1 || !named {
			return fmt.Errorf("want an index and a limit in %q", text)
		}
		bench.page = bench.engine.Execute(bdd.QueryRequest{
			Index: arguments[0], Limit: int(limit),
		})
		return nil

	case strings.HasPrefix(text, "I count") && strings.Contains(text, "grouped by"):
		if len(arguments) != 1 {
			return fmt.Errorf("want one index name, got %v", arguments)
		}
		field := strings.TrimSpace(text[strings.Index(text, "grouped by ")+len("grouped by "):])
		bench.page = bench.engine.Execute(bdd.QueryRequest{
			Index: arguments[0],
			Aggregate: &bdd.QueryAggregate{
				GroupBy: []string{field}, Func: "count", Alias: "count",
			},
		})
		return nil

	case strings.HasPrefix(text, "the query returns") && strings.Contains(text, "rows"):
		wanted, named := after(text, "the query returns ")
		if !named {
			return fmt.Errorf("want a row count in %q", text)
		}
		if len(bench.page.Rows) != int(wanted) {
			return fmt.Errorf("want %d rows, got %d", wanted, len(bench.page.Rows))
		}
		return nil

	case strings.HasPrefix(text, "every returned row has latency_ms greater than"):
		bound, named := after(text, "greater than ")
		if !named {
			return fmt.Errorf("want a bound in %q", text)
		}
		return everyRowAbove(bench, bound)

	case strings.HasPrefix(text, "the returned latency_ms values are"):
		if len(arguments) != 1 {
			return fmt.Errorf("want the expected values, got %v", arguments)
		}
		return valuesInOrder(bench, list(arguments[0]))

	case strings.HasPrefix(text, "the page total is"):
		total, named := after(text, "the page total is ")
		if !named {
			return fmt.Errorf("want a total in %q", text)
		}
		if bench.page.Total != int(total) {
			return fmt.Errorf("want the page total %d, got %d", total, bench.page.Total)
		}
		return nil

	case strings.HasPrefix(text, "group ") && strings.Contains(text, " has count "):
		count, named := after(text, " has count ")
		if len(arguments) != 1 || !named {
			return fmt.Errorf("want a group and a count in %q", text)
		}
		return groupHasCount(bench, arguments[0], count)

	default:
		return fmt.Errorf("this runner does not implement the step")
	}
}

// seedSampleRows seeds the rows every client's runner seeds, so one feature
// pins one set of answers.
func seedSampleRows(bench *queryBench, index string) error {
	bench.engine = bdd.NewQueryEngine()
	bench.index = index
	for _, row := range [][2]string{
		{"200", "10"}, {"200", "550"}, {"500", "900"}, {"200", "30"},
	} {
		bench.engine.Insert(index, bdd.QueryRow{
			"status": row[0], "latency_ms": row[1],
		})
	}
	return nil
}

func everyRowAbove(bench *queryBench, bound uint64) error {
	for _, row := range bench.page.Rows {
		value, err := strconv.ParseUint(row["latency_ms"], 10, 64)
		if err != nil {
			return fmt.Errorf("row latency_ms %q is not a number", row["latency_ms"])
		}
		if value <= bound {
			return fmt.Errorf("want every row above %d, got %d", bound, value)
		}
	}
	return nil
}

func valuesInOrder(bench *queryBench, expected []string) error {
	got := make([]string, 0, len(bench.page.Rows))
	for _, row := range bench.page.Rows {
		got = append(got, row["latency_ms"])
	}
	if !equalStrings(got, expected) {
		return fmt.Errorf("want the values %v in order, got %v", expected, got)
	}
	return nil
}

func groupHasCount(bench *queryBench, group string, count uint64) error {
	for _, row := range bench.page.Rows {
		if row["status"] != group {
			continue
		}
		if row["count"] != strconv.FormatUint(count, 10) {
			return fmt.Errorf("want group %q count %d, got %q",
				group, count, row["count"])
		}
		return nil
	}
	return fmt.Errorf("want a group %q among %v", group, bench.page.Rows)
}
