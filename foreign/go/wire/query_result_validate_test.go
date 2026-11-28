package wire_test

import (
	"math"
	"strings"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenAResultPageWhenValidatedThenShouldRefuseWhatItsSchemaAndContextContradict(t *testing.T) {
	infinite := float32(math.Inf(1))
	for name, broken := range map[string]struct {
		change func(*wire.QueryResult)
		reason string
	}{
		"a value of another type": {func(result *wire.QueryResult) {
			result.Rows[0].Values[0] = wire.StringTyped("seven")
		}, ""},
		"a null in a required field": {func(result *wire.QueryResult) {
			result.Fields[0].Required = true
			result.Rows[0].Values[0] = wire.TypedValue{Kind: wire.TypedNull}
		}, ""},
		"a score that is not finite": {func(result *wire.QueryResult) {
			result.Rows[0].Score = &infinite
		}, "query row score must be finite"},
		"a row count that differs from the rows": {func(result *wire.QueryResult) {
			result.Context.RowCount = 2
		}, "query result row count does not match the page rows"},
		"a context without an engine": {func(result *wire.QueryResult) {
			result.Context.Engine.Name = ""
		}, "query engine name"},
	} {
		result := soundResult()
		if err := result.Validate(); err != nil {
			t.Fatalf("the sound result must validate: %v", err)
		}
		broken.change(&result)

		err := result.Validate()

		if err == nil {
			t.Errorf("%s: want a refusal", name)
			continue
		}
		if !strings.Contains(err.Error(), broken.reason) {
			t.Errorf("%s: got %q, want it to name %q", name, err, broken.reason)
		}
	}
}

func soundResult() wire.QueryResult {
	return wire.QueryResult{
		Fields: []wire.LogicalField{{ID: 1, Name: "id", FieldType: wire.LogicalType{Kind: wire.KindLong}}},
		Rows:   []wire.Row{{Values: []wire.TypedValue{wire.LongTyped(7)}}},
		Page:   wire.Page{Limit: 50},
		Context: wire.QueryContext{
			ExecutionID: wire.QueryExecutionID{ID: wire.IDFromBytes([16]byte{15: 9})},
			Engine:      wire.QueryEngine{Name: "engine", Version: "1"},
			ResolvedTarget: wire.NewResolvedOperationalTarget("readings",
				wire.BackendResourceID{ID: wire.IDFromBytes([16]byte{15: 1})}, 1, 1),
			RequestedConsistency: wire.ConsistencyEventual,
			DeliveredConsistency: wire.ConsistencyEventual,
			RowCount:             1,
		},
	}
}
