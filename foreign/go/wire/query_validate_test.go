package wire_test

import (
	"math"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// soundQuery is the smallest query the contract accepts, which each case
// below breaks in exactly one way.
func soundQuery() wire.Query {
	return wire.NewQuery(
		wire.QueryExecutionID{ID: wire.IDFromBytes([16]byte{15: 9})},
		wire.OperationalTarget("readings"),
		1_700_000_000_000_000,
	)
}

// A query is refused here rather than by the server, so a malformed one fails
// where the mistake was made. Each rule matters on its own: a client that
// accepted what the server refuses reports the failure a round trip too late,
// and one that refused what the server accepts denies a legal query outright.
func TestGivenAMalformedQueryWhenValidatedThenShouldNameTheRuleItBreaks(t *testing.T) {
	if err := soundQuery().Validate(); err != nil {
		t.Fatalf("the minimal query must validate: %v", err)
	}

	for name, break_ := range map[string]func(*wire.Query){
		"no execution id": func(q *wire.Query) { q.ExecutionID = wire.QueryExecutionID{} },
		"no deadline":     func(q *wire.Query) { q.DeadlineMicros = 0 },
		"an empty index":  func(q *wire.Query) { q.Target = wire.OperationalTarget("") },
		"a padded index": func(q *wire.Query) {
			q.Target = wire.OperationalTarget(" readings ")
		},
		"a zero limit": func(q *wire.Query) { q.Page.Limit = 0 },
		"a limit past the cap": func(q *wire.Query) {
			q.Page.Limit = wire.MaxPageSize + 1
		},
		"an offset past the cap": func(q *wire.Query) {
			offset := uint64(wire.MaxQueryOffset + 1)
			q.Page.Offset = &offset
		},
		"an inverted time range": func(q *wire.Query) {
			q.TimeRange = &wire.TimeRange{Start: 2000, End: 1000}
		},
		"a having filter with no aggregate": func(q *wire.Query) {
			having := wire.AllOf(wire.Pred("a", wire.CmpEq, wire.IntTyped(1)))
			q.Having = &having
		},
		"an empty conjunction": func(q *wire.Query) {
			empty := wire.AllOf()
			q.Filter = &empty
		},
	} {
		t.Run(name, func(t *testing.T) {
			query := soundQuery()
			break_(&query)
			if err := query.Validate(); err == nil {
				t.Errorf("%s must be refused", name)
			}
		})
	}
}

// The four result shapes each order and bound their rows their own way, so
// two at once has no defined answer and a third is refused outright.
func TestGivenTwoResultShapesWhenValidatedThenShouldBeRefused(t *testing.T) {
	text := &wire.TextQuery{Query: "anomaly"}
	vector := &wire.VectorQuery{Field: "embedding", Embedding: []float32{0.5}, TopK: 5}

	both := soundQuery()
	both.Text, both.Vector = text, vector
	if err := both.Validate(); err == nil {
		t.Error("a query naming two result shapes must be refused")
	}

	// A text query ranks by relevance, so an explicit order contradicts it.
	ordered := soundQuery()
	ordered.Text = text
	ordered.Order = []wire.Sort{{Field: "at", Dir: wire.DirAsc}}
	if err := ordered.Validate(); err == nil {
		t.Error("a text query takes no order")
	}

	// A vector query returns its nearest rows, so an offset would skip the
	// nearest ones and still call the result nearest.
	offset := uint64(10)
	skipped := soundQuery()
	skipped.Vector = vector
	skipped.Page.Offset = &offset
	if err := skipped.Validate(); err == nil {
		t.Error("a vector query takes no offset")
	}
}

// Raw SQL replaces the structured expression rather than extending it, so a
// query carrying both has two answers and no rule for choosing.
func TestGivenRawSQLBesideTheStructuredFormWhenValidatedThenShouldBeRefused(t *testing.T) {
	alone := soundQuery()
	alone.RawSQL = &wire.RawSql{SQL: "SELECT 1"}
	if err := alone.Validate(); err != nil {
		t.Fatalf("raw SQL on its own validates: %v", err)
	}

	combined := soundQuery()
	combined.RawSQL = &wire.RawSql{SQL: "SELECT 1"}
	combined.Order = []wire.Sort{{Field: "at", Dir: wire.DirAsc}}
	if err := combined.Validate(); err == nil {
		t.Error("raw SQL beside a structured order must be refused")
	}

	blank := soundQuery()
	blank.RawSQL = &wire.RawSql{SQL: "   "}
	if err := blank.Validate(); err == nil {
		t.Error("an empty statement must be refused")
	}
}

// An aggregate names its result columns, so two calls cannot share an alias,
// and each function takes the arguments its own shape defines.
func TestGivenAnAggregateWhenValidatedThenShouldMatchEachFunctionsShape(t *testing.T) {
	field := "latency"
	fraction := 0.95
	sound := wire.Aggregate{Funcs: []wire.AggCall{
		{Func: wire.AggCount, Alias: "rows"},
		{Func: wire.AggPercentile, Field: &field, Arg: &fraction, Alias: "p95"},
	}}
	if err := sound.Validate(); err != nil {
		t.Fatalf("a sound aggregate validates: %v", err)
	}

	repeated := wire.Aggregate{Funcs: []wire.AggCall{
		{Func: wire.AggCount, Alias: "rows"},
		{Func: wire.AggCount, Alias: "rows"},
	}}
	if err := repeated.Validate(); err == nil {
		t.Error("two calls sharing one alias must be refused")
	}

	// A percentile without its fraction, or past the unit interval, names no
	// percentile at all.
	outside := 1.5
	for name, call := range map[string]wire.AggCall{
		"a percentile with no fraction": {Func: wire.AggPercentile, Field: &field, Alias: "p"},
		"a fraction past one": {
			Func: wire.AggPercentile, Field: &field, Arg: &outside, Alias: "p",
		},
		"a count with a field argument": {
			Func: wire.AggCount, Arg: &fraction, Alias: "rows",
		},
	} {
		if err := (wire.Aggregate{Funcs: []wire.AggCall{call}}).Validate(); err == nil {
			t.Errorf("%s must be refused", name)
		}
	}

	if err := (wire.Aggregate{}).Validate(); err == nil {
		t.Error("an aggregate with no function must be refused")
	}
}

// A value must have one canonical spelling, or two clients holding the same
// value would encode different bytes.
func TestGivenANonCanonicalValueWhenValidatedThenShouldBeRefused(t *testing.T) {
	if err := wire.LongTyped(7).Validate(); err != nil {
		t.Fatalf("a plain value validates: %v", err)
	}

	negativeZero := wire.DoubleTyped(math.Copysign(0, -1))
	if err := negativeZero.Validate(); err == nil {
		t.Error("a negative zero has two spellings and must be refused")
	}
	if err := wire.DoubleTyped(math.Inf(1)).Validate(); err == nil {
		t.Error("a non-finite value has no encoding and must be refused")
	}

	// A struct's fields are ordered by id, so unordered or repeated ids would
	// let one struct encode two ways.
	unordered := wire.TypedValue{Kind: wire.TypedStruct, Fields: []wire.FieldValue{
		{FieldID: 2, Value: wire.IntTyped(1)},
		{FieldID: 1, Value: wire.IntTyped(2)},
	}}
	if err := unordered.Validate(); err == nil {
		t.Error("struct fields out of id order must be refused")
	}

	// Map entries are ordered by their canonical key for the same reason.
	unsorted := wire.TypedValue{Kind: wire.TypedMap, MapValues: []wire.MapEntry{
		{Key: wire.StringTyped("b"), Value: wire.IntTyped(1)},
		{Key: wire.StringTyped("a"), Value: wire.IntTyped(2)},
	}}
	if err := unsorted.Validate(); err == nil {
		t.Error("map entries out of canonical key order must be refused")
	}

	// A floating key has no deterministic ordering across clients.
	floatKeyed := wire.TypedValue{Kind: wire.TypedMap, MapValues: []wire.MapEntry{
		{Key: wire.DoubleTyped(1.5), Value: wire.IntTyped(1)},
	}}
	if err := floatKeyed.Validate(); err == nil {
		t.Error("a floating map key must be refused")
	}

	// A decimal's unscaled bytes are minimal two's complement, so a leading
	// byte the next one's sign already implies is redundant.
	redundant := wire.TypedValue{Kind: wire.TypedDecimal, Decimal: &wire.DecimalValue{
		Precision: 10, Scale: 2, Unscaled: []byte{0x00, 0x01},
	}}
	if err := redundant.Validate(); err == nil {
		t.Error("non-minimal decimal bytes must be refused")
	}
}
