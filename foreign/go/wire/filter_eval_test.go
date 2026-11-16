package wire_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// evalCorpus mirrors wire/fixtures/filter_eval_cases.json: the shared verdict
// corpus every client's evaluator must agree on.
type evalCorpus struct {
	EvaluatorVersion uint32 `json:"evaluator_version"`
	Limits           struct {
		MaxPayloadBytes int `json:"max_payload_bytes"`
		MaxDepth        int `json:"max_depth"`
	} `json:"limits"`
	Cases []evalCase `json:"cases"`
}

type evalCase struct {
	Name     string       `json:"name"`
	Filter   jsonFilter   `json:"filter"`
	Payload  string       `json:"payload"`
	Headers  []jsonHeader `json:"headers"`
	Expected string       `json:"expected"`
	Fault    *string      `json:"fault"`
}

// The corpus writes the contract types in their serde JSON form, which the Go
// types carry in their CBOR form, so each one is read explicitly here. A field
// read wrongly would make a case pass for the wrong reason.
type jsonFilter struct {
	V                uint32   `json:"v"`
	EvaluatorVersion uint32   `json:"evaluator_version"`
	Expr             jsonExpr `json:"expr"`
	Codec            string   `json:"codec"`
	FaultPolicy      string   `json:"fault_policy"`
	ForeignPolicy    string   `json:"foreign_policy"`
	MismatchPolicy   string   `json:"mismatch_policy"`
	SchemaRefs       []uint32 `json:"schema_refs"`
	SchemaStream     *string  `json:"schema_stream"`
}

// jsonExpr is the externally tagged expression: exactly one field is set.
type jsonExpr struct {
	All        []jsonExpr    `json:"all"`
	Any        []jsonExpr    `json:"any"`
	Not        *jsonExpr     `json:"not"`
	Pred       *jsonPred     `json:"pred"`
	PredAs     *jsonCoerced  `json:"pred_as"`
	Present    *string       `json:"present"`
	Absent     *string       `json:"absent"`
	Header     *jsonHeaderOp `json:"header"`
	Text       *jsonText     `json:"text"`
	HeaderText *jsonText     `json:"header_text"`
}

type jsonPred struct {
	Field string    `json:"field"`
	Op    string    `json:"op"`
	Value jsonTyped `json:"value"`
}

type jsonCoerced struct {
	Pred   jsonPred `json:"pred"`
	Coerce struct {
		Kind   string `json:"kind"`
		Format string `json:"format"`
	} `json:"coerce"`
}

type jsonHeaderOp struct {
	Key   string    `json:"key"`
	Op    string    `json:"op"`
	Value jsonTyped `json:"value"`
}

type jsonText struct {
	Field           string `json:"field"`
	Kind            string `json:"kind"`
	Pattern         string `json:"pattern"`
	CaseInsensitive bool   `json:"case_insensitive"`
}

// jsonTyped is an adjacently tagged typed value: a kind and its payload.
type jsonTyped struct {
	Kind  string          `json:"kind"`
	Value json.RawMessage `json:"value"`
}

type jsonHeader struct {
	Key   string    `json:"key"`
	Value jsonTyped `json:"value"`
}

func (value jsonTyped) typed(t *testing.T) wire.TypedValue {
	t.Helper()
	switch value.Kind {
	case "null":
		return wire.NullTyped()
	case "boolean":
		var flag bool
		decodeTyped(t, value.Value, &flag)
		return wire.BooleanTyped(flag)
	case "int":
		var number int32
		decodeTyped(t, value.Value, &number)
		return wire.IntTyped(number)
	case "long":
		var number int64
		decodeTyped(t, value.Value, &number)
		return wire.LongTyped(number)
	case "double":
		var number float64
		decodeTyped(t, value.Value, &number)
		return wire.DoubleTyped(number)
	case "string":
		var text string
		decodeTyped(t, value.Value, &text)
		return wire.StringTyped(text)
	case "list":
		var items []jsonTyped
		decodeTyped(t, value.Value, &items)
		values := make([]wire.TypedValue, len(items))
		for index, item := range items {
			values[index] = item.typed(t)
		}
		return wire.ListTyped(values...)
	default:
		t.Fatalf("the corpus uses an unported typed value kind %q", value.Kind)
		return wire.TypedValue{}
	}
}

func (value jsonTyped) headerScalar(t *testing.T) wire.HeaderScalar {
	t.Helper()
	switch value.Kind {
	case "bool":
		var flag bool
		decodeTyped(t, value.Value, &flag)
		return wire.BoolHeaderScalar(flag)
	case "int":
		var number int64
		decodeTyped(t, value.Value, &number)
		return wire.IntHeaderScalar(number)
	case "uint":
		var number uint64
		decodeTyped(t, value.Value, &number)
		return wire.UintHeaderScalar(number)
	case "float":
		var number float64
		decodeTyped(t, value.Value, &number)
		return wire.FloatHeaderScalar(number)
	case "string":
		var text string
		decodeTyped(t, value.Value, &text)
		return wire.StringHeaderScalar(text)
	case "raw":
		var octets []byte
		decodeTyped(t, value.Value, &octets)
		return wire.RawHeaderScalar(octets)
	default:
		t.Fatalf("the corpus uses an unported header scalar kind %q", value.Kind)
		return wire.HeaderScalar{}
	}
}

func decodeTyped(t *testing.T, raw json.RawMessage, into any) {
	t.Helper()
	if err := json.Unmarshal(raw, into); err != nil {
		t.Fatalf("decode %s as %T: %v", raw, into, err)
	}
}

func (expr jsonExpr) expression(t *testing.T) wire.FilterExpr {
	t.Helper()
	switch {
	case expr.All != nil:
		return wire.ExprAllOf(expr.children(t, expr.All)...)
	case expr.Any != nil:
		return wire.ExprAnyOf(expr.children(t, expr.Any)...)
	case expr.Not != nil:
		return wire.ExprNegate(expr.Not.expression(t))
	case expr.Pred != nil:
		return wire.ExprPredicate(expr.Pred.Field, wire.CmpOp(expr.Pred.Op),
			expr.Pred.Value.typed(t))
	case expr.PredAs != nil:
		coerce := wire.NumberCoercion()
		if expr.PredAs.Coerce.Kind == "timestamp" {
			coerce = wire.TimestampCoercion(wire.TimestampFormat(expr.PredAs.Coerce.Format))
		}
		return wire.ExprCoerced(expr.PredAs.Pred.Field, wire.CmpOp(expr.PredAs.Pred.Op),
			expr.PredAs.Pred.Value.typed(t), coerce)
	case expr.Present != nil:
		return presenceExpr(t, wire.ExprPresentPath, *expr.Present)
	case expr.Absent != nil:
		return presenceExpr(t, wire.ExprAbsentPath, *expr.Absent)
	case expr.Header != nil:
		return wire.ExprHeaderPredicate(expr.Header.Key, wire.CmpOp(expr.Header.Op),
			expr.Header.Value.typed(t))
	case expr.Text != nil:
		return expr.Text.build(wire.ExprTextMatch)
	case expr.HeaderText != nil:
		return expr.HeaderText.build(wire.ExprHeaderTextMatch)
	default:
		t.Fatal("the corpus holds an expression with no variant")
		return wire.FilterExpr{}
	}
}

func (expr jsonExpr) children(t *testing.T, items []jsonExpr) []wire.FilterExpr {
	t.Helper()
	children := make([]wire.FilterExpr, len(items))
	for index, item := range items {
		children[index] = item.expression(t)
	}
	return children
}

func (text jsonText) build(
	make func(string, wire.TextMatch, string) wire.FilterExpr,
) wire.FilterExpr {
	expr := make(text.Field, wire.TextMatch(text.Kind), text.Pattern)
	if text.CaseInsensitive {
		expr = expr.CaseInsensitive()
	}
	return expr
}

func presenceExpr(
	t *testing.T, make func(string) (wire.FilterExpr, error), path string,
) wire.FilterExpr {
	t.Helper()
	expr, err := make(path)
	if err != nil {
		t.Fatalf("parse the field path %q: %v", path, err)
	}
	return expr
}

func (filter jsonFilter) consumerFilter(t *testing.T) wire.ConsumerFilter {
	t.Helper()
	built := wire.ConsumerFilter{
		V:                filter.V,
		EvaluatorVersion: filter.EvaluatorVersion,
		Expr:             filter.Expr.expression(t),
		Codec:            wire.FilterCodec(filter.Codec),
		FaultPolicy:      wire.FaultPolicy(filter.FaultPolicy),
		ForeignPolicy:    wire.RecordPolicy(filter.ForeignPolicy),
		MismatchPolicy:   wire.RecordPolicy(filter.MismatchPolicy),
		SchemaRefs:       filter.SchemaRefs,
		SchemaStream:     filter.SchemaStream,
	}
	// An absent policy is the reject default, which is what an omitted field
	// means on the wire.
	if built.ForeignPolicy == "" {
		built.ForeignPolicy = wire.PolicyReject
	}
	if built.MismatchPolicy == "" {
		built.MismatchPolicy = wire.PolicyReject
	}
	return built
}

// The evaluation corpus is behaviour, not encoding: every client must reach
// the same verdict for the same record under the same filter. A divergence
// here means a Go consumer delivers a record a Rust consumer skips, or skips
// one a Rust consumer delivers.
func TestGivenTheFilterEvalCorpusWhenEvaluatedThenShouldMatchEveryVerdict(t *testing.T) {
	var corpus evalCorpus
	if err := json.Unmarshal(readFixture(t, "filter_eval_cases.json"), &corpus); err != nil {
		t.Fatalf("decode the corpus: %v", err)
	}
	if len(corpus.Cases) == 0 {
		t.Fatal("the corpus lists cases")
	}
	if corpus.EvaluatorVersion != wire.FilterEvaluatorVersion {
		t.Fatalf("the corpus is written for evaluator version %d, this build is %d",
			corpus.EvaluatorVersion, wire.FilterEvaluatorVersion)
	}
	limits := wire.DecodeLimits{
		MaxPayloadBytes: corpus.Limits.MaxPayloadBytes,
		MaxDepth:        corpus.Limits.MaxDepth,
	}

	for _, testCase := range corpus.Cases {
		t.Run(testCase.Name, func(t *testing.T) {
			compiled, err := wire.CompileFilter(testCase.Filter.consumerFilter(t))
			if err != nil {
				t.Fatalf("compile the filter: %v", err)
			}
			headers := make([]wire.FilterHeader, len(testCase.Headers))
			for index, header := range testCase.Headers {
				headers[index] = wire.FilterHeader{
					Key: header.Key, Value: header.Value.headerScalar(t),
				}
			}
			record := wire.FilterRecord{Payload: []byte(testCase.Payload), Headers: headers}

			verdict, fault := compiled.EvaluateWithFault(record, limits)
			if string(verdict) != testCase.Expected {
				t.Errorf("verdict is %q, the corpus expects %q", verdict, testCase.Expected)
			}
			// A corpus case that names a fault pins the reason too, so a
			// record rejected for the wrong reason is caught.
			if testCase.Fault != nil && string(fault) != *testCase.Fault {
				t.Errorf("fault is %q, the corpus expects %q", fault, *testCase.Fault)
			}

			// Evaluate agrees with EvaluateWithFault, and an explanation
			// reports the same verdict as the plain evaluation.
			if again := compiled.Evaluate(record, limits); again != verdict {
				t.Errorf("Evaluate says %q where EvaluateWithFault says %q", again, verdict)
			}
			explanation := compiled.Explain(record, limits)
			if explanation.Verdict != verdict {
				t.Errorf("the explanation says %q where the evaluation says %q",
					explanation.Verdict, verdict)
			}
			if explanation.Root.Label == "" {
				t.Error("every explanation node names what it tested")
			}
		})
	}
}

// A compiled filter is immutable, so a consumer may evaluate one from several
// goroutines without serializing on it.
func TestGivenOneCompiledFilterWhenEvaluatedConcurrentlyThenShouldAgree(t *testing.T) {
	filter := wire.NewConsumerFilter(
		wire.ExprPredicate("mode", wire.CmpEq, wire.StringTyped("safe")), wire.CodecJSON)
	compiled, err := wire.CompileFilter(filter)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	limits := wire.DefaultDecodeLimits()

	const workers = 8
	results := make(chan string, workers)
	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			payload := fmt.Sprintf(`{"mode":"safe","worker":%d}`, worker)
			record := wire.FilterRecord{Payload: []byte(payload)}
			results <- string(compiled.Evaluate(record, limits))
		}(worker)
	}
	for worker := 0; worker < workers; worker++ {
		if verdict := <-results; verdict != string(wire.VerdictSelected) {
			t.Fatalf("a concurrent evaluation said %q", verdict)
		}
	}
}
