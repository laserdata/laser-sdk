package bdd_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	bdd "github.com/laserdata/laser-sdk/bdd/go"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// TestGivenTheSharedFilterScenariosWhenRunThenShouldHold runs the consumer
// filter evaluation feature.
//
// It reaches no deployment: these are the evaluation rules the streaming
// server and every client share, so the point is that this client's evaluator
// answers the same as the others on byte-identical records.
func TestGivenTheSharedFilterScenariosWhenRunThenShouldHold(t *testing.T) {
	runLocalFeature(t, "filters.feature", time.Minute, filterStep)
}

// recordFixtures is the shared record corpus. It is read from the scenarios
// directory rather than restated here, so every runner evaluates the same
// bytes.
type recordFixtures struct {
	Records map[string]struct {
		Text string `json:"text"`
		Hex  string `json:"hex"`
	} `json:"records"`
	Feed []string `json:"feed"`
}

func loadRecords() (map[string][]byte, error) {
	raw, err := os.ReadFile(filepath.Join(scenarios, "filter_records.json"))
	if err != nil {
		return nil, err
	}
	var fixtures recordFixtures
	if err := json.Unmarshal(raw, &fixtures); err != nil {
		return nil, err
	}
	records := make(map[string][]byte, len(fixtures.Records))
	for name, payload := range fixtures.Records {
		if payload.Hex != "" {
			decoded, err := hex.DecodeString(payload.Hex)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", name, err)
			}
			records[name] = decoded
			continue
		}
		records[name] = []byte(payload.Text)
	}
	return records, nil
}

// satellites is the predicate three branches of the strict filter share.
func satellites() wire.FilterExpr {
	return wire.ExprPredicate("table", wire.CmpEq, wire.StringTyped("satellites"))
}

// namedFilters are the filters the scenarios call for by name, spelled the
// same way as in the other runners so a verdict that differs is a difference
// in the evaluator rather than in the filter.
func namedFilters() (map[string]wire.ConsumerFilter, error) {
	present, err := wire.ExprPresentPath("fields.mode")
	if err != nil {
		return nil, err
	}
	safeMode := wire.ExprAnyOf(
		wire.ExprAllOf(
			satellites(),
			wire.ExprPredicate("op", wire.CmpEq, wire.StringTyped("u")),
			wire.ExprPredicate("changed", wire.CmpContains, wire.StringTyped("mode")),
			wire.ExprPredicate("after.mode", wire.CmpEq, wire.StringTyped("safe")),
		),
		wire.ExprAllOf(
			satellites(),
			wire.ExprPredicate("op", wire.CmpEq, wire.StringTyped("d")),
		),
		wire.ExprAllOf(
			wire.ExprPredicate("event", wire.CmpEq,
				wire.StringTyped("satellite.telemetry_changed")),
			wire.ExprPredicate("fields.mode", wire.CmpEq, wire.StringTyped("safe")),
		),
	)
	return map[string]wire.ConsumerFilter{
		"mode present": wire.NewConsumerFilter(present, wire.CodecJSON),
		"safe mode":    wire.NewConsumerFilter(safeMode, wire.CodecJSON),
		"safe mode values": wire.NewConsumerFilter(wire.ExprAllOf(
			satellites(),
			wire.ExprPredicate("op", wire.CmpEq, wire.StringTyped("u")),
			wire.ExprPredicate("after.mode", wire.CmpEq, wire.StringTyped("safe")),
		), wire.CodecJSON),
		"mode is not safe": wire.NewConsumerFilter(wire.ExprNegate(
			wire.ExprPredicate("after.mode", wire.CmpEq, wire.StringTyped("safe")),
		), wire.CodecJSON),
		// The catalog number is past what a float64 holds exactly, which is
		// the point of the scenario: it must compare as the integer it is.
		"catalog number 9007199254740993": wire.NewConsumerFilter(
			wire.ExprPredicate("after.norad_id", wire.CmpEq,
				wire.LongTyped(9007199254740993)), wire.CodecJSON),
		"contact after noon UTC": wire.NewConsumerFilter(
			wire.ExprCoerced("contact_at", wire.CmpGt,
				wire.StringTyped("2026-09-21T12:00:00Z"),
				wire.TimestampCoercion(wire.FormatRFC3339)), wire.CodecJSON),
		"critical priority": wire.HeadersOnlyFilter(
			wire.ExprHeaderPredicate("priority", wire.CmpEq,
				wire.StringTyped("critical"))),
	}, nil
}

func filterStep(_ context.Context, state *world, step bdd.Step) error {
	text := step.Text
	arguments := argumentsOf(step)

	switch {
	case strings.HasPrefix(text, "the \"") && strings.HasSuffix(text, "\" filter"):
		return chooseFilter(state, arguments)

	case strings.HasPrefix(text, "it evaluates the"):
		return evaluateRecord(state, text, arguments)

	case strings.HasPrefix(text, "the record is "):
		return checkVerdict(state, strings.TrimPrefix(text, "the record is "))

	case text == "its digest survives a round trip through its wire form":
		return digestRoundTrip(state)

	case text == "a different fault policy gives a different digest":
		return digestSeparatesFaultPolicy(state)

	default:
		return fmt.Errorf("this runner does not implement the step")
	}
}

func chooseFilter(state *world, arguments []string) error {
	if len(arguments) != 1 {
		return fmt.Errorf("want one filter name, got %v", arguments)
	}
	filters, err := namedFilters()
	if err != nil {
		return err
	}
	filter, found := filters[arguments[0]]
	if !found {
		return fmt.Errorf("no fixture filter named %q", arguments[0])
	}
	state.put("filter", filter)
	return nil
}

func evaluateRecord(state *world, text string, arguments []string) error {
	if len(arguments) == 0 {
		return fmt.Errorf("want a record name, got none")
	}
	records, err := loadRecords()
	if err != nil {
		return err
	}
	payload, found := records[arguments[0]]
	if !found {
		return fmt.Errorf("no fixture record named %q", arguments[0])
	}
	var headers []wire.FilterHeader
	if strings.Contains(text, "with header") {
		if len(arguments) != 3 {
			return fmt.Errorf("want a record, a header and a value, got %v", arguments)
		}
		headers = []wire.FilterHeader{{
			Key:   arguments[1],
			Value: wire.StringHeaderScalar(arguments[2]),
		}}
	}
	filter, err := currentFilter(state)
	if err != nil {
		return err
	}
	compiled, err := wire.CompileFilter(filter)
	if err != nil {
		return err
	}
	verdict := compiled.Evaluate(
		wire.FilterRecord{Payload: payload, Headers: headers},
		wire.DefaultDecodeLimits(),
	)
	state.put("verdict", verdict)
	return nil
}

func currentFilter(state *world) (wire.ConsumerFilter, error) {
	value, found := state.get("filter")
	if !found {
		return wire.ConsumerFilter{}, fmt.Errorf("the scenario chose no filter")
	}
	return value.(wire.ConsumerFilter), nil
}

func checkVerdict(state *world, want string) error {
	value, found := state.get("verdict")
	if !found {
		return fmt.Errorf("the scenario evaluated no record")
	}
	// The scenarios spell the fault verdict "a fault", because the sentence
	// reads as English rather than as the wire word.
	expected := wire.Verdict(want)
	if want == "a fault" {
		expected = wire.VerdictFault
	}
	if got := value.(wire.Verdict); got != expected {
		return fmt.Errorf("want %q, got %q", expected, got)
	}
	return nil
}

// digestRoundTrip proves the digest identifies the filter's semantics rather
// than the bytes one encoder happened to produce.
func digestRoundTrip(state *world) error {
	filter, err := currentFilter(state)
	if err != nil {
		return err
	}
	encoded, err := wire.Marshal(filter)
	if err != nil {
		return err
	}
	var back wire.ConsumerFilter
	if err := wire.Unmarshal(encoded, &back); err != nil {
		return err
	}
	before, err := filter.Digest()
	if err != nil {
		return err
	}
	after, err := back.Digest()
	if err != nil {
		return err
	}
	if !bytes.Equal(before.Bytes, after.Bytes) {
		return fmt.Errorf("the digest changed over a round trip")
	}
	return nil
}

// digestSeparatesFaultPolicy proves the digest covers everything that changes
// what the filter selects, not only its expression.
func digestSeparatesFaultPolicy(state *world) error {
	filter, err := currentFilter(state)
	if err != nil {
		return err
	}
	dropping := wire.NewConsumerFilter(filter.Expr, filter.Codec)
	dropping.FaultPolicy = wire.FaultDrop
	if dropping.FaultPolicy == filter.FaultPolicy {
		return fmt.Errorf("the scenario needs two different fault policies")
	}
	before, err := filter.Digest()
	if err != nil {
		return err
	}
	after, err := dropping.Digest()
	if err != nil {
		return err
	}
	if bytes.Equal(before.Bytes, after.Bytes) {
		return fmt.Errorf("a different fault policy kept the same digest")
	}
	return nil
}
