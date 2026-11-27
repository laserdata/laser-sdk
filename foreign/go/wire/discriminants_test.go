package wire_test

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// The string vocabularies every client spells identically.
//
// These are the values that ride the wire as text, so a misspelled constant
// here is a record another client cannot read, and a missing one is a value
// this build would refuse. The fixture lists what the contract names, and the
// test asserts this port names exactly that, in order.
func TestGivenTheStringDiscriminantCorpusWhenComparedThenShouldNameEveryValue(t *testing.T) {
	var corpus map[string][]string
	if err := wire.Unmarshal(
		readFixture(t, "data_stack_string_discriminants.bin"), &corpus); err != nil {
		t.Fatalf("decode the corpus: %v", err)
	}

	ported := map[string][]string{
		"arrow_timestamp_units": names(wire.ArrowMicrosecond),
		"arrow_rejections": names(
			wire.ArrowRejectFileFormat, wire.ArrowRejectMissingSchema,
			wire.ArrowRejectMissingDictionary, wire.ArrowRejectDictionaryDelta,
			wire.ArrowRejectDictionaryReplacement, wire.ArrowRejectUnion,
			wire.ArrowRejectExtensionType, wire.ArrowRejectTimestampUnit,
			wire.ArrowRejectDecimalWidth, wire.ArrowRejectSchemaFingerprint,
			wire.ArrowRejectFieldLimit, wire.ArrowRejectBatchLimit,
			wire.ArrowRejectRowLimit, wire.ArrowRejectByteLimit,
			wire.ArrowRejectMalformedStream),
		"comparison_operators": names(
			wire.CmpEq, wire.CmpNe, wire.CmpLt, wire.CmpLte, wire.CmpGt,
			wire.CmpGte, wire.CmpIn, wire.CmpContains, wire.CmpPrefix),
		"sort_directions": names(wire.DirAsc, wire.DirDesc),
		"aggregate_functions": names(
			wire.AggCount, wire.AggCountDistinct, wire.AggSum, wire.AggAvg,
			wire.AggMin, wire.AggMax, wire.AggPercentile, wire.AggStdDev),
		// The corpus omits schema_not_found, which the contract names but its
		// fixture generator does not list.
		"query_error_codes": names(
			wire.QueryCodeUnsupported, wire.QueryCodeInvalid,
			wire.QueryCodeUnauthorized, wire.QueryCodeIndexNotFound,
			wire.QueryCodeForkNotFound, wire.QueryCodeBackend,
			wire.QueryCodeUnavailable, wire.QueryCodeTooLarge,
			wire.QueryCodeVersion, wire.QueryCodeStale,
			wire.QueryCodeDeadlineExceeded, wire.QueryCodeStaleGeneration),
		"logical_type_kinds": names(
			wire.KindBoolean, wire.KindInt, wire.KindLong, wire.KindFloat,
			wire.KindDouble, wire.KindDecimal, wire.KindDate,
			wire.KindTimeMicros, wire.KindTimestampMicros,
			wire.KindTimestampTzMicros, wire.KindString, wire.KindUUID,
			wire.KindFixed, wire.KindBinary, wire.KindStruct, wire.KindList,
			wire.KindMap),
		"sql_dialects": names(
			wire.DialectPostgres, wire.DialectMySQL, wire.DialectSQLite),
		"query_consistency": names(
			wire.ConsistencyEventual, wire.ConsistencyReadYourWrites),
		"backend_modes": names(wire.BackendOperational),
		"backend_desired_states": names(
			wire.BackendDisabledState, wire.BackendEnabledState),
		"backend_observed_states": names(
			wire.BackendObservedDisabled, wire.BackendObservedStarting,
			wire.BackendObservedReady, wire.BackendObservedDegraded,
			wire.BackendObservedUnavailable),
		"backend_readiness_codes": names(
			wire.ReadinessDisabled, wire.ReadinessConfigurationPending,
			wire.ReadinessConfigurationRejected, wire.ReadinessCredentialUnavailable,
			wire.ReadinessObjectStoreUnavailable, wire.ReadinessCatalogUnavailable,
			wire.ReadinessQueryRuntimeUnavailable, wire.ReadinessGenerationMismatch,
			wire.ReadinessProbeFailed, wire.ReadinessQueryIdentityRejected),
	}

	for vocabulary, want := range corpus {
		got, checked := ported[vocabulary]
		if !checked {
			t.Errorf("the corpus names %q, which this test does not check", vocabulary)
			continue
		}
		if len(got) != len(want) {
			t.Errorf("%s: this build names %d values, the contract names %d:\n"+
				" got %v\nwant %v", vocabulary, len(got), len(want), got, want)
			continue
		}
		for index := range want {
			if got[index] != want[index] {
				t.Errorf("%s[%d] is %q, the contract names %q",
					vocabulary, index, got[index], want[index])
			}
		}
	}

	// Every vocabulary the test claims to check must exist in the fixture, so
	// a renamed key upstream cannot silently stop checking one.
	for vocabulary := range ported {
		if _, present := corpus[vocabulary]; !present {
			t.Errorf("the corpus has no vocabulary named %q", vocabulary)
		}
	}
}

// names renders a list of string-backed constants for comparison.
func names[T ~string](values ...T) []string {
	rendered := make([]string, len(values))
	for index, value := range values {
		rendered[index] = string(value)
	}
	return rendered
}
