package wire_test

import (
	"encoding/json"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// classificationCorpus mirrors wire/fixtures/error_classification_cases.json:
// the shared corpus that pins how every client classifies a failure.
type classificationCorpus struct {
	Classifiers []string `json:"classifiers"`
	Cases       []struct {
		Name    string                     `json:"name"`
		Failure map[string]json.RawMessage `json:"failure"`
		Code    string                     `json:"code"`
		Flags   []string                   `json:"flags"`
	} `json:"cases"`
}

// The surfaces whose typed errors this port carries. The corpus also covers
// the forms a client meets a failure in outside the contract, such as a
// transport error or a local validation failure, which belong to an SDK this
// port does not have yet.
var classifiedSurfaces = map[string]func(*testing.T, []byte) classifiedFailure{
	"query":   decodeQueryFailure,
	"kv":      decodeKvFailure,
	"fork":    decodeForkFailure,
	"graph":   decodeGraphFailure,
	"session": decodeSessionFailure,
	"authz":   decodeAuthzFailure,
	"filter":  decodeFilterFailure,
	"command": decodeCommandFailure,
}

// classifiedFailure is one decoded failure and the two classifications a
// client dispatches on: the shared result code, and whether the typed error
// names a condition the code alone does not.
type classifiedFailure struct {
	code wire.ResultCode
	// The three conditions the shared code cannot tell apart from their
	// neighbours: a lost lease, a conditional-write conflict and a wrong
	// leader all answer a code another failure answers too.
	leaseLost       bool
	notLeader       bool
	versionConflict bool
}

// The classification corpus is behaviour, not encoding: the same failure must
// reach the same code and the same retry decision in every client. A
// divergence here means a Go caller retries a failure a Rust caller reports,
// or reports one a Rust caller retries.
func TestGivenTheClassificationCorpusWhenClassifiedThenShouldMatchEveryVerdict(t *testing.T) {
	var corpus classificationCorpus
	if err := json.Unmarshal(
		readFixture(t, "error_classification_cases.json"), &corpus); err != nil {
		t.Fatalf("decode the corpus: %v", err)
	}
	if len(corpus.Cases) == 0 {
		t.Fatal("the corpus lists cases")
	}

	covered := 0
	for _, testCase := range corpus.Cases {
		if len(testCase.Failure) != 1 {
			t.Fatalf("%s names %d failures, want one", testCase.Name, len(testCase.Failure))
		}
		var surface string
		var payload json.RawMessage
		for name, raw := range testCase.Failure {
			surface, payload = name, raw
		}
		decode, ported := classifiedSurfaces[surface]
		if !ported {
			continue
		}
		covered++

		t.Run(testCase.Name, func(t *testing.T) {
			failure := decode(t, payload)
			if got := failure.code.String(); got != wireSpelling(testCase.Code) {
				t.Errorf("want the code %s, got %s", testCase.Code, got)
			}

			want := make(map[string]bool, len(testCase.Flags))
			for _, flag := range testCase.Flags {
				want[flag] = true
			}
			for _, classifier := range corpus.Classifiers {
				answer, known := failure.classify(classifier)
				if !known {
					// A classifier this port cannot answer must not be
					// claimed on a surface it does carry.
					if want[classifier] {
						t.Errorf("the corpus expects %s, which this port does not classify",
							classifier)
					}
					continue
				}
				if answer != want[classifier] {
					t.Errorf("%s: want %v, got %v", classifier, want[classifier], answer)
				}
			}
		})
	}

	if covered == 0 {
		t.Fatal("no corpus case reached a ported surface")
	}
}

// classify answers one named classifier. The second result is false for a
// classifier that reads an SDK-level failure form this port does not carry.
func (failure classifiedFailure) classify(name string) (bool, bool) {
	switch name {
	case "retryable":
		return failure.code.IsRetryable(), true
	case "unavailable":
		return failure.code.IsUnavailable(), true
	case "unsupported":
		return failure.code == wire.ResultUnsupported, true
	case "not_found":
		return failure.code == wire.ResultNotFound, true
	case "version_skew":
		return failure.code == wire.ResultVersionSkew, true
	case "permission_denied":
		return failure.code.IsPermissionDenied(), true
	case "stale":
		return failure.code == wire.ResultStale, true
	case "version_conflict":
		return failure.versionConflict, true
	case "lease_lost":
		return failure.leaseLost, true
	case "not_leader":
		return failure.notLeader, true
	default:
		return false, false
	}
}

func decodeQueryFailure(t *testing.T, payload []byte) classifiedFailure {
	t.Helper()
	var failure wire.QueryError
	decodeFailureInto(t, payload, &failure)
	return classifiedFailure{code: failure.ResultCode()}
}

func decodeKvFailure(t *testing.T, payload []byte) classifiedFailure {
	t.Helper()
	var failure wire.KvError
	decodeFailureInto(t, payload, &failure)
	return classifiedFailure{
		code:            failure.ResultCode(),
		leaseLost:       failure.Kind == wire.KvLeaseLost,
		notLeader:       failure.Kind == wire.KvNotLeader,
		versionConflict: failure.Kind == wire.KvVersionConflict,
	}
}

func decodeForkFailure(t *testing.T, payload []byte) classifiedFailure {
	t.Helper()
	var failure wire.ForkError
	decodeFailureInto(t, payload, &failure)
	return classifiedFailure{
		code:      failure.ResultCode(),
		notLeader: failure.Kind == wire.ForkNotLeader,
	}
}

func decodeGraphFailure(t *testing.T, payload []byte) classifiedFailure {
	t.Helper()
	var failure wire.GraphError
	decodeFailureInto(t, payload, &failure)
	return classifiedFailure{code: failure.ResultCode()}
}

func decodeSessionFailure(t *testing.T, payload []byte) classifiedFailure {
	t.Helper()
	var failure wire.SessionError
	decodeFailureInto(t, payload, &failure)
	return classifiedFailure{code: failure.ResultCode()}
}

func decodeAuthzFailure(t *testing.T, payload []byte) classifiedFailure {
	t.Helper()
	var failure wire.AuthzError
	decodeFailureInto(t, payload, &failure)
	return classifiedFailure{code: failure.ResultCode()}
}

func decodeFilterFailure(t *testing.T, payload []byte) classifiedFailure {
	t.Helper()
	var failure wire.FilterError
	decodeFailureInto(t, payload, &failure)
	return classifiedFailure{code: failure.ResultCode()}
}

func decodeCommandFailure(t *testing.T, payload []byte) classifiedFailure {
	t.Helper()
	var failure wire.CommandError
	decodeFailureInto(t, payload, &failure)
	return classifiedFailure{code: failure.Code}
}

func decodeFailureInto(t *testing.T, payload []byte, target any) {
	t.Helper()
	if err := wire.UnmarshalJSON(payload, target); err != nil {
		t.Fatalf("decode %s: %v", payload, err)
	}
}

// Every classifier the corpus names must be either answered or deliberately
// left to an SDK this port does not have, so a new one upstream is noticed
// rather than silently passing as unanswered.
func TestGivenEveryCorpusClassifierWhenCheckedThenShouldBeAnsweredOrDeclared(t *testing.T) {
	var corpus classificationCorpus
	if err := json.Unmarshal(
		readFixture(t, "error_classification_cases.json"), &corpus); err != nil {
		t.Fatalf("decode the corpus: %v", err)
	}

	// The classifiers that read a failure form only an SDK meets: a publish
	// that failed after its retries, a typed decode, a consumer-group setup,
	// and the agent runtime's routing verdicts.
	awaitingTheSDK := map[string]bool{
		"ambiguous_mutation":        true,
		"fence_violation":           true,
		"budget_exceeded":           true,
		"quarantined":               true,
		"no_capable_agent":          true,
		"stream_or_topic_not_found": true,
	}

	var unanswered []string
	for _, classifier := range corpus.Classifiers {
		if _, known := (classifiedFailure{}).classify(classifier); known {
			continue
		}
		if awaitingTheSDK[classifier] {
			continue
		}
		unanswered = append(unanswered, classifier)
	}
	sort.Strings(unanswered)
	if len(unanswered) != 0 {
		t.Errorf("the corpus names classifiers this port neither answers nor defers: %v",
			unanswered)
	}
}

// corpusCodeWord splits a corpus code name into its words, including the
// numeric argument the passthrough carries.
var corpusCodeWord = regexp.MustCompile(`[A-Z][a-z0-9]*|\d+`)

// wireSpelling renders a corpus code name the way this port spells it.
//
// The corpus names a code by its contract variant, where the wire and every
// client render it in snake_case, so the two are compared in one spelling
// rather than each test carrying both.
func wireSpelling(name string) string {
	if open := strings.IndexByte(name, '('); open >= 0 {
		return strings.ToLower(name[:open]) + name[open:]
	}
	return strings.ToLower(strings.Join(corpusCodeWord.FindAllString(name, -1), "_"))
}
