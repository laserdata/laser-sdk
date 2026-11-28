package laser_test

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"testing"

	ierror "github.com/apache/iggy/foreign/go/errors"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenEveryFailureWhenAskedForAMissingResourceThenShouldNameOnlyTheNotFoundOnes(t *testing.T) {
	assertClassified(t, laser.IsNotFound,
		"transport stream id missing", "transport topic name missing", "transport resource missing",
		"kv not found", "query index not found", "fork not found", "graph not found",
		"authz unknown role", "session not found", "filter not found", "command not found",
		"no capable agent", "no inbox")
}

func TestGivenEveryFailureWhenAskedForAMissingStreamOrTopicThenShouldNameOnlyTheServerRefusals(t *testing.T) {
	assertClassified(t, laser.IsStreamOrTopicNotFound,
		"transport stream id missing", "transport topic name missing")
}

func TestGivenEveryFailureWhenAskedForARefusedCredentialThenShouldNameForbiddenAndUnauthenticated(t *testing.T) {
	assertClassified(t, laser.IsPermissionDenied,
		"transport unauthorized", "transport unauthenticated", "kv forbidden", "query unauthorized",
		"graph unauthorized", "authz unauthorized", "session unauthorized", "command forbidden",
		"signature", "quarantined", "policy blocked", "route principal mismatch", "edge denial")
}

func TestGivenEveryFailureWhenAskedForATransientOutageThenShouldNameOnlyTheUnavailableOnes(t *testing.T) {
	assertClassified(t, laser.IsUnavailable,
		"transport disconnected", "kv unavailable", "kv not leader", "query unavailable",
		"fork unavailable", "fork not leader", "graph unavailable", "session unavailable",
		"command backend unavailable")
}

func TestGivenEveryFailureWhenAskedForALostLeaderThenShouldNameOnlyKvAndForkNotLeader(t *testing.T) {
	assertClassified(t, laser.IsNotLeader, "kv not leader", "fork not leader")
}

func TestGivenEveryFailureWhenAskedForALaggingReadThenShouldNameOnlyTheStaleOnes(t *testing.T) {
	assertClassified(t, laser.IsStale, "kv stale", "query stale")
}

func TestGivenEveryFailureWhenAskedForALostPreconditionThenShouldNameOnlyTheKvVersionConflict(t *testing.T) {
	assertClassified(t, laser.IsVersionConflict, "kv version conflict")
}

func TestGivenEveryFailureWhenAskedForVersionSkewThenShouldNameOnlyTheVersionRefusals(t *testing.T) {
	assertClassified(t, laser.IsVersionSkew,
		"kv version mismatch", "query version mismatch", "fork version mismatch",
		"graph version mismatch", "authz version mismatch", "pinned version")
}

func TestGivenEveryFailureWhenAskedForALostLeaseThenShouldNameOnlyTheKvLeaseLost(t *testing.T) {
	assertClassified(t, laser.IsLeaseLost, "kv lease lost")
}

func TestGivenEveryFailureWhenAskedForALostFenceThenShouldNameOnlyTheFenceViolation(t *testing.T) {
	assertClassified(t, laser.IsFenceViolation, "fence violation")
}

func TestGivenEveryFailureWhenAskedForAQuarantineThenShouldNameOnlyTheQuarantinedAgent(t *testing.T) {
	assertClassified(t, laser.IsQuarantined, "quarantined")
}

func TestGivenEveryFailureWhenAskedForItsResultCodeThenShouldAgreeWithTheBroadClassifiers(t *testing.T) {
	if code := laser.ResultCode(nil); code != wire.ResultOk {
		t.Errorf("no failure: want %v, got %v", wire.ResultOk, code)
	}
	for name, failure := range failureSamples(t) {
		for _, wrapped := range []error{failure, fmt.Errorf("publish: %w", failure)} {
			code := laser.ResultCode(wrapped)
			if got, want := laser.IsNotFound(wrapped), code == wire.ResultNotFound; got != want {
				t.Errorf("%s: not found is %v under code %v", name, got, code)
			}
			if got, want := laser.IsUnavailable(wrapped), code.IsUnavailable(); got != want {
				t.Errorf("%s: unavailable is %v under code %v", name, got, code)
			}
			if got, want := laser.IsStale(wrapped), code == wire.ResultStale; got != want {
				t.Errorf("%s: stale is %v under code %v", name, got, code)
			}
			if got, want := laser.IsVersionSkew(wrapped), code == wire.ResultVersionSkew; got != want {
				t.Errorf("%s: version skew is %v under code %v", name, got, code)
			}
		}
	}
	missing := &laser.KvError{Failure: wire.KvError{Kind: wire.KvVersionConflict}}
	if code := laser.ResultCode(missing); code != wire.ResultConflict {
		t.Errorf("a version conflict: want %v, got %v", wire.ResultConflict, code)
	}
}

func TestGivenNoFailureWhenClassifiedThenShouldAnswerNoToEveryQuestion(t *testing.T) {
	for name, predicate := range map[string]func(error) bool{
		"not found": laser.IsNotFound, "stream or topic": laser.IsStreamOrTopicNotFound,
		"permission": laser.IsPermissionDenied, "unavailable": laser.IsUnavailable,
		"not leader": laser.IsNotLeader, "stale": laser.IsStale,
		"version conflict": laser.IsVersionConflict, "version skew": laser.IsVersionSkew,
		"lease lost": laser.IsLeaseLost, "fence": laser.IsFenceViolation,
		"quarantined": laser.IsQuarantined,
	} {
		if predicate(nil) {
			t.Fatalf("%s answered yes for no failure", name)
		}
	}
}

func TestGivenARefusalWhenRenderedThenShouldNameWhatWasRefused(t *testing.T) {
	quarantined := &laser.QuarantinedError{Agent: mustAgent(t, "planner")}
	if got := quarantined.Error(); got != "quarantined: planner" {
		t.Fatalf("got %q", got)
	}
	fence := &laser.FenceViolationError{Held: 3, Current: 5}
	if got := fence.Error(); got != "fence violation: held 3, current 5" {
		t.Fatalf("got %q", got)
	}
}

// assertClassified checks the predicate over every sample, bare and under each wrapper.
func assertClassified(t *testing.T, predicate func(error) bool, positives ...string) {
	t.Helper()
	samples := failureSamples(t)
	for _, name := range positives {
		if _, known := samples[name]; !known {
			t.Fatalf("no sample named %q", name)
		}
	}
	names := make([]string, 0, len(samples))
	for name := range samples {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		want := slices.Contains(positives, name)
		failure := samples[name]
		for wrapper, wrapped := range map[string]error{
			"bare":              failure,
			"failed publish":    &laser.PublishFailure{Cause: failure, Stream: "agents", Topic: "work"},
			"group setup":       &laser.ConsumerGroupSetupError{Name: "workers", Cause: failure},
			"formatted wrapper": fmt.Errorf("handle the record: %w", failure),
		} {
			if got := predicate(wrapped); got != want {
				t.Fatalf("%s (%s): got %v, want %v", name, wrapper, got, want)
			}
		}
	}
}

func failureSamples(t *testing.T) map[string]error {
	t.Helper()
	agent := mustAgent(t, "planner")
	kv := func(kind wire.KvErrorKind) error { return &laser.KvError{Failure: wire.KvError{Kind: kind}} }
	query := func(kind wire.QueryErrorKind) error {
		return &laser.QueryError{Failure: wire.QueryError{Kind: kind}}
	}
	fork := func(kind wire.ForkErrorKind) error { return &laser.ForkError{Failure: wire.ForkError{Kind: kind}} }
	graph := func(kind wire.GraphErrorKind) error {
		return &laser.GraphError{Failure: wire.GraphError{Kind: kind}}
	}
	authz := func(kind wire.AuthzErrorKind) error {
		return &laser.AuthzError{Failure: wire.AuthzError{Kind: kind}}
	}
	session := func(kind wire.SessionErrorKind) error {
		return &laser.SessionError{Failure: wire.SessionError{Kind: kind}}
	}
	command := func(code wire.ResultCode) error {
		return &laser.CommandFailure{Failure: wire.NewCommandError(code, "refused")}
	}
	transport := func(cause error) error {
		return &laser.TransportError{Reason: "send the request", Cause: cause}
	}
	return map[string]error{
		"transport stream id missing":   transport(ierror.StreamIdNotFound{}),
		"transport topic name missing":  transport(ierror.TopicNameNotFound{}),
		"transport resource missing":    transport(ierror.ResourceNotFound{}),
		"transport unauthorized":        transport(ierror.Unauthorized{}),
		"transport unauthenticated":     transport(ierror.Unauthenticated{}),
		"transport disconnected":        transport(ierror.Disconnected{}),
		"transport invalid command":     transport(ierror.InvalidCommand{}),
		"transport closed":              &laser.TransportError{Reason: "the client is closed"},
		"kv not found":                  kv(wire.KvNotFound),
		"kv unavailable":                kv(wire.KvUnavailable),
		"kv not leader":                 kv(wire.KvNotLeader),
		"kv stale":                      kv(wire.KvStale),
		"kv barrier unreachable":        kv(wire.KvBarrierUnreachable),
		"kv version conflict":           kv(wire.KvVersionConflict),
		"kv version mismatch":           kv(wire.KvVersionMismatch),
		"kv lease lost":                 kv(wire.KvLeaseLost),
		"kv lease held":                 kv(wire.KvLeaseHeld),
		"kv forbidden":                  kv(wire.KvForbidden),
		"kv backend":                    kv(wire.KvBackend),
		"query index not found":         query(wire.QueryIndexNotFound),
		"query unauthorized":            query(wire.QueryUnauthorized),
		"query unavailable":             query(wire.QueryUnavailable),
		"query stale":                   query(wire.QueryStale),
		"query stale generation":        query(wire.QueryStaleGeneration),
		"query version mismatch":        query(wire.QueryVersionMismatch),
		"query deadline exceeded":       query(wire.QueryDeadlineExceeded),
		"fork not found":                fork(wire.ForkNotFound),
		"fork unavailable":              fork(wire.ForkUnavailable),
		"fork not leader":               fork(wire.ForkNotLeader),
		"fork conflict":                 fork(wire.ForkConflict),
		"fork version mismatch":         fork(wire.ForkVersionMismatch),
		"graph not found":               graph(wire.GraphNotFound),
		"graph unauthorized":            graph(wire.GraphUnauthorized),
		"graph unavailable":             graph(wire.GraphUnavailable),
		"graph version mismatch":        graph(wire.GraphVersionMismatch),
		"authz unknown role":            authz(wire.AuthzUnknownRole),
		"authz unauthorized":            authz(wire.AuthzUnauthorized),
		"authz conflict":                authz(wire.AuthzConflict),
		"authz version mismatch":        authz(wire.AuthzVersionMismatch),
		"session not found":             session(wire.SessionNotFound),
		"session unauthorized":          session(wire.SessionUnauthorized),
		"session unavailable":           session(wire.SessionUnavailable),
		"session stale":                 session(wire.SessionStale),
		"filter not found":              &laser.FilterError{Failure: wire.FilterError{Code: wire.ResultNotFound}},
		"filter conflict":               &laser.FilterError{Failure: wire.FilterError{Code: wire.ResultConflict}},
		"filter fault":                  &laser.FilterFaultError{PartitionID: 1, Offset: 7},
		"filter oversized record":       &laser.FilterFaultError{PartitionID: 1, Offset: 7, Oversized: true},
		"command not found":             command(wire.ResultNotFound),
		"command forbidden":             command(wire.ResultForbidden),
		"command backend unavailable":   command(wire.ResultBackendUnavailable),
		"command pending":               command(wire.ResultPending),
		"unsupported":                   &laser.UnsupportedError{Surface: "the key-value surface"},
		"invalid":                       &laser.InvalidError{Reason: "refused"},
		"wire invalid":                  &wire.InvalidError{Reason: "refused"},
		"config":                        &laser.ConfigError{Reason: "refused"},
		"no respond topic":              &laser.NoRespondTopicError{},
		"codec":                         &laser.CodecError{Reason: "decode", Cause: errors.New("short")},
		"protocol":                      &laser.ProtocolError{Reason: "impossible"},
		"pinned version":                &laser.VersionError{Surface: "kv", Pinned: 1, Served: 2},
		"presence conflict":             &laser.PresenceConflictError{Advertised: agent, Requested: mustAgent(t, "writer")},
		"fence violation":               &laser.FenceViolationError{Held: 3, Current: 5},
		"session ended":                 &laser.SessionEndedError{},
		"signature":                     &laser.SignatureError{Reason: "unverified"},
		"quarantined":                   &laser.QuarantinedError{Agent: agent},
		"policy blocked":                &laser.PolicyBlockedError{Reason: "refused"},
		"policy deferred":               &laser.PolicyDeferredError{Reason: "held"},
		"route principal mismatch":      &laser.RoutePrincipalMismatchError{Agent: agent, Expected: 7},
		"edge denial":                   &laser.EdgeDenial{Kind: laser.DenialWrongAudience},
		"step up required":              &laser.StepUpRequiredError{Scope: "tool:write"},
		"integrity":                     &laser.IntegrityError{Reference: "blob"},
		"no capable agent":              &laser.NoCapableAgentError{Skill: "review"},
		"no inbox":                      &laser.NoInboxError{Agent: agent},
		"step timed out":                &laser.StepFailedError{Step: "report", Reason: laser.StepTimedOut},
		"step failed":                   &laser.StepFailedError{Step: "report", Reason: laser.StepFailed},
		"journal truncated":             &laser.JournalTruncatedError{},
		"stale lane":                    &laser.StaleLaneError{Reason: "recreated"},
		"budget exceeded":               &laser.BudgetExceededError{},
		"ambiguous mutation":            &laser.AmbiguousMutationError{Reason: "unknown"},
		"reply timeout":                 &laser.TimeoutError{},
		"stage timeout":                 &laser.StageTimeoutError{Stage: "a reply"},
		"state store":                   &laser.StateStoreError{Reason: "disk"},
		"undecodable record":            &laser.UndecodableRecord{Cause: errors.New("broken spine")},
		"typed decode over a not found": &laser.TypedDecodeError{Cause: kv(wire.KvNotFound)},
		"codec over a not found":        &laser.CodecError{Reason: "decode", Cause: kv(wire.KvNotFound)},
		"plain":                         errors.New("something else"),
	}
}

func mustAgent(t *testing.T, name string) wire.AgentID {
	t.Helper()
	agent, err := wire.NewAgentID(name)
	if err != nil {
		t.Fatalf("agent id %q: %v", name, err)
	}
	return agent
}
