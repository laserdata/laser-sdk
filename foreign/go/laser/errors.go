// Package laser is the LaserData client for Go. It connects to a LaserData
// deployment over Apache Iggy and speaks the AGDX contract that
// [github.com/laserdata/laser-sdk/foreign/go/wire] defines.
//
// The client connects over TCP, plain or TLS. QUIC and WebSocket are served by
// the Rust and Python clients, because the Apache Iggy Go SDK this client
// builds on implements the TCP transport only.
package laser

import (
	"errors"
	"strconv"

	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// ConfigError reports a connection string, environment variable, or builder
// setting the client cannot act on, the Go form of laser_sdk's
// LaserError::Config. It is raised before any network call, so a misconfigured
// client fails without dialing anything.
//
// A connection string carries a password, so no ConfigError ever quotes the
// value it rejected.
type ConfigError struct{ Reason string }

func (e *ConfigError) Error() string { return e.Reason }

// configError keeps the reason a constant at every call site, which is what
// stops a credential reaching a log through an error message.
func configError(reason string) error { return &ConfigError{Reason: reason} }

// TransportError reports a connection that could not be established, used, or
// closed, the Go form of laser_sdk's LaserError::Transport.
//
// The cause is kept so a caller can match on the Apache Iggy error underneath,
// and the reason names the stage rather than echoing the connection string,
// which carries a password.
type TransportError struct {
	Reason string
	Cause  error
}

func (e *TransportError) Error() string {
	if e.Cause == nil {
		return e.Reason
	}
	return e.Reason + ": " + e.Cause.Error()
}

func (e *TransportError) Unwrap() error { return e.Cause }

// IggyErrorCode is the Apache Iggy error code underneath, and whether one was
// there to read.
//
// A failure that came from the transport carries a code every Iggy client
// classifies the same way, which is what lets a caller branch on the server's
// own vocabulary rather than on message text.
func (e *TransportError) IggyErrorCode() (ierror.Code, bool) {
	var coded ierror.IggyError
	if errors.As(e.Cause, &coded) {
		return coded.Code(), true
	}
	return 0, false
}

// UncertainOperation is the request this failure left with an unknown
// outcome, to settle with Laser.ResolveOperation or Laser.AbandonOperation,
// and whether this failure named one at all.
//
// The transport retains such a request so its outcome can still be proved.
// Until it is resolved or abandoned the lane it blocks stays blocked, so a
// caller that ignores the id stalls the operations behind it.
func (e *TransportError) UncertainOperation() (uint64, bool) {
	var uncertain ierror.OperationUncertain
	if errors.As(e.Cause, &uncertain) {
		return uncertain.OperationID, true
	}
	var blocked ierror.LaneBlocked
	if errors.As(e.Cause, &blocked) {
		return blocked.OperationID, true
	}
	var partition ierror.PartitionLaneBlocked
	if errors.As(e.Cause, &partition) {
		return partition.OperationID, true
	}
	return 0, false
}

// InvalidError reports a value the client refuses to send, the Go form of
// laser_sdk's LaserError::Invalid.
type InvalidError struct{ Reason string }

func (e *InvalidError) Error() string { return e.Reason }

// UnsupportedError reports a surface the connected deployment does not serve,
// the Go form of laser_sdk's LaserError::Unsupported.
//
// It is raised before the call leaves, because plain Apache Iggy answers an
// unknown managed command with a transport error that says nothing about why.
type UnsupportedError struct {
	// Surface is the surface the refused call came through.
	Surface string
	// Feature is the capability within the surface, when the refusal names one.
	Feature string
	// Message says what was asked and why it was refused. Empty for a surface the deployment lacks.
	Message string
}

func (e *UnsupportedError) Error() string {
	switch {
	case e.Message == "":
		return e.Surface + " need a managed backend, which this deployment does not have"
	case e.Feature == "":
		return "unsupported: " + e.Surface + ": " + e.Message
	case e.Feature == e.Surface:
		return "unsupported: " + e.Surface + ": " + e.Message +
			" (the `" + e.Feature + "` capability serves this on LaserData Cloud)"
	default:
		return "unsupported: " + e.Surface + "." + e.Feature + ": " + e.Message +
			" (the `" + e.Feature + "` capability serves this on LaserData Cloud)"
	}
}

// ResultCode projects onto the shared unsupported code, which is what a
// deployment that does serve the surface would answer for the same refusal.
//
// Without this a caller could not ask a refusal what it was in any uniform
// way: every other refusal in this package answers a code, and this one
// being the exception meant "the surface is not served" was the single
// outcome that could only be read by matching the Go type.
func (e *UnsupportedError) ResultCode() wire.ResultCode {
	return wire.ResultUnsupported
}

// CodecError reports a value that could not be encoded or decoded, the Go form
// of laser_sdk's LaserError::Codec.
type CodecError struct {
	Reason string
	Cause  error
}

func (e *CodecError) Error() string {
	if e.Cause == nil {
		return e.Reason
	}
	return e.Reason + ": " + e.Cause.Error()
}

func (e *CodecError) Unwrap() error { return e.Cause }

// NoInboxError reports an advertised inbox route that resolved an agent
// which advertises no inbox topic, so there is nowhere to address it, the Go
// form of laser_sdk's LaserError::NoInbox.
//
// It never falls back to a shared topic name: a destination nobody agreed to
// consume is worse than a refusal a caller can act on.
type NoInboxError struct{ Agent wire.AgentID }

func (e *NoInboxError) Error() string {
	return "agent has no advertised inbox: " + e.Agent.String() +
		": start the agent with capabilities so it advertises its inbox, " +
		"or give the contract a fixed inbox topic (FixedInbox)"
}

// RoutePrincipalMismatchError reports a route that selected an agent whose
// live connection is not bound to the principal the caller required, the Go
// form of laser_sdk's LaserError::RoutePrincipalMismatch.
//
// Actual is nil when the agent has no live presence at all, which is a
// different fact from presence under the wrong principal and is reported as
// such: one is an agent that is not there, the other is an agent that is
// there as somebody else.
type RoutePrincipalMismatchError struct {
	Agent    wire.AgentID
	Expected PrincipalID
	Actual   *PrincipalID
}

func (e *RoutePrincipalMismatchError) Error() string {
	actual := "none"
	if e.Actual != nil {
		actual = strconv.FormatUint(uint64(*e.Actual), 10)
	}
	return "agent " + e.Agent.String() + " is not bound to principal " +
		strconv.FormatUint(uint64(e.Expected), 10) + " (actual: " + actual + ")"
}

// ProtocolError reports a deployment answering in a way the protocol does not
// allow, the Go form of laser_sdk's LaserError::Protocol.
//
// It is distinct from CodecError, which is bytes that would not decode. These
// bytes decoded. What they said was impossible, such as a page cursor that
// does not advance.
type ProtocolError struct{ Reason string }

func (e *ProtocolError) Error() string { return e.Reason }

// VersionError reports a surface whose advertised op version is not the one
// this client pins, the Go form of a surface's typed Version error.
//
// It is raised before the round trip, because a payload sent across a version
// gap would be read under a contract neither side agreed to.
type VersionError struct {
	Surface string
	Pinned  uint32
	Served  uint32
}

func (e *VersionError) Error() string {
	return e.Surface + " is served at op version " + itoa(int(e.Served)) +
		", but this client speaks " + itoa(int(e.Pinned))
}

// PresenceConflictError reports that this connection already advertises
// another agent's live presence, the Go form of laser_sdk's
// LaserError::PresenceConflict. One physical connection may advertise one
// logical agent: a second would overwrite connection metadata and steal the
// first agent's route.
type PresenceConflictError struct {
	Advertised wire.AgentID
	Requested  wire.AgentID
}

func (e *PresenceConflictError) Error() string {
	return "connection already advertises agent " + e.Advertised.String() +
		", cannot advertise " + e.Requested.String()
}

// NoProvenanceError reports a record that carries no agentic spine at all,
// which is the normal case on a plain data topic.
//
// It is kept apart from a spine that is present but broken, because the two
// want opposite handling: a record with no spine is delivered without one,
// while a record whose fence or deadline failed to decode must not be
// delivered as though it had neither.
type NoProvenanceError struct{}

func (e *NoProvenanceError) Error() string {
	return "the record names no conversation, so it carries no provenance"
}

// StaleLaneError reports a session handle refused because the lane it was
// pinned to is no longer the lane underneath it.
//
// A stream or topic that was deleted and created again takes the same name,
// and can take the same numeric id, so a retained handle would otherwise
// write to the replacement as though nothing had happened, and its records
// would be read back as part of a session that, on the lane the reader sees,
// never had them. Recover by building a fresh handle, which pins the lane as
// it is now.
type StaleLaneError struct{ Reason string }

func (e *StaleLaneError) Error() string { return "session: stale session generation: " + e.Reason }

// ResultCode projects onto the shared stale-generation code, which is what
// the server answers for the same condition.
func (e *StaleLaneError) ResultCode() wire.ResultCode {
	return wire.ResultStaleGeneration
}

// QuarantinedError reports an agent that is quarantined and may not act, the
// Go form of laser_sdk's LaserError::Quarantined.
type QuarantinedError struct{ Agent wire.AgentID }

func (e *QuarantinedError) Error() string { return "quarantined: " + e.Agent.String() }

// FenceViolationError reports a fenced write that lost the fence to a newer
// holder, the Go form of laser_sdk's LaserError::FenceViolation.
type FenceViolationError struct {
	Held    uint64
	Current uint64
}

func (e *FenceViolationError) Error() string {
	return "fence violation: held " + strconv.FormatUint(e.Held, 10) +
		", current " + strconv.FormatUint(e.Current, 10)
}

// IsNotFound reports a failure that names a missing resource: an index, fork,
// session, key, graph, role, stream, or a capable agent.
func IsNotFound(err error) bool { return ResultCode(err) == wire.ResultNotFound }

// IsStreamOrTopicNotFound reports that the target stream or topic does not
// exist for this principal. A stream the principal cannot see answers the same.
func IsStreamOrTopicNotFound(err error) bool {
	transport, refused := classified(err).(*TransportError)
	if !refused {
		return false
	}
	return errors.Is(transport.Cause, ierror.StreamIdNotFound{}) ||
		errors.Is(transport.Cause, ierror.StreamNameNotFound{}) ||
		errors.Is(transport.Cause, ierror.TopicIdNotFound{}) ||
		errors.Is(transport.Cause, ierror.TopicNameNotFound{})
}

// IsPermissionDenied reports a refused credential or a missing grant. The fix
// is a grant, never a retry.
func IsPermissionDenied(err error) bool { return ResultCode(err).IsPermissionDenied() }

// IsUnavailable reports a transient outage: the identical request may succeed
// on a later attempt.
func IsUnavailable(err error) bool { return ResultCode(err).IsUnavailable() }

// IsNotLeader reports a conditional write the managed backend declined because
// it does not own the mutation partition.
func IsNotLeader(err error) bool {
	switch refused := classified(err).(type) {
	case *KvError:
		return refused.Failure.Kind == wire.KvNotLeader
	case *ForkError:
		return refused.Failure.Kind == wire.ForkNotLeader
	default:
		return false
	}
}

// IsStale reports a read-consistency barrier that was not met yet. Retryable.
func IsStale(err error) bool { return ResultCode(err) == wire.ResultStale }

// IsVersionConflict reports a compare-and-swap that lost its precondition.
// Read the value again, recompute, and commit again.
func IsVersionConflict(err error) bool { return kvFailureIs(err, wire.KvVersionConflict) }

// IsVersionSkew reports a wire version the deployment did not accept.
// Permanent for this client build.
func IsVersionSkew(err error) bool { return ResultCode(err) == wire.ResultVersionSkew }

// IsLeaseLost reports a lease that expired, was released, or was taken by
// another holder.
func IsLeaseLost(err error) bool { return kvFailureIs(err, wire.KvLeaseLost) }

// IsFenceViolation reports a fenced write that lost the fence. Never retryable
// by the loser.
func IsFenceViolation(err error) bool {
	_, lost := classified(err).(*FenceViolationError)
	return lost
}

// IsQuarantined reports a route that names a quarantined agent.
func IsQuarantined(err error) bool {
	_, held := classified(err).(*QuarantinedError)
	return held
}

func kvFailureIs(err error, kind wire.KvErrorKind) bool {
	refused, kv := classified(err).(*KvError)
	return kv && refused.Failure.Kind == kind
}

// ResultCode is the failure's place in the shared result codes. No failure is ok.
func ResultCode(err error) wire.ResultCode {
	if err == nil {
		return wire.ResultOk
	}
	code, _ := failureCode(classified(err))
	return code
}

// classified is the first typed failure of a chain, so a failed publish and a
// group setup failure answer for their cause.
func classified(err error) error {
	for current := err; current != nil; current = errors.Unwrap(current) {
		if _, known := failureCode(current); known {
			return current
		}
	}
	return err
}

// failureCode classifies one typed failure, and reports whether it is one.
func failureCode(failure error) (wire.ResultCode, bool) {
	switch refused := failure.(type) {
	case *TransportError:
		return transportResultCode(refused.Cause), true
	case *KvError:
		return refused.Failure.ResultCode(), true
	case *QueryError:
		return refused.Failure.ResultCode(), true
	case *ForkError:
		return refused.Failure.ResultCode(), true
	case *GraphError:
		return refused.Failure.ResultCode(), true
	case *AuthzError:
		return refused.Failure.ResultCode(), true
	case *FilterFaultError:
		if refused.Oversized {
			return wire.ResultTooLarge, true
		}
		return wire.ResultInvalidArgument, true
	case *InvalidError, *wire.InvalidError, *ConfigError, *NoRespondTopicError:
		return wire.ResultInvalidArgument, true
	case *VersionError:
		return wire.ResultVersionSkew, true
	case *FenceViolationError, *PresenceConflictError, *SessionEndedError:
		return wire.ResultConflict, true
	case *SignatureError:
		return wire.ResultUnauthenticated, true
	case *QuarantinedError, *PolicyBlockedError, *RoutePrincipalMismatchError:
		return wire.ResultForbidden, true
	case *StepUpRequiredError:
		return wire.ResultStepUpRequired, true
	case *NoCapableAgentError, *NoInboxError:
		return wire.ResultNotFound, true
	case *StepFailedError:
		if refused.Reason == StepTimedOut || refused.Reason == StepNotConsumed {
			return wire.ResultDeadlineExceeded, true
		}
		return wire.ResultBackend, true
	case *JournalTruncatedError:
		return wire.ResultExpiredSnapshot, true
	case *CancelledError:
		return wire.ResultCancelled, true
	case *CodecError, *ProtocolError, *TypedDecodeError, *UndecodableRecord,
		*StateStoreError, *IntegrityError, *AmbiguousMutationError,
		*BudgetExceededError, *PolicyDeferredError, *TimeoutError, *StageTimeoutError:
		return wire.ResultBackend, true
	case interface{ ResultCode() wire.ResultCode }:
		return refused.ResultCode(), true
	default:
		return wire.ResultBackend, false
	}
}

// transportResultCode classifies the Apache Iggy failure under a transport error.
func transportResultCode(cause error) wire.ResultCode {
	switch {
	case isMissingResource(cause):
		return wire.ResultNotFound
	case errors.Is(cause, ierror.Unauthorized{}):
		return wire.ResultForbidden
	case errors.Is(cause, ierror.Unauthenticated{}):
		return wire.ResultUnauthenticated
	case transientTransport(cause):
		return wire.ResultUnavailable
	default:
		return wire.ResultBackend
	}
}
