package wire

import (
	"fmt"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// ResultCode is one logical outcome spanning query, key-value, fork and browse.
//
// Each managed surface keeps its own typed error for the detail a caller needs,
// and every one of those errors also projects onto a code here, so a generic
// client, the HTTP status mapper and a cross-language port all dispatch on one
// small dictionary instead of parsing per-surface strings.
//
// The numeric code is the identity, pinned across repos and ports. A code from
// a newer peer this build does not name decodes and re-encodes unchanged, so an
// old build relays it rather than failing.
type ResultCode uint16

// The pinned codes. Values of 21 and up are unrecognized by this build.
const (
	// ResultOk means the operation succeeded, with no error to classify.
	ResultOk ResultCode = 0
	// ResultUnsupported means the operation, or the managed surface, is not
	// available here.
	ResultUnsupported ResultCode = 1
	// ResultNotFound means a named entity does not exist.
	ResultNotFound ResultCode = 2
	// ResultInvalidArgument means the request was malformed or out of range.
	ResultInvalidArgument ResultCode = 3
	// ResultTooLarge means a result or value exceeded a size cap.
	ResultTooLarge ResultCode = 4
	// ResultConflict means a precondition lost a race.
	ResultConflict ResultCode = 5
	// ResultStale means a consistency level could not be met within the
	// deadline, because the read model is still catching up.
	ResultStale ResultCode = 6
	// ResultVersionSkew means the wire op version is not accepted by the peer.
	ResultVersionSkew ResultCode = 7
	// ResultUnauthenticated means no credential, or an invalid one.
	ResultUnauthenticated ResultCode = 8
	// ResultBackend means the managed backend failed or was unreachable, as a
	// fault the caller cannot retry away.
	ResultBackend ResultCode = 9
	// ResultForbidden means authenticated, but missing the needed grant.
	ResultForbidden ResultCode = 10
	// ResultStepUpRequired means the operation needs stronger authentication
	// than the caller holds.
	ResultStepUpRequired ResultCode = 11
	// ResultUnavailable means the request could not be served right now and
	// the same request may succeed later. The one code a generic client may
	// safely retry with backoff.
	ResultUnavailable ResultCode = 12
	// ResultResourceLimit means a server-enforced resource budget was passed.
	ResultResourceLimit ResultCode = 13
	// ResultCancelled means the query was cancelled through its execution
	// identity.
	ResultCancelled ResultCode = 14
	// ResultDeadlineExceeded means the query did not finish before its
	// absolute deadline.
	ResultDeadlineExceeded ResultCode = 15
	// ResultExpiredSnapshot means the requested historical snapshot is no
	// longer retained.
	ResultExpiredSnapshot ResultCode = 16
	// ResultStaleGeneration means the requested destination or backend
	// generation is no longer current.
	ResultStaleGeneration ResultCode = 17
	// ResultTargetUnavailable means the resolved target is not ready to serve.
	ResultTargetUnavailable ResultCode = 18
	// ResultPending means the mutation was accepted into the log but its
	// outcome is not known yet. Only a retry with the same operation identity
	// may follow, because a new identity could apply the change a second time.
	ResultPending ResultCode = 19
	// ResultBackendUnavailable means the node could not reach its managed
	// backend, or the backend did not answer in time. The request may still
	// apply, so a mutation repeats only under the same operation identity.
	ResultBackendUnavailable ResultCode = 20

	// firstUnrecognizedResultCode is the first value this build does not name.
	firstUnrecognizedResultCode ResultCode = 21
)

// resultCodeNames are the snake_case names the codes ride under.
var resultCodeNames = [...]string{
	"ok", "unsupported", "not_found", "invalid_argument", "too_large",
	"conflict", "stale", "version_skew", "unauthenticated", "backend",
	"forbidden", "step_up_required", "unavailable", "resource_limit",
	"cancelled", "deadline_exceeded", "expired_snapshot", "stale_generation",
	"target_unavailable", "pending", "backend_unavailable",
}

// Code is the pinned numeric value.
func (code ResultCode) Code() uint16 { return uint16(code) }

// IsRecognized reports whether this build names the code.
func (code ResultCode) IsRecognized() bool { return code < firstUnrecognizedResultCode }

// String is the snake_case name, or the unrecognized form carrying its code.
func (code ResultCode) String() string {
	if code.IsRecognized() {
		return resultCodeNames[code]
	}
	return fmt.Sprintf("unrecognized(%d)", uint16(code))
}

// ParseResultCode reads a snake_case name.
func ParseResultCode(name string) (ResultCode, error) {
	for value, candidate := range resultCodeNames {
		if candidate == name {
			return ResultCode(value), nil
		}
	}
	return 0, &InvalidError{Reason: fmt.Sprintf("unknown result code %q", name)}
}

// HTTPStatus is the status this code maps to, the one mapping every surface
// shares, so a status need not be decided per surface or per route.
func (code ResultCode) HTTPStatus() uint16 {
	switch code {
	case ResultOk:
		return 200
	case ResultUnsupported:
		return 501
	case ResultNotFound:
		return 404
	case ResultInvalidArgument, ResultVersionSkew:
		return 400
	case ResultTooLarge:
		return 413
	case ResultConflict, ResultCancelled, ResultStaleGeneration:
		return 409
	case ResultUnauthenticated:
		return 401
	case ResultBackend:
		return 502
	// Authenticated-but-forbidden is 403, distinct from the 401 an
	// unauthenticated caller gets. Step-up also lands on 403 unless the HTTP
	// layer has a better challenge status for the chosen scheme.
	case ResultForbidden, ResultStepUpRequired:
		return 403
	// Retry-after territory, the same status a lagging read model gets. Pending
	// sits here too: the outcome is still being decided, so the same request
	// comes back later, though it is not generically retryable.
	case ResultStale, ResultUnavailable, ResultTargetUnavailable,
		ResultPending, ResultBackendUnavailable:
		return 503
	case ResultResourceLimit:
		return 429
	case ResultDeadlineExceeded:
		return 408
	case ResultExpiredSnapshot:
		return 410
	default:
		return 500
	}
}

// IsRetryable reports whether a caller may retry the identical request and
// reasonably expect a different outcome.
//
// True only for the transient classes: the store was momentarily unreachable,
// or the read model had not caught up. Every other code needs the request, the
// credential or the data to change first, so retrying it unchanged only wastes
// the attempt. Pending is deliberately not in this set, since only the original
// operation identity may be retried.
func (code ResultCode) IsRetryable() bool {
	switch code {
	case ResultUnavailable, ResultStale, ResultTargetUnavailable, ResultBackendUnavailable:
		return true
	default:
		return false
	}
}

// IsUnavailable reports a request that could not be served right now: the
// retryable codes other than a lagging read model.
//
// It is the broad classifier every client exposes, and it is narrower than
// IsRetryable because a stale read is a reader that has to catch up rather
// than a surface that is out of reach.
func (code ResultCode) IsUnavailable() bool {
	switch code {
	case ResultUnavailable, ResultTargetUnavailable, ResultBackendUnavailable:
		return true
	default:
		return false
	}
}

// IsPermissionDenied reports a refused credential or grant: no credential or
// an unverified one, or a missing grant. The fix is a credential or a grant,
// never a retry.
func (code ResultCode) IsPermissionDenied() bool {
	return code == ResultForbidden || code == ResultUnauthenticated
}

// IsPending reports a mutation accepted with its outcome still unknown. The
// caller repeats the exact request under the same operation identity.
func (code ResultCode) IsPending() bool { return code == ResultPending }

// IsBackendUnavailable reports that the node itself could not reach its managed
// backend. Unlike Unavailable, which a busy node or unreachable destination
// answers, this names the node as the problem, and a mutation may be repeated
// under the same operation identity on this node or another.
func (code ResultCode) IsBackendUnavailable() bool { return code == ResultBackendUnavailable }

// unrecognizedResultVariant is the externally-tagged name serde gives the
// passthrough variant.
const unrecognizedResultVariant = "unrecognized"

// MarshalCBOR writes the snake_case name, or the tagged passthrough form for a
// code this build does not name.
func (code ResultCode) MarshalCBOR() (cborform.Value, error) {
	if code.IsRecognized() {
		return cborform.Text(resultCodeNames[code]), nil
	}
	return cborform.Map{{
		Key:   cborform.Text(unrecognizedResultVariant),
		Value: cborform.Uint(uint64(code)),
	}}, nil
}

// UnmarshalCBOR reads either form, so a code from a newer peer survives.
func (code *ResultCode) UnmarshalCBOR(item cborform.Value) error {
	switch typed := item.(type) {
	case cborform.Text:
		parsed, err := ParseResultCode(string(typed))
		if err != nil {
			return err
		}
		*code = parsed
		return nil
	case cborform.Map:
		if len(typed) != 1 {
			return fmt.Errorf("wire: a result code variant carries one entry, got %d", len(typed))
		}
		name, ok := typed[0].Key.(cborform.Text)
		if !ok || string(name) != unrecognizedResultVariant {
			return fmt.Errorf("wire: %v is not a result code variant", typed[0].Key)
		}
		number, ok := typed[0].Value.(cborform.Uint)
		if !ok || number > 0xffff {
			return fmt.Errorf("wire: an unrecognized result code must be a u16")
		}
		if ResultCode(number) < firstUnrecognizedResultCode {
			return fmt.Errorf(
				"wire: code %d is named, so it must ride as its name rather than the passthrough form",
				number)
		}
		*code = ResultCode(number)
		return nil
	default:
		return fmt.Errorf("wire: cannot decode %T as a result code", item)
	}
}

// CommandError is the canonical surface-agnostic error reply.
//
// Every managed surface has its own typed reply, but a server receiving a
// command code it does not handle has no one surface to answer in: a
// query-shaped error fails to decode in a client awaiting a key-value reply and
// surfaces as an opaque transport error. This is that fallback, so a client
// that cannot decode the surface's typed reply tries this next and turns a
// wrong-surface reply into a typed code.
type CommandError struct {
	Code    ResultCode `cbor:"code"`
	Message string     `cbor:"message"`
}

// NewCommandError builds a command error from a classified code and a message.
func NewCommandError(code ResultCode, message string) CommandError {
	return CommandError{Code: code, Message: message}
}

// UnsupportedCommandError is the reply for a command code a server does not
// handle.
func UnsupportedCommandError(message string) CommandError {
	return NewCommandError(ResultUnsupported, message)
}

// Error lets a command error be returned as an ordinary Go error.
func (e CommandError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}
