package laser

import (
	"strconv"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// KvError reports a key-value command the store refused, carrying the
// contract's own classification so a caller can tell a retryable condition
// from a fault without matching on message text.
type KvError struct{ Failure wire.KvError }

func (e *KvError) Error() string {
	if e.Failure.Message != "" {
		return "key-value: " + e.Failure.Message
	}
	return "key-value: " + kvKindName(e.Failure.Kind)
}

// Kind is the contract's classification of the failure.
func (e *KvError) Kind() wire.KvErrorKind { return e.Failure.Kind }

// Retryable reports a condition where the identical request may succeed later.
// Every other kind is a fault that repeating will not fix.
func (e *KvError) Retryable() bool {
	switch e.Failure.Kind {
	case wire.KvUnavailable, wire.KvStale, wire.KvBarrierUnreachable:
		return true
	default:
		return false
	}
}

// Conflict reports a precondition that was not met, which a caller resolves by
// re-reading rather than by retrying the same write.
func (e *KvError) Conflict() bool {
	switch e.Failure.Kind {
	case wire.KvVersionConflict, wire.KvVersionMismatch:
		return true
	default:
		return false
	}
}

func kvKindName(kind wire.KvErrorKind) string {
	switch kind {
	case wire.KvUnsupported:
		return "unsupported"
	case wire.KvInvalidKey:
		return "invalid key"
	case wire.KvInvalidNamespace:
		return "invalid namespace"
	case wire.KvTooLarge:
		return "too large"
	case wire.KvBackend:
		return "backend"
	case wire.KvUnavailable:
		return "unavailable"
	case wire.KvVersionMismatch:
		return "version mismatch"
	case wire.KvVersionConflict:
		return "version conflict"
	case wire.KvLeaseLost:
		return "lease lost"
	case wire.KvLeaseHeld:
		return "lease held"
	case wire.KvStale:
		return "stale"
	case wire.KvNotFound:
		return "not found"
	case wire.KvBarrierUnreachable:
		return "barrier unreachable"
	default:
		return "kind " + strconv.Itoa(int(kind))
	}
}
