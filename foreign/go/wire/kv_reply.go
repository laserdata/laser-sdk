package wire

import (
	"fmt"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// KvOutcomeKind discriminates a successful key-value outcome.
type KvOutcomeKind uint8

// The outcome variants, shaped per operation.
const (
	// KvValue is a get: the live entry, or absent when missing or expired.
	KvValue KvOutcomeKind = iota
	// KvWritten is a set that was applied.
	KvWritten
	// KvCommitted is a compare-and-swap that applied, carrying the new
	// version so a caller can chain a conditional write without a re-read.
	KvCommitted
	// KvDeleted reports whether a live entry was removed.
	KvDeleted
	// KvDeletedMany is the count removed by a filtered bulk delete.
	KvDeletedMany
	// KvPageOutcome is one page of scanned entries.
	KvPageOutcome
	// KvNamespacesOutcome lists every namespace holding entries, sorted.
	KvNamespacesOutcome
	// KvNotModified is a conditional-get hit: the live version matched, so the
	// body was not transferred.
	KvNotModified
	// KvMetadataOutcome is an exists reply.
	KvMetadataOutcome
	// KvVersioned is an expire or patch that applied. Expire leaves the
	// version unchanged, where patch bumps it.
	KvVersioned
	// KvLeased is a granted lease, carrying the fencing token, the granted
	// nonzero TTL, and the position at which the answering fold applied the
	// grant, which a takeover read passes back as a barrier.
	KvLeased
	// KvRenewed is an extended lease, carrying the unchanged token, the
	// granted TTL, and the renewal's applied position.
	KvRenewed
	// KvReleased reports whether a held lease was released.
	KvReleased
)

// KvLeaseGrant is the payload of a granted or renewed lease.
type KvLeaseGrant struct {
	LeaseToken       uint64           `cbor:"lease_token"`
	GrantedTTLMicros uint64           `cbor:"granted_ttl_micros"`
	Position         MutationPosition `cbor:"position"`
}

type kvVersionPayload struct {
	Version uint64 `cbor:"version"`
}

// KvOutcome is the successful outcome of a key-value command.
type KvOutcome struct {
	Kind        KvOutcomeKind
	Entry       *KvEntry
	Version     uint64
	Deleted     bool
	DeletedMany uint64
	Page        *KvPage
	Namespaces  []KvNamespaceInfo
	Metadata    *KvMetadata
	Lease       *KvLeaseGrant
	Released    bool
}

// The externally tagged outcome names.
const (
	kvValueVariant       = "Value"
	kvWrittenVariant     = "Written"
	kvCommittedVariant   = "Committed"
	kvDeletedVariant     = "Deleted"
	kvDeletedManyVariant = "DeletedMany"
	kvPageVariant        = "Page"
	kvNamespacesVariant  = "Namespaces"
	kvNotModifiedVariant = "NotModified"
	kvMetadataVariant    = "Metadata"
	kvVersionedVariant   = "Versioned"
	kvLeasedVariant      = "Leased"
	kvRenewedVariant     = "Renewed"
	kvReleasedVariant    = "Released"
)

// MarshalCBOR writes the externally tagged outcome.
func (outcome KvOutcome) MarshalCBOR() (cborform.Value, error) {
	switch outcome.Kind {
	case KvValue:
		if outcome.Entry == nil {
			return taggedVariant(kvValueVariant, cborform.Null{}), nil
		}
		return marshalVariantStruct(kvValueVariant, *outcome.Entry)
	case KvWritten:
		return unitVariant(kvWrittenVariant), nil
	case KvCommitted:
		return marshalVariantStruct(kvCommittedVariant, kvVersionPayload{Version: outcome.Version})
	case KvDeleted:
		return taggedVariant(kvDeletedVariant, cborform.Bool(outcome.Deleted)), nil
	case KvDeletedMany:
		return taggedVariant(kvDeletedManyVariant, cborform.Uint(outcome.DeletedMany)), nil
	case KvPageOutcome:
		if outcome.Page == nil {
			return nil, fmt.Errorf("wire: a page outcome carries no page")
		}
		return marshalVariantStruct(kvPageVariant, *outcome.Page)
	case KvNamespacesOutcome:
		payload, err := toValueOf(outcome.Namespaces)
		if err != nil {
			return nil, err
		}
		return taggedVariant(kvNamespacesVariant, payload), nil
	case KvNotModified:
		return unitVariant(kvNotModifiedVariant), nil
	case KvMetadataOutcome:
		if outcome.Metadata == nil {
			return taggedVariant(kvMetadataVariant, cborform.Null{}), nil
		}
		return marshalVariantStruct(kvMetadataVariant, *outcome.Metadata)
	case KvVersioned:
		return marshalVariantStruct(kvVersionedVariant, kvVersionPayload{Version: outcome.Version})
	case KvLeased:
		if outcome.Lease == nil {
			return nil, fmt.Errorf("wire: a lease outcome carries no grant")
		}
		return marshalVariantStruct(kvLeasedVariant, *outcome.Lease)
	case KvRenewed:
		if outcome.Lease == nil {
			return nil, fmt.Errorf("wire: a renewal outcome carries no grant")
		}
		return marshalVariantStruct(kvRenewedVariant, *outcome.Lease)
	case KvReleased:
		return taggedVariant(kvReleasedVariant, cborform.Bool(outcome.Released)), nil
	default:
		return nil, fmt.Errorf("wire: unknown key-value outcome %d", outcome.Kind)
	}
}

// UnmarshalCBOR reads the externally tagged outcome.
//
// It decodes fail-closed: an unknown variant is an error rather than a success,
// because a caller treating one as success would, for instance, trust an
// unextended lease deadline.
func (outcome *KvOutcome) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}

	needPayload := func() error { return requirePayload("KvOutcome", name, payload) }

	switch name {
	case kvValueVariant:
		if err := needPayload(); err != nil {
			return err
		}
		if _, isNull := payload.(cborform.Null); isNull {
			*outcome = KvOutcome{Kind: KvValue}
			return nil
		}
		var entry KvEntry
		if err := fromValueInto(payload, &entry); err != nil {
			return err
		}
		*outcome = KvOutcome{Kind: KvValue, Entry: &entry}
		return nil

	case kvWrittenVariant:
		*outcome = KvOutcome{Kind: KvWritten}
		return nil

	case kvCommittedVariant, kvVersionedVariant:
		if err := needPayload(); err != nil {
			return err
		}
		var version kvVersionPayload
		if err := fromValueInto(payload, &version); err != nil {
			return err
		}
		kind := KvCommitted
		if name == kvVersionedVariant {
			kind = KvVersioned
		}
		*outcome = KvOutcome{Kind: kind, Version: version.Version}
		return nil

	case kvDeletedVariant, kvReleasedVariant:
		if err := needPayload(); err != nil {
			return err
		}
		flag, ok := payload.(cborform.Bool)
		if !ok {
			return fmt.Errorf("wire: %q carries a boolean, got %T", name, payload)
		}
		if name == kvDeletedVariant {
			*outcome = KvOutcome{Kind: KvDeleted, Deleted: bool(flag)}
		} else {
			*outcome = KvOutcome{Kind: KvReleased, Released: bool(flag)}
		}
		return nil

	case kvDeletedManyVariant:
		if err := needPayload(); err != nil {
			return err
		}
		count, ok := payload.(cborform.Uint)
		if !ok {
			return fmt.Errorf("wire: a bulk delete count must be an integer, got %T", payload)
		}
		*outcome = KvOutcome{Kind: KvDeletedMany, DeletedMany: uint64(count)}
		return nil

	case kvPageVariant:
		if err := needPayload(); err != nil {
			return err
		}
		var page KvPage
		if err := fromValueInto(payload, &page); err != nil {
			return err
		}
		*outcome = KvOutcome{Kind: KvPageOutcome, Page: &page}
		return nil

	case kvNamespacesVariant:
		if err := needPayload(); err != nil {
			return err
		}
		var namespaces []KvNamespaceInfo
		if err := fromValueInto(payload, &namespaces); err != nil {
			return err
		}
		*outcome = KvOutcome{Kind: KvNamespacesOutcome, Namespaces: namespaces}
		return nil

	case kvNotModifiedVariant:
		*outcome = KvOutcome{Kind: KvNotModified}
		return nil

	case kvMetadataVariant:
		if err := needPayload(); err != nil {
			return err
		}
		if _, isNull := payload.(cborform.Null); isNull {
			*outcome = KvOutcome{Kind: KvMetadataOutcome}
			return nil
		}
		var metadata KvMetadata
		if err := fromValueInto(payload, &metadata); err != nil {
			return err
		}
		*outcome = KvOutcome{Kind: KvMetadataOutcome, Metadata: &metadata}
		return nil

	case kvLeasedVariant, kvRenewedVariant:
		if err := needPayload(); err != nil {
			return err
		}
		var grant KvLeaseGrant
		if err := fromValueInto(payload, &grant); err != nil {
			return err
		}
		kind := KvLeased
		if name == kvRenewedVariant {
			kind = KvRenewed
		}
		*outcome = KvOutcome{Kind: kind, Lease: &grant}
		return nil

	default:
		return unknownVariant("KvOutcome", name)
	}
}

// KvErrorKind discriminates a key-value failure.
type KvErrorKind uint8

const (
	KvUnsupported KvErrorKind = iota
	KvInvalidKey
	// KvInvalidNamespace names a namespace that fails the namespace rule.
	KvInvalidNamespace
	KvTooLarge
	KvBackend
	// KvUnavailable means the identical request may succeed later: the store
	// was momentarily out of reach, a connection dropped, or a concurrency
	// conflict was aborted. Retry with backoff rather than treating it as a
	// fault.
	KvUnavailable
	KvVersionMismatch
	// KvVersionConflict means a compare-and-swap precondition was not met.
	KvVersionConflict
	// KvLeaseLost means a held lease expired, was released, or was re-acquired
	// by another holder, so its token no longer protects mutations.
	KvLeaseLost
	// KvLeaseHeld means another holder owns the lease. The expiry is the
	// earliest a fresh acquisition can succeed unless the holder renews.
	KvLeaseHeld
	// KvStale means a barriered read could not be answered: the local fold did
	// not reach the required position within the wait bound, or the topic
	// generation does not match. Never mapped to an absent value.
	KvStale
	// KvNotFound means an in-place op targeted a key that is absent or expired.
	KvNotFound
	// KvBarrierUnreachable means a barriered read could not reach the
	// position the request required within the wait bound. Retry against a
	// caught-up fold.
	KvBarrierUnreachable
	// KvNotLeader means this backend does not own the mutation partition for
	// the key. The caller may retry after leadership refreshes.
	KvNotLeader
	// KvForbidden means the caller authenticated but lacks the grant the
	// operation needs on the namespace.
	KvForbidden
	// KvInvalidExpiry means a write named both an absolute expiry and a
	// relative lifetime.
	KvInvalidExpiry
)

type kvTooLargePayload struct {
	What string `cbor:"what"`
	Size uint64 `cbor:"size"`
	Cap  uint64 `cbor:"cap"`
}

type kvVersionPayloadPair struct {
	Expected uint32 `cbor:"expected"`
	Got      uint32 `cbor:"got"`
}

type kvVersionConflictPayload struct {
	Current *uint64 `cbor:"current"`
}

type kvLeaseHeldPayload struct {
	ExpiresAtMicros *uint64 `cbor:"expires_at_micros"`
}

type kvStalePayload struct {
	Required MutationPosition `cbor:"required"`
}

// KvError is why a key-value operation failed.
type KvError struct {
	Kind    KvErrorKind
	Message string
	// TooLarge detail.
	What string
	Size uint64
	Cap  uint64
	// Version skew detail.
	Expected uint32
	Got      uint32
	// Current is the key's present version on a conflict, or nil when the key
	// does not exist, so a caller can re-read and retry or learn that an
	// absent precondition lost a race.
	Current *uint64
	// Required is the position a barriered read waited for.
	Required MutationPosition
	// ExpiresAtMicros is the holder's expiry in log time, present when
	// another holder owns the lease.
	ExpiresAtMicros *uint64
}

// The externally tagged error names.
const (
	kvUnsupportedVariant      = "Unsupported"
	kvInvalidKeyVariant       = "InvalidKey"
	kvInvalidNamespaceVariant = "InvalidNamespace"
	kvTooLargeVariant         = "TooLarge"
	kvBackendVariant          = "Backend"
	kvUnavailableVariant      = "Unavailable"
	kvVersionVariant          = "Version"
	kvVersionConflictVariant  = "VersionConflict"
	kvLeaseLostVariant        = "LeaseLost"
	kvStaleVariant            = "Stale"
	kvNotFoundVariant         = "NotFound"
	kvLeaseHeldVariant        = "LeaseHeld"
	kvBarrierUnreachableVar   = "BarrierUnreachable"
	kvNotLeaderVariant        = "NotLeader"
	kvForbiddenVariant        = "Forbidden"
	kvInvalidExpiryVariant    = "InvalidExpiry"
)

// MarshalCBOR writes the externally tagged error.
func (kvError KvError) MarshalCBOR() (cborform.Value, error) {
	switch kvError.Kind {
	case KvUnsupported:
		return taggedVariant(kvUnsupportedVariant, cborform.Text(kvError.Message)), nil
	case KvInvalidKey:
		return taggedVariant(kvInvalidKeyVariant, cborform.Text(kvError.Message)), nil
	case KvInvalidNamespace:
		return taggedVariant(kvInvalidNamespaceVariant, cborform.Text(kvError.Message)), nil
	case KvBackend:
		return taggedVariant(kvBackendVariant, cborform.Text(kvError.Message)), nil
	case KvUnavailable:
		return taggedVariant(kvUnavailableVariant, cborform.Text(kvError.Message)), nil
	case KvTooLarge:
		return marshalVariantStruct(kvTooLargeVariant, kvTooLargePayload{
			What: kvError.What, Size: kvError.Size, Cap: kvError.Cap,
		})
	case KvVersionMismatch:
		return marshalVariantStruct(kvVersionVariant, kvVersionPayloadPair{
			Expected: kvError.Expected, Got: kvError.Got,
		})
	case KvVersionConflict:
		return marshalVariantStruct(kvVersionConflictVariant, kvVersionConflictPayload{
			Current: kvError.Current,
		})
	case KvLeaseLost:
		return unitVariant(kvLeaseLostVariant), nil
	case KvLeaseHeld:
		return marshalVariantStruct(kvLeaseHeldVariant, kvLeaseHeldPayload{
			ExpiresAtMicros: kvError.ExpiresAtMicros,
		})
	case KvStale:
		return marshalVariantStruct(kvStaleVariant, kvStalePayload{Required: kvError.Required})
	case KvBarrierUnreachable:
		return marshalVariantStruct(kvBarrierUnreachableVar,
			kvStalePayload{Required: kvError.Required})
	case KvNotFound:
		return unitVariant(kvNotFoundVariant), nil
	case KvNotLeader:
		return unitVariant(kvNotLeaderVariant), nil
	case KvForbidden:
		return taggedVariant(kvForbiddenVariant, cborform.Text(kvError.Message)), nil
	case KvInvalidExpiry:
		return taggedVariant(kvInvalidExpiryVariant, cborform.Text(kvError.Message)), nil
	default:
		return nil, fmt.Errorf("wire: unknown key-value error %d", kvError.Kind)
	}
}

// UnmarshalCBOR reads the externally tagged error.
func (kvError *KvError) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}

	text := func(kind KvErrorKind) error {
		if err := requirePayload("KvError", name, payload); err != nil {
			return err
		}
		message, ok := payload.(cborform.Text)
		if !ok {
			return fmt.Errorf("wire: %q carries a message, got %T", name, payload)
		}
		*kvError = KvError{Kind: kind, Message: string(message)}
		return nil
	}

	switch name {
	case kvUnsupportedVariant:
		return text(KvUnsupported)
	case kvInvalidKeyVariant:
		return text(KvInvalidKey)
	case kvInvalidNamespaceVariant:
		return text(KvInvalidNamespace)
	case kvBackendVariant:
		return text(KvBackend)
	case kvUnavailableVariant:
		return text(KvUnavailable)
	case kvForbiddenVariant:
		return text(KvForbidden)
	case kvInvalidExpiryVariant:
		return text(KvInvalidExpiry)

	case kvTooLargeVariant:
		if err := requirePayload("KvError", name, payload); err != nil {
			return err
		}
		var detail kvTooLargePayload
		if err := fromValueInto(payload, &detail); err != nil {
			return err
		}
		*kvError = KvError{Kind: KvTooLarge, What: detail.What, Size: detail.Size, Cap: detail.Cap}
		return nil

	case kvVersionVariant:
		if err := requirePayload("KvError", name, payload); err != nil {
			return err
		}
		var detail kvVersionPayloadPair
		if err := fromValueInto(payload, &detail); err != nil {
			return err
		}
		*kvError = KvError{Kind: KvVersionMismatch, Expected: detail.Expected, Got: detail.Got}
		return nil

	case kvVersionConflictVariant:
		if err := requirePayload("KvError", name, payload); err != nil {
			return err
		}
		var detail kvVersionConflictPayload
		if err := fromValueInto(payload, &detail); err != nil {
			return err
		}
		*kvError = KvError{Kind: KvVersionConflict, Current: detail.Current}
		return nil

	case kvStaleVariant, kvBarrierUnreachableVar:
		if err := requirePayload("KvError", name, payload); err != nil {
			return err
		}
		var detail kvStalePayload
		if err := fromValueInto(payload, &detail); err != nil {
			return err
		}
		kind := KvStale
		if name == kvBarrierUnreachableVar {
			kind = KvBarrierUnreachable
		}
		*kvError = KvError{Kind: kind, Required: detail.Required}
		return nil

	case kvLeaseHeldVariant:
		if err := requirePayload("KvError", name, payload); err != nil {
			return err
		}
		var detail kvLeaseHeldPayload
		if err := fromValueInto(payload, &detail); err != nil {
			return err
		}
		*kvError = KvError{Kind: KvLeaseHeld, ExpiresAtMicros: detail.ExpiresAtMicros}
		return nil

	case kvLeaseLostVariant:
		*kvError = KvError{Kind: KvLeaseLost}
		return nil
	case kvNotFoundVariant:
		*kvError = KvError{Kind: KvNotFound}
		return nil
	case kvNotLeaderVariant:
		*kvError = KvError{Kind: KvNotLeader}
		return nil

	default:
		return unknownVariant("KvError", name)
	}
}

// Error renders the failure, matching the Rust messages so logs read the same
// across clients.
func (kvError KvError) Error() string {
	switch kvError.Kind {
	case KvUnsupported:
		return "kv not supported: " + kvError.Message
	case KvInvalidKey:
		return "invalid key: " + kvError.Message
	case KvInvalidNamespace:
		return "invalid namespace: " + kvError.Message
	case KvTooLarge:
		return fmt.Sprintf("%s is %dB, exceeds cap %dB", kvError.What, kvError.Size, kvError.Cap)
	case KvBackend:
		return "kv backend error: " + kvError.Message
	case KvUnavailable:
		return "temporarily unavailable: " + kvError.Message
	case KvVersionMismatch:
		return fmt.Sprintf("unsupported kv op version (expected %d, got %d)",
			kvError.Expected, kvError.Got)
	case KvVersionConflict:
		if kvError.Current == nil {
			return "kv version conflict (current: None)"
		}
		return fmt.Sprintf("kv version conflict (current: Some(%d))", *kvError.Current)
	case KvLeaseLost:
		return "kv lease lost"
	case KvLeaseHeld:
		if kvError.ExpiresAtMicros == nil {
			return "kv lease held (expires at: None)"
		}
		return fmt.Sprintf("kv lease held (expires at: Some(%d))", *kvError.ExpiresAtMicros)
	case KvStale:
		return fmt.Sprintf("kv read barrier not reached (required: %+v)", kvError.Required)
	case KvBarrierUnreachable:
		return fmt.Sprintf("kv read barrier unreachable (required: %+v)", kvError.Required)
	case KvNotFound:
		return "kv key not found"
	case KvNotLeader:
		return "not the partition leader for this key"
	case KvForbidden:
		return "kv operation not permitted: " + kvError.Message
	case KvInvalidExpiry:
		return "invalid kv expiry: " + kvError.Message
	default:
		return "unknown kv error"
	}
}

// ResultCode projects the failure onto the shared outcome dictionary.
func (kvError KvError) ResultCode() ResultCode {
	switch kvError.Kind {
	case KvUnsupported:
		return ResultUnsupported
	case KvInvalidKey, KvInvalidNamespace:
		return ResultInvalidArgument
	case KvTooLarge:
		return ResultTooLarge
	case KvBackend:
		return ResultBackend
	case KvUnavailable, KvNotLeader:
		return ResultUnavailable
	case KvVersionMismatch:
		return ResultVersionSkew
	case KvVersionConflict, KvLeaseLost, KvLeaseHeld:
		return ResultConflict
	case KvStale:
		return ResultStale
	case KvBarrierUnreachable:
		return ResultStaleGeneration
	case KvForbidden:
		return ResultForbidden
	case KvInvalidExpiry:
		return ResultInvalidArgument
	case KvNotFound:
		return ResultNotFound
	default:
		return ResultBackend
	}
}

// KvReply is the result of a key-value operation.
type KvReply struct {
	// Err is set on failure, and Outcome carries the success otherwise.
	Err     *KvError
	Outcome *KvOutcome
}

// OkReply wraps a successful outcome.
func OkReply(outcome KvOutcome) KvReply { return KvReply{Outcome: &outcome} }

// ErrReply wraps a failure.
func ErrReply(kvError KvError) KvReply { return KvReply{Err: &kvError} }

// IsOk reports a successful reply.
func (reply KvReply) IsOk() bool { return reply.Err == nil && reply.Outcome != nil }

const (
	kvOkVariant  = "Ok"
	kvErrVariant = "Err"
)

// MarshalCBOR writes the externally tagged reply.
func (reply KvReply) MarshalCBOR() (cborform.Value, error) {
	switch {
	case reply.Err != nil && reply.Outcome != nil:
		return nil, fmt.Errorf("wire: a key-value reply is either an outcome or an error")
	case reply.Err != nil:
		payload, err := reply.Err.MarshalCBOR()
		if err != nil {
			return nil, err
		}
		return taggedVariant(kvErrVariant, payload), nil
	case reply.Outcome != nil:
		payload, err := reply.Outcome.MarshalCBOR()
		if err != nil {
			return nil, err
		}
		return taggedVariant(kvOkVariant, payload), nil
	default:
		return nil, fmt.Errorf("wire: an empty key-value reply")
	}
}

// UnmarshalCBOR reads the externally tagged reply.
func (reply *KvReply) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	if err := requirePayload("KvReply", name, payload); err != nil {
		return err
	}
	switch name {
	case kvOkVariant:
		var outcome KvOutcome
		if err := outcome.UnmarshalCBOR(payload); err != nil {
			return err
		}
		*reply = KvReply{Outcome: &outcome}
		return nil
	case kvErrVariant:
		var kvError KvError
		if err := kvError.UnmarshalCBOR(payload); err != nil {
			return err
		}
		*reply = KvReply{Err: &kvError}
		return nil
	default:
		return unknownVariant("KvReply", name)
	}
}
