package wire

import (
	"fmt"
	"unicode/utf8"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// MemoryRowScope is the memory scope a read-view row carries, stamped by the
// fold from the record's headers so recall reconstructs a memory item and
// narrows by scope. A generic key-value entry carries none.
type MemoryRowScope struct {
	// Kind is the item kind word, from the record body.
	Kind *string `cbor:"kind,omitempty"`
	// Agent is the producing agent.
	Agent *string `cbor:"agent,omitempty"`
	// User is the user scope layer.
	User *string `cbor:"user,omitempty"`
	// App is the app scope layer.
	App *string `cbor:"app,omitempty"`
	// Conversation is the conversation that wrote the record.
	Conversation *string `cbor:"conversation,omitempty"`
	// Source is the origin log record this item was folded from, so a reader
	// can navigate back to the source message while it is still on the log.
	Source *SourceRef `cbor:"source,omitempty"`
	// TimestampMicros is the streaming server's append time of the source
	// record. With the source address it orders items remembered on different
	// partitions.
	TimestampMicros *uint64 `cbor:"timestamp_micros,omitempty"`
	// Weight is the feedback weight folded for this item, the sum of every
	// feedback record that targets it. Recall ranks every strategy except
	// recent by it. Absent when no feedback was recorded.
	Weight *float32 `cbor:"weight,omitempty"`
}

// KvEntry is one stored entry: arbitrary-bytes key and value plus optional
// expiry. Key and value ride the wire as CBOR byte strings, byte-exact.
type KvEntry struct {
	Key   []byte `cbor:"key"`
	Value []byte `cbor:"value"`
	// ExpiresAtMicros is an absolute expiry, or absent for none. Expired
	// entries are hidden on read.
	ExpiresAtMicros *uint64 `cbor:"expires_at_micros,omitempty"`
	// Version is the optimistic-concurrency token, assigned by the store and
	// bumped on every successful mutation. A versioned backend reports one or
	// more for a live entry and reserves zero solely for "unversioned", so a
	// caller must never treat zero as a valid compare token. Skipped on the
	// wire when zero, so those entries stay byte-identical.
	Version uint64 `cbor:"version,omitempty"`
	// Scope is present only on a memory read-view row.
	Scope *MemoryRowScope `cbor:"scope,omitempty"`
	// Source is the origin log record this entry was folded from. Every
	// managed write goes log-first through a mutation topic, so a stored entry
	// points back to the record that wrote it.
	Source *SourceRef `cbor:"source,omitempty"`
}

// KeyString is the key decoded as UTF-8. The second result is false when the
// key is not valid UTF-8, since keys are arbitrary bytes and a binary key has
// no string form.
func (entry KvEntry) KeyString() (string, bool) {
	if !utf8.Valid(entry.Key) {
		return "", false
	}
	return string(entry.Key), true
}

// KvPage is one page of scanned entries, in key order. Pages count from one.
type KvPage struct {
	Items        []KvEntry `cbor:"items"`
	Page         uint32    `cbor:"page"`
	TotalPages   uint32    `cbor:"total_pages"`
	TotalResults uint32    `cbor:"total_results"`
}

// KvGet reads the value at a key.
//
// With IfNoneMatch set to a version, the read returns NotModified instead of
// the value when the live version matches, so an up-to-date cache skips the
// body transfer. With MinPosition set the read is barriered: the answering fold
// must have applied at least that position, so a takeover reader cannot observe
// a fold predating its own lease acquisition. Only a deployment advertising
// FeatureKvFencedLeases honors the barrier, so a client must not send it
// otherwise.
type KvGet struct {
	V           uint32            `cbor:"v"`
	Namespace   string            `cbor:"namespace"`
	Key         []byte            `cbor:"key"`
	IfNoneMatch *uint64           `cbor:"if_none_match,omitempty"`
	MinPosition *MutationPosition `cbor:"min_position,omitempty"`
}

// KvSet writes a value at a key, with an optional expiry.
type KvSet struct {
	V               uint32      `cbor:"v"`
	Namespace       string      `cbor:"namespace"`
	Session         *SessionRef `cbor:"session,omitempty"`
	Key             []byte      `cbor:"key"`
	Value           []byte      `cbor:"value"`
	ExpiresAtMicros *uint64     `cbor:"expires_at_micros,omitempty"`
	// TTLMicros is the relative form a client clock cannot skew, resolved
	// once at fold time so every replica stores the same expiry. It is
	// mutually exclusive with ExpiresAtMicros, and a request carrying both
	// is refused.
	TTLMicros *uint64 `cbor:"ttl_micros,omitempty"`
}

// CasExpectKind discriminates a compare-and-swap precondition.
type CasExpectKind uint8

const (
	// CasMatch applies only at an exact version.
	CasMatch CasExpectKind = iota
	// CasAbsent applies only if the key does not exist, a create-if-absent.
	CasAbsent
)

// CasExpect is the precondition a compare-and-swap write must satisfy: the
// compare half of lock-free optimistic concurrency.
type CasExpect struct {
	Kind    CasExpectKind
	Version uint64
}

// MatchVersion applies only at this exact entry version.
func MatchVersion(version uint64) CasExpect {
	return CasExpect{Kind: CasMatch, Version: version}
}

// AbsentKey applies only if the key does not currently exist.
func AbsentKey() CasExpect { return CasExpect{Kind: CasAbsent} }

const (
	casMatchVariant  = "Match"
	casAbsentVariant = "Absent"
)

// MarshalCBOR writes the externally tagged precondition.
func (expect CasExpect) MarshalCBOR() (cborform.Value, error) {
	switch expect.Kind {
	case CasMatch:
		return taggedVariant(casMatchVariant, cborform.Uint(expect.Version)), nil
	case CasAbsent:
		return unitVariant(casAbsentVariant), nil
	default:
		return nil, fmt.Errorf("wire: unknown compare-and-swap precondition %d", expect.Kind)
	}
}

// UnmarshalCBOR reads the externally tagged precondition.
func (expect *CasExpect) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	switch name {
	case casMatchVariant:
		if err := requirePayload("CasExpect", name, payload); err != nil {
			return err
		}
		version, ok := payload.(cborform.Uint)
		if !ok {
			return fmt.Errorf("wire: a version precondition must be an integer, got %T", payload)
		}
		*expect = MatchVersion(uint64(version))
		return nil
	case casAbsentVariant:
		*expect = AbsentKey()
		return nil
	default:
		return unknownVariant("CasExpect", name)
	}
}

// KvCas is a compare-and-swap write: apply the value only if the precondition
// holds, else fail with a version conflict.
type KvCas struct {
	V               uint32      `cbor:"v"`
	Namespace       string      `cbor:"namespace"`
	Session         *SessionRef `cbor:"session,omitempty"`
	Key             []byte      `cbor:"key"`
	Value           []byte      `cbor:"value"`
	ExpiresAtMicros *uint64     `cbor:"expires_at_micros,omitempty"`
	// TTLMicros is the relative form a client clock cannot skew, resolved
	// once at fold time so every replica stores the same expiry. It is
	// mutually exclusive with ExpiresAtMicros, and a request carrying both
	// is refused.
	TTLMicros *uint64   `cbor:"ttl_micros,omitempty"`
	Expect    CasExpect `cbor:"expect"`
}

// KvCasFenced is a compare-and-swap applied in one backend transaction that
// also requires a live lease and a fence sequence still equal to the token.
//
// A released or expired lease, or a stale token, maps to a lost lease even
// before another holder acquires. A failed precondition maps to a version
// conflict.
type KvCasFenced struct {
	V               uint32      `cbor:"v"`
	Namespace       string      `cbor:"namespace"`
	Session         *SessionRef `cbor:"session,omitempty"`
	Key             []byte      `cbor:"key"`
	Value           []byte      `cbor:"value"`
	ExpiresAtMicros *uint64     `cbor:"expires_at_micros,omitempty"`
	// TTLMicros is the relative form a client clock cannot skew, resolved
	// once at fold time so every replica stores the same expiry. It is
	// mutually exclusive with ExpiresAtMicros, and a request carrying both
	// is refused.
	TTLMicros *uint64   `cbor:"ttl_micros,omitempty"`
	Expect    CasExpect `cbor:"expect"`
	// FenceNamespace holds the lease and fence rows. It is authorized
	// separately from the target namespace, so a state writer can present a
	// fence without being able to mutate it.
	FenceNamespace string `cbor:"fence_namespace"`
	// FenceKey is the lease key whose live row and fence sequence gate this
	// write.
	FenceKey []byte `cbor:"fence_key"`
	// FenceToken is the fence-sequence value the holder was granted. Strictly
	// monotonic per key, and never the lease-row version.
	FenceToken uint64 `cbor:"fence_token"`
}

// KvDelete removes a key. With IfMatch set, the delete applies only when the
// live version matches.
type KvDelete struct {
	V         uint32      `cbor:"v"`
	Namespace string      `cbor:"namespace"`
	Session   *SessionRef `cbor:"session,omitempty"`
	Key       []byte      `cbor:"key"`
	IfMatch   *uint64     `cbor:"if_match,omitempty"`
}

// KvExists tests presence and reads metadata without the value: the cheap way
// to check a precondition before transferring a large value.
type KvExists struct {
	V         uint32 `cbor:"v"`
	Namespace string `cbor:"namespace"`
	Key       []byte `cbor:"key"`
}

// KvExpire sets, refreshes or clears a key's expiry in place without rewriting
// its value. An absent expiry clears it.
type KvExpire struct {
	V               uint32  `cbor:"v"`
	Namespace       string  `cbor:"namespace"`
	Key             []byte  `cbor:"key"`
	ExpiresAtMicros *uint64 `cbor:"expires_at_micros,omitempty"`
	// TTLMicros expires the entry this long after the mutation's server
	// timestamp, the relative form a client clock cannot skew. It is mutually
	// exclusive with ExpiresAtMicros, and a request carrying both is refused.
	TTLMicros *uint64 `cbor:"ttl_micros,omitempty"`
}

// KvPatch applies a merge patch to a structured value without transferring the
// whole object. The patch is a codec-specific document.
type KvPatch struct {
	V         uint32      `cbor:"v"`
	Namespace string      `cbor:"namespace"`
	Session   *SessionRef `cbor:"session,omitempty"`
	Key       []byte      `cbor:"key"`
	Patch     []byte      `cbor:"patch"`
	IfMatch   *uint64     `cbor:"if_match,omitempty"`
}

// KvCopy copies a value to another key, optionally into another namespace, in
// one backend transaction. The destination is overwritten, so a guarded copy
// composes exists and compare-and-swap instead.
type KvCopy struct {
	V         uint32 `cbor:"v"`
	Namespace string `cbor:"namespace"`
	Key       []byte `cbor:"key"`
	// ToNamespace is the destination namespace. Absent means the source.
	ToNamespace *string `cbor:"to_namespace,omitempty"`
	ToKey       []byte  `cbor:"to_key"`
}

// KvMove is a copy plus a delete of the source, in one backend transaction.
type KvMove struct {
	V           uint32  `cbor:"v"`
	Namespace   string  `cbor:"namespace"`
	Key         []byte  `cbor:"key"`
	ToNamespace *string `cbor:"to_namespace,omitempty"`
	ToKey       []byte  `cbor:"to_key"`
}

// KvLease acquires a revocable lease, a bounded-TTL distributed lock.
//
// On success the store atomically creates the live lease row and bumps the
// never-expiring fence sequence. A live lease always conflicts: renewal is a
// renew request, never re-acquisition.
type KvLease struct {
	V         uint32 `cbor:"v"`
	Namespace string `cbor:"namespace"`
	Key       []byte `cbor:"key"`
	// LeaseTTLMicros is the requested maximum lifetime, within the pinned
	// range. The store may grant less and never grants more, which is why the
	// range is validated on the request rather than clamped.
	LeaseTTLMicros uint64 `cbor:"lease_ttl_micros"`
	// HolderID is the stable identity of the acquiring holder, never a
	// connection-scoped value. Renewal and release must present the same
	// holder, so another process cannot extend or drop a lease it does not
	// hold.
	HolderID string `cbor:"holder_id"`
	// SubjectUserID names the user the lease protects mutations for. Absent
	// leases for the authenticated caller. Set is a delegated acquisition and
	// requires the lease-admin grant.
	SubjectUserID *uint32 `cbor:"subject_user_id,omitempty"`
}

// KvLeaseRenew extends a held lease without changing its token.
//
// The store requires the same holder and subject, a live lease row and the
// current fence token, then moves the expiry forward. Renewal never bumps the
// fence. An expired or re-acquired lease answers lease-lost, so a deposed
// holder learns immediately.
type KvLeaseRenew struct {
	V              uint32  `cbor:"v"`
	Namespace      string  `cbor:"namespace"`
	Key            []byte  `cbor:"key"`
	HolderID       string  `cbor:"holder_id"`
	SubjectUserID  *uint32 `cbor:"subject_user_id,omitempty"`
	LeaseToken     uint64  `cbor:"lease_token"`
	LeaseTTLMicros uint64  `cbor:"lease_ttl_micros"`
}

// KvRelease releases a held lease early.
//
// The token is validated against the fence sequence, never the lease-row
// version, which resets on re-acquire. A missing or expired row under a
// still-current fence is an idempotent release, and a stale fence is
// lease-lost.
type KvRelease struct {
	V          uint32 `cbor:"v"`
	Namespace  string `cbor:"namespace"`
	Key        []byte `cbor:"key"`
	LeaseToken uint64 `cbor:"lease_token"`
	HolderID   string `cbor:"holder_id"`
}

// KvMetadata is one key's metadata without its value.
type KvMetadata struct {
	Version         uint64  `cbor:"version"`
	ExpiresAtMicros *uint64 `cbor:"expires_at_micros,omitempty"`
	SizeBytes       uint64  `cbor:"size_bytes"`
}

// KvNamespaces lists every namespace holding at least one entry for the caller.
type KvNamespaces struct {
	V uint32 `cbor:"v"`
	// Stream is the stream whose resources the listing covers. It is required
	// in stream tenancy.
	Stream *string `cbor:"stream,omitempty"`
}

// KvNamespaceInfo is one namespace summary.
type KvNamespaceInfo struct {
	Namespace string `cbor:"namespace"`
	Entries   uint64 `cbor:"entries"`
}

// KvScan lists entries in a namespace.
//
// With no bounds it lists the whole namespace. Prefix matches keys starting
// with it in byte order. Start and End bound an inclusive-start,
// exclusive-end range. KeyContains keeps only keys that are valid UTF-8 and
// hold the substring, skipping binary keys. All bounds compose.
type KvScan struct {
	V           uint32  `cbor:"v"`
	Namespace   string  `cbor:"namespace"`
	Prefix      []byte  `cbor:"prefix,omitempty"`
	Start       []byte  `cbor:"start,omitempty"`
	End         []byte  `cbor:"end,omitempty"`
	KeyContains *string `cbor:"key_contains,omitempty"`
	// Conversation is the conversation lens: keep only rows that conversation
	// wrote. The memory read view stamps it on each record it materializes and
	// generic rows leave it unset, so a scan narrows to one conversation's
	// memory.
	Conversation *string `cbor:"conversation,omitempty"`
	// Stream is the stream the lens conversation belongs to, set alongside it
	// by a client that scopes its resources to a stream.
	Stream *string `cbor:"stream,omitempty"`
	// User, Agent and App are the memory scope lenses: keep only memory-view
	// rows stamped with them. Generic key-value rows carry none, so a lens
	// filters them out.
	User  *string `cbor:"user,omitempty"`
	Agent *string `cbor:"agent,omitempty"`
	App   *string `cbor:"app,omitempty"`
	// Page is 1-based, over the matches in key order. Zero reads the first
	// page.
	Page uint32 `cbor:"page"`
	// Results is the entries per page, at most the scan cap. Zero reads the
	// default.
	Results uint32 `cbor:"results"`
}

// KvDeleteMany bulk-deletes entries matching the same composed bounds as a
// scan. With no bounds it clears the whole namespace. Expiry is ignored, so a
// matching expired entry is removed too.
type KvDeleteMany struct {
	V            uint32  `cbor:"v"`
	Namespace    string  `cbor:"namespace"`
	Prefix       []byte  `cbor:"prefix,omitempty"`
	Start        []byte  `cbor:"start,omitempty"`
	End          []byte  `cbor:"end,omitempty"`
	KeyContains  *string `cbor:"key_contains,omitempty"`
	Conversation *string `cbor:"conversation,omitempty"`
	Stream       *string `cbor:"stream,omitempty"`
}

// ValidateKvNamespace is the canonical namespace rule, shared by the client
// edge and every serving tier: non-empty, within the cap, and free of ASCII
// control characters. The charset is otherwise open, so slash-style hierarchy
// stays legal. The bound is a size and sanity cap, not a safelist.
func ValidateKvNamespace(namespace string) error { return ValidateNamespace(namespace) }

// ResolveExpiry turns a write's two expiry forms into the one absolute expiry
// the store records.
//
// A write names an absolute instant or a relative lifetime, never both: the
// relative form is resolved once here, at fold time, so every replica stores
// the same expiry rather than each resolving against its own clock.
func ResolveExpiry(expiresAtMicros, ttlMicros *uint64, nowMicros uint64) (*uint64, error) {
	switch {
	case expiresAtMicros != nil && ttlMicros != nil:
		return nil, &InvalidError{
			Reason: "a write names either expires_at_micros or ttl_micros, not both",
		}
	case expiresAtMicros != nil:
		return expiresAtMicros, nil
	case ttlMicros != nil:
		// Saturating, so a lifetime past the end of the epoch pins there
		// rather than wrapping to an expiry in the past.
		expiry := nowMicros + *ttlMicros
		if expiry < nowMicros {
			expiry = ^uint64(0)
		}
		return &expiry, nil
	default:
		return nil, nil
	}
}
