package laser

import (
	"context"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// Kv is a handle on one key-value namespace.
//
// The namespace is scoped to the handle's default stream unless the handle
// opted out, so two streams in one deployment keep separate namespaces. Keys
// are arbitrary bytes, which is why every read hands back a []byte rather than
// a string.
type Kv struct {
	laser     *Laser
	namespace string
}

// Kv opens a handle on a key-value namespace. Nothing is sent until a call is
// made on it.
func (laser *Laser) Kv(namespace string) *Kv {
	return &Kv{laser: laser, namespace: namespace}
}

// Namespace is the caller's own name for this namespace, unscoped.
func (kv *Kv) Namespace() string { return kv.namespace }

// ResourceNamespace is the name sent to the deployment, scoped when the handle
// scopes.
func (kv *Kv) ResourceNamespace() string { return kv.laser.ResourceName(kv.namespace) }

// execute sends one key-value command and opens its reply.
//
// The surface is gated twice: a deployment that does not serve the store is
// refused before the call leaves, and a deployment serving a different op
// version is refused rather than sent a payload it would read under the wrong
// contract.
func (kv *Kv) execute(
	ctx context.Context, code uint32, request any,
) (*wire.KvOutcome, error) {
	if err := kv.laser.requireKv(); err != nil {
		return nil, err
	}
	if err := wire.ValidateKvNamespace(kv.namespace); err != nil {
		return nil, &InvalidError{Reason: err.Error()}
	}
	payload, err := wire.Marshal(request)
	if err != nil {
		return nil, &CodecError{Reason: "encode key-value request", Cause: err}
	}
	raw, err := kv.laser.SendManaged(ctx, code, payload)
	if err != nil {
		return nil, err
	}
	var reply wire.KvReply
	if err := wire.Unmarshal(raw, &reply); err != nil {
		return nil, &CodecError{Reason: "decode key-value reply", Cause: err}
	}
	if !reply.IsOk() {
		if reply.Err == nil {
			return nil, &CodecError{Reason: "key-value reply carried neither an outcome nor an error"}
		}
		return nil, &KvError{Failure: *reply.Err}
	}
	return reply.Outcome, nil
}

// requireKv refuses a key-value call a deployment cannot serve.
func (laser *Laser) requireKv() error {
	if !laser.capabilities.Managed {
		return &UnsupportedError{Surface: "the key-value surface"}
	}
	if !laser.capabilities.Kv.Available {
		return &UnsupportedError{Surface: "the key-value surface"}
	}
	return laser.requireOpVersion("key-value", wire.KvOpVersion, func(v wire.OpVersions) uint32 {
		return v.Kv
	})
}

// requireOpVersion refuses a surface whose advertised op version is not the one
// this client pins.
//
// A server that advertised nothing predates the announcement, so the check
// passes and the round trip decides. A mismatch is refused here, because the
// payload would otherwise be read under a contract neither side agreed to.
func (laser *Laser) requireOpVersion(
	surface string, pinned uint32, advertised func(wire.OpVersions) uint32,
) error {
	if laser.capabilities.Versions == nil {
		return nil
	}
	served := advertised(*laser.capabilities.Versions)
	if served == pinned {
		return nil
	}
	return &VersionError{Surface: surface, Pinned: pinned, Served: served}
}

// Get reads the value at key, or nil when the key is absent or expired. The
// second result separates a stored empty value from an absent one.
func (kv *Kv) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	entry, err := kv.GetEntry(ctx, key)
	if err != nil || entry == nil {
		return nil, false, err
	}
	return entry.Value, true, nil
}

// GetEntry reads the whole entry at key (value, expiry, version), nil when the
// key is absent or expired.
func (kv *Kv) GetEntry(ctx context.Context, key []byte) (*wire.KvEntry, error) {
	if err := validatedKey(key); err != nil {
		return nil, err
	}
	outcome, err := kv.execute(ctx, wire.AgdxKvGetCode, wire.KvGet{
		V: wire.KvOpVersion, Namespace: kv.ResourceNamespace(), Key: key,
	})
	if err != nil {
		return nil, err
	}
	if outcome.Kind != wire.KvValue {
		return nil, unexpectedKvOutcome("get", outcome.Kind)
	}
	return outcome.Entry, nil
}

// GetEntryAtLeast reads the entry at key once the store has applied at least
// position, rather than reading whatever the serving replica holds now.
//
// This is what makes a handover safe. A lease grant carries the position its
// own write reached, so the next holder reading at that position sees the
// previous holder's work instead of racing it: a plain Get can land on a
// replica that has not applied it yet and answer "absent" for a key that was
// certainly written. Ported from Rust's Kv::get_entry_at_least; the wire has
// carried the bound (KvGet.MinPosition) all along.
//
// nil when the key is absent or expired, as with GetEntry.
func (kv *Kv) GetEntryAtLeast(
	ctx context.Context, key []byte, position wire.MutationPosition,
) (*wire.KvEntry, error) {
	if err := validatedKey(key); err != nil {
		return nil, err
	}
	outcome, err := kv.execute(ctx, wire.AgdxKvGetCode, wire.KvGet{
		V: wire.KvOpVersion, Namespace: kv.ResourceNamespace(), Key: key,
		MinPosition: &position,
	})
	if err != nil {
		return nil, err
	}
	if outcome.Kind != wire.KvValue {
		return nil, unexpectedKvOutcome("get", outcome.Kind)
	}
	return outcome.Entry, nil
}

// Revalidation is the answer to a conditional read.
type Revalidation struct {
	// NotModified is true when the live version still matched, so the body was
	// not transferred.
	NotModified bool
	// Entry is the current entry when it changed, nil when the key is absent.
	Entry *wire.KvEntry
}

// GetEntryIfChanged is a conditional read: it reports NotModified without the
// value while key still holds version, so an up-to-date cache skips the body
// transfer.
func (kv *Kv) GetEntryIfChanged(
	ctx context.Context, key []byte, version uint64,
) (Revalidation, error) {
	if err := validatedKey(key); err != nil {
		return Revalidation{}, err
	}
	outcome, err := kv.execute(ctx, wire.AgdxKvGetCode, wire.KvGet{
		V: wire.KvOpVersion, Namespace: kv.ResourceNamespace(), Key: key,
		IfNoneMatch: &version,
	})
	if err != nil {
		return Revalidation{}, err
	}
	switch outcome.Kind {
	case wire.KvNotModified:
		return Revalidation{NotModified: true}, nil
	case wire.KvValue:
		return Revalidation{Entry: outcome.Entry}, nil
	default:
		return Revalidation{}, unexpectedKvOutcome("get", outcome.Kind)
	}
}

// Exists reads a key's metadata without its value: the cheap way to check a
// precondition before transferring a large value. It returns nil when the key
// is absent or expired.
func (kv *Kv) Exists(ctx context.Context, key []byte) (*wire.KvMetadata, error) {
	if err := validatedKey(key); err != nil {
		return nil, err
	}
	outcome, err := kv.execute(ctx, wire.AgdxKvExistsCode, wire.KvExists{
		V: wire.KvOpVersion, Namespace: kv.ResourceNamespace(), Key: key,
	})
	if err != nil {
		return nil, err
	}
	if outcome.Kind != wire.KvMetadataOutcome {
		return nil, unexpectedKvOutcome("exists", outcome.Kind)
	}
	return outcome.Metadata, nil
}

// Delete removes a key, reporting whether a live entry was there to remove.
func (kv *Kv) Delete(ctx context.Context, key []byte) (bool, error) {
	return kv.delete(ctx, key, nil)
}

// DeleteIfVersion removes a key only while it still holds version, so a delete
// cannot race a writer that changed the value in between.
func (kv *Kv) DeleteIfVersion(ctx context.Context, key []byte, version uint64) (bool, error) {
	return kv.delete(ctx, key, &version)
}

func (kv *Kv) delete(ctx context.Context, key []byte, ifMatch *uint64) (bool, error) {
	if err := validatedKey(key); err != nil {
		return false, err
	}
	outcome, err := kv.execute(ctx, wire.AgdxKvDeleteCode, wire.KvDelete{
		V: wire.KvOpVersion, Namespace: kv.ResourceNamespace(), Key: key, IfMatch: ifMatch,
	})
	if err != nil {
		return false, err
	}
	if outcome.Kind != wire.KvDeleted {
		return false, unexpectedKvOutcome("delete", outcome.Kind)
	}
	return outcome.Deleted, nil
}

// Expire sets a key's expiry to ttl from the mutation's server timestamp,
// without rewriting the value. The relative form is used rather than an
// absolute one because a client clock cannot skew it.
func (kv *Kv) Expire(ctx context.Context, key []byte, ttl time.Duration) error {
	micros, err := ttlMicros(ttl)
	if err != nil {
		return err
	}
	return kv.expire(ctx, key, nil, &micros)
}

// ExpireAt sets a key's expiry to an absolute time, or clears it when zero.
func (kv *Kv) ExpireAt(ctx context.Context, key []byte, at time.Time) error {
	if at.IsZero() {
		return kv.expire(ctx, key, nil, nil)
	}
	micros := uint64(at.UnixMicro())
	return kv.expire(ctx, key, &micros, nil)
}

func (kv *Kv) expire(ctx context.Context, key []byte, at, ttl *uint64) error {
	if err := validatedKey(key); err != nil {
		return err
	}
	outcome, err := kv.execute(ctx, wire.AgdxKvExpireCode, wire.KvExpire{
		V: wire.KvOpVersion, Namespace: kv.ResourceNamespace(), Key: key,
		ExpiresAtMicros: at, TTLMicros: ttl,
	})
	if err != nil {
		return err
	}
	if outcome.Kind != wire.KvVersioned {
		return unexpectedKvOutcome("expire", outcome.Kind)
	}
	return nil
}

// Namespaces lists every namespace holding at least one entry for the caller,
// with the handle's own names rather than the scoped ones the store stores.
func (laser *Laser) KvNamespaces(ctx context.Context) ([]wire.KvNamespaceInfo, error) {
	if err := laser.requireKv(); err != nil {
		return nil, err
	}
	request := wire.KvNamespaces{V: wire.KvOpVersion}
	// In stream tenancy the listing names the stream it covers, so a
	// deployment serving many streams answers for this one.
	if stream := laser.resourceStream(); stream != "" {
		request.Stream = &stream
	}
	payload, err := wire.Marshal(request)
	if err != nil {
		return nil, &CodecError{Reason: "encode key-value request", Cause: err}
	}
	raw, err := laser.SendManaged(ctx, wire.AgdxKvNamespacesCode, payload)
	if err != nil {
		return nil, err
	}
	var reply wire.KvReply
	if err := wire.Unmarshal(raw, &reply); err != nil {
		return nil, &CodecError{Reason: "decode key-value reply", Cause: err}
	}
	if !reply.IsOk() {
		if reply.Err == nil {
			return nil, &CodecError{Reason: "key-value reply carried neither an outcome nor an error"}
		}
		return nil, &KvError{Failure: *reply.Err}
	}
	if reply.Outcome.Kind != wire.KvNamespacesOutcome {
		return nil, unexpectedKvOutcome("namespaces", reply.Outcome.Kind)
	}
	// A listing is answered with the stored names, so each is mapped back to
	// the caller's own. One belonging to another stream is not addressable
	// from this handle and is left out rather than handed back unusable.
	local := make([]wire.KvNamespaceInfo, 0, len(reply.Outcome.Namespaces))
	for _, info := range reply.Outcome.Namespaces {
		name, ok := laser.LocalResourceName(info.Namespace)
		if !ok {
			continue
		}
		local = append(local, wire.KvNamespaceInfo{Namespace: name, Entries: info.Entries})
	}
	return local, nil
}

func validatedKey(key []byte) error {
	if len(key) == 0 {
		return &InvalidError{Reason: "key-value key must not be empty"}
	}
	if len(key) > wire.MaxKeyBytes {
		return &InvalidError{Reason: "key-value key exceeds the " +
			itoa(wire.MaxKeyBytes) + " byte cap"}
	}
	return nil
}

// ttlMicros converts a relative lifetime, refusing one the contract cannot
// carry rather than truncating it into a different lifetime.
func ttlMicros(ttl time.Duration) (uint64, error) {
	if ttl <= 0 {
		return 0, &InvalidError{Reason: "a time to live must be positive"}
	}
	return uint64(ttl.Microseconds()), nil
}

func unexpectedKvOutcome(operation string, kind wire.KvOutcomeKind) error {
	return &CodecError{Reason: "key-value " + operation +
		" answered with an unexpected outcome: " + kvOutcomeName(kind)}
}

func kvOutcomeName(kind wire.KvOutcomeKind) string {
	switch kind {
	case wire.KvValue:
		return "value"
	case wire.KvWritten:
		return "written"
	case wire.KvCommitted:
		return "committed"
	case wire.KvDeleted:
		return "deleted"
	case wire.KvDeletedMany:
		return "deleted many"
	case wire.KvPageOutcome:
		return "page"
	case wire.KvNamespacesOutcome:
		return "namespaces"
	case wire.KvNotModified:
		return "not modified"
	case wire.KvMetadataOutcome:
		return "metadata"
	case wire.KvVersioned:
		return "versioned"
	case wire.KvLeased:
		return "leased"
	case wire.KvRenewed:
		return "renewed"
	case wire.KvReleased:
		return "released"
	default:
		return "kind " + itoa(int(kind))
	}
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	var digits [20]byte
	position := len(digits)
	for value > 0 {
		position--
		digits[position] = byte('0' + value%10)
		value /= 10
	}
	return string(digits[position:])
}
