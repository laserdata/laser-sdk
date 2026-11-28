package laser

import (
	"context"
	"encoding/json"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// KvSetRequest is an unsent write. Nothing reaches the store until Send, or
// until a precondition turns it into a compare-and-swap.
//
// A builder error is carried rather than returned at each step, so a chain
// reads as one expression and still reports the first thing that was wrong.
type KvSetRequest struct {
	kv          *Kv
	key         []byte
	value       []byte
	expiresAt   *uint64
	ttl         *uint64
	conditional bool
	err         error
}

// Set begins a write at key.
func (kv *Kv) Set(key []byte) *KvSetRequest {
	request := &KvSetRequest{kv: kv, key: key}
	request.err = validatedKey(key)
	return request
}

// Bytes sets the value to store.
func (request *KvSetRequest) Bytes(value []byte) *KvSetRequest {
	request.value = value
	return request
}

// JSON encodes value as JSON and stores that.
func (request *KvSetRequest) JSON(value any) *KvSetRequest {
	encoded, err := json.Marshal(value)
	if err != nil {
		request.fail(&CodecError{Reason: "encode the key-value value as JSON", Cause: err})
		return request
	}
	request.value = encoded
	return request
}

// EncodeWith encodes value with codec and stores the bytes.
func (request *KvSetRequest) EncodeWith(codec wire.Codec, value any) *KvSetRequest {
	if codec == nil {
		request.fail(&InvalidError{Reason: "missing key-value value codec"})
		return request
	}
	encoded, err := codec.Encode(value)
	if err != nil {
		request.fail(&CodecError{Reason: "encode the key-value value", Cause: err})
		return request
	}
	request.value = encoded
	return request
}

// Msgpack encodes value as MessagePack and stores that.
func (request *KvSetRequest) Msgpack(value any) *KvSetRequest {
	return request.EncodeWith(wire.MsgpackCodec, value)
}

// TTL expires the entry this long after the mutation's server timestamp. The
// relative form is used rather than an absolute one because a client clock
// cannot skew it.
func (request *KvSetRequest) TTL(ttl time.Duration) *KvSetRequest {
	micros, err := ttlMicros(ttl)
	if err != nil {
		request.fail(err)
		return request
	}
	request.expiresAt = nil
	request.ttl = &micros
	return request
}

// ExpiresAt expires the entry at an absolute time.
func (request *KvSetRequest) ExpiresAt(at time.Time) *KvSetRequest {
	if at.UnixMicro() < 0 {
		request.fail(&InvalidError{Reason: "expiry must be at or after the Unix epoch"})
		return request
	}
	request.ttl = nil
	micros := uint64(at.UnixMicro())
	request.expiresAt = &micros
	return request
}

// ExpectVersion turns the write into a compare-and-swap that applies only
// while the key still holds version.
func (request *KvSetRequest) ExpectVersion(version uint64) *KvCasRequest {
	request.conditional = true
	return &KvCasRequest{set: request, expect: wire.MatchVersion(version)}
}

// ExpectAbsent turns the write into a compare-and-swap that applies only while
// the key is absent, which is how a caller claims a name exactly once.
func (request *KvSetRequest) ExpectAbsent() *KvCasRequest {
	request.conditional = true
	return &KvCasRequest{set: request, expect: wire.AbsentKey()}
}

// fail keeps the first error, because a later step acting on an already broken
// request would report the wrong cause.
func (request *KvSetRequest) fail(err error) {
	if request.err == nil {
		request.err = err
	}
}

// Send stores the value, overwriting whatever was there.
func (request *KvSetRequest) Send(ctx context.Context) error {
	if request.err != nil {
		return request.err
	}
	if request.conditional {
		return &InvalidError{Reason: "a conditional key-value write requires Commit"}
	}
	write := wire.KvSet{
		Session:         request.kv.session,
		V:               wire.KvOpVersion,
		Namespace:       request.kv.ResourceNamespace(),
		Key:             request.key,
		Value:           request.value,
		ExpiresAtMicros: request.expiresAt,
		TTLMicros:       request.ttl,
	}
	if err := write.Validate(); err != nil {
		return &InvalidError{Reason: err.Error()}
	}
	outcome, err := request.kv.execute(ctx, wire.AgdxKvSetCode, write)
	if err != nil {
		return err
	}
	if outcome.Kind != wire.KvWritten {
		return unexpectedKvOutcome("set", outcome.Kind)
	}
	return nil
}

// KvCasRequest is a conditional write. It applies only while the key matches
// the precondition, so two writers racing on one key cannot both win.
type KvCasRequest struct {
	set    *KvSetRequest
	expect wire.CasExpect
}

// TTL expires the entry this long after the mutation's server timestamp.
func (request *KvCasRequest) TTL(ttl time.Duration) *KvCasRequest {
	request.set.TTL(ttl)
	return request
}

// ExpiresAt expires the entry at an absolute time.
func (request *KvCasRequest) ExpiresAt(at time.Time) *KvCasRequest {
	request.set.ExpiresAt(at)
	return request
}

// Commit applies the write and returns the new version, which a caller chains
// into the next conditional write without a re-read.
//
// A precondition that did not hold is reported as a KvError whose Conflict
// reports true, not as a retryable failure: repeating the same write would lose
// the same race again.
func (request *KvCasRequest) Commit(ctx context.Context) (uint64, error) {
	if request.set.err != nil {
		return 0, request.set.err
	}
	if len(request.set.value) > wire.MaxValueBytes {
		return 0, &InvalidError{Reason: "key-value value exceeds the size cap"}
	}
	if !request.set.kv.laser.Capabilities().Kv.CAS {
		return 0, &UnsupportedError{Surface: "key-value compare-and-swap"}
	}
	outcome, err := request.set.kv.execute(ctx, wire.AgdxKvCasCode, wire.KvCas{
		Session:         request.set.kv.session,
		V:               wire.KvOpVersion,
		Namespace:       request.set.kv.ResourceNamespace(),
		Key:             request.set.key,
		Value:           request.set.value,
		ExpiresAtMicros: request.set.expiresAt,
		TTLMicros:       request.set.ttl,
		Expect:          request.expect,
	})
	if err != nil {
		return 0, err
	}
	if outcome.Kind != wire.KvCommitted {
		return 0, unexpectedKvOutcome("compare-and-swap", outcome.Kind)
	}
	return outcome.Version, nil
}

// Patch applies a merge patch and returns the entry's new version.
func (kv *Kv) Patch(ctx context.Context, key, patch []byte) (uint64, error) {
	return kv.patch(ctx, key, patch, nil)
}

// PatchIfVersion applies a merge patch only if the entry still has version.
func (kv *Kv) PatchIfVersion(ctx context.Context, key, patch []byte, version uint64) (uint64, error) {
	return kv.patch(ctx, key, patch, &version)
}

func (kv *Kv) patch(ctx context.Context, key, patch []byte, version *uint64) (uint64, error) {
	if err := validatedKey(key); err != nil {
		return 0, err
	}
	outcome, err := kv.execute(ctx, wire.AgdxKvPatchCode, wire.KvPatch{
		V: wire.KvOpVersion, Namespace: kv.ResourceNamespace(), Session: kv.session,
		Key: key, Patch: patch, IfMatch: version,
	})
	if err != nil {
		return 0, err
	}
	if outcome.Kind != wire.KvVersioned {
		return 0, unexpectedKvOutcome("patch", outcome.Kind)
	}
	return outcome.Version, nil
}

// KvCopyRequest copies or moves an entry in one managed command.
type KvCopyRequest struct {
	kv          *Kv
	key, toKey  []byte
	toNamespace *string
	code        uint32
}

// CopyTo copies key to toKey when Send is called.
func (kv *Kv) CopyTo(key, toKey []byte) *KvCopyRequest {
	return &KvCopyRequest{kv: kv, key: key, toKey: toKey, code: wire.AgdxKvCopyCode}
}

// MoveTo moves key to toKey when Send is called.
func (kv *Kv) MoveTo(key, toKey []byte) *KvCopyRequest {
	return &KvCopyRequest{kv: kv, key: key, toKey: toKey, code: wire.AgdxKvMoveCode}
}

// IntoNamespace selects the destination namespace.
func (request *KvCopyRequest) IntoNamespace(namespace string) *KvCopyRequest {
	request.toNamespace = &namespace
	return request
}

// Send applies the copy or move and returns the destination version.
func (request *KvCopyRequest) Send(ctx context.Context) (uint64, error) {
	if err := validatedKey(request.key); err != nil {
		return 0, err
	}
	if err := validatedKey(request.toKey); err != nil {
		return 0, err
	}
	body := wire.KvCopy{V: wire.KvOpVersion, Namespace: request.kv.ResourceNamespace(), Key: request.key, ToKey: request.toKey}
	if request.toNamespace != nil {
		if err := wire.ValidateKvNamespace(*request.toNamespace); err != nil {
			return 0, &InvalidError{Reason: err.Error()}
		}
		dst := request.kv.laser.ResourceName(*request.toNamespace)
		if err := wire.ValidateKvNamespace(dst); err != nil {
			return 0, &InvalidError{Reason: err.Error()}
		}
		body.ToNamespace = &dst
	}
	outcome, err := request.kv.execute(ctx, request.code, body)
	if err != nil {
		return 0, err
	}
	if outcome.Kind != wire.KvCommitted {
		return 0, unexpectedKvOutcome("copy or move", outcome.Kind)
	}
	return outcome.Version, nil
}
