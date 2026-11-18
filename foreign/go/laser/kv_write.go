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
	kv        *Kv
	key       []byte
	value     []byte
	expiresAt *uint64
	ttl       *uint64
	err       error
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

// TTL expires the entry this long after the mutation's server timestamp. The
// relative form is used rather than an absolute one because a client clock
// cannot skew it.
func (request *KvSetRequest) TTL(ttl time.Duration) *KvSetRequest {
	micros, err := ttlMicros(ttl)
	if err != nil {
		request.fail(err)
		return request
	}
	if request.expiresAt != nil {
		request.fail(&InvalidError{
			Reason: "a key-value write takes a time to live or an absolute expiry, not both"})
		return request
	}
	request.ttl = &micros
	return request
}

// ExpiresAt expires the entry at an absolute time.
func (request *KvSetRequest) ExpiresAt(at time.Time) *KvSetRequest {
	if request.ttl != nil {
		request.fail(&InvalidError{
			Reason: "a key-value write takes a time to live or an absolute expiry, not both"})
		return request
	}
	micros := uint64(at.UnixMicro())
	request.expiresAt = &micros
	return request
}

// ExpectVersion turns the write into a compare-and-swap that applies only
// while the key still holds version.
func (request *KvSetRequest) ExpectVersion(version uint64) *KvCasRequest {
	return &KvCasRequest{set: request, expect: wire.MatchVersion(version)}
}

// ExpectAbsent turns the write into a compare-and-swap that applies only while
// the key is absent, which is how a caller claims a name exactly once.
func (request *KvSetRequest) ExpectAbsent() *KvCasRequest {
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
	outcome, err := request.kv.execute(ctx, wire.AgdxKvSetCode, wire.KvSet{
		V:               wire.KvOpVersion,
		Namespace:       request.kv.ResourceNamespace(),
		Key:             request.key,
		Value:           request.value,
		ExpiresAtMicros: request.expiresAt,
		TTLMicros:       request.ttl,
	})
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
	if !request.set.kv.laser.capabilities.Kv.CAS {
		return 0, &UnsupportedError{Surface: "key-value compare-and-swap"}
	}
	outcome, err := request.set.kv.execute(ctx, wire.AgdxKvCasCode, wire.KvCas{
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
