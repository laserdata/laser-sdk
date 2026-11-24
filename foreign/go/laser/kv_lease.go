package laser

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// Lease is a granted revocable lease: the fencing token to present on
// protected mutations, the TTL the store granted (which may be shorter than
// requested), the mutation position at which the answering fold applied the
// grant or renewal (the barrier a takeover read passes back), and the
// instant the lease stops being valid for this process.
type Lease struct {
	Token      uint64
	GrantedTTL time.Duration
	Position   wire.MutationPosition
	// ValidUntil is this send's own time plus GrantedTTL. The store starts
	// the TTL no earlier than it received the request, so work that stops
	// before this instant never outlives the lease, however late the reply
	// arrived.
	ValidUntil time.Time
}

func leaseFromGrant(sent time.Time, grant *wire.KvLeaseGrant) *Lease {
	ttl := time.Duration(grant.GrantedTTLMicros) * time.Microsecond
	return &Lease{
		Token: grant.LeaseToken, GrantedTTL: ttl,
		Position: grant.Position, ValidUntil: sent.Add(ttl),
	}
}

// requireFencedLeases refuses a lease or fenced-CAS call a deployment
// cannot serve, before anything is sent. A server without the fenced-lease
// bit would decode the reshaped lease payloads under the old contract
// instead of rejecting them, so every lease path fails fast client-side.
func (kv *Kv) requireFencedLeases() error {
	if err := kv.laser.requireKv(); err != nil {
		return err
	}
	if !kv.laser.capabilities.Kv.FencedLeases {
		return &UnsupportedError{Surface: "the fenced-lease contract"}
	}
	return nil
}

// Lease acquires a revocable lease (a bounded-TTL distributed lock) on key
// as holder (a stable node or worker id, never a connection-scoped value).
// ttl is a requested maximum within [wire.MinLeaseTTLMicros,
// wire.MaxLeaseTTLMicros]; the store may grant less and never more, so a
// value outside that range is refused before the round trip. A holder
// needing longer renews with RenewLease, never by re-acquiring. A live
// lease always conflicts with a *KvError whose Kind is wire.KvLeaseHeld,
// naming the holder's expiry; TryLease answers (nil, nil) instead of
// failing. Requires the deployment's fenced-lease capability.
//
// Unlike Rust's Kv::lease, this does not retry an ambiguous outcome (a
// network failure after send but before reply): no Go key-value operation
// does, lease included, so an ambiguous send surfaces as a plain transport
// error here, the same as Set or a plain compare-and-swap already do.
func (kv *Kv) Lease(
	ctx context.Context, key []byte, holder string, ttl time.Duration,
) (*Lease, error) {
	if err := kv.requireFencedLeases(); err != nil {
		return nil, err
	}
	if err := validatedKey(key); err != nil {
		return nil, err
	}
	request := wire.KvLease{
		V: wire.KvLeaseOpVersion, Namespace: kv.ResourceNamespace(), Key: key,
		LeaseTTLMicros: uint64(ttl.Microseconds()), HolderID: holder,
	}
	if err := request.Validate(); err != nil {
		return nil, &InvalidError{Reason: err.Error()}
	}
	sent := time.Now()
	outcome, err := kv.execute(ctx, wire.AgdxKvLeaseCode, request)
	if err != nil {
		return nil, err
	}
	if outcome.Kind != wire.KvLeased || outcome.Lease == nil {
		return nil, unexpectedKvOutcome("lease", outcome.Kind)
	}
	return leaseFromGrant(sent, outcome.Lease), nil
}

// TryLease is Lease, answering (nil, nil) instead of failing when a live
// lease already holds key. Every other failure, an ambiguous outcome
// included, is returned as Lease returns it.
func (kv *Kv) TryLease(
	ctx context.Context, key []byte, holder string, ttl time.Duration,
) (*Lease, error) {
	lease, err := kv.Lease(ctx, key, holder, ttl)
	if err == nil {
		return lease, nil
	}
	var kvErr *KvError
	if errors.As(err, &kvErr) && kvErr.Kind() == wire.KvLeaseHeld {
		return nil, nil
	}
	return nil, err
}

// RenewLease extends a held lease without changing its token, presenting
// the same holder and the token the grant returned. ttl obeys the same
// range as Lease. An expired, released, or re-acquired lease surfaces as a
// *KvError whose Kind is wire.KvLeaseLost: stop the protected work, a new
// epoch needs a fresh Lease.
func (kv *Kv) RenewLease(
	ctx context.Context, key []byte, holder string, token uint64, ttl time.Duration,
) (*Lease, error) {
	if err := kv.requireFencedLeases(); err != nil {
		return nil, err
	}
	if err := validatedKey(key); err != nil {
		return nil, err
	}
	request := wire.KvLeaseRenew{
		V: wire.KvLeaseOpVersion, Namespace: kv.ResourceNamespace(), Key: key,
		HolderID: holder, LeaseToken: token, LeaseTTLMicros: uint64(ttl.Microseconds()),
	}
	if err := request.Validate(); err != nil {
		return nil, &InvalidError{Reason: err.Error()}
	}
	sent := time.Now()
	outcome, err := kv.execute(ctx, wire.AgdxKvLeaseRenewCode, request)
	if err != nil {
		return nil, err
	}
	if outcome.Kind != wire.KvRenewed || outcome.Lease == nil {
		return nil, unexpectedKvOutcome("renew_lease", outcome.Kind)
	}
	return leaseFromGrant(sent, outcome.Lease), nil
}

// ReleaseLease releases a held lease early, presenting the same holder and
// the token the grant returned. Returns true when a held lease was
// released, false when none was held (idempotent). A stale token surfaces
// as a *KvError whose Kind is wire.KvLeaseLost.
func (kv *Kv) ReleaseLease(
	ctx context.Context, key []byte, holder string, token uint64,
) (bool, error) {
	if err := kv.requireFencedLeases(); err != nil {
		return false, err
	}
	if err := validatedKey(key); err != nil {
		return false, err
	}
	request := wire.KvRelease{
		V: wire.KvLeaseOpVersion, Namespace: kv.ResourceNamespace(), Key: key,
		LeaseToken: token, HolderID: holder,
	}
	outcome, err := kv.execute(ctx, wire.AgdxKvReleaseCode, request)
	if err != nil {
		return false, err
	}
	if outcome.Kind != wire.KvReleased {
		return false, unexpectedKvOutcome("release", outcome.Kind)
	}
	return outcome.Released, nil
}

// LeaseGuard is a lease held by a caller, releasing it on Release.
//
// Unlike Rust's LeaseGuard, dropping this without calling Release does not
// release the lease in the background: Go has no destructor to hook a
// best-effort cleanup task to, so an exit path that forgets Release leaves
// the key held until its TTL lapses rather than freeing it at once.
type LeaseGuard struct {
	kv     *Kv
	key    []byte
	holder string
	lease  *Lease
	held   bool
}

// LeaseGuard is Lease, held by a LeaseGuard.
func (kv *Kv) LeaseGuard(
	ctx context.Context, key []byte, holder string, ttl time.Duration,
) (*LeaseGuard, error) {
	lease, err := kv.Lease(ctx, key, holder, ttl)
	if err != nil {
		return nil, err
	}
	return &LeaseGuard{kv: kv, key: key, holder: holder, lease: lease, held: true}, nil
}

// Lease is the current grant: the fencing token to present and its local
// deadline.
func (guard *LeaseGuard) Lease() *Lease { return guard.lease }

// Held reports whether this guard still believes it holds the lease. A
// lease can expire or be lost without this process learning until its next
// Renew or a protected mutation refuses.
func (guard *LeaseGuard) Held() bool { return guard.held }

// Renew extends the lease for ttl, keeping its token.
func (guard *LeaseGuard) Renew(ctx context.Context, ttl time.Duration) (*Lease, error) {
	lease, err := guard.kv.RenewLease(ctx, guard.key, guard.holder, guard.lease.Token, ttl)
	if err != nil {
		var kvErr *KvError
		if errors.As(err, &kvErr) && kvErr.Kind() == wire.KvLeaseLost {
			guard.held = false
		}
		return nil, err
	}
	guard.lease = lease
	return guard.lease, nil
}

// Release releases the lease now. true when it was still held.
func (guard *LeaseGuard) Release(ctx context.Context) (bool, error) {
	guard.held = false
	return guard.kv.ReleaseLease(ctx, guard.key, guard.holder, guard.lease.Token)
}

// KvCasFencedRequest is an unsent fenced compare-and-swap. Nothing reaches
// the store until Commit.
type KvCasFencedRequest struct {
	kv             *Kv
	key            []byte
	value          []byte
	expiresAt      *uint64
	ttl            *uint64
	expect         wire.CasExpect
	fenceNamespace string
	fenceKey       []byte
	fenceToken     uint64
	err            error
}

// CasFenced begins a fenced compare-and-swap on key: the write applies in
// one backend transaction that requires a live lease at (fenceNamespace,
// fenceKey) and a fence sequence still equal to fenceToken (the values a
// Lease/LeaseGuard returned). Supply a value and a precondition
// (ExpectVersion/ExpectAbsent), then Commit. The at-most-one-effective-
// writer gate for an exclusive effect: a zombie holder at an older token,
// or one whose lease expired or was released, is rejected before another
// holder acquires.
func (kv *Kv) CasFenced(
	key []byte, fenceNamespace string, fenceKey []byte, fenceToken uint64,
) *KvCasFencedRequest {
	request := &KvCasFencedRequest{
		kv: kv, key: key, fenceNamespace: kv.laser.ResourceName(fenceNamespace),
		fenceKey: fenceKey, fenceToken: fenceToken,
	}
	request.err = validatedKey(key)
	return request
}

// Bytes sets the value to store.
func (request *KvCasFencedRequest) Bytes(value []byte) *KvCasFencedRequest {
	request.value = value
	return request
}

// JSON encodes value as JSON and stores that.
func (request *KvCasFencedRequest) JSON(value any) *KvCasFencedRequest {
	encoded, err := json.Marshal(value)
	if err != nil {
		request.fail(&CodecError{Reason: "encode the key-value value as JSON", Cause: err})
		return request
	}
	request.value = encoded
	return request
}

// TTL expires the entry this long after the mutation's server timestamp.
func (request *KvCasFencedRequest) TTL(ttl time.Duration) *KvCasFencedRequest {
	micros, err := ttlMicros(ttl)
	if err != nil {
		request.fail(err)
		return request
	}
	request.ttl = &micros
	return request
}

// ExpiresAt expires the entry at an absolute time.
func (request *KvCasFencedRequest) ExpiresAt(at time.Time) *KvCasFencedRequest {
	micros := uint64(at.UnixMicro())
	request.expiresAt = &micros
	return request
}

// ExpectVersion applies the write only while key still holds version.
func (request *KvCasFencedRequest) ExpectVersion(version uint64) *KvCasFencedRequest {
	request.expect = wire.MatchVersion(version)
	return request
}

// ExpectAbsent applies the write only while key is absent.
func (request *KvCasFencedRequest) ExpectAbsent() *KvCasFencedRequest {
	request.expect = wire.AbsentKey()
	return request
}

func (request *KvCasFencedRequest) fail(err error) {
	if request.err == nil {
		request.err = err
	}
}

// Commit applies the fenced write and returns the new version, which a
// caller chains into the next conditional write without a re-read.
func (request *KvCasFencedRequest) Commit(ctx context.Context) (uint64, error) {
	if request.err != nil {
		return 0, request.err
	}
	if err := request.kv.requireFencedLeases(); err != nil {
		return 0, err
	}
	outcome, err := request.kv.execute(ctx, wire.AgdxKvCasFencedCode, wire.KvCasFenced{
		V: wire.KvOpVersion, Namespace: request.kv.ResourceNamespace(),
		Key: request.key, Value: request.value,
		ExpiresAtMicros: request.expiresAt, TTLMicros: request.ttl,
		Expect:         request.expect,
		FenceNamespace: request.fenceNamespace, FenceKey: request.fenceKey,
		FenceToken: request.fenceToken,
	})
	if err != nil {
		return 0, err
	}
	if outcome.Kind != wire.KvCommitted {
		return 0, unexpectedKvOutcome("cas_fenced", outcome.Kind)
	}
	return outcome.Version, nil
}
