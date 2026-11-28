package laser

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	iggy "github.com/apache/iggy/foreign/go/client"

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

func (kv *Kv) leaseFromGrant(sent time.Time, requested time.Duration, grant *wire.KvLeaseGrant) (*Lease, error) {
	if grant.LeaseToken == 0 || grant.GrantedTTLMicros > uint64(requested.Microseconds()) {
		return nil, &CodecError{Reason: "invalid token or TTL in lease grant"}
	}
	ttl := time.Duration(grant.GrantedTTLMicros) * time.Microsecond
	validUntil := sent.Add(ttl)
	if !time.Now().Before(validUntil) {
		return nil, &KvError{Failure: wire.KvError{Kind: wire.KvLeaseLost}}
	}
	kv.laser.observeKvPosition(grant.Position)
	return &Lease{
		Token: grant.LeaseToken, GrantedTTL: ttl,
		Position: grant.Position, ValidUntil: validUntil,
	}, nil
}

// requireFencedLeases refuses a lease or fenced-CAS call a deployment
// cannot serve, before anything is sent. A server without the fenced-lease
// bit would decode the reshaped lease payloads under the old contract
// instead of rejecting them, so every lease path fails fast client-side.
func (kv *Kv) requireFencedLeases() error {
	if err := kv.laser.requireKv(); err != nil {
		return err
	}
	if !kv.laser.Capabilities().Kv.FencedLeases {
		return &UnsupportedError{Surface: "the fenced-lease contract"}
	}
	return nil
}

// Lease acquires a revocable lease (a bounded-TTL distributed lock) on key
// as holder (a stable node or worker id, never a connection-scoped value).
// ttl is a requested maximum within [wire.MinLeaseTTLMicros,
// wire.MaxLeaseTTLMicros]. The store may grant less and never more, so a
// value outside that range is refused before the round trip. A holder
// needing longer renews with RenewLease, never by re-acquiring. A live
// lease always conflicts with a *KvError whose Kind is wire.KvLeaseHeld,
// naming the holder's expiry. TryLease answers (nil, nil) instead of
// failing. Requires the deployment's fenced-lease capability.
//
// The acquisition runs on this connection's dedicated coordination
// connection, so a handle from FromClient refuses with a ConfigError. An
// ambiguous outcome repeats the same operation identity until the requested
// TTL since the first send has passed, and only then is an
// AmbiguousMutationError. A grant that arrives after its ValidUntil is a
// *KvError whose Kind is wire.KvLeaseLost.
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
	if err := wire.ValidateKvNamespace(kv.namespace); err != nil {
		return nil, &InvalidError{Reason: err.Error()}
	}
	if err := request.Validate(); err != nil {
		return nil, &InvalidError{Reason: err.Error()}
	}
	if err := kv.laser.ensureScopable(wire.AgdxKvLeaseCode); err != nil {
		return nil, err
	}
	coordination, err := kv.laser.coordinationClient()
	if err != nil {
		return nil, err
	}
	prepared, err := coordination.PrepareAcquire(request)
	if err != nil {
		return nil, err
	}
	lease, err := coordination.acquireResolving(ctx, prepared)
	if err != nil {
		return nil, err
	}
	if lease.Token == 0 || lease.GrantedTTL.Microseconds() > ttl.Microseconds() {
		return nil, &CodecError{Reason: "invalid token or TTL in lease grant"}
	}
	kv.laser.observeKvPosition(lease.Position)
	return lease, nil
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
	if outcome.Lease.LeaseToken != token {
		return nil, &CodecError{Reason: "lease renewal changed its fencing token"}
	}
	return kv.leaseFromGrant(sent, ttl, outcome.Lease)
}

// ReleaseLease releases a held lease early, presenting the same holder and
// the token the grant returned. Returns true when a held lease was
// released, false when none was held (idempotent). A stale token surfaces
// as a *KvError whose Kind is wire.KvLeaseLost.
func (kv *Kv) ReleaseLease(
	ctx context.Context, key []byte, holder string, token uint64,
) (bool, error) {
	prepared, err := kv.PrepareRelease(key, holder, token)
	if err != nil {
		return false, err
	}
	return prepared.Execute(ctx)
}

// LeaseGuard is a lease held by a caller, releasing it on Release.
//
// Unlike Rust's LeaseGuard, dropping this without calling Release does not
// release the lease in the background: Go has no destructor to hook a
// best-effort cleanup task to, so an exit path that forgets Release leaves
// the key held until its TTL lapses rather than freeing it at once.
type LeaseGuard struct {
	mutex  sync.Mutex
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
	return &LeaseGuard{kv: kv, key: bytes.Clone(key), holder: holder, lease: lease, held: true}, nil
}

// Lease is the current grant: the fencing token to present and its local
// deadline.
func (guard *LeaseGuard) Lease() *Lease {
	guard.mutex.Lock()
	defer guard.mutex.Unlock()
	snapshot := *guard.lease
	return &snapshot
}

// Held reports whether this guard still believes it holds the lease. A
// lease can expire or be lost without this process learning until its next
// Renew or a protected mutation refuses.
func (guard *LeaseGuard) Held() bool {
	guard.mutex.Lock()
	defer guard.mutex.Unlock()
	return guard.held
}

// Renew extends the lease for ttl, keeping its token.
func (guard *LeaseGuard) Renew(ctx context.Context, ttl time.Duration) (*Lease, error) {
	guard.mutex.Lock()
	defer guard.mutex.Unlock()
	lease, err := guard.kv.RenewLease(ctx, guard.key, guard.holder, guard.lease.Token, ttl)
	if err != nil {
		var kvErr *KvError
		if errors.As(err, &kvErr) && kvErr.Kind() == wire.KvLeaseLost {
			guard.held = false
		}
		return nil, err
	}
	guard.lease = lease
	snapshot := *lease
	return &snapshot, nil
}

// Release releases the lease now. true when it was still held.
func (guard *LeaseGuard) Release(ctx context.Context) (bool, error) {
	guard.mutex.Lock()
	defer guard.mutex.Unlock()
	guard.held = false
	return guard.kv.ReleaseLease(ctx, guard.key, guard.holder, guard.lease.Token)
}

// renewUntil renews on the coordination connection, repeating an ambiguous renewal under one identity until deadline.
func (guard *LeaseGuard) renewUntil(ctx context.Context, ttl time.Duration, deadline time.Time) (*Lease, error) {
	guard.mutex.Lock()
	defer guard.mutex.Unlock()
	coordination, err := guard.kv.laser.coordinationClient()
	if err != nil {
		return nil, err
	}
	prepared, err := coordination.PrepareRenew(wire.KvLeaseRenew{
		V: wire.KvLeaseOpVersion, Namespace: guard.kv.ResourceNamespace(), Key: guard.key,
		HolderID: guard.holder, LeaseToken: guard.lease.Token, LeaseTTLMicros: uint64(ttl.Microseconds()),
	})
	if err != nil {
		return nil, err
	}
	lease, err := coordination.renewResolving(ctx, prepared, deadline)
	if err != nil {
		var kvErr *KvError
		if errors.As(err, &kvErr) && kvErr.Kind() == wire.KvLeaseLost {
			guard.held = false
		}
		return nil, err
	}
	guard.kv.laser.observeKvPosition(lease.Position)
	guard.lease = lease
	snapshot := *lease
	return &snapshot, nil
}

// KvCasFencedRequest is an unsent fenced compare-and-swap. Nothing reaches
// the store until Commit.
type KvCasFencedRequest struct {
	kv             *Kv
	key            []byte
	value          []byte
	expiresAt      *uint64
	ttl            *uint64
	expect         *wire.CasExpect
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

// EncodeWith encodes value with codec and stores that.
func (request *KvCasFencedRequest) EncodeWith(codec wire.Codec, value any) *KvCasFencedRequest {
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
func (request *KvCasFencedRequest) Msgpack(value any) *KvCasFencedRequest {
	return request.EncodeWith(wire.MsgpackCodec, value)
}

// TTL expires the entry this long after the mutation's server timestamp.
func (request *KvCasFencedRequest) TTL(ttl time.Duration) *KvCasFencedRequest {
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
func (request *KvCasFencedRequest) ExpiresAt(at time.Time) *KvCasFencedRequest {
	if at.UnixMicro() < 0 {
		request.fail(&InvalidError{Reason: "expiry must be at or after the Unix epoch"})
		return request
	}
	request.ttl = nil
	micros := uint64(at.UnixMicro())
	request.expiresAt = &micros
	return request
}

// ExpectVersion applies the write only while key still holds version.
func (request *KvCasFencedRequest) ExpectVersion(version uint64) *KvCasFencedRequest {
	request.expect = ptrOf(wire.MatchVersion(version))
	return request
}

// ExpectAbsent applies the write only while key is absent.
func (request *KvCasFencedRequest) ExpectAbsent() *KvCasFencedRequest {
	request.expect = ptrOf(wire.AbsentKey())
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
	if request.expect == nil {
		return 0, &InvalidError{Reason: "fenced commit requires ExpectAbsent or ExpectVersion"}
	}
	if err := request.kv.requireFencedLeases(); err != nil {
		return 0, err
	}
	write := wire.KvCasFenced{
		Session: request.kv.session,
		V:       wire.KvLeaseOpVersion, Namespace: request.kv.ResourceNamespace(),
		Key: request.key, Value: request.value,
		ExpiresAtMicros: request.expiresAt, TTLMicros: request.ttl,
		Expect:         *request.expect,
		FenceNamespace: request.fenceNamespace, FenceKey: request.fenceKey,
		FenceToken: request.fenceToken,
	}
	if err := write.Validate(); err != nil {
		return 0, &InvalidError{Reason: err.Error()}
	}
	outcome, err := request.kv.execute(ctx, wire.AgdxKvCasFencedCode, write)
	if err != nil {
		return 0, err
	}
	if outcome.Kind != wire.KvCommitted {
		return 0, unexpectedKvOutcome("cas_fenced", outcome.Kind)
	}
	return outcome.Version, nil
}

// coordinationClient is this connection's fenced-lease client on its own connection, opened on first use.
func (laser *Laser) coordinationClient() (*FencedLeaseClient, error) {
	shared := laser.state()
	shared.mutex.Lock()
	defer shared.mutex.Unlock()
	if shared.client == nil {
		return nil, &TransportError{Reason: "the client is closed"}
	}
	if shared.coordination != nil {
		return shared.coordination, nil
	}
	parsed := laser.Connection()
	if parsed == nil {
		return nil, configError("a lease needs a Laser opened from a connection string, " +
			"use FencedLeaseClient with your own transport for a client passed to FromClient")
	}
	shared.coordination = NewFencedLeaseClient(
		&DedicatedKvTransport{dial: dialCoordination(parsed)})
	return shared.coordination, nil
}

// dialCoordination dials the deployment this handle is connected to, from its parsed connection string.
func dialCoordination(parsed *ConnectionString) func(context.Context) (*Laser, error) {
	return func(ctx context.Context) (*Laser, error) {
		options, err := parsed.TCPOptions()
		if err != nil {
			return nil, err
		}
		connected, err := iggy.NewIggyClient(iggy.WithTcp(options...))
		if err != nil {
			return nil, &TransportError{Reason: "build the client", Cause: err}
		}
		if err := connected.Connect(ctx); err != nil {
			_ = connected.Close()
			return nil, &TransportError{Reason: "connect", Cause: err}
		}
		configured := OpenCapabilities()
		return &Laser{
			shared:         &connection{client: connected, configured: &configured, linkState: EventConnected},
			connectionInfo: func() *ConnectionString { return parsed },
			capabilities:   NegotiateCapabilities(ctx, connected, configured),
			publish:        DefaultPublishOptions(),
		}, nil
	}
}

// observeKvPosition raises the key-value read frontier to a position a grant or renewal returned.
func (laser *Laser) observeKvPosition(position wire.MutationPosition) {
	shared := laser.state()
	shared.mutex.Lock()
	wire.MergeFrontier(&shared.frontier, wire.AppliedFrontier{Kv: &position})
	shared.mutex.Unlock()
}
