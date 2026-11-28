package laser_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// fencedCapabilities is a deployment serving the store's full fenced-lease
// contract, which every lease or fenced-CAS call requires.
func fencedCapabilities() *laser.Capabilities {
	capabilities := kvCapabilities()
	capabilities.Kv.FencedLeases = true
	capabilities.Kv.CASFenced = true
	return capabilities
}

// fencedLeaseHandle is a fenced deployment whose lease acquisitions reach the fake
// server over an injected coordination transport.
func fencedLeaseHandle(server iggcon.Client) *laser.Laser {
	return laser.CoordinateOverClientForTest(
		kvHandle(server, laser.Options{Capabilities: fencedCapabilities()}))
}

func TestGivenAGrantedLeaseWhenAcquiredThenShouldReturnTheTokenAndTTL(t *testing.T) {
	server := &kvServer{outcomes: []wire.KvOutcome{{
		Kind: wire.KvLeased,
		Lease: &wire.KvLeaseGrant{
			LeaseToken: 7, GrantedTTLMicros: uint64((2 * time.Second).Microseconds()),
		},
	}}}
	kv := fencedLeaseHandle(server).Kv("ns")

	lease, err := kv.Lease(context.Background(), []byte("job-1"), "worker-a", 5*time.Second)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	if lease.Token != 7 {
		t.Errorf("want token 7, got %d", lease.Token)
	}
	if lease.GrantedTTL != 2*time.Second {
		t.Errorf("want the granted (possibly shorter) TTL, got %s", lease.GrantedTTL)
	}
	if !lease.ValidUntil.After(time.Now()) {
		t.Error("want ValidUntil in the future")
	}

	var request wire.KvLease
	decodeRequest(t, server, 0, &request)
	if request.HolderID != "worker-a" {
		t.Errorf("want the holder carried, got %q", request.HolderID)
	}
	if string(request.Key) != "job-1" {
		t.Errorf("want the key carried, got %q", request.Key)
	}
}

func TestGivenALeaseHeldByAnotherHolderWhenTriedThenShouldAnswerNil(t *testing.T) {
	expires := uint64(time.Now().Add(time.Minute).UnixMicro())
	server := &kvServer{failure: &wire.KvError{
		Kind: wire.KvLeaseHeld, ExpiresAtMicros: &expires,
	}}
	kv := fencedLeaseHandle(server).Kv("ns")

	lease, err := kv.TryLease(context.Background(), []byte("job-1"), "worker-a", 5*time.Second)
	if err != nil {
		t.Fatalf("want no error for a held lease, got %v", err)
	}
	if lease != nil {
		t.Errorf("want nil for a held lease, got %+v", lease)
	}
}

func TestGivenAnOrdinaryLeaseFailureWhenTriedThenShouldStillReportIt(t *testing.T) {
	server := &kvServer{failure: &wire.KvError{Kind: wire.KvBackend, Message: "down"}}
	kv := fencedLeaseHandle(server).Kv("ns")

	_, err := kv.TryLease(context.Background(), []byte("job-1"), "worker-a", 5*time.Second)

	var failure *laser.KvError
	if !errors.As(err, &failure) || failure.Kind() != wire.KvBackend {
		t.Fatalf("want the backend failure reported, got %T: %v", err, err)
	}
}

func TestGivenNoFencedLeaseCapabilityWhenLeasedThenShouldRefuseBeforeSending(t *testing.T) {
	server := &kvServer{}
	kv := kvHandle(server, laser.Options{Capabilities: kvCapabilities()}).Kv("ns")

	_, err := kv.Lease(context.Background(), []byte("job-1"), "worker-a", 5*time.Second)

	var unsupported *laser.UnsupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("want an UnsupportedError, got %T: %v", err, err)
	}
	if len(server.codes) != 0 {
		t.Error("an unsupported lease call must not reach the socket")
	}
}

func TestGivenATTLOutsideThePinnedRangeWhenLeasedThenShouldRefuseBeforeSending(t *testing.T) {
	server := &kvServer{}
	kv := fencedLeaseHandle(server).Kv("ns")

	_, err := kv.Lease(context.Background(), []byte("job-1"), "worker-a", time.Millisecond)

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
	if len(server.codes) != 0 {
		t.Error("an out-of-range TTL must not reach the socket")
	}
}

func TestGivenARenewalWhenSentThenShouldKeepTheSameTokenAndExtendTheTTL(t *testing.T) {
	server := &kvServer{outcomes: []wire.KvOutcome{{
		Kind: wire.KvRenewed,
		Lease: &wire.KvLeaseGrant{
			LeaseToken: 7, GrantedTTLMicros: uint64((3 * time.Second).Microseconds()),
		},
	}}}
	kv := kvHandle(server, laser.Options{Capabilities: fencedCapabilities()}).Kv("ns")

	lease, err := kv.RenewLease(context.Background(), []byte("job-1"), "worker-a", 7, 10*time.Second)
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	if lease.Token != 7 {
		t.Errorf("want the same token, got %d", lease.Token)
	}

	var request wire.KvLeaseRenew
	decodeRequest(t, server, 0, &request)
	if request.LeaseToken != 7 {
		t.Errorf("want the token presented back, got %d", request.LeaseToken)
	}
}

func TestGivenALeaseGuardWhenReleasedThenShouldReportWhetherItWasHeld(t *testing.T) {
	server := &kvServer{outcomes: []wire.KvOutcome{
		{Kind: wire.KvLeased, Lease: &wire.KvLeaseGrant{LeaseToken: 1, GrantedTTLMicros: 1_000_000}},
		{Kind: wire.KvReleased, Released: true},
	}}
	kv := fencedLeaseHandle(server).Kv("ns")

	guard, err := kv.LeaseGuard(context.Background(), []byte("job-1"), "worker-a", time.Second)
	if err != nil {
		t.Fatalf("lease guard: %v", err)
	}
	released, err := guard.Release(context.Background())
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if !released {
		t.Error("want the lease reported released")
	}
	if guard.Held() {
		t.Error("want the guard to no longer consider itself held")
	}

	var request wire.KvRelease
	decodeRequest(t, server, 1, &request)
	if request.LeaseToken != 1 {
		t.Errorf("want the guard's own token presented, got %d", request.LeaseToken)
	}
}

func TestGivenALeaseLostWhenRenewedThenShouldMarkTheGuardNotHeld(t *testing.T) {
	server := &kvServer{outcomes: []wire.KvOutcome{
		{Kind: wire.KvLeased, Lease: &wire.KvLeaseGrant{LeaseToken: 1, GrantedTTLMicros: 1_000_000}},
	}}
	kv := fencedLeaseHandle(server).Kv("ns")
	guard, err := kv.LeaseGuard(context.Background(), []byte("job-1"), "worker-a", time.Second)
	if err != nil {
		t.Fatalf("lease guard: %v", err)
	}
	server.outcomes = nil
	server.failure = &wire.KvError{Kind: wire.KvLeaseLost}

	_, err = guard.Renew(context.Background(), time.Second)

	var failure *laser.KvError
	if !errors.As(err, &failure) || failure.Kind() != wire.KvLeaseLost {
		t.Fatalf("want a lease-lost failure, got %T: %v", err, err)
	}
	if guard.Held() {
		t.Error("want the guard to mark itself no longer held")
	}
}

func TestGivenAFencedCompareAndSwapWhenCommittedThenShouldCarryTheFenceToken(t *testing.T) {
	server := &kvServer{outcomes: []wire.KvOutcome{{Kind: wire.KvCommitted, Version: 3}}}
	kv := kvHandle(server, laser.Options{Capabilities: fencedCapabilities()}).Kv("ns")

	version, err := kv.CasFenced([]byte("job-1"), "agdx.workflow.fence", []byte("run-1"), 7).
		Bytes([]byte("done")).
		ExpectAbsent().
		Commit(context.Background())
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if version != 3 {
		t.Errorf("want the new version, got %d", version)
	}

	var request wire.KvCasFenced
	decodeRequest(t, server, 0, &request)
	if request.FenceToken != 7 {
		t.Errorf("want the fence token carried, got %d", request.FenceToken)
	}
	if string(request.FenceKey) != "run-1" {
		t.Errorf("want the fence key carried, got %q", request.FenceKey)
	}
	if request.Expect.Kind != wire.CasAbsent {
		t.Errorf("want the absent precondition carried, got %v", request.Expect.Kind)
	}
}

func TestGivenNoFencedLeaseCapabilityWhenCommittedThenShouldRefuseBeforeSending(t *testing.T) {
	server := &kvServer{}
	kv := kvHandle(server, laser.Options{Capabilities: kvCapabilities()}).Kv("ns")

	_, err := kv.CasFenced([]byte("job-1"), "fence-ns", []byte("run-1"), 7).
		Bytes([]byte("done")).
		ExpectAbsent().
		Commit(context.Background())

	var unsupported *laser.UnsupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("want an UnsupportedError, got %T: %v", err, err)
	}
	if len(server.codes) != 0 {
		t.Error("an unsupported fenced CAS must not reach the socket")
	}
}

func TestGivenAPreparedReleaseWhenRepeatedThenShouldSendTheIdenticalFrame(t *testing.T) {
	server := &kvServer{outcomes: []wire.KvOutcome{{Kind: wire.KvReleased, Released: true}}}
	kv := kvHandle(server, laser.Options{Capabilities: fencedCapabilities()}).Kv("coord")
	key := []byte("lease")
	release, err := kv.PrepareRelease(key, "worker", 1)
	if err != nil {
		t.Fatal(err)
	}
	key[0] = 'x'
	if len(server.requests) != 0 || release.OperationID().IsZero() {
		t.Fatal("expected a local preparation with a nonzero operation ID")
	}
	for range 2 {
		released, err := release.Execute(t.Context())
		if !released || err != nil {
			t.Fatalf("release: %t, %v", released, err)
		}
	}
	var request wire.KvRelease
	decodeRequest(t, server, 0, &request)
	if string(request.Key) != "lease" || !bytes.Equal(server.requests[0], server.requests[1]) {
		t.Fatal("repeated release changed the key or operation identity")
	}
}

func TestGivenAnExpiredGrantWhenAcquiredThenShouldReturnLeaseLost(t *testing.T) {
	server := &kvServer{outcomes: []wire.KvOutcome{{
		Kind: wire.KvLeased, Lease: &wire.KvLeaseGrant{LeaseToken: 7},
	}}}
	kv := fencedLeaseHandle(server).Kv("ns")
	lease, err := kv.Lease(context.Background(), []byte("work"), "worker", time.Second)
	var failure *laser.KvError
	if lease != nil || !errors.As(err, &failure) || failure.Kind() != wire.KvLeaseLost {
		t.Fatalf("expected lease lost, got %+v, %v", lease, err)
	}
}

func TestGivenAHandleOverAnInjectedClientWhenLeasedThenShouldRefuseAsConfigBeforeSending(t *testing.T) {
	server := &kvServer{}
	kv := kvHandle(server, laser.Options{Capabilities: fencedCapabilities()}).Kv("ns")

	lease, err := kv.Lease(context.Background(), []byte("job-1"), "worker-a", 5*time.Second)
	guard, guarded := kv.LeaseGuard(context.Background(), []byte("job-1"), "worker-a", 5*time.Second)
	tried, attempted := kv.TryLease(context.Background(), []byte("job-1"), "worker-a", 5*time.Second)

	for _, refused := range []error{err, guarded, attempted} {
		var config *laser.ConfigError
		if !errors.As(refused, &config) {
			t.Fatalf("a handle with no connection string cannot dial a coordination connection, got %T: %v", refused, refused)
		}
	}
	if lease != nil || guard != nil || tried != nil || len(server.codes) != 0 {
		t.Errorf("nothing may be granted or sent, got %+v, %+v, %+v and %d requests", lease, guard, tried, len(server.codes))
	}
}

func TestGivenAnAmbiguousAcquisitionWhenLeasedThenShouldRepeatTheSameIdentityUntilGranted(t *testing.T) {
	transport := answering(
		unknownOutcomeReply(t, wire.ResultPending),
		scripted{err: &laser.TransportError{Reason: "managed command", Cause: ierror.Disconnected{}}},
		okReply(t, leasedOutcome(4, 5*time.Second)),
	)
	handle := laser.SetCoordinationTransportForTest(
		kvHandle(&kvServer{}, laser.Options{Capabilities: fencedCapabilities()}), transport)

	lease, err := handle.Kv("ns").Lease(context.Background(), []byte("job-1"), "worker-a", 5*time.Second)

	if err != nil {
		t.Fatalf("the repeat answers the recorded grant: %v", err)
	}
	if lease.Token != 4 || lease.GrantedTTL != 5*time.Second {
		t.Errorf("want token 4 for 5s, got %+v", lease)
	}
	sent := transport.frames()
	if len(sent) != 3 || !bytes.Equal(sent[0].frame, sent[1].frame) || !bytes.Equal(sent[1].frame, sent[2].frame) {
		t.Fatalf("want three attempts under one identity, got %d frames", len(sent))
	}
	if seen := handle.SeenPositions().Kv; seen == nil || *seen != leasePosition() {
		t.Errorf("the grant position joins the read frontier, got %+v", seen)
	}
}

func TestGivenAnAcquisitionStillUnknownWhenItsTTLPassesThenShouldReportAmbiguityWithTheIdentity(t *testing.T) {
	replies := make([]scripted, 64)
	for index := range replies {
		replies[index] = unknownOutcomeReply(t, wire.ResultPending)
	}
	transport := answering(replies...)
	handle := laser.SetCoordinationTransportForTest(
		kvHandle(&kvServer{}, laser.Options{Capabilities: fencedCapabilities()}), transport)

	_, err := handle.Kv("ns").Lease(context.Background(), []byte("job-1"), "worker-a", time.Second)

	var ambiguous *laser.AmbiguousMutationError
	if !errors.As(err, &ambiguous) || ambiguous.OperationID.IsZero() {
		t.Fatalf("want an ambiguous acquisition naming its identity, got %T: %v", err, err)
	}
	if sent := transport.frames(); len(sent) < 3 {
		t.Errorf("repeats fill the TTL window, got %d attempts", len(sent))
	}
}

func TestGivenAGrantBeyondTheRequestWhenLeasedThenShouldRefuseIt(t *testing.T) {
	for _, grant := range []wire.KvOutcome{leasedOutcome(0, time.Second), leasedOutcome(7, time.Minute)} {
		transport := answering(okReply(t, grant))
		handle := laser.SetCoordinationTransportForTest(
			kvHandle(&kvServer{}, laser.Options{Capabilities: fencedCapabilities()}), transport)

		lease, err := handle.Kv("ns").Lease(context.Background(), []byte("job-1"), "worker-a", 5*time.Second)

		var codec *laser.CodecError
		if lease != nil || !errors.As(err, &codec) {
			t.Errorf("want a zero token or a TTL past the request refused, got %+v and %T: %v", lease, err, err)
		}
	}
}

func TestGivenAHandleWithACoordinationConnectionWhenClosedThenShouldRetireIt(t *testing.T) {
	transport := answering(okReply(t, leasedOutcome(4, 5*time.Second)))
	handle := laser.SetCoordinationTransportForTest(
		kvHandle(&kvServer{}, laser.Options{Capabilities: fencedCapabilities()}), transport)
	if _, err := handle.Kv("ns").Lease(context.Background(), []byte("job-1"), "worker-a", 5*time.Second); err != nil {
		t.Fatalf("lease: %v", err)
	}

	if err := handle.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	_, err := handle.Kv("ns").Lease(context.Background(), []byte("job-2"), "worker-a", 5*time.Second)

	if resets := transport.resetCount(); resets != 1 {
		t.Errorf("want the coordination connection retired once, got %d", resets)
	}
	var closed *laser.TransportError
	if !errors.As(err, &closed) || len(transport.frames()) != 1 {
		t.Errorf("a closed handle leases nothing, got %T: %v after %d frames", err, err, len(transport.frames()))
	}
}
