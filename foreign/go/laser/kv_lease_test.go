package laser_test

import (
	"context"
	"errors"
	"testing"
	"time"

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

func TestGivenAGrantedLeaseWhenAcquiredThenShouldReturnTheTokenAndTTL(t *testing.T) {
	server := &kvServer{outcomes: []wire.KvOutcome{{
		Kind: wire.KvLeased,
		Lease: &wire.KvLeaseGrant{
			LeaseToken: 7, GrantedTTLMicros: uint64((2 * time.Second).Microseconds()),
		},
	}}}
	kv := kvHandle(server, laser.Options{Capabilities: fencedCapabilities()}).Kv("ns")

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
	kv := kvHandle(server, laser.Options{Capabilities: fencedCapabilities()}).Kv("ns")

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
	kv := kvHandle(server, laser.Options{Capabilities: fencedCapabilities()}).Kv("ns")

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
	kv := kvHandle(server, laser.Options{Capabilities: fencedCapabilities()}).Kv("ns")

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
	kv := kvHandle(server, laser.Options{Capabilities: fencedCapabilities()}).Kv("ns")

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
	kv := kvHandle(server, laser.Options{Capabilities: fencedCapabilities()}).Kv("ns")
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
