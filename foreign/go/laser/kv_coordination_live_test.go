//go:build integration

package laser_test

import (
	"errors"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenALiveStoreWhenCoordinatingOverADedicatedConnectionThenShouldFenceEveryWriteByTheLease(t *testing.T) {
	ctx := liveContext(t)
	client := dialLive(t)
	if !client.Capabilities().Kv.FencedLeases {
		t.Skip("the deployment does not serve the fenced-lease contract")
	}
	scratch := liveScratch(t, client, ctx, "laser-live-coordination").Laser()
	leases := scratch.Kv("leases")
	state := scratch.Kv("state").ResourceNamespace()
	key := []byte("source-owner")
	coordination := laser.NewDedicatedFencedLeaseClient(liveConnectionString())
	t.Cleanup(func() {
		if err := coordination.Close(); err != nil {
			t.Errorf("close the coordination client: %v", err)
		}
	})

	acquire, err := coordination.PrepareAcquire(wire.KvLease{
		V: wire.KvLeaseOpVersion, Namespace: leases.ResourceNamespace(), Key: key,
		LeaseTTLMicros: uint64((30 * time.Second).Microseconds()), HolderID: "node-1",
	})
	if err != nil {
		t.Fatalf("prepare the acquisition: %v", err)
	}
	granted, err := coordination.Acquire(ctx, acquire)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if granted.Token == 0 || !granted.ValidUntil.After(time.Now()) {
		t.Fatalf("want a live grant, got %+v", granted)
	}
	repeated, err := coordination.Acquire(ctx, acquire)
	if err != nil || repeated.Token != granted.Token {
		t.Fatalf("a repeated identity answers its recorded grant, got %+v and %v", repeated, err)
	}
	if other, err := leases.TryLease(ctx, key, "node-2", 30*time.Second); err != nil || other != nil {
		t.Fatalf("a held lease refuses another holder, got %+v and %v", other, err)
	}

	renew, err := coordination.PrepareRenew(wire.KvLeaseRenew{
		V: wire.KvLeaseOpVersion, Namespace: leases.ResourceNamespace(), Key: key,
		HolderID: "node-1", LeaseToken: granted.Token,
		LeaseTTLMicros: uint64((30 * time.Second).Microseconds()),
	})
	if err != nil {
		t.Fatalf("prepare the renewal: %v", err)
	}
	renewed, err := coordination.Renew(ctx, renew)
	if err != nil || renewed.Token != granted.Token {
		t.Fatalf("a renewal keeps its token, got %+v and %v", renewed, err)
	}

	fenced := wire.KvCasFenced{
		V: wire.KvLeaseOpVersion, Namespace: state, Key: []byte("cursor"), Value: []byte("42"),
		Expect: wire.AbsentKey(), FenceNamespace: leases.ResourceNamespace(), FenceKey: key,
		FenceToken: granted.Token,
	}
	write, err := coordination.PrepareCasFenced(fenced)
	if err != nil {
		t.Fatalf("prepare the fenced write: %v", err)
	}
	version, err := coordination.CasFenced(ctx, write)
	if err != nil || version == 0 {
		t.Fatalf("want a committed version, got %d and %v", version, err)
	}
	entry, err := coordination.Get(ctx, wire.KvGet{
		V: wire.KvOpVersion, Namespace: state, Key: []byte("cursor"), MinPosition: &renewed.Position,
	})
	if err != nil {
		t.Fatalf("read behind the renewal: %v", err)
	}
	if entry == nil || string(entry.Value) != "42" || entry.Version != version {
		t.Fatalf("want the fenced value at version %d, got %+v", version, entry)
	}

	release, err := coordination.PrepareRelease(wire.KvRelease{
		V: wire.KvLeaseOpVersion, Namespace: leases.ResourceNamespace(), Key: key,
		LeaseToken: granted.Token, HolderID: "node-1",
	})
	if err != nil {
		t.Fatalf("prepare the release: %v", err)
	}
	if released, err := coordination.Release(ctx, release); err != nil || !released {
		t.Fatalf("want the held lease released, got %v and %v", released, err)
	}

	fenced.Expect = wire.MatchVersion(version)
	fenced.Value = []byte("43")
	stale, err := coordination.PrepareCasFenced(fenced)
	if err != nil {
		t.Fatalf("prepare the stale write: %v", err)
	}
	_, err = coordination.CasFenced(ctx, stale)
	var refused *laser.KvError
	if !errors.As(err, &refused) || refused.Kind() != wire.KvLeaseLost {
		t.Fatalf("a released lease fences its old token out, got %T: %v", err, err)
	}
}

func TestGivenALiveStoreWhenLeasingThroughTheHandleThenShouldHoldRenewFenceAndRelease(t *testing.T) {
	ctx := liveContext(t)
	client := dialLive(t)
	if !client.Capabilities().Kv.FencedLeases {
		t.Skip("the deployment does not serve the fenced-lease contract")
	}
	scratch := liveScratch(t, client, ctx, "laser-live-leases").Laser()
	leases, key := scratch.Kv("leases"), []byte("job")

	guard, err := leases.LeaseGuard(ctx, key, "worker-a", 30*time.Second)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	granted := guard.Lease()
	if other, err := leases.TryLease(ctx, key, "worker-b", 30*time.Second); err != nil || other != nil {
		t.Fatalf("a held lease refuses another holder, got %+v and %v", other, err)
	}
	renewed, err := guard.Renew(ctx, 30*time.Second)
	if err != nil || renewed.Token != granted.Token {
		t.Fatalf("a renewal keeps its token, got %+v and %v", renewed, err)
	}
	version, err := scratch.Kv("state").CasFenced([]byte("result"), "leases", key, granted.Token).
		Bytes([]byte("first")).ExpectAbsent().Commit(ctx)
	if err != nil || version == 0 {
		t.Fatalf("want a committed version, got %d and %v", version, err)
	}
	seen := scratch.SeenPositions().Kv
	if seen == nil {
		t.Fatal("the grant position joins the read frontier")
	}
	if behind, comparable := seen.Precedes(granted.Position); behind || !comparable {
		t.Errorf("the read frontier is at least the grant, got %+v for %+v", seen, granted.Position)
	}
	if released, err := guard.Release(ctx); err != nil || !released {
		t.Fatalf("want the held lease released, got %v and %v", released, err)
	}
	next, err := leases.Lease(ctx, key, "worker-b", 30*time.Second)
	if err != nil || next.Token <= granted.Token {
		t.Fatalf("the next holder takes a later token, got %+v and %v", next, err)
	}
}

func TestGivenAnOpenServerWhenCoordinatingThenShouldRefuseAsUnsupportedWithoutSending(t *testing.T) {
	ctx := liveContext(t)
	client := dialLive(t)
	if client.Capabilities().Kv.FencedLeases {
		t.Skip("the deployment serves the fenced-lease contract")
	}
	coordination := laser.NewDedicatedFencedLeaseClient(liveConnectionString())
	t.Cleanup(func() {
		if err := coordination.Close(); err != nil {
			t.Errorf("close the coordination client: %v", err)
		}
	})
	acquire, err := coordination.PrepareAcquire(wire.KvLease{
		V: wire.KvLeaseOpVersion, Namespace: "leases", Key: []byte("source-owner"),
		LeaseTTLMicros: uint64((30 * time.Second).Microseconds()), HolderID: "node-1",
	})
	if err != nil {
		t.Fatalf("prepare the acquisition: %v", err)
	}

	for range 2 {
		_, err = coordination.Acquire(ctx, acquire)
		var unsupported *laser.UnsupportedError
		if !errors.As(err, &unsupported) {
			t.Fatalf("want an UnsupportedError, got %T: %v", err, err)
		}
	}
}
