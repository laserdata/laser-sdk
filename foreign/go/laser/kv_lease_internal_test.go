package laser

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenAConnectionBackedHandleWhenLeasedThenShouldAcquireOnItsDedicatedConnectionAndCloseItWithTheHandle(t *testing.T) {
	shared, dedicated := &coordinationNode{}, &coordinationNode{
		answers: []coordinationAnswer{{reply: grantReply(t, wire.KvLeased, 3, 5*time.Second)}},
	}
	handle := connectionBackedHandle(shared)
	first, err := handle.coordinationClient()
	if err != nil {
		t.Fatalf("open the coordination client: %v", err)
	}
	transport, dedicatedTransport := first.transport.(*DedicatedKvTransport)
	if !dedicatedTransport || transport.slot != nil {
		t.Fatalf("want an undialed dedicated transport, got %T", first.transport)
	}
	transport.dial = func(context.Context) (*Laser, error) {
		return FromClient(dedicated, Options{Capabilities: versionedLeaseCapabilities()}), nil
	}

	lease, err := handle.Kv("ns").Lease(context.Background(), []byte("job-1"), "worker-a", 5*time.Second)
	again, _ := handle.coordinationClient()
	closed := handle.Close()

	if err != nil || lease.Token != 3 {
		t.Fatalf("want token 3, got %+v and %v", lease, err)
	}
	if again != first {
		t.Error("one connection keeps one coordination client")
	}
	if len(shared.codes) != 0 || len(dedicated.codes) != 1 || dedicated.codes[0] != wire.AgdxKvLeaseCode {
		t.Errorf("the acquisition rides the dedicated connection, got shared %v and dedicated %v",
			shared.codes, dedicated.codes)
	}
	if closed != nil || dedicated.closed != 1 || shared.closed != 1 {
		t.Errorf("closing the handle closes both connections, got %v, dedicated %d, shared %d",
			closed, dedicated.closed, shared.closed)
	}
}

func TestGivenAnAmbiguousRenewalWhenAGuardRenewsUntilItsDeadlineThenShouldRepeatTheIdentityAndKeepTheToken(t *testing.T) {
	transport := &repeatingTransport{replies: [][]byte{
		grantReply(t, wire.KvLeased, 7, 5*time.Second),
		unknownCoordinationReply(t, wire.ResultPending),
		grantReply(t, wire.KvRenewed, 7, 4*time.Second),
	}}
	handle := SetCoordinationTransportForTest(
		FromClient(&coordinationNode{}, Options{Capabilities: versionedLeaseCapabilities()}), transport)
	guard, err := handle.Kv("ns").LeaseGuard(context.Background(), []byte("job-1"), "worker-a", 5*time.Second)
	if err != nil {
		t.Fatalf("lease guard: %v", err)
	}

	renewed, err := guard.renewUntil(context.Background(), 5*time.Second, time.Now().Add(5*time.Second))

	if err != nil {
		t.Fatalf("the repeat answers the renewal: %v", err)
	}
	if renewed.Token != 7 || renewed.GrantedTTL != 4*time.Second || guard.Lease().GrantedTTL != 4*time.Second {
		t.Errorf("want the guard to hold the renewed lease, got %+v and %+v", renewed, guard.Lease())
	}
	sent := transport.frames()
	if len(sent) != 3 || !bytes.Equal(sent[1], sent[2]) {
		t.Fatalf("want one repeat under the renewal's identity, got %d frames", len(sent))
	}
	var envelope wire.ManagedRequestEnvelope
	var renewal wire.KvLeaseRenew
	if wire.Unmarshal(sent[1], &envelope) != nil || wire.Unmarshal(envelope.Payload, &renewal) != nil {
		t.Fatal("the renewal is a managed envelope")
	}
	if renewal.LeaseToken != 7 || renewal.HolderID != "worker-a" || string(renewal.Key) != "job-1" {
		t.Errorf("want the guard's own lease renewed, got %+v", renewal)
	}
}

func TestGivenARenewalStillUnknownWhenTheLeaseRunsOutThenShouldMarkTheGuardNotHeld(t *testing.T) {
	transport := &repeatingTransport{
		replies: [][]byte{grantReply(t, wire.KvLeased, 7, 5*time.Second)},
		always:  unknownCoordinationReply(t, wire.ResultPending),
	}
	handle := SetCoordinationTransportForTest(
		FromClient(&coordinationNode{}, Options{Capabilities: versionedLeaseCapabilities()}), transport)
	guard, err := handle.Kv("ns").LeaseGuard(context.Background(), []byte("job-1"), "worker-a", 5*time.Second)
	if err != nil {
		t.Fatalf("lease guard: %v", err)
	}

	_, err = guard.renewUntil(context.Background(), 5*time.Second, time.Now().Add(300*time.Millisecond))

	var refused *KvError
	if !errors.As(err, &refused) || refused.Kind() != wire.KvLeaseLost {
		t.Fatalf("want lease lost at the deadline, got %T: %v", err, err)
	}
	if guard.Held() {
		t.Error("a guard whose lease ran out is no longer held")
	}
}

func connectionBackedHandle(client *coordinationNode) *Laser {
	configured := *versionedLeaseCapabilities()
	return &Laser{
		shared:         &connection{client: client, configured: &configured},
		connectionInfo: func() *ConnectionString { return &ConnectionString{Host: "127.0.0.1", Port: 8090} },
		capabilities:   configured,
	}
}

func versionedLeaseCapabilities() *Capabilities {
	capabilities := fencedLeaseCapabilities()
	versions := wire.NewOpVersions(
		wire.QueryOpVersion, wire.ControlOpVersion, wire.KvOpVersion, wire.ForkOpVersion)
	capabilities.Versions = &versions
	return capabilities
}
