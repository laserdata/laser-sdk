package laser

import (
	"context"
	"sync"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenARoutedPartitionWhenReadOrPreviewedOutsideAGroupThenShouldAskItsPrimary(t *testing.T) {
	server := &routeRecorder{policyServer: newPolicyServer(0)}
	server.routes = map[uint32]uint8{0: 2}
	server.heads[0] = 1
	filters := standaloneFilters(server)

	// The policy server answers as a group, so only where each request went is read.
	_, _ = filters.Read(0, wire.SavedRevision(1, 1)).Fetch(context.Background())
	_, _ = filters.Preview(0, wire.SavedRevision(1, 1)).Send(context.Background())

	want := []routedSend{
		{code: wire.AgdxFilteredPollCode, replica: 2, named: true},
		{code: wire.AgdxFilterPreviewCode, replica: 2, named: true},
	}
	if got := server.sent(); len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("want both sent to the partition's primary %+v, got %+v", want, got)
	}
}

func TestGivenNoRouteWhenReadOutsideAGroupThenShouldAskTheCallersNode(t *testing.T) {
	server := &routeRecorder{policyServer: newPolicyServer(0)}
	server.heads[0] = 1
	filters := standaloneFilters(server)

	_, _ = filters.Read(0, wire.SavedRevision(1, 1)).Fetch(context.Background())

	if got := server.sent(); len(got) != 1 || got[0].named {
		t.Fatalf("want one request to the caller's node, got %+v", got)
	}
}

func TestGivenAPrimaryThatMovedWhenReadOutsideAGroupThenShouldResendToTheCallersNode(t *testing.T) {
	server := &routeRecorder{policyServer: newPolicyServer(0), staleReplica: 2}
	server.routes = map[uint32]uint8{0: 2}
	server.heads[0] = 1
	filters := standaloneFilters(server)

	_, _ = filters.Read(0, wire.SavedRevision(1, 1)).Fetch(context.Background())

	got := server.sent()
	if len(got) != 2 || !got[0].named || got[1].named {
		t.Fatalf("want the primary then the caller's node, got %+v", got)
	}
}

type routedSend struct {
	code    uint32
	replica uint8
	named   bool
}

// routeRecorder records which node each filter request named.
type routeRecorder struct {
	*policyServer
	// staleReplica refuses requests naming it as no longer the primary.
	staleReplica uint8
	// movesTo is the replica the route names once the first read has been sent, zero to leave it.
	movesTo uint8
	// silentReplica never answers a read, as a stopped primary does.
	silentReplica uint8
	recordMutex   sync.Mutex
	sends         []routedSend
}

func (server *routeRecorder) sent() []routedSend {
	server.recordMutex.Lock()
	defer server.recordMutex.Unlock()
	return append([]routedSend(nil), server.sends...)
}

func (server *routeRecorder) record(send routedSend) {
	server.recordMutex.Lock()
	defer server.recordMutex.Unlock()
	server.sends = append(server.sends, send)
}

func (server *routeRecorder) SendBinaryRequest(ctx context.Context, code uint32, payload []byte) ([]byte, error) {
	if code != getPartitionRouteCode {
		server.record(routedSend{code: code})
	}
	return server.policyServer.SendBinaryRequest(ctx, code, payload)
}

func (server *routeRecorder) SendBinaryRequestToReplica(ctx context.Context, code uint32, payload []byte, replica uint8) ([]byte, error) {
	server.record(routedSend{code: code, replica: replica, named: true})
	if server.staleReplica != 0 && replica == server.staleReplica {
		return nil, ierror.NotPrimary{}
	}
	if code == wire.AgdxFilteredPollCode && server.movesTo != 0 {
		server.mutex.Lock()
		server.routes[0] = server.movesTo
		server.mutex.Unlock()
	}
	if code == wire.AgdxFilteredPollCode && server.silentReplica != 0 && replica == server.silentReplica {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return server.policyServer.SendBinaryRequestToReplica(ctx, code, payload, replica)
}

func (server *routeRecorder) GetClusterMetadata(context.Context) (*iggcon.ClusterMetadata, error) {
	return &iggcon.ClusterMetadata{Nodes: []iggcon.ClusterNode{{ReplicaID: 0}}}, nil
}

func standaloneFilters(server *routeRecorder) *Filters {
	capabilities := OpenCapabilities()
	capabilities.Filters.Native = true
	handle := FromClient(server, Options{DefaultStream: "agents", Capabilities: &capabilities})
	return handle.Topic("fleet").Filters(wire.IndependentConsumer("auditor"))
}

func TestGivenABackupThatAnswersUnavailableWhenAGroupReaderReadsThenShouldLookTheRouteUpAgainAndResend(t *testing.T) {
	server := &routeRecorder{policyServer: newPolicyServer(0), movesTo: 2}
	server.routes = map[uint32]uint8{0: 1}
	server.heads[0] = 1
	server.pollRefusal[0] = []wire.FilterErrorReason{wire.ReasonUnavailable}
	reader := routedReader(t, server)

	page, err := reader.Read(0, wire.GroupPolicy()).Fetch(context.Background())

	if err != nil || len(page.Records) != 1 {
		t.Fatalf("want the resend served by the new primary, got %v", err)
	}
	if got := server.pollReplicas(); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("want the read on replica 1 then 2, got %v", got)
	}
}

func TestGivenAPrimaryThatStopsAnsweringWhenTheRouteMovesThenShouldFollowThePartition(t *testing.T) {
	previous := routeRecheck
	routeRecheck = 10 * time.Millisecond
	t.Cleanup(func() { routeRecheck = previous })
	server := &routeRecorder{policyServer: newPolicyServer(0), movesTo: 2, silentReplica: 1}
	server.routes = map[uint32]uint8{0: 1}
	server.heads[0] = 1
	reader := routedReader(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	page, err := reader.Read(0, wire.GroupPolicy()).Fetch(ctx)

	if err != nil || len(page.Records) != 1 {
		t.Fatalf("want the read to follow the partition to its new primary, got %v", err)
	}
	if got := server.pollReplicas(); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("want the read on replica 1 then 2, got %v", got)
	}
}

func (server *routeRecorder) pollReplicas() []uint8 {
	var replicas []uint8
	for _, send := range server.sent() {
		if send.code == wire.AgdxFilteredPollCode && send.named {
			replicas = append(replicas, send.replica)
		}
	}
	return replicas
}

func routedReader(t *testing.T, server *routeRecorder) *GroupReader {
	t.Helper()
	capabilities := OpenCapabilities()
	capabilities.Managed = true
	capabilities.Hello = HelloAnswered
	capabilities.Filters = FilterCaps{Native: true, GroupPolicyReads: true, MaxWait: server.maxWait}
	handle := FromClient(server, Options{DefaultStream: "agents", Capabilities: &capabilities})
	reader, err := handle.Topic("work").Group("workers").OpenReader(context.Background())
	if err != nil {
		t.Fatalf("open the group reader: %v", err)
	}
	t.Cleanup(func() { _ = reader.Close(context.Background()) })
	return reader
}
