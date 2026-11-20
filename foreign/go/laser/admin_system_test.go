package laser_test

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"sync"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

// systemServer answers the server-wide reads and records the raw requests.
type systemServer struct {
	clusterOfOne
	mutex sync.Mutex

	stats   *iggcon.Stats
	me      *iggcon.ClientInfoDetails
	clients []iggcon.ClientInfo
	// client is what a single client read answers with, nil for a client id
	// no connection holds.
	client *iggcon.ClientInfoDetails
	// blankClient makes the client read answer with neither details nor an
	// error, which a client is free to do.
	blankClient bool
	// cluster is the roster, nil to answer with no cluster at all.
	cluster *iggcon.ClusterMetadata
	// archive is what the snapshot request answers with.
	archive []byte
	// options is the catalog a describe answers with, and scope records the
	// one that was asked for.
	options []iggcon.OptionSpec
	scope   iggcon.OptionsScope

	// requests records the raw command codes and bodies that were sent.
	requests []rawRequest
	pinged   bool
	fail     error
}

type rawRequest struct {
	code uint32
	body []byte
}

func (server *systemServer) Close() error { return nil }

func (server *systemServer) Ping(_ context.Context) error {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.fail != nil {
		return server.fail
	}
	server.pinged = true
	return nil
}

func (server *systemServer) GetStats(_ context.Context) (*iggcon.Stats, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.fail != nil {
		return nil, server.fail
	}
	return server.stats, nil
}

func (server *systemServer) GetMe(_ context.Context) (*iggcon.ClientInfoDetails, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.fail != nil {
		return nil, server.fail
	}
	return server.me, nil
}

func (server *systemServer) GetClients(_ context.Context) ([]iggcon.ClientInfo, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.fail != nil {
		return nil, server.fail
	}
	return server.clients, nil
}

func (server *systemServer) GetClient(
	_ context.Context, _ uint32,
) (*iggcon.ClientInfoDetails, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.fail != nil {
		return nil, server.fail
	}
	if server.blankClient {
		return nil, nil
	}
	if server.client == nil {
		return nil, ierror.ClientNotFound{}
	}
	return server.client, nil
}

func (server *systemServer) GetClusterMetadata(
	_ context.Context,
) (*iggcon.ClusterMetadata, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.fail != nil {
		return nil, server.fail
	}
	return server.cluster, nil
}

func (server *systemServer) SendBinaryRequest(
	_ context.Context, code uint32, payload []byte,
) ([]byte, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.fail != nil {
		return nil, server.fail
	}
	server.requests = append(server.requests,
		rawRequest{code: code, body: append([]byte(nil), payload...)})
	return server.archive, nil
}

func (server *systemServer) DescribeOptions(
	_ context.Context, scope iggcon.OptionsScope,
) ([]iggcon.OptionSpec, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.fail != nil {
		return nil, server.fail
	}
	server.scope = scope
	return server.options, nil
}

func (server *systemServer) describedScope() iggcon.OptionsScope {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	return server.scope
}

func (server *systemServer) sent() []rawRequest {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	return append([]rawRequest(nil), server.requests...)
}

func systemHandle(server *systemServer) *laser.Laser {
	return laser.FromClient(server, laser.Options{DefaultStream: "agents"})
}

func TestGivenAServerWhenPingedThenShouldReachIt(t *testing.T) {
	server := &systemServer{}

	if err := systemHandle(server).Ping(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if !server.pinged {
		t.Fatal("the ping never left")
	}
}

func TestGivenAServerWhenItsStatisticsAreReadThenShouldReportThem(t *testing.T) {
	server := &systemServer{stats: &iggcon.Stats{
		ProcessId: 42, CpuUsage: 1.5, TotalCpuUsage: 12.5,
		MemoryUsage: 2048, TotalMemory: 8192, AvailableMemory: 4096,
		RunTime: 90, StartTime: 1_700_000_000_000_000,
		ReadBytes: 10, WrittenBytes: 20, MessagesSizeBytes: 30,
		StreamsCount: 1, TopicsCount: 2, PartitionsCount: 3, SegmentsCount: 4,
		MessagesCount: 5, ClientsCount: 6, ConsumerGroupsCount: 7,
		Hostname: "node-a", OsName: "linux", OsVersion: "6.1",
		KernelVersion: "6.1.0", IggyServerVersion: "0.12.0",
		ThreadsCount: 8, FreeDiskSpace: 100, TotalDiskSpace: 200,
		OpenFilesCount: 9, OpenFilesLimit: 1024,
		CacheMetrics: []iggcon.CacheMetrics{
			{StreamId: 2, TopicId: 1, PartitionId: 1, Hits: 3, Misses: 1, HitRatio: 0.75},
			{StreamId: 1, TopicId: 3, PartitionId: 1},
			{StreamId: 1, TopicId: 1, PartitionId: 2},
			{StreamId: 1, TopicId: 1, PartitionId: 1},
		},
	}}

	stats, err := systemHandle(server).Stats(context.Background())
	if err != nil || stats == nil {
		t.Fatalf("got %+v %v", stats, err)
	}
	if stats.ProcessID != 42 || stats.CPUUsage != 1.5 || stats.TotalCPUUsage != 12.5 ||
		stats.MemoryUsageBytes != 2048 || stats.TotalMemoryBytes != 8192 ||
		stats.AvailableMemoryBytes != 4096 || stats.RunTimeMicros != 90 ||
		stats.StartTimeMicros != 1_700_000_000_000_000 {
		t.Fatalf("got %+v", stats)
	}
	if stats.ReadBytes != 10 || stats.WrittenBytes != 20 ||
		stats.MessagesSizeBytes != 30 || stats.StreamsCount != 1 ||
		stats.TopicsCount != 2 || stats.PartitionsCount != 3 ||
		stats.SegmentsCount != 4 || stats.MessagesCount != 5 ||
		stats.ClientsCount != 6 || stats.ConsumerGroupsCount != 7 {
		t.Fatalf("got %+v", stats)
	}
	if stats.Hostname != "node-a" || stats.OsName != "linux" ||
		stats.OsVersion != "6.1" || stats.KernelVersion != "6.1.0" ||
		stats.ServerVersion != "0.12.0" || stats.ThreadsCount != 8 ||
		stats.FreeDiskSpaceBytes != 100 || stats.TotalDiskSpaceBytes != 200 ||
		stats.OpenFilesCount != 9 || stats.OpenFilesLimit != 1024 {
		t.Fatalf("got %+v", stats)
	}
	// The server reports the per-partition metrics in whatever order it
	// holds them, so they are ordered here and two reads of an unchanged
	// server compare equal.
	want := [][3]uint32{{1, 1, 1}, {1, 1, 2}, {1, 3, 1}, {2, 1, 1}}
	if len(stats.CacheMetrics) != len(want) {
		t.Fatalf("got %+v", stats.CacheMetrics)
	}
	for index, metrics := range stats.CacheMetrics {
		got := [3]uint32{metrics.StreamID, metrics.TopicID, metrics.PartitionID}
		if got != want[index] {
			t.Fatalf("got %v at %d, want %v", got, index, want[index])
		}
	}
	if stats.CacheMetrics[3].Hits != 3 || stats.CacheMetrics[3].Misses != 1 ||
		stats.CacheMetrics[3].HitRatio != 0.75 {
		t.Fatalf("got %+v", stats.CacheMetrics[3])
	}
}

func TestGivenStatisticsThatReportNothingThenShouldSaySo(t *testing.T) {
	// Nothing came back to report, so the call must not hand out an empty
	// set of statistics as though the server had no load at all.
	server := &systemServer{}
	if _, err := systemHandle(server).Stats(context.Background()); err == nil {
		t.Fatal("statistics that were never reported must not read as zeroes")
	}
}

func TestGivenAConnectionWhenItReadsItselfThenShouldReportItsGroups(t *testing.T) {
	server := &systemServer{me: &iggcon.ClientInfoDetails{
		ClientInfo: iggcon.ClientInfo{
			ID: 7, UserID: 3, Address: "127.0.0.1:1234", Transport: "TCP",
			ConsumerGroupsCount: 2,
		},
		ConsumerGroups: []iggcon.ConsumerGroupInfo{
			{StreamId: 1, TopicId: 2, GroupId: 9},
			{StreamId: 1, TopicId: 2, GroupId: 4},
		},
	}}

	me, err := systemHandle(server).Me(context.Background())
	if err != nil || me == nil {
		t.Fatalf("got %+v %v", me, err)
	}
	if me.ClientID != 7 || me.UserID == nil || *me.UserID != 3 ||
		me.Address != "127.0.0.1:1234" || me.Transport != "TCP" ||
		me.ConsumerGroupsCount != 2 {
		t.Fatalf("got %+v", me)
	}
	// The memberships are ordered so two reads of an unchanged client
	// compare equal, which a caller diffing them depends on.
	want := []laser.ClientConsumerGroup{
		{StreamID: 1, TopicID: 2, GroupID: 4},
		{StreamID: 1, TopicID: 2, GroupID: 9},
	}
	if len(me.ConsumerGroups) != 2 || me.ConsumerGroups[0] != want[0] ||
		me.ConsumerGroups[1] != want[1] {
		t.Fatalf("got %+v, want %+v", me.ConsumerGroups, want)
	}
}

func TestGivenAConnectionTheServerDoesNotDescribeThenShouldSaySo(t *testing.T) {
	// This connection exists, so an answer that describes nothing is a
	// failure rather than the absence a client read reports.
	server := &systemServer{}
	if _, err := systemHandle(server).Me(context.Background()); err == nil {
		t.Fatal("a connection the server did not describe must not read as one")
	}
}

func TestGivenConnectedClientsWhenListedThenShouldReportWhoIsSignedIn(t *testing.T) {
	server := &systemServer{clients: []iggcon.ClientInfo{
		{ID: 7, UserID: 3, Address: "127.0.0.1:1", Transport: "TCP",
			ConsumerGroupsCount: 2},
		// A client that has connected but not signed in has no user, which
		// the server sends as the id no user can hold.
		{ID: 8, UserID: math.MaxUint32, Address: "127.0.0.1:2", Transport: "QUIC"},
	}}

	listed, err := systemHandle(server).Clients(context.Background())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(listed) != 2 {
		t.Fatalf("got %+v", listed)
	}
	if listed[0].ClientID != 7 || listed[0].UserID == nil || *listed[0].UserID != 3 ||
		listed[0].Address != "127.0.0.1:1" || listed[0].Transport != "TCP" ||
		listed[0].ConsumerGroupsCount != 2 {
		t.Fatalf("got %+v", listed[0])
	}
	// Reporting that id would have a caller look up a user that cannot
	// exist, so it reads as no user at all.
	if listed[1].UserID != nil {
		t.Fatalf("a client that has not signed in holds no user, got %v",
			*listed[1].UserID)
	}
	// A listing carries counts only, so no memberships are invented for it.
	if len(listed[0].ConsumerGroups) != 0 {
		t.Fatalf("got %+v", listed[0].ConsumerGroups)
	}
}

func TestGivenAClientIdWhenReadThenShouldReportItOrNothing(t *testing.T) {
	server := &systemServer{client: &iggcon.ClientInfoDetails{
		ClientInfo: iggcon.ClientInfo{ID: 7, UserID: 3},
		ConsumerGroups: []iggcon.ConsumerGroupInfo{
			{StreamId: 1, TopicId: 2, GroupId: 4},
		},
	}}
	handle := systemHandle(server)

	info, err := handle.ClientInfo(context.Background(), 7)
	if err != nil || info == nil || info.ClientID != 7 ||
		len(info.ConsumerGroups) != 1 {
		t.Fatalf("got %+v %v", info, err)
	}

	// A client id belongs to one connection and stops existing when it
	// closes, so asking about one that is gone is a read with the answer
	// "nobody", not a failure.
	server.client = nil
	info, err = handle.ClientInfo(context.Background(), 7)
	if err != nil || info != nil {
		t.Fatalf("got %+v %v", info, err)
	}
}

func TestGivenAClientReadThatAnswersWithNothingThenShouldReadAsAbsent(t *testing.T) {
	// A client is free to answer a connection that is gone with neither
	// details nor an error. Reading past that would dereference nothing.
	server := &systemServer{blankClient: true}

	info, err := systemHandle(server).ClientInfo(context.Background(), 7)
	if err != nil || info != nil {
		t.Fatalf("got %+v %v", info, err)
	}
}

func TestGivenAClusterWhenReadThenShouldReportEveryNodeAndItsPorts(t *testing.T) {
	server := &systemServer{cluster: &iggcon.ClusterMetadata{
		Name: "band", Nodes: []iggcon.ClusterNode{
			{
				ReplicaID: 0, Name: "node-a", IP: "10.0.0.1",
				Endpoints: iggcon.NewTransportEndpoints(8090, 8080, 3000, 0),
				Role:      iggcon.RoleLeader, Status: iggcon.Healthy,
			},
			{
				ReplicaID: 1, Name: "node-b", IP: "10.0.0.2",
				Role: iggcon.RoleFollower, Status: iggcon.Maintenance,
			},
		},
	}}

	cluster, err := systemHandle(server).Cluster(context.Background())
	if err != nil || cluster == nil || cluster.Name != "band" {
		t.Fatalf("got %+v %v", cluster, err)
	}
	if len(cluster.Nodes) != 2 {
		t.Fatalf("got %+v", cluster.Nodes)
	}
	want := laser.ClusterNode{
		ReplicaID: 0, Name: "node-a", IP: "10.0.0.1",
		TCPPort: 8090, QUICPort: 8080, HTTPPort: 3000, WebSocketPort: 0,
		Role: laser.ClusterLeader, Status: laser.ClusterHealthy,
	}
	if cluster.Nodes[0] != want {
		t.Fatalf("got %+v, want %+v", cluster.Nodes[0], want)
	}
	if cluster.Nodes[1].Role != laser.ClusterFollower ||
		cluster.Nodes[1].Status != laser.ClusterMaintenance {
		t.Fatalf("got %+v", cluster.Nodes[1])
	}
}

func TestGivenNoClusterWhenReadThenShouldSaySo(t *testing.T) {
	// Every deployment is at least one node, so an answer that names no
	// cluster is a failure rather than a cluster with no nodes.
	server := &systemServer{}
	if _, err := systemHandle(server).Cluster(context.Background()); err == nil {
		t.Fatal("a cluster that was never named must not read as an empty one")
	}
}

func TestGivenClusterWordsWhenPrintedThenShouldMatchTheOtherClients(t *testing.T) {
	for word, role := range map[string]laser.ClusterNodeRole{
		"leader": laser.ClusterLeader, "follower": laser.ClusterFollower,
	} {
		if role.String() != word {
			t.Fatalf("got %q, want %q", role.String(), word)
		}
	}
	for word, status := range map[string]laser.ClusterNodeStatus{
		"healthy": laser.ClusterHealthy, "starting": laser.ClusterStarting,
		"stopping": laser.ClusterStopping, "unreachable": laser.ClusterUnreachable,
		"maintenance": laser.ClusterMaintenance, "unknown": laser.ClusterUnknown,
	} {
		if status.String() != word {
			t.Fatalf("got %q, want %q", status.String(), word)
		}
	}
}

func TestGivenASnapshotWhenTakenThenShouldAskForEverySectionDeflated(t *testing.T) {
	server := &systemServer{archive: []byte{'P', 'K', 3, 4}}

	archive, err := systemHandle(server).Snapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(archive) != 4 || archive[0] != 'P' || archive[1] != 'K' {
		t.Fatalf("got %v", archive)
	}
	sent := server.sent()
	if len(sent) != 1 || sent[0].code != 11 {
		t.Fatalf("got %+v", sent)
	}
	// Deflate with every section, which is what the Rust client asks for, so
	// an archive taken through any client unpacks the same way.
	want := []byte{2, 1, 100}
	if len(sent[0].body) != len(want) {
		t.Fatalf("got %v, want %v", sent[0].body, want)
	}
	for index, piece := range want {
		if sent[0].body[index] != piece {
			t.Fatalf("got %v, want %v", sent[0].body, want)
		}
	}
}

func TestGivenASnapshotWhenTakenThenShouldNotAliasTheReplyBuffer(t *testing.T) {
	// The reply buffer belongs to the connection, which reuses it, so an
	// archive that aliased it would change under the caller.
	reply := []byte{'P', 'K', 3, 4}
	server := &systemServer{archive: reply}

	archive, err := systemHandle(server).Snapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	reply[0] = 0
	if archive[0] != 'P' {
		t.Fatal("the archive must not alias the reply buffer")
	}
}

func TestGivenAServerThatRefusesWhenItIsAdministeredThenShouldReportIt(t *testing.T) {
	server := &systemServer{fail: ierror.Disconnected{}}
	handle := systemHandle(server)

	for name, call := range map[string]func() error{
		"ping": func() error { return handle.Ping(context.Background()) },
		"stats": func() error {
			_, err := handle.Stats(context.Background())
			return err
		},
		"me": func() error {
			_, err := handle.Me(context.Background())
			return err
		},
		"clients": func() error {
			_, err := handle.Clients(context.Background())
			return err
		},
		"client": func() error {
			_, err := handle.ClientInfo(context.Background(), 7)
			return err
		},
		"cluster": func() error {
			_, err := handle.Cluster(context.Background())
			return err
		},
		"snapshot": func() error {
			_, err := handle.Snapshot(context.Background())
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := call()
			var transport *laser.TransportError
			if !errors.As(err, &transport) {
				t.Fatalf("want a TransportError, got %T: %v", err, err)
			}
		})
	}
}

func TestGivenAClosedHandleWhenTheServerIsAdministeredThenShouldSaySo(t *testing.T) {
	handle := systemHandle(&systemServer{})
	if err := handle.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if err := handle.Ping(context.Background()); err == nil {
		t.Fatal("a closed handle has no connection to ping over")
	}
	if _, err := handle.Stats(context.Background()); err == nil {
		t.Fatal("a closed handle has no connection to read statistics over")
	}
	if _, err := handle.Me(context.Background()); err == nil {
		t.Fatal("a closed handle has no connection to describe")
	}
	if _, err := handle.Clients(context.Background()); err == nil {
		t.Fatal("a closed handle has no connection to list clients over")
	}
	if _, err := handle.ClientInfo(context.Background(), 7); err == nil {
		t.Fatal("a closed handle has no connection to read a client over")
	}
	if _, err := handle.Cluster(context.Background()); err == nil {
		t.Fatal("a closed handle has no connection to read a cluster over")
	}
	if _, err := handle.Snapshot(context.Background()); err == nil {
		t.Fatal("a closed handle has no connection to snapshot over")
	}
}

func TestGivenAScopeWhenItsOptionsAreDescribedThenShouldReportTheCatalog(t *testing.T) {
	server := &systemServer{options: []iggcon.OptionSpec{{
		Key: "segment_size",
		DefaultValue: iggcon.HeaderValue{
			Kind: iggcon.Uint64, Value: leBytes(1_073_741_824, 8)},
		Updatable: true, Resettable: true, Secret: false, ReadOnly: false,
		Critical: true, Description: "segment size",
	}}}

	listed, err := systemHandle(server).DescribeOptions(
		context.Background(), laser.OptionsScopeTopic)
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	want := laser.OptionInfo{
		Key: "segment_size", Kind: "uint64", Updatable: true, Resettable: true,
		Critical: true, DefaultValue: "1073741824", Description: "segment size",
	}
	if len(listed) != 1 || listed[0] != want {
		t.Fatalf("got %+v, want %+v", listed, want)
	}
	if scope := server.describedScope(); scope != iggcon.OptionsScopeTopic {
		t.Fatalf("got scope %v", scope)
	}
}

func TestGivenAnUnknownScopeWhenOptionsAreDescribedThenShouldRefuseIt(t *testing.T) {
	// A scope the server does not know names no catalog, so it is refused
	// here rather than sent for the server to reject.
	server := &systemServer{}
	if _, err := systemHandle(server).DescribeOptions(
		context.Background(), laser.OptionsScope(9)); err == nil {
		t.Fatal("an unknown scope must be refused")
	}
	if server.describedScope() != 0 {
		t.Fatal("nothing must be sent")
	}
}

func TestGivenScopeWordsWhenParsedThenShouldRoundTripThroughTheirText(t *testing.T) {
	for _, scope := range []laser.OptionsScope{
		laser.OptionsScopeTopic, laser.OptionsScopeStream, laser.OptionsScopeUser,
	} {
		parsed, err := laser.ParseOptionsScope(scope.String())
		if err != nil || parsed != scope {
			t.Fatalf("got %v %v for %v", parsed, err, scope)
		}
	}
	if _, err := laser.ParseOptionsScope("partition"); err == nil {
		t.Fatal("an unknown scope word must be refused")
	}
}

func TestGivenAnOptionDefaultWhenRenderedThenShouldMatchItsTextForm(t *testing.T) {
	// The text is what a raw option patch carries back, so it has to be the
	// one form the server reads for each kind.
	for _, probe := range []struct {
		name  string
		value iggcon.HeaderValue
		kind  string
		text  string
	}{
		{"a string", iggcon.HeaderValue{
			Kind: iggcon.String, Value: []byte("lz4")}, "string", "lz4"},
		{"raw bytes", iggcon.HeaderValue{
			Kind: iggcon.Raw, Value: []byte{1, 2, 3}}, "raw", "AQID"},
		{"a true", iggcon.HeaderValue{
			Kind: iggcon.Bool, Value: []byte{1}}, "bool", "true"},
		{"a false", iggcon.HeaderValue{
			Kind: iggcon.Bool, Value: []byte{0}}, "bool", "false"},
		{"a negative int8", iggcon.HeaderValue{
			Kind: iggcon.Int8, Value: []byte{0xff}}, "int8", "-1"},
		{"a negative int16", iggcon.HeaderValue{
			Kind: iggcon.Int16, Value: []byte{0xff, 0xff}}, "int16", "-1"},
		{"a negative int32", iggcon.HeaderValue{
			Kind:  iggcon.Int32,
			Value: []byte{0xff, 0xff, 0xff, 0xff}}, "int32", "-1"},
		{"a negative int64", iggcon.HeaderValue{
			Kind:  iggcon.Int64,
			Value: leBytes(0xffff_ffff_ffff_ffff, 8)}, "int64", "-1"},
		{"a uint8", iggcon.HeaderValue{
			Kind: iggcon.Uint8, Value: []byte{200}}, "uint8", "200"},
		{"a uint16", iggcon.HeaderValue{
			Kind: iggcon.Uint16, Value: leBytes(65_535, 2)}, "uint16", "65535"},
		{"a uint32", iggcon.HeaderValue{
			Kind:  iggcon.Uint32,
			Value: leBytes(4_294_967_295, 4)}, "uint32", "4294967295"},
		{"a uint64", iggcon.HeaderValue{
			Kind:  iggcon.Uint64,
			Value: leBytes(0xffff_ffff_ffff_ffff, 8)}, "uint64",
			"18446744073709551615"},
		{"a uint128", iggcon.HeaderValue{
			Kind: iggcon.Uint128, Value: wideBytes(1, 0)}, "uint128",
			"18446744073709551616"},
		{"a negative int128", iggcon.HeaderValue{
			Kind: iggcon.Int128, Value: wideBytes(
				0xffff_ffff_ffff_ffff, 0xffff_ffff_ffff_ffff)}, "int128", "-1"},
		// A float is carried as the hex of its bits, because a decimal of a
		// float does not round trip through text.
		{"a float32", iggcon.HeaderValue{
			Kind:  iggcon.Float,
			Value: leBytes(uint64(math.Float32bits(1.5)), 4)}, "float32",
			"0x3fc00000"},
		{"a float64", iggcon.HeaderValue{
			Kind:  iggcon.Double,
			Value: leBytes(math.Float64bits(1.5), 8)}, "float64",
			"0x3ff8000000000000"},
		// The hex is padded to the width of the float, so one value has one
		// text form and a reader can tell the two widths apart.
		{"a float32 with leading zero bits", iggcon.HeaderValue{
			Kind: iggcon.Float, Value: leBytes(1, 4)}, "float32", "0x00000001"},
		{"a float64 with leading zero bits", iggcon.HeaderValue{
			Kind:  iggcon.Double,
			Value: leBytes(1, 8)}, "float64", "0x0000000000000001"},
	} {
		t.Run(probe.name, func(t *testing.T) {
			server := &systemServer{options: []iggcon.OptionSpec{
				{Key: "k", DefaultValue: probe.value}}}
			listed, err := systemHandle(server).DescribeOptions(
				context.Background(), laser.OptionsScopeStream)
			if err != nil || len(listed) != 1 {
				t.Fatalf("got %+v %v", listed, err)
			}
			if listed[0].Kind != probe.kind || listed[0].DefaultValue != probe.text {
				t.Fatalf("got %q %q, want %q %q", listed[0].Kind,
					listed[0].DefaultValue, probe.kind, probe.text)
			}
		})
	}
}

func TestGivenAnOptionValueTheKindCannotHoldThenShouldReportNoDefault(t *testing.T) {
	// The bytes would have to be read under a layout the server did not
	// write them in, so no default is reported rather than a wrong one.
	for _, probe := range []struct {
		name  string
		value iggcon.HeaderValue
	}{
		{"no value at all", iggcon.HeaderValue{Kind: iggcon.Bool}},
		{"a short integer", iggcon.HeaderValue{
			Kind: iggcon.Uint64, Value: []byte{1, 2}}},
		{"a long integer", iggcon.HeaderValue{
			Kind: iggcon.Uint32, Value: leBytes(1, 8)}},
		{"a bool that is neither", iggcon.HeaderValue{
			Kind: iggcon.Bool, Value: []byte{2}}},
		{"a string that is not text", iggcon.HeaderValue{
			Kind: iggcon.String, Value: []byte{0xff, 0xfe}}},
		{"a kind this build has no name for", iggcon.HeaderValue{
			Kind: iggcon.HeaderKind(99), Value: []byte{1}}},
		{"a value longer than the codec allows", iggcon.HeaderValue{
			Kind: iggcon.Raw, Value: make([]byte, 256)}},
	} {
		t.Run(probe.name, func(t *testing.T) {
			server := &systemServer{options: []iggcon.OptionSpec{
				{Key: "k", DefaultValue: probe.value}}}
			listed, err := systemHandle(server).DescribeOptions(
				context.Background(), laser.OptionsScopeUser)
			if err != nil || len(listed) != 1 {
				t.Fatalf("got %+v %v", listed, err)
			}
			if listed[0].DefaultValue != "" {
				t.Fatalf("got %q", listed[0].DefaultValue)
			}
		})
	}
}

func TestGivenAServerThatRefusesWhenOptionsAreDescribedThenShouldReportIt(t *testing.T) {
	server := &systemServer{fail: ierror.Disconnected{}}
	_, err := systemHandle(server).DescribeOptions(
		context.Background(), laser.OptionsScopeTopic)
	var transport *laser.TransportError
	if !errors.As(err, &transport) {
		t.Fatalf("want a TransportError, got %T: %v", err, err)
	}
}

func TestGivenAClosedHandleWhenOptionsAreDescribedThenShouldSaySo(t *testing.T) {
	handle := systemHandle(&systemServer{})
	if err := handle.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := handle.DescribeOptions(
		context.Background(), laser.OptionsScopeTopic); err == nil {
		t.Fatal("a closed handle has no connection to describe options over")
	}
}

// leBytes is a little-endian integer of width bytes, the way the option
// codec writes one.
func leBytes(value uint64, width int) []byte {
	encoded := make([]byte, 8)
	binary.LittleEndian.PutUint64(encoded, value)
	return encoded[:width]
}

// wideBytes is a little-endian 128-bit integer from its two halves.
func wideBytes(high, low uint64) []byte {
	encoded := make([]byte, 16)
	binary.LittleEndian.PutUint64(encoded[0:8], low)
	binary.LittleEndian.PutUint64(encoded[8:16], high)
	return encoded
}
