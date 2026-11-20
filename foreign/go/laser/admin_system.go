package laser

import (
	"context"
	"errors"
	"math"
	"sort"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
)

// getSnapshotCode asks the server for a diagnostics archive.
//
// The Apache Iggy Go SDK keeps its command codes in an internal package and
// wraps no typed call for this one, so the code is spelled here, the way the
// partition route code is. It is part of the server's wire protocol rather
// than that module's API, so it moves with the protocol and not with the SDK.
const getSnapshotCode uint32 = 11

// The snapshot request body, which the server reads as
// [compression][types count][type]*.
const (
	// snapshotDeflated compresses the archive with Deflate, as the Rust
	// client asks for, so an archive taken through any client unpacks the
	// same way.
	snapshotDeflated byte = 2
	// snapshotAllSections is every section the server collects: the file
	// system overview, the process list, resource use, the server logs and
	// the server configuration.
	snapshotAllSections byte = 100
)

// notSignedIn is the user id the server sends for a client that has
// connected but has not signed in yet.
const notSignedIn uint32 = math.MaxUint32

// ClientConsumerGroup is one consumer group a connected client is a member
// of, by native ids.
type ClientConsumerGroup struct {
	StreamID uint32
	TopicID  uint32
	GroupID  uint32
}

// ClientInfo is one connected client as the server reports it. A client is
// not a user: it is one connection, and several of them can belong to the
// same user.
type ClientInfo struct {
	ClientID uint32
	// UserID is the signed-in user, nil for a client that has connected but
	// not signed in.
	UserID              *uint32
	Address             string
	Transport           string
	ConsumerGroupsCount uint32
	// ConsumerGroups is empty in a listing, which carries counts only.
	ConsumerGroups []ClientConsumerGroup
}

// CacheMetrics is the cache use of one partition, by native ids.
type CacheMetrics struct {
	StreamID    uint32
	TopicID     uint32
	PartitionID uint32
	Hits        uint64
	Misses      uint64
	HitRatio    float32
}

// ServerStats is the server statistics. Sizes are in bytes and times in
// microseconds.
type ServerStats struct {
	ProcessID            uint32
	CPUUsage             float32
	TotalCPUUsage        float32
	MemoryUsageBytes     uint64
	TotalMemoryBytes     uint64
	AvailableMemoryBytes uint64
	RunTimeMicros        uint64
	StartTimeMicros      uint64
	ReadBytes            uint64
	WrittenBytes         uint64
	MessagesSizeBytes    uint64
	StreamsCount         uint32
	TopicsCount          uint32
	PartitionsCount      uint32
	SegmentsCount        uint32
	MessagesCount        uint64
	ClientsCount         uint32
	ConsumerGroupsCount  uint32
	Hostname             string
	OsName               string
	OsVersion            string
	KernelVersion        string
	ServerVersion        string
	ThreadsCount         uint32
	FreeDiskSpaceBytes   uint64
	TotalDiskSpaceBytes  uint64
	OpenFilesCount       uint64
	OpenFilesLimit       uint64
	CacheMetrics         []CacheMetrics
}

// ClusterNodeRole is the role of one cluster node.
type ClusterNodeRole uint8

const (
	// ClusterLeader is the node that submits the replicated writes.
	ClusterLeader ClusterNodeRole = iota
	// ClusterFollower is a node that applies what the leader submitted.
	ClusterFollower
)

// String is the role word the other clients use.
func (role ClusterNodeRole) String() string {
	if role == ClusterFollower {
		return "follower"
	}
	return "leader"
}

// ClusterNodeStatus is the state of one cluster node.
type ClusterNodeStatus uint8

const (
	ClusterHealthy ClusterNodeStatus = iota
	ClusterStarting
	ClusterStopping
	ClusterUnreachable
	ClusterMaintenance
	// ClusterUnknown is a state this build does not know, which is also what
	// a node reports while nothing has been established about it.
	ClusterUnknown
)

// String is the status word the other clients use.
func (status ClusterNodeStatus) String() string {
	switch status {
	case ClusterHealthy:
		return "healthy"
	case ClusterStarting:
		return "starting"
	case ClusterStopping:
		return "stopping"
	case ClusterUnreachable:
		return "unreachable"
	case ClusterMaintenance:
		return "maintenance"
	default:
		return "unknown"
	}
}

// ClusterNode is one node of the cluster. A port is zero when the node does
// not serve that transport.
type ClusterNode struct {
	ReplicaID     uint8
	Name          string
	IP            string
	TCPPort       uint16
	QUICPort      uint16
	HTTPPort      uint16
	WebSocketPort uint16
	Role          ClusterNodeRole
	Status        ClusterNodeStatus
}

// ClusterInfo is the cluster as the server reports it. A single server
// reports one node.
type ClusterInfo struct {
	Name  string
	Nodes []ClusterNode
}

// Ping checks that the server answers. It is the one call a connection can
// make before it signs in.
func (laser *Laser) Ping(ctx context.Context) error {
	client, err := laser.connected()
	if err != nil {
		return err
	}
	if err := client.Ping(ctx); err != nil {
		return &TransportError{Reason: "ping the server", Cause: err}
	}
	return nil
}

// Stats is the server statistics. It needs the read servers permission.
func (laser *Laser) Stats(ctx context.Context) (*ServerStats, error) {
	client, err := laser.connected()
	if err != nil {
		return nil, err
	}
	stats, err := client.GetStats(ctx)
	switch {
	case err != nil:
		return nil, &TransportError{Reason: "read the server statistics", Cause: err}
	case stats == nil:
		return nil, &TransportError{
			Reason: "read the server statistics, which reported none back"}
	}
	return serverStatsOf(stats), nil
}

// Me is this connection as the server sees it.
func (laser *Laser) Me(ctx context.Context) (*ClientInfo, error) {
	client, err := laser.connected()
	if err != nil {
		return nil, err
	}
	details, err := client.GetMe(ctx)
	switch {
	case err != nil:
		return nil, &TransportError{Reason: "read this connection", Cause: err}
	case details == nil:
		return nil, &TransportError{
			Reason: "read this connection, which the server did not describe"}
	}
	info := clientDetailsOf(details)
	return &info, nil
}

// Clients is every client connected to the server, without their consumer
// groups. It needs the read servers permission.
func (laser *Laser) Clients(ctx context.Context) ([]ClientInfo, error) {
	client, err := laser.connected()
	if err != nil {
		return nil, err
	}
	clients, err := client.GetClients(ctx)
	if err != nil {
		return nil, &TransportError{Reason: "list the connected clients", Cause: err}
	}
	listed := make([]ClientInfo, 0, len(clients))
	for _, connected := range clients {
		listed = append(listed, clientInfoOf(connected))
	}
	return listed, nil
}

// ClientInfo is one connected client with its consumer groups, nil when no
// client holds clientID. It needs the read servers permission.
//
// A client id belongs to one connection, so it stops existing the moment that
// connection closes. Asking about one that is gone is a normal read with the
// answer "nobody", not a failure.
func (laser *Laser) ClientInfo(
	ctx context.Context, clientID uint32,
) (*ClientInfo, error) {
	client, err := laser.connected()
	if err != nil {
		return nil, err
	}
	details, err := client.GetClient(ctx, clientID)
	switch {
	case isMissingClient(err):
		return nil, nil
	case err != nil:
		return nil, &TransportError{Reason: "read the connected client", Cause: err}
	case details == nil:
		return nil, nil
	}
	info := clientDetailsOf(details)
	return &info, nil
}

// Cluster is the cluster name with its nodes, their roles and their states.
func (laser *Laser) Cluster(ctx context.Context) (*ClusterInfo, error) {
	client, err := laser.connected()
	if err != nil {
		return nil, err
	}
	metadata, err := client.GetClusterMetadata(ctx)
	switch {
	case err != nil:
		return nil, &TransportError{Reason: "read the cluster metadata", Cause: err}
	case metadata == nil:
		return nil, &TransportError{
			Reason: "read the cluster metadata, which named no cluster"}
	}
	cluster := &ClusterInfo{
		Name:  metadata.Name,
		Nodes: make([]ClusterNode, 0, len(metadata.Nodes)),
	}
	for _, node := range metadata.Nodes {
		cluster.Nodes = append(cluster.Nodes, ClusterNode{
			ReplicaID:     node.ReplicaID,
			Name:          node.Name,
			IP:            node.IP,
			TCPPort:       node.Endpoints.Tcp,
			QUICPort:      node.Endpoints.Quic,
			HTTPPort:      node.Endpoints.Http,
			WebSocketPort: node.Endpoints.WebSocket,
			Role:          clusterRoleOf(node.Role),
			Status:        clusterStatusOf(node.Status),
		})
	}
	return cluster, nil
}

// Snapshot is a server diagnostics archive as the bytes of a ZIP file, with
// every section the server collects. It needs the manage servers permission.
//
// The whole archive arrives in one reply and is held in memory, so take one
// when something needs diagnosing rather than on a schedule.
func (laser *Laser) Snapshot(ctx context.Context) ([]byte, error) {
	client, err := laser.connected()
	if err != nil {
		return nil, err
	}
	archive, err := client.SendBinaryRequest(ctx, getSnapshotCode,
		[]byte{snapshotDeflated, 1, snapshotAllSections})
	if err != nil {
		return nil, &TransportError{Reason: "take a server snapshot", Cause: err}
	}
	// The reply buffer belongs to the connection, which reuses it, so the
	// archive is copied out before it is handed over.
	return append([]byte(nil), archive...), nil
}

// isMissingClient reports the error that means no connection holds that id,
// which reads as absence rather than as failure.
func isMissingClient(err error) bool {
	return errors.Is(err, ierror.ClientNotFound{}) ||
		errors.Is(err, ierror.ResourceNotFound{})
}

func serverStatsOf(stats *iggcon.Stats) *ServerStats {
	server := &ServerStats{
		ProcessID:            stats.ProcessId,
		CPUUsage:             stats.CpuUsage,
		TotalCPUUsage:        stats.TotalCpuUsage,
		MemoryUsageBytes:     stats.MemoryUsage,
		TotalMemoryBytes:     stats.TotalMemory,
		AvailableMemoryBytes: stats.AvailableMemory,
		RunTimeMicros:        stats.RunTime,
		StartTimeMicros:      stats.StartTime,
		ReadBytes:            stats.ReadBytes,
		WrittenBytes:         stats.WrittenBytes,
		MessagesSizeBytes:    stats.MessagesSizeBytes,
		StreamsCount:         stats.StreamsCount,
		TopicsCount:          stats.TopicsCount,
		PartitionsCount:      stats.PartitionsCount,
		SegmentsCount:        stats.SegmentsCount,
		MessagesCount:        stats.MessagesCount,
		ClientsCount:         stats.ClientsCount,
		ConsumerGroupsCount:  stats.ConsumerGroupsCount,
		Hostname:             stats.Hostname,
		OsName:               stats.OsName,
		OsVersion:            stats.OsVersion,
		KernelVersion:        stats.KernelVersion,
		ServerVersion:        stats.IggyServerVersion,
		ThreadsCount:         stats.ThreadsCount,
		FreeDiskSpaceBytes:   stats.FreeDiskSpace,
		TotalDiskSpaceBytes:  stats.TotalDiskSpace,
		OpenFilesCount:       stats.OpenFilesCount,
		OpenFilesLimit:       stats.OpenFilesLimit,
	}
	for _, metrics := range stats.CacheMetrics {
		server.CacheMetrics = append(server.CacheMetrics, CacheMetrics{
			StreamID:    metrics.StreamId,
			TopicID:     metrics.TopicId,
			PartitionID: metrics.PartitionId,
			Hits:        metrics.Hits,
			Misses:      metrics.Misses,
			HitRatio:    metrics.HitRatio,
		})
	}
	// The server reports the per-partition metrics in whatever order it
	// holds them. Ordering them by partition keeps two reads of an unchanged
	// server comparable, which is what a statistics reader does with them.
	sort.Slice(server.CacheMetrics, func(first, second int) bool {
		left, right := server.CacheMetrics[first], server.CacheMetrics[second]
		if left.StreamID != right.StreamID {
			return left.StreamID < right.StreamID
		}
		if left.TopicID != right.TopicID {
			return left.TopicID < right.TopicID
		}
		return left.PartitionID < right.PartitionID
	})
	return server
}

func clientInfoOf(client iggcon.ClientInfo) ClientInfo {
	info := ClientInfo{
		ClientID:            client.ID,
		Address:             client.Address,
		Transport:           client.Transport,
		ConsumerGroupsCount: client.ConsumerGroupsCount,
	}
	// A client that has connected but not signed in has no user, which the
	// server sends as the id no user can hold. Reporting that number would
	// have a caller look up a user that cannot exist.
	if client.UserID != notSignedIn {
		userID := client.UserID
		info.UserID = &userID
	}
	return info
}

func clientDetailsOf(details *iggcon.ClientInfoDetails) ClientInfo {
	info := clientInfoOf(details.ClientInfo)
	info.ConsumerGroups = make([]ClientConsumerGroup, 0, len(details.ConsumerGroups))
	for _, group := range details.ConsumerGroups {
		info.ConsumerGroups = append(info.ConsumerGroups, ClientConsumerGroup{
			StreamID: group.StreamId,
			TopicID:  group.TopicId,
			GroupID:  group.GroupId,
		})
	}
	// The memberships are ordered so two reads of an unchanged client
	// compare equal, which a caller diffing them depends on.
	sort.Slice(info.ConsumerGroups, func(first, second int) bool {
		left, right := info.ConsumerGroups[first], info.ConsumerGroups[second]
		if left.StreamID != right.StreamID {
			return left.StreamID < right.StreamID
		}
		if left.TopicID != right.TopicID {
			return left.TopicID < right.TopicID
		}
		return left.GroupID < right.GroupID
	})
	return info
}

func clusterRoleOf(role iggcon.ClusterNodeRole) ClusterNodeRole {
	if role == iggcon.RoleFollower {
		return ClusterFollower
	}
	return ClusterLeader
}

func clusterStatusOf(status iggcon.ClusterNodeStatus) ClusterNodeStatus {
	switch status {
	case iggcon.Healthy:
		return ClusterHealthy
	case iggcon.Starting:
		return ClusterStarting
	case iggcon.Stopping:
		return ClusterStopping
	case iggcon.Unreachable:
		return ClusterUnreachable
	case iggcon.Maintenance:
		return ClusterMaintenance
	default:
		return ClusterUnknown
	}
}
