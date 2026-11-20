package laser

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"math"
	"math/big"
	"sort"
	"strconv"
	"unicode/utf8"

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

// OptionsScope is the resource an option catalog describes.
type OptionsScope uint8

const (
	OptionsScopeTopic  OptionsScope = 1
	OptionsScopeStream OptionsScope = 2
	OptionsScopeUser   OptionsScope = 3
)

// String is the scope word the other clients use.
func (scope OptionsScope) String() string {
	switch scope {
	case OptionsScopeTopic:
		return "topic"
	case OptionsScopeStream:
		return "stream"
	case OptionsScopeUser:
		return "user"
	default:
		return "unknown"
	}
}

// ParseOptionsScope reads a scope word.
func ParseOptionsScope(word string) (OptionsScope, error) {
	switch word {
	case "topic":
		return OptionsScopeTopic, nil
	case "stream":
		return OptionsScopeStream, nil
	case "user":
		return OptionsScopeUser, nil
	default:
		return 0, &InvalidError{Reason: "unknown options scope " + quoted(word) +
			", expected topic, stream or user"}
	}
}

// OptionInfo is one option the server accepts for a scope.
type OptionInfo struct {
	Key string
	// Kind names the value type, such as uint64, bool or string.
	Kind string
	// Updatable means an update may set the option.
	Updatable bool
	// Resettable means an update may reset the option to its default.
	Resettable bool
	// Secret means the server accepts the option and never returns it.
	Secret bool
	// ReadOnly means the server computes the option and never accepts it.
	ReadOnly bool
	// Critical means a node that does not know the option must not serve the
	// resource.
	Critical bool
	// DefaultValue is the default in its text form, empty when the option has
	// no default. Integers are decimal, floats are the 0x hex of their bits,
	// and raw bytes are base64.
	DefaultValue string
	Description  string
}

// DescribeOptions is every option the server accepts for scope, with its
// type, rules, default and description.
//
// The catalog of a server newer than this build names options this client
// has no field for, which is the point of asking: it says what a raw option
// patch may carry.
func (laser *Laser) DescribeOptions(
	ctx context.Context, scope OptionsScope,
) ([]OptionInfo, error) {
	// A scope the server does not know names no catalog, so it is refused
	// here rather than sent for the server to reject.
	if err := iggcon.OptionsScope(scope).Validate(); err != nil {
		return nil, &InvalidError{
			Reason: "an options scope must be topic, stream or user"}
	}
	client, err := laser.connected()
	if err != nil {
		return nil, err
	}
	specs, err := client.DescribeOptions(ctx, iggcon.OptionsScope(scope))
	if err != nil {
		return nil, &TransportError{Reason: "describe the options", Cause: err}
	}
	listed := make([]OptionInfo, 0, len(specs))
	for _, spec := range specs {
		listed = append(listed, OptionInfo{
			Key:          spec.Key,
			Kind:         headerKindName(spec.DefaultValue.Kind),
			Updatable:    spec.Updatable,
			Resettable:   spec.Resettable,
			Secret:       spec.Secret,
			ReadOnly:     spec.ReadOnly,
			Critical:     spec.Critical,
			DefaultValue: optionDefaultText(spec.DefaultValue),
			Description:  spec.Description,
		})
	}
	return listed, nil
}

// headerKindName is the type word the Rust, Python and TypeScript clients
// print for a value kind, so one catalog reads the same through every
// client.
func headerKindName(kind iggcon.HeaderKind) string {
	switch kind {
	case iggcon.Raw:
		return "raw"
	case iggcon.String:
		return "string"
	case iggcon.Bool:
		return "bool"
	case iggcon.Int8:
		return "int8"
	case iggcon.Int16:
		return "int16"
	case iggcon.Int32:
		return "int32"
	case iggcon.Int64:
		return "int64"
	case iggcon.Int128:
		return "int128"
	case iggcon.Uint8:
		return "uint8"
	case iggcon.Uint16:
		return "uint16"
	case iggcon.Uint32:
		return "uint32"
	case iggcon.Uint64:
		return "uint64"
	case iggcon.Uint128:
		return "uint128"
	// Go spells the two floats Float and Double, where the contract and the
	// other clients spell them by their width.
	case iggcon.Float:
		return "float32"
	case iggcon.Double:
		return "float64"
	default:
		return "unknown"
	}
}

// maxOptionValueBytes is the field length the option codec enforces, so a
// longer value is one this client never wrote and cannot read back.
const maxOptionValueBytes = 255

// optionDefaultText renders an option default the way the resource option
// JSON does, so a default read here can be sent back in a raw patch.
//
// A value whose length does not match its kind reports no default rather
// than a wrong one: the bytes would have to be read under a layout the
// server did not write them in.
func optionDefaultText(value iggcon.HeaderValue) string {
	if len(value.Value) == 0 || len(value.Value) > maxOptionValueBytes {
		return ""
	}
	if size := value.Kind.ExpectedSize(); size >= 0 && len(value.Value) != size {
		return ""
	}
	switch value.Kind {
	case iggcon.Raw:
		return base64.StdEncoding.EncodeToString(value.Value)
	case iggcon.String:
		if !utf8.Valid(value.Value) {
			return ""
		}
		return string(value.Value)
	case iggcon.Bool:
		switch value.Value[0] {
		case 0:
			return "false"
		case 1:
			return "true"
		default:
			return ""
		}
	case iggcon.Int8:
		return strconv.FormatInt(int64(int8(value.Value[0])), 10)
	case iggcon.Int16:
		return strconv.FormatInt(
			int64(int16(binary.LittleEndian.Uint16(value.Value))), 10)
	case iggcon.Int32:
		return strconv.FormatInt(
			int64(int32(binary.LittleEndian.Uint32(value.Value))), 10)
	case iggcon.Int64:
		return strconv.FormatInt(
			int64(binary.LittleEndian.Uint64(value.Value)), 10)
	case iggcon.Uint8:
		return strconv.FormatUint(uint64(value.Value[0]), 10)
	case iggcon.Uint16:
		return strconv.FormatUint(
			uint64(binary.LittleEndian.Uint16(value.Value)), 10)
	case iggcon.Uint32:
		return strconv.FormatUint(
			uint64(binary.LittleEndian.Uint32(value.Value)), 10)
	case iggcon.Uint64:
		return strconv.FormatUint(binary.LittleEndian.Uint64(value.Value), 10)
	case iggcon.Int128, iggcon.Uint128:
		return wideIntegerText(value.Value, value.Kind == iggcon.Int128)
	case iggcon.Float:
		return "0x" + paddedHex(
			uint64(binary.LittleEndian.Uint32(value.Value)), 8)
	case iggcon.Double:
		return "0x" + paddedHex(binary.LittleEndian.Uint64(value.Value), 16)
	default:
		return ""
	}
}

// wideIntegerText renders a 16-byte little-endian integer in decimal, which
// no Go integer type is wide enough to hold.
func wideIntegerText(value []byte, signed bool) string {
	reversed := make([]byte, len(value))
	for index, piece := range value {
		reversed[len(value)-1-index] = piece
	}
	number := new(big.Int).SetBytes(reversed)
	// A negative value is the two's complement of its magnitude, so the
	// width is subtracted back off once the sign bit is set.
	if signed && reversed[0]&0x80 != 0 {
		number.Sub(number, new(big.Int).Lsh(big.NewInt(1), uint(len(value))*8))
	}
	return number.String()
}

// paddedHex renders the bits of a float in the fixed width the option text
// uses, so one value has one text form.
func paddedHex(bits uint64, width int) string {
	text := strconv.FormatUint(bits, 16)
	for len(text) < width {
		text = "0" + text
	}
	return text
}
