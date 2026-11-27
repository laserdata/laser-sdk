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

// MetadataChangeGroup is a family of metadata change records, and the unit a
// query filters on.
type MetadataChangeGroup uint8

const (
	MetadataGroupStreams              MetadataChangeGroup = 1
	MetadataGroupTopics               MetadataChangeGroup = 2
	MetadataGroupPartitions           MetadataChangeGroup = 3
	MetadataGroupConsumerGroups       MetadataChangeGroup = 4
	MetadataGroupConsumerGroupMembers MetadataChangeGroup = 5
	MetadataGroupUsers                MetadataChangeGroup = 6
	MetadataGroupPersonalAccessTokens MetadataChangeGroup = 7
	MetadataGroupSessions             MetadataChangeGroup = 8
)

// Known reports whether this client understands the group. A page can carry
// a group a newer server added, which is kept as its wire code so a mirror
// can skip it rather than mistake it for one it knows.
func (group MetadataChangeGroup) Known() bool {
	return iggcon.MetadataChangeGroup(group).IsKnown()
}

// String is the group word the other clients use.
func (group MetadataChangeGroup) String() string {
	return iggcon.MetadataChangeGroup(group).String()
}

// ParseMetadataChangeGroup reads a group word.
func ParseMetadataChangeGroup(word string) (MetadataChangeGroup, error) {
	group, err := iggcon.ParseMetadataChangeGroup(word)
	if err != nil {
		return 0, &InvalidError{
			Reason: "unknown metadata change group " + quoted(word)}
	}
	return MetadataChangeGroup(group), nil
}

// MetadataChangeKind is what one metadata change record says happened.
type MetadataChangeKind uint8

const (
	MetadataKindStreamCreated                    MetadataChangeKind = 1
	MetadataKindStreamUpdated                    MetadataChangeKind = 2
	MetadataKindStreamDeleted                    MetadataChangeKind = 3
	MetadataKindTopicCreated                     MetadataChangeKind = 10
	MetadataKindTopicUpdated                     MetadataChangeKind = 11
	MetadataKindTopicDeleted                     MetadataChangeKind = 12
	MetadataKindPartitionsChanged                MetadataChangeKind = 20
	MetadataKindConsumerGroupCreated             MetadataChangeKind = 30
	MetadataKindConsumerGroupDeleted             MetadataChangeKind = 31
	MetadataKindConsumerGroupMemberJoined        MetadataChangeKind = 40
	MetadataKindConsumerGroupMemberLeft          MetadataChangeKind = 41
	MetadataKindConsumerGroupPartitionHandedOver MetadataChangeKind = 42
	MetadataKindUserCreated                      MetadataChangeKind = 50
	MetadataKindUserUpdated                      MetadataChangeKind = 51
	MetadataKindUserDeleted                      MetadataChangeKind = 52
	MetadataKindUserPasswordChanged              MetadataChangeKind = 53
	MetadataKindUserPermissionsUpdated           MetadataChangeKind = 54
	MetadataKindPersonalAccessTokenCreated       MetadataChangeKind = 60
	MetadataKindPersonalAccessTokenDeleted       MetadataChangeKind = 61
	MetadataKindSessionRegistered                MetadataChangeKind = 70
	MetadataKindSessionClosed                    MetadataChangeKind = 71
)

// Known reports whether this client understands the kind. A record of an
// unknown kind carries an empty subject, because nothing can be read out of
// a body whose shape this build does not have.
func (kind MetadataChangeKind) Known() bool {
	return iggcon.MetadataChangeKind(kind).Group() != 0
}

// String is the kind word the other clients use.
func (kind MetadataChangeKind) String() string {
	return iggcon.MetadataChangeKind(kind).String()
}

// MetadataChangeStatus is the outcome of a metadata change query.
type MetadataChangeStatus uint8

const (
	// MetadataStatusOk carries the records or items after the start.
	MetadataStatusOk MetadataChangeStatus = 0
	// MetadataStatusBehind means the answering node has not applied the
	// start yet, so ask again.
	MetadataStatusBehind MetadataChangeStatus = 1
	// MetadataStatusExpired means the node no longer holds every record
	// after the start.
	MetadataStatusExpired MetadataChangeStatus = 2
	// MetadataStatusReset means the start belongs to another history.
	MetadataStatusReset MetadataChangeStatus = 3
)

// String is the status word the other clients use.
func (status MetadataChangeStatus) String() string {
	return iggcon.MetadataChangeStatus(status).String()
}

// MetadataChangePhase is whether a page holds change records or current
// state items.
type MetadataChangePhase uint8

const (
	MetadataPhaseChanges MetadataChangePhase = 0
	MetadataPhaseCurrent MetadataChangePhase = 1
)

// String is the phase word the other clients use.
func (phase MetadataChangePhase) String() string {
	return iggcon.MetadataChangePhase(phase).String()
}

// MetadataChangeReason is why a member left, a token was deleted, a session
// closed, or a user's permissions changed.
//
// The server keeps one set of reasons per change, and they overlap, so they
// are gathered into one list here the way the other clients gather them.
type MetadataChangeReason uint8

const (
	MetadataReasonLeft MetadataChangeReason = iota
	MetadataReasonSessionEnded
	MetadataReasonDeleted
	MetadataReasonExpired
	MetadataReasonLogout
	MetadataReasonRevoked
	MetadataReasonUpdated
	MetadataReasonResourceDeleted
)

// String is the reason word the other clients use.
func (reason MetadataChangeReason) String() string {
	switch reason {
	case MetadataReasonLeft:
		return "left"
	case MetadataReasonSessionEnded:
		return "session_ended"
	case MetadataReasonDeleted:
		return "deleted"
	case MetadataReasonExpired:
		return "expired"
	case MetadataReasonLogout:
		return "logout"
	case MetadataReasonRevoked:
		return "revoked"
	case MetadataReasonUpdated:
		return "updated"
	case MetadataReasonResourceDeleted:
		return "resource_deleted"
	default:
		return "unknown"
	}
}

// MetadataChangeStartMode is where a metadata change query starts.
type MetadataChangeStartMode uint8

const (
	// MetadataStartEarliest is the oldest record the requested groups still
	// hold completely.
	MetadataStartEarliest MetadataChangeStartMode = iota
	// MetadataStartLatest returns no records, only a cursor at the newest
	// one.
	MetadataStartLatest
	// MetadataStartAfter continues from the Next cursor of an earlier page.
	MetadataStartAfter
	// MetadataStartCurrent pages the current state instead of the changes.
	MetadataStartCurrent
)

// MetadataChangeStart says where a query begins.
type MetadataChangeStart struct {
	Mode MetadataChangeStartMode
	// Cursor is the Next cursor of an earlier page, for the after start.
	Cursor string
	// PageToken continues a current state scan, empty to start one.
	PageToken string
}

// MetadataChangesQuery is one request for a page of metadata changes, or of
// the current state.
//
// The zero value asks for the earliest records of every group at the
// server's page size, which is what reading the feed from its start means.
type MetadataChangesQuery struct {
	Start MetadataChangeStart
	// Groups selects the record families, empty means every group.
	Groups []MetadataChangeGroup
	// SinceMicros and UntilMicros keep records stamped inside the inclusive
	// range. They apply only to the earliest and after starts.
	SinceMicros *uint64
	UntilMicros *uint64
	// Limit is 1 to 1000 records or items. Zero selects the server's page
	// size, so the zero query reads the feed rather than asking for nothing.
	Limit uint32
}

// invalidMetadataQuery says what the server's request rules are, because a
// query that breaks one of them is refused as a whole rather than per rule.
const invalidMetadataQuery = "a metadata change query needs a limit of 1 to " +
	"1000, since no later than until, no time bound with the latest or " +
	"current start, and every parent group in a current scan"

// MetadataClientID is the 128-bit id the server gives one connection. Go has
// no integer that wide, so it is carried as its two halves.
type MetadataClientID struct {
	High uint64
	Low  uint64
}

// MetadataSubject is what one change record or current state item is about.
//
// Each kind fills the fields that name its resource and what changed, and
// leaves the rest unset. Ids are native ids. A created op or a user creation
// time tells a recreated resource apart from an older one with the same id.
// Resource options are not carried.
type MetadataSubject struct {
	StreamID            *uint32
	StreamCreatedOp     *uint64
	TopicID             *uint32
	TopicCreatedOp      *uint64
	GroupID             *uint32
	MemberID            *uint32
	PartitionID         *uint32
	UserID              *uint32
	UserCreatedAtMicros *uint64
	ClientID            *MetadataClientID
	Session             *uint64
	// Name is the stream, topic, consumer group or personal access token
	// name, empty when the change names none.
	Name string
	// Username is empty when the change names no user.
	Username            string
	CreatedAtMicros     *uint64
	PartitionsCount     *uint32
	PartitionIDs        []uint32
	RemovedPartitionIDs []uint32
	ConsumerGroupsCount *uint32
	MembersCount        *uint32
	// RequestedOp is the op of the request a deferred delete answers.
	RequestedOp     *uint64
	Status          *UserStatus
	Permissions     *Permissions
	ExpiresAtMicros *uint64
	Reason          *MetadataChangeReason
}

// MetadataChangeRecord is one committed metadata change.
type MetadataChangeRecord struct {
	Op uint64
	// Ordinal is the position of this record among the records of one op.
	Ordinal         uint32
	TimestampMicros uint64
	// ActorUserID is nil for a change the server made itself.
	ActorUserID *uint32
	Group       MetadataChangeGroup
	// Kind is the wire code. A kind this client does not know carries an
	// empty subject, which Kind.Known reports.
	Kind    MetadataChangeKind
	Subject MetadataSubject
}

// MetadataItem is one resource of the current state. A group this client
// does not know carries an empty subject, which Group.Known reports.
type MetadataItem struct {
	Group   MetadataChangeGroup
	Subject MetadataSubject
}

// MetadataChanges is one page of metadata changes, or of the current state.
//
// Send Next back as the cursor of an after start to read on, and PageToken
// as the token of a current start to continue a current state scan.
type MetadataChanges struct {
	Status MetadataChangeStatus
	Phase  MetadataChangePhase
	// HeadOp is the newest op the answering node applied.
	HeadOp uint64
	// OldestOp is the oldest op from which the requested groups are
	// complete.
	OldestOp uint64
	// RetainedSinceMicros is the time after which the requested groups are
	// complete, zero when the node dropped nothing.
	RetainedSinceMicros uint64
	// Next is the cursor after this page, empty on an expired and a reset
	// status.
	Next string
	// HasMore says more records follow Next, or another current state page
	// exists.
	HasMore bool
	Records []MetadataChangeRecord
	Items   []MetadataItem
	// PageToken is the token of the next current state page, empty once the
	// scan ends.
	PageToken string
}

// MetadataChanges is one page of the server's metadata changes, or of its
// current state. It needs the manage servers permission.
//
// A query that breaks the server's request rules is refused before anything
// is sent, because the rules are about the request rather than about the
// state, so the round trip could only come back with the same answer.
func (laser *Laser) MetadataChanges(
	ctx context.Context, query MetadataChangesQuery,
) (*MetadataChanges, error) {
	asked, err := query.native()
	if err != nil {
		return nil, err
	}
	client, err := laser.connected()
	if err != nil {
		return nil, err
	}
	page, err := client.GetMetadataChanges(ctx, asked)
	switch {
	case err != nil:
		return nil, &TransportError{Reason: "read the metadata changes", Cause: err}
	case page == nil:
		return nil, &TransportError{
			Reason: "read the metadata changes, which reported no page back"}
	}
	return metadataPageOf(page), nil
}

// native is the query the server reads, checked against its request rules.
func (query MetadataChangesQuery) native() (iggcon.MetadataChangesQuery, error) {
	start, err := query.Start.native()
	if err != nil {
		return iggcon.MetadataChangesQuery{}, err
	}
	asked := iggcon.MetadataChangesQuery{
		Start: start,
		Since: query.SinceMicros,
		Until: query.UntilMicros,
		// The zero value has to mean what the Rust client's default means,
		// so an unset limit asks for the server's page size rather than for
		// nothing at all.
		Limit: iggcon.DefaultMetadataChangesLimit,
	}
	if query.Limit != 0 {
		asked.Limit = query.Limit
	}
	for _, group := range query.Groups {
		asked.Groups = append(asked.Groups, iggcon.MetadataChangeGroup(group))
	}
	if err := asked.Validate(); err != nil {
		return iggcon.MetadataChangesQuery{},
			&InvalidError{Reason: invalidMetadataQuery}
	}
	return asked, nil
}

// native is the start the server reads. A cursor and a page token are opaque
// text the server minted, so malformed text is named back rather than sent.
func (start MetadataChangeStart) native() (iggcon.MetadataChangeStart, error) {
	switch start.Mode {
	case MetadataStartEarliest:
		return iggcon.EarliestMetadataChangeStart(), nil
	case MetadataStartLatest:
		return iggcon.LatestMetadataChangeStart(), nil
	case MetadataStartAfter:
		cursor, err := iggcon.ParseMetadataChangeCursor(start.Cursor)
		if err != nil {
			return iggcon.MetadataChangeStart{}, &InvalidError{
				Reason: "malformed metadata change cursor " + quoted(start.Cursor)}
		}
		return iggcon.AfterMetadataChangeStart(cursor), nil
	case MetadataStartCurrent:
		if start.PageToken == "" {
			return iggcon.CurrentMetadataChangeStart(nil), nil
		}
		token, err := iggcon.ParseMetadataCurrentPageToken(start.PageToken)
		if err != nil {
			return iggcon.MetadataChangeStart{}, &InvalidError{
				Reason: "malformed metadata page token " + quoted(start.PageToken)}
		}
		return iggcon.CurrentMetadataChangeStart(token), nil
	default:
		return iggcon.MetadataChangeStart{}, &InvalidError{
			Reason: "a metadata change query starts at the earliest record, " +
				"the latest one, after a cursor, or at the current state"}
	}
}

func metadataPageOf(page *iggcon.MetadataChanges) *MetadataChanges {
	read := &MetadataChanges{
		Status:              MetadataChangeStatus(page.Status),
		Phase:               MetadataChangePhase(page.Phase),
		HeadOp:              page.HeadOp,
		OldestOp:            page.OldestOp,
		RetainedSinceMicros: page.RetainedSince,
		HasMore:             page.HasMore,
		Records:             make([]MetadataChangeRecord, 0, len(page.Records)),
		Items:               make([]MetadataItem, 0, len(page.Items)),
	}
	if page.Next != nil {
		read.Next = page.Next.String()
	}
	if page.CurrentPageToken != nil {
		read.PageToken = page.CurrentPageToken.String()
	}
	for _, record := range page.Records {
		read.Records = append(read.Records, MetadataChangeRecord{
			Op:              record.Op,
			Ordinal:         record.Ordinal,
			TimestampMicros: record.Timestamp,
			ActorUserID:     record.ActorUserID,
			Group:           MetadataChangeGroup(record.Group),
			Kind:            MetadataChangeKind(record.Kind),
			Subject:         metadataSubjectOf(record.Body),
		})
	}
	for _, item := range page.Items {
		read.Items = append(read.Items, MetadataItem{
			Group:   MetadataChangeGroup(item.Group),
			Subject: metadataSubjectOf(item.Body),
		})
	}
	return read
}

// metadataSubjectOf flattens one typed body into the fields that name its
// resource and what changed.
//
// A body this build does not know reports an empty subject rather than a
// guess, because its shape is the only thing that says what its bytes mean.
func metadataSubjectOf(body iggcon.MetadataChangeBody) MetadataSubject {
	switch body := body.(type) {
	case *iggcon.MetadataStreamChange:
		subject := streamSubject(body.MetadataStreamKey)
		subject.CreatedAtMicros = ptrOf(body.CreatedAt)
		subject.Name = body.Name
		return subject
	case *iggcon.MetadataStreamDeleted:
		subject := streamSubject(body.MetadataStreamKey)
		subject.CreatedAtMicros = ptrOf(body.CreatedAt)
		subject.Name = body.Name
		subject.RequestedOp = body.RequestedOp
		return subject
	case *iggcon.MetadataTopicChange:
		subject := topicSubject(body.MetadataTopicKey)
		subject.CreatedAtMicros = ptrOf(body.CreatedAt)
		subject.Name = body.Name
		subject.PartitionsCount = ptrOf(body.PartitionsCount)
		return subject
	case *iggcon.MetadataTopicItem:
		subject := topicSubject(body.MetadataTopicKey)
		subject.CreatedAtMicros = ptrOf(body.CreatedAt)
		subject.Name = body.Name
		return subject
	case *iggcon.MetadataTopicDeleted:
		subject := topicSubject(body.MetadataTopicKey)
		subject.CreatedAtMicros = ptrOf(body.CreatedAt)
		subject.Name = body.Name
		subject.RequestedOp = body.RequestedOp
		return subject
	case *iggcon.MetadataPartitionsChanged:
		subject := topicSubject(body.MetadataTopicKey)
		subject.PartitionsCount = ptrOf(body.PartitionsCount)
		subject.RemovedPartitionIDs = copyIDs(body.RemovedPartitionIDs)
		subject.RequestedOp = body.RequestedOp
		return subject
	case *iggcon.MetadataPartitionsItem:
		subject := topicSubject(body.MetadataTopicKey)
		subject.PartitionsCount = ptrOf(body.PartitionsCount)
		return subject
	case *iggcon.MetadataConsumerGroupCreated:
		subject := topicSubject(body.MetadataTopicKey)
		subject.GroupID = ptrOf(body.GroupID)
		subject.Name = body.Name
		subject.ConsumerGroupsCount = ptrOf(body.ConsumerGroupsCount)
		return subject
	case *iggcon.MetadataConsumerGroupDeleted:
		subject := topicSubject(body.MetadataTopicKey)
		subject.GroupID = ptrOf(body.GroupID)
		subject.Name = body.Name
		subject.ConsumerGroupsCount = ptrOf(body.ConsumerGroupsCount)
		subject.RequestedOp = body.RequestedOp
		return subject
	case *iggcon.MetadataConsumerGroupItem:
		subject := topicSubject(body.MetadataTopicKey)
		subject.GroupID = ptrOf(body.GroupID)
		subject.Name = body.Name
		return subject
	case *iggcon.MetadataMemberJoined:
		subject := topicSubject(body.MetadataTopicKey)
		subject.GroupID = ptrOf(body.GroupID)
		subject.MemberID = ptrOf(body.MemberID)
		subject.ClientID = clientIdentityOf(body.ClientID)
		subject.MembersCount = ptrOf(body.MembersCount)
		subject.PartitionIDs = copyIDs(body.PartitionIDs)
		return subject
	case *iggcon.MetadataMemberLeft:
		subject := topicSubject(body.MetadataTopicKey)
		subject.GroupID = ptrOf(body.GroupID)
		subject.MemberID = ptrOf(body.MemberID)
		subject.ClientID = clientIdentityOf(body.ClientID)
		subject.MembersCount = ptrOf(body.MembersCount)
		subject.Reason = memberLeftReason(body.Reason)
		return subject
	case *iggcon.MetadataPartitionHandedOver:
		subject := topicSubject(body.MetadataTopicKey)
		subject.GroupID = ptrOf(body.GroupID)
		subject.PartitionID = ptrOf(body.PartitionID)
		subject.MemberID = body.MemberID
		return subject
	case *iggcon.MetadataMemberItem:
		subject := topicSubject(body.MetadataTopicKey)
		subject.GroupID = ptrOf(body.GroupID)
		subject.MemberID = ptrOf(body.MemberID)
		subject.ClientID = clientIdentityOf(body.ClientID)
		subject.PartitionIDs = copyIDs(body.PartitionIDs)
		return subject
	case *iggcon.MetadataUserChange:
		subject := userSubject(body.MetadataUserKey)
		subject.Username = body.Username
		subject.Status = ptrOf(userStatusOf(body.Status))
		subject.Permissions = permissionsOf(body.Permissions)
		return subject
	case *iggcon.MetadataUserIdentity:
		subject := userSubject(body.MetadataUserKey)
		subject.Username = body.Username
		return subject
	case *iggcon.MetadataUserPermissionsUpdated:
		subject := userSubject(body.MetadataUserKey)
		subject.Username = body.Username
		subject.Permissions = permissionsOf(body.Permissions)
		subject.Reason = permissionsUpdatedReason(body.Reason)
		return subject
	case *iggcon.MetadataTokenChange:
		subject := userSubject(body.MetadataUserKey)
		subject.Name = body.Name
		subject.ExpiresAtMicros = body.Expiry
		return subject
	case *iggcon.MetadataTokenDeleted:
		subject := userSubject(body.MetadataUserKey)
		subject.Name = body.Name
		subject.Reason = tokenDeletedReason(body.Reason)
		return subject
	case *iggcon.MetadataSession:
		subject := userSubject(body.MetadataUserKey)
		subject.ClientID = clientIdentityOf(body.ClientID)
		subject.Session = ptrOf(body.Session)
		return subject
	case *iggcon.MetadataSessionClosed:
		subject := userSubject(body.MetadataUserKey)
		subject.ClientID = clientIdentityOf(body.ClientID)
		subject.Session = ptrOf(body.Session)
		subject.Reason = sessionClosedReason(body.Reason)
		return subject
	default:
		return MetadataSubject{}
	}
}

func streamSubject(key iggcon.MetadataStreamKey) MetadataSubject {
	return MetadataSubject{
		StreamID:        ptrOf(key.StreamID),
		StreamCreatedOp: ptrOf(key.StreamCreatedOp),
	}
}

func topicSubject(key iggcon.MetadataTopicKey) MetadataSubject {
	subject := streamSubject(key.MetadataStreamKey)
	subject.TopicID = ptrOf(key.TopicID)
	subject.TopicCreatedOp = ptrOf(key.TopicCreatedOp)
	return subject
}

func userSubject(key iggcon.MetadataUserKey) MetadataSubject {
	return MetadataSubject{
		UserID:              ptrOf(key.UserID),
		UserCreatedAtMicros: ptrOf(key.UserCreatedAt),
	}
}

func clientIdentityOf(id iggcon.Unsigned128) *MetadataClientID {
	return &MetadataClientID{High: id.High, Low: id.Low}
}

// copyIDs takes the partition ids out of the decoded page, so a subject
// outlives the page it was read from. An empty list copies to none, which is
// what a change that names no partition carries.
func copyIDs(ids []uint32) []uint32 {
	return append([]uint32(nil), ids...)
}

func memberLeftReason(reason iggcon.MetadataMemberLeftReason) *MetadataChangeReason {
	if reason == iggcon.MetadataMemberLeftReasonSessionEnded {
		return ptrOf(MetadataReasonSessionEnded)
	}
	return ptrOf(MetadataReasonLeft)
}

func tokenDeletedReason(reason iggcon.MetadataTokenDeletedReason) *MetadataChangeReason {
	if reason == iggcon.MetadataTokenDeletedReasonExpired {
		return ptrOf(MetadataReasonExpired)
	}
	return ptrOf(MetadataReasonDeleted)
}

func sessionClosedReason(reason iggcon.MetadataSessionClosedReason) *MetadataChangeReason {
	switch reason {
	case iggcon.MetadataSessionClosedReasonExpired:
		return ptrOf(MetadataReasonExpired)
	case iggcon.MetadataSessionClosedReasonRevoked:
		return ptrOf(MetadataReasonRevoked)
	default:
		return ptrOf(MetadataReasonLogout)
	}
}

func permissionsUpdatedReason(
	reason iggcon.MetadataPermissionsUpdatedReason,
) *MetadataChangeReason {
	if reason == iggcon.MetadataPermissionsUpdatedReasonResourceDeleted {
		return ptrOf(MetadataReasonResourceDeleted)
	}
	return ptrOf(MetadataReasonUpdated)
}
