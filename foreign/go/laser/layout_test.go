package laser_test

import (
	"context"
	"encoding/binary"
	"strings"
	"sync"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// routedSend is where one record landed.
type routedSend struct {
	topic        string
	partitioning iggcon.Partitioning
}

// routingServer records the topic each send reached, which the shared stream
// fake does not, because where a record lands is the whole claim here.
type routingServer struct {
	clusterOfOne
	mutex   sync.Mutex
	sends   []routedSend
	created []string
	// options is the shape each created topic was given, paired with
	// created by index, and createdOn is the stream it was created on.
	options   []*iggcon.TopicOptions
	createdOn []string
	// managed is every managed command code that reached the band, so a test
	// can tell a control record that was published from one that was not.
	managed []uint32
	// topicsExist makes every topic look provisioned, so a bootstrap that
	// creates nothing is distinguishable from one that creates everything.
	topicsExist bool
}

func (server *routingServer) Close() error { return nil }

func (server *routingServer) GetStream(
	_ context.Context, _ iggcon.Identifier,
) (*iggcon.StreamDetails, error) {
	return laneStream(), nil
}

func (server *routingServer) CreateStream(
	_ context.Context, _ string, _ *iggcon.StreamOptions,
) (*iggcon.StreamDetails, error) {
	return laneStream(), nil
}

func (server *routingServer) GetTopic(
	_ context.Context, _, _ iggcon.Identifier,
) (*iggcon.TopicDetails, error) {
	if server.topicsExist {
		return &iggcon.TopicDetails{}, nil
	}
	return nil, ierror.TopicNameNotFound{}
}

func (server *routingServer) CreateTopic(
	_ context.Context, streamID iggcon.Identifier, name string,
	options *iggcon.TopicOptions,
) (*iggcon.TopicDetails, error) {
	stream, err := streamID.String()
	if err != nil {
		return nil, err
	}
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.created = append(server.created, name)
	server.options = append(server.options, options)
	server.createdOn = append(server.createdOn, stream)
	return &iggcon.TopicDetails{}, nil
}

func (server *routingServer) SendBinaryRequest(
	_ context.Context, code uint32, payload []byte,
) ([]byte, error) {
	server.mutex.Lock()
	server.managed = append(server.managed, code)
	server.mutex.Unlock()
	return nil, ierror.InvalidCommand{}
}

// managedCodes is what reached the managed band.
func (server *routingServer) managedCodes() []uint32 {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	return append([]uint32(nil), server.managed...)
}

func (server *routingServer) SendMessages(
	_ context.Context, _, topicID iggcon.Identifier,
	partitioning iggcon.Partitioning, _ []iggcon.IggyMessage,
) (*iggcon.SendMessagesResponse, error) {
	name, err := topicID.String()
	if err != nil {
		return nil, err
	}
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.sends = append(server.sends, routedSend{topic: name, partitioning: partitioning})
	return &iggcon.SendMessagesResponse{}, nil
}

// landed is the single send the server saw.
func (server *routingServer) landed(t *testing.T) routedSend {
	t.Helper()
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if len(server.sends) != 1 {
		t.Fatalf("want one send, got %d", len(server.sends))
	}
	return server.sends[0]
}

// partition reads back a send that named one.
func (send routedSend) partition(t *testing.T) uint32 {
	t.Helper()
	if send.partitioning.Kind != iggcon.PartitionIdKind {
		t.Fatalf("want a partition, got kind %d", send.partitioning.Kind)
	}
	return binary.LittleEndian.Uint32(send.partitioning.Value)
}

// key reads back a send keyed by a value.
func (send routedSend) key(t *testing.T) string {
	t.Helper()
	if send.partitioning.Kind != iggcon.MessageKey {
		t.Fatalf("want a message key, got kind %d", send.partitioning.Kind)
	}
	return string(send.partitioning.Value)
}

// layoutHandle is a handle whose default stream declares layout.
func layoutHandle(
	t *testing.T, server iggcon.Client, layout laser.SessionLayout,
) *laser.Laser {
	t.Helper()
	handle := streamHandle(server, laser.Options{DefaultStream: "agents"})
	handle.SessionsWith(laser.SessionConfig{Layout: layout})
	return handle
}

func inboxLayout(t *testing.T) laser.SessionLayout {
	t.Helper()
	return laser.PerAgentTopicLayout(map[wire.AgentID]string{
		agentID(t, "planner"):  "planner.inbox",
		agentID(t, "executor"): "executor.inbox",
	})
}

func partitionLayout(t *testing.T) laser.SessionLayout {
	t.Helper()
	return laser.PerAgentPartitionLayout(map[wire.AgentID]uint32{
		agentID(t, "planner"):  0,
		agentID(t, "executor"): 2,
	})
}

func TestGivenNoDeclaredLayoutWhenSendingThenShouldKeepTheLane(t *testing.T) {
	server := &routingServer{}
	handle := streamHandle(server, laser.Options{DefaultStream: "agents"})
	agdx := handle.Agdx(agentID(t, "planner"), conversation(1))

	if _, err := agdx.Command(correlationID(2), []byte("{}")).
		WithTarget(agentID(t, "executor")).Send(context.Background()); err != nil {
		t.Fatalf("send: %v", err)
	}

	landed := server.landed(t)
	if landed.topic != wire.AgentSessions {
		t.Fatalf("want the lane, got %q", landed.topic)
	}
	if landed.key(t) != conversation(1).String() {
		t.Fatalf("want the conversation as the key, got %q", landed.key(t))
	}
}

func TestGivenADeclaredTopicLayoutWhenSendingThenShouldMoveOnlyAddressedWork(t *testing.T) {
	for _, probe := range []struct {
		name  string
		send  func(*laser.Agdx) *laser.AgdxSend
		topic string
	}{
		{"a command to a declared agent", func(agdx *laser.Agdx) *laser.AgdxSend {
			return agdx.Command(correlationID(2), []byte("{}")).
				WithTarget(agentID(t, "executor"))
		}, "executor.inbox"},
		{"a reply to its requester", func(agdx *laser.Agdx) *laser.AgdxSend {
			return agdx.Respond(correlationID(2), []byte("{}")).
				WithTarget(agentID(t, "planner"))
		}, "planner.inbox"},
		{"a command to an undeclared agent", func(agdx *laser.Agdx) *laser.AgdxSend {
			return agdx.Command(correlationID(2), []byte("{}")).
				WithTarget(agentID(t, "stranger"))
		}, wire.AgentSessions},
		{"a broadcast command", func(agdx *laser.Agdx) *laser.AgdxSend {
			return agdx.Command(correlationID(2), []byte("{}")).Broadcast()
		}, wire.AgentSessions},
		{"an event to a declared agent", func(agdx *laser.Agdx) *laser.AgdxSend {
			return agdx.Emit([]byte("{}")).WithTarget(agentID(t, "executor"))
		}, wire.AgentSessions},
	} {
		t.Run(probe.name, func(t *testing.T) {
			server := &routingServer{}
			handle := layoutHandle(t, server, inboxLayout(t))
			agdx := handle.Agdx(agentID(t, "planner"), conversation(1))
			if _, err := probe.send(agdx).Send(context.Background()); err != nil {
				t.Fatalf("send: %v", err)
			}
			landed := server.landed(t)
			if landed.topic != probe.topic {
				t.Fatalf("landed on %q, want %q", landed.topic, probe.topic)
			}
			// Moving a record off the lane never changes its key: an agent's
			// own topic is still ordered by session.
			if landed.key(t) != conversation(1).String() {
				t.Fatalf("want the conversation as the key, got %q", landed.key(t))
			}
		})
	}
}

func TestGivenADeclaredPartitionLayoutWhenSendingThenShouldMoveOnlyAddressedWork(t *testing.T) {
	for _, probe := range []struct {
		name      string
		send      func(*laser.Agdx) *laser.AgdxSend
		partition int64
	}{
		{"a command to a declared agent", func(agdx *laser.Agdx) *laser.AgdxSend {
			return agdx.Command(correlationID(2), []byte("{}")).
				WithTarget(agentID(t, "executor"))
		}, 2},
		{"a reply to its requester", func(agdx *laser.Agdx) *laser.AgdxSend {
			return agdx.Respond(correlationID(2), []byte("{}")).
				WithTarget(agentID(t, "planner"))
		}, 0},
		{"a command to an undeclared agent", func(agdx *laser.Agdx) *laser.AgdxSend {
			return agdx.Command(correlationID(2), []byte("{}")).
				WithTarget(agentID(t, "stranger"))
		}, -1},
		{"a status nobody is addressed by", func(agdx *laser.Agdx) *laser.AgdxSend {
			return agdx.Emit([]byte("{}")).WithTarget(agentID(t, "executor"))
		}, -1},
	} {
		t.Run(probe.name, func(t *testing.T) {
			server := &routingServer{}
			handle := layoutHandle(t, server, partitionLayout(t))
			agdx := handle.Agdx(agentID(t, "planner"), conversation(1))
			if _, err := probe.send(agdx).Send(context.Background()); err != nil {
				t.Fatalf("send: %v", err)
			}
			landed := server.landed(t)
			// A per-agent partition layout never moves a record off the lane.
			if landed.topic != wire.AgentSessions {
				t.Fatalf("landed on %q, want the lane", landed.topic)
			}
			if probe.partition < 0 {
				if landed.key(t) != conversation(1).String() {
					t.Fatalf("want the session's own partition, got %q", landed.key(t))
				}
				return
			}
			if got := landed.partition(t); got != uint32(probe.partition) {
				t.Fatalf("landed on partition %d, want %d", got, probe.partition)
			}
		})
	}
}

func TestGivenADeclaredTopicLayoutWhenSendingARecordWithNoEnvelopeThenShouldRouteByItsAddressee(t *testing.T) {
	server := &routingServer{}
	handle := layoutHandle(t, server, inboxLayout(t))
	target := agentID(t, "executor")
	provenance := &laser.Provenance{
		Conversation: conversation(1),
		Agent:        ptrTo(agentID(t, "planner")),
		TargetAgent:  &target,
	}

	if _, err := handle.SendAgent(
		context.Background(), wire.AgentSessions, []byte("payload"), provenance); err != nil {
		t.Fatalf("send: %v", err)
	}

	if landed := server.landed(t); landed.topic != "executor.inbox" {
		t.Fatalf("landed on %q, want executor.inbox", landed.topic)
	}
}

func TestGivenADeclaredTopicLayoutWhenAskedWhereToReadThenShouldNameTheAgentsTopic(t *testing.T) {
	server := &routingServer{}
	handle := layoutHandle(t, server, inboxLayout(t))

	if got := handle.DeclaredTopic(agentID(t, "executor")); got != "executor.inbox" {
		t.Fatalf("got %q, want executor.inbox", got)
	}
	if got := handle.DeclaredTopic(agentID(t, "stranger")); got != "" {
		t.Fatalf("an undeclared agent reads the lane, got %q", got)
	}
	plain := streamHandle(server, laser.Options{DefaultStream: "agents"})
	if got := plain.DeclaredTopic(agentID(t, "executor")); got != "" {
		t.Fatalf("no layout declares nothing, got %q", got)
	}
}

func TestGivenALayoutWhenInspectedThenShouldDescribeItself(t *testing.T) {
	if got := (laser.SessionLayout{}).Kind(); got != laser.LayoutShared {
		t.Fatalf("the zero layout is the shared one, got %q", got)
	}
	if got := laser.SharedLayout().Kind(); got != laser.LayoutShared {
		t.Fatalf("got %q", got)
	}
	if got := laser.SinglePartitionLayout().Kind(); got != laser.LayoutSinglePartition {
		t.Fatalf("got %q", got)
	}
	topics := inboxLayout(t)
	if got := topics.Topics(); len(got) != 2 || got[agentID(t, "planner")] != "planner.inbox" {
		t.Fatalf("got %v", got)
	}
	if len(topics.Partitions()) != 0 {
		t.Fatal("a topic layout declares no partitions")
	}
	partitions := partitionLayout(t)
	if got := partitions.Partitions(); len(got) != 2 || got[agentID(t, "executor")] != 2 {
		t.Fatalf("got %v", got)
	}
	if len(partitions.Topics()) != 0 {
		t.Fatal("a partition layout declares no topics")
	}
	// The maps handed out are copies, so a caller cannot reroute a declared
	// layout after the fact.
	topics.Topics()[agentID(t, "planner")] = "elsewhere"
	if topics.Topics()[agentID(t, "planner")] != "planner.inbox" {
		t.Fatal("a declared layout must not be reachable through its copy")
	}
}

func TestGivenALayoutThisClientCannotRouteUnderWhenValidatedThenShouldRefuseIt(t *testing.T) {
	for _, probe := range []struct {
		name   string
		layout laser.SessionLayout
	}{
		{"a topic layout declaring the lane", laser.PerAgentTopicLayout(
			map[wire.AgentID]string{agentID(t, "planner"): wire.AgentSessions})},
		{"a topic layout declaring the control topic", laser.PerAgentTopicLayout(
			map[wire.AgentID]string{agentID(t, "planner"): wire.AgentControl})},
		{"a topic layout with an unnamed topic", laser.PerAgentTopicLayout(
			map[wire.AgentID]string{agentID(t, "planner"): ""})},
		{"a topic layout with an unnamed agent", laser.PerAgentTopicLayout(
			map[wire.AgentID]string{{}: "planner.inbox"})},
		{"a partition layout with an unnamed agent", laser.PerAgentPartitionLayout(
			map[wire.AgentID]uint32{{}: 1})},
	} {
		t.Run(probe.name, func(t *testing.T) {
			if err := probe.layout.Validate(); err == nil {
				t.Fatal("the layout must be refused")
			}
		})
	}
	if err := inboxLayout(t).Validate(); err != nil {
		t.Fatalf("a declared layout validates: %v", err)
	}
	if err := partitionLayout(t).Validate(); err != nil {
		t.Fatalf("a partition layout validates: %v", err)
	}
	if err := (laser.SessionLayout{}).Validate(); err != nil {
		t.Fatalf("the zero layout validates: %v", err)
	}
}

func TestGivenADeclaredTopicLayoutWhenListedThenShouldNameEachTopicOnce(t *testing.T) {
	shared := laser.PerAgentTopicLayout(map[wire.AgentID]string{
		agentID(t, "planner"):  "shared.inbox",
		agentID(t, "executor"): "shared.inbox",
		agentID(t, "auditor"):  "auditor.inbox",
	})
	declared, err := shared.DeclaredTopics()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(declared) != 2 || declared[0] != "auditor.inbox" || declared[1] != "shared.inbox" {
		t.Fatalf("got %v, want each topic once in name order", declared)
	}
	for _, layout := range []laser.SessionLayout{
		laser.SharedLayout(), laser.SinglePartitionLayout(), partitionLayout(t),
	} {
		names, err := layout.DeclaredTopics()
		if err != nil || len(names) != 0 {
			t.Fatalf("%s declares no topics, got %v %v", layout.Kind(), names, err)
		}
	}
}

func TestGivenADeclaredTopicLayoutWhenBootstrappedThenShouldCreateEachTopic(t *testing.T) {
	server := &routingServer{}
	handle := streamHandle(server, laser.Options{DefaultStream: "agents"})
	sessions := handle.SessionsWith(laser.SessionConfig{Layout: inboxLayout(t)})

	report, err := sessions.Bootstrap(context.Background(), 4, laser.ExpireAfter(time.Hour))
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if report.Registered {
		t.Fatal("a deployment that does not index sessions registers nothing")
	}
	created := map[string]bool{}
	for _, name := range server.created {
		created[name] = true
	}
	for _, want := range []string{
		wire.AgentSessions, wire.AgentHeartbeats, "executor.inbox", "planner.inbox",
	} {
		if !created[want] {
			t.Fatalf("%s was not created, got %v", want, server.created)
		}
	}
}

func TestGivenASinglePartitionLayoutWhenBootstrappedThenShouldCreateOnePartition(t *testing.T) {
	server := &routingServer{}
	handle := streamHandle(server, laser.Options{DefaultStream: "agents"})
	sessions := handle.SessionsWith(
		laser.SessionConfig{Layout: laser.SinglePartitionLayout()})

	if _, err := sessions.Bootstrap(
		context.Background(), 8, laser.ExpireAfter(time.Hour)); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	// The layout's whole point is that each topic has exactly one partition,
	// whatever partition count the caller asked for.
	if len(server.created) == 0 {
		t.Fatal("nothing was created")
	}
	for index, name := range server.created {
		options := server.options[index]
		if options == nil || options.PartitionsCount == nil {
			t.Fatalf("%s was created without a partition count", name)
		}
		if *options.PartitionsCount != 1 {
			t.Fatalf("%s got %d partitions, want 1", name, *options.PartitionsCount)
		}
	}
}

func TestGivenADeclaredTopicLayoutWhenBootstrappedThenShouldGiveEachTopicTheLanesShape(t *testing.T) {
	server := &routingServer{}
	handle := streamHandle(server, laser.Options{DefaultStream: "agents"})
	sessions := handle.SessionsWith(laser.SessionConfig{Layout: inboxLayout(t)})

	if _, err := sessions.Bootstrap(
		context.Background(), 4, laser.ExpireAfter(time.Hour)); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	// A declared topic carries a share of the lane's work, so it takes the
	// lane's partition count and its retention rather than the defaults.
	for index, name := range server.created {
		if name != "planner.inbox" && name != "executor.inbox" {
			continue
		}
		options := server.options[index]
		if options == nil || options.PartitionsCount == nil ||
			*options.PartitionsCount != 4 {
			t.Fatalf("%s did not take the lane's partition count: %+v", name, options)
		}
		if options.MessageExpiry == nil ||
			*options.MessageExpiry != iggcon.Duration(time.Hour.Microseconds()) {
			t.Fatalf("%s did not take the lane's retention: %+v", name, options)
		}
	}
}

func TestGivenALayoutThisClientCannotRouteUnderWhenBootstrappedThenShouldRefuseIt(t *testing.T) {
	server := &routingServer{}
	handle := streamHandle(server, laser.Options{DefaultStream: "agents"})
	sessions := handle.SessionsWith(laser.SessionConfig{
		Layout: laser.PerAgentTopicLayout(
			map[wire.AgentID]string{agentID(t, "planner"): wire.AgentSessions}),
	})

	if _, err := sessions.Bootstrap(
		context.Background(), 4, laser.ExpireAfter(time.Hour)); err == nil {
		t.Fatal("a layout that declares the lane must be refused")
	}
	if len(server.created) != 0 {
		t.Fatalf("nothing must be created, got %v", server.created)
	}
}

func TestGivenNoSessionIndexWhenUnregisteringThenShouldSayTheSurfaceIsUnserved(t *testing.T) {
	server := &routingServer{}
	handle := streamHandle(server, laser.Options{DefaultStream: "agents"})
	if err := handle.Sessions().Unregister(context.Background()); err == nil {
		t.Fatal("a deployment that does not index sessions serves no unregister")
	}
}

func ptrTo[T any](value T) *T { return &value }

func TestGivenADeclaredLayoutWhenSendingOffTheLaneThenShouldLeaveTheRecordWhereItWas(t *testing.T) {
	// A layout routes the session lane and nothing else. A record on another
	// agent topic keeps its topic and its key whatever the layout declares,
	// because that topic's readers are not the ones the layout split up.
	for _, probe := range []struct {
		name   string
		layout laser.SessionLayout
	}{
		{"a topic layout", inboxLayout(t)},
		{"a partition layout", partitionLayout(t)},
	} {
		t.Run(probe.name, func(t *testing.T) {
			server := &routingServer{}
			handle := layoutHandle(t, server, probe.layout)
			agdx := handle.AgdxOn(
				"agents", wire.AgentStreams, agentID(t, "planner"), conversation(1))
			if _, err := agdx.Command(correlationID(2), []byte("{}")).
				WithTarget(agentID(t, "executor")).Send(context.Background()); err != nil {
				t.Fatalf("send: %v", err)
			}
			landed := server.landed(t)
			if landed.topic != wire.AgentStreams {
				t.Fatalf("landed on %q, want %q", landed.topic, wire.AgentStreams)
			}
			if landed.key(t) != conversation(1).String() {
				t.Fatalf("want the conversation as the key, got %q", landed.key(t))
			}
		})
	}
}

func TestGivenAnUnnamedAgentWhenBootstrappedThenShouldRefuseTheLayout(t *testing.T) {
	// A partition layout declares no topics, so only the layout check itself
	// can catch an agent it never named.
	server := &routingServer{}
	handle := streamHandle(server, laser.Options{DefaultStream: "agents"})
	sessions := handle.SessionsWith(laser.SessionConfig{
		Layout: laser.PerAgentPartitionLayout(map[wire.AgentID]uint32{{}: 1}),
	})

	if _, err := sessions.Bootstrap(
		context.Background(), 4, laser.ExpireAfter(time.Hour)); err == nil {
		t.Fatal("a layout that names no agent must be refused")
	}
	if len(server.created) != 0 {
		t.Fatalf("nothing must be created, got %v", server.created)
	}
}

func TestGivenNoSessionIndexWhenBootstrappedThenShouldRegisterNothing(t *testing.T) {
	// The deployment serves the managed band but does not index sessions, so
	// a registration would be a control record nothing folds.
	server := &routingServer{}
	handle := streamHandle(server, laser.Options{
		DefaultStream: "agents",
		Capabilities:  &laser.Capabilities{Managed: true},
	})
	sessions := handle.Sessions()

	report, err := sessions.Bootstrap(
		context.Background(), 2, laser.ExpireAfter(time.Hour))
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if report.Registered {
		t.Fatal("a deployment that does not index sessions registers nothing")
	}
	if codes := server.managedCodes(); len(codes) != 0 {
		t.Fatalf("no control record must be published, got codes %v", codes)
	}
}

func TestGivenNoSessionIndexWhenUnregisteringThenShouldNameTheUnservedSurface(t *testing.T) {
	server := &routingServer{}
	handle := streamHandle(server, laser.Options{
		DefaultStream: "agents",
		Capabilities:  &laser.Capabilities{Managed: true},
	})

	err := handle.Sessions().Unregister(context.Background())
	if err == nil || !strings.Contains(err.Error(), "sessions") {
		t.Fatalf("got %v, want the sessions surface named", err)
	}
	if codes := server.managedCodes(); len(codes) != 0 {
		t.Fatalf("no control record must be published, got codes %v", codes)
	}
}

func TestGivenSessionsOnAnotherStreamWhenBootstrappedThenShouldBuildThatStream(t *testing.T) {
	server := &routingServer{}
	handle := streamHandle(server, laser.Options{DefaultStream: "agents"})
	sessions := handle.SessionsWith(laser.SessionConfig{Stream: "other"})

	if _, err := sessions.Bootstrap(
		context.Background(), 2, laser.ExpireAfter(time.Hour)); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if len(server.createdOn) == 0 {
		t.Fatal("nothing was created")
	}
	for index, stream := range server.createdOn {
		if stream != "other" {
			t.Fatalf("%s was created on %q, want other", server.created[index], stream)
		}
	}
}

func TestGivenAPartitionLayoutWhenItsMapIsInspectedThenShouldHandOutACopy(t *testing.T) {
	layout := partitionLayout(t)
	layout.Partitions()[agentID(t, "executor")] = 99
	if layout.Partitions()[agentID(t, "executor")] != 2 {
		t.Fatal("a declared layout must not be reachable through its copy")
	}
}
