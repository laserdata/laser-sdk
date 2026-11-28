package laser_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// groupServer answers the consumer group verbs and records what was asked.
//
// It holds groups by name, which is how the server keys them, and hands out
// ids in creation order so a test can address one group both ways and check
// that both reach the same group.
type groupServer struct {
	clusterOfOne
	mutex sync.Mutex

	groups  map[string]*iggcon.ConsumerGroupDetails
	nextID  uint32
	created []string
	deleted []string
	// stream and topic are the incarnations an identity is read from. A nil
	// one reads as a resource that is not there.
	stream *iggcon.StreamDetails
	topic  *iggcon.TopicDetails
	// fail is returned by every verb when set, for the transport failure path.
	fail error
}

func newGroupServer() *groupServer {
	return &groupServer{
		groups: map[string]*iggcon.ConsumerGroupDetails{},
		nextID: 1,
		stream: &iggcon.StreamDetails{
			Stream: iggcon.Stream{Id: 7, Name: "orders", CreatedAt: 1_000},
		},
		topic: &iggcon.TopicDetails{
			Topic: iggcon.Topic{Id: 3, Name: "events", CreatedAt: 2_000},
		},
	}
}

func (server *groupServer) Close() error { return nil }

func (server *groupServer) GetStream(
	_ context.Context, _ iggcon.Identifier,
) (*iggcon.StreamDetails, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.fail != nil {
		return nil, server.fail
	}
	if server.stream == nil {
		return nil, ierror.StreamNameNotFound{}
	}
	return server.stream, nil
}

func (server *groupServer) GetTopic(
	_ context.Context, _ iggcon.Identifier, _ iggcon.Identifier,
) (*iggcon.TopicDetails, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.fail != nil {
		return nil, server.fail
	}
	if server.topic == nil {
		return nil, ierror.TopicNameNotFound{}
	}
	return server.topic, nil
}

func (server *groupServer) CreateConsumerGroup(
	_ context.Context, _ iggcon.Identifier, _ iggcon.Identifier, name string,
) (*iggcon.ConsumerGroupDetails, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.fail != nil {
		return nil, server.fail
	}
	server.created = append(server.created, name)
	if _, found := server.groups[name]; found {
		return nil, ierror.ConsumerGroupNameAlreadyExists{}
	}
	details := &iggcon.ConsumerGroupDetails{
		ConsumerGroup: iggcon.ConsumerGroup{
			Id: server.nextID, Name: name, PartitionsCount: 4, MembersCount: 0,
		},
	}
	server.nextID++
	server.groups[name] = details
	return details, nil
}

func (server *groupServer) GetConsumerGroup(
	_ context.Context, _ iggcon.Identifier, _ iggcon.Identifier, group iggcon.Identifier,
) (*iggcon.ConsumerGroupDetails, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.fail != nil {
		return nil, server.fail
	}
	found := server.lookup(group)
	if found == nil {
		return nil, ierror.ConsumerGroupNameNotFound{}
	}
	return found, nil
}

func (server *groupServer) DeleteConsumerGroup(
	_ context.Context, _ iggcon.Identifier, _ iggcon.Identifier, group iggcon.Identifier,
) error {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.fail != nil {
		return server.fail
	}
	found := server.lookup(group)
	if found == nil {
		return ierror.ConsumerGroupNameNotFound{}
	}
	server.deleted = append(server.deleted, found.Name)
	delete(server.groups, found.Name)
	return nil
}

// lookup finds a group by whichever form the identifier carries.
func (server *groupServer) lookup(group iggcon.Identifier) *iggcon.ConsumerGroupDetails {
	if name, err := group.String(); err == nil {
		return server.groups[name]
	}
	id, err := group.Uint32()
	if err != nil {
		return nil
	}
	for _, details := range server.groups {
		if details.Id == id {
			return details
		}
	}
	return nil
}

// groupTopic is a topic on a client wired to server.
func groupTopic(server *groupServer) *laser.Topic {
	client := laser.FromClient(server, laser.Options{DefaultStream: "orders"})
	return client.Topic("events")
}

func TestGivenANameWhenAddressingAGroupThenShouldCarryTheNameAndNoID(t *testing.T) {
	t.Parallel()
	group := groupTopic(newGroupServer()).Group("workers")

	name, byName := group.Name()
	if !byName || name != "workers" {
		t.Fatalf("a named handle must carry its name, got %q (%v)", name, byName)
	}
	if _, byID := group.ID(); byID {
		t.Error("a named handle must carry no numeric id: the server assigns that")
	}
	if group.Topic().Name() != "events" {
		t.Errorf("the handle lost its topic, got %q", group.Topic().Name())
	}
}

func TestGivenANumericIDWhenAddressingAGroupThenShouldCarryTheIDAndNoName(t *testing.T) {
	t.Parallel()
	group := groupTopic(newGroupServer()).GroupByID(9)

	id, byID := group.ID()
	if !byID || id != 9 {
		t.Fatalf("a numeric handle must carry its id, got %d (%v)", id, byID)
	}
	if _, byName := group.Name(); byName {
		t.Error("a numeric handle must carry no name until it reads one")
	}
}

func TestGivenAnAbsentGroupWhenEnsuredThenShouldCreateItOnce(t *testing.T) {
	t.Parallel()
	server := newGroupServer()
	group := groupTopic(server).Group("workers")

	if err := group.EnsureNative(context.Background()); err != nil {
		t.Fatalf("create the group: %v", err)
	}
	// Repeating must leave the first group alone. A create that failed on the
	// name already being taken is the outcome the caller asked for.
	if err := group.EnsureNative(context.Background()); err != nil {
		t.Fatalf("ensure must be repeatable, the second call failed: %v", err)
	}
	if len(server.created) != 2 {
		t.Fatalf("ensure must ask the server each time, got %v", server.created)
	}
	if len(server.groups) != 1 {
		t.Errorf("ensure must not create a second group, got %d", len(server.groups))
	}
}

func TestGivenANumericHandleWhenEnsuredThenShouldRefuseIt(t *testing.T) {
	t.Parallel()
	group := groupTopic(newGroupServer()).GroupByID(4)

	err := group.EnsureNative(context.Background())
	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("creating by id must be refused, got %v", err)
	}
	// The remedy is asserted for the same reason as in the join above: without
	// the guard the handle creates a group under an empty name, which fails as
	// invalid too, so the error type alone does not tell the two apart.
	if !strings.Contains(err.Error(), "Topic.Group") {
		t.Errorf("the refusal must name the remedy, got %v", err)
	}
}

func TestGivenAGroupWhenReadingItsInfoThenShouldCarryItsExactIdentity(t *testing.T) {
	t.Parallel()
	server := newGroupServer()
	group := groupTopic(server).Group("workers")
	if err := group.EnsureNative(context.Background()); err != nil {
		t.Fatalf("create the group: %v", err)
	}

	info, err := group.Info(context.Background())
	if err != nil {
		t.Fatalf("read the group: %v", err)
	}
	if info.Name != "workers" || info.ID != 1 {
		t.Fatalf("the group read back as %q (#%d)", info.Name, info.ID)
	}
	if info.PartitionsCount != 4 {
		t.Errorf("the group's partition count read as %d", info.PartitionsCount)
	}
	// The identity is what stops a recreated group inheriting an old policy,
	// so every component of it has to come from the server rather than a
	// default.
	want := wire.FilterGroupIdentity{
		StreamID: 7, StreamCreatedAtMicros: 1_000,
		TopicID: 3, TopicCreatedAtMicros: 2_000,
		GroupID: 1,
	}
	if info.Identity != want {
		t.Errorf("the identity read as %+v, want %+v", info.Identity, want)
	}
	// This deployment serves no filter catalog, so there is nothing that could
	// have configured a policy and the binding must read as absent.
	if info.Filter != nil {
		t.Errorf("a group on a catalogless deployment must run no policy, got %+v",
			info.Filter)
	}
}

func TestGivenAGroupAddressedByIDWhenReadingItsInfoThenShouldFindTheSameGroup(t *testing.T) {
	t.Parallel()
	server := newGroupServer()
	topic := groupTopic(server)
	if err := topic.Group("workers").EnsureNative(context.Background()); err != nil {
		t.Fatalf("create the group: %v", err)
	}

	info, err := topic.GroupByID(1).Info(context.Background())
	if err != nil {
		t.Fatalf("read the group by id: %v", err)
	}
	if info.Name != "workers" {
		t.Errorf("addressing by id reached %q rather than the group created", info.Name)
	}
}

func TestGivenANumericHandleWhenNamingTheGroupThenShouldReadTheNameFromTheServer(t *testing.T) {
	t.Parallel()
	server := newGroupServer()
	topic := groupTopic(server)
	if err := topic.Group("workers").EnsureNative(context.Background()); err != nil {
		t.Fatalf("create the group: %v", err)
	}

	// The catalog keys a binding by name, so a numeric handle has to resolve
	// one before it can address a policy at all.
	ref, err := topic.GroupByID(1).GroupRef(context.Background())
	if err != nil {
		t.Fatalf("resolve the group reference: %v", err)
	}
	want := wire.FilterGroupRef{Stream: "orders", Topic: "events", Group: "workers"}
	if ref != want {
		t.Errorf("the group reference read as %+v, want %+v", ref, want)
	}
}

func TestGivenAnAbsentGroupWhenReadingItsInfoThenShouldReportItNotFound(t *testing.T) {
	t.Parallel()
	group := groupTopic(newGroupServer()).Group("nobody")

	_, err := group.Info(context.Background())
	var failure *laser.FilterError
	if !errors.As(err, &failure) || !failure.NotFound() {
		t.Fatalf("an absent group must read as not found, got %v", err)
	}
}

func TestGivenAGroupWhenDeletedThenShouldReportWhetherOneWasThere(t *testing.T) {
	t.Parallel()
	server := newGroupServer()
	group := groupTopic(server).Group("workers")
	if err := group.EnsureNative(context.Background()); err != nil {
		t.Fatalf("create the group: %v", err)
	}

	removed, err := group.Delete(context.Background())
	if err != nil {
		t.Fatalf("delete the group: %v", err)
	}
	if !removed {
		t.Fatal("deleting an existing group must report that one was removed")
	}
	// A group already gone is an absence rather than a failure, which is what
	// lets a deferred cleanup run after an explicit delete.
	removed, err = group.Delete(context.Background())
	if err != nil {
		t.Fatalf("deleting an absent group must not fail: %v", err)
	}
	if removed {
		t.Error("deleting an absent group must report that nothing was removed")
	}
	if len(server.deleted) != 1 {
		t.Errorf("the server was asked to delete %v", server.deleted)
	}
}

func TestGivenACreateWithoutAPolicyWhenBuiltThenShouldReturnTheGroup(t *testing.T) {
	t.Parallel()
	server := newGroupServer()
	group := groupTopic(server).Group("workers")

	info, err := group.Create().Build(context.Background())
	if err != nil {
		t.Fatalf("build the group: %v", err)
	}
	if info.Name != "workers" || info.ID != 1 {
		t.Fatalf("the group built as %q (#%d)", info.Name, info.ID)
	}
	if info.Filter != nil {
		t.Errorf("a group built with no policy must run none, got %+v", info.Filter)
	}
}

func TestGivenACreateWithAPolicyWhenTheDeploymentServesNoCatalogThenShouldRefuseBeforeCreating(t *testing.T) {
	t.Parallel()
	server := newGroupServer()
	group := groupTopic(server).Group("workers")

	_, err := group.Create().
		Filter("urgent", wire.ConsumerFilter{}).
		Build(context.Background())

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("a policy on a catalogless deployment must be refused, got %v", err)
	}
	// Refusing before the native group is created is the point: creating it
	// and failing afterwards would strand a group the caller then has to
	// clean up under a name it did not choose to keep.
	if len(server.created) != 0 {
		t.Errorf("nothing must be created when the policy cannot be served, got %v",
			server.created)
	}
}

func TestGivenABuiltGroupWhenRebuiltThenShouldKeepTheFirstGroup(t *testing.T) {
	t.Parallel()
	server := newGroupServer()
	group := groupTopic(server).Group("workers")

	first, err := group.Create().Build(context.Background())
	if err != nil {
		t.Fatalf("build the group: %v", err)
	}
	second, err := group.Create().Build(context.Background())
	if err != nil {
		t.Fatalf("rebuild the group: %v", err)
	}
	if first.ID != second.ID {
		t.Errorf("rebuilding must reach the same group, got #%d then #%d",
			first.ID, second.ID)
	}
}

func TestGivenAHandleThatConfiguredNothingWhenReadThenShouldCarryNoCatalogPosition(t *testing.T) {
	t.Parallel()
	group := groupTopic(newGroupServer()).Group("workers")

	if group.CatalogPosition() != nil {
		t.Error("a handle that configured nothing must carry no position to read through")
	}
}

func TestGivenAFailingServerWhenReadingAGroupThenShouldReportTheTransportFailure(t *testing.T) {
	t.Parallel()
	server := newGroupServer()
	server.fail = errors.New("the socket went away")
	group := groupTopic(server).Group("workers")

	_, err := group.Info(context.Background())
	var transport *laser.TransportError
	if !errors.As(err, &transport) {
		t.Fatalf("a failed read must surface as a transport failure, got %v", err)
	}
}

func TestGivenNoStreamWhenAddressingAGroupThenShouldRefuseWithTheRemedy(t *testing.T) {
	t.Parallel()
	client := laser.FromClient(newGroupServer(), laser.Options{})
	group := client.Topic("events").Group("workers")

	_, err := group.GroupRef(context.Background())
	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("a group with no stream must be refused with the remedy, got %v", err)
	}
}

func TestGivenANamedGroupWhenSelectingItThenShouldFollowItByName(t *testing.T) {
	t.Parallel()
	selector := groupTopic(newGroupServer()).Group("workers").Selector()

	if selector.Kind != wire.ConsumerGroupName || selector.Name != "workers" {
		t.Fatalf("a named group must be followed by name, got %+v", selector)
	}
	if !selector.IsGroup() {
		t.Error("a group selector must read as a group")
	}
}

func TestGivenANumericGroupWhenSelectingItThenShouldFollowItByID(t *testing.T) {
	t.Parallel()
	// A numeric handle selects by id rather than resolving a name, because
	// resolving would cost a round trip on every filtered read.
	selector := groupTopic(newGroupServer()).GroupByID(12).Selector()

	if selector.Kind != wire.ConsumerGroupID || selector.ID != 12 {
		t.Fatalf("a numeric group must be followed by id, got %+v", selector)
	}
}

func TestGivenANumericHandleWhenJoiningAsAMemberThenShouldUseItsID(t *testing.T) {
	t.Parallel()
	server := &consumerServer{}
	group := streamHandle(server, laser.Options{DefaultStream: "orders"}).Topic("events").GroupByID(3)
	consumer, err := group.Consumer(context.Background(), laser.ConsumerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close(context.Background())
	server.mutex.Lock()
	defer server.mutex.Unlock()
	id, err := server.joinedIdentifier.Uint32()
	if err != nil || id != 3 || len(server.created) != 0 {
		t.Fatalf("wanted to join group 3, got id %d, %v, created %v", id, err, server.created)
	}
}

func TestGivenAnInjectedClientWhenJoiningAGroupThenShouldProbeAndJoin(t *testing.T) {
	server := &consumerServer{}
	group := laser.FromClient(server, laser.Options{DefaultStream: "agents"}).Topic("work").Group("workers")
	consumer, err := group.Consumer(context.Background(), laser.ConsumerOptions{})
	if err != nil {
		t.Fatalf("an injected client on Apache Iggy must join natively: %v", err)
	}
	defer consumer.Close(context.Background())
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.joined != "workers" || len(server.created) != 1 {
		t.Fatalf("joined %q, created %v", server.joined, server.created)
	}
}

// probeFailingServer cannot answer the capability probe.
type probeFailingServer struct{ consumerServer }

func (*probeFailingServer) SendBinaryRequest(context.Context, uint32, []byte) ([]byte, error) {
	return nil, errors.New("connection reset")
}

func TestGivenAProbeThatFailsWhenJoiningAGroupThenShouldRefuseBeforeJoining(t *testing.T) {
	server := &probeFailingServer{}
	group := laser.FromClient(server, laser.Options{DefaultStream: "agents"}).Topic("work").Group("workers")
	if _, err := group.Consumer(context.Background(), laser.ConsumerOptions{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a probe that established nothing must refuse, got %v", err)
	}
	if len(server.created) != 0 || server.joined != "" {
		t.Fatal("created or joined a group without knowing its policy capability")
	}
}

func TestGivenNoCatalogWhenReadingAPolicyVerbThenShouldNameTheMissingBackend(t *testing.T) {
	t.Parallel()
	server := newGroupServer()
	group := groupTopic(server).Group("workers")
	if err := group.EnsureNative(context.Background()); err != nil {
		t.Fatalf("create the group: %v", err)
	}

	// A policy verb on a deployment with no filter catalog must say that is
	// what is missing, rather than reporting the group as unpolicied. The two
	// are different: one is a deployment that cannot hold a policy, the other
	// a group that was never given one, and only the first is worth changing
	// the deployment over.
	_, err := group.Filter().Revisions(context.Background(), 1, 10)
	if err == nil {
		t.Fatal("a policy verb without a catalog must be refused")
	}
	if !strings.Contains(err.Error(), "managed backend") {
		t.Errorf("the failure must name the missing backend, got %v", err)
	}
}

func TestGivenAGroupWithNoOwnFilterWhenDeletingItThenShouldReportNothingRemoved(t *testing.T) {
	t.Parallel()
	server := newGroupServer()
	group := groupTopic(server).Group("workers")
	if err := group.EnsureNative(context.Background()); err != nil {
		t.Fatalf("create the group: %v", err)
	}

	// Without a catalog there is no own filter to find, so the delete is a
	// no-op rather than a failure: nothing was there to remove.
	removed, err := group.Filter().Delete(context.Background())
	if err == nil && removed {
		t.Fatal("a group with no own filter must report that nothing was removed")
	}
}
