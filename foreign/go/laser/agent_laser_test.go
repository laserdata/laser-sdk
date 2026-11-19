package laser_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// topologyServer records the streams and topics a bootstrap created.
type topologyServer struct {
	iggcon.Client
	guard    sync.Mutex
	streams  []string
	topics   []string
	options  []*iggcon.TopicOptions
	existing map[string]bool
}

func newTopology() *topologyServer {
	return &topologyServer{existing: map[string]bool{}}
}

func (server *topologyServer) Close() error { return nil }

func (server *topologyServer) GetStream(
	_ context.Context, _ iggcon.Identifier,
) (*iggcon.StreamDetails, error) {
	server.guard.Lock()
	defer server.guard.Unlock()
	if !server.existing["stream"] {
		return nil, ierror.StreamIdNotFound{}
	}
	return &iggcon.StreamDetails{Stream: iggcon.Stream{Name: "agents"}}, nil
}

func (server *topologyServer) CreateStream(
	_ context.Context, name string, _ *iggcon.StreamOptions,
) (*iggcon.StreamDetails, error) {
	server.guard.Lock()
	defer server.guard.Unlock()
	server.streams = append(server.streams, name)
	server.existing["stream"] = true
	return &iggcon.StreamDetails{Stream: iggcon.Stream{Name: name}}, nil
}

func (server *topologyServer) GetTopic(
	_ context.Context, _ iggcon.Identifier, topic iggcon.Identifier,
) (*iggcon.TopicDetails, error) {
	named, err := topic.String()
	if err != nil {
		return nil, err
	}
	server.guard.Lock()
	defer server.guard.Unlock()
	if !server.existing[named] {
		return nil, ierror.TopicIdNotFound{}
	}
	return &iggcon.TopicDetails{Topic: iggcon.Topic{Name: named}}, nil
}

func (server *topologyServer) CreateTopic(
	_ context.Context, _ iggcon.Identifier, name string,
	options *iggcon.TopicOptions,
) (*iggcon.TopicDetails, error) {
	server.guard.Lock()
	defer server.guard.Unlock()
	server.topics = append(server.topics, name)
	server.options = append(server.options, options)
	server.existing[name] = true
	return &iggcon.TopicDetails{Topic: iggcon.Topic{Name: name}}, nil
}

// created is every topic the bootstrap created, with the options it used.
func (server *topologyServer) created() ([]string, []*iggcon.TopicOptions) {
	server.guard.Lock()
	defer server.guard.Unlock()
	return append([]string(nil), server.topics...),
		append([]*iggcon.TopicOptions(nil), server.options...)
}

func topologyHandle(server iggcon.Client) *laser.Laser {
	return laser.FromClient(server, laser.Options{DefaultStream: "agents"})
}

// A bootstrap creates the whole agent topology, so an application does not
// have to know which topics the runtime needs.
func TestGivenAStreamWhenBootstrappedThenShouldCreateTheAgentTopology(t *testing.T) {
	server := newTopology()

	if err := topologyHandle(server).Bootstrap(
		context.Background(), 4, laser.ExpireAfter(24*time.Hour)); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	topics, options := server.created()
	want := map[string]bool{
		wire.AgentSessions: false, wire.AgentHeartbeats: false,
		wire.AgentStreams: false, wire.AgentMemory: false,
		wire.AgentDLQ: false, wire.AgentAudit: false, wire.AgentJournal: false,
	}
	for _, topic := range topics {
		if _, expected := want[topic]; !expected {
			t.Errorf("created a topic nothing asked for: %q", topic)
			continue
		}
		want[topic] = true
	}
	for topic, created := range want {
		if !created {
			t.Errorf("want %q created", topic)
		}
	}
	// The control topic is only operators' to write, so a client creating it
	// would be claiming a privilege it does not have.
	for _, topic := range topics {
		if topic == wire.AgentControl {
			t.Error("a client must not create the control topic")
		}
	}
	// Every topic takes the partition count asked for.
	for index, option := range options {
		if option == nil || option.PartitionsCount == nil ||
			*option.PartitionsCount != 4 {
			t.Errorf("%s: want four partitions, got %+v", topics[index], option)
		}
	}
	if len(server.streams) != 1 {
		t.Errorf("want the stream created once, got %d", len(server.streams))
	}
}

// The session lane is the record of everything every agent did, so it takes
// the retention the caller chose while the heartbeats expire on their own.
func TestGivenABootstrapWhenRetainedThenShouldBoundTheLaneAndTheHeartbeats(t *testing.T) {
	server := newTopology()

	if err := topologyHandle(server).Bootstrap(
		context.Background(), 1, laser.ExpireAfter(24*time.Hour)); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	topics, options := server.created()
	retention := map[string]*iggcon.TopicOptions{}
	for index, topic := range topics {
		retention[topic] = options[index]
	}
	lane := retention[wire.AgentSessions]
	if lane == nil || lane.MessageExpiry == nil ||
		*lane.MessageExpiry != iggcon.Duration((24*time.Hour).Microseconds()) {
		t.Errorf("want the lane's chosen expiry, got %+v", lane)
	}
	// Heartbeats only prove liveness, so there is nothing to learn from an
	// old one.
	beats := retention[wire.AgentHeartbeats]
	if beats == nil || beats.MessageExpiry == nil ||
		*beats.MessageExpiry != iggcon.Duration(laser.HeartbeatExpiry.Microseconds()) {
		t.Errorf("want the heartbeats to expire after an hour, got %+v", beats)
	}
	// A satellite takes the server's own defaults.
	if memory := retention[wire.AgentMemory]; memory == nil ||
		memory.MessageExpiry != nil || memory.MaxTopicSize != nil {
		t.Errorf("want the satellites on server defaults, got %+v", memory)
	}
}

// A retention that bounds nothing lets the session lane grow until the
// deployment runs out of disk, and the first anyone hears of it is a server
// that stops accepting writes.
func TestGivenAnUnboundedRetentionWhenBootstrappedThenShouldRefuseIt(t *testing.T) {
	server := newTopology()

	err := topologyHandle(server).Bootstrap(
		context.Background(), 1, laser.TopicRetention{})

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
	if topics, _ := server.created(); len(topics) != 0 {
		t.Error("a refused bootstrap must create nothing")
	}

	// A size bound alone is a bound.
	if err := topologyHandle(newTopology()).Bootstrap(
		context.Background(), 1, laser.KeepAtMost(1<<30)); err != nil {
		t.Errorf("a size bound is a bound: %v", err)
	}
	// A retention of nothing is raised to the smallest the wire carries,
	// because expiring a record before anything reads it is not a retention.
	if laser.ExpireAfter(0).Expiry != time.Microsecond {
		t.Error("want a zero age raised to the smallest the wire carries")
	}
}

// A bootstrap is idempotent, and a topic somebody already provisioned keeps
// the retention they chose rather than having this one's imposed on it.
func TestGivenAnExistingTopologyWhenBootstrappedThenShouldLeaveItAlone(t *testing.T) {
	server := newTopology()
	handle := topologyHandle(server)
	if err := handle.Bootstrap(
		context.Background(), 1, laser.ExpireAfter(time.Hour)); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	before, _ := server.created()

	if err := handle.Bootstrap(
		context.Background(), 8, laser.ExpireAfter(time.Minute)); err != nil {
		t.Fatalf("bootstrap again: %v", err)
	}

	after, _ := server.created()
	if len(after) != len(before) {
		t.Errorf("want nothing recreated, got %d then %d", len(before), len(after))
	}
}

// A bootstrap needs a stream to put the topology on.
func TestGivenNoStreamWhenBootstrappedThenShouldRefuseIt(t *testing.T) {
	server := newTopology()
	handle := laser.FromClient(server, laser.Options{})

	if err := handle.Bootstrap(
		context.Background(), 1, laser.ExpireAfter(time.Hour)); err == nil {
		t.Error("a bootstrap without a stream must be refused")
	}
	// A count of zero is refused before anything is created, rather than
	// after the session lane was already made with no partitions.
	bare := newTopology()
	if err := topologyHandle(bare).Bootstrap(
		context.Background(), 0, laser.ExpireAfter(time.Hour)); err == nil {
		t.Error("a topic needs at least one partition")
	}
	if topics, _ := bare.created(); len(topics) != 0 {
		t.Errorf("a refused bootstrap must create nothing, got %v", topics)
	}
}

// A child conversation carries both its parent and the root, so a
// reconstruction three levels down knows what the work was for without
// walking every parent.
func TestGivenAParentWhenASubconversationIsSpawnedThenShouldCarryTheAncestry(t *testing.T) {
	handle := topologyHandle(newTopology())
	author, _ := wire.NewAgentID("planner")
	root := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}
	parent := &laser.Provenance{
		Conversation:     wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 2})},
		RootConversation: &root,
	}

	child := handle.SpawnSubconversation(parent, author)

	if child.Conversation == parent.Conversation {
		t.Error("a child is its own conversation")
	}
	if child.ParentConversation == nil ||
		*child.ParentConversation != parent.Conversation {
		t.Errorf("want the parent named, got %v", child.ParentConversation)
	}
	if child.RootConversation == nil || *child.RootConversation != root {
		t.Errorf("want the root carried through, got %v", child.RootConversation)
	}
	if child.Agent == nil || *child.Agent != author {
		t.Errorf("want the author named, got %v", child.Agent)
	}

	// A parent that is itself the root becomes the root of its children.
	orphan := &laser.Provenance{
		Conversation: wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 3})},
	}
	first := handle.SpawnSubconversation(orphan, author)
	if first.RootConversation == nil ||
		*first.RootConversation != orphan.Conversation {
		t.Errorf("want the parent as the root, got %v", first.RootConversation)
	}
}

// Every record on a shared session topic carries an addressee, so a role
// group bound to the addressee filter still receives one nobody addressed.
func TestGivenAnUnaddressedSendWhenPublishedThenShouldAddressEveryAgent(t *testing.T) {
	server := &publishServer{}
	handle := governedHandle(server)
	source, _ := wire.NewAgentID("planner")

	if _, err := handle.SendAgent(context.Background(), wire.AgentSessions,
		[]byte("work"), &laser.Provenance{
			Conversation: wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})},
			Agent:        &source,
		}); err != nil {
		t.Fatalf("send: %v", err)
	}

	published := server.published(t)
	if len(published) != 1 {
		t.Fatalf("want one record, got %d", len(published))
	}
	headers, err := iggcon.DeserializeHeaders(published[0].UserHeaders)
	if err != nil {
		t.Fatalf("read the headers: %v", err)
	}
	addressee := ""
	for _, entry := range headers {
		if string(entry.Key.Value) == wire.HeaderTargetAgentID {
			addressee = string(entry.Value.Value)
		}
	}
	if addressee != wire.BroadcastAddressee().String() {
		t.Errorf("want every agent addressed, got %q", addressee)
	}
}

// A topic other than the shared lanes keeps the caller's headers verbatim,
// because the addressee rule is the lane's rather than every topic's.
func TestGivenASendOnAnotherTopicWhenPublishedThenShouldKeepItsHeaders(t *testing.T) {
	server := &publishServer{}
	handle := governedHandle(server)
	source, _ := wire.NewAgentID("planner")

	if _, err := handle.SendAgent(context.Background(), wire.AgentMemory,
		[]byte("work"), &laser.Provenance{
			Conversation: wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})},
			Agent:        &source,
		}); err != nil {
		t.Fatalf("send: %v", err)
	}

	headers, err := iggcon.DeserializeHeaders(server.published(t)[0].UserHeaders)
	if err != nil {
		t.Fatalf("read the headers: %v", err)
	}
	for _, entry := range headers {
		if string(entry.Key.Value) == wire.HeaderTargetAgentID {
			t.Error("a satellite topic takes no addressee it was not given")
		}
	}
}

// A send without a provenance has no conversation to key by and no author to
// attribute, so it is refused rather than published as from nobody.
func TestGivenNoProvenanceWhenSendingThenShouldRefuseIt(t *testing.T) {
	handle := governedHandle(&publishServer{})

	_, err := handle.SendAgent(
		context.Background(), wire.AgentSessions, []byte("work"), nil)

	var missing *laser.NoProvenanceError
	if !errors.As(err, &missing) {
		t.Fatalf("want a NoProvenanceError, got %T: %v", err, err)
	}
}

// A send reports what it is to a policy, so a request or a memory write is
// not decided as a plain send.
func TestGivenAGovernedSendWhenNamedThenShouldReportItsKind(t *testing.T) {
	governor := &scriptedGovernor{decision: laser.Allow()}
	server := &publishServer{}
	handle := governedHandle(server).WithGovernor(governor, laser.GovernorEnforce)
	source, _ := wire.NewAgentID("planner")

	if _, err := handle.SendAgentAs(context.Background(), laser.ActionMemoryWrite,
		wire.AgentMemory, []byte("remember"), &laser.Provenance{
			Conversation: wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})},
			Agent:        &source,
		}); err != nil {
		t.Fatalf("send: %v", err)
	}

	asked := governor.asked()
	if len(asked) != 1 || asked[0].Kind != laser.ActionMemoryWrite {
		t.Fatalf("want the named kind, got %+v", asked)
	}
	if asked[0].Source != "planner" {
		t.Errorf("want the author named, got %q", asked[0].Source)
	}
}

// Readiness is about the backends an operator asked to be running: a disabled
// one says nothing about whether the deployment can serve.
func TestGivenBackendStatesWhenReadinessIsJudgedThenShouldCountOnlyTheEnabled(t *testing.T) {
	ready := wire.BackendDescriptor{
		DesiredState:  wire.BackendEnabledState,
		ObservedState: wire.BackendObservedReady,
		Readiness:     wire.BackendReadiness{Ready: true},
	}
	starting := wire.BackendDescriptor{
		DesiredState:  wire.BackendEnabledState,
		ObservedState: wire.BackendObservedStarting,
	}
	disabled := wire.BackendDescriptor{
		DesiredState:  wire.BackendDisabledState,
		ObservedState: wire.BackendObservedDisabled,
	}
	// A backend can report itself ready while the deployment observes it as
	// degraded. The observation is what a caller waiting on readiness needs:
	// the backend's own account of itself is the less reliable half.
	degraded := wire.BackendDescriptor{
		DesiredState:  wire.BackendEnabledState,
		ObservedState: wire.BackendObservedDegraded,
		Readiness:     wire.BackendReadiness{Ready: true},
	}

	for name, want := range map[string]struct {
		backends []wire.BackendDescriptor
		managed  bool
		ready    bool
	}{
		"every enabled backend serving": {
			backends: []wire.BackendDescriptor{ready}, managed: true, ready: true,
		},
		"one enabled backend starting": {
			backends: []wire.BackendDescriptor{ready, starting},
			managed:  true, ready: false,
		},
		"a disabled backend beside a ready one": {
			backends: []wire.BackendDescriptor{ready, disabled},
			managed:  true, ready: true,
		},
		"an enabled backend observed degraded": {
			backends: []wire.BackendDescriptor{ready, degraded},
			managed:  true, ready: false,
		},
		"nothing enabled at all": {
			backends: []wire.BackendDescriptor{disabled}, managed: true, ready: false,
		},
		"no managed backend": {
			backends: []wire.BackendDescriptor{ready}, managed: false, ready: false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			capabilities := laser.OpenCapabilities()
			capabilities.Managed = want.managed
			capabilities.Backends = want.backends

			if capabilities.Ready() != want.ready {
				t.Errorf("want ready %v, got %v", want.ready, capabilities.Ready())
			}
		})
	}
}

// A deployment that announces no backend at all will never become ready, so
// waiting out the bound only delays saying so.
func TestGivenNoBackendsWhenWaitingForReadinessThenShouldRefuseAtOnce(t *testing.T) {
	capabilities := laser.OpenCapabilities()
	capabilities.Managed = true
	handle := laser.FromClient(&publishServer{},
		laser.Options{Capabilities: &capabilities})

	started := time.Now()
	_, err := handle.WaitUntilReady(context.Background(), time.Hour)

	var unsupported *laser.UnsupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("want an UnsupportedError, got %T: %v", err, err)
	}
	if time.Since(started) > time.Second {
		t.Error("want it refused at once rather than waited out")
	}
}

// A deployment whose backends never come up is waited for until the bound,
// and then reported rather than waited on forever.
func TestGivenBackendsThatNeverReadyWhenWaitingThenShouldTimeOut(t *testing.T) {
	capabilities := laser.OpenCapabilities()
	capabilities.Managed = true
	capabilities.Backends = []wire.BackendDescriptor{{
		DesiredState:  wire.BackendEnabledState,
		ObservedState: wire.BackendObservedStarting,
	}}
	handle := laser.FromClient(&publishServer{},
		laser.Options{Capabilities: &capabilities})

	_, err := handle.WaitUntilReady(context.Background(), 150*time.Millisecond)

	var timeout *laser.StageTimeoutError
	if !errors.As(err, &timeout) {
		t.Fatalf("want a StageTimeoutError, got %T: %v", err, err)
	}
}

// A deployment already serving is not waited on at all.
func TestGivenAReadyDeploymentWhenWaitingThenShouldReturnAtOnce(t *testing.T) {
	capabilities := laser.OpenCapabilities()
	capabilities.Managed = true
	capabilities.Backends = []wire.BackendDescriptor{{
		DesiredState:  wire.BackendEnabledState,
		ObservedState: wire.BackendObservedReady,
		Readiness:     wire.BackendReadiness{Ready: true},
	}}
	handle := laser.FromClient(&publishServer{},
		laser.Options{Capabilities: &capabilities})

	ready, err := handle.WaitUntilReady(context.Background(), time.Hour)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if !ready.Ready() {
		t.Error("want the ready capabilities back")
	}
}

// A watcher that fell behind wants the latest state rather than a backlog of
// stale transitions.
func TestGivenConnectionEventsWhenAWatcherFallsBehindThenShouldKeepTheLatest(t *testing.T) {
	handle := topologyHandle(newTopology())
	events := handle.ConnectionEvents()
	defer events.Close()

	// More than the buffer holds, so the oldest are dropped.
	for range 64 {
		events.Report(laser.EventDisconnected)
	}
	events.Report(laser.EventConnected)

	var last laser.ConnectionEvent
	for range 64 {
		select {
		case event := <-events.Events():
			last = event
		default:
		}
	}
	if last != laser.EventConnected {
		t.Errorf("want the latest state kept, got %q", last)
	}
}

// Closing a watcher is safe more than once, and reporting to a closed one
// does nothing rather than panicking.
func TestGivenAClosedWatcherWhenReportedToThenShouldDoNothing(t *testing.T) {
	handle := topologyHandle(newTopology())
	events := handle.ConnectionEvents()

	events.Close()
	events.Close()
	events.Report(laser.EventShutdown)

	if _, open := <-events.Events(); open {
		t.Error("a closed watcher's channel is closed")
	}
}

// redriveServer holds one record and records what a redrive republished.
type redriveServer struct {
	iggcon.Client
	guard       sync.Mutex
	stream      string
	topic       string
	held        map[uint64]iggcon.IggyMessage
	republished []iggcon.IggyMessage
	keys        []iggcon.Partitioning
	missing     string
}

func newRedrive(stream, topic string) *redriveServer {
	return &redriveServer{
		stream: stream, topic: topic, held: map[uint64]iggcon.IggyMessage{},
	}
}

func (server *redriveServer) Close() error { return nil }

func (server *redriveServer) SendBinaryRequest(
	_ context.Context, _ uint32, _ []byte,
) ([]byte, error) {
	return nil, ierror.InvalidCommand{}
}

// hold puts one record at an offset, as the original the capsule names.
func (server *redriveServer) hold(
	offset uint64, payload []byte, headers []iggcon.HeaderEntry,
) {
	message, err := iggcon.NewIggyMessage(payload, iggcon.WithUserHeaders(headers))
	if err != nil {
		panic(err)
	}
	message.Header.Offset = offset
	server.guard.Lock()
	defer server.guard.Unlock()
	server.held[offset] = message
}

func (server *redriveServer) GetStream(
	_ context.Context, _ iggcon.Identifier,
) (*iggcon.StreamDetails, error) {
	if server.missing == "stream" {
		return nil, ierror.StreamIdNotFound{}
	}
	return &iggcon.StreamDetails{
		Stream: iggcon.Stream{Name: server.stream},
	}, nil
}

func (server *redriveServer) GetTopic(
	_ context.Context, _ iggcon.Identifier, _ iggcon.Identifier,
) (*iggcon.TopicDetails, error) {
	if server.missing == "topic" {
		return nil, ierror.TopicIdNotFound{}
	}
	return &iggcon.TopicDetails{Topic: iggcon.Topic{Name: server.topic}}, nil
}

func (server *redriveServer) PollMessages(
	_ context.Context, _ iggcon.Identifier, _ iggcon.Identifier, _ uint32,
	_ iggcon.Consumer, strategy iggcon.PollingStrategy, _ iggcon.PollOptions,
) (*iggcon.PolledMessage, error) {
	server.guard.Lock()
	defer server.guard.Unlock()
	held, present := server.held[strategy.Value]
	if !present {
		return &iggcon.PolledMessage{}, nil
	}
	return &iggcon.PolledMessage{Messages: []iggcon.IggyMessage{held}}, nil
}

func (server *redriveServer) SendMessages(
	_ context.Context, _ iggcon.Identifier, _ iggcon.Identifier,
	partitioning iggcon.Partitioning, messages []iggcon.IggyMessage,
) (*iggcon.SendMessagesResponse, error) {
	server.guard.Lock()
	defer server.guard.Unlock()
	server.republished = append(server.republished, messages...)
	server.keys = append(server.keys, partitioning)
	return &iggcon.SendMessagesResponse{}, nil
}

func (server *redriveServer) sent() ([]iggcon.IggyMessage, []iggcon.Partitioning) {
	server.guard.Lock()
	defer server.guard.Unlock()
	return append([]iggcon.IggyMessage(nil), server.republished...),
		append([]iggcon.Partitioning(nil), server.keys...)
}

// deadLetterHeaders is the header block a dead-lettered agent record carried:
// the envelope stamps and the spine, as a real record has them.
func deadLetterHeaders(
	t *testing.T, envelope wire.AgentEnvelope, key string,
) []iggcon.HeaderEntry {
	t.Helper()
	block := wire.HeadersForEnvelope(&envelope, wire.ContentJSON, true)
	block.IdempotencyKey = &key
	headers, err := laser.EncodeHeaderBlock(block)
	if err != nil {
		t.Fatalf("encode the headers: %v", err)
	}
	return headers
}

// deadLetteredEnvelope is an agent record on the log, with its own
// idempotency key.
func deadLetteredEnvelope(t *testing.T, key string) wire.AgentEnvelope {
	t.Helper()
	source, err := wire.NewAgentID("planner")
	if err != nil {
		t.Fatalf("agent id: %v", err)
	}
	idempotency, err := wire.NewIdempotencyKey(key)
	if err != nil {
		t.Fatalf("idempotency key: %v", err)
	}
	record := wire.RecordID{ID: wire.IDFromBytes([16]byte{15: 4})}
	return wire.AgentEnvelope{
		Kind:           wire.KindEvent,
		Record:         &record,
		Conversation:   wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})},
		Source:         source,
		IdempotencyKey: &idempotency,
		Body:           []byte("the work that failed"),
	}
}

// A dead-lettered record is replayed verbatim so a fixed handler reprocesses
// it, but re-keyed so a consumer that still remembers the original does not
// drop the copy as a duplicate.
func TestGivenADeadLetterWhenRedrivenThenShouldReplayItRekeyed(t *testing.T) {
	envelope := deadLetteredEnvelope(t, "order-7")
	payload, err := wire.Marshal(envelope)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	server := newRedrive("agents", "agent.sessions")
	server.hold(42, payload, deadLetterHeaders(t, envelope, "order-7"))
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	if err := handle.RedriveDeadLetter(context.Background(), wire.AgentDeadLetter{
		Source: wire.LogPosition{PartitionID: 3, Offset: 42},
	}); err != nil {
		t.Fatalf("redrive: %v", err)
	}

	republished, keys := server.sent()
	if len(republished) != 1 {
		t.Fatalf("want one record republished, got %d", len(republished))
	}
	var copied wire.AgentEnvelope
	if err := wire.Unmarshal(republished[0].Payload, &copied); err != nil {
		t.Fatalf("decode the copy: %v", err)
	}
	// The payload is the work itself, replayed as it was.
	if string(copied.Body) != "the work that failed" {
		t.Errorf("want the body verbatim, got %q", copied.Body)
	}
	// The key is derived from where the original sat, so two redrives of one
	// capsule agree and a redrive of another does not.
	want := "order-7/redrive/3-42"
	if copied.IdempotencyKey == nil || copied.IdempotencyKey.String() != want {
		t.Errorf("want %q, got %v", want, copied.IdempotencyKey)
	}
	// The record id is minted fresh, because the copy is a different record.
	if copied.Record == nil || *copied.Record == *envelope.Record {
		t.Error("want a freshly minted record id")
	}
	// It lands on the conversation's own partition, so the copy stays ordered
	// with the rest of it.
	if len(keys) != 1 || string(keys[0].Value) !=
		envelope.Conversation.String() {
		t.Errorf("want the conversation key, got %q", keys[0].Value)
	}
	// The header is re-keyed too, for a consumer that dedups on it.
	headers, err := iggcon.DeserializeHeaders(republished[0].UserHeaders)
	if err != nil {
		t.Fatalf("read the headers: %v", err)
	}
	for _, entry := range headers {
		if string(entry.Key.Value) == wire.HeaderIdempotencyKey &&
			string(entry.Value.Value) != want {
			t.Errorf("want the header re-keyed, got %q", entry.Value.Value)
		}
	}
}

// A capsule names its own source, and any writer that can append to the
// dead-letter topic can forge one, so a redrive is confined to this
// connection's stream.
func TestGivenACapsuleNamingAnotherStreamWhenRedrivenThenShouldRefuseIt(t *testing.T) {
	server := newRedrive("somebody-elses-stream", "agent.sessions")
	server.hold(42, []byte("work"), nil)
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	err := handle.RedriveDeadLetter(context.Background(), wire.AgentDeadLetter{
		Source: wire.LogPosition{PartitionID: 0, Offset: 42},
	})

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
	if republished, _ := server.sent(); len(republished) != 0 {
		t.Error("a refused redrive must publish nothing")
	}
}

// A source that no longer exists, or a record retention removed, has nothing
// to redrive. Saying so is better than republishing whatever sits there now.
func TestGivenAMissingSourceWhenRedrivenThenShouldSayWhatIsGone(t *testing.T) {
	for name, build := range map[string]func() *redriveServer{
		"the stream": func() *redriveServer {
			server := newRedrive("agents", "agent.sessions")
			server.missing = "stream"
			return server
		},
		"the topic": func() *redriveServer {
			server := newRedrive("agents", "agent.sessions")
			server.missing = "topic"
			return server
		},
		"the record": func() *redriveServer {
			// The stream and topic are there, the record is not.
			return newRedrive("agents", "agent.sessions")
		},
	} {
		t.Run(name, func(t *testing.T) {
			server := build()
			handle := laser.FromClient(server,
				laser.Options{DefaultStream: "agents"})

			err := handle.RedriveDeadLetter(context.Background(),
				wire.AgentDeadLetter{
					Source: wire.LogPosition{PartitionID: 0, Offset: 42},
				})

			var invalid *laser.InvalidError
			if !errors.As(err, &invalid) {
				t.Fatalf("want an InvalidError, got %T: %v", err, err)
			}
		})
	}
}

// A signed record cannot be redriven: the signature covers the record id and
// the idempotency key, both of which a redrive has to change.
func TestGivenASignedDeadLetterWhenRedrivenThenShouldRefuseIt(t *testing.T) {
	key, _ := laser.GenerateSigningKey()
	envelope := deadLetteredEnvelope(t, "order-7")
	signature, err := key.Sign(envelope)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	envelope.Signature = signature
	payload, err := wire.Marshal(envelope)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	server := newRedrive("agents", "agent.sessions")
	server.hold(42, payload, deadLetterHeaders(t, envelope, "order-7"))
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	redriveErr := handle.RedriveDeadLetter(context.Background(),
		wire.AgentDeadLetter{Source: wire.LogPosition{Offset: 42}})

	var invalid *laser.InvalidError
	if !errors.As(redriveErr, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", redriveErr, redriveErr)
	}
	if republished, _ := server.sent(); len(republished) != 0 {
		t.Error("a signed dead letter must not be republished")
	}
}

// A record already past its deadline would only dead-letter again, so
// redriving it is refused rather than wasting the round trip.
func TestGivenAnExpiredDeadLetterWhenRedrivenThenShouldRefuseIt(t *testing.T) {
	envelope := deadLetteredEnvelope(t, "order-7")
	past := uint64(time.Now().Add(-time.Hour).UnixMicro())
	envelope.DeadlineMicros = &past
	payload, err := wire.Marshal(envelope)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	server := newRedrive("agents", "agent.sessions")
	server.hold(42, payload, deadLetterHeaders(t, envelope, "order-7"))
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	redriveErr := handle.RedriveDeadLetter(context.Background(),
		wire.AgentDeadLetter{Source: wire.LogPosition{Offset: 42}})

	var invalid *laser.InvalidError
	if !errors.As(redriveErr, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", redriveErr, redriveErr)
	}
	// It says the record expired rather than failing somewhere downstream, so
	// a caller knows there is nothing to fix on its side.
	if invalid.Reason != "the dead-lettered agent record is past its deadline" {
		t.Errorf("want the deadline named, got %q", invalid.Reason)
	}
}

// A record that is not an agent envelope has no key to re-key inside it, so
// its body is replayed byte for byte.
func TestGivenANonAgentDeadLetterWhenRedrivenThenShouldReplayItVerbatim(t *testing.T) {
	server := newRedrive("agents", "events")
	server.hold(42, []byte("just some bytes"), nil)
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	if err := handle.RedriveDeadLetter(context.Background(),
		wire.AgentDeadLetter{Source: wire.LogPosition{Offset: 42}}); err != nil {
		t.Fatalf("redrive: %v", err)
	}

	republished, _ := server.sent()
	if len(republished) != 1 ||
		string(republished[0].Payload) != "just some bytes" {
		t.Fatalf("want the bytes verbatim, got %+v", republished)
	}
}

// A consumer that has never committed reads as not yet consumed, with how far
// short it is.
func TestGivenAConsumerOffsetWhenAskedThenShouldSayHowFarItGot(t *testing.T) {
	for name, want := range map[string]struct {
		stored *iggcon.ConsumerOffsetInfo
		at     uint64
		kind   laser.ConsumptionStatusKind
		behind uint64
	}{
		"never committed": {
			stored: nil, at: 10, kind: laser.NotYetConsumed, behind: 11,
		},
		"still behind": {
			stored: &iggcon.ConsumerOffsetInfo{StoredOffset: 4, CurrentOffset: 20},
			at:     10, kind: laser.NotYetConsumed, behind: 6,
		},
		"committed past it": {
			stored: &iggcon.ConsumerOffsetInfo{StoredOffset: 12, CurrentOffset: 20},
			at:     10, kind: laser.Consumed,
		},
		"committed exactly at it": {
			stored: &iggcon.ConsumerOffsetInfo{StoredOffset: 10, CurrentOffset: 20},
			at:     10, kind: laser.Consumed,
		},
	} {
		t.Run(name, func(t *testing.T) {
			server := &offsetServer{stored: want.stored}
			handle := laser.FromClient(server,
				laser.Options{DefaultStream: "agents"})
			reader, _ := iggcon.NewIdentifier("worker")

			status, err := handle.Consumed(context.Background(),
				iggcon.NewSingleConsumer(reader),
				wire.LogPosition{StreamID: 1, TopicID: 2, Offset: want.at})
			if err != nil {
				t.Fatalf("consumed: %v", err)
			}

			if status.Kind != want.kind {
				t.Errorf("want %q, got %q", want.kind, status.Kind)
			}
			if want.behind != 0 && status.BehindBy != want.behind {
				t.Errorf("want it %d short, got %d", want.behind, status.BehindBy)
			}
			if want.kind == laser.Consumed && status.Head != 20 {
				t.Errorf("want the head reported, got %d", status.Head)
			}
		})
	}
}

// offsetServer answers one stored consumer offset.
type offsetServer struct {
	iggcon.Client
	stored *iggcon.ConsumerOffsetInfo
}

func (server *offsetServer) Close() error { return nil }

func (server *offsetServer) SendBinaryRequest(
	_ context.Context, _ uint32, _ []byte,
) ([]byte, error) {
	return nil, ierror.InvalidCommand{}
}

func (server *offsetServer) GetConsumerOffset(
	_ context.Context, _ iggcon.Consumer, _ iggcon.Identifier,
	_ iggcon.Identifier, _ *uint32,
) (*iggcon.ConsumerOffsetInfo, error) {
	if server.stored == nil {
		return nil, nil
	}
	return server.stored, nil
}
