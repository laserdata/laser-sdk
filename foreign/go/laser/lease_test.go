package laser_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// heartbeatServer records the heartbeats a lease registry published.
type heartbeatServer struct {
	iggcon.Client
	mutex sync.Mutex
	// generation is the stream's creation time, which a test changes to
	// stand for a stream recreated underneath a lease.
	generation uint64
	missing    bool
	// blank makes the stream read answer with neither details nor an error,
	// which is not the same as a read that failed.
	blank  bool
	beats  []wire.SessionHeartbeat
	topics []string
}

func newHeartbeatServer() *heartbeatServer {
	return &heartbeatServer{generation: 1_000}
}

func (server *heartbeatServer) Close() error { return nil }

func (server *heartbeatServer) GetStream(
	_ context.Context, _ iggcon.Identifier,
) (*iggcon.StreamDetails, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.missing {
		return nil, ierror.StreamNameNotFound{}
	}
	if server.blank {
		return nil, nil
	}
	return &iggcon.StreamDetails{Stream: iggcon.Stream{CreatedAt: server.generation}}, nil
}

func (server *heartbeatServer) SendMessages(
	_ context.Context, _, topicID iggcon.Identifier,
	_ iggcon.Partitioning, messages []iggcon.IggyMessage,
) (*iggcon.SendMessagesResponse, error) {
	name, err := topicID.String()
	if err != nil {
		return nil, err
	}
	for _, message := range messages {
		var envelope wire.AgentEnvelope
		if err := wire.Unmarshal(message.Payload, &envelope); err != nil {
			return nil, err
		}
		var beat wire.SessionHeartbeat
		if err := wire.Unmarshal(envelope.Body, &beat); err != nil {
			return nil, err
		}
		server.mutex.Lock()
		server.beats = append(server.beats, beat)
		server.topics = append(server.topics, name)
		server.mutex.Unlock()
	}
	return &iggcon.SendMessagesResponse{}, nil
}

func (server *heartbeatServer) published() []wire.SessionHeartbeat {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	return append([]wire.SessionHeartbeat(nil), server.beats...)
}

func (server *heartbeatServer) beatTopics() []string {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	return append([]string(nil), server.topics...)
}

func (server *heartbeatServer) recreate(generation uint64) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.generation = generation
}

// awaitBeats waits for at least want heartbeats, or fails the test.
func (server *heartbeatServer) awaitBeats(t *testing.T, want int) []wire.SessionHeartbeat {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if beats := server.published(); len(beats) >= want {
			return beats
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("want %d heartbeats, got %d", want, len(server.published()))
	return nil
}

// awaitStopped waits for the registry to hold nothing and its loop to end.
func awaitStopped(t *testing.T, registry *laser.LeaseRegistry) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if registry.Held() == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("the registry still holds %d leases", registry.Held())
}

// awaitLoops waits for the registry to run exactly want heartbeat loops.
func awaitLoops(t *testing.T, registry *laser.LeaseRegistry, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if registry.Loops() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("want %d heartbeat loops, got %d", want, registry.Loops())
}

const leaseTick = 2 * time.Millisecond

func leaseHandle(server iggcon.Client) *laser.Laser {
	return streamHandle(server, laser.Options{DefaultStream: "agents"})
}

func TestGivenASessionLeaseWhenHeldThenShouldListItInTheHeartbeat(t *testing.T) {
	server := newHeartbeatServer()
	handle := leaseHandle(server)

	lease, err := handle.LeaseSession(
		context.Background(), conversation(1), agentID(t, "planner"), leaseTick)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	defer lease.Release()

	if lease.Session() != conversation(1) || lease.Stream() != "agents" {
		t.Fatalf("the lease names %v on %q", lease.Session(), lease.Stream())
	}
	beat := server.awaitBeats(t, 1)[0]
	if beat.Stream != "agents" {
		t.Fatalf("the heartbeat names stream %q", beat.Stream)
	}
	if len(beat.Sessions) != 1 || beat.Sessions[0] != conversation(1) {
		t.Fatalf("the heartbeat lists %v", beat.Sessions)
	}
	if beat.Process != handle.Leases().Process() || beat.Process == "" {
		t.Fatalf("the heartbeat names process %q", beat.Process)
	}
	if beat.StreamGeneration == nil || *beat.StreamGeneration != 1_000 {
		t.Fatalf("the heartbeat names generation %v", beat.StreamGeneration)
	}
	if topics := server.beatTopics(); topics[0] != wire.AgentHeartbeats {
		t.Fatalf("the heartbeat rode %q", topics[0])
	}
}

func TestGivenTheLastLeaseIsReleasedWhenTickingataThenShouldStopBeating(t *testing.T) {
	server := newHeartbeatServer()
	handle := leaseHandle(server)
	registry := handle.Leases()

	lease, err := handle.LeaseSession(
		context.Background(), conversation(1), agentID(t, "planner"), leaseTick)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	server.awaitBeats(t, 1)
	lease.Release()
	awaitStopped(t, registry)
	// The loop ends with the last lease rather than ticking on over an empty
	// registry.
	awaitLoops(t, registry, 0)

	// A process that holds no lease publishes nothing, so once the loop has
	// wound down the count stops moving.
	time.Sleep(20 * leaseTick)
	settled := len(server.published())
	time.Sleep(20 * leaseTick)
	if grown := len(server.published()); grown != settled {
		t.Fatalf("beats grew from %d to %d after the last release", settled, grown)
	}
}

func TestGivenTwoHoldersOfOneSessionWhenOneReleasesThenShouldKeepTheLease(t *testing.T) {
	server := newHeartbeatServer()
	handle := leaseHandle(server)
	registry := handle.Leases()

	first, err := handle.LeaseSession(
		context.Background(), conversation(1), agentID(t, "planner"), leaseTick)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	second, err := handle.LeaseSession(
		context.Background(), conversation(1), agentID(t, "planner"), leaseTick)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	if registry.Held() != 1 {
		t.Fatalf("two holders of one session is one lease, got %d", registry.Held())
	}

	first.Release()
	if registry.Held() != 1 {
		t.Fatal("the second holder still holds the session")
	}
	// Releasing the same lease twice must not drop the holder the other one
	// still has.
	first.Release()
	if registry.Held() != 1 {
		t.Fatal("a repeated release must change nothing")
	}
	second.Release()
	awaitStopped(t, registry)
}

func TestGivenSeveralLeasesWhenBeatingThenShouldUseTheShortestInterval(t *testing.T) {
	server := newHeartbeatServer()
	handle := leaseHandle(server)

	// The tight lease is taken first, so the loop is already ticking at its
	// interval. Adding an hour-long one must not slow it down: the holder
	// that asked for the tightest liveness window is the one that needs it.
	fast, err := handle.LeaseSession(
		context.Background(), conversation(1), agentID(t, "planner"), leaseTick)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	defer fast.Release()
	server.awaitBeats(t, 1)
	slow, err := handle.LeaseSession(
		context.Background(), conversation(2), agentID(t, "planner"), time.Hour)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	defer slow.Release()

	beats := server.awaitBeats(t, len(server.published())+3)
	last := beats[len(beats)-1]
	if len(last.Sessions) != 2 {
		t.Fatalf("both sessions ride one heartbeat, got %v", last.Sessions)
	}
}

func TestGivenTheStreamIsRecreatedWhenBeatingThenShouldStopNamingItsSessions(t *testing.T) {
	server := newHeartbeatServer()
	handle := leaseHandle(server)

	lease, err := handle.LeaseSession(
		context.Background(), conversation(1), agentID(t, "planner"), leaseTick)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	defer lease.Release()
	server.awaitBeats(t, 1)

	// The stream the lease was taken on is gone, so the sessions it named are
	// gone with it: claiming them alive would keep a deployment waiting on
	// work nobody is doing.
	server.recreate(2_000)
	time.Sleep(20 * leaseTick)
	settled := len(server.published())
	time.Sleep(20 * leaseTick)
	if grown := len(server.published()); grown != settled {
		t.Fatalf("beats grew from %d to %d after the stream was recreated", settled, grown)
	}
}

func TestGivenTheStreamIsGoneWhenBeatingThenShouldRetryRatherThanFail(t *testing.T) {
	server := newHeartbeatServer()
	handle := leaseHandle(server)

	lease, err := handle.LeaseSession(
		context.Background(), conversation(1), agentID(t, "planner"), leaseTick)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	defer lease.Release()
	server.awaitBeats(t, 1)

	server.mutex.Lock()
	server.missing = true
	server.mutex.Unlock()
	time.Sleep(20 * leaseTick)
	server.mutex.Lock()
	server.missing = false
	server.mutex.Unlock()

	// A failed read is a missed liveness signal, not work that went wrong, so
	// the loop is still beating once the stream answers again.
	before := len(server.published())
	server.awaitBeats(t, before+1)
}

func TestGivenMoreSessionsThanOneHeartbeatCarriesWhenBeatingThenShouldSplitThem(t *testing.T) {
	server := newHeartbeatServer()
	handle := leaseHandle(server)
	total := wire.MaxHeartbeatSessions + 2
	held := make([]*laser.SessionLease, 0, total)
	for index := 0; index < total; index++ {
		lease, err := handle.LeaseSession(context.Background(),
			wire.ConversationID{ID: wire.IDFromBytes(laser.NewOperationID())},
			agentID(t, "planner"), leaseTick)
		if err != nil {
			t.Fatalf("lease %d: %v", index, err)
		}
		held = append(held, lease)
	}
	defer func() {
		for _, lease := range held {
			lease.Release()
		}
	}()

	// Once every lease is held, one tick writes the whole set as a run of
	// heartbeats, each inside the contract's cap. Earlier ticks saw fewer
	// leases, so the run is looked for rather than assumed to be first.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if splitRun(t, server.published(), total) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("no tick listed all %d sessions, saw %d heartbeats",
		total, len(server.published()))
}

// splitRun reports a run of heartbeats that together list total sessions,
// every full one at exactly the contract's cap.
func splitRun(t *testing.T, beats []wire.SessionHeartbeat, total int) bool {
	t.Helper()
	for start := range beats {
		listed := 0
		for _, beat := range beats[start:] {
			if err := beat.Validate(); err != nil {
				t.Fatalf("the heartbeat must be one the contract carries: %v", err)
			}
			listed += len(beat.Sessions)
			if listed == total {
				return true
			}
			// A heartbeat short of the cap ends its tick, so a run cannot
			// continue past one.
			if len(beat.Sessions) != wire.MaxHeartbeatSessions {
				break
			}
		}
	}
	return false
}

func TestGivenNoStreamOrAgentWhenLeasingThenShouldRefuseTheLease(t *testing.T) {
	server := newHeartbeatServer()
	streamless := streamHandle(server, laser.Options{})
	_, noStream := streamless.LeaseSession(
		context.Background(), conversation(1), agentID(t, "planner"), leaseTick)
	if noStream == nil || !strings.Contains(noStream.Error(), "no stream selected") {
		t.Fatalf("got %v, want the missing stream named", noStream)
	}
	handle := leaseHandle(server)
	_, err := handle.LeaseSession(
		context.Background(), conversation(1), wire.AgentID{}, leaseTick)
	if err == nil || !strings.Contains(err.Error(), "agent") {
		t.Fatalf("got %v, want the holder named", err)
	}
	server.mutex.Lock()
	server.missing = true
	server.mutex.Unlock()
	if _, err := handle.LeaseSession(
		context.Background(), conversation(1), agentID(t, "planner"), leaseTick); err == nil {
		t.Fatal("a lease on a stream that does not exist must be refused")
	}
}

func TestGivenNoIntervalWhenLeasingThenShouldTakeTheDefault(t *testing.T) {
	server := newHeartbeatServer()
	handle := leaseHandle(server)

	lease, err := handle.LeaseSession(
		context.Background(), conversation(1), agentID(t, "planner"), 0)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	defer lease.Release()
	// The default is far longer than this test waits, so nothing has beaten
	// yet: a zero interval must not read as "beat as fast as you can".
	time.Sleep(20 * leaseTick)
	if beats := server.published(); len(beats) != 0 {
		t.Fatalf("a default interval must not beat at once, got %d", len(beats))
	}
	if handle.Leases().Held() != 1 {
		t.Fatal("the lease is held")
	}
	// A zero interval stored as zero would read as an empty registry and end
	// the loop, so the lease would never beat at all.
	awaitLoops(t, handle.Leases(), 1)
}

func TestGivenOneConnectionWhenLeasingFromTwoHandlesThenShouldShareOneRegistry(t *testing.T) {
	server := newHeartbeatServer()
	handle := leaseHandle(server)
	derived := handle.WithDefaultStream("agents")

	lease, err := derived.LeaseSession(
		context.Background(), conversation(1), agentID(t, "planner"), time.Hour)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	defer lease.Release()

	// Two registries on one connection would each claim the same sessions
	// under a different process identity.
	if handle.Leases() != derived.Leases() {
		t.Fatal("one connection holds one lease registry")
	}
	if handle.Leases().Held() != 1 {
		t.Fatalf("the derived handle's lease is the connection's, got %d",
			handle.Leases().Held())
	}
}

func TestGivenManyLeasesWhenHeldThenShouldRunOneHeartbeatLoop(t *testing.T) {
	server := newHeartbeatServer()
	handle := leaseHandle(server)
	registry := handle.Leases()

	var held []*laser.SessionLease
	for index := 0; index < 4; index++ {
		lease, err := handle.LeaseSession(context.Background(),
			wire.ConversationID{ID: wire.IDFromBytes(laser.NewOperationID())},
			agentID(t, "planner"), leaseTick)
		if err != nil {
			t.Fatalf("lease %d: %v", index, err)
		}
		held = append(held, lease)
	}
	awaitLoops(t, registry, 1)
	// A second loop would publish the same sessions twice a tick under one
	// process identity, which reads downstream as two processes claiming the
	// same work.
	time.Sleep(20 * leaseTick)
	if got := registry.Loops(); got != 1 {
		t.Fatalf("one connection runs one heartbeat loop, got %d", got)
	}
	for _, lease := range held {
		lease.Release()
	}
	awaitLoops(t, registry, 0)
}

func TestGivenLeasesOnTwoStreamIncarnationsWhenBeatingThenShouldKeepThemApart(t *testing.T) {
	server := newHeartbeatServer()
	handle := leaseHandle(server)

	// The first lease is taken on the old incarnation. The stream is then
	// recreated, so the second lease belongs to a different scope even
	// though both name the same stream.
	stale, err := handle.LeaseSession(
		context.Background(), conversation(1), agentID(t, "planner"), leaseTick)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	defer stale.Release()
	server.recreate(2_000)
	live, err := handle.LeaseSession(
		context.Background(), conversation(2), agentID(t, "planner"), leaseTick)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	defer live.Release()

	// Only the live incarnation's lease is published. Grouping the two
	// together would either name a session on a stream that no longer holds
	// it, or drop the live one.
	beat := server.awaitBeats(t, 1)[0]
	if len(beat.Sessions) != 1 || beat.Sessions[0] != conversation(2) {
		t.Fatalf("the heartbeat lists %v, want only the live session", beat.Sessions)
	}
	if beat.StreamGeneration == nil || *beat.StreamGeneration != 2_000 {
		t.Fatalf("the heartbeat names generation %v", beat.StreamGeneration)
	}
}

func TestGivenSeveralSessionsWhenBeatingThenShouldListThemInAFixedOrder(t *testing.T) {
	server := newHeartbeatServer()
	handle := leaseHandle(server)

	var held []*laser.SessionLease
	for index := 1; index <= 9; index++ {
		lease, err := handle.LeaseSession(context.Background(),
			conversation(byte(index)), agentID(t, "planner"), leaseTick)
		if err != nil {
			t.Fatalf("lease %d: %v", index, err)
		}
		held = append(held, lease)
	}
	defer func() {
		for _, lease := range held {
			lease.Release()
		}
	}()

	// A map iterates in a different order every run, so a heartbeat built
	// straight off one would list its sessions differently each tick and a
	// reader could not tell a changed set from a reshuffled one.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, beat := range server.published() {
			if len(beat.Sessions) != 9 {
				continue
			}
			for index := 1; index < len(beat.Sessions); index++ {
				if beat.Sessions[index-1].String() >= beat.Sessions[index].String() {
					t.Fatalf("the heartbeat lists %v out of order", beat.Sessions)
				}
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("no heartbeat listed every session")
}

func TestGivenAStreamReadThatAnswersWithNothingWhenLeasingThenShouldRefuseIt(t *testing.T) {
	server := newHeartbeatServer()
	server.mutex.Lock()
	server.blank = true
	server.mutex.Unlock()
	handle := leaseHandle(server)

	_, err := handle.LeaseSession(
		context.Background(), conversation(1), agentID(t, "planner"), leaseTick)
	if err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("got %v, want the missing stream named", err)
	}
}

func TestGivenASecondHolderWantsATighterIntervalWhenTakenThenShouldTightenIt(t *testing.T) {
	server := newHeartbeatServer()
	handle := leaseHandle(server)
	registry := handle.Leases()

	slow, err := handle.LeaseSession(
		context.Background(), conversation(1), agentID(t, "planner"), time.Hour)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	defer slow.Release()
	if got := registry.ShortestInterval(); got != time.Hour {
		t.Fatalf("the next tick waits %v, want an hour", got)
	}

	// The same session, taken again by a holder that needs a tighter
	// liveness window. One entry holds the session, so the entry is what has
	// to tighten.
	fast, err := handle.LeaseSession(
		context.Background(), conversation(1), agentID(t, "planner"), leaseTick)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	defer fast.Release()
	if got := registry.ShortestInterval(); got != leaseTick {
		t.Fatalf("the next tick waits %v, want %v", got, leaseTick)
	}
	if registry.Held() != 1 {
		t.Fatalf("two holders of one session is one lease, got %d", registry.Held())
	}
}

func TestGivenNoLeasesWhenAskedForTheIntervalThenShouldReportNone(t *testing.T) {
	if got := laser.NewLeaseRegistry().ShortestInterval(); got != 0 {
		t.Fatalf("an empty registry waits %v, want nothing", got)
	}
}
