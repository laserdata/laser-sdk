package laser_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// laneServer is a lane whose identity the test can change underneath a
// retained handle, which is the whole point of the guard.
type laneServer struct {
	clusterOfOne
	mutex sync.Mutex
	// lane is the identity GetStream currently answers with.
	lane iggcon.StreamDetails
	// sent counts the records that reached the log, so a refused write is
	// visibly one that never got there.
	sent int
	// registered, when set, is the lane the managed index claims. nil
	// answers NotRegistered, which is not a mismatch.
	registered *wire.LaneIdentity
}

func newLaneServer() *laneServer {
	return &laneServer{lane: iggcon.StreamDetails{
		Stream: iggcon.Stream{Id: 1, Name: "agents", CreatedAt: 1},
		Topics: []iggcon.Topic{{
			Id: 1, Name: wire.AgentSessions, CreatedAt: 1, PartitionsCount: 1,
		}},
	}}
}

func (server *laneServer) Close() error { return nil }

func (server *laneServer) GetStream(
	_ context.Context, _ iggcon.Identifier,
) (*iggcon.StreamDetails, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	held := server.lane
	held.Topics = append([]iggcon.Topic(nil), server.lane.Topics...)
	return &held, nil
}

func (server *laneServer) GetTopic(
	_ context.Context, _, _ iggcon.Identifier,
) (*iggcon.TopicDetails, error) {
	return &iggcon.TopicDetails{
		Topic:      iggcon.Topic{PartitionsCount: 1},
		Partitions: []iggcon.PartitionContract{{Id: 0}},
	}, nil
}

func (server *laneServer) SendMessages(
	_ context.Context, _, _ iggcon.Identifier,
	_ iggcon.Partitioning, messages []iggcon.IggyMessage,
) (*iggcon.SendMessagesResponse, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.sent += len(messages)
	return &iggcon.SendMessagesResponse{}, nil
}

func (server *laneServer) SendBinaryRequest(
	_ context.Context, code uint32, _ []byte,
) ([]byte, error) {
	if code != wire.AgdxSessionSourcesCode {
		return nil, nil
	}
	server.mutex.Lock()
	registered := server.registered
	server.mutex.Unlock()
	if registered == nil {
		return wire.Marshal(wire.SessionReply{Err: &wire.SessionError{
			Kind: wire.SessionNotRegistered, Message: "no registration"}})
	}
	return wire.Marshal(wire.SessionReply{Outcome: &wire.SessionOutcome{
		Kind:    wire.SessionSourcesOutcome,
		Sources: &wire.SessionSources{Lane: registered},
	}})
}

func (server *laneServer) written() int {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	return server.sent
}

// recreateTopic is the topic deleted and made again: same name, same id, a
// new creation time. The case a numeric id alone cannot see.
func (server *laneServer) recreateTopic() {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.lane.Topics[0].CreatedAt++
}

func (server *laneServer) recreateStream() {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.lane.CreatedAt++
}

func (server *laneServer) repartition(count uint32) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.lane.Topics[0].PartitionsCount = count
}

func (server *laneServer) dropTopic() {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.lane.Topics = nil
}

// laneHandle opens a client over the lane server. Sessions capability off by
// default, so a test that is not about the registered lane does not have to
// answer for one.
func laneHandle(server *laneServer, sessions bool) *laser.Laser {
	capabilities := laser.Capabilities{Managed: sessions, Sessions: sessions}
	return laser.FromClient(server, laser.Options{
		DefaultStream: "agents", Capabilities: &capabilities,
	})
}

// laneSession opens one session handle on the lane.
func laneSession(t *testing.T, handle *laser.Laser) *laser.Session {
	t.Helper()
	return handle.Sessions().
		Open(conversation(7), "").
		As(agentID(t, "planner"))
}

// A handle that keeps writing to a lane that was replaced underneath it would
// append to the replacement, and a reader of the recovered lane would never
// see those records as part of the session they belong to.
func TestGivenARetainedHandleWhenItsLaneIsReplacedThenShouldRefuseTheWrite(t *testing.T) {
	for _, change := range []struct {
		name  string
		apply func(*laneServer)
	}{
		{"topic generation", (*laneServer).recreateTopic},
		{"stream generation", (*laneServer).recreateStream},
		{"partition count", func(server *laneServer) { server.repartition(4) }},
	} {
		t.Run(change.name, func(t *testing.T) {
			server := newLaneServer()
			session := laneSession(t, laneHandle(server, false))
			// The first write pins the lane as it is now.
			if _, err := session.Complete(context.Background(), "first"); err != nil {
				t.Fatalf("the first write: %v", err)
			}
			pinned := server.written()

			change.apply(server)

			_, err := session.Progress(context.Background(),
				wire.SessionTransition{})
			var stale *laser.StaleLaneError
			if !errors.As(err, &stale) {
				t.Fatalf("want the write refused as stale, got %v", err)
			}
			if server.written() != pinned {
				t.Fatalf("a refused write must not reach the log: %d then %d",
					pinned, server.written())
			}
		})
	}
}

// Recovery is opening a fresh handle, not retrying the old one: the old
// handle's pin is what refuses, and nothing clears it.
func TestGivenARecoveredLaneWhenAFreshHandleWritesThenShouldWriteAndTheOldStillRefuse(
	t *testing.T,
) {
	server := newLaneServer()
	handle := laneHandle(server, false)
	retained := laneSession(t, handle)
	if _, err := retained.Complete(context.Background(), "first"); err != nil {
		t.Fatalf("the first write: %v", err)
	}

	server.recreateTopic()

	fresh := laneSession(t, handle)
	before := server.written()
	if _, err := fresh.Complete(context.Background(), "recovered"); err != nil {
		t.Fatalf("a fresh handle must write on the recovered lane: %v", err)
	}
	if server.written() != before+1 {
		t.Fatal("the fresh handle's record never reached the log")
	}

	var stale *laser.StaleLaneError
	if _, err := retained.Progress(context.Background(),
		wire.SessionTransition{}); !errors.As(err, &stale) {
		t.Fatalf("want the retained handle still refused, got %v", err)
	}
}

// A lane that is gone, or that has no partitions, can hold nothing. Refused
// as stale rather than left to fail later as an opaque publish error.
func TestGivenAnUnusableLaneWhenWritingThenShouldRefuseItAsStale(t *testing.T) {
	for _, unusable := range []struct {
		name  string
		apply func(*laneServer)
	}{
		{"the topic is gone", (*laneServer).dropTopic},
		{"the topic has no partitions", func(server *laneServer) { server.repartition(0) }},
	} {
		t.Run(unusable.name, func(t *testing.T) {
			server := newLaneServer()
			unusable.apply(server)
			session := laneSession(t, laneHandle(server, false))

			_, err := session.Complete(context.Background(), "done")
			var stale *laser.StaleLaneError
			if !errors.As(err, &stale) {
				t.Fatalf("want the write refused as stale, got %v", err)
			}
			if server.written() != 0 {
				t.Fatal("a refused write must not reach the log")
			}
		})
	}
}

// A lane that is healthy in itself but is not the one the deployment folds
// would take writes no aggregate read would ever return.
func TestGivenARegisteredLaneThatDisagreesWhenWritingThenShouldRefuseIt(t *testing.T) {
	server := newLaneServer()
	// The index names a different topic generation than the source carries.
	server.registered = &wire.LaneIdentity{TopicID: 1, Generation: 99, Partitions: 1}
	session := laneSession(t, laneHandle(server, true))

	_, err := session.Complete(context.Background(), "done")
	var stale *laser.StaleLaneError
	if !errors.As(err, &stale) {
		t.Fatalf("want the write refused as stale, got %v", err)
	}
	if server.written() != 0 {
		t.Fatal("a refused write must not reach the log")
	}
}

// The agreeing case has to write, or the test above would pass for a client
// that refused every managed session write.
func TestGivenARegisteredLaneThatAgreesWhenWritingThenShouldWriteIt(t *testing.T) {
	server := newLaneServer()
	server.registered = &wire.LaneIdentity{TopicID: 1, Generation: 1, Partitions: 1}
	session := laneSession(t, laneHandle(server, true))

	if _, err := session.Complete(context.Background(), "done"); err != nil {
		t.Fatalf("want the write accepted, got %v", err)
	}
	if server.written() != 1 {
		t.Fatal("the record never reached the log")
	}
}

// A deployment that holds no registration for the session has nothing to
// disagree with, so the pin is the whole check.
func TestGivenNoRegistrationWhenWritingThenShouldStillWrite(t *testing.T) {
	server := newLaneServer()
	session := laneSession(t, laneHandle(server, true))

	if _, err := session.Complete(context.Background(), "done"); err != nil {
		t.Fatalf("want the write accepted, got %v", err)
	}
	if server.written() != 1 {
		t.Fatal("the record never reached the log")
	}
}
