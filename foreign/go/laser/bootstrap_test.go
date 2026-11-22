package laser_test

import (
	"context"
	"sync"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// flakyServer fails its sends until it is told to stop.
type flakyServer struct {
	clusterOfOne
	guard   sync.Mutex
	failing bool
}

func (server *flakyServer) Close() error { return nil }

// Connect is what the publish recovery calls between attempts, which this
// fake answers rather than redialing anything.
func (server *flakyServer) Connect(context.Context) error { return nil }

// SendBinaryRequest answers the way raw Apache Iggy does: it does not know
// the managed commands, which is a refusal rather than a lost link.
func (server *flakyServer) SendBinaryRequest(
	context.Context, uint32, []byte,
) ([]byte, error) {
	return nil, ierror.InvalidCommand{}
}

func (server *flakyServer) SendMessages(
	_ context.Context, _, _ iggcon.Identifier,
	_ iggcon.Partitioning, _ []iggcon.IggyMessage,
) (*iggcon.SendMessagesResponse, error) {
	server.guard.Lock()
	defer server.guard.Unlock()
	if server.failing {
		return nil, ierror.Disconnected{}
	}
	return &iggcon.SendMessagesResponse{}, nil
}

func (server *flakyServer) recover() {
	server.guard.Lock()
	defer server.guard.Unlock()
	server.failing = false
}

// drain reads the events a watcher holds right now.
func drain(events *laser.ConnectionEvents) []laser.ConnectionEvent {
	var seen []laser.ConnectionEvent
	for {
		select {
		case event, open := <-events.Events():
			if !open {
				return seen
			}
			seen = append(seen, event)
		default:
			return seen
		}
	}
}

func TestGivenALostLinkWhenACallFailsThenShouldReportOneDisconnect(t *testing.T) {
	server := &flakyServer{failing: true}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "fleet"})
	watcher := handle.ConnectionEvents()
	defer watcher.Close()

	for range 3 {
		_, _ = handle.Topic("telemetry").Send(
			context.Background(), []byte("tick"), nil, "")
	}

	// A run of failing calls is one connection that went down, not three.
	seen := drain(watcher)
	if len(seen) != 1 || seen[0] != laser.EventDisconnected {
		t.Fatalf("want one disconnect, got %v", seen)
	}
}

func TestGivenARecoveredLinkWhenACallSucceedsThenShouldReportTheReconnect(t *testing.T) {
	server := &flakyServer{failing: true}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "fleet"})
	watcher := handle.ConnectionEvents()
	defer watcher.Close()
	_, _ = handle.Topic("telemetry").Send(context.Background(), []byte("tick"), nil, "")
	server.recover()

	if _, err := handle.Topic("telemetry").Send(
		context.Background(), []byte("tick"), nil, ""); err != nil {
		t.Fatalf("publish after recovery: %v", err)
	}

	seen := drain(watcher)
	if len(seen) != 2 || seen[0] != laser.EventDisconnected ||
		seen[1] != laser.EventConnected {
		t.Fatalf("want a disconnect then a reconnect, got %v", seen)
	}
}

func TestGivenAnAnsweredRefusalWhenReportedThenShouldNotCallTheLinkDown(t *testing.T) {
	server := &flakyServer{}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "fleet"})
	watcher := handle.ConnectionEvents()
	defer watcher.Close()

	// A managed command raw Apache Iggy does not know is a refusal the server
	// answered, so the connection that carried it is working.
	_, _ = handle.SendManaged(context.Background(), wire.AgdxKvGetCode, nil)

	for _, event := range drain(watcher) {
		if event == laser.EventDisconnected {
			t.Fatal("a refusal the server answered is not a lost connection")
		}
	}
}

func TestGivenAClosedHandleWhenWatchedThenShouldReportShutdownAndEnd(t *testing.T) {
	handle := laser.FromClient(&flakyServer{}, laser.Options{DefaultStream: "fleet"})
	watcher := handle.ConnectionEvents()

	if err := handle.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	seen := drain(watcher)
	if len(seen) != 1 || seen[0] != laser.EventShutdown {
		t.Fatalf("want the shutdown reported, got %v", seen)
	}
	// The channel closes with the connection, so a reader ranging over it
	// ends rather than blocking on a handle that is gone.
	if _, open := <-watcher.Events(); open {
		t.Error("the watcher must end when the connection does")
	}
}
