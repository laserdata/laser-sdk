package laser_test

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"sync"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

// scratchServer answers the two calls a scratch stream makes and remembers
// every name it was asked to create, so a test can state that two runs did
// not collide rather than only that neither failed.
type scratchServer struct {
	clusterOfOne
	mutex sync.Mutex

	createFails  error
	deleteFails  error
	missingOnDel bool

	created []string
	deleted []string
}

func (server *scratchServer) Close() error { return nil }

func (server *scratchServer) CreateStream(
	_ context.Context, name string, _ *iggcon.StreamOptions,
) (*iggcon.StreamDetails, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.created = append(server.created, name)
	if server.createFails != nil {
		return nil, server.createFails
	}
	return &iggcon.StreamDetails{}, nil
}

func (server *scratchServer) DeleteStream(
	_ context.Context, identifier iggcon.Identifier,
) error {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.deleted = append(server.deleted, string(identifier.Value()))
	if server.missingOnDel {
		return ierror.StreamNameNotFound{}
	}
	return server.deleteFails
}

func (server *scratchServer) creates() []string {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	return append([]string(nil), server.created...)
}

func (server *scratchServer) deletes() []string {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	return append([]string(nil), server.deleted...)
}

var scratchName = regexp.MustCompile(`^runs-[0-9a-f]{8}$`)

func TestGivenAPrefixWhenMakingAScratchStreamThenShouldNameItUniquely(t *testing.T) {
	server := &scratchServer{}
	client := laser.FromClient(server, laser.Options{})
	scratch, err := client.ScratchStream(context.Background(), "runs")
	if err != nil {
		t.Fatalf("scratch stream: %v", err)
	}
	if !scratchName.MatchString(scratch.Name()) {
		t.Fatalf("named %q, want the prefix and eight lowercase hex characters",
			scratch.Name())
	}
	if created := server.creates(); len(created) != 1 || created[0] != scratch.Name() {
		t.Fatalf("created %v, want just %q", created, scratch.Name())
	}
}

func TestGivenTwoScratchStreamsWhenMadeFromOnePrefixThenShouldNotCollide(t *testing.T) {
	server := &scratchServer{}
	client := laser.FromClient(server, laser.Options{})
	seen := map[string]struct{}{}
	// Eight hex characters is 32 bits, so a collision across a handful of
	// runs would mean the name is not actually random rather than bad luck.
	for range 16 {
		scratch, err := client.ScratchStream(context.Background(), "runs")
		if err != nil {
			t.Fatalf("scratch stream: %v", err)
		}
		if _, repeated := seen[scratch.Name()]; repeated {
			t.Fatalf("two scratch streams were both named %q", scratch.Name())
		}
		seen[scratch.Name()] = struct{}{}
	}
}

func TestGivenAScratchStreamWhenReadingItsHandleThenShouldDefaultToThatStream(t *testing.T) {
	server := &scratchServer{}
	client := laser.FromClient(server, laser.Options{})
	scratch, err := client.ScratchStream(context.Background(), "runs")
	if err != nil {
		t.Fatalf("scratch stream: %v", err)
	}
	if scratch.Laser() == client {
		t.Fatal("the scratch handle must be its own, not the one it was made from")
	}
	if got := scratch.Laser().DefaultStream(); got != scratch.Name() {
		t.Fatalf("the handle defaults to %q, want the scratch stream %q",
			got, scratch.Name())
	}
	// The handle it was made from keeps whatever default it had, so a
	// scratch stream never redirects the caller's own writes.
	if got := client.DefaultStream(); got != "" {
		t.Fatalf("the original handle now defaults to %q, want it unchanged", got)
	}
}

func TestGivenAStreamThatAlreadyExistsWhenMakingAScratchStreamThenShouldRefuseIt(t *testing.T) {
	server := &scratchServer{createFails: errors.New("already exists")}
	client := laser.FromClient(server, laser.Options{})
	scratch, err := client.ScratchStream(context.Background(), "runs")
	if err == nil {
		t.Fatal("a name that is taken must be refused, never reused")
	}
	if scratch != nil {
		t.Fatal("a refused scratch stream must not be handed back")
	}
	var transport *laser.TransportError
	if !errors.As(err, &transport) {
		t.Fatalf("reported %T, want *laser.TransportError", err)
	}
}

func TestGivenAScratchStreamWhenDeletedThenShouldRemoveThatStream(t *testing.T) {
	server := &scratchServer{}
	client := laser.FromClient(server, laser.Options{})
	scratch, err := client.ScratchStream(context.Background(), "runs")
	if err != nil {
		t.Fatalf("scratch stream: %v", err)
	}
	if err := scratch.Delete(context.Background()); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if deleted := server.deletes(); len(deleted) != 1 || deleted[0] != scratch.Name() {
		t.Fatalf("deleted %v, want just %q", deleted, scratch.Name())
	}
}

func TestGivenADeletedScratchStreamWhenDeletedAgainThenShouldNotAskTwice(t *testing.T) {
	server := &scratchServer{}
	client := laser.FromClient(server, laser.Options{})
	scratch, err := client.ScratchStream(context.Background(), "runs")
	if err != nil {
		t.Fatalf("scratch stream: %v", err)
	}
	for range 3 {
		if err := scratch.Delete(context.Background()); err != nil {
			t.Fatalf("delete: %v", err)
		}
	}
	// A deferred delete after an explicit one is the shape this is for, so
	// the second call must be free rather than another round trip.
	if deleted := server.deletes(); len(deleted) != 1 {
		t.Fatalf("asked the server %d times, want once", len(deleted))
	}
}

func TestGivenAStreamAlreadyGoneWhenDeletingAScratchStreamThenShouldSucceed(t *testing.T) {
	server := &scratchServer{missingOnDel: true}
	client := laser.FromClient(server, laser.Options{})
	scratch, err := client.ScratchStream(context.Background(), "runs")
	if err != nil {
		t.Fatalf("scratch stream: %v", err)
	}
	if err := scratch.Delete(context.Background()); err != nil {
		t.Fatalf("a stream something else removed is not a failure: %v", err)
	}
}

func TestGivenAFailedDeleteWhenDeletingAgainThenShouldRetry(t *testing.T) {
	server := &scratchServer{deleteFails: errors.New("the link dropped")}
	client := laser.FromClient(server, laser.Options{})
	scratch, err := client.ScratchStream(context.Background(), "runs")
	if err != nil {
		t.Fatalf("scratch stream: %v", err)
	}
	if err := scratch.Delete(context.Background()); err == nil {
		t.Fatal("a delete the server refused must be reported")
	}
	server.mutex.Lock()
	server.deleteFails = nil
	server.mutex.Unlock()
	if err := scratch.Delete(context.Background()); err != nil {
		t.Fatalf("a failed delete must stay retryable: %v", err)
	}
	if deleted := server.deletes(); len(deleted) != 2 {
		t.Fatalf("asked the server %d times, want twice", len(deleted))
	}
}

func TestGivenAPrefixThatIsNotANameWhenMakingAScratchStreamThenShouldRefuseIt(t *testing.T) {
	server := &scratchServer{}
	client := laser.FromClient(server, laser.Options{})
	scratch, err := client.ScratchStream(
		context.Background(), strings.Repeat("a", 512))
	if err == nil {
		t.Fatal("a prefix that cannot name a stream must be refused")
	}
	if scratch != nil {
		t.Fatal("a refused scratch stream must not be handed back")
	}
	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("reported %T, want *laser.InvalidError", err)
	}
	if created := server.creates(); len(created) != 0 {
		t.Fatalf("a refused name still reached the server as %v", created)
	}
}

func TestGivenAClosedClientWhenMakingAScratchStreamThenShouldRefuseIt(t *testing.T) {
	client := laser.FromClient(&scratchServer{}, laser.Options{})
	if err := client.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := client.ScratchStream(context.Background(), "runs"); err == nil {
		t.Fatal("a closed client must refuse rather than panic")
	}
}
