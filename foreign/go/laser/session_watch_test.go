package laser_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// A watch reads the page its next sequence falls on, waits out a stale index
// and an unchanged page, and hands each change over once.
func TestGivenASessionWatchWhenTheIndexIsStaleThenUnchangedThenShouldWaitForTheNextChange(t *testing.T) {
	first, second := conversation(1), conversation(2)
	stale, err := wire.Marshal(wire.CommandError{Code: wire.ResultStale, Message: "the index is behind"})
	if err != nil {
		t.Fatal(err)
	}
	server := &watchServer{t: t, replies: [][]byte{
		changesReply(t, wire.SessionChanges{TotalResults: 4}),
		stale,
		changesReply(t, wire.SessionChanges{TotalResults: 4}),
		changesReply(t, wire.SessionChanges{TotalResults: 5, Items: []wire.SessionChangeRow{
			{Seq: 5, Sessions: []wire.ConversationID{first}}}}),
		changesReply(t, wire.SessionChanges{TotalResults: 6, Items: []wire.SessionChangeRow{
			{Seq: 5, Sessions: []wire.ConversationID{first}},
			{Seq: 6, Sessions: []wire.ConversationID{second}}}}),
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	watch, err := sessionHandle(server, laser.Options{DefaultStream: "agents"}).
		Sessions().Watch(ctx, time.Millisecond)
	if err != nil {
		t.Fatalf("open the watch: %v", err)
	}

	for _, want := range []wire.ConversationID{first, second} {
		change, err := watch.Next(ctx)
		if err != nil {
			t.Fatalf("wait for a change: %v", err)
		}
		if change.Resync || !slices.Equal(change.Sessions, []wire.ConversationID{want}) {
			t.Fatalf("want the change of %v alone, got %+v", want, change)
		}
	}
	// The start reads page one, and sequences five and six both sit on page three.
	if !slices.Equal(server.pages, []uint32{1, 3, 3, 3, 3}) {
		t.Fatalf("want the watch to read the page of its next sequence, got %v", server.pages)
	}
}

func TestGivenASessionWatchWhenItsContextEndsThenShouldStopWaiting(t *testing.T) {
	server := &watchServer{t: t, replies: [][]byte{changesReply(t, wire.SessionChanges{TotalResults: 4})}}
	watch, err := sessionHandle(server, laser.Options{DefaultStream: "agents"}).
		Sessions().Watch(context.Background(), time.Millisecond)
	if err != nil {
		t.Fatalf("open the watch: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	if _, err := watch.Next(ctx); err == nil {
		t.Fatal("want a watch with nothing to report to end with its context")
	}
}

// watchServer answers each change read with the next scripted reply, the last
// one repeating, and remembers the pages it was asked for.
type watchServer struct {
	clusterOfOne
	t       *testing.T
	replies [][]byte
	pages   []uint32
}

func (server *watchServer) Close() error { return nil }

func (server *watchServer) SendBinaryRequest(
	_ context.Context, code uint32, payload []byte,
) ([]byte, error) {
	var request wire.SessionChangesRequest
	decodeManagedTestRequest(server.t, code, payload, &request)
	reply := server.replies[min(len(server.pages), len(server.replies)-1)]
	server.pages = append(server.pages, request.Page)
	return reply, nil
}

func changesReply(t *testing.T, changes wire.SessionChanges) []byte {
	t.Helper()
	changes.Series, changes.Results = 1, 2
	encoded, err := wire.Marshal(wire.SessionReply{Outcome: &wire.SessionOutcome{
		Kind: wire.SessionChangesOutcome, Changes: &changes}})
	if err != nil {
		t.Fatalf("encode the change page: %v", err)
	}
	return encoded
}
