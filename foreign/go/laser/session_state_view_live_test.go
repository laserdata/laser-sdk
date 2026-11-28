//go:build integration

package laser_test

import (
	"context"
	"strings"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenALiveLaneThatLostItsBaselineWhenAFreshHandleWritesThenShouldContinueFromTheManagedView(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	handle := liveScratch(t, dialLive(t), ctx, "laser-session-view").Laser()
	if capabilities := handle.Capabilities(); !capabilities.Managed || !capabilities.Sessions {
		t.Skip("the deployment indexes no sessions")
	}
	// One megabyte segments, so the lane's first records sit in a segment that can be deleted.
	stream, err := iggcon.NewIdentifier(handle.DefaultStream())
	if err != nil {
		t.Fatalf("name the stream: %v", err)
	}
	partitions, segment := uint32(1), uint64(1<<20)
	if _, err := handle.Client().CreateTopic(ctx, stream, wire.AgentSessions,
		&iggcon.TopicOptions{PartitionsCount: &partitions, SegmentSize: &segment}); err != nil {
		t.Fatalf("create the session lane: %v", err)
	}
	bootstrap, err := handle.Sessions().Bootstrap(ctx, 1, laser.ExpireAfter(time.Hour))
	if err != nil || !bootstrap.Registered {
		t.Fatalf("bootstrap the stream as a session source: %+v, %v", bootstrap, err)
	}
	planner := agentID(t, "planner")
	session := handle.Sessions().Start().As(planner)
	if _, _, err := session.Begin(ctx); err != nil {
		t.Fatalf("begin: %v", err)
	}
	conversation := session.Conversation()
	state := session.State()
	if _, err := state.Set(ctx, "phase", text("triage")); err != nil {
		t.Fatalf("set the phase: %v", err)
	}
	padding := text(strings.Repeat("x", 100_000))
	for range 14 {
		if _, err := state.Set(ctx, "pad", padding); err != nil {
			t.Fatalf("pad the lane: %v", err)
		}
	}
	if _, err := state.Set(ctx, "owner", text("planner")); err != nil {
		t.Fatalf("set the owner: %v", err)
	}
	const written = 16
	indexed := awaitStateRevision(t, ctx, handle, conversation, written)
	if indexed.Revision != written {
		t.Fatalf("want the index at revision %d before the lane is cut, got %d", written, indexed.Revision)
	}

	lane := handle.Topic(wire.AgentSessions)
	partition := livePartitions(t, lane, ctx)[0]
	cut := false
	for deadline := time.Now().Add(time.Minute); time.Now().Before(deadline) && !cut; {
		if err := lane.DeleteSegments(ctx, partition, 1); err != nil {
			t.Fatalf("delete the oldest segment: %v", err)
		}
		folded, err := handle.Sessions().Open(conversation, "").State().Get(ctx)
		if err != nil {
			t.Fatalf("fold the lane: %v", err)
		}
		cut = !folded.Complete
		if !cut {
			select {
			case <-ctx.Done():
				t.Fatalf("wait for the cut: %v", ctx.Err())
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
	if !cut {
		t.Fatal("the lane still holds its baseline after its oldest segment was deleted")
	}

	held, err := handle.ReconstructState(ctx, conversation)
	if err != nil || held == nil || !held.Equal(indexed.Document) {
		t.Fatalf("want the view's document from a lane without its baseline, got %v, %v", held, err)
	}

	fresh := handle.Sessions().Open(conversation, "").As(planner)
	if _, err := fresh.State().Set(ctx, "after", text("cut")); err != nil {
		t.Fatalf("write from a fresh handle: %v", err)
	}

	continued := awaitStateRevision(t, ctx, handle, conversation, written+1)
	if continued.Revision != written+1 {
		t.Fatalf("want the fresh handle to continue at revision %d, got %d", written+1, continued.Revision)
	}
	want := document("after", text("cut"), "owner", text("planner"), "pad", padding, "phase", text("triage"))
	if !continued.Document.Equal(want) {
		t.Fatalf("the view after the fresh write is not the earlier document plus the new key")
	}
}

// awaitStateRevision reads the indexed state until it reaches revision, or the context ends.
func awaitStateRevision(
	t *testing.T, ctx context.Context, handle *laser.Laser, conversation wire.ConversationID, revision uint64,
) wire.SessionStateView {
	t.Helper()
	var last wire.SessionStateView
	for {
		view, err := handle.Sessions().State(ctx, conversation, 0)
		if err == nil && view != nil {
			last = *view
			if view.Revision >= revision {
				return last
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("the index stayed at revision %d, want %d: %v", last.Revision, revision, err)
		case <-time.After(100 * time.Millisecond):
		}
	}
}
