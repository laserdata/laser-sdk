//go:build integration

package laser_test

import (
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

func TestGivenALiveSessionWhenItsStateIsReconstructedThenShouldReturnTheDocumentItWrote(t *testing.T) {
	ctx := liveContext(t)
	handle := liveScratch(t, dialLive(t), ctx, "laser-session-state").Laser()
	if err := handle.Bootstrap(ctx, 1, laser.ExpireAfter(time.Hour)); err != nil {
		t.Fatalf("bootstrap the stream: %v", err)
	}
	session := handle.Sessions().Start().As(agentID(t, "planner"))
	if _, _, err := session.Begin(ctx); err != nil {
		t.Fatalf("begin: %v", err)
	}
	before, err := handle.ReconstructState(ctx, session.Conversation())
	if err != nil || before != nil {
		t.Fatalf("want no document before any state record, got %v, %v", before, err)
	}
	if _, err := session.State().Set(ctx, "phase", text("triage")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if _, err := session.State().Set(ctx, "owner", text("planner")); err != nil {
		t.Fatalf("set: %v", err)
	}

	held, err := handle.ReconstructState(ctx, session.Conversation())

	if err != nil {
		t.Fatalf("reconstruct: %v", err)
	}
	want := document("owner", text("planner"), "phase", text("triage"))
	if held == nil || !held.Equal(want) {
		t.Fatalf("got %v, want %v", held, want)
	}
}
