package laser_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenACompleteLaneWhenSeedingThenShouldUseTheFoldAndNeverAskTheView(t *testing.T) {
	server := &viewServer{view: aheadView()}
	server.session = conversation(1)
	rivalDelta(t, &server.stateServer, 0, "01ARZ3NDEKTSV4RRFFQ69G5FAV", nil,
		wire.AddOperation("/n", text("0")))
	state := viewSession(t, server, true).State()

	if _, err := state.Set(context.Background(), "k", text("v")); err != nil {
		t.Fatalf("set: %v", err)
	}

	if base := lastBase(t, server); base != 1 {
		t.Fatalf("want the write to continue from the fold at revision 1, got %d", base)
	}
	if server.viewed() != 0 {
		t.Fatalf("a complete fold must not ask the managed view, got %d reads", server.viewed())
	}
}

func TestGivenALaneWithoutItsBaselineWhenSeedingThenShouldContinueFromTheManagedView(t *testing.T) {
	server := &viewServer{view: aheadView()}
	truncatedLane(t, server)
	state := viewSession(t, server, true).State()

	if _, err := state.Set(context.Background(), "k", text("v")); err != nil {
		t.Fatalf("set: %v", err)
	}

	deltas := server.deltas(t)
	written := deltas[len(deltas)-1]
	if written.BaseRevision != 8 {
		t.Fatalf("want the write to continue from the view at revision 8, got %d", written.BaseRevision)
	}
	if written.BaseOpID == nil || *written.BaseOpID != viewHead {
		t.Fatalf("want the write chained to the view's head, got %v", written.BaseOpID)
	}
	if server.viewed() != 1 {
		t.Fatalf("want one read of the managed view, got %d", server.viewed())
	}
	var request wire.SessionStateRequest
	decodeManagedTestRequest(t, wire.AgdxSessionStateCode, server.viewRequest(), &request)
	want := wire.SessionStateRequest{Stream: "agents", ID: conversation(1)}
	if request != want {
		t.Fatalf("got the view request %+v, want %+v", request, want)
	}
}

func TestGivenADeploymentWithoutSessionsWhenSeedingFromATruncatedLaneThenShouldKeepTheFold(t *testing.T) {
	server := &viewServer{view: aheadView()}
	truncatedLane(t, server)
	state := viewSession(t, server, false).State()

	if _, err := state.Set(context.Background(), "k", text("v")); err != nil {
		t.Fatalf("set: %v", err)
	}

	if base := lastBase(t, server); base != 0 {
		t.Fatalf("want the fold kept at revision 0, got %d", base)
	}
	if server.viewed() != 0 {
		t.Fatalf("a deployment without sessions has no view to ask, got %d reads", server.viewed())
	}
}

func TestGivenAViewWhoseBackendIsOutOfReachWhenSeedingThenShouldAskOnceAndKeepTheFold(t *testing.T) {
	server := &viewServer{unreachable: true}
	truncatedLane(t, server)
	state := viewSession(t, server, true).State()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if _, err := state.Set(ctx, "k", text("v")); err != nil {
		t.Fatalf("set: %v", err)
	}

	if base := lastBase(t, server); base != 0 {
		t.Fatalf("want the fold kept at revision 0, got %d", base)
	}
	if server.viewed() != 1 {
		t.Fatalf("want the view asked once, got %d", server.viewed())
	}
}

func TestGivenASessionTheIndexDoesNotKnowWhenSeedingThenShouldKeepTheFold(t *testing.T) {
	server := &viewServer{}
	truncatedLane(t, server)
	state := viewSession(t, server, true).State()

	if _, err := state.Set(context.Background(), "k", text("v")); err != nil {
		t.Fatalf("set: %v", err)
	}

	if base := lastBase(t, server); base != 0 {
		t.Fatalf("want the fold kept at revision 0, got %d", base)
	}
}

func TestGivenAHandleSeededFromTheViewWhenSnapshottingThenShouldHoldTheViewsDocument(t *testing.T) {
	server := &viewServer{view: aheadView()}
	truncatedLane(t, server)
	state := viewSession(t, server, true).State()

	if _, err := state.Snapshot(context.Background()); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	snapshot := lastSnapshot(t, server)
	want := wire.StateSnapshot{BaseRevision: 8, Document: document("n", text("7")), BaseOpID: aheadView().HeadOpID}
	if snapshot.BaseRevision != want.BaseRevision || !snapshot.Document.Equal(want.Document) ||
		snapshot.BaseOpID == nil || *snapshot.BaseOpID != *want.BaseOpID {
		t.Fatalf("got the snapshot %+v, want %+v", snapshot, want)
	}
}

func TestGivenALaneWithoutItsBaselineWhenTheStateIsReconstructedThenShouldReturnTheViewsDocument(t *testing.T) {
	server := &viewServer{view: aheadView()}
	truncatedLane(t, server)
	handle := viewHandle(server, true)

	held, err := handle.ReconstructState(context.Background(), conversation(1))

	if err != nil {
		t.Fatalf("reconstruct: %v", err)
	}
	if held == nil || !held.Equal(document("n", text("7"))) {
		t.Fatalf("want the view's document, got %v", held)
	}
}

func TestGivenNoStateRecordWhenTheStateIsReconstructedThenShouldReturnNone(t *testing.T) {
	server := &viewServer{}
	server.session = conversation(1)
	handle := viewHandle(server, false)

	held, err := handle.ReconstructState(context.Background(), conversation(1))

	if err != nil || held != nil {
		t.Fatalf("want no document before any state record, got %v, %v", held, err)
	}
}

func TestGivenACompleteLaneWhenTheStateIsReconstructedThenShouldReturnTheFold(t *testing.T) {
	server := &viewServer{view: aheadView()}
	server.session = conversation(1)
	rivalDelta(t, &server.stateServer, 0, "01ARZ3NDEKTSV4RRFFQ69G5FAV", nil,
		wire.AddOperation("/n", text("0")))
	handle := viewHandle(server, true)

	held, err := handle.ReconstructState(context.Background(), conversation(1))

	if err != nil {
		t.Fatalf("reconstruct: %v", err)
	}
	if held == nil || !held.Equal(document("n", text("0"))) {
		t.Fatalf("want the folded document, got %v", held)
	}
}

const viewHead = "01ARZ3NDEKTSV4RRFFQ69G5FB8"

// viewServer is a session lane beside a managed state view.
type viewServer struct {
	stateServer
	viewMutex sync.Mutex
	// view is what the managed view answers with, nil to answer not found.
	view *wire.SessionStateView
	// unreachable makes the view answer that its backend is out of reach.
	unreachable bool
	viewReads   int
	lastRequest []byte
}

func (server *viewServer) SendBinaryRequest(
	_ context.Context, code uint32, payload []byte,
) ([]byte, error) {
	if code == wire.AgdxSessionSourcesCode {
		return wire.Marshal(wire.SessionReply{Outcome: &wire.SessionOutcome{
			Kind: wire.SessionSourcesOutcome,
			Sources: &wire.SessionSources{Lane: &wire.LaneIdentity{
				TopicID: 1, Generation: 1, Partitions: 1}},
		}})
	}
	if code != wire.AgdxSessionStateCode {
		return nil, nil
	}
	server.viewMutex.Lock()
	server.viewReads++
	server.lastRequest = payload
	view, unreachable := server.view, server.unreachable
	server.viewMutex.Unlock()
	if unreachable {
		return wire.Marshal(wire.NewCommandError(wire.ResultBackendUnavailable, "the backend is out of reach"))
	}
	if view == nil {
		return wire.Marshal(wire.SessionReply{Err: &wire.SessionError{
			Kind: wire.SessionNotFound, Message: "no such session"}})
	}
	return wire.Marshal(wire.SessionReply{Outcome: &wire.SessionOutcome{
		Kind: wire.SessionStateOutcome, State: view}})
}

func (server *viewServer) viewed() int {
	server.viewMutex.Lock()
	defer server.viewMutex.Unlock()
	return server.viewReads
}

func (server *viewServer) viewRequest() []byte {
	server.viewMutex.Lock()
	defer server.viewMutex.Unlock()
	return server.lastRequest
}

func viewHandle(server *viewServer, indexed bool) *laser.Laser {
	capabilities := laser.Capabilities{Managed: indexed, Sessions: indexed}
	return laser.FromClient(server, laser.Options{
		DefaultStream: "agents", Capabilities: &capabilities,
	})
}

func viewSession(t *testing.T, server *viewServer, indexed bool) *laser.Session {
	t.Helper()
	server.session = conversation(1)
	return viewHandle(server, indexed).Sessions().Open(server.session, "").As(agentID(t, "writer"))
}

// aheadView is a managed view past anything the truncated lane folds to.
func aheadView() *wire.SessionStateView {
	head := viewHead
	return &wire.SessionStateView{
		Revision: 8, Document: document("n", text("7")), Complete: true, HeadOpID: &head,
	}
}

// truncatedLane holds only deltas based on revisions it never reached, so it
// folds incomplete at revision 0.
func truncatedLane(t *testing.T, server *viewServer) {
	t.Helper()
	server.session = conversation(1)
	rivalDelta(t, &server.stateServer, 6, "01ARZ3NDEKTSV4RRFFQ69G5FB6", nil,
		wire.AddOperation("/n", text("6")))
	rivalDelta(t, &server.stateServer, 7, "01ARZ3NDEKTSV4RRFFQ69G5FB7", nil,
		wire.AddOperation("/n", text("7")))
}

func lastBase(t *testing.T, server *viewServer) uint64 {
	t.Helper()
	deltas := server.deltas(t)
	return deltas[len(deltas)-1].BaseRevision
}

func lastSnapshot(t *testing.T, server *viewServer) wire.StateSnapshot {
	t.Helper()
	server.mutex.Lock()
	defer server.mutex.Unlock()
	var envelope wire.AgentEnvelope
	if err := wire.Unmarshal(server.lane[len(server.lane)-1].Payload, &envelope); err != nil {
		t.Fatalf("decode the envelope: %v", err)
	}
	if envelope.Operation == nil || *envelope.Operation != wire.OperationStateSnapshot {
		t.Fatalf("want a snapshot last on the lane, got %v", envelope.Operation)
	}
	var snapshot wire.StateSnapshot
	if err := wire.Unmarshal(envelope.Body, &snapshot); err != nil {
		t.Fatalf("decode the snapshot: %v", err)
	}
	return snapshot
}
