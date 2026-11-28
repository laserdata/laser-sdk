package laser_test

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

type forkServer struct {
	clusterOfOne
	outcome  wire.ForkOutcome
	failure  *wire.ForkError
	codes    []uint32
	requests [][]byte
}

func (server *forkServer) Close() error { return nil }

func (server *forkServer) SendBinaryRequest(
	_ context.Context, code uint32, payload []byte,
) ([]byte, error) {
	server.codes = append(server.codes, code)
	server.requests = append(server.requests, payload)
	if server.failure != nil {
		return wire.Marshal(wire.ForkReply{Err: server.failure})
	}
	return wire.Marshal(wire.ForkReply{Outcome: &server.outcome})
}

func forkCapabilities() *laser.Capabilities {
	versions := wire.NewOpVersions(
		wire.QueryOpVersion, wire.ControlOpVersion, wire.KvOpVersion, wire.ForkOpVersion)
	capabilities := laser.OpenCapabilities()
	capabilities.Managed = true
	capabilities.Forks = true
	capabilities.Hello = laser.HelloAnswered
	capabilities.Versions = &versions
	return &capabilities
}

func forkHandle(server iggcon.Client, options laser.Options) *laser.Laser {
	if options.Capabilities == nil {
		options.Capabilities = forkCapabilities()
	}
	return laser.FromClient(server, options)
}

func decodeForkRequest[T any](t *testing.T, server *forkServer, index int, target *T) {
	t.Helper()
	decodeManagedTestRequest(t, server.codes[index], server.requests[index], target)
}

// A fork is continuous unless the caller freezes it, because a branch that
// stops seeing the trunk is the surprising choice, not the default one.
func TestGivenANewForkWhenCreatedThenShouldBeContinuousUnlessSevered(t *testing.T) {
	server := &forkServer{outcome: wire.ForkOutcome{
		Kind: wire.ForkCreated,
		Info: &wire.ForkInfo{ForkID: "stream:agents/what-if",
			Kind: wire.ForkContinuous, Status: wire.ForkOpen},
	}}
	handle := forkHandle(server, laser.Options{DefaultStream: "agents"})

	info, err := handle.Fork("what-if").Create().Send(context.Background())
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	// The caller gets its own name back, not the one the deployment stores.
	if info.ForkID != "what-if" {
		t.Errorf("want the caller's own name, got %q", info.ForkID)
	}

	var request wire.ForkCreate
	decodeForkRequest(t, server, 0, &request)
	if request.Kind != wire.ForkContinuous {
		t.Errorf("want a continuous fork by default, got %q", request.Kind)
	}
	if request.ForkID != "stream:agents/what-if" {
		t.Errorf("want the scoped fork id, got %q", request.ForkID)
	}
	// The server that logs the create stamps the freeze points, so every
	// replica folds the same fork. A client must leave them unset.
	if len(request.Points) != 0 {
		t.Errorf("a client must not stamp freeze points, got %+v", request.Points)
	}
}

func TestGivenASeveredForkWhenCreatedThenShouldAskToBeFrozen(t *testing.T) {
	server := &forkServer{outcome: wire.ForkOutcome{
		Kind: wire.ForkCreated, Info: &wire.ForkInfo{ForkID: "what-if",
			Kind: wire.ForkSevered, Status: wire.ForkOpen},
	}}

	if _, err := forkHandle(server, laser.Options{}).Fork("what-if").
		Create().Severed().Tables("events", "users").
		Send(context.Background()); err != nil {
		t.Fatalf("create: %v", err)
	}

	var request wire.ForkCreate
	decodeForkRequest(t, server, 0, &request)
	if request.Kind != wire.ForkSevered {
		t.Errorf("want a severed fork, got %q", request.Kind)
	}
	if len(request.Tables) != 2 {
		t.Errorf("want both tables named, got %+v", request.Tables)
	}
}

// A parent and the tables are resources too, so they are scoped like the fork
// itself or they would address another stream's.
func TestGivenAParentForkWhenCreatedThenShouldScopeItToo(t *testing.T) {
	server := &forkServer{outcome: wire.ForkOutcome{
		Kind: wire.ForkCreated, Info: &wire.ForkInfo{ForkID: "stream:agents/child",
			Kind: wire.ForkContinuous, Status: wire.ForkOpen},
	}}

	if _, err := forkHandle(server, laser.Options{DefaultStream: "agents"}).
		Fork("child").Create().Parent("base").Tables("events").
		Send(context.Background()); err != nil {
		t.Fatalf("create: %v", err)
	}

	var request wire.ForkCreate
	decodeForkRequest(t, server, 0, &request)
	if request.Parent == nil || *request.Parent != "stream:agents/base" {
		t.Errorf("want the scoped parent, got %v", request.Parent)
	}
	if len(request.Tables) != 1 || request.Tables[0] != "stream:agents/events" {
		t.Errorf("want the scoped table, got %+v", request.Tables)
	}
}

func TestGivenAForkWhenPromotedThenShouldReportTheRowsWritten(t *testing.T) {
	server := &forkServer{outcome: wire.ForkOutcome{Kind: wire.ForkPromotedRows, Rows: 17}}

	rows, err := forkHandle(server, laser.Options{}).Fork("what-if").
		Promote(context.Background())
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	if rows != 17 {
		t.Errorf("want 17 rows, got %d", rows)
	}
	if server.codes[0] != wire.AgdxForkPromoteCode {
		t.Errorf("want the promote code, got %d", server.codes[0])
	}
}

// Squashing a fork that was already gone is not an error, and the result says
// which happened.
func TestGivenAForkWhenSquashedThenShouldReportWhetherOneWasThere(t *testing.T) {
	for name, existed := range map[string]bool{"open": true, "already gone": false} {
		t.Run(name, func(t *testing.T) {
			server := &forkServer{outcome: wire.ForkOutcome{
				Kind: wire.ForkDeleted, Deleted: existed}}

			removed, err := forkHandle(server, laser.Options{}).Fork("what-if").
				Squash(context.Background())
			if err != nil {
				t.Fatalf("squash: %v", err)
			}
			if removed != existed {
				t.Errorf("want removed %v, got %v", existed, removed)
			}
		})
	}
}

// A speculative row overlays the trunk row at one coordinate, so the
// coordinate and the projection it belongs to both have to reach the wire.
func TestGivenASpeculativeRowWhenWrittenThenShouldCarryItsCoordinate(t *testing.T) {
	server := &forkServer{outcome: wire.ForkOutcome{Kind: wire.ForkWritten}}

	if err := forkHandle(server, laser.Options{DefaultStream: "agents"}).
		Fork("what-if").PutRow("events", 3, 99).
		Projection("by-kind", 2).
		Field("kind", "click").Metadata("note", "speculative").
		Payload([]byte("body")).
		Send(context.Background()); err != nil {
		t.Fatalf("put row: %v", err)
	}

	var request wire.ForkPut
	decodeForkRequest(t, server, 0, &request)
	if request.PartitionID != 3 || request.Offset != 99 {
		t.Errorf("want the coordinate carried, got partition %d offset %d",
			request.PartitionID, request.Offset)
	}
	if request.Table != "stream:agents/events" {
		t.Errorf("want the scoped table, got %q", request.Table)
	}
	if request.ProjectionID != "stream:agents/by-kind" || request.ProjectionVersion != 2 {
		t.Errorf("want the scoped projection, got %q v%d",
			request.ProjectionID, request.ProjectionVersion)
	}
	if request.Fields["kind"] != "click" || request.Metadata["note"] != "speculative" {
		t.Errorf("want the fields carried, got %+v / %+v", request.Fields, request.Metadata)
	}
	if request.Tombstone {
		t.Error("a row with a value is not a tombstone")
	}
}

func TestGivenATombstoneWhenWrittenThenShouldHideTheTrunkRow(t *testing.T) {
	server := &forkServer{outcome: wire.ForkOutcome{Kind: wire.ForkWritten}}

	if err := forkHandle(server, laser.Options{}).Fork("what-if").
		PutRow("events", 0, 1).Tombstone().Send(context.Background()); err != nil {
		t.Fatalf("put row: %v", err)
	}

	var request wire.ForkPut
	decodeForkRequest(t, server, 0, &request)
	if !request.Tombstone {
		t.Error("want the row marked as a tombstone")
	}
}

// An embedding travels as a JSON array literal, so it has to be rendered the
// way the store reads it back.
func TestGivenAnEmbeddingWhenWrittenThenShouldTravelAsAJSONArray(t *testing.T) {
	server := &forkServer{outcome: wire.ForkOutcome{Kind: wire.ForkWritten}}

	if err := forkHandle(server, laser.Options{}).Fork("what-if").
		PutRow("events", 0, 1).Embedding([]float32{0.5, -1.5}).
		Send(context.Background()); err != nil {
		t.Fatalf("put row: %v", err)
	}

	var request wire.ForkPut
	decodeForkRequest(t, server, 0, &request)
	if request.Embedding == nil || *request.Embedding != "[0.5,-1.5]" {
		t.Errorf("want a JSON array literal, got %v", request.Embedding)
	}
}

// JSON has no spelling for a non-finite number, so one would travel as
// something a reader cannot compare, silently changing which rows a
// nearest-neighbour search returns.
func TestGivenANonFiniteEmbeddingWhenWrittenThenShouldRefuseIt(t *testing.T) {
	for name, component := range map[string]float32{
		"not a number": float32(math.NaN()),
		"infinite":     float32(math.Inf(1)),
	} {
		t.Run(name, func(t *testing.T) {
			server := &forkServer{outcome: wire.ForkOutcome{Kind: wire.ForkWritten}}

			err := forkHandle(server, laser.Options{}).Fork("what-if").
				PutRow("events", 0, 1).Embedding([]float32{0.5, component}).
				Send(context.Background())

			var invalid *laser.InvalidError
			if !errors.As(err, &invalid) {
				t.Fatalf("want an InvalidError, got %T: %v", err, err)
			}
			if !strings.Contains(err.Error(), "finite") {
				t.Errorf("want the reason named, got %v", err)
			}
			if len(server.codes) != 0 {
				t.Error("a refused row must not reach the socket")
			}
		})
	}
}

// A listing answers with the stored names, so each is mapped back, and one
// belonging to another stream is left out rather than handed back unusable.
func TestGivenAForkListingWhenScopedThenShouldReturnOnlyThisStreamsOwn(t *testing.T) {
	parent := "stream:agents/base"
	elsewhere := "stream:billing/base"
	server := &forkServer{outcome: wire.ForkOutcome{
		Kind: wire.ForkListed,
		List: []wire.ForkInfo{
			{ForkID: "stream:agents/what-if", Parent: &parent,
				Kind: wire.ForkContinuous, Status: wire.ForkOpen},
			{ForkID: "stream:billing/theirs", Parent: &elsewhere,
				Kind: wire.ForkContinuous, Status: wire.ForkOpen},
			{ForkID: "unscoped", Kind: wire.ForkContinuous, Status: wire.ForkOpen},
		},
	}}
	handle := forkHandle(server, laser.Options{DefaultStream: "agents"})

	forks, err := handle.Forks(context.Background())
	if err != nil {
		t.Fatalf("forks: %v", err)
	}
	if len(forks) != 1 {
		t.Fatalf("want only this stream's forks, got %+v", forks)
	}
	if forks[0].ForkID != "what-if" {
		t.Errorf("want the caller's own name, got %q", forks[0].ForkID)
	}
	if forks[0].Parent == nil || *forks[0].Parent != "base" {
		t.Errorf("want the parent mapped back too, got %v", forks[0].Parent)
	}
	var request wire.ForkList
	decodeForkRequest(t, server, 0, &request)
	if request.Stream == nil || *request.Stream != "agents" {
		t.Errorf("a scoped listing names its stream, got %v", request.Stream)
	}
}

func TestGivenNoForkSurfaceWhenCalledThenShouldRefuseIt(t *testing.T) {
	capabilities := forkCapabilities()
	capabilities.Forks = false
	server := &forkServer{}

	_, err := forkHandle(server, laser.Options{Capabilities: capabilities}).
		Fork("what-if").Promote(context.Background())

	var unsupported *laser.UnsupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("want an UnsupportedError, got %T: %v", err, err)
	}
	if len(server.codes) != 0 {
		t.Error("an unsupported call must not reach the socket")
	}
}

func TestGivenAnInvalidForkIDWhenCreatedThenShouldRefuseBeforeSending(t *testing.T) {
	server := &forkServer{}

	_, err := forkHandle(server, laser.Options{}).Fork("").Create().Send(context.Background())

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
	if len(server.codes) != 0 {
		t.Error("an invalid fork id must not reach the socket")
	}
}

func TestGivenAForkFailureWhenReportedThenShouldCarryItsKind(t *testing.T) {
	server := &forkServer{failure: &wire.ForkError{
		Kind: wire.ForkNotFound, Message: "no such fork"}}

	_, err := forkHandle(server, laser.Options{}).Fork("what-if").
		Promote(context.Background())

	var failure *laser.ForkError
	if !errors.As(err, &failure) {
		t.Fatalf("want a ForkError, got %T: %v", err, err)
	}
	if failure.Kind() != wire.ForkNotFound {
		t.Errorf("want the not-found kind, got %d", failure.Kind())
	}
}

// An outcome shaped for another operation means the client and the server
// disagree about the contract.
func TestGivenAForkOutcomeForAnotherOperationWhenReadThenShouldRefuseIt(t *testing.T) {
	server := &forkServer{outcome: wire.ForkOutcome{Kind: wire.ForkWritten}}

	_, err := forkHandle(server, laser.Options{}).Fork("what-if").
		Promote(context.Background())

	var codec *laser.CodecError
	if !errors.As(err, &codec) {
		t.Fatalf("want a CodecError, got %T: %v", err, err)
	}
}
