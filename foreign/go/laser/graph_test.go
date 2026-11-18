package laser_test

import (
	"context"
	"errors"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

type graphServer struct {
	iggcon.Client
	result   wire.GraphResult
	failure  *wire.GraphError
	codes    []uint32
	requests [][]byte
}

func (server *graphServer) Close() error { return nil }

func (server *graphServer) SendBinaryRequest(
	_ context.Context, code uint32, payload []byte,
) ([]byte, error) {
	server.codes = append(server.codes, code)
	server.requests = append(server.requests, payload)
	if server.failure != nil {
		return wire.Marshal(wire.ErrGraphReply(*server.failure))
	}
	return wire.Marshal(wire.OkGraphReply(server.result))
}

func graphCapabilities() *laser.Capabilities {
	versions := wire.NewOpVersions(
		wire.QueryOpVersion, wire.ControlOpVersion, wire.KvOpVersion, wire.ForkOpVersion).
		WithGraph(wire.GraphOpVersion)
	capabilities := laser.OpenCapabilities()
	capabilities.Managed = true
	capabilities.Graph = true
	capabilities.Hello = laser.HelloAnswered
	capabilities.Versions = &versions
	return &capabilities
}

func graphHandle(server iggcon.Client, options laser.Options) *laser.Laser {
	if options.Capabilities == nil {
		options.Capabilities = graphCapabilities()
	}
	return laser.FromClient(server, options)
}

func decodeGraphRequest[T any](t *testing.T, server *graphServer, index int, target *T) {
	t.Helper()
	body := server.requests[index]
	if wire.IsIdempotentManagedRequest(server.codes[index]) {
		var envelope wire.ManagedRequestEnvelope
		if err := wire.Unmarshal(body, &envelope); err != nil {
			t.Fatalf("decode the managed envelope of request %d: %v", index, err)
		}
		body = envelope.Payload
	}
	if err := wire.Unmarshal(body, target); err != nil {
		t.Fatalf("decode request %d: %v", index, err)
	}
}

func TestGivenATraversalWhenBuiltThenShouldCarryItsStartAndHops(t *testing.T) {
	server := &graphServer{}
	node := wire.ContentNodeID("entity", []byte("operator:42"))

	if _, err := graphHandle(server, laser.Options{DefaultStream: "agents"}).
		Graph("knowledge").StartIDs(node).Out("opened").In("assigned").
		Limit(25).Fetch(context.Background()); err != nil {
		t.Fatalf("fetch: %v", err)
	}

	var query wire.GraphQuery
	decodeGraphRequest(t, server, 0, &query)
	if query.Graph != "stream:agents/knowledge" {
		t.Errorf("want the scoped graph, got %q", query.Graph)
	}
	if query.Start.Kind != wire.StartIDs || len(query.Start.IDs) != 1 {
		t.Errorf("want the start carried, got %+v", query.Start)
	}
	if len(query.Traverse) != 2 {
		t.Fatalf("want both hops, got %d", len(query.Traverse))
	}
	// Outgoing is the default direction and is omitted on the wire, so it
	// comes back empty and IsOut is what reads it.
	if !query.Traverse[0].Dir.IsOut() || *query.Traverse[0].EdgeType != "opened" {
		t.Errorf("want the outgoing hop first, got %+v", query.Traverse[0])
	}
	if query.Traverse[1].Dir != wire.EdgeIn || *query.Traverse[1].EdgeType != "assigned" {
		t.Errorf("want the incoming hop second, got %+v", query.Traverse[1])
	}
	if query.Limit != 25 {
		t.Errorf("want the limit carried, got %d", query.Limit)
	}
	if server.codes[0] != wire.AgdxGraphQueryCode {
		t.Errorf("want the graph query code, got %d", server.codes[0])
	}
}

// A traversal with no start reaches nothing rather than everything, which is
// the safe reading of a query the caller never finished.
func TestGivenNoStartWhenFetchedThenShouldReachNothing(t *testing.T) {
	server := &graphServer{}

	if _, err := graphHandle(server, laser.Options{}).Graph("knowledge").
		Fetch(context.Background()); err != nil {
		t.Fatalf("fetch: %v", err)
	}

	var query wire.GraphQuery
	decodeGraphRequest(t, server, 0, &query)
	if query.Start.Kind != wire.StartIDs || len(query.Start.IDs) != 0 {
		t.Errorf("want an empty id start, got %+v", query.Start)
	}
}

func TestGivenAHopPastTheDepthCapWhenFetchedThenShouldRefuseBeforeSending(t *testing.T) {
	server := &graphServer{}

	_, err := graphHandle(server, laser.Options{}).Graph("knowledge").
		StartIDs(wire.ContentNodeID("entity", []byte("a"))).
		Hop("knows", wire.EdgeOut, wire.MaxGraphTraverseDepth+1).
		Fetch(context.Background())

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
	if len(server.codes) != 0 {
		t.Error("a refused traversal must not reach the socket")
	}
}

// A valid-time read keeps only edges whose window contains the instant, which
// is how a question about the past is asked.
func TestGivenAValidTimeReadWhenFetchedThenShouldCarryTheInstant(t *testing.T) {
	server := &graphServer{}

	if _, err := graphHandle(server, laser.Options{}).Graph("knowledge").
		StartIDs(wire.ContentNodeID("entity", []byte("a"))).
		AsOf(1_700_000_000_000_000).ReturnPaths().
		Fetch(context.Background()); err != nil {
		t.Fatalf("fetch: %v", err)
	}

	var query wire.GraphQuery
	decodeGraphRequest(t, server, 0, &query)
	if query.AsOf == nil || *query.AsOf != 1_700_000_000_000_000 {
		t.Errorf("want the instant carried, got %v", query.AsOf)
	}
	if query.Return != wire.ReturnPaths {
		t.Errorf("want paths returned, got %q", query.Return)
	}
}

func TestGivenNearestStartWhenFetchedThenShouldCarryTheEmbedding(t *testing.T) {
	server := &graphServer{}

	if _, err := graphHandle(server, laser.Options{}).Graph("knowledge").
		StartNearest([]float32{0.1, 0.2}, 5).Fetch(context.Background()); err != nil {
		t.Fatalf("fetch: %v", err)
	}

	var query wire.GraphQuery
	decodeGraphRequest(t, server, 0, &query)
	if query.Start.Kind != wire.StartNearest || query.Start.Nearest == nil {
		t.Fatalf("want a nearest start, got %+v", query.Start)
	}
	if query.Start.Nearest.K != 5 || len(query.Start.Nearest.Embedding) != 2 {
		t.Errorf("want the seed carried, got %+v", query.Start.Nearest)
	}
}

func TestGivenANeighborReadWhenSentThenShouldCarryItsDirectionAndDepth(t *testing.T) {
	server := &graphServer{}
	node := wire.ContentNodeID("entity", []byte("operator:42"))

	if _, err := graphHandle(server, laser.Options{}).Graph("knowledge").
		Neighbors(context.Background(), node, wire.EdgeBoth, "knows", 2); err != nil {
		t.Fatalf("neighbors: %v", err)
	}

	var request wire.GraphNeighbors
	decodeGraphRequest(t, server, 0, &request)
	if request.Dir != wire.EdgeBoth || request.Depth != 2 {
		t.Errorf("want both directions at depth 2, got %q depth %d", request.Dir, request.Depth)
	}
	if request.EdgeType == nil || *request.EdgeType != "knows" {
		t.Errorf("want the edge type carried, got %v", request.EdgeType)
	}
	if server.codes[0] != wire.AgdxGraphNeighborsCode {
		t.Errorf("want the neighbors code, got %d", server.codes[0])
	}
}

// An empty edge type follows any type, so it must be absent on the wire rather
// than sent as a type named by the empty string.
func TestGivenNoEdgeTypeWhenReadingNeighborsThenShouldFollowAnyType(t *testing.T) {
	server := &graphServer{}

	if _, err := graphHandle(server, laser.Options{}).Graph("knowledge").
		Neighbors(context.Background(),
			wire.ContentNodeID("entity", []byte("a")), wire.EdgeOut, "", 1); err != nil {
		t.Fatalf("neighbors: %v", err)
	}

	var request wire.GraphNeighbors
	decodeGraphRequest(t, server, 0, &request)
	if request.EdgeType != nil {
		t.Errorf("want no edge type, got %q", *request.EdgeType)
	}
}

// Re-linking the same triple converges on the same nodes and edge, so it is
// safe to call on every observation.
func TestGivenTheSameTripleWhenLinkedTwiceThenShouldAddressTheSameElements(t *testing.T) {
	server := &graphServer{}
	handle := graphHandle(server, laser.Options{})

	for range 2 {
		if err := handle.Graph("knowledge").
			Link(context.Background(), "operator:42", "opened", "ticket:7"); err != nil {
			t.Fatalf("link: %v", err)
		}
	}

	var first, second wire.GraphUpsert
	decodeGraphRequest(t, server, 0, &first)
	decodeGraphRequest(t, server, 1, &second)
	if len(first.Nodes) != 2 || len(first.Edges) != 1 {
		t.Fatalf("want two nodes and an edge, got %d / %d", len(first.Nodes), len(first.Edges))
	}
	if first.Edges[0].ID != second.Edges[0].ID {
		t.Error("the same triple must address the same edge")
	}
	if first.Nodes[0].ID != second.Nodes[0].ID {
		t.Error("the same entity must address the same node")
	}
	if first.Edges[0].EdgeType != "opened" {
		t.Errorf("want the relation carried, got %q", first.Edges[0].EdgeType)
	}
}

// The provenance a handle carries is stamped on what it writes, so the
// deployment records which component and record each fact came from.
func TestGivenAProducerAndSourceWhenUpsertingThenShouldStampThem(t *testing.T) {
	server := &graphServer{}
	producer := wire.ProducerInfo{Name: "extractor", Version: "2.0.0"}

	if err := graphHandle(server, laser.Options{}).Graph("knowledge").
		ProducedBy(producer).
		Link(context.Background(), "a", "knows", "b"); err != nil {
		t.Fatalf("link: %v", err)
	}

	var upsert wire.GraphUpsert
	decodeGraphRequest(t, server, 0, &upsert)
	if upsert.Edges[0].Producer == nil || upsert.Edges[0].Producer.Name != "extractor" {
		t.Errorf("want the producer stamped, got %+v", upsert.Edges[0].Producer)
	}
}

// A producer the caller already set on an edge is its own, so the handle's
// must not overwrite it.
func TestGivenAnEdgeWithItsOwnProducerWhenUpsertedThenShouldKeepIt(t *testing.T) {
	server := &graphServer{}
	from := wire.EntityNode("entity", "a")
	to := wire.EntityNode("entity", "b")
	edge := wire.RelateNodes(from, "knows", to)
	edge.Producer = &wire.ProducerInfo{Name: "its-own", Version: "1.0.0"}

	if err := graphHandle(server, laser.Options{}).Graph("knowledge").
		ProducedBy(wire.ProducerInfo{Name: "the-handle", Version: "1.0.0"}).
		Upsert(context.Background(),
			[]wire.GraphNode{from, to}, []wire.GraphEdge{edge}); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	var upsert wire.GraphUpsert
	decodeGraphRequest(t, server, 0, &upsert)
	if upsert.Edges[0].Producer.Name != "its-own" {
		t.Errorf("want the edge's own producer kept, got %q", upsert.Edges[0].Producer.Name)
	}
}

// Stamping must not reach back into the caller's slice, or a retry would see
// an edge the caller never built.
func TestGivenAnUpsertWhenStampedThenShouldNotMutateTheCallersEdges(t *testing.T) {
	server := &graphServer{}
	from := wire.EntityNode("entity", "a")
	to := wire.EntityNode("entity", "b")
	edges := []wire.GraphEdge{wire.RelateNodes(from, "knows", to)}

	if err := graphHandle(server, laser.Options{}).Graph("knowledge").
		ProducedBy(wire.ProducerInfo{Name: "extractor", Version: "1.0.0"}).
		Upsert(context.Background(), []wire.GraphNode{from, to}, edges); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	if edges[0].Producer != nil {
		t.Error("stamping must not reach back into the caller's slice")
	}
}

func TestGivenASessionWhenUpsertingThenShouldLinkTheWrite(t *testing.T) {
	server := &graphServer{}
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 7})}
	session := wire.SessionRef{Stream: "agents", Session: conversation}

	if err := graphHandle(server, laser.Options{}).Graph("knowledge").
		InSession(session).Link(context.Background(), "a", "knows", "b"); err != nil {
		t.Fatalf("link: %v", err)
	}

	var upsert wire.GraphUpsert
	decodeGraphRequest(t, server, 0, &upsert)
	if upsert.Session == nil || upsert.Session.Session != conversation {
		t.Errorf("want the session linked, got %+v", upsert.Session)
	}
}

func TestGivenNoGraphSurfaceWhenCalledThenShouldRefuseIt(t *testing.T) {
	capabilities := graphCapabilities()
	capabilities.Graph = false
	server := &graphServer{}

	_, err := graphHandle(server, laser.Options{Capabilities: capabilities}).
		Graph("knowledge").StartIDs(wire.ContentNodeID("entity", []byte("a"))).
		Fetch(context.Background())

	var unsupported *laser.UnsupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("want an UnsupportedError, got %T: %v", err, err)
	}
	if len(server.codes) != 0 {
		t.Error("an unsupported call must not reach the socket")
	}
}

func TestGivenAGraphFailureWhenReportedThenShouldCarryItsKind(t *testing.T) {
	server := &graphServer{failure: &wire.GraphError{
		Kind: wire.GraphNotFound, Message: "no such graph"}}

	_, err := graphHandle(server, laser.Options{}).Graph("knowledge").
		StartIDs(wire.ContentNodeID("entity", []byte("a"))).Fetch(context.Background())

	var failure *laser.GraphError
	if !errors.As(err, &failure) {
		t.Fatalf("want a GraphError, got %T: %v", err, err)
	}
	if failure.Kind() != wire.GraphNotFound {
		t.Errorf("want the not-found kind, got %d", failure.Kind())
	}
}

// An empty edge type follows any type, so it must be absent on the wire rather
// than sent as a type named by the empty string, which would match nothing.
func TestGivenAHopWithNoEdgeTypeWhenFetchedThenShouldFollowAnyType(t *testing.T) {
	server := &graphServer{}

	if _, err := graphHandle(server, laser.Options{}).Graph("knowledge").
		StartIDs(wire.ContentNodeID("entity", []byte("a"))).
		Hop("", wire.EdgeBoth, 3).Fetch(context.Background()); err != nil {
		t.Fatalf("fetch: %v", err)
	}

	var query wire.GraphQuery
	decodeGraphRequest(t, server, 0, &query)
	if len(query.Traverse) != 1 {
		t.Fatalf("want one hop, got %d", len(query.Traverse))
	}
	if query.Traverse[0].EdgeType != nil {
		t.Errorf("want no edge type, got %q", *query.Traverse[0].EdgeType)
	}
	if query.Traverse[0].Max != 3 {
		t.Errorf("want the hop bound carried, got %d", query.Traverse[0].Max)
	}
}
