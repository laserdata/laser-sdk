package laser

import (
	"context"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// Graph is a fluent traversal of one knowledge graph.
//
// Set a start, add hops, pick what to return, and finish with Fetch. A handle
// carries the traversal being built, so each step returns it and nothing is
// sent until the terminal call.
type Graph struct {
	laser      *Laser
	name       string
	start      *wire.GraphStart
	hops       []wire.Hop
	nodeFilter *wire.Filter
	edgeFilter *wire.Filter
	want       wire.GraphReturn
	limit      uint64
	asOf       *uint64
	session    *wire.SessionRef
	producer   *wire.ProducerInfo
	source     *wire.SourceRef
	err        error
}

// Graph opens a traversal of a named knowledge graph.
func (laser *Laser) Graph(name string) *Graph {
	return &Graph{
		laser: laser,
		name:  laser.ResourceName(name),
		want:  wire.ReturnNodes,
		limit: wire.DefaultRecallLimit,
	}
}

// Name is the graph this handle addresses, as the deployment names it.
func (graph *Graph) Name() string { return graph.name }

func (graph *Graph) fail(err error) *Graph {
	if graph.err == nil {
		graph.err = err
	}
	return graph
}

// StartIDs begins the traversal at explicit nodes.
func (graph *Graph) StartIDs(ids ...wire.NodeID) *Graph {
	start := wire.StartAtIDs(ids...)
	graph.start = &start
	return graph
}

// StartMatch begins at the nodes a predicate matches.
func (graph *Graph) StartMatch(filter wire.Filter) *Graph {
	start := wire.StartAtMatch(filter)
	graph.start = &start
	return graph
}

// StartNearest begins at the k nodes nearest an embedding.
func (graph *Graph) StartNearest(embedding []float32, k uint64) *Graph {
	start := wire.StartAtNearest(embedding, k)
	graph.start = &start
	return graph
}

// Out follows outgoing edges of a type, one hop.
func (graph *Graph) Out(edgeType string) *Graph {
	return graph.Hop(edgeType, wire.EdgeOut, 1)
}

// In follows incoming edges of a type, one hop.
func (graph *Graph) In(edgeType string) *Graph {
	return graph.Hop(edgeType, wire.EdgeIn, 1)
}

// Both follows edges of a type in either direction, one hop.
func (graph *Graph) Both(edgeType string) *Graph {
	return graph.Hop(edgeType, wire.EdgeBoth, 1)
}

// Hop adds one traversal step, following edges of a type in a direction up to
// max times. An empty edge type follows any type.
func (graph *Graph) Hop(edgeType string, dir wire.EdgeDir, max uint32) *Graph {
	hop := wire.Hop{Dir: dir, Max: max}
	if edgeType != "" {
		hop.EdgeType = &edgeType
	}
	graph.hops = append(graph.hops, hop)
	return graph
}

// NodeFilter keeps only nodes a predicate matches.
func (graph *Graph) NodeFilter(filter wire.Filter) *Graph {
	graph.nodeFilter = &filter
	return graph
}

// EdgeFilter keeps only edges a predicate matches.
func (graph *Graph) EdgeFilter(filter wire.Filter) *Graph {
	graph.edgeFilter = &filter
	return graph
}

// ReturnNodes, ReturnEdges, ReturnPaths and ReturnTriplets pick what the
// traversal returns. Nodes is the default.
func (graph *Graph) ReturnNodes() *Graph    { graph.want = wire.ReturnNodes; return graph }
func (graph *Graph) ReturnEdges() *Graph    { graph.want = wire.ReturnEdges; return graph }
func (graph *Graph) ReturnPaths() *Graph    { graph.want = wire.ReturnPaths; return graph }
func (graph *Graph) ReturnTriplets() *Graph { graph.want = wire.ReturnTriplets; return graph }

// Limit bounds the elements a traversal returns.
func (graph *Graph) Limit(limit uint64) *Graph {
	graph.limit = limit
	return graph
}

// AsOf reads the graph as it stood at an instant, keeping only edges whose
// valid-time window contains it. The system-time axis is the upsert's log
// offset, so this reads the facts rather than the writes.
func (graph *Graph) AsOf(micros uint64) *Graph {
	graph.asOf = &micros
	return graph
}

// InSession links every upsert from this handle to a session, so the
// deployment records which session wrote each element.
func (graph *Graph) InSession(session wire.SessionRef) *Graph {
	graph.session = &session
	return graph
}

// ProducedBy names the component that produced the facts this handle writes.
func (graph *Graph) ProducedBy(producer wire.ProducerInfo) *Graph {
	graph.producer = &producer
	return graph
}

// SourcedFrom names the record the facts this handle writes came from.
func (graph *Graph) SourcedFrom(source wire.SourceRef) *Graph {
	graph.source = &source
	return graph
}

// Query renders the traversal as the request it will send, without sending it.
func (graph *Graph) Query() wire.GraphQuery {
	query := wire.GraphQuery{
		V:          wire.GraphOpVersion,
		Graph:      graph.name,
		Traverse:   graph.hops,
		NodeFilter: graph.nodeFilter,
		EdgeFilter: graph.edgeFilter,
		Return:     graph.want,
		Limit:      graph.limit,
		AsOf:       graph.asOf,
	}
	if graph.start != nil {
		query.Start = *graph.start
	} else {
		// A traversal with no start reaches nothing rather than everything,
		// which is the safe reading of an unfinished query.
		query.Start = wire.StartAtIDs()
	}
	if stream := graph.laser.resourceStream(); stream != "" {
		query.Stream = &stream
	}
	return query
}

// Fetch runs the traversal.
func (graph *Graph) Fetch(ctx context.Context) (*wire.GraphResult, error) {
	if graph.err != nil {
		return nil, graph.err
	}
	query := graph.Query()
	if err := query.Validate(); err != nil {
		return nil, &InvalidError{Reason: err.Error()}
	}
	return graph.execute(ctx, wire.AgdxGraphQueryCode, query)
}

// Neighbors reads the nodes reachable from one node over an edge type, in a
// direction, following the same hop depth times. The cheap common traversal.
func (graph *Graph) Neighbors(
	ctx context.Context, node wire.NodeID, dir wire.EdgeDir, edgeType string, depth uint32,
) (*wire.GraphResult, error) {
	if graph.err != nil {
		return nil, graph.err
	}
	request := wire.GraphNeighbors{
		V:     wire.GraphOpVersion,
		Graph: graph.name,
		Node:  node,
		Dir:   dir,
		Depth: depth,
		Limit: graph.limit,
		AsOf:  graph.asOf,
	}
	if edgeType != "" {
		request.EdgeType = &edgeType
	}
	if stream := graph.laser.resourceStream(); stream != "" {
		request.Stream = &stream
	}
	if err := request.Validate(); err != nil {
		return nil, &InvalidError{Reason: err.Error()}
	}
	return graph.execute(ctx, wire.AgdxGraphNeighborsCode, request)
}

// Upsert writes nodes and edges into the graph.
//
// Ids are content-addressed, so re-applying the same extraction converges on
// the same elements instead of duplicating them. That is what lets a projector
// replay without the graph growing.
func (graph *Graph) Upsert(
	ctx context.Context, nodes []wire.GraphNode, edges []wire.GraphEdge,
) error {
	if graph.err != nil {
		return graph.err
	}
	stamped := make([]wire.GraphEdge, len(edges))
	copy(stamped, edges)
	for index := range stamped {
		if graph.producer != nil && stamped[index].Producer == nil {
			stamped[index].Producer = graph.producer
		}
		if graph.source != nil && stamped[index].Source == nil {
			stamped[index].Source = graph.source
		}
	}
	request := wire.GraphUpsert{
		V:       wire.GraphOpVersion,
		Graph:   graph.name,
		Session: graph.session,
		Nodes:   nodes,
		Edges:   stamped,
	}
	if err := request.Validate(); err != nil {
		return &InvalidError{Reason: err.Error()}
	}
	_, err := graph.execute(ctx, wire.AgdxGraphUpsertCode, request)
	return err
}

// Link relates two entities in one call, upserting both content-addressed
// entity nodes and the typed edge between them.
//
// Re-linking the same triple converges on the same nodes and edge, so it is
// safe to call on every observation.
func (graph *Graph) Link(ctx context.Context, from, relation, to string) error {
	fromNode := wire.EntityNode("entity", from)
	toNode := wire.EntityNode("entity", to)
	return graph.Upsert(ctx,
		[]wire.GraphNode{fromNode, toNode},
		[]wire.GraphEdge{wire.RelateNodes(fromNode, relation, toNode)})
}

// execute sends one graph command and opens its reply.
func (graph *Graph) execute(
	ctx context.Context, code uint32, request any,
) (*wire.GraphResult, error) {
	if !graph.laser.capabilities.Managed || !graph.laser.capabilities.Graph {
		return nil, &UnsupportedError{Surface: "the graph surface"}
	}
	if err := graph.laser.requireOpVersion("graph", wire.GraphOpVersion,
		func(versions wire.OpVersions) uint32 { return versions.Graph }); err != nil {
		return nil, err
	}
	payload, err := wire.Marshal(request)
	if err != nil {
		return nil, &CodecError{Reason: "encode graph request", Cause: err}
	}
	raw, err := graph.laser.SendManaged(ctx, code, payload)
	if err != nil {
		return nil, err
	}
	var reply wire.GraphReply
	if err := wire.Unmarshal(raw, &reply); err != nil {
		return nil, &CodecError{Reason: "decode graph reply", Cause: err}
	}
	if !reply.IsOk() {
		if reply.Err == nil {
			return nil, &CodecError{Reason: "graph reply carried neither a result nor an error"}
		}
		return nil, &GraphError{Failure: *reply.Err}
	}
	return reply.Result, nil
}

// GraphError reports a graph command the server refused.
type GraphError struct{ Failure wire.GraphError }

func (e *GraphError) Error() string { return "graph: " + e.Failure.Error() }

// Kind is the contract's classification of the failure.
func (e *GraphError) Kind() wire.GraphErrorKind { return e.Failure.Kind }
