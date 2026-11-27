package bdd

import "sort"

// Reference knowledge-graph engine: the executable specification of the
// traversal and convergence semantics every LaserData SDK and the managed
// backend must reproduce. Pure and transport-free, mirroring the Rust BDD
// crate's GraphEngine and bdd/python/reference.py's.

const (
	fnvOffset uint64 = 0xCBF29CE484222325
	fnvPrime  uint64 = 0x100000001B3
	// nodeSalt is the salt a node id is hashed under, so a node id and a
	// memory id of the same bytes are different values.
	nodeSalt byte = 0x6E
)

// nodeID is a node's content-addressed id: the same salted FNV-1a the other
// clients' graph engines use, so the same entity converges on one node
// across them.
func nodeID(value string) uint64 {
	hash := fnvOffset
	for _, octet := range append([]byte{nodeSalt}, value...) {
		hash ^= uint64(octet)
		hash *= fnvPrime
	}
	return hash
}

// GraphDirection is which way a hop walks an edge.
type GraphDirection int

const (
	// GraphOut follows an edge from its source to its target.
	GraphOut GraphDirection = iota
	// GraphIn walks it backward.
	GraphIn
)

// GraphHop is one step of a traversal.
type GraphHop struct {
	Type      string
	Direction GraphDirection
}

// graphEdge is one typed edge and its valid-time window.
type graphEdge struct {
	from, to  uint64
	edgeType  string
	validFrom uint64
	hasFrom   bool
	validTo   uint64
	hasTo     bool
}

// edgeKey identifies an edge for provenance.
type edgeKey struct {
	from, to uint64
	edgeType string
}

// GraphEngine is an in-memory graph of labelled nodes and typed edges, keyed
// by node value so re-adding a value is the same node.
type GraphEngine struct {
	nodes map[uint64]string
	edges []graphEdge
	// nodeSources is first-writer: the first record a node was seen in.
	nodeSources map[uint64]string
	// edgeSources is last-writer: the most recent record that asserted it.
	edgeSources map[edgeKey]string
}

// NewGraphEngine opens an empty graph.
func NewGraphEngine() *GraphEngine {
	return &GraphEngine{
		nodes:       map[uint64]string{},
		nodeSources: map[uint64]string{},
		edgeSources: map[edgeKey]string{},
	}
}

// UpsertNode adds a node, converging on the one already there for a value.
func (engine *GraphEngine) UpsertNode(value, source string) uint64 {
	id := nodeID(value)
	if _, held := engine.nodes[id]; !held {
		engine.nodes[id] = value
	}
	if source != "" {
		// First-writer: a node keeps the record it was first seen in, so
		// its provenance points at where it entered the graph.
		if _, held := engine.nodeSources[id]; !held {
			engine.nodeSources[id] = source
		}
	}
	return id
}

// GraphEdgeWindow is an edge's optional valid-time bounds.
type GraphEdgeWindow struct {
	From    uint64
	HasFrom bool
	To      uint64
	HasTo   bool
}

// AddEdge asserts a typed edge, optionally within a valid-time window.
func (engine *GraphEngine) AddEdge(
	from uint64, edgeType string, to uint64, window GraphEdgeWindow, source string,
) {
	engine.edges = append(engine.edges, graphEdge{
		from: from, to: to, edgeType: edgeType,
		validFrom: window.From, hasFrom: window.HasFrom,
		validTo: window.To, hasTo: window.HasTo,
	})
	if source != "" {
		// Last-writer: an edge's provenance names the most recent record
		// that asserted it, since the latest assertion is the live one.
		engine.edgeSources[edgeKey{from: from, to: to, edgeType: edgeType}] = source
	}
}

// NodeCount is how many distinct nodes the graph holds.
func (engine *GraphEngine) NodeCount() int { return len(engine.nodes) }

// NodeSource is the record a node was first observed in.
func (engine *GraphEngine) NodeSource(value string) string {
	return engine.nodeSources[nodeID(value)]
}

// EdgeSource is the record that most recently asserted an edge.
func (engine *GraphEngine) EdgeSource(from, edgeType, to string) string {
	return engine.edgeSources[edgeKey{
		from: nodeID(from), to: nodeID(to), edgeType: edgeType,
	}]
}

// validAt reports whether an edge's window contains an instant, half-open
// [from, to). An open bound is unbounded on that side.
func (edge graphEdge) validAt(at uint64, asOf bool) bool {
	if !asOf {
		return true
	}
	if edge.hasFrom && at < edge.validFrom {
		return false
	}
	if edge.hasTo && at >= edge.validTo {
		return false
	}
	return true
}

// Traverse walks the hops from a start value and answers the node values it
// reaches, sorted so two runs answer the same way.
//
// asOf follows only edges whose valid-time window contains that instant;
// without it the traversal reads the current graph.
func (engine *GraphEngine) Traverse(
	start string, hops []GraphHop, at uint64, asOf bool,
) []string {
	frontier := map[uint64]struct{}{nodeID(start): {}}
	for _, hop := range hops {
		next := map[uint64]struct{}{}
		for _, edge := range engine.edges {
			if edge.edgeType != hop.Type || !edge.validAt(at, asOf) {
				continue
			}
			if _, held := frontier[edge.from]; hop.Direction == GraphOut && held {
				next[edge.to] = struct{}{}
			}
			if _, held := frontier[edge.to]; hop.Direction == GraphIn && held {
				next[edge.from] = struct{}{}
			}
		}
		frontier = next
	}
	reached := make([]string, 0, len(frontier))
	for id := range frontier {
		if value, held := engine.nodes[id]; held {
			reached = append(reached, value)
		}
	}
	sort.Strings(reached)
	return reached
}
