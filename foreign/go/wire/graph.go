package wire

import (
	"fmt"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// Attr is one attribute of a graph element. It rides the wire as a two-element
// array, the form a Rust tuple encodes to.
type Attr struct {
	Key   string
	Value Value
}

// MarshalCBOR writes the pair as a two-element array.
func (attr Attr) MarshalCBOR() (cborform.Value, error) {
	value, err := attr.Value.MarshalCBOR()
	if err != nil {
		return nil, err
	}
	return cborform.Array{cborform.Text(attr.Key), value}, nil
}

// UnmarshalCBOR reads the two-element array.
func (attr *Attr) UnmarshalCBOR(item cborform.Value) error {
	pair, ok := item.(cborform.Array)
	if !ok || len(pair) != 2 {
		return fmt.Errorf("wire: an attribute is a two-element array, got %T", item)
	}
	key, ok := pair[0].(cborform.Text)
	if !ok {
		return fmt.Errorf("wire: an attribute key must be text, got %T", pair[0])
	}
	var value Value
	if err := value.UnmarshalCBOR(pair[1]); err != nil {
		return err
	}
	*attr = Attr{Key: string(key), Value: value}
	return nil
}

// EdgeDir is which way a hop follows edges from the current frontier.
type EdgeDir string

const (
	// EdgeOut follows outgoing edges. The default, omitted on the wire.
	EdgeOut EdgeDir = "out"
	// EdgeIn follows incoming edges.
	EdgeIn EdgeDir = "in"
	// EdgeBoth follows both directions.
	EdgeBoth EdgeDir = "both"
)

// IsOut reports the default direction, which is omitted on the wire.
func (dir EdgeDir) IsOut() bool { return dir == EdgeOut || dir == "" }

// GraphReturn is what a graph query returns.
type GraphReturn string

const (
	// ReturnNodes returns the reachable nodes. The default, omitted on the wire.
	ReturnNodes GraphReturn = "nodes"
	// ReturnEdges returns the traversed edges.
	ReturnEdges GraphReturn = "edges"
	// ReturnPaths returns full node and edge id sequences.
	ReturnPaths GraphReturn = "paths"
	// ReturnTriplets returns source, relationship and target along the walk.
	ReturnTriplets GraphReturn = "triplets"
)

// IsNodes reports the default return, which is omitted on the wire.
func (want GraphReturn) IsNodes() bool { return want == ReturnNodes || want == "" }

// Hop is one traversal step: follow edges of an optional type in a direction,
// up to a maximum number of hops at this step.
type Hop struct {
	EdgeType *string `cbor:"edge_type,omitempty"`
	Dir      EdgeDir `cbor:"dir,omitempty"`
	Max      uint32  `cbor:"max"`
}

// GraphStartKind discriminates where a traversal begins.
type GraphStartKind uint8

const (
	// StartIDs begins at explicit node ids.
	StartIDs GraphStartKind = iota
	// StartMatch begins at the nodes matching a predicate.
	StartMatch
	// StartNearest begins at the nodes nearest an embedding.
	StartNearest
)

// NearestStart seeds a traversal from the nodes nearest an embedding.
type NearestStart struct {
	Embedding []float32 `cbor:"embedding"`
	K         uint64    `cbor:"k"`
}

// GraphStart is where a traversal starts.
type GraphStart struct {
	Kind    GraphStartKind
	IDs     []NodeID
	Match   *Filter
	Nearest *NearestStart
}

// StartAtIDs begins at explicit node ids.
func StartAtIDs(ids ...NodeID) GraphStart {
	return GraphStart{Kind: StartIDs, IDs: ids}
}

// StartAtMatch begins at the nodes a predicate matches.
func StartAtMatch(filter Filter) GraphStart {
	return GraphStart{Kind: StartMatch, Match: &filter}
}

// StartAtNearest begins at the nodes nearest an embedding.
func StartAtNearest(embedding []float32, k uint64) GraphStart {
	return GraphStart{Kind: StartNearest, Nearest: &NearestStart{Embedding: embedding, K: k}}
}

// The externally tagged start variant names.
const (
	startIDsVariant     = "Ids"
	startMatchVariant   = "Match"
	startNearestVariant = "Nearest"
)

// MarshalCBOR writes the externally tagged start.
func (start GraphStart) MarshalCBOR() (cborform.Value, error) {
	switch start.Kind {
	case StartIDs:
		payload, err := toValueOf(start.IDs)
		if err != nil {
			return nil, err
		}
		return taggedVariant(startIDsVariant, payload), nil
	case StartMatch:
		if start.Match == nil {
			return nil, fmt.Errorf("wire: a match start carries no filter")
		}
		payload, err := start.Match.MarshalCBOR()
		if err != nil {
			return nil, err
		}
		return taggedVariant(startMatchVariant, payload), nil
	case StartNearest:
		if start.Nearest == nil {
			return nil, fmt.Errorf("wire: a nearest start carries no embedding")
		}
		return marshalVariantStruct(startNearestVariant, *start.Nearest)
	default:
		return nil, fmt.Errorf("wire: unknown graph start kind %d", start.Kind)
	}
}

// UnmarshalCBOR reads the externally tagged start.
func (start *GraphStart) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	if err := requirePayload("GraphStart", name, payload); err != nil {
		return err
	}
	switch name {
	case startIDsVariant:
		var ids []NodeID
		if err := fromValueInto(payload, &ids); err != nil {
			return err
		}
		*start = GraphStart{Kind: StartIDs, IDs: ids}
		return nil
	case startMatchVariant:
		var filter Filter
		if err := filter.UnmarshalCBOR(payload); err != nil {
			return err
		}
		*start = GraphStart{Kind: StartMatch, Match: &filter}
		return nil
	case startNearestVariant:
		var nearest NearestStart
		if err := fromValueInto(payload, &nearest); err != nil {
			return err
		}
		*start = GraphStart{Kind: StartNearest, Nearest: &nearest}
		return nil
	default:
		return unknownVariant("GraphStart", name)
	}
}

// GraphQuery is a traversal: start, hop spec, optional node and edge filters,
// and what to return. It reuses the query filter language, so there is one
// filter grammar across query and graph.
type GraphQuery struct {
	V           uint32      `cbor:"v"`
	Graph       string      `cbor:"graph"`
	Start       GraphStart  `cbor:"start"`
	Traverse    []Hop       `cbor:"traverse,omitempty"`
	NodeFilter  *Filter     `cbor:"node_filter,omitempty"`
	EdgeFilter  *Filter     `cbor:"edge_filter,omitempty"`
	Return      GraphReturn `cbor:"return_,omitempty"`
	Limit       uint64      `cbor:"limit"`
	Fork        *string     `cbor:"fork,omitempty"`
	Consistency Consistency `cbor:"consistency,omitempty"`
	// AsOf is a valid-time read: keep only edges whose valid-time window
	// contains this instant. Absent traverses the current graph.
	AsOf *uint64 `cbor:"as_of,omitempty"`
	// Conversation is the conversation lens, matched against each element's
	// source reference: show only what this conversation put in the graph.
	Conversation *string `cbor:"conversation,omitempty"`
	// Stream is the stream the lens conversation belongs to.
	Stream *string `cbor:"stream,omitempty"`
}

// GraphNeighbors is a one-hop neighbor read, the cheap common traversal. Depth
// follows the same hop repeatedly.
type GraphNeighbors struct {
	V            uint32  `cbor:"v"`
	Graph        string  `cbor:"graph"`
	Node         NodeID  `cbor:"node"`
	Dir          EdgeDir `cbor:"dir,omitempty"`
	EdgeType     *string `cbor:"edge_type,omitempty"`
	Depth        uint32  `cbor:"depth"`
	Limit        uint64  `cbor:"limit"`
	AsOf         *uint64 `cbor:"as_of,omitempty"`
	Conversation *string `cbor:"conversation,omitempty"`
	Stream       *string `cbor:"stream,omitempty"`
}

// ProducerInfo is the component that produced a memory or graph fact.
type ProducerInfo struct {
	Name    string `cbor:"name"`
	Version string `cbor:"version"`
}

// GraphNode is one node: its id, labels, attributes, optional embedding and
// optional source.
type GraphNode struct {
	ID     NodeID   `cbor:"id"`
	Labels []string `cbor:"labels,omitempty"`
	Attrs  []Attr   `cbor:"attrs,omitempty"`
	// Embedding is the node's vector, when it carries one.
	Embedding []float32 `cbor:"embedding,omitempty"`
	// Source is the record this node was first observed in, kept first-writer
	// so a re-observed node's stored bytes stay stable.
	Source *SourceRef `cbor:"source,omitempty"`
	// Producer is the component that first produced this node, excluded from
	// its id.
	Producer *ProducerInfo `cbor:"producer,omitempty"`
}

// EntityNode builds a node for an entity, content-addressed over its label and
// value so re-observing the same entity converges on one node.
//
// The value is kept as a "value" attribute so a label or attribute match start
// can find it: the ergonomic way to build a node without hand-minting an id.
func EntityNode(label, value string) GraphNode {
	return GraphNode{
		ID:     ContentNodeID(label, []byte(value)),
		Labels: []string{label},
		Attrs:  []Attr{{Key: "value", Value: StrValue(value)}},
	}
}

// GraphEdge is one edge: its id, endpoints, type, weight, attributes and an
// optional valid-time window for bitemporal facts.
type GraphEdge struct {
	ID       EdgeID  `cbor:"id"`
	From     NodeID  `cbor:"from"`
	To       NodeID  `cbor:"to"`
	EdgeType string  `cbor:"edge_type"`
	Weight   float32 `cbor:"weight"`
	Attrs    []Attr  `cbor:"attrs,omitempty"`
	// ValidFrom is when the relationship became true. Absent is open-ended.
	// The system-time axis is the upsert's log offset, so a fact is superseded
	// by closing ValidTo and opening a new edge rather than overwriting.
	ValidFrom *uint64 `cbor:"valid_from,omitempty"`
	// ValidTo is when the relationship stopped being true. Absent is still
	// valid.
	ValidTo *uint64 `cbor:"valid_to,omitempty"`
	// Source is the record that most recently asserted this relationship.
	Source *SourceRef `cbor:"source,omitempty"`
	// Producer is the component that last produced this edge.
	Producer *ProducerInfo `cbor:"producer,omitempty"`
}

// RelateNodes builds an edge of a type between two nodes with weight one,
// content-addressed over the endpoints and type so the same relationship is one
// edge.
func RelateNodes(from GraphNode, edgeType string, to GraphNode) GraphEdge {
	return GraphEdge{
		ID:       ContentEdgeID(from.ID, edgeType, to.ID),
		From:     from.ID,
		To:       to.ID,
		EdgeType: edgeType,
		Weight:   1,
	}
}

// ValidAt reports whether the edge's valid-time window contains an instant.
func (edge GraphEdge) ValidAt(at uint64) bool {
	if edge.ValidFrom != nil && at < *edge.ValidFrom {
		return false
	}
	if edge.ValidTo != nil && at >= *edge.ValidTo {
		return false
	}
	return true
}

// Path is one path through the graph: parallel node and edge id sequences.
type Path struct {
	Nodes []NodeID `cbor:"nodes"`
	Edges []EdgeID `cbor:"edges"`
}

// GraphResult is the data a traversal returns. Which fields are populated
// depends on the query's return selection.
type GraphResult struct {
	Nodes []GraphNode `cbor:"nodes,omitempty"`
	Edges []GraphEdge `cbor:"edges,omitempty"`
	Paths []Path      `cbor:"paths,omitempty"`
	// Truncated means the traversal stopped at its element limit while more
	// matched. A result that fits the limit exactly is not truncated.
	Truncated bool `cbor:"truncated,omitempty"`
}

// GraphUpsert writes nodes and edges into a graph. The projector path:
// idempotent on content-addressed ids, so re-applying the same extraction is a
// no-op.
type GraphUpsert struct {
	V       uint32      `cbor:"v"`
	Graph   string      `cbor:"graph"`
	Session *SessionRef `cbor:"session,omitempty"`
	Nodes   []GraphNode `cbor:"nodes,omitempty"`
	Edges   []GraphEdge `cbor:"edges,omitempty"`
}

// GraphErrorKind discriminates a graph failure.
type GraphErrorKind uint8

const (
	GraphUnsupported GraphErrorKind = iota
	GraphUnauthorized
	// GraphInvalidName names a graph that fails the graph-name rule.
	GraphInvalidName
	GraphNotFound
	GraphTooLarge
	GraphBackend
	GraphUnavailable
	GraphVersionMismatch
)

type graphTooLargePayload struct {
	What string `cbor:"what"`
	Size uint64 `cbor:"size"`
	Cap  uint64 `cbor:"cap"`
}

// GraphError is why a graph operation failed.
type GraphError struct {
	Kind     GraphErrorKind
	Message  string
	What     string
	Size     uint64
	Cap      uint64
	Expected uint32
	Got      uint32
}

// The externally tagged graph error names.
const (
	graphUnsupportedVariant  = "Unsupported"
	graphUnauthorizedVariant = "Unauthorized"
	graphInvalidNameVariant  = "InvalidName"
	graphNotFoundVariant     = "NotFound"
	graphTooLargeVariant     = "TooLarge"
	graphBackendVariant      = "Backend"
	graphUnavailableVariant  = "Unavailable"
	graphVersionVariant      = "Version"
)

// MarshalCBOR writes the externally tagged error.
func (graphError GraphError) MarshalCBOR() (cborform.Value, error) {
	text := func(name string) (cborform.Value, error) {
		return taggedVariant(name, cborform.Text(graphError.Message)), nil
	}
	switch graphError.Kind {
	case GraphUnsupported:
		return text(graphUnsupportedVariant)
	case GraphUnauthorized:
		return text(graphUnauthorizedVariant)
	case GraphInvalidName:
		return text(graphInvalidNameVariant)
	case GraphNotFound:
		return text(graphNotFoundVariant)
	case GraphBackend:
		return text(graphBackendVariant)
	case GraphUnavailable:
		return text(graphUnavailableVariant)
	case GraphTooLarge:
		return marshalVariantStruct(graphTooLargeVariant, graphTooLargePayload{
			What: graphError.What, Size: graphError.Size, Cap: graphError.Cap,
		})
	case GraphVersionMismatch:
		return marshalVariantStruct(graphVersionVariant, kvVersionPayloadPair{
			Expected: graphError.Expected, Got: graphError.Got,
		})
	default:
		return nil, fmt.Errorf("wire: unknown graph error %d", graphError.Kind)
	}
}

// UnmarshalCBOR reads the externally tagged error.
func (graphError *GraphError) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	if err := requirePayload("GraphError", name, payload); err != nil {
		return err
	}

	text := func(kind GraphErrorKind) error {
		message, ok := payload.(cborform.Text)
		if !ok {
			return fmt.Errorf("wire: %q carries a message, got %T", name, payload)
		}
		*graphError = GraphError{Kind: kind, Message: string(message)}
		return nil
	}

	switch name {
	case graphUnsupportedVariant:
		return text(GraphUnsupported)
	case graphUnauthorizedVariant:
		return text(GraphUnauthorized)
	case graphInvalidNameVariant:
		return text(GraphInvalidName)
	case graphNotFoundVariant:
		return text(GraphNotFound)
	case graphBackendVariant:
		return text(GraphBackend)
	case graphUnavailableVariant:
		return text(GraphUnavailable)
	case graphTooLargeVariant:
		var detail graphTooLargePayload
		if err := fromValueInto(payload, &detail); err != nil {
			return err
		}
		*graphError = GraphError{
			Kind: GraphTooLarge, What: detail.What, Size: detail.Size, Cap: detail.Cap,
		}
		return nil
	case graphVersionVariant:
		var detail kvVersionPayloadPair
		if err := fromValueInto(payload, &detail); err != nil {
			return err
		}
		*graphError = GraphError{
			Kind: GraphVersionMismatch, Expected: detail.Expected, Got: detail.Got,
		}
		return nil
	default:
		return unknownVariant("GraphError", name)
	}
}

// ResultCode projects the failure onto the shared outcome dictionary.
//
// An unauthorized traversal is forbidden rather than unauthenticated: the
// per-graph check refused it because the caller authenticated but lacks the
// grant.
func (graphError GraphError) ResultCode() ResultCode {
	switch graphError.Kind {
	case GraphUnsupported:
		return ResultUnsupported
	case GraphUnauthorized:
		return ResultForbidden
	case GraphInvalidName:
		return ResultInvalidArgument
	case GraphNotFound:
		return ResultNotFound
	case GraphTooLarge:
		return ResultTooLarge
	case GraphUnavailable:
		return ResultUnavailable
	case GraphVersionMismatch:
		return ResultVersionSkew
	default:
		return ResultBackend
	}
}

// Error renders the failure, matching the Rust messages.
func (graphError GraphError) Error() string {
	switch graphError.Kind {
	case GraphUnsupported:
		return "graph not supported: " + graphError.Message
	case GraphUnauthorized:
		return "unauthorized: " + graphError.Message
	case GraphInvalidName:
		return "invalid graph name: " + graphError.Message
	case GraphNotFound:
		return "graph not found: " + graphError.Message
	case GraphTooLarge:
		return fmt.Sprintf("traversal too large: %s is %d, exceeds cap %d",
			graphError.What, graphError.Size, graphError.Cap)
	case GraphBackend:
		return "graph backend error: " + graphError.Message
	case GraphUnavailable:
		return "temporarily unavailable: " + graphError.Message
	case GraphVersionMismatch:
		return fmt.Sprintf("unsupported graph op version (expected %d, got %d)",
			graphError.Expected, graphError.Got)
	default:
		return "unknown graph error"
	}
}

// GraphReply is the result of a graph operation.
type GraphReply struct {
	Err    *GraphError
	Result *GraphResult
}

// OkGraphReply wraps a successful traversal.
func OkGraphReply(result GraphResult) GraphReply { return GraphReply{Result: &result} }

// ErrGraphReply wraps a failure.
func ErrGraphReply(graphError GraphError) GraphReply { return GraphReply{Err: &graphError} }

// IsOk reports a successful reply.
func (reply GraphReply) IsOk() bool { return reply.Err == nil && reply.Result != nil }

// MarshalCBOR writes the externally tagged reply.
func (reply GraphReply) MarshalCBOR() (cborform.Value, error) {
	switch {
	case reply.Err != nil && reply.Result != nil:
		return nil, fmt.Errorf("wire: a graph reply is either a result or an error")
	case reply.Err != nil:
		payload, err := reply.Err.MarshalCBOR()
		if err != nil {
			return nil, err
		}
		return taggedVariant(kvErrVariant, payload), nil
	case reply.Result != nil:
		payload, err := toValueOf(*reply.Result)
		if err != nil {
			return nil, err
		}
		return taggedVariant(kvOkVariant, payload), nil
	default:
		return nil, fmt.Errorf("wire: an empty graph reply")
	}
}

// UnmarshalCBOR reads the externally tagged reply.
func (reply *GraphReply) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	if err := requirePayload("GraphReply", name, payload); err != nil {
		return err
	}
	switch name {
	case kvOkVariant:
		var result GraphResult
		if err := fromValueInto(payload, &result); err != nil {
			return err
		}
		*reply = GraphReply{Result: &result}
		return nil
	case kvErrVariant:
		var graphError GraphError
		if err := graphError.UnmarshalCBOR(payload); err != nil {
			return err
		}
		*reply = GraphReply{Err: &graphError}
		return nil
	default:
		return unknownVariant("GraphReply", name)
	}
}

// ValidateGraphName is the canonical graph-name rule, shared by the client edge
// and the managed backend: non-empty, within the cap, and free of ASCII control
// characters.
func ValidateGraphName(name string) error {
	if name == "" {
		return &InvalidError{Reason: "graph name must not be empty"}
	}
	if len(name) > MaxGraphNameBytes {
		return &InvalidError{Reason: fmt.Sprintf(
			"graph name is %dB, exceeds cap %dB", len(name), MaxGraphNameBytes)}
	}
	for i := 0; i < len(name); i++ {
		if name[i] < 0x20 || name[i] == 0x7f {
			return &InvalidError{Reason: "graph name must not contain ASCII control characters"}
		}
	}
	return nil
}

// IsDefaultValue reports the default direction, omitted on the wire.
func (dir EdgeDir) IsDefaultValue() bool { return dir.IsOut() }

// IsDefaultValue reports the default return selection, omitted on the wire.
func (want GraphReturn) IsDefaultValue() bool { return want.IsNodes() }

// UnmarshalCBOR reads the value, refusing one outside the closed vocabulary.
func (value *EdgeDir) UnmarshalCBOR(item cborform.Value) error {
	return decodeClosedEnum(item, value, "edge dir", EdgeOut, EdgeIn, EdgeBoth)
}

// UnmarshalCBOR reads the value, refusing one outside the closed vocabulary.
func (value *GraphReturn) UnmarshalCBOR(item cborform.Value) error {
	return decodeClosedEnum(item, value, "graph return", ReturnNodes, ReturnEdges, ReturnPaths, ReturnTriplets)
}
