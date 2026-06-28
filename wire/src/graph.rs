use crate::agent::{IdParseError, crockford_decode, crockford_encode};
use crate::query::{Consistency, Filter, Value};
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;

crate::agent::wire_id!(
    /// A graph node's identity. Content-addressed (the hash of the node's label
    /// and canonical value), so the same entity extracted from different messages
    /// converges on one node. Minted SDK- or projector-side.
    NodeId
);
crate::agent::wire_id!(
    /// A graph edge's identity. Content-addressed over its endpoints and type, so
    /// the same relationship is one edge however many times it is observed.
    EdgeId
);

impl NodeId {
    /// A content-addressed node id: the stable hash of the entity's `label` and
    /// canonical `value`, so the same entity extracted from different records (or
    /// upserted by different callers, in any SDK) converges on one node, which is
    /// what makes a graph rather than disconnected pairs. The one canonical
    /// [`content_id`](crate::hashing::content_id), so every SDK mints the same id
    /// from the same segments (pinned by the golden vector below).
    pub fn content(label: &str, value: &[u8]) -> Self {
        Self::from_u128(crate::hashing::content_id(&[label.as_bytes(), &[0], value]))
    }
}

impl EdgeId {
    /// A content-addressed edge id over its endpoints and type, so the same
    /// relationship observed any number of times is one edge. Idempotent upsert
    /// keys off this id.
    pub fn content(from: NodeId, edge_type: &str, to: NodeId) -> Self {
        Self::from_u128(crate::hashing::content_id(&[
            &from.to_bytes(),
            &[0],
            edge_type.as_bytes(),
            &[0],
            &to.to_bytes(),
        ]))
    }
}

/// Which way a hop follows edges from the current frontier.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeDir {
    /// Outgoing edges (from -> to). The default.
    #[default]
    Out,
    /// Incoming edges (to -> from).
    In,
    /// Both directions.
    Both,
}

/// What a graph query returns.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphReturn {
    /// The reachable nodes. The default.
    #[default]
    Nodes,
    /// The traversed edges.
    Edges,
    /// The full paths (node and edge id sequences).
    Paths,
    /// The triplets (source -> relationship -> target) along the traversal.
    Triplets,
}

/// One traversal step: follow edges of an optional type in `dir`, up to `max`
/// hops at this step.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Hop {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edge_type: Option<String>,
    #[serde(default, skip_serializing_if = "EdgeDir::is_out")]
    pub dir: EdgeDir,
    pub max: u32,
}

impl EdgeDir {
    /// Whether this is the default `Out` direction (omitted on the wire).
    pub fn is_out(&self) -> bool {
        matches!(self, EdgeDir::Out)
    }
}

/// Where a traversal starts: explicit node ids, the nodes matching a predicate,
/// or the nodes nearest an embedding (vector-seeded traversal).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum GraphStart {
    Ids(Vec<NodeId>),
    Match(Filter),
    Nearest { embedding: Vec<f32>, k: usize },
}

/// A graph traversal: start, hop spec, optional node and edge filters, and what
/// to return. Reuses the query [`Filter`] predicate language, so there is one
/// filter grammar across query and graph.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GraphQuery {
    pub v: u32,
    pub graph: String,
    pub start: GraphStart,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub traverse: Vec<Hop>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_filter: Option<Filter>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edge_filter: Option<Filter>,
    #[serde(default, skip_serializing_if = "GraphReturn::is_nodes")]
    pub return_: GraphReturn,
    pub limit: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fork: Option<String>,
    #[serde(default, skip_serializing_if = "Consistency::is_eventual")]
    pub consistency: Consistency,
    /// Valid-time "as of" read (epoch micros): keep only edges whose valid-time
    /// window contains this instant. `None` traverses the current graph.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub as_of: Option<u64>,
}

impl GraphReturn {
    /// Whether this is the default `Nodes` return (omitted on the wire).
    pub fn is_nodes(&self) -> bool {
        matches!(self, GraphReturn::Nodes)
    }
}

/// A one-hop neighbor read: the cheap, common traversal. `depth` follows the same
/// hop repeatedly.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GraphNeighbors {
    pub v: u32,
    pub graph: String,
    pub node: NodeId,
    #[serde(default, skip_serializing_if = "EdgeDir::is_out")]
    pub dir: EdgeDir,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edge_type: Option<String>,
    pub depth: u32,
    pub limit: usize,
    /// Valid-time "as of" read (epoch micros): keep only edges whose valid-time
    /// window contains this instant. `None` reads the current graph.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub as_of: Option<u64>,
}

/// Where a graph element was last observed: the source record an extraction came
/// from, so a reader can navigate back to its origin. On an edge this is the
/// record that asserted the relationship (the meaningful provenance, kept
/// last-writer since the edge is rewritten on each observation to maintain its
/// validity window). On a node it is the first record the entity was seen in
/// (first-writer, so a re-observed node's stored bytes stay stable). Excluded
/// from the content-addressed id, so it never affects identity or idempotent
/// upsert. The complete history is the source log, which the projector can
/// replay. Absent on the wire when unknown.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceRef {
    /// A record on the message log, addressed by its stream, topic, partition,
    /// and offset.
    Message {
        stream: String,
        topic: String,
        partition: u32,
        offset: u64,
    },
    /// A key in the managed key-value store.
    Kv { namespace: String, key: String },
    /// A managed memory item, by its id.
    Memory { id: String },
}

/// One node: its id, labels, attributes, optional embedding, and optional source.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GraphNode {
    pub id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attrs: Vec<(String, Value)>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding: Option<Vec<f32>>,
    /// The source this node was first observed in, if known. See [`SourceRef`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceRef>,
}

impl GraphNode {
    /// A node for the entity `value` labelled `label`. Its id is content-addressed
    /// over the label and value (so re-observing the same entity converges on one
    /// node), and the value is kept as a `value` attribute so a `label` or
    /// attribute [`Match`](GraphStart::Match) start can find it. The ergonomic way
    /// to build a node for [`GraphUpsert`] without hand-minting an id.
    pub fn entity(label: impl Into<String>, value: impl Into<String>) -> Self {
        let label = label.into();
        let value = value.into();
        let id = NodeId::content(&label, value.as_bytes());
        Self {
            id,
            labels: vec![label],
            attrs: vec![("value".to_owned(), Value::from(value))],
            embedding: None,
            source: None,
        }
    }
}

/// One edge: its id, endpoints, type, weight, attributes, and an optional
/// valid-time window for bitemporal facts.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GraphEdge {
    pub id: EdgeId,
    pub from: NodeId,
    pub to: NodeId,
    pub edge_type: String,
    pub weight: f32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attrs: Vec<(String, Value)>,
    /// Valid-time start (epoch micros): when the relationship became true. `None`
    /// is open-ended. The system-time axis (when observed) is the upsert's log
    /// offset, so a fact can be superseded by closing `valid_to` and opening a new
    /// edge rather than overwriting. Absent on the wire when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_from: Option<u64>,
    /// Valid-time end (epoch micros): when the relationship stopped being true.
    /// `None` is still valid.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_to: Option<u64>,
    /// The source that most recently asserted this relationship, if known. See
    /// [`SourceRef`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceRef>,
}

impl GraphEdge {
    /// An edge of `edge_type` from `from` to `to`, weight `1.0`. Its id is
    /// content-addressed over the endpoints and type, so the same relationship is
    /// one edge. The ergonomic way to relate two [`GraphNode`]s for an upsert.
    pub fn relate(from: &GraphNode, edge_type: impl Into<String>, to: &GraphNode) -> Self {
        let edge_type = edge_type.into();
        Self {
            id: EdgeId::content(from.id, &edge_type, to.id),
            from: from.id,
            to: to.id,
            edge_type,
            weight: 1.0,
            attrs: Vec::new(),
            valid_from: None,
            valid_to: None,
            source: None,
        }
    }

    /// Set the source that asserted this relationship. See [`SourceRef`]. The
    /// edge id is unchanged: provenance is metadata, not identity.
    pub fn with_source(mut self, source: SourceRef) -> Self {
        self.source = Some(source);
        self
    }

    /// Set the valid-time window (epoch micros) on this edge, for a bitemporal
    /// fact. Either bound may be `None` for open-ended. The edge id is unchanged:
    /// validity is metadata on the relationship, not part of its identity, so
    /// re-observing the same relationship with a new window updates the same edge.
    pub fn valid(mut self, from: Option<u64>, to: Option<u64>) -> Self {
        self.valid_from = from;
        self.valid_to = to;
        self
    }

    /// Whether this edge's valid-time window contains `at` (epoch micros). An
    /// open bound is treated as unbounded, so an edge with no window always holds.
    /// The half-open convention is `[valid_from, valid_to)`.
    pub fn valid_at(&self, at: u64) -> bool {
        self.valid_from.is_none_or(|from| at >= from) && self.valid_to.is_none_or(|to| at < to)
    }
}

/// One path through the graph: parallel node and edge id sequences.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Path {
    pub nodes: Vec<NodeId>,
    pub edges: Vec<EdgeId>,
}

/// The data a graph traversal returns. Which fields are populated depends on the
/// query's [`GraphReturn`].
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct GraphResult {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nodes: Vec<GraphNode>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub edges: Vec<GraphEdge>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub paths: Vec<Path>,
}

/// Upsert nodes and edges into a graph. The projector path: idempotent on
/// content-addressed ids, so re-applying the same extraction is a no-op.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct GraphUpsert {
    pub v: u32,
    pub graph: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nodes: Vec<GraphNode>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub edges: Vec<GraphEdge>,
}

/// The result of a graph operation: `Ok` with the traversal data, or `Err` with
/// a structured failure.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub enum GraphReply {
    Ok(GraphResult),
    Err(GraphError),
}

/// Why a graph operation failed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[non_exhaustive]
pub enum GraphError {
    #[error("graph not supported: {0}")]
    Unsupported(String),
    #[error("graph not found: {0}")]
    NotFound(String),
    #[error("traversal too large: {what} is {size}, exceeds cap {cap}")]
    TooLarge {
        what: String,
        size: usize,
        cap: usize,
    },
    #[error("graph backend error: {0}")]
    Backend(String),
    #[error("unsupported graph op version (expected {expected}, got {got})")]
    Version { expected: u32, got: u32 },
}

#[cfg(all(test, feature = "cbor"))]
mod tests {
    use super::*;
    use crate::codes::GRAPH_OP_VERSION;
    use crate::framing::{decode_named, encode_named};
    use crate::query::CmpOp;

    #[test]
    fn given_a_graph_query_when_round_tripped_then_should_preserve_traversal() {
        let query = GraphQuery {
            v: GRAPH_OP_VERSION,
            graph: "knowledge".to_owned(),
            start: GraphStart::Match(Filter::pred("label", CmpOp::Eq, "Person")),
            traverse: vec![
                Hop {
                    edge_type: Some("works_at".to_owned()),
                    dir: EdgeDir::Out,
                    max: 1,
                },
                Hop {
                    edge_type: Some("located_in".to_owned()),
                    dir: EdgeDir::Out,
                    max: 1,
                },
            ],
            node_filter: None,
            edge_filter: None,
            return_: GraphReturn::Paths,
            limit: 100,
            fork: None,
            consistency: Consistency::Eventual,
            as_of: Some(1_900_000_000_000_000),
        };
        let bytes = encode_named(&query).expect("serializes");
        let back: GraphQuery = decode_named(&bytes).expect("deserializes");
        assert_eq!(back.graph, "knowledge");
        assert_eq!(back.traverse.len(), 2);
        assert_eq!(back.return_, GraphReturn::Paths);
        assert_eq!(back.as_of, Some(1_900_000_000_000_000));
    }

    #[test]
    fn given_a_graph_result_when_round_tripped_then_should_preserve_nodes_and_edges() {
        let reply = GraphReply::Ok(GraphResult {
            nodes: vec![GraphNode {
                id: NodeId::from_u128(1),
                labels: vec!["Person".to_owned()],
                attrs: vec![("name".to_owned(), Value::from("Alice"))],
                embedding: None,
                source: None,
            }],
            edges: vec![GraphEdge {
                id: EdgeId::from_u128(2),
                from: NodeId::from_u128(1),
                to: NodeId::from_u128(3),
                edge_type: "works_at".to_owned(),
                weight: 1.0,
                attrs: Vec::new(),
                valid_from: None,
                valid_to: None,
                source: None,
            }],
            paths: Vec::new(),
        });
        let bytes = encode_named(&reply).expect("serializes");
        let back: GraphReply = decode_named(&bytes).expect("deserializes");
        let GraphReply::Ok(result) = back else {
            panic!("expected Ok");
        };
        assert_eq!(result.nodes.len(), 1);
        assert_eq!(result.edges[0].edge_type, "works_at");
    }

    #[test]
    fn given_a_nearest_start_when_round_tripped_then_should_preserve_the_seed() {
        let query = GraphQuery {
            v: GRAPH_OP_VERSION,
            graph: "knowledge".to_owned(),
            start: GraphStart::Nearest {
                embedding: vec![0.1, 0.2, 0.3],
                k: 5,
            },
            traverse: Vec::new(),
            node_filter: None,
            edge_filter: None,
            return_: GraphReturn::Nodes,
            limit: 10,
            fork: None,
            consistency: Consistency::Eventual,
            as_of: None,
        };
        let bytes = encode_named(&query).expect("serializes");
        let back: GraphQuery = decode_named(&bytes).expect("deserializes");
        match back.start {
            GraphStart::Nearest { embedding, k } => {
                assert_eq!(embedding, vec![0.1, 0.2, 0.3]);
                assert_eq!(k, 5);
            }
            other => panic!("expected Nearest, got {other:?}"),
        }
    }

    #[test]
    fn given_a_node_id_when_round_tripped_through_a_string_then_should_be_equal() {
        let id = NodeId::from_u128(987_654_321);
        let parsed: NodeId = id.to_string().parse().expect("a node id parses");
        assert_eq!(parsed, id);
    }

    #[test]
    fn given_the_same_entity_when_addressed_twice_then_should_converge_on_one_node_id() {
        let a = NodeId::content("Person", b"Alice");
        let b = NodeId::content("Person", b"Alice");
        assert_eq!(a, b, "the same entity is one node");
        // A different label or value is a different node.
        assert_ne!(a, NodeId::content("Company", b"Alice"));
        assert_ne!(a, NodeId::content("Person", b"Bob"));
    }

    #[test]
    fn given_the_pinned_entity_when_addressed_then_should_match_the_golden_id() {
        // The cross-SDK golden vector: the Person entity "Alice". Every SDK renders
        // this NodeId identically, so a graph shared across languages converges.
        assert_eq!(
            NodeId::content("Person", b"Alice").to_string(),
            "13NCEPHNVFHHGNK9GD3MT0W1AB"
        );
    }

    #[test]
    fn given_two_nodes_when_related_then_should_content_address_the_edge() {
        let alice = GraphNode::entity("Person", "Alice");
        let acme = GraphNode::entity("Company", "Acme");
        let one = GraphEdge::relate(&alice, "works_at", &acme);
        let two = GraphEdge::relate(&alice, "works_at", &acme);
        assert_eq!(one.id, two.id, "the same relationship is one edge");
        assert_eq!(one.from, alice.id);
        assert_eq!(one.to, acme.id);
        // The direction is part of the identity: the reverse edge is a different id.
        assert_ne!(one.id, GraphEdge::relate(&acme, "works_at", &alice).id);
    }

    #[test]
    fn given_an_edge_validity_window_when_checked_then_should_hold_only_inside_it() {
        let alice = GraphNode::entity("User", "alice");
        let pro = GraphNode::entity("Plan", "pro");
        let edge = GraphEdge::relate(&alice, "on_plan", &pro).valid(Some(100), Some(200));
        assert!(!edge.valid_at(99), "before the window");
        assert!(edge.valid_at(100), "the lower bound is inclusive");
        assert!(edge.valid_at(150), "inside the window");
        assert!(!edge.valid_at(200), "the upper bound is exclusive");
        let open = GraphEdge::relate(&alice, "on_plan", &pro);
        assert!(open.valid_at(0) && open.valid_at(u64::MAX));
        assert_eq!(edge.id, open.id, "validity is not part of edge identity");
    }

    #[test]
    fn given_an_edge_without_validity_when_serialized_then_should_omit_the_window() {
        let edge = GraphEdge::relate(
            &GraphNode::entity("A", "x"),
            "rel",
            &GraphNode::entity("B", "y"),
        );
        let json = serde_json::to_string(&edge).expect("serializes");
        assert!(
            !json.contains("valid_from") && !json.contains("valid_to"),
            "an unset window must be omitted so a pre-bitemporal edge is byte-identical: {json}"
        );
    }

    #[test]
    fn given_a_node_without_a_source_when_serialized_then_should_omit_it() {
        let node = GraphNode::entity("Person", "Alice");
        let json = serde_json::to_string(&node).expect("serializes");
        assert!(
            !json.contains("source"),
            "an unknown source must be omitted so a pre-provenance node is byte-identical: {json}"
        );
    }

    #[test]
    fn given_a_node_with_a_source_when_round_tripped_then_should_preserve_it_and_keep_identity() {
        let mut node = GraphNode::entity("Component", "cache");
        node.source = Some(SourceRef::Message {
            stream: "orders".to_owned(),
            topic: "events".to_owned(),
            partition: 3,
            offset: 4096,
        });
        let bytes = encode_named(&node).expect("serializes");
        let back: GraphNode = decode_named(&bytes).expect("deserializes");
        assert_eq!(back.source, node.source);
        assert_eq!(
            back.id,
            GraphNode::entity("Component", "cache").id,
            "source is not part of node identity"
        );
    }

    #[test]
    fn given_an_edge_with_a_source_when_round_tripped_then_should_preserve_it_and_keep_identity() {
        let from = GraphNode::entity("A", "x");
        let to = GraphNode::entity("B", "y");
        let edge = GraphEdge::relate(&from, "rel", &to).with_source(SourceRef::Kv {
            namespace: "ns".to_owned(),
            key: "k".to_owned(),
        });
        let bytes = encode_named(&edge).expect("serializes");
        let back: GraphEdge = decode_named(&bytes).expect("deserializes");
        assert_eq!(back.source, edge.source);
        assert_eq!(
            back.id,
            GraphEdge::relate(&from, "rel", &to).id,
            "source is not part of edge identity"
        );
    }

    #[test]
    fn given_a_max_element_reply_with_source_when_encoded_then_should_fit_one_frame() {
        use crate::limits::{MAX_FRAME_BYTES, MAX_GRAPH_RESULT_ELEMENTS};
        let source = SourceRef::Message {
            stream: "a-fairly-long-stream-name-for-headroom".to_owned(),
            topic: "a-fairly-long-topic-name-for-headroom".to_owned(),
            partition: u32::MAX,
            offset: u64::MAX,
        };
        let half = (MAX_GRAPH_RESULT_ELEMENTS / 2) as u128;
        let nodes = (0..half)
            .map(|i| {
                let mut node = GraphNode::entity("Component", format!("entity-{i}"));
                node.source = Some(source.clone());
                node
            })
            .collect();
        let edges = (0..half)
            .map(|i| GraphEdge {
                id: EdgeId::from_u128(i),
                from: NodeId::from_u128(i),
                to: NodeId::from_u128(i + 1),
                edge_type: "relates_to".to_owned(),
                weight: 1.0,
                attrs: Vec::new(),
                valid_from: None,
                valid_to: None,
                source: Some(source.clone()),
            })
            .collect();
        let reply = GraphReply::Ok(GraphResult {
            nodes,
            edges,
            paths: Vec::new(),
        });
        let encoded = encode_named(&reply).expect("serializes");
        assert!(
            encoded.len() < MAX_FRAME_BYTES,
            "a full {MAX_GRAPH_RESULT_ELEMENTS}-element reply with source is {} bytes, over the frame cap {MAX_FRAME_BYTES}",
            encoded.len()
        );
    }
}
