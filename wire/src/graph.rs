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
}

/// One node: its id, labels, attributes, and optional embedding.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GraphNode {
    pub id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attrs: Vec<(String, Value)>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding: Option<Vec<f32>>,
}

/// One edge: its id, endpoints, type, weight, and attributes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GraphEdge {
    pub id: EdgeId,
    pub from: NodeId,
    pub to: NodeId,
    pub edge_type: String,
    pub weight: f32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attrs: Vec<(String, Value)>,
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
        };
        let bytes = encode_named(&query).expect("serializes");
        let back: GraphQuery = decode_named(&bytes).expect("deserializes");
        assert_eq!(back.graph, "knowledge");
        assert_eq!(back.traverse.len(), 2);
        assert_eq!(back.return_, GraphReturn::Paths);
    }

    #[test]
    fn given_a_graph_result_when_round_tripped_then_should_preserve_nodes_and_edges() {
        let reply = GraphReply::Ok(GraphResult {
            nodes: vec![GraphNode {
                id: NodeId::from_u128(1),
                labels: vec!["Person".to_owned()],
                attrs: vec![("name".to_owned(), Value::from("Alice"))],
                embedding: None,
            }],
            edges: vec![GraphEdge {
                id: EdgeId::from_u128(2),
                from: NodeId::from_u128(1),
                to: NodeId::from_u128(3),
                edge_type: "works_at".to_owned(),
                weight: 1.0,
                attrs: Vec::new(),
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
}
