use crate::error::LaserError;
use crate::laser::Laser;
use crate::types::ConversationId;

impl Laser {
    /// A handle to the knowledge-graph surface `name`. Traversals require the
    /// `graph` capability (LaserData Cloud) and ride the managed binary
    /// transport. Against raw Apache Iggy a fetch returns
    /// [`LaserError::Unsupported`].
    pub fn graph(&self, name: impl Into<String>) -> GraphHandle<'_> {
        GraphHandle {
            laser: self,
            name: name.into(),
            start: None,
            hops: Vec::new(),
            node_filter: None,
            edge_filter: None,
            return_: laser_wire::graph::GraphReturn::Nodes,
            limit: laser_wire::limits::DEFAULT_RECALL_LIMIT,
            as_of: None,
            conversation: None,
        }
    }
}

/// A fluent knowledge-graph traversal, created by [`Laser::graph`]. Set a start
/// (`start_ids`/`start_match`/`start_nearest`), add hops (`out`/`incoming`), pick
/// what to return, and finish with `.fetch().await`. Gated on the `graph`
/// feature: a traversal rides the managed binary transport.
pub struct GraphHandle<'a> {
    laser: &'a Laser,
    name: String,
    start: Option<laser_wire::graph::GraphStart>,
    hops: Vec<laser_wire::graph::Hop>,
    node_filter: Option<laser_wire::query::Filter>,
    edge_filter: Option<laser_wire::query::Filter>,
    return_: laser_wire::graph::GraphReturn,
    limit: usize,
    as_of: Option<u64>,
    conversation: Option<String>,
}

impl GraphHandle<'_> {
    /// Narrow the traversal to elements a single conversation asserted (the
    /// conversation lens): only nodes and edges whose source records that
    /// `conversation` are traversed and returned. Applies to both `fetch` and
    /// `neighbors`. Reads the whole graph when unset.
    #[must_use]
    pub fn conversation(mut self, conversation: ConversationId) -> Self {
        self.conversation = Some(conversation.to_string());
        self
    }
    /// Start the traversal from explicit node ids.
    #[must_use]
    pub fn start_ids(mut self, ids: Vec<laser_wire::graph::NodeId>) -> Self {
        self.start = Some(laser_wire::graph::GraphStart::Ids(ids));
        self
    }

    /// Start from the nodes matching a predicate.
    #[must_use]
    pub fn start_match(mut self, filter: laser_wire::query::Filter) -> Self {
        self.start = Some(laser_wire::graph::GraphStart::Match(filter));
        self
    }

    /// Start from the nodes nearest an embedding (vector-seeded traversal).
    #[must_use]
    pub fn start_nearest(mut self, embedding: Vec<f32>, k: usize) -> Self {
        self.start = Some(laser_wire::graph::GraphStart::Nearest { embedding, k });
        self
    }

    /// Follow outgoing edges of `edge_type` one hop.
    #[must_use]
    pub fn out(mut self, edge_type: impl Into<String>) -> Self {
        self.hops.push(laser_wire::graph::Hop {
            edge_type: Some(edge_type.into()),
            dir: laser_wire::graph::EdgeDir::Out,
            max: 1,
        });
        self
    }

    /// Follow incoming edges of `edge_type` one hop.
    #[must_use]
    pub fn incoming(mut self, edge_type: impl Into<String>) -> Self {
        self.hops.push(laser_wire::graph::Hop {
            edge_type: Some(edge_type.into()),
            dir: laser_wire::graph::EdgeDir::In,
            max: 1,
        });
        self
    }

    /// Follow edges of `edge_type` one hop in both directions.
    #[must_use]
    pub fn both(mut self, edge_type: impl Into<String>) -> Self {
        self.hops.push(laser_wire::graph::Hop {
            edge_type: Some(edge_type.into()),
            dir: laser_wire::graph::EdgeDir::Both,
            max: 1,
        });
        self
    }

    /// Return the traversed edges instead of the reachable nodes.
    #[must_use]
    pub fn return_edges(mut self) -> Self {
        self.return_ = laser_wire::graph::GraphReturn::Edges;
        self
    }

    /// Return the traversed edges as `(source, type, destination)` triplets
    /// instead of the reachable nodes.
    #[must_use]
    pub fn return_triplets(mut self) -> Self {
        self.return_ = laser_wire::graph::GraphReturn::Triplets;
        self
    }

    /// Return whole paths (node and edge id sequences) instead of nodes.
    #[must_use]
    pub fn return_paths(mut self) -> Self {
        self.return_ = laser_wire::graph::GraphReturn::Paths;
        self
    }

    /// Cap the number of elements returned.
    #[must_use]
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    /// Read the graph as of `micros` (valid-time, epoch micros): only edges whose
    /// valid-time window contains that instant are traversed. Applies to both
    /// `fetch` and `neighbors`.
    #[must_use]
    pub fn as_of(mut self, micros: u64) -> Self {
        self.as_of = Some(micros);
        self
    }

    /// Run the traversal. Requires `managed_graph`. Otherwise returns
    /// [`LaserError::Unsupported`].
    pub async fn fetch(self) -> Result<laser_wire::graph::GraphResult, LaserError> {
        use laser_wire::graph::{GraphQuery, GraphStart};
        self.require_graph()?;
        let query = GraphQuery {
            v: laser_wire::codes::GRAPH_OP_VERSION,
            graph: self.name,
            start: self.start.unwrap_or(GraphStart::Ids(Vec::new())),
            traverse: self.hops,
            node_filter: self.node_filter,
            edge_filter: self.edge_filter,
            return_: self.return_,
            limit: self.limit,
            fork: None,
            consistency: laser_wire::query::Consistency::Eventual,
            as_of: self.as_of,
            conversation: self.conversation,
        };
        let payload = laser_wire::framing::encode_named(&query)
            .map_err(|error| LaserError::Codec(format!("encode graph query: {error}")))?;
        let payload = self
            .laser
            .send_raw_with_response(laser_wire::codes::AGDX_GRAPH_QUERY_CODE, payload)
            .await?;
        decode_graph_reply(&payload)
    }

    /// Read a node's neighbors: the nodes reachable in `dir` over `edge_type` (or
    /// any type when `None`), following the same hop `depth` times. The cheap,
    /// common traversal. Requires `managed_graph`. Otherwise returns
    /// [`LaserError::Unsupported`].
    pub async fn neighbors(
        self,
        node: laser_wire::graph::NodeId,
        dir: laser_wire::graph::EdgeDir,
        edge_type: Option<String>,
        depth: u32,
    ) -> Result<laser_wire::graph::GraphResult, LaserError> {
        use laser_wire::graph::GraphNeighbors;
        self.require_graph()?;
        let request = GraphNeighbors {
            v: laser_wire::codes::GRAPH_OP_VERSION,
            graph: self.name,
            node,
            dir,
            edge_type,
            depth,
            limit: self.limit,
            as_of: self.as_of,
            conversation: self.conversation,
        };
        let payload = laser_wire::framing::encode_named(&request)
            .map_err(|error| LaserError::Codec(format!("encode graph neighbors: {error}")))?;
        let payload = self
            .laser
            .send_raw_with_response(laser_wire::codes::AGDX_GRAPH_NEIGHBORS_CODE, payload)
            .await?;
        decode_graph_reply(&payload)
    }

    /// Relate two entities in one call: `link("customer:42", "opened",
    /// "ticket:7")` upserts both content-addressed entity nodes and the typed
    /// edge between them. Sugar over [`upsert`](Self::upsert), so re-linking
    /// the same triple converges on the same nodes and edge. The entity ids
    /// double as their labels' values (`kind:value` strings work well).
    pub async fn link(
        self,
        from: impl Into<String>,
        relation: impl Into<String>,
        to: impl Into<String>,
    ) -> Result<(), LaserError> {
        use laser_wire::graph::GraphEdge;
        let from = entity_node(&from.into());
        let to = entity_node(&to.into());
        let edge = GraphEdge::relate(&from, relation, &to);
        self.upsert(vec![from, to], vec![edge]).await
    }

    /// Assert the latest value of a single-valued relationship: close every
    /// live `relation` edge from `from` that points at a DIFFERENT target
    /// (`valid_to` now, the bitemporal supersede), then link `from` to `to`.
    /// The deterministic fact-invalidation mechanics (same subject, same
    /// predicate, new object supersedes the old), returning how many edges
    /// were closed. Contradiction detection beyond same-subject-same-predicate
    /// is the caller's policy, and recording an `improve` feedback item that
    /// links superseded to superseding is the caller's memory concern.
    pub async fn relink(
        self,
        from: impl Into<String>,
        relation: impl Into<String>,
        to: impl Into<String>,
    ) -> Result<usize, LaserError> {
        use laser_wire::graph::EdgeDir;
        let laser = self.laser;
        let graph = self.name.clone();
        let from = from.into();
        let relation = relation.into();
        let to = to.into();
        let from_node = entity_node(&from);
        let to_node = entity_node(&to);
        let live = laser
            .graph(&graph)
            .neighbors(from_node.id, EdgeDir::Out, Some(relation.clone()), 1)
            .await?;
        let now = now_micros();
        let superseded: Vec<laser_wire::graph::GraphEdge> = live
            .edges
            .into_iter()
            .filter(|edge| edge.to != to_node.id && edge.valid_to.is_none())
            .map(|mut edge| {
                edge.valid_to = Some(now);
                edge
            })
            .collect();
        let closed = superseded.len();
        if !superseded.is_empty() {
            laser.graph(&graph).upsert(Vec::new(), superseded).await?;
        }
        self.link(from, relation, to).await?;
        Ok(closed)
    }

    /// Close the relationship `link` opened: rewrite the edge with `valid_to`
    /// now, so the fact is superseded without being destroyed (a bitemporal
    /// close). The nodes stay.
    pub async fn unlink(
        self,
        from: impl Into<String>,
        relation: impl Into<String>,
        to: impl Into<String>,
    ) -> Result<(), LaserError> {
        use laser_wire::graph::GraphEdge;
        let from = entity_node(&from.into());
        let to = entity_node(&to.into());
        let mut edge = GraphEdge::relate(&from, relation, &to);
        edge.valid_to = Some(now_micros());
        self.upsert(Vec::new(), vec![edge]).await
    }

    /// Write `nodes` and `edges` into the graph: the projector path, surfaced for
    /// callers that build the graph directly rather than through a `graph`
    /// projection. Idempotent on content-addressed ids
    /// ([`GraphNode::entity`](laser_wire::graph::GraphNode::entity),
    /// [`GraphEdge::relate`](laser_wire::graph::GraphEdge::relate)), so re-applying
    /// the same entities is a no-op. Requires `managed_graph`. Otherwise returns
    /// [`LaserError::Unsupported`].
    pub async fn upsert(
        self,
        nodes: Vec<laser_wire::graph::GraphNode>,
        edges: Vec<laser_wire::graph::GraphEdge>,
    ) -> Result<(), LaserError> {
        use laser_wire::graph::GraphUpsert;
        self.require_graph()?;
        let request = GraphUpsert {
            v: laser_wire::codes::GRAPH_OP_VERSION,
            graph: self.name,
            nodes,
            edges,
        };
        let payload = laser_wire::framing::encode_named(&request)
            .map_err(|error| LaserError::Codec(format!("encode graph upsert: {error}")))?;
        let payload = self
            .laser
            .send_raw_with_response(laser_wire::codes::AGDX_GRAPH_UPSERT_CODE, payload)
            .await?;
        decode_graph_reply(&payload).map(|_| ())
    }

    // Every graph op rides the managed binary transport, so it is unavailable
    // against raw Apache Iggy. Fail the same way before encoding any request.
    fn require_graph(&self) -> Result<(), LaserError> {
        laser_wire::graph::validate_graph_name(&self.name)?;
        if self.laser.capabilities.graph {
            Ok(())
        } else {
            Err(LaserError::unsupported(
                "graph",
                "graph traversal is not served by this deployment",
            ))
        }
    }
}

// Decode a managed `GraphReply` into the `Ok` result or its typed error.
fn decode_graph_reply(payload: &[u8]) -> Result<laser_wire::graph::GraphResult, LaserError> {
    use laser_wire::graph::GraphReply;
    match crate::error::decode_managed_reply::<GraphReply>(payload)? {
        GraphReply::Ok(result) => Ok(result),
        GraphReply::Err(error) => Err(error.into()),
        _ => Err(LaserError::Protocol(
            "graph: unknown reply variant".to_owned(),
        )),
    }
}

/// The entity node for a `kind:value` style id: the id string is both the
/// label-ish identity and the `value` attribute, so `link`ed entities converge
/// on one content-addressed node per id.
fn entity_node(id: &str) -> laser_wire::graph::GraphNode {
    let (label, value) = id.split_once(':').unwrap_or(("entity", id));
    laser_wire::graph::GraphNode::entity(label, value)
}

fn now_micros() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_micros() as u64)
        .unwrap_or(0)
}
