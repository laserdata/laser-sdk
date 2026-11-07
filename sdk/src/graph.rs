use crate::error::LaserError;
use crate::laser::Laser;
use crate::types::ConversationId;
use laser_wire::graph::{EdgeDir, GraphEdge, GraphNeighbors, GraphReply, GraphUpsert};
use laser_wire::validate::Validate;
use std::time::{SystemTime, UNIX_EPOCH};

pub use laser_wire::limits::MAX_GRAPH_LABEL_BYTES;

impl Laser {
    /// A handle to the knowledge-graph surface `name`. Traversals require the
    /// `graph` capability (LaserData Cloud) and ride the managed binary
    /// transport. Against Apache Iggy a fetch returns
    /// [`LaserError::Unsupported`]. The name is sent as
    /// [`resource_name`](Self::resource_name) names it.
    pub fn graph(&self, name: impl Into<String>) -> GraphHandle {
        GraphHandle {
            laser: self.clone(),
            name: self.resource_name(&name.into()),
            start: None,
            hops: Vec::new(),
            node_filter: None,
            edge_filter: None,
            return_: laser_wire::graph::GraphReturn::Nodes,
            limit: laser_wire::limits::DEFAULT_RECALL_LIMIT as u32,
            as_of: None,
            conversation: None,
            stream: None,
            session: None,
            producer: None,
            source: None,
        }
    }
}

/// A fluent knowledge-graph traversal, created by [`Laser::graph`]. Set a start
/// (`start_ids`/`start_match`/`start_nearest`), add hops (`out`/`incoming`), pick
/// what to return, and finish with `.fetch().await`. Gated on the `graph`
/// feature: a traversal rides the managed binary transport. It owns a clone
/// of the `Laser`, so a traversal can move into a spawned task.
pub struct GraphHandle {
    laser: Laser,
    name: String,
    start: Option<laser_wire::graph::GraphStart>,
    hops: Vec<laser_wire::graph::Hop>,
    node_filter: Option<laser_wire::query::Filter>,
    edge_filter: Option<laser_wire::query::Filter>,
    return_: laser_wire::graph::GraphReturn,
    limit: u32,
    as_of: Option<u64>,
    conversation: Option<String>,
    stream: Option<String>,
    session: Option<laser_wire::agent::SessionRef>,
    producer: Option<laser_wire::graph::ProducerInfo>,
    source: Option<laser_wire::graph::SourceRef>,
}

impl GraphHandle {
    /// Link every upsert to `session`, so the deployment records which session
    /// wrote each element.
    #[must_use]
    pub fn in_session(mut self, session: laser_wire::agent::SessionRef) -> Self {
        self.session = Some(session);
        self
    }

    /// Stamp `producer` on every upserted element that has none.
    #[must_use]
    pub fn produced_by(mut self, producer: laser_wire::graph::ProducerInfo) -> Self {
        self.producer = Some(producer);
        self
    }

    /// Stamp `source` on every upserted element that has none.
    #[must_use]
    pub fn sourced_from(mut self, source: laser_wire::graph::SourceRef) -> Self {
        self.source = Some(source);
        self
    }

    /// Narrow the traversal to elements a single conversation asserted (the
    /// conversation lens): only nodes and edges whose source records that
    /// `conversation` are traversed and returned. Applies to both `fetch` and
    /// `neighbors`. Reads the whole graph when unset.
    #[must_use]
    pub fn conversation(mut self, conversation: ConversationId) -> Self {
        self.conversation = Some(conversation.to_string());
        self.stream = self.laser.resource_stream().map(str::to_owned);
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
    pub fn start_nearest(mut self, embedding: Vec<f32>, k: u32) -> Self {
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

    /// Follow edges in `dir`, of `edge_type` or of any type when `None`, up to
    /// `max` steps at this hop.
    #[must_use]
    pub fn hop(
        mut self,
        dir: laser_wire::graph::EdgeDir,
        edge_type: Option<String>,
        max: u32,
    ) -> Self {
        self.hops.push(laser_wire::graph::Hop {
            edge_type,
            dir,
            max,
        });
        self
    }

    /// Admit only nodes matching `filter` at every hop. A rejected node is
    /// neither returned nor expanded.
    #[must_use]
    pub fn node_filter(mut self, filter: laser_wire::query::Filter) -> Self {
        self.node_filter = Some(filter);
        self
    }

    /// Follow only edges matching `filter`. A rejected edge is neither
    /// traversed nor returned.
    #[must_use]
    pub fn edge_filter(mut self, filter: laser_wire::query::Filter) -> Self {
        self.edge_filter = Some(filter);
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

    /// Cap the number of elements returned. Zero means up to
    /// [`MAX_GRAPH_RESULT_ELEMENTS`](laser_wire::limits::MAX_GRAPH_RESULT_ELEMENTS).
    #[must_use]
    pub fn limit(mut self, limit: u32) -> Self {
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

    /// The traversal request this handle sends, for inspection or the HTTP
    /// route's body.
    pub fn into_query(self) -> laser_wire::graph::GraphQuery {
        laser_wire::graph::GraphQuery {
            v: laser_wire::codes::GRAPH_OP_VERSION,
            graph: self.name,
            start: self
                .start
                .unwrap_or(laser_wire::graph::GraphStart::Ids(Vec::new())),
            traverse: self.hops,
            node_filter: self.node_filter,
            edge_filter: self.edge_filter,
            return_: self.return_,
            limit: self.limit,
            as_of: self.as_of,
            conversation: self.conversation,
            stream: self.stream,
        }
    }

    /// Run the traversal. Refuses a hop step outside one to
    /// [`MAX_GRAPH_TRAVERSE_DEPTH`](laser_wire::limits::MAX_GRAPH_TRAVERSE_DEPTH),
    /// an expanded depth past it, or a limit past the element cap with
    /// [`LaserError::Invalid`] before sending. Requires `managed_graph`.
    /// Otherwise returns [`LaserError::Unsupported`].
    pub async fn fetch(self) -> Result<laser_wire::graph::GraphResult, LaserError> {
        let laser = self.laser.clone();
        let query = self.into_query();
        query.validate()?;
        graph_served(&laser).await?;
        let payload = laser_wire::framing::encode_named(&query)
            .map_err(|error| LaserError::Codec(format!("encode graph query: {error}")))?;
        let payload = laser
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
        self.require_graph().await?;
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
            stream: self.stream,
        };
        request.validate()?;
        let payload = laser_wire::framing::encode_named(&request)
            .map_err(|error| LaserError::Codec(format!("encode graph neighbors: {error}")))?;
        let payload = self
            .laser
            .send_raw_with_response(laser_wire::codes::AGDX_GRAPH_NEIGHBORS_CODE, payload)
            .await?;
        decode_graph_reply(&payload)
    }

    /// Relate two entities in one call: `link("operator:42", "opened",
    /// "ticket:7")` upserts both content-addressed entity nodes and the typed
    /// edge between them, valid from now. Sugar over [`upsert`](Self::upsert),
    /// so re-linking the same triple converges on the same nodes and edge. The
    /// entity ids double as their labels' values (`kind:value` strings work
    /// well).
    pub async fn link(
        self,
        from: impl Into<String>,
        relation: impl Into<String>,
        to: impl Into<String>,
    ) -> Result<(), LaserError> {
        let (nodes, edge) = linked(&from.into(), relation.into(), &to.into(), now_micros());
        self.upsert(nodes, vec![edge]).await
    }

    /// Assert the latest value of a single-valued relationship: close every
    /// live `relation` edge from `from` that points at a DIFFERENT target
    /// (`valid_to` now, the bitemporal supersede), then link `from` to `to`
    /// valid from that same instant.
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
        let laser = self.laser.clone();
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
        let closing = superseded(live.edges, to_node.id, now);
        let closed = closing.len();
        if !closing.is_empty() {
            laser.graph(&graph).upsert(Vec::new(), closing).await?;
        }
        let (nodes, edge) = linked(&from, relation, &to, now);
        self.upsert(nodes, vec![edge]).await?;
        Ok(closed)
    }

    /// Close the relationship `link` opened: rewrite the edge with `valid_to`
    /// now, so the fact is superseded without being destroyed (a bitemporal
    /// close). The deployment keeps the stored `valid_from`, and a later
    /// `link` keeps this closed interval for `as_of` reads. The nodes stay.
    pub async fn unlink(
        self,
        from: impl Into<String>,
        relation: impl Into<String>,
        to: impl Into<String>,
    ) -> Result<(), LaserError> {
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
        self.require_graph().await?;
        let (mut nodes, mut edges) = (nodes, edges);
        for node in &mut nodes {
            if node.producer.is_none() {
                node.producer.clone_from(&self.producer);
            }
            if node.source.is_none() {
                node.source.clone_from(&self.source);
            }
        }
        for edge in &mut edges {
            if edge.producer.is_none() {
                edge.producer.clone_from(&self.producer);
            }
            if edge.source.is_none() {
                edge.source.clone_from(&self.source);
            }
        }
        let request = GraphUpsert {
            v: laser_wire::codes::GRAPH_OP_VERSION,
            graph: self.name,
            session: self.session,
            nodes,
            edges,
        };
        // Spend the cap failure locally rather than amplifying an over-cap
        // upsert against the managed backend, which rejects it with the same
        // reason.
        request.validate()?;
        let payload = laser_wire::framing::encode_named(&request)
            .map_err(|error| LaserError::Codec(format!("encode graph upsert: {error}")))?;
        let payload = self
            .laser
            .send_raw_with_response(laser_wire::codes::AGDX_GRAPH_UPSERT_CODE, payload)
            .await?;
        decode_graph_reply(&payload).map(|_| ())
    }

    async fn require_graph(&self) -> Result<(), LaserError> {
        laser_wire::graph::validate_graph_name(&self.name)?;
        graph_served(&self.laser).await
    }
}

// Every graph op rides the managed binary transport, so it is unavailable
// against Apache Iggy. Fail the same way before encoding any request. The
// capability read reprobes a deployment that was not managed at connect.
async fn graph_served(laser: &Laser) -> Result<(), LaserError> {
    let capabilities = laser.capabilities().await;
    if !capabilities.graph {
        return Err(LaserError::unsupported(
            "graph",
            "graph traversal is not served by this deployment",
        ));
    }
    match capabilities.versions {
        Some(versions) if versions.graph != laser_wire::codes::GRAPH_OP_VERSION => {
            Err(laser_wire::graph::GraphError::Version {
                expected: versions.graph,
                got: laser_wire::codes::GRAPH_OP_VERSION,
            }
            .into())
        }
        _ => Ok(()),
    }
}

// Decode a managed `GraphReply` into the `Ok` result or its typed error.
fn decode_graph_reply(payload: &[u8]) -> Result<laser_wire::graph::GraphResult, LaserError> {
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

// The two entity nodes a link upserts and the edge between them, valid from
// `at`, so an `as_of` before the link does not see it.
fn linked(
    from: &str,
    relation: String,
    to: &str,
    at: u64,
) -> (
    Vec<laser_wire::graph::GraphNode>,
    laser_wire::graph::GraphEdge,
) {
    let from = entity_node(from);
    let to = entity_node(to);
    let edge = GraphEdge::relate(&from, relation, &to).valid(Some(at), None);
    (vec![from, to], edge)
}

// The live edges of `live` that point at a target other than `to`, closed at `at`.
fn superseded(
    live: Vec<laser_wire::graph::GraphEdge>,
    to: laser_wire::graph::NodeId,
    at: u64,
) -> Vec<laser_wire::graph::GraphEdge> {
    live.into_iter()
        .filter(|edge| edge.to != to && edge.valid_to.is_none())
        .map(|mut edge| {
            edge.valid_to = Some(at);
            edge
        })
        .collect()
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_micros()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::{linked, superseded};
    use crate::capabilities::Capabilities;
    use crate::error::LaserError;
    use crate::laser::{Laser, ResourceNaming};
    use crate::types::ConversationId;
    use laser_wire::codes::GRAPH_OP_VERSION;
    use laser_wire::graph::{
        EdgeDir, EdgeId, GraphEdge, GraphNode, GraphQuery, GraphReturn, GraphStart, Hop,
    };
    use laser_wire::limits::{MAX_GRAPH_RESULT_ELEMENTS, MAX_GRAPH_TRAVERSE_DEPTH};
    use laser_wire::query::{CmpOp, Filter};

    #[test]
    fn given_a_link_when_built_then_should_open_the_edge_at_the_given_instant() {
        let (nodes, edge) = linked("host:node-7", "opened".to_owned(), "ticket:7", 42);
        let host = GraphNode::entity("host", "node-7");
        let ticket = GraphNode::entity("ticket", "7");
        assert_eq!(nodes, vec![host.clone(), ticket.clone()]);
        assert_eq!(
            edge,
            GraphEdge::relate(&host, "opened", &ticket).valid(Some(42), None)
        );
        assert_eq!(edge.id, EdgeId::content(host.id, "opened", ticket.id));
    }

    #[test]
    fn given_live_and_closed_edges_when_superseding_then_should_close_only_live_edges_to_other_targets()
     {
        let host = GraphNode::entity("host", "node-7");
        let old = GraphNode::entity("ticket", "7");
        let kept = GraphNode::entity("ticket", "9");
        let live = GraphEdge::relate(&host, "opened", &old).valid(Some(10), None);
        let closed = GraphEdge::relate(&host, "opened", &old).valid(Some(1), Some(5));
        let current = GraphEdge::relate(&host, "opened", &kept).valid(Some(20), None);
        let closing = superseded(vec![live.clone(), closed, current], kept.id, 99);
        assert_eq!(closing, vec![live.valid(Some(10), Some(99))]);
    }

    #[test]
    fn given_filters_and_a_multi_step_hop_when_building_then_should_carry_them_in_the_query() {
        let laser = laser().with_resource_naming(ResourceNaming::Bare);
        let node_filter = Filter::pred("label", CmpOp::Eq, "Person");
        let edge_filter = Filter::pred("edge_type", CmpOp::Eq, "knows");
        let query = laser
            .graph("kg")
            .hop(EdgeDir::Both, None, 3)
            .out("works_at")
            .node_filter(node_filter.clone())
            .edge_filter(edge_filter.clone())
            .limit(7)
            .into_query();
        assert_eq!(
            query,
            GraphQuery {
                v: GRAPH_OP_VERSION,
                graph: "kg".to_owned(),
                start: GraphStart::Ids(Vec::new()),
                traverse: vec![
                    Hop {
                        edge_type: None,
                        dir: EdgeDir::Both,
                        max: 3,
                    },
                    Hop {
                        edge_type: Some("works_at".to_owned()),
                        dir: EdgeDir::Out,
                        max: 1,
                    },
                ],
                node_filter: Some(node_filter),
                edge_filter: Some(edge_filter),
                return_: GraphReturn::Nodes,
                limit: 7,
                as_of: None,
                conversation: None,
                stream: None,
            }
        );
    }

    #[tokio::test]
    async fn given_an_invalid_hop_or_limit_when_fetching_then_should_refuse_before_sending() {
        let laser =
            laser().with_capabilities(Capabilities::OPEN.with_managed(true).with_graph(true));
        let past_cap = u32::try_from(MAX_GRAPH_RESULT_ELEMENTS + 1).expect("the cap fits u32");
        let refusals = [
            laser.graph("kg").hop(EdgeDir::Out, None, 0).fetch().await,
            laser
                .graph("kg")
                .hop(EdgeDir::Out, None, MAX_GRAPH_TRAVERSE_DEPTH + 1)
                .fetch()
                .await,
            laser
                .graph("kg")
                .hop(EdgeDir::Out, None, 5)
                .hop(EdgeDir::Out, None, 4)
                .fetch()
                .await,
            laser.graph("kg").limit(past_cap).fetch().await,
        ];
        for refusal in refusals {
            assert!(
                matches!(refusal, Err(LaserError::Invalid(_))),
                "refused before the transport, got {refusal:?}"
            );
        }
    }

    #[test]
    fn given_a_default_stream_when_opening_a_graph_then_should_scope_the_name_and_the_lens() {
        let laser = laser();
        let handle = laser
            .graph("knowledge")
            .conversation(ConversationId::derive("lens"));
        assert_eq!(handle.name, "stream:acme/knowledge");
        assert_eq!(handle.stream.as_deref(), Some("acme"));
    }

    #[test]
    fn given_bare_naming_when_opening_a_graph_then_should_send_the_name_and_lens_unchanged() {
        let laser = laser().with_resource_naming(ResourceNaming::Bare);
        let handle = laser
            .graph("knowledge")
            .conversation(ConversationId::derive("lens"));
        assert_eq!(handle.name, "knowledge");
        assert_eq!(handle.stream, None);
    }

    #[tokio::test]
    async fn given_another_graph_op_version_when_upserting_then_should_report_version_skew() {
        let laser = laser().with_capabilities(
            Capabilities::OPEN
                .with_managed(true)
                .with_graph(true)
                .with_versions(Some(
                    crate::capabilities::OpVersions::new(1, 1, 1, 1)
                        .with_graph(GRAPH_OP_VERSION + 1),
                )),
        );
        // A refusal answers at once, a request that passed goes on to send.
        let upsert = tokio::time::timeout(
            std::time::Duration::from_millis(300),
            laser.graph("kg").upsert(Vec::new(), Vec::new()),
        )
        .await;
        assert!(
            matches!(&upsert, Ok(Err(error)) if error.is_version_skew()),
            "{upsert:?}"
        );
    }

    fn laser() -> Laser {
        Laser::from_client(crate::iggy::prelude::IggyClient::default()).with_default_stream("acme")
    }
}
