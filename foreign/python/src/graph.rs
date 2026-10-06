use crate::async_bridge::future_into_py;
use crate::client::PyLaser;
use crate::convert::{py_to_de, ser_to_py};
use crate::errors::{InvalidError, to_pyerr};
use laser_sdk::laser::Laser;
use laser_sdk::query::Filter;
use laser_sdk::types::ConversationId;
use laser_sdk::wire::graph::{
    EdgeDir, EdgeId, GraphEdge, GraphNode, GraphReturn, NodeId, SourceRef,
};
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyfunction, gen_stub_pymethods};
use std::str::FromStr;

// Nodes, edges, traversal results, and source references cross as the serde
// dicts of their Rust types. Ids are Crockford strings and attributes are
// `[name, value]` pairs, the same shape every SDK reads off the wire.

/// The content-addressed id for the entity `value` labelled `label`, as a
/// Crockford string. The same entity always yields the same id, in any SDK, so a
/// graph shared across languages converges on one node.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn node_id_content(label: &str, value: &str) -> String {
    NodeId::content(label, value.as_bytes()).to_string()
}

/// The content-addressed id for the edge `edge_type` from the node id `from_`
/// to the node id `to`, as a Crockford string.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn edge_id_content(from_: &str, edge_type: &str, to: &str) -> PyResult<String> {
    let from = parse_node_id(from_)?;
    let to = parse_node_id(to)?;
    Ok(EdgeId::content(from, edge_type, to).to_string())
}

/// The node dict for the entity `value` labelled `label`: its id
/// content-addressed and the value kept as a `value` attribute, so re-observing
/// the same entity converges.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn graph_node_entity(py: Python<'_>, label: &str, value: &str) -> PyResult<Py<PyAny>> {
    ser_to_py(py, &GraphNode::entity(label, value))
}

/// The edge dict of `edge_type` from the node dict `from_` to the node dict
/// `to`, weight `1.0`, its id content-addressed over the endpoints and type.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn graph_edge_relate(
    py: Python<'_>,
    from_: &Bound<'_, PyAny>,
    edge_type: &str,
    to: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let from: GraphNode = py_to_de(from_)?;
    let to: GraphNode = py_to_de(to)?;
    ser_to_py(py, &GraphEdge::relate(&from, edge_type, &to))
}

/// `edge` with the source record that asserted it. The edge id is unchanged.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn graph_edge_with_source(
    py: Python<'_>,
    edge: &Bound<'_, PyAny>,
    source: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let edge: GraphEdge = py_to_de(edge)?;
    let source: SourceRef = py_to_de(source)?;
    ser_to_py(py, &edge.with_source(source))
}

/// `edge` with the valid-time window `[from_, to)` in epoch micros. Either
/// bound may be `None` for open-ended. The edge id is unchanged.
#[gen_stub_pyfunction]
#[pyfunction]
#[pyo3(signature = (edge, from_=None, to=None))]
pub fn graph_edge_valid(
    py: Python<'_>,
    edge: &Bound<'_, PyAny>,
    from_: Option<u64>,
    to: Option<u64>,
) -> PyResult<Py<PyAny>> {
    let edge: GraphEdge = py_to_de(edge)?;
    ser_to_py(py, &edge.valid(from_, to))
}

/// Whether `edge`'s valid-time window contains `at` (epoch micros). An open
/// bound is unbounded.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn graph_edge_valid_at(edge: &Bound<'_, PyAny>, at: u64) -> PyResult<bool> {
    let edge: GraphEdge = py_to_de(edge)?;
    Ok(edge.valid_at(at))
}

/// Whether `dir` (`"out"`, `"in"`, or `"both"`) is the default `"out"`.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn edge_dir_is_out(dir: &Bound<'_, PyAny>) -> PyResult<bool> {
    Ok(py_to_de::<EdgeDir>(dir)?.is_out())
}

/// Whether `returns` (`"nodes"`, `"edges"`, `"paths"`, or `"triplets"`) is the
/// default `"nodes"`.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn graph_return_is_nodes(returns: &Bound<'_, PyAny>) -> PyResult<bool> {
    Ok(py_to_de::<GraphReturn>(returns)?.is_nodes())
}

/// A fluent knowledge-graph traversal over the graph `name`, built with
/// `laser.graph(name)`. Set a start (`start_ids`, `start_match`,
/// `start_nearest`), add hops (`out`, `incoming`, `both`), pick what to return,
/// and await `fetch()`. `neighbors` reads one node's neighborhood, and `link`,
/// `relink`, `unlink`, and `upsert` write. A managed feature: against Apache
/// Iggy every read and write raises `UnsupportedError`.
#[gen_stub_pyclass]
#[pyclass(name = "GraphHandle")]
pub struct PyGraph {
    laser: Laser,
    name: String,
    traversal: Traversal,
}

impl PyGraph {
    pub(crate) fn new(laser: Laser, name: String) -> Self {
        Self {
            laser,
            name,
            traversal: Traversal::default(),
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyGraph {
    /// Narrow the traversal to elements the conversation asserted. Applies to
    /// `fetch` and `neighbors`.
    fn conversation(
        mut slf: PyRefMut<'_, Self>,
        conversation: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.traversal.conversation =
            Some(ConversationId::from_str(&conversation).map_err(|error| {
                InvalidError::new_err(format!("invalid conversation id: {error}"))
            })?);
        Ok(slf)
    }

    /// Start the traversal from explicit node ids.
    fn start_ids(mut slf: PyRefMut<'_, Self>, ids: Vec<String>) -> PyResult<PyRefMut<'_, Self>> {
        let ids = ids
            .iter()
            .map(|id| parse_node_id(id))
            .collect::<PyResult<Vec<_>>>()?;
        slf.traversal.start = Some(Start::Ids(ids));
        Ok(slf)
    }

    /// Start from the nodes matching a `Filter` predicate over node fields.
    fn start_match<'py>(
        mut slf: PyRefMut<'py, Self>,
        filter: PyRef<'_, crate::query::PyQueryFilter>,
    ) -> PyRefMut<'py, Self> {
        slf.traversal.start = Some(Start::Match(filter.inner.clone()));
        slf
    }

    /// Start from the `k` nodes nearest `embedding`.
    fn start_nearest(
        mut slf: PyRefMut<'_, Self>,
        embedding: Vec<f32>,
        k: usize,
    ) -> PyRefMut<'_, Self> {
        slf.traversal.start = Some(Start::Nearest(embedding, k));
        slf
    }

    /// Follow outgoing edges of `edge_type` one hop.
    fn out(mut slf: PyRefMut<'_, Self>, edge_type: String) -> PyRefMut<'_, Self> {
        slf.traversal.hops.push((edge_type, EdgeDir::Out));
        slf
    }

    /// Follow incoming edges of `edge_type` one hop.
    fn incoming(mut slf: PyRefMut<'_, Self>, edge_type: String) -> PyRefMut<'_, Self> {
        slf.traversal.hops.push((edge_type, EdgeDir::In));
        slf
    }

    /// Follow edges of `edge_type` one hop in both directions.
    fn both(mut slf: PyRefMut<'_, Self>, edge_type: String) -> PyRefMut<'_, Self> {
        slf.traversal.hops.push((edge_type, EdgeDir::Both));
        slf
    }

    /// Return the traversed edges instead of the reachable nodes.
    fn return_edges(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.traversal.returns = GraphReturn::Edges;
        slf
    }

    /// Return the traversed edges as `(source, type, destination)` triplets.
    fn return_triplets(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.traversal.returns = GraphReturn::Triplets;
        slf
    }

    /// Return whole paths (node and edge id sequences) instead of nodes.
    fn return_paths(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.traversal.returns = GraphReturn::Paths;
        slf
    }

    /// Cap the number of elements returned.
    fn limit(mut slf: PyRefMut<'_, Self>, limit: usize) -> PyRefMut<'_, Self> {
        slf.traversal.limit = Some(limit);
        slf
    }

    /// Read the graph as of `micros` (valid time, epoch micros). Applies to
    /// `fetch` and `neighbors`.
    fn as_of(mut slf: PyRefMut<'_, Self>, micros: u64) -> PyRefMut<'_, Self> {
        slf.traversal.as_of = Some(micros);
        slf
    }

    /// Run the traversal. Returns the result dict with `nodes`, `edges`, and
    /// `paths`, each present when the traversal populated it.
    fn fetch<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let name = self.name.clone();
        let traversal = self.traversal.clone();
        future_into_py(py, async move {
            let result = traversal
                .apply(laser.graph(name))
                .fetch()
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &result))
        })
    }

    /// Read `node`'s neighbors: the nodes reachable in `dir` (`"out"`, `"in"`,
    /// or `"both"`) over `edge_type` (any type when `None`), following the same
    /// hop `depth` times. Honors `limit`, `as_of`, and `conversation`.
    #[pyo3(signature = (node, dir="out", edge_type=None, depth=1))]
    fn neighbors<'py>(
        &self,
        py: Python<'py>,
        node: String,
        dir: &str,
        edge_type: Option<String>,
        depth: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let node = parse_node_id(&node)?;
        let dir = parse_dir(dir)?;
        let laser = self.laser.clone();
        let name = self.name.clone();
        let traversal = self.traversal.clone();
        future_into_py(py, async move {
            let result = traversal
                .apply(laser.graph(name))
                .neighbors(node, dir, edge_type, depth)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &result))
        })
    }

    /// Relate two entities in one call: upserts both content-addressed entity
    /// nodes (`kind:value` style ids) and the typed edge between them.
    /// Re-linking the same triple converges.
    #[pyo3(signature = (from_, relation, to))]
    fn link<'py>(
        &self,
        py: Python<'py>,
        from_: String,
        relation: String,
        to: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let name = self.name.clone();
        future_into_py(py, async move {
            laser
                .graph(&name)
                .link(from_, relation, to)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Close the relationship `link` opened (`valid_to` now, the bitemporal
    /// supersede). The nodes stay.
    #[pyo3(signature = (from_, relation, to))]
    fn unlink<'py>(
        &self,
        py: Python<'py>,
        from_: String,
        relation: String,
        to: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let name = self.name.clone();
        future_into_py(py, async move {
            laser
                .graph(&name)
                .unlink(from_, relation, to)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Assert the latest value of a single-valued relationship: close every
    /// live same-relation edge to a different target, then link the new one.
    /// Returns how many superseded edges were closed.
    #[pyo3(signature = (from_, relation, to))]
    fn relink<'py>(
        &self,
        py: Python<'py>,
        from_: String,
        relation: String,
        to: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let name = self.name.clone();
        future_into_py(py, async move {
            laser
                .graph(&name)
                .relink(from_, relation, to)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Write `nodes` and `edges` (node and edge dicts, as `graph_node_entity`
    /// and `graph_edge_relate` build them) into the graph. Idempotent on the
    /// content-addressed ids, so re-upserting the same entities is a no-op.
    fn upsert<'py>(
        &self,
        py: Python<'py>,
        nodes: &Bound<'_, PyAny>,
        edges: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let nodes: Vec<GraphNode> = py_to_de(nodes)?;
        let edges: Vec<GraphEdge> = py_to_de(edges)?;
        let laser = self.laser.clone();
        let name = self.name.clone();
        future_into_py(py, async move {
            laser
                .graph(name)
                .upsert(nodes, edges)
                .await
                .map_err(to_pyerr)
        })
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    /// A traversal handle over the knowledge graph `name`. A managed feature:
    /// against Apache Iggy its reads and writes raise `UnsupportedError`.
    fn graph(&self, name: String) -> PyGraph {
        PyGraph::new(self.inner.clone(), name)
    }
}

// Render a node's or edge's source reference, the serde dict a `KvEntry`
// shares with graph elements.
pub(crate) fn source_to_py(py: Python<'_>, source: &SourceRef) -> PyResult<Py<PyAny>> {
    ser_to_py(py, source)
}

// The traversal a Python handle accumulates, replayed onto the borrowed Rust
// handle at each terminal call.
#[derive(Clone, Default)]
struct Traversal {
    start: Option<Start>,
    hops: Vec<(String, EdgeDir)>,
    returns: GraphReturn,
    limit: Option<usize>,
    as_of: Option<u64>,
    conversation: Option<ConversationId>,
}

#[derive(Clone)]
enum Start {
    Ids(Vec<NodeId>),
    Match(Filter),
    Nearest(Vec<f32>, usize),
}

impl Traversal {
    fn apply(
        self,
        mut handle: laser_sdk::graph::GraphHandle<'_>,
    ) -> laser_sdk::graph::GraphHandle<'_> {
        handle = match self.start {
            Some(Start::Ids(ids)) => handle.start_ids(ids),
            Some(Start::Match(filter)) => handle.start_match(filter),
            Some(Start::Nearest(embedding, k)) => handle.start_nearest(embedding, k),
            None => handle,
        };
        for (edge_type, dir) in self.hops {
            handle = match dir {
                EdgeDir::Out => handle.out(edge_type),
                EdgeDir::In => handle.incoming(edge_type),
                EdgeDir::Both => handle.both(edge_type),
            };
        }
        handle = match self.returns {
            GraphReturn::Edges => handle.return_edges(),
            GraphReturn::Triplets => handle.return_triplets(),
            GraphReturn::Paths => handle.return_paths(),
            GraphReturn::Nodes => handle,
        };
        if let Some(limit) = self.limit {
            handle = handle.limit(limit);
        }
        if let Some(micros) = self.as_of {
            handle = handle.as_of(micros);
        }
        if let Some(conversation) = self.conversation {
            handle = handle.conversation(conversation);
        }
        handle
    }
}

fn parse_node_id(text: &str) -> PyResult<NodeId> {
    NodeId::from_str(text)
        .map_err(|error| InvalidError::new_err(format!("invalid node id: {error}")))
}

fn parse_dir(direction: &str) -> PyResult<EdgeDir> {
    match direction {
        "out" => Ok(EdgeDir::Out),
        "in" => Ok(EdgeDir::In),
        "both" => Ok(EdgeDir::Both),
        other => Err(InvalidError::new_err(format!(
            "direction must be 'out', 'in', or 'both', got '{other}'"
        ))),
    }
}
