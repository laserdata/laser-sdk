use crate::client::PyLaser;
use crate::convert::py_to_de;
use crate::errors::{InvalidError, to_pyerr};
use laser_sdk::laser::Laser;
use laser_sdk::query::{CmpOp, Filter, Projection, ProjectionKind, Value};
use laser_sdk::wire::graph::{
    EdgeDir, EdgeId, GraphEdge, GraphNode, GraphResult, NodeId, SourceRef,
};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyfunction, gen_stub_pymethods};
use std::str::FromStr;

// A node or edge is a plain dict on the Python surface (string ids, so a graph
// reads like JSON), built by `graph_node` / `graph_edge` and parsed back here.
// The wire ids serialize as bytes for compactness, so the conversion is manual
// rather than a serde round-trip, keeping the Python view in Crockford strings.

fn parse_node_id(text: &str) -> PyResult<NodeId> {
    NodeId::from_str(text)
        .map_err(|error| InvalidError::new_err(format!("invalid node id: {error}")))
}

// Parse the optional conversation-lens filter: a Crockford conversation id, or
// `None` to read the whole graph.
fn parse_conversation(
    conversation: Option<String>,
) -> PyResult<Option<laser_sdk::types::ConversationId>> {
    match conversation {
        Some(text) => laser_sdk::types::ConversationId::from_str(&text)
            .map(Some)
            .map_err(|error| InvalidError::new_err(format!("invalid conversation id: {error}"))),
        None => Ok(None),
    }
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

// Render a graph attribute value back to a Python scalar.
fn value_to_py(py: Python<'_>, value: &Value) -> PyResult<Py<PyAny>> {
    let any = match value {
        Value::Str(text) => text.into_pyobject(py)?.into_any().unbind(),
        Value::Int(number) => number.into_pyobject(py)?.into_any().unbind(),
        Value::Uint(number) => number.into_pyobject(py)?.into_any().unbind(),
        Value::Float(number) => number.into_pyobject(py)?.into_any().unbind(),
        Value::Bool(flag) => flag.into_pyobject(py)?.to_owned().into_any().unbind(),
        _ => py.None(),
    };
    Ok(any)
}

fn attrs_from_dict(dict: &Bound<'_, PyDict>) -> PyResult<Vec<(String, Value)>> {
    let mut attrs = Vec::new();
    if let Some(value) = dict.get_item("attrs")? {
        let attr_dict = value
            .cast::<PyDict>()
            .map_err(|_| InvalidError::new_err("node/edge 'attrs' must be a dict"))?;
        for (key, value) in attr_dict.iter() {
            attrs.push((
                key.extract::<String>()?,
                crate::convert::py_to_value(&value)?,
            ));
        }
    }
    Ok(attrs)
}

// Parse an optional `source` provenance dict (the inverse of `source_to_py`),
// so a caller can stamp where a hand-built node or edge came from. Mirrors the
// `SourceRef` variants by their `kind` tag.
fn source_from_dict(dict: &Bound<'_, PyDict>) -> PyResult<Option<SourceRef>> {
    let Some(value) = dict.get_item("source")? else {
        return Ok(None);
    };
    let source = value
        .cast::<PyDict>()
        .map_err(|_| InvalidError::new_err("'source' must be a dict"))?;
    let get = |key: &str| -> PyResult<String> {
        source
            .get_item(key)?
            .ok_or_else(|| InvalidError::new_err(format!("source needs '{key}'")))?
            .extract::<String>()
    };
    let kind = get("kind")?;
    let parsed = match kind.as_str() {
        "message" => SourceRef::Message {
            stream: source
                .get_item("stream")?
                .ok_or_else(|| InvalidError::new_err("source needs 'stream'"))?
                .extract::<u32>()?,
            topic: source
                .get_item("topic")?
                .ok_or_else(|| InvalidError::new_err("source needs 'topic'"))?
                .extract::<u32>()?,
            partition: source
                .get_item("partition")?
                .ok_or_else(|| InvalidError::new_err("source needs 'partition'"))?
                .extract::<u32>()?,
            offset: source
                .get_item("offset")?
                .ok_or_else(|| InvalidError::new_err("source needs 'offset'"))?
                .extract::<u64>()?,
            conversation: match source.get_item("conversation")? {
                Some(value) => Some(value.extract::<String>()?),
                None => None,
            },
        },
        "kv" => SourceRef::Kv {
            namespace: get("namespace")?,
            key: get("key")?,
        },
        "memory" => SourceRef::Memory { id: get("id")? },
        other => {
            return Err(InvalidError::new_err(format!(
                "source 'kind' must be 'message', 'kv', or 'memory', got '{other}'"
            )));
        }
    };
    Ok(Some(parsed))
}

fn node_from_dict(obj: &Bound<'_, PyAny>) -> PyResult<GraphNode> {
    let dict = obj
        .cast::<PyDict>()
        .map_err(|_| InvalidError::new_err("a node must be a dict"))?;
    let id = match dict.get_item("id")? {
        Some(value) => parse_node_id(&value.extract::<String>()?)?,
        None => return Err(InvalidError::new_err("a node dict needs an 'id'")),
    };
    let labels = match dict.get_item("labels")? {
        Some(value) => value.extract::<Vec<String>>()?,
        None => Vec::new(),
    };
    Ok(GraphNode {
        id,
        labels,
        attrs: attrs_from_dict(dict)?,
        embedding: None,
        source: source_from_dict(dict)?,
    })
}

fn edge_from_dict(obj: &Bound<'_, PyAny>) -> PyResult<GraphEdge> {
    let dict = obj
        .cast::<PyDict>()
        .map_err(|_| InvalidError::new_err("an edge must be a dict"))?;
    let from = parse_node_id(
        &dict
            .get_item("from")?
            .ok_or_else(|| InvalidError::new_err("an edge dict needs 'from'"))?
            .extract::<String>()?,
    )?;
    let to = parse_node_id(
        &dict
            .get_item("to")?
            .ok_or_else(|| InvalidError::new_err("an edge dict needs 'to'"))?
            .extract::<String>()?,
    )?;
    let edge_type = dict
        .get_item("edge_type")?
        .ok_or_else(|| InvalidError::new_err("an edge dict needs 'edge_type'"))?
        .extract::<String>()?;
    let id = match dict.get_item("id")? {
        Some(value) => EdgeId::from_str(&value.extract::<String>()?)
            .map_err(|error| InvalidError::new_err(format!("invalid edge id: {error}")))?,
        None => EdgeId::content(from, &edge_type, to),
    };
    let weight = match dict.get_item("weight")? {
        Some(value) => value.extract::<f32>()?,
        None => 1.0,
    };
    let valid_from = match dict.get_item("valid_from")? {
        Some(value) => Some(value.extract::<u64>()?),
        None => None,
    };
    let valid_to = match dict.get_item("valid_to")? {
        Some(value) => Some(value.extract::<u64>()?),
        None => None,
    };
    Ok(GraphEdge {
        id,
        from,
        to,
        edge_type,
        weight,
        attrs: attrs_from_dict(dict)?,
        valid_from,
        valid_to,
        source: source_from_dict(dict)?,
    })
}

// Render a node's or edge's source provenance to a tagged Python dict, so a
// reader sees where a graph element came from. Mirrors the `SourceRef` variants.
pub(crate) fn source_to_py(py: Python<'_>, source: &SourceRef) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    match source {
        SourceRef::Message {
            stream,
            topic,
            partition,
            offset,
            conversation,
        } => {
            dict.set_item("kind", "message")?;
            dict.set_item("stream", stream)?;
            dict.set_item("topic", topic)?;
            dict.set_item("partition", *partition)?;
            dict.set_item("offset", *offset)?;
            if let Some(conversation) = conversation {
                dict.set_item("conversation", conversation)?;
            }
        }
        SourceRef::Kv { namespace, key } => {
            dict.set_item("kind", "kv")?;
            dict.set_item("namespace", namespace)?;
            dict.set_item("key", key)?;
        }
        SourceRef::Memory { id } => {
            dict.set_item("kind", "memory")?;
            dict.set_item("id", id)?;
        }
    }
    Ok(dict.into_any().unbind())
}

fn node_to_py(py: Python<'_>, node: &GraphNode) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("id", node.id.to_string())?;
    dict.set_item("labels", node.labels.clone())?;
    let attrs = PyDict::new(py);
    for (key, value) in &node.attrs {
        attrs.set_item(key, value_to_py(py, value)?)?;
    }
    dict.set_item("attrs", attrs)?;
    if let Some(source) = &node.source {
        dict.set_item("source", source_to_py(py, source)?)?;
    }
    Ok(dict.into_any().unbind())
}

fn edge_to_py(py: Python<'_>, edge: &GraphEdge) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("id", edge.id.to_string())?;
    dict.set_item("from", edge.from.to_string())?;
    dict.set_item("to", edge.to.to_string())?;
    dict.set_item("edge_type", &edge.edge_type)?;
    dict.set_item("weight", edge.weight)?;
    if let Some(valid_from) = edge.valid_from {
        dict.set_item("valid_from", valid_from)?;
    }
    if let Some(valid_to) = edge.valid_to {
        dict.set_item("valid_to", valid_to)?;
    }
    if let Some(source) = &edge.source {
        dict.set_item("source", source_to_py(py, source)?)?;
    }
    Ok(dict.into_any().unbind())
}

fn result_to_py(py: Python<'_>, result: &GraphResult) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    let nodes = PyList::empty(py);
    for node in &result.nodes {
        nodes.append(node_to_py(py, node)?)?;
    }
    let edges = PyList::empty(py);
    for edge in &result.edges {
        edges.append(edge_to_py(py, edge)?)?;
    }
    dict.set_item("nodes", nodes)?;
    dict.set_item("edges", edges)?;
    if !result.paths.is_empty() {
        let paths = PyList::empty(py);
        for path in &result.paths {
            let entry = PyDict::new(py);
            entry.set_item(
                "nodes",
                path.nodes
                    .iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>(),
            )?;
            entry.set_item(
                "edges",
                path.edges
                    .iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>(),
            )?;
            paths.append(entry)?;
        }
        dict.set_item("paths", paths)?;
    }
    Ok(dict.into_any().unbind())
}

/// The content-addressed id for the entity `value` labelled `label`, as a
/// Crockford string. The same entity always yields the same id, in any SDK, so a
/// graph shared across languages converges on one node.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn node_id(label: &str, value: &str) -> String {
    NodeId::content(label, value.as_bytes()).to_string()
}

/// The content-addressed id for the edge `edge_type` from `from_id` to `to_id`.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn edge_id(from_id: &str, edge_type: &str, to_id: &str) -> PyResult<String> {
    let from = parse_node_id(from_id)?;
    let to = parse_node_id(to_id)?;
    Ok(EdgeId::content(from, edge_type, to).to_string())
}

/// Build a node dict for the entity `value` labelled `label`: its id
/// content-addressed and the value kept as a `value` attribute, so re-observing
/// the same entity converges. The ergonomic way to assemble a node for `upsert`.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn graph_node(py: Python<'_>, label: &str, value: &str) -> PyResult<Py<PyAny>> {
    node_to_py(py, &GraphNode::entity(label, value))
}

/// Build an edge dict of `edge_type` from `from_node` to `to_node` (both node
/// dicts), its id content-addressed over the endpoints and type.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn graph_edge(
    py: Python<'_>,
    from_node: &Bound<'_, PyAny>,
    edge_type: &str,
    to_node: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let from = node_from_dict(from_node)?;
    let to = node_from_dict(to_node)?;
    edge_to_py(py, &GraphEdge::relate(&from, edge_type, &to))
}

/// A handle to the knowledge-graph surface `name`, built with `laser.graph(name)`.
/// `upsert` writes nodes and edges, `neighbors` reads a node's neighborhood, and
/// `query` runs a multi-hop traversal. A managed feature: against raw Apache Iggy
/// every call raises `UnsupportedError`.
#[gen_stub_pyclass]
#[pyclass(name = "Graph", frozen)]
pub struct PyGraph {
    laser: Laser,
    name: String,
}

impl PyGraph {
    pub(crate) fn new(laser: Laser, name: String) -> Self {
        Self { laser, name }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyGraph {
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

    /// Write `nodes` and `edges` (lists of dicts from `graph_node` / `graph_edge`)
    /// into the graph. Idempotent on the content-addressed ids, so re-upserting the
    /// same entities is a no-op.
    fn upsert<'py>(
        &self,
        py: Python<'py>,
        nodes: &Bound<'_, PyList>,
        edges: &Bound<'_, PyList>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let parsed_nodes = nodes
            .iter()
            .map(|node| node_from_dict(&node))
            .collect::<PyResult<Vec<_>>>()?;
        let parsed_edges = edges
            .iter()
            .map(|edge| edge_from_dict(&edge))
            .collect::<PyResult<Vec<_>>>()?;
        let laser = self.laser.clone();
        let name = self.name.clone();
        future_into_py(py, async move {
            laser
                .graph(name)
                .upsert(parsed_nodes, parsed_edges)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Read `node`'s neighbors: the nodes reachable in `direction` over
    /// `edge_type` (any type when `None`), following the same hop `depth` times.
    /// Returns a `{"nodes": [...], "edges": [...]}` dict.
    #[pyo3(signature = (node, *, direction="out", edge_type=None, depth=1, limit=0, as_of=None, conversation=None))]
    #[allow(clippy::too_many_arguments)]
    fn neighbors<'py>(
        &self,
        py: Python<'py>,
        node: String,
        direction: &str,
        edge_type: Option<String>,
        depth: u32,
        limit: usize,
        as_of: Option<u64>,
        conversation: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let node = parse_node_id(&node)?;
        let dir = parse_dir(direction)?;
        let conversation = parse_conversation(conversation)?;
        let laser = self.laser.clone();
        let name = self.name.clone();
        future_into_py(py, async move {
            let mut handle = laser.graph(name).limit(limit);
            if let Some(at) = as_of {
                handle = handle.as_of(at);
            }
            if let Some(conversation) = conversation {
                handle = handle.conversation(conversation);
            }
            let result = handle
                .neighbors(node, dir, edge_type, depth)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| result_to_py(py, &result))
        })
    }

    /// Run a traversal. Start from explicit node `start_ids`, from every node
    /// whose label equals `match_label`, or from the `nearest` nodes to an
    /// embedding (a `(embedding, k)` pair). `hops` is a list of `(edge_type,
    /// direction)` tuples, one per step. `returns` is `"nodes"`, `"edges"`,
    /// `"triplets"`, or `"paths"`. `as_of` (epoch micros) follows only edges
    /// valid at that instant.
    /// Returns a `{"nodes": [...], "edges": [...], "paths": [...]}` dict.
    #[pyo3(signature = (*, start_ids=None, match_label=None, nearest=None, hops=None, returns="nodes", limit=0, as_of=None, conversation=None))]
    #[allow(clippy::too_many_arguments)]
    fn query<'py>(
        &self,
        py: Python<'py>,
        start_ids: Option<Vec<String>>,
        match_label: Option<String>,
        nearest: Option<(Vec<f32>, usize)>,
        hops: Option<Vec<(String, String)>>,
        returns: &str,
        limit: usize,
        as_of: Option<u64>,
        conversation: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if [
            start_ids.is_some(),
            match_label.is_some(),
            nearest.is_some(),
        ]
        .iter()
        .filter(|set| **set)
        .count()
            != 1
        {
            return Err(InvalidError::new_err(
                "pass exactly one of 'start_ids', 'match_label', or 'nearest'",
            ));
        }
        let ids = match start_ids {
            Some(ids) => Some(
                ids.iter()
                    .map(|id| parse_node_id(id))
                    .collect::<PyResult<Vec<_>>>()?,
            ),
            None => None,
        };
        let parsed_hops = hops
            .unwrap_or_default()
            .into_iter()
            .map(|(edge_type, direction)| Ok((edge_type, parse_dir(&direction)?)))
            .collect::<PyResult<Vec<_>>>()?;
        let returns = returns.to_owned();
        let conversation = parse_conversation(conversation)?;
        let laser = self.laser.clone();
        let name = self.name.clone();
        future_into_py(py, async move {
            let mut handle = laser.graph(name).limit(limit);
            if let Some(conversation) = conversation {
                handle = handle.conversation(conversation);
            }
            handle = match (ids, nearest) {
                (Some(ids), _) => handle.start_ids(ids),
                (_, Some((embedding, k))) => handle.start_nearest(embedding, k),
                _ => handle.start_match(Filter::pred(
                    "label",
                    CmpOp::Eq,
                    match_label.unwrap_or_default(),
                )),
            };
            for (edge_type, dir) in parsed_hops {
                handle = match dir {
                    EdgeDir::Out => handle.out(edge_type),
                    EdgeDir::In => handle.incoming(edge_type),
                    EdgeDir::Both => handle.both(edge_type),
                };
            }
            handle = match returns.as_str() {
                "edges" => handle.return_edges(),
                "triplets" => handle.return_triplets(),
                "paths" => handle.return_paths(),
                _ => handle,
            };
            if let Some(at) = as_of {
                handle = handle.as_of(at);
            }
            let result = handle.fetch().await.map_err(to_pyerr)?;
            Python::attach(|py| result_to_py(py, &result))
        })
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    /// A handle to the knowledge-graph surface `name`. A managed feature: against
    /// raw Apache Iggy its calls raise `UnsupportedError`.
    fn graph(&self, name: String) -> PyGraph {
        PyGraph {
            laser: self.inner.clone(),
            name,
        }
    }

    /// Register a graph projection from a dict (a projection with `kind = "graph"`
    /// and an `entity_schema`). It records the named knowledge graph and its
    /// node/edge extraction plan. Graph data is written via `graph(name).upsert(..)`,
    /// or by the projector when the projection is bound to a source topic, which
    /// applies the entity schema to each record. Applied asynchronously.
    fn register_graph<'py>(
        &self,
        py: Python<'py>,
        projection: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let mut projection: Projection = py_to_de(projection)?;
        // A graph projection is always `kind = Graph`, so the dict need not carry
        // the wire code: set it here from the entity schema the caller passed.
        projection.kind = ProjectionKind::Graph;
        future_into_py(py, async move {
            laser
                .projections()
                .register_graph(projection)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Drop the graph projection registered under `id`. Materialized nodes and
    /// edges are left untouched.
    fn drop_graph<'py>(&self, py: Python<'py>, id: String) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        future_into_py(py, async move {
            laser.projections().drop_graph(id).await.map_err(to_pyerr)
        })
    }
}
