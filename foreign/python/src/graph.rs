use crate::client::PyLaser;
use crate::convert::py_to_de;
use crate::errors::{InvalidError, to_pyerr};
use laser_sdk::laser::Laser;
use laser_sdk::query::{CmpOp, Filter, Projection, ProjectionKind, Value};
use laser_sdk::wire::graph::{EdgeDir, EdgeId, GraphEdge, GraphNode, GraphResult, NodeId};
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
    Ok(GraphEdge {
        id,
        from,
        to,
        edge_type,
        weight,
        attrs: attrs_from_dict(dict)?,
    })
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
    Ok(dict.into_any().unbind())
}

fn edge_to_py(py: Python<'_>, edge: &GraphEdge) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("id", edge.id.to_string())?;
    dict.set_item("from", edge.from.to_string())?;
    dict.set_item("to", edge.to.to_string())?;
    dict.set_item("edge_type", &edge.edge_type)?;
    dict.set_item("weight", edge.weight)?;
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

#[gen_stub_pymethods]
#[pymethods]
impl PyGraph {
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
    #[pyo3(signature = (node, *, direction="out", edge_type=None, depth=1, limit=0))]
    fn neighbors<'py>(
        &self,
        py: Python<'py>,
        node: String,
        direction: &str,
        edge_type: Option<String>,
        depth: u32,
        limit: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        let node = parse_node_id(&node)?;
        let dir = parse_dir(direction)?;
        let laser = self.laser.clone();
        let name = self.name.clone();
        future_into_py(py, async move {
            let result = laser
                .graph(name)
                .limit(limit)
                .neighbors(node, dir, edge_type, depth)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| result_to_py(py, &result))
        })
    }

    /// Run a traversal. Start from explicit node `start_ids`, or from every node
    /// whose label equals `match_label`. `hops` is a list of `(edge_type,
    /// direction)` tuples, one per step. `returns` is `"nodes"` or `"edges"`.
    /// Returns a `{"nodes": [...], "edges": [...]}` dict.
    #[pyo3(signature = (*, start_ids=None, match_label=None, hops=None, returns="nodes", limit=0))]
    fn query<'py>(
        &self,
        py: Python<'py>,
        start_ids: Option<Vec<String>>,
        match_label: Option<String>,
        hops: Option<Vec<(String, String)>>,
        returns: &str,
        limit: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        if start_ids.is_some() == match_label.is_some() {
            return Err(InvalidError::new_err(
                "pass exactly one of 'start_ids' or 'match_label'",
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
        let returns_edges = returns == "edges";
        let laser = self.laser.clone();
        let name = self.name.clone();
        future_into_py(py, async move {
            let mut handle = laser.graph(name).limit(limit);
            handle = match ids {
                Some(ids) => handle.start_ids(ids),
                None => handle.start_match(Filter::pred(
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
            if returns_edges {
                handle = handle.return_edges();
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
    /// and an `entity_schema`). The managed host extracts nodes and edges from the
    /// bound source into the named knowledge graph. Applied asynchronously.
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
