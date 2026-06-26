use crate::errors::to_pyerr;
use laser_sdk::capabilities::{BackendDescriptor, Capabilities};
use laser_sdk::laser::Laser;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

/// The connected LaserData / Apache Iggy client. Cheap to clone: the connection
/// and producer cache are shared internally, so one `Laser` serves any number of
/// concurrent operations.
#[gen_stub_pyclass]
#[pyclass(name = "Laser", frozen)]
pub struct PyLaser {
    pub(crate) inner: Laser,
}

impl PyLaser {
    pub(crate) fn from_inner(inner: Laser) -> Self {
        Self { inner }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    /// Connect using an Iggy connection string. The scheme is optional
    /// (`iggy://` is assumed). Pin a default `stream` so the convenience methods
    /// (`publish`, `bootstrap`, the agent verbs) take just a topic, otherwise
    /// name the stream per operation with the `*_on` methods.
    #[staticmethod]
    #[pyo3(signature = (connection_string, *, stream=None, ops_stream=None, control_topic=None))]
    fn connect<'py>(
        py: Python<'py>,
        connection_string: String,
        stream: Option<String>,
        ops_stream: Option<String>,
        control_topic: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move {
            let mut builder = Laser::builder().connection_string(connection_string);
            if let Some(stream) = stream {
                builder = builder.stream(stream);
            }
            if let Some(ops_stream) = ops_stream {
                builder = builder.ops_stream(ops_stream);
            }
            if let Some(control_topic) = control_topic {
                builder = builder.control_topic(control_topic);
            }
            let laser = builder.build().await.map_err(to_pyerr)?;
            Ok(PyLaser::from_inner(laser))
        })
    }

    /// A clone of this client pinned to a default data `stream`, sharing the one
    /// connection and producer cache. Re-scope a long-lived connection to as many
    /// streams as you like.
    fn with_stream(&self, stream: String) -> PyLaser {
        PyLaser::from_inner(self.inner.with_stream(stream))
    }

    /// A clone whose query / control surface rides `ops_stream` instead of the
    /// default `_agdx`. Production keeps the default.
    fn with_ops_stream(&self, ops_stream: String) -> PyLaser {
        PyLaser::from_inner(self.inner.clone().with_ops_stream(ops_stream))
    }

    /// A clone whose control commands publish to `control_topic` on the ops
    /// stream instead of the default `control.commands`.
    fn with_control_topic(&self, control_topic: String) -> PyLaser {
        PyLaser::from_inner(self.inner.clone().with_control_topic(control_topic))
    }

    /// This client's default data stream, or `None` for a connection-only handle.
    #[getter]
    fn stream(&self) -> Option<String> {
        self.inner.stream().map(str::to_owned)
    }

    /// The Iggy stream carrying this client's query / control ops surface.
    #[getter]
    fn ops_stream(&self) -> String {
        self.inner.ops_stream().to_owned()
    }

    /// The control-command topic on the ops stream.
    #[getter]
    fn control_topic(&self) -> String {
        self.inner.control_topic().to_owned()
    }

    /// The capability set this client negotiated with the connected infrastructure.
    fn capabilities<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            Ok(PyCapabilities::from(inner.capabilities().await))
        })
    }

    /// Idempotently create `topic` on the default stream with `partitions`,
    /// creating the stream first if needed. Requires a default stream.
    #[pyo3(signature = (topic, partitions))]
    fn ensure_topic<'py>(
        &self,
        py: Python<'py>,
        topic: String,
        partitions: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner
                .ensure_topic(&topic, partitions)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Idempotently create `topic` on an explicit `stream`.
    #[pyo3(signature = (stream, topic, partitions))]
    fn ensure_topic_on<'py>(
        &self,
        py: Python<'py>,
        stream: String,
        topic: String,
        partitions: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner
                .ensure_topic_on(&stream, &topic, partitions)
                .await
                .map_err(to_pyerr)
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "Laser(stream={:?}, ops_stream={:?})",
            self.inner.stream(),
            self.inner.ops_stream()
        )
    }
}

/// A read-only snapshot of the premium capability set the connected
/// infrastructure advertised. All flags are false against raw Apache Iggy.
#[gen_stub_pyclass]
#[pyclass(name = "Capabilities", frozen, get_all)]
pub struct PyCapabilities {
    /// Connected to a managed plane (the root managed switch).
    pub managed: bool,
    /// The managed query surface is served.
    pub query: bool,
    /// The strongest read-consistency the query surface serves
    /// (`eventual` / `read_your_writes` / `strong`).
    pub query_consistency: String,
    /// The managed key-value surface is served.
    pub kv: bool,
    /// The key-value store serves compare-and-swap.
    pub kv_cas: bool,
    /// The managed knowledge-graph surface is served.
    pub graph: bool,
    /// Managed copy-on-write forks are served.
    pub forks: bool,
    /// A managed A2A gateway is available.
    pub a2a_gateway: bool,
    /// Platform-native session lifecycle.
    pub sessions: bool,
    /// Platform-side durable deduplication.
    pub durable_dedup: bool,
    /// The materialization backends the connected server exposes (identity
    /// only). Empty against raw Apache Iggy and servers that advertise none.
    pub backends: Vec<PyBackendDescriptor>,
}

impl From<Capabilities> for PyCapabilities {
    fn from(value: Capabilities) -> Self {
        Self {
            managed: value.managed,
            query: value.query.available,
            query_consistency: match value.query.consistency {
                laser_sdk::query::Consistency::ReadYourWrites => "read_your_writes",
                laser_sdk::query::Consistency::Strong => "strong",
                _ => "eventual",
            }
            .to_owned(),
            kv: value.kv.available,
            kv_cas: value.kv.cas,
            graph: value.graph,
            forks: value.forks,
            a2a_gateway: value.a2a_gateway,
            sessions: value.sessions,
            durable_dedup: value.durable_dedup,
            backends: value
                .backends
                .into_iter()
                .map(PyBackendDescriptor::from)
                .collect(),
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyCapabilities {
    fn __repr__(&self) -> String {
        format!(
            "Capabilities(managed={}, query={}, kv={}, graph={}, forks={}, kv_cas={}, backends={})",
            self.managed,
            self.query,
            self.kv,
            self.graph,
            self.forks,
            self.kv_cas,
            self.backends.len()
        )
    }
}

/// One materialization backend the connected server exposes: identity only, a
/// stable `id` and an opaque engine `kind`, with optional advisory `label` and
/// `version` and a set of opaque `capabilities` tags the backend declares about
/// itself (e.g. "ingest", "query", a query-surface feature). A consumer routes
/// only to an advertised `id` and matches the tags it understands.
#[gen_stub_pyclass]
#[pyclass(name = "BackendDescriptor", frozen, get_all, skip_from_py_object)]
#[derive(Clone)]
pub struct PyBackendDescriptor {
    pub id: String,
    pub kind: String,
    pub label: Option<String>,
    pub version: Option<String>,
    pub capabilities: Vec<String>,
}

impl From<BackendDescriptor> for PyBackendDescriptor {
    fn from(value: BackendDescriptor) -> Self {
        Self {
            id: value.id,
            kind: value.kind,
            label: value.label,
            version: value.version,
            capabilities: value.capabilities,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyBackendDescriptor {
    /// Whether the backend declared the opaque capability `tag`.
    fn has_capability(&self, tag: &str) -> bool {
        self.capabilities.iter().any(|c| c == tag)
    }

    fn __repr__(&self) -> String {
        format!(
            "BackendDescriptor(id={}, kind={}, capabilities={:?})",
            self.id, self.kind, self.capabilities
        )
    }
}
