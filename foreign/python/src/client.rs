use crate::async_bridge::future_into_py;
use crate::errors::{InvalidError, to_pyerr};
use crate::sign::PyKeyRegistry;
use laser_sdk::capabilities::{BackendDescriptor, Capabilities};
use laser_sdk::laser::Laser;
use pyo3::prelude::*;
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
    /// Connect with a bare `user:password@host:port` endpoint. Pinning `stream` only enables the default-stream shortcuts.
    #[staticmethod]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (connection_string, *, stream=None, ops_stream=None, control_topic=None, dlq_topic=None, changes_topic=None, verifier=None))]
    fn connect<'py>(
        py: Python<'py>,
        connection_string: String,
        stream: Option<String>,
        ops_stream: Option<String>,
        control_topic: Option<String>,
        dlq_topic: Option<String>,
        changes_topic: Option<String>,
        verifier: Option<&PyKeyRegistry>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let verifier = verifier.map(PyKeyRegistry::snapshot);
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
            if let Some(dlq_topic) = dlq_topic {
                builder = builder.dlq_topic(dlq_topic);
            }
            if let Some(changes_topic) = changes_topic {
                builder = builder.changes_topic(changes_topic);
            }
            if let Some(verifier) = verifier {
                builder = builder.verifier(verifier);
            }
            let laser = builder.build().await.map_err(to_pyerr)?;
            Ok(PyLaser::from_inner(laser))
        })
    }

    /// A clone of this client pinned to a default data `stream`, sharing the one
    /// connection and producer cache. Re-scope a long-lived connection to as many
    /// streams as you like.
    fn with_stream(&self, stream: String) -> PyLaser {
        PyLaser::from_inner(self.inner.with_default_stream(stream))
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

    /// A clone whose dead-letter capsules publish to `dlq_topic` on the ops
    /// stream instead of the default `dlq`.
    fn with_dlq_topic(&self, dlq_topic: String) -> PyLaser {
        PyLaser::from_inner(self.inner.clone().with_dlq_topic(dlq_topic))
    }

    /// A clone whose change-feed records publish to `changes_topic` on the ops
    /// stream instead of the default `changes`.
    fn with_changes_topic(&self, changes_topic: String) -> PyLaser {
        PyLaser::from_inner(self.inner.clone().with_changes_topic(changes_topic))
    }

    /// Return a clone with selected negotiated capabilities overridden. This is
    /// intended for bring-your-own backends and deterministic pre-gate tests.
    /// Omitted fields preserve the current capability set.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (*, managed=None, query=None, query_consistency=None, query_keyword=None, kv=None, kv_cas=None, kv_cas_fenced=None, graph=None, forks=None, agent_workflow=None, watch=None, authz=None, a2a_gateway=None, sessions=None, durable_dedup=None))]
    fn with_capabilities<'py>(
        &self,
        py: Python<'py>,
        managed: Option<bool>,
        query: Option<bool>,
        query_consistency: Option<String>,
        query_keyword: Option<bool>,
        kv: Option<bool>,
        kv_cas: Option<bool>,
        kv_cas_fenced: Option<bool>,
        graph: Option<bool>,
        forks: Option<bool>,
        agent_workflow: Option<bool>,
        watch: Option<bool>,
        authz: Option<bool>,
        a2a_gateway: Option<bool>,
        sessions: Option<bool>,
        durable_dedup: Option<bool>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let mut capabilities = inner.capabilities().await.clone();
            if let Some(value) = managed {
                capabilities.managed = value;
            }
            if let Some(value) = query {
                capabilities.query.available = value;
            }
            if let Some(value) = query_consistency {
                capabilities.query.consistency = match value.as_str() {
                    "eventual" => laser_sdk::query::Consistency::Eventual,
                    "read_your_writes" => laser_sdk::query::Consistency::ReadYourWrites,
                    "strong" => laser_sdk::query::Consistency::Strong,
                    _ => {
                        return Err(InvalidError::new_err(
                            "query_consistency must be eventual, read_your_writes, or strong",
                        ));
                    }
                };
            }
            if let Some(value) = query_keyword {
                capabilities.query.keyword = value;
            }
            if let Some(value) = kv {
                capabilities.kv.available = value;
            }
            if let Some(value) = kv_cas {
                capabilities.kv.cas = value;
            }
            if let Some(value) = kv_cas_fenced {
                capabilities.kv.cas_fenced = value;
            }
            if let Some(value) = graph {
                capabilities.graph = value;
            }
            if let Some(value) = forks {
                capabilities.forks = value;
            }
            if let Some(value) = agent_workflow {
                capabilities.agent_workflow = value;
            }
            if let Some(value) = watch {
                capabilities.watch = value;
            }
            if let Some(value) = authz {
                capabilities.authz = value;
            }
            if let Some(value) = a2a_gateway {
                capabilities.a2a_gateway = value;
            }
            if let Some(value) = sessions {
                capabilities.sessions = value;
            }
            if let Some(value) = durable_dedup {
                capabilities.durable_dedup = value;
            }
            Ok(PyLaser::from_inner(inner.with_capabilities(capabilities)))
        })
    }

    /// This client's default data stream, or `None` for a connection-only handle.
    #[getter]
    fn default_stream(&self) -> Option<String> {
        self.inner.default_stream().map(str::to_owned)
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

    /// The dead-letter topic on the ops stream.
    #[getter]
    fn dlq_topic(&self) -> String {
        self.inner.dlq_topic().to_owned()
    }

    /// The change-feed topic on the ops stream.
    #[getter]
    fn changes_topic(&self) -> String {
        self.inner.changes_topic().to_owned()
    }

    /// The capability set this client negotiated with the connected infrastructure.
    fn capabilities<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            Ok(PyCapabilities::from(inner.capabilities().await.clone()))
        })
    }

    /// Enter `async with`: yield the client for the block. The client is cheap to
    /// clone and shares one connection, so `async with await Laser.connect(...) as
    /// laser:` reads naturally. Note the `with_stream` / `with_ops_stream` /
    /// `with_control_topic` / `with_dlq_topic` / `with_changes_topic` methods
    /// return aliasing clones over the *same* connection: exiting the block does
    /// not close a clone still in use elsewhere.
    fn __aenter__<'py>(slf: Py<Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move { Ok(slf) })
    }

    /// Exit `async with`. The connection is reference-counted and closes when the
    /// last handle is dropped, so there is no explicit disconnect to call here.
    /// Returns `False` so an exception in the body is not suppressed.
    #[pyo3(signature = (_exc_type, _exc_value, _traceback))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: &Bound<'_, PyAny>,
        _exc_value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move { Ok(false) })
    }

    fn __repr__(&self) -> String {
        format!(
            "Laser(stream={:?}, ops_stream={:?})",
            self.inner.default_stream(),
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
    /// The query surface serves lexical keyword search (`Query.text(...)`).
    pub query_keyword: bool,
    /// The managed key-value surface is served.
    pub kv: bool,
    /// The key-value store serves compare-and-swap.
    pub kv_cas: bool,
    /// The key-value store serves fenced compare-and-swap (the monotonic fence an
    /// exclusive workflow step needs for an at-most-once effect).
    pub kv_cas_fenced: bool,
    /// The managed knowledge-graph surface is served.
    pub graph: bool,
    /// Managed copy-on-write forks are served.
    pub forks: bool,
    /// The managed run registry is served (`Laser.runs()` submit / cancel /
    /// status / list, and `registered=True` workflows).
    pub agent_workflow: bool,
    /// The change feed is published (`Laser.watch()`).
    pub watch: bool,
    /// The authorization control surface is served (`Laser.whoami()` and the
    /// role/binding verbs).
    pub authz: bool,
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
            query_keyword: value.query.keyword,
            kv: value.kv.available,
            kv_cas: value.kv.cas,
            kv_cas_fenced: value.kv.cas_fenced,
            graph: value.graph,
            forks: value.forks,
            agent_workflow: value.agent_workflow,
            watch: value.watch,
            authz: value.authz,
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
