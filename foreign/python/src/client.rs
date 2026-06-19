use crate::errors::to_pyerr;
use laser_sdk::capabilities::Capabilities;
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
    pub sessions: bool,
    pub forks: bool,
    pub durable_dedup: bool,
    pub managed_memory: bool,
    pub managed_query: bool,
    pub managed_kv: bool,
    pub managed_host: bool,
    pub a2a_gateway: bool,
    pub kv_cas: bool,
    pub read_your_writes: bool,
    pub strong_consistency: bool,
}

impl From<Capabilities> for PyCapabilities {
    fn from(value: Capabilities) -> Self {
        Self {
            sessions: value.sessions,
            forks: value.forks,
            durable_dedup: value.durable_dedup,
            managed_memory: value.managed_memory,
            managed_query: value.managed_query,
            managed_kv: value.managed_kv,
            managed_host: value.managed_host,
            a2a_gateway: value.a2a_gateway,
            kv_cas: value.kv_cas,
            read_your_writes: value.read_your_writes,
            strong_consistency: value.strong_consistency,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyCapabilities {
    fn __repr__(&self) -> String {
        format!(
            "Capabilities(managed_host={}, managed_query={}, managed_kv={}, forks={}, kv_cas={})",
            self.managed_host, self.managed_query, self.managed_kv, self.forks, self.kv_cas
        )
    }
}
