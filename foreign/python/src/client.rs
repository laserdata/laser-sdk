use crate::async_bridge::future_into_py;
use crate::convert::{duration_seconds, py_to_de};
use crate::errors::{InvalidError, to_pyerr};
use crate::sign::PyKeyRegistry;
use laser_sdk::capabilities::{BackendDescriptor, Capabilities, OpVersions};
use laser_sdk::laser::Laser;
use laser_sdk::wire::batch::BatchItem;
use laser_sdk::wire::checkpoint::CheckpointReadConsistency;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
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
    /// Connect with a bare `user:password@host:port` endpoint. Pinning `stream` only enables the default-stream shortcuts. Connecting gives up after `connect_timeout_ms`, default 30000 or `LASER_CONNECT_TIMEOUT_MS`, with a `TimeoutError` naming whether the server never accepted the connection or never answered the login.
    #[staticmethod]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (connection_string, *, stream=None, ops_stream=None, control_topic=None, dlq_topic=None, changes_topic=None, verifier=None, connect_timeout_ms=None, publish_timeout_ms=None, publish_max_retries=None, publish_retry_backoff_ms=None))]
    fn connect<'py>(
        py: Python<'py>,
        connection_string: String,
        stream: Option<String>,
        ops_stream: Option<String>,
        control_topic: Option<String>,
        dlq_topic: Option<String>,
        changes_topic: Option<String>,
        verifier: Option<&PyKeyRegistry>,
        connect_timeout_ms: Option<u64>,
        publish_timeout_ms: Option<u64>,
        publish_max_retries: Option<u32>,
        publish_retry_backoff_ms: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let verifier = verifier.map(PyKeyRegistry::snapshot);
        future_into_py(py, async move {
            let mut builder = Laser::builder().connection_string(connection_string);
            if let Some(value) = connect_timeout_ms {
                builder = builder.connect_timeout(std::time::Duration::from_millis(value));
            }
            if let Some(value) = publish_timeout_ms {
                builder = builder.publish_timeout(std::time::Duration::from_millis(value));
            }
            if let Some(value) = publish_max_retries {
                builder = builder.publish_max_retries(value);
            }
            if let Some(value) = publish_retry_backoff_ms {
                builder = builder.publish_retry_backoff(std::time::Duration::from_millis(value));
            }
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

    /// Connect from the environment: `LASER_CONNECTION_STRING` (required, else
    /// `ConfigError`) and an optional `LASER_STREAM` default stream. The timeout
    /// and publish variables (`LASER_CONNECT_TIMEOUT_MS` and the `LASER_PUBLISH_*`
    /// family) apply as on `connect`.
    #[staticmethod]
    fn connect_env(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
        future_into_py(py, async move {
            let laser = Laser::connect_env().await.map_err(to_pyerr)?;
            Ok(PyLaser::from_inner(laser))
        })
    }

    /// Connect to a local Apache Iggy on `iggy:iggy@127.0.0.1:8090`, the Laser
    /// Stack default.
    #[staticmethod]
    fn local(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
        future_into_py(py, async move {
            let laser = Laser::local().await.map_err(to_pyerr)?;
            Ok(PyLaser::from_inner(laser))
        })
    }

    /// Connect and pin a default `stream`, the shorthand for
    /// `connect(connection_string, stream=stream)`.
    #[staticmethod]
    fn connect_with_stream(
        py: Python<'_>,
        connection_string: String,
        stream: String,
    ) -> PyResult<Bound<'_, PyAny>> {
        future_into_py(py, async move {
            let laser = Laser::connect_with_stream(&connection_string, &stream)
                .await
                .map_err(to_pyerr)?;
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
    #[pyo3(signature = (*, managed=None, query=None, query_consistency=None, query_keyword=None, destinations=None, destinations_consistency=None, kv=None, kv_cas=None, kv_cas_fenced=None, kv_fenced_leases=None, graph=None, forks=None, agent_workflow=None, watch=None, authz=None, filters=None, filters_catalog=None, filters_group_policy_reads=None, a2a_gateway=None, query_execution=None, versions=None, backends=None))]
    fn with_capabilities<'py>(
        &self,
        py: Python<'py>,
        managed: Option<bool>,
        query: Option<bool>,
        query_consistency: Option<String>,
        query_keyword: Option<bool>,
        destinations: Option<bool>,
        destinations_consistency: Option<String>,
        kv: Option<bool>,
        kv_cas: Option<bool>,
        kv_cas_fenced: Option<bool>,
        kv_fenced_leases: Option<bool>,
        graph: Option<bool>,
        forks: Option<bool>,
        agent_workflow: Option<bool>,
        watch: Option<bool>,
        authz: Option<bool>,
        filters: Option<bool>,
        filters_catalog: Option<bool>,
        filters_group_policy_reads: Option<bool>,
        a2a_gateway: Option<bool>,
        query_execution: Option<(bool, bool, bool)>,
        versions: Option<PyRef<'_, PyOpVersions>>,
        backends: Option<Vec<PyRef<'_, PyBackendDescriptor>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let versions = versions.map(|value| OpVersions::from(&*value));
        let backends: Option<Vec<BackendDescriptor>> =
            backends.map(|values| values.iter().map(|value| value.inner.clone()).collect());
        future_into_py(py, async move {
            let mut capabilities = inner.capabilities().await.clone();
            if let Some(value) = managed {
                capabilities.managed = value;
            }
            if let Some(value) = query {
                capabilities.query.available = value;
            }
            if let Some(value) = query_consistency {
                capabilities.query.consistency = consistency_level(&value)?;
            }
            if let Some(value) = query_keyword {
                capabilities.query.keyword = value;
            }
            if let Some(value) = destinations {
                capabilities.destinations.available = value;
            }
            if let Some(value) = destinations_consistency {
                capabilities.destinations.consistency = match value.as_str() {
                    "linearizable" => CheckpointReadConsistency::Linearizable,
                    "potentially_stale" => CheckpointReadConsistency::PotentiallyStale,
                    _ => {
                        return Err(InvalidError::new_err(
                            "destinations_consistency must be linearizable or potentially_stale",
                        ));
                    }
                };
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
            if let Some(value) = kv_fenced_leases {
                capabilities.kv.fenced_leases = value;
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
            if let Some(value) = filters {
                capabilities.filters.native = value;
            }
            if let Some(value) = filters_catalog {
                capabilities.filters.catalog = value;
            }
            if let Some(value) = filters_group_policy_reads {
                capabilities.filters.group_policy_reads = value;
            }
            if let Some(value) = a2a_gateway {
                capabilities.a2a_gateway = value;
            }
            if let Some((paging, cancellation, status)) = query_execution {
                capabilities = capabilities.with_query_execution(paging, cancellation, status);
            }
            if let Some(value) = versions {
                capabilities = capabilities.with_versions(Some(value));
            }
            if let Some(value) = backends {
                capabilities = capabilities.with_backends(value);
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
            Ok(PyCapabilities::from(inner.capabilities().await))
        })
    }

    /// Refresh managed readiness, operation versions, backends, and announced topology.
    fn refresh_capabilities<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            Ok(PyCapabilities::from(inner.refresh_capabilities().await))
        })
    }

    fn wait_until_ready<'py>(
        &self,
        py: Python<'py>,
        timeout_secs: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let timeout = duration_seconds(timeout_secs, "timeout_secs")?;
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner
                .wait_until_ready(timeout)
                .await
                .map(PyCapabilities::from)
                .map_err(to_pyerr)
        })
    }

    /// Execute independent managed command frames in one round trip. Each
    /// input dict has `code` and raw `payload` fields. Results are raw reply
    /// frames in the same order. This is not a transaction.
    fn execute_batch<'py>(
        &self,
        py: Python<'py>,
        operations: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let operations: Vec<BatchItem> = py_to_de(operations)?;
        future_into_py(py, async move {
            let results = inner.execute_batch(operations).await.map_err(to_pyerr)?;
            Python::attach(|py| {
                let results = results
                    .iter()
                    .map(|result| PyBytes::new(py, result).into_any().unbind())
                    .collect::<Vec<_>>();
                Ok(results.into_pyobject(py)?.unbind().into_any())
            })
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

    /// Close the shared connection. Every clone from `with_stream` and the other `with_*` methods loses it too. Safe to call more than once.
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        future_into_py(py, async move { laser.close().await.map_err(to_pyerr) })
    }

    /// Exit `async with`. The connection is reference-counted and closes when the
    /// last handle is dropped, so exiting does not disconnect a clone still in use.
    /// Call `close` to end the connection explicitly. Returns `False` so an
    /// exception in the body is not suppressed.
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

/// Consumer-filter evaluator version and codecs advertised by the server.
#[gen_stub_pyclass]
#[pyclass(name = "FilterAnnounce", frozen, get_all, skip_from_py_object)]
#[derive(Clone)]
pub struct PyFilterAnnounce {
    pub evaluator_version: u32,
    pub codecs: Vec<String>,
}

impl From<laser_sdk::wire::hello::FilterAnnounce> for PyFilterAnnounce {
    fn from(value: laser_sdk::wire::hello::FilterAnnounce) -> Self {
        Self {
            evaluator_version: value.evaluator_version,
            codecs: value
                .codecs
                .into_iter()
                .map(|codec| codec.to_string())
                .collect(),
        }
    }
}

/// A read-only snapshot of the connected deployment's capabilities.
/// Managed features depend on advertised backend support. The server can provide consumer-group reads without a plane.
#[gen_stub_pyclass]
#[pyclass(name = "Capabilities", frozen)]
pub struct PyCapabilities {
    /// Connected to a managed plane (the root managed switch).
    #[pyo3(get)]
    pub managed: bool,
    /// The managed query surface is served.
    #[pyo3(get)]
    pub query: bool,
    /// The strongest read-consistency the query surface serves
    /// (`eventual` / `read_your_writes` / `strong`).
    #[pyo3(get)]
    pub query_consistency: String,
    /// The query surface serves lexical keyword search (`Query.text(...)`).
    #[pyo3(get)]
    pub query_keyword: bool,
    /// Materialization destination declarations and the checkpoint lifecycle
    /// are served.
    #[pyo3(get)]
    pub destinations: bool,
    /// The checkpoint read consistency the destination surface serves
    /// (`linearizable` / `potentially_stale`).
    #[pyo3(get)]
    pub destinations_consistency: String,
    /// The managed key-value surface is served.
    #[pyo3(get)]
    pub kv: bool,
    /// The key-value store serves compare-and-swap.
    #[pyo3(get)]
    pub kv_cas: bool,
    /// The key-value store serves fenced compare-and-swap (the monotonic fence an
    /// exclusive workflow step needs for an at-most-once effect).
    #[pyo3(get)]
    pub kv_cas_fenced: bool,
    /// The key-value store serves the revocable fenced-lease contract:
    /// holder-scoped acquire, renewal, fence-validated release, fenced
    /// compare-and-swap requiring a live lease, and the barriered read. The
    /// lease, renew, release, and `cas_fenced` calls all gate on this.
    #[pyo3(get)]
    pub kv_fenced_leases: bool,
    /// The managed knowledge-graph surface is served.
    #[pyo3(get)]
    pub graph: bool,
    /// Managed copy-on-write forks are served.
    #[pyo3(get)]
    pub forks: bool,
    /// The managed run registry is served (`Laser.runs()` submit / cancel /
    /// status / list, and `registered=True` workflows).
    #[pyo3(get)]
    pub agent_workflow: bool,
    /// The change feed is published (`Laser.watch()`).
    #[pyo3(get)]
    pub watch: bool,
    /// The authorization control surface is served (`Laser.whoami()` and the
    /// role/binding verbs).
    #[pyo3(get)]
    pub authz: bool,
    /// Consumer filters are served by the streaming server (group readers,
    /// previews, sample tests).
    #[pyo3(get)]
    pub filters: bool,
    /// The group filter catalog is served, so a consumer group can carry a
    /// filter policy.
    #[pyo3(get)]
    pub filters_catalog: bool,
    /// The streaming server resolves a consumer group's own policy, so a
    /// group consumer runs the group's filter, or none, without naming one.
    #[pyo3(get)]
    pub filters_group_policy_reads: bool,
    /// A managed A2A gateway is available.
    #[pyo3(get)]
    pub a2a_gateway: bool,
    /// The per-surface operation versions the server advertised, or `None`
    /// against Apache Iggy and pre-versioned servers.
    #[pyo3(get)]
    pub versions: Option<PyOpVersions>,
    /// The materialization backends the connected server exposes (identity
    /// only). Empty against Apache Iggy and servers that advertise none.
    #[pyo3(get)]
    pub backends: Vec<PyBackendDescriptor>,
    #[pyo3(get)]
    pub evaluation: Option<PyFilterAnnounce>,
    inner: Capabilities,
}

impl From<Capabilities> for PyCapabilities {
    fn from(value: Capabilities) -> Self {
        let inner = value.clone();
        Self {
            inner,
            managed: value.managed,
            query: value.query.available,
            query_consistency: match value.query.consistency {
                laser_sdk::query::Consistency::ReadYourWrites => "read_your_writes",
                laser_sdk::query::Consistency::Strong => "strong",
                _ => "eventual",
            }
            .to_owned(),
            query_keyword: value.query.keyword,
            destinations: value.destinations.available,
            destinations_consistency: match value.destinations.consistency {
                CheckpointReadConsistency::Linearizable => "linearizable",
                _ => "potentially_stale",
            }
            .to_owned(),
            kv: value.kv.available,
            kv_cas: value.kv.cas,
            kv_cas_fenced: value.kv.cas_fenced,
            kv_fenced_leases: value.kv.fenced_leases,
            graph: value.graph,
            forks: value.forks,
            agent_workflow: value.agent_workflow,
            watch: value.watch,
            authz: value.authz,
            filters: value.filters.native,
            filters_catalog: value.filters.catalog,
            filters_group_policy_reads: value.filters.group_policy_reads,
            evaluation: value.filters.evaluation.map(PyFilterAnnounce::from),
            a2a_gateway: value.a2a_gateway,
            versions: value.versions.map(PyOpVersions::from),
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
    fn backend(&self, resource_id: &str) -> Option<PyBackendDescriptor> {
        self.backends
            .iter()
            .find(|backend| backend.resource_id == resource_id)
            .cloned()
    }

    fn enabled_backends(&self) -> Vec<PyBackendDescriptor> {
        self.backends
            .iter()
            .filter(|backend| backend.desired_state == "enabled")
            .cloned()
            .collect()
    }

    fn unready_backends(&self) -> Vec<PyBackendDescriptor> {
        self.enabled_backends()
            .into_iter()
            .filter(|backend| backend.observed_state != "ready" || !backend.ready)
            .collect()
    }

    fn readiness_reasons(&self, py: Python<'_>, resource_id: &str) -> PyResult<Py<PyAny>> {
        let reasons = self
            .backends
            .iter()
            .find(|backend| backend.resource_id == resource_id)
            .map(|backend| backend.inner.readiness.reasons.clone());
        crate::convert::ser_to_py(py, &reasons)
    }

    /// True when nothing beyond open streaming is available: the capability
    /// set equals the one an original Apache Iggy server negotiates.
    fn is_open_only(&self) -> bool {
        self.inner.is_open_only()
    }

    /// True when the query surface serves reads at `level` (`eventual`,
    /// `read_your_writes`, or `strong`) or stronger.
    fn serves_consistency(&self, level: &str) -> PyResult<bool> {
        Ok(self.inner.serves_consistency(consistency_level(level)?))
    }

    fn is_ready(&self) -> bool {
        let enabled = self.enabled_backends();
        self.managed
            && !enabled.is_empty()
            && enabled
                .iter()
                .all(|backend| backend.observed_state == "ready" && backend.ready)
    }

    fn __repr__(&self) -> String {
        format!(
            "Capabilities(managed={}, query={}, destinations={}, kv={}, graph={}, forks={}, kv_cas={}, backends={})",
            self.managed,
            self.query,
            self.destinations,
            self.kv,
            self.graph,
            self.forks,
            self.kv_cas,
            self.backends.len()
        )
    }
}

fn consistency_level(level: &str) -> PyResult<laser_sdk::query::Consistency> {
    match level {
        "eventual" => Ok(laser_sdk::query::Consistency::Eventual),
        "read_your_writes" => Ok(laser_sdk::query::Consistency::ReadYourWrites),
        "strong" => Ok(laser_sdk::query::Consistency::Strong),
        _ => Err(InvalidError::new_err(
            "query consistency must be eventual, read_your_writes, or strong",
        )),
    }
}

/// The managed operation versions advertised by the connected server.
#[gen_stub_pyclass]
#[pyclass(name = "OpVersions", frozen, get_all, skip_from_py_object)]
#[derive(Clone)]
pub struct PyOpVersions {
    pub query: u32,
    pub control: u32,
    pub kv: u32,
    pub fork: u32,
    pub agent: u32,
    pub graph: u32,
    pub checkpoint: u32,
    pub filter: u32,
    pub features: u64,
}

impl From<OpVersions> for PyOpVersions {
    fn from(value: OpVersions) -> Self {
        Self {
            query: value.query,
            control: value.control,
            kv: value.kv,
            fork: value.fork,
            agent: value.agent,
            graph: value.graph,
            checkpoint: value.checkpoint,
            filter: value.filter,
            features: value.features,
        }
    }
}

impl From<&PyOpVersions> for OpVersions {
    fn from(value: &PyOpVersions) -> Self {
        serde_json::from_value(serde_json::json!({
            "query": value.query,
            "control": value.control,
            "kv": value.kv,
            "fork": value.fork,
            "agent": value.agent,
            "graph": value.graph,
            "checkpoint": value.checkpoint,
            "filter": value.filter,
            "features": value.features,
        }))
        .expect("op versions decode from their own fields")
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyOpVersions {
    /// Operation versions for `Laser.with_capabilities(versions=)`, the
    /// bring-your-own backend and test path. Zero means not advertised.
    #[new]
    #[pyo3(signature = (*, query=1, control=1, kv=1, fork=1, agent=0, graph=0, checkpoint=0, filter=0, features=0))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        query: u32,
        control: u32,
        kv: u32,
        fork: u32,
        agent: u32,
        graph: u32,
        checkpoint: u32,
        filter: u32,
        features: u64,
    ) -> Self {
        Self {
            query,
            control,
            kv,
            fork,
            agent,
            graph,
            checkpoint,
            filter,
            features,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "OpVersions(query={}, control={}, kv={}, fork={}, agent={}, graph={}, checkpoint={}, filter={}, features={})",
            self.query,
            self.control,
            self.kv,
            self.fork,
            self.agent,
            self.graph,
            self.checkpoint,
            self.filter,
            self.features
        )
    }
}

/// One structured, versioned backend observation from the connected server.
#[gen_stub_pyclass]
#[pyclass(name = "BackendDescriptor", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PyBackendDescriptor {
    #[pyo3(get)]
    pub descriptor_version: u32,
    #[pyo3(get)]
    pub resource_id: String,
    #[pyo3(get)]
    pub mode: String,
    #[pyo3(get)]
    pub label: String,
    #[pyo3(get)]
    pub kind: String,
    #[pyo3(get)]
    pub version: String,
    #[pyo3(get)]
    pub observed_backend_generation: u64,
    #[pyo3(get)]
    pub observed_runtime_configuration_revision: u64,
    #[pyo3(get)]
    pub desired_state: String,
    #[pyo3(get)]
    pub observed_state: String,
    #[pyo3(get)]
    pub ready: bool,
    inner: BackendDescriptor,
}

impl From<BackendDescriptor> for PyBackendDescriptor {
    fn from(value: BackendDescriptor) -> Self {
        Self {
            descriptor_version: value.descriptor_version,
            resource_id: value.resource_id.to_string(),
            mode: serde_word(&value.mode),
            label: value.label.clone(),
            kind: value.implementation.kind.clone(),
            version: value.implementation.version.clone(),
            observed_backend_generation: value.observed_backend_generation,
            observed_runtime_configuration_revision: value.runtime_configuration_revision,
            desired_state: serde_word(&value.desired_state),
            observed_state: serde_word(&value.observed_state),
            ready: value.readiness.ready,
            inner: value,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyBackendDescriptor {
    /// A descriptor from its wire dict (the shape `readiness` and
    /// `materialization` use), for `Laser.with_capabilities(backends=)`.
    #[staticmethod]
    fn from_dict(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        let inner: BackendDescriptor = crate::convert::py_to_de(value)?;
        Ok(Self::from(inner))
    }

    /// This descriptor as its wire dict.
    fn to_dict(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        crate::convert::ser_to_py(py, &self.inner)
    }

    #[getter]
    fn readiness(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        crate::convert::ser_to_py(py, &self.inner.readiness)
    }

    #[getter]
    fn materialization(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        crate::convert::ser_to_py(py, &self.inner.materialization)
    }

    #[getter]
    fn query(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        crate::convert::ser_to_py(py, &self.inner.query)
    }

    #[getter]
    fn schema(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        crate::convert::ser_to_py(py, &self.inner.schema)
    }

    #[getter]
    fn maintenance(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        crate::convert::ser_to_py(py, &self.inner.maintenance)
    }

    #[getter]
    fn limits(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        crate::convert::ser_to_py(py, &self.inner.limits)
    }

    fn __repr__(&self) -> String {
        format!(
            "BackendDescriptor(resource_id={}, mode={}, kind={}, ready={})",
            self.resource_id, self.mode, self.kind, self.ready
        )
    }
}

fn serde_word<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_default()
}
