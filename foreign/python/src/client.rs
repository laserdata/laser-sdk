use crate::async_bridge::future_into_py;
use crate::convert::{duration_seconds, py_to_de};
use crate::errors::{InvalidError, to_pyerr};
use crate::sign::PyKeyRegistry;
use laser_sdk::capabilities::{
    BackendDescriptor, BackendReadiness, Capabilities, HelloOutcome, OpVersions,
};
use laser_sdk::laser::Laser;
use laser_sdk::wire::batch::BatchItem;
use laser_sdk::wire::checkpoint::CheckpointReadConsistency;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyfunction, gen_stub_pymethods};

// The SDK's public defaults, read from the Rust constants. A duration carries
// the unit of the Python argument that takes it.
macro_rules! constants {
    ($($name:literal: $ty:ty = $value:expr,)*) => {
        $(pyo3_stub_gen::module_variable!("laser_sdk", $name, $ty, $value);)*

        pub(crate) fn register_constants(module: &Bound<'_, PyModule>) -> PyResult<()> {
            $(module.add($name, { let value: $ty = $value; value })?;)*
            Ok(())
        }
    };
}

constants! {
    "CONTEXT_READ_WINDOW": usize = laser_sdk::context::CONTEXT_READ_WINDOW,
    "OPS_STREAM_DEFAULT": &str = laser_sdk::laser::OPS_STREAM_DEFAULT,
    "DEFAULT_MAX_RECORDS": usize = laser_sdk::batching::DEFAULT_MAX_RECORDS,
    "DEFAULT_MAX_BYTES": usize = laser_sdk::batching::DEFAULT_MAX_BYTES,
    "DEFAULT_LINGER_MS": u128 = laser_sdk::batching::DEFAULT_LINGER.as_millis(),
    "MIN_LINGER_MS": u128 = laser_sdk::batching::MIN_LINGER.as_millis(),
    "DEFAULT_KEY_NAMESPACE": &str = laser_sdk::sign::DEFAULT_KEY_NAMESPACE,
    "DEFAULT_ATTEMPT_TIMEOUT_SECS": f64 = laser_sdk::kv::DEFAULT_ATTEMPT_TIMEOUT.as_secs_f64(),
    "DEFAULT_SNAPSHOT_NAMESPACE": &str = laser_sdk::snapshot::DEFAULT_SNAPSHOT_NAMESPACE,
    "DEFAULT_SNAPSHOT_TOPIC": &str = laser_sdk::snapshot::DEFAULT_SNAPSHOT_TOPIC,
    "DEFAULT_MEMORY_TOPIC_TTL_SECS": f64 = laser_sdk::memory::DEFAULT_MEMORY_TOPIC_TTL.as_secs_f64(),
    "POLICY_DECISION_OPERATION": &str = laser_sdk::govern::POLICY_DECISION_OPERATION,
    "A2A_PROTOCOL_VERSION": &str = laser_sdk::a2a::A2A_PROTOCOL_VERSION,
    "A2A_JSONRPC_BINDING": &str = laser_sdk::a2a::A2A_JSONRPC_BINDING,
    "DEFAULT_OUTCOME_WAIT_SECS": f64 = laser_sdk::filters::DEFAULT_OUTCOME_WAIT.as_secs_f64(),
    "FILTER_EVALUATOR_VERSION": u32 = laser_sdk::filters::FILTER_EVALUATOR_VERSION,
    "FINISH_REASON_ABANDONED": &str = laser_sdk::agent::FINISH_REASON_ABANDONED,
    "FINISH_REASON_GAP": &str = laser_sdk::agent::FINISH_REASON_GAP,
    "DEFAULT_SESSION_TOPICS": Vec<&str> = laser_sdk::agent::DEFAULT_SESSION_TOPICS
        .iter()
        .filter_map(|topic| topic.name())
        .collect(),
    "DEFAULT_SESSION_MEMORY_NAMESPACE": &str = laser_sdk::agent::DEFAULT_SESSION_MEMORY_NAMESPACE,
    "DEFAULT_SESSION_CONTEXT_TURNS": usize = laser_sdk::agent::DEFAULT_SESSION_CONTEXT_TURNS,
    "DEFAULT_SESSION_CONTEXT_TOKENS": usize = laser_sdk::agent::DEFAULT_SESSION_CONTEXT_TOKENS,
    "WORKFLOW_FENCE_NAMESPACE": &str = laser_sdk::agent::WORKFLOW_FENCE_NAMESPACE,
    "DEFAULT_CHUNK_FLUSH_BYTES": usize = laser_sdk::agent::DEFAULT_CHUNK_FLUSH_BYTES,
    "DEFAULT_CHUNK_LINGER_MS": u64 = laser_sdk::agent::DEFAULT_CHUNK_LINGER_MS,
    "MAX_CHUNK_BODY_BYTES": usize = laser_sdk::agent::MAX_CHUNK_BODY_BYTES,
    "AGDX_AUTHZ_BIND_ROLES_CODE": u32 = laser_sdk::wire::codes::AGDX_AUTHZ_BIND_ROLES_CODE,
    "AGDX_AUTHZ_DEFINE_ROLE_CODE": u32 = laser_sdk::wire::codes::AGDX_AUTHZ_DEFINE_ROLE_CODE,
    "AGDX_AUTHZ_DELETE_ROLE_CODE": u32 = laser_sdk::wire::codes::AGDX_AUTHZ_DELETE_ROLE_CODE,
    "AGDX_AUTHZ_GET_BINDINGS_CODE": u32 = laser_sdk::wire::codes::AGDX_AUTHZ_GET_BINDINGS_CODE,
    "AGDX_AUTHZ_GET_ROLE_CODE": u32 = laser_sdk::wire::codes::AGDX_AUTHZ_GET_ROLE_CODE,
    "AGDX_AUTHZ_HISTORY_CODE": u32 = laser_sdk::wire::codes::AGDX_AUTHZ_HISTORY_CODE,
    "AGDX_AUTHZ_LIST_ROLES_CODE": u32 = laser_sdk::wire::codes::AGDX_AUTHZ_LIST_ROLES_CODE,
    "AGDX_AUTHZ_WHOAMI_CODE": u32 = laser_sdk::wire::codes::AGDX_AUTHZ_WHOAMI_CODE,
    "AGDX_COMMAND_BASE": u32 = laser_sdk::wire::codes::AGDX_COMMAND_BASE,
    "AGDX_DECODE_RECORD_CODE": u32 = laser_sdk::wire::codes::AGDX_DECODE_RECORD_CODE,
    "AGDX_FILTERED_ACK_CODE": u32 = laser_sdk::wire::codes::AGDX_FILTERED_ACK_CODE,
    "AGDX_FILTERED_POLL_CODE": u32 = laser_sdk::wire::codes::AGDX_FILTERED_POLL_CODE,
    "AGDX_FILTER_MUTATE_CODE": u32 = laser_sdk::wire::codes::AGDX_FILTER_MUTATE_CODE,
    "AGDX_FILTER_OPERATION_CODE": u32 = laser_sdk::wire::codes::AGDX_FILTER_OPERATION_CODE,
    "AGDX_FILTER_PREVIEW_CODE": u32 = laser_sdk::wire::codes::AGDX_FILTER_PREVIEW_CODE,
    "AGDX_FILTER_TEST_CODE": u32 = laser_sdk::wire::codes::AGDX_FILTER_TEST_CODE,
    "AGDX_FILTER_VALIDATE_CODE": u32 = laser_sdk::wire::codes::AGDX_FILTER_VALIDATE_CODE,
    "AGDX_FORK_BASE": u32 = laser_sdk::wire::codes::AGDX_FORK_BASE,
    "AGDX_FORK_CREATE_CODE": u32 = laser_sdk::wire::codes::AGDX_FORK_CREATE_CODE,
    "AGDX_FORK_DELETE_CODE": u32 = laser_sdk::wire::codes::AGDX_FORK_DELETE_CODE,
    "AGDX_FORK_LIST_CODE": u32 = laser_sdk::wire::codes::AGDX_FORK_LIST_CODE,
    "AGDX_FORK_PROMOTE_CODE": u32 = laser_sdk::wire::codes::AGDX_FORK_PROMOTE_CODE,
    "AGDX_FORK_PUT_CODE": u32 = laser_sdk::wire::codes::AGDX_FORK_PUT_CODE,
    "AGDX_GET_FILTER_BINDING_CODE": u32 = laser_sdk::wire::codes::AGDX_GET_FILTER_BINDING_CODE,
    "AGDX_GET_FILTER_CODE": u32 = laser_sdk::wire::codes::AGDX_GET_FILTER_CODE,
    "AGDX_GET_PROJECTION_CODE": u32 = laser_sdk::wire::codes::AGDX_GET_PROJECTION_CODE,
    "AGDX_GET_SCHEMA_CODE": u32 = laser_sdk::wire::codes::AGDX_GET_SCHEMA_CODE,
    "AGDX_HELLO_CODE": u32 = laser_sdk::wire::codes::AGDX_HELLO_CODE,
    "AGDX_KV_BASE": u32 = laser_sdk::wire::codes::AGDX_KV_BASE,
    "AGDX_KV_CAS_CODE": u32 = laser_sdk::wire::codes::AGDX_KV_CAS_CODE,
    "AGDX_KV_CAS_FENCED_CODE": u32 = laser_sdk::wire::codes::AGDX_KV_CAS_FENCED_CODE,
    "AGDX_KV_COPY_CODE": u32 = laser_sdk::wire::codes::AGDX_KV_COPY_CODE,
    "AGDX_KV_DELETE_CODE": u32 = laser_sdk::wire::codes::AGDX_KV_DELETE_CODE,
    "AGDX_KV_DELETE_MANY_CODE": u32 = laser_sdk::wire::codes::AGDX_KV_DELETE_MANY_CODE,
    "AGDX_KV_EXISTS_CODE": u32 = laser_sdk::wire::codes::AGDX_KV_EXISTS_CODE,
    "AGDX_KV_EXPIRE_CODE": u32 = laser_sdk::wire::codes::AGDX_KV_EXPIRE_CODE,
    "AGDX_KV_GET_CODE": u32 = laser_sdk::wire::codes::AGDX_KV_GET_CODE,
    "AGDX_KV_LEASE_CODE": u32 = laser_sdk::wire::codes::AGDX_KV_LEASE_CODE,
    "AGDX_KV_LEASE_RENEW_CODE": u32 = laser_sdk::wire::codes::AGDX_KV_LEASE_RENEW_CODE,
    "AGDX_KV_MOVE_CODE": u32 = laser_sdk::wire::codes::AGDX_KV_MOVE_CODE,
    "AGDX_KV_NAMESPACES_CODE": u32 = laser_sdk::wire::codes::AGDX_KV_NAMESPACES_CODE,
    "AGDX_KV_PATCH_CODE": u32 = laser_sdk::wire::codes::AGDX_KV_PATCH_CODE,
    "AGDX_KV_RELEASE_CODE": u32 = laser_sdk::wire::codes::AGDX_KV_RELEASE_CODE,
    "AGDX_KV_SCAN_CODE": u32 = laser_sdk::wire::codes::AGDX_KV_SCAN_CODE,
    "AGDX_KV_SET_CODE": u32 = laser_sdk::wire::codes::AGDX_KV_SET_CODE,
    "AGDX_LIST_FILTERS_CODE": u32 = laser_sdk::wire::codes::AGDX_LIST_FILTERS_CODE,
    "AGDX_LIST_FILTER_BINDINGS_CODE": u32 = laser_sdk::wire::codes::AGDX_LIST_FILTER_BINDINGS_CODE,
    "AGDX_LIST_FILTER_REVISIONS_CODE": u32 = laser_sdk::wire::codes::AGDX_LIST_FILTER_REVISIONS_CODE,
    "AGDX_LIST_PROJECTIONS_CODE": u32 = laser_sdk::wire::codes::AGDX_LIST_PROJECTIONS_CODE,
    "AGDX_LIST_SCHEMAS_CODE": u32 = laser_sdk::wire::codes::AGDX_LIST_SCHEMAS_CODE,
    "AGDX_QUERY_CANCEL_CODE": u32 = laser_sdk::wire::codes::AGDX_QUERY_CANCEL_CODE,
    "AGDX_QUERY_CODE": u32 = laser_sdk::wire::codes::AGDX_QUERY_CODE,
    "AGDX_QUERY_PAGE_CODE": u32 = laser_sdk::wire::codes::AGDX_QUERY_PAGE_CODE,
    "AGDX_QUERY_STATUS_CODE": u32 = laser_sdk::wire::codes::AGDX_QUERY_STATUS_CODE,
    "AGDX_REGISTER_SCHEMA_CODE": u32 = laser_sdk::wire::codes::AGDX_REGISTER_SCHEMA_CODE,
    "AUTHZ_OP_VERSION": u32 = laser_sdk::wire::codes::AUTHZ_OP_VERSION,
    "CONTROL_OP_VERSION": u32 = laser_sdk::wire::codes::CONTROL_OP_VERSION,
    "FILTER_OP_VERSION": u32 = laser_sdk::wire::codes::FILTER_OP_VERSION,
    "FORK_OP_VERSION": u32 = laser_sdk::wire::codes::FORK_OP_VERSION,
    "KV_LEASE_OP_VERSION": u32 = laser_sdk::wire::codes::KV_LEASE_OP_VERSION,
    "KV_OP_VERSION": u32 = laser_sdk::wire::codes::KV_OP_VERSION,
    "QUERY_OP_VERSION": u32 = laser_sdk::wire::codes::QUERY_OP_VERSION,
    "DEFAULT_NAMESPACE": &str = laser_sdk::wire::limits::DEFAULT_NAMESPACE,
    "DEFAULT_SCAN_LIMIT": usize = laser_sdk::wire::limits::DEFAULT_SCAN_LIMIT,
    "DEFAULT_STREAM_PAGE_SIZE": usize = laser_sdk::wire::limits::DEFAULT_STREAM_PAGE_SIZE,
    "MAX_FILTERED_PAGE_BYTES": u32 = laser_sdk::wire::limits::MAX_FILTERED_PAGE_BYTES,
    "MAX_FILTERED_PAGE_EXAMINED": u32 = laser_sdk::wire::limits::MAX_FILTERED_PAGE_EXAMINED,
    "MAX_FILTERED_PAGE_RECORDS": u32 = laser_sdk::wire::limits::MAX_FILTERED_PAGE_RECORDS,
    "MAX_FILTER_BYTES": usize = laser_sdk::wire::limits::MAX_FILTER_BYTES,
    "MAX_FILTER_CATALOG_PAGE": u32 = laser_sdk::wire::limits::MAX_FILTER_CATALOG_PAGE,
    "MAX_FILTER_NAME_BYTES": usize = laser_sdk::wire::limits::MAX_FILTER_NAME_BYTES,
    "MAX_FILTER_PREVIEW_EXAMINED": u32 = laser_sdk::wire::limits::MAX_FILTER_PREVIEW_EXAMINED,
    "MAX_FILTER_PREVIEW_RECORDS": u32 = laser_sdk::wire::limits::MAX_FILTER_PREVIEW_RECORDS,
    "MAX_HOLDER_ID_BYTES": usize = laser_sdk::wire::limits::MAX_HOLDER_ID_BYTES,
    "MAX_INDEX_ENTRIES_PER_RECORD": usize = laser_sdk::wire::limits::MAX_INDEX_ENTRIES_PER_RECORD,
    "MAX_KEY_BYTES": usize = laser_sdk::wire::limits::MAX_KEY_BYTES,
    "MAX_LEASE_TTL_MICROS": u64 = laser_sdk::wire::limits::MAX_LEASE_TTL_MICROS,
    "MAX_PAGE_SIZE": usize = laser_sdk::wire::limits::MAX_PAGE_SIZE,
    "MAX_ROLE_NAME_BYTES": usize = laser_sdk::wire::limits::MAX_ROLE_NAME_BYTES,
    "MAX_SCAN_LIMIT": usize = laser_sdk::wire::limits::MAX_SCAN_LIMIT,
    "MAX_VALUE_BYTES": usize = laser_sdk::wire::limits::MAX_VALUE_BYTES,
    "MIN_LEASE_TTL_MICROS": u64 = laser_sdk::wire::limits::MIN_LEASE_TTL_MICROS,
    "AGENT_ID": &str = laser_sdk::wire::headers::AGENT_ID,
    "CAUSAL_PARENT": &str = laser_sdk::wire::headers::CAUSAL_PARENT,
    "CONTENT_TYPE": &str = laser_sdk::wire::headers::CONTENT_TYPE,
    "CONVERSATION_ID": &str = laser_sdk::wire::headers::CONVERSATION_ID,
    "CORRELATION_ID": &str = laser_sdk::wire::headers::CORRELATION_ID,
    "COST_USD": &str = laser_sdk::wire::headers::COST_USD,
    "DEADLINE": &str = laser_sdk::wire::headers::DEADLINE,
    "FENCE": &str = laser_sdk::wire::headers::FENCE,
    "FIELD_MESSAGE_TYPE": &str = laser_sdk::wire::headers::FIELD_MESSAGE_TYPE,
    "FIELD_TS": &str = laser_sdk::wire::headers::FIELD_TS,
    "HEADER_FRAMING_BYTES": usize = laser_sdk::wire::headers::HEADER_FRAMING_BYTES,
    "HEADER_SOFT_CAP": usize = laser_sdk::wire::headers::HEADER_SOFT_CAP,
    "HEADER_VALUE_MAX": usize = laser_sdk::wire::headers::HEADER_VALUE_MAX,
    "IDEMPOTENCY_KEY": &str = laser_sdk::wire::headers::IDEMPOTENCY_KEY,
    "IDX_PREFIX": &str = laser_sdk::wire::headers::IDX_PREFIX,
    "INLINE_PAYLOAD": &str = laser_sdk::wire::headers::INLINE_PAYLOAD,
    "LOGICAL_SCHEMA_FINGERPRINT": &str = laser_sdk::wire::headers::LOGICAL_SCHEMA_FINGERPRINT,
    "PARENT_CONVERSATION_ID": &str = laser_sdk::wire::headers::PARENT_CONVERSATION_ID,
    "PROJECTION_REF": &str = laser_sdk::wire::headers::PROJECTION_REF,
    "ROOT_CONVERSATION_ID": &str = laser_sdk::wire::headers::ROOT_CONVERSATION_ID,
    "SCHEMA_ID": &str = laser_sdk::wire::headers::SCHEMA_ID,
    "TARGET_AGENT_ID": &str = laser_sdk::wire::headers::TARGET_AGENT_ID,
    "USAGE_INPUT_TOKENS": &str = laser_sdk::wire::headers::USAGE_INPUT_TOKENS,
    "USAGE_OUTPUT_TOKENS": &str = laser_sdk::wire::headers::USAGE_OUTPUT_TOKENS,
    "VECTOR_FIELD": &str = laser_sdk::wire::headers::VECTOR_FIELD,
    "WINDOW_START": &str = laser_sdk::wire::headers::WINDOW_START,
    "CONTROL_TOPIC": &str = laser_sdk::wire::topics::CONTROL_TOPIC,
    "DLQ_TOPIC": &str = laser_sdk::wire::topics::DLQ_TOPIC,
    "OPS_STREAM": &str = laser_sdk::wire::topics::OPS_STREAM,
    "DIGEST32_BYTES": usize = laser_sdk::wire::schema::Digest32::BYTES,
    "SCHEMA_FINGERPRINT_BYTES": usize = laser_sdk::wire::schema::SchemaFingerprint::BYTES,
    "UUID_VALUE_BYTES": usize = laser_sdk::wire::schema::UuidValue::BYTES,
}

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
    /// Connect with a connection string or a bare `user:password@host:port`
    /// endpoint, or over TCP with `address` (`host:port`) and `credentials`
    /// (`(username, password)`). Pinning `stream` only enables the
    /// default-stream shortcuts. `capabilities` seeds the capability set
    /// (default `Capabilities.OPEN`), and `governor` with `governor_mode` (and
    /// optionally `governor_retention`) enrolls a pre-effect policy hook like
    /// `with_governor` after connect. Connecting gives up after
    /// `connect_timeout_ms`, default 30000 or `LASER_CONNECT_TIMEOUT_MS`, with a
    /// `TimeoutError` naming whether the server never accepted the connection
    /// or never answered the login.
    #[staticmethod]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (connection_string=None, *, address=None, credentials=None, stream=None, ops_stream=None, control_topic=None, dlq_topic=None, changes_topic=None, verifier=None, capabilities=None, governor=None, governor_mode=None, governor_retention=None, connect_timeout_ms=None, publish_timeout_ms=None, publish_max_retries=None, publish_retry_backoff_ms=None))]
    fn connect<'py>(
        py: Python<'py>,
        connection_string: Option<String>,
        address: Option<String>,
        credentials: Option<(String, String)>,
        stream: Option<String>,
        ops_stream: Option<String>,
        control_topic: Option<String>,
        dlq_topic: Option<String>,
        changes_topic: Option<String>,
        verifier: Option<&PyKeyRegistry>,
        capabilities: Option<PyRef<'_, PyCapabilities>>,
        governor: Option<&Bound<'_, PyAny>>,
        governor_mode: Option<&str>,
        governor_retention: Option<crate::govern::PyGovernorRetention>,
        connect_timeout_ms: Option<u64>,
        publish_timeout_ms: Option<u64>,
        publish_max_retries: Option<u32>,
        publish_retry_backoff_ms: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let verifier = verifier.map(PyKeyRegistry::snapshot);
        let capabilities = capabilities.map(|value| value.inner.clone());
        let governor = match governor {
            Some(governor) => Some((
                std::sync::Arc::new(crate::govern::PyActionGovernor::new(governor)?),
                crate::govern::parse_mode(governor_mode.ok_or_else(|| {
                    InvalidError::new_err("governor needs governor_mode (observe or enforce)")
                })?)?,
            )),
            None => None,
        };
        future_into_py(py, async move {
            let mut builder = Laser::builder();
            if let Some(value) = connection_string {
                builder = builder.connection_string(value);
            }
            if let Some(value) = address {
                builder = builder.address(value);
            }
            if let Some((username, password)) = credentials {
                builder = builder.credentials(username, password);
            }
            if let Some(value) = capabilities {
                builder = builder.capabilities(value);
            }
            if let Some((governor, mode)) = governor {
                builder = match governor_retention {
                    Some(retention) => {
                        builder.governor_with_retention(governor, mode, retention.inner)
                    }
                    None => builder.governor(governor, mode),
                };
            }
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
    fn with_default_stream(&self, stream: String) -> PyLaser {
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
        ops: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let ops: Vec<BatchItem> = py_to_de(ops)?;
        future_into_py(py, async move {
            let results = inner.execute_batch(ops).await.map_err(to_pyerr)?;
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
    /// laser:` reads naturally. Note the `with_default_stream` / `with_ops_stream` /
    /// `with_control_topic` / `with_dlq_topic` / `with_changes_topic` methods
    /// return aliasing clones over the *same* connection: exiting the block does
    /// not close a clone still in use elsewhere.
    fn __aenter__<'py>(slf: Py<Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move { Ok(slf) })
    }

    /// Close the shared connection. Every clone from `with_default_stream` and the other `with_*` methods loses it too. Safe to call more than once.
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
#[pyclass(name = "FilterAnnounce", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PyFilterAnnounce {
    #[pyo3(get)]
    pub evaluator_version: u32,
    #[pyo3(get)]
    pub codecs: Vec<String>,
    inner: laser_sdk::capabilities::FilterAnnounce,
}

impl From<laser_sdk::capabilities::FilterAnnounce> for PyFilterAnnounce {
    fn from(value: laser_sdk::capabilities::FilterAnnounce) -> Self {
        Self {
            evaluator_version: value.evaluator_version,
            codecs: value.codecs.iter().map(ToString::to_string).collect(),
            inner: value,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyFilterAnnounce {
    /// What this build of the evaluator serves.
    #[staticmethod]
    fn served() -> Self {
        laser_sdk::capabilities::FilterAnnounce::served().into()
    }

    /// Whether a filter built for `evaluator_version` with `codec` evaluates
    /// here exactly as it would in the client that built it.
    fn evaluates(&self, evaluator_version: u32, codec: &str) -> PyResult<bool> {
        let codec = codec
            .parse::<laser_sdk::wire::filter::FilterCodec>()
            .map_err(|_| InvalidError::new_err(format!("unknown codec '{codec}'")))?;
        Ok(self.inner.evaluates(evaluator_version, codec))
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
    /// The managed query surface and the read consistency it serves.
    #[pyo3(get)]
    pub query: PyQueryCaps,
    /// Materialization destination declarations and the checkpoint lifecycle.
    #[pyo3(get)]
    pub destinations: PyDestinationCaps,
    /// The managed key-value surface and its conditional-write support.
    #[pyo3(get)]
    pub kv: PyKvCaps,
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
    /// Server-side consumer filters.
    #[pyo3(get)]
    pub filters: PyFilterCaps,
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
    /// What the connect-time probe established: `unknown` (no probe ran),
    /// `answered` (a managed announcement), `rejected` (the server answered
    /// without one, so the managed surfaces are absent), or `failed`.
    #[pyo3(get)]
    pub hello: String,
    inner: Capabilities,
}

/// The managed query surface and the strongest read consistency it serves.
#[gen_stub_pyclass]
#[pyclass(name = "QueryCaps", frozen, get_all, skip_from_py_object)]
#[derive(Clone)]
pub struct PyQueryCaps {
    /// Whether `Laser.query` is served.
    pub available: bool,
    /// The strongest read consistency the surface honors (`eventual`,
    /// `read_your_writes`, or `strong`). A level implies every weaker one.
    pub consistency: String,
    /// Whether the surface serves lexical keyword search (`Query.text(...)`).
    pub keyword: bool,
    /// Whether an executing query continues through opaque cursor pages.
    pub cursor_paging: bool,
    /// Whether query cancellation is served.
    pub cancellation: bool,
    /// Whether query execution status is served.
    pub execution_status: bool,
}

impl From<laser_sdk::capabilities::QueryCaps> for PyQueryCaps {
    fn from(value: laser_sdk::capabilities::QueryCaps) -> Self {
        Self {
            available: value.available,
            consistency: match value.consistency {
                laser_sdk::query::Consistency::ReadYourWrites => "read_your_writes",
                laser_sdk::query::Consistency::Strong => "strong",
                _ => "eventual",
            }
            .to_owned(),
            keyword: value.keyword,
            cursor_paging: value.cursor_paging,
            cancellation: value.cancellation,
            execution_status: value.execution_status,
        }
    }
}

/// Materialization destination declarations and managed checkpoint lifecycle.
#[gen_stub_pyclass]
#[pyclass(name = "DestinationCaps", frozen, get_all, skip_from_py_object)]
#[derive(Clone)]
pub struct PyDestinationCaps {
    /// Whether destination declarations and checkpoints are served.
    pub available: bool,
    /// The checkpoint read consistency served (`linearizable` or
    /// `potentially_stale`).
    pub consistency: String,
}

impl From<laser_sdk::capabilities::DestinationCaps> for PyDestinationCaps {
    fn from(value: laser_sdk::capabilities::DestinationCaps) -> Self {
        Self {
            available: value.available,
            consistency: match value.consistency {
                CheckpointReadConsistency::Linearizable => "linearizable",
                _ => "potentially_stale",
            }
            .to_owned(),
        }
    }
}

/// The managed key-value surface and its conditional-write support.
#[gen_stub_pyclass]
#[pyclass(name = "KvCaps", frozen, get_all, skip_from_py_object)]
#[derive(Clone)]
pub struct PyKvCaps {
    /// Whether the managed key-value store is served.
    pub available: bool,
    /// Whether the store serves compare-and-swap.
    pub cas: bool,
    /// Whether the store serves fenced compare-and-swap.
    pub cas_fenced: bool,
    /// Whether the store serves the revocable fenced-lease contract. The
    /// lease, renew, release, and `cas_fenced` calls all gate on this.
    pub fenced_leases: bool,
}

impl From<laser_sdk::capabilities::KvCaps> for PyKvCaps {
    fn from(value: laser_sdk::capabilities::KvCaps) -> Self {
        Self {
            available: value.available,
            cas: value.cas,
            cas_fenced: value.cas_fenced,
            fenced_leases: value.fenced_leases,
        }
    }
}

/// Server-side consumer filters.
#[gen_stub_pyclass]
#[pyclass(name = "FilterCaps", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PyFilterCaps {
    /// Filtered reads, previews, sample tests, and validation, served by the
    /// streaming server without a managed plane.
    #[pyo3(get)]
    pub native: bool,
    /// Saved filters, revisions, group bindings, and mutation outcomes.
    #[pyo3(get)]
    pub catalog: bool,
    /// The server resolves a consumer group's own policy, so a group consumer
    /// runs the group's filter, or none, without naming one.
    #[pyo3(get)]
    pub group_policy_reads: bool,
    /// The evaluator version and codecs the server announced, or `None` from
    /// a server that predates the announcement.
    #[pyo3(get)]
    pub evaluation: Option<PyFilterAnnounce>,
    inner: laser_sdk::capabilities::FilterCaps,
}

impl From<laser_sdk::capabilities::FilterCaps> for PyFilterCaps {
    fn from(value: laser_sdk::capabilities::FilterCaps) -> Self {
        Self {
            native: value.native,
            catalog: value.catalog,
            group_policy_reads: value.group_policy_reads,
            evaluation: value.evaluation.clone().map(PyFilterAnnounce::from),
            inner: value,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyFilterCaps {
    /// Whether the server evaluates a filter built for `evaluator_version`
    /// with `codec` (`json`, `cbor`, `avro`, `protobuf`, `headers_only`)
    /// exactly as this build does.
    fn evaluates(&self, evaluator_version: u32, codec: &str) -> PyResult<bool> {
        let codec = codec
            .parse::<laser_sdk::wire::filter::FilterCodec>()
            .map_err(|_| InvalidError::new_err(format!("unknown codec '{codec}'")))?;
        Ok(self.inner.evaluates(evaluator_version, codec))
    }
}

impl From<Capabilities> for PyCapabilities {
    fn from(value: Capabilities) -> Self {
        let inner = value.clone();
        Self {
            inner,
            managed: value.managed,
            query: PyQueryCaps::from(value.query),
            destinations: PyDestinationCaps::from(value.destinations),
            kv: PyKvCaps::from(value.kv),
            graph: value.graph,
            forks: value.forks,
            agent_workflow: value.agent_workflow,
            watch: value.watch,
            authz: value.authz,
            filters: PyFilterCaps::from(value.filters),
            hello: match value.hello {
                HelloOutcome::Unknown => "unknown",
                HelloOutcome::Answered => "answered",
                HelloOutcome::Rejected => "rejected",
                HelloOutcome::Failed => "failed",
            }
            .to_owned(),
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
    /// The baseline every deployment has: nothing beyond the open SDK surface.
    #[classattr]
    #[allow(non_snake_case)]
    fn OPEN() -> PyCapabilities {
        PyCapabilities::from(Capabilities::OPEN)
    }

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
            .filter(|backend| backend.observed_state != "ready" || !backend.inner.readiness.ready)
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
        self.inner.is_ready()
    }

    fn __repr__(&self) -> String {
        format!(
            "Capabilities(managed={}, query={}, destinations={}, kv={}, graph={}, forks={}, kv_cas={}, backends={})",
            self.managed,
            self.query.available,
            self.destinations.available,
            self.kv.available,
            self.graph,
            self.forks,
            self.kv.cas,
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

    /// A copy advertising this agent-envelope version.
    fn with_agent(&self, agent: u32) -> Self {
        OpVersions::from(self).with_agent(agent).into()
    }

    /// A copy advertising this knowledge-graph op version.
    fn with_graph(&self, graph: u32) -> Self {
        OpVersions::from(self).with_graph(graph).into()
    }

    /// A copy advertising this destination and checkpoint version.
    fn with_checkpoint(&self, checkpoint: u32) -> Self {
        OpVersions::from(self).with_checkpoint(checkpoint).into()
    }

    /// A copy advertising this consumer-filter catalog version.
    fn with_filter(&self, filter: u32) -> Self {
        OpVersions::from(self).with_filter(filter).into()
    }

    /// A copy advertising the capability feature bits in `features`.
    fn with_features(&self, features: u64) -> Self {
        OpVersions::from(self).with_features(features).into()
    }

    /// Whether the feature bit (or every bit of a set) in `bit` is advertised.
    fn has_feature(&self, bit: u64) -> bool {
        OpVersions::from(self).has_feature(bit)
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
    pub observed_backend_generation: u64,
    #[pyo3(get)]
    pub desired_state: String,
    #[pyo3(get)]
    pub observed_state: String,
    inner: BackendDescriptor,
}

impl From<BackendDescriptor> for PyBackendDescriptor {
    fn from(value: BackendDescriptor) -> Self {
        Self {
            descriptor_version: value.descriptor_version,
            resource_id: value.resource_id.to_string(),
            mode: serde_word(&value.mode),
            label: value.label.clone(),
            observed_backend_generation: value.observed_backend_generation,
            desired_state: serde_word(&value.desired_state),
            observed_state: serde_word(&value.observed_state),
            inner: value,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyBackendDescriptor {
    /// A descriptor for one backend resource, disabled and not ready until
    /// `with_state` reports otherwise. `mode` is `operational` or `lakehouse`,
    /// `implementation` a `{"kind", "version"}` dict.
    #[new]
    fn new(
        resource_id: &str,
        mode: &str,
        label: String,
        implementation: &Bound<'_, PyAny>,
        observed_backend_generation: u64,
        runtime_configuration_revision: u64,
    ) -> PyResult<Self> {
        let resource_id = resource_id
            .parse::<laser_sdk::capabilities::BackendResourceId>()
            .map_err(|error| InvalidError::new_err(error.to_string()))?;
        Ok(Self::from(BackendDescriptor::new(
            resource_id,
            serde_word_value(mode, "mode")?,
            label,
            py_to_de(implementation)?,
            observed_backend_generation,
            runtime_configuration_revision,
        )))
    }

    /// A descriptor from its wire dict (the shape `readiness` and
    /// `materialization` use), for `Laser.with_capabilities(backends=)`.
    #[staticmethod]
    fn from_dict(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        let inner: BackendDescriptor = crate::convert::py_to_de(value)?;
        Ok(Self::from(inner))
    }

    /// A copy with the desired state (`disabled`, `enabled`), the observed
    /// state (`disabled`, `starting`, `ready`, `degraded`, `unavailable`), and
    /// the readiness dict replaced.
    fn with_state(
        &self,
        desired_state: &str,
        observed_state: &str,
        readiness: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        Ok(Self::from(self.inner.clone().with_state(
            serde_word_value(desired_state, "desired_state")?,
            serde_word_value(observed_state, "observed_state")?,
            py_to_de(readiness)?,
        )))
    }

    /// A copy advertising the materialization capability dicts in `capabilities`.
    fn with_materialization(&self, capabilities: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self::from(
            self.inner
                .clone()
                .with_materialization(py_to_de(capabilities)?),
        ))
    }

    /// A copy advertising the query capability dict `capabilities`.
    fn with_query(&self, capabilities: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self::from(
            self.inner.clone().with_query(py_to_de(capabilities)?),
        ))
    }

    /// A copy advertising the schema capability dict `capabilities`.
    fn with_schema(&self, capabilities: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self::from(
            self.inner.clone().with_schema(py_to_de(capabilities)?),
        ))
    }

    /// A copy advertising the maintenance capability dict `capabilities`.
    fn with_maintenance(&self, capabilities: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self::from(
            self.inner.clone().with_maintenance(py_to_de(capabilities)?),
        ))
    }

    /// A copy advertising the backend limits dict `limits`.
    fn with_limits(&self, limits: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self::from(
            self.inner.clone().with_limits(py_to_de(limits)?),
        ))
    }

    /// This descriptor as its wire dict.
    fn to_dict(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        crate::convert::ser_to_py(py, &self.inner)
    }

    #[getter]
    fn implementation(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        crate::convert::ser_to_py(py, &self.inner.implementation)
    }

    #[getter]
    fn runtime_configuration_revision(&self) -> u64 {
        self.inner.runtime_configuration_revision
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
            self.resource_id, self.mode, self.inner.implementation.kind, self.inner.readiness.ready
        )
    }
}

/// A not-ready readiness dict carrying the one stable reason `code`, such as
/// `disabled` or `configuration_pending`, for `BackendDescriptor.with_state`.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn backend_readiness_not_ready(py: Python<'_>, code: &str) -> PyResult<Py<PyAny>> {
    let readiness = BackendReadiness::not_ready(serde_word_value(code, "readiness code")?);
    crate::convert::ser_to_py(py, &readiness)
}

/// A ready readiness dict observed at `observed_at_micros`, for
/// `BackendDescriptor.with_state`.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn backend_readiness_ready(py: Python<'_>, observed_at_micros: u64) -> PyResult<Py<PyAny>> {
    crate::convert::ser_to_py(py, &BackendReadiness::ready(observed_at_micros))
}

fn serde_word<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_default()
}

// The wire word of a unit enum variant (`lakehouse`, `ready`) into the enum.
fn serde_word_value<T: serde::de::DeserializeOwned>(word: &str, label: &str) -> PyResult<T> {
    serde_json::from_value(serde_json::Value::String(word.to_owned()))
        .map_err(|_| InvalidError::new_err(format!("unknown {label} '{word}'")))
}
