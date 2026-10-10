use crate::async_bridge::future_into_py;
use crate::client::PyLaser;
use crate::context::{
    PyContextMessage, PyContextScope, PyScopedMemory, bounded_policy, context_policy, take_failure,
};
use crate::convert::py_to_de;
use crate::errors::{InvalidError, to_pyerr};
use crate::memory::{Backend, PyMemory};
use laser_sdk::agent::{
    Session, SessionBuilder, SessionConfig, SessionLayout, SessionLease, SessionPolicy,
    SessionTurn, Sessions, TopicRetention,
};
use laser_sdk::context::{Checkpoint, ContextMessage};
use laser_sdk::types::ConversationId;
use laser_sdk::wire::agent::{AgentEnvelope, AgentErrorBody, AgentErrorCode, AgentId, Budget};
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{
    gen_stub_pyclass, gen_stub_pyclass_complex_enum, gen_stub_pyfunction, gen_stub_pymethods,
};
use std::str::FromStr;
use std::sync::Mutex;
use std::time::Duration;

fn conversation(value: &str) -> PyResult<ConversationId> {
    ConversationId::from_str(value).map_err(|error| to_pyerr(error.into()))
}

fn agent(value: &str) -> PyResult<AgentId> {
    value
        .parse()
        .map_err(|_| InvalidError::new_err(format!("invalid agent id `{value}`")))
}

fn millis(value: f64, name: &str) -> PyResult<Duration> {
    crate::convert::duration_ms(value, name)
}

/// A session's token and cost ceiling, the `Budget` a session start record
/// carries. A dimension left `None` is unbounded. `cost_micros` is in
/// millionths of the cost unit.
#[gen_stub_pyclass]
#[pyclass(name = "Budget", frozen, eq, from_py_object)]
#[derive(Clone, PartialEq)]
pub struct PyBudget {
    pub(crate) inner: Budget,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyBudget {
    #[new]
    #[pyo3(signature = (*, tokens=None, cost_micros=None))]
    fn new(tokens: Option<u64>, cost_micros: Option<u64>) -> Self {
        Self {
            inner: Budget {
                tokens,
                cost_micros,
            },
        }
    }

    /// The summed input and output token ceiling, or `None`.
    #[getter]
    fn tokens(&self) -> Option<u64> {
        self.inner.tokens
    }

    /// The summed cost ceiling in micros, or `None`.
    #[getter]
    fn cost_micros(&self) -> Option<u64> {
        self.inner.cost_micros
    }

    fn __repr__(&self) -> String {
        format!(
            "Budget(tokens={:?}, cost_micros={:?})",
            self.inner.tokens, self.inner.cost_micros
        )
    }
}

/// How long `agent.sessions` keeps records. `expiry_ms` is in milliseconds
/// and `max_size` in bytes. Never-expire with no size bound is refused.
#[gen_stub_pyclass]
#[pyclass(name = "TopicRetention", frozen)]
pub struct PyTopicRetention {
    pub(crate) inner: TopicRetention,
    expiry_ms: Option<f64>,
    max_size: Option<u64>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyTopicRetention {
    #[new]
    #[pyo3(signature = (expiry_ms=None, max_size=None))]
    fn new(expiry_ms: Option<f64>, max_size: Option<u64>) -> PyResult<Self> {
        use laser_sdk::iggy::prelude::{IggyByteSize, IggyDuration, IggyExpiry, MaxTopicSize};
        let iggy_expiry = match expiry_ms {
            Some(ms) => {
                let micros =
                    u64::try_from(millis(ms, "expiry_ms")?.as_micros()).unwrap_or(u64::MAX);
                IggyExpiry::ExpireDuration(IggyDuration::from(micros.max(1)))
            }
            None => IggyExpiry::NeverExpire,
        };
        let iggy_size = match max_size {
            Some(bytes) => MaxTopicSize::Custom(IggyByteSize::from(bytes)),
            None => MaxTopicSize::ServerDefault,
        };
        Ok(Self {
            inner: TopicRetention::new(iggy_expiry, iggy_size).map_err(to_pyerr)?,
            expiry_ms,
            max_size,
        })
    }

    /// Expire records after `age_ms` milliseconds, with the server's default
    /// size bound.
    #[staticmethod]
    fn expire_after(age_ms: f64) -> PyResult<Self> {
        Ok(Self {
            inner: TopicRetention::expire_after(millis(age_ms, "age_ms")?),
            expiry_ms: Some(age_ms),
            max_size: None,
        })
    }

    /// The message expiry in milliseconds, or None to never expire.
    #[getter]
    fn expiry_ms(&self) -> Option<f64> {
        self.expiry_ms
    }

    /// The topic size bound in bytes, or None for the server default.
    #[getter]
    fn max_size(&self) -> Option<u64> {
        self.max_size
    }
}

/// Where a stream's agents put their work. Lifecycle and state always ride
/// the session lane on `agent.sessions`.
#[gen_stub_pyclass_complex_enum]
#[pyclass(name = "SessionLayout", frozen, eq, from_py_object)]
#[derive(Clone, PartialEq)]
pub enum PySessionLayout {
    /// One `agent.sessions` topic keyed by session. The default.
    Shared(),
    /// Each declared agent reads its own topic, `topics` maps an agent id to
    /// its topic. On `agent.sessions` sends, a command addressed to a declared
    /// agent goes to that agent's topic and a response, error, or chunk to its
    /// addressee's topic. Lifecycle, state, broadcast records, and undeclared
    /// agents stay on the lane. Bootstrap creates the declared topics.
    PerAgentTopic {
        topics: std::collections::BTreeMap<String, String>,
    },
    /// Work rides `agent.sessions` on a declared partition per agent.
    PerAgentPartition {
        partitions: std::collections::BTreeMap<String, u32>,
    },
    /// Everything on one partition.
    SinglePartition(),
}

impl PySessionLayout {
    fn to_rust(&self) -> PyResult<SessionLayout> {
        Ok(match self {
            Self::Shared() => SessionLayout::Shared,
            Self::PerAgentTopic { topics } => SessionLayout::PerAgentTopic(
                topics
                    .iter()
                    .map(|(name, topic)| Ok((agent(name)?, topic.clone())))
                    .collect::<PyResult<_>>()?,
            ),
            Self::SinglePartition() => SessionLayout::SinglePartition,
            Self::PerAgentPartition { partitions } => SessionLayout::PerAgentPartition(
                partitions
                    .iter()
                    .map(|(name, partition)| Ok((agent(name)?, *partition)))
                    .collect::<PyResult<_>>()?,
            ),
        })
    }

    fn from_rust(layout: &SessionLayout) -> Self {
        match layout {
            SessionLayout::Shared => Self::Shared(),
            SessionLayout::PerAgentTopic(map) => Self::PerAgentTopic {
                topics: map
                    .iter()
                    .map(|(agent, topic)| (agent.to_string(), topic.clone()))
                    .collect(),
            },
            SessionLayout::SinglePartition => Self::SinglePartition(),
            SessionLayout::PerAgentPartition(map) => Self::PerAgentPartition {
                partitions: map
                    .iter()
                    .map(|(agent, partition)| (agent.to_string(), *partition))
                    .collect(),
            },
        }
    }
}

/// The SDK that created a session.
#[gen_stub_pyclass]
#[pyclass(name = "SdkInfo", frozen, eq)]
#[derive(PartialEq)]
pub struct PySdkInfo {
    language: String,
    version: String,
}

#[gen_stub_pymethods]
#[pymethods]
impl PySdkInfo {
    #[getter]
    fn language(&self) -> &str {
        &self.language
    }

    #[getter]
    fn version(&self) -> &str {
        &self.version
    }
}

/// What `Sessions.bootstrap` set up.
#[gen_stub_pyclass]
#[pyclass(name = "SessionBootstrap", frozen)]
pub struct PySessionBootstrap {
    registered: bool,
}

#[gen_stub_pymethods]
#[pymethods]
impl PySessionBootstrap {
    /// Whether the stream is now registered as a session source.
    #[getter]
    fn registered(&self) -> bool {
        self.registered
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    /// The session accessor. `stream` puts the sessions on another stream,
    /// `idle_timeout_ms` and `heartbeat_ms` replace the 300,000 and 60,000
    /// millisecond defaults, `register_source` and `fail_on_dead_letter` replace the on
    /// and off defaults, and `memory_namespace`, `context_turns`,
    /// `context_tokens` replace `agent.session`, 50, and 4000. `layout` is a
    /// `SessionLayout`, and `sdk=(language, version)` names another SDK on
    /// session starts.
    #[pyo3(signature = (*, stream=None, layout=None, idle_timeout_ms=None, heartbeat_ms=None, register_source=None, fail_on_dead_letter=None, memory_namespace=None, context_turns=None, context_tokens=None, sdk=None))]
    #[allow(clippy::too_many_arguments)]
    fn sessions(
        &self,
        stream: Option<String>,
        layout: Option<PySessionLayout>,
        idle_timeout_ms: Option<f64>,
        heartbeat_ms: Option<f64>,
        register_source: Option<bool>,
        fail_on_dead_letter: Option<bool>,
        memory_namespace: Option<String>,
        context_turns: Option<usize>,
        context_tokens: Option<usize>,
        sdk: Option<(String, String)>,
    ) -> PyResult<PySessions> {
        let (language, version) =
            sdk.unwrap_or_else(|| ("python".to_owned(), env!("CARGO_PKG_VERSION").to_owned()));
        let mut config = SessionConfig::new().sdk(language, version);
        if let Some(stream) = stream {
            config = config.stream(stream);
        }
        if let Some(layout) = layout {
            config = config.layout(layout.to_rust()?);
        }
        if let Some(ms) = idle_timeout_ms {
            config = config.idle_timeout(millis(ms, "idle_timeout_ms")?);
        }
        if let Some(ms) = heartbeat_ms {
            config = config.heartbeat(millis(ms, "heartbeat_ms")?);
        }
        if let Some(register) = register_source {
            config = config.register_source(register);
        }
        if let Some(fail) = fail_on_dead_letter {
            config = config.fail_on_dead_letter(fail);
        }
        if let Some(namespace) = memory_namespace {
            config = config.memory_namespace(namespace);
        }
        if let Some(turns) = context_turns {
            config = config.context_turns(turns);
        }
        if let Some(tokens) = context_tokens {
            config = config.context_tokens(tokens);
        }
        Ok(PySessions {
            inner: self.inner.sessions_with(config),
        })
    }
}

/// The session id `label` derives to in `stream` under `namespace`.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn derive_session_id(stream: &str, namespace: &str, label: &str) -> String {
    laser_sdk::agent::derive_session_id(stream, namespace, label).to_string()
}

/// The session factory. Build it with `Laser.sessions`.
#[gen_stub_pyclass]
#[pyclass(name = "Sessions")]
pub struct PySessions {
    pub(crate) inner: Sessions,
}

#[gen_stub_pymethods]
#[pymethods]
impl PySessions {
    /// A session named `label`. Its id derives from the stream, the
    /// namespace, and the label.
    fn create(&self, label: String) -> PySessionBuilder {
        PySessionBuilder::new(self.inner.create(label))
    }

    /// A fresh session with a new id.
    fn start(&self) -> PySessionBuilder {
        PySessionBuilder::new(self.inner.start())
    }

    /// The session over an existing id, as a lens: no IO, no lease, and no
    /// author until `Session.as_agent` names one.
    fn open(&self, conversation_id: String) -> PyResult<PySession> {
        Ok(PySession::new(
            self.inner.open(conversation(&conversation_id)?),
        ))
    }

    /// Create the agent topics, then register the stream as a session source
    /// when the deployment serves sessions. Returns whether it registered.
    fn bootstrap<'py>(
        &self,
        py: Python<'py>,
        partitions: u32,
        retention: &PyTopicRetention,
    ) -> PyResult<Bound<'py, PyAny>> {
        let sessions = self.inner.clone();
        let retention = retention.inner;
        future_into_py(py, async move {
            let bootstrap = sessions
                .bootstrap(partitions, retention)
                .await
                .map_err(to_pyerr)?;
            Ok(PySessionBootstrap {
                registered: bootstrap.registered,
            })
        })
    }

    /// The stream the sessions ride.
    #[getter]
    fn stream(&self) -> PyResult<String> {
        Ok(self.inner.stream().map_err(to_pyerr)?.to_owned())
    }

    /// The layout every session of this factory uses.
    #[getter]
    fn config(&self) -> PySessionConfig {
        PySessionConfig {
            inner: self.inner.config().clone(),
        }
    }
}

/// How a `Sessions` factory lays its sessions out. Read it from
/// `Sessions.config` or `Session.config`, set it through the
/// `Laser.sessions` keywords.
#[gen_stub_pyclass]
#[pyclass(name = "SessionConfig", frozen)]
pub struct PySessionConfig {
    inner: SessionConfig,
}

#[gen_stub_pymethods]
#[pymethods]
impl PySessionConfig {
    /// The stream sessions ride, or `None` for the connection's default stream.
    #[getter]
    fn stream_name(&self) -> Option<&str> {
        self.inner.stream_name()
    }

    /// Where agents put their work.
    #[getter]
    fn layout(&self) -> PySessionLayout {
        PySessionLayout::from_rust(self.inner.layout_kind())
    }

    /// The SDK a session start names.
    #[getter]
    fn sdk(&self) -> PySdkInfo {
        PySdkInfo {
            language: self.inner.sdk_info().language.clone(),
            version: self.inner.sdk_info().version.clone(),
        }
    }

    /// The default idle timeout in milliseconds.
    #[getter]
    fn idle_timeout_ms(&self) -> f64 {
        self.inner.idle_timeout_value().as_secs_f64() * 1000.0
    }

    /// The heartbeat interval in milliseconds.
    #[getter]
    fn heartbeat_ms(&self) -> f64 {
        self.inner.heartbeat_value().as_secs_f64() * 1000.0
    }

    /// Whether bootstrap registers the session source.
    #[getter]
    fn registers_source(&self) -> bool {
        self.inner.registers_source()
    }

    /// Whether a dead letter fails its session.
    #[getter]
    fn fails_on_dead_letter(&self) -> bool {
        self.inner.fails_on_dead_letter()
    }

    /// The memory namespace `Session.memory` opens.
    #[getter]
    fn memory_namespace_name(&self) -> &str {
        self.inner.memory_namespace_name()
    }

    /// The turn bound of `Session.context`.
    #[getter]
    fn context_turn_bound(&self) -> usize {
        self.inner.context_turn_bound()
    }

    /// The estimated token bound of `Session.context`.
    #[getter]
    fn context_token_bound(&self) -> usize {
        self.inner.context_token_bound()
    }

    fn __repr__(&self) -> String {
        format!(
            "SessionConfig(stream={:?}, memory_namespace={:?}, context_turns={}, context_tokens={})",
            self.inner.stream_name(),
            self.inner.memory_namespace_name(),
            self.inner.context_turn_bound(),
            self.inner.context_token_bound()
        )
    }
}

/// A session about to start. Chain the settings, then `await begin()` for a
/// `(Session, SessionLease)` pair, or use it as `async with`: the session
/// ends when the block finishes and fails with the traceback when it raises.
#[gen_stub_pyclass]
#[pyclass(name = "SessionBuilder")]
pub struct PySessionBuilder {
    inner: Mutex<Option<SessionBuilder>>,
    entered: Mutex<Option<(Session, SessionLease)>>,
}

impl PySessionBuilder {
    fn new(inner: SessionBuilder) -> Self {
        Self {
            inner: Mutex::new(Some(inner)),
            entered: Mutex::new(None),
        }
    }

    fn update(&self, change: impl FnOnce(SessionBuilder) -> SessionBuilder) -> PyResult<()> {
        let mut inner = self.inner.lock().expect("session builder lock");
        let builder = inner
            .take()
            .ok_or_else(|| InvalidError::new_err("this session builder already began"))?;
        *inner = Some(change(builder));
        Ok(())
    }

    fn take(&self) -> PyResult<SessionBuilder> {
        self.inner
            .lock()
            .expect("session builder lock")
            .take()
            .ok_or_else(|| InvalidError::new_err("this session builder already began"))
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PySessionBuilder {
    /// The agent that owns and writes the session. Required.
    fn agent<'py>(slf: PyRef<'py, Self>, agent_id: &str) -> PyResult<PyRef<'py, Self>> {
        let agent = agent(agent_id)?;
        slf.update(|builder| builder.agent(agent))?;
        Ok(slf)
    }

    /// The namespace a labeled session id derives under.
    fn namespace<'py>(slf: PyRef<'py, Self>, namespace: String) -> PyResult<PyRef<'py, Self>> {
        slf.update(|builder| builder.namespace(namespace))?;
        Ok(slf)
    }

    /// Make this a child of `parent`, in the tree rooted at `root`.
    fn parent<'py>(slf: PyRef<'py, Self>, parent: &str, root: &str) -> PyResult<PyRef<'py, Self>> {
        let (parent, root) = (conversation(parent)?, conversation(root)?);
        slf.update(|builder| builder.parent(parent, root))?;
        Ok(slf)
    }

    /// Start the session under an explicit id.
    fn with_id<'py>(slf: PyRef<'py, Self>, id: &str) -> PyResult<PyRef<'py, Self>> {
        let id = conversation(id)?;
        slf.update(|builder| builder.with_id(id))?;
        Ok(slf)
    }

    /// The idle timeout in milliseconds, instead of the configured default.
    fn idle_timeout<'py>(slf: PyRef<'py, Self>, timeout_ms: f64) -> PyResult<PyRef<'py, Self>> {
        let timeout = millis(timeout_ms, "timeout_ms")?;
        slf.update(|builder| builder.idle_timeout(timeout))?;
        Ok(slf)
    }

    /// The token and cost ceiling a reader compares the session's usage with.
    fn budget<'py>(slf: PyRef<'py, Self>, budget: PyBudget) -> PyResult<PyRef<'py, Self>> {
        slf.update(|builder| builder.budget(budget.inner))?;
        Ok(slf)
    }

    /// One searchable tag.
    fn tag<'py>(slf: PyRef<'py, Self>, tag: String) -> PyResult<PyRef<'py, Self>> {
        slf.update(|builder| builder.tag(tag))?;
        Ok(slf)
    }

    /// The id this builder starts.
    fn id(&self) -> PyResult<String> {
        let inner = self.inner.lock().expect("session builder lock");
        let builder = inner
            .as_ref()
            .ok_or_else(|| InvalidError::new_err("this session builder already began"))?;
        Ok(builder.id().map_err(to_pyerr)?.to_string())
    }

    /// Write the session start and take a lease. Returns `(Session, SessionLease)`.
    fn begin<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let builder = self.take()?;
        future_into_py(py, async move {
            let (session, lease) = builder.begin().await.map_err(to_pyerr)?;
            Ok((PySession::new(session), PySessionLease::new(lease)))
        })
    }

    fn __aenter__<'py>(slf: Py<Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let builder = slf.borrow(py).take()?;
        future_into_py(py, async move {
            let (session, lease) = builder.begin().await.map_err(to_pyerr)?;
            let handle = PySession::new(session.clone());
            Python::attach(|py| {
                *slf.borrow(py).entered.lock().expect("session entry lock") =
                    Some((session, lease));
            });
            Ok(handle)
        })
    }

    #[pyo3(signature = (exc_type=None, exc_value=None, traceback=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        exc_type: Option<Bound<'py, PyAny>>,
        exc_value: Option<Bound<'py, PyAny>>,
        traceback: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let entered = self.entered.lock().expect("session entry lock").take();
        let failure = match (&exc_type, &exc_value) {
            (Some(kind), Some(value)) if !kind.is_none() => {
                let formatted = py
                    .import("traceback")?
                    .call_method1("format_exception", (kind, value, traceback))?
                    .extract::<Vec<String>>()?
                    .concat();
                Some((
                    value.str()?.to_string(),
                    kind.getattr("__name__")?.to_string(),
                    formatted,
                ))
            }
            _ => None,
        };
        let raised = failure.is_some();
        future_into_py(py, async move {
            let Some((session, lease)) = entered else {
                return Ok(false);
            };
            let ended = match failure {
                Some((message, kind, traceback)) => {
                    session
                        .fail(AgentErrorBody {
                            code: AgentErrorCode::Internal,
                            message: Some(message),
                            retryable: false,
                            detail: Some(std::collections::BTreeMap::from([
                                (
                                    "exception".to_owned(),
                                    laser_sdk::wire::query::Value::Str(kind),
                                ),
                                (
                                    "traceback".to_owned(),
                                    laser_sdk::wire::query::Value::Str(traceback),
                                ),
                            ])),
                        })
                        .await
                }
                None => session.end().await,
            };
            drop(lease);
            // The original exception wins over a failed terminal write.
            if ended.is_err() && raised {
                return Ok(false);
            }
            ended.map_err(to_pyerr)?;
            Ok(false)
        })
    }
}

/// Ownership of one session's liveness: while held, the process lists the
/// session in its heartbeat. Release it with `release` or by dropping it.
#[gen_stub_pyclass]
#[pyclass(name = "SessionLease")]
pub struct PySessionLease {
    inner: Mutex<Option<SessionLease>>,
    session: String,
    stream: String,
}

impl PySessionLease {
    fn new(lease: SessionLease) -> Self {
        Self {
            session: lease.session().to_string(),
            stream: lease.stream().to_owned(),
            inner: Mutex::new(Some(lease)),
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PySessionLease {
    /// The leased session id.
    #[getter]
    fn session(&self) -> &str {
        &self.session
    }

    /// The stream the leased session lives in.
    #[getter]
    fn stream(&self) -> &str {
        &self.stream
    }

    /// Release the lease now.
    fn release(&self) {
        self.inner.lock().expect("session lease lock").take();
    }
}

/// One session. A cheap handle: copies share the terminal latch. Build it with
/// `Sessions.create`, `Sessions.start`, or `Sessions.open`.
#[gen_stub_pyclass]
#[pyclass(name = "Session")]
pub struct PySession {
    pub(crate) inner: Session,
}

impl PySession {
    pub(crate) fn new(inner: Session) -> Self {
        Self { inner }
    }
}

/// The conversation id a session `policy` maps `key` to: `per_call` mints a
/// fresh one on every call, `per_user` derives the same one for the same key.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn session_policy_conversation_for(policy: &str, key: &str) -> PyResult<String> {
    let policy = match policy {
        "per_call" => SessionPolicy::PerCall,
        "per_user" => SessionPolicy::PerUser,
        other => {
            return Err(InvalidError::new_err(format!(
                "unknown session policy '{other}' (expected per_call or per_user)"
            )));
        }
    };
    Ok(policy.conversation_for(key).to_string())
}

#[gen_stub_pymethods]
#[pymethods]
impl PySession {
    /// This handle writing as `agent_id`.
    fn as_agent(&self, agent_id: &str) -> PyResult<PySession> {
        Ok(PySession::new(
            self.inner.clone().as_agent(agent(agent_id)?),
        ))
    }

    /// This session's id.
    #[getter]
    fn conversation(&self) -> String {
        self.inner.conversation().to_string()
    }

    /// The stream this session lives in.
    #[getter]
    fn stream(&self) -> PyResult<String> {
        Ok(self.inner.stream().map_err(to_pyerr)?.to_owned())
    }

    /// The agent this handle writes as, or None.
    #[getter]
    fn agent(&self) -> Option<String> {
        self.inner.agent().map(ToString::to_string)
    }

    /// The parent session id of a child session, or None.
    #[getter]
    fn parent(&self) -> Option<String> {
        self.inner.parent().map(|id| id.to_string())
    }

    /// The root session id of a child session, or None.
    #[getter]
    fn root(&self) -> Option<String> {
        self.inner.root().map(|id| id.to_string())
    }

    /// The layout this session uses, its factory's `config`.
    #[getter]
    fn config(&self) -> PySessionConfig {
        PySessionConfig {
            inner: self.inner.config().clone(),
        }
    }

    /// The underlying `ContextScope`, for a topic outside the session lane or
    /// an explicit read shape.
    fn scope(&self) -> PyContextScope {
        PyContextScope::new(self.inner.scope().clone())
    }

    /// Append one typed envelope, given as the dict `decode_agent_envelope`
    /// returns, to the session lane. It must name this session as its
    /// conversation. Returns an `AgdxReceipt`.
    fn append<'py>(
        &self,
        py: Python<'py>,
        envelope: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        let envelope: AgentEnvelope = py_to_de(envelope)?;
        future_into_py(py, async move {
            let receipt = session.append(envelope).await.map_err(to_pyerr)?;
            Ok(crate::agdx::PyAgdxReceipt::from(receipt))
        })
    }

    /// End the session as completed.
    fn end<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        future_into_py(py, async move { session.end().await.map_err(to_pyerr) })
    }

    /// End the session as failed with `error`, an `AgentErrorBody` dict
    /// (`code`, and optionally `message`, `retryable`, and `detail`), the same
    /// shape `Agdx.fail` takes.
    fn fail<'py>(
        &self,
        py: Python<'py>,
        #[gen_stub(override_type(type_repr = "dict[str, typing.Any]", imports = ("typing",)))]
        error: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let error: AgentErrorBody = crate::convert::py_to_de(error)?;
        let session = self.inner.clone();
        future_into_py(
            py,
            async move { session.fail(error).await.map_err(to_pyerr) },
        )
    }

    /// End the session as canceled.
    fn cancel<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        future_into_py(py, async move { session.cancel().await.map_err(to_pyerr) })
    }

    /// Run `work(session)`, which may be async, and end the session by its
    /// outcome: completed on success, failed with the exception type and
    /// traceback when it raises. The original exception is re-raised, and it
    /// wins over a failed terminal write. The lease is released at the end.
    fn run<'py>(
        &self,
        py: Python<'py>,
        lease: &PySessionLease,
        work: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        let held = lease.inner.lock().expect("session lease lock").take();
        let hook = crate::async_bridge::PyHook::new(work, "__call__", "session work")?;
        let handle = Py::new(py, PySession::new(session.clone()))?;
        future_into_py(py, async move {
            let outcome = hook
                .call(move |py| pyo3::types::PyTuple::new(py, [handle.clone_ref(py)]))
                .await;
            let result = match outcome {
                Ok(value) => session.end().await.map(|()| value).map_err(to_pyerr),
                Err(error) => {
                    let body = Python::attach(|py| failure_body(py, &error));
                    let _ = session.fail(body).await;
                    Err(error)
                }
            };
            drop(held);
            result
        })
    }

    /// The model-ready context: the last `last_n` records on the session lane,
    /// trimmed to `token_budget` estimated tokens. Both default to the
    /// factory's configured bounds.
    #[pyo3(signature = (*, last_n=None, token_budget=None))]
    fn context<'py>(
        &self,
        py: Python<'py>,
        last_n: Option<usize>,
        token_budget: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        let config = self.inner.config();
        let policy = bounded_policy(
            last_n.unwrap_or(config.context_turn_bound()),
            Some(token_budget.unwrap_or(config.context_token_bound())),
        );
        future_into_py(py, async move {
            let turns = session.context_with(policy).await.map_err(to_pyerr)?;
            Ok(turns
                .into_iter()
                .map(PySessionTurn::from)
                .collect::<Vec<_>>())
        })
    }

    /// The context under an explicit policy: `LastN`, `TokenBudget`,
    /// `RoleFilter`, or a `Chain` of them.
    fn context_with<'py>(
        &self,
        py: Python<'py>,
        policy: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        let (policy, failure) = context_policy(policy)?;
        future_into_py(py, async move {
            let turns = session.context_with(policy).await.map_err(to_pyerr)?;
            if let Some(error) = take_failure(&failure) {
                return Err(error);
            }
            Ok(turns
                .into_iter()
                .map(PySessionTurn::from)
                .collect::<Vec<_>>())
        })
    }

    /// The knowledge graph `name`, the same graph `laser.graph(name)` returns.
    fn graph(&self, name: String) -> crate::graph::PyGraph {
        crate::graph::PyGraph::new(self.inner.scope().laser().clone(), name)
    }

    /// This session's memory, scoped to the session: `memory` defaults to
    /// `laser.memory(<configured namespace>)`, or pass any memory handle.
    #[pyo3(signature = (memory=None))]
    fn memory(&self, memory: Option<&PyMemory>) -> PyScopedMemory {
        let backend = match memory {
            Some(memory) => memory.backend(),
            None => Backend::new(
                self.inner
                    .scope()
                    .laser()
                    .memory(self.inner.config().memory_namespace_name()),
            ),
        };
        PyScopedMemory::new(backend, self.inner.conversation())
    }

    /// This session's memory in an explicit `namespace`, scoped to the session.
    fn memory_in(&self, namespace: String) -> PyScopedMemory {
        PyScopedMemory::new(
            Backend::new(self.inner.scope().laser().memory(namespace)),
            self.inner.conversation(),
        )
    }

    /// Where the session lane ends right now. Persist it with
    /// `Checkpoint.to_json` and hand it to `turns_at`, `turns_since`,
    /// `state_at`, or `replay`.
    fn checkpoint<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        future_into_py(py, async move {
            let checkpoint = session.checkpoint().await.map_err(to_pyerr)?;
            Ok(PyCheckpoint { inner: checkpoint })
        })
    }

    /// The records up to `checkpoint`.
    fn turns_at<'py>(
        &self,
        py: Python<'py>,
        checkpoint: &PyCheckpoint,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        let checkpoint = checkpoint.inner.clone();
        future_into_py(py, async move {
            let turns = session.turns_at(checkpoint).await.map_err(to_pyerr)?;
            Ok(turns
                .into_iter()
                .map(PySessionTurn::from)
                .collect::<Vec<_>>())
        })
    }

    /// The records appended after `checkpoint`.
    fn turns_since<'py>(
        &self,
        py: Python<'py>,
        checkpoint: &PyCheckpoint,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        let checkpoint = checkpoint.inner.clone();
        future_into_py(py, async move {
            let turns = session.turns_since(checkpoint).await.map_err(to_pyerr)?;
            Ok(turns
                .into_iter()
                .map(PySessionTurn::from)
                .collect::<Vec<_>>())
        })
    }

    /// Fold the records up to `checkpoint` with `fold(state, turn) -> state`,
    /// starting from `init`: state as it stood then.
    fn state_at<'py>(
        &self,
        py: Python<'py>,
        checkpoint: &PyCheckpoint,
        init: Py<PyAny>,
        fold: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        let checkpoint = checkpoint.inner.clone();
        future_into_py(py, async move {
            let turns = session.turns_at(checkpoint).await.map_err(to_pyerr)?;
            fold_turns(turns, init, fold)
        })
    }

    /// Fold the records appended after `checkpoint` with `fold(state, turn)
    /// -> state`, starting from `init`.
    fn replay<'py>(
        &self,
        py: Python<'py>,
        checkpoint: &PyCheckpoint,
        init: Py<PyAny>,
        fold: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        let checkpoint = checkpoint.inner.clone();
        future_into_py(py, async move {
            let turns = session.turns_since(checkpoint).await.map_err(to_pyerr)?;
            fold_turns(turns, init, fold)
        })
    }
}

// A failed session's error body from a Python exception: its message, type,
// and formatted traceback.
fn failure_body(py: Python<'_>, error: &PyErr) -> AgentErrorBody {
    let kind = error
        .get_type(py)
        .name()
        .map(|name| name.to_string())
        .unwrap_or_default();
    let traceback = error
        .traceback(py)
        .and_then(|traceback| traceback.format().ok())
        .unwrap_or_default();
    AgentErrorBody {
        code: AgentErrorCode::Internal,
        message: Some(error.value(py).to_string()),
        retryable: false,
        detail: Some(std::collections::BTreeMap::from([
            (
                "exception".to_owned(),
                laser_sdk::wire::query::Value::Str(kind),
            ),
            (
                "traceback".to_owned(),
                laser_sdk::wire::query::Value::Str(traceback),
            ),
        ])),
    }
}

fn fold_turns(turns: Vec<SessionTurn>, initial: Py<PyAny>, fold: Py<PyAny>) -> PyResult<Py<PyAny>> {
    Python::attach(|py| {
        let mut state = initial;
        for turn in turns {
            let turn = Py::new(py, PySessionTurn::from(turn))?;
            state = fold.call1(py, (state, turn))?;
        }
        Ok(state)
    })
}

/// One record read back from a `Session`: the message plus how a timeline
/// shows it.
#[gen_stub_pyclass]
#[pyclass(name = "SessionTurn", frozen)]
pub struct PySessionTurn {
    display: &'static str,
    message: ContextMessage,
}

impl From<SessionTurn> for PySessionTurn {
    fn from(turn: SessionTurn) -> Self {
        Self {
            display: turn.display.as_str(),
            message: turn.message,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PySessionTurn {
    /// How a timeline shows the record, such as `session.started` or
    /// `model.request`.
    #[getter]
    fn display(&self) -> &str {
        self.display
    }

    /// The record's body as UTF-8, lossy: the envelope body of a typed
    /// record, else the raw payload.
    fn text(&self) -> String {
        let body = match &self.message.envelope {
            Some(envelope) => &envelope.body,
            None => &self.message.payload,
        };
        String::from_utf8_lossy(body).into_owned()
    }

    /// The message off the log, with its provenance and topic.
    #[getter]
    fn message(&self) -> PyContextMessage {
        PyContextMessage::new(self.message.clone())
    }

    fn __repr__(&self) -> String {
        format!(
            "SessionTurn(display={:?}, text={:?})",
            self.display,
            self.text()
        )
    }
}

/// A point in a session's log: the next offset each partition of each topic
/// will write, as of `Session.checkpoint`. A client-side bookmark, never a
/// record on the log. Round-trip it through `to_json`/`from_json` to persist
/// it.
#[gen_stub_pyclass]
#[pyclass(name = "Checkpoint", frozen)]
pub struct PyCheckpoint {
    inner: Checkpoint,
}

impl PyCheckpoint {
    pub(crate) fn new(inner: Checkpoint) -> Self {
        Self { inner }
    }

    pub(crate) fn inner(&self) -> &Checkpoint {
        &self.inner
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyCheckpoint {
    /// True when no topic was checkpointed.
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// The checkpointed offsets of `topic` as a dict partition -> offset, or
    /// None when the topic was not checkpointed.
    fn topic_offsets(&self, topic: &str) -> Option<std::collections::BTreeMap<u32, u64>> {
        self.inner.topic_offsets(topic).cloned()
    }

    /// Every checkpointed topic with its partition offsets.
    fn topics(&self) -> std::collections::BTreeMap<String, std::collections::BTreeMap<u32, u64>> {
        self.inner
            .topics()
            .map(|(topic, offsets)| (topic.to_owned(), offsets.clone()))
            .collect()
    }

    /// This checkpoint as JSON.
    fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.inner)
            .map_err(|error| crate::errors::CodecError::new_err(error.to_string()))
    }

    /// A checkpoint from `to_json` output.
    #[staticmethod]
    fn from_json(json: &str) -> PyResult<Self> {
        let inner = serde_json::from_str(json)
            .map_err(|error| crate::errors::CodecError::new_err(error.to_string()))?;
        Ok(Self { inner })
    }

    fn __repr__(&self) -> String {
        format!("Checkpoint({})", self.to_json().unwrap_or_default())
    }
}
