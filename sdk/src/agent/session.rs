use crate::agent::ReplayBound;
use crate::agent::agdx::AgdxReceipt;
use crate::agent::lease::{LeaseKey, SessionLease};
use crate::context::{Chain, Checkpoint, ContextMessage, ContextPolicy, LastN, TokenBudget};
use crate::context_scope::{ContextScope, ScopedMemory};
use crate::error::LaserError;
use crate::laser::Laser;
use crate::provenance::AgentTopic;
use crate::types::{ConversationId, MintUlid};
use futures::FutureExt;
use iggy::prelude::{Identifier, IggyExpiry, MaxTopicSize, StreamClient};
use laser_wire::agent::{
    AgentEnvelope, AgentErrorBody, AgentErrorCode, AgentId, Budget, OPERATION_SESSION, RecordId,
    SdkInfo, SessionEnd, SessionStart, TaskState,
};
use laser_wire::content::ContentType;
use laser_wire::control::{ControlCommand, SessionTopics};
use laser_wire::dispatch::{DisplayType, SessionRecord, display_type};
use laser_wire::framing::encode_named;
use laser_wire::validate::Validate;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// The idle timeout a session start records unless it sets its own.
pub const DEFAULT_SESSION_IDLE_TIMEOUT: Duration = Duration::from_secs(300);

/// The heartbeat interval of a process that holds session leases.
pub const DEFAULT_SESSION_HEARTBEAT: Duration = Duration::from_secs(60);

/// The default memory namespace of a [`Session`]. Conversation scoping keeps
/// one session's memory apart from another's, so one shared namespace is the
/// intended layout. [`SessionConfig::memory_namespace`] changes it.
pub const DEFAULT_SESSION_MEMORY_NAMESPACE: &str = "agent.session";

/// The default turn bound of [`Session::context`].
pub const DEFAULT_SESSION_CONTEXT_TURNS: usize = 50;

/// The default estimated token bound of [`Session::context`].
pub const DEFAULT_SESSION_CONTEXT_TOKENS: usize = 4000;

/// How a user key maps to a conversation: a fresh one per call, or a stable one per user.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionPolicy {
    PerCall,
    PerUser,
}

impl SessionPolicy {
    /// The conversation id for `key` (random for `PerCall`, derived deterministically for `PerUser`).
    pub fn conversation_for(&self, key: &str) -> ConversationId {
        match self {
            Self::PerCall => ConversationId::new(),
            Self::PerUser => ConversationId::derive(key),
        }
    }
}

/// How long `agent.sessions` keeps records. Bootstrap needs one, because the
/// session topic holds every session's lane and there is no safe default.
/// A policy that never expires and has no size bound is refused.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TopicRetention {
    expiry: IggyExpiry,
    max_size: MaxTopicSize,
}

impl TopicRetention {
    /// A retention policy, refused when it would keep records forever with no
    /// size bound.
    pub fn new(expiry: IggyExpiry, max_size: MaxTopicSize) -> Result<Self, LaserError> {
        let unbounded = matches!(
            max_size,
            MaxTopicSize::Unlimited | MaxTopicSize::ServerDefault
        );
        if matches!(expiry, IggyExpiry::NeverExpire | IggyExpiry::ServerDefault) && unbounded {
            return Err(LaserError::Invalid(
                "agent.sessions retention must set an expiry or a size bound".to_owned(),
            ));
        }
        Ok(Self { expiry, max_size })
    }

    /// Expire records after `age`, with the server's default size bound. A
    /// zero age is raised to one microsecond.
    pub fn expire_after(age: Duration) -> Self {
        let micros = micros(age).max(1);
        Self {
            expiry: IggyExpiry::ExpireDuration(iggy::prelude::IggyDuration::from(micros)),
            max_size: MaxTopicSize::ServerDefault,
        }
    }

    /// The message expiry.
    pub fn expiry(&self) -> IggyExpiry {
        self.expiry
    }

    /// The topic size bound.
    pub fn max_size(&self) -> MaxTopicSize {
        self.max_size
    }
}

/// Where a stream's agents put their work. Lifecycle and state always ride the
/// session's lane on `agent.sessions` in every layout.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum SessionLayout {
    /// One `agent.sessions` topic keyed by session. The default.
    #[default]
    Shared,
    /// Each declared agent reads its own topic, keyed by session. On
    /// `agent.sessions` sends, a command addressed to a declared agent goes to
    /// that agent's topic, and a response, error, or chunk goes to its
    /// addressee's declared topic. Lifecycle, state, broadcast records, and
    /// records for an undeclared agent stay on the session lane. Bootstrap
    /// creates the declared topics with the lane's partition count and
    /// retention.
    PerAgentTopic(BTreeMap<AgentId, String>),
    /// Work rides `agent.sessions` on a declared partition per agent: a
    /// command lands on its addressee's partition and a reply on its
    /// requester's. Lifecycle, state, and records for an undeclared agent
    /// stay on the session's partition.
    PerAgentPartition(BTreeMap<AgentId, u32>),
    /// Everything on one partition: bootstrap creates every agent topic with
    /// one partition.
    SinglePartition,
}

impl SessionLayout {
    /// A [`PerAgentTopic`](Self::PerAgentTopic) layout from agent and topic
    /// pairs. Takes the prelude agent id or the wire one.
    pub fn per_agent_topic<A, T>(topics: impl IntoIterator<Item = (A, T)>) -> Self
    where
        A: Into<AgentId>,
        T: Into<String>,
    {
        Self::PerAgentTopic(
            topics
                .into_iter()
                .map(|(agent, topic)| (agent.into(), topic.into()))
                .collect(),
        )
    }

    /// A [`PerAgentPartition`](Self::PerAgentPartition) layout from agent and
    /// partition pairs. Takes the prelude agent id or the wire one.
    pub fn per_agent_partition<A>(partitions: impl IntoIterator<Item = (A, u32)>) -> Self
    where
        A: Into<AgentId>,
    {
        Self::PerAgentPartition(
            partitions
                .into_iter()
                .map(|(agent, partition)| (agent.into(), partition))
                .collect(),
        )
    }
}

// How often bootstrap checks whether the registration is served.
const REGISTRATION_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// The layout and defaults of [`Laser::sessions`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionConfig {
    stream: Option<String>,
    layout: Option<SessionLayout>,
    idle_timeout: Duration,
    heartbeat: Duration,
    register_source: bool,
    fail_on_dead_letter: bool,
    memory_namespace: String,
    context_turns: usize,
    context_tokens: usize,
    sdk: SdkInfo,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            stream: None,
            layout: None,
            idle_timeout: DEFAULT_SESSION_IDLE_TIMEOUT,
            heartbeat: DEFAULT_SESSION_HEARTBEAT,
            register_source: true,
            fail_on_dead_letter: false,
            memory_namespace: DEFAULT_SESSION_MEMORY_NAMESPACE.to_owned(),
            context_turns: DEFAULT_SESSION_CONTEXT_TURNS,
            context_tokens: DEFAULT_SESSION_CONTEXT_TOKENS,
            sdk: SdkInfo {
                language: "rust".to_owned(),
                version: env!("CARGO_PKG_VERSION").to_owned(),
            },
        }
    }
}

impl SessionConfig {
    /// The defaults.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Lay sessions out on `stream` instead of the connection's default stream.
    #[must_use]
    pub fn stream(mut self, stream: impl Into<String>) -> Self {
        self.stream = Some(stream.into());
        self
    }

    /// Where agents put their work.
    #[must_use]
    pub fn layout(mut self, layout: SessionLayout) -> Self {
        self.layout = Some(layout);
        self
    }

    /// The idle timeout a session start records unless it sets its own.
    #[must_use]
    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// How often a process with session leases publishes its heartbeat.
    #[must_use]
    pub fn heartbeat(mut self, interval: Duration) -> Self {
        self.heartbeat = interval;
        self
    }

    /// Whether bootstrap registers the stream as a session source on a
    /// deployment that serves sessions. On by default.
    #[must_use]
    pub fn register_source(mut self, register: bool) -> Self {
        self.register_source = register;
        self
    }

    /// Fail a session when one of its records is dead-lettered. Off by
    /// default: a dead letter counts as an error and never ends a session.
    #[must_use]
    pub fn fail_on_dead_letter(mut self, fail: bool) -> Self {
        self.fail_on_dead_letter = fail;
        self
    }

    /// The memory namespace [`Session::memory`] opens.
    #[must_use]
    pub fn memory_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.memory_namespace = namespace.into();
        self
    }

    /// The turn bound of [`Session::context`].
    #[must_use]
    pub fn context_turns(mut self, turns: usize) -> Self {
        self.context_turns = turns;
        self
    }

    /// The estimated token bound of [`Session::context`].
    #[must_use]
    pub fn context_tokens(mut self, tokens: usize) -> Self {
        self.context_tokens = tokens;
        self
    }

    /// The SDK a session start names. Language bindings set their own.
    #[must_use]
    pub fn sdk(mut self, language: impl Into<String>, version: impl Into<String>) -> Self {
        self.sdk = SdkInfo {
            language: language.into(),
            version: version.into(),
        };
        self
    }

    /// The stream sessions ride, or `None` for the connection's default stream.
    #[must_use]
    pub fn stream_name(&self) -> Option<&str> {
        self.stream.as_deref()
    }

    /// The layout.
    #[must_use]
    pub fn layout_kind(&self) -> &SessionLayout {
        const SHARED: &SessionLayout = &SessionLayout::Shared;
        self.layout.as_ref().unwrap_or(SHARED)
    }

    /// The default idle timeout.
    #[must_use]
    pub fn idle_timeout_value(&self) -> Duration {
        self.idle_timeout
    }

    /// The heartbeat interval.
    #[must_use]
    pub fn heartbeat_value(&self) -> Duration {
        self.heartbeat
    }

    /// Whether bootstrap registers the session source.
    #[must_use]
    pub fn registers_source(&self) -> bool {
        self.register_source
    }

    /// Whether a dead letter fails its session.
    #[must_use]
    pub fn fails_on_dead_letter(&self) -> bool {
        self.fail_on_dead_letter
    }

    /// The memory namespace [`Session::memory`] opens.
    #[must_use]
    pub fn memory_namespace_name(&self) -> &str {
        &self.memory_namespace
    }

    /// The turn bound of [`Session::context`].
    #[must_use]
    pub fn context_turn_bound(&self) -> usize {
        self.context_turns
    }

    /// The estimated token bound of [`Session::context`].
    #[must_use]
    pub fn context_token_bound(&self) -> usize {
        self.context_tokens
    }

    /// The SDK a session start names.
    #[must_use]
    pub fn sdk_info(&self) -> &SdkInfo {
        &self.sdk
    }

    fn policy(&self) -> Box<dyn ContextPolicy> {
        Box::new(Chain(vec![
            Box::new(LastN(self.context_turns)),
            Box::new(TokenBudget::new(self.context_tokens)),
        ]))
    }
}

/// The session id `label` derives to in `stream` under `namespace`. The same
/// three always reach the same session, so two applications that share a
/// stream share a session on purpose only when they pick the same namespace
/// and label.
pub fn derive_session_id(stream: &str, namespace: &str, label: &str) -> ConversationId {
    ConversationId::derive(&format!("{stream}\u{1f}{namespace}\u{1f}{label}"))
}

impl Laser {
    /// The session accessor under [`SessionConfig::default`]. Free and
    /// synchronous, IO happens at the verbs.
    pub fn sessions(&self) -> Sessions {
        self.sessions_with(SessionConfig::default())
    }

    /// [`sessions`](Self::sessions) under an explicit [`SessionConfig`].
    pub fn sessions_with(&self, config: SessionConfig) -> Sessions {
        let laser = match &config.stream {
            Some(stream) => self.with_default_stream(stream.clone()),
            None => self.clone(),
        };
        // A declared layout holds for every later send on the stream through
        // this connection, so agents and lenses route the same way.
        if let (Some(layout), Some(stream)) = (&config.layout, laser.default_stream()) {
            laser.declare_layout(stream, layout.clone());
        }
        Sessions {
            laser,
            config: Arc::new(config),
            lane_guard: Arc::default(),
        }
    }
}

/// The session factory. Build it with [`Laser::sessions`] or
/// [`Laser::sessions_with`].
#[derive(Clone)]
pub struct Sessions {
    laser: Laser,
    config: Arc<SessionConfig>,
    lane_guard: Arc<LaneGuard>,
}

impl Sessions {
    /// A session named `label`. Its id derives from the stream, the namespace,
    /// and the label, so the same label reaches the same session.
    pub fn create(&self, label: impl Into<String>) -> SessionBuilder {
        SessionBuilder::new(self.clone(), Some(label.into()))
    }

    /// A fresh session with a new id.
    pub fn start(&self) -> SessionBuilder {
        SessionBuilder::new(self.clone(), None)
    }

    /// The session over an existing `conversation`, as a lens: no IO, no
    /// lease, and no author until [`Session::as_agent`] names one.
    pub fn open(&self, conversation: ConversationId) -> Session {
        Session::lens(self.laser.clone(), Arc::clone(&self.config), conversation)
            .with_lane_guard(Arc::clone(&self.lane_guard))
    }

    /// This factory's layout.
    pub fn config(&self) -> &SessionConfig {
        &self.config
    }

    /// Create the agent topics with [`Laser::bootstrap`], then, when the
    /// deployment serves sessions and [`SessionConfig::register_source`] is
    /// on, register the stream as a session source. A refused registration
    /// is logged and reported as `registered: false`, never an error, because
    /// an account without session administration rights relies on
    /// provisioning to register the stream.
    pub async fn bootstrap(
        &self,
        partitions: u32,
        retention: TopicRetention,
    ) -> Result<SessionBootstrap, LaserError> {
        let partitions = match self.config.layout_kind() {
            SessionLayout::SinglePartition => 1,
            _ => partitions,
        };
        let declared = match self.config.layout_kind() {
            SessionLayout::PerAgentTopic(topics) => declared_topics(topics)?,
            _ => Vec::new(),
        };
        self.laser.bootstrap(partitions, retention).await?;
        // Each declared agent topic carries a share of the session's work, so
        // it takes the lane's partition count and retention.
        let stream = self.stream()?.to_owned();
        for topic in &declared {
            crate::laser::ensure_topic_retained(
                &self.laser.client(),
                &stream,
                topic,
                partitions,
                retention.expiry(),
                retention.max_size(),
            )
            .await?;
        }
        self.lane_guard.observe(&self.laser).await?;
        if !self.config.register_source || !self.laser.capabilities().await.sessions {
            return Ok(SessionBootstrap { registered: false });
        }
        let stream = self.stream()?.to_owned();
        let registered = match self
            .laser
            .publish_control(ControlCommand::RegisterSessionSource {
                stream: stream.clone(),
                topics: SessionTopics::All,
            })
            .await
        {
            Ok(()) => self.registration_visible(&stream).await,
            Err(error) => {
                tracing::warn!(stream = %stream, %error, "session source registration refused");
                false
            }
        };
        Ok(SessionBootstrap { registered })
    }

    /// The stream the sessions ride.
    pub fn stream(&self) -> Result<&str, LaserError> {
        self.laser.stream_required()
    }

    pub(crate) fn laser(&self) -> &Laser {
        &self.laser
    }

    // Wait until the deployment serves reads for the registered stream, up to
    // the publish timeout. Registration applies after its control record
    // folds, so reads and linked writes right after bootstrap would otherwise
    // race it.
    async fn registration_visible(&self, stream: &str) -> bool {
        let deadline = tokio::time::Instant::now() + self.laser.publish_options().timeout;
        let mut last_unavailable = None;
        loop {
            match self.changes(0, 1).await {
                Err(LaserError::Session(
                    laser_wire::session::SessionError::NotRegistered(_)
                    | laser_wire::session::SessionError::Stale(_),
                )) => {}
                Ok(_) => return true,
                Err(error) if error.is_unavailable() => {
                    last_unavailable = Some(error.to_string());
                }
                Err(error) => {
                    tracing::warn!(stream = %stream, %error, "session source registration not confirmed");
                    return false;
                }
            }
            if tokio::time::Instant::now() >= deadline {
                tracing::warn!(stream = %stream, last_unavailable = ?last_unavailable, "session source registration not visible before the publish timeout");
                return false;
            }
            tokio::time::sleep(REGISTRATION_POLL_INTERVAL).await;
        }
    }
}

// The distinct topics a per-agent topic layout declares, each a valid topic
// name that is not one of the topics the layout routes around.
fn declared_topics(topics: &BTreeMap<AgentId, String>) -> Result<Vec<String>, LaserError> {
    let mut declared: Vec<String> = Vec::new();
    for topic in topics.values() {
        iggy::prelude::Identifier::named(topic)?;
        if topic == laser_wire::topics::AGENT_SESSIONS || topic == laser_wire::topics::AGENT_CONTROL
        {
            return Err(LaserError::Invalid(format!(
                "a per-agent topic layout cannot declare `{topic}`"
            )));
        }
        if !declared.contains(topic) {
            declared.push(topic.clone());
        }
    }
    Ok(declared)
}

/// What [`Sessions::bootstrap`] set up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionBootstrap {
    /// Whether the stream is now registered as a session source.
    pub registered: bool,
}

/// A session about to start. Chain the optional settings, then
/// [`begin`](Self::begin).
#[must_use = "a session builder does nothing until begin().await"]
pub struct SessionBuilder {
    sessions: Sessions,
    label: Option<String>,
    namespace: Option<String>,
    agent: Option<AgentId>,
    parent: Option<ConversationId>,
    root: Option<ConversationId>,
    idle_timeout: Option<Duration>,
    budget: Option<Budget>,
    tags: Vec<String>,
    id: Option<ConversationId>,
}

impl SessionBuilder {
    fn new(sessions: Sessions, label: Option<String>) -> Self {
        Self {
            sessions,
            label,
            namespace: None,
            agent: None,
            parent: None,
            root: None,
            idle_timeout: None,
            budget: None,
            tags: Vec::new(),
            id: None,
        }
    }

    /// The agent that owns and writes the session. Required.
    pub fn agent(mut self, agent: impl Into<AgentId>) -> Self {
        let agent = agent.into();
        self.agent = Some(agent);
        self
    }

    /// The namespace a labeled session id derives under.
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Make this a child of `parent`, whose tree is rooted at `root` (the
    /// parent itself when it has no parent).
    pub fn parent(mut self, parent: ConversationId, root: ConversationId) -> Self {
        self.parent = Some(parent);
        self.root = Some(root);
        self
    }

    /// Start the session under an explicit id instead of a label or a fresh id.
    pub fn with_id(mut self, id: ConversationId) -> Self {
        self.id = Some(id);
        self
    }

    /// The idle timeout, instead of the configured default.
    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = Some(timeout);
        self
    }

    /// The token and cost ceiling a reader compares the session's usage with.
    pub fn budget(mut self, budget: Budget) -> Self {
        self.budget = Some(budget);
        self
    }

    /// One searchable tag.
    pub fn tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// The id this builder starts: the explicit id, the label's derived id,
    /// or a fresh one on each call for an unlabeled session.
    pub fn id(&self) -> Result<ConversationId, LaserError> {
        if let Some(id) = self.id {
            return Ok(id);
        }
        Ok(match &self.label {
            Some(label) => derive_session_id(
                self.sessions.stream()?,
                self.namespace.as_deref().unwrap_or_default(),
                label,
            ),
            None => ConversationId::new(),
        })
    }

    /// Write the session start on the lane and take a lease that keeps the
    /// session listed in this process's heartbeat.
    pub async fn begin(self) -> Result<(Session, SessionLease), LaserError> {
        let agent = self
            .agent
            .clone()
            .ok_or_else(|| LaserError::Invalid("a session needs an agent".to_owned()))?;
        let id = self.id()?;
        let config = Arc::clone(&self.sessions.config);
        let idle_timeout = self.idle_timeout.unwrap_or(config.idle_timeout);
        let start = SessionStart {
            label: self.label,
            namespace: self.namespace,
            agent: agent.clone(),
            sdk: config.sdk.clone(),
            parent: self.parent.map(Into::into),
            root: self.root.map(Into::into),
            idle_timeout_micros: Some(micros(idle_timeout)),
            budget: self.budget,
            tags: self.tags,
        };
        start.validate()?;
        let session = Session::lens(self.sessions.laser.clone(), config, id)
            .with_lane_guard(Arc::clone(&self.sessions.lane_guard))
            .as_agent(agent)
            .with_ancestry(self.parent, self.root);
        let generation = session.stream_generation().await?;
        let lane = session.lane()?;
        let status = lane
            .status(OPERATION_SESSION)
            .with_task_state(TaskState::Working)
            .body(encode_named(&start)?)
            .content_type(ContentType::Cbor);
        session.with_envelope_ancestry(status).send().await?;
        let lease = session.lease(generation, idle_timeout);
        Ok((session, lease))
    }
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

// The first terminal a session handle wrote or tried to write. Every clone
// shares it, so a retry repeats the same record and a different verb is
// refused.
#[derive(Clone)]
struct TerminalIntent {
    state: TaskState,
    end: SessionEnd,
    record: RecordId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LaneIdentity {
    pub stream_id: u32,
    pub stream_generation: u64,
    pub topic_id: u32,
    pub topic_generation: u64,
    pub partitions: u32,
}

#[derive(Default)]
pub(crate) struct LaneGuard {
    identity: Mutex<Option<LaneIdentity>>,
    registered: std::sync::atomic::AtomicBool,
}

impl LaneGuard {
    pub(crate) async fn observe(&self, laser: &Laser) -> Result<LaneIdentity, LaserError> {
        let stream = Identifier::named(laser.stream_required()?)?;
        let client = laser.client();
        let details = client
            .get_stream(&stream)
            .await?
            .ok_or_else(|| stale_lane("the session stream does not exist"))?;
        let topic = details
            .topics
            .iter()
            .find(|topic| topic.name == laser_wire::topics::AGENT_SESSIONS)
            .ok_or_else(|| stale_lane("agent.sessions does not exist"))?;
        let current = LaneIdentity {
            stream_id: details.id,
            stream_generation: details.created_at.as_micros(),
            topic_id: topic.id,
            topic_generation: topic.created_at.as_micros(),
            partitions: topic.partitions_count,
        };
        if current.partitions == 0 {
            return Err(stale_lane("agent.sessions has no partitions"));
        }
        let mut pinned = self.identity.lock().expect("session lane identity");
        match *pinned {
            Some(identity) if identity != current => Err(stale_lane(
                "the session lane identity or partition count changed",
            )),
            Some(identity) => Ok(identity),
            None => {
                *pinned = Some(current);
                Ok(current)
            }
        }
    }

    pub(crate) async fn check(
        &self,
        laser: &Laser,
        conversation: ConversationId,
    ) -> Result<LaneIdentity, LaserError> {
        let identity = self.observe(laser).await?;
        if !self.registered.load(std::sync::atomic::Ordering::Acquire)
            && laser.capabilities().await.sessions
        {
            match laser.sessions().registered_lane(conversation).await {
                Ok(view)
                    if view
                        == Some((
                            identity.topic_id,
                            identity.topic_generation,
                            identity.partitions,
                        )) =>
                {
                    self.registered
                        .store(true, std::sync::atomic::Ordering::Release);
                }
                Ok(_) => {
                    return Err(stale_lane(
                        "the registered session lane does not match the current source",
                    ));
                }
                Err(LaserError::Session(laser_wire::session::SessionError::NotRegistered(_))) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(identity)
    }
}

fn stale_lane(message: &str) -> LaserError {
    LaserError::Session(laser_wire::session::SessionError::Stale(message.to_owned()))
}

/// One session. A cheap handle: clones share the terminal latch. Build it
/// with [`Sessions::create`], [`Sessions::start`], or [`Sessions::open`].
#[derive(Clone)]
pub struct Session {
    pub(crate) laser: Laser,
    config: Arc<SessionConfig>,
    scope: ContextScope,
    pub(crate) agent: Option<AgentId>,
    parent: Option<ConversationId>,
    root: Option<ConversationId>,
    terminal: Arc<Mutex<Option<TerminalIntent>>>,
    lane_guard: Arc<LaneGuard>,
    pub(crate) redactor: Arc<dyn Fn(&mut serde_json::Value) + Send + Sync>,
    pub(crate) state: Arc<Mutex<crate::agent::session_ops::StateCursor>>,
    pub(crate) current: Option<laser_wire::graph::SourceRef>,
    // The control state of the runtime that handed this lens to a handler.
    pub(crate) control: Option<Arc<crate::agent::control::ControlBook>>,
    #[cfg(feature = "sign")]
    key: Option<crate::sign::SigningKey>,
}

impl Session {
    fn lens(laser: Laser, config: Arc<SessionConfig>, conversation: ConversationId) -> Self {
        Self {
            scope: laser.context(conversation),
            laser,
            config,
            agent: None,
            parent: None,
            root: None,
            terminal: Arc::new(Mutex::new(None)),
            lane_guard: Arc::default(),
            redactor: Arc::new(crate::agent::session_ops::default_redact),
            state: Arc::default(),
            current: None,
            control: None,
            #[cfg(feature = "sign")]
            key: None,
        }
    }

    fn with_lane_guard(mut self, guard: Arc<LaneGuard>) -> Self {
        self.lane_guard = guard;
        self
    }

    /// This handle signing its terminal record with `key`, so a verifying
    /// reader can prove which agent ended the session. Feature `sign`.
    #[cfg(feature = "sign")]
    #[must_use]
    pub fn signed_by(mut self, key: crate::sign::SigningKey) -> Self {
        self.key = Some(key);
        self
    }

    /// This handle writing as `agent`.
    #[must_use]
    pub fn as_agent(mut self, agent: impl Into<AgentId>) -> Self {
        let agent = agent.into();
        self.agent = Some(agent);
        self
    }

    pub(crate) fn with_ancestry(
        mut self,
        parent: Option<ConversationId>,
        root: Option<ConversationId>,
    ) -> Self {
        self.parent = parent;
        self.root = root;
        self
    }

    /// This session's id.
    pub fn conversation(&self) -> ConversationId {
        self.scope.conversation()
    }

    /// The stream this session lives in.
    pub fn stream(&self) -> Result<&str, LaserError> {
        self.laser.stream_required()
    }

    /// The agent this handle writes as.
    pub fn agent(&self) -> Option<&AgentId> {
        self.agent.as_ref()
    }

    /// The parent session, for a child session.
    pub fn parent(&self) -> Option<ConversationId> {
        self.parent
    }

    /// The root of this session's tree, for a child session.
    pub fn root(&self) -> Option<ConversationId> {
        self.root
    }

    /// The layout this session follows.
    pub fn config(&self) -> &SessionConfig {
        &self.config
    }

    /// The underlying [`ContextScope`], for a topic outside the session lane
    /// or an explicit [`ContextPolicy`].
    pub fn scope(&self) -> &ContextScope {
        &self.scope
    }

    /// Append one typed envelope to the session lane. The envelope must name
    /// this session as its conversation.
    pub async fn append(&self, envelope: AgentEnvelope) -> Result<AgdxReceipt, LaserError> {
        if ConversationId::from(envelope.conversation) != self.conversation() {
            return Err(LaserError::Invalid(
                "an appended envelope must belong to this session".to_owned(),
            ));
        }
        self.laser
            .agdx(
                AgentTopic::Sessions,
                envelope.source.clone(),
                envelope.conversation,
            )
            .with_lane_guard(Arc::clone(&self.lane_guard))
            .publish_envelope(envelope)
            .await
    }

    /// End the session as completed. State changed since the last snapshot is
    /// snapshotted first, so a reader can start from the final document.
    pub async fn end(&self) -> Result<(), LaserError> {
        self.state().snapshot_if_changed().await?;
        self.terminate(TaskState::Completed, SessionEnd::default())
            .await
    }

    /// End the session as failed with `error`.
    pub async fn fail(&self, error: AgentErrorBody) -> Result<(), LaserError> {
        self.terminate(
            TaskState::Failed,
            SessionEnd {
                reason: None,
                error: Some(error),
            },
        )
        .await
    }

    /// End the session as canceled.
    pub async fn cancel(&self) -> Result<(), LaserError> {
        self.terminate(TaskState::Canceled, SessionEnd::default())
            .await
    }

    /// Run `work` inside the session and end it by the outcome: completed on
    /// success, failed on an error or a panic. A panic is re-raised after the
    /// failure is written. When the terminal write also fails, the error from
    /// `work` is returned. A dropped future, `panic = "abort"`, an unawaited
    /// child task, or process death is not captured, and the session then
    /// shows idle once its heartbeat stops.
    pub async fn run<F, Fut, T>(self, lease: SessionLease, work: F) -> Result<T, LaserError>
    where
        F: FnOnce(Session) -> Fut,
        Fut: std::future::Future<Output = Result<T, LaserError>>,
    {
        let outcome = std::panic::AssertUnwindSafe(work(self.clone()))
            .catch_unwind()
            .await;
        let result = match outcome {
            Ok(Ok(value)) => self.end().await.map(|()| value),
            Ok(Err(error)) => {
                let _ = self.fail(error_body(error.to_string())).await;
                Err(error)
            }
            Err(panic) => {
                let mut body = error_body(panic_message(panic.as_ref()));
                body.detail = Some(BTreeMap::from([(
                    "panic".to_owned(),
                    laser_wire::query::Value::Bool(true),
                )]));
                let _ = self.fail(body).await;
                drop(lease);
                std::panic::resume_unwind(panic);
            }
        };
        drop(lease);
        result
    }

    // Mark a submitted session working as this handle's agent and take a lease.
    pub(crate) async fn pick_up(&self) -> Result<SessionLease, LaserError> {
        let generation = self.stream_generation().await?;
        let transition = laser_wire::agent::SessionTransition {
            actor: self.agent.clone(),
            acknowledges: None,
        };
        let lane = self.lane()?;
        let status = lane
            .status(OPERATION_SESSION)
            .with_task_state(TaskState::Working)
            .body(encode_named(&transition)?)
            .content_type(ContentType::Cbor);
        self.with_envelope_ancestry(status).send().await?;
        Ok(self.lease(generation, self.config.idle_timeout))
    }

    pub(crate) async fn terminate(
        &self,
        state: TaskState,
        end: SessionEnd,
    ) -> Result<(), LaserError> {
        let intent = {
            let mut latch = self.terminal.lock().expect("session terminal latch");
            match latch.as_ref() {
                Some(intent) if intent.state != state => {
                    return Err(LaserError::Invalid(format!(
                        "session already ending as {}",
                        intent.state
                    )));
                }
                Some(intent) => intent.clone(),
                None => {
                    let intent = TerminalIntent {
                        state,
                        end,
                        record: RecordId::mint(),
                    };
                    *latch = Some(intent.clone());
                    intent
                }
            }
        };
        let lane = self.lane()?;
        let status = lane
            .status(OPERATION_SESSION)
            .with_task_state(intent.state)
            .with_record(intent.record)
            .body(encode_named(&intent.end)?)
            .content_type(ContentType::Cbor)
            .last();
        #[cfg(feature = "sign")]
        let status = match &self.key {
            Some(key) => status.signed_by(key),
            None => status,
        };
        self.with_envelope_ancestry(status).send().await.map(|_| ())
    }

    pub(crate) fn lane(&self) -> Result<crate::agent::Agdx, LaserError> {
        let agent = self
            .agent
            .clone()
            .ok_or_else(|| LaserError::Invalid("this session handle has no agent".to_owned()))?;
        Ok(self
            .laser
            .agdx(AgentTopic::Sessions, agent, self.conversation().into())
            .with_lane_guard(Arc::clone(&self.lane_guard)))
    }

    fn with_envelope_ancestry<'a>(
        &self,
        send: crate::agent::AgdxSend<'a>,
    ) -> crate::agent::AgdxSend<'a> {
        send.with_ancestry(self.parent.map(Into::into), self.root.map(Into::into))
    }

    async fn stream_generation(&self) -> Result<u64, LaserError> {
        Ok(self
            .lane_guard
            .check(&self.laser, self.conversation())
            .await?
            .stream_generation)
    }

    fn lease(&self, generation: u64, idle_timeout: Duration) -> SessionLease {
        let interval = self
            .config
            .heartbeat
            .min(idle_timeout / 5)
            .max(Duration::from_millis(10));
        self.laser.lease_registry().acquire(
            &self.laser,
            LeaseKey {
                stream: self.stream().unwrap_or_default().to_owned(),
                stream_generation: generation,
                session: self.conversation(),
            },
            self.agent.clone().expect("a leased session has an agent"),
            interval,
        )
    }

    /// The model-ready context: the configured last records on the session
    /// lane, trimmed to the configured estimated token bound.
    pub async fn context(&self) -> Result<Vec<SessionTurn>, LaserError> {
        self.context_with(self.config.policy()).await
    }

    /// [`context`](Self::context) under an explicit policy.
    pub async fn context_with(
        &self,
        policy: Box<dyn ContextPolicy>,
    ) -> Result<Vec<SessionTurn>, LaserError> {
        let messages = self.scope.fetch_with(lane_topics(), policy).await?;
        Ok(turns_of(messages))
    }

    /// This session's memory in the configured namespace, scoped to the
    /// session.
    pub fn memory(&self) -> ScopedMemory {
        self.memory_in(self.config.memory_namespace.clone())
    }

    /// [`memory`](Self::memory) in an explicit namespace.
    pub fn memory_in(&self, namespace: impl Into<String>) -> ScopedMemory {
        self.scope.memory(namespace)
    }

    /// The knowledge graph `name`, the same graph [`Laser::graph`] returns.
    /// Feature `graph`.
    #[cfg(feature = "graph")]
    pub fn graph(&self, name: impl Into<String>) -> crate::graph::GraphHandle<'_> {
        self.scope.graph(name)
    }

    /// Where the session lane ends right now. Persist it (it serializes) and
    /// hand it to [`state_at`](Self::state_at) or [`replay`](Self::replay).
    pub async fn checkpoint(&self) -> Result<Checkpoint, LaserError> {
        self.scope.checkpoint(&lane_topics()).await
    }

    /// The records up to `checkpoint`.
    pub async fn turns_at(&self, checkpoint: Checkpoint) -> Result<Vec<SessionTurn>, LaserError> {
        self.turns(ReplayBound::At(checkpoint)).await
    }

    /// The records appended after `checkpoint`.
    pub async fn turns_since(
        &self,
        checkpoint: Checkpoint,
    ) -> Result<Vec<SessionTurn>, LaserError> {
        self.turns(ReplayBound::FromCheckpoint(checkpoint)).await
    }

    /// Fold the records up to `checkpoint` into `S`: state as it stood then.
    pub async fn state_at<S, F>(
        &self,
        checkpoint: Checkpoint,
        init: S,
        fold: F,
    ) -> Result<S, LaserError>
    where
        F: FnMut(S, &SessionTurn) -> S,
    {
        Ok(self.turns_at(checkpoint).await?.iter().fold(init, fold))
    }

    /// Fold the records appended after `checkpoint` into `S`: bring state
    /// saved at that checkpoint up to date.
    pub async fn replay<S, F>(
        &self,
        checkpoint: Checkpoint,
        init: S,
        fold: F,
    ) -> Result<S, LaserError>
    where
        F: FnMut(S, &SessionTurn) -> S,
    {
        Ok(self.turns_since(checkpoint).await?.iter().fold(init, fold))
    }

    async fn turns(&self, bound: ReplayBound) -> Result<Vec<SessionTurn>, LaserError> {
        let messages = self
            .scope
            .state(
                lane_topics(),
                bound,
                Vec::new(),
                |mut acc: Vec<ContextMessage>, message| {
                    acc.push(message.clone());
                    acc
                },
            )
            .await?;
        Ok(turns_of(messages))
    }
}

fn lane_topics() -> Vec<AgentTopic<'static>> {
    vec![AgentTopic::Sessions]
}

fn turns_of(messages: Vec<ContextMessage>) -> Vec<SessionTurn> {
    messages
        .into_iter()
        .map(|message| SessionTurn {
            display: match &message.envelope {
                Some(envelope) => display_type(
                    SessionRecord::Envelope(envelope),
                    laser_wire::topics::AGENT_SESSIONS,
                ),
                None => DisplayType::AgentMessage,
            },
            message,
        })
        .collect()
}

fn error_body(message: String) -> AgentErrorBody {
    AgentErrorBody {
        code: AgentErrorCode::Internal,
        message: Some(message),
        retryable: false,
        detail: None,
    }
}

fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|message| (*message).to_owned())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "the session work panicked".to_owned())
}

/// One record read back from a [`Session`]: the message off the log plus how
/// a timeline shows it.
#[derive(Debug, Clone)]
pub struct SessionTurn {
    pub display: DisplayType,
    pub message: ContextMessage,
}

impl SessionTurn {
    /// The record's body as UTF-8, lossy: the envelope body of a typed
    /// record, else the raw payload.
    pub fn text(&self) -> String {
        let body = match &self.message.envelope {
            Some(envelope) => &envelope.body,
            None => &self.message.payload,
        };
        String::from_utf8_lossy(body).into_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_per_user_policy_when_deriving_for_a_key_then_should_be_stable_and_distinct() {
        let policy = SessionPolicy::PerUser;
        assert_eq!(
            policy.conversation_for("alice"),
            policy.conversation_for("alice")
        );
        assert_ne!(
            policy.conversation_for("alice"),
            policy.conversation_for("bob")
        );
    }

    #[test]
    fn given_prelude_agent_ids_when_declaring_a_layout_then_should_match_the_wire_map() {
        let planner = crate::types::AgentId::new("planner").unwrap();
        let worker = crate::types::AgentId::new("worker").unwrap();
        let wire = |name: &str| name.parse::<AgentId>().unwrap();

        assert_eq!(
            SessionLayout::per_agent_topic([
                (planner.clone(), "planner.inbox"),
                (worker.clone(), "worker.inbox")
            ]),
            SessionLayout::PerAgentTopic(BTreeMap::from([
                (wire("planner"), "planner.inbox".to_owned()),
                (wire("worker"), "worker.inbox".to_owned()),
            ]))
        );
        assert_eq!(
            SessionLayout::per_agent_partition([(planner, 0), (worker, 1)]),
            SessionLayout::PerAgentPartition(BTreeMap::from([
                (wire("planner"), 0),
                (wire("worker"), 1)
            ]))
        );
    }

    #[test]
    fn given_per_call_policy_when_deriving_twice_then_should_be_unique() {
        let policy = SessionPolicy::PerCall;
        assert_ne!(policy.conversation_for("x"), policy.conversation_for("x"));
    }

    #[test]
    fn given_labels_namespaces_and_streams_when_derived_then_should_separate_each_dimension() {
        let base = derive_session_id("agents", "ops", "incident");
        assert_eq!(base, derive_session_id("agents", "ops", "incident"));
        assert_ne!(base, derive_session_id("other", "ops", "incident"));
        assert_ne!(base, derive_session_id("agents", "desk", "incident"));
        assert_ne!(base, derive_session_id("agents", "ops", "refund"));
        assert_ne!(
            derive_session_id("a", "bc", "d"),
            derive_session_id("ab", "c", "d")
        );
    }

    #[test]
    fn given_an_unbounded_retention_when_built_then_should_be_refused() {
        assert!(TopicRetention::new(IggyExpiry::NeverExpire, MaxTopicSize::Unlimited).is_err());
        assert!(TopicRetention::new(IggyExpiry::NeverExpire, MaxTopicSize::ServerDefault).is_err());
        assert!(
            TopicRetention::new(
                IggyExpiry::ExpireDuration(iggy::prelude::IggyDuration::from(86_400_000_000u64)),
                MaxTopicSize::Unlimited
            )
            .is_ok()
        );
    }

    #[test]
    fn given_the_defaults_when_read_then_should_match_the_documented_values() {
        let config = SessionConfig::default();
        assert_eq!(config.idle_timeout_value(), Duration::from_secs(300));
        assert_eq!(config.heartbeat_value(), Duration::from_secs(60));
        assert!(config.registers_source());
        assert!(!config.fails_on_dead_letter());
        assert_eq!(config.sdk_info().language, "rust");
    }
}
