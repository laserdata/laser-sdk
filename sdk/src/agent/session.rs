use crate::agent::ReplayBound;
use crate::context::{Chain, Checkpoint, ContextMessage, ContextPolicy, LastN, TokenBudget};
use crate::context_scope::{ContextScope, ScopedMemory};
use crate::error::LaserError;
use crate::laser::Laser;
use crate::provenance::AgentTopic;
use crate::types::ConversationId;
use std::collections::BTreeMap;
use std::sync::Arc;
use strum::{Display, EnumString};

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

/// The default topics a [`Session`] reads and writes: every conversation-level
/// agent topic, one per [`SessionTurnKind`]. [`SessionConfig::topic`] moves a
/// kind onto another topic.
pub const DEFAULT_SESSION_TOPICS: [AgentTopic<'static>; 6] = [
    AgentTopic::Commands,
    AgentTopic::Responses,
    AgentTopic::LlmIo,
    AgentTopic::ToolCalls,
    AgentTopic::ToolResults,
    AgentTopic::HumanInput,
];

/// The default memory namespace of a [`Session`]. Conversation scoping keeps
/// one session's memory apart from another's, so one shared namespace is the
/// intended layout. [`SessionConfig::memory_namespace`] changes it.
pub const DEFAULT_SESSION_MEMORY_NAMESPACE: &str = "agent.session";

/// The default turn bound of [`Session::context`].
pub const DEFAULT_SESSION_CONTEXT_TURNS: usize = 50;

/// The default estimated token bound of [`Session::context`].
pub const DEFAULT_SESSION_CONTEXT_TOKENS: usize = 4000;

/// What a [`Session::append`] records. Each kind rides exactly one
/// conversation-level [`AgentTopic`], so a session turn is an ordinary agent
/// message: any topic reader sees it, and any agent message on those topics
/// reads back as a turn. The string form (`instruction`, `model.response`,
/// `tool.call`, ...) is shared with the Python and TypeScript SDKs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, EnumString)]
pub enum SessionTurnKind {
    /// An instruction driving the agent: a user turn or a system directive.
    #[strum(serialize = "instruction")]
    Instruction,
    /// The agent's reply to the conversation.
    #[strum(serialize = "response")]
    Response,
    /// A model completion, the raw model trace.
    #[strum(serialize = "model.response")]
    ModelResponse,
    /// An outbound tool invocation.
    #[strum(serialize = "tool.call")]
    ToolCall,
    /// A tool's result.
    #[strum(serialize = "tool.result")]
    ToolResult,
    /// A human-in-the-loop prompt or decision.
    #[strum(serialize = "human.input")]
    HumanInput,
}

impl SessionTurnKind {
    /// The topic this kind rides.
    #[must_use]
    pub const fn topic(self) -> AgentTopic<'static> {
        match self {
            Self::Instruction => AgentTopic::Commands,
            Self::Response => AgentTopic::Responses,
            Self::ModelResponse => AgentTopic::LlmIo,
            Self::ToolCall => AgentTopic::ToolCalls,
            Self::ToolResult => AgentTopic::ToolResults,
            Self::HumanInput => AgentTopic::HumanInput,
        }
    }

    /// The kind that rides the topic named `topic` under the default layout,
    /// or `None` for a topic outside [`DEFAULT_SESSION_TOPICS`].
    #[must_use]
    pub fn for_topic(topic: &str) -> Option<Self> {
        [
            Self::Instruction,
            Self::Response,
            Self::ModelResponse,
            Self::ToolCall,
            Self::ToolResult,
            Self::HumanInput,
        ]
        .into_iter()
        .find(|kind| kind.topic().name() == Some(topic))
    }
}

/// One turn read back from a [`Session`]: the message off the log plus the
/// kind its topic implies.
#[derive(Debug, Clone)]
pub struct SessionTurn {
    pub kind: SessionTurnKind,
    pub message: ContextMessage,
}

impl SessionTurn {
    /// The payload as UTF-8, lossy.
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.message.payload).into_owned()
    }
}

/// How a [`Sessions`] factory lays its sessions out on the log: the stream,
/// the topic each turn kind rides, the memory namespace, and the context
/// bounds. The defaults are the conversation-level agent topics on the
/// connection's default stream. A fleet of agents that must not share a
/// topic's partitions with another fleet points its kinds at its own topics,
/// or the whole factory at its own stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionConfig {
    stream: Option<String>,
    topics: BTreeMap<SessionTurnKind, AgentTopic<'static>>,
    memory_namespace: String,
    context_turns: usize,
    context_tokens: usize,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            stream: None,
            topics: [
                SessionTurnKind::Instruction,
                SessionTurnKind::Response,
                SessionTurnKind::ModelResponse,
                SessionTurnKind::ToolCall,
                SessionTurnKind::ToolResult,
                SessionTurnKind::HumanInput,
            ]
            .into_iter()
            .map(|kind| (kind, kind.topic()))
            .collect(),
            memory_namespace: DEFAULT_SESSION_MEMORY_NAMESPACE.to_owned(),
            context_turns: DEFAULT_SESSION_CONTEXT_TURNS,
            context_tokens: DEFAULT_SESSION_CONTEXT_TOKENS,
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

    /// Ride `kind` on `topic`, which may be any `AgentTopic::Custom`. A turn's
    /// kind is its topic, so every kind needs a topic of its own:
    /// [`Laser::sessions_with`] rejects a layout where two kinds share one.
    #[must_use]
    pub fn topic(mut self, kind: SessionTurnKind, topic: AgentTopic<'static>) -> Self {
        self.topics.insert(kind, topic);
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

    /// The stream sessions ride, or `None` for the connection's default stream.
    #[must_use]
    pub fn stream_name(&self) -> Option<&str> {
        self.stream.as_deref()
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

    /// The topic `kind` rides under this config.
    #[must_use]
    pub fn topic_for(&self, kind: SessionTurnKind) -> AgentTopic<'static> {
        self.topics
            .get(&kind)
            .cloned()
            .unwrap_or_else(|| kind.topic())
    }

    /// The kind that rides the topic named `topic` under this config.
    #[must_use]
    pub fn kind_for(&self, topic: &str) -> Option<SessionTurnKind> {
        self.topics
            .iter()
            .find(|(_, candidate)| candidate.topic_string() == topic)
            .map(|(kind, _)| *kind)
    }

    /// Every topic this config reads, in kind order.
    #[must_use]
    pub fn topics(&self) -> Vec<AgentTopic<'static>> {
        self.topics.values().cloned().collect()
    }

    fn validate(&self) -> Result<(), LaserError> {
        let mut seen: BTreeMap<String, SessionTurnKind> = BTreeMap::new();
        for (kind, topic) in &self.topics {
            if let Some(other) = seen.insert(topic.topic_string(), *kind) {
                return Err(LaserError::Invalid(format!(
                    "session turn kinds {other} and {kind} share topic {}, each kind needs its own topic",
                    topic.topic_string()
                )));
            }
        }
        Ok(())
    }

    fn policy(&self) -> Box<dyn ContextPolicy> {
        Box::new(Chain(vec![
            Box::new(LastN(self.context_turns)),
            Box::new(TokenBudget::new(self.context_tokens)),
        ]))
    }
}

impl Laser {
    /// The session accessor: one conversation seen the way an agent runtime
    /// wants it, as typed turns, a model-ready context, scoped memory, and
    /// checkpointed replay. Built on [`Laser::context`], so a session is never
    /// a second store. Free and synchronous, IO happens at the verbs. Uses
    /// [`SessionConfig::default`], see [`sessions_with`](Self::sessions_with).
    pub fn sessions(&self) -> Sessions {
        self.sessions_with(SessionConfig::default())
            .expect("the default session layout gives every kind its own topic")
    }

    /// [`sessions`](Self::sessions) under an explicit [`SessionConfig`]. Fails
    /// with [`LaserError::Invalid`] when two turn kinds share a topic.
    pub fn sessions_with(&self, config: SessionConfig) -> Result<Sessions, LaserError> {
        config.validate()?;
        let laser = match &config.stream {
            Some(stream) => self.with_default_stream(stream.clone()),
            None => self.clone(),
        };
        Ok(Sessions {
            laser,
            config: Arc::new(config),
        })
    }
}

/// The session factory. Build it with [`Laser::sessions`] or
/// [`Laser::sessions_with`].
#[derive(Clone)]
pub struct Sessions {
    laser: Laser,
    config: Arc<SessionConfig>,
}

impl Sessions {
    /// The durable session named `id`. The conversation derives from `id`
    /// ([`SessionPolicy::PerUser`]), so the same id always reaches the same
    /// history, and nothing is created on the server until a turn is appended.
    pub fn create(&self, id: impl AsRef<str>) -> Session {
        self.open(SessionPolicy::PerUser.conversation_for(id.as_ref()))
    }

    /// A fresh anonymous session. Keep [`Session::conversation`] to
    /// [`open`](Self::open) it again later.
    pub fn start(&self) -> Session {
        self.open(ConversationId::new())
    }

    /// The session over an existing `conversation`: one minted by
    /// [`start`](Self::start), carried by an inbound message's provenance, or
    /// spawned as a sub-conversation.
    pub fn open(&self, conversation: ConversationId) -> Session {
        Session {
            scope: self.laser.context(conversation),
            config: Arc::clone(&self.config),
        }
    }

    /// This factory's layout.
    pub fn config(&self) -> &SessionConfig {
        &self.config
    }
}

/// One agent session over a conversation. Turns are agent messages on the
/// configured topics, the context is a bounded assembly of them, memory is the
/// conversation's scoped memory, and a [`Checkpoint`] bounds point-in-time and
/// incremental replay. Build it with [`Laser::sessions`].
#[derive(Clone)]
pub struct Session {
    scope: ContextScope,
    config: Arc<SessionConfig>,
}

impl Session {
    /// This session's conversation id.
    pub fn conversation(&self) -> ConversationId {
        self.scope.conversation()
    }

    /// The layout this session follows.
    pub fn config(&self) -> &SessionConfig {
        &self.config
    }

    /// The underlying [`ContextScope`], for a topic outside the configured set,
    /// an explicit [`ContextPolicy`], or the knowledge graph.
    pub fn scope(&self) -> &ContextScope {
        &self.scope
    }

    /// Append one turn.
    pub async fn append(
        &self,
        kind: SessionTurnKind,
        data: impl Into<Vec<u8>>,
    ) -> Result<(), LaserError> {
        self.scope.append(self.config.topic_for(kind), data).await
    }

    /// The model-ready context: the configured last turns across the session's
    /// topics, trimmed to the configured estimated token bound.
    pub async fn context(&self) -> Result<Vec<SessionTurn>, LaserError> {
        self.context_with(self.config.policy()).await
    }

    /// [`context`](Self::context) under an explicit policy.
    pub async fn context_with(
        &self,
        policy: Box<dyn ContextPolicy>,
    ) -> Result<Vec<SessionTurn>, LaserError> {
        let messages = self.scope.fetch_with(self.config.topics(), policy).await?;
        Ok(self.turns_of(messages))
    }

    /// This session's memory in the configured namespace, scoped to the
    /// conversation.
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

    /// Where this session's topics end right now. Persist it (it serializes)
    /// and hand it to [`state_at`](Self::state_at) or [`replay`](Self::replay).
    pub async fn checkpoint(&self) -> Result<Checkpoint, LaserError> {
        self.scope.checkpoint(&self.config.topics()).await
    }

    /// The turns up to `checkpoint`.
    pub async fn turns_at(&self, checkpoint: Checkpoint) -> Result<Vec<SessionTurn>, LaserError> {
        self.turns(ReplayBound::At(checkpoint)).await
    }

    /// The turns appended after `checkpoint`.
    pub async fn turns_since(
        &self,
        checkpoint: Checkpoint,
    ) -> Result<Vec<SessionTurn>, LaserError> {
        self.turns(ReplayBound::FromCheckpoint(checkpoint)).await
    }

    /// Fold the turns up to `checkpoint` into `S`: state as it stood then.
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

    /// Fold the turns appended after `checkpoint` into `S`: bring state saved
    /// at that checkpoint up to date.
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
                self.config.topics(),
                bound,
                Vec::new(),
                |mut acc: Vec<ContextMessage>, message| {
                    acc.push(message.clone());
                    acc
                },
            )
            .await?;
        Ok(self.turns_of(messages))
    }

    fn turns_of(&self, messages: Vec<ContextMessage>) -> Vec<SessionTurn> {
        messages
            .into_iter()
            .filter_map(|message| {
                let kind = self.config.kind_for(&message.topic)?;
                Some(SessionTurn { kind, message })
            })
            .collect()
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
    fn given_per_call_policy_when_deriving_twice_then_should_be_unique() {
        let policy = SessionPolicy::PerCall;
        assert_ne!(policy.conversation_for("x"), policy.conversation_for("x"));
    }

    #[test]
    fn given_each_turn_kind_when_mapped_then_should_ride_its_topic_and_read_back() {
        for (kind, topic, name) in [
            (
                SessionTurnKind::Instruction,
                AgentTopic::Commands,
                "instruction",
            ),
            (SessionTurnKind::Response, AgentTopic::Responses, "response"),
            (
                SessionTurnKind::ModelResponse,
                AgentTopic::LlmIo,
                "model.response",
            ),
            (
                SessionTurnKind::ToolCall,
                AgentTopic::ToolCalls,
                "tool.call",
            ),
            (
                SessionTurnKind::ToolResult,
                AgentTopic::ToolResults,
                "tool.result",
            ),
            (
                SessionTurnKind::HumanInput,
                AgentTopic::HumanInput,
                "human.input",
            ),
        ] {
            assert_eq!(kind.topic(), topic);
            assert_eq!(kind.to_string(), name);
            assert_eq!(name.parse::<SessionTurnKind>(), Ok(kind));
            assert_eq!(
                SessionTurnKind::for_topic(topic.name().expect("named topic")),
                Some(kind)
            );
        }
        assert!("event".parse::<SessionTurnKind>().is_err());
        assert_eq!(SessionTurnKind::for_topic("agent.audit"), None);
    }

    #[test]
    fn given_a_custom_layout_when_a_kind_moves_topic_then_should_map_both_ways() {
        static INBOX: std::sync::LazyLock<iggy::prelude::Identifier> =
            std::sync::LazyLock::new(|| {
                iggy::prelude::Identifier::named("support.turns").expect("a valid identifier")
            });
        let config = SessionConfig::new()
            .stream("support")
            .topic(SessionTurnKind::Instruction, AgentTopic::Custom(&INBOX))
            .memory_namespace("support.sessions")
            .context_turns(10)
            .context_tokens(800);
        assert_eq!(
            config.topic_for(SessionTurnKind::Instruction),
            AgentTopic::Custom(&INBOX)
        );
        assert_eq!(
            config.topic_for(SessionTurnKind::ToolCall),
            AgentTopic::ToolCalls
        );
        assert_eq!(
            config.kind_for("support.turns"),
            Some(SessionTurnKind::Instruction)
        );
        assert_eq!(config.kind_for("agent.commands"), None);
        assert_eq!(config.stream_name(), Some("support"));
        assert_eq!(config.memory_namespace_name(), "support.sessions");
        assert_eq!(config.context_turn_bound(), 10);
        assert_eq!(config.context_token_bound(), 800);
        assert!(config.validate().is_ok());
        assert_eq!(
            SessionConfig::default().topics(),
            DEFAULT_SESSION_TOPICS.to_vec()
        );
    }

    #[test]
    fn given_two_kinds_on_one_topic_when_validated_then_should_be_rejected() {
        let shared = SessionConfig::new().topic(SessionTurnKind::Response, AgentTopic::Commands);
        assert!(matches!(shared.validate(), Err(LaserError::Invalid(_))));
    }

    #[test]
    fn given_the_default_session_topics_when_listed_then_should_be_exactly_the_turn_kinds() {
        let mut from_kinds: Vec<_> = [
            SessionTurnKind::Instruction,
            SessionTurnKind::Response,
            SessionTurnKind::ModelResponse,
            SessionTurnKind::ToolCall,
            SessionTurnKind::ToolResult,
            SessionTurnKind::HumanInput,
        ]
        .into_iter()
        .map(|kind| kind.topic().name().expect("named topic"))
        .collect();
        from_kinds.sort_unstable();
        let mut defaults: Vec<_> = DEFAULT_SESSION_TOPICS
            .iter()
            .map(|topic| topic.name().expect("named topic"))
            .collect();
        defaults.sort_unstable();
        assert_eq!(defaults, from_kinds);
    }
}
