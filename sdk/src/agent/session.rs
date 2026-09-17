use crate::agent::ReplayBound;
use crate::context::{Chain, Checkpoint, ContextMessage, ContextPolicy, LastN, TokenBudget};
use crate::context_scope::{ContextScope, ScopedMemory};
use crate::error::LaserError;
use crate::laser::Laser;
use crate::provenance::AgentTopic;
use crate::types::ConversationId;

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

/// The topics a [`Session`] reads and writes by default: the turn log an AI
/// agent session actually needs. Fixed on purpose -- reach for
/// [`Session::scope`] and `AgentTopic::Custom`/`ContextScope::fetch_with`
/// when a use case needs a topic outside this set.
pub const DEFAULT_SESSION_TOPICS: [AgentTopic<'static>; 4] = [
    AgentTopic::Commands,
    AgentTopic::ToolCalls,
    AgentTopic::ToolResults,
    AgentTopic::LlmIo,
];

/// The memory namespace a [`Session`] reads and writes by default.
/// Conversation scoping already isolates one session's memory from
/// another's, so sharing one namespace across sessions is deliberate, not a
/// leak -- use [`Session::memory_in`] for a different one.
pub const DEFAULT_SESSION_MEMORY_NAMESPACE: &str = "agent.session";

const DEFAULT_SESSION_CONTEXT_TURNS: usize = 50;
const DEFAULT_SESSION_CONTEXT_TOKENS: usize = 4000;

/// What kind of turn a [`Session::append`] is recording. Each kind rides the
/// existing [`AgentTopic`] it corresponds to, so a `Session`-recorded turn is
/// byte-identical to one written directly through
/// [`ContextScope::append`] and reads back with any existing topic reader
/// (`ContextScope::fetch`, `ConversationState::load`, a raw `Cursor`). This
/// introduces no new wire format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionEventKind {
    /// An instruction driving the agent: a user turn or a system directive.
    Instruction,
    /// A model completion.
    ModelResponse,
    /// An outbound tool invocation.
    ToolCall,
    /// A tool's result.
    ToolResult,
    /// A human-in-the-loop prompt or decision.
    HumanInput,
}

impl SessionEventKind {
    /// The topic this kind rides.
    #[must_use]
    pub const fn topic(self) -> AgentTopic<'static> {
        match self {
            Self::Instruction => AgentTopic::Commands,
            Self::ModelResponse => AgentTopic::LlmIo,
            Self::ToolCall => AgentTopic::ToolCalls,
            Self::ToolResult => AgentTopic::ToolResults,
            Self::HumanInput => AgentTopic::HumanInput,
        }
    }
}

impl Laser {
    /// The session accessor: AI-agent-session primitives built entirely from
    /// [`ContextScope`]/[`ConversationState`](crate::agent::ConversationState)/
    /// [`ScopedMemory`] -- typed turn append, a ready-to-feed context, scoped
    /// memory search, and point-in-time or incremental replay via
    /// [`Checkpoint`]. Free and synchronous like [`Laser::context`]; IO
    /// happens at the verbs.
    pub fn sessions(&self) -> Sessions {
        Sessions {
            laser: self.clone(),
        }
    }
}

/// The session factory. Build it with [`Laser::sessions`].
#[derive(Clone)]
pub struct Sessions {
    laser: Laser,
}

impl Sessions {
    /// The session for `id`: a conversation derived deterministically from
    /// `id` ([`SessionPolicy::PerUser`]), so `create` is idempotent -- the
    /// same `id` always reaches the same conversation, with everything
    /// already appended still there. There is no separate server-side
    /// "create" step: a session exists the moment anything is appended to
    /// it, the same log-native model every other primitive here follows.
    pub fn create(&self, id: impl AsRef<str>) -> Session {
        Session::new(
            &self.laser,
            SessionPolicy::PerUser.conversation_for(id.as_ref()),
        )
    }

    /// [`create`](Self::create), named for the call site that means to
    /// reattach to an existing session rather than start one. Identical
    /// behavior -- both resolve to the same deterministic conversation.
    pub fn resume(&self, id: impl AsRef<str>) -> Session {
        self.create(id)
    }

    /// A fresh, anonymous session with a random id. Read it back with
    /// [`Session::id`] to `resume` it later -- an anonymous session's id is
    /// not derivable from anything, so the caller must hold onto it (or use
    /// [`create`](Self::create) with a caller-chosen id instead).
    pub fn start(&self) -> Session {
        Session::new(&self.laser, ConversationId::new())
    }
}

/// One AI-agent session: a conversation viewed through the lens a
/// session-oriented agent runtime wants -- typed turns, a ready-to-feed
/// context, scoped memory, and point-in-time or incremental replay. Built
/// entirely from [`ContextScope`], [`ConversationState`](crate::agent::ConversationState),
/// and [`ScopedMemory`]: a `Session` writes and reads the exact same records
/// those primitives do, so nothing here is a second store, and code that
/// already speaks `ContextScope` against the same conversation id
/// interoperates for free. Build it with [`Laser::sessions`].
#[derive(Clone)]
pub struct Session {
    scope: ContextScope,
}

impl Session {
    fn new(laser: &Laser, conversation: ConversationId) -> Self {
        Self {
            scope: laser.context(conversation),
        }
    }

    /// This session's conversation id: the stable id `create`/`resume`
    /// derived, or the fresh one [`Sessions::start`] minted.
    pub fn id(&self) -> ConversationId {
        self.scope.conversation()
    }

    /// The underlying [`ContextScope`], for anything this facade does not
    /// cover: an `AgentTopic::Custom` topic, an explicit [`ContextPolicy`],
    /// or the knowledge graph.
    pub fn scope(&self) -> &ContextScope {
        &self.scope
    }

    /// Append one typed turn.
    pub async fn append(
        &self,
        kind: SessionEventKind,
        data: impl Into<Vec<u8>>,
    ) -> Result<(), LaserError> {
        self.scope.append(kind.topic(), data).await
    }

    /// This session's context, ready to feed a model: the default topic set,
    /// kept to the last 50 turns and trimmed to an estimated 4000-token
    /// budget. For a custom topic set or policy, use [`scope`](Self::scope)'s
    /// `fetch`/`fetch_with`.
    pub async fn context(&self) -> Result<Vec<ContextMessage>, LaserError> {
        self.context_with(Box::new(Chain(vec![
            Box::new(LastN(DEFAULT_SESSION_CONTEXT_TURNS)),
            Box::new(TokenBudget::new(DEFAULT_SESSION_CONTEXT_TOKENS)),
        ])))
        .await
    }

    /// [`context`](Self::context) under an explicit policy.
    pub async fn context_with(
        &self,
        policy: Box<dyn ContextPolicy>,
    ) -> Result<Vec<ContextMessage>, LaserError> {
        self.scope
            .fetch_with(DEFAULT_SESSION_TOPICS.to_vec(), policy)
            .await
    }

    /// This session's memory, pre-scoped to its conversation under the
    /// default session namespace ([`DEFAULT_SESSION_MEMORY_NAMESPACE`]). For
    /// a different namespace or backend, use [`memory_in`](Self::memory_in)
    /// or [`scope`](Self::scope)'s `memory_with`.
    pub fn memory(&self) -> ScopedMemory {
        self.scope.memory(DEFAULT_SESSION_MEMORY_NAMESPACE)
    }

    /// [`memory`](Self::memory) in an explicit namespace.
    pub fn memory_in(&self, namespace: impl Into<String>) -> ScopedMemory {
        self.scope.memory(namespace)
    }

    /// A point in this session's default topics, right now -- pass it to
    /// [`state_at`](Self::state_at) or [`replay`](Self::replay) later to
    /// fold up to, or resume from, exactly this point.
    pub async fn checkpoint(&self) -> Result<Checkpoint, LaserError> {
        self.scope.checkpoint(&DEFAULT_SESSION_TOPICS).await
    }

    /// Fold this session's default topics, up to and including `checkpoint`,
    /// into `S` -- the point-in-time counterpart to [`replay`](Self::replay).
    /// `init`/`fold` describe how a turn becomes state; accumulate into a
    /// `Vec<ContextMessage>` for the raw turn history, or inspect via
    /// [`scope`](Self::scope) directly for anything more custom.
    pub async fn state_at<S, F>(
        &self,
        checkpoint: Checkpoint,
        init: S,
        fold: F,
    ) -> Result<S, LaserError>
    where
        F: FnMut(S, &ContextMessage) -> S,
    {
        self.scope
            .state(
                DEFAULT_SESSION_TOPICS.to_vec(),
                ReplayBound::At(checkpoint),
                init,
                fold,
            )
            .await
    }

    /// Fold everything appended to this session's default topics at or after
    /// `checkpoint` into `S` -- the incremental counterpart to
    /// [`state_at`](Self::state_at).
    pub async fn replay<S, F>(
        &self,
        checkpoint: Checkpoint,
        init: S,
        fold: F,
    ) -> Result<S, LaserError>
    where
        F: FnMut(S, &ContextMessage) -> S,
    {
        self.scope
            .state(
                DEFAULT_SESSION_TOPICS.to_vec(),
                ReplayBound::FromCheckpoint(checkpoint),
                init,
                fold,
            )
            .await
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
    fn given_each_session_event_kind_when_mapped_then_should_ride_its_agent_topic() {
        // Pins the mapping every `Session::append` call relies on: change one
        // of these and a `Session`-recorded turn starts reading back from a
        // different topic than before, silently splitting existing history.
        assert_eq!(SessionEventKind::Instruction.topic(), AgentTopic::Commands);
        assert_eq!(SessionEventKind::ModelResponse.topic(), AgentTopic::LlmIo);
        assert_eq!(SessionEventKind::ToolCall.topic(), AgentTopic::ToolCalls);
        assert_eq!(
            SessionEventKind::ToolResult.topic(),
            AgentTopic::ToolResults
        );
        assert_eq!(SessionEventKind::HumanInput.topic(), AgentTopic::HumanInput);
    }

    #[test]
    fn given_the_default_session_topics_when_named_then_none_should_be_missing() {
        // `Checkpoint`/`ContextAssembler` key their per-topic bounds by
        // `AgentTopic::name()`; a `Custom` entry here would silently drop out
        // of every `Session::checkpoint`/`state_at`/`replay` bound.
        for topic in DEFAULT_SESSION_TOPICS {
            assert!(
                topic.name().is_some(),
                "every default session topic must have a stable name"
            );
        }
    }
}
