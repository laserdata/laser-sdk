use crate::agent::{ConversationState, ReplayBound};
use crate::context::{ContextAssembler, ContextMessage, ContextPolicy, LastN};
use crate::error::LaserError;
use crate::laser::Laser;
use crate::memory::{
    ConsolidationReport, MemoryBackend, MemoryHandle, MemoryScope, RecallBuilder, RememberBuilder,
};
use crate::provenance::{AgentTopic, Provenance};
use crate::snapshot::SnapshotStore;
use crate::types::ConversationId;

impl Laser {
    /// The context accessor: the working record of one `conversation` on the
    /// log. Append rides an ordinary publish keyed by the conversation, reads
    /// ride the context assembler, so the context is never a second store.
    /// Free and synchronous, IO happens at the verbs.
    pub fn context(&self, conversation: ConversationId) -> ContextScope {
        ContextScope {
            laser: self.clone(),
            conversation,
        }
    }
}

/// One conversation's working context: append to it, read it back bounded, or
/// render the prompt-ready block. Build it with [`Laser::context`].
#[derive(Clone)]
pub struct ContextScope {
    laser: Laser,
    conversation: ConversationId,
}

impl ContextScope {
    /// Append `payload` to `topic` within this conversation. The provenance is
    /// pinned to the conversation, so a later [`fetch`](Self::fetch) reads it
    /// back in order.
    pub async fn append(
        &self,
        topic: AgentTopic<'_>,
        payload: impl Into<Vec<u8>>,
    ) -> Result<(), LaserError> {
        let provenance = Provenance::builder()
            .conversation_id(self.conversation)
            .build();
        self.laser.send_agent(topic, payload, &provenance).await
    }

    /// Read this conversation's history from `topics`, bounded to the last
    /// `n` messages. The bound is required: an unbounded read of a long-lived
    /// conversation is a replay nobody asked for, so the full walk has its own
    /// deliberate spelling ([`fetch_with`](Self::fetch_with)).
    pub async fn fetch(
        &self,
        topics: Vec<AgentTopic<'static>>,
        n: usize,
    ) -> Result<Vec<ContextMessage>, LaserError> {
        self.fetch_with(topics, Box::new(LastN(n))).await
    }

    /// Read this conversation's history from `topics` under an explicit
    /// [`ContextPolicy`], the deep form behind [`fetch`](Self::fetch).
    pub async fn fetch_with(
        &self,
        topics: Vec<AgentTopic<'static>>,
        policy: Box<dyn ContextPolicy>,
    ) -> Result<Vec<ContextMessage>, LaserError> {
        ContextAssembler::builder()
            .conversation_id(self.conversation)
            .topics(topics)
            .policy(policy)
            .build()
            .assemble(&self.laser)
            .await
    }

    /// The last `n` messages rendered as one newline-joined text block, the
    /// prompt-ready form (each payload as UTF-8, lossy).
    pub async fn block(
        &self,
        topics: Vec<AgentTopic<'static>>,
        n: usize,
    ) -> Result<String, LaserError> {
        let messages = self.fetch(topics, n).await?;
        Ok(messages
            .iter()
            .map(|message| String::from_utf8_lossy(&message.payload))
            .collect::<Vec<_>>()
            .join("\n"))
    }

    /// Rebuild in-memory state by folding this conversation's log under the
    /// explicit `bound` (the [`ReplayBound`] vocabulary: offsets, last-n, or
    /// the deliberately spelled full walk).
    pub async fn state<S, F>(
        &self,
        topics: Vec<AgentTopic<'static>>,
        bound: ReplayBound,
        init: S,
        fold: F,
    ) -> Result<S, LaserError>
    where
        F: FnMut(S, &ContextMessage) -> S,
    {
        ConversationState::load(&self.laser, self.conversation, topics, bound, init, fold).await
    }

    /// Like [`state`](Self::state) but seeded through a [`SnapshotStore`]:
    /// the newest snapshot's state (JSON-decoded into `S`) plus a replay of
    /// only the tail past it.
    pub async fn state_with<Store, S, F>(
        &self,
        store: &Store,
        topics: Vec<AgentTopic<'static>>,
        init: S,
        fold: F,
    ) -> Result<S, LaserError>
    where
        Store: SnapshotStore + Sync,
        S: serde::de::DeserializeOwned,
        F: FnMut(S, &ContextMessage) -> S,
    {
        ConversationState::load_with(&self.laser, store, self.conversation, topics, init, fold)
            .await
    }

    /// This conversation's memory in `namespace`, pre-scoped so recall and
    /// remember never repeat the conversation id: the same session the
    /// messages belong to, seen as memory. Durable facts and the knowledge
    /// graph stay deliberately cross-conversation (a fact learned in one
    /// session is worth recalling in the next), so they are reached through
    /// [`Laser::memory`]/[`Laser::graph`] and not narrowed here.
    pub fn memory(&self, namespace: impl Into<String>) -> ScopedMemory {
        self.memory_with(namespace, MemoryBackend::Auto)
    }

    /// [`memory`](Self::memory) on an explicit [`MemoryBackend`].
    pub fn memory_with(
        &self,
        namespace: impl Into<String>,
        backend: MemoryBackend,
    ) -> ScopedMemory {
        ScopedMemory {
            handle: self.laser.memory_with(namespace, backend),
            conversation: self.conversation,
        }
    }

    /// The knowledge graph `name`, reached from this scope for the common flow
    /// where a task streams messages, keeps session memory, and resolves the
    /// dependencies between them. The graph is returned unnarrowed on purpose:
    /// a dependency or knowledge graph is shared across conversations (a
    /// service-to-component edge holds no matter which task asked), so scoping
    /// it to one conversation would hide the very relationships the caller
    /// wants. Identical to [`Laser::graph`], offered here so one scope reaches
    /// every primitive. Feature `graph`.
    #[cfg(feature = "graph")]
    pub fn graph(&self, name: impl Into<String>) -> crate::graph::GraphHandle<'_> {
        self.laser.graph(name)
    }

    /// This context's conversation id.
    pub fn conversation(&self) -> ConversationId {
        self.conversation
    }
}

/// One conversation's memory: a [`MemoryHandle`] with the conversation already
/// applied, so [`recall`](Self::recall) and [`remember`](Self::remember) take
/// no conversation argument. Build it with [`ContextScope::memory`]. The
/// underlying handle stays reachable through [`handle`](Self::handle) for the
/// cross-conversation verbs (durable remember, graph writes).
pub struct ScopedMemory {
    handle: MemoryHandle,
    conversation: ConversationId,
}

impl ScopedMemory {
    /// Recall within this conversation. Chain `.semantic`/`.keyword`/`.limit`
    /// and finish with `.fetch().await`, exactly like the unscoped builder
    /// minus the conversation argument.
    pub fn recall(&self) -> RecallBuilder<'_> {
        self.handle.recall(self.conversation)
    }

    /// Remember `payload` in this conversation's session scope. Chain `.kind`
    /// /`.dedup` and finish with `.send().await`.
    pub fn remember(&self, payload: impl Into<Vec<u8>>) -> RememberBuilder<'_> {
        self.handle.remember(payload).scope(self.conversation)
    }

    /// The one-call context altitude: recall this conversation's most relevant
    /// items rendered as one prompt-ready block under an optional token budget.
    pub async fn block(&self, token_budget: Option<usize>) -> Result<String, LaserError> {
        self.handle.context(self.conversation, token_budget).await
    }

    /// One consolidation pass over this conversation, keeping the most relevant
    /// `max_items`.
    pub async fn consolidate(&self, max_items: usize) -> Result<ConsolidationReport, LaserError> {
        let scope = MemoryScope::builder()
            .conversation(self.conversation)
            .build();
        self.handle.consolidate(&scope, max_items).await
    }

    /// The underlying handle, for the cross-conversation verbs this scoped face
    /// does not narrow.
    pub fn handle(&self) -> &MemoryHandle {
        &self.handle
    }

    /// This scoped memory's conversation id.
    pub fn conversation(&self) -> ConversationId {
        self.conversation
    }
}
