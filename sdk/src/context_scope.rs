use crate::agent::{ConversationState, ReplayBound};
use crate::context::{Checkpoint, ContextAssembler, ContextMessage, ContextPolicy, LastN};
use crate::error::LaserError;
use crate::laser::Laser;
use crate::memory::{
    ConsolidationReport, Feedback, MemoryBackend, MemoryHandle, MemoryId, MemoryScope,
    RecallBuilder, RememberBuilder,
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
    /// `n` messages. `token_budget` trims the selected messages to an
    /// estimated token count after `n`, so the read is bounded by turns and by
    /// prompt size at once. The bound is required: an unbounded read of a
    /// long-lived conversation is a replay nobody asked for, so the full walk
    /// has its own deliberate spelling ([`fetch_with`](Self::fetch_with)).
    pub async fn fetch(
        &self,
        topics: Vec<AgentTopic<'static>>,
        n: usize,
        token_budget: Option<usize>,
    ) -> Result<Vec<ContextMessage>, LaserError> {
        self.fetch_with(topics, bounded_policy(n, token_budget))
            .await
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
    /// prompt-ready form (each payload as UTF-8, lossy). `token_budget` trims
    /// the selected messages to an estimated token count after `n`, so the
    /// block is bounded by turns and by prompt size at once.
    pub async fn block(
        &self,
        topics: Vec<AgentTopic<'static>>,
        n: usize,
        token_budget: Option<usize>,
    ) -> Result<String, LaserError> {
        let messages = self.fetch(topics, n, token_budget).await?;
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
            origin: None,
            producer: None,
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

    /// The current tail of `topics` as a [`Checkpoint`]: fold up to it with
    /// [`ReplayBound::At`] or resume after it with
    /// [`ReplayBound::FromCheckpoint`]. Offsets are per topic partition, not
    /// per conversation.
    pub async fn checkpoint(
        &self,
        topics: &[AgentTopic<'static>],
    ) -> Result<Checkpoint, LaserError> {
        crate::context::checkpoint(&self.laser, topics).await
    }

    /// This context's conversation id.
    pub fn conversation(&self) -> ConversationId {
        self.conversation
    }

    /// The `Laser` this scope reads and writes through.
    pub fn laser(&self) -> &Laser {
        &self.laser
    }
}

// The last `n` messages, trimmed to `token_budget` estimated tokens when set.
fn bounded_policy(n: usize, token_budget: Option<usize>) -> Box<dyn ContextPolicy> {
    match token_budget {
        Some(max_tokens) => Box::new(crate::context::Chain(vec![
            Box::new(LastN(n)),
            Box::new(crate::context::TokenBudget::new(max_tokens)),
        ])),
        None => Box::new(LastN(n)),
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
    origin: Option<laser_wire::graph::SourceRef>,
    producer: Option<laser_wire::graph::ProducerInfo>,
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
        let mut builder = self.handle.remember(payload).scope(self.conversation);
        if let Some(origin) = &self.origin {
            builder = builder.origin(origin.clone());
        }
        if let Some(producer) = &self.producer {
            builder = builder.producer(producer.clone());
        }
        builder
    }

    /// This scope with `origin` and `producer` stamped on every remembered item.
    #[must_use]
    pub fn with_lineage(
        mut self,
        origin: Option<laser_wire::graph::SourceRef>,
        producer: Option<laser_wire::graph::ProducerInfo>,
    ) -> Self {
        self.origin = origin;
        self.producer = producer;
        self
    }

    /// The one-call context altitude: recall this conversation's most relevant
    /// items rendered as one prompt-ready block under an optional token budget.
    pub async fn block(&self, token_budget: Option<usize>) -> Result<String, LaserError> {
        self.handle.context(self.conversation, token_budget).await
    }

    /// Keyword recall for `query` within this conversation, up to 50 items.
    /// It needs no [`Embedder`](crate::memory::Embedder), so it works on the
    /// default log-backed memory. Await it directly, or chain `.limit(n)` and
    /// `.folded()` first. Use [`recall`](Self::recall) for semantic or hybrid
    /// recall.
    pub fn search(&self, query: impl Into<String>) -> RecallBuilder<'_> {
        self.recall().keyword(query)
    }

    /// One consolidation pass over this conversation, keeping the newest
    /// `max_items` and pruning the rest.
    pub async fn consolidate(&self, max_items: usize) -> Result<ConsolidationReport, LaserError> {
        self.handle.consolidate(&self.scope(), max_items).await
    }

    /// [`consolidate`](Self::consolidate) with the summarize pass, see
    /// [`MemoryHandle::consolidate_with`].
    pub async fn consolidate_with(
        &self,
        max_items: usize,
        summarizer: impl crate::memory::Summarizer + Sync,
        prune_summarized: bool,
    ) -> Result<ConsolidationReport, LaserError> {
        self.handle
            .consolidate_with(&self.scope(), max_items, summarizer, prune_summarized)
            .await
    }

    /// Forget the item `id` if it was remembered in this conversation. A
    /// tombstone for an item of another conversation changes nothing.
    pub async fn forget(&self, id: MemoryId) -> Result<(), LaserError> {
        self.handle.forget(&self.scope(), id).await
    }

    /// Record `feedback` on the item it targets. Like [`forget`](Self::forget),
    /// it counts only when the item was remembered in this conversation.
    pub async fn improve(&self, feedback: Feedback) -> Result<MemoryId, LaserError> {
        self.handle.improve(&self.scope(), feedback).await
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

    /// The origin stamped on every remembered item, if any.
    pub fn origin(&self) -> Option<&laser_wire::graph::SourceRef> {
        self.origin.as_ref()
    }

    /// The producer stamped on every remembered item, if any.
    pub fn producer(&self) -> Option<&laser_wire::graph::ProducerInfo> {
        self.producer.as_ref()
    }

    fn scope(&self) -> MemoryScope {
        MemoryScope::builder()
            .conversation(self.conversation)
            .build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::Embedder;

    struct UnitEmbedder;

    impl Embedder for UnitEmbedder {
        async fn embed(&self, _text: &str) -> Result<Vec<f32>, LaserError> {
            Ok(vec![1.0])
        }
    }

    #[tokio::test]
    async fn given_an_item_of_another_conversation_when_forgotten_through_a_scope_then_should_keep_it()
     {
        let owner = ConversationId::new();
        let handle = MemoryHandle::vector(UnitEmbedder);
        let id = handle
            .remember("a fact")
            .scope(owner)
            .send()
            .await
            .expect("remember");
        let elsewhere = ScopedMemory {
            handle,
            conversation: ConversationId::new(),
            origin: None,
            producer: None,
        };
        elsewhere.forget(id).await.expect("forget by id");
        let left = elsewhere
            .handle()
            .recall(owner)
            .fetch()
            .await
            .expect("recall");
        assert_eq!(left.len(), 1, "another conversation cannot forget the item");
    }

    struct JoinSummarizer;

    impl crate::memory::Summarizer for JoinSummarizer {
        async fn summarize(&self, bodies: Vec<Vec<u8>>) -> Result<Vec<u8>, LaserError> {
            Ok(bodies.concat())
        }
    }

    fn scoped(handle: MemoryHandle, conversation: ConversationId) -> ScopedMemory {
        ScopedMemory {
            handle,
            conversation,
            origin: None,
            producer: None,
        }
    }

    #[tokio::test]
    async fn given_two_conversations_when_recalled_without_one_then_should_return_both() {
        let handle = MemoryHandle::vector(UnitEmbedder);
        for conversation in [ConversationId::new(), ConversationId::new()] {
            handle
                .remember("a fact")
                .scope(conversation)
                .send()
                .await
                .expect("remember");
        }
        let everywhere = handle
            .recall(None)
            .await
            .expect("recall every conversation");
        assert_eq!(everywhere.len(), 2);
    }

    #[tokio::test]
    async fn given_a_search_when_limited_then_should_cap_the_items() {
        let conversation = ConversationId::new();
        let memory = scoped(MemoryHandle::vector(UnitEmbedder), conversation);
        for body in ["deploy one", "deploy two", "deploy three"] {
            memory.remember(body).send().await.expect("remember");
        }
        assert_eq!(memory.search("deploy").await.expect("search").len(), 3);
        let capped = memory.search("deploy").limit(1).await.expect("search");
        assert_eq!(capped.len(), 1);
    }

    #[tokio::test]
    async fn given_message_turns_when_consolidated_with_a_summarizer_then_should_replace_them() {
        let conversation = ConversationId::new();
        let memory = scoped(MemoryHandle::vector(UnitEmbedder), conversation);
        for body in ["one ", "two"] {
            memory
                .remember(body)
                .kind(crate::memory::MemoryKind::Message)
                .send()
                .await
                .expect("remember");
        }
        let report = memory
            .consolidate_with(10, JoinSummarizer, true)
            .await
            .expect("consolidate");
        assert_eq!(report.summarized, 2);
        assert_eq!(report.pruned, 2);
    }
}
