use crate::agent::Laser;
use crate::error::LaserError;
use crate::provenance::{AgentTopic, Provenance};
use crate::types::{AgentId, ConversationId, IdError};
use iggy::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt::{self, Display, Formatter};
use std::str::FromStr;
use tokio::sync::Mutex;
use ulid::Ulid;

/// What a remembered item is. Agentic memory is an SDK layer over the streaming,
/// query, and graph primitives, so this is an SDK type, not a wire op field.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryKind {
    /// A standalone fact (the default).
    #[default]
    Fact,
    /// A conversation turn.
    Message,
    /// A summary distilled from other items.
    Summary,
    /// An extracted entity, the body of a graph node.
    Entity,
    /// A feedback signal that reweights recall.
    Feedback,
}

impl MemoryKind {
    /// A stable one-byte discriminator, mixed into a content-addressed id so the
    /// same body under different kinds gets distinct ids.
    pub const fn code(self) -> u8 {
        match self {
            MemoryKind::Fact => 1,
            MemoryKind::Message => 2,
            MemoryKind::Summary => 3,
            MemoryKind::Entity => 4,
            MemoryKind::Feedback => 5,
        }
    }
}

/// How long a memory lives. `Session` is conversation-scoped and prunable, and
/// `Durable` is shared across conversations and feeds the knowledge graph.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Lifetime {
    /// Conversation-scoped, prunable. The default.
    #[default]
    Session,
    /// Shared across conversations, graph-backed.
    Durable,
}

/// How recall finds items. `Auto` lets the backend route from the available
/// state (an embedder ranks semantically, a graph traverses, otherwise recency).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecallStrategy {
    /// Route to the best available strategy. The default.
    #[default]
    Auto,
    /// Most recent items first.
    Recent,
    /// Rank by semantic similarity to the query.
    Semantic,
    /// Traverse the knowledge graph from the query seed.
    Graph,
    /// Items inside the time range, most recent first.
    Temporal,
    /// Blend semantic and graph.
    Hybrid,
}

// Per-partition poll size when `LogMemory` catches its projection up to the tail.
const READ_BATCH: u32 = 1000;

/// A memory item's id (a ULID, so it sorts by creation time).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MemoryId(Ulid);

impl MemoryId {
    /// A fresh, random memory id.
    pub fn new() -> Self {
        Self(Ulid::new())
    }

    /// The raw 128-bit value.
    pub fn as_u128(self) -> u128 {
        self.0.0
    }

    /// Wrap a raw 128-bit value.
    pub fn from_u128(value: u128) -> Self {
        Self(Ulid(value))
    }

    /// A deterministic, content-addressed id: the same `owner`, `kind`, and
    /// `body` always produce the same id, so a deduped remember stores one item.
    /// The owner is the durable scope (stream and agent, never the conversation),
    /// so the same fact in two conversations is one durable memory. The hash is
    /// the wire crate's one canonical [`content_id`](laser_wire::hashing::content_id),
    /// so every SDK reproduces it from the same byte segments.
    pub fn content(owner: &MemoryScope, kind: MemoryKind, body: &[u8]) -> Self {
        let stream = owner.stream.as_deref().unwrap_or("").as_bytes();
        let agent = owner.agent.as_ref().map(AgentId::as_str).unwrap_or("");
        let kind = [kind.code()];
        let segments: [&[u8]; 5] = [stream, &[0], agent.as_bytes(), &[0], &kind];
        // The body is the last segment, appended so the segment order matches the
        // pinned cross-SDK vector (owner, separators, kind byte, body).
        let mut all = segments.to_vec();
        all.push(body);
        Self::from_u128(laser_wire::hashing::content_id(&all))
    }
}

impl Serialize for MemoryId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for MemoryId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

impl Default for MemoryId {
    fn default() -> Self {
        Self::new()
    }
}

impl Display for MemoryId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl FromStr for MemoryId {
    type Err = IdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ulid::from_string(s)
            .map(Self)
            .map_err(|_| IdError::InvalidUlid(s.to_owned()))
    }
}

/// Scopes memory to a stream, an agent, and a conversation, with a lifetime
/// tier. Any field left unset widens recall to match across that dimension.
#[derive(Debug, Clone, Default, bon::Builder)]
pub struct MemoryScope {
    /// Restrict to this stream (none = any). The widest scope tier.
    pub stream: Option<String>,
    /// Restrict to this agent (none = any).
    pub agent: Option<AgentId>,
    /// Restrict to this conversation (none = any).
    pub conversation: Option<ConversationId>,
    /// Whether the item is conversation-scoped and prunable (`Session`, the
    /// default) or shared across conversations and graph-backed (`Durable`).
    #[builder(default)]
    pub lifetime: Lifetime,
}

/// A remembered item: its id, payload, provenance, kind, and recall score.
#[derive(Debug, Clone)]
pub struct MemoryItem {
    /// The item's id.
    pub id: MemoryId,
    /// The remembered body. Owned `Vec<u8>` so the public API never leaks the
    /// `bytes` crate.
    pub payload: Vec<u8>,
    /// The scope it was stored under.
    pub provenance: Provenance,
    /// What the item is. `Fact` unless set on remember.
    pub kind: MemoryKind,
    /// The recall score, set by a ranking strategy (e.g. semantic similarity),
    /// `None` for an unranked recall.
    pub score: Option<f32>,
}

impl MemoryItem {
    // Build an item with the default kind and no score, the common case for a
    // backend that does not rank or type its items.
    fn plain(id: MemoryId, payload: Vec<u8>, provenance: Provenance) -> Self {
        Self {
            id,
            payload,
            provenance,
            kind: MemoryKind::Fact,
            score: None,
        }
    }
}

/// How to recall: a result limit, an optional agent filter, and an optional semantic query.
#[derive(Debug, Clone, bon::Builder)]
pub struct MemoryQuery {
    /// Max items to return.
    #[builder(default = 50)]
    pub limit: usize,
    pub agent: Option<AgentId>,
    /// A semantic query. Backends that embed (e.g. `VectorMemory`) rank results
    /// by similarity to this text. The log-backed default ignores it.
    pub semantic: Option<String>,
    /// How recall finds items. `Auto` (the default) lets the backend route from
    /// the available state: a backend with an embedder and a `semantic` query
    /// ranks by similarity, otherwise it returns the most recent.
    #[builder(default)]
    pub strategy: RecallStrategy,
}

/// A feedback signal on a recalled item: which item, and how to reweight it. A
/// positive `weight` promotes the item in future recall, a negative one demotes
/// it.
#[derive(Debug, Clone)]
pub struct Feedback {
    /// The item the feedback is about.
    pub target: MemoryId,
    /// The reweighting, positive to promote, negative to demote.
    pub weight: f32,
    /// An optional human note.
    pub note: Option<String>,
}

impl Feedback {
    /// Feedback that promotes (`weight > 0`) or demotes (`weight < 0`) `target`.
    pub fn new(target: MemoryId, weight: f32) -> Self {
        Self {
            target,
            weight,
            note: None,
        }
    }
}

/// Agent memory: `remember` / `recall` / `improve` / `forget`. (`Memory` is the
/// `Send` variant.)
#[trait_variant::make(Memory: Send)]
pub trait LocalMemory {
    /// Append `payload` to the memory under `scope`. The body is opaque bytes,
    /// so encode it with whatever codec you like before calling.
    async fn remember(&self, scope: &MemoryScope, payload: Vec<u8>)
    -> Result<MemoryId, LaserError>;
    async fn recall(
        &self,
        scope: &MemoryScope,
        query: &MemoryQuery,
    ) -> Result<Vec<MemoryItem>, LaserError>;
    /// Record `feedback` on a recalled item, the log-native signal a ranking
    /// backend folds into future recall. Returns the feedback record's id.
    async fn improve(
        &self,
        scope: &MemoryScope,
        feedback: Feedback,
    ) -> Result<MemoryId, LaserError>;
    async fn forget(&self, scope: &MemoryScope, id: MemoryId) -> Result<(), LaserError>;
}

/// One record on the log-backed memory topic. A typed, CBOR-encoded entry rather
/// than a magic-prefixed payload: the fold matches on the variant, so the control
/// records (forget, feedback) cannot collide with an item body.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum MemoryLogEntry {
    /// A remembered item: its id, kind, and body.
    Item {
        id: MemoryId,
        kind: MemoryKind,
        body: Vec<u8>,
    },
    /// A tombstone removing an item from recall.
    Forget { target: MemoryId },
    /// A feedback signal reweighting an item's recall rank.
    Feedback { target: MemoryId, weight: f32 },
}

impl MemoryLogEntry {
    fn encode(&self) -> Result<Vec<u8>, LaserError> {
        laser_wire::framing::encode_named(self)
            .map_err(|error| LaserError::Codec(format!("encode memory entry: {error}")))
    }
}

/// `Memory` over the append-only Iggy log (`AgentTopic::Audit`). `remember`
/// appends, `forget` appends a tombstone, `recall` returns the folded items for a
/// conversation (most recent first by `limit`), applying tombstones and the agent
/// filter. The log is the source of truth.
///
/// Recall is **incremental**: `LogMemory` keeps a folded projection plus a
/// per-partition cursor, and each `recall` drains only what was appended since the
/// last one (the same partition-drain the [`Cursor`](crate::cursor::Cursor) is built
/// on), with no rescan from offset 0. A fresh instance rebuilds the projection
/// from the log on its first recall. There is no side checkpoint to keep
/// because the log already holds the truth (contrast [`KvMemory`], whose store
/// is the truth).
pub struct LogMemory {
    laser: Laser,
    projection: Mutex<Projection>,
}

// The folded view `recall` reads: every memory item seen so far (in arrival order),
// the tombstoned ids, the accumulated feedback weight per item, and the
// per-partition offset the fold has consumed up to.
#[derive(Default)]
struct Projection {
    items: Vec<MemoryItem>,
    forgotten: HashSet<MemoryId>,
    feedback: std::collections::HashMap<MemoryId, f32>,
    offsets: Vec<u64>,
}

impl Projection {
    // Fold one audit message in by decoding its typed entry: a `Forget` records a
    // tombstone, a `Feedback` accumulates a weight on its target, an `Item` becomes
    // a recalled item. A payload that is not a memory entry is ignored. Pure, so it
    // is unit-tested.
    fn absorb(&mut self, payload: &[u8], provenance: Provenance) {
        let Ok(entry) = laser_wire::framing::decode_named::<MemoryLogEntry>(payload) else {
            return;
        };
        match entry {
            MemoryLogEntry::Forget { target } => {
                // Drop the tombstoned item so its body is not pinned for the life of
                // the projection. Keep the id so a tombstone that arrives before its
                // item (a cross-partition reorder) still suppresses it on arrival.
                self.items.retain(|item| item.id != target);
                self.forgotten.insert(target);
            }
            MemoryLogEntry::Feedback { target, weight } => {
                *self.feedback.entry(target).or_insert(0.0) += weight;
            }
            MemoryLogEntry::Item { id, kind, body } => {
                if self.forgotten.contains(&id) {
                    return;
                }
                // Dedup on fold: a content-addressed id appended twice (a deduped
                // remember of the same body) is one item.
                if self.items.iter().any(|item| item.id == id) {
                    return;
                }
                self.items.push(MemoryItem {
                    id,
                    payload: body,
                    provenance,
                    kind,
                    score: None,
                });
            }
        }
    }
}

impl LogMemory {
    /// A log-backed memory over an owned `Laser` (cheap to clone, shares the one
    /// connection). Hold a single instance to keep recall incremental: it folds
    /// only what was appended since the last recall, never rescanning from offset
    /// zero. A fresh instance rebuilds the projection from the log on its first
    /// recall, so reuse one instance for an agent that recalls repeatedly.
    pub fn new(laser: Laser) -> Self {
        Self {
            laser,
            projection: Mutex::new(Projection::default()),
        }
    }

    fn provenance(scope: &MemoryScope, idempotency_key: String) -> Provenance {
        let mut provenance = Provenance::builder()
            .conversation_id(scope.conversation.unwrap_or_default())
            .build();
        provenance.agent = scope.agent.clone();
        provenance.idempotency_key = Some(idempotency_key);
        provenance
    }

    // Append an item under an explicit id and kind. The facade passes a
    // content-addressed id for a deduped remember (the fold drops a repeat of an
    // id it already holds) and a fresh id otherwise.
    pub(crate) async fn append(
        &self,
        scope: &MemoryScope,
        id: MemoryId,
        kind: MemoryKind,
        payload: Vec<u8>,
    ) -> Result<MemoryId, LaserError> {
        let entry = MemoryLogEntry::Item {
            id,
            kind,
            body: payload,
        };
        let provenance = Self::provenance(scope, id.to_string());
        self.laser
            .send_agent(AgentTopic::Audit, entry.encode()?, &provenance)
            .await?;
        Ok(id)
    }

    // Drain the audit topic from the projection's saved offsets to the current tail,
    // folding only the new messages. Reads incrementally, not from offset 0.
    //
    // The network polling runs WITHOUT the projection lock held, so a round-trip
    // never blocks a concurrent recall. The lock is taken only to snapshot the
    // frontier and, afterwards, to fold + advance (a CPU-bound critical section). A
    // partition's batch is applied only if no concurrent `catch_up` advanced that
    // partition past our snapshot in the meantime - otherwise the other caller
    // already folded those messages and re-applying would duplicate items.
    async fn catch_up(&self) -> Result<(), LaserError> {
        let stream = Identifier::named(self.laser.stream_required()?)?;
        let topic = AgentTopic::Audit.as_identifier();
        let Some(details) = self.laser.client().get_topic(&stream, &topic).await? else {
            return Ok(());
        };
        let partitions = details.partitions_count;
        let consumer = Consumer::new(Identifier::named("laser-log-memory")?);

        let from = {
            let mut projection = self.projection.lock().await;
            if projection.offsets.len() < partitions as usize {
                projection.offsets.resize(partitions as usize, 0);
            }
            projection.offsets.clone()
        };

        let mut drained: Vec<(u32, u64, Vec<IggyMessage>)> = Vec::new();
        for partition in 0..partitions {
            let batch = crate::poll::drain_partition(
                self.laser.client(),
                &stream,
                &topic,
                &consumer,
                partition,
                from[partition as usize],
                READ_BATCH,
            )
            .await?;
            if !batch.messages.is_empty() {
                drained.push((partition, batch.next_offset, batch.messages));
            }
        }
        if drained.is_empty() {
            return Ok(());
        }

        let mut projection = self.projection.lock().await;
        if projection.offsets.len() < partitions as usize {
            projection.offsets.resize(partitions as usize, 0);
        }
        for (partition, next_offset, messages) in drained {
            if projection.offsets[partition as usize] != from[partition as usize] {
                continue; // a concurrent catch_up already folded this partition
            }
            for message in messages {
                let Ok(provenance) = Provenance::try_from(&message) else {
                    continue;
                };
                projection.absorb(&message.payload, provenance);
            }
            projection.offsets[partition as usize] = next_offset;
        }
        Ok(())
    }
}

impl Memory for LogMemory {
    async fn remember(
        &self,
        scope: &MemoryScope,
        payload: Vec<u8>,
    ) -> Result<MemoryId, LaserError> {
        self.append(scope, MemoryId::new(), MemoryKind::Fact, payload)
            .await
    }

    async fn recall(
        &self,
        scope: &MemoryScope,
        query: &MemoryQuery,
    ) -> Result<Vec<MemoryItem>, LaserError> {
        let Some(conversation) = scope.conversation else {
            return Ok(Vec::new());
        };
        self.catch_up().await?;
        let projection = self.projection.lock().await;
        // `items` is in per-partition arrival order, not globally timestamp-sorted, so
        // `last limit` is the most-recent N only because all of one conversation's
        // messages land on one partition (the audit topic partitions by
        // `conversation_id`, see `Provenance::partition_key`) and are folded in offset
        // order. Filtering to a single conversation first makes the tail correct.
        // The recall filter honors the scope it was stored under, and an explicit
        // `query.agent` narrows it further, overriding `scope.agent`.
        let agent_filter = query.agent.as_ref().or(scope.agent.as_ref());
        let mut items: Vec<MemoryItem> = projection
            .items
            .iter()
            .filter(|item| item.provenance.conversation_id == conversation)
            .filter(|item| !projection.forgotten.contains(&item.id))
            .filter(|item| {
                agent_filter.is_none_or(|agent| item.provenance.agent.as_ref() == Some(agent))
            })
            .cloned()
            .collect();
        // Apply any accumulated feedback as a score. When feedback exists, sort
        // promoted items to the front (stable, so recency breaks ties), the
        // log-native re-rank `improve` feeds. Without feedback the order stays the
        // most-recent tail, matching the pre-feedback behavior.
        if !projection.feedback.is_empty() {
            for item in &mut items {
                if let Some(weight) = projection.feedback.get(&item.id) {
                    item.score = Some(*weight);
                }
            }
            items.sort_by(|a, b| {
                let a = a.score.unwrap_or(0.0);
                let b = b.score.unwrap_or(0.0);
                b.total_cmp(&a)
            });
            items.truncate(query.limit);
        } else if items.len() > query.limit {
            items = items.split_off(items.len() - query.limit);
        }
        Ok(items)
    }

    async fn improve(
        &self,
        scope: &MemoryScope,
        feedback: Feedback,
    ) -> Result<MemoryId, LaserError> {
        // Append a feedback record. The fold (`Projection::absorb`) accumulates the
        // weight on its target and keeps it out of the recalled items, so recall
        // ranks by the accumulated feedback.
        let id = MemoryId::new();
        let entry = MemoryLogEntry::Feedback {
            target: feedback.target,
            weight: feedback.weight,
        };
        let provenance = Self::provenance(scope, id.to_string());
        self.laser
            .send_agent(AgentTopic::Audit, entry.encode()?, &provenance)
            .await?;
        Ok(id)
    }

    async fn forget(&self, scope: &MemoryScope, id: MemoryId) -> Result<(), LaserError> {
        let entry = MemoryLogEntry::Forget { target: id };
        let provenance = Self::provenance(scope, MemoryId::new().to_string());
        self.laser
            .send_agent(AgentTopic::Audit, entry.encode()?, &provenance)
            .await?;
        Ok(())
    }
}

/// Turns text into an embedding vector. The SDK stays model-agnostic: the
/// implementation (an API call, a local model) lives in application code, the
/// same boundary as the `LlmClient` seam in the examples.
#[trait_variant::make(Embedder: Send)]
pub trait LocalEmbedder {
    async fn embed(&self, text: &str) -> Result<Vec<f32>, LaserError>;
}

/// A `Memory` backend that rides the query layer: `remember` publishes the
/// payload and embedding as a record on `topic`, `recall` runs a vector query
/// against the materialized index. Production-friendly: it inherits
/// LaserData Cloud's durability (records survive worker restarts because the
/// Iggy log is the source of truth), and it scales to whatever the projector
/// backend supports, from local test workers to clustered LaserData Cloud stores.
///
/// Requires the `query` feature, and the caller must have advertised
/// `Capabilities::managed_query` on the `Laser` it hands in. Use [`VectorMemory`]
/// for in-process tests with no streaming infrastructure, or [`LogMemory`] for the simple
/// append-only path that pre-dates the query layer.
#[cfg(feature = "query")]
pub struct QueryMemory<'a, E> {
    laser: &'a Laser,
    embedder: E,
    topic: String,
}

#[cfg(feature = "query")]
#[derive(serde::Serialize, serde::Deserialize)]
struct MemoryDoc {
    id: String,
    // Stored as the JSON array of bytes so any payload (text or binary) survives
    // round-trip - small overhead but exact, matching `VectorMemory`'s semantics.
    payload: Vec<u8>,
    // The projector extracts this via the `VECTOR_FIELD` convention so `.nearest`
    // can rank rows without re-embedding at recall time.
    embedding: Vec<f32>,
}

#[cfg(feature = "query")]
impl<'a, E> QueryMemory<'a, E> {
    /// A LaserData-Cloud-backed memory: publishes to `topic`, embeds with `embedder`.
    pub fn new(laser: &'a Laser, embedder: E, topic: impl Into<String>) -> Self {
        Self {
            laser,
            embedder,
            topic: topic.into(),
        }
    }
}

#[cfg(feature = "query")]
impl<E: Embedder + Sync> Memory for QueryMemory<'_, E> {
    async fn remember(
        &self,
        scope: &MemoryScope,
        payload: Vec<u8>,
    ) -> Result<MemoryId, LaserError> {
        let id = MemoryId::new();
        let embedding = self
            .embedder
            .embed(&String::from_utf8_lossy(&payload))
            .await?;
        let doc = MemoryDoc {
            id: id.to_string(),
            payload: payload.to_vec(),
            embedding,
        };
        let mut publish = self
            .laser
            .publish(&self.topic)
            .index("memory_id", id.to_string())
            .inline_payload();
        if let Some(conversation) = scope.conversation {
            publish = publish.index("conversation_id", conversation.to_string());
        }
        if let Some(agent) = &scope.agent {
            publish = publish.index("agent_id", agent.to_string());
        }
        publish.json(&doc)?.send().await?;
        Ok(id)
    }

    async fn recall(
        &self,
        scope: &MemoryScope,
        query: &MemoryQuery,
    ) -> Result<Vec<MemoryItem>, LaserError> {
        let mut request = self.laser.query(&self.topic);
        if let Some(conversation) = scope.conversation {
            request = request.where_eq("conversation_id", conversation.to_string());
        }
        let agent_filter = query.agent.as_ref().or(scope.agent.as_ref());
        if let Some(agent) = agent_filter {
            request = request.where_eq("agent_id", agent.to_string());
        }
        let result = if let Some(text) = &query.semantic {
            let embedding = self.embedder.embed(text).await?;
            request
                .nearest(embedding, query.limit)
                .with_payload()
                .fetch()
                .await?
        } else {
            // No semantic query - ULIDs sort by creation time, so order_desc
            // on memory_id gives the most recent `limit` memories.
            request
                .order_desc("memory_id")
                .limit(query.limit)
                .with_payload()
                .fetch()
                .await?
        };
        let mut items = Vec::with_capacity(result.rows.len());
        for row in result.rows {
            let doc: MemoryDoc = row.decode_json()?;
            let id = doc.id.parse().map_err(|error: IdError| {
                LaserError::Codec(format!("decode memory id: {error}"))
            })?;
            let mut provenance = Provenance::builder()
                .conversation_id(scope.conversation.unwrap_or_default())
                .build();
            provenance.agent = scope.agent.clone();
            provenance.idempotency_key = Some(doc.id);
            items.push(MemoryItem::plain(id, doc.payload, provenance));
        }
        Ok(items)
    }

    async fn improve(
        &self,
        _scope: &MemoryScope,
        _feedback: Feedback,
    ) -> Result<MemoryId, LaserError> {
        // Feedback weighting folds into the materialized index on the projector,
        // which lands with the managed graph. Until then this is unsupported
        // rather than a silent no-op.
        Err(LaserError::Unsupported(
            "QueryMemory::improve needs projector feedback support".to_owned(),
        ))
    }

    async fn forget(&self, _scope: &MemoryScope, _id: MemoryId) -> Result<(), LaserError> {
        // The query layer is append-only by design (the Iggy log is the truth).
        // A tombstone primitive on the projector is the future hook for this.
        // Until then, `QueryMemory::forget` is intentionally not implemented -
        // use `VectorMemory` or `LogMemory` when full lifecycle is required.
        Err(LaserError::Unsupported(
            "QueryMemory::forget needs projector tombstone support".to_owned(),
        ))
    }
}

/// A semantic `Memory` backend: embeds payloads on `remember` and ranks `recall`
/// by cosine similarity to `query.semantic`. The index is in-memory. A durable
/// vector store is a future drop-in behind the same trait.
pub struct VectorMemory<E> {
    embedder: E,
    items: Mutex<Vec<VectorEntry>>,
}

struct VectorEntry {
    id: MemoryId,
    scope: MemoryScope,
    embedding: Vec<f32>,
    item: MemoryItem,
    // Accumulated feedback weight, mutated by `improve` and read by `recall`,
    // both under the one `items` lock. No separate map or lock: `recall` already
    // scans the whole `Vec` under this lock, so the weight rides with its entry.
    feedback: f32,
}

impl<E> VectorMemory<E> {
    /// An in-memory semantic memory using `embedder`.
    pub fn new(embedder: E) -> Self {
        Self {
            embedder,
            items: Mutex::new(Vec::new()),
        }
    }
}

impl<E: Embedder + Sync> Memory for VectorMemory<E> {
    async fn remember(
        &self,
        scope: &MemoryScope,
        payload: Vec<u8>,
    ) -> Result<MemoryId, LaserError> {
        let id = MemoryId::new();
        let embedding = self
            .embedder
            .embed(&String::from_utf8_lossy(&payload))
            .await?;
        let mut provenance = Provenance::builder()
            .conversation_id(scope.conversation.unwrap_or_default())
            .build();
        provenance.agent = scope.agent.clone();
        provenance.idempotency_key = Some(id.to_string());
        let item = MemoryItem::plain(id, payload, provenance);
        self.items.lock().await.push(VectorEntry {
            id,
            scope: scope.clone(),
            embedding,
            item,
            feedback: 0.0,
        });
        Ok(id)
    }

    async fn recall(
        &self,
        scope: &MemoryScope,
        query: &MemoryQuery,
    ) -> Result<Vec<MemoryItem>, LaserError> {
        // Embed the query before locking so the network/model call never holds the lock.
        let query_embedding = match &query.semantic {
            Some(text) => Some(self.embedder.embed(text).await?),
            None => None,
        };
        let agent_filter = query.agent.as_ref().or(scope.agent.as_ref());
        let items = self.items.lock().await;
        let mut matched: Vec<&VectorEntry> = items
            .iter()
            .filter(|entry| {
                scope
                    .conversation
                    .is_none_or(|c| entry.scope.conversation == Some(c))
                    && agent_filter.is_none_or(|a| entry.scope.agent.as_ref() == Some(a))
            })
            .collect();

        let any_feedback = matched.iter().any(|entry| entry.feedback != 0.0);

        if let Some(query_embedding) = &query_embedding {
            // Rank by similarity plus the entry's accumulated feedback boost.
            let score =
                |entry: &VectorEntry| cosine(query_embedding, &entry.embedding) + entry.feedback;
            matched.sort_by(|a, b| score(b).total_cmp(&score(a)));
            Ok(matched
                .into_iter()
                .take(query.limit)
                .map(|entry| {
                    let mut item = entry.item.clone();
                    item.score = Some(score(entry));
                    item
                })
                .collect())
        } else if any_feedback {
            // No semantic query but feedback exists: rank promoted items first.
            matched.sort_by(|a, b| b.feedback.total_cmp(&a.feedback));
            Ok(matched
                .into_iter()
                .take(query.limit)
                .map(|entry| {
                    let mut item = entry.item.clone();
                    item.score = Some(entry.feedback);
                    item
                })
                .collect())
        } else {
            // No semantic query and no feedback: the most recent `limit`.
            let start = matched.len().saturating_sub(query.limit);
            Ok(matched[start..]
                .iter()
                .map(|entry| entry.item.clone())
                .collect())
        }
    }

    async fn improve(
        &self,
        _scope: &MemoryScope,
        feedback: Feedback,
    ) -> Result<MemoryId, LaserError> {
        // Accumulate the weight on its target under the one `items` lock, so the
        // next recall reranks. A signal for an unknown target is a no-op.
        let mut items = self.items.lock().await;
        if let Some(entry) = items.iter_mut().find(|entry| entry.id == feedback.target) {
            entry.feedback += feedback.weight;
        }
        Ok(MemoryId::new())
    }

    async fn forget(&self, _scope: &MemoryScope, id: MemoryId) -> Result<(), LaserError> {
        self.items.lock().await.retain(|entry| entry.id != id);
        Ok(())
    }
}

fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let norm_a = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm_a == 0.0 || norm_b == 0.0 {
        0.0
    } else {
        dot / (norm_a * norm_b)
    }
}

/// A `Memory` backend over the managed key-value store (`kv` feature, requires
/// LaserData Cloud). Each item is one KV entry keyed `"<conversation>/<id>"`.
/// `recall` prefix-scans the conversation in creation order, `forget` is an O(1)
/// delete that actually reclaims the entry, and [`with_ttl`](Self::with_ttl)
/// makes entries expire on their own. The difference from [`LogMemory`] is the
/// data model, not recall speed (a stateful, offset-committing log reader can
/// recall incrementally too): KV is mutable point state, so `forget` truly
/// removes where the append-only log keeps a replayed tombstone, and entries can
/// carry a per-key TTL where the log has only coarse, whole-segment retention.
/// Not semantic: a `MemoryQuery::semantic` is ignored and recall returns the most
/// recent items, like `LogMemory`. Pick it for durable working memory that should
/// be pruned or expire.
#[cfg(feature = "kv")]
pub struct KvMemory<'a> {
    laser: &'a Laser,
    namespace: &'a str,
    ttl: Option<std::time::Duration>,
}

// The KV value for one remembered item. `payload` rides as a JSON byte array so
// any body (text or binary) round-trips exactly, matching `QueryMemory`'s doc.
#[cfg(feature = "kv")]
#[derive(serde::Serialize, serde::Deserialize)]
struct KvMemoryDoc {
    payload: Vec<u8>,
    agent: Option<String>,
}

#[cfg(feature = "kv")]
impl<'a> KvMemory<'a> {
    /// Memory in `namespace`, entries never expire. The namespace is borrowed for
    /// the handle's lifetime, so a per-op handle costs no allocation.
    pub fn new(laser: &'a Laser, namespace: &'a str) -> Self {
        Self {
            laser,
            namespace,
            ttl: None,
        }
    }

    /// Expire every remembered item `ttl` after it is written, so the store
    /// self-prunes (the log-backed memories have no equivalent).
    pub fn with_ttl(mut self, ttl: std::time::Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    // Key `"<conversation>/<id>"`: the conversation is the scan prefix and the
    // ULID id sorts the scan in creation-time order.
    fn key(conversation: ConversationId, id: MemoryId) -> String {
        format!("{conversation}/{id}")
    }

    // Write `payload` under an explicit id. A deduped remember passes a
    // content-addressed id, so the same body re-writes the same key (idempotent).
    pub(crate) async fn append(
        &self,
        scope: &MemoryScope,
        id: MemoryId,
        payload: Vec<u8>,
    ) -> Result<MemoryId, LaserError> {
        let conversation = scope.conversation.unwrap_or_default();
        let doc = KvMemoryDoc {
            payload,
            agent: scope.agent.as_ref().map(ToString::to_string),
        };
        let mut set = self
            .laser
            .kv(self.namespace)
            .set(Self::key(conversation, id))
            .json(&doc)?;
        if let Some(ttl) = self.ttl {
            set = set.ttl(ttl);
        }
        set.send().await?;
        Ok(id)
    }
}

#[cfg(feature = "kv")]
impl Memory for KvMemory<'_> {
    async fn remember(
        &self,
        scope: &MemoryScope,
        payload: Vec<u8>,
    ) -> Result<MemoryId, LaserError> {
        self.append(scope, MemoryId::new(), payload).await
    }

    async fn recall(
        &self,
        scope: &MemoryScope,
        query: &MemoryQuery,
    ) -> Result<Vec<MemoryItem>, LaserError> {
        let Some(conversation) = scope.conversation else {
            return Ok(Vec::new());
        };
        // An explicit `query.agent` narrows recall, overriding `scope.agent`.
        let agent_filter = query
            .agent
            .as_ref()
            .or(scope.agent.as_ref())
            .map(ToString::to_string);
        let prefix = format!("{conversation}/");
        let entries = self
            .laser
            .kv(self.namespace)
            .scan()
            .prefix(&prefix)
            .entries()
            .await?;
        let mut items = Vec::new();
        for entry in entries {
            let Some(id) = entry
                .key_str()
                .and_then(|key| key.strip_prefix(&prefix))
                .and_then(|suffix| suffix.parse::<MemoryId>().ok())
            else {
                continue;
            };
            let doc: KvMemoryDoc = entry.decode_value()?;
            if let Some(want) = &agent_filter
                && doc.agent.as_deref() != Some(want.as_str())
            {
                continue;
            }
            let mut provenance = Provenance::builder().conversation_id(conversation).build();
            provenance.agent = doc.agent.as_deref().and_then(|agent| agent.parse().ok());
            provenance.idempotency_key = Some(id.to_string());
            items.push(MemoryItem::plain(id, doc.payload, provenance));
        }
        // Entries arrive in key (creation) order. Keep the most recent `limit`.
        if items.len() > query.limit {
            items = items.split_off(items.len() - query.limit);
        }
        Ok(items)
    }

    async fn forget(&self, scope: &MemoryScope, id: MemoryId) -> Result<(), LaserError> {
        let conversation = scope.conversation.unwrap_or_default();
        self.laser
            .kv(self.namespace)
            .delete(Self::key(conversation, id))
            .await?;
        Ok(())
    }

    async fn improve(
        &self,
        _scope: &MemoryScope,
        _feedback: Feedback,
    ) -> Result<MemoryId, LaserError> {
        // KV is mutable point state with no recall fold to reweight, so feedback
        // has no home here. Use `LogMemory` or `VectorMemory` for ranked recall.
        Err(LaserError::Unsupported(
            "KvMemory does not support feedback-weighted recall".to_owned(),
        ))
    }
}

/// The front door to agent memory. Created by [`Laser::memory`], it picks the
/// strongest backend the negotiated capabilities allow (the managed key-value
/// store when available, otherwise the append-only log) and runs the four verbs
/// through it. Hold one instance per namespace and reuse it: the log backend
/// keeps its incremental recall cursor across calls.
pub enum MemoryHandle {
    /// The append-only log backend (open core).
    Log(LogMemory),
    /// The managed key-value backend.
    #[cfg(feature = "kv")]
    Kv { laser: Laser, namespace: String },
}

impl Laser {
    /// Memory in `namespace`. Picks the managed key-value backend when the
    /// connection advertises it, otherwise the append-only log backend (which
    /// works on raw Apache Iggy). For semantic or graph recall, construct
    /// [`VectorMemory`], [`QueryMemory`], or use [`graph`](Self::graph) with an
    /// embedder. The `namespace` scopes the key-value backend. The log backend
    /// rides the agent audit topic and ignores it.
    pub fn memory(&self, namespace: impl Into<String>) -> MemoryHandle {
        #[cfg(feature = "kv")]
        if self.caps().kv.available {
            return MemoryHandle::Kv {
                laser: self.clone(),
                namespace: namespace.into(),
            };
        }
        let _ = namespace;
        MemoryHandle::Log(LogMemory::new(self.clone()))
    }

    /// A handle to the knowledge-graph surface `name`. Traversals require the
    /// `graph` capability (LaserData Cloud) and ride the managed binary
    /// transport, so this is gated on the `query` feature. Against raw Apache
    /// Iggy a fetch returns [`LaserError::Unsupported`].
    #[cfg(feature = "query")]
    pub fn graph(&self, name: impl Into<String>) -> GraphHandle<'_> {
        GraphHandle {
            laser: self,
            name: name.into(),
            start: None,
            hops: Vec::new(),
            node_filter: None,
            edge_filter: None,
            return_: laser_wire::graph::GraphReturn::Nodes,
            limit: laser_wire::limits::DEFAULT_RECALL_LIMIT,
        }
    }
}

impl MemoryHandle {
    // Append `payload` under `id` and `kind`, dispatching to the chosen backend.
    async fn append_id(
        &self,
        scope: &MemoryScope,
        id: MemoryId,
        kind: MemoryKind,
        payload: Vec<u8>,
    ) -> Result<MemoryId, LaserError> {
        match self {
            MemoryHandle::Log(memory) => memory.append(scope, id, kind, payload).await,
            #[cfg(feature = "kv")]
            MemoryHandle::Kv { laser, namespace } => {
                KvMemory::new(laser, namespace)
                    .append(scope, id, payload)
                    .await
            }
        }
    }

    /// Start a fluent remember. Chain `.scope`/`.agent`/`.durable`/`.dedup` and
    /// finish with `.send().await`.
    pub fn remember(&self, payload: impl Into<Vec<u8>>) -> RememberBuilder<'_> {
        RememberBuilder {
            handle: self,
            payload: payload.into(),
            scope: MemoryScope::default(),
            kind: MemoryKind::Fact,
            dedup: false,
        }
    }

    /// Start a fluent recall in `conversation`. Chain `.semantic`/`.limit` and
    /// finish with `.fetch().await`.
    pub fn recall(&self, conversation: ConversationId) -> RecallBuilder<'_> {
        RecallBuilder {
            handle: self,
            scope: MemoryScope::builder().conversation(conversation).build(),
            limit: 50,
            semantic: None,
            strategy: RecallStrategy::Auto,
        }
    }

    /// Record `feedback` on a recalled item under `scope`.
    pub async fn improve(
        &self,
        scope: &MemoryScope,
        feedback: Feedback,
    ) -> Result<MemoryId, LaserError> {
        Memory::improve(self, scope, feedback).await
    }

    /// Forget the item `id` under `scope`.
    pub async fn forget(&self, scope: &MemoryScope, id: MemoryId) -> Result<(), LaserError> {
        Memory::forget(self, scope, id).await
    }
}

impl Memory for MemoryHandle {
    async fn remember(
        &self,
        scope: &MemoryScope,
        payload: Vec<u8>,
    ) -> Result<MemoryId, LaserError> {
        self.append_id(scope, MemoryId::new(), MemoryKind::Fact, payload)
            .await
    }

    async fn recall(
        &self,
        scope: &MemoryScope,
        query: &MemoryQuery,
    ) -> Result<Vec<MemoryItem>, LaserError> {
        match self {
            MemoryHandle::Log(memory) => Memory::recall(memory, scope, query).await,
            #[cfg(feature = "kv")]
            MemoryHandle::Kv { laser, namespace } => {
                Memory::recall(&KvMemory::new(laser, namespace), scope, query).await
            }
        }
    }

    async fn improve(
        &self,
        scope: &MemoryScope,
        feedback: Feedback,
    ) -> Result<MemoryId, LaserError> {
        match self {
            MemoryHandle::Log(memory) => Memory::improve(memory, scope, feedback).await,
            #[cfg(feature = "kv")]
            MemoryHandle::Kv { laser, namespace } => {
                Memory::improve(&KvMemory::new(laser, namespace), scope, feedback).await
            }
        }
    }

    async fn forget(&self, scope: &MemoryScope, id: MemoryId) -> Result<(), LaserError> {
        match self {
            MemoryHandle::Log(memory) => Memory::forget(memory, scope, id).await,
            #[cfg(feature = "kv")]
            MemoryHandle::Kv { laser, namespace } => {
                Memory::forget(&KvMemory::new(laser, namespace), scope, id).await
            }
        }
    }
}

/// A fluent remember, created by [`MemoryHandle::remember`].
pub struct RememberBuilder<'a> {
    handle: &'a MemoryHandle,
    payload: Vec<u8>,
    scope: MemoryScope,
    kind: MemoryKind,
    dedup: bool,
}

impl RememberBuilder<'_> {
    /// Scope to a conversation.
    #[must_use]
    pub fn scope(mut self, conversation: ConversationId) -> Self {
        self.scope.conversation = Some(conversation);
        self
    }

    /// Attribute to an agent.
    #[must_use]
    pub fn agent(mut self, agent: AgentId) -> Self {
        self.scope.agent = Some(agent);
        self
    }

    /// Scope to a stream (the widest tier).
    #[must_use]
    pub fn stream(mut self, stream: impl Into<String>) -> Self {
        self.scope.stream = Some(stream.into());
        self
    }

    /// Store as durable, shared across conversations (default is session-scoped).
    #[must_use]
    pub fn durable(mut self) -> Self {
        self.scope.lifetime = Lifetime::Durable;
        self
    }

    /// Set the item kind.
    #[must_use]
    pub fn kind(mut self, kind: MemoryKind) -> Self {
        self.kind = kind;
        self
    }

    /// Content-address the id, so storing the same body under the same durable
    /// owner is idempotent (remember twice, store once).
    #[must_use]
    pub fn dedup(mut self) -> Self {
        self.dedup = true;
        self
    }

    /// Store the item, returning its id.
    pub async fn send(self) -> Result<MemoryId, LaserError> {
        let id = if self.dedup {
            MemoryId::content(&self.scope, self.kind, &self.payload)
        } else {
            MemoryId::new()
        };
        self.handle
            .append_id(&self.scope, id, self.kind, self.payload)
            .await
    }
}

/// A fluent recall, created by [`MemoryHandle::recall`].
pub struct RecallBuilder<'a> {
    handle: &'a MemoryHandle,
    scope: MemoryScope,
    limit: usize,
    semantic: Option<String>,
    strategy: RecallStrategy,
}

impl RecallBuilder<'_> {
    /// Rank by semantic similarity to `text` (sets the `Semantic` strategy).
    /// Honored by an embedding backend. The log backend returns the most recent.
    #[must_use]
    pub fn semantic(mut self, text: impl Into<String>) -> Self {
        self.semantic = Some(text.into());
        self.strategy = RecallStrategy::Semantic;
        self
    }

    /// Return the most recent items (sets the `Recent` strategy).
    #[must_use]
    pub fn recent(mut self) -> Self {
        self.strategy = RecallStrategy::Recent;
        self
    }

    /// Narrow to an agent.
    #[must_use]
    pub fn agent(mut self, agent: AgentId) -> Self {
        self.scope.agent = Some(agent);
        self
    }

    /// Cap the number of items returned.
    #[must_use]
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    /// Run the recall.
    pub async fn fetch(self) -> Result<Vec<MemoryItem>, LaserError> {
        let query = MemoryQuery::builder()
            .limit(self.limit)
            .maybe_semantic(self.semantic)
            .strategy(self.strategy)
            .maybe_agent(self.scope.agent.clone())
            .build();
        Memory::recall(self.handle, &self.scope, &query).await
    }
}

/// A fluent knowledge-graph traversal, created by [`Laser::graph`]. Set a start
/// (`start_ids`/`start_match`/`start_nearest`), add hops (`out`/`incoming`), pick
/// what to return, and finish with `.fetch().await`. Gated on the `query`
/// feature: a traversal rides the managed binary transport.
#[cfg(feature = "query")]
pub struct GraphHandle<'a> {
    laser: &'a Laser,
    name: String,
    start: Option<laser_wire::graph::GraphStart>,
    hops: Vec<laser_wire::graph::Hop>,
    node_filter: Option<laser_wire::query::Filter>,
    edge_filter: Option<laser_wire::query::Filter>,
    return_: laser_wire::graph::GraphReturn,
    limit: usize,
}

#[cfg(feature = "query")]
impl GraphHandle<'_> {
    /// Start the traversal from explicit node ids.
    #[must_use]
    pub fn start_ids(mut self, ids: Vec<laser_wire::graph::NodeId>) -> Self {
        self.start = Some(laser_wire::graph::GraphStart::Ids(ids));
        self
    }

    /// Start from the nodes matching a predicate.
    #[must_use]
    pub fn start_match(mut self, filter: laser_wire::query::Filter) -> Self {
        self.start = Some(laser_wire::graph::GraphStart::Match(filter));
        self
    }

    /// Start from the nodes nearest an embedding (vector-seeded traversal).
    #[must_use]
    pub fn start_nearest(mut self, embedding: Vec<f32>, k: usize) -> Self {
        self.start = Some(laser_wire::graph::GraphStart::Nearest { embedding, k });
        self
    }

    /// Follow outgoing edges of `edge_type` one hop.
    #[must_use]
    pub fn out(mut self, edge_type: impl Into<String>) -> Self {
        self.hops.push(laser_wire::graph::Hop {
            edge_type: Some(edge_type.into()),
            dir: laser_wire::graph::EdgeDir::Out,
            max: 1,
        });
        self
    }

    /// Follow incoming edges of `edge_type` one hop.
    #[must_use]
    pub fn incoming(mut self, edge_type: impl Into<String>) -> Self {
        self.hops.push(laser_wire::graph::Hop {
            edge_type: Some(edge_type.into()),
            dir: laser_wire::graph::EdgeDir::In,
            max: 1,
        });
        self
    }

    /// Follow edges of `edge_type` one hop in both directions.
    #[must_use]
    pub fn both(mut self, edge_type: impl Into<String>) -> Self {
        self.hops.push(laser_wire::graph::Hop {
            edge_type: Some(edge_type.into()),
            dir: laser_wire::graph::EdgeDir::Both,
            max: 1,
        });
        self
    }

    /// Return the traversed edges instead of the reachable nodes.
    #[must_use]
    pub fn return_edges(mut self) -> Self {
        self.return_ = laser_wire::graph::GraphReturn::Edges;
        self
    }

    /// Return whole paths (node and edge id sequences) instead of nodes.
    #[must_use]
    pub fn return_paths(mut self) -> Self {
        self.return_ = laser_wire::graph::GraphReturn::Paths;
        self
    }

    /// Cap the number of elements returned.
    #[must_use]
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    /// Run the traversal. Requires `managed_graph`. Otherwise returns
    /// [`LaserError::Unsupported`].
    pub async fn fetch(self) -> Result<laser_wire::graph::GraphResult, LaserError> {
        use laser_wire::graph::{GraphQuery, GraphStart};
        self.require_graph()?;
        let query = GraphQuery {
            v: laser_wire::codes::GRAPH_OP_VERSION,
            graph: self.name,
            start: self.start.unwrap_or(GraphStart::Ids(Vec::new())),
            traverse: self.hops,
            node_filter: self.node_filter,
            edge_filter: self.edge_filter,
            return_: self.return_,
            limit: self.limit,
            fork: None,
            consistency: laser_wire::query::Consistency::Eventual,
        };
        let payload = bytes::Bytes::from(
            laser_wire::framing::encode_named(&query)
                .map_err(|error| LaserError::Codec(format!("encode graph query: {error}")))?,
        );
        let bytes = self
            .laser
            .send_raw_with_response(laser_wire::codes::AGDX_GRAPH_QUERY_CODE, payload)
            .await?;
        decode_graph_reply(&bytes)
    }

    /// Read a node's neighbors: the nodes reachable in `dir` over `edge_type` (or
    /// any type when `None`), following the same hop `depth` times. The cheap,
    /// common traversal. Requires `managed_graph`. Otherwise returns
    /// [`LaserError::Unsupported`].
    pub async fn neighbors(
        self,
        node: laser_wire::graph::NodeId,
        dir: laser_wire::graph::EdgeDir,
        edge_type: Option<String>,
        depth: u32,
    ) -> Result<laser_wire::graph::GraphResult, LaserError> {
        use laser_wire::graph::GraphNeighbors;
        self.require_graph()?;
        let request = GraphNeighbors {
            v: laser_wire::codes::GRAPH_OP_VERSION,
            graph: self.name,
            node,
            dir,
            edge_type,
            depth,
            limit: self.limit,
        };
        let payload = bytes::Bytes::from(
            laser_wire::framing::encode_named(&request)
                .map_err(|error| LaserError::Codec(format!("encode graph neighbors: {error}")))?,
        );
        let bytes = self
            .laser
            .send_raw_with_response(laser_wire::codes::AGDX_GRAPH_NEIGHBORS_CODE, payload)
            .await?;
        decode_graph_reply(&bytes)
    }

    /// Write `nodes` and `edges` into the graph: the projector path, surfaced for
    /// callers that build the graph directly rather than through a `graph`
    /// projection. Idempotent on content-addressed ids
    /// ([`GraphNode::entity`](laser_wire::graph::GraphNode::entity),
    /// [`GraphEdge::relate`](laser_wire::graph::GraphEdge::relate)), so re-applying
    /// the same entities is a no-op. Requires `managed_graph`. Otherwise returns
    /// [`LaserError::Unsupported`].
    pub async fn upsert(
        self,
        nodes: Vec<laser_wire::graph::GraphNode>,
        edges: Vec<laser_wire::graph::GraphEdge>,
    ) -> Result<(), LaserError> {
        use laser_wire::graph::GraphUpsert;
        self.require_graph()?;
        let request = GraphUpsert {
            v: laser_wire::codes::GRAPH_OP_VERSION,
            graph: self.name,
            nodes,
            edges,
        };
        let payload = bytes::Bytes::from(
            laser_wire::framing::encode_named(&request)
                .map_err(|error| LaserError::Codec(format!("encode graph upsert: {error}")))?,
        );
        let bytes = self
            .laser
            .send_raw_with_response(laser_wire::codes::AGDX_GRAPH_UPSERT_CODE, payload)
            .await?;
        decode_graph_reply(&bytes).map(|_| ())
    }

    // Every graph op rides the managed binary transport, so it is unavailable
    // against raw Apache Iggy. Fail the same way before encoding any request.
    fn require_graph(&self) -> Result<(), LaserError> {
        if self.laser.caps().graph {
            Ok(())
        } else {
            Err(LaserError::Unsupported(
                "graph traversal requires managed_graph (LaserData Cloud)".to_owned(),
            ))
        }
    }
}

// Decode a managed `GraphReply` into the `Ok` result or its typed error.
#[cfg(feature = "query")]
fn decode_graph_reply(bytes: &[u8]) -> Result<laser_wire::graph::GraphResult, LaserError> {
    use laser_wire::graph::GraphReply;
    match crate::error::decode_managed_reply::<GraphReply>(bytes)? {
        GraphReply::Ok(result) => Ok(result),
        GraphReply::Err(error) => Err(error.into()),
        _ => Err(LaserError::Protocol(
            "graph: unknown reply variant".to_owned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_a_memory_id_when_round_tripped_through_a_string_then_should_be_equal() {
        let id = MemoryId::new();
        let parsed = id
            .to_string()
            .parse::<MemoryId>()
            .expect("a formatted memory id should parse");
        assert_eq!(parsed, id);
    }

    fn item_entry(id: MemoryId, body: &[u8]) -> Vec<u8> {
        MemoryLogEntry::Item {
            id,
            kind: MemoryKind::Fact,
            body: body.to_vec(),
        }
        .encode()
        .expect("an item entry encodes")
    }

    #[test]
    fn given_messages_when_absorbed_then_should_fold_items_and_apply_tombstones() {
        let conversation = ConversationId::new();
        let prov = || Provenance::builder().conversation_id(conversation).build();
        let mut projection = Projection::default();
        let id_a = MemoryId::new();
        let id_b = MemoryId::new();
        projection.absorb(&item_entry(id_a, b"a"), prov());
        projection.absorb(&item_entry(id_b, b"b"), prov());
        assert_eq!(projection.items.len(), 2, "two items folded in");

        // A tombstone for the first item records it as forgotten AND reclaims it
        // from `items` (no body pinned for the life of the projection).
        let tombstone = MemoryLogEntry::Forget { target: id_a }
            .encode()
            .expect("a forget entry encodes");
        projection.absorb(&tombstone, prov());
        assert_eq!(projection.items.len(), 1, "tombstoned item reclaimed");
        assert_eq!(projection.items[0].id, id_b, "only the survivor remains");
        assert!(projection.forgotten.contains(&id_a), "first item forgotten");

        // An item whose id was already tombstoned (reorder) is not re-added.
        projection.absorb(&item_entry(id_a, b"a-again"), prov());
        assert_eq!(projection.items.len(), 1, "forgotten id not re-added");

        // A payload that is not a memory entry is ignored.
        projection.absorb(b"not an entry", prov());
        assert_eq!(projection.items.len(), 1, "non-entry payload dropped");
    }

    #[test]
    fn given_a_feedback_entry_when_absorbed_then_should_accumulate_the_weight() {
        let conversation = ConversationId::new();
        let prov = || Provenance::builder().conversation_id(conversation).build();
        let mut projection = Projection::default();
        let target = MemoryId::new();
        let feedback = MemoryLogEntry::Feedback {
            target,
            weight: 2.5,
        }
        .encode()
        .expect("a feedback entry encodes");
        projection.absorb(&feedback, prov());
        projection.absorb(&feedback, prov());
        assert_eq!(projection.feedback.get(&target), Some(&5.0));
        assert!(projection.items.is_empty(), "feedback is not an item");
    }

    // Deterministic stand-in for a real embedding model: a 3-dim count vector.
    struct WordEmbedder;

    impl Embedder for WordEmbedder {
        async fn embed(&self, text: &str) -> Result<Vec<f32>, LaserError> {
            Ok(vec![
                text.matches("cat").count() as f32,
                text.matches("dog").count() as f32,
                text.matches("fish").count() as f32,
            ])
        }
    }

    // `use super::*` brings both the base `LocalMemory` and the `Memory` Send
    // variant into scope, so the trait is named explicitly on each call.
    #[tokio::test]
    async fn given_a_semantic_query_when_recalling_then_should_rank_by_similarity() {
        let memory = VectorMemory::new(WordEmbedder);
        let scope = MemoryScope::builder()
            .conversation(ConversationId::new())
            .build();
        Memory::remember(&memory, &scope, b"the cat sat".to_vec())
            .await
            .expect("remembering the cat note should succeed");
        Memory::remember(&memory, &scope, b"the dog ran".to_vec())
            .await
            .expect("remembering the dog note should succeed");

        let top = Memory::recall(
            &memory,
            &scope,
            &MemoryQuery::builder()
                .semantic("cat".to_owned())
                .limit(1)
                .build(),
        )
        .await
        .expect("recall should succeed");
        assert_eq!(top.len(), 1);
        assert_eq!(top[0].payload.as_slice(), b"the cat sat");
    }

    #[test]
    fn given_the_same_content_when_addressed_then_should_produce_the_same_id() {
        let owner = MemoryScope::builder()
            .agent("planner".parse().expect("valid agent"))
            .lifetime(Lifetime::Durable)
            .build();
        let a = MemoryId::content(&owner, MemoryKind::Fact, b"the budget is 5000");
        let b = MemoryId::content(&owner, MemoryKind::Fact, b"the budget is 5000");
        assert_eq!(a, b, "same owner, kind, and body must dedup to one id");
        let other = MemoryId::content(&owner, MemoryKind::Fact, b"the budget is 6000");
        assert_ne!(a, other, "a different body must produce a different id");
    }

    #[test]
    fn given_a_known_content_when_addressed_then_should_match_the_cross_sdk_id() {
        // Pins the content-addressed id to a golden value shared with the Python
        // reference engine, so a fact deduped in one SDK has the same id in the
        // other. Owner: agent "agent", no stream, kind Fact, body "x".
        let owner = MemoryScope::builder()
            .agent("agent".parse().expect("valid agent"))
            .build();
        let id = MemoryId::content(&owner, MemoryKind::Fact, b"x");
        assert_eq!(id.to_string(), "1A9GVS6SJ6SNS4KY0H19130WCW");
    }

    #[tokio::test]
    async fn given_positive_feedback_when_recalling_then_should_rank_the_target_first() {
        let memory = VectorMemory::new(WordEmbedder);
        let scope = MemoryScope::builder()
            .conversation(ConversationId::new())
            .build();
        let first = Memory::remember(&memory, &scope, b"the cat sat".to_vec())
            .await
            .expect("remember first");
        let second = Memory::remember(&memory, &scope, b"the dog ran".to_vec())
            .await
            .expect("remember second");
        // With no feedback, recall returns creation order (first, then second).
        let before = Memory::recall(&memory, &scope, &MemoryQuery::builder().build())
            .await
            .expect("recall before feedback");
        assert_eq!(before[0].id, first);

        // Promote the second item, then it ranks ahead.
        Memory::improve(&memory, &scope, Feedback::new(second, 5.0))
            .await
            .expect("improve");
        let after = Memory::recall(&memory, &scope, &MemoryQuery::builder().build())
            .await
            .expect("recall after feedback");
        assert_eq!(after[0].id, second, "the promoted item ranks first");
        assert_eq!(after[0].score, Some(5.0));
    }

    #[cfg(feature = "kv")]
    #[test]
    fn given_a_kv_memory_doc_when_round_tripped_then_should_preserve_payload_and_agent() {
        let doc = KvMemoryDoc {
            payload: vec![0x00, 0xff, 0x10],
            agent: Some("planner".to_owned()),
        };
        let bytes = serde_json::to_vec(&doc).expect("a memory doc should serialize");
        let back: KvMemoryDoc =
            serde_json::from_slice(&bytes).expect("a memory doc should deserialize");
        assert_eq!(back.payload, vec![0x00, 0xff, 0x10]);
        assert_eq!(back.agent.as_deref(), Some("planner"));
    }

    #[cfg(feature = "kv")]
    #[test]
    fn given_a_kv_memory_key_when_built_then_should_prefix_with_the_conversation() {
        let conversation = ConversationId::new();
        let id = MemoryId::new();
        let key = KvMemory::key(conversation, id);
        assert_eq!(key, format!("{conversation}/{id}"));
        assert!(key.starts_with(&format!("{conversation}/")));
    }

    #[tokio::test]
    async fn given_a_forgotten_id_when_recalling_then_should_be_excluded() {
        let memory = VectorMemory::new(WordEmbedder);
        let scope = MemoryScope::builder()
            .conversation(ConversationId::new())
            .build();
        let id = Memory::remember(&memory, &scope, b"cat".to_vec())
            .await
            .expect("remember should succeed");
        Memory::forget(&memory, &scope, id)
            .await
            .expect("forget should succeed");
        let all = Memory::recall(&memory, &scope, &MemoryQuery::builder().build())
            .await
            .expect("recall should succeed");
        assert!(all.is_empty());
    }
}
