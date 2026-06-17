use crate::agent::Laser;
use crate::error::LaserError;
use crate::provenance::{AgentTopic, Provenance};
use crate::types::{AgentId, ConversationId, IdError};
use iggy::prelude::*;
use std::collections::HashSet;
use std::fmt::{self, Display, Formatter};
use std::str::FromStr;
use tokio::sync::Mutex;
use ulid::Ulid;

const FORGET_PREFIX: &[u8] = b"\x00agdx.forget\x00";
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

/// Scopes memory to an agent and/or a conversation.
#[derive(Debug, Clone, Default, bon::Builder)]
pub struct MemoryScope {
    /// Narrow recall to this agent (overrides the scope's agent).
    /// Restrict to this agent (none = any).
    pub agent: Option<AgentId>,
    /// Restrict to this conversation (none = any).
    pub conversation: Option<ConversationId>,
}

/// A remembered item: its id, payload, and provenance.
#[derive(Debug, Clone)]
pub struct MemoryItem {
    /// The item's id.
    pub id: MemoryId,
    /// The remembered body. Owned `Vec<u8>` so the public API never leaks the
    /// `bytes` crate.
    pub payload: Vec<u8>,
    /// The scope it was stored under.
    pub provenance: Provenance,
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
}

/// Agent memory: `remember` / `recall` / `forget`. (`Memory` is the `Send` variant.)
#[trait_variant::make(Memory: Send)]
pub trait LocalMemory {
    /// Append `payload` to the memory under `scope`. The body is opaque bytes -
    /// encode it with whatever codec you like before calling.
    async fn remember(&self, scope: &MemoryScope, payload: Vec<u8>)
    -> Result<MemoryId, LaserError>;
    async fn recall(
        &self,
        scope: &MemoryScope,
        query: &MemoryQuery,
    ) -> Result<Vec<MemoryItem>, LaserError>;
    async fn forget(&self, scope: &MemoryScope, id: MemoryId) -> Result<(), LaserError>;
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
pub struct LogMemory<'a> {
    laser: &'a Laser,
    projection: Mutex<Projection>,
}

// The folded view `recall` reads: every memory item seen so far (in arrival order),
// the tombstoned ids, and the per-partition offset the fold has consumed up to.
#[derive(Default)]
struct Projection {
    items: Vec<MemoryItem>,
    forgotten: HashSet<MemoryId>,
    offsets: Vec<u64>,
}

impl Projection {
    // Fold one audit message in: a tombstone records a forgotten id, anything else
    // with a parseable `MemoryId` becomes an item. Pure, so it is unit-tested.
    fn absorb(&mut self, payload: Vec<u8>, provenance: Provenance) {
        if let Some(target) = parse_tombstone(&payload) {
            // Drop the tombstoned item so its payload is not pinned in memory for the
            // life of the projection. Keep the id so a tombstone that arrives before
            // its item (a cross-partition reorder) still suppresses it on arrival.
            self.items.retain(|item| item.id != target);
            self.forgotten.insert(target);
            return;
        }
        let Some(id) = provenance
            .idempotency_key
            .as_deref()
            .and_then(|key| key.parse::<MemoryId>().ok())
        else {
            return;
        };
        if self.forgotten.contains(&id) {
            return;
        }
        self.items.push(MemoryItem {
            id,
            payload,
            provenance,
        });
    }
}

impl<'a> LogMemory<'a> {
    /// A log-backed memory over `laser`.
    pub fn new(laser: &'a Laser) -> Self {
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
                projection.absorb(message.payload.to_vec(), provenance);
            }
            projection.offsets[partition as usize] = next_offset;
        }
        Ok(())
    }
}

impl Memory for LogMemory<'_> {
    async fn remember(
        &self,
        scope: &MemoryScope,
        payload: Vec<u8>,
    ) -> Result<MemoryId, LaserError> {
        let id = MemoryId::new();
        let provenance = Self::provenance(scope, id.to_string());
        self.laser
            .send_agent(AgentTopic::Audit, payload, &provenance)
            .await?;
        Ok(id)
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
        if items.len() > query.limit {
            items = items.split_off(items.len() - query.limit);
        }
        Ok(items)
    }

    async fn forget(&self, scope: &MemoryScope, id: MemoryId) -> Result<(), LaserError> {
        let mut payload = Vec::with_capacity(FORGET_PREFIX.len() + 26);
        payload.extend_from_slice(FORGET_PREFIX);
        payload.extend_from_slice(id.to_string().as_bytes());
        let provenance = Self::provenance(scope, MemoryId::new().to_string());
        self.laser
            .send_agent(AgentTopic::Audit, payload, &provenance)
            .await?;
        Ok(())
    }
}

fn parse_tombstone(payload: &[u8]) -> Option<MemoryId> {
    payload
        .strip_prefix(FORGET_PREFIX)
        .and_then(|rest| std::str::from_utf8(rest).ok())
        .and_then(|id| id.parse().ok())
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
            items.push(MemoryItem {
                id,
                payload: doc.payload,
                provenance,
            });
        }
        Ok(items)
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
        let item = MemoryItem {
            id,
            payload,
            provenance,
        };
        self.items.lock().await.push(VectorEntry {
            id,
            scope: scope.clone(),
            embedding,
            item,
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

        if let Some(query_embedding) = &query_embedding {
            matched.sort_by(|a, b| {
                cosine(query_embedding, &b.embedding)
                    .total_cmp(&cosine(query_embedding, &a.embedding))
            });
            Ok(matched
                .into_iter()
                .take(query.limit)
                .map(|entry| entry.item.clone())
                .collect())
        } else {
            // No semantic query: return the most recent `limit`, like the log backend.
            let start = matched.len().saturating_sub(query.limit);
            Ok(matched[start..]
                .iter()
                .map(|entry| entry.item.clone())
                .collect())
        }
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
    namespace: String,
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
    /// Memory in `namespace`, entries never expire.
    pub fn new(laser: &'a Laser, namespace: impl Into<String>) -> Self {
        Self {
            laser,
            namespace: namespace.into(),
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
}

#[cfg(feature = "kv")]
impl Memory for KvMemory<'_> {
    async fn remember(
        &self,
        scope: &MemoryScope,
        payload: Vec<u8>,
    ) -> Result<MemoryId, LaserError> {
        let id = MemoryId::new();
        let conversation = scope.conversation.unwrap_or_default();
        let doc = KvMemoryDoc {
            payload,
            agent: scope.agent.as_ref().map(ToString::to_string),
        };
        let mut set = self
            .laser
            .kv(&self.namespace)
            .set(Self::key(conversation, id))
            .json(&doc)?;
        if let Some(ttl) = self.ttl {
            set = set.ttl(ttl);
        }
        set.send().await?;
        Ok(id)
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
            .kv(&self.namespace)
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
            items.push(MemoryItem {
                id,
                payload: doc.payload,
                provenance,
            });
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
            .kv(&self.namespace)
            .delete(Self::key(conversation, id))
            .await?;
        Ok(())
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

    #[test]
    fn given_messages_when_absorbed_then_should_fold_items_and_apply_tombstones() {
        let conversation = ConversationId::new();
        let item = |payload: &[u8]| {
            let id = MemoryId::new();
            let mut provenance = Provenance::builder().conversation_id(conversation).build();
            provenance.idempotency_key = Some(id.to_string());
            (id, provenance, payload.to_vec())
        };
        let mut projection = Projection::default();
        let (id_a, prov_a, payload_a) = item(b"a");
        let (id_b, prov_b, payload_b) = item(b"b");
        projection.absorb(payload_a, prov_a);
        projection.absorb(payload_b, prov_b);
        assert_eq!(projection.items.len(), 2, "two items folded in");

        // A tombstone for the first item records it as forgotten AND reclaims it
        // from `items` (no payload pinned for the life of the projection).
        let mut tombstone = FORGET_PREFIX.to_vec();
        tombstone.extend_from_slice(id_a.to_string().as_bytes());
        let mut tomb_prov = Provenance::builder().conversation_id(conversation).build();
        tomb_prov.idempotency_key = Some(MemoryId::new().to_string());
        projection.absorb(tombstone, tomb_prov);
        assert_eq!(projection.items.len(), 1, "tombstoned item reclaimed");
        assert_eq!(projection.items[0].id, id_b, "only the survivor remains");
        assert!(projection.forgotten.contains(&id_a), "first item forgotten");

        // An item whose id was already tombstoned (reorder) is not re-added.
        let (_, mut prov_a_again, _) = item(b"a-again");
        prov_a_again.idempotency_key = Some(id_a.to_string());
        projection.absorb(b"a-again".to_vec(), prov_a_again);
        assert_eq!(projection.items.len(), 1, "forgotten id not re-added");

        // A payload whose provenance carries no parseable MemoryId is ignored.
        let no_id = Provenance::builder().conversation_id(conversation).build();
        projection.absorb(b"orphan".to_vec(), no_id);
        assert_eq!(projection.items.len(), 1, "no-id payload dropped");
    }

    #[test]
    fn given_a_tombstone_payload_when_parsed_then_should_recover_the_forgotten_id() {
        let id = MemoryId::new();
        let mut payload = FORGET_PREFIX.to_vec();
        payload.extend_from_slice(id.to_string().as_bytes());
        assert_eq!(parse_tombstone(&payload), Some(id));
        assert_eq!(parse_tombstone(b"regular payload"), None);
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
