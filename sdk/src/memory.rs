use crate::agent::Laser;
use crate::error::LaserError;
use crate::provenance::{AgentTopic, Provenance};
use crate::types::{AgentId, ConversationId, IdError};
use iggy::prelude::*;
use laser_wire::graph::SourceRef;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt::{self, Display, Formatter};
use std::str::FromStr;
use tokio::sync::Mutex;
use ulid::Ulid;

// Per-partition poll size when `LogMemory` catches its projection up to the tail.
const READ_BATCH: u32 = 1000;

/// How many items one consolidation pass scans (the recall window it operates
/// over). Items older than this window are not recalled, so they are neither kept
/// nor pruned by a pass, they simply age out of view.
const CONSOLIDATION_WINDOW: usize = 10_000;

/// The default message-expiry for a configured memory topic: how long the audit
/// history of every remember, forget, and feedback lives on the stream. Bounds
/// the topic without dropping too soon. The materialized read view keeps its own
/// separate retention, so a value pruned there is still on the stream until this
/// window passes. Thirty days. Override with
/// [`MemoryTopicBuilder::ttl`]/[`no_expiry`](MemoryTopicBuilder::no_expiry).
pub const DEFAULT_MEMORY_TOPIC_TTL: std::time::Duration =
    std::time::Duration::from_secs(30 * 24 * 60 * 60);

/// What a remembered item is. Agentic memory is an SDK layer over the streaming,
/// query, and graph primitives, so this is an SDK type, not a wire op field.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryKind {
    /// A standalone fact (the default). Semantic memory: what is known.
    #[default]
    Fact,
    /// A conversation turn. Episodic memory: what happened.
    Message,
    /// A summary distilled from other items.
    Summary,
    /// An extracted entity, the body of a graph node.
    Entity,
    /// A feedback signal that reweights recall.
    Feedback,
    /// A reusable procedure: a skill or workflow recalled by task similarity and
    /// improved by outcome feedback. Procedural memory: how things are done.
    Procedure,
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
            MemoryKind::Procedure => 6,
        }
    }

    /// The snake-case kind word, the form that rides the wire record and the
    /// read view. `Fact` for an unknown word, so a newer kind read by an older
    /// build degrades rather than fails.
    pub fn from_word(word: &str) -> MemoryKind {
        match word {
            "message" => MemoryKind::Message,
            "summary" => MemoryKind::Summary,
            "entity" => MemoryKind::Entity,
            "feedback" => MemoryKind::Feedback,
            "procedure" => MemoryKind::Procedure,
            _ => MemoryKind::Fact,
        }
    }

    /// The memory class this kind belongs to, in the converging
    /// episodic / semantic / procedural taxonomy. `Episodic` is what happened
    /// (messages), `Procedural` is how things are done (procedures), and
    /// everything else is `Semantic` (what is known).
    pub const fn class(self) -> MemoryClass {
        match self {
            MemoryKind::Message => MemoryClass::Episodic,
            MemoryKind::Procedure => MemoryClass::Procedural,
            MemoryKind::Fact | MemoryKind::Summary | MemoryKind::Entity | MemoryKind::Feedback => {
                MemoryClass::Semantic
            }
        }
    }
}

/// The high-level class a [`MemoryKind`] falls into, the field's converging
/// taxonomy: episodic (what happened), semantic (what is known), procedural (how
/// things are done).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryClass {
    /// What happened: conversation turns and events.
    Episodic,
    /// What is known: facts, summaries, entities.
    Semantic,
    /// How things are done: procedures, skills, workflows.
    Procedural,
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
    /// Rank by semantic similarity to the query (vector signal).
    Semantic,
    /// Rank by lexical overlap with the query (keyword signal), the complement to
    /// `Semantic` for exact-term matches an embedding can blur.
    Keyword,
    /// Traverse the knowledge graph from the query seed.
    Graph,
    /// Items inside the time range, most recent first.
    Temporal,
    /// Fuse every available signal (semantic + keyword + feedback) into one score.
    /// The multi-signal default for quality recall.
    Hybrid,
}

/// A memory item's id (a ULID, so it sorts by creation time).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MemoryId(Ulid);

impl MemoryId {
    /// A fresh, random memory id.
    pub fn new() -> Self {
        Self(Ulid::generate())
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

/// Scopes memory along the converging identity layers (user / agent / session /
/// app) plus the physical stream, with a lifetime tier. Any field left unset
/// widens recall to match across that dimension.
///
/// `conversation` is the session layer (a single run or thread). `user` and `app`
/// are the broader identity layers: the end user the memory is about, and the
/// application or org it belongs to within a deployment. They narrow recall on
/// backends that store them (e.g. [`VectorMemory`]). The physical isolation
/// boundary stays the Iggy stream. (There is deliberately no customer layer
/// here: a deployment is the customer's alone, and `app` is a scope within it.)
#[derive(Debug, Clone, Default, bon::Builder)]
pub struct MemoryScope {
    /// Restrict to this stream (none = any). The physical isolation boundary.
    pub stream: Option<String>,
    /// Restrict to this end user (none = any). The identity the memory is about.
    pub user: Option<String>,
    /// Restrict to this agent (none = any).
    pub agent: Option<AgentId>,
    /// Restrict to this conversation, the session layer (none = any).
    pub conversation: Option<ConversationId>,
    /// Restrict to this application or org within the deployment (none = any).
    pub app: Option<String>,
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
    /// Which signals produced this candidate and their pre-fusion standing, so
    /// a fused recall (`Hybrid`, a routed `Auto`) stays inspectable. Empty for
    /// an unranked recall. Additive: nothing on the wire carries it.
    pub signals: Vec<RecallSignal>,
    /// The origin log record this item was folded from (stream, topic, partition,
    /// offset), when the managed read view carried it, so a reader can navigate
    /// back to the source message while it is still on the log. `None` for an
    /// in-process recall or a read view without provenance.
    pub source: Option<SourceRef>,
}

#[cfg(test)]
impl MemoryItem {
    fn plain(id: MemoryId, payload: Vec<u8>, provenance: Provenance) -> Self {
        Self {
            id,
            payload,
            provenance,
            kind: MemoryKind::Fact,
            score: None,
            signals: Vec::new(),
            source: None,
        }
    }
}

/// One signal's contribution to a recalled item: the strategy that surfaced
/// it, its rank within that signal (0 is best), and that signal's own score
/// before fusion.
#[derive(Debug, Clone, PartialEq)]
pub struct RecallSignal {
    /// The strategy that produced the candidate. For a routed `Auto` this is
    /// the strategy `Auto` resolved to, which is how the routing is reported.
    pub strategy: RecallStrategy,
    /// The candidate's rank within this signal, 0 is best.
    pub rank: usize,
    /// The signal's own score before fusion (similarity, relevance, weight).
    pub score: Option<f32>,
}

/// Render recalled `items` into one context block for a prompt, in rank order,
/// filling up to `token_budget` (advisory tokens, estimated at ~4 bytes each).
/// Each item's payload is read as UTF-8 (lossily). When the budget is reached the
/// remaining items are dropped and a `[... N more recalled item(s) omitted ...]`
/// marker is appended (summarize-on-overflow, the cheap form). `None` budget
/// renders every item. The byte-based estimate avoids a tokenizer dependency, so
/// it is a safe lower bound on a real model's count, not an exact one.
pub fn to_context_block(items: &[MemoryItem], token_budget: Option<usize>) -> String {
    let mut block = String::new();
    let mut spent = 0usize;
    let mut rendered = 0usize;
    for item in items {
        let text = String::from_utf8_lossy(&item.payload);
        let cost = estimate_tokens(&text);
        if let Some(budget) = token_budget
            && rendered > 0
            && spent + cost > budget
        {
            break;
        }
        if rendered > 0 {
            block.push_str("\n\n");
        }
        block.push_str(&text);
        spent += cost;
        rendered += 1;
    }
    let omitted = items.len() - rendered;
    if omitted > 0 {
        if rendered > 0 {
            block.push_str("\n\n");
        }
        block.push_str(&format!(
            "[... {omitted} more recalled item(s) omitted ...]"
        ));
    }
    block
}

/// A cheap token estimate: about four bytes per token, the common rough proxy
/// when no tokenizer is available. Rounds up so an item never estimates as zero.
fn estimate_tokens(text: &str) -> usize {
    text.len().div_ceil(4)
}

/// How to recall: a result limit, an optional agent filter, and an optional semantic query.
#[derive(Debug, Clone, bon::Builder)]
pub struct MemoryQuery {
    /// Max items to return.
    #[builder(default = 50)]
    pub limit: usize,
    /// An optional context-window budget in tokens, alongside the item-count
    /// `limit`. The recall result is rendered to fit it by
    /// [`to_context_block`], which fills in rank order until the budget is
    /// reached. Advisory: tokens are estimated, not tokenized (no model
    /// dependency in the SDK).
    pub token_budget: Option<usize>,
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

/// The dyn-safe form of [`Memory`], returning boxed futures so an external
/// backend composes into the facade as a trait object. [`Memory`] is RPITIT (via
/// `trait_variant`), so it is not object-safe. A blanket impl adapts any `Memory`,
/// and a custom backend plugs in through
/// [`Laser::memory_custom`](crate::laser::Laser::memory_custom). This mirrors the
/// boxed-future seam pattern the reliable consumer uses for `AgentMiddleware`.
pub trait DynMemory: Send + Sync {
    /// Boxed [`Memory::remember`].
    fn remember<'a>(
        &'a self,
        scope: &'a MemoryScope,
        payload: Vec<u8>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<MemoryId, LaserError>> + Send + 'a>,
    >;
    /// Boxed [`Memory::recall`].
    fn recall<'a>(
        &'a self,
        scope: &'a MemoryScope,
        query: &'a MemoryQuery,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<MemoryItem>, LaserError>> + Send + 'a>,
    >;
    /// Boxed [`Memory::improve`].
    fn improve<'a>(
        &'a self,
        scope: &'a MemoryScope,
        feedback: Feedback,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<MemoryId, LaserError>> + Send + 'a>,
    >;
    /// Boxed [`Memory::forget`].
    fn forget<'a>(
        &'a self,
        scope: &'a MemoryScope,
        id: MemoryId,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), LaserError>> + Send + 'a>>;
}

impl<M: Memory + Send + Sync> DynMemory for M {
    fn remember<'a>(
        &'a self,
        scope: &'a MemoryScope,
        payload: Vec<u8>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<MemoryId, LaserError>> + Send + 'a>,
    > {
        Box::pin(Memory::remember(self, scope, payload))
    }

    fn recall<'a>(
        &'a self,
        scope: &'a MemoryScope,
        query: &'a MemoryQuery,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<MemoryItem>, LaserError>> + Send + 'a>,
    > {
        Box::pin(Memory::recall(self, scope, query))
    }

    fn improve<'a>(
        &'a self,
        scope: &'a MemoryScope,
        feedback: Feedback,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<MemoryId, LaserError>> + Send + 'a>,
    > {
        Box::pin(Memory::improve(self, scope, feedback))
    }

    fn forget<'a>(
        &'a self,
        scope: &'a MemoryScope,
        id: MemoryId,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), LaserError>> + Send + 'a>>
    {
        Box::pin(Memory::forget(self, scope, id))
    }
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

    fn decode(payload: &[u8]) -> Result<Self, LaserError> {
        laser_wire::framing::decode_named(payload)
            .map_err(|error| LaserError::Codec(format!("decode memory entry: {error}")))
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
/// because the log already holds the truth.
pub struct LogMemory {
    laser: Laser,
    // The stream memory records ride. `None` uses the client's default stream.
    stream: Option<String>,
    // The topic memory records ride. Defaults to `AgentTopic::Audit`. A
    // deployment can point it at its own topic (the configurable memory topic).
    topic: Identifier,
    // The namespace prefixing the named-item altitude's keys
    // (set/fetch/update/remove), so two namespaces on one topic never collide.
    // Defaults to the topic's name.
    namespace: String,
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
    // The named-item altitude's point state, folded from the same topic: the
    // latest body per prefixed key. A named record's id is not a ULID, so it
    // decodes as the wire `MemoryRecord` (below), never as a recall item.
    named: std::collections::HashMap<String, Vec<u8>>,
    offsets: Vec<u64>,
}

impl Projection {
    // Fold one audit message in by decoding its typed entry: a `Forget` records a
    // tombstone, a `Feedback` accumulates a weight on its target, an `Item` becomes
    // a recalled item. A record whose id is not a recall id (a named-item key)
    // folds into the point-state map instead. A payload that is not a memory entry
    // is ignored. Pure, so it is unit-tested.
    fn absorb(&mut self, payload: &[u8], provenance: Provenance) {
        // A named-item key is not a ULID, so `MemoryLogEntry` (whose ids are
        // `MemoryId`) fails to decode it, so fall through to the string-id wire
        // record, which the plane folds too.
        let Ok(entry) = laser_wire::framing::decode_named::<MemoryLogEntry>(payload) else {
            self.absorb_named(payload);
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
                    source: None,
                    kind,
                    score: None,
                    signals: Vec::new(),
                });
            }
        }
    }

    // Fold a named-item record (a keyed set or forget). The record is the wire
    // `MemoryRecord`, byte-identical to `MemoryLogEntry` but with a string id, so
    // the named-item write path and the plane's fold agree on the bytes.
    fn absorb_named(&mut self, payload: &[u8]) {
        let Ok(record) =
            laser_wire::framing::decode_named::<laser_wire::memory::MemoryRecord>(payload)
        else {
            return;
        };
        match record {
            laser_wire::memory::MemoryRecord::Item { id, body, .. } => {
                self.named.insert(id, body);
            }
            laser_wire::memory::MemoryRecord::Forget { target } => {
                self.named.remove(&target);
            }
            laser_wire::memory::MemoryRecord::Feedback { .. } => {}
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
            stream: None,
            topic: AgentTopic::Audit.as_identifier(),
            namespace: AgentTopic::Audit.name().unwrap_or("memory").to_owned(),
            projection: Mutex::new(Projection::default()),
        }
    }

    /// Like [`new`](Self::new) but carries a caller-named namespace: the prefix
    /// the named-item altitude keys on. The records still ride the default
    /// `AgentTopic::Audit`. This is what [`Laser::memory`] builds.
    pub fn in_namespace(laser: Laser, namespace: impl Into<String>) -> Self {
        Self {
            laser,
            stream: None,
            topic: AgentTopic::Audit.as_identifier(),
            namespace: namespace.into(),
            projection: Mutex::new(Projection::default()),
        }
    }

    /// Like [`new`](Self::new) but writes and folds a caller-named memory topic
    /// instead of the default `AgentTopic::Audit`. The configurable memory
    /// stream: one deployment can keep several isolated memory topics. The
    /// namespace defaults to the topic's name.
    pub fn on_topic(laser: Laser, topic: Identifier) -> Self {
        Self::on_stream_topic(laser, None, topic)
    }

    /// Like [`on_topic`](Self::on_topic) but on a caller-named stream too, not
    /// the client's default. `None` keeps the default stream.
    pub fn on_stream_topic(laser: Laser, stream: Option<String>, topic: Identifier) -> Self {
        let namespace = topic.get_string_value().unwrap_or_default();
        Self {
            laser,
            stream,
            topic,
            namespace,
            projection: Mutex::new(Projection::default()),
        }
    }

    /// [`on_topic`](Self::on_topic) from a topic name, validating it.
    pub fn on_topic_named(laser: Laser, topic: &str) -> Result<Self, LaserError> {
        Ok(Self::on_topic(laser, Identifier::named(topic)?))
    }

    /// [`on_stream_topic`](Self::on_stream_topic) from a topic name, validating
    /// it.
    pub fn on_stream_topic_named(
        laser: Laser,
        stream: Option<String>,
        topic: &str,
    ) -> Result<Self, LaserError> {
        Ok(Self::on_stream_topic(
            laser,
            stream,
            Identifier::named(topic)?,
        ))
    }

    async fn govern_payload(
        &self,
        provenance: &Provenance,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, LaserError> {
        let topic = self.topic.to_string();
        let stream = match &self.stream {
            Some(stream) => stream.as_str(),
            None => self.laser.stream_required()?,
        };
        let action = crate::govern::GovernedAction {
            kind: crate::govern::ActionKind::MemoryWrite,
            stream,
            topic: &topic,
            source: provenance.agent.as_ref().map(|agent| agent.as_str()),
            target: None,
            conversation: Some(provenance.conversation_id),
            correlation: None,
            operation: None,
            tool: None,
            on_behalf_of: None,
            purpose: None,
            data_classification: None,
            payload: &payload,
            signed: false,
            counters: crate::govern::ActionCounters::default(),
        };
        Ok(self.laser.govern(action).await?.unwrap_or(payload))
    }

    // Publish an encoded entry to this memory's stream and topic, keyed by the
    // conversation so a scope's records share a partition.
    async fn publish_unchecked(
        &self,
        provenance: &Provenance,
        scope: &MemoryScope,
        payload: Vec<u8>,
    ) -> Result<(), LaserError> {
        let mut headers: std::collections::BTreeMap<HeaderKey, HeaderValue> =
            provenance.try_into()?;
        // The memory scope rides as headers so the read view is keyed on the
        // scope, not the topic: one shared topic still resolves every context.
        // The namespace always rides. User and app widen recall when unset.
        put_header(
            &mut headers,
            laser_wire::headers::MEMORY_NAMESPACE,
            &self.namespace,
        )?;
        if let Some(user) = &scope.user {
            put_header(&mut headers, laser_wire::headers::MEMORY_USER, user)?;
        }
        if let Some(app) = &scope.app {
            put_header(&mut headers, laser_wire::headers::MEMORY_APP, app)?;
        }
        let key = provenance.partition_key();
        let topic = self.topic.to_string();
        match &self.stream {
            Some(stream) => {
                self.laser
                    .send_with_headers_on(stream, &topic, payload, headers, Some(&key))
                    .await
            }
            None => {
                self.laser
                    .send_with_headers(&topic, payload, headers, Some(&key))
                    .await
            }
        }
    }

    async fn publish(
        &self,
        provenance: &Provenance,
        scope: &MemoryScope,
        payload: Vec<u8>,
    ) -> Result<(), LaserError> {
        let payload = self.govern_payload(provenance, payload).await?;
        self.publish_unchecked(provenance, scope, payload).await
    }

    fn provenance(scope: &MemoryScope, idempotency_key: String) -> Provenance {
        let mut provenance = Provenance::builder()
            .conversation_id(scope.conversation.unwrap_or_default())
            .build();
        provenance.agent = scope.agent.clone();
        provenance.idempotency_key = Some(idempotency_key);
        provenance
    }

    // Provenance for a named point-state write. Named items carry no conversation,
    // so the partition key is derived from the item's own id: every set, update,
    // and forget for one key then shares a partition and folds in offset order. A
    // default (random) conversation would scatter a key's writes across partitions,
    // and the fold applies partitions in index order, so an older write could win.
    fn named_provenance(id: String) -> Provenance {
        let mut provenance = Provenance::builder()
            .conversation_id(ConversationId::derive(&id))
            .build();
        provenance.idempotency_key = Some(id);
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
        let provenance = Self::provenance(scope, id.to_string());
        let body = self.govern_payload(&provenance, payload).await?;
        let entry = MemoryLogEntry::Item { id, kind, body };
        self.publish_unchecked(&provenance, scope, entry.encode()?)
            .await?;
        Ok(id)
    }

    // The named-item altitude keys point state under `<namespace>/<key>`, so two
    // namespaces on one topic never collide.
    fn named_key(&self, key: &str) -> String {
        format!("{}/{}", self.namespace, key)
    }

    /// Write named point state: publish a keyed item to the topic (a record, like
    /// every memory write) and reflect it in the projection for read-your-writes.
    pub async fn set_named(&self, key: &str, body: Vec<u8>) -> Result<(), LaserError> {
        let id = self.named_key(key);
        let record = laser_wire::memory::MemoryRecord::Item {
            id: id.clone(),
            // Named point state is a fact, and the wire record carries the kind word.
            kind: "fact".to_owned(),
            body: body.clone(),
        };
        let payload = laser_wire::framing::encode_named(&record)
            .map_err(|error| LaserError::Codec(format!("encode named memory item: {error}")))?;
        let scope = MemoryScope::default();
        let provenance = Self::named_provenance(id.clone());
        self.publish(&provenance, &scope, payload).await?;
        self.projection.lock().await.named.insert(id, body);
        Ok(())
    }

    /// Merge-patch named point state (RFC 7386): read the current value, merge
    /// `patch`, and write the result back as one event.
    pub async fn update_named(&self, key: &str, patch: Vec<u8>) -> Result<(), LaserError> {
        // Read-merge-write needs read-your-writes, which the in-process fold gives
        // and the eventually-consistent read view does not, so the merge folds.
        let current = self.fetch_named_folded(key).await?;
        let merged = merge_named(current.as_deref(), &patch)?;
        self.set_named(key, merged).await
    }

    /// Delete named point state: publish a keyed forget (a tombstone the plane
    /// applies as a delete) and drop it from the projection.
    pub async fn forget_named(&self, key: &str) -> Result<(), LaserError> {
        let id = self.named_key(key);
        let record = laser_wire::memory::MemoryRecord::Forget { target: id.clone() };
        let payload = laser_wire::framing::encode_named(&record)
            .map_err(|error| LaserError::Codec(format!("encode named memory forget: {error}")))?;
        let scope = MemoryScope::default();
        let provenance = Self::named_provenance(id.clone());
        self.publish(&provenance, &scope, payload).await?;
        self.projection.lock().await.named.remove(&id);
        Ok(())
    }

    /// Read named point state, folding the topic first so a fresh handle sees what
    /// was written before it.
    pub async fn fetch_named(&self, key: &str) -> Result<Option<Vec<u8>>, LaserError> {
        // Read the managed key-value read view, like recall. Folding the topic is
        // the opt-in `fetch_named_folded`, never the default.
        #[cfg(feature = "kv")]
        {
            self.laser
                .kv(self.namespace.clone())
                .get(self.named_key(key))
                .await
        }
        #[cfg(not(feature = "kv"))]
        {
            let _ = key;
            Err(LaserError::unsupported(
                "memory fetch",
                "named point-state read reads the managed key-value view. Build with the `kv` \
                 feature, or call `fetch_named_folded` to fold the topic in process",
            ))
        }
    }

    /// Read named point state by folding the topic in process, the opt-in path
    /// for small serverless scenarios. Prefer [`fetch_named`](Self::fetch_named),
    /// which reads the materialized key-value view.
    pub async fn fetch_named_folded(&self, key: &str) -> Result<Option<Vec<u8>>, LaserError> {
        self.catch_up().await?;
        let id = self.named_key(key);
        Ok(self.projection.lock().await.named.get(&id).cloned())
    }

    // Drain the audit topic from the projection's saved offsets to the current tail,
    // folding only the new messages. Reads incrementally, not from offset 0.
    //
    // The network polling runs WITHOUT the projection lock held, so a round-trip
    // never blocks a concurrent recall. The lock is taken only to snapshot the
    // frontier and, afterwards, to fold + advance (a CPU-bound critical section). A
    // partition's batch is applied only if no concurrent `catch_up` advanced that
    // partition past our snapshot in the meantime. Otherwise the other caller
    // already folded those messages and re-applying would duplicate items.
    async fn catch_up(&self) -> Result<(), LaserError> {
        let stream = match &self.stream {
            Some(stream) => Identifier::named(stream)?,
            None => Identifier::named(self.laser.stream_required()?)?,
        };
        let topic = self.topic.clone();
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

    /// Recall by folding the topic in process: read every memory record for the
    /// scope's conversation off the log and return the most-recent items. This is
    /// the opt-in path for small, serverless scenarios with no managed read view.
    /// It does not scale: a large topic folds every record, so prefer the default
    /// [`recall`](Memory::recall), which reads the materialized key-value view.
    pub async fn recall_folded(
        &self,
        scope: &MemoryScope,
        query: &MemoryQuery,
    ) -> Result<Vec<MemoryItem>, LaserError> {
        self.catch_up().await?;
        let projection = self.projection.lock().await;
        // `items` is in per-partition arrival order, not globally timestamp-sorted, so
        // `last limit` is the most-recent N only because all of one conversation's
        // messages land on one partition (the audit topic partitions by
        // `conversation_id`, see `Provenance::partition_key`) and are folded in offset
        // order. Narrowing to a single conversation first makes that tail correct.
        // An unset `scope.conversation` WIDENS recall across every conversation
        // (matching the `MemoryScope` widening rule) rather than returning nothing.
        // The tail is then only approximately most-recent, since items interleave
        // across partitions. The recall filter honors the scope it was stored under,
        // and an explicit `query.agent` narrows it further, overriding `scope.agent`.
        let agent_filter = query.agent.as_ref().or(scope.agent.as_ref());
        let mut items: Vec<MemoryItem> = projection
            .items
            .iter()
            .filter(|item| {
                scope
                    .conversation
                    .is_none_or(|conversation| item.provenance.conversation_id == conversation)
            })
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

    /// Recall by reading the managed key-value read view. Scans the memory
    /// namespace narrowed to the scope's conversation (the lens the fold stamps),
    /// rebuilds each item from the stored scope, filters by agent/user/app, and
    /// returns the most-recent by id. Backs the default [`recall`](Memory::recall).
    #[cfg(feature = "kv")]
    async fn recall_kv(
        &self,
        scope: &MemoryScope,
        query: &MemoryQuery,
    ) -> Result<Vec<MemoryItem>, LaserError> {
        let Some(conversation) = scope.conversation else {
            return Ok(Vec::new());
        };
        let kv = self.laser.kv(self.namespace.clone());
        // One conversation's memory is bounded by the lens, so page through it and
        // keep the most-recent `limit`. The scan is key-ascending and ids are
        // time-ordered ULIDs, so the tail after sorting is the most recent.
        let mut entries = Vec::new();
        let mut cursor: Option<Vec<u8>> = None;
        loop {
            let mut scan = kv.scan().conversation(conversation);
            if let Some(resume) = &cursor {
                scan = scan.cursor(resume);
            }
            let page = scan.fetch().await?;
            let done = page.cursor.is_none();
            cursor = page.cursor;
            entries.extend(page.entries);
            if done {
                break;
            }
        }
        let agent_filter = query.agent.as_ref().or(scope.agent.as_ref());
        let mut items: Vec<MemoryItem> = entries
            .into_iter()
            .filter_map(|entry| self.item_from_entry(conversation, entry))
            .filter(|item| {
                agent_filter.is_none_or(|agent| item.provenance.agent.as_ref() == Some(agent))
            })
            .collect();
        items.sort_by_key(|item| item.id);
        if items.len() > query.limit {
            items = items.split_off(items.len() - query.limit);
        }
        Ok(items)
    }

    // Rebuild a memory item from a read-view entry, or `None` when the key is a
    // named-item key (not a recall id) or the row carries no memory scope.
    #[cfg(feature = "kv")]
    fn item_from_entry(
        &self,
        conversation: ConversationId,
        entry: laser_wire::kv::KvEntry,
    ) -> Option<MemoryItem> {
        let id: MemoryId = entry.key_str()?.parse().ok()?;
        let stored = *entry.scope?;
        let kind = stored
            .kind
            .as_deref()
            .map(MemoryKind::from_word)
            .unwrap_or_default();
        let mut provenance = Provenance::builder().conversation_id(conversation).build();
        provenance.agent = stored.agent.as_deref().and_then(|agent| agent.parse().ok());
        Some(MemoryItem {
            id,
            payload: entry.value,
            provenance,
            kind,
            source: stored.source,
            score: None,
            signals: Vec::new(),
        })
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
        // Recall reads the managed key-value read view the deployment materializes
        // from the memory topic, so a huge topic never folds in process. Folding
        // is the opt-in [`recall_folded`](LogMemory::recall_folded), for small
        // serverless scenarios, never the default.
        #[cfg(feature = "kv")]
        {
            self.recall_kv(scope, query).await
        }
        #[cfg(not(feature = "kv"))]
        {
            let _ = (scope, query);
            Err(LaserError::unsupported(
                "memory recall",
                "durable recall reads the managed key-value view. Build with the `kv` feature, \
                 or call `recall_folded` to fold the topic in process",
            ))
        }
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
            .send_agent_as(
                crate::govern::ActionKind::MemoryWrite,
                AgentTopic::Custom(&self.topic),
                entry.encode()?,
                &provenance,
            )
            .await?;
        Ok(id)
    }

    async fn forget(&self, scope: &MemoryScope, id: MemoryId) -> Result<(), LaserError> {
        let entry = MemoryLogEntry::Forget { target: id };
        let provenance = Self::provenance(scope, MemoryId::new().to_string());
        self.laser
            .send_agent_as(
                crate::govern::ActionKind::MemoryWrite,
                AgentTopic::Custom(&self.topic),
                entry.encode()?,
                &provenance,
            )
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

/// Re-scores recall candidates after the first-pass retrieval: the optional
/// second stage of a multi-signal recall pipeline (candidates -> fuse -> rerank
/// -> top_k). The SDK stays model-agnostic, so a cross-encoder, an LLM judge, or
/// a hosted rerank API lives in application code, the same seam as [`Embedder`].
/// `rerank` receives the candidate items already ordered by the first pass and
/// returns them re-ordered (and MAY drop or rescore them).
#[trait_variant::make(Reranker: Send)]
pub trait LocalReranker {
    async fn rerank(
        &self,
        query: &str,
        items: Vec<MemoryItem>,
    ) -> Result<Vec<MemoryItem>, LaserError>;
}

/// Folds a batch of session bodies into one durable summary body. The seam the
/// consolidation summarize pass calls: a model client, a template, or a hosted
/// summarize API lives in application code, the same boundary as [`Embedder`]
/// and [`Reranker`]. The SDK ships no model client, ever.
#[trait_variant::make(Summarizer: Send)]
pub trait LocalSummarizer {
    async fn summarize(&self, bodies: Vec<Vec<u8>>) -> Result<Vec<u8>, LaserError>;
}

/// What one consolidation pass changed. All counts are best-effort and advisory.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConsolidationReport {
    /// Items folded into durable summaries.
    pub summarized: usize,
    /// Items whose recall weight was adjusted from feedback.
    pub reweighted: usize,
    /// Stale items pruned.
    pub pruned: usize,
    /// New facts or edges derived (e.g. graph extraction).
    pub derived: usize,
}

/// Background memory evolution, the "memify" / sleep-time-compute pass: it runs off
/// the log to summarize sessions into durable items, fold feedback into recall
/// weights, prune stale items, and derive new facts and edges (the home for
/// projector-driven graph extraction managed-side). It is the asynchronous
/// counterpart to the synchronous [`Memory::improve`] verb. A seam like
/// [`Embedder`] and [`Reranker`]: the policy lives in application or managed code,
/// so the SDK ships the contract and a deployment fills it.
#[trait_variant::make(Consolidator: Send)]
pub trait LocalConsolidator {
    /// Run one consolidation pass over `scope`, returning what it changed.
    async fn consolidate(&self, scope: &MemoryScope) -> Result<ConsolidationReport, LaserError>;
}

/// The mechanical consolidation pass: keep the most recent `max_items` per scope,
/// prune the rest (oldest first, by the ULID id's creation-time order). A real,
/// dependency-free bound on unbounded memory growth, the "prune" of the memify
/// pass. Reweighting from feedback already happens in the recall backends. The
/// summarize and fact-invalidation passes (closing a superseded graph edge's
/// `valid_to`) need a generation seam and the managed graph, and layer on top of
/// this rather than replacing it.
pub struct DefaultConsolidator<M, S = NoSummarizer> {
    memory: M,
    max_items: usize,
    summarizer: Option<S>,
    prune_summarized: bool,
}

/// The placeholder for a consolidator built without a summarize pass. Never
/// called: `summarizer: None` skips the pass entirely.
pub struct NoSummarizer;

impl Summarizer for NoSummarizer {
    async fn summarize(&self, _bodies: Vec<Vec<u8>>) -> Result<Vec<u8>, LaserError> {
        Err(LaserError::Config(
            "no summarizer was registered on this consolidator",
        ))
    }
}

impl<M> DefaultConsolidator<M> {
    /// Keep at most `max_items` per scope on each pass (the prune half).
    pub fn new(memory: M, max_items: usize) -> Self {
        Self {
            memory,
            max_items,
            summarizer: None,
            prune_summarized: false,
        }
    }
}

impl<M, S> DefaultConsolidator<M, S> {
    /// Add the summarize pass: session-lifetime `Message` items in the scope
    /// are folded through `summarizer` into one durable `Summary` per pass.
    /// The SDK ships no model client, the seam is yours.
    pub fn with_summarizer<S2>(self, summarizer: S2) -> DefaultConsolidator<M, S2> {
        DefaultConsolidator {
            memory: self.memory,
            max_items: self.max_items,
            summarizer: Some(summarizer),
            prune_summarized: self.prune_summarized,
        }
    }

    /// Forget the session items a summarize pass folded, so the durable
    /// summary replaces them instead of joining them.
    #[must_use]
    pub fn prune_summarized(mut self) -> Self {
        self.prune_summarized = true;
        self
    }
}

impl<M: Memory + Sync, S: Summarizer + Sync> Consolidator for DefaultConsolidator<M, S> {
    async fn consolidate(&self, scope: &MemoryScope) -> Result<ConsolidationReport, LaserError> {
        let query = MemoryQuery::builder().limit(CONSOLIDATION_WINDOW).build();
        let mut items = self.memory.recall(scope, &query).await?;
        let mut report = ConsolidationReport::default();

        // The summarize pass: fold this scope's session messages into one
        // durable summary through the injected seam. Only `Message`-kind items
        // participate, durable facts are never re-written by a summarizer. The
        // summary lands through the backend's plain `remember` (the `Memory`
        // trait carries no kind. Tagging `Summary` is the handle's remember
        // builder's concern when the application stores one directly).
        if let Some(summarizer) = &self.summarizer {
            let sessions: Vec<&MemoryItem> = items
                .iter()
                .filter(|item| item.kind == MemoryKind::Message)
                .collect();
            if !sessions.is_empty() {
                let bodies = sessions.iter().map(|item| item.payload.clone()).collect();
                let summary = summarizer.summarize(bodies).await?;
                self.memory.remember(scope, summary).await?;
                report.summarized = sessions.len();
                if self.prune_summarized {
                    let ids: Vec<MemoryId> = sessions.iter().map(|item| item.id).collect();
                    for id in ids {
                        if self.memory.forget(scope, id).await.is_ok() {
                            report.pruned += 1;
                        }
                    }
                    items.retain(|item| item.kind != MemoryKind::Message);
                }
            }
        }

        if items.len() <= self.max_items {
            return Ok(report);
        }
        // Oldest first, by creation-time-ordered id, so the prune drops the oldest.
        items.sort_by_key(|item| item.id);
        let prune_count = items.len() - self.max_items;
        for item in items.into_iter().take(prune_count) {
            if self.memory.forget(scope, item.id).await.is_ok() {
                report.pruned += 1;
            }
        }
        Ok(report)
    }
}

/// Wraps any [`Memory`] with a [`Reranker`] second stage: `recall` runs the inner
/// backend's retrieval, then reorders the candidates through the reranker when the
/// query carries a `semantic` string (the rerank query). `remember` / `improve` /
/// `forget` delegate unchanged. Composition, not configuration: the rerank stage
/// is a wrapper so any backend gains it without a backend-specific flag.
pub struct RerankedMemory<M, R> {
    inner: M,
    reranker: R,
}

impl<M, R> RerankedMemory<M, R> {
    /// Wrap `inner` so its recall is reranked by `reranker`.
    pub fn new(inner: M, reranker: R) -> Self {
        Self { inner, reranker }
    }
}

impl<M: Memory + Sync, R: Reranker + Sync> Memory for RerankedMemory<M, R> {
    async fn remember(
        &self,
        scope: &MemoryScope,
        payload: Vec<u8>,
    ) -> Result<MemoryId, LaserError> {
        self.inner.remember(scope, payload).await
    }

    async fn recall(
        &self,
        scope: &MemoryScope,
        query: &MemoryQuery,
    ) -> Result<Vec<MemoryItem>, LaserError> {
        let candidates = self.inner.recall(scope, query).await?;
        match &query.semantic {
            Some(text) => self.reranker.rerank(text, candidates).await,
            None => Ok(candidates),
        }
    }

    async fn improve(
        &self,
        scope: &MemoryScope,
        feedback: Feedback,
    ) -> Result<MemoryId, LaserError> {
        self.inner.improve(scope, feedback).await
    }

    async fn forget(&self, scope: &MemoryScope, id: MemoryId) -> Result<(), LaserError> {
        self.inner.forget(scope, id).await
    }
}

/// A semantic `Memory` backend: embeds payloads on `remember` and ranks `recall`
/// by cosine similarity to `query.semantic`. The index is in-memory. A durable
/// vector store is a future drop-in behind the same trait.
pub struct VectorMemory<E> {
    embedder: E,
    items: Mutex<Vec<VectorEntry>>,
    governance: Option<VectorGovernance>,
}

#[derive(Clone)]
struct VectorGovernance {
    laser: Laser,
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
        Self::with_governance(embedder, None)
    }

    /// An in-memory semantic memory whose writes run through the governor
    /// enrolled on `laser`. Directly constructed vector memories remain
    /// standalone. `Laser::memory_with(.., MemoryBackend::Vector)` uses this
    /// governed form.
    pub fn governed(laser: Laser, embedder: E) -> Self {
        Self::with_governance(embedder, Some(VectorGovernance { laser }))
    }

    fn with_governance(embedder: E, governance: Option<VectorGovernance>) -> Self {
        Self {
            embedder,
            items: Mutex::new(Vec::new()),
            governance,
        }
    }

    async fn govern_entry(
        &self,
        scope: &MemoryScope,
        entry: MemoryLogEntry,
    ) -> Result<MemoryLogEntry, LaserError> {
        let Some(governance) = &self.governance else {
            return Ok(entry);
        };
        let topic = AgentTopic::Audit.topic_string();
        let payload = match &entry {
            MemoryLogEntry::Item { body, .. } => body.clone(),
            MemoryLogEntry::Forget { .. } | MemoryLogEntry::Feedback { .. } => entry.encode()?,
        };
        let conversation = scope.conversation.unwrap_or_default();
        let action = crate::govern::GovernedAction {
            kind: crate::govern::ActionKind::MemoryWrite,
            stream: governance.laser.stream_required()?,
            topic: &topic,
            source: scope.agent.as_ref().map(|agent| agent.as_str()),
            target: None,
            conversation: Some(conversation),
            correlation: None,
            operation: None,
            tool: None,
            on_behalf_of: None,
            purpose: None,
            data_classification: None,
            payload: &payload,
            signed: false,
            counters: crate::govern::ActionCounters::default(),
        };
        match governance.laser.govern(action).await? {
            Some(modified) => match entry {
                MemoryLogEntry::Item { id, kind, .. } => Ok(MemoryLogEntry::Item {
                    id,
                    kind,
                    body: modified,
                }),
                MemoryLogEntry::Forget { .. } | MemoryLogEntry::Feedback { .. } => {
                    MemoryLogEntry::decode(&modified)
                }
            },
            None => Ok(entry),
        }
    }

    async fn append(
        &self,
        scope: &MemoryScope,
        id: MemoryId,
        kind: MemoryKind,
        payload: Vec<u8>,
    ) -> Result<MemoryId, LaserError>
    where
        E: Embedder + Sync,
    {
        let entry = self
            .govern_entry(
                scope,
                MemoryLogEntry::Item {
                    id,
                    kind,
                    body: payload,
                },
            )
            .await?;
        let MemoryLogEntry::Item { id, kind, body } = entry else {
            return Err(LaserError::Invalid(
                "a memory-item governor modification must remain an item".to_owned(),
            ));
        };
        let embedding = self.embedder.embed(&String::from_utf8_lossy(&body)).await?;
        let mut provenance = Provenance::builder()
            .conversation_id(scope.conversation.unwrap_or_default())
            .build();
        provenance.agent = scope.agent.clone();
        provenance.idempotency_key = Some(id.to_string());
        let item = MemoryItem {
            id,
            payload: body,
            provenance,
            kind,
            score: None,
            signals: Vec::new(),
            source: None,
        };
        let mut items = self.items.lock().await;
        if items.iter().any(|entry| entry.id == id) {
            return Ok(id);
        }
        items.push(VectorEntry {
            id,
            scope: scope.clone(),
            embedding,
            item,
            feedback: 0.0,
        });
        Ok(id)
    }
}

impl<E: Embedder + Sync> Memory for VectorMemory<E> {
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
        // Which signals this strategy fuses. `Auto`/`Semantic`/`Hybrid` use the
        // embedding. `Keyword`/`Hybrid` add lexical overlap. `Keyword` skips the
        // embed call entirely (no model round-trip when only lexical is wanted).
        let wants_semantic = !matches!(query.strategy, RecallStrategy::Keyword);
        let wants_keyword = matches!(
            query.strategy,
            RecallStrategy::Keyword | RecallStrategy::Hybrid
        );
        // Embed the query before locking so the network/model call never holds the lock.
        let query_embedding = match (&query.semantic, wants_semantic) {
            (Some(text), true) => Some(self.embedder.embed(text).await?),
            _ => None,
        };
        let query_tokens = match (&query.semantic, wants_keyword) {
            (Some(text), true) => Some(tokenize(text)),
            _ => None,
        };
        let agent_filter = query.agent.as_ref().or(scope.agent.as_ref());
        let items = self.items.lock().await;
        let matched: Vec<&VectorEntry> = items
            .iter()
            .filter(|entry| {
                scope
                    .conversation
                    .is_none_or(|c| entry.scope.conversation == Some(c))
                    && agent_filter.is_none_or(|a| entry.scope.agent.as_ref() == Some(a))
                    && scope
                        .user
                        .as_ref()
                        .is_none_or(|u| entry.scope.user.as_ref() == Some(u))
                    && scope
                        .app
                        .as_ref()
                        .is_none_or(|a| entry.scope.app.as_ref() == Some(a))
            })
            .collect();

        let any_feedback = matched.iter().any(|entry| entry.feedback != 0.0);

        // Fuse the active signals into one score: semantic similarity, lexical
        // overlap, and the accumulated feedback boost. A signal that is off
        // contributes nothing, so `Auto` with a query reduces to the prior
        // similarity-plus-feedback behavior exactly.
        let score = |entry: &VectorEntry| {
            let mut total = entry.feedback;
            if let Some(query_embedding) = &query_embedding {
                total += cosine(query_embedding, &entry.embedding);
            }
            if let Some(query_tokens) = &query_tokens {
                total += keyword_score(query_tokens, &entry.item.payload);
            }
            total
        };

        let any_query_signal = query_embedding.is_some() || query_tokens.is_some();
        if any_query_signal || any_feedback {
            // Score each entry once, then sort, so the cosine is not recomputed
            // per comparison.
            let mut scored: Vec<(f32, &VectorEntry)> =
                matched.iter().map(|&entry| (score(entry), entry)).collect();
            scored.sort_by(|a, b| b.0.total_cmp(&a.0));
            Ok(scored
                .into_iter()
                .take(query.limit)
                .map(|(score, entry)| {
                    let mut item = entry.item.clone();
                    item.score = Some(score);
                    item
                })
                .collect())
        } else {
            // No query signal and no feedback: the most recent `limit`.
            let start = matched.len().saturating_sub(query.limit);
            Ok(matched[start..]
                .iter()
                .map(|entry| entry.item.clone())
                .collect())
        }
    }

    async fn improve(
        &self,
        scope: &MemoryScope,
        feedback: Feedback,
    ) -> Result<MemoryId, LaserError> {
        let entry = self
            .govern_entry(
                scope,
                MemoryLogEntry::Feedback {
                    target: feedback.target,
                    weight: feedback.weight,
                },
            )
            .await?;
        let MemoryLogEntry::Feedback { target, weight } = entry else {
            return Err(LaserError::Invalid(
                "a memory-feedback governor modification must remain feedback".to_owned(),
            ));
        };
        // Accumulate the weight on its target under the one `items` lock, so the
        // next recall reranks. A signal for an unknown target is a no-op.
        let mut items = self.items.lock().await;
        if let Some(entry) = items.iter_mut().find(|entry| entry.id == target) {
            entry.feedback += weight;
        }
        Ok(MemoryId::new())
    }

    async fn forget(&self, scope: &MemoryScope, id: MemoryId) -> Result<(), LaserError> {
        let entry = self
            .govern_entry(scope, MemoryLogEntry::Forget { target: id })
            .await?;
        let MemoryLogEntry::Forget { target } = entry else {
            return Err(LaserError::Invalid(
                "a memory-forget governor modification must remain a tombstone".to_owned(),
            ));
        };
        self.items.lock().await.retain(|entry| entry.id != target);
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

// Lowercase alphanumeric word tokens, the unit the in-process keyword signal
// scores on. A real backend uses BM25 over an inverted index. This is the
// dependency-free equivalent for `VectorMemory`.
fn tokenize(text: &str) -> HashSet<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(str::to_ascii_lowercase)
        .collect()
}

// Insert one memory scope header, mapping an over-long key or value to a codec error.
fn put_header(
    headers: &mut std::collections::BTreeMap<HeaderKey, HeaderValue>,
    key: &str,
    value: &str,
) -> Result<(), LaserError> {
    let key = HeaderKey::from_str(key)
        .map_err(|error| LaserError::Codec(format!("memory header key {key}: {error}")))?;
    let value = HeaderValue::from_str(value)
        .map_err(|error| LaserError::Codec(format!("memory header value: {error}")))?;
    headers.insert(key, value);
    Ok(())
}

/// Fuse several signals' ranked item lists into one by reciprocal rank: each
/// item scores the sum of `1 / (60 + rank)` over the signals that surfaced it
/// (60 is the standard RRF damping constant), so agreement between signals
/// outranks a single signal's top hit. Pure and portable across backends: the
/// per-signal attribution is preserved on each fused item's `signals`.
pub fn fuse_reciprocal_rank(signals: Vec<Vec<MemoryItem>>, limit: usize) -> Vec<MemoryItem> {
    const RRF_K: f32 = 60.0;
    let mut fused: Vec<MemoryItem> = Vec::new();
    for ranked in signals {
        for item in ranked {
            let contribution = item
                .signals
                .first()
                .map_or(0.0, |signal| 1.0 / (RRF_K + signal.rank as f32));
            match fused.iter_mut().find(|held| held.id == item.id) {
                Some(held) => {
                    held.score = Some(held.score.unwrap_or(0.0) + contribution);
                    held.signals.extend(item.signals);
                }
                None => {
                    let mut item = item;
                    item.score = Some(contribution);
                    fused.push(item);
                }
            }
        }
    }
    fused.sort_by(|a, b| b.score.unwrap_or(0.0).total_cmp(&a.score.unwrap_or(0.0)));
    fused.truncate(limit);
    fused
}

// The lexical-overlap signal for the hybrid recall: the fraction of the query's
// tokens that appear in the item body, in [0, 1] so it composes on the same scale
// as cosine similarity. Returns 0 when the body is not UTF-8 or the query is empty.
fn keyword_score(query_tokens: &HashSet<String>, body: &[u8]) -> f32 {
    if query_tokens.is_empty() {
        return 0.0;
    }
    let Ok(text) = std::str::from_utf8(body) else {
        return 0.0;
    };
    let body_tokens = tokenize(text);
    let hits = query_tokens
        .iter()
        .filter(|token| body_tokens.contains(*token))
        .count();
    hits as f32 / query_tokens.len() as f32
}

/// The backend a memory handle runs on. [`Auto`](Self::Auto) is the default and
/// what [`Laser::memory`] uses. There is one durable model: memory over the
/// stream. [`Vector`](Self::Vector) is the in-process similarity index for tests
/// and offline recall.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MemoryBackend {
    /// Agent memory over the stream: every write publishes to the memory topic
    /// (the durable, replayable audit) that a deployment materializes into the
    /// versioned key-value read view recall serves. The default.
    #[default]
    Auto,
    /// The same durable stream model as [`Auto`](Self::Auto), named explicitly.
    Log,
    /// An in-process vector index: embeds on remember, ranks recall by
    /// similarity, gone on drop. Needs an embedder registered with
    /// [`MemoryHandle::embedder`] before the first verb.
    Vector,
}

/// Configures a memory topic, built with [`Laser::memory_topic`]. Tune the
/// stream, partition count, and message-expiry, then [`build`](Self::build)
/// ensures the topic and returns the handle.
pub struct MemoryTopicBuilder {
    laser: Laser,
    stream: Option<String>,
    topic: String,
    partitions: u32,
    ttl: Option<std::time::Duration>,
}

impl MemoryTopicBuilder {
    /// The stream the memory topic lives on (default the client's default
    /// stream). Names an explicit stream so memory can be isolated from data.
    #[must_use]
    pub fn stream(mut self, stream: impl Into<String>) -> Self {
        self.stream = Some(stream.into());
        self
    }

    /// The topic's partition count (default 1). Each scope's records share a
    /// partition key, so more partitions spread scopes, never split one scope.
    #[must_use]
    pub fn partitions(mut self, partitions: u32) -> Self {
        self.partitions = partitions.max(1);
        self
    }

    /// Expire records on the stream after this long (default
    /// [`DEFAULT_MEMORY_TOPIC_TTL`], thirty days), bounding the audit history.
    /// The materialized read view keeps its own retention, so this only sets how
    /// long the raw history lives on the topic.
    #[must_use]
    pub fn ttl(mut self, ttl: std::time::Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Keep records on the stream indefinitely (until topic retention rotates
    /// them out), opting out of the default message-expiry.
    #[must_use]
    pub fn no_expiry(mut self) -> Self {
        self.ttl = None;
        self
    }

    /// Ensure the stream and topic (partitions and expiry) and return memory
    /// on it.
    pub async fn build(self) -> Result<MemoryHandle, LaserError> {
        let stream = match &self.stream {
            Some(stream) => stream.clone(),
            None => self.laser.stream_required()?.to_owned(),
        };
        let expiry = match self.ttl {
            Some(ttl) => IggyExpiry::ExpireDuration(IggyDuration::from(ttl)),
            None => IggyExpiry::NeverExpire,
        };
        self.laser
            .ensure_topic_on_with(&stream, &self.topic, self.partitions, expiry)
            .await?;
        let topic = Identifier::named(&self.topic)?;
        Ok(MemoryHandle::Log(LogMemory::on_stream_topic(
            self.laser,
            self.stream,
            topic,
        )))
    }
}

/// The front door to agent memory: remember, recall, improve, forget. Built by
/// [`Laser::memory`], or [`Laser::memory_with`] for an explicit
/// [`MemoryBackend`]. Hold one instance per namespace and reuse it so recall
/// stays incremental across calls.
// The log variant carries its cursor inline (the durable path stays
// allocation-free), so it is larger than the pointer-sized vector variant. A
// handle is built once per `memory()` call and never stored in bulk, so the
// stack size of the largest variant is irrelevant here.
#[allow(clippy::large_enum_variant)]
pub enum MemoryHandle {
    /// The durable stream model: every write publishes to the memory topic, and
    /// a deployment materializes it into the versioned key-value read view.
    Log(LogMemory),
    /// The in-process vector backend, shared so the handle stays cheap to
    /// clone while the index lives across calls.
    Vector(std::sync::Arc<VectorMemory<SharedEmbedder>>),
    /// Any backend wrapped with a rerank second stage (see
    /// [`embedder`](Self::embedder) / [`reranker`](Self::reranker)).
    Reranked {
        inner: Box<MemoryHandle>,
        reranker: SharedReranker,
    },
    /// An external durable backend plugged in through
    /// [`Laser::memory_custom`](crate::laser::Laser::memory_custom).
    Custom(std::sync::Arc<dyn DynMemory>),
}

/// A cloneable, type-erased [`Embedder`], how a handle owns the seam without a
/// generic parameter. Build one implicitly through
/// [`MemoryHandle::embedder`].
#[derive(Clone)]
pub struct SharedEmbedder(std::sync::Arc<dyn DynEmbedder>);

impl Embedder for SharedEmbedder {
    async fn embed(&self, text: &str) -> Result<Vec<f32>, LaserError> {
        self.0.embed_dyn(text).await
    }
}

trait DynEmbedder: Send + Sync {
    fn embed_dyn<'a>(
        &'a self,
        text: &'a str,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<Vec<f32>, LaserError>> + Send + 'a>>;
}

impl<E: Embedder + Sync> DynEmbedder for E {
    fn embed_dyn<'a>(
        &'a self,
        text: &'a str,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<Vec<f32>, LaserError>> + Send + 'a>> {
        Box::pin(self.embed(text))
    }
}

/// A cloneable, type-erased [`Reranker`], the rerank sibling of
/// [`SharedEmbedder`].
#[derive(Clone)]
pub struct SharedReranker(std::sync::Arc<dyn DynReranker>);

impl Reranker for SharedReranker {
    async fn rerank(
        &self,
        query: &str,
        items: Vec<MemoryItem>,
    ) -> Result<Vec<MemoryItem>, LaserError> {
        self.0.rerank_dyn(query, items).await
    }
}

trait DynReranker: Send + Sync {
    fn rerank_dyn<'a>(
        &'a self,
        query: &'a str,
        items: Vec<MemoryItem>,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<Vec<MemoryItem>, LaserError>> + Send + 'a>>;
}

impl<R: Reranker + Sync> DynReranker for R {
    fn rerank_dyn<'a>(
        &'a self,
        query: &'a str,
        items: Vec<MemoryItem>,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<Vec<MemoryItem>, LaserError>> + Send + 'a>>
    {
        Box::pin(self.rerank(query, items))
    }
}

impl MemoryHandle {
    /// Which backend this handle resolved to: `"log"` for the durable stream
    /// model (the default), `"vector"` for the in-process similarity index.
    #[must_use]
    pub fn backend(&self) -> &'static str {
        match self {
            Self::Log(_) => "log",
            Self::Vector(_) => "vector",
            Self::Reranked { inner, .. } => inner.backend(),
            Self::Custom(_) => "custom",
        }
    }

    /// Register the embedding seam for similarity recall. It configures the
    /// in-process vector index. The durable log model recalls by recency, so an
    /// embedder on a log handle is unused (build with
    /// `memory_with(.., MemoryBackend::Vector)` for similarity recall).
    #[must_use]
    pub fn embedder(self, embedder: impl Embedder + Sync + 'static) -> Self {
        self.embedder_shared(SharedEmbedder(std::sync::Arc::new(embedder)))
    }

    fn embedder_shared(self, shared: SharedEmbedder) -> Self {
        match self {
            Self::Vector(memory) => Self::Vector(std::sync::Arc::new(
                VectorMemory::with_governance(shared, memory.governance.clone()),
            )),
            Self::Reranked { inner, reranker } => Self::Reranked {
                inner: Box::new(inner.embedder_shared(shared)),
                reranker,
            },
            Self::Log(memory) => Self::Log(memory),
            // A custom backend owns its own retrieval. An embedder does not apply.
            Self::Custom(memory) => Self::Custom(memory),
        }
    }

    /// Wrap recall with a rerank second stage, exactly as [`RerankedMemory`]
    /// does: the backend retrieves, the reranker reorders when the query
    /// carries its `semantic` text. Composition, not configuration.
    #[must_use]
    pub fn reranker(self, reranker: impl Reranker + Sync + 'static) -> Self {
        Self::Reranked {
            inner: Box::new(self),
            reranker: SharedReranker(std::sync::Arc::new(reranker)),
        }
    }
}

impl Laser {
    /// Agent memory in `namespace`: remember facts, recall the relevant ones,
    /// improve their ranking with feedback, and forget what is stale. One model -
    /// every write publishes to the memory topic (the durable, replayable audit)
    /// that a deployment materializes into the versioned key-value read view.
    /// `namespace` prefixes the named-item altitude's keys
    /// ([`set`](MemoryHandle::set) / [`fetch`](MemoryHandle::fetch)), which ride
    /// the same topic as every other write. For an isolated, per-topic memory stream use
    /// [`memory_on_topic`](Self::memory_on_topic) or
    /// [`memory_topic`](Self::memory_topic). For similarity recall build
    /// [`memory_with`](Self::memory_with) with [`MemoryBackend::Vector`]. For the
    /// knowledge graph use [`graph`](Self::graph).
    pub fn memory(&self, namespace: impl Into<String>) -> MemoryHandle {
        self.memory_with(namespace, MemoryBackend::Auto)
    }

    /// Agent memory in a caller-named topic, so a deployment can keep several
    /// isolated memory streams. The same verbs as [`memory`](Self::memory).
    /// Ensure the topic up front like any other, for example
    /// `laser.topic(name).ensure(partitions)`.
    pub fn memory_on_topic(&self, topic: impl AsRef<str>) -> Result<MemoryHandle, LaserError> {
        let topic = Identifier::named(topic.as_ref())?;
        Ok(MemoryHandle::Log(LogMemory::on_topic(self.clone(), topic)))
    }

    /// Configure a memory topic before use: pick its name, partition count, and
    /// message-expiry, then `build` to ensure it and get the handle. Records for
    /// one scope always share a partition key (the conversation), so a
    /// multi-partition topic keeps each scope's history in order while spreading
    /// scopes across partitions.
    pub fn memory_topic(&self, topic: impl Into<String>) -> MemoryTopicBuilder {
        MemoryTopicBuilder {
            laser: self.clone(),
            stream: None,
            topic: topic.into(),
            partitions: 1,
            ttl: Some(DEFAULT_MEMORY_TOPIC_TTL),
        }
    }

    /// Memory in `namespace` on an explicit [`MemoryBackend`]. `Auto` and `Log`
    /// are the durable stream model [`memory`](Self::memory) uses. `Vector` is
    /// the in-process similarity index (register an embedder with
    /// [`MemoryHandle::embedder`]) for tests and offline recall.
    pub fn memory_with(
        &self,
        namespace: impl Into<String>,
        backend: MemoryBackend,
    ) -> MemoryHandle {
        let namespace = namespace.into();
        match backend {
            MemoryBackend::Auto | MemoryBackend::Log => {
                MemoryHandle::Log(LogMemory::in_namespace(self.clone(), namespace))
            }
            MemoryBackend::Vector => {
                let _ = namespace;
                MemoryHandle::Vector(std::sync::Arc::new(VectorMemory::governed(
                    self.clone(),
                    SharedEmbedder(std::sync::Arc::new(MaybeEmbedder(None))),
                )))
            }
        }
    }

    /// A memory handle over a custom [`DynMemory`]
    /// backend, so an external durable store plugs into the facade as a
    /// first-class backend beyond the `Auto`/`Log`/`Vector` built-ins. The backend
    /// owns its own scoping and id minting. The four verbs route straight to it.
    /// Any type implementing [`Memory`] qualifies via the
    /// blanket `DynMemory` impl.
    pub fn memory_custom(
        &self,
        backend: std::sync::Arc<dyn crate::memory::DynMemory>,
    ) -> MemoryHandle {
        MemoryHandle::Custom(backend)
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
            MemoryHandle::Vector(memory) => memory.append(scope, id, kind, payload).await,
            MemoryHandle::Reranked { inner, .. } => {
                append_id_boxed(inner, scope, id, kind, payload).await
            }
            // `remember` special-cases Custom before reaching here, but the match
            // must be exhaustive: delegate, letting the backend mint its own id.
            MemoryHandle::Custom(memory) => memory.remember(scope, payload).await,
        }
    }

    /// The simple altitude: write named point state under your own `key`.
    /// `remember`/`recall` address items by content and relevance.
    /// `set`/`fetch`/`update`/`remove` address them by name, the working-note
    /// shape ("the current plan", "the user's tier"). Every write is a durable
    /// event on the memory topic, like the rest of memory, so a deployment
    /// materializes it and other consumers see it. The in-process vector handle
    /// has no key space and refuses typed.
    pub async fn set(
        &self,
        key: impl AsRef<str>,
        payload: impl Into<Vec<u8>>,
    ) -> Result<(), LaserError> {
        self.named_memory("set")?
            .set_named(key.as_ref(), payload.into())
            .await
    }

    /// Point-read the named item written by [`set`](Self::set), or `None`.
    pub async fn fetch(&self, key: impl AsRef<str>) -> Result<Option<Vec<u8>>, LaserError> {
        self.named_memory("fetch")?.fetch_named(key.as_ref()).await
    }

    /// Point-read the named item by folding the topic in process, the opt-in path
    /// for a deployment with no managed read view. Prefer [`fetch`](Self::fetch),
    /// which reads the materialized key-value view.
    pub async fn fetch_folded(&self, key: impl AsRef<str>) -> Result<Option<Vec<u8>>, LaserError> {
        self.named_memory("fetch_folded")?
            .fetch_named_folded(key.as_ref())
            .await
    }

    /// Merge-patch the named item (RFC 7386 over a JSON value): fields in
    /// `patch` overwrite, `null` removes. Reads the current value, merges, and
    /// writes the result back as one event. For a whole-value overwrite use
    /// [`set`](Self::set).
    pub async fn update(
        &self,
        key: impl AsRef<str>,
        patch: impl Into<Vec<u8>>,
    ) -> Result<(), LaserError> {
        self.named_memory("update")?
            .update_named(key.as_ref(), patch.into())
            .await
    }

    /// Delete the named item. Idempotent: removing an absent key is fine.
    pub async fn remove(&self, key: impl AsRef<str>) -> Result<(), LaserError> {
        self.named_memory("remove")?
            .forget_named(key.as_ref())
            .await
    }

    // The durable memory the named-item altitude rides. Only the log-backed
    // handle has a key space, and the vector handle refuses typed.
    fn named_memory(&self, verb: &str) -> Result<&LogMemory, LaserError> {
        match self {
            MemoryHandle::Log(memory) => Ok(memory),
            MemoryHandle::Reranked { inner, .. } => inner.named_memory(verb),
            other => Err(LaserError::unsupported(
                "memory",
                format!(
                    "{verb}(key) is the named-item altitude and needs the durable memory handle \
                     (this handle resolved to `{}`)",
                    other.backend()
                ),
            )),
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

    /// Recall by folding the topic in process (the opt-in behind
    /// [`RecallBuilder::folded`]). The log backend folds, the vector backend is
    /// already in process, and a reranked backend folds its inner then reranks.
    pub async fn recall_folded(
        &self,
        scope: &MemoryScope,
        query: &MemoryQuery,
    ) -> Result<Vec<MemoryItem>, LaserError> {
        match self {
            MemoryHandle::Log(memory) => memory.recall_folded(scope, query).await,
            MemoryHandle::Vector(memory) => Memory::recall(memory.as_ref(), scope, query).await,
            MemoryHandle::Reranked { inner, .. } => recall_folded_boxed(inner, scope, query).await,
            // A custom backend has no separate in-process fold: its own recall is
            // the closest, so folded recall delegates to it.
            MemoryHandle::Custom(memory) => memory.recall(scope, query).await,
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
            folded: false,
        }
    }

    /// The one-call context altitude: recall `conversation`'s most relevant
    /// items and render them as one prompt-ready block under an optional token
    /// budget (see [`to_context_block`]). The 90% case above the deep
    /// [`recall`](Self::recall) builder: assembled, budgeted, ready to prompt.
    pub async fn context(
        &self,
        conversation: ConversationId,
        token_budget: Option<usize>,
    ) -> Result<String, LaserError> {
        let items = self.recall(conversation).fetch().await?;
        Ok(to_context_block(&items, token_budget))
    }

    /// One consolidation pass over `scope` with the default policy: keep the
    /// newest `max_items`, prune the rest. For the summarize and invalidation
    /// passes build a [`DefaultConsolidator`] with its seams and run it
    /// directly. This is the one-call altitude.
    pub async fn consolidate(
        &self,
        scope: &MemoryScope,
        max_items: usize,
    ) -> Result<ConsolidationReport, LaserError> {
        Consolidator::consolidate(&DefaultConsolidator::new(self, max_items), scope).await
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
        // A custom backend owns its own id and kind, so it does not route through
        // `append_id` (which mints a Fact for the built-in log/vector handles).
        if let MemoryHandle::Custom(memory) = self {
            return memory.remember(scope, payload).await;
        }
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
            MemoryHandle::Vector(memory) => Memory::recall(memory.as_ref(), scope, query).await,
            MemoryHandle::Reranked { inner, reranker } => {
                let items = recall_boxed(inner, scope, query).await?;
                match &query.semantic {
                    Some(text) => Reranker::rerank(reranker, text, items).await,
                    None => Ok(items),
                }
            }
            MemoryHandle::Custom(memory) => memory.recall(scope, query).await,
        }
    }

    async fn improve(
        &self,
        scope: &MemoryScope,
        feedback: Feedback,
    ) -> Result<MemoryId, LaserError> {
        match self {
            MemoryHandle::Log(memory) => Memory::improve(memory, scope, feedback).await,
            MemoryHandle::Vector(memory) => Memory::improve(memory.as_ref(), scope, feedback).await,
            MemoryHandle::Reranked { inner, .. } => improve_boxed(inner, scope, feedback).await,
            MemoryHandle::Custom(memory) => memory.improve(scope, feedback).await,
        }
    }

    async fn forget(&self, scope: &MemoryScope, id: MemoryId) -> Result<(), LaserError> {
        match self {
            MemoryHandle::Log(memory) => Memory::forget(memory, scope, id).await,
            MemoryHandle::Vector(memory) => Memory::forget(memory.as_ref(), scope, id).await,
            MemoryHandle::Reranked { inner, .. } => forget_boxed(inner, scope, id).await,
            MemoryHandle::Custom(memory) => memory.forget(scope, id).await,
        }
    }
}

impl Memory for &MemoryHandle {
    async fn remember(
        &self,
        scope: &MemoryScope,
        payload: Vec<u8>,
    ) -> Result<MemoryId, LaserError> {
        Memory::remember(*self, scope, payload).await
    }

    async fn recall(
        &self,
        scope: &MemoryScope,
        query: &MemoryQuery,
    ) -> Result<Vec<MemoryItem>, LaserError> {
        Memory::recall(*self, scope, query).await
    }

    async fn improve(
        &self,
        scope: &MemoryScope,
        feedback: Feedback,
    ) -> Result<MemoryId, LaserError> {
        Memory::improve(*self, scope, feedback).await
    }

    async fn forget(&self, scope: &MemoryScope, id: MemoryId) -> Result<(), LaserError> {
        Memory::forget(*self, scope, id).await
    }
}

/// A cloneable, type-erased [`Consolidator`], how the agent builder's periodic
/// tick owns one without a generic parameter.
#[derive(Clone)]
pub struct SharedConsolidator(std::sync::Arc<dyn DynConsolidator>);

impl SharedConsolidator {
    /// Erase `consolidator` for shared ownership.
    pub fn new(consolidator: impl Consolidator + Sync + 'static) -> Self {
        Self(std::sync::Arc::new(consolidator))
    }
}

impl Consolidator for SharedConsolidator {
    async fn consolidate(&self, scope: &MemoryScope) -> Result<ConsolidationReport, LaserError> {
        self.0.consolidate_dyn(scope).await
    }
}

trait DynConsolidator: Send + Sync {
    fn consolidate_dyn<'a>(
        &'a self,
        scope: &'a MemoryScope,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<ConsolidationReport, LaserError>> + Send + 'a>>;
}

impl<C: Consolidator + Sync> DynConsolidator for C {
    fn consolidate_dyn<'a>(
        &'a self,
        scope: &'a MemoryScope,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<ConsolidationReport, LaserError>> + Send + 'a>>
    {
        Box::pin(Consolidator::consolidate(self, scope))
    }
}

// Boxed recursion helpers: a `Reranked` handle wraps another handle, and the
// trait's anonymous futures cannot reference themselves, so the recursive hop
// goes through one boxed indirection per wrapper level.
fn append_id_boxed<'a>(
    inner: &'a MemoryHandle,
    scope: &'a MemoryScope,
    id: MemoryId,
    kind: MemoryKind,
    payload: Vec<u8>,
) -> std::pin::Pin<Box<dyn Future<Output = Result<MemoryId, LaserError>> + Send + 'a>> {
    Box::pin(inner.append_id(scope, id, kind, payload))
}

fn recall_boxed<'a>(
    inner: &'a MemoryHandle,
    scope: &'a MemoryScope,
    query: &'a MemoryQuery,
) -> std::pin::Pin<Box<dyn Future<Output = Result<Vec<MemoryItem>, LaserError>> + Send + 'a>> {
    Box::pin(Memory::recall(inner, scope, query))
}

fn recall_folded_boxed<'a>(
    inner: &'a MemoryHandle,
    scope: &'a MemoryScope,
    query: &'a MemoryQuery,
) -> std::pin::Pin<Box<dyn Future<Output = Result<Vec<MemoryItem>, LaserError>> + Send + 'a>> {
    Box::pin(inner.recall_folded(scope, query))
}

fn improve_boxed<'a>(
    inner: &'a MemoryHandle,
    scope: &'a MemoryScope,
    feedback: Feedback,
) -> std::pin::Pin<Box<dyn Future<Output = Result<MemoryId, LaserError>> + Send + 'a>> {
    Box::pin(Memory::improve(inner, scope, feedback))
}

fn forget_boxed<'a>(
    inner: &'a MemoryHandle,
    scope: &'a MemoryScope,
    id: MemoryId,
) -> std::pin::Pin<Box<dyn Future<Output = Result<(), LaserError>> + Send + 'a>> {
    Box::pin(Memory::forget(inner, scope, id))
}

// Merge `patch` into the named item's current value by RFC 7386 (JSON merge-patch)
// and return the bytes to write back. An absent current value merges onto null.
fn merge_named(current: Option<&[u8]>, patch: &[u8]) -> Result<Vec<u8>, LaserError> {
    let patch: serde_json::Value = serde_json::from_slice(patch)
        .map_err(|error| LaserError::Codec(format!("update patch is not JSON: {error}")))?;
    let mut base: serde_json::Value = match current {
        Some(payload) => serde_json::from_slice(payload)
            .map_err(|error| LaserError::Codec(format!("stored value is not JSON: {error}")))?,
        None => serde_json::Value::Null,
    };
    merge_patch(&mut base, &patch);
    serde_json::to_vec(&base)
        .map_err(|error| LaserError::Codec(format!("encode merged value: {error}")))
}

// RFC 7386: an object patch overwrites matching fields (a null field removes),
// any non-object patch replaces the base wholesale. Recurses into nested objects.
fn merge_patch(base: &mut serde_json::Value, patch: &serde_json::Value) {
    let serde_json::Value::Object(fields) = patch else {
        *base = patch.clone();
        return;
    };
    if !base.is_object() {
        *base = serde_json::Value::Object(serde_json::Map::new());
    }
    let map = base.as_object_mut().expect("base was just made an object");
    for (key, value) in fields {
        if value.is_null() {
            map.remove(key);
        } else {
            merge_patch(
                map.entry(key.clone()).or_insert(serde_json::Value::Null),
                value,
            );
        }
    }
}

/// A [`SharedEmbedder`] that may be absent: the keyword and recency strategies
/// never call it, the semantic ones surface the registration hint.
#[derive(Clone)]
pub struct MaybeEmbedder(Option<SharedEmbedder>);

impl Embedder for MaybeEmbedder {
    async fn embed(&self, text: &str) -> Result<Vec<f32>, LaserError> {
        match &self.0 {
            Some(embedder) => Embedder::embed(embedder, text).await,
            None => Err(LaserError::Config(
                "this memory needs an embedder for semantic recall: register one with \
                 MemoryHandle::embedder(..)",
            )),
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
    folded: bool,
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

    /// Fold the topic in process instead of reading the managed key-value view,
    /// the opt-in path for small serverless scenarios with no deployment. The
    /// default reads the read view, which does not fold a large topic.
    #[must_use]
    pub fn folded(mut self) -> Self {
        self.folded = true;
        self
    }

    /// Rank by lexical relevance to `text` (sets the `Keyword` strategy, the
    /// managed `Query.text` rider). The complement to `semantic` for the
    /// exact-term matches an embedding blurs.
    #[must_use]
    pub fn keyword(mut self, text: impl Into<String>) -> Self {
        self.semantic = Some(text.into());
        self.strategy = RecallStrategy::Keyword;
        self
    }

    /// Fuse the semantic and keyword signals for `text` by reciprocal rank
    /// (sets the `Hybrid` strategy). Each fused item's `signals` keeps the
    /// per-signal attribution.
    #[must_use]
    pub fn hybrid(mut self, text: impl Into<String>) -> Self {
        self.semantic = Some(text.into());
        self.strategy = RecallStrategy::Hybrid;
        self
    }

    /// Set the recall strategy explicitly, the deep form behind the sugar
    /// above. Pair a text-driven strategy with the query text (`semantic`,
    /// `keyword`, or `hybrid` set it in one call).
    #[must_use]
    pub fn strategy(mut self, strategy: RecallStrategy) -> Self {
        self.strategy = strategy;
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
        if self.folded {
            self.handle.recall_folded(&self.scope, &query).await
        } else {
            Memory::recall(self.handle, &self.scope, &query).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn given_a_custom_memory_backend_when_used_through_the_handle_then_should_delegate() {
        // Any `Memory` impl plugs into the facade via `MemoryHandle::Custom` (the
        // blanket `DynMemory` bridge), so remember/recall route straight to it.
        struct FakeMemory(std::sync::Mutex<Vec<Vec<u8>>>);
        impl Memory for FakeMemory {
            async fn remember(
                &self,
                _scope: &MemoryScope,
                payload: Vec<u8>,
            ) -> Result<MemoryId, LaserError> {
                self.0.lock().expect("lock").push(payload);
                Ok(MemoryId::new())
            }
            async fn recall(
                &self,
                _scope: &MemoryScope,
                _query: &MemoryQuery,
            ) -> Result<Vec<MemoryItem>, LaserError> {
                let provenance = Provenance::builder()
                    .conversation_id(ConversationId::new())
                    .build();
                Ok(self
                    .0
                    .lock()
                    .expect("lock")
                    .iter()
                    .map(|payload| {
                        MemoryItem::plain(MemoryId::new(), payload.clone(), provenance.clone())
                    })
                    .collect())
            }
            async fn improve(
                &self,
                _scope: &MemoryScope,
                _feedback: Feedback,
            ) -> Result<MemoryId, LaserError> {
                Ok(MemoryId::new())
            }
            async fn forget(&self, _scope: &MemoryScope, _id: MemoryId) -> Result<(), LaserError> {
                Ok(())
            }
        }

        let handle = MemoryHandle::Custom(std::sync::Arc::new(FakeMemory(std::sync::Mutex::new(
            Vec::new(),
        ))));
        let scope = MemoryScope::default();
        Memory::remember(&handle, &scope, b"hi".to_vec())
            .await
            .expect("custom remember");
        let items = Memory::recall(&handle, &scope, &MemoryQuery::builder().limit(10).build())
            .await
            .expect("custom recall");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].payload, b"hi");
    }

    #[test]
    fn given_a_log_entry_when_encoded_then_the_wire_memory_record_decodes_it() {
        // The plane folds the memory topic through `laser_wire::memory::MemoryRecord`,
        // so the SDK's on-topic entry must decode into it byte-for-byte or the
        // materialization would miss records.
        let id = MemoryId::new();
        let entry = MemoryLogEntry::Item {
            id,
            kind: MemoryKind::Message,
            body: b"checkout is slow".to_vec(),
        };
        let payload = entry.encode().expect("entry encodes");
        let record: laser_wire::memory::MemoryRecord =
            laser_wire::framing::decode_named(&payload).expect("wire record decodes the SDK entry");
        match record {
            laser_wire::memory::MemoryRecord::Item {
                id: rid,
                kind,
                body,
            } => {
                assert_eq!(rid, id.to_string());
                assert_eq!(kind, "message");
                assert_eq!(body, b"checkout is slow");
            }
            other => panic!("expected an Item record, got {other:?}"),
        }
    }

    #[test]
    fn given_a_memory_id_when_round_tripped_through_a_string_then_should_be_equal() {
        let id = MemoryId::new();
        let parsed = id
            .to_string()
            .parse::<MemoryId>()
            .expect("a formatted memory id should parse");
        assert_eq!(parsed, id);
    }

    // An in-memory Memory for testing the consolidator: stores items in a Vec,
    // recall returns them (up to the limit), forget removes by id.
    struct MockMemory {
        items: std::sync::Mutex<Vec<MemoryItem>>,
    }

    impl Memory for MockMemory {
        async fn remember(
            &self,
            _scope: &MemoryScope,
            payload: Vec<u8>,
        ) -> Result<MemoryId, LaserError> {
            let id = MemoryId::new();
            let provenance = Provenance::builder()
                .conversation_id(crate::types::ConversationId::new())
                .build();
            self.items
                .lock()
                .expect("lock")
                .push(MemoryItem::plain(id, payload, provenance));
            Ok(id)
        }

        async fn recall(
            &self,
            _scope: &MemoryScope,
            query: &MemoryQuery,
        ) -> Result<Vec<MemoryItem>, LaserError> {
            Ok(self
                .items
                .lock()
                .expect("lock")
                .iter()
                .take(query.limit)
                .cloned()
                .collect())
        }

        async fn improve(
            &self,
            _scope: &MemoryScope,
            _feedback: Feedback,
        ) -> Result<MemoryId, LaserError> {
            Ok(MemoryId::new())
        }

        async fn forget(&self, _scope: &MemoryScope, id: MemoryId) -> Result<(), LaserError> {
            self.items
                .lock()
                .expect("lock")
                .retain(|item| item.id != id);
            Ok(())
        }
    }

    #[test]
    fn given_more_than_max_items_when_consolidated_then_should_prune_down_to_the_cap() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime builds");
        runtime.block_on(async {
            let mock = MockMemory {
                items: std::sync::Mutex::new(Vec::new()),
            };
            let scope = MemoryScope::builder().build();
            for i in 0..5 {
                Memory::remember(&mock, &scope, format!("item-{i}").into_bytes())
                    .await
                    .expect("remember");
            }
            let consolidator = DefaultConsolidator::new(mock, 2);
            let report = Consolidator::consolidate(&consolidator, &scope)
                .await
                .expect("consolidate");
            assert_eq!(
                report.pruned, 3,
                "five items down to a cap of two prunes three"
            );

            let remaining = Memory::recall(
                &consolidator.memory,
                &scope,
                &MemoryQuery::builder().limit(100).build(),
            )
            .await
            .expect("recall");
            assert_eq!(remaining.len(), 2);
        });
    }

    struct StubSummarizer;

    impl Summarizer for StubSummarizer {
        async fn summarize(&self, bodies: Vec<Vec<u8>>) -> Result<Vec<u8>, LaserError> {
            Ok(format!("summary of {}", bodies.len()).into_bytes())
        }
    }

    #[tokio::test]
    async fn given_session_messages_when_consolidated_then_should_store_one_summary() {
        let memory = LogMemoryStub::default();
        let scope = MemoryScope::default();
        for body in [b"turn one".to_vec(), b"turn two".to_vec()] {
            memory.push(MemoryKind::Message, body);
        }
        memory.push(MemoryKind::Fact, b"a durable fact".to_vec());
        let consolidator =
            DefaultConsolidator::new(memory.clone(), 100).with_summarizer(StubSummarizer);
        let report = Consolidator::consolidate(&consolidator, &scope)
            .await
            .expect("consolidates");
        assert_eq!(report.summarized, 2, "both session turns fold");
        assert_eq!(report.pruned, 0, "prune_summarized was not opted in");
        assert!(
            memory.bodies().iter().any(|body| body == b"summary of 2"),
            "the summary landed"
        );
    }

    #[tokio::test]
    async fn given_prune_summarized_when_consolidated_then_should_forget_the_turns() {
        let memory = LogMemoryStub::default();
        let scope = MemoryScope::default();
        memory.push(MemoryKind::Message, b"turn".to_vec());
        let consolidator = DefaultConsolidator::new(memory.clone(), 100)
            .with_summarizer(StubSummarizer)
            .prune_summarized();
        let report = Consolidator::consolidate(&consolidator, &scope)
            .await
            .expect("consolidates");
        assert_eq!(report.summarized, 1);
        assert_eq!(report.pruned, 1, "the folded turn is forgotten");
    }

    /// A minimal in-memory `Memory` for consolidator tests: pushes are visible
    /// to recall, forget removes, nothing ranks. Clones share the store so a
    /// test can hand one clone to the consolidator and assert on the other.
    #[derive(Clone, Default)]
    struct LogMemoryStub {
        items: std::sync::Arc<std::sync::Mutex<Vec<MemoryItem>>>,
    }

    impl LogMemoryStub {
        fn push(&self, kind: MemoryKind, payload: Vec<u8>) {
            let provenance = Provenance::builder()
                .conversation_id(crate::types::ConversationId::new())
                .build();
            let mut item = MemoryItem::plain(MemoryId::new(), payload, provenance);
            item.kind = kind;
            self.items.lock().expect("stub lock").push(item);
        }

        fn bodies(&self) -> Vec<Vec<u8>> {
            self.items
                .lock()
                .expect("stub lock")
                .iter()
                .map(|item| item.payload.clone())
                .collect()
        }
    }

    impl Memory for LogMemoryStub {
        async fn remember(
            &self,
            _scope: &MemoryScope,
            payload: Vec<u8>,
        ) -> Result<MemoryId, LaserError> {
            let id = MemoryId::new();
            let provenance = Provenance::builder()
                .conversation_id(crate::types::ConversationId::new())
                .build();
            self.items
                .lock()
                .expect("stub lock")
                .push(MemoryItem::plain(id, payload, provenance));
            Ok(id)
        }

        async fn recall(
            &self,
            _scope: &MemoryScope,
            _query: &MemoryQuery,
        ) -> Result<Vec<MemoryItem>, LaserError> {
            Ok(self.items.lock().expect("stub lock").clone())
        }

        async fn improve(
            &self,
            _scope: &MemoryScope,
            _feedback: Feedback,
        ) -> Result<MemoryId, LaserError> {
            Ok(MemoryId::new())
        }

        async fn forget(&self, _scope: &MemoryScope, id: MemoryId) -> Result<(), LaserError> {
            self.items
                .lock()
                .expect("stub lock")
                .retain(|item| item.id != id);
            Ok(())
        }
    }

    fn ranked_item(id: MemoryId, strategy: RecallStrategy, rank: usize) -> MemoryItem {
        let provenance = Provenance::builder()
            .conversation_id(crate::types::ConversationId::new())
            .build();
        let mut item = MemoryItem::plain(id, b"body".to_vec(), provenance);
        item.signals = vec![RecallSignal {
            strategy,
            rank,
            score: None,
        }];
        item
    }

    #[test]
    fn given_two_signals_when_fused_then_should_rank_agreement_first() {
        let shared = MemoryId::new();
        let semantic_only = MemoryId::new();
        let keyword_only = MemoryId::new();
        let semantic = vec![
            ranked_item(semantic_only, RecallStrategy::Semantic, 0),
            ranked_item(shared, RecallStrategy::Semantic, 1),
        ];
        let keyword = vec![
            ranked_item(keyword_only, RecallStrategy::Keyword, 0),
            ranked_item(shared, RecallStrategy::Keyword, 1),
        ];
        let fused = fuse_reciprocal_rank(vec![semantic, keyword], 10);
        assert_eq!(fused[0].id, shared, "both signals agree on it");
        assert_eq!(fused[0].signals.len(), 2, "attribution keeps both signals");
        assert_eq!(fused.len(), 3);
    }

    #[test]
    fn given_a_limit_when_fused_then_should_truncate_after_ranking() {
        let lists = vec![vec![
            ranked_item(MemoryId::new(), RecallStrategy::Semantic, 0),
            ranked_item(MemoryId::new(), RecallStrategy::Semantic, 1),
            ranked_item(MemoryId::new(), RecallStrategy::Semantic, 2),
        ]];
        assert_eq!(fuse_reciprocal_rank(lists, 2).len(), 2);
    }

    fn context_item(body: &str) -> MemoryItem {
        let provenance = Provenance::builder()
            .conversation_id(crate::types::ConversationId::new())
            .build();
        MemoryItem::plain(MemoryId::new(), body.as_bytes().to_vec(), provenance)
    }

    #[test]
    fn given_no_budget_when_rendered_then_should_join_every_item() {
        let items = [context_item("alpha"), context_item("beta")];
        assert_eq!(to_context_block(&items, None), "alpha\n\nbeta");
    }

    #[test]
    fn given_a_token_budget_when_rendered_then_should_fill_in_order_and_mark_the_overflow() {
        // ~4 bytes per token: a 6-token budget (24 bytes) fits the two 20-byte
        // items below only one at a time.
        let items = [
            context_item("first twenty bytes!!"),
            context_item("second twenty bytes!"),
        ];
        let block = to_context_block(&items, Some(6));
        assert_eq!(
            block,
            "first twenty bytes!!\n\n[... 1 more recalled item(s) omitted ...]"
        );
    }

    #[test]
    fn given_a_budget_smaller_than_the_first_item_when_rendered_then_should_keep_at_least_one() {
        // The first item always renders, so a block is never empty when items exist.
        let items = [context_item("a much longer single memory item")];
        let block = to_context_block(&items, Some(1));
        assert_eq!(block, "a much longer single memory item");
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

    #[tokio::test]
    async fn given_a_deduped_vector_remember_when_repeated_then_should_store_one_item() {
        let handle = MemoryHandle::Vector(std::sync::Arc::new(VectorMemory::new(SharedEmbedder(
            std::sync::Arc::new(WordEmbedder),
        ))));
        let conversation = ConversationId::new();
        let first = handle
            .remember(b"stable fact".to_vec())
            .scope(conversation)
            .dedup()
            .send()
            .await
            .expect("first remember succeeds");
        let second = handle
            .remember(b"stable fact".to_vec())
            .scope(conversation)
            .dedup()
            .send()
            .await
            .expect("second remember succeeds");
        let recalled = handle
            .recall(conversation)
            .fetch()
            .await
            .expect("vector recall succeeds");
        assert_eq!(first, second);
        assert_eq!(recalled.len(), 1);
    }

    #[test]
    fn given_a_memory_backend_when_defaulted_then_should_be_auto() {
        assert_eq!(MemoryBackend::default(), MemoryBackend::Auto);
    }

    #[test]
    fn given_memory_kinds_when_classified_then_should_map_to_the_taxonomy() {
        assert_eq!(MemoryKind::Message.class(), MemoryClass::Episodic);
        assert_eq!(MemoryKind::Procedure.class(), MemoryClass::Procedural);
        assert_eq!(MemoryKind::Fact.class(), MemoryClass::Semantic);
        assert_eq!(MemoryKind::Summary.class(), MemoryClass::Semantic);
    }

    #[tokio::test]
    async fn given_a_keyword_strategy_when_recalling_then_should_match_exact_terms() {
        let memory = VectorMemory::new(WordEmbedder);
        let scope = MemoryScope::builder()
            .conversation(ConversationId::new())
            .build();
        Memory::remember(&memory, &scope, b"alpha beta".to_vec())
            .await
            .expect("remember alpha");
        Memory::remember(&memory, &scope, b"gamma delta".to_vec())
            .await
            .expect("remember gamma");
        let top = Memory::recall(
            &memory,
            &scope,
            &MemoryQuery::builder()
                .semantic("gamma".to_owned())
                .strategy(RecallStrategy::Keyword)
                .limit(1)
                .build(),
        )
        .await
        .expect("keyword recall");
        assert_eq!(top.len(), 1);
        assert_eq!(top[0].payload.as_slice(), b"gamma delta");
    }

    #[tokio::test]
    async fn given_a_user_scope_when_recalling_then_should_narrow_to_that_user() {
        let memory = VectorMemory::new(WordEmbedder);
        let conversation = ConversationId::new();
        let alice = MemoryScope::builder()
            .conversation(conversation)
            .user("alice".to_owned())
            .build();
        let bob = MemoryScope::builder()
            .conversation(conversation)
            .user("bob".to_owned())
            .build();
        Memory::remember(&memory, &alice, b"alice secret".to_vec())
            .await
            .expect("remember alice");
        Memory::remember(&memory, &bob, b"bob secret".to_vec())
            .await
            .expect("remember bob");
        let hits = Memory::recall(&memory, &alice, &MemoryQuery::builder().build())
            .await
            .expect("recall alice");
        assert_eq!(hits.len(), 1, "only alice's item is in scope");
        assert_eq!(hits[0].payload.as_slice(), b"alice secret");
    }

    struct ReverseReranker;

    impl Reranker for ReverseReranker {
        async fn rerank(
            &self,
            _query: &str,
            mut items: Vec<MemoryItem>,
        ) -> Result<Vec<MemoryItem>, LaserError> {
            items.reverse();
            Ok(items)
        }
    }

    #[tokio::test]
    async fn given_a_reranked_memory_when_recalling_then_should_apply_the_reranker() {
        let inner = VectorMemory::new(WordEmbedder);
        let scope = MemoryScope::builder()
            .conversation(ConversationId::new())
            .build();
        Memory::remember(&inner, &scope, b"the cat sat".to_vec())
            .await
            .expect("remember cat");
        Memory::remember(&inner, &scope, b"the dog ran".to_vec())
            .await
            .expect("remember dog");
        let reranked = RerankedMemory::new(inner, ReverseReranker);
        let hits = Memory::recall(
            &reranked,
            &scope,
            &MemoryQuery::builder().semantic("cat".to_owned()).build(),
        )
        .await
        .expect("reranked recall");
        assert_eq!(hits.len(), 2);
        assert_eq!(
            hits[0].payload.as_slice(),
            b"the dog ran",
            "the reranker moved the dog note to the front"
        );
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

    #[test]
    fn given_a_named_record_when_absorbed_then_should_fold_into_point_state() {
        let mut projection = Projection::default();
        let item = laser_wire::memory::MemoryRecord::Item {
            id: "notes/current-plan".to_owned(),
            kind: "fact".to_owned(),
            body: b"ship on friday".to_vec(),
        };
        projection.absorb(
            &laser_wire::framing::encode_named(&item).expect("encodes"),
            Provenance::builder()
                .conversation_id(ConversationId::new())
                .build(),
        );
        assert_eq!(
            projection
                .named
                .get("notes/current-plan")
                .map(Vec::as_slice),
            Some(b"ship on friday".as_slice()),
        );
        // The named record is not a recall item.
        assert!(projection.items.is_empty());

        let forget = laser_wire::memory::MemoryRecord::Forget {
            target: "notes/current-plan".to_owned(),
        };
        projection.absorb(
            &laser_wire::framing::encode_named(&forget).expect("encodes"),
            Provenance::builder()
                .conversation_id(ConversationId::new())
                .build(),
        );
        assert!(projection.named.is_empty());
    }

    #[test]
    fn given_a_merge_patch_when_applied_then_should_overwrite_add_and_remove_fields() {
        let current = br#"{"tier":"pro","seats":3,"beta":true}"#;
        let patch = br#"{"tier":"enterprise","region":"eu","beta":null}"#;
        let merged = merge_named(Some(current), patch).expect("merges");
        let value: serde_json::Value = serde_json::from_slice(&merged).expect("json");
        assert_eq!(value["tier"], "enterprise"); // overwritten
        assert_eq!(value["seats"], 3); // untouched
        assert_eq!(value["region"], "eu"); // added
        assert!(value.get("beta").is_none()); // null removes
    }

    #[test]
    fn given_no_current_value_when_merge_patched_then_should_start_from_the_patch() {
        let merged = merge_named(None, br#"{"plan":"enterprise"}"#).expect("merges");
        let value: serde_json::Value = serde_json::from_slice(&merged).expect("json");
        assert_eq!(value["plan"], "enterprise");
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
