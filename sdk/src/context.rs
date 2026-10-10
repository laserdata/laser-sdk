use crate::agent::Laser;
use crate::error::LaserError;
use crate::provenance::{AgentTopic, Provenance};
use crate::types::{AgentId, ConversationId, MessageId};
use iggy::prelude::*;
use std::collections::{BTreeMap, HashSet};

pub(crate) const READ_BATCH: u32 = 1000;

/// The most raw records a context read examines in each partition: the newest
/// ones, or the newest ones before a [`Checkpoint`] for a point-in-time read.
/// The conversation filter runs after the read, so on a busy shared partition
/// turns older than this window are not returned. Python and TypeScript use
/// the same window.
pub const CONTEXT_READ_WINDOW: usize = 10_000;

const _: () = assert!(CONTEXT_READ_WINDOW == crate::poll::MAX_DRAIN_MESSAGES);

/// One message read back from the log for context assembly.
#[derive(Debug, Clone)]
pub struct ContextMessage {
    /// Where the message sits on the log.
    pub id: MessageId,
    /// Provenance decoded off the message (synthesized from the envelope for an
    /// AGDX message).
    pub provenance: Provenance,
    /// The raw message body. Owned `Vec<u8>` so the public API never leaks the
    /// `bytes` crate.
    pub payload: Vec<u8>,
    /// The decoded AGDX envelope when the message carries one, else `None`.
    pub envelope: Option<laser_wire::agent::AgentEnvelope>,
    /// The name of the topic the message was read from.
    pub topic: String,
    /// The broker's append time in microseconds.
    pub timestamp_micros: u64,
    /// The numeric id of the stream the message was read from.
    pub stream_id: u32,
    /// The numeric id of the topic the message was read from.
    pub topic_id: u32,
}

/// A point in a conversation's log: the next offset each partition of each
/// named topic will write, as captured by
/// [`ContextScope::checkpoint`](crate::context_scope::ContextScope::checkpoint).
/// A [`ContextAssembler`] built with `to_checkpoint` folds history up to it,
/// and one built with `from_checkpoint` resumes after it. It is a client-side
/// bookmark keyed by topic ([`AgentTopic::topic_string`]), never a record on
/// the log.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Checkpoint {
    per_topic: BTreeMap<String, BTreeMap<u32, u64>>,
}

impl Checkpoint {
    pub(crate) fn from_topic_offsets(per_topic: BTreeMap<String, BTreeMap<u32, u64>>) -> Self {
        Self { per_topic }
    }

    /// The per-partition offsets for `topic` (by [`AgentTopic::topic_string`]),
    /// or `None` when the checkpoint was not taken over that topic.
    pub fn topic_offsets(&self, topic: &str) -> Option<&BTreeMap<u32, u64>> {
        self.per_topic.get(topic)
    }

    /// Every checkpointed topic with its partition offsets, by topic name.
    pub fn topics(&self) -> impl Iterator<Item = (&str, &BTreeMap<u32, u64>)> {
        self.per_topic
            .iter()
            .map(|(topic, offsets)| (topic.as_str(), offsets))
    }

    /// True when no named topic was checkpointed.
    pub fn is_empty(&self) -> bool {
        self.per_topic.is_empty()
    }
}

/// The current tail of `topics` on `laser`'s default stream, one entry per
/// topic. Every partition that exists at capture time gets an entry,
/// an empty one at offset `0`, so a later bounded read can tell an empty
/// partition apart from one created after the checkpoint.
pub async fn checkpoint(
    laser: &Laser,
    topics: &[AgentTopic<'static>],
) -> Result<Checkpoint, LaserError> {
    let stream = Identifier::named(laser.stream_required()?)?;
    let mut per_topic = BTreeMap::new();
    for topic in topics {
        let name = topic.topic_string();
        let topic_id = topic.as_identifier();
        let Some(details) = laser.client().get_topic(&stream, &topic_id).await? else {
            per_topic.insert(name, BTreeMap::new());
            continue;
        };
        let count = crate::poll::bounded_partitions(details.partitions_count);
        let consumer = Consumer::new(Identifier::named("laser-checkpoint")?);
        let mut tails = tokio::task::JoinSet::new();
        for partition in 0..count {
            let laser = laser.clone();
            let stream = stream.clone();
            let topic_id = topic_id.clone();
            let consumer = consumer.clone();
            tails.spawn(async move {
                let offset = crate::poll::current_tail_offset(
                    &laser.client(),
                    &stream,
                    &topic_id,
                    &consumer,
                    partition,
                )
                .await?;
                Ok::<_, LaserError>((partition, offset))
            });
        }
        let mut offsets = BTreeMap::new();
        while let Some(joined) = tails.join_next().await {
            let (partition, offset) = joined.map_err(join_failed)??;
            offsets.insert(partition, offset);
        }
        per_topic.insert(name, offsets);
    }
    Ok(Checkpoint { per_topic })
}

/// Selects which assembled messages feed an LLM call.
pub trait ContextPolicy: Send + Sync {
    fn select(&self, history: &[ContextMessage]) -> Vec<ContextMessage>;

    /// The policy name a context manifest records.
    fn name(&self) -> String {
        "custom".to_owned()
    }

    /// The policy version a context manifest records.
    fn version(&self) -> String {
        "1".to_owned()
    }

    /// The kept and dropped records of one selection, and why.
    fn selection(&self, history: &[ContextMessage]) -> Selection {
        let kept = self.select(history);
        let dropped = history
            .iter()
            .filter(|message| {
                !kept
                    .iter()
                    .any(|kept| kept.id == message.id && kept.topic == message.topic)
            })
            .cloned()
            .collect();
        Selection {
            kept,
            dropped,
            reason: self.name(),
        }
    }
}

/// What a [`ContextPolicy`] kept and dropped, and the policy that decided.
#[derive(Debug, Clone)]
pub struct Selection {
    pub kept: Vec<ContextMessage>,
    pub dropped: Vec<ContextMessage>,
    pub reason: String,
}

/// Keep the most recent N messages.
pub struct LastN(pub usize);

impl ContextPolicy for LastN {
    fn name(&self) -> String {
        format!("last_n({})", self.0)
    }

    fn select(&self, history: &[ContextMessage]) -> Vec<ContextMessage> {
        let start = history.len().saturating_sub(self.0);
        history[start..].to_vec()
    }
}

/// Keep only messages from the given agents.
pub struct RoleFilter(pub HashSet<AgentId>);

impl ContextPolicy for RoleFilter {
    fn name(&self) -> String {
        "role_filter".to_owned()
    }

    fn select(&self, history: &[ContextMessage]) -> Vec<ContextMessage> {
        history
            .iter()
            .filter(|message| {
                message
                    .provenance
                    .agent
                    .as_ref()
                    .is_some_and(|agent| self.0.contains(agent))
            })
            .cloned()
            .collect()
    }
}

/// Apply several policies in order, each narrowing the previous result: the
/// composable form, so a caller pipelines (say) a `RoleFilter` then a `LastN`
/// then a `TokenBudget` into the one `policy` slot instead of picking exactly one.
pub struct Chain(pub Vec<Box<dyn ContextPolicy>>);

impl ContextPolicy for Chain {
    fn name(&self) -> String {
        let names: Vec<String> = self.0.iter().map(|policy| policy.name()).collect();
        format!("chain({})", names.join(","))
    }

    fn select(&self, history: &[ContextMessage]) -> Vec<ContextMessage> {
        let mut current = history.to_vec();
        for policy in &self.0 {
            current = policy.select(&current);
        }
        current
    }
}

/// Keep the most-recent messages that fit within `max_tokens`, estimated per
/// message. Meets memory's `to_context_block(token_budget)` in the middle so a
/// prompt built from history and recalled memory shares one budget notion. The
/// default estimate is a coarse ~4-bytes-per-token heuristic over the payload.
/// Pass a real tokenizer with [`with_estimator`](Self::with_estimator).
pub struct TokenBudget {
    max_tokens: usize,
    estimate: Box<dyn Fn(&ContextMessage) -> usize + Send + Sync>,
}

impl TokenBudget {
    /// A budget using the coarse `payload.len() / 4` token heuristic.
    #[must_use]
    pub fn new(max_tokens: usize) -> Self {
        Self {
            max_tokens,
            estimate: Box::new(|message| {
                usize::try_from(laser_wire::agent::estimate_tokens(message.payload.len()))
                    .unwrap_or(usize::MAX)
            }),
        }
    }

    /// A budget with a caller-supplied per-message token estimate (a real
    /// tokenizer, or a model-specific counter).
    #[must_use]
    pub fn with_estimator(
        max_tokens: usize,
        estimate: impl Fn(&ContextMessage) -> usize + Send + Sync + 'static,
    ) -> Self {
        Self {
            max_tokens,
            estimate: Box::new(estimate),
        }
    }
}

impl ContextPolicy for TokenBudget {
    fn name(&self) -> String {
        format!("token_budget({})", self.max_tokens)
    }

    fn select(&self, history: &[ContextMessage]) -> Vec<ContextMessage> {
        // Walk newest-first, keeping messages while the running estimate fits, so
        // the kept set is the most-recent tail under budget. Always keep at least
        // one message so a single over-budget message is not silently dropped.
        let mut kept = Vec::new();
        let mut total = 0usize;
        for message in history.iter().rev() {
            let cost = (self.estimate)(message);
            if !kept.is_empty() && total.saturating_add(cost) > self.max_tokens {
                break;
            }
            total = total.saturating_add(cost);
            kept.push(message.clone());
        }
        kept.reverse();
        kept
    }
}

/// Reads a conversation's history off the log and applies a `ContextPolicy`.
#[derive(bon::Builder)]
pub struct ContextAssembler {
    conversation_id: ConversationId,
    #[builder(default = false)]
    across_subconversations: bool,
    #[builder(default = vec![AgentTopic::Sessions])]
    topics: Vec<AgentTopic<'static>>,
    #[builder(default = Box::new(LastN(50)))]
    policy: Box<dyn ContextPolicy>,
    /// Per-partition start offsets: partition `p` is read from
    /// `from_offsets[p]` (default `0`). The incremental-resume seam: a fold
    /// seeded from a snapshot passes the snapshot's resume offsets here and
    /// replays only the tail (the bounded-reads law). One map for every topic
    /// in `topics`. A [`from_checkpoint`](Self::from_checkpoint) takes
    /// precedence for the entire read, with missing entries starting at zero.
    #[builder(default)]
    from_offsets: BTreeMap<u32, u64>,
    /// Resume after a [`Checkpoint`], per topic and partition, and read to the
    /// tail.
    from_checkpoint: Option<Checkpoint>,
    /// Stop at a [`Checkpoint`], per topic and partition: the point-in-time
    /// read behind `ReplayBound::At`. `None` reads to the current tail.
    to_checkpoint: Option<Checkpoint>,
}

impl ContextAssembler {
    /// Read the configured topics, order by Iggy timestamp, and apply the policy.
    /// Every (topic, partition) is drained concurrently: a conversation can span
    /// many partitions across several topics, and reading them serially makes
    /// recovery pay one round trip after another. Each partition read covers
    /// at most the newest [`CONTEXT_READ_WINDOW`] records of its range.
    pub async fn assemble(self, laser: &Laser) -> Result<Vec<ContextMessage>, LaserError> {
        self.read(laser, ReadSpan::Window).await
    }

    // Like `assemble`, but every partition read covers its whole range in
    // bounded chunks, for a state fold that must see every record.
    pub(crate) async fn replay(self, laser: &Laser) -> Result<Vec<ContextMessage>, LaserError> {
        self.read(laser, ReadSpan::Whole).await
    }

    async fn read(
        mut self,
        laser: &Laser,
        span: ReadSpan,
    ) -> Result<Vec<ContextMessage>, LaserError> {
        // A topic named twice is read once.
        let mut seen = std::collections::BTreeSet::new();
        self.topics
            .retain(|topic| seen.insert(topic.topic_string()));
        let stream = Identifier::named(laser.stream_required()?)?;
        let Some(stream_details) = laser.client().get_stream(&stream).await? else {
            return Ok(Vec::new());
        };
        let stream_id = stream_details.id;

        // Resolve each topic's id and partition count concurrently.
        let mut meta = tokio::task::JoinSet::new();
        for (topic_idx, topic) in self.topics.iter().enumerate() {
            let laser = laser.clone();
            let stream = stream.clone();
            let topic_id = topic.as_identifier();
            meta.spawn(async move {
                let details = laser
                    .client()
                    .get_topic(&stream, &topic_id)
                    .await?
                    .map(|details| {
                        (
                            details.id,
                            crate::poll::bounded_partitions(details.partitions_count),
                        )
                    });
                Ok::<_, LaserError>((topic_idx, topic_id, details))
            });
        }
        let mut sources = Vec::new();
        while let Some(joined) = meta.join_next().await {
            let (topic_idx, topic_id, details) = joined.map_err(join_failed)??;
            let Some((numeric_topic, count)) = details else {
                continue;
            };
            for partition in 0..count {
                sources.push((topic_idx, topic_id.clone(), numeric_topic, partition));
            }
        }

        // Drain every partition concurrently.
        let mut drains = tokio::task::JoinSet::new();
        for (topic_idx, topic_id, numeric_topic, partition) in sources {
            let laser = laser.clone();
            let stream = stream.clone();
            let topic_name = self.topics[topic_idx].topic_string();
            let from = match &self.from_checkpoint {
                Some(checkpoint) => checkpoint
                    .topic_offsets(&topic_name)
                    .and_then(|offsets| offsets.get(&partition).copied())
                    .unwrap_or(0),
                None => self.from_offsets.get(&partition).copied().unwrap_or(0),
            };
            let until = self.to_checkpoint.as_ref().map(|checkpoint| {
                checkpoint
                    .topic_offsets(&topic_name)
                    .and_then(|offsets| offsets.get(&partition).copied())
                    .unwrap_or(0)
            });
            let lens = (self.conversation_id, self.across_subconversations);
            drains.spawn(async move {
                let client = laser.client();
                let consumer = Consumer::new(Identifier::named("laser-context-reader")?);
                let Some(mut range) = partition_range(
                    &client, &stream, &topic_id, &consumer, partition, from, until, span,
                )
                .await?
                else {
                    return Ok::<_, LaserError>((topic_idx, Vec::new()));
                };
                // The conversation filter runs per chunk, so a replay holds only
                // the matching records, never a whole partition.
                let mut matched = Vec::new();
                loop {
                    let batch = crate::poll::drain_partition(
                        &client, &stream, &topic_id, &consumer, range, READ_BATCH,
                    )
                    .await?;
                    let read = batch.messages.len();
                    for message in batch.messages {
                        let Ok((provenance, envelope)) =
                            crate::agent::provenance_and_envelope(&message)
                        else {
                            continue;
                        };
                        if in_conversation(&provenance, lens.0, lens.1) {
                            matched.push((
                                message.header.timestamp,
                                ContextMessage {
                                    id: MessageId::new(partition, message.header.offset),
                                    provenance,
                                    payload: message.payload.to_vec(),
                                    envelope,
                                    topic: topic_name.clone(),
                                    timestamp_micros: message.header.timestamp,
                                    stream_id,
                                    topic_id: numeric_topic,
                                },
                            ));
                        }
                    }
                    if !reads_further(span, read, batch.next_offset, range.end) {
                        break;
                    }
                    range.from = batch.next_offset;
                }
                Ok::<_, LaserError>((topic_idx, matched))
            });
        }
        let mut collected: Vec<(u64, usize, ContextMessage)> = Vec::new();
        while let Some(joined) = drains.join_next().await {
            let (topic_idx, matched) = joined.map_err(join_failed)??;
            collected.extend(
                matched
                    .into_iter()
                    .map(|(timestamp, message)| (timestamp, topic_idx, message)),
            );
        }
        // Order by Iggy-assigned timestamp: a single global clock across topics,
        // since each topic has its own independent offset space. Ties break on
        // (topic, partition, offset), which is deterministic but not strictly
        // chronological for messages stamped in the same microsecond on different
        // topics, ordering Apache Iggy cannot provide across offset spaces.
        collected.sort_by_key(|(timestamp, topic_idx, message)| {
            (
                *timestamp,
                *topic_idx,
                message.id.partition_id,
                message.id.offset,
            )
        });
        let ordered: Vec<ContextMessage> = collected
            .into_iter()
            .map(|(_, _, message)| message)
            .collect();
        Ok(self.policy.select(&ordered))
    }
}

// How much of its range one partition read covers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReadSpan {
    // The newest `CONTEXT_READ_WINDOW` records, the context window.
    Window,
    // Every record, read in bounded chunks.
    Whole,
}

// The offsets one partition read covers, `None` when the range is empty.
#[allow(clippy::too_many_arguments)]
async fn partition_range(
    client: &IggyClient,
    stream: &Identifier,
    topic: &Identifier,
    consumer: &Consumer,
    partition: u32,
    from: u64,
    until: Option<u64>,
    span: ReadSpan,
) -> Result<Option<crate::poll::DrainRange>, LaserError> {
    // A checkpoint holds the next offset to write, and so does the tail a
    // replay pins at its start, so a bounded read ends one before it. Pinning
    // the tail keeps a replay of a busy partition from chasing new writes.
    let next = match (until, span) {
        (Some(next), _) => next,
        // Context selection keeps the most recent records, so a partition
        // longer than the drain ceiling is read from a tail-anchored window
        // rather than from its head.
        (None, ReadSpan::Window) => {
            let start =
                crate::poll::tail_anchored_offset(client, stream, topic, consumer, partition, from)
                    .await?;
            return Ok(Some(crate::poll::DrainRange::open(partition, start)));
        }
        (None, ReadSpan::Whole) => {
            crate::poll::current_tail_offset(client, stream, topic, consumer, partition).await?
        }
    };
    let Some(end) = next.checked_sub(1).filter(|end| *end >= from) else {
        return Ok(None);
    };
    Ok(Some(crate::poll::DrainRange::until(
        partition,
        range_start(span, from, end),
        end,
    )))
}

// Where a read ending at `end` starts. The context window is anchored at
// `end`, not at a tail that may have moved far past it. A replay starts at
// `from`.
fn range_start(span: ReadSpan, from: u64, end: u64) -> u64 {
    match span {
        ReadSpan::Window => from.max(end.saturating_sub(CONTEXT_READ_WINDOW as u64 - 1)),
        ReadSpan::Whole => from,
    }
}

// Whether a replay drains another chunk after one that read `read` records
// and resumes at `next_offset`. A drain stops at its record ceiling, so only a
// full chunk can leave records behind, and never past `end`.
fn reads_further(span: ReadSpan, read: usize, next_offset: u64, end: Option<u64>) -> bool {
    span == ReadSpan::Whole
        && read >= CONTEXT_READ_WINDOW
        && end.is_none_or(|end| next_offset <= end)
}

fn in_conversation(
    provenance: &Provenance,
    conversation: ConversationId,
    across_subconversations: bool,
) -> bool {
    if provenance.conversation_id == conversation {
        return true;
    }
    across_subconversations
        && (provenance.root_conversation_id == Some(conversation)
            || provenance.parent_conversation_id == Some(conversation))
}

fn join_failed(error: tokio::task::JoinError) -> LaserError {
    LaserError::HandlerConfig(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_a_range_longer_than_the_window_when_replayed_then_should_start_at_its_first_offset() {
        let end = 25_000;
        assert_eq!(range_start(ReadSpan::Whole, 0, end), 0);
        assert_eq!(
            range_start(ReadSpan::Window, 0, end),
            end - CONTEXT_READ_WINDOW as u64 + 1
        );
        assert_eq!(range_start(ReadSpan::Window, 20_000, end), 20_000);
    }

    #[test]
    fn given_a_full_chunk_when_replaying_then_should_read_on_until_the_range_ends() {
        assert!(reads_further(
            ReadSpan::Whole,
            CONTEXT_READ_WINDOW,
            10_000,
            Some(24_999)
        ));
        assert!(!reads_further(
            ReadSpan::Whole,
            CONTEXT_READ_WINDOW,
            25_000,
            Some(24_999)
        ));
        assert!(
            !reads_further(ReadSpan::Whole, 42, 10_042, Some(24_999)),
            "a short chunk reached the tail"
        );
        assert!(
            !reads_further(ReadSpan::Window, CONTEXT_READ_WINDOW, 10_000, Some(24_999)),
            "a context read covers one window"
        );
    }

    #[test]
    fn given_a_checkpoint_when_queried_by_topic_then_should_return_only_its_own_offsets() {
        let checkpoint = Checkpoint {
            per_topic: BTreeMap::from([
                (
                    "agent.sessions".to_owned(),
                    BTreeMap::from([(0, 5), (1, 2)]),
                ),
                ("agent.streams".to_owned(), BTreeMap::new()),
            ]),
        };
        assert_eq!(
            checkpoint.topic_offsets("agent.sessions"),
            Some(&BTreeMap::from([(0, 5), (1, 2)]))
        );
        assert_eq!(
            checkpoint.topic_offsets("agent.streams"),
            Some(&BTreeMap::new())
        );
        assert_eq!(checkpoint.topic_offsets("agent.memory"), None);
        assert!(!checkpoint.is_empty());
        assert!(Checkpoint::default().is_empty());
    }

    fn message(agent: &str, offset: u64) -> ContextMessage {
        ContextMessage {
            id: MessageId::new(1, offset),
            provenance: Provenance::builder()
                .conversation_id(ConversationId::new())
                .agent(agent.parse().expect("agent id is valid"))
                .build(),
            payload: Vec::new(),
            envelope: None,
            topic: "agent.sessions".to_owned(),
            timestamp_micros: offset,
            stream_id: 1,
            topic_id: 1,
        }
    }

    #[test]
    fn given_a_history_when_applying_last_n_then_should_keep_the_tail() {
        let history = vec![message("a", 0), message("b", 1), message("c", 2)];
        let selected = LastN(2).select(&history);
        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0].id.offset, 1);
        assert_eq!(selected[1].id.offset, 2);
    }

    #[test]
    fn given_a_history_over_budget_when_applying_token_budget_then_should_keep_the_recent_tail() {
        let history: Vec<ContextMessage> = (0..5)
            .map(|offset| {
                let mut message = message("a", offset);
                message.payload = vec![b'x'; 400]; // ~100 tokens each (4 bytes/token)
                message
            })
            .collect();
        // ~250 tokens fits two 100-token messages. The third would cross it.
        let selected = TokenBudget::new(250).select(&history);
        assert_eq!(selected.len(), 2);
        assert_eq!(selected[1].id.offset, 4, "keeps the most-recent tail");
    }

    #[test]
    fn given_composed_policies_when_chained_then_should_apply_in_order() {
        let history = vec![
            message("planner", 0),
            message("executor", 1),
            message("planner", 2),
        ];
        let chain = Chain(vec![
            Box::new(RoleFilter(HashSet::from(["planner"
                .parse()
                .expect("planner is a valid agent id")]))),
            Box::new(LastN(1)),
        ]);
        let selected = chain.select(&history);
        assert_eq!(selected.len(), 1);
        assert_eq!(
            selected[0].id.offset, 2,
            "role filter then last-1 keeps the latest planner"
        );
    }

    #[test]
    fn given_a_history_when_applying_a_role_filter_then_should_keep_only_matching_agents() {
        let history = vec![message("planner", 0), message("executor", 1)];
        let planner = HashSet::from(["planner".parse().expect("planner is a valid agent id")]);
        let selected = RoleFilter(planner).select(&history);
        assert_eq!(selected.len(), 1);
        assert_eq!(
            selected[0]
                .provenance
                .agent
                .as_ref()
                .expect("agent should be set")
                .as_str(),
            "planner"
        );
    }
}
