use crate::agent::Laser;
use crate::error::LaserError;
use crate::provenance::{AgentTopic, Provenance};
use crate::types::{AgentId, ConversationId, MessageId};
use iggy::prelude::*;
use std::collections::{BTreeMap, HashSet};

const READ_BATCH: u32 = 1000;

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
}

/// A point in a conversation's log: the next-offset-to-read on each
/// partition of each named topic, as of when the checkpoint was captured.
/// Two symmetric uses on [`ContextAssembler`]: [`to_checkpoint`] folds
/// history up to and including it, a point-in-time read
/// (`Session::state_at`); [`from_checkpoint`] resumes forward from it
/// instead (`Session::replay`). Capture one with
/// [`ContextScope::checkpoint`](crate::context_scope::ContextScope::checkpoint).
///
/// Keyed by topic name, not by wire format, so this is a client-side
/// bookmark only, not a record on the log. A topic addressed as
/// [`AgentTopic::Custom`] has no stable name and is never represented here:
/// a checkpoint captured over topics that include one simply does not bound
/// that topic.
///
/// [`to_checkpoint`]: ContextAssembler::to_checkpoint
/// [`from_checkpoint`]: ContextAssembler::from_checkpoint
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Checkpoint {
    per_topic: BTreeMap<String, BTreeMap<u32, u64>>,
}

impl Checkpoint {
    /// This checkpoint's offsets for `topic` (by [`AgentTopic::name`]), or
    /// `None` when the checkpoint was not taken over that topic.
    pub fn topic_offsets(&self, topic: &str) -> Option<&BTreeMap<u32, u64>> {
        self.per_topic.get(topic)
    }

    /// True when this checkpoint carries no topics at all (an empty
    /// `topics` list was checkpointed, or none of them had a stable name).
    pub fn is_empty(&self) -> bool {
        self.per_topic.is_empty()
    }
}

/// The current tail of `topics` on `laser`'s default stream, one entry per
/// named topic (an [`AgentTopic::Custom`] topic is skipped -- it has no
/// stable name to key a checkpoint by). Every partition that exists at
/// capture time gets an entry, including an empty one (offset `0`), so a
/// later bounded read can tell "nothing yet on this partition" apart from
/// "this partition postdates the checkpoint."
pub async fn checkpoint(
    laser: &Laser,
    topics: &[AgentTopic<'static>],
) -> Result<Checkpoint, LaserError> {
    let stream = Identifier::named(laser.stream_required()?)?;
    let mut per_topic = BTreeMap::new();
    for topic in topics {
        let Some(name) = topic.name() else {
            continue;
        };
        let topic_id = topic.as_identifier();
        let Some(details) = laser.client().get_topic(&stream, &topic_id).await? else {
            per_topic.insert(name.to_owned(), BTreeMap::new());
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
        per_topic.insert(name.to_owned(), offsets);
    }
    Ok(Checkpoint { per_topic })
}

/// Selects which assembled messages feed an LLM call.
pub trait ContextPolicy: Send + Sync {
    fn select(&self, history: &[ContextMessage]) -> Vec<ContextMessage>;
}

/// Keep the most recent N messages.
pub struct LastN(pub usize);

impl ContextPolicy for LastN {
    fn select(&self, history: &[ContextMessage]) -> Vec<ContextMessage> {
        let start = history.len().saturating_sub(self.0);
        history[start..].to_vec()
    }
}

/// Keep only messages from the given agents.
pub struct RoleFilter(pub HashSet<AgentId>);

impl ContextPolicy for RoleFilter {
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
            estimate: Box::new(|message| message.payload.len().div_ceil(4)),
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
    #[builder(default = vec![AgentTopic::Commands, AgentTopic::Responses])]
    topics: Vec<AgentTopic<'static>>,
    #[builder(default = Box::new(LastN(50)))]
    policy: Box<dyn ContextPolicy>,
    /// Per-partition start offsets: partition `p` is read from
    /// `from_offsets[p]` (default `0`). The incremental-resume seam: a fold
    /// seeded from a snapshot passes the snapshot's resume offsets here and
    /// replays only the tail (the bounded-reads law). Ignored for a topic
    /// where [`from_checkpoint`](Self::from_checkpoint) also names it --
    /// set at most one of the two.
    #[builder(default)]
    from_offsets: BTreeMap<u32, u64>,
    /// Like `from_offsets`, but a [`Checkpoint`] taken with
    /// [`ContextScope::checkpoint`](crate::context_scope::ContextScope::checkpoint):
    /// correctly per-topic (`from_offsets` is one map shared across every
    /// topic in `topics`, so it does not distinguish two topics whose
    /// offsets have diverged). Read forward from it to the tail, same as an
    /// unbounded assemble -- the resuming counterpart to `to_checkpoint`,
    /// which stops instead of continuing. `Option<T>` is implicitly
    /// optional to the builder (defaults to `None`), so no `#[builder(default)]`
    /// here.
    from_checkpoint: Option<Checkpoint>,
    /// Never read past this [`Checkpoint`], per topic: the point-in-time
    /// bound behind `Session::state_at` and `ReplayBound::At`. `None` (the
    /// default) reads to the current tail, unchanged from before this
    /// field existed.
    to_checkpoint: Option<Checkpoint>,
}

impl ContextAssembler {
    /// Read the configured topics, order by Iggy timestamp, and apply the policy.
    /// Every (topic, partition) is drained concurrently: a conversation can span
    /// many partitions across several topics, and reading them serially makes
    /// recovery pay one round trip after another.
    pub async fn assemble(self, laser: &Laser) -> Result<Vec<ContextMessage>, LaserError> {
        let stream = Identifier::named(laser.stream_required()?)?;

        // Resolve each topic's partition count concurrently.
        let mut meta = tokio::task::JoinSet::new();
        for (topic_idx, topic) in self.topics.iter().enumerate() {
            let laser = laser.clone();
            let stream = stream.clone();
            let topic_id = topic.as_identifier();
            meta.spawn(async move {
                let count = laser
                    .client()
                    .get_topic(&stream, &topic_id)
                    .await?
                    .map(|details| crate::poll::bounded_partitions(details.partitions_count));
                Ok::<_, LaserError>((topic_idx, topic_id, count))
            });
        }
        let mut sources = Vec::new();
        while let Some(joined) = meta.join_next().await {
            let (topic_idx, topic_id, count) = joined.map_err(join_failed)??;
            for partition in 0..count.unwrap_or(0) {
                sources.push((topic_idx, topic_id.clone(), partition));
            }
        }

        // Drain every partition concurrently.
        let mut drains = tokio::task::JoinSet::new();
        for (topic_idx, topic_id, partition) in sources {
            let laser = laser.clone();
            let stream = stream.clone();
            let topic_name = self.topics[topic_idx].name();
            let from = match &self.from_checkpoint {
                Some(checkpoint) => topic_name
                    .and_then(|name| checkpoint.topic_offsets(name))
                    .and_then(|offsets| offsets.get(&partition).copied())
                    .unwrap_or(0),
                None => self.from_offsets.get(&partition).copied().unwrap_or(0),
            };
            // The point-in-time ceiling for this (topic, partition), if the
            // caller set one: `Some(end)` never reads past `end`, `None`
            // reads to the current tail exactly as before this existed.
            let cap = self.to_checkpoint.as_ref().map(|checkpoint| {
                topic_name
                    .and_then(|name| checkpoint.topic_offsets(name))
                    .and_then(|offsets| offsets.get(&partition).copied())
                    .unwrap_or(0)
            });
            drains.spawn(async move {
                let consumer = Consumer::new(Identifier::named("laser-context-reader")?);
                let start = match cap {
                    // A bounded-above read anchors its window to the
                    // checkpoint, not the current tail: the checkpoint may
                    // sit far behind a tail that has since grown, and
                    // anchoring there (like the tail-anchored path below
                    // does for an unbounded read) would skip straight past
                    // the very history being asked for.
                    Some(end) => {
                        from.max(end.saturating_sub(crate::poll::MAX_DRAIN_MESSAGES as u64 - 1))
                    }
                    // Context selection keeps the most recent records, so a
                    // partition longer than the drain ceiling is read from a
                    // tail-anchored window rather than from its head.
                    None => {
                        crate::poll::tail_anchored_offset(
                            &laser.client(),
                            &stream,
                            &topic_id,
                            &consumer,
                            partition,
                            from,
                        )
                        .await?
                    }
                };
                let batch = crate::poll::drain_partition(
                    &laser.client(),
                    &stream,
                    &topic_id,
                    &consumer,
                    partition,
                    start,
                    READ_BATCH,
                    cap,
                )
                .await?;
                Ok::<_, LaserError>((topic_idx, partition, batch.messages))
            });
        }
        let mut collected: Vec<(u64, usize, ContextMessage)> = Vec::new();
        while let Some(joined) = drains.join_next().await {
            let (topic_idx, partition, messages) = joined.map_err(join_failed)??;
            for message in messages {
                let Ok((provenance, envelope)) = crate::agent::provenance_and_envelope(&message)
                else {
                    continue;
                };
                if self.matches(&provenance) {
                    collected.push((
                        message.header.timestamp,
                        topic_idx,
                        ContextMessage {
                            id: MessageId::new(partition, message.header.offset),
                            provenance,
                            payload: message.payload.to_vec(),
                            envelope,
                        },
                    ));
                }
            }
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

    fn matches(&self, provenance: &Provenance) -> bool {
        if provenance.conversation_id == self.conversation_id {
            return true;
        }
        self.across_subconversations
            && (provenance.root_conversation_id == Some(self.conversation_id)
                || provenance.parent_conversation_id == Some(self.conversation_id))
    }
}

fn join_failed(error: tokio::task::JoinError) -> LaserError {
    LaserError::HandlerConfig(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_a_checkpoint_when_queried_by_topic_then_should_return_only_its_own_offsets() {
        let checkpoint = Checkpoint {
            per_topic: BTreeMap::from([
                (
                    "agent.commands".to_owned(),
                    BTreeMap::from([(0, 5), (1, 2)]),
                ),
                ("agent.llm_io".to_owned(), BTreeMap::new()),
            ]),
        };
        assert_eq!(
            checkpoint.topic_offsets("agent.commands"),
            Some(&BTreeMap::from([(0, 5), (1, 2)]))
        );
        assert_eq!(
            checkpoint.topic_offsets("agent.llm_io"),
            Some(&BTreeMap::new())
        );
        assert_eq!(checkpoint.topic_offsets("agent.tool_calls"), None);
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
