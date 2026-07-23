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
    /// replays only the tail (the bounded-reads law).
    #[builder(default)]
    from_offsets: BTreeMap<u32, u64>,
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
                    .map(|details| details.partitions_count);
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
            let start = self.from_offsets.get(&partition).copied().unwrap_or(0);
            drains.spawn(async move {
                let consumer = Consumer::new(Identifier::named("laser-context-reader")?);
                let batch = crate::poll::drain_partition(
                    laser.client(),
                    &stream,
                    &topic_id,
                    &consumer,
                    partition,
                    start,
                    READ_BATCH,
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
        // Order by the Iggy-assigned timestamp: a single global clock across topics,
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
