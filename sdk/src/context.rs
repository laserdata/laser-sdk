use crate::agent::Laser;
use crate::error::LaserError;
use crate::provenance::{AgentTopic, Provenance};
use crate::types::{AgentId, ConversationId, MessageId};
use iggy::prelude::*;
use std::collections::HashSet;

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
            drains.spawn(async move {
                let consumer = Consumer::new(Identifier::named("laser-context-reader")?);
                let batch = crate::poll::drain_partition(
                    laser.client(),
                    &stream,
                    &topic_id,
                    &consumer,
                    partition,
                    0,
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
    LaserError::Handler(error.to_string())
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
