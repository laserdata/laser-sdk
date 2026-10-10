use crate::context::{Checkpoint, ContextAssembler, ContextMessage};
use crate::error::LaserError;
use crate::laser::Laser;
use crate::provenance::AgentTopic;
use crate::snapshot::SnapshotStore;
use crate::types::ConversationId;
use iggy::prelude::*;
use laser_wire::snapshot::{FoldSnapshot, SnapshotOffset};
use laser_wire::validate::Validate;
use std::collections::{BTreeMap, BTreeSet};

/// How much of a conversation's log a state fold replays. Explicit at every
/// call (the bounded-reads law): a long-lived conversation is a long
/// partition, and a full-partition walk must be the caller writing the word,
/// never a silent default. Every bound except [`Last`](Self::Last) folds its
/// whole range, read in bounded chunks.
pub enum ReplayBound {
    /// Fold only messages at or after these per-partition offsets, the
    /// incremental form (a persisted cursor, a snapshot's resume offsets).
    /// Each topic has its own partition offsets.
    FromOffsets(BTreeMap<String, BTreeMap<u32, u64>>),
    /// Fold only the last `n` messages found in the newest
    /// [`CONTEXT_READ_WINDOW`](crate::context::CONTEXT_READ_WINDOW) records of
    /// each partition, the window a context read covers. On a busy shared
    /// partition a quiet conversation can have fewer than `n` there.
    Last(usize),
    /// Fold the whole partition from offset zero. Correct for a short
    /// conversation and for a first snapshot build, expensive everywhere else.
    Full,
    /// Fold only what was appended after a [`Checkpoint`], per topic and
    /// partition, up to the tail.
    FromCheckpoint(Checkpoint),
    /// Fold the history up to a [`Checkpoint`], per topic and partition, and
    /// stop there.
    At(Checkpoint),
}

/// Rebuilds in-memory state by folding a conversation's logged events (event sourcing).
pub struct ConversationState;

impl ConversationState {
    /// Replay `topics` for `conversation` under the explicit `bound` and fold
    /// every message through `fold`, starting from `init`.
    pub async fn load<S, F>(
        laser: &Laser,
        conversation: ConversationId,
        topics: Vec<AgentTopic<'static>>,
        bound: ReplayBound,
        init: S,
        fold: F,
    ) -> Result<S, LaserError>
    where
        F: FnMut(S, &ContextMessage) -> S,
    {
        let assembler = ContextAssembler::builder()
            .conversation_id(conversation)
            .topics(topics);
        let history = match bound {
            ReplayBound::FromOffsets(offsets) => {
                assembler
                    .policy(Box::new(crate::context::LastN(usize::MAX)))
                    .from_checkpoint(Checkpoint::from_topic_offsets(offsets))
                    .build()
                    .replay(laser)
                    .await?
            }
            ReplayBound::Last(n) => {
                assembler
                    .policy(Box::new(crate::context::LastN(n)))
                    .build()
                    .assemble(laser)
                    .await?
            }
            ReplayBound::Full => {
                assembler
                    .policy(Box::new(crate::context::LastN(usize::MAX)))
                    .build()
                    .replay(laser)
                    .await?
            }
            ReplayBound::FromCheckpoint(checkpoint) => {
                assembler
                    .policy(Box::new(crate::context::LastN(usize::MAX)))
                    .from_checkpoint(checkpoint)
                    .build()
                    .replay(laser)
                    .await?
            }
            ReplayBound::At(checkpoint) => {
                assembler
                    .policy(Box::new(crate::context::LastN(usize::MAX)))
                    .to_checkpoint(checkpoint)
                    .build()
                    .replay(laser)
                    .await?
            }
        };
        Ok(history.iter().fold(init, fold))
    }

    /// Replay through a [`SnapshotStore`]: seed the fold with the newest
    /// snapshot's state (decoded as JSON into `S`) and replay only from
    /// `as_of + 1` per partition, so a thousand-record conversation
    /// snapshotted at nine hundred folds one hundred records, not a thousand.
    /// A conversation with no snapshot folds fully from `init`, the honest
    /// first build.
    pub async fn load_with<Store, S, F>(
        laser: &Laser,
        store: &Store,
        conversation: ConversationId,
        topics: Vec<AgentTopic<'static>>,
        init: S,
        fold: F,
    ) -> Result<S, LaserError>
    where
        Store: SnapshotStore + Sync,
        S: serde::de::DeserializeOwned,
        F: FnMut(S, &ContextMessage) -> S,
    {
        let (seed, bound) = match store.latest(conversation.into()).await? {
            Some(snapshot) => {
                if snapshot.conversation != conversation.into() {
                    return Err(LaserError::Invalid(
                        "snapshot conversation does not match the fold".to_owned(),
                    ));
                }
                let checkpoint = checkpoint_from_snapshot(laser, &snapshot, &topics).await?;
                let state: S = serde_json::from_slice(&snapshot.state).map_err(|error| {
                    LaserError::Codec(format!("decode snapshot state: {error}"))
                })?;
                (state, ReplayBound::FromCheckpoint(checkpoint))
            }
            None => (init, ReplayBound::Full),
        };
        Self::load(laser, conversation, topics, bound, seed, fold).await
    }
}

/// The per-partition offsets a fold resumes from after `snapshot`: one past
/// each partition's last folded offset.
pub fn resume_offsets(snapshot: &FoldSnapshot) -> Vec<SnapshotOffset> {
    snapshot
        .as_of
        .iter()
        .map(|entry| {
            SnapshotOffset::new(
                entry.topic_id,
                entry.topic_created_at_micros,
                entry.partition_id,
                entry.offset.saturating_add(1),
            )
        })
        .collect()
}

/// A snapshot of `state` folded up to `checkpoint` for `conversation` under
/// the fold named `fold`. It records the stream and every checkpointed
/// topic by id and creation time, so a resume refuses a recreated source.
/// Empty partitions are left out.
pub async fn snapshot_from_checkpoint(
    laser: &Laser,
    conversation: ConversationId,
    fold: &str,
    checkpoint: &Checkpoint,
    state: Vec<u8>,
) -> Result<FoldSnapshot, LaserError> {
    let stream_name = laser.stream_required()?;
    let stream = Identifier::named(stream_name)?;
    let client = laser.client();
    let details = client
        .get_stream(&stream)
        .await?
        .ok_or_else(|| LaserError::Invalid(format!("stream `{stream_name}` does not exist")))?;
    let mut as_of = Vec::new();
    for (topic, partitions) in checkpoint.topics() {
        let topic_details = client
            .get_topic(&stream, &Identifier::named(topic)?)
            .await?
            .ok_or_else(|| LaserError::Invalid(format!("topic `{topic}` does not exist")))?;
        for (&partition, &next) in partitions {
            if next > 0 {
                as_of.push(SnapshotOffset::new(
                    topic_details.id,
                    topic_details.created_at.as_micros(),
                    partition,
                    next - 1,
                ));
            }
        }
    }
    as_of.sort_by_key(|entry| {
        (
            entry.topic_id,
            entry.topic_created_at_micros,
            entry.partition_id,
        )
    });
    let snapshot = FoldSnapshot {
        stream: stream_name.to_owned(),
        stream_id: details.id,
        stream_created_at_micros: details.created_at.as_micros(),
        conversation: conversation.into(),
        fold: fold.to_owned(),
        as_of,
        state,
    };
    snapshot
        .validate()
        .map_err(|error| LaserError::Invalid(error.to_string()))?;
    Ok(snapshot)
}

pub async fn checkpoint_from_snapshot(
    laser: &Laser,
    snapshot: &FoldSnapshot,
    topics: &[AgentTopic<'static>],
) -> Result<Checkpoint, LaserError> {
    snapshot
        .validate()
        .map_err(|error| LaserError::Invalid(error.to_string()))?;
    let stream_name = laser.stream_required()?;
    if snapshot.stream != stream_name {
        return Err(LaserError::Invalid(
            "snapshot stream does not match the client".to_owned(),
        ));
    }
    let stream = Identifier::named(stream_name)?;
    let client = laser.client();
    let current_stream = client
        .get_stream(&stream)
        .await?
        .ok_or_else(|| LaserError::Invalid("snapshot stream no longer exists".to_owned()))?;
    if snapshot.stream_id != current_stream.id
        || snapshot.stream_created_at_micros != current_stream.created_at.as_micros()
    {
        return Err(LaserError::Invalid(
            "snapshot stream generation changed".to_owned(),
        ));
    }
    let mut per_topic = BTreeMap::new();
    let mut current_sources = BTreeSet::new();
    for topic in topics {
        let name = topic.topic_string();
        let details = client
            .get_topic(&stream, &topic.as_identifier())
            .await?
            .ok_or_else(|| {
                LaserError::Invalid(format!("snapshot source topic {name} is missing"))
            })?;
        let generation = details.created_at.as_micros();
        current_sources.insert((details.id, generation));
        let offsets = (0..crate::poll::bounded_partitions(details.partitions_count))
            .map(|partition| {
                (
                    partition,
                    snapshot.resume_offset(details.id, generation, partition),
                )
            })
            .collect();
        per_topic.insert(name, offsets);
    }
    if snapshot
        .as_of
        .iter()
        .any(|entry| !current_sources.contains(&(entry.topic_id, entry.topic_created_at_micros)))
    {
        return Err(LaserError::Invalid(
            "snapshot source topic generation changed".to_owned(),
        ));
    }
    Ok(Checkpoint::from_topic_offsets(per_topic))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::FoldSnapshot;
    use laser_wire::snapshot::SnapshotOffset;

    #[test]
    fn given_a_snapshot_when_resuming_then_should_start_one_past_each_folded_offset() {
        let snapshot = FoldSnapshot {
            stream: "agents".to_owned(),
            stream_id: 0,
            stream_created_at_micros: 100,
            conversation: laser_wire::agent::ConversationId::from_u128(1),
            fold: "planner".to_owned(),
            as_of: vec![
                SnapshotOffset::new(2, 20, 0, 899),
                SnapshotOffset::new(2, 20, 2, 41),
            ],
            state: b"{}".to_vec(),
        };
        let offsets = resume_offsets(&snapshot);
        assert_eq!(
            offsets,
            vec![
                SnapshotOffset::new(2, 20, 0, 900),
                SnapshotOffset::new(2, 20, 2, 42)
            ]
        );
    }
}
