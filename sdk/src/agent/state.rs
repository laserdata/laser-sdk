use crate::context::{ContextAssembler, ContextMessage};
use crate::error::LaserError;
use crate::laser::Laser;
use crate::provenance::AgentTopic;
use crate::snapshot::SnapshotStore;
use crate::types::ConversationId;
use std::collections::BTreeMap;

/// How much of a conversation's log a state fold replays. Explicit at every
/// call (the bounded-reads law): a long-lived conversation is a long
/// partition, and a full-partition walk must be the caller writing the word,
/// never a silent default.
pub enum ReplayBound {
    /// Fold only messages at or after these per-partition offsets, the
    /// incremental form (a persisted cursor, a snapshot's resume offsets).
    FromOffsets(BTreeMap<u32, u64>),
    /// Fold only the last `n` messages.
    Last(usize),
    /// Fold the whole partition from offset zero. Correct for a short
    /// conversation and for a first snapshot build, expensive everywhere else.
    Full,
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
                    .from_offsets(offsets)
                    .build()
                    .assemble(laser)
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
                    .assemble(laser)
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
                let state: S = serde_json::from_slice(&snapshot.state).map_err(|error| {
                    LaserError::Codec(format!("decode snapshot state: {error}"))
                })?;
                (state, ReplayBound::FromOffsets(resume_offsets(&snapshot)))
            }
            None => (init, ReplayBound::Full),
        };
        Self::load(laser, conversation, topics, bound, seed, fold).await
    }
}

/// The per-partition offsets a fold resumes from after `snapshot`: one past
/// each partition's last folded offset.
pub fn resume_offsets(snapshot: &crate::snapshot::FoldSnapshot) -> BTreeMap<u32, u64> {
    snapshot
        .as_of
        .keys()
        .map(|&partition| (partition, snapshot.resume_offset(partition)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::FoldSnapshot;

    #[test]
    fn given_a_snapshot_when_resuming_then_should_start_one_past_each_folded_offset() {
        let snapshot = FoldSnapshot {
            conversation: laser_wire::agent::ConversationId::from_u128(1),
            as_of: BTreeMap::from([(0, 899), (2, 41)]),
            state: b"{}".to_vec(),
        };
        let offsets = resume_offsets(&snapshot);
        assert_eq!(offsets, BTreeMap::from([(0, 900), (2, 42)]));
    }
}
