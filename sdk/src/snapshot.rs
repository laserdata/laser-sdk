use crate::error::LaserError;
use laser_wire::framing::{decode_named, encode_named};
pub use laser_wire::snapshot::FoldSnapshot;

/// The key-value namespace [`KvSnapshotStore::new`] uses.
#[cfg(all(feature = "agent", feature = "kv"))]
pub const DEFAULT_SNAPSHOT_NAMESPACE: &str = "agent.snapshots";

/// The topic [`TopicSnapshotStore::new`] uses.
#[cfg(feature = "agent")]
pub const DEFAULT_SNAPSHOT_TOPIC: &str = "agent.snapshots";

/// How many records each backward scan window reads while looking for a
/// conversation's newest checkpoint.
#[cfg(feature = "agent")]
const SNAPSHOT_SCAN_BATCH: u32 = 256;

/// Where fold snapshots live, so a long conversation resumes from its last
/// checkpoint instead of replaying every record (the bounded-reads law). Same
/// seam pattern as `StateStore`: one trait, honest backends, no behavior
/// difference visible to the caller. [`KvSnapshotStore`] is the managed
/// point-state form, [`TopicSnapshotStore`] the log-native one that works on
/// raw Apache Iggy.
#[cfg(feature = "agent")]
#[trait_variant::make(SnapshotStore: Send)]
pub trait LocalSnapshotStore {
    /// The newest snapshot for `conversation`, or `None` when it has never
    /// been snapshotted (the fold starts from offset zero).
    async fn latest(
        &self,
        conversation: laser_wire::agent::ConversationId,
    ) -> Result<Option<FoldSnapshot>, LaserError>;
    /// Persist `snapshot` as the conversation's newest checkpoint.
    async fn save(&self, snapshot: &FoldSnapshot) -> Result<(), LaserError>;
}

/// Fold snapshots in the managed key-value store, one key per conversation in
/// a dedicated namespace (default `agent.snapshots`). Point read, point write,
/// no scan. Managed: against raw Apache Iggy every call returns the kv
/// surface's unsupported error, so pick [`TopicSnapshotStore`] there.
#[cfg(all(feature = "agent", feature = "kv"))]
pub struct KvSnapshotStore {
    laser: crate::laser::Laser,
    namespace: String,
}

#[cfg(all(feature = "agent", feature = "kv"))]
impl KvSnapshotStore {
    /// A store over the default `agent.snapshots` namespace.
    pub fn new(laser: crate::laser::Laser) -> Self {
        Self::in_namespace(laser, DEFAULT_SNAPSHOT_NAMESPACE)
    }

    /// A store over `namespace`, for keeping several folds' snapshots apart.
    pub fn in_namespace(laser: crate::laser::Laser, namespace: impl Into<String>) -> Self {
        Self {
            laser,
            namespace: namespace.into(),
        }
    }
}

#[cfg(all(feature = "agent", feature = "kv"))]
impl SnapshotStore for KvSnapshotStore {
    async fn latest(
        &self,
        conversation: laser_wire::agent::ConversationId,
    ) -> Result<Option<FoldSnapshot>, LaserError> {
        let kv = self.laser.kv(&self.namespace);
        match kv.get(conversation.to_string()).await? {
            Some(payload) => Ok(Some(decode(&payload)?)),
            None => Ok(None),
        }
    }

    async fn save(&self, snapshot: &FoldSnapshot) -> Result<(), LaserError> {
        let payload = encode(snapshot)?;
        self.laser
            .kv(&self.namespace)
            .set(snapshot.conversation.to_string())
            .bytes(payload)
            .send()
            .await
    }
}

/// Fold snapshots as records on a dedicated snapshots topic (default
/// `agent.snapshots`), partitioned by conversation so one conversation's
/// checkpoints stay ordered. Log-native: works on raw Apache Iggy.
///
/// `latest` walks the topic backward from each partition's tail and stops at
/// the first (newest) record for the conversation, so a hit costs the tail
/// distance to the last checkpoint. A conversation with no snapshot walks the
/// topic fully before answering `None`: keep the snapshots topic on retention
/// (its history is checkpoints, not truth) so that walk stays bounded.
#[cfg(feature = "agent")]
pub struct TopicSnapshotStore {
    laser: crate::laser::Laser,
    topic: String,
}

#[cfg(feature = "agent")]
impl TopicSnapshotStore {
    /// A store over the default `agent.snapshots` topic.
    pub fn new(laser: crate::laser::Laser) -> Self {
        Self::on_topic(laser, DEFAULT_SNAPSHOT_TOPIC)
    }

    /// A store over `topic`, for keeping several folds' snapshots apart.
    pub fn on_topic(laser: crate::laser::Laser, topic: impl Into<String>) -> Self {
        Self {
            laser,
            topic: topic.into(),
        }
    }
}

#[cfg(feature = "agent")]
impl SnapshotStore for TopicSnapshotStore {
    async fn latest(
        &self,
        conversation: laser_wire::agent::ConversationId,
    ) -> Result<Option<FoldSnapshot>, LaserError> {
        use iggy::prelude::*;
        let stream = Identifier::named(self.laser.stream_required()?)?;
        let topic = Identifier::named(&self.topic)?;
        let consumer = Consumer::new(Identifier::named("laser-snapshot-reader")?);
        let client = self.laser.client();
        let Some(details) = client.get_topic(&stream, &topic).await? else {
            // No topic yet means nothing was ever saved.
            return Ok(None);
        };
        // The partitioner owns the conversation-to-partition mapping, so every
        // partition's tail is walked and the newest match across them wins.
        let mut newest: Option<(u64, FoldSnapshot)> = None;
        for partition in 0..details.partitions_count {
            let tail = client
                .poll_messages(
                    &stream,
                    &topic,
                    Some(partition),
                    &consumer,
                    &PollingStrategy::last(),
                    1,
                    false,
                )
                .await?;
            let Some(last) = tail.messages.last() else {
                continue;
            };
            let mut end = last.header.offset + 1;
            // Walk backward in windows: newest window first, and within a
            // window the highest matching offset wins, so the scan stops at
            // the conversation's most recent checkpoint.
            while end > 0 {
                let start = end.saturating_sub(u64::from(SNAPSHOT_SCAN_BATCH));
                let window = client
                    .poll_messages(
                        &stream,
                        &topic,
                        Some(partition),
                        &consumer,
                        &PollingStrategy::offset(start),
                        SNAPSHOT_SCAN_BATCH,
                        false,
                    )
                    .await?;
                let found = window
                    .messages
                    .iter()
                    .rev()
                    .filter(|message| message.header.offset < end)
                    .find_map(|message| {
                        decode(&message.payload)
                            .ok()
                            .filter(|snapshot| snapshot.conversation == conversation)
                            .map(|snapshot| (message.header.offset, snapshot))
                    });
                if let Some((offset, snapshot)) = found {
                    if newest.as_ref().is_none_or(|(best, _)| offset > *best) {
                        newest = Some((offset, snapshot));
                    }
                    break;
                }
                end = start;
            }
        }
        Ok(newest.map(|(_, snapshot)| snapshot))
    }

    async fn save(&self, snapshot: &FoldSnapshot) -> Result<(), LaserError> {
        let payload = encode(snapshot)?;
        let conversation = snapshot.conversation.to_string();
        self.laser
            .topic(&self.topic)
            .send(
                payload,
                std::collections::BTreeMap::new(),
                Some(&conversation),
            )
            .await
    }
}

/// Encode a fold snapshot to its canonical bytes, for storage as a key-value
/// value or a snapshot-topic body. The inverse of [`decode`].
pub fn encode(snapshot: &FoldSnapshot) -> Result<Vec<u8>, LaserError> {
    encode_named(snapshot).map_err(|error| LaserError::Codec(format!("encode snapshot: {error}")))
}

/// Decode a fold snapshot from stored bytes. The inverse of [`encode`].
pub fn decode(payload: &[u8]) -> Result<FoldSnapshot, LaserError> {
    decode_named(payload).map_err(LaserError::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use laser_wire::agent::ConversationId;
    use std::collections::BTreeMap;

    #[test]
    fn given_a_snapshot_when_round_tripped_through_bytes_then_should_be_unchanged() {
        let snapshot = FoldSnapshot {
            conversation: ConversationId::from_u128(7),
            as_of: BTreeMap::from([(0, 41), (1, 9)]),
            state: br#"{"folded":true}"#.to_vec(),
        };
        assert_eq!(
            decode(&encode(&snapshot).expect("encodes")).expect("decodes"),
            snapshot
        );
    }
}
