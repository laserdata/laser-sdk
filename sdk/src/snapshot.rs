use crate::error::LaserError;
use laser_wire::framing::{decode_named, encode_named};
pub use laser_wire::snapshot::FoldSnapshot;
use laser_wire::validate::Validate;

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
/// Apache Iggy.
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
/// no scan. Managed: against Apache Iggy every call returns the kv
/// surface's unsupported error, so pick [`TopicSnapshotStore`] there.
#[cfg(all(feature = "agent", feature = "kv"))]
pub struct KvSnapshotStore {
    laser: crate::laser::Laser,
    namespace: String,
    fold: String,
}

#[cfg(all(feature = "agent", feature = "kv"))]
impl KvSnapshotStore {
    /// A store over the default `agent.snapshots` namespace.
    pub fn new(laser: crate::laser::Laser, fold: impl Into<String>) -> Self {
        Self::in_namespace(laser, DEFAULT_SNAPSHOT_NAMESPACE, fold)
    }

    /// A store over `namespace`, for keeping several folds' snapshots apart.
    pub fn in_namespace(
        laser: crate::laser::Laser,
        namespace: impl Into<String>,
        fold: impl Into<String>,
    ) -> Self {
        Self {
            laser,
            namespace: namespace.into(),
            fold: fold.into(),
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
        let key = snapshot_key(self.laser.stream_required()?, conversation, &self.fold);
        match kv.get(key).await? {
            Some(payload) => {
                let snapshot = decode(&payload)?;
                if snapshot.conversation != conversation || snapshot.fold != self.fold {
                    return Err(LaserError::Invalid(
                        "snapshot identity does not match the store".to_owned(),
                    ));
                }
                Ok(Some(snapshot))
            }
            None => Ok(None),
        }
    }

    async fn save(&self, snapshot: &FoldSnapshot) -> Result<(), LaserError> {
        validate_store_identity(self.laser.stream_required()?, &self.fold, snapshot)?;
        let payload = encode(snapshot)?;
        let key = snapshot_key(&snapshot.stream, snapshot.conversation, &snapshot.fold);
        self.laser
            .kv(&self.namespace)
            .set(key)
            .bytes(payload)
            .send()
            .await
    }
}

/// Fold snapshots as records on a dedicated snapshots topic (default
/// `agent.snapshots`), partitioned by conversation so one conversation's
/// checkpoints stay ordered. Log-native: works on Apache Iggy.
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
    fold: String,
}

#[cfg(feature = "agent")]
impl TopicSnapshotStore {
    /// A store over the default `agent.snapshots` topic.
    pub fn new(laser: crate::laser::Laser, fold: impl Into<String>) -> Self {
        Self::on_topic(laser, DEFAULT_SNAPSHOT_TOPIC, fold)
    }

    /// A store over `topic`, for keeping several folds' snapshots apart.
    pub fn on_topic(
        laser: crate::laser::Laser,
        topic: impl Into<String>,
        fold: impl Into<String>,
    ) -> Self {
        Self {
            laser,
            topic: topic.into(),
            fold: fold.into(),
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
        let stream_name = self.laser.stream_required()?.to_owned();
        let stream = Identifier::named(&stream_name)?;
        let topic = Identifier::named(&self.topic)?;
        let consumer = Consumer::new(Identifier::named("laser-snapshot-reader")?);
        let client = self.laser.client();
        let Some(details) = client.get_topic(&stream, &topic).await? else {
            // No topic yet means nothing was ever saved.
            return Ok(None);
        };
        let mut newest: Option<((u64, u32, u64), FoldSnapshot)> = None;
        for partition in 0..crate::poll::bounded_partitions(details.partitions_count) {
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
            let mut end = last.header.offset.saturating_add(1);
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
                            .filter(|snapshot| {
                                snapshot.conversation == conversation
                                    && snapshot.fold == self.fold
                                    && snapshot.stream == stream_name
                            })
                            .map(|snapshot| {
                                (message.header.timestamp, message.header.offset, snapshot)
                            })
                    });
                if let Some((timestamp, offset, snapshot)) = found {
                    let position = (timestamp, partition, offset);
                    if newest.as_ref().is_none_or(|(best, _)| position > *best) {
                        newest = Some((position, snapshot));
                    }
                    break;
                }
                end = start;
            }
        }
        Ok(newest.map(|(_, snapshot)| snapshot))
    }

    async fn save(&self, snapshot: &FoldSnapshot) -> Result<(), LaserError> {
        validate_store_identity(self.laser.stream_required()?, &self.fold, snapshot)?;
        let payload = encode(snapshot)?;
        let partition_key = format!(
            "{}:{}:{}",
            snapshot.conversation,
            snapshot.fold.len(),
            snapshot.fold
        );
        self.laser
            .topic(&self.topic)
            .send(
                payload,
                std::collections::BTreeMap::new(),
                Some(&partition_key),
            )
            .await
            .map(|_| ())
    }
}

/// Encode a fold snapshot to its canonical bytes, for storage as a key-value
/// value or a snapshot-topic body. The inverse of [`decode`].
pub fn encode(snapshot: &FoldSnapshot) -> Result<Vec<u8>, LaserError> {
    snapshot
        .validate()
        .map_err(|error| LaserError::Invalid(error.to_string()))?;
    encode_named(snapshot).map_err(|error| LaserError::Codec(format!("encode snapshot: {error}")))
}

/// Decode a fold snapshot from stored bytes. The inverse of [`encode`].
pub fn decode(payload: &[u8]) -> Result<FoldSnapshot, LaserError> {
    let snapshot: FoldSnapshot = decode_named(payload).map_err(LaserError::from)?;
    snapshot
        .validate()
        .map_err(|error| LaserError::Invalid(error.to_string()))?;
    Ok(snapshot)
}

#[cfg(all(feature = "agent", feature = "kv"))]
fn snapshot_key(
    stream: &str,
    conversation: laser_wire::agent::ConversationId,
    fold: &str,
) -> String {
    format!("{}:{stream}:{conversation}:{fold}", stream.len())
}

#[cfg(feature = "agent")]
fn validate_store_identity(
    stream: &str,
    fold: &str,
    snapshot: &FoldSnapshot,
) -> Result<(), LaserError> {
    if snapshot.stream != stream || snapshot.fold != fold {
        return Err(LaserError::Invalid(
            "snapshot identity does not match the store".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use laser_wire::agent::ConversationId;
    use laser_wire::snapshot::SnapshotOffset;

    #[test]
    fn given_a_snapshot_when_round_tripped_through_bytes_then_should_be_unchanged() {
        let snapshot = FoldSnapshot {
            stream: "agents".to_owned(),
            stream_id: 0,
            stream_created_at_micros: 100,
            conversation: ConversationId::from_u128(7),
            fold: "planner".to_owned(),
            as_of: vec![
                SnapshotOffset::new(2, 20, 0, 41),
                SnapshotOffset::new(2, 20, 1, 9),
            ],
            state: br#"{"folded":true}"#.to_vec(),
        };
        assert_eq!(
            decode(&encode(&snapshot).expect("encodes")).expect("decodes"),
            snapshot
        );
    }

    #[test]
    #[cfg(all(feature = "agent", feature = "kv"))]
    fn given_two_folds_in_one_conversation_when_keyed_then_should_have_distinct_keys() {
        let conversation = ConversationId::from_u128(7);
        assert_ne!(
            snapshot_key("agents", conversation, "planner"),
            snapshot_key("agents", conversation, "worker")
        );
        assert_ne!(
            snapshot_key("agents", conversation, "planner"),
            snapshot_key("other", conversation, "planner")
        );
    }
}
