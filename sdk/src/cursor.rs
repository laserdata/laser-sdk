use crate::error::LaserError;
use crate::laser::Laser;
use crate::message::Message;
use crate::types::MessageId;
use iggy::prelude::*;
use std::collections::BTreeMap;

const DEFAULT_BATCH: u32 = 1000;

impl Laser {
    /// A resumable, offset-addressable reader over `topic`. Each [`poll`](Cursor::poll)
    /// drains every partition from where the previous poll stopped, so repeated
    /// calls read only what is new, never a rescan from zero. You own the offsets:
    /// read them with [`offsets`](Cursor::offsets), persist them in any
    /// [`StateStore`](crate::state_store::StateStore) (in-memory, file, or managed
    /// `Kv`), and resume after a restart with [`from_offsets`](Cursor::from_offsets).
    ///
    /// This is the open streaming primitive the `Agent` runtime sits above: reach
    /// for the runtime when you want Apache Iggy to track offsets for you (consumer
    /// groups, dedup, DLQ), and for a `Cursor` when you want to own the cursor and
    /// checkpoint it yourself.
    ///
    /// Reads from the default stream. Use [`reader_on`](Self::reader_on) to read a
    /// topic on an explicit stream.
    #[cfg(feature = "agent")]
    pub(crate) fn reader(&self, topic: impl Into<String>) -> Result<Cursor, LaserError> {
        let stream = self.stream_required()?.to_owned();
        Cursor::new(self, &stream, &topic.into())
    }

    /// A resumable reader over `topic` on an explicit `stream`, for connection-only
    /// handles or reading across several streams from one connection.
    pub(crate) fn reader_on(
        &self,
        stream: impl Into<String>,
        topic: impl Into<String>,
    ) -> Result<Cursor, LaserError> {
        Cursor::new(self, &stream.into(), &topic.into())
    }
}

/// A resumable reader over one topic. Build it with [`Topic::replay`]. Owns a
/// cheap `Laser` clone, so it moves into a `tokio::spawn` or is stored beside the
/// `Laser` rather than borrowing it.
///
/// [`Topic::replay`]: crate::stream::Topic::replay
pub struct Cursor {
    laser: Laser,
    stream: Identifier,
    topic: Identifier,
    consumer: Consumer,
    offsets: Vec<u64>,
    batch: u32,
}

impl Cursor {
    fn new(laser: &Laser, stream: &str, topic: &str) -> Result<Self, LaserError> {
        Ok(Self {
            stream: Identifier::named(stream)?,
            topic: Identifier::named(topic)?,
            consumer: Consumer::new(Identifier::named("laser-cursor")?),
            laser: laser.clone(),
            offsets: Vec::new(),
            batch: DEFAULT_BATCH,
        })
    }

    /// Resume from previously persisted per-partition offsets: the next offset to
    /// read on each partition, exactly what an earlier [`offsets`](Self::offsets)
    /// returned. A shorter vec than the topic's partition count is padded with 0
    /// (read those partitions from the start).
    pub fn from_offsets(mut self, offsets: Vec<u64>) -> Self {
        self.offsets = offsets;
        self
    }

    /// Read at most `batch` messages per partition per poll (default 1000).
    pub fn batch(mut self, batch: u32) -> Self {
        self.batch = batch.max(1);
        self
    }

    // Attribute the reads to a caller-chosen reader identity (the typed
    // reader passes its name through here). Offsets stay client-owned
    // either way, the identity only names the reader server-side.
    pub(crate) fn consumer_named(mut self, name: &str) -> Result<Self, LaserError> {
        self.consumer = Consumer::new(Identifier::named(name)?);
        Ok(self)
    }

    /// The next offset to read on each partition. Persist this to resume later with
    /// [`from_offsets`](Self::from_offsets).
    pub fn offsets(&self) -> &[u64] {
        &self.offsets
    }

    /// This cursor as a [`Stream`](futures::Stream) of messages, one at a time,
    /// draining everything currently appended and ending (like the async reader in
    /// the Python binding) once caught up. A later `stream()` on a fresh cursor
    /// seeded from the persisted [`offsets`](Self::offsets) resumes from there. Use
    /// it with `futures::StreamExt`: `while let Some(message) = stream.next().await`.
    /// The bounded-read primitive as a stream, not a substitute for the reliable
    /// consumer's continuous delivery.
    pub fn stream(self) -> impl futures::Stream<Item = Result<Message, LaserError>> {
        futures::stream::unfold(
            (self, std::collections::VecDeque::new(), false),
            |(mut cursor, mut buffered, stop)| async move {
                if stop {
                    return None;
                }
                loop {
                    if let Some(message) = buffered.pop_front() {
                        return Some((Ok(message), (cursor, buffered, false)));
                    }
                    match cursor.poll().await {
                        Ok(batch) if batch.is_empty() => return None,
                        Ok(batch) => buffered.extend(batch),
                        Err(error) => return Some((Err(error), (cursor, buffered, true))),
                    }
                }
            },
        )
    }

    /// Drain everything appended since the last poll, advancing the cursor. Returns
    /// the new messages ordered by Iggy timestamp (then partition, offset), or an
    /// empty vec when the reader is caught up (or the topic does not exist yet).
    #[tracing::instrument(target = "laser", level = "debug", skip_all, fields(topic = %self.topic, operation = "poll"))]
    pub async fn poll(&mut self) -> Result<Vec<Message>, LaserError> {
        let Some(details) = self
            .laser
            .client()
            .get_topic(&self.stream, &self.topic)
            .await?
        else {
            return Ok(Vec::new());
        };
        let partitions = details.partitions_count as usize;
        if self.offsets.len() < partitions {
            self.offsets.resize(partitions, 0);
        }
        // (timestamp, partition, offset, message) so the merge across partitions is
        // ordered by Iggy's single clock, like `ContextAssembler`.
        let mut collected: Vec<(u64, u32, u64, Message)> = Vec::new();
        for partition in 0..partitions as u32 {
            let batch = crate::poll::drain_partition(
                self.laser.client(),
                &self.stream,
                &self.topic,
                &self.consumer,
                partition,
                self.offsets[partition as usize],
                self.batch,
            )
            .await?;
            self.offsets[partition as usize] = batch.next_offset;
            for message in batch.messages {
                let offset = message.header.offset;
                collected.push((
                    message.header.timestamp,
                    partition,
                    offset,
                    Message {
                        payload: message.payload.to_vec(),
                        id: MessageId::new(partition, offset),
                        headers: headers_to_strings(&message),
                    },
                ));
            }
        }
        collected
            .sort_by_key(|(timestamp, partition, offset, _)| (*timestamp, *partition, *offset));
        Ok(collected
            .into_iter()
            .map(|(_, _, _, message)| message)
            .collect())
    }
}

// The message's user headers decoded to strings. Non-UTF-8 keys/values are
// dropped (the agent layer reconstructs `Provenance` from these same headers).
fn headers_to_strings(message: &IggyMessage) -> BTreeMap<String, String> {
    let Ok(Some(headers)) = message.user_headers_map() else {
        return BTreeMap::new();
    };
    headers
        .iter()
        .filter_map(|(key, value)| {
            Some((
                key.as_str().ok()?.to_owned(),
                value.as_str().ok()?.to_owned(),
            ))
        })
        .collect()
}
