use crate::agent::consumer::AgentMessage;
use crate::agent::{ChunkAssembler, StreamEvent};
use crate::context::ContextAssembler;
use crate::error::LaserError;
use crate::laser::{Laser, ensure_stream, ensure_topic};
use crate::provenance::{AgentTopic, Provenance};
use crate::types::{ConversationId, MessageId};
use iggy::prelude::*;
use laser_wire::agent::{AgentDeadLetter, AgentEnvelope, AgentKind, ChannelId, CorrelationId};
use laser_wire::framing::decode_named;
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::time::{Instant, sleep};
use tracing::info;

const WELL_KNOWN_TOPICS: [AgentTopic<'static>; 8] = [
    AgentTopic::Commands,
    AgentTopic::Responses,
    AgentTopic::ToolCalls,
    AgentTopic::ToolResults,
    AgentTopic::LlmIo,
    AgentTopic::HumanInput,
    AgentTopic::Audit,
    AgentTopic::Dlq,
];

const REPLY_BATCH: u32 = 1000;

impl Laser {
    /// Create the default data stream and the well-known agent topics (commands, responses, ...), `partitions` each. Idempotent. Requires a default stream (see [`Laser::with_stream`]).
    ///
    /// Warms a producer for every well-known topic concurrently, so that cost is
    /// paid once up front instead of lazily on a handler's first reply.
    pub async fn bootstrap(&self, partitions: u32) -> Result<(), LaserError> {
        let stream = self.stream_required()?;
        ensure_stream(self.client(), stream).await?;
        for topic in WELL_KNOWN_TOPICS {
            ensure_topic(self.client(), stream, &topic.topic_string(), partitions).await?;
        }
        let mut warming = tokio::task::JoinSet::new();
        for topic in WELL_KNOWN_TOPICS {
            let laser = self.clone();
            let stream = stream.to_owned();
            warming.spawn(async move { laser.producer_on(&stream, &topic.topic_string()).await });
        }
        while let Some(joined) = warming.join_next().await {
            joined.map_err(|_| LaserError::Config("producer warm-up task panicked"))??;
        }
        info!(stream = %stream, partitions, "bootstrapped agent topology");
        Ok(())
    }

    /// Append `payload` to an agent `topic`, stamping `provenance` as headers and keying the partition by conversation.
    pub async fn send_agent(
        &self,
        topic: AgentTopic<'_>,
        payload: impl Into<Vec<u8>>,
        provenance: &Provenance,
    ) -> Result<(), LaserError> {
        let headers: BTreeMap<HeaderKey, HeaderValue> = provenance.try_into()?;
        let key = provenance.partition_key();
        self.send_with_headers(&topic.topic_string(), payload, headers, Some(&key))
            .await
    }

    /// Redrive a dead-lettered message: read the original record at the
    /// capsule's `source` position and republish it verbatim (headers and body)
    /// to that stream and topic, re-keyed by its conversation, so a fixed
    /// handler reprocesses it. Errors if the source stream/topic no longer
    /// exists or the record has aged out of the log's retention window.
    pub async fn redrive_dead_letter(&self, capsule: &AgentDeadLetter) -> Result<(), LaserError> {
        let source = capsule.source;
        let stream = Identifier::numeric(source.stream_id)?;
        let topic = Identifier::numeric(source.topic_id)?;
        let stream_name = self
            .client()
            .get_stream(&stream)
            .await?
            .ok_or_else(|| {
                LaserError::Invalid(format!(
                    "dead-letter source stream {} no longer exists",
                    source.stream_id
                ))
            })?
            .name;
        let topic_name = self
            .client()
            .get_topic(&stream, &topic)
            .await?
            .ok_or_else(|| {
                LaserError::Invalid(format!(
                    "dead-letter source topic {} no longer exists",
                    source.topic_id
                ))
            })?
            .name;
        let reader = Consumer::new(Identifier::named("laser-redrive")?);
        let polled = self
            .client()
            .poll_messages(
                &stream,
                &topic,
                Some(source.partition_id),
                &reader,
                &PollingStrategy::offset(source.offset),
                1,
                false,
            )
            .await?;
        let original = polled
            .messages
            .into_iter()
            .find(|message| message.header.offset == source.offset)
            .ok_or_else(|| {
                LaserError::Invalid(format!(
                    "dead-letter source record at offset {} is no longer on the log",
                    source.offset
                ))
            })?;
        let headers = original.user_headers_map()?.unwrap_or_default();
        // Re-key by the original conversation so the redriven copy lands on the
        // same partition and stays ordered with the rest of its conversation.
        let key = Provenance::try_from(&original)
            .ok()
            .map(|provenance| provenance.partition_key());
        let message = IggyMessage::builder()
            .payload(original.payload)
            .user_headers(headers)
            .build()?;
        self.send_batch_on(&stream_name, &topic_name, vec![message], key.as_deref())
            .await
    }

    /// Reassemble a chunk stream from the log: read `conversation` on `topic`,
    /// take the `chunk` envelopes for `channel` in `sequence` order, and replay
    /// them through a [`ChunkAssembler`] into the ordered [`StreamEvent`]s. This
    /// is the log-native form of resuming or replaying a token stream. Offset
    /// replay does what SSE replay cannot, so a finished stream reconstructs
    /// deterministically after the fact with no external transcript store.
    pub async fn reassemble_channel(
        &self,
        conversation: ConversationId,
        topic: AgentTopic<'static>,
        channel: ChannelId,
    ) -> Result<Vec<StreamEvent>, LaserError> {
        let messages = ContextAssembler::builder()
            .conversation_id(conversation)
            .topics(vec![topic])
            .build()
            .assemble(self)
            .await?;
        let mut chunks: Vec<_> = messages
            .iter()
            .filter_map(|message| message.envelope.as_ref())
            .filter(|envelope| {
                envelope.kind == AgentKind::Chunk && envelope.channel == Some(channel)
            })
            .collect();
        // Within one conversation partition the server delivers in publish
        // order, but a stream can be keyed onto several partitions, so ordering by
        // `sequence` reassembles deterministically regardless.
        chunks.sort_by_key(|envelope| envelope.sequence);
        let mut assembler = ChunkAssembler::new();
        let mut events = Vec::new();
        for chunk in chunks {
            events.extend(assembler.feed(chunk));
        }
        Ok(events)
    }

    /// Wait for the AGDX `response`/`error` carrying `correlation` on `reply_topic`,
    /// up to `timeout`. Backed by a forward-advancing reader: the reply topic is
    /// read incrementally (offsets only move forward), never re-scanned from the
    /// start each poll. Used by the synchronous bridge calls (MCP `tools/call`).
    pub(crate) async fn await_agdx_reply(
        &self,
        reply_topic: AgentTopic<'_>,
        correlation: CorrelationId,
        timeout: Duration,
    ) -> Result<AgentEnvelope, LaserError> {
        let mut reader =
            AgentReplyReader::new(self.stream_required()?, reply_topic.as_identifier())?;
        let deadline = Instant::now() + timeout;
        loop {
            match reader.next_agdx_match(self.client(), correlation).await? {
                ReplyScan::Found(envelope) => return Ok(*envelope),
                ReplyScan::More => continue,
                ReplyScan::CaughtUp => {
                    if Instant::now() >= deadline {
                        return Err(LaserError::Timeout("the AGDX reply"));
                    }
                    sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }

    /// One forward pass over `reply_topic` for the AGDX `response`/`error`
    /// carrying `correlation`: the answer if it has already landed, else `None`.
    /// A point lookup (no waiting), for the stateless A2A `tasks/get`.
    #[cfg(feature = "a2a-bridge")]
    pub(crate) async fn find_agdx_reply(
        &self,
        reply_topic: AgentTopic<'_>,
        correlation: CorrelationId,
    ) -> Result<Option<AgentEnvelope>, LaserError> {
        let mut reader =
            AgentReplyReader::new(self.stream_required()?, reply_topic.as_identifier())?;
        loop {
            match reader.next_agdx_match(self.client(), correlation).await? {
                ReplyScan::Found(envelope) => return Ok(Some(*envelope)),
                ReplyScan::More => continue,
                ReplyScan::CaughtUp => return Ok(None),
            }
        }
    }

    /// A fresh child conversation of `parent`, carrying its parent/root ids for causality.
    pub fn spawn_subconversation(&self, parent: &Provenance) -> Provenance {
        let root = parent
            .root_conversation_id
            .unwrap_or(parent.conversation_id);
        Provenance::builder()
            .conversation_id(ConversationId::new())
            .parent_conversation_id(parent.conversation_id)
            .root_conversation_id(root)
            .build()
    }

    /// Send a request and await its correlated reply. Correlation is a fresh
    /// `idempotency_key` (Ulid) stamped on the request. The responder echoes it
    /// back via `AgentCtx::respond`, and this scans the reply topic for a
    /// matching key until the timeout. `conversation_id` alone is NOT used for
    /// correlation, so a forged reply that guesses the conversation id cannot
    /// hijack the request. If `provenance.idempotency_key` is already set, that
    /// value is used as-is (caller-supplied correlation).
    pub async fn request(
        &self,
        request_topic: AgentTopic<'_>,
        reply_topic: AgentTopic<'_>,
        payload: impl Into<Vec<u8>>,
        provenance: &Provenance,
        timeout: Duration,
    ) -> Result<AgentMessage, LaserError> {
        let correlation = provenance
            .idempotency_key
            .clone()
            .unwrap_or_else(|| ulid::Ulid::new().to_string());
        let mut provenance = provenance.clone();
        provenance.idempotency_key = Some(correlation.clone());
        self.send_agent(request_topic, payload, &provenance).await?;
        let mut reader =
            AgentReplyReader::new(self.stream_required()?, reply_topic.as_identifier())?;
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(reply) = reader.next_match(self.client(), &correlation).await? {
                return Ok(reply);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(LaserError::Timeout("reply"));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

// The agentic reply reader: matches by `agdx.idem` (fresh per-request
// nonce stamped by `Laser::request`), decodes `Provenance`, returns
// `AgentMessage`. The generic core has its own reader matching
// `agdx.corr` and returning `Message`. Caches `partitions_count` on
// first call so a 30s timeout does not do 300 `get_topic` round-trips.
struct AgentReplyReader {
    stream: Identifier,
    topic: Identifier,
    consumer: Consumer,
    offsets: Vec<u64>,
    partitions: Option<u32>,
}

impl AgentReplyReader {
    fn new(stream: &str, topic: Identifier) -> Result<Self, LaserError> {
        Ok(Self {
            stream: Identifier::named(stream)?,
            topic,
            consumer: Consumer::new(Identifier::named("laser-agent-reply-reader")?),
            offsets: Vec::new(),
            partitions: None,
        })
    }

    async fn next_match(
        &mut self,
        client: &IggyClient,
        correlation: &str,
    ) -> Result<Option<AgentMessage>, LaserError> {
        let partitions = match self.partitions {
            Some(value) => value,
            None => {
                let Some(details) = client.get_topic(&self.stream, &self.topic).await? else {
                    return Ok(None);
                };
                self.partitions = Some(details.partitions_count);
                details.partitions_count
            }
        };
        if (self.offsets.len() as u32) < partitions {
            self.offsets.resize(partitions as usize, 0);
        }
        for partition in 0..partitions {
            let from = self.offsets[partition as usize];
            let batch = crate::poll::drain_partition(
                client,
                &self.stream,
                &self.topic,
                &self.consumer,
                partition,
                from,
                REPLY_BATCH,
            )
            .await?;
            self.offsets[partition as usize] = batch.next_offset;
            for message in batch.messages {
                if let Ok(provenance) = Provenance::try_from(&message)
                    && provenance.idempotency_key.as_deref() == Some(correlation)
                {
                    return Ok(Some(AgentMessage {
                        provenance,
                        id: MessageId::new(partition, message.header.offset),
                        payload: message.payload.to_vec(),
                        // The request/reply scan matches on the provenance
                        // idempotency-key header (a `send_agent` reply), so there
                        // is no AGDX envelope to attach here.
                        envelope: None,
                    }));
                }
            }
        }
        Ok(None)
    }

    // Forward scan for the AGDX `response`/`error` carrying `correlation`: drain
    // one batch per partition from the advancing offsets, decode each envelope,
    // and return the first match. `Found` short-circuits, `More` means messages
    // were read without a match (drain again), `CaughtUp` means nothing new.
    // Never re-reads from 0: offsets only move forward across calls.
    async fn next_agdx_match(
        &mut self,
        client: &IggyClient,
        correlation: CorrelationId,
    ) -> Result<ReplyScan, LaserError> {
        let partitions = match self.partitions {
            Some(value) => value,
            None => {
                let Some(details) = client.get_topic(&self.stream, &self.topic).await? else {
                    return Ok(ReplyScan::CaughtUp);
                };
                self.partitions = Some(details.partitions_count);
                details.partitions_count
            }
        };
        if (self.offsets.len() as u32) < partitions {
            self.offsets.resize(partitions as usize, 0);
        }
        let mut read_any = false;
        for partition in 0..partitions {
            let from = self.offsets[partition as usize];
            let batch = crate::poll::drain_partition(
                client,
                &self.stream,
                &self.topic,
                &self.consumer,
                partition,
                from,
                REPLY_BATCH,
            )
            .await?;
            self.offsets[partition as usize] = batch.next_offset;
            for message in batch.messages {
                read_any = true;
                if let Ok(envelope) = decode_named::<AgentEnvelope>(&message.payload)
                    && envelope.correlation == Some(correlation)
                    && matches!(envelope.kind, AgentKind::Response | AgentKind::Error)
                {
                    return Ok(ReplyScan::Found(Box::new(envelope)));
                }
            }
        }
        Ok(if read_any {
            ReplyScan::More
        } else {
            ReplyScan::CaughtUp
        })
    }
}

// The outcome of one forward scan step in [`AgentReplyReader::next_agdx_match`].
// The matched envelope is boxed so the common no-match variants stay small.
enum ReplyScan {
    Found(Box<AgentEnvelope>),
    More,
    CaughtUp,
}
