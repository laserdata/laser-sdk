use crate::agent::consumer::AgentMessage;
use crate::agent::{ChunkAssembler, StreamEvent};
use crate::context::ContextAssembler;
use crate::error::LaserError;
use crate::govern::{ActionCounters, ActionKind, GovernedAction};
use crate::laser::{Laser, ensure_stream, ensure_topic};
use crate::provenance::{AgentTopic, Provenance};
use crate::types::{ConsumerGroupName, ConversationId, MessageId};
use iggy::prelude::*;
use laser_wire::agent::{
    AgentDeadLetter, AgentEnvelope, AgentKind, ChannelId, CorrelationId, LogPosition,
    OPERATION_TASK, TaskState,
};
use laser_wire::framing::decode_named;
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::time::{Instant, sleep};
use tracing::info;

const WELL_KNOWN_TOPICS: [AgentTopic<'static>; 9] = [
    AgentTopic::Commands,
    AgentTopic::Responses,
    AgentTopic::ToolCalls,
    AgentTopic::ToolResults,
    AgentTopic::LlmIo,
    AgentTopic::HumanInput,
    AgentTopic::Audit,
    AgentTopic::WorkflowJournal,
    AgentTopic::Dlq,
];

const REPLY_BATCH: u32 = 1000;

impl Laser {
    /// Create the default data stream and the well-known agent topics (commands, responses, ...), `partitions` each. Idempotent. Requires a default stream (see [`Laser::connect_with_stream`]).
    ///
    /// Warms a producer for every well-known topic concurrently, so that cost is
    /// paid once up front instead of lazily on a handler's first reply.
    #[tracing::instrument(
        target = "laser",
        level = "info",
        skip_all,
        fields(operation = "bootstrap")
    )]
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
    ///
    /// The low-altitude form taking a caller-built [`Provenance`] verbatim.
    /// Application code usually reads better as
    /// [`laser.agent(id).send(..)`](crate::agent::AgentScope::send), which pins
    /// the acting identity for you.
    #[tracing::instrument(target = "laser", level = "debug", skip_all, fields(conversation = %provenance.conversation_id, topic = %topic.topic_string(), operation = "send"))]
    pub async fn send_agent(
        &self,
        topic: AgentTopic<'_>,
        payload: impl Into<Vec<u8>>,
        provenance: &Provenance,
    ) -> Result<(), LaserError> {
        self.send_agent_as(ActionKind::Send, topic, payload, provenance)
            .await
    }

    // `send_agent` with the governed-action kind named by the caller, so the
    // request path and the memory writes report what they are to the governor
    // instead of all reading as plain sends.
    pub(crate) async fn send_agent_as(
        &self,
        kind: ActionKind,
        topic: AgentTopic<'_>,
        payload: impl Into<Vec<u8>>,
        provenance: &Provenance,
    ) -> Result<(), LaserError> {
        let headers: BTreeMap<HeaderKey, HeaderValue> = provenance.try_into()?;
        let key = provenance.partition_key();
        let topic_name = topic.topic_string();
        let mut payload: Vec<u8> = payload.into();
        let action = GovernedAction {
            kind,
            stream: self.stream_required()?,
            topic: &topic_name,
            source: provenance.agent.as_ref().map(|agent| agent.as_str()),
            target: provenance
                .target_agent_id
                .as_ref()
                .map(|agent| agent.as_str()),
            conversation: Some(provenance.conversation_id),
            correlation: provenance.correlation_id.as_deref(),
            operation: None,
            tool: None,
            on_behalf_of: None,
            purpose: None,
            data_classification: None,
            payload: &payload,
            signed: false,
            counters: ActionCounters::default(),
        };
        if let Some(modified) = self.govern(action).await? {
            payload = modified;
        }
        self.send_with_headers(&topic_name, payload, headers, Some(&key))
            .await
    }

    /// Redrive a dead-lettered message: read the original record at the
    /// capsule's `source` position and republish its payload verbatim to that
    /// stream and topic, partitioned by its conversation, so a fixed handler
    /// reprocesses it. The idempotency header is re-keyed by the source
    /// position (the consumer already observed the original key), so the copy
    /// survives dedup while a double redrive of one capsule does not. Errors if
    /// the source stream/topic no longer exists or the record has aged out of
    /// the log's retention window.
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
        let mut headers = original.user_headers_map()?.unwrap_or_default();
        // The consumer's deduplicator observed the original idempotency key when
        // the message first arrived (before it dead-lettered), so a byte-verbatim
        // redrive would be dropped as a duplicate by any consumer still holding
        // that window. The redriven copy is re-keyed by the source position:
        // deterministic, so redriving the same capsule twice still deduplicates,
        // while the payload itself stays verbatim.
        use std::str::FromStr;
        let idempotency = HeaderKey::from_str(crate::provenance::keys::IDEMPOTENCY_KEY)
            .map_err(|_| LaserError::Invalid("idempotency header key".to_owned()))?;
        if let Some(existing) = headers.get(&idempotency) {
            let redrive_key = format!(
                "{}/redrive/{}-{}",
                existing.as_str()?,
                source.partition_id,
                source.offset
            );
            headers.insert(
                idempotency,
                HeaderValue::from_str(&redrive_key)
                    .map_err(|_| LaserError::Invalid("redrive idempotency value".to_owned()))?,
            );
        }
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

    /// A reply reader seeded at `reply_topic`'s current tail, for the synchronous
    /// request/reply bridges. The caller builds it BEFORE sending the request, so
    /// the scan reads only the reply (which lands after the send) instead of
    /// walking the topic's history, then awaits it with
    /// [`await_agdx_reply`](Self::await_agdx_reply).
    pub(crate) async fn agdx_reply_reader(
        &self,
        reply_topic: AgentTopic<'_>,
    ) -> Result<AgentReplyReader, LaserError> {
        #[allow(unused_mut)]
        let mut reader = AgentReplyReader::new_at_tail(
            self.client(),
            self.stream_required()?,
            reply_topic.as_identifier(),
        )
        .await?;
        #[cfg(feature = "sign")]
        {
            reader.verifier = self.registry_verifier();
        }
        Ok(reader)
    }

    /// Wait on a pre-seeded `reader` for the AGDX `response`/`error` carrying
    /// `correlation`, up to `timeout`. The reader is forward-advancing (offsets
    /// only move forward, seeded at the tail before the request was sent), so the
    /// reply topic is read incrementally, never re-scanned from the start. Used by
    /// the synchronous bridge calls (MCP `tools/call`, human-input request).
    pub(crate) async fn await_agdx_reply(
        &self,
        reader: &mut AgentReplyReader,
        correlation: CorrelationId,
        timeout: Duration,
    ) -> Result<AgentEnvelope, LaserError> {
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
    /// `correlation_id` (Ulid, the `agdx.corr` header) stamped on the request. The
    /// responder echoes it back via `AgentCtx::respond`, and this scans the reply
    /// topic for a matching correlation until the timeout. `conversation_id` alone
    /// is NOT used for correlation, so a forged reply that guesses the conversation
    /// id cannot hijack the request. A caller-supplied `provenance.correlation_id`
    /// is used as-is. The business `idempotency_key` is left untouched, so setting
    /// a real dedup key and retrying no longer cross-matches replies.
    ///
    /// The low-altitude form taking a caller-built [`Provenance`] verbatim.
    /// Application code usually reads better as
    /// [`laser.agent(id).ask(..)`](crate::agent::AgentScope::ask), which pins
    /// the acting identity for you.
    #[tracing::instrument(target = "laser", level = "debug", skip_all, fields(conversation = %provenance.conversation_id, topic = %request_topic.topic_string(), operation = "ask"))]
    pub async fn request(
        &self,
        request_topic: AgentTopic<'_>,
        reply_topic: AgentTopic<'_>,
        payload: impl Into<Vec<u8>>,
        provenance: &Provenance,
        timeout: Duration,
    ) -> Result<AgentMessage, LaserError> {
        let correlation = provenance
            .correlation_id
            .clone()
            .unwrap_or_else(|| ulid::Ulid::generate().to_string());
        let mut provenance = provenance.clone();
        provenance.correlation_id = Some(correlation.clone());
        // Register with the shared reply dispatcher BEFORE sending: the reply
        // cannot exist until the request is sent, and the hub reads the topic once
        // for every waiter, so N concurrent requests share one read stream instead
        // of each scanning the topic.
        let hub = self.reply_hub(&reply_topic).await?;
        let ticket = hub.subscribe(correlation);
        self.send_agent_as(ActionKind::Request, request_topic, payload, &provenance)
            .await?;
        ticket.wait(timeout).await
    }

    /// Has `target` committed past the log position `at`? The consumption
    /// acknowledgment: a proof-of-pickup that distinguishes "the target never
    /// consumed the message" from "it consumed but has not finished," by
    /// comparing the target consumer's stored offset against the message's
    /// publish position. Composes the Iggy `get_consumer_offset`, where an
    /// agent's group is named by its agent id.
    ///
    /// A no-stored-offset answer is reported as [`ConsumptionStatus::NotYetConsumed`].
    /// The server returns that same empty answer both for a consumer that has
    /// genuinely never committed and for a topic-read it could not authorize or
    /// resolve, so a caller using this to decide a reassignment should hold
    /// verified topic-read access. Otherwise an auth or resolution misconfig reads
    /// as "not consumed" and could drive a needless reassignment.
    pub async fn consumed(
        &self,
        target: ConsumerRef,
        at: LogPosition,
    ) -> Result<ConsumptionStatus, LaserError> {
        let consumer = match target {
            ConsumerRef::Group(name) => Consumer::group(Identifier::named(name.as_str())?),
            ConsumerRef::Consumer(id) => Consumer::new(Identifier::named(&id)?),
        };
        let info = self
            .client()
            .get_consumer_offset(
                &consumer,
                &Identifier::numeric(at.stream_id)?,
                &Identifier::numeric(at.topic_id)?,
                Some(at.partition_id),
            )
            .await?;
        Ok(match info {
            Some(offset) if offset.stored_offset >= at.offset => ConsumptionStatus::Consumed {
                committed: offset.stored_offset,
                head: offset.current_offset,
            },
            Some(offset) => ConsumptionStatus::NotYetConsumed {
                behind_by: at.offset.saturating_sub(offset.stored_offset),
            },
            None => ConsumptionStatus::NotYetConsumed {
                behind_by: at.offset.saturating_add(1),
            },
        })
    }
}

/// Whether a target consumer has committed past a published message's position.
/// The consumption-acknowledgment result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumptionStatus {
    /// The target's stored offset is still behind the message by `behind_by`.
    NotYetConsumed { behind_by: u64 },
    /// The target has committed past the message: `committed` is its stored
    /// offset, `head` the partition head at the time of the probe.
    Consumed { committed: u64, head: u64 },
}

/// Which consumer to probe in [`Laser::consumed`]: a deployment consumer group
/// or a named individual consumer.
#[derive(Debug, Clone)]
pub enum ConsumerRef {
    Group(ConsumerGroupName),
    Consumer(String),
}

// The agentic reply reader: matches by `agdx.idem` (fresh per-request
// nonce stamped by `Laser::request`), decodes `Provenance`, returns
// `AgentMessage`. The generic core has its own reader matching
// `agdx.corr` and returning `Message`. Caches `partitions_count` on
// first call so a 30s timeout does not do 300 `get_topic` round-trips.
pub(crate) struct AgentReplyReader {
    stream: Identifier,
    topic: Identifier,
    consumer: Consumer,
    offsets: Vec<u64>,
    partitions: Option<u32>,
    /// The enrolled-key verifier a contract checks an AGDX reply against. When
    /// set, a `Working` ack or terminal is honored only if its signature verifies,
    /// so a peer that can read the reply topic cannot forge a terminal to
    /// short-circuit the contract by echoing its correlation. Envelope-less
    /// replies are refused outright under a verifier (they carry nothing to
    /// verify), mirroring the consumer's fail-closed posture.
    #[cfg(feature = "sign")]
    verifier: Option<std::sync::Arc<crate::sign::KeyRegistry>>,
    /// The principal the verified reply signer must equal, normally the resolved
    /// contract target, so a valid signature from a different enrolled key
    /// cannot answer for the routed agent.
    #[cfg(feature = "sign")]
    pub(crate) expected_signer: Option<String>,
}

impl AgentReplyReader {
    fn new(stream: &str, topic: Identifier) -> Result<Self, LaserError> {
        Ok(Self {
            stream: Identifier::named(stream)?,
            topic,
            consumer: Consumer::new(Identifier::named("laser-agent-reply-reader")?),
            offsets: Vec::new(),
            partitions: None,
            #[cfg(feature = "sign")]
            verifier: None,
            #[cfg(feature = "sign")]
            expected_signer: None,
        })
    }

    /// Whether an AGDX reply envelope is honored: always when no verifier is
    /// enrolled, otherwise only when its signature verifies against an enrolled
    /// key AND the verified principal is the expected signer (when one is
    /// bound), so any other enrolled key cannot answer for the routed target.
    #[cfg(feature = "sign")]
    fn verified_principal(&self, envelope: &AgentEnvelope) -> Result<Option<String>, ()> {
        match &self.verifier {
            Some(registry) => match registry.verify(envelope) {
                Ok(principal)
                    if self
                        .expected_signer
                        .as_deref()
                        .is_none_or(|expected| expected == principal) =>
                {
                    Ok(Some(principal.to_owned()))
                }
                Ok(_) | Err(_) => Err(()),
            },
            None => Ok(None),
        }
    }

    #[cfg(not(feature = "sign"))]
    fn verified_principal(&self, _envelope: &AgentEnvelope) -> Result<Option<String>, ()> {
        Ok(None)
    }

    /// Whether an envelope-less correlated reply may complete a contract: only
    /// when no verifier is enrolled. A plain reply carries no signature, so
    /// with verification on it is refused, the same fail-closed posture the
    /// reliable consumer takes before dispatch.
    #[cfg(feature = "sign")]
    fn plain_reply_ok(&self) -> bool {
        self.verifier.is_none()
    }

    #[cfg(not(feature = "sign"))]
    fn plain_reply_ok(&self) -> bool {
        true
    }

    /// A reader seeded at the reply topic's current tail, so the scan reads only
    /// records appended after this point instead of walking the topic's history
    /// from offset zero. The caller MUST construct it before sending the request
    /// it awaits, so the reply (which cannot exist before the request is sent)
    /// always lands at or after the seeded tail and is never missed.
    async fn new_at_tail(
        client: &IggyClient,
        stream: &str,
        topic: Identifier,
    ) -> Result<Self, LaserError> {
        let mut reader = Self::new(stream, topic)?;
        let partitions = match client.get_topic(&reader.stream, &reader.topic).await? {
            Some(details) => details.partitions_count,
            // The topic does not exist yet: leave the reader unseeded (offsets at
            // zero), so once it is created the scan still finds the reply.
            None => return Ok(reader),
        };
        reader.partitions = Some(partitions);
        reader.offsets = vec![0; partitions as usize];
        for partition in 0..partitions {
            let polled = client
                .poll_messages(
                    &reader.stream,
                    &reader.topic,
                    Some(partition),
                    &reader.consumer,
                    &PollingStrategy::last(),
                    1,
                    false,
                )
                .await?;
            // Resume after the last existing record. An empty partition stays at 0.
            if let Some(last) = polled.messages.last() {
                reader.offsets[partition as usize] = last.header.offset + 1;
            }
        }
        Ok(reader)
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

impl AgentReplyReader {
    // One forward pass for the task-contract signals carrying `correlation`: the
    // `Working` ack (an AGDX `status` the target emits on pickup) and the terminal
    // (an AGDX `response`/`error`, or a plain `send_agent` reply matched by the
    // correlation echoed into its idempotency key). Processes the whole drained
    // batch so a `Working` and a terminal in the same batch are both observed,
    // never skipping the terminal. Offsets only move forward across calls.
    pub(crate) async fn poll_contract(
        &mut self,
        client: &IggyClient,
        correlation: CorrelationId,
    ) -> Result<ContractPass, LaserError> {
        let correlation_key = correlation.to_string();
        let partitions = match self.partitions {
            Some(value) => value,
            None => {
                let Some(details) = client.get_topic(&self.stream, &self.topic).await? else {
                    return Ok(ContractPass::default());
                };
                self.partitions = Some(details.partitions_count);
                details.partitions_count
            }
        };
        if (self.offsets.len() as u32) < partitions {
            self.offsets.resize(partitions as usize, 0);
        }
        let mut pass = ContractPass::default();
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
                pass.read_any = true;
                let Ok((provenance, envelope)) = crate::agent::provenance_and_envelope(&message)
                else {
                    continue;
                };
                // Classify the correlated record into a small Copy signal first, so
                // the envelope borrow ends before the record is moved into a reply.
                let signal = match &envelope {
                    // A forged reply that merely echoes the correlation cannot
                    // short-circuit the contract: with a verifier enrolled, an
                    // unsigned or wrongly-signed reply is ignored.
                    Some(agdx) if agdx.correlation == Some(correlation) => {
                        let Ok(verified_principal) = self.verified_principal(agdx) else {
                            continue;
                        };
                        if agdx.kind == AgentKind::Status
                            && agdx.operation.as_deref() == Some(OPERATION_TASK)
                            && agdx.task_state == Some(TaskState::Working)
                        {
                            ContractSignal::Working
                        } else {
                            match agdx.kind {
                                AgentKind::Response => {
                                    ContractSignal::Completed(verified_principal)
                                }
                                AgentKind::Error => ContractSignal::Failed(verified_principal),
                                _ => ContractSignal::Ignore,
                            }
                        }
                    }
                    // A plain `send_agent` reply (no envelope), correlated by the
                    // echoed correlation (no error discriminant, so any such reply
                    // is a completion). Gated to the envelope-less shape so an AGDX
                    // record that merely echoes the correlation on a different one
                    // can never be mis-read as this contract's terminal, and to the
                    // no-verifier case: under a verifier a plain reply carries
                    // nothing to verify and is refused.
                    None if provenance.correlation_id.as_deref()
                        == Some(correlation_key.as_str())
                        && self.plain_reply_ok() =>
                    {
                        ContractSignal::Completed(None)
                    }
                    _ => ContractSignal::Ignore,
                };
                let content_type = crate::agent::consumer::content_type_of(&message)?;
                let reply = |verified_principal| AgentMessage {
                    provenance,
                    id: MessageId::new(partition, message.header.offset),
                    payload: message.payload.to_vec(),
                    envelope,
                    content_type,
                    verified_principal,
                };
                match signal {
                    ContractSignal::Working => pass.consumed = true,
                    ContractSignal::Completed(principal) => {
                        pass.terminal = Some(ContractTerminal::Completed(reply(principal)));
                        return Ok(pass);
                    }
                    ContractSignal::Failed(principal) => {
                        pass.terminal = Some(ContractTerminal::Failed(reply(principal)));
                        return Ok(pass);
                    }
                    ContractSignal::Ignore => {}
                }
            }
        }
        Ok(pass)
    }
}

// One forward pass of the task-contract reply scan: whether the `Working` ack was
// seen, the terminal if it landed, and whether any record was read at all.
#[derive(Default)]
pub(crate) struct ContractPass {
    pub consumed: bool,
    pub terminal: Option<ContractTerminal>,
    pub read_any: bool,
}

// A task-contract terminal reply: the target's completion or its failure.
pub(crate) enum ContractTerminal {
    Completed(AgentMessage),
    Failed(AgentMessage),
}

// One correlated record's contribution to the contract scan, computed while the
// envelope is borrowed so the record can then be moved into a reply.
#[derive(Clone)]
enum ContractSignal {
    Working,
    Completed(Option<String>),
    Failed(Option<String>),
    Ignore,
}
