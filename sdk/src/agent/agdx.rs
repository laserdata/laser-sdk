use crate::agent::clock::{Clock, SystemClock};
use crate::error::LaserError;
use crate::govern::{ActionCounters, ActionKind, GovernedAction};
use crate::laser::Laser;
use crate::provenance::AgentTopic;
use crate::types::MintUlid;
use iggy::prelude::{HeaderKey, HeaderValue};
use laser_wire::agent::{
    AgentEnvelope, AgentErrorBody, AgentId, AgentKind, ChannelId, ConversationId, CorrelationId,
    IdempotencyKey, LogPosition, OPERATION_TASK, RecordId, TaskState, TokenUsage, validate,
};
#[cfg(feature = "sign")]
use laser_wire::codes::AGENT_OP_VERSION;
use laser_wire::content::ContentType;
use laser_wire::dispatch::OPERATION_INPUT_REQUEST;
use laser_wire::framing::{decode_named, encode_named};
use laser_wire::query::Value;
use std::collections::BTreeMap;
use std::time::Duration;

// Producer-side chunking guidance, DRAFT until the benchmark step pins them:
// flush a chunk at the byte target or the linger bound, whichever first, and
// never exceed the hard chunk cap.
/// Draft chunk flush target, in body bytes.
pub const DEFAULT_CHUNK_FLUSH_BYTES: usize = 512;
/// Draft chunk linger bound, in milliseconds.
pub const DEFAULT_CHUNK_LINGER_MS: u64 = 20;
/// Draft hard cap on one chunk's body.
pub const MAX_CHUNK_BODY_BYTES: usize = 64 * 1024;
/// How long after its opening a chunk stream may run before readers abandon
/// it, unless the writer sets its own bound.
pub const DEFAULT_CHUNK_ABANDON_MS: u64 = 600_000;

/// The typed AGDX producer over one agent topic: every send is a validated
/// [`AgentEnvelope`] (invalid envelopes are unrepresentable or rejected at
/// publish time), encoded as named-field CBOR, stamped with the routing
/// headers (`agdx.av` u32, `agdx.ct` u8, the conversation as a Crockford string,
/// and the target as the agent's name string), and partition-keyed by the
/// conversation's canonical base32 form so one conversation stays ordered.
#[derive(Clone)]
pub struct Agdx {
    laser: Laser,
    topic: String,
    source: AgentId,
    conversation: ConversationId,
    lane_guard: Option<std::sync::Arc<crate::agent::session::LaneGuard>>,
    stream_generation: Option<u64>,
}

impl Laser {
    /// A typed AGDX producer publishing as `source` within `conversation` on
    /// `topic`. The topic is resolved to its name at construction, so a runtime
    /// `AgentTopic::Custom` built from a borrowed identifier is accepted.
    pub fn agdx(
        &self,
        topic: AgentTopic<'_>,
        source: impl Into<AgentId>,
        conversation: ConversationId,
    ) -> Agdx {
        let source = source.into();
        Agdx {
            laser: self.clone(),
            topic: topic.topic_string(),
            source,
            conversation,
            lane_guard: None,
            stream_generation: None,
        }
    }
}

impl Agdx {
    pub(crate) fn with_lane_guard(
        mut self,
        guard: std::sync::Arc<crate::agent::session::LaneGuard>,
    ) -> Self {
        self.lane_guard = Some(guard);
        self
    }

    pub(crate) fn with_stream_generation(mut self, generation: u64) -> Self {
        self.stream_generation = Some(generation);
        self
    }

    /// A `command`: expects a reply or effect under `correlation`.
    pub fn command(&self, correlation: CorrelationId, body: Vec<u8>) -> AgdxSend<'static> {
        self.send_of(AgentEnvelope::command(
            RecordId::mint(),
            self.conversation,
            self.source.clone(),
            correlation,
            body,
        ))
    }

    /// A `response`: the paired answer to a command (same `correlation`).
    pub fn respond(&self, correlation: CorrelationId, body: Vec<u8>) -> AgdxSend<'static> {
        self.send_of(AgentEnvelope::response(
            RecordId::mint(),
            self.conversation,
            self.source.clone(),
            correlation,
            body,
        ))
    }

    /// An `event`: expects nothing.
    pub fn emit(&self, body: Vec<u8>) -> AgdxSend<'static> {
        self.send_of(AgentEnvelope::event(
            RecordId::mint(),
            self.conversation,
            self.source.clone(),
            body,
        ))
    }

    /// A `status` signal discriminated by `operation` (`task` | `card` |
    /// `progress`). Task updates additionally chain
    /// [`with_correlation`](AgdxSend::with_correlation) and
    /// [`with_task_state`](AgdxSend::with_task_state).
    pub fn status(&self, operation: impl Into<String>) -> AgdxSend<'static> {
        self.send_of(AgentEnvelope::status(
            RecordId::mint(),
            self.conversation,
            self.source.clone(),
            operation,
        ))
    }

    /// An `error` terminal for `correlation`. The body is the encoded
    /// [`AgentErrorBody`] (so `agdx.ct` is forced to cbor).
    pub fn fail(
        &self,
        correlation: CorrelationId,
        error: &AgentErrorBody,
    ) -> Result<AgdxSend<'static>, LaserError> {
        let body = encode_named(error)?;
        Ok(self
            .send_of(AgentEnvelope::error(
                RecordId::mint(),
                self.conversation,
                self.source.clone(),
                correlation,
                body,
            ))
            .content_type(ContentType::Cbor))
    }

    /// A chunk-stream writer under `correlation` on a fresh channel. The
    /// `purpose` is the pinned chunk-stream vocabulary (`chat` | `reasoning`
    /// | `tool_args`), declared on the opening chunk.
    pub fn stream(&self, correlation: CorrelationId, purpose: impl Into<String>) -> AgdxStream {
        AgdxStream {
            agdx: self.clone(),
            correlation,
            channel: ChannelId::mint(),
            purpose: purpose.into(),
            sequence: 0,
            deadline_micros: Some(
                SystemClock
                    .now_micros()
                    .saturating_add(DEFAULT_CHUNK_ABANDON_MS.saturating_mul(1_000)),
            ),
            target: None,
            content_type: ContentType::Raw,
            buffer: None,
        }
    }

    /// Human-in-the-loop interrupt/resume. Pauses on a human: publishes an
    /// `input-required` task status and the prompt as an `input_request`
    /// command under a fresh interrupt correlation on this producer's topic,
    /// then awaits the correlated `response` on `reply_topic` up to `timeout`
    /// and returns its body. Only an agent that lists `input_request` among its
    /// operations receives the prompt as work. A responder answers with
    /// [`AgentCtx::respond_input`](crate::agent::AgentCtx::respond_input) (a
    /// `response`) or rejects with an `error`, which surfaces here as
    /// [`LaserError::Rejected`]. It blocks the caller until the response lands
    /// or the timeout elapses. The prompt is addressed to every agent
    /// (`agdx.to = *`). Use [`request_input_from`](Self::request_input_from)
    /// to ask one approver and accept only its answer.
    pub async fn request_input(
        &self,
        reply_topic: AgentTopic<'_>,
        prompt: impl Into<Vec<u8>>,
        timeout: Duration,
    ) -> Result<Vec<u8>, LaserError> {
        Ok(self
            .input_reply(None, reply_topic, prompt.into(), timeout)
            .await?
            .body)
    }

    /// [`request_input`](Self::request_input) with the prompt addressed to
    /// `target`. Only a reply authored by `target`, and signed by it when a
    /// verifier is configured, answers the request.
    pub async fn request_input_from(
        &self,
        target: impl Into<AgentId>,
        reply_topic: AgentTopic<'_>,
        prompt: impl Into<Vec<u8>>,
        timeout: Duration,
    ) -> Result<Vec<u8>, LaserError> {
        Ok(self
            .input_reply(Some(target.into()), reply_topic, prompt.into(), timeout)
            .await?
            .body)
    }

    // Publish one input request and await its approving reply, from `target`
    // alone when the request names one.
    pub(crate) async fn input_reply(
        &self,
        target: Option<AgentId>,
        reply_topic: AgentTopic<'_>,
        prompt: Vec<u8>,
        timeout: Duration,
    ) -> Result<AgentEnvelope, LaserError> {
        let interrupt = CorrelationId::mint();
        // Seed the reply reader at the topic tail before sending the prompt, so it
        // reads only the human's response rather than the topic's history.
        let mut reader = self
            .laser
            .agdx_reply_reader(reply_topic, Some(&self.source))
            .await?;
        #[cfg(feature = "sign")]
        {
            reader.expected_signer = target.as_ref().map(ToString::to_string);
        }
        reader.expected_source = target.clone();
        self.status(OPERATION_TASK)
            .with_correlation(interrupt)
            .with_task_state(TaskState::InputRequired)
            .send()
            .await?;
        let command = self
            .command(interrupt, prompt)
            .with_operation(OPERATION_INPUT_REQUEST);
        match target {
            Some(target) => command.with_target(target).send().await?,
            None => command.send().await?,
        };
        let reply = self
            .laser
            .await_agdx_reply(&mut reader, interrupt, timeout)
            .await?;
        if reply.kind == AgentKind::Error {
            let message = decode_named::<AgentErrorBody>(&reply.body)
                .ok()
                .and_then(|body| body.message)
                .unwrap_or_else(|| "the input request was rejected".to_owned());
            return Err(LaserError::Rejected(message));
        }
        Ok(reply)
    }

    // Publish an envelope built elsewhere on this producer's topic, after the
    // governor. A replacement body would void a signature the caller already
    // attached, so a signed envelope the governor rewrites is refused.
    pub(crate) async fn publish_envelope(
        &self,
        mut envelope: AgentEnvelope,
    ) -> Result<AgdxReceipt, LaserError> {
        let signed = envelope.signature.is_some();
        let correlation = envelope.correlation.map(|value| value.to_string());
        let action = GovernedAction {
            kind: action_kind(envelope.kind),
            stream: self.laser.stream_required()?,
            topic: &self.topic,
            source: Some(envelope.source.as_str()),
            target: envelope.target.as_ref().map(|target| target.as_str()),
            conversation: Some(envelope.conversation.into()),
            correlation: correlation.as_deref(),
            operation: envelope.operation.as_deref(),
            tool: envelope.tool.as_deref(),
            on_behalf_of: metadata_str(&envelope, laser_wire::agent::METADATA_DELEGATED_BY),
            purpose: metadata_str(&envelope, laser_wire::agent::METADATA_PURPOSE),
            data_classification: metadata_str(
                &envelope,
                laser_wire::agent::METADATA_DATA_CLASSIFICATION,
            ),
            payload: &envelope.body,
            signed,
            approval: None,
            counters: ActionCounters::default(),
        };
        if let Some(modified) = self.laser.govern(action).await?
            && modified != envelope.body
        {
            if signed {
                return Err(LaserError::PolicyBlocked(
                    "the governor replaced the body of a signed envelope".to_owned(),
                ));
            }
            envelope.body = modified;
        }
        self.publish(envelope, ContentType::Raw).await
    }

    fn send_of(&self, envelope: AgentEnvelope) -> AgdxSend<'static> {
        AgdxSend {
            agdx: self.clone(),
            envelope,
            content_type: ContentType::Raw,
            #[cfg(feature = "sign")]
            sign_key: None,
            claim_check: None,
            approval: None,
        }
    }

    #[tracing::instrument(target = "laser", level = "debug", skip_all, fields(conversation = %self.conversation, correlation = envelope.correlation.as_ref().map(tracing::field::display), topic = %self.topic, operation = "agdx"))]
    async fn publish(
        &self,
        envelope: AgentEnvelope,
        content_type: ContentType,
    ) -> Result<AgdxReceipt, LaserError> {
        let record = envelope.record;
        // A declared per-agent topic layout moves addressed work off the lane.
        // The lane identity check covers lane records only.
        let destination = self.laser.agent_destination(
            &self.topic,
            Some(envelope.kind),
            envelope.target.as_ref(),
        );
        let (message, partitioning) = self.assemble(envelope, content_type)?;
        let sent = self
            .laser
            .send_batch_partitioned_on_checked(
                self.laser.stream_required()?,
                destination.as_deref().unwrap_or(&self.topic),
                vec![message],
                Ok(partitioning.clone()),
                || async {
                    if destination.is_some() {
                        return Ok(None);
                    }
                    let (stream_id, topic_id, partitions) = if let Some(guard) = &self.lane_guard {
                        let identity = guard.check(&self.laser, self.conversation.into()).await?;
                        (identity.stream_id, identity.topic_id, identity.partitions)
                    } else if let Some(generation) = self.stream_generation {
                        use iggy::prelude::StreamClient;
                        let details = self
                            .laser
                            .client()
                            .get_stream(&iggy::prelude::Identifier::named(
                                self.laser.stream_required()?,
                            )?)
                            .await?
                            .ok_or_else(|| {
                                LaserError::Session(laser_wire::session::SessionError::Stale(
                                    "the heartbeat stream does not exist".to_owned(),
                                ))
                            })?;
                        if details.created_at.as_micros() != generation {
                            return Err(LaserError::Session(
                                laser_wire::session::SessionError::Stale(
                                    "the heartbeat stream generation changed".to_owned(),
                                ),
                            ));
                        }
                        let topic = details
                            .topics
                            .iter()
                            .find(|topic| topic.name == self.topic)
                            .filter(|topic| topic.partitions_count > 0)
                            .ok_or_else(|| {
                                LaserError::Session(laser_wire::session::SessionError::Stale(
                                    "the heartbeat topic does not exist".to_owned(),
                                ))
                            })?;
                        (details.id, topic.id, topic.partitions_count)
                    } else {
                        return Ok(None);
                    };
                    let route = if partitioning.kind == iggy_common::PartitioningKind::MessagesKey {
                        iggy::prelude::Partitioning::partition_id(
                            iggy_common::calculate_32(&partitioning.value) % partitions,
                        )
                    } else {
                        partitioning.clone()
                    };
                    Ok(Some((stream_id, topic_id, route)))
                },
            )
            .await?;
        let confirmation = sent.confirmations.first();
        Ok(AgdxReceipt {
            record,
            partition_id: confirmation.map(|confirmation| confirmation.partition_id),
            offset: confirmation.map(|confirmation| confirmation.base_offset),
        })
    }

    // Validate and encode one envelope into a ready message plus its
    // partitioning: the single assembly the per-send publish and the
    // buffered stream's batch append share, so buffering changes when bytes
    // move, never what they are.
    fn assemble(
        &self,
        envelope: AgentEnvelope,
        content_type: ContentType,
    ) -> Result<(iggy::prelude::IggyMessage, iggy::prelude::Partitioning), LaserError> {
        validate(&envelope)?;
        let payload = encode_named(&envelope)?;
        // The session topics carry an addressee on every record, `*` when
        // untargeted, so the addressee filter of a role group keeps them.
        let headers = agdx_headers(
            &envelope,
            content_type,
            self.topic == laser_wire::topics::AGENT_SESSIONS
                || self.topic == laser_wire::topics::AGENT_CONTROL,
        )?;
        let partitioning = self.partitioning(envelope.kind, envelope.target.as_ref())?;
        let message = iggy::prelude::IggyMessage::builder()
            .payload(bytes::Bytes::from(payload))
            .user_headers(headers)
            .build()?;
        Ok((message, partitioning))
    }

    // The partition a record of `kind` addressed to `target` lands on under
    // this stream's declared layout.
    fn partitioning(
        &self,
        kind: laser_wire::agent::AgentKind,
        target: Option<&AgentId>,
    ) -> Result<iggy::prelude::Partitioning, LaserError> {
        let layout = self
            .laser
            .default_stream()
            .and_then(|stream| self.laser.layout(stream));
        crate::agent::partitioning::AgentPartitioning::resolve(
            layout.as_ref(),
            &self.topic,
            kind,
            target,
        )
        .into_partitioning(self.conversation)
    }
}

/// Where one published envelope was committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgdxReceipt {
    /// The envelope's record id. Chunks have none.
    pub record: Option<RecordId>,
    /// The partition the record landed on, when the server confirmed it.
    pub partition_id: Option<u32>,
    /// The record's offset, when the server confirmed it.
    pub offset: Option<u64>,
}

/// One pending AGDX send: the envelope built by its verb, refined by the
/// `with_*` setters, validated and published by [`send`](Self::send). The
/// lifetime is the borrowed signing key, claim-check store and approval grant,
/// `'static` without them.
#[must_use = "an unsent envelope does nothing until you call .send().await"]
pub struct AgdxSend<'a> {
    agdx: Agdx,
    envelope: AgentEnvelope,
    content_type: ContentType,
    /// When set, the envelope is signed with this key just before encoding (the
    /// last step, so all the `with_*` refinements are covered). A verifying
    /// consumer rejects an unsigned or unverified record on a control or effect
    /// topic (the mandatory-verification gate), which is what makes a cancel or
    /// quarantine authorizable.
    #[cfg(feature = "sign")]
    sign_key: Option<&'a crate::sign::SigningKey>,
    /// When set, a body at or over the threshold is externalized to the store
    /// and replaced by the `BodyRef` capsule at send (content-type `ref`).
    claim_check: Option<(&'a dyn crate::blob::BlobStore, usize)>,
    approval: Option<&'a crate::agent::ApprovalGrant>,
}

impl<'a> AgdxSend<'a> {
    /// Use `record` as the envelope's record id, so a retried send repeats the
    /// same record.
    pub(crate) fn with_record(mut self, record: RecordId) -> Self {
        self.envelope.record = Some(record);
        self
    }

    /// Mark the envelope as part of a child session whose parent is `parent`
    /// and whose tree is rooted at `root`.
    pub fn with_ancestry(
        mut self,
        parent: Option<laser_wire::agent::ConversationId>,
        root: Option<laser_wire::agent::ConversationId>,
    ) -> Self {
        self.envelope.parent = parent;
        self.envelope.root = root;
        self
    }

    /// Carry `grant` to the action governor, so a step-up it asks for in the
    /// grant's conversation proceeds on that approval.
    pub fn with_grant(mut self, grant: &'a crate::agent::ApprovalGrant) -> Self {
        self.approval = Some(grant);
        self
    }

    /// Narrow delivery to one agent within the shared topic (routing, never an ACL).
    pub fn with_target(mut self, target: impl Into<AgentId>) -> Self {
        self.envelope = self.envelope.with_target(target.into());
        self
    }

    /// Stamp the causal parent (identity, plus the locator when known).
    pub fn with_cause(mut self, cause: RecordId, cause_at: Option<LogPosition>) -> Self {
        self.envelope = self.envelope.with_cause(cause, cause_at);
        self
    }

    /// Pair with a correlation id (required by status task updates).
    pub fn with_correlation(mut self, correlation: CorrelationId) -> Self {
        self.envelope = self.envelope.with_correlation(correlation);
        self
    }

    /// Attach a business idempotency key.
    pub fn with_idempotency_key(mut self, key: IdempotencyKey) -> Self {
        self.envelope = self.envelope.with_idempotency_key(key);
        self
    }

    /// Declare the drop-dead time, epoch micros.
    pub fn with_deadline_at_micros(mut self, deadline_at_micros: u64) -> Self {
        self.envelope = self.envelope.with_deadline_micros(deadline_at_micros);
        self
    }

    /// Attach a task state (status task updates require it).
    pub fn with_task_state(mut self, state: TaskState) -> Self {
        self.envelope = self.envelope.with_task_state(state);
        self
    }

    /// Set the OTel operation name (open vocabulary kinds only).
    pub fn with_operation(mut self, operation: impl Into<String>) -> Self {
        self.envelope = self.envelope.with_operation(operation);
        self
    }

    /// Set why the model stopped generating.
    pub fn with_finish_reason(mut self, reason: impl Into<String>) -> Self {
        self.envelope.finish_reason = Some(reason.into());
        self
    }

    /// Set the OTel tool name.
    pub fn with_tool(mut self, tool: impl Into<String>) -> Self {
        self.envelope = self.envelope.with_tool(tool);
        self
    }

    /// Attach token accounting (advisory).
    pub fn with_usage(mut self, usage: TokenUsage) -> Self {
        self.envelope = self.envelope.with_usage(usage);
        self
    }

    /// Add one AGDX-native metadata entry.
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.envelope = self.envelope.with_metadata(key, value);
        self
    }

    /// Mark a status terminal (`last = true` on the final task update).
    pub fn last(mut self) -> Self {
        self.envelope.last = true;
        self
    }

    /// Declare the body's codec (`agdx.ct`). Defaults to raw. An `error`
    /// envelope always carries a CBOR [`AgentErrorBody`], so any other content
    /// type on [`Agdx::fail`] fails at send.
    pub fn content_type(mut self, content_type: ContentType) -> Self {
        self.content_type = content_type;
        self
    }

    /// Attach the body for the kinds whose verb does not take one (`status`,
    /// notably the `card` body of a registry advertisement). The body-carrying
    /// verbs (`command`/`respond`/`emit`/`fail`) set it directly, so this is for
    /// refining a `status`.
    pub fn body(mut self, body: impl Into<Vec<u8>>) -> Self {
        self.envelope.body = body.into();
        self
    }

    /// Sign the envelope with `key` just before sending (after every `with_*`
    /// refinement, so the signature covers the final shape). The consuming SDK
    /// verifies it against the enrolled key registry, the authorship and
    /// authorization gate. Requires the `sign` feature.
    #[cfg(feature = "sign")]
    pub fn signed_by(mut self, key: &'a crate::sign::SigningKey) -> Self {
        self.sign_key = Some(key);
        self
    }

    /// Claim-check the body against `store` when it is at or over
    /// `threshold_bytes` at send time: the body is externalized and replaced
    /// by the [`BodyRef`](laser_wire::agent::BodyRef) capsule, content-type
    /// `ref`. Applied before signing, so a signature covers the capsule the
    /// log actually carries. Readers resolve with
    /// [`AgentMessage::resolve_body`](crate::agent::AgentMessage::resolve_body).
    pub fn claim_check(
        mut self,
        store: &'a dyn crate::blob::BlobStore,
        threshold_bytes: usize,
    ) -> Self {
        self.claim_check = Some((store, threshold_bytes));
        self
    }

    /// Validate, encode, stamp the headers, and publish. Returns the minted
    /// record id (chunks have none). When [`signed_by`](Self::signed_by) set a
    /// key, the envelope is signed here, last, so the signature covers every
    /// refinement.
    pub async fn send(self) -> Result<Option<RecordId>, LaserError> {
        Ok(self.send_receipt().await?.record)
    }

    /// Like [`send`](Self::send), and also return where the record was
    /// committed.
    pub async fn send_receipt(self) -> Result<AgdxReceipt, LaserError> {
        let mut envelope = self.envelope;
        let mut content_type = self.content_type;
        if envelope.kind == AgentKind::Error && content_type != ContentType::Cbor {
            return Err(LaserError::Invalid(
                "an error envelope carries a CBOR AgentErrorBody, so its content type cannot change"
                    .to_owned(),
            ));
        }
        // The pre-effect policy hook, before claim-check and signing so a
        // modified body is what gets externalized and what the signature covers.
        #[cfg(feature = "sign")]
        let will_sign = self.sign_key.is_some();
        #[cfg(not(feature = "sign"))]
        let will_sign = false;
        let correlation = envelope.correlation.map(|value| value.to_string());
        let action = GovernedAction {
            kind: action_kind(envelope.kind),
            stream: self.agdx.laser.stream_required()?,
            topic: &self.agdx.topic,
            source: Some(envelope.source.as_str()),
            target: envelope.target.as_ref().map(|target| target.as_str()),
            conversation: Some(envelope.conversation.into()),
            correlation: correlation.as_deref(),
            operation: envelope.operation.as_deref(),
            tool: envelope.tool.as_deref(),
            on_behalf_of: metadata_str(&envelope, laser_wire::agent::METADATA_DELEGATED_BY),
            purpose: metadata_str(&envelope, laser_wire::agent::METADATA_PURPOSE),
            data_classification: metadata_str(
                &envelope,
                laser_wire::agent::METADATA_DATA_CLASSIFICATION,
            ),
            payload: &envelope.body,
            signed: will_sign,
            counters: ActionCounters::default(),
            approval: self.approval,
        };
        if let Some(modified) = self.agdx.laser.govern(action).await? {
            envelope.body = modified;
        }
        if let Some((store, threshold)) = self.claim_check {
            let body = std::mem::take(&mut envelope.body);
            let (body, replaced) = crate::blob::check_in(store, threshold, body).await?;
            envelope.body = body;
            if let Some(replaced) = replaced {
                content_type = replaced;
            }
        }
        #[cfg(feature = "sign")]
        let envelope = if let Some(key) = self.sign_key {
            // Bind the interpretation attributes the log will stamp out of band
            // (`agdx.ct` = the content-type code, `agdx.av` = the wire version)
            // into the signature, so neither can be flipped on the signed record.
            let context = laser_wire::agent::SignatureContext {
                content_type: Some(content_type.code()),
                agent_version: Some(AGENT_OP_VERSION),
            };
            let signature = key.sign_with_context(&envelope, context)?;
            envelope.with_signature(signature)
        } else {
            envelope
        };
        self.agdx.publish(envelope, content_type).await
    }
}

/// A chunk-stream writer: auto-incremented `sequence`, the purpose and the
/// abandonment bound on the opening chunk, one terminal (`finish` or `fail`).
/// Dropping the writer without a terminal is the producer-death case readers
/// abandon by deadline.
pub struct AgdxStream {
    agdx: Agdx,
    correlation: CorrelationId,
    channel: ChannelId,
    purpose: String,
    sequence: u64,
    deadline_micros: Option<u64>,
    target: Option<AgentId>,
    content_type: ContentType,
    buffer: Option<ChunkBuffer>,
}

// The opt-in write buffer: chunk envelopes held until `max_chunks` or
// `linger` trips, then appended as ONE batch. Sequences are assigned at
// write, so reassembly semantics are untouched.
struct ChunkBuffer {
    max_chunks: usize,
    linger: std::time::Duration,
    first_at: Option<std::time::Instant>,
    // Envelopes, assembled at flush, so a failed append keeps them to resend.
    envelopes: Vec<(AgentEnvelope, ContentType)>,
}

impl AgdxStream {
    /// The stream's channel id.
    pub fn channel(&self) -> ChannelId {
        self.channel
    }

    /// Declare the reader-local abandonment bound, epoch micros (rides the
    /// opening chunk, so set it before the first write). Defaults to
    /// [`DEFAULT_CHUNK_ABANDON_MS`] after the writer opened.
    pub fn with_deadline_at_micros(mut self, deadline_at_micros: u64) -> Self {
        self.deadline_micros = deadline_at_micros.into();
        self
    }

    /// Narrow delivery to one agent within the shared topic.
    pub fn with_target(mut self, target: AgentId) -> Self {
        self.target = Some(target);
        self
    }

    /// Declare the chunk bodies' codec (`agdx.ct`). Defaults to raw.
    pub fn content_type(mut self, content_type: ContentType) -> Self {
        self.content_type = content_type;
        self
    }

    /// Buffer writes and append them as ONE batch when `max_chunks` fill or
    /// `linger` has passed since the first buffered chunk, whichever first.
    /// Opt-in for chunk-heavy token streams. The unbuffered stream publishes
    /// each write exactly as before. The linger is checked at write (no
    /// background task drives this writer), so a stalled producer holds its
    /// buffered chunks until the next `write`, an explicit
    /// [`flush`](Self::flush), or the terminal, which always flushes.
    #[must_use]
    pub fn buffered(mut self, max_chunks: usize, linger: std::time::Duration) -> Self {
        self.buffer = Some(ChunkBuffer {
            max_chunks: max_chunks.max(1),
            linger,
            first_at: None,
            envelopes: Vec::new(),
        });
        self
    }

    /// Publish the next chunk. The opening chunk (`sequence` 0) carries the
    /// stream purpose and the abandonment bound. A failed publish keeps the
    /// sequence, so writing the body again continues the stream without a
    /// gap. A buffered write whose flush fails keeps its chunks buffered for
    /// the next [`flush`](Self::flush).
    pub async fn write(&mut self, body: Vec<u8>) -> Result<(), LaserError> {
        ensure_chunk_body_within_cap(&body)?;
        let envelope = self.chunk(body, false, None, None);
        match &mut self.buffer {
            None => {
                self.agdx.publish(envelope, self.content_type).await?;
                self.sequence += 1;
            }
            Some(buffer) => {
                validate(&envelope)?;
                self.sequence += 1;
                buffer.first_at.get_or_insert_with(std::time::Instant::now);
                buffer.envelopes.push((envelope, self.content_type));
                let linger_tripped = buffer
                    .first_at
                    .is_some_and(|first| first.elapsed() >= buffer.linger);
                if buffer.envelopes.len() >= buffer.max_chunks || linger_tripped {
                    self.flush().await?;
                }
            }
        }
        Ok(())
    }

    /// Append everything buffered as one batch. A no-op unbuffered or empty.
    /// A failed append keeps the chunks buffered, so the next flush sends them.
    pub async fn flush(&mut self) -> Result<(), LaserError> {
        let partitioning = self
            .agdx
            .partitioning(laser_wire::agent::AgentKind::Chunk, self.target.as_ref())?;
        let destination = self.destination();
        let Some(buffer) = &mut self.buffer else {
            return Ok(());
        };
        if buffer.envelopes.is_empty() {
            return Ok(());
        }
        let batch = buffer
            .envelopes
            .iter()
            .map(|(envelope, content_type)| {
                self.agdx
                    .assemble(envelope.clone(), *content_type)
                    .map(|(message, _)| message)
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.agdx
            .laser
            .send_batch_partitioned_on(
                self.agdx.laser.stream_required()?,
                &destination,
                batch,
                partitioning,
            )
            .await?;
        buffer.envelopes.clear();
        buffer.first_at = None;
        Ok(())
    }

    /// Publish the terminal chunk (`last = true`) with the reason the stream
    /// ended and the whole-stream accounting. A buffered stream appends its
    /// held chunks and the terminal together, one batch.
    pub async fn finish(
        mut self,
        finish_reason: impl Into<String>,
        usage: Option<TokenUsage>,
    ) -> Result<(), LaserError> {
        let envelope = self.chunk(Vec::new(), true, Some(finish_reason.into()), usage);
        self.send_terminal(envelope, self.content_type).await
    }

    /// Terminate the stream with a `kind = error` terminal carrying this
    /// channel and the next sequence.
    pub async fn fail(mut self, error: &AgentErrorBody) -> Result<(), LaserError> {
        let body = encode_named(error)?;
        let mut envelope = AgentEnvelope::error(
            RecordId::mint(),
            self.agdx.conversation,
            self.agdx.source.clone(),
            self.correlation,
            body,
        );
        envelope.channel = Some(self.channel);
        envelope.sequence = Some(self.sequence);
        if let Some(target) = self.target.clone() {
            envelope = envelope.with_target(target);
        }
        self.send_terminal(envelope, ContentType::Cbor).await
    }

    // The one terminal path: a buffered stream appends its held chunks and
    // the terminal as ONE batch (ordered, sequence-stamped at write), an
    // unbuffered one publishes the terminal alone.
    async fn send_terminal(
        &mut self,
        envelope: AgentEnvelope,
        content_type: ContentType,
    ) -> Result<(), LaserError> {
        match &mut self.buffer {
            None => {
                self.agdx.publish(envelope, content_type).await?;
            }
            Some(buffer) => {
                let (message, partitioning) = self.agdx.assemble(envelope, content_type)?;
                let mut batch = std::mem::take(&mut buffer.envelopes)
                    .into_iter()
                    .map(|(envelope, content_type)| {
                        self.agdx
                            .assemble(envelope, content_type)
                            .map(|(message, _)| message)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                batch.push(message);
                buffer.first_at = None;
                let destination = self.destination();
                self.agdx
                    .laser
                    .send_batch_partitioned_on(
                        self.agdx.laser.stream_required()?,
                        &destination,
                        batch,
                        partitioning,
                    )
                    .await?;
            }
        }
        Ok(())
    }

    // Where this stream's buffered chunks land: the addressee's declared
    // topic under a per-agent topic layout, else the producer's topic.
    fn destination(&self) -> String {
        self.agdx
            .laser
            .agent_destination(
                &self.agdx.topic,
                Some(laser_wire::agent::AgentKind::Chunk),
                self.target.as_ref(),
            )
            .unwrap_or_else(|| self.agdx.topic.clone())
    }

    fn chunk(
        &self,
        body: Vec<u8>,
        last: bool,
        finish_reason: Option<String>,
        usage: Option<TokenUsage>,
    ) -> AgentEnvelope {
        let mut envelope = AgentEnvelope::chunk(
            self.agdx.conversation,
            self.agdx.source.clone(),
            self.correlation,
            self.channel,
            self.sequence,
            body,
        );
        if self.sequence == 0 {
            envelope = envelope.with_operation(self.purpose.clone());
            if let Some(deadline) = self.deadline_micros {
                envelope = envelope.with_deadline_micros(deadline);
            }
        }
        if let Some(target) = self.target.clone() {
            envelope = envelope.with_target(target);
        }
        if last {
            envelope.last = true;
            envelope.finish_reason = finish_reason;
            if let Some(usage) = usage {
                envelope = envelope.with_usage(usage);
            }
        }
        envelope
    }
}

// The governed-action kind of an envelope kind. Chunk streams bypass
// `AgdxSend` (their writer publishes directly), so the wildcard is unreachable
// today and maps any future kind to a plain send rather than skipping the hook.
fn action_kind(kind: AgentKind) -> ActionKind {
    match kind {
        AgentKind::Command => ActionKind::Command,
        AgentKind::Response => ActionKind::Response,
        AgentKind::Event => ActionKind::Event,
        AgentKind::Status => ActionKind::Status,
        AgentKind::Error => ActionKind::Error,
        _ => ActionKind::Send,
    }
}

// A string metadata entry of `envelope`, for the advisory policy inputs.
fn metadata_str<'e>(envelope: &'e AgentEnvelope, key: &str) -> Option<&'e str> {
    match envelope.metadata.as_ref()?.get(key)? {
        Value::Str(value) => Some(value.as_str()),
        _ => None,
    }
}

fn ensure_chunk_body_within_cap(body: &[u8]) -> Result<(), LaserError> {
    if body.len() > MAX_CHUNK_BODY_BYTES {
        return Err(LaserError::Invalid(format!(
            "chunk body is {}B, exceeds cap {}B",
            body.len(),
            MAX_CHUNK_BODY_BYTES
        )));
    }
    Ok(())
}

// The AGDX routing headers from the shared wire encoder: `agdx.av` selects the
// decoder before any body byte is read, `agdx.ct` names the inner body codec,
// the conversation and its ancestry ride as canonical strings, and the author
// and addressee ride as agent names, so projections and plain consumers route
// without decoding the CBOR body.
pub(crate) fn agdx_headers(
    envelope: &AgentEnvelope,
    content_type: ContentType,
    broadcast: bool,
) -> Result<BTreeMap<HeaderKey, HeaderValue>, LaserError> {
    let headers =
        laser_wire::headers::RecordHeaders::for_envelope(envelope, content_type, broadcast);
    Ok(crate::provenance::header_map(&headers)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use laser_wire::agent::OPERATION_CHAT;
    use laser_wire::fixtures::assert_matches;
    use laser_wire::headers::{
        AGENT_ID, AGENT_VERSION, CONTENT_TYPE, CONVERSATION_ID, PARENT_CONVERSATION_ID,
        ROOT_CONVERSATION_ID, TARGET_AGENT_ID,
    };
    use serde::Serialize;
    use serde::Serializer;
    use std::env;
    use std::fs;
    use std::path::PathBuf;
    use std::str::FromStr;

    #[tokio::test]
    async fn given_an_owned_agdx_send_when_spawned_then_should_send_on_its_own_task() {
        let laser = Laser::from_client(iggy::prelude::IggyClient::default());
        let agdx = laser.agdx(
            AgentTopic::Sessions,
            AgentId::from_str("worker").expect("a valid agent id"),
            ConversationId::mint(),
        );
        let sent = tokio::spawn(agdx.emit(b"event".to_vec()).send());
        assert!(
            sent.await
                .expect("the send task joins")
                .is_err_and(|error| matches!(error, LaserError::NoStream))
        );
    }

    #[tokio::test]
    async fn given_a_governor_when_a_prebuilt_envelope_is_published_then_should_govern_it_first() {
        let decided = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let laser = Laser::from_client(iggy::prelude::IggyClient::default())
            .with_default_stream("agdx-governed")
            .with_governor(
                std::sync::Arc::new(Refusing {
                    decided: std::sync::Arc::clone(&decided),
                }),
                crate::govern::GovernorMode::Enforce,
            );
        let conversation = ConversationId::mint();
        let agdx = laser.agdx(
            AgentTopic::Sessions,
            AgentId::from_str("worker").expect("a valid agent id"),
            conversation,
        );
        let envelope = AgentEnvelope::event(
            RecordId::mint(),
            conversation,
            AgentId::from_str("worker").expect("a valid agent id"),
            b"appended".to_vec(),
        );
        let published = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            agdx.publish_envelope(envelope),
        )
        .await;
        assert!(
            matches!(&published, Ok(Err(LaserError::PolicyBlocked(message))) if message == "refused by test"),
            "{published:?}"
        );
        assert_eq!(decided.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn given_a_fail_with_another_content_type_when_sent_then_should_refuse_it() {
        let laser = Laser::from_client(iggy::prelude::IggyClient::default())
            .with_default_stream("agdx-fail");
        let agdx = laser.agdx(
            AgentTopic::Sessions,
            AgentId::from_str("worker").expect("a valid agent id"),
            ConversationId::mint(),
        );
        let error = AgentErrorBody {
            code: laser_wire::agent::AgentErrorCode::Internal,
            message: Some("boom".to_owned()),
            retryable: false,
            detail: None,
        };
        let result = agdx
            .fail(CorrelationId::mint(), &error)
            .expect("the error body encodes")
            .content_type(ContentType::Json)
            .send_receipt()
            .await;
        assert!(matches!(result, Err(LaserError::Invalid(_))));
    }

    #[tokio::test]
    async fn given_a_chunk_publish_failure_when_writing_then_should_keep_the_sequence_for_the_retry()
     {
        // No default stream, so every publish fails before any IO.
        let laser = Laser::from_client(iggy::prelude::IggyClient::default());
        let agdx = laser.agdx(
            AgentTopic::Sessions,
            AgentId::from_str("worker").expect("a valid agent id"),
            ConversationId::mint(),
        );
        let mut stream = agdx.stream(CorrelationId::mint(), OPERATION_CHAT);
        let failed = stream.write(b"t".to_vec()).await;
        assert!(matches!(failed, Err(LaserError::NoStream)), "{failed:?}");
        assert_eq!(stream.sequence, 0, "the unsent chunk keeps its sequence");

        let mut buffered = agdx
            .stream(CorrelationId::mint(), OPERATION_CHAT)
            .buffered(1, Duration::from_secs(60));
        let failed = buffered.write(b"t".to_vec()).await;
        assert!(matches!(failed, Err(LaserError::NoStream)), "{failed:?}");
        assert_eq!(buffered.sequence, 1);
        assert_eq!(
            buffered
                .buffer
                .as_ref()
                .map_or(0, |buffer| buffer.envelopes.len()),
            1,
            "the unsent chunk stays buffered for the next flush"
        );
    }

    #[test]
    fn given_a_new_chunk_stream_when_opened_then_should_carry_the_default_abandonment_bound() {
        let laser = Laser::from_client(iggy::prelude::IggyClient::default())
            .with_default_stream("agdx-stream-bound");
        let opened = SystemClock.now_micros();
        let stream = laser
            .agdx(
                AgentTopic::Sessions,
                AgentId::from_str("worker").expect("a valid agent id"),
                ConversationId::mint(),
            )
            .stream(CorrelationId::mint(), OPERATION_CHAT);
        let bound = stream
            .deadline_micros
            .expect("the opening chunk is bounded");
        assert!(bound >= opened + DEFAULT_CHUNK_ABANDON_MS * 1_000);
    }

    #[test]
    fn given_an_absolute_deadline_when_set_on_a_chunk_stream_then_should_carry_it_unchanged() {
        let laser = Laser::from_client(iggy::prelude::IggyClient::default())
            .with_default_stream("agdx-stream-deadline");
        let stream = laser
            .agdx(
                AgentTopic::Sessions,
                AgentId::from_str("worker").expect("a valid agent id"),
                ConversationId::mint(),
            )
            .stream(CorrelationId::mint(), OPERATION_CHAT)
            .with_deadline_at_micros(1_717_171_777_000_000);
        assert_eq!(stream.deadline_micros, Some(1_717_171_777_000_000));
    }

    // Record conformance, the header half: the typed header encodings the
    // runtime stamps, asserted byte-for-byte. The payload half is pinned
    // against the shared corpus below.
    #[test]
    fn given_agdx_headers_when_stamped_then_should_pin_the_typed_encodings() {
        let (record, conversation, source, correlation) = ids();
        let envelope =
            AgentEnvelope::command(record, conversation, source, correlation, b"x".to_vec())
                .with_target("target-agent".parse().expect("valid agent id"));
        let headers = agdx_headers(&envelope, ContentType::Json, false).expect("headers stamp");

        let value = |key: &str| {
            headers
                .get(&HeaderKey::from_str(key).expect("key parses"))
                .expect("header present")
        };
        // agdx.av: u32, little-endian.
        assert_eq!(value(AGENT_VERSION).as_bytes(), [1, 0, 0, 0]);
        // agdx.ct: one byte, json = 1.
        assert_eq!(value(CONTENT_TYPE).as_bytes(), [1]);
        assert_eq!(
            value(CONVERSATION_ID).as_str().expect("string"),
            conversation.to_string()
        );
        // The agent routing id is a string (the agent's name), not a numeric id.
        assert_eq!(
            value(TARGET_AGENT_ID).as_str().expect("string"),
            "target-agent"
        );
        // The author rides beside the addressee so a header filter selects both.
        assert_eq!(
            value(AGENT_ID).as_str().expect("string"),
            envelope.source.as_str()
        );
        // The partition key is the conversation's canonical base32 form.
        assert_eq!(envelope.conversation.to_string().len(), 26);
    }

    #[test]
    fn given_child_session_envelope_when_stamped_then_should_carry_canonical_ancestry_headers() {
        let (record, conversation, source, correlation) = ids();
        let mut envelope =
            AgentEnvelope::command(record, conversation, source, correlation, b"x".to_vec());
        envelope.parent = Some(laser_wire::agent::ConversationId::from_u128(2));
        envelope.root = Some(laser_wire::agent::ConversationId::from_u128(1));
        let headers = agdx_headers(&envelope, ContentType::Cbor, false).expect("headers stamp");
        for (name, expected) in [
            (PARENT_CONVERSATION_ID, envelope.parent.expect("parent")),
            (ROOT_CONVERSATION_ID, envelope.root.expect("root")),
        ] {
            let value = headers
                .get(&HeaderKey::from_str(name).expect("key"))
                .expect("header");
            assert_eq!(value.as_str().expect("string"), expected.to_string());
        }
    }

    // The payload half of record conformance: the verb path's encoding is
    // byte-identical to the shared golden corpus.
    #[test]
    fn given_the_canonical_command_when_encoded_then_should_match_the_corpus() {
        let (record, conversation, source, correlation) = ids();
        let command = AgentEnvelope::command(
            record,
            conversation,
            source,
            correlation,
            br#"{"ask":"plan the rollout"}"#.to_vec(),
        )
        .with_target("target-agent".parse().expect("valid agent id"))
        .with_idempotency_key("job-123-attempt-2".parse().expect("valid key"))
        .with_deadline_micros(1_717_171_777_000_000)
        .with_operation(OPERATION_CHAT)
        .with_metadata("priority", "high");
        let encoded = encode_named(&command).expect("encodes");
        assert_matches("agent_command.bin", &encoded);
    }

    #[test]
    fn given_an_oversized_chunk_body_when_checked_then_should_reject_before_publish() {
        let body = vec![0u8; MAX_CHUNK_BODY_BYTES + 1];
        let error = ensure_chunk_body_within_cap(&body).expect_err("oversized body rejects");
        assert!(matches!(error, LaserError::Invalid(_)));
    }

    #[test]
    fn given_the_canonical_record_when_encoded_then_should_match_record_fixture() {
        let (record, conversation, source, correlation) = ids();
        let envelope = AgentEnvelope::command(
            record,
            conversation,
            source,
            correlation,
            br#"{"ask":"plan the rollout"}"#.to_vec(),
        )
        .with_target("target-agent".parse().expect("valid agent id"))
        .with_idempotency_key("job-123-attempt-2".parse().expect("valid key"))
        .with_deadline_micros(1_717_171_777_000_000)
        .with_operation(OPERATION_CHAT)
        .with_metadata("priority", "high");
        let headers = agdx_headers(&envelope, ContentType::Json, false).expect("headers stamp");
        let payload = encode_named(&envelope).expect("payload encodes");
        let record =
            CanonicalAlpRecord::from_headers(envelope.conversation.to_string(), headers, payload);
        let encoded = encode_named(&record).expect("record fixture encodes");
        assert_record_fixture("agent_record.bin", &encoded);
    }

    #[derive(Serialize)]
    struct CanonicalAlpRecord {
        partition_key: String,
        headers: BTreeMap<String, CanonicalHeader>,
        payload: Binary,
    }

    impl CanonicalAlpRecord {
        fn from_headers(
            partition_key: String,
            headers: BTreeMap<HeaderKey, HeaderValue>,
            payload: Vec<u8>,
        ) -> Self {
            let mut canonical_headers = BTreeMap::new();
            for (key, value) in headers {
                let key = key.as_str().expect("header key is utf8").to_owned();
                let kind = match key.as_str() {
                    AGENT_VERSION => "u32",
                    CONTENT_TYPE => "u8",
                    CONVERSATION_ID | TARGET_AGENT_ID | AGENT_ID => "string",
                    _ => "bytes",
                };
                canonical_headers.insert(
                    key,
                    CanonicalHeader {
                        kind,
                        bytes: Binary(value.as_bytes().to_vec()),
                    },
                );
            }
            Self {
                partition_key,
                headers: canonical_headers,
                payload: Binary(payload),
            }
        }
    }

    #[derive(Serialize)]
    struct CanonicalHeader {
        kind: &'static str,
        bytes: Binary,
    }

    struct Binary(Vec<u8>);

    impl Serialize for Binary {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serializer.serialize_bytes(&self.0)
        }
    }

    fn assert_record_fixture(name: &str, encoded: &[u8]) {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("sdk crate has workspace parent")
            .join("wire")
            .join("fixtures")
            .join(name);
        if env::var("AGDX_WIRE_FIXTURES_REGEN").is_ok() {
            fs::write(&path, encoded).expect("write record fixture");
        }
        let golden = fs::read(&path).unwrap_or_else(|error| {
            panic!("read fixture {name}: {error} (regen with AGDX_WIRE_FIXTURES_REGEN=1)")
        });
        assert_eq!(
            encoded, golden,
            "fixture `{name}` drifted from the canonical AGDX record"
        );
    }

    fn ids() -> (RecordId, ConversationId, AgentId, CorrelationId) {
        (
            RecordId::from_u128(0x0190_3c1f_aa00_0000_0000_0000_0000_0001),
            ConversationId::from_u128(0x0190_3c1f_aa00_0000_0000_0000_0000_0002),
            "source-agent".parse().expect("valid agent id"),
            CorrelationId::from_u128(0x0190_3c1f_aa00_0000_0000_0000_0000_0005),
        )
    }

    // Fails every decision, so a governed path stops before any I/O.
    struct Refusing {
        decided: std::sync::Arc<std::sync::atomic::AtomicU64>,
    }

    #[async_trait::async_trait]
    impl crate::govern::ActionGovernor for Refusing {
        async fn decide(
            &self,
            _: &crate::govern::GovernedAction<'_>,
        ) -> Result<crate::govern::ActionDecision, LaserError> {
            self.decided
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(LaserError::PolicyBlocked("refused by test".to_owned()))
        }
    }
}
