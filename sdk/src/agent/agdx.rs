use crate::error::LaserError;
use crate::laser::Laser;
use crate::provenance::AgentTopic;
use crate::types::MintUlid;
use iggy::prelude::{HeaderKey, HeaderValue};
use laser_wire::agent::{
    AgentEnvelope, AgentErrorBody, AgentId, AgentKind, ChannelId, ConversationId, CorrelationId,
    IdempotencyKey, LogPosition, RecordId, TaskState, TokenUsage, validate,
};
use laser_wire::codes::AGENT_OP_VERSION;
use laser_wire::content::ContentType;
use laser_wire::framing::{decode_named, encode_named};
use laser_wire::headers::{AGENT_VERSION, CONTENT_TYPE, CONVERSATION_ID, TARGET_AGENT_ID};
use laser_wire::query::Value;
use std::collections::BTreeMap;
use std::str::FromStr;
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

/// The typed AGDX producer over one agent topic: every send is a validated
/// [`AgentEnvelope`] (invalid envelopes are unrepresentable or rejected at
/// publish time), encoded as named-field CBOR, stamped with the routing
/// headers (`agdx.av` u32, `agdx.ct` u8, the conversation as a typed `Uint128` and
/// the target as the agent's name string), and partition-keyed by the
/// conversation's canonical base32 form so one conversation stays ordered.
#[derive(Clone)]
pub struct Agdx {
    laser: Laser,
    topic: String,
    source: AgentId,
    conversation: ConversationId,
}

impl Laser {
    /// A typed AGDX producer publishing as `source` within `conversation` on
    /// `topic`. The topic is resolved to its name at construction, so a runtime
    /// `AgentTopic::Custom` built from a borrowed identifier is accepted.
    pub fn agdx(
        &self,
        topic: AgentTopic<'_>,
        source: AgentId,
        conversation: ConversationId,
    ) -> Agdx {
        Agdx {
            laser: self.clone(),
            topic: topic.topic_string(),
            source,
            conversation,
        }
    }
}

impl Agdx {
    /// A `command`: expects a reply or effect under `correlation`.
    pub fn command(&self, correlation: CorrelationId, body: Vec<u8>) -> AgdxSend<'_> {
        self.send_of(AgentEnvelope::command(
            RecordId::mint(),
            self.conversation,
            self.source.clone(),
            correlation,
            body,
        ))
    }

    /// A `response`: the paired answer to a command (same `correlation`).
    pub fn respond(&self, correlation: CorrelationId, body: Vec<u8>) -> AgdxSend<'_> {
        self.send_of(AgentEnvelope::response(
            RecordId::mint(),
            self.conversation,
            self.source.clone(),
            correlation,
            body,
        ))
    }

    /// An `event`: expects nothing.
    pub fn emit(&self, body: Vec<u8>) -> AgdxSend<'_> {
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
    pub fn status(&self, operation: impl Into<String>) -> AgdxSend<'_> {
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
    ) -> Result<AgdxSend<'_>, LaserError> {
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
            deadline_micros: None,
            target: None,
            content_type: ContentType::Raw,
        }
    }

    /// Human-in-the-loop interrupt/resume. Pauses on a human: publishes a prompt
    /// `command` under a fresh interrupt correlation on this producer's topic,
    /// then awaits the human's correlated `response` on `reply_topic` up to
    /// `timeout` and returns its body. A responder answers with
    /// [`AgentCtx::respond_input`](crate::agent::AgentCtx::respond_input) (a
    /// `response`) or rejects with an `error`, which surfaces here as
    /// [`LaserError::Rejected`]. Composes existing verbs, so it adds nothing to
    /// the wire. It blocks the caller until the response lands or the timeout
    /// elapses, which is the point: the task is genuinely paused on a human.
    pub async fn request_input(
        &self,
        reply_topic: AgentTopic<'_>,
        prompt: impl Into<Vec<u8>>,
        timeout: Duration,
    ) -> Result<Vec<u8>, LaserError> {
        let interrupt = CorrelationId::mint();
        // Seed the reply reader at the topic tail before sending the prompt, so it
        // reads only the human's response rather than the topic's history.
        let mut reader = self.laser.agdx_reply_reader(reply_topic).await?;
        self.command(interrupt, prompt.into()).send().await?;
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
        Ok(reply.body)
    }

    fn send_of(&self, envelope: AgentEnvelope) -> AgdxSend<'_> {
        AgdxSend {
            agdx: self,
            envelope,
            content_type: ContentType::Raw,
            #[cfg(feature = "sign")]
            sign_key: None,
        }
    }

    async fn publish(
        &self,
        envelope: AgentEnvelope,
        content_type: ContentType,
    ) -> Result<Option<RecordId>, LaserError> {
        validate(&envelope)?;
        let record = envelope.record;
        let payload = encode_named(&envelope)?;
        let headers = agdx_headers(&envelope, content_type)?;
        let partition_key = envelope.conversation.to_string();
        self.laser
            .send_with_headers(&self.topic, payload, headers, Some(&partition_key))
            .await?;
        Ok(record)
    }
}

/// One pending AGDX send: the envelope built by its verb, refined by the
/// `with_*` setters, validated and published by [`send`](Self::send).
#[must_use = "an unsent envelope does nothing until you call .send().await"]
pub struct AgdxSend<'a> {
    agdx: &'a Agdx,
    envelope: AgentEnvelope,
    content_type: ContentType,
    /// When set, the envelope is signed with this key just before encoding (the
    /// last step, so all the `with_*` refinements are covered). A verifying
    /// consumer rejects an unsigned or unverified record on a control or effect
    /// topic (the mandatory-verification gate), which is what makes a cancel or
    /// quarantine authorizable.
    #[cfg(feature = "sign")]
    sign_key: Option<&'a crate::sign::SigningKey>,
}

impl<'a> AgdxSend<'a> {
    /// Narrow delivery to one agent within the shared topic (routing, never an ACL).
    pub fn with_target(mut self, target: AgentId) -> Self {
        self.envelope = self.envelope.with_target(target);
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
    pub fn with_deadline_micros(mut self, deadline_micros: u64) -> Self {
        self.envelope = self.envelope.with_deadline_micros(deadline_micros);
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

    /// Declare the body's codec (`agdx.ct`). Defaults to raw.
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
    /// control-plane authorization gate. Requires the `sign` feature.
    #[cfg(feature = "sign")]
    pub fn signed_by(mut self, key: &'a crate::sign::SigningKey) -> Self {
        self.sign_key = Some(key);
        self
    }

    /// Validate, encode, stamp the headers, and publish. Returns the minted
    /// record id (chunks have none). When [`signed_by`](Self::signed_by) set a
    /// key, the envelope is signed here, last, so the signature covers every
    /// refinement.
    pub async fn send(self) -> Result<Option<RecordId>, LaserError> {
        #[cfg(feature = "sign")]
        let envelope = if let Some(key) = self.sign_key {
            let signature = key.sign(&self.envelope)?;
            self.envelope.with_signature(signature)
        } else {
            self.envelope
        };
        #[cfg(not(feature = "sign"))]
        let envelope = self.envelope;
        self.agdx.publish(envelope, self.content_type).await
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
}

impl AgdxStream {
    /// The stream's channel id.
    pub fn channel(&self) -> ChannelId {
        self.channel
    }

    /// Declare the reader-local abandonment bound (rides the opening chunk,
    /// so set it before the first write).
    pub fn with_deadline_micros(mut self, deadline_micros: u64) -> Self {
        self.deadline_micros = deadline_micros.into();
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

    /// Publish the next chunk. The opening chunk (`sequence` 0) carries the
    /// stream purpose and the abandonment bound.
    pub async fn write(&mut self, body: Vec<u8>) -> Result<(), LaserError> {
        ensure_chunk_body_within_cap(&body)?;
        let envelope = self.chunk(body, false, None, None);
        self.agdx.publish(envelope, self.content_type).await?;
        self.sequence += 1;
        Ok(())
    }

    /// Publish the terminal chunk (`last = true`) with the reason the stream
    /// ended and the whole-stream accounting.
    pub async fn finish(
        self,
        finish_reason: impl Into<String>,
        usage: Option<TokenUsage>,
    ) -> Result<(), LaserError> {
        let envelope = self.chunk(Vec::new(), true, Some(finish_reason.into()), usage);
        self.agdx.publish(envelope, self.content_type).await?;
        Ok(())
    }

    /// Terminate the stream with a `kind = error` terminal carrying this
    /// channel and the next sequence.
    pub async fn fail(self, error: &AgentErrorBody) -> Result<(), LaserError> {
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
        self.agdx.publish(envelope, ContentType::Cbor).await?;
        Ok(())
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

// The AGDX routing headers: `agdx.av` selects the decoder before any body byte
// is read, `agdx.ct` names the inner body codec, the conversation id rides as a
// typed Uint128 (little-endian on the server wire, per Iggy's header encoding),
// and the target rides as the agent's name string, so projections and plain
// consumers route without decoding the CBOR body.
fn agdx_headers(
    envelope: &AgentEnvelope,
    content_type: ContentType,
) -> Result<BTreeMap<HeaderKey, HeaderValue>, LaserError> {
    let mut headers = BTreeMap::new();
    headers.insert(
        HeaderKey::from_str(AGENT_VERSION)?,
        HeaderValue::from(AGENT_OP_VERSION),
    );
    headers.insert(
        HeaderKey::from_str(CONTENT_TYPE)?,
        HeaderValue::from(content_type.code()),
    );
    headers.insert(
        HeaderKey::from_str(CONVERSATION_ID)?,
        HeaderValue::from(envelope.conversation.as_u128()),
    );
    if let Some(target) = &envelope.target {
        headers.insert(
            HeaderKey::from_str(TARGET_AGENT_ID)?,
            HeaderValue::from_str(target.as_str())?,
        );
    }
    Ok(headers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use laser_wire::agent::OPERATION_CHAT;
    use laser_wire::fixtures::assert_matches;
    use serde::Serialize;
    use serde::Serializer;
    use std::env;
    use std::fs;
    use std::path::PathBuf;

    // Record conformance, the header half: the typed header encodings the
    // runtime stamps, asserted byte-for-byte. The payload half is pinned
    // against the shared corpus below.
    #[test]
    fn given_agdx_headers_when_stamped_then_should_pin_the_typed_encodings() {
        let (record, conversation, source, correlation) = ids();
        let envelope =
            AgentEnvelope::command(record, conversation, source, correlation, b"x".to_vec())
                .with_target("target-agent".parse().expect("valid agent id"));
        let headers = agdx_headers(&envelope, ContentType::Json).expect("headers stamp");

        let value = |key: &str| {
            headers
                .get(&HeaderKey::from_str(key).expect("key parses"))
                .expect("header present")
        };
        // agdx.av: u32, little-endian.
        assert_eq!(value(AGENT_VERSION).as_bytes(), [1, 0, 0, 0]);
        // agdx.ct: one byte, json = 1.
        assert_eq!(value(CONTENT_TYPE).as_bytes(), [1]);
        // The conversation routing id: Uint128, little-endian 16 bytes (the
        // HEADER encoding, and the payload rides the same id big-endian as a
        // 16-byte CBOR byte string).
        assert_eq!(
            value(CONVERSATION_ID).as_uint128().expect("uint128"),
            conversation.as_u128()
        );
        assert_eq!(
            value(CONVERSATION_ID).as_bytes(),
            conversation.as_u128().to_le_bytes()
        );
        // The agent routing id is a string (the agent's name), not a numeric id.
        assert_eq!(
            value(TARGET_AGENT_ID).as_str().expect("string"),
            "target-agent"
        );
        // The partition key is the conversation's canonical base32 form.
        assert_eq!(envelope.conversation.to_string().len(), 26);
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
            br#"{"ask":"plan the trip"}"#.to_vec(),
        )
        .with_target("target-agent".parse().expect("valid agent id"))
        .with_idempotency_key("order-123-attempt-2".parse().expect("valid key"))
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
            br#"{"ask":"plan the trip"}"#.to_vec(),
        )
        .with_target("target-agent".parse().expect("valid agent id"))
        .with_idempotency_key("order-123-attempt-2".parse().expect("valid key"))
        .with_deadline_micros(1_717_171_777_000_000)
        .with_operation(OPERATION_CHAT)
        .with_metadata("priority", "high");
        let headers = agdx_headers(&envelope, ContentType::Json).expect("headers stamp");
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
                    CONVERSATION_ID => "uint128",
                    TARGET_AGENT_ID => "string",
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
}
