use super::keys;
use crate::types::{AgentId, ConversationId, MessageId};
use iggy::prelude::{HeaderKey, HeaderKind, HeaderValue, IggyError, IggyMessage, IggyTimestamp};
use laser_wire::headers::{Addressee, HeaderField, RecordHeaders};
use std::collections::BTreeMap;
use std::str::FromStr;

pub use super::topic::AgentTopic;
pub use laser_wire::headers::{HEADER_FRAMING_BYTES, HEADER_SOFT_CAP, HEADER_VALUE_MAX};

/// Why encoding or decoding provenance headers failed.
#[derive(Debug, thiserror::Error)]
pub enum ProvenanceError {
    #[error("missing required header `{0}`")]
    MissingRequired(&'static str),
    #[error("provenance headers {got}B exceed soft cap {cap}B")]
    TooLarge { got: usize, cap: usize },
    #[error("invalid value for header `{0}`")]
    InvalidValue(&'static str),
    #[error("header `{key}` value must not contain control characters or NUL")]
    InvalidValueBytes { key: &'static str },
    #[error("non-finite floating-point value for header `{0}`")]
    NonFinite(&'static str),
    #[error("header `{0}` value must not be empty")]
    EmptyValue(&'static str),
    #[error("header `{key}` value is {got}B, exceeds max {max}B")]
    ValueTooLong {
        key: &'static str,
        got: usize,
        max: usize,
    },
    #[error("malformed Iggy headers on this message: {0}")]
    MalformedHeaders(String),
    #[error(transparent)]
    Header(#[from] IggyError),
    #[error(transparent)]
    Id(#[from] crate::types::IdError),
}

/// Token / cost usage for an LLM call, carried on provenance for rollup.
#[derive(Debug, Clone, Default, PartialEq, bon::Builder)]
pub struct LlmUsage {
    /// Prompt tokens, if known.
    pub input_tokens: Option<u64>,
    /// Completion tokens, if known.
    pub output_tokens: Option<u64>,
    /// Call cost in USD, if known.
    pub cost_usd: Option<f64>,
}

/// The agentic message spine: conversation, causality, routing, usage. Encoded to/from message headers.
#[derive(Debug, Clone, bon::Builder)]
pub struct Provenance {
    /// The conversation this message belongs to (the partition key).
    pub conversation_id: ConversationId,
    /// The message this one is a reply to, if any.
    pub causal_parent: Option<MessageId>,
    /// The conversation this was spawned from (sub-conversations).
    pub parent_conversation_id: Option<ConversationId>,
    /// The root of the conversation tree (sub-conversations).
    pub root_conversation_id: Option<ConversationId>,
    /// The agent that produced this message.
    pub agent: Option<AgentId>,
    /// The agent this message is addressed to (set by `Router`).
    pub target_agent_id: Option<AgentId>,
    /// LLM token / cost usage for this step.
    pub usage: Option<LlmUsage>,
    /// Drop-dead time. A consumer past it dead-letters the message.
    pub deadline: Option<IggyTimestamp>,
    /// Business dedup key: a producer-supplied key that suppresses a duplicate of
    /// the same logical operation. Distinct from [`correlation_id`](Self::correlation_id):
    /// setting a real dedup key and retrying no longer cross-matches replies.
    pub idempotency_key: Option<String>,
    /// Reply-correlation key: matches a reply to the request that minted it,
    /// carried in the `agdx.corr` header. [`Laser::request`](crate::laser::Laser)
    /// mints one when unset. Kept separate from [`idempotency_key`](Self::idempotency_key)
    /// so a business dedup key and a reply correlation never overload one slot.
    pub correlation_id: Option<String>,
    /// The strictly-monotonic per-task fence the producer held, when this message
    /// carries a fenced effect. The reliable consumer drops a message whose fence
    /// is below the highest it has accepted for the task (a stale-holder replay).
    pub fence_token: Option<u64>,
}

impl Provenance {
    /// The Iggy partition key (the conversation id), so one conversation stays ordered.
    pub fn partition_key(&self) -> String {
        self.conversation_id.to_string()
    }
}

impl TryFrom<&Provenance> for BTreeMap<HeaderKey, HeaderValue> {
    type Error = ProvenanceError;

    fn try_from(p: &Provenance) -> Result<Self, Self::Error> {
        let mut headers = RecordHeaders::new(p.conversation_id.into());
        headers.parent = p.parent_conversation_id.map(Into::into);
        headers.root = p.root_conversation_id.map(Into::into);
        headers.agent = p.agent.as_ref().map(wire_agent).transpose()?;
        headers.addressee = p
            .target_agent_id
            .as_ref()
            .map(|target| wire_agent(target).map(Addressee::Agent))
            .transpose()?;
        headers.causal_parent = p.causal_parent.map(|parent| parent.to_string());
        headers.idempotency_key = p.idempotency_key.clone();
        headers.correlation = p.correlation_id.clone();
        headers.fence = p.fence_token;
        headers.deadline_micros = p.deadline.map(|deadline| deadline.as_micros());
        if let Some(usage) = &p.usage {
            headers.input_tokens = usage.input_tokens;
            headers.output_tokens = usage.output_tokens;
            headers.cost_usd = usage.cost_usd;
        }
        header_map(&headers)
    }
}

/// The Iggy header map of one record's header block, checked against the
/// value and soft size caps.
pub(crate) fn header_map(
    headers: &RecordHeaders,
) -> Result<BTreeMap<HeaderKey, HeaderValue>, ProvenanceError> {
    let mut map = BTreeMap::new();
    for (key, field) in headers.encode() {
        let value = match field {
            HeaderField::Text(text) => text_value(key, &text)?,
            HeaderField::Uint8(value) => HeaderValue::from(value),
            HeaderField::Uint32(value) => HeaderValue::from(value),
            HeaderField::Uint64(value) => HeaderValue::from(value),
            HeaderField::Float64(value) if value.is_finite() => HeaderValue::from(value),
            HeaderField::Float64(_) => return Err(ProvenanceError::NonFinite(key)),
        };
        map.insert(HeaderKey::from_str(key)?, value);
    }
    let size: usize = map
        .iter()
        .map(|(k, v)| k.as_bytes().len() + v.as_bytes().len() + HEADER_FRAMING_BYTES)
        .sum();
    if size > HEADER_SOFT_CAP {
        return Err(ProvenanceError::TooLarge {
            got: size,
            cap: HEADER_SOFT_CAP,
        });
    }
    Ok(map)
}

fn wire_agent(agent: &AgentId) -> Result<laser_wire::agent::AgentId, ProvenanceError> {
    agent
        .as_str()
        .parse()
        .map_err(|_| ProvenanceError::InvalidValue(keys::AGENT_ID))
}

impl TryFrom<&IggyMessage> for Provenance {
    type Error = ProvenanceError;

    fn try_from(message: &IggyMessage) -> Result<Self, Self::Error> {
        let headers = message
            .user_headers_map()
            .map_err(|err| ProvenanceError::MalformedHeaders(err.to_string()))?
            .unwrap_or_default();
        provenance_from_headers(&headers)
    }
}

/// Decode provenance from an already-parsed Iggy header map. Split out so a
/// caller that has the map in hand (the envelope-aware consumer, which checks
/// for `agdx.av` first) decodes the headers once rather than re-parsing them.
pub(crate) fn provenance_from_headers(
    headers: &BTreeMap<HeaderKey, HeaderValue>,
) -> Result<Provenance, ProvenanceError> {
    let mut conversation_id: Option<ConversationId> = None;
    let mut causal_parent: Option<MessageId> = None;
    let mut parent_conversation_id: Option<ConversationId> = None;
    let mut root_conversation_id: Option<ConversationId> = None;
    let mut agent: Option<AgentId> = None;
    let mut target_agent_id: Option<AgentId> = None;
    let mut idempotency_key: Option<String> = None;
    let mut correlation_id: Option<String> = None;
    let mut fence_token: Option<u64> = None;
    let mut deadline: Option<IggyTimestamp> = None;
    let mut usage = LlmUsage::default();
    let mut has_usage = false;

    for (key, value) in headers {
        let key_str = key
            .as_str()
            .map_err(|err| ProvenanceError::MalformedHeaders(err.to_string()))?;
        // Match on the key first. A header outside the provenance dictionary
        // (the AGDX `agdx.ct` u8, `agdx.av` u32, any app-custom key) is foreign
        // and ignored. Ids ride as strings and numbers ride typed. A known key
        // with another value kind is corruption, not something to drop, so it
        // is a decode error.
        match key_str {
            keys::CONVERSATION_ID => {
                conversation_id = Some(match value.kind() {
                    HeaderKind::Uint128 => laser_wire::agent::ConversationId::from_u128(
                        value
                            .as_uint128()
                            .map_err(|_| ProvenanceError::InvalidValue(keys::CONVERSATION_ID))?,
                    )
                    .into(),
                    _ => str_value(value, keys::CONVERSATION_ID)?.parse()?,
                });
            }
            keys::CAUSAL_PARENT => {
                causal_parent = Some(str_value(value, keys::CAUSAL_PARENT)?.parse()?);
            }
            keys::PARENT_CONVERSATION_ID => {
                parent_conversation_id =
                    Some(str_value(value, keys::PARENT_CONVERSATION_ID)?.parse()?);
            }
            keys::ROOT_CONVERSATION_ID => {
                root_conversation_id = Some(str_value(value, keys::ROOT_CONVERSATION_ID)?.parse()?);
            }
            keys::AGENT_ID => agent = Some(str_value(value, keys::AGENT_ID)?.parse()?),
            keys::TARGET_AGENT_ID => {
                // Broadcast `*` addresses every agent and is never an agent id.
                let target = str_value(value, keys::TARGET_AGENT_ID)?;
                if target != laser_wire::headers::BROADCAST {
                    target_agent_id = Some(target.parse()?);
                }
            }
            keys::IDEMPOTENCY_KEY => {
                idempotency_key = Some(str_value(value, keys::IDEMPOTENCY_KEY)?.to_owned());
            }
            keys::CORRELATION_ID => {
                correlation_id = Some(str_value(value, keys::CORRELATION_ID)?.to_owned());
            }
            // A malformed fence must be a decode error, never `.ok()`-ed to `None`:
            // silently dropping it would skip the gate and let a tampered record
            // through as unfenced.
            keys::FENCE => fence_token = Some(u64_value(value, keys::FENCE)?),
            keys::DEADLINE => {
                deadline = Some(IggyTimestamp::from(u64_value(value, keys::DEADLINE)?));
            }
            keys::USAGE_INPUT_TOKENS => {
                usage.input_tokens = Some(u64_value(value, keys::USAGE_INPUT_TOKENS)?);
                has_usage = true;
            }
            keys::USAGE_OUTPUT_TOKENS => {
                usage.output_tokens = Some(u64_value(value, keys::USAGE_OUTPUT_TOKENS)?);
                has_usage = true;
            }
            keys::COST_USD => {
                let cost = value
                    .as_float64()
                    .map_err(|_| ProvenanceError::InvalidValue(keys::COST_USD))?;
                if !cost.is_finite() {
                    return Err(ProvenanceError::NonFinite(keys::COST_USD));
                }
                usage.cost_usd = Some(cost);
                has_usage = true;
            }
            _ => {}
        }
    }

    Ok(Provenance {
        conversation_id: conversation_id
            .ok_or(ProvenanceError::MissingRequired(keys::CONVERSATION_ID))?,
        causal_parent,
        parent_conversation_id,
        root_conversation_id,
        agent,
        target_agent_id,
        usage: has_usage.then_some(usage),
        deadline,
        idempotency_key,
        correlation_id,
        fence_token,
    })
}

fn text_value(key: &'static str, value: &str) -> Result<HeaderValue, ProvenanceError> {
    if value.is_empty() {
        return Err(ProvenanceError::EmptyValue(key));
    }
    if value.len() > HEADER_VALUE_MAX {
        return Err(ProvenanceError::ValueTooLong {
            key,
            got: value.len(),
            max: HEADER_VALUE_MAX,
        });
    }
    // Reject ASCII control characters and DEL. Header values ride through Iggy
    // and downstream log formatters / OTel exporters, and a stray `\n` or `\0`
    // breaks line-oriented parsers and corrupts traces.
    if value.bytes().any(|b| b < 0x20 || b == 0x7f) {
        return Err(ProvenanceError::InvalidValueBytes { key });
    }
    Ok(HeaderValue::from_str(value)?)
}

// A malformed number must be a decode error, never dropped: a silently missing
// fence would skip the gate and let a tampered record through as unfenced.
fn u64_value(value: &HeaderValue, key: &'static str) -> Result<u64, ProvenanceError> {
    value
        .as_uint64()
        .map_err(|_| ProvenanceError::InvalidValue(key))
}

// A known provenance key's value must be a string, and a non-string value there is
// corruption, reported as an error rather than silently dropped.
fn str_value<'a>(value: &'a HeaderValue, key: &'static str) -> Result<&'a str, ProvenanceError> {
    value
        .as_str()
        .map_err(|_| ProvenanceError::InvalidValue(key))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn given_provenance_when_round_tripped_through_headers_then_should_preserve_every_field() {
        let conversation_id = ConversationId::new();
        let provenance = Provenance::builder()
            .conversation_id(conversation_id)
            .causal_parent(MessageId::new(2, 7))
            .agent("planner".parse().expect("planner is a valid agent id"))
            .target_agent_id("executor".parse().expect("executor is a valid agent id"))
            .idempotency_key("key-1".to_owned())
            .correlation_id("corr-1".to_owned())
            .fence_token(7)
            .usage(
                LlmUsage::builder()
                    .input_tokens(10)
                    .output_tokens(20)
                    .build(),
            )
            .build();

        let headers: BTreeMap<HeaderKey, HeaderValue> = (&provenance)
            .try_into()
            .expect("provenance should encode into headers");
        let message = IggyMessage::builder()
            .payload(Bytes::from_static(b"hello"))
            .user_headers(headers)
            .build()
            .expect("the message should build");

        let back =
            Provenance::try_from(&message).expect("provenance should decode from the message");
        assert_eq!(back.conversation_id, conversation_id);
        assert_eq!(back.causal_parent, Some(MessageId::new(2, 7)));
        assert_eq!(back.agent.expect("agent should be set").as_str(), "planner");
        assert_eq!(
            back.target_agent_id
                .expect("target agent should be set")
                .as_str(),
            "executor"
        );
        assert_eq!(back.idempotency_key.as_deref(), Some("key-1"));
        assert_eq!(back.correlation_id.as_deref(), Some("corr-1"));
        assert_eq!(back.fence_token, Some(7));
        let usage = back.usage.expect("usage should be set");
        assert_eq!(usage.input_tokens, Some(10));
        assert_eq!(usage.output_tokens, Some(20));
    }

    #[test]
    fn given_a_malformed_fence_header_when_decoded_then_should_error_not_skip() {
        let conversation = ConversationId::new();
        let mut headers = BTreeMap::new();
        headers.insert(
            HeaderKey::from_str(keys::CONVERSATION_ID).expect("a valid header key"),
            HeaderValue::from_str(&conversation.to_string()).expect("a valid header value"),
        );
        // A non-numeric fence must be rejected, never `.ok()`-ed to absent, or a
        // tampered record would slip through the gate as unfenced.
        headers.insert(
            HeaderKey::from_str(keys::FENCE).expect("a valid header key"),
            HeaderValue::from_str("not-a-number").expect("a valid header value"),
        );
        assert!(provenance_from_headers(&headers).is_err());
    }

    #[test]
    fn given_a_message_without_a_conversation_id_when_decoded_then_should_error() {
        let message = IggyMessage::builder()
            .payload(Bytes::from_static(b"x"))
            .build()
            .expect("the message should build");
        assert!(matches!(
            Provenance::try_from(&message),
            Err(ProvenanceError::MissingRequired(keys::CONVERSATION_ID))
        ));
    }

    #[test]
    fn given_a_typed_conversation_header_when_decoded_then_should_preserve_the_id() {
        let conversation = ConversationId::new();
        let mut headers = BTreeMap::new();
        headers.insert(
            HeaderKey::from_str(keys::CONVERSATION_ID).expect("a valid header key"),
            HeaderValue::from(conversation.as_u128()),
        );
        let message = IggyMessage::builder()
            .payload(Bytes::from_static(b"typed"))
            .user_headers(headers)
            .build()
            .expect("the message should build");

        let decoded = Provenance::try_from(&message).expect("typed conversation should decode");
        assert_eq!(decoded.conversation_id, conversation);
    }

    #[test]
    fn given_a_typed_non_string_header_when_decoded_then_should_skip_it_not_error() {
        let conversation = ConversationId::new();
        let mut headers = BTreeMap::new();
        headers.insert(
            HeaderKey::from_str(keys::CONVERSATION_ID).expect("a valid header key"),
            HeaderValue::from_str(&conversation.to_string()).expect("a valid header value"),
        );
        // A typed (u8) header riding alongside provenance, like the AGDX `agdx.ct`.
        headers.insert(
            HeaderKey::from_str("agdx.ct").expect("a valid header key"),
            HeaderValue::from(7u8),
        );
        let message = IggyMessage::builder()
            .payload(Bytes::from_static(b"x"))
            .user_headers(headers)
            .build()
            .expect("the message should build");
        let provenance = Provenance::try_from(&message)
            .expect("a foreign typed header is ignored, not a decode error");
        assert_eq!(provenance.conversation_id, conversation);
    }

    #[test]
    fn given_a_known_key_with_the_wrong_value_kind_when_decoded_then_should_error() {
        let conversation = ConversationId::new();
        let mut headers = BTreeMap::new();
        headers.insert(
            HeaderKey::from_str(keys::CONVERSATION_ID).expect("a valid header key"),
            HeaderValue::from_str(&conversation.to_string()).expect("a valid header value"),
        );
        // `agdx.deadline` is a known provenance key that rides as a typed u64, so
        // a text value there is corruption, not a header to silently drop.
        headers.insert(
            HeaderKey::from_str(keys::DEADLINE).expect("a valid header key"),
            HeaderValue::from_str("42").expect("a valid header value"),
        );
        let message = IggyMessage::builder()
            .payload(Bytes::from_static(b"x"))
            .user_headers(headers)
            .build()
            .expect("the message should build");
        assert!(matches!(
            Provenance::try_from(&message),
            Err(ProvenanceError::InvalidValue(keys::DEADLINE))
        ));
    }

    #[test]
    fn given_an_oversized_idempotency_key_when_encoded_then_should_report_a_clear_error() {
        let provenance = Provenance::builder()
            .conversation_id(ConversationId::new())
            .idempotency_key("x".repeat(HEADER_VALUE_MAX + 1))
            .build();
        let result: Result<BTreeMap<HeaderKey, HeaderValue>, _> = (&provenance).try_into();
        assert!(matches!(
            result,
            Err(ProvenanceError::ValueTooLong {
                key: keys::IDEMPOTENCY_KEY,
                ..
            })
        ));
    }

    #[test]
    fn given_an_empty_idempotency_key_when_encoded_then_should_report_a_clear_error() {
        let provenance = Provenance::builder()
            .conversation_id(ConversationId::new())
            .idempotency_key(String::new())
            .build();
        let result: Result<BTreeMap<HeaderKey, HeaderValue>, _> = (&provenance).try_into();
        assert!(matches!(
            result,
            Err(ProvenanceError::EmptyValue(keys::IDEMPOTENCY_KEY))
        ));
    }

    #[test]
    fn given_the_current_header_names_when_checked_then_they_stay_current() {
        assert_eq!(keys::CONVERSATION_ID, "gen_ai.conversation.id");
        assert_eq!(keys::AGENT_ID, "gen_ai.agent.id");
        assert_eq!(keys::USAGE_INPUT_TOKENS, "gen_ai.usage.input_tokens");
        assert_eq!(keys::USAGE_OUTPUT_TOKENS, "gen_ai.usage.output_tokens");
        assert_eq!(keys::CAUSAL_PARENT, "agdx.cause");
        assert_eq!(keys::PARENT_CONVERSATION_ID, "agdx.parent_conv");
        assert_eq!(keys::ROOT_CONVERSATION_ID, "agdx.root_conv");
        assert_eq!(keys::TARGET_AGENT_ID, "agdx.to");
        assert_eq!(keys::IDEMPOTENCY_KEY, "agdx.idem");
        assert_eq!(keys::COST_USD, "agdx.cost");
    }
}
