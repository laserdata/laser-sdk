use crate::agent::Laser;
use crate::error::LaserError;
use crate::provenance::AgentTopic;
use crate::types::ConversationId;
#[cfg(feature = "a2a-http")]
use axum::Router;
#[cfg(feature = "a2a-http")]
use axum::extract::State;
#[cfg(feature = "a2a-http")]
use axum::routing::post;
use laser_wire::agent::{
    self as agdx, AgentEnvelope, AgentId, CapabilityDescriptor, ContentRef, CorrelationId,
    OPERATION_CHAT,
};
use laser_wire::content::ContentType;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
#[cfg(feature = "a2a-http")]
use serde_json::to_value;
#[cfg(feature = "a2a-http")]
use std::str::FromStr;
#[cfg(feature = "a2a-http")]
use std::sync::Arc;
#[cfg(feature = "a2a-http")]
use strum::{Display, EnumString};

pub use laser_wire::agent::TaskState;

#[cfg(feature = "a2a-http")]
const JSONRPC_VERSION: &str = "2.0";
// JSON-RPC reserved range ends at -32000, and -32000..=-32099 is for application errors.
#[cfg(feature = "a2a-http")]
const APP_ERROR_CODE: i32 = -32000;

/// The A2A JSON-RPC methods the bridge serves. `Display`/`FromStr` (strum) carry
/// the exact wire spelling, so the dispatch never matches on bare string literals.
#[cfg(feature = "a2a-http")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, EnumString)]
pub enum A2aMethod {
    #[strum(serialize = "message/send")]
    MessageSend,
    #[strum(serialize = "message/stream")]
    MessageStream,
    #[strum(serialize = "tasks/get")]
    TasksGet,
    #[strum(serialize = "tasks/cancel")]
    TasksCancel,
}

/// The A2A protocol version this bridge's Agent Card declares (the latest
/// established A2A spec, since v1.0 is still draft).
pub const A2A_PROTOCOL_VERSION: &str = "0.3.0";

/// The bridge's A2A Agent Card: the discovery document an A2A client fetches.
/// Field names follow the A2A `AgentCard` schema (`protocolVersion`, `name`,
/// `description`, `url`, `version`, `capabilities`, default I/O modes, `skills`).
#[derive(Debug, Clone, Serialize)]
pub struct AgentCard {
    #[serde(rename = "protocolVersion")]
    pub protocol_version: String,
    pub name: String,
    pub description: String,
    pub url: String,
    pub version: String,
    pub capabilities: AgentCardCapabilities,
    #[serde(rename = "defaultInputModes")]
    pub default_input_modes: Vec<String>,
    #[serde(rename = "defaultOutputModes")]
    pub default_output_modes: Vec<String>,
    pub skills: Vec<AgentSkill>,
}

/// The A2A `AgentCapabilities` flags of an [`AgentCard`].
#[derive(Debug, Clone, Serialize)]
pub struct AgentCardCapabilities {
    /// Whether the agent streams (AGDX chunk channels ride the log, consumed
    /// over Iggy's own transport rather than SSE).
    pub streaming: bool,
    #[serde(rename = "pushNotifications")]
    pub push_notifications: bool,
    #[serde(rename = "stateTransitionHistory")]
    pub state_transition_history: bool,
}

/// One A2A `AgentSkill` advertised on the card.
#[derive(Debug, Clone, Serialize)]
pub struct AgentSkill {
    pub id: String,
    pub name: String,
    pub description: String,
    pub tags: Vec<String>,
    /// The skill's accepted input content modes (A2A `inputModes`), derived from
    /// the capability descriptor's input `ContentRef`. Omitted when unspecified.
    #[serde(rename = "inputModes", skip_serializing_if = "Vec::is_empty")]
    pub input_modes: Vec<String>,
    /// The skill's produced output content modes (A2A `outputModes`).
    #[serde(rename = "outputModes", skip_serializing_if = "Vec::is_empty")]
    pub output_modes: Vec<String>,
}

/// Project a capability's [`ContentRef`] to an A2A content mode string: a
/// content-type code to its media type, a registered schema id to an AGDX schema
/// media type. Unknown codes fall back to opaque bytes.
fn content_ref_mode(reference: &ContentRef) -> String {
    match reference {
        ContentRef::ContentType(content_type) => match content_type {
            ContentType::Raw => "application/octet-stream",
            ContentType::Json => "application/json",
            ContentType::Msgpack => "application/msgpack",
            ContentType::Cbor => "application/cbor",
            ContentType::Bson => "application/bson",
            ContentType::Avro => "application/avro",
            ContentType::Protobuf => "application/protobuf",
            ContentType::Arrow => "application/vnd.apache.arrow.stream",
            ContentType::Ref => "application/x-agdx-ref",
            ContentType::Any => "*/*",
            _ => "application/octet-stream",
        }
        .to_owned(),
        ContentRef::SchemaId(id) => format!("application/x-agdx-schema;id={id}"),
    }
}

/// An A2A task's current state. `state` is the ONE wire dictionary
/// (`laser_wire::agent::TaskState`, re-exported below): the bridge maps its
/// codes to A2A's kebab-case names at the JSON boundary, never a second enum.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStatus {
    #[serde(with = "task_state_json")]
    pub state: TaskState,
}

// The JSON boundary for the task-state dictionary: A2A speaks the kebab-case
// names (the dictionary's `Display`/`FromStr`), while the wire type itself
// rides CBOR as a u8 code. An unknown inbound name is a protocol error.
mod task_state_json {
    use super::TaskState;
    use serde::de::Error;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(state: &TaskState, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(state)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<TaskState, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(D::Error::custom)
    }
}

/// An output artifact produced by an A2A task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Artifact {
    pub text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// An A2A task: its id, status, and artifacts.
pub struct Task {
    pub id: String,
    pub status: TaskStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<Artifact>,
}

/// A JSON-RPC request envelope.
#[derive(Debug, Deserialize)]
pub struct JsonRpcRequest {
    pub id: JsonValue,
    pub method: String,
    #[serde(default)]
    pub params: JsonValue,
}

/// A JSON-RPC response envelope.
#[derive(Debug, Serialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: &'static str,
    pub id: JsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<JsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

/// A JSON-RPC error object.
#[derive(Debug, Serialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
}

/// Map an inbound `message/send` onto its on-log `command` envelope: the
/// mapped core is the ids (the fresh task id rides `correlation`), and the
/// tunneled remainder is the ORIGINAL params JSON, carried byte-identical in
/// `body` with `agdx.ct = json`, so a round trip back out returns the foreign
/// payload untouched instead of flattening it into a lossy intermediate.
pub fn command_from_message_send(
    record: agdx::RecordId,
    conversation: agdx::ConversationId,
    source: agdx::AgentId,
    correlation: agdx::CorrelationId,
    params_json: Vec<u8>,
) -> AgentEnvelope {
    AgentEnvelope::command(record, conversation, source, correlation, params_json)
        .with_operation(OPERATION_CHAT)
}

/// The outbound view: the envelope answering a task becomes the A2A task.
/// `task_state` maps through the dictionary when present. Otherwise a
/// `response` reads completed and an `error` reads failed. A non-empty body
/// becomes the text artifact.
pub fn task_from_envelope(task_id: impl Into<String>, envelope: &AgentEnvelope) -> Task {
    let state = envelope.task_state.unwrap_or(match envelope.kind {
        agdx::AgentKind::Error => TaskState::Failed,
        _ => TaskState::Completed,
    });
    let artifacts = if envelope.body.is_empty() {
        Vec::new()
    } else {
        vec![Artifact {
            text: String::from_utf8_lossy(&envelope.body).into_owned(),
        }]
    };
    Task {
        id: task_id.into(),
        status: TaskStatus { state },
        artifacts,
    }
}

/// Bridges the synchronous A2A JSON-RPC edge onto durable agent topics: a request
/// becomes a message on `request_topic` keyed by a fresh task (conversation) id,
/// and a task lookup replays `reply_topic` for that conversation. The log is the
/// source of truth, so tasks survive a bridge restart and stay replayable.
pub struct A2aBridge {
    laser: Laser,
    source: AgentId,
    request_topic: AgentTopic<'static>,
    reply_topic: AgentTopic<'static>,
    capabilities: Vec<CapabilityDescriptor>,
    /// When set, control records the bridge emits (the cancel) are signed, so a
    /// verifying consumer can authorize them against the enrolled key. Closes the
    /// unauthenticated-cancel hole. Requires the `sign` feature.
    #[cfg(feature = "sign")]
    signing_key: Option<crate::sign::SigningKey>,
}

impl A2aBridge {
    /// A bridge mapping A2A JSON-RPC methods onto agent topics over `laser`,
    /// publishing as `source` (the bridge's agent id). Every topic lives on the
    /// stream of `laser`. Pass `laser.with_stream(stream)` to run the bridge on a
    /// non-default stream (the unit of multi-stream topologies and per-stream
    /// Iggy RBAC). `request_topic`/`reply_topic` are any [`AgentTopic`], including
    /// `AgentTopic::Custom` for an arbitrary name.
    pub fn new(
        laser: Laser,
        source: AgentId,
        request_topic: AgentTopic<'static>,
        reply_topic: AgentTopic<'static>,
    ) -> Self {
        Self {
            laser,
            source,
            request_topic,
            reply_topic,
            capabilities: Vec::new(),
            #[cfg(feature = "sign")]
            signing_key: None,
        }
    }

    /// Sign the control records the bridge emits (the cancel) with `key`, so a
    /// verifying consumer authorizes them against the enrolled key registry. This
    /// is what corrects the otherwise-unauthenticated cancel. Requires the `sign`
    /// feature.
    #[cfg(feature = "sign")]
    #[must_use]
    pub fn with_signing_key(mut self, key: crate::sign::SigningKey) -> Self {
        self.signing_key = Some(key);
        self
    }

    /// Advertise the agent's structured capabilities on the A2A card. Each
    /// [`CapabilityDescriptor`] is projected to an A2A `AgentSkill` by
    /// [`card`](Self::card), so a remote A2A client discovers the agent's skills
    /// and their I/O shapes. Consumes and returns self for chaining.
    #[must_use]
    pub fn with_capabilities(mut self, capabilities: Vec<CapabilityDescriptor>) -> Self {
        self.capabilities = capabilities;
        self
    }

    /// `message/send`: publish the params as a typed AGDX `command` on a fresh
    /// task conversation, tunneling the foreign JSON byte-identical in the body
    /// (`agdx.ct = json`), and returns Submitted. The task id is the conversation, and
    /// the A2A task identity rides `correlation` (derived from it, so the lookup
    /// stays stateless).
    pub async fn submit(&self, params_json: Vec<u8>) -> Result<Task, LaserError> {
        let task = ConversationId::new();
        self.laser
            .agdx(self.request_topic.clone(), self.source.clone(), task.into())
            .command(correlation_of(task), params_json)
            .with_operation(OPERATION_CHAT)
            .content_type(ContentType::Json)
            .send()
            .await?;
        Ok(Task {
            id: task.to_string(),
            status: TaskStatus {
                state: TaskState::Submitted,
            },
            artifacts: Vec::new(),
        })
    }

    /// `tasks/get`: replay the reply topic for the task and map the answering
    /// AGDX envelope (the `response`/`error` carrying this task's `correlation`)
    /// to the A2A task. Still Working until one lands.
    pub async fn task(&self, id: &str) -> Result<Task, LaserError> {
        let conversation = id
            .parse::<ConversationId>()
            .map_err(|_| LaserError::Handler(format!("invalid task id `{id}`")))?;
        let correlation = correlation_of(conversation);
        // A point lookup over the reply topic via the forward reply reader (no
        // full re-scan + sort of the conversation each call).
        let answer = self
            .laser
            .find_agdx_reply(self.reply_topic.clone(), correlation)
            .await?;
        Ok(match answer {
            Some(envelope) => task_from_envelope(id, &envelope),
            None => Task {
                id: id.to_owned(),
                status: TaskStatus {
                    state: TaskState::Working,
                },
                artifacts: Vec::new(),
            },
        })
    }

    /// `tasks/cancel`: publish an AGDX `error` terminal (code `Cancelled`,
    /// `task_state = Canceled`) correlated to the task, so a later `tasks/get`
    /// also reports Canceled, and returns the canceled task.
    pub async fn cancel(&self, id: &str) -> Result<Task, LaserError> {
        let conversation = id
            .parse::<ConversationId>()
            .map_err(|_| LaserError::Handler(format!("invalid task id `{id}`")))?;
        let error = agdx::AgentErrorBody {
            code: agdx::AgentErrorCode::Cancelled,
            message: Some("canceled by the A2A client".to_owned()),
            retryable: false,
            detail: None,
        };
        let producer = self.laser.agdx(
            self.reply_topic.clone(),
            self.source.clone(),
            conversation.into(),
        );
        let send = producer
            .fail(correlation_of(conversation), &error)?
            .with_task_state(TaskState::Canceled);
        // Sign the cancel when a key is configured, so a verifying consumer can
        // authorize it. Without a key it rides unsigned, and a topic that
        // mandates verification rejects it.
        #[cfg(feature = "sign")]
        let send = match &self.signing_key {
            Some(key) => send.signed_by(key),
            None => send,
        };
        send.send().await?;
        Ok(Task {
            id: id.to_owned(),
            status: TaskStatus {
                state: TaskState::Canceled,
            },
            artifacts: Vec::new(),
        })
    }

    /// The bridge's A2A Agent Card, for discovery. Streaming is advertised
    /// because AGDX chunk channels replay off the log. Each advertised
    /// [`CapabilityDescriptor`] (set via [`with_capabilities`](Self::with_capabilities))
    /// projects to an A2A `AgentSkill`: the skill id is its id and name, and the
    /// I/O `ContentRef`s become the skill's input and output modes.
    pub fn card(&self) -> AgentCard {
        let skills = self
            .capabilities
            .iter()
            .map(|capability| AgentSkill {
                id: capability.skill_id.clone(),
                name: capability.skill_id.clone(),
                description: String::new(),
                tags: Vec::new(),
                input_modes: capability.input.iter().map(content_ref_mode).collect(),
                output_modes: capability.output.iter().map(content_ref_mode).collect(),
            })
            .collect();
        AgentCard {
            protocol_version: A2A_PROTOCOL_VERSION.to_owned(),
            name: self.source.as_str().to_owned(),
            description: "LaserData AGDX bridge over the durable log".to_owned(),
            url: "/".to_owned(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            capabilities: AgentCardCapabilities {
                streaming: true,
                push_notifications: false,
                state_transition_history: false,
            },
            default_input_modes: vec!["text/plain".to_owned()],
            default_output_modes: vec!["text/plain".to_owned()],
            skills,
        }
    }

    /// An axum router: the JSON-RPC endpoint at `/` plus the Agent Card at the
    /// A2A well-known discovery path. Requires the `a2a-http` feature. The bridge
    /// adapter (`submit` / `task` / `cancel` / `card`) is usable without it.
    #[cfg(feature = "a2a-http")]
    pub fn router(self: Arc<Self>) -> Router {
        Router::new()
            .route("/", post(handle_rpc))
            .route(
                "/.well-known/agent-card.json",
                axum::routing::get(handle_card),
            )
            .with_state(self)
    }
}

#[cfg(feature = "a2a-http")]
async fn handle_card(State(bridge): State<Arc<A2aBridge>>) -> axum::Json<AgentCard> {
    axum::Json(bridge.card())
}

// The A2A task identity rides AGDX `correlation`, and deriving it from the task's
// conversation keeps the bridge stateless - `tasks/get` reconstructs it from
// the id alone with no mapping table.
fn correlation_of(conversation: ConversationId) -> CorrelationId {
    CorrelationId::from_u128(conversation.as_u128())
}

#[cfg(feature = "a2a-http")]
async fn handle_rpc(
    State(bridge): State<Arc<A2aBridge>>,
    axum::Json(request): axum::Json<JsonRpcRequest>,
) -> axum::Json<JsonRpcResponse> {
    let outcome = match A2aMethod::from_str(&request.method) {
        // `message/send` and `message/stream` both publish the task. The stream
        // is consumed log-natively over Iggy (`Laser::reassemble_channel`), not
        // re-emitted as SSE, so they map to the same publish here.
        Ok(A2aMethod::MessageSend | A2aMethod::MessageStream) => {
            match serde_json::to_vec(&request.params) {
                // The whole params object tunnels byte-identical in the AGDX body.
                Ok(params_json) => bridge.submit(params_json).await,
                Err(error) => Err(LaserError::Codec(format!(
                    "message params are not serializable: {error}"
                ))),
            }
        }
        Ok(A2aMethod::TasksGet) => {
            let id = request
                .params
                .get("id")
                .and_then(JsonValue::as_str)
                .unwrap_or_default();
            bridge.task(id).await
        }
        Ok(A2aMethod::TasksCancel) => {
            let id = request
                .params
                .get("id")
                .and_then(JsonValue::as_str)
                .unwrap_or_default();
            bridge.cancel(id).await
        }
        Err(_) => Err(LaserError::Handler(format!(
            "unknown A2A method `{}`",
            request.method
        ))),
    };
    let response = match outcome {
        Ok(task) => match to_value(task) {
            Ok(value) => JsonRpcResponse {
                jsonrpc: JSONRPC_VERSION,
                id: request.id,
                result: Some(value),
                error: None,
            },
            Err(error) => JsonRpcResponse {
                jsonrpc: JSONRPC_VERSION,
                id: request.id,
                result: None,
                error: Some(JsonRpcError {
                    code: APP_ERROR_CODE,
                    message: format!("serialization failure: {error}"),
                }),
            },
        },
        Err(error) => JsonRpcResponse {
            jsonrpc: JSONRPC_VERSION,
            id: request.id,
            result: None,
            error: Some(JsonRpcError {
                code: APP_ERROR_CODE,
                message: error.to_string(),
            }),
        },
    };
    axum::Json(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use laser_wire::framing::{decode_named, encode_named};
    use serde_json::{from_str, to_string};

    #[test]
    fn given_task_states_when_serialized_then_should_use_the_kebab_case_wire_names() {
        let status = TaskStatus {
            state: TaskState::InputRequired,
        };
        assert_eq!(
            to_string(&status).expect("status serializes"),
            r#"{"state":"input-required"}"#
        );
        let back: TaskStatus = from_str(r#"{"state":"auth-required"}"#).expect("status parses");
        assert_eq!(back.state, TaskState::AuthRequired);
        // An unknown inbound name is a protocol error, never a guess.
        assert!(from_str::<TaskStatus>(r#"{"state":"nope"}"#).is_err());
    }

    #[cfg(feature = "a2a-http")]
    #[test]
    fn given_a2a_methods_when_round_tripped_then_should_match_the_wire_names() {
        assert_eq!(A2aMethod::MessageSend.to_string(), "message/send");
        assert_eq!(
            "tasks/get".parse::<A2aMethod>().expect("the method parses"),
            A2aMethod::TasksGet
        );
        assert!("nope/now".parse::<A2aMethod>().is_err());
    }

    #[test]
    fn given_a_message_send_when_mapped_then_the_foreign_payload_should_tunnel_byte_identical() {
        let params = br#"{"message":{"role":"user","parts":[{"kind":"text","text":"hi"}]},"metadata":{"trace":"abc"}}"#;
        let envelope = command_from_message_send(
            agdx::RecordId::from_u128(1),
            agdx::ConversationId::from_u128(2),
            "source-agent".parse().expect("valid agent id"),
            agdx::CorrelationId::from_u128(4),
            params.to_vec(),
        );
        agdx::validate(&envelope).expect("the mapped command validates");
        let bytes = encode_named(&envelope).expect("encodes");
        let back: AgentEnvelope = decode_named(&bytes).expect("decodes");
        assert_eq!(
            back.body,
            params.to_vec(),
            "the tunneled remainder must round-trip byte-identical"
        );
        assert_eq!(back.correlation, envelope.correlation);

        // The way back out: the answering envelope renders as the A2A task.
        let reply = AgentEnvelope::response(
            agdx::RecordId::from_u128(5),
            agdx::ConversationId::from_u128(2),
            "responder-agent".parse().expect("valid agent id"),
            agdx::CorrelationId::from_u128(4),
            b"plan ready".to_vec(),
        )
        .with_task_state(TaskState::Completed);
        let task = task_from_envelope("t-1", &reply);
        assert_eq!(
            to_string(&task).expect("task serializes"),
            r#"{"id":"t-1","status":{"state":"completed"},"artifacts":[{"text":"plan ready"}]}"#
        );

        // A failure terminal reads failed without a task_state.
        let failure = AgentEnvelope::error(
            agdx::RecordId::from_u128(7),
            agdx::ConversationId::from_u128(2),
            "responder-agent".parse().expect("valid agent id"),
            agdx::CorrelationId::from_u128(4),
            b"boom".to_vec(),
        );
        assert_eq!(
            task_from_envelope("t-1", &failure).status.state,
            TaskState::Failed
        );
    }

    #[test]
    fn given_a_jsonrpc_message_send_when_parsed_then_should_expose_the_text_part() {
        let request: JsonRpcRequest = from_str(
            r#"{"jsonrpc":"2.0","id":1,"method":"message/send",
                "params":{"message":{"role":"user","parts":[{"kind":"text","text":"hi"}]}}}"#,
        )
        .expect("the request parses");
        assert_eq!(request.method, "message/send");
        assert_eq!(
            request
                .params
                .pointer("/message/parts/0/text")
                .and_then(JsonValue::as_str),
            Some("hi")
        );
    }
}
