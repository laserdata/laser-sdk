use crate::agent::Laser;
use crate::bridge_hops::{call_hops, hops_metadata, inbound_hops};
#[cfg(feature = "a2a-http")]
use crate::bridge_origin::origin_allowed;
use crate::error::LaserError;
use crate::poll::DrainRange;
use crate::provenance::AgentTopic;
use crate::types::ConversationId;
#[cfg(feature = "a2a-http")]
use axum::Router;
#[cfg(feature = "a2a-http")]
use axum::extract::State;
#[cfg(feature = "a2a-http")]
use axum::http::{HeaderMap, StatusCode};
#[cfg(feature = "a2a-http")]
use axum::response::{IntoResponse, Response};
#[cfg(feature = "a2a-http")]
use axum::routing::post;
use iggy::prelude::{
    Consumer, Identifier, IggyMessage, IggyTimestamp, MessageClient, PollOptions, PollingStrategy,
    StreamClient, TopicClient,
};
use laser_wire::agent::{
    self as agdx, AgentEnvelope, AgentId, AgentKind, CapabilityDescriptor, ContentRef,
    CorrelationId, METADATA_BRIDGE_HOPS, OPERATION_CHAT,
};
use laser_wire::content::ContentType;
use laser_wire::result::ResultCode;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use serde_json::to_value;
use std::str::FromStr;
#[cfg(feature = "a2a-http")]
use std::sync::Arc;
use std::time::Duration;
use strum::{Display, EnumString};

pub use crate::bridge_hops::{BRIDGE_HOPS_META_KEY, enter_bridge};
pub use laser_wire::agent::TaskState;

const JSONRPC_VERSION: &str = "2.0";
const INVALID_REQUEST: i32 = -32600;
const METHOD_NOT_FOUND: i32 = -32601;
const INVALID_PARAMS: i32 = -32602;
const INTERNAL_ERROR: i32 = -32603;
const TASK_NOT_FOUND: i32 = -32001;
const TASK_NOT_CANCELABLE: i32 = -32002;
const UNSUPPORTED_OPERATION: i32 = -32004;
// How far before a task's minted time its lookup starts, so a server clock
// behind the bridge's still finds the command.
const LOOKUP_CLOCK_SLACK: Duration = Duration::from_secs(300);
const LOOKUP_BATCH: u32 = 1000;
const MAX_LOOKUP_PASSES: usize = 32;
const LOOKUP_READER: &str = "laser-a2a-task-lookup";
const DEFAULT_CARD_DESCRIPTION: &str = "LaserData AGDX bridge over the durable log";

/// The A2A JSON-RPC methods the bridge serves. `Display`/`FromStr` (strum) carry
/// the exact wire spelling (v1.0's PascalCase operation names), so the dispatch
/// never matches on bare string literals. v1.0's `ListTasks` is not served: the
/// bridge is stateless over the log, so it answers -32601 method not found.
/// `SendStreamingMessage` answers -32004 unsupported operation, as the card
/// does not advertise streaming.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, EnumString)]
pub enum A2aMethod {
    #[strum(serialize = "SendMessage")]
    MessageSend,
    #[strum(serialize = "SendStreamingMessage")]
    MessageStream,
    #[strum(serialize = "GetTask")]
    TasksGet,
    #[strum(serialize = "CancelTask")]
    TasksCancel,
}

/// The A2A protocol version this bridge's Agent Card declares.
pub const A2A_PROTOCOL_VERSION: &str = "1.0";

/// The JSON-RPC binding label of a v1.0 `AgentInterface`.
pub const A2A_JSONRPC_BINDING: &str = "JSONRPC";

/// The bridge's A2A Agent Card: the discovery document an A2A client fetches.
/// Field names follow the v1.0 `AgentCard` schema: the endpoint and its
/// protocol version live in `supportedInterfaces` (v1.0 dropped the top-level
/// `protocolVersion` / `url` / `preferredTransport`), and `signatures` carries
/// the optional detached JWS set (`sign` feature).
#[derive(Debug, Clone, Serialize)]
pub struct AgentCard {
    pub name: String,
    pub description: String,
    pub version: String,
    #[serde(rename = "supportedInterfaces")]
    pub supported_interfaces: Vec<AgentInterface>,
    pub capabilities: AgentCardCapabilities,
    #[serde(rename = "defaultInputModes")]
    pub default_input_modes: Vec<String>,
    #[serde(rename = "defaultOutputModes")]
    pub default_output_modes: Vec<String>,
    pub skills: Vec<AgentSkill>,
    /// Detached JWS signatures over the JCS-canonicalized card (RFC 8785 +
    /// RFC 7515), empty when unsigned.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub signatures: Vec<AgentCardSignature>,
}

/// One v1.0 `AgentInterface`: where the agent serves and over what binding.
#[derive(Debug, Clone, Serialize)]
pub struct AgentInterface {
    pub url: String,
    #[serde(rename = "protocolBinding")]
    pub protocol_binding: String,
    #[serde(rename = "protocolVersion")]
    pub protocol_version: String,
}

/// One v1.0 `AgentCardSignature`: an RFC 7515 JWS over the card, detached (the
/// payload is the JCS-canonicalized card without `signatures`, reconstructed by
/// the verifier).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentCardSignature {
    /// The base64url-encoded protected JWS header (carries `alg`).
    pub protected: String,
    /// The base64url-encoded signature.
    pub signature: String,
}

/// The A2A `AgentCapabilities` flags of an [`AgentCard`].
#[derive(Debug, Clone, Serialize)]
pub struct AgentCardCapabilities {
    /// Whether the agent streams over SSE. Never: AGDX chunk channels ride the
    /// log, consumed over Iggy's own transport.
    pub streaming: bool,
    #[serde(rename = "pushNotifications")]
    pub push_notifications: bool,
    #[serde(rename = "stateTransitionHistory")]
    pub state_transition_history: bool,
    /// v1.0: whether an authenticated extended card is served (never, the
    /// well-known card is the whole truth here).
    #[serde(rename = "extendedAgentCard")]
    pub extended_agent_card: bool,
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
/// codes to A2A v1.0's `TASK_STATE_*` names at the JSON boundary, never a
/// second enum.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStatus {
    #[serde(with = "task_state_json")]
    pub state: TaskState,
}

// The JSON boundary for the task-state dictionary: A2A v1.0 speaks the
// `TASK_STATE_*` enum names, while the wire type itself rides CBOR as a u8
// code. Paused has no A2A name and reads as working, a state no A2A name covers
// reads as unspecified. An unknown inbound name is a protocol error.
mod task_state_json {
    use super::TaskState;
    use serde::de::Error;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(state: &TaskState, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(match state {
            TaskState::Submitted => "TASK_STATE_SUBMITTED",
            TaskState::Working | TaskState::Paused => "TASK_STATE_WORKING",
            TaskState::InputRequired => "TASK_STATE_INPUT_REQUIRED",
            TaskState::Completed => "TASK_STATE_COMPLETED",
            TaskState::Canceled => "TASK_STATE_CANCELED",
            TaskState::Failed => "TASK_STATE_FAILED",
            TaskState::Rejected => "TASK_STATE_REJECTED",
            TaskState::AuthRequired => "TASK_STATE_AUTH_REQUIRED",
            TaskState::Unknown | TaskState::Unrecognized(_) => "TASK_STATE_UNSPECIFIED",
        })
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<TaskState, D::Error> {
        Ok(match String::deserialize(deserializer)?.as_str() {
            "TASK_STATE_SUBMITTED" => TaskState::Submitted,
            "TASK_STATE_WORKING" => TaskState::Working,
            "TASK_STATE_INPUT_REQUIRED" => TaskState::InputRequired,
            "TASK_STATE_COMPLETED" => TaskState::Completed,
            "TASK_STATE_CANCELED" => TaskState::Canceled,
            "TASK_STATE_FAILED" => TaskState::Failed,
            "TASK_STATE_REJECTED" => TaskState::Rejected,
            "TASK_STATE_AUTH_REQUIRED" => TaskState::AuthRequired,
            "TASK_STATE_UNSPECIFIED" => TaskState::Unknown,
            other => return Err(D::Error::custom(format!("unknown task state `{other}`"))),
        })
    }
}

/// One A2A `Part`, carrying text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Part {
    pub text: String,
}

/// An A2A `Artifact`: the answer's text as one part, named by its record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Artifact {
    #[serde(rename = "artifactId")]
    pub artifact_id: String,
    pub parts: Vec<Part>,
}

/// An A2A `Task`. `context_id` is the client's `message.contextId`, or the
/// task's own conversation when the client sent none.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: String,
    #[serde(rename = "contextId")]
    pub context_id: String,
    pub status: TaskStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<Artifact>,
}

/// A JSON-RPC request envelope.
#[derive(Debug, Deserialize)]
pub struct JsonRpcRequest {
    #[serde(default)]
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

// A request refused with its JSON-RPC or A2A error code and a fixed public
// message.
enum A2aFault {
    InvalidRequest,
    MethodNotFound,
    InvalidParams,
    TaskNotFound,
    TaskNotCancelable,
    UnsupportedOperation(&'static str),
    Laser(LaserError),
}

impl A2aFault {
    fn rpc_error(&self) -> JsonRpcError {
        let (code, message) = match self {
            Self::InvalidRequest => (INVALID_REQUEST, "invalid request"),
            Self::MethodNotFound => (METHOD_NOT_FOUND, "method not found"),
            Self::InvalidParams => (INVALID_PARAMS, "invalid params"),
            Self::TaskNotFound => (TASK_NOT_FOUND, "task not found"),
            Self::TaskNotCancelable => (TASK_NOT_CANCELABLE, "task cannot be canceled"),
            Self::UnsupportedOperation(message) => (UNSUPPORTED_OPERATION, *message),
            Self::Laser(error) if error.code() == ResultCode::InvalidArgument => {
                (INVALID_PARAMS, "invalid params")
            }
            Self::Laser(error) => (INTERNAL_ERROR, crate::error::public_error_message(error)),
        };
        JsonRpcError {
            code,
            message: message.to_owned(),
        }
    }
}

impl From<LaserError> for A2aFault {
    fn from(error: LaserError) -> Self {
        Self::Laser(error)
    }
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
/// becomes the one text artifact, named by the answer's record id.
pub fn task_from_envelope(
    task_id: impl Into<String>,
    context_id: impl Into<String>,
    envelope: &AgentEnvelope,
) -> Task {
    let id = task_id.into();
    let artifacts = if envelope.body.is_empty() {
        Vec::new()
    } else {
        vec![Artifact {
            artifact_id: envelope
                .record
                .map_or_else(|| format!("{id}-artifact"), |record| record.to_string()),
            parts: vec![Part {
                text: String::from_utf8_lossy(&envelope.body).into_owned(),
            }],
        }]
    };
    Task {
        id,
        context_id: context_id.into(),
        status: TaskStatus {
            state: answer_state(envelope),
        },
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
    // The `bridge_hops` path stamped on every record the bridge publishes,
    // ending in its own id.
    hops: Vec<String>,
    /// When set, every record the bridge publishes (the task command and the
    /// cancel) is signed, so a verifying consumer can authorize it against the
    /// enrolled key. Requires the `sign` feature.
    #[cfg(feature = "sign")]
    signing_key: Option<crate::sign::SigningKey>,
    /// When set, every task id is bound to the caller that submitted it.
    task_binding: Option<TaskBinding>,
    // The agent every task that names no target is addressed to.
    target: Option<AgentId>,
    // The card's endpoint, description and agent version.
    public_url: Option<String>,
    description: String,
    version: String,
    // The browser origins the HTTP router accepts besides same-origin pages
    // on an IP literal or `localhost` host.
    #[cfg(feature = "a2a-http")]
    allowed_origins: Vec<String>,
}

/// The key and lifetime task ids are bound with.
#[derive(Clone)]
struct TaskBinding {
    key: [u8; 32],
    ttl: std::time::Duration,
}

impl TaskBinding {
    /// The keyed digest a bound task id carries: the caller, the stream
    /// generation, the conversation and the expiry, so an id works only for
    /// its caller, on the stream incarnation it was made on, until it expires.
    fn mac(
        &self,
        caller: &str,
        stream_generation: u64,
        task: ConversationId,
        expires: u64,
    ) -> blake3::Hash {
        let mut hasher = blake3::Hasher::new_keyed(&self.key);
        hasher.update(b"laserdata a2a task v1\0");
        hasher.update(&(caller.len() as u64).to_le_bytes());
        hasher.update(caller.as_bytes());
        hasher.update(&stream_generation.to_le_bytes());
        hasher.update(&task.as_u128().to_le_bytes());
        hasher.update(&expires.to_le_bytes());
        hasher.finalize()
    }
}

/// The authenticated caller of an A2A HTTP request. A host that binds tasks
/// to callers inserts it into the request extensions from its own
/// authentication, and the router passes it to the bridge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct A2aCallerPrincipal(pub String);

/// The bridge acting for one authenticated caller. Build it with
/// [`A2aBridge::caller`].
pub struct A2aCaller<'a> {
    bridge: &'a A2aBridge,
    principal: String,
}

impl A2aCaller<'_> {
    /// [`A2aBridge::submit`] for this caller.
    pub async fn submit(&self, params_json: Vec<u8>) -> Result<Task, LaserError> {
        self.bridge
            .submit_addressed(
                None,
                None,
                params_json,
                Some(&self.principal),
                &self.bridge.hops,
            )
            .await
    }

    /// [`A2aBridge::submit_with_hops`] for this caller.
    pub async fn submit_with_hops(
        &self,
        previous: &[String],
        target: Option<AgentId>,
        params_json: Vec<u8>,
    ) -> Result<Task, LaserError> {
        let hops = call_hops(&self.bridge.hops, previous)?;
        self.bridge
            .submit_addressed(target, None, params_json, Some(&self.principal), &hops)
            .await
    }

    /// [`A2aBridge::submit_to`] for this caller.
    pub async fn submit_to(
        &self,
        target: impl Into<AgentId>,
        params_json: Vec<u8>,
    ) -> Result<Task, LaserError> {
        self.bridge
            .submit_addressed(
                Some(target.into()),
                None,
                params_json,
                Some(&self.principal),
                &self.bridge.hops,
            )
            .await
    }

    /// [`A2aBridge::submit_in`] for this caller.
    pub async fn submit_in(
        &self,
        parent: ConversationId,
        root: ConversationId,
        params_json: Vec<u8>,
    ) -> Result<Task, LaserError> {
        self.bridge
            .submit_addressed(
                None,
                Some((parent, root)),
                params_json,
                Some(&self.principal),
                &self.bridge.hops,
            )
            .await
    }

    /// [`A2aBridge::submit_in_to`] for this caller.
    pub async fn submit_in_to(
        &self,
        target: impl Into<AgentId>,
        parent: ConversationId,
        root: ConversationId,
        params_json: Vec<u8>,
    ) -> Result<Task, LaserError> {
        self.bridge
            .submit_addressed(
                Some(target.into()),
                Some((parent, root)),
                params_json,
                Some(&self.principal),
                &self.bridge.hops,
            )
            .await
    }

    /// [`A2aBridge::task`] for this caller. Another caller's task id is
    /// refused like an unknown one.
    pub async fn task(&self, id: &str) -> Result<Task, LaserError> {
        self.bridge.task_for(id, Some(&self.principal)).await
    }

    /// [`A2aBridge::cancel`] for this caller. Another caller's task id is
    /// refused before anything is published.
    pub async fn cancel(&self, id: &str) -> Result<Task, LaserError> {
        self.bridge.cancel_for(id, Some(&self.principal)).await
    }

    /// [`A2aBridge::handle_rpc`] for this caller.
    pub async fn handle_rpc(&self, request: JsonValue) -> JsonRpcResponse {
        self.bridge
            .handle_rpc_for(request, Some(&self.principal))
            .await
    }
}

impl A2aBridge {
    /// A bridge mapping A2A JSON-RPC methods onto agent topics over `laser`,
    /// publishing as `source` (the bridge's agent id). Every topic lives on the
    /// stream of `laser`. Pass `laser.with_default_stream(stream)` to run the bridge on a
    /// non-default stream (the unit of multi-stream topologies and per-stream
    /// Iggy RBAC). `request_topic`/`reply_topic` are any [`AgentTopic`], including
    /// `AgentTopic::Custom` for an arbitrary name.
    pub fn new(
        laser: Laser,
        source: impl Into<AgentId>,
        request_topic: AgentTopic<'static>,
        reply_topic: AgentTopic<'static>,
    ) -> Self {
        let source = source.into();
        let hops = vec![source.as_str().to_owned()];
        Self {
            laser,
            source,
            request_topic,
            reply_topic,
            capabilities: Vec::new(),
            hops,
            #[cfg(feature = "sign")]
            signing_key: None,
            task_binding: None,
            target: None,
            public_url: None,
            description: DEFAULT_CARD_DESCRIPTION.to_owned(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            #[cfg(feature = "a2a-http")]
            allowed_origins: Vec::new(),
        }
    }

    /// The browser origins the HTTP router's JSON-RPC endpoint accepts, each
    /// as the exact `Origin` header value (`https://app.example.com`). A
    /// request without an `Origin` header, a non-browser client, is always
    /// accepted, and so is a same-origin page on an IP literal or `localhost`
    /// host. Any other browser request is refused with 403, the DNS rebinding
    /// guard. Requires the `a2a-http` feature.
    #[cfg(feature = "a2a-http")]
    #[must_use]
    pub fn with_allowed_origins(mut self, origins: Vec<String>) -> Self {
        self.allowed_origins = origins;
        self
    }

    /// Bind every task id to the caller that submitted it, keyed with `key`
    /// and valid for `ttl`. A bound id names the caller, the stream
    /// incarnation, the conversation and an expiry under a keyed digest, so
    /// another caller cannot read or cancel the task. Once set, task
    /// operations need a caller: use [`caller`](Self::caller), and the HTTP
    /// router reads [`A2aCallerPrincipal`] from the request extensions.
    #[must_use]
    pub fn with_task_binding(mut self, key: [u8; 32], ttl: std::time::Duration) -> Self {
        self.task_binding = Some(TaskBinding { key, ttl });
        self
    }

    /// Address every task that names no target to `agent`, the agent this
    /// bridge fronts. Its answer must then be signed by `agent` when the
    /// `Laser` verifies replies.
    #[must_use]
    pub fn with_target(mut self, agent: impl Into<AgentId>) -> Self {
        self.target = Some(agent.into());
        self
    }

    /// The absolute `http` or `https` URL A2A clients reach the JSON-RPC
    /// endpoint at, which the card advertises. The card cannot be built
    /// without it. Anything else is refused with [`LaserError::Invalid`].
    pub fn with_public_url(mut self, url: impl Into<String>) -> Result<Self, LaserError> {
        let url = url.into();
        let absolute = ["https://", "http://"].iter().any(|scheme| {
            url.strip_prefix(scheme)
                .is_some_and(|rest| !rest.is_empty() && !rest.starts_with('/'))
        });
        if !absolute {
            return Err(LaserError::Invalid(
                "the A2A card URL must be an absolute http or https URL".to_owned(),
            ));
        }
        self.public_url = Some(url);
        Ok(self)
    }

    /// The card's description.
    #[must_use]
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    /// The card's agent version, the SDK version by default.
    #[must_use]
    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.version = version.into();
        self
    }

    /// The bridge acting for one authenticated caller.
    pub fn caller(&self, principal: impl Into<String>) -> A2aCaller<'_> {
        A2aCaller {
            bridge: self,
            principal: principal.into(),
        }
    }

    /// The creation time of the bridge's stream, which tells a recreated
    /// stream apart from the one a task id was made on.
    async fn stream_generation(&self) -> Result<u64, LaserError> {
        let stream = Identifier::named(self.laser.stream_required()?)?;
        let details = StreamClient::get_stream(self.laser.client().as_ref(), &stream)
            .await?
            .ok_or_else(|| {
                LaserError::HandlerConfig("the bridge stream does not exist".to_owned())
            })?;
        Ok(details.created_at.as_micros())
    }

    /// The id a new task is known by.
    async fn task_id(
        &self,
        task: ConversationId,
        caller: Option<&str>,
    ) -> Result<String, LaserError> {
        let Some(binding) = &self.task_binding else {
            return Ok(task.to_string());
        };
        let caller = caller.ok_or_else(unbound_caller)?;
        let generation = self.stream_generation().await?;
        let expires =
            now_micros().saturating_add(u64::try_from(binding.ttl.as_micros()).unwrap_or(u64::MAX));
        let mac = binding.mac(caller, generation, task, expires);
        Ok(format!("{task}.{expires}.{}", mac.to_hex()))
    }

    /// The conversation a task id names, refused like a malformed id when it
    /// is not this caller's, expired, or from another stream incarnation.
    async fn open_task(
        &self,
        id: &str,
        caller: Option<&str>,
    ) -> Result<ConversationId, LaserError> {
        let invalid = || LaserError::HandlerConfig(format!("invalid task id `{id}`"));
        let Some(binding) = &self.task_binding else {
            return id.parse::<ConversationId>().map_err(|_| invalid());
        };
        let caller = caller.ok_or_else(unbound_caller)?;
        let mut parts = id.split('.');
        let (Some(task), Some(expires), Some(mac), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(invalid());
        };
        let task = task.parse::<ConversationId>().map_err(|_| invalid())?;
        let expires = expires.parse::<u64>().map_err(|_| invalid())?;
        let mac = blake3::Hash::from_hex(mac).map_err(|_| invalid())?;
        if expires < now_micros() {
            return Err(invalid());
        }
        let generation = self.stream_generation().await?;
        // `blake3::Hash` compares in constant time.
        if binding.mac(caller, generation, task, expires) != mac {
            return Err(invalid());
        }
        Ok(task)
    }

    // `open_task` for a remote request: an id this caller cannot open is a
    // task that does not exist for it.
    async fn open_requested_task(
        &self,
        id: &str,
        caller: Option<&str>,
    ) -> Result<ConversationId, A2aFault> {
        if self.task_binding.is_some() && caller.is_none() {
            return Err(unbound_caller().into());
        }
        self.open_task(id, caller)
            .await
            .map_err(|error| match error {
                LaserError::HandlerConfig(_) => A2aFault::TaskNotFound,
                other => A2aFault::Laser(other),
            })
    }

    /// Continue the loop-guard path of work that already crossed other
    /// bridges: `previous` is its `bridge_hops`, and this bridge's id is
    /// appended to it. Every record the bridge publishes carries the path.
    /// Refuses a path that already holds this bridge, the loop the guard
    /// exists to stop.
    pub fn with_bridge_hops(mut self, previous: &[String]) -> Result<Self, LaserError> {
        self.hops = enter_bridge(self.source.as_str(), previous)?;
        Ok(self)
    }

    /// Sign every record the bridge publishes, the task command and the
    /// cancel, with `key`, so a verifying consumer authorizes them against the
    /// enrolled key registry. Without it a consumer with a verifier
    /// dead-letters every task this bridge submits. Requires the `sign`
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

    /// `SendMessage`: publish the params as a typed AGDX `command` on a fresh
    /// task conversation, tunneling the foreign JSON byte-identical in the body
    /// (`agdx.ct = json`), and returns Submitted. The task id is the conversation, and
    /// the A2A task identity rides `correlation` (derived from it, so the lookup
    /// stays stateless). The command is addressed to the
    /// [`with_target`](Self::with_target) agent, else to every agent
    /// (`agdx.to = *`), so on a shared session topic every listening agent
    /// receives it. Use [`submit_to`](Self::submit_to) to address one agent.
    pub async fn submit(&self, params_json: Vec<u8>) -> Result<Task, LaserError> {
        self.submit_addressed(None, None, params_json, None, &self.hops)
            .await
    }

    /// [`submit`](Self::submit) for work that already crossed other bridges:
    /// `previous` is its `bridge_hops`, and the task carries that path with
    /// this bridge appended. A path that already holds this bridge is refused
    /// before anything is published. `target` addresses one agent, as in
    /// [`submit_to`](Self::submit_to).
    pub async fn submit_with_hops(
        &self,
        previous: &[String],
        target: Option<AgentId>,
        params_json: Vec<u8>,
    ) -> Result<Task, LaserError> {
        let hops = call_hops(&self.hops, previous)?;
        self.submit_addressed(target, None, params_json, None, &hops)
            .await
    }

    /// [`submit`](Self::submit) addressed to `target`, so only that agent
    /// handles the task on a shared session topic.
    pub async fn submit_to(
        &self,
        target: impl Into<AgentId>,
        params_json: Vec<u8>,
    ) -> Result<Task, LaserError> {
        let target = target.into();
        self.submit_addressed(Some(target), None, params_json, None, &self.hops)
            .await
    }

    /// `SendMessage` as a child session of `parent`, in the tree rooted at
    /// `root`: the task's submitted start lands on `agent.sessions` with the
    /// ancestry before the command, and the command carries it too. The
    /// agent that handles the task ends the child. The command is addressed
    /// like [`submit`](Self::submit).
    pub async fn submit_in(
        &self,
        parent: ConversationId,
        root: ConversationId,
        params_json: Vec<u8>,
    ) -> Result<Task, LaserError> {
        self.submit_addressed(None, Some((parent, root)), params_json, None, &self.hops)
            .await
    }

    /// [`submit_in`](Self::submit_in) addressed to `target`.
    pub async fn submit_in_to(
        &self,
        target: impl Into<AgentId>,
        parent: ConversationId,
        root: ConversationId,
        params_json: Vec<u8>,
    ) -> Result<Task, LaserError> {
        let target = target.into();
        self.submit_addressed(
            Some(target),
            Some((parent, root)),
            params_json,
            None,
            &self.hops,
        )
        .await
    }

    async fn submit_addressed(
        &self,
        target: Option<AgentId>,
        ancestry: Option<(ConversationId, ConversationId)>,
        params_json: Vec<u8>,
        caller: Option<&str>,
        hops: &[String],
    ) -> Result<Task, LaserError> {
        let task = ConversationId::new();
        // Minted before anything is published, so an unbound caller on a
        // binding bridge publishes nothing.
        let id = self.task_id(task, caller).await?;
        if let Some((parent, root)) = ancestry {
            crate::agent::contract::start_child(
                &self.laser,
                &self.source,
                &self.source,
                task.into(),
                parent.into(),
                root.into(),
            )
            .await?;
        }
        let context_id = context_id_of(&params_json, task);
        self.publish_command(
            task,
            params_json,
            target.or_else(|| self.target.clone()),
            ancestry.map(|(parent, root)| (Some(parent.into()), Some(root.into()))),
            hops,
        )
        .await?;
        Ok(state_task(id, context_id, TaskState::Submitted))
    }

    async fn publish_command(
        &self,
        task: ConversationId,
        params_json: Vec<u8>,
        target: Option<AgentId>,
        ancestry: Option<(Option<agdx::ConversationId>, Option<agdx::ConversationId>)>,
        hops: &[String],
    ) -> Result<(), LaserError> {
        let producer =
            self.laser
                .agdx(self.request_topic.clone(), self.source.clone(), task.into());
        let mut command = producer
            .command(correlation_of(task), params_json)
            .with_operation(OPERATION_CHAT)
            .with_metadata(METADATA_BRIDGE_HOPS, hops_metadata(hops))
            .content_type(ContentType::Json);
        if let Some((parent, root)) = ancestry {
            command = command.with_ancestry(parent, root);
        }
        if let Some(target) = target {
            command = command.with_target(target);
        }
        #[cfg(feature = "sign")]
        if let Some(key) = &self.signing_key {
            command = command.signed_by(key);
        }
        command.send().await?;
        Ok(())
    }

    /// `GetTask`: the task's current state, read forward from the time its id
    /// was minted. Working until an answer lands, and Unknown when the bounded
    /// scan could not reach the end of the topic. An id this bridge published
    /// no task for is refused with [`LaserError::HandlerConfig`].
    pub async fn task(&self, id: &str) -> Result<Task, LaserError> {
        self.task_for(id, None).await
    }

    async fn task_for(&self, id: &str, caller: Option<&str>) -> Result<Task, LaserError> {
        let conversation = self.open_task(id, caller).await?;
        self.view(conversation, id)
            .await?
            .map(|view| view.task)
            .ok_or_else(|| unknown_task(id))
    }

    /// `CancelTask`: publish an AGDX `error` terminal (code `Cancelled`,
    /// `task_state = Canceled`) correlated to a task still in progress, so a
    /// later `GetTask` also reports Canceled, and returns the canceled task. An
    /// unknown task is refused with [`LaserError::HandlerConfig`], one that
    /// already ended with [`LaserError::Rejected`].
    pub async fn cancel(&self, id: &str) -> Result<Task, LaserError> {
        self.cancel_for(id, None).await
    }

    async fn cancel_for(&self, id: &str, caller: Option<&str>) -> Result<Task, LaserError> {
        // Checked before the cancel is published, so a foreign id has no effect.
        let conversation = self.open_task(id, caller).await?;
        let view = self
            .view(conversation, id)
            .await?
            .ok_or_else(|| unknown_task(id))?;
        if view.terminal {
            return Err(LaserError::Rejected("the task already ended".to_owned()));
        }
        self.publish_cancel(conversation, id, view.task.context_id)
            .await
    }

    async fn publish_cancel(
        &self,
        conversation: ConversationId,
        id: &str,
        context_id: String,
    ) -> Result<Task, LaserError> {
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
            .with_task_state(TaskState::Canceled)
            .with_metadata(METADATA_BRIDGE_HOPS, hops_metadata(&self.hops));
        // Sign the cancel when a key is configured, so a verifying consumer can
        // authorize it. Without a key it rides unsigned, and a topic that
        // mandates verification rejects it.
        #[cfg(feature = "sign")]
        let send = match &self.signing_key {
            Some(key) => send.signed_by(key),
            None => send,
        };
        send.send().await?;
        Ok(state_task(id.to_owned(), context_id, TaskState::Canceled))
    }

    // What the log says about the task: `None` when this bridge published no
    // command for it and nothing answered it. A bound id proves the bridge
    // minted it, so it exists even when its command has left retention.
    async fn view(
        &self,
        conversation: ConversationId,
        id: &str,
    ) -> Result<Option<TaskView>, LaserError> {
        let records = self.scan_task(conversation).await?;
        if records.command.is_none()
            && records.answers.is_empty()
            && records.complete
            && self.task_binding.is_none()
        {
            return Ok(None);
        }
        let context_id = records.command.as_ref().map_or_else(
            || conversation.to_string(),
            |command| context_id_of(&command.body, conversation),
        );
        let target = records
            .command
            .as_ref()
            .and_then(|command| command.target.clone())
            .or_else(|| self.target.clone());
        let accepted: Vec<&AgentEnvelope> = records
            .answers
            .iter()
            .filter(|answer| self.accepts(&answer.signer, target.as_ref()))
            .map(|answer| &answer.envelope)
            .collect();
        // The first terminal answer settles the task, else the latest one in
        // progress (an input-required answer superseded by a later reply).
        let settled = accepted
            .iter()
            .find(|envelope| answer_state(envelope).is_terminal())
            .or_else(|| accepted.last());
        let view = match settled {
            Some(envelope) => TaskView {
                task: task_from_envelope(id, context_id, envelope),
                terminal: answer_state(envelope).is_terminal(),
                command: records.command,
            },
            // The bounded scan did not reach the end of the topic, so the task
            // may already be answered. Unknown asks the caller to look again.
            None => TaskView {
                task: state_task(
                    id.to_owned(),
                    context_id,
                    if records.complete {
                        TaskState::Working
                    } else {
                        TaskState::Unknown
                    },
                ),
                terminal: false,
                command: records.command,
            },
        };
        Ok(Some(view))
    }

    // Whether an answer may settle the task: always without a verifier, else
    // only when it verifies and comes from the task's target, or is this
    // bridge's own signed cancel.
    fn accepts(&self, signer: &Signer, target: Option<&AgentId>) -> bool {
        match signer {
            Signer::Unchecked => true,
            Signer::Refused => false,
            Signer::Verified { principal, key_id } => {
                target.is_none_or(|target| target.as_str() == principal) || self.own_key(key_id)
            }
        }
    }

    #[cfg(feature = "sign")]
    fn own_key(&self, key_id: &[u8]) -> bool {
        self.signing_key
            .as_ref()
            .is_some_and(|key| key.key_id() == key_id)
    }

    #[cfg(not(feature = "sign"))]
    fn own_key(&self, _key_id: &[u8]) -> bool {
        false
    }

    // Whether a command is this bridge's own: its source, and with a signing
    // key a signature by that key that verifies when the `Laser` verifies.
    fn authored(&self, envelope: &AgentEnvelope, signer: &Signer) -> bool {
        if envelope.source != self.source {
            return false;
        }
        #[cfg(feature = "sign")]
        if let Some(key) = &self.signing_key {
            return envelope
                .signature
                .as_ref()
                .is_some_and(|signature| signature.key_id == key.key_id())
                && !matches!(signer, Signer::Refused);
        }
        let _ = signer;
        true
    }

    // The bridge's command for `task` and every answer to it, read forward
    // from the time the task id was minted rather than from offset zero.
    async fn scan_task(&self, task: ConversationId) -> Result<TaskRecords, LaserError> {
        let correlation = correlation_of(task);
        let minted_micros = u64::try_from(task.as_u128() >> 80)
            .map_or(u64::MAX, |millis| millis.saturating_mul(1_000));
        let since = minted_micros
            .saturating_sub(u64::try_from(LOOKUP_CLOCK_SLACK.as_micros()).unwrap_or(u64::MAX));
        let mut records = TaskRecords {
            command: None,
            answers: Vec::new(),
            complete: true,
        };
        let same_topic = self.request_topic.topic_string() == self.reply_topic.topic_string();
        self.scan_topic(&self.reply_topic, since, correlation, &mut records)
            .await?;
        if !same_topic {
            self.scan_topic(&self.request_topic, since, correlation, &mut records)
                .await?;
        }
        Ok(records)
    }

    async fn scan_topic(
        &self,
        topic: &AgentTopic<'static>,
        since: u64,
        correlation: CorrelationId,
        records: &mut TaskRecords,
    ) -> Result<(), LaserError> {
        let client = self.laser.client();
        let stream = Identifier::named(self.laser.stream_required()?)?;
        let topic = topic.as_identifier();
        let Some(details) = client.get_topic(&stream, &topic).await? else {
            return Ok(());
        };
        let consumer = Consumer::new(Identifier::named(LOOKUP_READER)?);
        for partition in 0..crate::poll::bounded_partitions(details.partitions_count) {
            let first = client
                .poll_messages(
                    &stream,
                    &topic,
                    partition,
                    &consumer,
                    &PollingStrategy::timestamp(IggyTimestamp::from(since)),
                    PollOptions {
                        count: 1,
                        ..PollOptions::default()
                    },
                )
                .await?;
            let Some(mut offset) = first.messages.first().map(|message| message.header.offset)
            else {
                continue;
            };
            let mut caught_up = false;
            for _ in 0..MAX_LOOKUP_PASSES {
                let batch = crate::poll::drain_partition(
                    &client,
                    &stream,
                    &topic,
                    &consumer,
                    DrainRange::open(partition, offset),
                    LOOKUP_BATCH,
                )
                .await?;
                if batch.messages.is_empty() {
                    caught_up = true;
                    break;
                }
                offset = batch.next_offset;
                for message in &batch.messages {
                    self.collect(message, correlation, records);
                }
            }
            records.complete &= caught_up;
        }
        Ok(())
    }

    fn collect(
        &self,
        message: &IggyMessage,
        correlation: CorrelationId,
        records: &mut TaskRecords,
    ) {
        let Ok(decoded) =
            crate::agent::decode_agent_record(message, laser_wire::agent::features::NONE)
        else {
            return;
        };
        let Some(envelope) = decoded.envelope else {
            return;
        };
        if envelope.correlation != Some(correlation) {
            return;
        }
        let signer = self.signer_of(
            &envelope,
            decoded.signature_context.as_ref(),
            decoded.observed_at_micros,
        );
        match envelope.kind {
            AgentKind::Command => {
                if records.command.is_none() && self.authored(&envelope, &signer) {
                    records.command = Some(envelope);
                }
            }
            AgentKind::Response | AgentKind::Error => {
                records.answers.push(Answer { envelope, signer });
            }
            _ => {}
        }
    }

    // Who an envelope verifies to under the `Laser`'s verifier: unchecked
    // without one, refused when it does not verify.
    #[cfg(feature = "sign")]
    fn signer_of(
        &self,
        envelope: &AgentEnvelope,
        context: Option<&agdx::SignatureContext>,
        observed_at_micros: u64,
    ) -> Signer {
        let Some(registry) = self.laser.registry_verifier() else {
            return Signer::Unchecked;
        };
        match (context, &envelope.signature) {
            (Some(context), Some(signature)) => registry
                .verify_observed_at(envelope, context, observed_at_micros)
                .map_or(Signer::Refused, |verified| Signer::Verified {
                    principal: verified.principal,
                    key_id: signature.key_id.clone(),
                }),
            _ => Signer::Refused,
        }
    }

    #[cfg(not(feature = "sign"))]
    fn signer_of(
        &self,
        _envelope: &AgentEnvelope,
        _context: Option<&agdx::SignatureContext>,
        _observed_at_micros: u64,
    ) -> Signer {
        Signer::Unchecked
    }

    /// The bridge's A2A Agent Card, for discovery. Streaming is not
    /// advertised: AGDX chunk channels ride the log, not SSE. Each advertised
    /// [`CapabilityDescriptor`] (set via [`with_capabilities`](Self::with_capabilities))
    /// projects to an A2A `AgentSkill`: the skill id is its id and name, and the
    /// I/O `ContentRef`s become the skill's input and output modes. Fails with
    /// [`LaserError::HandlerConfig`] until [`with_public_url`](Self::with_public_url)
    /// names the endpoint.
    pub fn card(&self) -> Result<AgentCard, LaserError> {
        let url = self.public_url.clone().ok_or_else(|| {
            LaserError::HandlerConfig(
                "the A2A card needs a public URL, set it with with_public_url".to_owned(),
            )
        })?;
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
        Ok(AgentCard {
            name: self.source.as_str().to_owned(),
            description: self.description.clone(),
            version: self.version.clone(),
            supported_interfaces: vec![AgentInterface {
                url,
                protocol_binding: A2A_JSONRPC_BINDING.to_owned(),
                protocol_version: A2A_PROTOCOL_VERSION.to_owned(),
            }],
            capabilities: AgentCardCapabilities {
                streaming: false,
                push_notifications: false,
                state_transition_history: false,
                extended_agent_card: false,
            },
            default_input_modes: vec!["text/plain".to_owned()],
            default_output_modes: vec!["text/plain".to_owned()],
            skills,
            signatures: Vec::new(),
        })
    }

    /// The card with a detached JWS signature attached (`sign` feature): the
    /// JCS-canonicalized card (RFC 8785, minus `signatures`) signed EdDSA with
    /// the same enrolled Ed25519 key the envelope scheme uses, so one principal
    /// key covers both surfaces. Verify with
    /// [`verify_card`](crate::sign::verify_card).
    #[cfg(feature = "sign")]
    pub fn signed_card(&self, key: &crate::sign::SigningKey) -> Result<AgentCard, LaserError> {
        let mut card = self.card()?;
        let value = serde_json::to_value(&card)
            .map_err(|error| LaserError::Codec(format!("card to JSON: {error}")))?;
        let signature = crate::sign::sign_card_value(key, &value)?;
        card.signatures.push(signature);
        Ok(card)
    }

    // The card the well-known path serves: signed with the bridge's key when
    // one is configured.
    #[cfg(feature = "a2a-http")]
    fn served_card(&self) -> Result<AgentCard, LaserError> {
        #[cfg(feature = "sign")]
        if let Some(key) = &self.signing_key {
            return self.signed_card(key);
        }
        self.card()
    }

    /// Handle one A2A JSON-RPC request. Failures answer the JSON-RPC codes
    /// (-32600 invalid request, -32601 method not found, -32602 invalid params,
    /// -32603 internal error) and the A2A ones (-32001 task not found, -32002
    /// task not cancelable, -32004 unsupported operation).
    pub async fn handle_rpc(&self, request: JsonValue) -> JsonRpcResponse {
        self.handle_rpc_for(request, None).await
    }

    async fn handle_rpc_for(&self, request: JsonValue, caller: Option<&str>) -> JsonRpcResponse {
        let id = request.get("id").cloned().unwrap_or(JsonValue::Null);
        match self.dispatch_rpc(&request, caller).await {
            Ok(result) => JsonRpcResponse {
                jsonrpc: JSONRPC_VERSION,
                id,
                result: Some(result),
                error: None,
            },
            Err(fault) => {
                if let A2aFault::Laser(error) = &fault {
                    tracing::warn!(error = %error, "A2A request failed");
                }
                JsonRpcResponse {
                    jsonrpc: JSONRPC_VERSION,
                    id,
                    result: None,
                    error: Some(fault.rpc_error()),
                }
            }
        }
    }

    async fn dispatch_rpc(
        &self,
        request: &JsonValue,
        caller: Option<&str>,
    ) -> Result<JsonValue, A2aFault> {
        // Serde also reads a struct from an array, so the object check comes first.
        if !request.is_object() {
            return Err(A2aFault::InvalidRequest);
        }
        let JsonRpcRequest { method, params, .. } =
            JsonRpcRequest::deserialize(request).map_err(|_| A2aFault::InvalidRequest)?;
        let params = if params.is_null() {
            JsonValue::Object(Default::default())
        } else {
            params
        };
        if !params.is_object() {
            return Err(A2aFault::InvalidParams);
        }
        let render = |task: Task| {
            to_value(task).map_err(|error| A2aFault::Laser(LaserError::Codec(error.to_string())))
        };
        match A2aMethod::from_str(&method) {
            Ok(A2aMethod::MessageSend) => {
                let task = self.send_message(&params, caller).await?;
                Ok(serde_json::json!({ "task": render(task)? }))
            }
            Ok(A2aMethod::MessageStream) => {
                Err(A2aFault::UnsupportedOperation("streaming is not supported"))
            }
            Ok(operation @ (A2aMethod::TasksGet | A2aMethod::TasksCancel)) => {
                let id = params
                    .get("id")
                    .and_then(JsonValue::as_str)
                    .ok_or(A2aFault::InvalidParams)?;
                let conversation = self.open_requested_task(id, caller).await?;
                let view = self
                    .view(conversation, id)
                    .await?
                    .ok_or(A2aFault::TaskNotFound)?;
                if operation == A2aMethod::TasksGet {
                    return render(view.task);
                }
                if view.terminal {
                    return Err(A2aFault::TaskNotCancelable);
                }
                render(
                    self.publish_cancel(conversation, id, view.task.context_id)
                        .await?,
                )
            }
            Err(_) => Err(A2aFault::MethodNotFound),
        }
    }

    // `SendMessage`: a message naming a task continues it in its conversation,
    // addressed to the agent the task went to. Any other message starts a task.
    async fn send_message(
        &self,
        params: &JsonValue,
        caller: Option<&str>,
    ) -> Result<Task, A2aFault> {
        let message = params
            .get("message")
            .filter(|message| message.is_object())
            .ok_or(A2aFault::InvalidParams)?;
        let previous = inbound_hops(params.get("metadata"))?;
        let hops = call_hops(&self.hops, &previous)?;
        let body =
            serde_json::to_vec(params).map_err(|error| LaserError::Codec(error.to_string()))?;
        let Some(task_id) = message.get("taskId") else {
            return Ok(self
                .submit_addressed(None, None, body, caller, &hops)
                .await?);
        };
        let task_id = task_id.as_str().ok_or(A2aFault::InvalidParams)?;
        let conversation = self.open_requested_task(task_id, caller).await?;
        let view = self
            .view(conversation, task_id)
            .await?
            .ok_or(A2aFault::TaskNotFound)?;
        if view.terminal {
            return Err(A2aFault::UnsupportedOperation(
                "the task ended and takes no further message",
            ));
        }
        let (target, ancestry) = match &view.command {
            Some(command) => (
                command.target.clone().or_else(|| self.target.clone()),
                Some((command.parent, command.root)),
            ),
            None => (self.target.clone(), None),
        };
        self.publish_command(conversation, body, target, ancestry, &hops)
            .await?;
        Ok(state_task(
            task_id.to_owned(),
            view.task.context_id,
            TaskState::Working,
        ))
    }

    /// An axum router: the JSON-RPC endpoint at `/` plus the Agent Card at the
    /// A2A well-known discovery path, signed with the bridge's key when one is
    /// configured. The card path answers 404 until
    /// [`with_public_url`](Self::with_public_url) names the endpoint. The
    /// JSON-RPC endpoint refuses a browser origin outside
    /// [`with_allowed_origins`](Self::with_allowed_origins) with 403, the card is
    /// public discovery and served to any origin. Requires
    /// the `a2a-http` feature. The bridge adapter (`submit` / `task` /
    /// `cancel` / `card`) is usable without it.
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

// What the log says about a task: its settled state, whether that state ended
// it, and the bridge's command when it is still retained.
struct TaskView {
    task: Task,
    terminal: bool,
    command: Option<AgentEnvelope>,
}

struct TaskRecords {
    command: Option<AgentEnvelope>,
    answers: Vec<Answer>,
    complete: bool,
}

struct Answer {
    envelope: AgentEnvelope,
    signer: Signer,
}

// Who an envelope verifies to under the `Laser`'s verifier.
#[cfg_attr(not(feature = "sign"), allow(dead_code))]
enum Signer {
    Unchecked,
    Refused,
    Verified { principal: String, key_id: Vec<u8> },
}

#[cfg(feature = "a2a-http")]
async fn handle_card(State(bridge): State<Arc<A2aBridge>>) -> Response {
    match bridge.served_card() {
        Ok(card) => axum::Json(card).into_response(),
        Err(error) => {
            tracing::warn!(error = %error, "A2A card is not served");
            StatusCode::NOT_FOUND.into_response()
        }
    }
}

// The A2A task identity rides AGDX `correlation`, and deriving it from the task's
// conversation keeps the bridge stateless: `tasks/get` reconstructs it from
// the id alone with no mapping table.
fn correlation_of(conversation: ConversationId) -> CorrelationId {
    CorrelationId::from_u128(conversation.as_u128())
}

#[cfg(feature = "a2a-http")]
async fn handle_rpc(
    State(bridge): State<Arc<A2aBridge>>,
    principal: Option<axum::Extension<A2aCallerPrincipal>>,
    headers: HeaderMap,
    axum::Json(request): axum::Json<JsonValue>,
) -> Response {
    if !origin_allowed(&headers, &bridge.allowed_origins) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let caller = principal.as_ref().map(|principal| principal.0.0.as_str());
    axum::Json(bridge.handle_rpc_for(request, caller).await).into_response()
}

fn now_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX)
        })
}

// The state an answer settles: its `task_state`, else completed for a
// response and failed for an error.
fn answer_state(envelope: &AgentEnvelope) -> TaskState {
    envelope.task_state.unwrap_or(match envelope.kind {
        AgentKind::Error => TaskState::Failed,
        _ => TaskState::Completed,
    })
}

fn state_task(id: String, context_id: String, state: TaskState) -> Task {
    Task {
        id,
        context_id,
        status: TaskStatus { state },
        artifacts: Vec::new(),
    }
}

// The context a task belongs to: the `message.contextId` of the params it was
// submitted with, else the task's own conversation.
fn context_id_of(params_json: &[u8], task: ConversationId) -> String {
    serde_json::from_slice::<JsonValue>(params_json)
        .ok()
        .and_then(|params| {
            params
                .pointer("/message/contextId")
                .and_then(JsonValue::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| task.to_string())
}

fn unknown_task(id: &str) -> LaserError {
    LaserError::HandlerConfig(format!("unknown task `{id}`"))
}

fn unbound_caller() -> LaserError {
    LaserError::HandlerConfig(
        "this bridge binds tasks to callers, so the request needs an authenticated caller"
            .to_owned(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use laser_wire::framing::{decode_named, encode_named};
    use serde_json::{from_str, to_string};
    #[cfg(feature = "a2a-http")]
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn given_a_task_binding_when_any_bound_field_changes_then_the_digest_should_change() {
        let binding = TaskBinding {
            key: [7; 32],
            ttl: std::time::Duration::from_secs(60),
        };
        let task = ConversationId::derive("task-42");
        let base = binding.mac("alice", 10, task, 1_000);
        assert_eq!(base, binding.mac("alice", 10, task, 1_000));
        assert_ne!(base, binding.mac("bob", 10, task, 1_000), "another caller");
        assert_ne!(
            base,
            binding.mac("alice", 11, task, 1_000),
            "a recreated stream"
        );
        assert_ne!(
            base,
            binding.mac("alice", 10, ConversationId::derive("task-43"), 1_000),
            "another task"
        );
        assert_ne!(
            base,
            binding.mac("alice", 10, task, 2_000),
            "another expiry"
        );
        let other_key = TaskBinding {
            key: [8; 32],
            ..binding.clone()
        };
        assert_ne!(base, other_key.mac("alice", 10, task, 1_000), "another key");
        // A caller name cannot be shifted into the following fields.
        assert_ne!(
            binding.mac("ab", 10, task, 1),
            binding.mac("a", 10, task, 1)
        );
        // The shared vector the Python and TypeScript SDKs check as well.
        assert_eq!(
            base.to_hex().as_str(),
            "d1ea2040a6c1451ff74ad5eb597715ce67fa2d144aa0a2b369d9e56ebf0fee1b"
        );
    }

    #[test]
    fn given_task_states_when_serialized_then_should_use_the_a2a_v1_names() {
        let status = TaskStatus {
            state: TaskState::InputRequired,
        };
        assert_eq!(
            to_string(&status).expect("status serializes"),
            r#"{"state":"TASK_STATE_INPUT_REQUIRED"}"#
        );
        let back: TaskStatus =
            from_str(r#"{"state":"TASK_STATE_AUTH_REQUIRED"}"#).expect("status parses");
        assert_eq!(back.state, TaskState::AuthRequired);
        let paused = TaskStatus {
            state: TaskState::Paused,
        };
        assert_eq!(
            to_string(&paused).expect("status serializes"),
            r#"{"state":"TASK_STATE_WORKING"}"#,
            "paused has no A2A name"
        );
        // An unknown inbound name is a protocol error, never a guess, and the
        // v0.3 kebab-case spellings are gone.
        assert!(from_str::<TaskStatus>(r#"{"state":"nope"}"#).is_err());
        assert!(from_str::<TaskStatus>(r#"{"state":"input-required"}"#).is_err());
    }

    #[cfg(feature = "a2a-http")]
    #[test]
    fn given_a2a_methods_when_round_tripped_then_should_match_the_wire_names() {
        assert_eq!(A2aMethod::MessageSend.to_string(), "SendMessage");
        assert_eq!(
            "GetTask".parse::<A2aMethod>().expect("the method parses"),
            A2aMethod::TasksGet
        );
        // The 0.3.0 path-style spelling is gone: v1.0 clients speak PascalCase.
        assert!("tasks/get".parse::<A2aMethod>().is_err());
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
        let payload = encode_named(&envelope).expect("encodes");
        let back: AgentEnvelope = decode_named(&payload).expect("decodes");
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
        let task = task_from_envelope("t-1", "c-1", &reply);
        assert_eq!(
            serde_json::to_value(&task).expect("task serializes"),
            serde_json::json!({
                "id": "t-1",
                "contextId": "c-1",
                "status": {"state": "TASK_STATE_COMPLETED"},
                "artifacts": [{
                    "artifactId": agdx::RecordId::from_u128(5).to_string(),
                    "parts": [{"text": "plan ready"}]
                }]
            })
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
            task_from_envelope("t-1", "c-1", &failure).status.state,
            TaskState::Failed
        );
    }

    fn bridge() -> A2aBridge {
        let laser = Laser::from_client(iggy::prelude::IggyClient::default());
        A2aBridge::new(
            laser,
            "a2a-edge".parse::<AgentId>().expect("valid agent id"),
            AgentTopic::Sessions,
            AgentTopic::Sessions,
        )
    }

    #[tokio::test]
    async fn given_malformed_rpc_requests_when_dispatched_then_should_echo_ids_and_return_public_errors()
     {
        for (request, code, message) in [
            (serde_json::json!(null), -32600, "invalid request"),
            (serde_json::json!([]), -32600, "invalid request"),
            (
                serde_json::json!([7, "GetTask", {"id": "invalid"}]),
                -32600,
                "invalid request",
            ),
            (serde_json::json!({"id": 7}), -32600, "invalid request"),
            (
                serde_json::json!({"id": 7, "method": "unknown"}),
                -32601,
                "method not found",
            ),
            (
                serde_json::json!({"id": 7, "method": "GetTask", "params": {"id": 12}}),
                -32602,
                "invalid params",
            ),
            (
                serde_json::json!({"id": 7, "method": "CancelTask", "params": []}),
                -32602,
                "invalid params",
            ),
            (
                serde_json::json!({"id": 7, "method": "GetTask", "params": {"id": "invalid"}}),
                -32001,
                "task not found",
            ),
            (
                serde_json::json!({"id": 7, "method": "SendMessage", "params": {}}),
                -32602,
                "invalid params",
            ),
        ] {
            let expected = request.get("id").cloned().unwrap_or(JsonValue::Null);
            let response = bridge().handle_rpc(request).await;
            assert_eq!(response.id, expected);
            assert_eq!(response.jsonrpc, "2.0");
            assert!(response.result.is_none());
            let error = response.error.expect("malformed request returns an error");
            assert_eq!(error.code, code);
            assert_eq!(error.message, message);
        }
    }

    #[tokio::test]
    async fn given_a2a_refusals_when_dispatched_then_should_answer_the_a2a_error_codes() {
        let bridge = bridge();
        let codes = [
            (serde_json::json!({"id": 1, "method": "ListTasks"}), -32601),
            (
                serde_json::json!({"id": 2, "method": "GetTask", "params": {"id": "nope"}}),
                -32001,
            ),
            (
                serde_json::json!({"id": 3, "method": "CancelTask", "params": {"id": "nope"}}),
                -32001,
            ),
            (
                serde_json::json!({"id": 4, "method": "SendStreamingMessage", "params": {}}),
                -32004,
            ),
            (serde_json::json!([]), -32600),
        ];
        for (request, code) in codes {
            let error = bridge
                .handle_rpc(request)
                .await
                .error
                .expect("the request is refused");
            assert_eq!(error.code, code);
        }
    }

    #[test]
    fn given_card_details_when_the_card_is_built_then_should_advertise_an_absolute_url_and_no_streaming()
     {
        assert!(matches!(bridge().card(), Err(LaserError::HandlerConfig(_))));
        assert!(matches!(
            bridge().with_public_url("/a2a"),
            Err(LaserError::Invalid(_))
        ));
        let card = bridge()
            .with_public_url("https://agents.example.com/a2a")
            .expect("an absolute URL")
            .with_description("Plans trips")
            .with_version("2.1.0")
            .card()
            .expect("the card builds");
        assert!(!card.capabilities.streaming);
        assert_eq!(card.description, "Plans trips");
        assert_eq!(card.version, "2.1.0");
        assert_eq!(
            card.supported_interfaces[0].url,
            "https://agents.example.com/a2a"
        );
    }

    #[test]
    fn given_command_bodies_when_the_context_is_read_then_should_prefer_the_message_context() {
        let task = ConversationId::new();
        assert_eq!(
            context_id_of(br#"{"message":{"contextId":"trip-7"}}"#, task),
            "trip-7"
        );
        assert_eq!(context_id_of(br#"{"message":{}}"#, task), task.to_string());
        assert_eq!(context_id_of(b"not json", task), task.to_string());
    }

    #[cfg(feature = "a2a-http")]
    #[tokio::test]
    async fn given_browser_origins_when_posting_to_the_router_then_should_refuse_foreign_and_rebound_pages()
     {
        let router =
            Arc::new(bridge().with_allowed_origins(vec!["https://app.example.com".to_owned()]))
                .router();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a local port binds");
        let port = listener.local_addr().expect("the port is known").port();
        tokio::spawn(async move { axum::serve(listener, router).await });
        let local = format!("127.0.0.1:{port}");
        let post = |host: String, origin: Option<String>| async move {
            http_status(port, &host, origin.as_deref()).await
        };
        assert_eq!(post(local.clone(), None).await, 200, "a non-browser client");
        assert_eq!(
            post(local.clone(), Some(format!("http://{local}"))).await,
            200,
            "a page the bridge itself serves"
        );
        assert_eq!(
            post(local.clone(), Some("https://app.example.com".to_owned())).await,
            200,
            "an allowlisted page"
        );
        assert_eq!(
            post(local.clone(), Some("https://evil.example".to_owned())).await,
            403,
            "a foreign page"
        );
        assert_eq!(
            post(
                format!("evil.example:{port}"),
                Some(format!("http://evil.example:{port}"))
            )
            .await,
            403,
            "a DNS rebinding page, same-origin by name only"
        );
    }

    // The HTTP status of one JSON-RPC POST to the local router.
    #[cfg(feature = "a2a-http")]
    async fn http_status(port: u16, host: &str, origin: Option<&str>) -> u16 {
        let body = r#"{"jsonrpc":"2.0","id":1,"method":"ListTasks"}"#;
        let origin = origin.map_or_else(String::new, |origin| format!("Origin: {origin}\r\n"));
        let request = format!(
            "POST / HTTP/1.1\r\nHost: {host}\r\n{origin}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("the router accepts");
        stream
            .write_all(request.as_bytes())
            .await
            .expect("the request writes");
        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .await
            .expect("the response reads");
        response
            .split(' ')
            .nth(1)
            .and_then(|status| status.parse().ok())
            .expect("an HTTP status line")
    }

    #[test]
    fn given_a_new_bridge_when_built_then_should_stamp_its_own_id_as_the_hop_path() {
        assert_eq!(bridge().hops, ["a2a-edge"]);
    }

    #[test]
    fn given_an_upstream_hop_path_when_continued_then_should_append_this_bridge() {
        let bridge = bridge()
            .with_bridge_hops(&["mcp-edge".to_owned()])
            .expect("no loop");
        assert_eq!(bridge.hops, ["mcp-edge", "a2a-edge"]);
        assert!(matches!(
            bridge.with_bridge_hops(&["a2a-edge".to_owned()]),
            Err(LaserError::Invalid(_))
        ));
    }

    #[test]
    fn given_a_jsonrpc_message_send_when_parsed_then_should_expose_the_text_part() {
        let request: JsonRpcRequest = from_str(
            r#"{"jsonrpc":"2.0","id":1,"method":"SendMessage",
                "params":{"message":{"role":"user","parts":[{"kind":"text","text":"hi"}]}}}"#,
        )
        .expect("the request parses");
        assert_eq!(request.method, "SendMessage");
        assert_eq!(
            request
                .params
                .pointer("/message/parts/0/text")
                .and_then(JsonValue::as_str),
            Some("hi")
        );
    }
}
