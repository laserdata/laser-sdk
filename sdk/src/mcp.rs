use crate::agent::Laser;
pub use crate::bridge_hops::{BRIDGE_HOPS_META_KEY, enter_bridge};
use crate::bridge_hops::{call_hops, hops_metadata, inbound_hops};
#[cfg(feature = "mcp-http")]
use crate::bridge_origin::origin_allowed;
use crate::error::LaserError;
use crate::provenance::AgentTopic;
use crate::types::ConversationId;

#[cfg(feature = "mcp-http")]
use axum::Router;
#[cfg(feature = "mcp-http")]
use axum::extract::State;
#[cfg(feature = "mcp-http")]
use axum::http::{HeaderMap, StatusCode};
#[cfg(feature = "mcp-http")]
use axum::response::{IntoResponse, Response};
#[cfg(feature = "mcp-http")]
use axum::routing::post;
use laser_wire::agent::{
    self as agdx, AgentEnvelope, AgentId, CorrelationId, METADATA_BRIDGE_HOPS,
};
use laser_wire::content::ContentType;
use laser_wire::result::ResultCode;
use serde::{Deserialize, Serialize};
use serde_json::to_value;
use serde_json::{Value as JsonValue, json};
use std::str::FromStr;
#[cfg(feature = "mcp-http")]
use std::sync::Arc;
use std::time::Duration;
use strum::{Display, EnumString};

const JSONRPC_VERSION: &str = "2.0";
const INVALID_REQUEST: i32 = -32600;
const METHOD_NOT_FOUND: i32 = -32601;
const INVALID_PARAMS: i32 = -32602;
const INTERNAL_ERROR: i32 = -32603;
const RESOURCE_NOT_FOUND: i32 = -32002;
// Answered on initialize when the client asks for a version this bridge does
// not serve: the latest it does.
const DEFAULT_PROTOCOL_VERSION: &str = "2025-11-25";
const DEFAULT_CALL_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(feature = "mcp-http")]
const PROTOCOL_VERSION_HEADER: &str = "mcp-protocol-version";

/// The MCP protocol versions this bridge serves, newest first. Every one
/// carries Streamable HTTP and the tool, resource and prompt shapes the bridge
/// answers with.
pub const MCP_SUPPORTED_PROTOCOL_VERSIONS: [&str; 3] = ["2025-11-25", "2025-06-18", "2025-03-26"];

/// MCP JSON-RPC methods the bridge serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, EnumString)]
pub enum McpMethod {
    #[strum(serialize = "initialize")]
    Initialize,
    #[strum(serialize = "ping")]
    Ping,
    #[strum(serialize = "tools/list")]
    ToolsList,
    #[strum(serialize = "tools/call")]
    ToolsCall,
    #[strum(serialize = "resources/list")]
    ResourcesList,
    #[strum(serialize = "resources/read")]
    ResourcesRead,
    #[strum(serialize = "prompts/list")]
    PromptsList,
    #[strum(serialize = "prompts/get")]
    PromptsGet,
}

/// A resource the bridge advertises in `resources/list` (the MCP `Resource`
/// shape). The bridge holds its text content for `resources/read`.
#[derive(Debug, Clone, Serialize)]
pub struct McpResource {
    pub uri: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "mimeType", skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
}

/// One argument of an [`McpPrompt`] (the MCP `PromptArgument` shape).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpPromptArgument {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required: Option<bool>,
}

/// A prompt the bridge advertises in `prompts/list` (the MCP `Prompt` shape).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpPrompt {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub arguments: Vec<McpPromptArgument>,
}

/// A tool the bridge advertises in `tools/list`, matching the MCP `Tool` schema
/// (`name`, optional `title`/`description`, `inputSchema`). `input_schema` is
/// the raw JSON Schema object MCP clients expect (use `{"type":"object",
/// "additionalProperties":false}` for a no-arg tool).
#[derive(Debug, Clone, Serialize)]
pub struct McpTool {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "inputSchema")]
    pub input_schema: JsonValue,
}

/// One MCP content item in a `tools/call` result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpContent {
    #[serde(rename = "type")]
    pub kind: String,
    pub text: String,
}

/// The MCP `tools/call` result view produced from an AGDX response or error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpToolResult {
    pub content: Vec<McpContent>,
    #[serde(rename = "isError", default, skip_serializing_if = "is_false")]
    pub is_error: bool,
}

/// Map an MCP `tools/call` request onto an AGDX `command`.
///
/// The mapped core is the tool name plus request correlation. The original
/// MCP params JSON tunnels byte-identical in `body` with `agdx.ct = json` at the
/// producer. MCP `_meta` and any future fields therefore round-trip whole
/// instead of being flattened into AGDX metadata.
pub fn tool_call_from_request(
    record: agdx::RecordId,
    conversation: agdx::ConversationId,
    source: agdx::AgentId,
    correlation: agdx::CorrelationId,
    tool_name: impl Into<String>,
    params_json: Vec<u8>,
) -> AgentEnvelope {
    AgentEnvelope::command(record, conversation, source, correlation, params_json)
        .with_tool(tool_name)
}

/// Render an AGDX response/error back as an MCP tool result.
pub fn tool_result_from_envelope(envelope: &AgentEnvelope) -> McpToolResult {
    let text = String::from_utf8_lossy(&envelope.body).into_owned();
    let content = if text.is_empty() {
        Vec::new()
    } else {
        vec![McpContent {
            kind: "text".to_owned(),
            text,
        }]
    };
    McpToolResult {
        content,
        is_error: envelope.kind == agdx::AgentKind::Error,
    }
}

// A failed tool call's error body for its child session.
fn tool_failure(message: &str) -> laser_wire::agent::AgentErrorBody {
    laser_wire::agent::AgentErrorBody {
        code: laser_wire::agent::AgentErrorCode::Internal,
        message: Some(message.to_owned()),
        retryable: false,
        detail: None,
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// Bridges the MCP JSON-RPC edge onto agent topics: `tools/call` becomes a typed
/// AGDX `command` on `tool_topic` (the tool name in `tool`, the MCP params
/// tunneled byte-identical in the body with `agdx.ct = json`), and the bridge
/// awaits the correlated AGDX `response`/`error` on `reply_topic` within
/// `timeout`, rendering it back as an MCP tool result (`isError` on a failure).
pub struct McpBridge {
    laser: Laser,
    source: AgentId,
    tool_topic: AgentTopic<'static>,
    reply_topic: AgentTopic<'static>,
    server_name: String,
    tools: Vec<McpTool>,
    resources: Vec<ResourceEntry>,
    prompts: Vec<PromptEntry>,
    timeout: Duration,
    // The `bridge_hops` path stamped on every tool call, ending in this
    // bridge's own id.
    hops: Vec<String>,
    // The agent every call that names no target is addressed to.
    target: Option<AgentId>,
    #[cfg(feature = "sign")]
    signing_key: Option<crate::sign::SigningKey>,
    // The browser origins the HTTP router accepts besides same-origin pages
    // on an IP literal or `localhost` host.
    #[cfg(feature = "mcp-http")]
    allowed_origins: Vec<String>,
}

// A request refused with its JSON-RPC error code and a fixed public message.
enum McpFault {
    InvalidRequest,
    MethodNotFound,
    InvalidParams(&'static str),
    ResourceNotFound,
    Laser(LaserError),
}

impl McpFault {
    fn rpc_error(&self) -> McpRpcError {
        let (code, message) = match self {
            Self::InvalidRequest => (INVALID_REQUEST, "invalid request"),
            Self::MethodNotFound => (METHOD_NOT_FOUND, "method not found"),
            Self::InvalidParams(message) => (INVALID_PARAMS, *message),
            Self::ResourceNotFound => (RESOURCE_NOT_FOUND, "resource not found"),
            Self::Laser(error) if error.code() == ResultCode::InvalidArgument => {
                (INVALID_PARAMS, "invalid params")
            }
            Self::Laser(error) => (INTERNAL_ERROR, crate::error::public_error_message(error)),
        };
        McpRpcError {
            code,
            message: message.to_owned(),
        }
    }
}

impl From<LaserError> for McpFault {
    fn from(error: LaserError) -> Self {
        Self::Laser(error)
    }
}

// A resource plus the text `resources/read` returns for it.
struct ResourceEntry {
    resource: McpResource,
    text: String,
}

// A prompt plus the messages `prompts/get` renders.
struct PromptEntry {
    prompt: McpPrompt,
    messages: Vec<(String, String)>,
}

impl McpBridge {
    /// A bridge serving MCP over `laser`, publishing tool calls as `source`.
    /// Every topic lives on the stream of `laser`. Pass `laser.with_default_stream(stream)`
    /// to run the bridge on a non-default stream (the unit of multi-stream
    /// topologies and per-stream Iggy RBAC). `tool_topic`/`reply_topic` are any
    /// [`AgentTopic`], including `AgentTopic::Custom` for an arbitrary name.
    pub fn new(
        laser: Laser,
        source: impl Into<AgentId>,
        tool_topic: AgentTopic<'static>,
        reply_topic: AgentTopic<'static>,
        server_name: impl Into<String>,
    ) -> Self {
        let source = source.into();
        let hops = vec![source.as_str().to_owned()];
        Self {
            laser,
            source,
            tool_topic,
            reply_topic,
            server_name: server_name.into(),
            tools: Vec::new(),
            resources: Vec::new(),
            prompts: Vec::new(),
            timeout: DEFAULT_CALL_TIMEOUT,
            hops,
            target: None,
            #[cfg(feature = "sign")]
            signing_key: None,
            #[cfg(feature = "mcp-http")]
            allowed_origins: Vec::new(),
        }
    }

    /// Sign every tool call with `key`, so an agent that verifies its input
    /// accepts the calls this bridge publishes. Requires the `sign` feature.
    #[cfg(feature = "sign")]
    #[must_use]
    pub fn with_signing_key(mut self, key: crate::sign::SigningKey) -> Self {
        self.signing_key = Some(key);
        self
    }

    /// Address every call that names no target to `agent`, the agent this
    /// bridge fronts. Its reply must then be signed by `agent` when the
    /// `Laser` verifies replies.
    #[must_use]
    pub fn with_target(mut self, agent: impl Into<AgentId>) -> Self {
        self.target = Some(agent.into());
        self
    }

    /// The browser origins the HTTP router accepts, each as the exact
    /// `Origin` header value (`https://app.example.com`). A request without an
    /// `Origin` header, a non-browser client, is always accepted, and so is a
    /// same-origin page on an IP literal or `localhost` host. Any other browser
    /// request is refused with 403, the DNS rebinding guard MCP Streamable
    /// HTTP requires. Requires the `mcp-http` feature.
    #[cfg(feature = "mcp-http")]
    #[must_use]
    pub fn with_allowed_origins(mut self, origins: Vec<String>) -> Self {
        self.allowed_origins = origins;
        self
    }

    /// Continue the loop-guard path of a call that already crossed other
    /// bridges: `previous` is its `bridge_hops`, and this bridge's id is
    /// appended to it. Every tool call the bridge publishes carries the path.
    /// Refuses a path that already holds this bridge, the loop the guard
    /// exists to stop.
    pub fn with_bridge_hops(mut self, previous: &[String]) -> Result<Self, LaserError> {
        self.hops = enter_bridge(self.source.as_str(), previous)?;
        Ok(self)
    }

    /// Advertise a resource (served from `text` on `resources/read`).
    pub fn with_resource(
        mut self,
        uri: impl Into<String>,
        name: impl Into<String>,
        mime_type: Option<String>,
        text: impl Into<String>,
    ) -> Self {
        self.resources.push(ResourceEntry {
            resource: McpResource {
                uri: uri.into(),
                name: name.into(),
                title: None,
                description: None,
                mime_type,
            },
            text: text.into(),
        });
        self
    }

    /// Advertise a prompt. `messages` are `(role, text)` pairs `prompts/get`
    /// renders into MCP prompt messages.
    pub fn with_prompt(mut self, prompt: McpPrompt, messages: Vec<(String, String)>) -> Self {
        self.prompts.push(PromptEntry { prompt, messages });
        self
    }

    /// Advertise a tool in `tools/list`. `input_schema` is the raw JSON Schema,
    /// which MCP requires to be an object. Any other JSON value is refused with
    /// [`LaserError::Invalid`].
    pub fn with_tool(
        self,
        name: impl Into<String>,
        description: Option<String>,
        input_schema: JsonValue,
    ) -> Result<Self, LaserError> {
        if !input_schema.is_object() {
            return Err(LaserError::Invalid(
                "MCP tool input schema must be a JSON object".to_owned(),
            ));
        }
        Ok(self.push_tool(name, description, input_schema))
    }

    fn push_tool(
        mut self,
        name: impl Into<String>,
        description: Option<String>,
        input_schema: JsonValue,
    ) -> Self {
        self.tools.push(McpTool {
            name: name.into(),
            title: None,
            description,
            input_schema,
        });
        self
    }

    /// Advertise the two canonical portable-memory tools, `remember` and
    /// `recall`, so an MCP host (Claude Desktop, Cursor, an IDE) sees this bridge
    /// as a memory server and can carry memory across tools. Both route through
    /// `call_tool` onto the agent's AGDX command topic exactly like any other
    /// tool, so the agent behind the bridge backs them with a [`Memory`] of its
    /// choosing. The schemas mirror the field's converging shape (a `text` to
    /// remember, a `query` plus `limit` to recall), so a host that knows the
    /// convention drives them without bespoke glue.
    ///
    /// [`Memory`]: crate::memory::Memory
    pub fn with_memory_tools(self) -> Self {
        self.push_tool(
            "remember",
            Some("Store a memory item for later recall.".to_owned()),
            json!({
                "type": "object",
                "properties": {
                    "text": { "type": "string", "description": "The content to remember." }
                },
                "required": ["text"]
            }),
        )
        .push_tool(
            "recall",
            Some("Retrieve the memory items most relevant to a query.".to_owned()),
            json!({
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "What to recall." },
                    "limit": { "type": "integer", "minimum": 1, "description": "Max items to return." }
                },
                "required": ["query"]
            }),
        )
    }

    /// Override the per-call reply timeout (default 30s).
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// `initialize`: the client's protocol version when this bridge serves it
    /// ([`MCP_SUPPORTED_PROTOCOL_VERSIONS`]), else the latest it serves, and
    /// the capabilities the bridge actually serves.
    pub fn initialize(&self, protocol_version: Option<&str>) -> JsonValue {
        let protocol_version = protocol_version
            .filter(|version| MCP_SUPPORTED_PROTOCOL_VERSIONS.contains(version))
            .unwrap_or(DEFAULT_PROTOCOL_VERSION);
        let mut capabilities = serde_json::Map::new();
        capabilities.insert("tools".to_owned(), json!({}));
        if !self.resources.is_empty() {
            capabilities.insert("resources".to_owned(), json!({}));
        }
        if !self.prompts.is_empty() {
            capabilities.insert("prompts".to_owned(), json!({}));
        }
        json!({
            "protocolVersion": protocol_version,
            "serverInfo": { "name": self.server_name, "version": env!("CARGO_PKG_VERSION") },
            "capabilities": capabilities,
        })
    }

    /// `tools/list`: the advertised tools.
    pub fn list_tools(&self) -> JsonValue {
        json!({ "tools": self.tools })
    }

    /// `resources/list`: the advertised resources.
    pub fn list_resources(&self) -> JsonValue {
        let resources: Vec<&McpResource> = self.resources.iter().map(|e| &e.resource).collect();
        json!({ "resources": resources })
    }

    /// `resources/read`: the contents of the resource at `uri`.
    pub fn read_resource(&self, uri: &str) -> Result<JsonValue, LaserError> {
        let entry = self
            .resources
            .iter()
            .find(|entry| entry.resource.uri == uri)
            .ok_or_else(|| LaserError::Invalid(format!("unknown resource `{uri}`")))?;
        let mut content = serde_json::Map::new();
        content.insert("uri".to_owned(), json!(entry.resource.uri));
        if let Some(mime_type) = &entry.resource.mime_type {
            content.insert("mimeType".to_owned(), json!(mime_type));
        }
        content.insert("text".to_owned(), json!(entry.text));
        Ok(json!({ "contents": [content] }))
    }

    /// `prompts/list`: the advertised prompts.
    pub fn list_prompts(&self) -> JsonValue {
        let prompts: Vec<&McpPrompt> = self.prompts.iter().map(|e| &e.prompt).collect();
        json!({ "prompts": prompts })
    }

    /// `prompts/get`: the rendered messages of the prompt `name`.
    pub fn get_prompt(&self, name: &str) -> Result<JsonValue, LaserError> {
        let entry = self
            .prompts
            .iter()
            .find(|entry| entry.prompt.name == name)
            .ok_or_else(|| LaserError::Invalid(format!("unknown prompt `{name}`")))?;
        let messages: Vec<JsonValue> = entry
            .messages
            .iter()
            .map(
                |(role, text)| json!({ "role": role, "content": { "type": "text", "text": text } }),
            )
            .collect();
        let mut rendered = serde_json::Map::new();
        if let Some(description) = &entry.prompt.description {
            rendered.insert("description".to_owned(), json!(description));
        }
        rendered.insert("messages".to_owned(), JsonValue::Array(messages));
        Ok(JsonValue::Object(rendered))
    }

    /// `tools/call`: map onto an AGDX `command`, await the correlated terminal,
    /// render the MCP tool result. The command is addressed to the
    /// [`with_target`](Self::with_target) agent, else to every agent
    /// (`agdx.to = *`), so on a shared session topic every listening agent
    /// receives it. Use [`call_tool_to`](Self::call_tool_to) to address one
    /// agent.
    pub async fn call_tool(
        &self,
        name: &str,
        params_json: Vec<u8>,
    ) -> Result<McpToolResult, LaserError> {
        self.call_tool_addressed(None, &self.hops, name, params_json)
            .await
    }

    /// [`call_tool`](Self::call_tool) for work that already crossed other
    /// bridges: `previous` is its `bridge_hops`, and the call carries that path
    /// with this bridge appended. A path that already holds this bridge is
    /// refused before anything is published. `target` addresses one agent,
    /// as in [`call_tool_to`](Self::call_tool_to).
    pub async fn call_tool_with_hops(
        &self,
        previous: &[String],
        target: Option<AgentId>,
        name: &str,
        params_json: Vec<u8>,
    ) -> Result<McpToolResult, LaserError> {
        let hops = call_hops(&self.hops, previous)?;
        self.call_tool_addressed(target, &hops, name, params_json)
            .await
    }

    /// [`call_tool`](Self::call_tool) addressed to `target`, so only that
    /// agent runs the tool on a shared session topic.
    pub async fn call_tool_to(
        &self,
        target: impl Into<AgentId>,
        name: &str,
        params_json: Vec<u8>,
    ) -> Result<McpToolResult, LaserError> {
        let target = target.into();
        self.call_tool_addressed(Some(target), &self.hops, name, params_json)
            .await
    }

    /// `tools/call` as a child session of `parent`, in the tree rooted at
    /// `root`: the call's submitted start lands on `agent.sessions` with the
    /// ancestry before the command, and the child ends by the result,
    /// completed on a tool result and failed on a tool error. The command is
    /// addressed to every agent, like [`call_tool`](Self::call_tool).
    pub async fn call_tool_in(
        &self,
        parent: ConversationId,
        root: ConversationId,
        name: &str,
        params_json: Vec<u8>,
    ) -> Result<McpToolResult, LaserError> {
        self.call_tool_in_addressed(None, parent, root, name, params_json)
            .await
    }

    /// [`call_tool_in`](Self::call_tool_in) addressed to `target`.
    pub async fn call_tool_in_to(
        &self,
        target: impl Into<AgentId>,
        parent: ConversationId,
        root: ConversationId,
        name: &str,
        params_json: Vec<u8>,
    ) -> Result<McpToolResult, LaserError> {
        let target = target.into();
        self.call_tool_in_addressed(Some(target), parent, root, name, params_json)
            .await
    }

    async fn call_tool_addressed(
        &self,
        target: Option<AgentId>,
        hops: &[String],
        name: &str,
        params_json: Vec<u8>,
    ) -> Result<McpToolResult, LaserError> {
        let conversation = ConversationId::new();
        let envelope = self
            .send_tool_call(conversation, target, None, hops, name, params_json)
            .await?;
        Ok(tool_result_from_envelope(&envelope))
    }

    async fn call_tool_in_addressed(
        &self,
        target: Option<AgentId>,
        parent: ConversationId,
        root: ConversationId,
        name: &str,
        params_json: Vec<u8>,
    ) -> Result<McpToolResult, LaserError> {
        let conversation = ConversationId::new();
        let child = crate::agent::contract::start_child(
            &self.laser,
            &self.source,
            &self.source,
            conversation.into(),
            parent.into(),
            root.into(),
        )
        .await?;
        let outcome = self
            .send_tool_call(
                conversation,
                target,
                Some((parent, root)),
                &self.hops,
                name,
                params_json,
            )
            .await;
        let ended = match &outcome {
            Ok(envelope) if envelope.kind != laser_wire::agent::AgentKind::Error => {
                child.end().await
            }
            Ok(_) => child.fail(tool_failure("the tool returned an error")).await,
            Err(error) => child.fail(tool_failure(&error.to_string())).await,
        };
        let envelope = outcome?;
        ended?;
        Ok(tool_result_from_envelope(&envelope))
    }

    // Publish one tool call command and await its correlated terminal. The
    // reply reader is seeded at the topic tail before sending, so it reads
    // only the reply rather than the topic's history.
    async fn send_tool_call(
        &self,
        conversation: ConversationId,
        target: Option<AgentId>,
        ancestry: Option<(ConversationId, ConversationId)>,
        hops: &[String],
        name: &str,
        params_json: Vec<u8>,
    ) -> Result<AgentEnvelope, LaserError> {
        let target = target.or_else(|| self.target.clone());
        let correlation = CorrelationId::from_u128(conversation.as_u128());
        #[allow(unused_mut)]
        let mut reader = self
            .laser
            .agdx_reply_reader(self.reply_topic.clone(), Some(&self.source))
            .await?;
        // A targeted call is answered only by its target when replies are verified.
        #[cfg(feature = "sign")]
        {
            reader.expected_signer = target.as_ref().map(ToString::to_string);
        }
        let producer = self.laser.agdx(
            self.tool_topic.clone(),
            self.source.clone(),
            conversation.into(),
        );
        let mut command = producer
            .command(correlation, params_json)
            .with_tool(name.to_owned())
            .with_metadata(METADATA_BRIDGE_HOPS, hops_metadata(hops))
            .content_type(ContentType::Json);
        if let Some((parent, root)) = ancestry {
            command = command.with_ancestry(Some(parent.into()), Some(root.into()));
        }
        if let Some(target) = target {
            command = command.with_target(target);
        }
        #[cfg(feature = "sign")]
        if let Some(key) = &self.signing_key {
            command = command.signed_by(key);
        }
        command.send().await?;
        self.laser
            .await_agdx_reply(&mut reader, correlation, self.timeout)
            .await
    }

    /// Handle one JSON-RPC message. A notification, a message without an
    /// `id`, is never answered and returns `None`.
    pub async fn handle_rpc(&self, request: JsonValue) -> Option<McpRpcResponse> {
        if request.is_object() && request.get("id").is_none() {
            return None;
        }
        let id = request.get("id").cloned().unwrap_or(JsonValue::Null);
        Some(match self.dispatch_rpc(&request).await {
            Ok(result) => McpRpcResponse {
                jsonrpc: JSONRPC_VERSION,
                id,
                result: Some(result),
                error: None,
            },
            Err(fault) => {
                if let McpFault::Laser(error) = &fault {
                    tracing::warn!(error = %error, "MCP request failed");
                }
                McpRpcResponse {
                    jsonrpc: JSONRPC_VERSION,
                    id,
                    result: None,
                    error: Some(fault.rpc_error()),
                }
            }
        })
    }

    async fn dispatch_rpc(&self, request: &JsonValue) -> Result<JsonValue, McpFault> {
        // Serde also reads a struct from an array, so the object check comes first.
        if !request.is_object() {
            return Err(McpFault::InvalidRequest);
        }
        let McpRpcRequest { method, params, .. } =
            McpRpcRequest::deserialize(request).map_err(|_| McpFault::InvalidRequest)?;
        let value = if params.is_null() {
            JsonValue::Object(Default::default())
        } else {
            params
        };
        let params = value
            .as_object()
            .ok_or(McpFault::InvalidParams("invalid params"))?;
        let required = |key| {
            params
                .get(key)
                .and_then(JsonValue::as_str)
                .ok_or(McpFault::InvalidParams("invalid params"))
        };
        match McpMethod::from_str(&method) {
            Ok(McpMethod::Initialize) => {
                let version = params
                    .get("protocolVersion")
                    .map(|value| {
                        value
                            .as_str()
                            .ok_or(McpFault::InvalidParams("invalid params"))
                    })
                    .transpose()?;
                Ok(self.initialize(version))
            }
            Ok(McpMethod::Ping) => Ok(json!({})),
            Ok(McpMethod::ToolsList) => Ok(self.list_tools()),
            Ok(McpMethod::ResourcesList) => Ok(self.list_resources()),
            Ok(McpMethod::ResourcesRead) => {
                let uri = required("uri")?;
                if !self.resources.iter().any(|entry| entry.resource.uri == uri) {
                    return Err(McpFault::ResourceNotFound);
                }
                Ok(self.read_resource(uri)?)
            }
            Ok(McpMethod::PromptsList) => Ok(self.list_prompts()),
            Ok(McpMethod::PromptsGet) => {
                let name = required("name")?;
                if !self.prompts.iter().any(|entry| entry.prompt.name == name) {
                    return Err(McpFault::InvalidParams("unknown prompt"));
                }
                Ok(self.get_prompt(name)?)
            }
            Ok(McpMethod::ToolsCall) => {
                let name = required("name")?;
                // Only an advertised tool is published, so a guessed name costs no wait.
                if !self.tools.iter().any(|tool| tool.name == name) {
                    return Err(McpFault::InvalidParams("unknown tool"));
                }
                let previous = inbound_hops(params.get("_meta"))?;
                let body = serde_json::to_vec(&value)
                    .map_err(|error| LaserError::Codec(error.to_string()))?;
                let result = self
                    .call_tool_with_hops(&previous, None, name, body)
                    .await?;
                to_value(result)
                    .map_err(|error| McpFault::Laser(LaserError::Codec(error.to_string())))
            }
            Err(_) => Err(McpFault::MethodNotFound),
        }
    }

    // The Streamable HTTP answer to one POST: 403 for a browser origin the
    // guard refuses, 400 for an unsupported protocol version header, 202 with
    // no body for a notification, else the JSON-RPC response.
    #[cfg(feature = "mcp-http")]
    async fn http_reply(&self, headers: &HeaderMap, request: JsonValue) -> Response {
        if !origin_allowed(headers, &self.allowed_origins) {
            return StatusCode::FORBIDDEN.into_response();
        }
        if let Some(version) = headers.get(PROTOCOL_VERSION_HEADER)
            && !version
                .to_str()
                .is_ok_and(|version| MCP_SUPPORTED_PROTOCOL_VERSIONS.contains(&version))
        {
            return StatusCode::BAD_REQUEST.into_response();
        }
        match self.handle_rpc(request).await {
            Some(response) => axum::Json(response).into_response(),
            None => StatusCode::ACCEPTED.into_response(),
        }
    }

    /// An axum router exposing the MCP JSON-RPC endpoint at `/`. Requires the
    /// `mcp-http` feature. The bridge adapter is usable without it.
    #[cfg(feature = "mcp-http")]
    pub fn router(self: Arc<Self>) -> Router {
        Router::new().route("/", post(handle_mcp)).with_state(self)
    }
}

/// A JSON-RPC request envelope.
#[derive(Debug, Deserialize)]
pub struct McpRpcRequest {
    #[serde(default)]
    pub id: JsonValue,
    pub method: String,
    #[serde(default)]
    pub params: JsonValue,
}

/// A JSON-RPC response envelope.
#[derive(Debug, Serialize)]
pub struct McpRpcResponse {
    pub jsonrpc: &'static str,
    pub id: JsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<JsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<McpRpcError>,
}

/// A JSON-RPC error object.
#[derive(Debug, Serialize)]
pub struct McpRpcError {
    pub code: i32,
    pub message: String,
}

#[cfg(feature = "mcp-http")]
async fn handle_mcp(
    State(bridge): State<Arc<McpBridge>>,
    headers: HeaderMap,
    axum::Json(request): axum::Json<JsonValue>,
) -> Response {
    bridge.http_reply(&headers, request).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use laser_wire::framing::{decode_named, encode_named};
    use serde_json::to_string;

    #[cfg(feature = "mcp-http")]
    #[test]
    fn given_mcp_method_when_round_tripped_then_should_match_the_wire_name() {
        assert_eq!(McpMethod::ToolsCall.to_string(), "tools/call");
        assert_eq!(
            "tools/call".parse::<McpMethod>().expect("method parses"),
            McpMethod::ToolsCall
        );
        assert_eq!(
            "initialize".parse::<McpMethod>().expect("method parses"),
            McpMethod::Initialize
        );
        assert_eq!(
            "tools/list".parse::<McpMethod>().expect("method parses"),
            McpMethod::ToolsList
        );
        assert_eq!(
            "resources/read"
                .parse::<McpMethod>()
                .expect("method parses"),
            McpMethod::ResourcesRead
        );
        assert_eq!(
            "prompts/get".parse::<McpMethod>().expect("method parses"),
            McpMethod::PromptsGet
        );
        assert!("completion/complete".parse::<McpMethod>().is_err());
    }

    fn bridge() -> McpBridge {
        let laser = Laser::from_client(iggy::prelude::IggyClient::default());
        McpBridge::new(
            laser,
            "mcp-edge".parse::<AgentId>().expect("valid agent id"),
            AgentTopic::Sessions,
            AgentTopic::Sessions,
            "tools",
        )
    }

    #[tokio::test]
    async fn given_rpc_catalog_requests_when_dispatched_then_should_work_without_http() {
        let bridge = bridge();
        let response = bridge
            .handle_rpc(json!({"id": "init", "method": "initialize"}))
            .await
            .expect("a request is answered");
        assert_eq!(response.id, "init");
        assert!(response.error.is_none());
        assert_eq!(
            response.result.expect("initialize succeeds")["protocolVersion"],
            DEFAULT_PROTOCOL_VERSION
        );
        let response = bridge
            .handle_rpc(json!({"id": null, "method": "tools/list", "params": null}))
            .await
            .expect("a request is answered");
        assert_eq!(response.id, JsonValue::Null);
        assert_eq!(response.result, Some(json!({"tools": []})));
        assert!(
            bridge
                .handle_rpc(json!({"jsonrpc": "2.0", "method": "notifications/initialized"}))
                .await
                .is_none(),
            "a notification is never answered"
        );
    }

    #[tokio::test]
    async fn given_malformed_rpc_requests_when_dispatched_then_should_echo_ids_and_return_public_errors()
     {
        for (request, code, message) in [
            (json!(null), -32600, "invalid request"),
            (json!([]), -32600, "invalid request"),
            (json!([7, "tools/list", null]), -32600, "invalid request"),
            (json!({"id": 7}), -32600, "invalid request"),
            (
                json!({"id": 7, "method": "unknown"}),
                -32601,
                "method not found",
            ),
            (
                json!({"id": 7, "method": "initialize", "params": {"protocolVersion": null}}),
                -32602,
                "invalid params",
            ),
            (
                json!({"id": 7, "method": "tools/list", "params": []}),
                -32602,
                "invalid params",
            ),
            (
                json!({"id": 7, "method": "resources/read"}),
                -32602,
                "invalid params",
            ),
            (
                json!({"id": 7, "method": "prompts/get", "params": {"name": 12}}),
                -32602,
                "invalid params",
            ),
            (
                json!({"id": 7, "method": "tools/call", "params": {"name": false}}),
                -32602,
                "invalid params",
            ),
        ] {
            let expected = request.get("id").cloned().unwrap_or(JsonValue::Null);
            let response = bridge()
                .handle_rpc(request)
                .await
                .expect("a malformed request is answered");
            assert_eq!(response.id, expected);
            assert!(response.result.is_none());
            let error = response.error.expect("malformed request returns an error");
            assert_eq!(error.code, code);
            assert_eq!(error.message, message);
        }
    }

    #[tokio::test]
    async fn given_protocol_details_when_dispatched_then_should_follow_the_mcp_schema() {
        let bridge = bridge()
            .with_prompt(
                McpPrompt {
                    name: "greet".to_owned(),
                    title: None,
                    description: None,
                    arguments: Vec::new(),
                },
                vec![("user".to_owned(), "hello".to_owned())],
            )
            .with_tool("search", None, json!({"type": "object"}))
            .expect("an object schema");
        let reply = |request| async {
            bridge
                .handle_rpc(request)
                .await
                .expect("a request is answered")
        };
        let ping = reply(json!({"id": 1, "method": "ping"})).await;
        assert_eq!(ping.result, Some(json!({})), "ping answers an empty result");
        let prompt = reply(json!({"id": 2, "method": "prompts/get", "params": {"name": "greet"}}))
            .await
            .result
            .expect("the prompt renders");
        assert!(
            prompt.get("description").is_none(),
            "a missing description is omitted, never null"
        );
        let init = reply(json!({"id": 3, "method": "initialize",
            "params": {"protocolVersion": "1999-01-01"}}))
        .await
        .result
        .expect("initialize succeeds");
        assert_eq!(init["protocolVersion"], DEFAULT_PROTOCOL_VERSION);
        let codes = [
            (json!({"id": 4, "method": "unknown"}), -32601),
            (
                json!({"id": 5, "method": "tools/call", "params": {"name": "nope"}}),
                -32602,
            ),
            (
                json!({"id": 6, "method": "prompts/get", "params": {"name": "nope"}}),
                -32602,
            ),
            (
                json!({"id": 7, "method": "resources/read", "params": {"uri": "file:///nope"}}),
                -32002,
            ),
            (
                json!({"id": 8, "method": "tools/call", "params": {"name": "search",
                    "_meta": {crate::bridge_hops::BRIDGE_HOPS_META_KEY: ["a2a-edge", "mcp-edge"]}}}),
                -32602,
            ),
        ];
        for (request, code) in codes {
            let error = reply(request).await.error.expect("the request is refused");
            assert_eq!(error.code, code);
        }
    }

    #[cfg(feature = "mcp-http")]
    #[tokio::test]
    async fn given_streamable_http_posts_when_answered_then_should_guard_origin_version_and_notifications()
     {
        let bridge = bridge().with_allowed_origins(vec!["https://app.example.com".to_owned()]);
        let headers = |pairs: &[(&'static str, &'static str)]| {
            let mut headers = HeaderMap::new();
            for (name, value) in pairs {
                headers.insert(*name, value.parse().expect("a valid header value"));
            }
            headers
        };
        let ping = || json!({"jsonrpc": "2.0", "id": 1, "method": "ping"});
        let status = |response: Response| response.status();
        assert_eq!(
            status(
                bridge
                    .http_reply(&headers(&[("origin", "https://evil.example")]), ping())
                    .await
            ),
            StatusCode::FORBIDDEN,
            "a DNS rebinding page is refused"
        );
        assert_eq!(
            status(
                bridge
                    .http_reply(&headers(&[("origin", "https://app.example.com")]), ping())
                    .await
            ),
            StatusCode::OK
        );
        assert_eq!(
            status(bridge.http_reply(&headers(&[]), ping()).await),
            StatusCode::OK,
            "a client without an Origin header is not a browser page"
        );
        assert_eq!(
            status(
                bridge
                    .http_reply(&headers(&[("mcp-protocol-version", "1999-01-01")]), ping())
                    .await
            ),
            StatusCode::BAD_REQUEST
        );
        let accepted = bridge
            .http_reply(
                &headers(&[("mcp-protocol-version", "2025-11-25")]),
                json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
            )
            .await;
        assert_eq!(accepted.status(), StatusCode::ACCEPTED);
        let body = axum::body::to_bytes(accepted.into_body(), 1024)
            .await
            .expect("the body reads");
        assert!(body.is_empty(), "202 carries no body");
    }

    #[cfg(feature = "mcp-http")]
    #[tokio::test]
    async fn given_same_origin_posts_when_answered_then_should_allow_only_hosts_a_rebinding_cannot_reach()
     {
        let bridge = bridge();
        let headers = |host: &str, origin: &str| {
            let mut headers = HeaderMap::new();
            headers.insert("host", host.parse().expect("a valid host header"));
            headers.insert("origin", origin.parse().expect("a valid origin header"));
            headers
        };
        let ping = || json!({"jsonrpc": "2.0", "id": 1, "method": "ping"});
        for (host, origin) in [
            ("127.0.0.1:8080", "http://127.0.0.1:8080"),
            ("[::1]:8080", "http://[::1]:8080"),
            ("localhost:8080", "http://localhost:8080"),
        ] {
            assert_eq!(
                bridge
                    .http_reply(&headers(host, origin), ping())
                    .await
                    .status(),
                StatusCode::OK,
                "a page the bridge itself serves on {host}"
            );
        }
        for (host, origin) in [
            ("evil.example:8080", "http://evil.example:8080"),
            ("127.0.0.1:8080", "http://127.0.0.1:9090"),
            ("127.0.0.1:8080", "null"),
        ] {
            assert_eq!(
                bridge
                    .http_reply(&headers(host, origin), ping())
                    .await
                    .status(),
                StatusCode::FORBIDDEN,
                "{origin} on {host}"
            );
        }
    }

    #[test]
    fn given_a_new_bridge_when_built_then_should_stamp_its_own_id_as_the_hop_path() {
        assert_eq!(bridge().hops, ["mcp-edge"]);
    }

    #[test]
    fn given_an_upstream_hop_path_when_continued_then_should_append_this_bridge() {
        let bridge = bridge()
            .with_bridge_hops(&["a2a-edge".to_owned()])
            .expect("no loop");
        assert_eq!(bridge.hops, ["a2a-edge", "mcp-edge"]);
        assert!(matches!(
            bridge.with_bridge_hops(&["mcp-edge".to_owned()]),
            Err(LaserError::Invalid(_))
        ));
    }

    #[test]
    fn given_a_tool_call_when_mapped_then_params_should_tunnel_byte_identical() {
        let params = br#"{"name":"search","arguments":{"q":"laser"},"_meta":{"trace":"abc"}}"#;
        let envelope = tool_call_from_request(
            agdx::RecordId::from_u128(1),
            agdx::ConversationId::from_u128(2),
            "source-agent".parse().expect("valid agent id"),
            agdx::CorrelationId::from_u128(4),
            "search",
            params.to_vec(),
        );
        agdx::validate(&envelope).expect("mapped tool call validates");
        let payload = encode_named(&envelope).expect("encodes");
        let back: AgentEnvelope = decode_named(&payload).expect("decodes");
        assert_eq!(back.tool.as_deref(), Some("search"));
        assert_eq!(back.body, params.to_vec());

        let response = AgentEnvelope::response(
            agdx::RecordId::from_u128(5),
            agdx::ConversationId::from_u128(2),
            "source-agent".parse().expect("valid agent id"),
            agdx::CorrelationId::from_u128(4),
            b"found".to_vec(),
        );
        let result = tool_result_from_envelope(&response);
        assert_eq!(
            to_string(&result).expect("result serializes"),
            r#"{"content":[{"type":"text","text":"found"}]}"#
        );

        let error = AgentEnvelope::error(
            agdx::RecordId::from_u128(6),
            agdx::ConversationId::from_u128(2),
            "source-agent".parse().expect("valid agent id"),
            agdx::CorrelationId::from_u128(4),
            b"failed".to_vec(),
        );
        assert!(tool_result_from_envelope(&error).is_error);
    }
}
