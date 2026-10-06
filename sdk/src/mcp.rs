use crate::agent::Laser;
use crate::bridge_hops::{enter_bridge, hops_metadata};
use crate::error::LaserError;
use crate::provenance::AgentTopic;
use crate::types::ConversationId;
#[cfg(feature = "mcp-http")]
use axum::Router;
#[cfg(feature = "mcp-http")]
use axum::extract::State;
#[cfg(feature = "mcp-http")]
use axum::routing::post;
use laser_wire::agent::{
    self as agdx, AgentEnvelope, AgentId, CorrelationId, METADATA_BRIDGE_HOPS,
};
use laser_wire::content::ContentType;
use serde::{Deserialize, Serialize};
use serde_json::to_value;
use serde_json::{Value as JsonValue, json};
use std::str::FromStr;
#[cfg(feature = "mcp-http")]
use std::sync::Arc;
use std::time::Duration;
use strum::{Display, EnumString};

const JSONRPC_VERSION: &str = "2.0";
const APP_ERROR_CODE: i32 = -32000;
// Echoed when the client sends no protocolVersion (an MCP server otherwise
// echoes the client's requested version on initialize).
const DEFAULT_PROTOCOL_VERSION: &str = "2025-11-25";
const DEFAULT_CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// MCP JSON-RPC methods the bridge serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, EnumString)]
pub enum McpMethod {
    #[strum(serialize = "initialize")]
    Initialize,
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
        source: AgentId,
        tool_topic: AgentTopic<'static>,
        reply_topic: AgentTopic<'static>,
        server_name: impl Into<String>,
    ) -> Self {
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
        }
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

    /// Advertise a tool in `tools/list`. `input_schema` is the raw JSON Schema.
    pub fn with_tool(
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
        self.with_tool(
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
        .with_tool(
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

    /// `initialize`: echo the client's protocol version and advertise the
    /// capabilities the bridge actually serves.
    pub fn initialize(&self, protocol_version: Option<&str>) -> JsonValue {
        let mut capabilities = serde_json::Map::new();
        capabilities.insert("tools".to_owned(), json!({}));
        if !self.resources.is_empty() {
            capabilities.insert("resources".to_owned(), json!({}));
        }
        if !self.prompts.is_empty() {
            capabilities.insert("prompts".to_owned(), json!({}));
        }
        json!({
            "protocolVersion": protocol_version.unwrap_or(DEFAULT_PROTOCOL_VERSION),
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
        Ok(json!({ "description": entry.prompt.description, "messages": messages }))
    }

    /// `tools/call`: map onto an AGDX `command`, await the correlated terminal,
    /// render the MCP tool result.
    pub async fn call_tool(
        &self,
        name: &str,
        params_json: Vec<u8>,
    ) -> Result<McpToolResult, LaserError> {
        let conversation = ConversationId::new();
        let correlation = CorrelationId::from_u128(conversation.as_u128());
        // Seed the reply reader at the topic tail before sending, so it reads only
        // the reply rather than the topic's history.
        let mut reader = self
            .laser
            .agdx_reply_reader(self.reply_topic.clone())
            .await?;
        self.laser
            .agdx(
                self.tool_topic.clone(),
                self.source.clone(),
                conversation.into(),
            )
            .command(correlation, params_json)
            .with_tool(name.to_owned())
            .with_metadata(METADATA_BRIDGE_HOPS, hops_metadata(&self.hops))
            .content_type(ContentType::Json)
            .send()
            .await?;
        let envelope = self
            .laser
            .await_agdx_reply(&mut reader, correlation, self.timeout)
            .await?;
        Ok(tool_result_from_envelope(&envelope))
    }

    pub async fn handle_rpc(&self, request: JsonValue) -> McpRpcResponse {
        let id = request.get("id").cloned().unwrap_or(JsonValue::Null);
        match self.dispatch_rpc(&request).await {
            Ok(result) => McpRpcResponse {
                jsonrpc: JSONRPC_VERSION,
                id,
                result: Some(result),
                error: None,
            },
            Err(error) => {
                tracing::warn!(error = %error, "MCP request failed");
                McpRpcResponse {
                    jsonrpc: JSONRPC_VERSION,
                    id,
                    result: None,
                    error: Some(McpRpcError {
                        code: APP_ERROR_CODE,
                        message: crate::error::public_error_message(&error).to_owned(),
                    }),
                }
            }
        }
    }

    async fn dispatch_rpc(&self, request: &JsonValue) -> Result<JsonValue, LaserError> {
        // Serde also reads a struct from an array, so the object check comes first.
        if !request.is_object() {
            return Err(LaserError::HandlerConfig(
                "MCP JSON-RPC request must be an object".to_owned(),
            ));
        }
        let McpRpcRequest { method, params, .. } = McpRpcRequest::deserialize(request)
            .map_err(|_| LaserError::HandlerConfig("MCP method must be a string".to_owned()))?;
        let value = if params.is_null() {
            JsonValue::Object(Default::default())
        } else {
            params
        };
        let params = value
            .as_object()
            .ok_or_else(|| LaserError::HandlerConfig("MCP params must be an object".to_owned()))?;
        let required = |key| {
            params
                .get(key)
                .and_then(JsonValue::as_str)
                .ok_or_else(|| LaserError::HandlerConfig(format!("MCP {key} must be a string")))
        };
        match McpMethod::from_str(&method) {
            Ok(McpMethod::Initialize) => {
                let version = params
                    .get("protocolVersion")
                    .map(|value| {
                        value.as_str().ok_or_else(|| {
                            LaserError::HandlerConfig(
                                "MCP protocolVersion must be a string".to_owned(),
                            )
                        })
                    })
                    .transpose()?;
                Ok(self.initialize(version))
            }
            Ok(McpMethod::ToolsList) => Ok(self.list_tools()),
            Ok(McpMethod::ResourcesList) => Ok(self.list_resources()),
            Ok(McpMethod::ResourcesRead) => self.read_resource(required("uri")?),
            Ok(McpMethod::PromptsList) => Ok(self.list_prompts()),
            Ok(McpMethod::PromptsGet) => self.get_prompt(required("name")?),
            Ok(McpMethod::ToolsCall) => {
                let name = required("name")?;
                let body = serde_json::to_vec(&value)
                    .map_err(|error| LaserError::Codec(error.to_string()))?;
                let result = self.call_tool(name, body).await?;
                to_value(result).map_err(|error| LaserError::Codec(error.to_string()))
            }
            Err(_) => Err(LaserError::HandlerConfig(format!(
                "unknown MCP method `{method}`"
            ))),
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
    axum::Json(request): axum::Json<JsonValue>,
) -> axum::Json<McpRpcResponse> {
    axum::Json(bridge.handle_rpc(request).await)
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
            "mcp-edge".parse().expect("valid agent id"),
            AgentTopic::Commands,
            AgentTopic::Responses,
            "tools",
        )
    }

    #[tokio::test]
    async fn given_rpc_catalog_requests_when_dispatched_then_should_work_without_http() {
        let bridge = bridge();
        let response = bridge
            .handle_rpc(json!({"id": "init", "method": "initialize"}))
            .await;
        assert_eq!(response.id, "init");
        assert!(response.error.is_none());
        assert_eq!(
            response.result.expect("initialize succeeds")["protocolVersion"],
            DEFAULT_PROTOCOL_VERSION
        );
        let response = bridge
            .handle_rpc(json!({"method": "tools/list", "params": null}))
            .await;
        assert_eq!(response.id, JsonValue::Null);
        assert_eq!(response.result, Some(json!({"tools": []})));
    }

    #[tokio::test]
    async fn given_malformed_rpc_requests_when_dispatched_then_should_echo_ids_and_return_public_errors()
     {
        for request in [
            json!(null),
            json!([]),
            json!([7, "tools/list", null]),
            json!({"id": 7}),
            json!({"id": 7, "method": "unknown"}),
            json!({"id": 7, "method": "initialize", "params": {"protocolVersion": null}}),
            json!({"id": 7, "method": "tools/list", "params": []}),
            json!({"id": 7, "method": "resources/read"}),
            json!({"id": 7, "method": "prompts/get", "params": {"name": 12}}),
            json!({"id": 7, "method": "tools/call", "params": {"name": false}}),
        ] {
            let expected = request.get("id").cloned().unwrap_or(JsonValue::Null);
            let response = bridge().handle_rpc(request).await;
            assert_eq!(response.id, expected);
            assert!(response.result.is_none());
            let error = response.error.expect("malformed request returns an error");
            assert_eq!(error.code, -32000);
            assert_eq!(error.message, "invalid request");
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
