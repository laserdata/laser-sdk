use std::sync::Arc;
use std::time::Duration;

use rmcp::handler::server::{router::tool::ToolRouter, wrapper::Parameters};
use rmcp::model::{CallToolRequestParams, ContentBlock, ServerCapabilities, ServerInfo};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use rmcp::{Peer, RoleClient, ServerHandler, tool, tool_handler, tool_router};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::TOOL_NAME;
use super::transport::{HTTP_VERSION, McpTransport};
use crate::BenchError;
use crate::agdx::{AgdxArmEvidence, AgdxArmSummary, AgdxCase, measured_arm_with_network, warmup};
use crate::engine::Operation;
use crate::network::NetworkByteMeasurement;

pub(super) const RMCP_VERSION: &str = "3.1.1";

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct McpMinimalSummary {
    pub streamable_http: AgdxArmSummary,
    pub request_bytes: usize,
    pub response_bytes: usize,
    pub network: NetworkByteMeasurement,
    pub configuration: Value,
}

pub struct McpMinimalEvidence {
    pub summary: McpMinimalSummary,
    pub streamable_http: AgdxArmEvidence,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct EchoRequest {
    sequence: u64,
    payload: String,
}

#[derive(Clone, Debug)]
struct MinimalServer {
    tool_router: ToolRouter<Self>,
}

impl MinimalServer {
    fn new() -> Self {
        Self {
            tool_router: Self::tool_router(),
        }
    }
}

#[tool_router]
impl MinimalServer {
    #[tool(description = "Echo a deterministic benchmark ticket")]
    fn echo(Parameters(request): Parameters<EchoRequest>) -> String {
        expected_response(request.sequence, &request.payload)
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for MinimalServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
    }
}

async fn start_transport(concurrency: usize) -> Result<McpTransport, BenchError> {
    let cancellation = CancellationToken::new();
    let service: StreamableHttpService<MinimalServer, LocalSessionManager> =
        StreamableHttpService::new(
            || Ok(MinimalServer::new()),
            Arc::new(LocalSessionManager::default()),
            StreamableHttpServerConfig::default()
                .with_sse_keep_alive(None)
                .with_cancellation_token(cancellation.child_token()),
        );
    let router = axum::Router::new().nest_service("/mcp", service);
    McpTransport::start(router, cancellation, concurrency).await
}

/// Measure the latency-favorable MCP Streamable HTTP control without AGDX or durable storage.
///
/// # Errors
///
/// Returns an error when MCP setup, request validation, measurement, or shutdown fails.
pub async fn run_mcp_minimal_evidence(
    case: &AgdxCase,
    seed: u64,
    monitored_processes: &[(String, u32)],
) -> Result<McpMinimalEvidence, BenchError> {
    let harness = start_transport(case.concurrency).await?;
    let payload = text_payload(case.payload_bytes, seed);
    let operation = minimal_operation(harness.peer.clone(), payload.clone());
    let timeout = Duration::from_millis(case.timeout_millis);
    warmup(case, timeout, Arc::clone(&operation)).await?;
    let streamable_http = measured_arm_with_network(
        "minimal_mcp_streamable_http",
        1,
        case,
        timeout,
        operation,
        monitored_processes,
        harness.server_port(),
    )
    .await;
    harness.stop().await?;
    let streamable_http = streamable_http?;
    let network = streamable_http.network.clone().ok_or_else(|| {
        BenchError::Invalid("minimal MCP network measurement was not captured".to_owned())
    })?;
    let (request_bytes, response_bytes) = application_bytes(&payload)?;
    Ok(McpMinimalEvidence {
        summary: McpMinimalSummary {
            streamable_http: streamable_http.summary.clone(),
            request_bytes,
            response_bytes,
            network,
            configuration: json!({
                "comparison_role": "minimal_mcp_control",
                "durability": "none",
                "handler": "deterministic_echo",
                "http": "streamable_http_sse_response",
                "http_version": HTTP_VERSION,
                "mcp_sdk": RMCP_VERSION,
                "connection_pool": "reqwest_keep_alive",
                "tcp_nodelay": true,
                "initialization": "outside_timed_region",
            }),
        },
        streamable_http,
    })
}

fn minimal_operation(peer: Peer<RoleClient>, payload: String) -> Operation {
    Arc::new(move |sequence| {
        let peer = peer.clone();
        let payload = payload.clone();
        Box::pin(async move {
            let arguments = serde_json::from_value(ticket_arguments(sequence, &payload))
                .map_err(|error| error.to_string())?;
            let result = peer
                .call_tool(CallToolRequestParams::new(TOOL_NAME).with_arguments(arguments))
                .await
                .map_err(|error| error.to_string())?;
            if result.is_error == Some(true) || result.content.len() != 1 {
                return Err("minimal MCP returned an error or unexpected content count".to_owned());
            }
            let actual = match &result.content[0] {
                ContentBlock::Text(text) => &text.text,
                _ => return Err("minimal MCP returned non-text content".to_owned()),
            };
            if actual != &expected_response(sequence, &payload) {
                return Err("minimal MCP response did not match the ticket".to_owned());
            }
            Ok(())
        })
    })
}

pub(super) fn text_payload(size: usize, seed: u64) -> String {
    let mut state = seed.max(1);
    (0..size)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            char::from(b'a' + state.to_le_bytes()[0] % 26)
        })
        .collect()
}

pub(super) fn expected_response(sequence: u64, payload: &str) -> String {
    format!("{sequence}:{payload}")
}

pub(super) fn ticket_arguments(sequence: u64, payload: &str) -> Value {
    json!({"sequence": sequence, "payload": payload})
}

pub(super) fn application_bytes(payload: &str) -> Result<(usize, usize), BenchError> {
    let request = json!({
        "jsonrpc": "2.0",
        "id": 0,
        "method": "tools/call",
        "params": {
            "name": TOOL_NAME,
            "arguments": ticket_arguments(0, payload),
        },
    });
    let response = json!({
        "jsonrpc": "2.0",
        "id": 0,
        "result": {
            "content": [{"type": "text", "text": expected_response(0, payload)}],
            "isError": false,
        },
    });
    Ok((
        serde_json::to_vec(&request)?.len(),
        serde_json::to_vec(&response)?.len(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_ticket_when_counted_then_should_include_exact_json_rpc_bytes() {
        let payload = "laser";
        let (request, response) = application_bytes(payload).expect("JSON should encode");

        assert!(request > payload.len());
        assert!(response > payload.len());
        assert_eq!(expected_response(7, payload), "7:laser");
    }

    #[tokio::test]
    async fn given_minimal_server_when_called_then_should_round_trip_ticket() {
        let harness = start_transport(2)
            .await
            .expect("minimal MCP harness should start");
        minimal_operation(harness.peer.clone(), "laser".to_owned())(7)
            .await
            .expect("minimal MCP call should match");
        harness
            .stop()
            .await
            .expect("minimal MCP harness should stop");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn given_minimal_mcp_arm_when_measured_then_should_capture_kernel_tcp_bytes() {
        let case = AgdxCase {
            payload_bytes: 64,
            chunks_per_stream: 1,
            operations: 1,
            duration_seconds: 1,
            concurrency: 1,
            partitions: 1,
            warmup_seconds: 1,
            timeout_millis: 1_000,
            offered_rate: None,
            spin_dispatch: false,
            max_in_flight: None,
        };
        let evidence = run_mcp_minimal_evidence(&case, 7, &[])
            .await
            .expect("minimal MCP evidence should complete");

        assert!(evidence.summary.network.complete);
        assert!(evidence.summary.network.client_to_server_bytes > 0);
        assert!(evidence.summary.network.server_to_client_bytes > 0);
    }
}
