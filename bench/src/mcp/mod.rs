use std::sync::Arc;
use std::time::Duration;

use axum::serve::ListenerExt;
use bytes::Bytes;
use laser_sdk::agent::{
    Agent, AgentCtx, AgentHandle, AgentHandler, AgentMessage, ConcurrencyPolicy,
};
use laser_sdk::error::LaserError;
use laser_sdk::laser::Laser;
use laser_sdk::mcp::McpBridge;
use laser_sdk::provenance::AgentTopic;
use laser_wire::agent::AgentId;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use strum::{Display, EnumString, IntoStaticStr};

use crate::BenchError;
use crate::agdx::{
    AgdxArmEvidence, AgdxArmSummary, AgdxCase, measured_arm, seeded_payload, warmup,
};
use crate::engine::Operation;
use transport::HTTP_VERSION;

mod comparison;
mod guaranteed;
mod minimal;
mod transport;
mod triage;

pub use comparison::{
    McpApplicationByteBoundary, McpByteAccounting, McpByteArm, McpByteVerdict,
    McpNetworkByteBoundary, McpTriageEvidence, McpTriageRun, McpTriageSummary,
    run_mcp_triage_evidence,
};
pub use guaranteed::{
    McpGuaranteedEvidence, McpGuaranteedRecoverySummary, McpGuaranteedSummary,
    run_mcp_guaranteed_evidence, run_mcp_guaranteed_recovery,
};
pub use minimal::{McpMinimalEvidence, McpMinimalSummary, run_mcp_minimal_evidence};
pub use triage::{AgdxTriageEvidence, AgdxTriageSummary, run_agdx_triage_evidence};

const TOOL_NAME: &str = "echo";

#[derive(
    Clone, Copy, Debug, Deserialize, Display, EnumString, IntoStaticStr, Serialize, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
#[strum(
    serialize_all = "snake_case",
    parse_err_ty = BenchError,
    parse_err_fn = invalid_mcp_driver
)]
pub enum McpDriver {
    McpBridge,
    McpGuaranteed,
    McpGuaranteedRecovery,
    McpMinimal,
    McpTriage,
}

fn invalid_mcp_driver(value: &str) -> BenchError {
    BenchError::Invalid(format!("unsupported MCP driver `{value}`"))
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct McpBridgeSummary {
    pub native: AgdxArmSummary,
    pub streamable_http: AgdxArmSummary,
    pub request_bytes: usize,
    pub response_bytes: usize,
    pub configuration: Value,
}

pub struct McpBridgeEvidence {
    pub summary: McpBridgeSummary,
    pub native: AgdxArmEvidence,
    pub streamable_http: AgdxArmEvidence,
}

struct McpHttpServer {
    client: reqwest::Client,
    endpoint: String,
    shutdown: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}

struct EchoTool {
    source: AgentId,
}

impl AgentHandler for EchoTool {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let command = message
            .envelope
            .as_ref()
            .ok_or_else(|| LaserError::Handler("MCP tool call has no AGDX envelope".to_owned()))?;
        let correlation = command
            .correlation
            .ok_or_else(|| LaserError::Handler("MCP tool call has no correlation".to_owned()))?;
        ctx.laser()
            .agdx(
                AgentTopic::ToolResults,
                self.source.clone(),
                command.conversation,
            )
            .respond(correlation, command.body.clone())
            .send()
            .await?;
        Ok(())
    }
}

/// Compare the native bridge call with the same call through MCP Streamable HTTP.
///
/// # Errors
///
/// Returns an error when setup, transport, response validation, or measurement fails.
pub async fn run_mcp_bridge_evidence(
    laser: &Laser,
    case: &AgdxCase,
    seed: u64,
    monitored_processes: &[(String, u32)],
) -> Result<McpBridgeEvidence, BenchError> {
    let scoped = prepare_stream(laser, case, seed).await?;
    let mut agent = start_agent(&scoped, case.partitions)?;
    agent.ready().await.map_err(sdk_error)?;
    let bridge = build_bridge(scoped, case)?;
    let server = McpHttpServer::start(Arc::clone(&bridge), case.concurrency).await?;
    let payload = seeded_payload(case.payload_bytes, seed);
    let sample_request = rpc_request(0, &payload);
    let request_bytes = serde_json::to_vec(&sample_request)?.len();
    let sample_response = rpc_response(0, &tool_params(0, &payload));
    let response_bytes = serde_json::to_vec(&sample_response)?.len();
    let result = measure_arms(case, seed, monitored_processes, bridge, &server, payload).await;
    server.stop().await?;
    let agent_result = agent.shutdown().await.map_err(sdk_error);
    let (native, streamable_http) = result?;
    agent_result?;

    Ok(McpBridgeEvidence {
        summary: McpBridgeSummary {
            native: native.summary.clone(),
            streamable_http: streamable_http.summary.clone(),
            request_bytes,
            response_bytes,
            configuration: json!({
                "comparison": "same_mcp_bridge_direct_call_vs_streamable_http",
                "http": "streamable_http_sse_response",
                "http_version": HTTP_VERSION,
                "client": "pooled_reqwest",
                "tcp_nodelay": true,
                "handler": "deterministic_echo",
                "tool": TOOL_NAME,
            }),
        },
        native,
        streamable_http,
    })
}

async fn prepare_stream(laser: &Laser, case: &AgdxCase, seed: u64) -> Result<Laser, BenchError> {
    let scoped = laser.with_default_stream(format!("bench-mcp-bridge-{seed:016x}"));
    for topic in [AgentTopic::ToolCalls, AgentTopic::ToolResults] {
        scoped
            .topic(topic.topic_string())
            .ensure(case.partitions)
            .await
            .map_err(sdk_error)?;
    }
    Ok(scoped)
}

fn start_agent(laser: &Laser, partitions: u32) -> Result<AgentHandle, BenchError> {
    let worker = "laser-bench-mcp-worker"
        .parse()
        .map_err(|error| BenchError::Invalid(format!("invalid MCP worker id: {error}")))?;
    let source = "laser-bench-mcp-worker"
        .parse()
        .map_err(|error| BenchError::Invalid(format!("invalid MCP source id: {error}")))?;
    let max_partitions = usize::try_from(partitions)
        .map_err(|_| BenchError::Invalid("MCP partition count exceeds usize".to_owned()))?;
    Ok(Agent::builder()
        .id(worker)
        .listen_on(AgentTopic::ToolCalls)
        .handler(EchoTool { source })
        .poll_interval(Duration::ZERO)
        .concurrency(ConcurrencyPolicy::SerialPerPartition { max_partitions })
        .build()
        .spawn(laser.clone()))
}

fn build_bridge(laser: Laser, case: &AgdxCase) -> Result<Arc<McpBridge>, BenchError> {
    let source = "laser-bench-mcp-bridge"
        .parse()
        .map_err(|error| BenchError::Invalid(format!("invalid MCP bridge id: {error}")))?;
    Ok(Arc::new(
        McpBridge::new(
            laser,
            source,
            AgentTopic::ToolCalls,
            AgentTopic::ToolResults,
            "laser-bench",
        )
        .with_tool(
            TOOL_NAME,
            Some("Echo a deterministic benchmark payload".to_owned()),
            json!({"type": "object"}),
        )
        .with_timeout(Duration::from_millis(case.timeout_millis)),
    ))
}

impl McpHttpServer {
    async fn start(bridge: Arc<McpBridge>, concurrency: usize) -> Result<Self, BenchError> {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .map_err(|error| {
                BenchError::Invalid(format!("failed to bind MCP listener: {error}"))
            })?;
        let address = listener.local_addr().map_err(|error| {
            BenchError::Invalid(format!("failed to read MCP listener address: {error}"))
        })?;
        let listener = listener.tap_io(|stream| {
            stream
                .set_nodelay(true)
                .expect("benchmark MCP server must enable TCP_NODELAY");
        });
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, bridge.router())
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
        });
        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(concurrency)
            .http1_only()
            .tcp_nodelay(true)
            .build()
            .map_err(|error| BenchError::Invalid(format!("failed to build MCP client: {error}")))?;
        Ok(Self {
            client,
            endpoint: format!("http://{address}/"),
            shutdown,
            task,
        })
    }

    async fn stop(self) -> Result<(), BenchError> {
        let _ = self.shutdown.send(());
        self.task
            .await
            .map_err(|error| BenchError::Invalid(format!("MCP server task failed: {error}")))?
            .map_err(|error| BenchError::Invalid(format!("MCP server failed: {error}")))
    }
}

async fn measure_arms(
    case: &AgdxCase,
    seed: u64,
    monitored_processes: &[(String, u32)],
    bridge: Arc<McpBridge>,
    server: &McpHttpServer,
    payload: Bytes,
) -> Result<(AgdxArmEvidence, AgdxArmEvidence), BenchError> {
    let timeout = Duration::from_millis(case.timeout_millis);
    let native = native_operation(bridge, payload.clone());
    let http = http_operation(server.client.clone(), server.endpoint.clone(), payload);
    warmup(case, timeout, Arc::clone(&native)).await?;
    warmup(case, timeout, Arc::clone(&http)).await?;
    if seed.is_multiple_of(2) {
        let native = measured_arm(
            "mcp_bridge_native",
            1,
            case,
            timeout,
            native,
            monitored_processes,
        )
        .await?;
        let http = measured_arm(
            "mcp_bridge_streamable_http",
            2,
            case,
            timeout,
            http,
            monitored_processes,
        )
        .await?;
        Ok((native, http))
    } else {
        let http = measured_arm(
            "mcp_bridge_streamable_http",
            1,
            case,
            timeout,
            http,
            monitored_processes,
        )
        .await?;
        let native = measured_arm(
            "mcp_bridge_native",
            2,
            case,
            timeout,
            native,
            monitored_processes,
        )
        .await?;
        Ok((native, http))
    }
}

fn native_operation(bridge: Arc<McpBridge>, payload: Bytes) -> Operation {
    Arc::new(move |sequence| {
        let bridge = Arc::clone(&bridge);
        let payload = payload.clone();
        Box::pin(async move {
            let params = tool_params(sequence, &payload);
            let body = serde_json::to_vec(&params).map_err(|error| error.to_string())?;
            let expected = String::from_utf8(body.clone()).map_err(|error| error.to_string())?;
            let result = bridge
                .call_tool(TOOL_NAME, body)
                .await
                .map_err(|error| error.to_string())?;
            validate_tool_result(&result, &expected)
        })
    })
}

fn http_operation(client: reqwest::Client, endpoint: String, payload: Bytes) -> Operation {
    Arc::new(move |sequence| {
        let client = client.clone();
        let endpoint = endpoint.clone();
        let payload = payload.clone();
        Box::pin(async move {
            let request = rpc_request(sequence, &payload);
            let expected =
                serde_json::to_string(&request["params"]).map_err(|error| error.to_string())?;
            let response = client
                .post(endpoint)
                .json(&request)
                .send()
                .await
                .map_err(|error| error.to_string())?;
            if !response.status().is_success() {
                return Err(format!("MCP HTTP returned {}", response.status()));
            }
            let body = response
                .json::<Value>()
                .await
                .map_err(|error| error.to_string())?;
            let actual = body["result"]["content"][0]["text"]
                .as_str()
                .ok_or_else(|| "MCP response has no text result".to_owned())?;
            if actual != expected {
                return Err("MCP HTTP response body did not match the request".to_owned());
            }
            Ok(())
        })
    })
}

fn tool_params(sequence: u64, payload: &Bytes) -> Value {
    let payload = payload
        .iter()
        .map(|byte| char::from(b'a' + (byte % 26)))
        .collect::<String>();
    json!({
        "name": TOOL_NAME,
        "arguments": {
            "sequence": sequence,
            "payload": payload,
        }
    })
}

fn rpc_request(sequence: u64, payload: &Bytes) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": sequence,
        "method": "tools/call",
        "params": tool_params(sequence, payload),
    })
}

fn rpc_response(sequence: u64, params: &Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": sequence,
        "result": {
            "content": [{"type": "text", "text": params.to_string()}]
        }
    })
}

fn validate_tool_result(
    result: &laser_sdk::mcp::McpToolResult,
    expected: &str,
) -> Result<(), String> {
    if result.is_error || result.content.len() != 1 || result.content[0].text != expected {
        return Err("native MCP bridge result did not match the request".to_owned());
    }
    Ok(())
}

fn sdk_error(error: impl std::fmt::Display) -> BenchError {
    BenchError::Invalid(format!("MCP benchmark SDK operation failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_snake_case_driver_when_parsed_then_should_round_trip() {
        let driver = "mcp_bridge"
            .parse::<McpDriver>()
            .expect("MCP driver parses");
        assert_eq!(driver, McpDriver::McpBridge);
        assert_eq!(driver.to_string(), "mcp_bridge");
    }

    #[test]
    fn given_tool_payload_when_encoded_then_should_preserve_sequence_and_size() {
        let payload = Bytes::from_static(b"laser");
        let request = rpc_request(7, &payload);
        assert_eq!(request["id"], 7);
        assert_eq!(
            request["params"]["arguments"]["payload"]
                .as_str()
                .expect("payload is text")
                .len(),
            payload.len()
        );
    }
}
