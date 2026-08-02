// Edge interop over the log: the same LLM-backed agent, reachable as an A2A
// agent, an MCP tool server, and an AG-UI event stream, all bridged onto the
// Agent Data Exchange Protocol on one Iggy connection. The model is the
// `LlmClient` seam: a deterministic MockLlm by default, a real backend with
// `--features llm-anthropic` (ANTHROPIC_API_KEY) or `--features llm-openai`
// (OPENAI_API_KEY). Nothing in the bridges changes between mock and real.
use laser_examples::{LlmClient, PARTITIONS, default_llm, init_tracing, laser, phase, stream_for};
use laser_sdk::prelude::full::*;
use laser_sdk::wire::agent::{
    AgentId as WireAgentId, ConversationId as WireConversationId, CorrelationId, OPERATION_CHAT,
};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    init_tracing();
    phase("connecting");
    let laser = laser(&stream_for("interop"), Capabilities::OPEN).await?;
    laser.bootstrap(PARTITIONS).await?;
    let llm = default_llm();

    // One worker reachable through A2A (on Commands -> Responses) and another
    // through MCP (on ToolCalls -> ToolResults). Same handler, same model.
    let a2a_worker = Agent::builder()
        .id("assistant".parse()?)
        .listen_on(AgentTopic::Commands)
        .handler(Worker {
            llm: llm.clone(),
            source: "assistant".parse()?,
            reply_topic: AgentTopic::Responses,
        })
        .build()
        .spawn(laser.clone());
    let mcp_worker = Agent::builder()
        .id("tool-runner".parse()?)
        .listen_on(AgentTopic::ToolCalls)
        .handler(Worker {
            llm: llm.clone(),
            source: "tool-runner".parse()?,
            reply_topic: AgentTopic::ToolResults,
        })
        .build()
        .spawn(laser.clone());

    // A2A: SendMessage publishes the task, the worker answers, GetTask completes.
    phase("A2A: SendMessage -> GetTask");
    let a2a = A2aBridge::new(
        laser.clone(),
        "a2a-gateway".parse()?,
        AgentTopic::Commands,
        AgentTopic::Responses,
    );
    let params =
        br#"{"message":{"role":"user","parts":[{"kind":"text","text":"summarize the incident"}]}}"#
            .to_vec();
    let task = a2a.submit(params).await?;
    let completed = poll_until(Duration::from_secs(15), || async {
        let task = a2a.task(&task.id).await?;
        Ok((task.status.state == TaskState::Completed).then_some(task))
    })
    .await?;
    info!(
        "A2A task {} -> {}: {}",
        completed.id,
        completed.status.state,
        completed
            .artifacts
            .first()
            .map(|artifact| artifact.text.as_str())
            .unwrap_or("(no artifact)")
    );

    // MCP: tools/call reaches the same worker and renders the answer as a tool result.
    phase("MCP: initialize / tools/list / tools/call");
    let mcp = McpBridge::new(
        laser.clone(),
        "mcp-gateway".parse()?,
        AgentTopic::ToolCalls,
        AgentTopic::ToolResults,
        "laser-mcp",
    )
    .with_tool(
        "ask",
        Some("ask the assistant a question".to_owned()),
        serde_json::json!({"type": "object", "properties": {"q": {"type": "string"}}}),
    )
    .with_timeout(Duration::from_secs(15));
    info!(
        "MCP tools/list: {}",
        serde_json::to_string(&mcp.list_tools())
            .map_err(|error| LaserError::Codec(error.to_string()))?
    );
    let call = serde_json::to_vec(&serde_json::json!({
        "name": "ask",
        "arguments": {"q": "what is the Agent Data Exchange Protocol?"}
    }))
    .map_err(|error| LaserError::Codec(error.to_string()))?;
    let result = mcp.call_tool("ask", call).await?;
    info!(
        "MCP tools/call -> isError={}, content: {}",
        result.is_error,
        result
            .content
            .first()
            .map(|content| content.text.as_str())
            .unwrap_or("(empty)")
    );

    // AG-UI: stream a chat answer onto the log, then render it as AG-UI events.
    phase("AG-UI: render a chat stream as events");
    let conversation = ConversationId::new();
    let answer = llm.complete("give a one-line status update").await;
    let mut chat = laser
        .agdx(
            AgentTopic::LlmIo,
            "assistant".parse()?,
            WireConversationId::from(conversation),
        )
        .stream(
            CorrelationId::from_u128(conversation.as_u128()),
            OPERATION_CHAT,
        );
    for token in answer.split_inclusive(' ') {
        chat.write(token.as_bytes().to_vec()).await?;
    }
    chat.finish("stop", None).await?;
    for event in laser.agui_events(conversation, AgentTopic::LlmIo).await? {
        info!(
            "AG-UI event: {}",
            serde_json::to_string(&event).map_err(|error| LaserError::Codec(error.to_string()))?
        );
    }

    // Human-in-the-loop: the orchestrator pauses for a human decision, the
    // approver resolves the interrupt it is handling. Built on AGDX
    // command/response, so it rides the same log as everything above.
    phase("Human-in-the-loop: request_input -> respond_input");
    let approver = Agent::builder()
        .id("approver".parse()?)
        .listen_on(AgentTopic::HumanInput)
        .handler(Approver)
        .build()
        .spawn(laser.clone());
    let decision = laser
        .agdx(
            AgentTopic::HumanInput,
            "orchestrator".parse()?,
            WireConversationId::from(ConversationId::new()),
        )
        .request_input(
            AgentTopic::Responses,
            b"approve a $500 refund?".to_vec(),
            Duration::from_secs(15),
        )
        .await?;
    info!("HITL decision: {}", String::from_utf8_lossy(&decision));

    approver.shutdown().await?;
    a2a_worker.shutdown().await?;
    mcp_worker.shutdown().await?;
    Ok(())
}

// The worker behind every bridge: it reads the decoded AGDX command envelope,
// asks the model, and answers with an AGDX `response` echoing the correlation.
// Bridges produce AGDX. The worker only ever speaks AGDX.
struct Worker {
    llm: Arc<dyn LlmClient>,
    source: WireAgentId,
    reply_topic: AgentTopic<'static>,
}

impl AgentHandler for Worker {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        // A message that is not an AGDX command, or one with no correlation to
        // answer, can never become valid on a retry. Reject with a permanent
        // (non-retryable) error so the consumer dead-letters it immediately
        // rather than burning the retry budget first.
        let command = message
            .envelope
            .as_ref()
            .ok_or_else(|| LaserError::Invalid("expected an AGDX command".to_owned()))?;
        let correlation = command
            .correlation
            .ok_or_else(|| LaserError::Invalid("the command carries no correlation".to_owned()))?;
        let prompt = String::from_utf8_lossy(&command.body).into_owned();
        let answer = self.llm.complete(&prompt).await;
        ctx.laser()
            .agdx(
                self.reply_topic.clone(),
                self.source.clone(),
                command.conversation,
            )
            .respond(correlation, answer.into_bytes())
            .send()
            .await?;
        Ok(())
    }
}

// The human behind the interrupt gate: it resolves every `request_input` it is
// handed. A real deployment routes this to a UI or a person.
struct Approver;

impl AgentHandler for Approver {
    async fn handle(&self, _message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        ctx.respond_input(AgentTopic::Responses, b"approved".to_vec())
            .await
    }
}

// Poll `op` until it yields `Some`, or the timeout elapses.
async fn poll_until<T, F, Fut>(timeout: Duration, mut op: F) -> Result<T, LaserError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Option<T>, LaserError>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(value) = op().await? {
            return Ok(value);
        }
        if Instant::now() >= deadline {
            return Err(LaserError::Invalid(
                "timed out waiting for the task".to_owned(),
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
