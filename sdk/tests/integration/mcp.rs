use crate::harness;
use laser_sdk::prelude::full::*;
use serde_json::json;
use std::time::Duration;

struct Tool;

impl AgentHandler for Tool {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        // A tool worker behind the bridge: reads the decoded AGDX command (the
        // tool name in `tool`, the MCP params tunneled in the body) and answers
        // with an AGDX `response` echoing the correlation.
        let command = message
            .envelope
            .as_ref()
            .ok_or_else(|| LaserError::Handler("expected an AGDX command".to_owned()))?;
        let correlation = command
            .correlation
            .ok_or_else(|| LaserError::Handler("the command carries no correlation".to_owned()))?;
        let tool = command.tool.clone().unwrap_or_default();
        let reply = format!("ran {tool}").into_bytes();
        ctx.laser()
            .agdx(
                AgentTopic::Sessions,
                "tool-worker"
                    .parse::<laser_sdk::types::AgentId>()
                    .expect("tool-worker is a valid agent id"),
                command.conversation,
            )
            .respond(correlation, reply)
            .send()
            .await?;
        Ok(())
    }
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_tools_call_when_the_tool_replies_then_should_render_the_mcp_result() {
    let laser = harness::laser().await;
    let _agent_lifetime_1 = Agent::builder()
        .id("tool-worker"
            .parse()
            .expect("tool-worker is a valid agent id"))
        .listen_on(AgentTopic::Sessions)
        .handler(Tool)
        .build()
        .spawn(laser.clone());

    let bridge = McpBridge::new(
        laser.clone(),
        "mcp-bridge"
            .parse::<laser_sdk::types::AgentId>()
            .expect("mcp-bridge is a valid agent id"),
        AgentTopic::Sessions,
        AgentTopic::Sessions,
        "test-server",
    )
    .with_tool(
        "search",
        Some("search the corpus".to_owned()),
        json!({"type": "object"}),
    )
    .expect("an object schema is accepted")
    .with_resource(
        "mem:///readme",
        "readme",
        Some("text/markdown".to_owned()),
        "# Hello",
    )
    .with_prompt(
        McpPrompt {
            name: "greet".to_owned(),
            title: None,
            description: Some("a greeting".to_owned()),
            arguments: vec![McpPromptArgument {
                name: "who".to_owned(),
                description: None,
                required: Some(true),
            }],
        },
        vec![("user".to_owned(), "hi there".to_owned())],
    )
    .with_timeout(Duration::from_secs(15));

    // initialize echoes the client's protocol version and advertises every
    // capability the bridge actually serves.
    let init = bridge.initialize(Some("2025-06-18"));
    assert_eq!(init["protocolVersion"], "2025-06-18");
    assert_eq!(init["serverInfo"]["name"], "test-server");
    assert!(init["capabilities"]["tools"].is_object());
    assert!(init["capabilities"]["resources"].is_object());
    assert!(init["capabilities"]["prompts"].is_object());

    // with no client-requested version, the default pin answers.
    let default_init = bridge.initialize(None);
    assert_eq!(default_init["protocolVersion"], "2025-11-25");

    // tools/list shows the advertised tool.
    let tools = bridge.list_tools();
    assert_eq!(tools["tools"][0]["name"], "search");

    // resources/list + resources/read.
    assert_eq!(
        bridge.list_resources()["resources"][0]["uri"],
        "mem:///readme"
    );
    let read = bridge
        .read_resource("mem:///readme")
        .expect("the resource reads");
    assert_eq!(read["contents"][0]["text"], "# Hello");
    assert!(bridge.read_resource("mem:///missing").is_err());

    // prompts/list + prompts/get.
    assert_eq!(bridge.list_prompts()["prompts"][0]["name"], "greet");
    let prompt = bridge.get_prompt("greet").expect("the prompt renders");
    assert_eq!(prompt["messages"][0]["content"]["text"], "hi there");

    // tools/call maps to an AGDX command, awaits the worker's AGDX response, and
    // renders the MCP result.
    let params = serde_json::to_vec(&json!({"name": "search", "arguments": {"q": "laser"}}))
        .expect("params serialize");
    let result = bridge
        .call_tool("search", params)
        .await
        .expect("the tool call completes");
    assert!(!result.is_error);
    assert_eq!(result.content.len(), 1);
    assert_eq!(result.content[0].text, "ran search");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_tools_call_in_a_parent_session_when_the_tool_replies_then_should_run_as_a_completed_child()
 {
    use laser_sdk::wire::agent::{SessionStart, TaskState};
    use laser_sdk::wire::framing::decode_named;
    let laser = harness::laser().await;
    let _agent_lifetime = Agent::builder()
        .id("tool-worker"
            .parse()
            .expect("tool-worker is a valid agent id"))
        .listen_on(AgentTopic::Sessions)
        .handler(Tool)
        .build()
        .spawn(laser.clone());
    let bridge = McpBridge::new(
        laser.clone(),
        "mcp-bridge"
            .parse::<laser_sdk::types::AgentId>()
            .expect("mcp-bridge is a valid agent id"),
        AgentTopic::Sessions,
        AgentTopic::Sessions,
        "test-server",
    )
    .with_timeout(Duration::from_secs(15));
    let parent = ConversationId::new();
    let result = bridge
        .call_tool_in(parent, parent, "search", br#"{"q":"x"}"#.to_vec())
        .await
        .expect("the tool call completes");
    assert!(!result.is_error);
    let starts = harness::eventually(|| async {
        let records = ContextAssembler::builder()
            .conversation_id(parent)
            .across_subconversations(true)
            .build()
            .assemble(&laser)
            .await
            .ok()?;
        let states: Vec<_> = records
            .iter()
            .filter_map(|record| {
                let envelope = record.envelope.as_ref()?;
                if envelope.operation.as_deref() != Some("session") {
                    return None;
                }
                Some((envelope.task_state?, envelope.body.clone()))
            })
            .collect();
        states
            .iter()
            .any(|state| state.0 == TaskState::Completed)
            .then_some(states)
    })
    .await;
    assert_eq!(starts[0].0, TaskState::Submitted);
    let start: SessionStart = decode_named(&starts[0].1).expect("the child start decodes");
    assert_eq!(start.parent, Some(parent.into()));
}

// A tool worker that answers in its own name and counts the calls it runs.
struct NamedTool {
    name: &'static str,
    calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl AgentHandler for NamedTool {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let Some(command) = message.envelope.as_ref() else {
            return Ok(());
        };
        let (Some(correlation), Some(_tool)) = (command.correlation, command.tool.as_ref()) else {
            return Ok(());
        };
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        ctx.laser()
            .agdx(
                AgentTopic::Sessions,
                self.name
                    .parse::<laser_sdk::types::AgentId>()
                    .expect("the worker name is a valid agent id"),
                command.conversation,
            )
            .respond(correlation, self.name.as_bytes().to_vec())
            .send()
            .await?;
        Ok(())
    }
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_two_tool_workers_when_calling_one_then_only_that_worker_should_run_the_tool() {
    let laser = harness::laser().await;
    let alpha = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let beta = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let _workers: Vec<_> = [("alpha", alpha.clone()), ("beta", beta.clone())]
        .into_iter()
        .map(|(name, calls)| {
            Agent::builder()
                .id(name.parse().expect("the worker name is a valid agent id"))
                .listen_on(AgentTopic::Sessions)
                .handler(NamedTool { name, calls })
                .build()
                .spawn(laser.clone())
        })
        .collect();
    let bridge = McpBridge::new(
        laser.clone(),
        "mcp-bridge"
            .parse::<laser_sdk::types::AgentId>()
            .expect("mcp-bridge is a valid agent id"),
        AgentTopic::Sessions,
        AgentTopic::Sessions,
        "test-server",
    )
    .with_timeout(Duration::from_secs(15));
    let result = bridge
        .call_tool_to(
            "beta"
                .parse::<laser_sdk::types::AgentId>()
                .expect("beta is a valid agent id"),
            "search",
            br#"{"q":"x"}"#.to_vec(),
        )
        .await
        .expect("the addressed tool call completes");
    assert_eq!(result.content[0].text, "beta");
    let parent = ConversationId::new();
    let result = bridge
        .call_tool_in_to(
            "alpha"
                .parse::<laser_sdk::types::AgentId>()
                .expect("alpha is a valid agent id"),
            parent,
            parent,
            "search",
            br#"{"q":"y"}"#.to_vec(),
        )
        .await
        .expect("the addressed child tool call completes");
    assert_eq!(result.content[0].text, "alpha");
    assert_eq!(alpha.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(beta.load(std::sync::atomic::Ordering::SeqCst), 1);
}
