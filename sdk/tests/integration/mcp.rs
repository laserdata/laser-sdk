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
                AgentTopic::ToolResults,
                "tool-worker"
                    .parse()
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
    Agent::builder()
        .id("tool-worker"
            .parse()
            .expect("tool-worker is a valid agent id"))
        .listen_on(AgentTopic::ToolCalls)
        .handler(Tool)
        .build()
        .spawn(laser.clone());

    let bridge = McpBridge::new(
        laser.clone(),
        "mcp-bridge"
            .parse()
            .expect("mcp-bridge is a valid agent id"),
        AgentTopic::ToolCalls,
        AgentTopic::ToolResults,
        "test-server",
    )
    .with_tool(
        "search",
        Some("search the corpus".to_owned()),
        json!({"type": "object"}),
    )
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
