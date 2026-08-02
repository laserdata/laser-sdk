use crate::harness;
use bytes::Bytes;
use laser_sdk::prelude::full::*;
use std::time::Duration;

struct ToolRunner;

impl AgentHandler for ToolRunner {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let output = Bytes::from(format!(
            "result: {}",
            String::from_utf8_lossy(&message.payload)
        ));
        ctx.reply_on(AgentTopic::ToolResults, output).await
    }
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_tool_runner_when_requesting_then_should_await_the_correlated_reply() {
    let laser = harness::laser().await;
    Agent::builder()
        .id("tool".parse().expect("tool is a valid agent id"))
        .listen_on(AgentTopic::ToolCalls)
        .handler(ToolRunner)
        .build()
        .spawn(laser.clone());

    let correlation = Provenance::builder()
        .conversation_id(ConversationId::new())
        .build();
    let reply = laser
        .request(
            AgentTopic::ToolCalls,
            AgentTopic::ToolResults,
            Bytes::from_static(b"search"),
            &correlation,
            Duration::from_secs(10),
        )
        .await
        .expect("the tool result should arrive before the timeout");

    assert_eq!(reply.payload.as_slice(), b"result: search");
    assert_eq!(
        reply
            .provenance
            .agent
            .as_ref()
            .expect("agent should be set")
            .as_str(),
        "tool"
    );
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_no_responder_when_requesting_then_should_time_out() {
    let laser = harness::laser().await;
    let correlation = Provenance::builder()
        .conversation_id(ConversationId::new())
        .build();
    let result = laser
        .request(
            AgentTopic::ToolCalls,
            AgentTopic::ToolResults,
            Bytes::from_static(b"unanswered"),
            &correlation,
            Duration::from_millis(300),
        )
        .await;
    assert!(matches!(result, Err(LaserError::Timeout("reply"))));
}
