use crate::harness;
use bytes::Bytes;
use laser_sdk::prelude::full::*;
use std::time::Duration;

struct ToolRunner;

struct SameTopicRunner;

impl AgentHandler for ToolRunner {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let output = Bytes::from(format!(
            "result: {}",
            String::from_utf8_lossy(&message.payload)
        ));
        ctx.reply_on(AgentTopic::Sessions, output).await
    }
}

impl AgentHandler for SameTopicRunner {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        if message.payload == b"search" {
            ctx.reply_on(
                AgentTopic::Sessions,
                Bytes::from_static(b"same-topic-result"),
            )
            .await?;
        }
        Ok(())
    }
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_tool_runner_when_requesting_then_should_await_the_correlated_reply() {
    let laser = harness::laser().await;
    let _agent_lifetime_1 = Agent::builder()
        .id("tool".parse().expect("tool is a valid agent id"))
        .listen_on(AgentTopic::Sessions)
        .handler(ToolRunner)
        .build()
        .spawn(laser.clone());

    let correlation = Provenance::builder()
        .conversation_id(ConversationId::new())
        .build();
    let reply = laser
        .request(
            AgentTopic::Sessions,
            AgentTopic::Sessions,
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
async fn given_one_topic_for_request_and_reply_then_should_skip_the_request_address() {
    let laser = harness::laser().await;
    let _agent_lifetime = Agent::builder()
        .id("same-topic".parse().expect("valid agent id"))
        .listen_on(AgentTopic::Sessions)
        .handler(SameTopicRunner)
        .build()
        .spawn(laser.clone());
    let provenance = Provenance::builder()
        .conversation_id(ConversationId::new())
        .build();
    let reply = laser
        .request(
            AgentTopic::Sessions,
            AgentTopic::Sessions,
            Bytes::from_static(b"search"),
            &provenance,
            Duration::from_secs(10),
        )
        .await
        .expect("same-topic reply arrives");
    assert_eq!(reply.payload.as_slice(), b"same-topic-result");
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
            AgentTopic::Sessions,
            AgentTopic::Sessions,
            Bytes::from_static(b"unanswered"),
            &correlation,
            Duration::from_millis(300),
        )
        .await;
    assert!(matches!(result, Err(LaserError::Timeout("reply"))));
}

struct MisaddressedFirst;

impl AgentHandler for MisaddressedFirst {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        if message.payload != b"search" {
            return Ok(());
        }
        let misaddressed = Provenance::builder()
            .conversation_id(message.provenance.conversation_id)
            .maybe_correlation_id(message.provenance.correlation_id.clone())
            .agent("tool".parse().expect("valid agent id"))
            .target_agent_id("bystander".parse().expect("valid agent id"))
            .build();
        ctx.send(
            AgentTopic::Sessions,
            Bytes::from_static(b"not-yours"),
            &misaddressed,
        )
        .await?;
        ctx.reply_on(AgentTopic::Sessions, Bytes::from_static(b"yours"))
            .await
    }
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_correlated_reply_addressed_to_another_agent_when_requesting_then_should_wait_for_the_requester_reply()
 {
    let laser = harness::laser().await;
    let _agent_lifetime = Agent::builder()
        .id("tool".parse().expect("valid agent id"))
        .listen_on(AgentTopic::Sessions)
        .handler(MisaddressedFirst)
        .build()
        .spawn(laser.clone());
    let provenance = Provenance::builder()
        .conversation_id(ConversationId::new())
        .agent("planner".parse().expect("valid agent id"))
        .build();
    let reply = laser
        .request(
            AgentTopic::Sessions,
            AgentTopic::Sessions,
            Bytes::from_static(b"search"),
            &provenance,
            Duration::from_secs(10),
        )
        .await
        .expect("the requester's reply arrives");
    assert_eq!(reply.payload.as_slice(), b"yours");
}
