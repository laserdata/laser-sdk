use crate::harness;
use bytes::Bytes;
use laser_sdk::prelude::full::*;
use laser_sdk::wire::agent::{
    AgentErrorBody, AgentErrorCode, AgentId as WireAgentId, ConversationId as WireConversationId,
};
use std::time::Duration;

// An approver that resolves every interrupt by approving, via `respond_input`.
struct Approver;

impl AgentHandler for Approver {
    async fn handle(&self, _message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        ctx.respond_input(AgentTopic::Responses, Bytes::from_static(b"approved"))
            .await
    }
}

// An approver that rejects every interrupt with an AGDX `error` terminal.
struct Rejecter;

impl AgentHandler for Rejecter {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let envelope = message
            .envelope
            .as_ref()
            .expect("the interrupt arrives as an AGDX command");
        let correlation = envelope
            .correlation
            .expect("the interrupt command carries a correlation");
        let error = AgentErrorBody {
            code: AgentErrorCode::Unauthorized,
            message: Some("denied by policy".to_owned()),
            retryable: false,
            detail: None,
        };
        ctx.laser()
            .agdx(
                AgentTopic::Responses,
                "approver".parse().expect("approver is a valid agent id"),
                envelope.conversation,
            )
            .fail(correlation, &error)?
            .send()
            .await?;
        Ok(())
    }
}

fn orchestrator(laser: &Laser) -> laser_sdk::agent::Agdx {
    laser.agdx(
        AgentTopic::HumanInput,
        "orchestrator"
            .parse::<WireAgentId>()
            .expect("orchestrator is a valid agent id"),
        WireConversationId::from(ConversationId::new()),
    )
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_an_approver_when_requesting_input_then_should_resume_with_the_decision() {
    let laser = harness::laser().await;
    Agent::builder()
        .id("approver".parse().expect("approver is a valid agent id"))
        .listen_on(AgentTopic::HumanInput)
        .handler(Approver)
        .build()
        .spawn(laser.clone());

    let decision = orchestrator(&laser)
        .request_input(
            AgentTopic::Responses,
            Bytes::from_static(b"approve a $500 credit?"),
            Duration::from_secs(10),
        )
        .await
        .expect("the approver should resolve the interrupt before the timeout");

    assert_eq!(decision.as_slice(), b"approved");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_rejecter_when_requesting_input_then_should_surface_a_rejected_error() {
    let laser = harness::laser().await;
    Agent::builder()
        .id("approver".parse().expect("approver is a valid agent id"))
        .listen_on(AgentTopic::HumanInput)
        .handler(Rejecter)
        .build()
        .spawn(laser.clone());

    let result = orchestrator(&laser)
        .request_input(
            AgentTopic::Responses,
            Bytes::from_static(b"approve a $500 credit?"),
            Duration::from_secs(10),
        )
        .await;

    assert!(
        matches!(result, Err(LaserError::Rejected(ref reason)) if reason == "denied by policy"),
        "an error reply must surface as Rejected, got {result:?}",
    );
}

// A handler that gates on a human decision via `ctx.approval_gate`, then reports
// the decision on the audit topic so the test can observe it.
struct Gatekeeper;

impl AgentHandler for Gatekeeper {
    async fn handle(&self, _message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let decision = ctx
            .approval_gate(
                AgentTopic::Responses,
                Bytes::from_static(b"approve a $500 credit?"),
                Duration::from_secs(10),
            )
            .await?;
        ctx.reply_on(AgentTopic::Audit, decision).await
    }
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_handler_gating_on_a_human_when_approved_then_should_resume_with_the_decision() {
    let laser = harness::laser().await;
    Agent::builder()
        .id("approver".parse().expect("approver is a valid agent id"))
        .listen_on(AgentTopic::HumanInput)
        .handler(Approver)
        .build()
        .spawn(laser.clone());
    let mut gatekeeper = Agent::builder()
        .id("gatekeeper"
            .parse()
            .expect("gatekeeper is a valid agent id"))
        .listen_on(AgentTopic::ToolCalls)
        .respond_on(AgentTopic::Responses)
        .handler(Gatekeeper)
        .build()
        .spawn(laser.clone());
    gatekeeper
        .ready()
        .await
        .expect("gatekeeper joins its group");

    let trigger = Provenance::builder()
        .conversation_id(ConversationId::new())
        .build();
    let conversation = trigger.conversation_id;
    laser
        .send_agent(AgentTopic::ToolCalls, Bytes::from_static(b"go"), &trigger)
        .await
        .expect("the trigger should be sent");

    let decision = harness::eventually(|| {
        let laser = laser.clone();
        async move {
            let audit = ContextAssembler::builder()
                .conversation_id(conversation)
                .topics(vec![AgentTopic::Audit])
                .build()
                .assemble(&laser)
                .await
                .expect("reading the audit topic should succeed");
            audit
                .into_iter()
                .find(|m| m.payload == b"approved")
                .map(|m| m.payload)
        }
    })
    .await;

    assert_eq!(decision.as_slice(), b"approved");
    gatekeeper.shutdown().await.expect("gatekeeper shuts down");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_no_approver_when_requesting_input_then_should_time_out() {
    let laser = harness::laser().await;
    let result = orchestrator(&laser)
        .request_input(
            AgentTopic::Responses,
            Bytes::from_static(b"unanswered"),
            Duration::from_millis(300),
        )
        .await;

    assert!(
        matches!(result, Err(LaserError::Timeout(_))),
        "an unanswered interrupt must time out, got {result:?}",
    );
}
