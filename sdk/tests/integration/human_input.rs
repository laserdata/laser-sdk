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
        ctx.respond_input(AgentTopic::Sessions, Bytes::from_static(b"approved"))
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
                AgentTopic::Sessions,
                "approver"
                    .parse::<laser_sdk::types::AgentId>()
                    .expect("approver is a valid agent id"),
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
        AgentTopic::Sessions,
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
    let _agent_lifetime_1 = Agent::builder()
        .id("approver".parse().expect("approver is a valid agent id"))
        .listen_on(AgentTopic::Sessions)
        .handler(Approver)
        .build()
        .spawn(laser.clone());

    let decision = orchestrator(&laser)
        .request_input(
            AgentTopic::Sessions,
            Bytes::from_static(b"approve draining node-7?"),
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
    let _agent_lifetime_2 = Agent::builder()
        .id("approver".parse().expect("approver is a valid agent id"))
        .listen_on(AgentTopic::Sessions)
        .handler(Rejecter)
        .build()
        .spawn(laser.clone());

    let result = orchestrator(&laser)
        .request_input(
            AgentTopic::Sessions,
            Bytes::from_static(b"approve draining node-7?"),
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
                AgentTopic::Sessions,
                Bytes::from_static(b"approve draining node-7?"),
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
    let _agent_lifetime_3 = Agent::builder()
        .id("approver".parse().expect("approver is a valid agent id"))
        .listen_on(AgentTopic::Sessions)
        .handler(Approver)
        .build()
        .spawn(laser.clone());
    let mut gatekeeper = Agent::builder()
        .id("gatekeeper"
            .parse()
            .expect("gatekeeper is a valid agent id"))
        .listen_on(AgentTopic::Sessions)
        .respond_on(AgentTopic::Sessions)
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
        .send_agent(AgentTopic::Sessions, Bytes::from_static(b"go"), &trigger)
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
            AgentTopic::Sessions,
            Bytes::from_static(b"unanswered"),
            Duration::from_millis(300),
        )
        .await;

    assert!(
        matches!(result, Err(LaserError::Timeout(_))),
        "an unanswered interrupt must time out, got {result:?}",
    );
}

// A responder that records every prompt it sees and answers it in its own name.
struct Witness {
    name: &'static str,
    seen: std::sync::Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
}

impl AgentHandler for Witness {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let Some(envelope) = message.envelope.as_ref() else {
            return Ok(());
        };
        if envelope.kind != laser_sdk::wire::agent::AgentKind::Command {
            return Ok(());
        }
        self.seen
            .lock()
            .expect("the seen list is not poisoned")
            .push(envelope.body.clone());
        ctx.respond_input(
            AgentTopic::Sessions,
            Bytes::copy_from_slice(self.name.as_bytes()),
        )
        .await
    }
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_two_responders_when_requesting_input_from_one_then_only_that_responder_should_see_the_prompt()
 {
    let laser = harness::laser().await;
    let approver_seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let bystander_seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let _responders: Vec<_> = [
        ("approver", approver_seen.clone()),
        ("bystander", bystander_seen.clone()),
    ]
    .into_iter()
    .map(|(name, seen)| {
        Agent::builder()
            .id(name
                .parse()
                .expect("the responder name is a valid agent id"))
            .listen_on(AgentTopic::Sessions)
            .handler(Witness { name, seen })
            .build()
            .spawn(laser.clone())
    })
    .collect();
    // One producer keeps both prompts in one session, so each responder reads
    // them in order: once the bystander has seen the broadcast, it has already
    // passed the addressed prompt.
    let gate = orchestrator(&laser);
    let decision = gate
        .request_input_from(
            "approver"
                .parse::<laser_sdk::types::AgentId>()
                .expect("approver is a valid agent id"),
            AgentTopic::Sessions,
            Bytes::from_static(b"addressed"),
            Duration::from_secs(10),
        )
        .await
        .expect("the approver answers the addressed prompt");
    assert_eq!(decision.as_slice(), b"approver");
    gate.request_input(
        AgentTopic::Sessions,
        Bytes::from_static(b"broadcast"),
        Duration::from_secs(10),
    )
    .await
    .expect("a responder answers the broadcast prompt");
    harness::eventually(|| {
        let seen = bystander_seen.clone();
        async move {
            (!seen
                .lock()
                .expect("the seen list is not poisoned")
                .is_empty())
            .then_some(())
        }
    })
    .await;
    assert_eq!(
        *bystander_seen
            .lock()
            .expect("the seen list is not poisoned"),
        [b"broadcast".to_vec()]
    );
    assert_eq!(
        approver_seen.lock().expect("the seen list is not poisoned")[0],
        b"addressed".to_vec()
    );
}
