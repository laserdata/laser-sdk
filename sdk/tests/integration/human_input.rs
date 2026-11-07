use crate::harness;
use bytes::Bytes;
use laser_sdk::prelude::full::*;
use laser_sdk::wire::agent::{
    AgentErrorBody, AgentErrorCode, AgentId as WireAgentId, ConversationId as WireConversationId,
};
use laser_sdk::wire::dispatch::OPERATION_INPUT_REQUEST;
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
        .operations(vec![OPERATION_INPUT_REQUEST.to_owned()])
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
        .operations(vec![OPERATION_INPUT_REQUEST.to_owned()])
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
// the decision on the audit topic so the test can observe it. It counts every
// record it handles, so a prompt it took as its own work would show.
struct Gatekeeper {
    handled: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl AgentHandler for Gatekeeper {
    async fn handle(&self, _message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        self.handled
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let grant = ctx
            .approval_gate(
                "approver"
                    .parse::<laser_sdk::types::AgentId>()
                    .expect("approver is a valid agent id"),
                AgentTopic::Sessions,
                Bytes::from_static(b"approve draining node-7?"),
                Duration::from_secs(10),
            )
            .await?;
        assert_eq!(grant.approver().as_str(), "approver");
        ctx.reply_on(AgentTopic::Audit, grant.decision().to_vec())
            .await
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
        .operations(vec![OPERATION_INPUT_REQUEST.to_owned()])
        .build()
        .spawn(laser.clone());
    let handled = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut gatekeeper = Agent::builder()
        .id("gatekeeper"
            .parse()
            .expect("gatekeeper is a valid agent id"))
        .listen_on(AgentTopic::Sessions)
        .respond_on(AgentTopic::Sessions)
        .handler(Gatekeeper {
            handled: handled.clone(),
        })
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
    assert_eq!(
        handled.load(std::sync::atomic::Ordering::Acquire),
        1,
        "the gatekeeper's own prompt is not its work"
    );
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_forged_answer_from_another_agent_when_requesting_input_from_an_approver_then_should_wait_for_the_approver()
 {
    let laser = harness::laser().await;
    let conversation = WireConversationId::from(ConversationId::new());
    let gate = laser.agdx(
        AgentTopic::Sessions,
        "orchestrator"
            .parse::<WireAgentId>()
            .expect("orchestrator is a valid agent id"),
        conversation,
    );
    let request = tokio::spawn({
        let gate = gate.clone();
        async move {
            gate.request_input_from(
                "approver"
                    .parse::<laser_sdk::types::AgentId>()
                    .expect("approver is a valid agent id"),
                AgentTopic::Sessions,
                Bytes::from_static(b"approve draining node-7?"),
                Duration::from_secs(20),
            )
            .await
        }
    });
    // Another agent reads the prompt off the session topic and answers it
    // under its own name before the approver runs.
    let prompt = harness::eventually(|| {
        let laser = laser.clone();
        async move {
            ContextAssembler::builder()
                .conversation_id(conversation.into())
                .topics(vec![AgentTopic::Sessions])
                .build()
                .assemble(&laser)
                .await
                .expect("reading the session topic should succeed")
                .into_iter()
                .filter_map(|message| message.envelope)
                .find(|envelope| envelope.operation.as_deref() == Some(OPERATION_INPUT_REQUEST))
        }
    })
    .await;
    laser
        .agdx(
            AgentTopic::Sessions,
            "mallory"
                .parse::<WireAgentId>()
                .expect("mallory is a valid agent id"),
            conversation,
        )
        .respond(
            prompt
                .correlation
                .expect("the prompt carries a correlation"),
            b"forged".to_vec(),
        )
        .with_target(prompt.source.clone())
        .send()
        .await
        .expect("the forged answer is published");
    let _approver = Agent::builder()
        .id("approver".parse().expect("approver is a valid agent id"))
        .listen_on(AgentTopic::Sessions)
        .handler(Approver)
        .operations(vec![OPERATION_INPUT_REQUEST.to_owned()])
        .build()
        .spawn(laser.clone());

    let decision = request
        .await
        .expect("the request task joins")
        .expect("the approver answers");
    assert_eq!(decision.as_slice(), b"approved");
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
            .operations(vec![OPERATION_INPUT_REQUEST.to_owned()])
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

// A handler that gates the work of one session on an approval and records
// every pass, so a test can see the slot freed and the resume run once.
struct ParkingGatekeeper {
    gated: ConversationId,
    passes: std::sync::Arc<std::sync::Mutex<Vec<(ConversationId, &'static str)>>>,
}

impl AgentHandler for ParkingGatekeeper {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let session = message.provenance.conversation_id;
        let record = |stage| {
            self.passes
                .lock()
                .expect("the passes are not poisoned")
                .push((session, stage));
        };
        record("before");
        if session == self.gated {
            let grant = ctx
                .approval_gate(
                    "approver"
                        .parse::<laser_sdk::types::AgentId>()
                        .expect("approver is a valid agent id"),
                    AgentTopic::Sessions,
                    Bytes::from_static(b"approve draining node-7?"),
                    Duration::from_secs(60),
                )
                .await?;
            assert_eq!(grant.decision(), b"approved");
        }
        record("after");
        Ok(())
    }
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_handler_waiting_on_an_approval_when_another_session_sends_work_then_should_handle_it_and_resume_the_first_once()
 {
    let laser = harness::laser().await;
    laser
        .topic(AgentTopic::Control.topic_string())
        .ensure(1, None)
        .await
        .expect("provisioning creates the control topic");
    let gated = ConversationId::new();
    let other = ConversationId::new();
    let passes = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut gatekeeper = Agent::builder()
        .id("gatekeeper"
            .parse()
            .expect("gatekeeper is a valid agent id"))
        .listen_on(AgentTopic::Sessions)
        .respond_on(AgentTopic::Sessions)
        .handler(ParkingGatekeeper {
            gated,
            passes: passes.clone(),
        })
        .build()
        .spawn(laser.clone());
    gatekeeper
        .ready()
        .await
        .expect("gatekeeper joins its group");
    let send = |session: ConversationId, body: &'static [u8]| {
        let laser = laser.clone();
        async move {
            laser
                .agdx(
                    AgentTopic::Sessions,
                    "client"
                        .parse::<WireAgentId>()
                        .expect("client is a valid agent id"),
                    session.into(),
                )
                .command(
                    <laser_sdk::wire::agent::CorrelationId as laser_sdk::types::MintUlid>::mint(),
                    body.to_vec(),
                )
                .with_target(
                    "gatekeeper"
                        .parse::<laser_sdk::types::AgentId>()
                        .expect("gatekeeper is a valid agent id"),
                )
                .send()
                .await
                .expect("the work is sent");
        }
    };
    send(gated, b"drain").await;
    // The second session's work runs while the first waits for its approver.
    send(other, b"status").await;
    harness::eventually(|| {
        let passes = passes.clone();
        async move {
            passes
                .lock()
                .expect("the passes are not poisoned")
                .contains(&(other, "after"))
                .then_some(())
        }
    })
    .await;

    let _approver = Agent::builder()
        .id("approver".parse().expect("approver is a valid agent id"))
        .listen_on(AgentTopic::Sessions)
        .handler(Approver)
        .operations(vec![OPERATION_INPUT_REQUEST.to_owned()])
        .build()
        .spawn(laser.clone());
    harness::eventually(|| {
        let passes = passes.clone();
        async move {
            passes
                .lock()
                .expect("the passes are not poisoned")
                .contains(&(gated, "after"))
                .then_some(())
        }
    })
    .await;
    gatekeeper.shutdown().await.expect("gatekeeper shuts down");
    let gated_passes: Vec<&str> = passes
        .lock()
        .expect("the passes are not poisoned")
        .iter()
        .filter(|(session, _)| *session == gated)
        .map(|(_, stage)| *stage)
        .collect();
    assert_eq!(
        gated_passes,
        ["before", "before", "after"],
        "the parked record replays once after the decision"
    );
}
