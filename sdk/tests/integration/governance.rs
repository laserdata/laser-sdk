use crate::harness::{eventually, laser};
use async_trait::async_trait;
use laser_sdk::govern::{
    ActionDecision, ActionGovernor, ActionKind, GovernedAction, GovernorMode,
    POLICY_DECISION_OPERATION, PolicyEvidence,
};
use laser_sdk::prelude::full::*;
use laser_sdk::wire::agent::{AgentEnvelope, METADATA_PURPOSE};
use laser_sdk::wire::framing::decode_named;
use std::sync::Arc;
use std::time::Duration;

// A governor blocking any payload that mentions a wire transfer: the enforce
// path must reject before the effect and leave a `block` decision on the audit
// topic, and the blocked payload must never reach the target topic.
#[tokio::test]
async fn given_an_enforcing_governor_when_an_action_is_blocked_then_should_reject_and_leave_evidence()
 {
    let laser = laser().await;
    let governed = laser.with_governor(Arc::new(BlockWires), GovernorMode::Enforce);
    let provenance = Provenance::builder()
        .conversation_id(ConversationId::new())
        .build();

    governed
        .send_agent(AgentTopic::Commands, "status report", &provenance)
        .await
        .expect("a benign send passes the governor");
    let error = governed
        .send_agent(AgentTopic::Commands, "wire-funds to acct 7", &provenance)
        .await
        .expect_err("the governor blocks the wire transfer");
    assert!(matches!(error, LaserError::PolicyBlocked(_)));
    assert!(!error.is_retryable());

    let evidence = eventually(|| async {
        let all = audit_evidence(&laser).await;
        all.into_iter().find(|record| record.decision == "block")
    })
    .await;
    assert_eq!(evidence.outcome, "blocked");
    assert_eq!(evidence.kind, "send");
    assert_eq!(evidence.mode, "enforce");
    assert_eq!(evidence.reason.as_deref(), Some("no wire transfers"));
    assert_eq!(evidence.receipt_digest.len(), 64);

    // The blocked payload never reached the log.
    let mut reader = laser
        .topic(AgentTopic::Commands.topic_string())
        .replay()
        .expect("reader");
    let landed = reader.poll().await.expect("poll commands");
    assert!(
        landed
            .iter()
            .all(|message| !message.payload.starts_with(b"wire-funds")),
        "a blocked send must not be published"
    );
}

// The same block-everything policy in observe mode: the send goes through, the
// would-be decision is recorded with outcome `effected`, production unimpacted.
#[tokio::test]
async fn given_observe_mode_when_a_block_decides_then_should_record_and_let_the_action_through() {
    let laser = laser().await;
    let governed = laser.with_governor(Arc::new(BlockWires), GovernorMode::Observe);
    let provenance = Provenance::builder()
        .conversation_id(ConversationId::new())
        .build();

    governed
        .send_agent(AgentTopic::Commands, "wire-funds to acct 7", &provenance)
        .await
        .expect("observe mode never blocks");

    let evidence = eventually(|| async {
        let all = audit_evidence(&laser).await;
        all.into_iter().find(|record| record.decision == "block")
    })
    .await;
    assert_eq!(evidence.outcome, "effected");
    assert_eq!(evidence.mode, "observe");

    let landed = eventually(|| async {
        let mut reader = laser
            .topic(AgentTopic::Commands.topic_string())
            .replay()
            .expect("reader");
        let messages = reader.poll().await.expect("poll commands");
        messages
            .into_iter()
            .find(|message| message.payload.starts_with(b"wire-funds"))
    })
    .await;
    assert!(!landed.payload.is_empty());
}

// A governor keyed on the advisory `purpose` metadata redacts the body: the
// modified body is what the log carries, and the decision is recorded.
#[tokio::test]
async fn given_a_modifying_governor_when_the_purpose_matches_then_should_publish_the_replaced_body()
{
    let laser = laser().await;
    let governed = laser.with_governor(Arc::new(RedactMarketing), GovernorMode::Enforce);
    let conversation = ConversationId::new();
    let source: laser_sdk::wire::agent::AgentId = "planner".parse().expect("agent id");

    governed
        .agdx(AgentTopic::LlmIo, source, conversation.into())
        .emit(b"customer pii here".to_vec())
        .with_metadata(METADATA_PURPOSE, "marketing")
        .send()
        .await
        .expect("the modified emit publishes");

    let envelope = eventually(|| async {
        let mut reader = laser
            .topic(AgentTopic::LlmIo.topic_string())
            .replay()
            .expect("reader");
        let messages = reader.poll().await.expect("poll llm io");
        messages
            .into_iter()
            .filter_map(|message| decode_named::<AgentEnvelope>(&message.payload).ok())
            .next()
    })
    .await;
    assert_eq!(envelope.body, b"[redacted]".to_vec());

    let evidence = eventually(|| async {
        let all = audit_evidence(&laser).await;
        all.into_iter().find(|record| record.decision == "modify")
    })
    .await;
    assert_eq!(evidence.outcome, "effected");
    assert_eq!(evidence.kind, "event");
}

// A step-up on the request path: the caller gets the scope to approve, and the
// evidence carries it, so a handler can gate on approval and re-send.
#[tokio::test]
async fn given_a_step_up_governor_when_a_request_runs_then_should_surface_the_scope() {
    let laser = laser().await;
    let governed = laser.with_governor(Arc::new(StepUpRequests), GovernorMode::Enforce);
    let provenance = Provenance::builder()
        .conversation_id(ConversationId::new())
        .build();

    let error = governed
        .request(
            AgentTopic::Commands,
            AgentTopic::Responses,
            "do the thing",
            &provenance,
            Duration::from_secs(2),
        )
        .await
        .expect_err("the request pauses on a step-up");
    assert!(matches!(&error, LaserError::StepUpRequired(scope) if scope == "payments:approve"));

    let evidence = eventually(|| async {
        let all = audit_evidence(&laser).await;
        all.into_iter().find(|record| record.decision == "step_up")
    })
    .await;
    assert_eq!(evidence.outcome, "step_up");
    assert_eq!(evidence.kind, "request");
    assert_eq!(evidence.approved_scope.as_deref(), Some("payments:approve"));
}

#[tokio::test]
async fn given_an_enforcing_governor_when_vector_memory_is_blocked_then_should_not_remember_it() {
    let laser = laser().await;
    let governed = laser.with_governor(Arc::new(BlockFabricatedMemory), GovernorMode::Enforce);
    let conversation = ConversationId::new();
    let memory = governed
        .memory_with("support", MemoryBackend::Vector)
        .embedder(IdentityEmbedder);

    let error = memory
        .remember(b"customer prefers blue [skew:fabricate_memory]".to_vec())
        .scope(conversation)
        .send()
        .await
        .expect_err("the governor blocks the fabricated memory");
    assert!(matches!(error, LaserError::PolicyBlocked(_)));
    assert!(
        memory
            .recall(conversation)
            .fetch()
            .await
            .expect("vector recall succeeds")
            .is_empty(),
        "the blocked memory must not enter the vector index"
    );

    let evidence = eventually(|| async {
        let all = audit_evidence(&laser).await;
        all.into_iter().find(|record| record.decision == "block")
    })
    .await;
    assert_eq!(evidence.kind, "memory_write");
    assert_eq!(evidence.outcome, "blocked");
}

#[cfg(feature = "query")]
// A governed plain publish should be blocked and audited the same way an agent
// send is, since the business-topic path is an effect boundary too.
#[tokio::test]
async fn given_an_enforcing_governor_when_a_business_publish_is_blocked_then_should_reject_and_leave_evidence()
 {
    let laser = laser().await;
    laser
        .topic("business.audit")
        .ensure(1)
        .await
        .expect("topic ready");
    let governed = laser.with_governor(Arc::new(BlockBusinessWires), GovernorMode::Enforce);
    let provenance = Provenance::builder()
        .conversation_id(ConversationId::new())
        .agent("publisher".parse().expect("publisher id parses"))
        .build();

    let error = governed
        .topic("business.audit")
        .publish()
        .provenance(&provenance)
        .payload(b"wire-funds to acct 7".to_vec())
        .send()
        .await
        .expect_err("the governor blocks the business publish");
    assert!(matches!(error, LaserError::PolicyBlocked(_)));

    let evidence = eventually(|| async {
        let all = audit_evidence(&laser).await;
        all.into_iter().find(|record| record.decision == "block")
    })
    .await;
    assert_eq!(evidence.outcome, "blocked");
    assert_eq!(evidence.kind, "publish");

    let mut reader = laser
        .topic("business.audit")
        .replay()
        .expect("business reader");
    let landed = reader.poll().await.expect("poll business topic");
    assert!(
        landed
            .iter()
            .all(|message| !message.payload.starts_with(b"wire-funds")),
        "a blocked publish must not be written"
    );
}

// Every policy_decision evidence event currently on this test's audit topic.
async fn audit_evidence(laser: &Laser) -> Vec<PolicyEvidence> {
    let mut reader = laser
        .topic(AgentTopic::Audit.topic_string())
        .replay()
        .expect("audit reader");
    let mut evidence = Vec::new();
    loop {
        let Ok(batch) = reader.poll().await else {
            return Vec::new();
        };
        if batch.is_empty() {
            break;
        }
        for message in batch {
            let Ok(envelope) = decode_named::<AgentEnvelope>(&message.payload) else {
                continue;
            };
            if envelope.operation.as_deref() != Some(POLICY_DECISION_OPERATION) {
                continue;
            }
            evidence.push(PolicyEvidence::decode(&envelope.body).expect("evidence decodes"));
        }
    }
    evidence
}

struct BlockWires;

#[cfg(feature = "query")]
struct BlockBusinessWires;

#[async_trait]
impl ActionGovernor for BlockWires {
    async fn decide(&self, action: &GovernedAction<'_>) -> Result<ActionDecision, LaserError> {
        if action.payload.starts_with(b"wire-funds") {
            return Ok(ActionDecision::block("no wire transfers"));
        }
        Ok(ActionDecision::allow())
    }
}

#[cfg(feature = "query")]
#[async_trait]
impl ActionGovernor for BlockBusinessWires {
    async fn decide(&self, action: &GovernedAction<'_>) -> Result<ActionDecision, LaserError> {
        if action.kind == ActionKind::Publish && action.payload.starts_with(b"wire-funds") {
            return Ok(ActionDecision::block("no wire transfers"));
        }
        Ok(ActionDecision::allow())
    }
}

struct RedactMarketing;

#[async_trait]
impl ActionGovernor for RedactMarketing {
    async fn decide(&self, action: &GovernedAction<'_>) -> Result<ActionDecision, LaserError> {
        if action.purpose == Some("marketing") {
            return Ok(ActionDecision::modify(b"[redacted]".to_vec()));
        }
        Ok(ActionDecision::allow())
    }
}

struct StepUpRequests;

struct BlockFabricatedMemory;

struct IdentityEmbedder;

impl Embedder for IdentityEmbedder {
    async fn embed(&self, text: &str) -> Result<Vec<f32>, LaserError> {
        Ok(vec![text.len() as f32])
    }
}

#[async_trait]
impl ActionGovernor for BlockFabricatedMemory {
    async fn decide(&self, action: &GovernedAction<'_>) -> Result<ActionDecision, LaserError> {
        let marker = b"[skew:fabricate_memory]";
        if action.kind == ActionKind::MemoryWrite
            && action
                .payload
                .windows(marker.len())
                .any(|window| window == marker)
        {
            return Ok(ActionDecision::block("fabricated memory marker"));
        }
        Ok(ActionDecision::allow())
    }
}

#[async_trait]
impl ActionGovernor for StepUpRequests {
    async fn decide(&self, action: &GovernedAction<'_>) -> Result<ActionDecision, LaserError> {
        if action.kind == ActionKind::Request {
            return Ok(ActionDecision::step_up("payments:approve"));
        }
        Ok(ActionDecision::allow())
    }
}
