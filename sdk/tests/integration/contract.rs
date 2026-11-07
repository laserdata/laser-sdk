use crate::harness;
use bytes::Bytes;
use laser_sdk::agent::{Clock, SystemClock};
use laser_sdk::govern::{ActionDecision, ActionGovernor, ActionKind, GovernedAction, GovernorMode};
use laser_sdk::prelude::full::*;
use laser_sdk::wire::agent::{AgentCard, AgentKind, CapabilityDescriptor, TaskState};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

struct Rotator;

impl AgentHandler for Rotator {
    async fn handle(&self, _message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        ctx.respond(Bytes::from("rotated")).await
    }
}

fn rotate_card() -> AgentCard {
    AgentCard {
        name: None,
        version: None,
        capabilities: vec![CapabilityDescriptor {
            skill_id: "rotate_credentials".to_owned(),
            input: None,
            output: None,
            cost_class: None,
            latency_class: None,
            max_concurrency: None,
            health: None,
            load: None,
        }],
        ttl_micros: None,
        inbox: None,
    }
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_capable_target_when_a_contract_is_sent_then_should_complete_with_the_reply() {
    let laser = harness::laser().await;
    let mut worker = Agent::builder()
        .id("rotator".parse().expect("rotator id is valid"))
        .listen_on(AgentTopic::Sessions)
        .respond_on(AgentTopic::Sessions)
        .capabilities(rotate_card().capabilities)
        .ack_on_pickup(true)
        .handler(Rotator)
        .build()
        .spawn(laser.clone());
    worker.ready().await.expect("worker joins its group");

    // A fixed inbox route, since Apache Iggy harness has no presence
    // command. The branch is target-filtered to the rotator on the shared topic.
    let outcome = laser
        .contract(Router::to_capable("rotate_credentials", RoutePolicy::Any))
        .from("orchestrator".parse().expect("orchestrator id is valid"))
        .payload(Bytes::from("rotate-42"))
        .inbox_route(InboxRoute::Fixed(AgentTopic::Sessions))
        .deadline(Duration::from_secs(10))
        .send()
        .await
        .expect("the contract resolves and sends");

    match outcome {
        Contract::Completed(reply) => {
            assert_eq!(reply.body(), b"rotated");
        }
        other => panic!("expected Completed, got {other:?}"),
    }

    worker.shutdown().await.expect("worker shuts down");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_no_responder_when_a_contract_is_sent_then_should_time_out() {
    let laser = harness::laser().await;

    // Addressed to an agent that is not running: the command lands but nothing
    // replies, so with no expiry set the contract returns TimedOut at the deadline
    // rather than hang.
    let outcome = laser
        .contract(Router::to("ghost".parse().expect("ghost id is valid")))
        .from("orchestrator".parse().expect("orchestrator id is valid"))
        .payload(Bytes::from("noop"))
        .inbox_route(InboxRoute::Fixed(AgentTopic::Sessions))
        .deadline(Duration::from_secs(2))
        .send()
        .await
        .expect("the contract sends");

    assert!(matches!(outcome, Contract::TimedOut));
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_no_running_target_when_a_contract_expires_unpicked_then_should_time_out_without_evidence()
 {
    let laser = harness::laser().await;

    // Nothing reads the command, so nothing proves it went unconsumed: the
    // missing pickup acknowledgment alone reads as a timeout.
    let outcome = laser
        .contract(Router::to("ghost".parse().expect("ghost id is valid")))
        .from("orchestrator".parse().expect("orchestrator id is valid"))
        .payload(Bytes::from("noop"))
        .inbox_route(InboxRoute::Fixed(AgentTopic::Sessions))
        .expire_if_not_consumed(Duration::from_millis(500))
        .deadline(Duration::from_secs(2))
        .send()
        .await
        .expect("the contract sends");

    assert!(matches!(outcome, Contract::TimedOut), "{outcome:?}");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_target_reading_the_command_after_its_expiry_when_contracted_then_should_report_not_consumed()
 {
    let laser = harness::laser().await;
    let expiry = Duration::from_millis(500);
    let contract = tokio::spawn({
        let laser = laser.clone();
        async move {
            laser
                .contract(Router::to("late".parse().expect("late id is valid")))
                .from("orchestrator".parse().expect("orchestrator id is valid"))
                .payload(Bytes::from("rotate-42"))
                .inbox_route(InboxRoute::Fixed(AgentTopic::Sessions))
                .expire_if_not_consumed(expiry)
                .deadline(Duration::from_secs(20))
                .send()
                .await
        }
    });
    // The command's expiry is an absolute time, so the target must start
    // after it to read the command expired.
    tokio::time::sleep(expiry * 2).await;
    let mut worker = Agent::builder()
        .id("late".parse().expect("late id is valid"))
        .listen_on(AgentTopic::Sessions)
        .respond_on(AgentTopic::Sessions)
        .handler(Rotator)
        .build()
        .spawn(laser.clone());
    worker.ready().await.expect("worker joins its group");

    let outcome = contract
        .await
        .expect("the contract task joins")
        .expect("the contract sends");
    assert!(matches!(outcome, Contract::NotConsumed), "{outcome:?}");
    worker.shutdown().await.expect("worker shuts down");
}

struct Refuser;

impl AgentHandler for Refuser {
    async fn handle(&self, _message: &AgentMessage, _ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        Err(LaserError::Rejected(
            "the rotation window is closed".to_owned(),
        ))
    }
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_target_refusing_the_command_when_contracted_then_should_fail_at_once() {
    let laser = harness::laser().await;
    let mut worker = Agent::builder()
        .id("refuser".parse().expect("refuser id is valid"))
        .listen_on(AgentTopic::Sessions)
        .respond_on(AgentTopic::Sessions)
        .handler(Refuser)
        .build()
        .spawn(laser.clone());
    worker.ready().await.expect("worker joins its group");

    let started = std::time::Instant::now();
    let outcome = laser
        .contract(Router::to("refuser".parse().expect("refuser id is valid")))
        .from("orchestrator".parse().expect("orchestrator id is valid"))
        .payload(Bytes::from("rotate-42"))
        .inbox_route(InboxRoute::Fixed(AgentTopic::Sessions))
        .deadline(Duration::from_secs(30))
        .send()
        .await
        .expect("the contract sends");

    let Contract::Failed(reply) = outcome else {
        panic!("expected Failed, got {outcome:?}");
    };
    let error: laser_sdk::wire::agent::AgentErrorBody =
        laser_sdk::wire::framing::decode_named(reply.body()).expect("an AGDX error body");
    assert_eq!(
        error.message.as_deref(),
        Some("rejected: the rotation window is closed")
    );
    assert!(
        started.elapsed() < Duration::from_secs(15),
        "the refusal answers before the deadline"
    );
    worker.shutdown().await.expect("worker shuts down");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_capable_agent_on_apache_iggy_when_contracted_through_its_advertised_inbox_then_should_complete()
 {
    let laser = harness::laser().await;
    let mut worker = Agent::builder()
        .id("rotator".parse().expect("rotator id is valid"))
        .listen_on(AgentTopic::Sessions)
        .respond_on(AgentTopic::Sessions)
        .capabilities(rotate_card().capabilities)
        .handler(Rotator)
        .build()
        .spawn(laser.clone());
    worker.ready().await.expect("worker joins its group");

    // No inbox override: the default advertised route resolves the card's
    // inbox, since Apache Iggy keeps no live presence.
    let outcome = laser
        .contract(Router::to_capable("rotate_credentials", RoutePolicy::Any))
        .from("orchestrator".parse().expect("orchestrator id is valid"))
        .payload(Bytes::from("rotate-42"))
        .deadline(Duration::from_secs(10))
        .send()
        .await
        .expect("the contract resolves and sends");
    assert!(matches!(outcome, Contract::Completed(_)), "{outcome:?}");

    let mut registry = laser.agent_registry().expect("the registry opens");
    registry
        .refresh(SystemClock.now_micros())
        .await
        .expect("the registry folds");
    let card = registry
        .lookup(&"rotator".parse().expect("rotator id is valid"))
        .expect("the card is folded")
        .card
        .clone();
    assert_eq!(card.inbox.as_deref(), Some("agent.sessions"));
    assert!(card.ttl_micros.is_some(), "a spawned agent's card expires");

    worker.shutdown().await.expect("worker shuts down");
    // A stopped agent marks its card unavailable, so it is no longer routable.
    harness::eventually(|| {
        let laser = laser.clone();
        async move {
            let mut registry = laser.agent_registry().expect("the registry opens");
            let now = SystemClock.now_micros();
            registry.refresh(now).await.expect("the registry folds");
            registry
                .resolve("rotate_credentials", now)
                .is_empty()
                .then_some(())
        }
    })
    .await;
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_quarantined_agent_when_contracted_by_name_then_should_refuse_it() {
    let laser = harness::laser().await;
    let agent: AgentId = "suspect".parse().expect("suspect id is valid");
    laser
        .quarantine("operator".parse().expect("operator id is valid"), &agent)
        .await
        .expect("the quarantine is recorded");

    let outcome = laser
        .contract(Router::to(agent))
        .from("orchestrator".parse().expect("orchestrator id is valid"))
        .payload(Bytes::from("noop"))
        .inbox_route(InboxRoute::Fixed(AgentTopic::Sessions))
        .deadline(Duration::from_secs(2))
        .send()
        .await;

    assert!(
        matches!(outcome, Err(LaserError::Quarantined { ref agent }) if agent == "suspect"),
        "{outcome:?}"
    );
}

#[cfg(feature = "sign")]
struct SignedRotator;

#[cfg(feature = "sign")]
impl AgentHandler for SignedRotator {
    async fn handle(&self, _message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        ctx.respond(Bytes::from("rotated")).await
    }
}

#[cfg(feature = "sign")]
struct SlowSignedRotator;

#[cfg(feature = "sign")]
impl AgentHandler for SlowSignedRotator {
    async fn handle(&self, _message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        tokio::time::sleep(Duration::from_millis(300)).await;
        ctx.respond(Bytes::from("rotated")).await
    }
}

#[cfg(feature = "sign")]
#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_verifier_when_the_target_signs_its_reply_then_should_complete_on_the_bound_signer()
{
    use laser_sdk::sign::{KeyRegistry, SigningKey};
    use std::sync::Arc;

    let laser = harness::laser().await;

    // Enroll the honest rotator plus an unrelated key, so the contract's
    // signer binding (accept only the resolved target's principal) is exercised
    // against a registry that holds more than one key.
    let rotator_key = SigningKey::from_bytes(&[7u8; 32]);
    let other_key = SigningKey::from_bytes(&[9u8; 32]);
    let registry = KeyRegistry::new();
    registry.enroll("rotator", rotator_key.verifying_key());
    registry.enroll("other", other_key.verifying_key());
    let verifier = Arc::new(registry);

    let mut worker = Agent::builder()
        .id("rotator".parse().expect("rotator id is valid"))
        .listen_on(AgentTopic::Sessions)
        .respond_on(AgentTopic::Sessions)
        .capabilities(rotate_card().capabilities)
        .ack_on_pickup(true)
        .signing_key(Arc::new(rotator_key))
        .handler(SignedRotator)
        .build()
        .spawn(laser.clone());
    worker.ready().await.expect("worker joins its group");

    let caller = harness::verified(&laser, verifier).await;
    let outcome = caller
        .contract(Router::to("rotator".parse().expect("rotator id is valid")))
        .from("orchestrator".parse().expect("orchestrator id is valid"))
        .payload(Bytes::from("rotate-42"))
        .inbox_route(InboxRoute::Fixed(AgentTopic::Sessions))
        .deadline(Duration::from_secs(10))
        .send()
        .await
        .expect("the signed contract resolves and sends");
    match outcome {
        Contract::Completed(reply) => {
            assert_eq!(reply.body(), b"rotated");
            assert_eq!(reply.verified_principal.as_deref(), Some("rotator"));
        }
        other => panic!("expected a signed completion, got {other:?}"),
    }
}

#[cfg(feature = "sign")]
#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_verified_slow_agent_when_pickup_expires_then_should_honor_its_signed_working_ack()
{
    use laser_sdk::sign::{KeyRegistry, SigningKey};
    use std::sync::Arc;

    let laser = harness::laser().await;
    let rotator_key = SigningKey::from_bytes(&[11u8; 32]);
    let registry = KeyRegistry::new();
    registry.enroll("slow-rotator", rotator_key.verifying_key());

    let mut worker = Agent::builder()
        .id("slow-rotator".parse().expect("slow-rotator id is valid"))
        .listen_on(AgentTopic::Sessions)
        .respond_on(AgentTopic::Sessions)
        .ack_on_pickup(true)
        .signing_key(Arc::new(rotator_key))
        .handler(SlowSignedRotator)
        .build()
        .spawn(laser.clone());
    worker.ready().await.expect("worker joins its group");

    let caller = harness::verified(&laser, Arc::new(registry)).await;
    // A fresh group member reads once its partition primary installed it, so a
    // first contract without an expiry proves the agent consumes before the
    // tight one is sent.
    let warmed = caller
        .contract(Router::to(
            "slow-rotator".parse().expect("slow-rotator id is valid"),
        ))
        .from("orchestrator".parse().expect("orchestrator id is valid"))
        .payload(Bytes::from("rotate-42"))
        .inbox_route(InboxRoute::Fixed(AgentTopic::Sessions))
        .deadline(Duration::from_secs(10))
        .send()
        .await
        .expect("the warm-up contract resolves and sends");
    assert!(
        matches!(warmed, Contract::Completed(_)),
        "the agent consumes before the expiring contract, got {warmed:?}"
    );
    let outcome = caller
        .contract(Router::to(
            "slow-rotator".parse().expect("slow-rotator id is valid"),
        ))
        .from("orchestrator".parse().expect("orchestrator id is valid"))
        .payload(Bytes::from("rotate-43"))
        .inbox_route(InboxRoute::Fixed(AgentTopic::Sessions))
        .expire_if_not_consumed(Duration::from_millis(100))
        .deadline(Duration::from_secs(2))
        .send()
        .await
        .expect("the signed contract resolves and sends");

    match outcome {
        Contract::Completed(reply) => assert_eq!(reply.body(), b"rotated"),
        other => panic!("expected a signed completion, got {other:?}"),
    }
    worker.shutdown().await.expect("worker shuts down");
}

// Holds the handler on a `slow` task, so a task queued behind it on the same
// conversation outlives its contract deadline before the handler reaches it.
struct Sluggish {
    late: Arc<AtomicUsize>,
}

impl AgentHandler for Sluggish {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        match message.body() {
            b"slow" => tokio::time::sleep(Duration::from_millis(1500)).await,
            b"late" => {
                self.late.fetch_add(1, Ordering::SeqCst);
            }
            _ => {}
        }
        ctx.respond(Bytes::from("done")).await
    }
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_task_queued_past_its_contract_deadline_when_the_target_reaches_it_then_should_drop_it()
 {
    let laser = harness::laser().await;
    let late = Arc::new(AtomicUsize::new(0));
    let mut worker = Agent::builder()
        .id("sluggish".parse().expect("worker id is valid"))
        .listen_on(AgentTopic::Sessions)
        .respond_on(AgentTopic::Sessions)
        .handler(Sluggish {
            late: Arc::clone(&late),
        })
        .build()
        .spawn(laser.clone());
    worker.ready().await.expect("worker joins its group");
    let conversation = ConversationId::new();
    let contract = |payload: &'static str, deadline: Duration| {
        laser
            .contract(Router::to("sluggish".parse().expect("valid agent id")))
            .from("orchestrator".parse().expect("orchestrator id is valid"))
            .payload(Bytes::from(payload))
            .inbox_route(InboxRoute::Fixed(AgentTopic::Sessions))
            .conversation(conversation)
            .deadline(deadline)
            .send()
    };

    let slow = contract("slow", Duration::from_secs(10));
    tokio::pin!(slow);
    let queued = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        contract("late", Duration::from_millis(300)).await
    };
    let (slow, queued) = tokio::join!(slow, queued);
    assert!(matches!(queued, Ok(Contract::TimedOut)), "{queued:?}");
    assert!(matches!(slow, Ok(Contract::Completed(_))), "{slow:?}");
    let probe = contract("probe", Duration::from_secs(10)).await;
    assert!(matches!(probe, Ok(Contract::Completed(_))), "{probe:?}");

    assert_eq!(
        late.load(Ordering::SeqCst),
        0,
        "a task picked up after its contract deadline must be dropped, not handled",
    );
    worker.shutdown().await.expect("worker shuts down");
}

// Blocks every AGDX command, so a contract's command never publishes.
struct BlockCommands;

#[async_trait::async_trait]
impl ActionGovernor for BlockCommands {
    async fn decide(&self, action: &GovernedAction<'_>) -> Result<ActionDecision, LaserError> {
        Ok(if action.kind == ActionKind::Command {
            ActionDecision::block("blocked for the test")
        } else {
            ActionDecision::allow()
        })
    }
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_child_contract_whose_command_fails_to_publish_when_sent_then_should_end_the_child_failed()
 {
    let laser = harness::laser()
        .await
        .with_governor(Arc::new(BlockCommands), GovernorMode::Enforce);
    let parent = ConversationId::new();
    let child = ConversationId::new();
    laser
        .contract(Router::to("worker".parse().expect("worker id is valid")))
        .from("orchestrator".parse().expect("orchestrator id is valid"))
        .payload(Bytes::from("task"))
        .inbox_route(InboxRoute::Fixed(AgentTopic::Sessions))
        .conversation(child)
        .parent(parent, parent)
        .deadline(Duration::from_secs(5))
        .send()
        .await
        .map(|_| ())
        .expect_err("the governor blocks the command");
    let lifecycle = || async {
        let turns = laser
            .sessions()
            .open(child)
            .context_with(Box::new(LastN(10)))
            .await
            .ok()?;
        let states: Vec<TaskState> = turns
            .iter()
            .filter_map(|turn| {
                let envelope = turn.message.envelope.as_ref()?;
                (envelope.kind == AgentKind::Status).then_some(envelope.task_state?)
            })
            .collect();
        (!states.is_empty()).then_some(states)
    };
    let states = harness::eventually(lifecycle).await;
    assert_eq!(
        states,
        [TaskState::Submitted, TaskState::Failed],
        "the child ends failed when its command never published"
    );
}
