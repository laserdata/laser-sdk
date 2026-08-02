// The grammar's decomposition guarantee, asserted on the log itself: every
// sugar verb lowers to the same observable records the deep calls make, no
// hidden channel, no side store. A contract is one AGDX command plus reply
// correlation, a scatter is one contract per capable agent, a context append
// is an ordinary publish with pinned provenance.

use crate::harness;
use bytes::Bytes;
use laser_sdk::agent::{Clock, SystemClock};
use laser_sdk::prelude::full::*;
use laser_sdk::wire::agent::{AgentCard, AgentEnvelope, AgentKind, CapabilityDescriptor};
use laser_sdk::wire::framing::decode_named;
use std::time::Duration;

struct Echo;

impl AgentHandler for Echo {
    async fn handle(&self, _message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        ctx.respond(Bytes::from("done")).await
    }
}

fn card(skill: &str) -> AgentCard {
    AgentCard {
        name: None,
        version: None,
        capabilities: vec![CapabilityDescriptor {
            skill_id: skill.to_owned(),
            input: None,
            output: None,
            cost_class: None,
            latency_class: None,
            max_concurrency: None,
            health: None,
            load: None,
        }],
        ttl_micros: None,
    }
}

// Every AGDX command envelope currently on the commands topic.
async fn commands_on_log(laser: &Laser) -> Vec<AgentEnvelope> {
    let commands = laser.topic(AgentTopic::Commands.topic_string());
    let mut cursor = commands.replay().expect("commands topic replays");
    cursor
        .poll()
        .await
        .expect("commands topic drains")
        .iter()
        .filter_map(|message| decode_named::<AgentEnvelope>(&message.payload).ok())
        .filter(|envelope| envelope.kind == AgentKind::Command)
        .collect()
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_contract_when_completed_then_should_decompose_to_one_command_on_the_log() {
    let laser = harness::laser().await;
    let mut worker = Agent::builder()
        .id("echo".parse().expect("echo id is valid"))
        .listen_on(AgentTopic::Commands)
        .respond_on(AgentTopic::Responses)
        .capabilities(card("echo").capabilities)
        .handler(Echo)
        .build()
        .spawn(laser.clone());
    worker.ready().await.expect("worker joins its group");

    let outcome = laser
        .contract(Router::to("echo".parse().expect("echo id is valid")))
        .from("caller".parse().expect("caller id is valid"))
        .payload(Bytes::from("job-1"))
        .inbox_route(InboxRoute::Fixed(AgentTopic::Commands))
        .deadline(Duration::from_secs(10))
        .send()
        .await
        .expect("the contract sends");
    assert!(matches!(outcome, Contract::Completed(_)));

    // The contract's entire ask is one AGDX command: correlated (via the envelope
    // correlation, what lets the reply match), targeted, and rebuildable by any
    // reader from the log alone. The business idempotency key is NOT overloaded
    // with the correlation (R4), so it stays unset on a bare contract.
    let commands = commands_on_log(&laser).await;
    assert_eq!(commands.len(), 1, "a contract is exactly one command");
    let command = &commands[0];
    command.correlation.expect("a contract command correlates");
    assert!(
        command.idempotency_key.is_none(),
        "the contract correlates via the envelope correlation, not an overloaded idempotency key",
    );
    assert_eq!(
        command.target.as_ref().map(|target| target.as_str()),
        Some("echo")
    );
    assert_eq!(command.source.as_str(), "caller");

    worker.shutdown().await.expect("worker shuts down");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_scatter_when_gathered_then_should_decompose_to_one_contract_per_capable_agent() {
    let laser = harness::laser().await;
    let mut workers = Vec::new();
    for id in ["scat-a", "scat-b"] {
        let mut worker = Agent::builder()
            .id(id.parse().expect("worker id is valid"))
            .listen_on(AgentTopic::Commands)
            .respond_on(AgentTopic::Responses)
            .capabilities(card("scatter_skill").capabilities)
            .handler(Echo)
            .build()
            .spawn(harness::reconnect(&laser).await);
        worker.ready().await.expect("worker joins its group");
        workers.push(worker);
    }

    harness::eventually(|| {
        let laser = laser.clone();
        async move {
            let now = SystemClock.now_micros();
            let mut registry = laser.agent_registry().ok()?;
            registry.refresh(now).await.ok()?;
            let capable = registry.resolve("scatter_skill", now).len();
            (capable == 2).then_some(())
        }
    })
    .await;

    let replies = laser
        .scatter(
            "caller".parse().expect("caller id is valid"),
            &CapabilitySelector::new("scatter_skill", RoutePolicy::Any),
            b"fan-1",
            &InboxRoute::Fixed(AgentTopic::Commands),
            Duration::from_secs(10),
        )
        .await
        .expect("the scatter gathers");
    assert_eq!(replies.len(), 2, "both capable agents complete");

    // A scatter is nothing but contracts: one targeted command per capable
    // agent, all sharing the payload, each its own correlation.
    let commands = commands_on_log(&laser).await;
    assert_eq!(commands.len(), 2, "one command per capable agent");
    let mut targets: Vec<&str> = commands
        .iter()
        .map(|command| {
            command
                .target
                .as_ref()
                .expect("a scatter command is targeted")
                .as_str()
        })
        .collect();
    targets.sort_unstable();
    assert_eq!(targets, ["scat-a", "scat-b"]);
    let correlations: std::collections::HashSet<_> = commands
        .iter()
        .map(|command| command.correlation.expect("each branch correlates"))
        .collect();
    assert_eq!(correlations.len(), 2, "each branch is its own contract");

    for worker in workers.drain(..) {
        worker.shutdown().await.expect("worker shuts down");
    }
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_context_append_when_replayed_then_should_be_an_ordinary_publish_with_provenance() {
    let laser = harness::laser().await;
    let conversation = ConversationId::new();

    laser
        .context(conversation)
        .append(AgentTopic::Audit, b"step done".to_vec())
        .await
        .expect("the append publishes");

    // The append is an ordinary record on the audit topic: the payload
    // verbatim, the conversation pinned in provenance, nothing else. The
    // context accessor is a reading, not a store.
    let history = laser
        .context(conversation)
        .fetch(vec![AgentTopic::Audit], 10)
        .await
        .expect("the context reads back");
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].payload, b"step done");
    assert_eq!(history[0].provenance.conversation_id, conversation);

    // And the raw topic replay sees the identical payload: no wrapper, no
    // envelope, the zero-overhead publish underneath.
    let audit = laser.topic(AgentTopic::Audit.topic_string());
    let mut cursor = audit.replay().expect("audit topic replays");
    let raw = cursor.poll().await.expect("audit topic drains");
    assert_eq!(raw.len(), 1);
    assert_eq!(raw[0].payload, b"step done");
}
