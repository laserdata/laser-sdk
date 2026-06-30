use crate::harness;
use bytes::Bytes;
use laser_sdk::prelude::*;
use laser_sdk::wire::agent::{AgentCard, CapabilityDescriptor, Health};
use std::time::Duration;

struct Worker;

impl AgentHandler for Worker {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let output = Bytes::from(format!(
            "done: {}",
            String::from_utf8_lossy(&message.payload)
        ));
        ctx.respond(output).await
    }
}

#[tokio::test]
async fn given_fanned_out_subconversations_when_aggregating_then_should_collect_every_result_at_the_root()
 {
    let laser = harness::laser().await;
    Agent::builder()
        .id("worker".parse().expect("worker is a valid agent id"))
        .listen_on(AgentTopic::Commands)
        .respond_on(AgentTopic::Responses)
        .handler(Worker)
        .build()
        .spawn(laser.clone());

    let root = Provenance::builder()
        .conversation_id(ConversationId::new())
        .build();
    let root_id = root.conversation_id;
    for i in 1..=3 {
        let subtask = laser.spawn_subconversation(&root);
        laser
            .send_agent(
                AgentTopic::Commands,
                Bytes::from(format!("sub{i}")),
                &subtask,
            )
            .await
            .expect("the subtask should be sent");
    }

    let results = harness::eventually(|| {
        let laser = laser.clone();
        async move {
            let results = ContextAssembler::builder()
                .conversation_id(root_id)
                .across_subconversations(true)
                .topics(vec![AgentTopic::Responses])
                .build()
                .assemble(&laser)
                .await
                .expect("aggregating across subconversations should succeed");
            (results.len() == 3).then_some(results)
        }
    })
    .await;

    assert_eq!(results.len(), 3);
    assert!(results.iter().all(|m| {
        m.provenance
            .agent
            .as_ref()
            .expect("agent should be set")
            .as_str()
            == "worker"
    }));
}

// A diagnose worker: replies on the shared responses topic, echoing the
// per-branch correlation so the orchestrator's gather matches it.
struct DiagnoseWorker {
    id: String,
}

impl AgentHandler for DiagnoseWorker {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let reply = Bytes::from(format!(
            "{}:{}",
            self.id,
            String::from_utf8_lossy(&message.payload)
        ));
        ctx.respond(reply).await
    }
}

// The orchestrator: on a trigger, fans out to every agent advertising the
// `diagnose` capability and reports the gather outcome on the audit topic so the
// test can observe it (a handler cannot return a value to the test directly).
struct Orchestrator;

impl AgentHandler for Orchestrator {
    async fn handle(&self, _message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let gather = ctx
            .fan_out(
                CapabilitySelector::new("diagnose", RoutePolicy::Any),
                Bytes::from("scan"),
                GatherPolicy::RequireAll,
                Duration::from_secs(10),
            )
            .await?;
        let summary = format!("ok={} failed={}", gather.ok.len(), gather.failures.len());
        ctx.reply_on(AgentTopic::Audit, Bytes::from(summary)).await
    }
}

fn diagnose_card() -> AgentCard {
    diagnose_card_with(None)
}

fn diagnose_card_with(health: Option<Health>) -> AgentCard {
    AgentCard {
        name: None,
        version: None,
        capabilities: vec![CapabilityDescriptor {
            skill_id: "diagnose".to_owned(),
            input: None,
            output: None,
            cost_class: None,
            latency_class: None,
            max_concurrency: None,
            health,
            load: None,
        }],
        ttl_micros: None,
    }
}

#[tokio::test]
async fn given_agents_advertising_a_capability_when_the_orchestrator_fans_out_then_should_route_to_each_and_gather_every_reply()
 {
    let laser = harness::laser().await;

    // Three workers, each its own consumer group, all on the shared commands
    // topic, each advertising the `diagnose` capability via a card.
    let worker_ids = ["worker-a", "worker-b", "worker-c"];
    let mut workers = Vec::new();
    for id in worker_ids {
        // The builder publishes the capability card on spawn (no manual publish),
        // so capability routing discovers the worker.
        let mut handle = Agent::builder()
            .id(id.parse().expect("worker id is valid"))
            .listen_on(AgentTopic::Commands)
            .respond_on(AgentTopic::Responses)
            .capabilities(diagnose_card().capabilities)
            .handler(DiagnoseWorker { id: id.to_owned() })
            .build()
            .spawn(laser.clone());
        handle.ready().await.expect("worker joins its group");
        workers.push(handle);
    }

    // The orchestrator routes fan-out to the shared commands topic (a fixed route,
    // since the stock Apache Iggy harness has no presence command), where each
    // branch is target-filtered to one worker.
    let mut orchestrator = Agent::builder()
        .id("orchestrator".parse().expect("orchestrator id is valid"))
        .listen_on(AgentTopic::ToolCalls)
        .respond_on(AgentTopic::Responses)
        .inbox_route(InboxRoute::Fixed(AgentTopic::Commands))
        .handler(Orchestrator)
        .build()
        .spawn(laser.clone());
    orchestrator
        .ready()
        .await
        .expect("orchestrator joins its group");

    // Trigger the fan-out. The audit reply is chained to this conversation, so the
    // test reads it back by conversation id.
    let trigger = Provenance::builder()
        .conversation_id(ConversationId::new())
        .build();
    let conversation = trigger.conversation_id;
    laser
        .send_agent(AgentTopic::ToolCalls, Bytes::from("go"), &trigger)
        .await
        .expect("the trigger should be sent");

    let summary = harness::eventually(|| {
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
                .find(|m| String::from_utf8_lossy(&m.payload).starts_with("ok="))
                .map(|m| String::from_utf8_lossy(&m.payload).into_owned())
        }
    })
    .await;

    // Every capable worker was routed to and every reply gathered, none lost.
    assert_eq!(summary, "ok=3 failed=0");

    for worker in workers {
        worker.shutdown().await.expect("worker shuts down");
    }
    orchestrator
        .shutdown()
        .await
        .expect("orchestrator shuts down");
}

#[tokio::test]
async fn given_an_unavailable_agent_when_fanning_out_then_should_route_around_it() {
    let laser = harness::laser().await;

    // Two healthy diagnose workers and one advertising itself Unavailable. The
    // unavailable worker is running, but capability resolution must skip it, so the
    // fan-out reaches only the two healthy workers.
    let healthy = ["healer-a", "healer-b"];
    let mut workers = Vec::new();
    for id in healthy {
        let mut handle = Agent::builder()
            .id(id.parse().expect("worker id is valid"))
            .listen_on(AgentTopic::Commands)
            .respond_on(AgentTopic::Responses)
            .capabilities(diagnose_card().capabilities)
            .handler(DiagnoseWorker { id: id.to_owned() })
            .build()
            .spawn(laser.clone());
        handle.ready().await.expect("worker joins its group");
        workers.push(handle);
    }
    let mut sick = Agent::builder()
        .id("sick".parse().expect("worker id is valid"))
        .listen_on(AgentTopic::Commands)
        .respond_on(AgentTopic::Responses)
        .capabilities(diagnose_card_with(Some(Health::Unavailable)).capabilities)
        .handler(DiagnoseWorker {
            id: "sick".to_owned(),
        })
        .build()
        .spawn(laser.clone());
    sick.ready().await.expect("sick worker joins its group");

    let mut orchestrator = Agent::builder()
        .id("orchestrator".parse().expect("orchestrator id is valid"))
        .listen_on(AgentTopic::ToolCalls)
        .respond_on(AgentTopic::Responses)
        .inbox_route(InboxRoute::Fixed(AgentTopic::Commands))
        .handler(Orchestrator)
        .build()
        .spawn(laser.clone());
    orchestrator
        .ready()
        .await
        .expect("orchestrator joins its group");

    let trigger = Provenance::builder()
        .conversation_id(ConversationId::new())
        .build();
    let conversation = trigger.conversation_id;
    laser
        .send_agent(AgentTopic::ToolCalls, Bytes::from("go"), &trigger)
        .await
        .expect("the trigger should be sent");

    let summary = harness::eventually(|| {
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
                .find(|m| String::from_utf8_lossy(&m.payload).starts_with("ok="))
                .map(|m| String::from_utf8_lossy(&m.payload).into_owned())
        }
    })
    .await;

    // Only the two healthy workers were routed to; the unavailable one was skipped.
    assert_eq!(summary, "ok=2 failed=0");

    for worker in workers {
        worker.shutdown().await.expect("worker shuts down");
    }
    sick.shutdown().await.expect("sick worker shuts down");
    orchestrator
        .shutdown()
        .await
        .expect("orchestrator shuts down");
}

#[tokio::test]
async fn given_a_quarantined_agent_when_fanning_out_then_should_exclude_it() {
    let laser = harness::laser().await;

    // Two healthy diagnose workers, then an operator quarantines one. The
    // quarantined worker stays running, but capability resolution must exclude it.
    let ids = ["q-worker-a", "q-worker-b"];
    let mut workers = Vec::new();
    for id in ids {
        let mut handle = Agent::builder()
            .id(id.parse().expect("worker id is valid"))
            .listen_on(AgentTopic::Commands)
            .respond_on(AgentTopic::Responses)
            .capabilities(diagnose_card().capabilities)
            .handler(DiagnoseWorker { id: id.to_owned() })
            .build()
            .spawn(laser.clone());
        handle.ready().await.expect("worker joins its group");
        workers.push(handle);
    }
    laser
        .quarantine(
            "operator".parse().expect("operator id is valid"),
            &"q-worker-a".parse().expect("worker id is valid"),
        )
        .await
        .expect("the quarantine fact publishes");

    let mut orchestrator = Agent::builder()
        .id("orchestrator".parse().expect("orchestrator id is valid"))
        .listen_on(AgentTopic::ToolCalls)
        .respond_on(AgentTopic::Responses)
        .inbox_route(InboxRoute::Fixed(AgentTopic::Commands))
        .handler(Orchestrator)
        .build()
        .spawn(laser.clone());
    orchestrator
        .ready()
        .await
        .expect("orchestrator joins its group");

    let trigger = Provenance::builder()
        .conversation_id(ConversationId::new())
        .build();
    let conversation = trigger.conversation_id;
    laser
        .send_agent(AgentTopic::ToolCalls, Bytes::from("go"), &trigger)
        .await
        .expect("the trigger should be sent");

    let summary = harness::eventually(|| {
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
                .find(|m| String::from_utf8_lossy(&m.payload).starts_with("ok="))
                .map(|m| String::from_utf8_lossy(&m.payload).into_owned())
        }
    })
    .await;

    // Only the one non-quarantined worker was routed to.
    assert_eq!(summary, "ok=1 failed=0");

    for worker in workers {
        worker.shutdown().await.expect("worker shuts down");
    }
    orchestrator
        .shutdown()
        .await
        .expect("orchestrator shuts down");
}
