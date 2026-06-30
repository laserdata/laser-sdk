use crate::harness;
use bytes::Bytes;
use laser_sdk::prelude::*;
use laser_sdk::wire::agent::{AgentCard, CapabilityDescriptor};
use std::time::Duration;

struct Crediter;

impl AgentHandler for Crediter {
    async fn handle(&self, _message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        ctx.respond(Bytes::from("credited")).await
    }
}

fn credit_card() -> AgentCard {
    AgentCard {
        name: None,
        version: None,
        capabilities: vec![CapabilityDescriptor {
            skill_id: "apply_credit".to_owned(),
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

#[tokio::test]
async fn given_a_capable_target_when_a_contract_is_sent_then_should_complete_with_the_reply() {
    let laser = harness::laser().await;
    let mut worker = Agent::builder()
        .id("crediter".parse().expect("crediter id is valid"))
        .listen_on(AgentTopic::Commands)
        .respond_on(AgentTopic::Responses)
        .capabilities(credit_card().capabilities)
        .ack_on_pickup(true)
        .handler(Crediter)
        .build()
        .spawn(laser.clone());
    worker.ready().await.expect("worker joins its group");

    // A fixed inbox route, since the stock Apache Iggy harness has no presence
    // command. The branch is target-filtered to the crediter on the shared topic.
    let outcome = laser
        .contract(Router::to_capable("apply_credit", RoutePolicy::Any))
        .from("orchestrator".parse().expect("orchestrator id is valid"))
        .payload(Bytes::from("refund-42"))
        .inbox_route(InboxRoute::Fixed(AgentTopic::Commands))
        .deadline(Duration::from_secs(10))
        .send()
        .await
        .expect("the contract resolves and sends");

    match outcome {
        Contract::Completed(reply) => {
            assert_eq!(reply.payload, b"credited");
        }
        other => panic!("expected Completed, got {other:?}"),
    }

    worker.shutdown().await.expect("worker shuts down");
}

#[tokio::test]
async fn given_no_responder_when_a_contract_is_sent_then_should_time_out() {
    let laser = harness::laser().await;

    // Addressed to an agent that is not running: the command lands but nothing
    // replies, so with no expiry set the contract returns TimedOut at the deadline
    // rather than hang.
    let outcome = laser
        .contract(Router::to("ghost".parse().expect("ghost id is valid")))
        .from("orchestrator".parse().expect("orchestrator id is valid"))
        .payload(Bytes::from("noop"))
        .inbox_route(InboxRoute::Fixed(AgentTopic::Commands))
        .deadline(Duration::from_secs(2))
        .send()
        .await
        .expect("the contract sends");

    assert!(matches!(outcome, Contract::TimedOut));
}

#[tokio::test]
async fn given_no_pickup_when_a_contract_has_an_expiry_then_should_report_not_consumed() {
    let laser = harness::laser().await;

    // Addressed to an agent that is not running, with a short expiry. No pickup
    // acknowledgment lands, so the contract reports NotConsumed once the expiry
    // passes, well before the longer completion deadline.
    let outcome = laser
        .contract(Router::to("ghost".parse().expect("ghost id is valid")))
        .from("orchestrator".parse().expect("orchestrator id is valid"))
        .payload(Bytes::from("noop"))
        .inbox_route(InboxRoute::Fixed(AgentTopic::Commands))
        .expire_if_not_consumed(Duration::from_secs(1))
        .deadline(Duration::from_secs(20))
        .send()
        .await
        .expect("the contract sends");

    assert!(matches!(outcome, Contract::NotConsumed));
}
