#![cfg(all(feature = "a2a-bridge", feature = "kv"))]
// A caller fails to build past its recursion limit, 128 by default, so the public futures stay under 110.
#![recursion_limit = "110"]

use iggy::prelude::IggyClient;
use laser_sdk::prelude::full::*;
use laser_sdk::types::AgentId;
use std::time::Duration;

struct Responder;

impl AgentHandler for Responder {
    async fn handle(&self, _message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        ctx.respond("done").await
    }
}

#[test]
fn given_a_publish_when_built_under_a_recursion_limit_of_110_then_should_lay_out_and_be_send() {
    let laser = Laser::from_client(IggyClient::default());
    let topic = laser.stream("stream").topic("topic");

    assert_bounded(|| topic.publish().send());
}

#[test]
fn given_a_group_consumer_read_when_built_under_a_recursion_limit_of_110_then_should_lay_out_and_be_send()
 {
    let laser = Laser::from_client(IggyClient::default());
    let topic = laser.stream("stream").topic("topic");

    assert_bounded(move || async move {
        let mut consumer = topic.consumer_group("group").consumer().build().await?;
        futures::StreamExt::next(&mut consumer).await;
        Ok::<_, LaserError>(())
    });
}

#[test]
fn given_a_contract_when_built_under_a_recursion_limit_of_110_then_should_lay_out_and_be_send() {
    let laser = Laser::from_client(IggyClient::default());

    assert_bounded(|| {
        laser
            .contract(Router::to_capable("skill", RoutePolicy::Any))
            .from(agent("caller"))
            .payload(b"task".as_slice())
            .inbox_route(InboxRoute::Fixed(AgentTopic::Sessions))
            .deadline(Duration::from_secs(1))
            .send()
    });
}

#[test]
fn given_a_scatter_when_built_under_a_recursion_limit_of_110_then_should_lay_out_and_be_send() {
    let laser = Laser::from_client(IggyClient::default());
    let selector = CapabilitySelector::new("skill", RoutePolicy::Any);
    let route = InboxRoute::Fixed(AgentTopic::Sessions);

    assert_bounded(|| {
        laser.scatter(
            agent("caller"),
            &selector,
            b"task".as_slice(),
            &route,
            Duration::from_secs(1),
        )
    });
}

#[test]
fn given_a_workflow_run_when_built_under_a_recursion_limit_of_110_then_should_lay_out_and_be_send()
{
    let laser = Laser::from_client(IggyClient::default());

    assert_bounded(|| {
        laser
            .workflow("workflow")
            .step(
                "step",
                Router::to_capable("skill", RoutePolicy::Any),
                |_context: &StepContext<'_>| Vec::new(),
            )
            .run()
    });
}

#[test]
fn given_an_a2a_submit_when_built_under_a_recursion_limit_of_110_then_should_lay_out_and_be_send() {
    let laser = Laser::from_client(IggyClient::default());
    let bridge = A2aBridge::new(
        laser,
        agent("gateway"),
        AgentTopic::Sessions,
        AgentTopic::Sessions,
    );

    assert_bounded(|| bridge.submit_to(agent("assistant"), Vec::new()));
}

#[test]
fn given_a_key_value_write_when_built_under_a_recursion_limit_of_110_then_should_lay_out_and_be_send()
 {
    let laser = Laser::from_client(IggyClient::default());

    assert_bounded(|| async { laser.kv("namespace").set("key").json(&1u32)?.send().await });
}

#[test]
fn given_an_agent_handler_that_responds_when_built_under_a_recursion_limit_of_110_then_should_be_accepted()
 {
    let _agent = Agent::builder().handler(Responder);
}

fn assert_bounded<F: Future + Send>(_create: impl FnOnce() -> F) {
    assert!(size_of::<F>() > 0);
}

fn agent(id: &str) -> AgentId {
    id.parse().expect("valid agent id")
}
