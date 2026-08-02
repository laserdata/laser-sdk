use laser_examples::{PARTITIONS, init_tracing, laser, phase, stream_for};
use laser_sdk::prelude::full::*;
use laser_sdk::wire::agent::CapabilityDescriptor;
use std::time::Duration;

// The Fabric primitive: agents that discover each other by capability,
// take directed work with a deadline, and reply, all over the log. The
// runtime is open core, so this needs no Cloud.
const CAPABILITY: &str = "resolve-ticket";
const DEADLINE: Duration = Duration::from_secs(60);

struct Triage;

impl AgentHandler for Triage {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        println!(
            "  triage picked up \"{}\"",
            String::from_utf8_lossy(message.body())
        );
        ctx.respond("on it").await
    }
}

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    init_tracing();
    let laser = laser(&stream_for("agent"), Capabilities::OPEN).await?;
    // The well-known agent topics (commands, responses, registry, ...) must
    // exist before an agent's consumer group joins one.
    laser.bootstrap(PARTITIONS).await?;

    phase("spawn a handler, then hand it a deadline-bounded task");
    let mut triage = Agent::builder()
        .id("triage".parse()?)
        .listen_on(AgentTopic::Commands)
        .respond_on(AgentTopic::Responses)
        // The advertised capability is what makes this agent addressable by
        // what it can do rather than by the name it happens to run under.
        .capabilities(vec![CapabilityDescriptor {
            skill_id: CAPABILITY.to_owned(),
            ..Default::default()
        }])
        // Acknowledge on pickup, so a crash mid-handler is a retry rather than
        // a silently dropped task.
        .ack_on_pickup(true)
        .handler(Triage)
        .build()
        .spawn(laser.clone());
    triage.ready().await?;

    // A contract is a directed task with a deadline and a real answer: consumed,
    // completed, failed, or timed out. Routed by capability, not by name.
    let outcome = laser
        .contract(Router::to_capable(CAPABILITY, RoutePolicy::Any))
        .from("orchestrator".parse()?)
        .payload("ticket #42 is stuck")
        .inbox_route(InboxRoute::Fixed(AgentTopic::Commands))
        .deadline(DEADLINE)
        .send()
        .await?;

    match outcome {
        Contract::Completed(reply) => {
            println!(
                "  contract completed: {}",
                String::from_utf8_lossy(reply.body())
            );
        }
        other => println!("  contract ended without a reply: {other:?}"),
    }
    triage.shutdown().await?;
    Ok(())
}
