use laser_examples::{PARTITIONS, init_tracing, laser, phase, stream_for};
use laser_sdk::prelude::full::*;

// The Context primitive: one conversation's full record, assembled on
// demand under a policy (here: the last 20 messages, further trimmed to a
// token budget). Rides ordinary log topics, so this needs no Cloud.
const LAST_N: usize = 20;
const TOKEN_BUDGET: usize = 4_000;

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    init_tracing();
    let laser = laser(&stream_for("context"), Capabilities::OPEN).await?;
    // Conversation turns ride the well-known agent topics, created once here.
    laser.bootstrap(PARTITIONS).await?;
    let conversation = ConversationId::new();

    phase("append a conversation, then assemble it under a budget");
    let scope = laser.context(conversation);
    scope
        .append(AgentTopic::Commands, "book me an aisle seat".as_bytes())
        .await?;
    scope
        .append(AgentTopic::Responses, "booked, aisle 12".as_bytes())
        .await?;

    // The shape of a prompt's context is a declared policy, not slicing logic
    // spread through the application: cap the turns, then fit the budget.
    let turns = scope
        .fetch_with(
            vec![AgentTopic::Commands, AgentTopic::Responses],
            Box::new(Chain(vec![
                Box::new(LastN(LAST_N)),
                Box::new(TokenBudget::new(TOKEN_BUDGET)),
            ])),
        )
        .await?;

    println!("  {} turn(s) within {TOKEN_BUDGET} tokens:", turns.len());
    for turn in &turns {
        println!("    {}", String::from_utf8_lossy(&turn.payload));
    }
    Ok(())
}
