use laser_examples::{PARTITIONS, init_tracing, laser, phase, stream_for};
use laser_sdk::prelude::full::*;

// The Memory primitive: remember, recall, improve, forget. This file is
// named `recall` (one of the four verbs) because `memory` already names the
// full deep-dive scenario next door. The accessor is still `laser.memory(..)`.
// `.folded()` reads the memory topic in process, so this runs with no
// managed deployment (the default `.fetch()` reads a managed read view).
const NAMESPACE: &str = "customer:42";
const FACT: &str = "Prefers aisle seats, travels monthly";

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    init_tracing();
    let laser = laser(&stream_for("recall"), Capabilities::OPEN).await?;
    // Memory records ride the well-known agent topics, created once here.
    laser.bootstrap(PARTITIONS).await?;
    let conversation = ConversationId::new();
    let scope = MemoryScope::builder().conversation(conversation).build();

    phase("all four verbs: remember, recall, improve, forget");
    let memory = laser.memory(NAMESPACE);

    let fact = memory
        .remember(FACT.as_bytes())
        .scope(conversation)
        .send()
        .await?;

    // Durable log memory recalls the newest matching facts. Similarity ranking
    // is the vector/reranker path shown in the full memory example.
    let hits = memory
        .recall(conversation)
        .recent()
        .limit(5)
        .folded()
        .fetch()
        .await?;

    println!("  newest recalled fact(s):");
    for hit in &hits {
        println!("    {}", String::from_utf8_lossy(&hit.payload));
    }

    // Reinforce what was useful, then retire it. Both are records on the
    // memory topic, so the store stays an auditable history, not a mutable cell.
    memory.improve(&scope, Feedback::new(fact, 1.0)).await?;
    memory.forget(&scope, fact).await?;
    println!("  reinforced then forgot {fact}");
    Ok(())
}
