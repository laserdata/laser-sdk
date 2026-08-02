use laser_examples::{init_tracing, laser, phase, stream_for};
use laser_sdk::prelude::full::*;
use serde::{Deserialize, Serialize};

// The Log primitive: a topic is an append-only record of every message
// in your system. Write once, read forever, from the beginning or from now.
const STREAM: &str = "shop";
const TOPIC: &str = "orders";

#[derive(Debug, Serialize, Deserialize)]
struct Order {
    id: u32,
    total: u32,
}

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    init_tracing();
    // Connect once. `stream_for("log")` only names this run's own isolated
    // stream (so every example can run side by side); the demo below addresses
    // `shop/orders` explicitly, which any connection can reach.
    let laser = laser(&stream_for("log"), Capabilities::OPEN).await?;

    phase("write two messages, then read them back");
    let topic = laser.stream(STREAM).topic(TOPIC);
    topic.ensure(2).await?;

    for order in [Order { id: 1, total: 99 }, Order { id: 2, total: 42 }] {
        topic.publish().json(&order)?.send().await?;
    }

    // One typed handle pins the contract: `Order` in on publish, `Order` out on
    // replay. The reader starts at offset 0 and ends once it is caught up.
    let mut replay = topic.json::<Order>().records("log-example")?;
    while let Some(next) = replay.next().await {
        let order = next?.value;
        println!("  order #{} total {}", order.id, order.total);
    }
    Ok(())
}
