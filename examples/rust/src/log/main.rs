use laser_examples::{fresh_run, init_tracing, laser, phase, stream_for};
use laser_sdk::prelude::full::*;
use serde::{Deserialize, Serialize};

// The Log primitive: a topic is an append-only record of every message
// in your system. Write once, read forever, from the beginning or from now.
const STREAM: &str = "fleet";
const TOPIC: &str = "readings";

#[derive(Debug, Serialize, Deserialize)]
struct Reading {
    host: String,
    cpu: u32,
}

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    init_tracing();
    // Connect once. `stream_for("log")` only names this run's own isolated
    // stream so every example can run side by side. The demo below addresses
    // `fleet/readings` explicitly, which any connection can reach.
    let laser = laser(&stream_for("log"), Capabilities::OPEN).await?;
    fresh_run(&laser, &stream_for("log"), async {
        phase("write two messages, then read them back");
        let topic = laser.stream(STREAM).topic(TOPIC);
        topic.ensure(2).await?;

        for reading in [
            Reading {
                host: "node-1".to_owned(),
                cpu: 42,
            },
            Reading {
                host: "node-2".to_owned(),
                cpu: 91,
            },
        ] {
            topic.publish().json(&reading)?.send().await?;
        }

        // One typed handle pins the contract: `Reading` in on publish, `Reading` out
        // on replay. The reader starts at offset 0 and ends once it is caught up.
        let mut replay = topic.json::<Reading>().records("log-example")?;
        while let Some(next) = replay.next().await {
            let reading = next?.value;
            println!("  reading {} cpu {}", reading.host, reading.cpu);
        }
        Ok(())
    })
    .await
}
