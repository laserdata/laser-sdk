use laser_examples::{
    PARTITIONS, ensure_view, fresh_run, index_for, init_tracing, laser, managed_feature_ready,
    phase, stream_for,
};
use laser_sdk::prelude::full::*;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

// The Change feed primitive: consume lightweight advancement records instead
// of rerunning a query on a timer. Rides the same view shape as the Views
// example (`readings_v1` over `readings`), so it also needs a managed deployment.
const TOPIC: &str = "readings";
const FIELDS: [&str; 3] = ["host", "cpu", "status"];
const CHANGE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Serialize, Deserialize)]
struct Reading {
    host: String,
    cpu: u32,
    status: String,
}

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    init_tracing();
    let laser = laser(&stream_for("watch"), Capabilities::OPEN).await?;
    fresh_run(&laser, &stream_for("watch"), async {
        let capabilities = laser.capabilities().await;
        if !(capabilities.query.available && capabilities.watch) {
            managed_feature_ready(false, "the change feed", "watch");
            return Ok(());
        }

        phase("watch a view, then publish something that advances it");
        laser.topic(TOPIC).ensure(PARTITIONS).await?;
        // The same view shape the Views example declares, under this run's own
        // name, so this binary runs on its own with no shared state.
        let index = index_for("readings_v1");
        ensure_view(&laser, TOPIC, &index, ContentType::Json, &FIELDS).await?;

        let mut feed = laser.watch().index(&index).records()?;

        laser
            .topic(TOPIC)
            .publish()
            .json(&Reading {
                host: "node-4".to_owned(),
                cpu: 88,
                status: "degraded".to_owned(),
            })?
            .send()
            .await?;

        let deadline = Instant::now() + CHANGE_TIMEOUT;
        loop {
            let changes = feed.poll().await?;
            if !changes.is_empty() {
                for change in changes {
                    println!(
                        "  view advanced: {} row(s), source offsets {}..{}",
                        change.rows, change.from_offset, change.to_offset
                    );
                }
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(LaserError::Invalid(format!(
                    "no change on `{index}` arrived in time"
                )));
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
}
