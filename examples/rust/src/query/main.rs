use laser_examples::{
    PARTITIONS, ensure_view, fresh_run, index_for, init_tracing, laser, managed_feature_ready,
    phase, stream_for, wait_for_rows,
};
use laser_sdk::prelude::full::*;
use serde::{Deserialize, Serialize};

// The Views primitive: a projection watches a topic and keeps an
// always-current, queryable table. This needs a managed deployment. Apache
// Iggy without a managed backend prints how to point at one and exits.
const TOPIC: &str = "readings";
const FIELDS: [&str; 3] = ["host", "cpu", "status"];

#[derive(Debug, Serialize, Deserialize)]
struct Reading {
    host: String,
    cpu: u32,
    status: String,
}

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    init_tracing();
    let laser = laser(&stream_for("query"), Capabilities::OPEN).await?;
    fresh_run(&laser, &stream_for("query"), async {
        if !laser.capabilities().await.query.available {
            managed_feature_ready(false, "views (query)", "query");
            return Ok(());
        }

        phase("keep a queryable view of a topic, then query it");
        laser.topic(TOPIC).ensure(PARTITIONS).await?;
        // Declare this run's `readings_v1_<token>` view over `readings`. From here the
        // view maintains itself: every record published to the topic lands in the
        // table, and the per-run name means the counts below are this run's alone.
        let index = index_for("readings_v1");
        ensure_view(&laser, TOPIC, &index, ContentType::Json, &FIELDS).await?;

        let readings = sample_readings();
        for reading in &readings {
            laser.topic(TOPIC).publish().json(reading)?.send().await?;
        }
        wait_for_rows(&laser, &index, readings.len() as u64).await?;

        // `where_eq` matches an indexed key, the cheap path a projection's key
        // columns answer directly. `filter_eq` and its siblings cover the rest.
        let degraded = laser
            .query(&index)
            .where_eq("status", "degraded")
            .limit(10)
            .fetch()
            .await?;

        println!(
            "  {} of {} hosts are degraded",
            degraded.rows.len(),
            readings.len()
        );
        for row in &degraded.rows {
            println!(
                "    host {} cpu {}",
                degraded
                    .value_text(row, "host")
                    .unwrap_or_else(|| "?".to_owned()),
                degraded
                    .value_text(row, "cpu")
                    .unwrap_or_else(|| "?".to_owned())
            );
        }
        Ok(())
    })
    .await
}

fn sample_readings() -> Vec<Reading> {
    [
        ("node-1", 42, "ok"),
        ("node-2", 91, "degraded"),
        ("node-3", 17, "ok"),
    ]
    .into_iter()
    .map(|(host, cpu, status)| Reading {
        host: host.to_owned(),
        cpu,
        status: status.to_owned(),
    })
    .collect()
}
