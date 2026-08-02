use laser_examples::{
    PARTITIONS, ensure_view, index_for, init_tracing, laser, managed_feature_ready, phase,
    stream_for, wait_for_rows,
};
use laser_sdk::prelude::full::*;
use serde::{Deserialize, Serialize};

// The Views primitive: a projection watches a topic and keeps an
// always-current, queryable table. This needs a managed deployment. Apache
// Iggy without a managed backend prints how to point at one and exits.
const TOPIC: &str = "orders";
const FIELDS: [&str; 3] = ["id", "total", "status"];

#[derive(Debug, Serialize, Deserialize)]
struct Order {
    id: u32,
    total: u32,
    status: String,
}

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    init_tracing();
    let laser = laser(&stream_for("query"), Capabilities::OPEN).await?;
    if !laser.capabilities().await.query.available {
        managed_feature_ready(false, "views (query)", "query");
        return Ok(());
    }

    phase("keep a queryable view of a topic, then query it");
    laser.topic(TOPIC).ensure(PARTITIONS).await?;
    // Declare this run's `orders_v1_<token>` view over `orders`. From here the
    // view maintains itself: every record published to the topic lands in the
    // table, and the per-run name means the counts below are this run's alone.
    let index = index_for("orders_v1");
    ensure_view(&laser, TOPIC, &index, ContentType::Json, &FIELDS).await?;

    let orders = sample_orders();
    for order in &orders {
        laser.topic(TOPIC).publish().json(order)?.send().await?;
    }
    wait_for_rows(&laser, &index, orders.len() as u64).await?;

    // `where_eq` matches an indexed key, the cheap path a projection's key
    // columns answer directly. `filter_eq` and its siblings cover the rest.
    let paid = laser
        .query(&index)
        .where_eq("status", "paid")
        .limit(10)
        .fetch()
        .await?;

    println!("  {} of {} orders are paid", paid.rows.len(), orders.len());
    for row in paid.rows {
        println!(
            "    order #{} total {}",
            row.headers["id"], row.headers["total"]
        );
    }
    Ok(())
}

fn sample_orders() -> Vec<Order> {
    [(1, 99, "paid"), (2, 42, "pending"), (3, 15, "paid")]
        .into_iter()
        .map(|(id, total, status)| Order {
            id,
            total,
            status: status.to_owned(),
        })
        .collect()
}
