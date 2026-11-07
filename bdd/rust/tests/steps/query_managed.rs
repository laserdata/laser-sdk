use crate::common::world::LaserWorld;
use cucumber::{given, then, when};
use laser_sdk::materialize::{MaterializationReadiness, MaterializationWait};
use laser_sdk::prelude::QueryResult;
use laser_sdk::query::{Projection, Row};
use laser_sdk::wire::content::ContentType;
use serde_json::json;
use std::time::Duration;

const CATCH_UP: Duration = Duration::from_secs(120);

#[given(regex = r#"^(\d+) api-call records published and materialized into "([^"]+)"$"#)]
async fn materialized_api_calls(world: &mut LaserWorld, count: i64, topic: String) {
    let handle = world.laser().topic(&topic);
    handle.ensure(1, None).await.expect("the topic exists");
    let projection = Projection::builder(topic.as_str())
        .content_type(ContentType::Json)
        .field("status")
        .field("latency_ms")
        .build();
    let materialization = handle
        .materialize()
        .typed(projection)
        .apply()
        .await
        .expect("the materialization is set up");
    let mut batch = handle.publish_batch();
    for latency in 0..count {
        let status = if latency % 4 == 0 { "500" } else { "200" };
        batch = batch
            .add_json(&json!({ "status": status, "latency_ms": latency }))
            .expect("the record encodes");
    }
    batch.send().await.expect("the records publish");
    let wait = materialization
        .wait(MaterializationReadiness::CaughtUp, CATCH_UP)
        .await
        .expect("the wait answers");
    assert!(
        matches!(wait, MaterializationWait::Ready { .. }),
        "{wait:?}"
    );
}

#[when(
    regex = r#"^the materialized "([^"]+)" is queried for latency_ms greater than (\d+) with its total$"#
)]
async fn filtered(world: &mut LaserWorld, index: String, bound: i64) {
    let result = world
        .laser()
        .query(&index)
        .filter_gt("latency_ms", bound)
        .with_total()
        .fetch()
        .await
        .expect("the query answers");
    world.last_query = Some(result);
}

#[when(
    regex = r#"^the materialized "([^"]+)" is queried ordered by latency_ms descending with limit (\d+)$"#
)]
async fn ordered(world: &mut LaserWorld, index: String, limit: usize) {
    let result = world
        .laser()
        .query(&index)
        .order_desc("latency_ms")
        .limit(limit)
        .fetch()
        .await
        .expect("the query answers");
    world.last_query = Some(result);
}

#[when(regex = r#"^the materialized "([^"]+)" is queried with limit (\d+) and its total$"#)]
async fn limited(world: &mut LaserWorld, index: String, limit: usize) {
    let result = world
        .laser()
        .query(&index)
        .limit(limit)
        .with_total()
        .fetch()
        .await
        .expect("the query answers");
    world.last_query = Some(result);
}

#[when(regex = r#"^the materialized "([^"]+)" is counted grouped by status$"#)]
async fn counted(world: &mut LaserWorld, index: String) {
    let result = world
        .laser()
        .query(&index)
        .count()
        .group_by(["status"])
        .fetch()
        .await
        .expect("the query answers");
    world.last_query = Some(result);
}

#[when(regex = r#"^the materialized "([^"]+)" is walked in pages of (\d+) ordered by latency_ms$"#)]
async fn walked(world: &mut LaserWorld, index: String, page: usize) {
    let mut latencies = Vec::new();
    loop {
        let result = world
            .laser()
            .query(&index)
            .order_asc("latency_ms")
            .limit(page)
            .offset(latencies.len())
            .fetch()
            .await
            .expect("the page answers");
        let rows = result.rows.len();
        latencies.extend(result.rows.iter().map(|row| latency(&result, row)));
        if rows < page {
            break;
        }
    }
    world.walked_latencies = latencies;
}

#[when(
    regex = r#"^the materialized "([^"]+)" is read whole in pages of (\d+) ordered by latency_ms$"#
)]
async fn read_whole(world: &mut LaserWorld, index: String, page: usize) {
    let walked = world
        .laser()
        .query(&index)
        .order_asc("latency_ms")
        .limit(page)
        .fetch_all()
        .await
        .expect("the whole read answers");
    world.walk_frontier_moved = Some(walked.frontier_moved);
    world.walked_latencies = walked
        .rows
        .iter()
        .map(|row| row.value_i64("latency_ms").expect("latency_ms is a number"))
        .collect();
}

#[then(regex = r"^the whole read reports its frontier unmoved$")]
fn frontier_unmoved(world: &mut LaserWorld) {
    assert_eq!(world.walk_frontier_moved, Some(false));
}

#[then(regex = r"^the materialized total is (\d+)$")]
fn total(world: &mut LaserWorld, expected: u64) {
    assert_eq!(answered(world).page.total, Some(expected));
}

#[then(regex = r"^every materialized row has latency_ms greater than (\d+)$")]
fn rows_above(world: &mut LaserWorld, bound: i64) {
    let result = answered(world);
    assert!(!result.rows.is_empty(), "the page holds matching rows");
    for row in &result.rows {
        assert!(latency(result, row) > bound);
    }
}

#[then(regex = r#"^the materialized latency_ms values are "([^"]+)" in order$"#)]
fn values_in_order(world: &mut LaserWorld, expected: String) {
    let result = answered(world);
    let got: Vec<String> = result
        .rows
        .iter()
        .map(|row| latency(result, row).to_string())
        .collect();
    let want: Vec<String> = expected.split(", ").map(str::to_owned).collect();
    assert_eq!(got, want);
}

#[then(regex = r"^the materialized page holds (\d+) rows$")]
fn page_rows(world: &mut LaserWorld, rows: usize) {
    assert_eq!(answered(world).rows.len(), rows);
}

#[then(regex = r#"^the materialized group "([^"]+)" has count (\d+)$"#)]
fn group_count(world: &mut LaserWorld, status: String, count: i64) {
    let result = answered(world);
    let row = result
        .rows
        .iter()
        .find(|row| result.value_text(row, "status").as_deref() == Some(status.as_str()))
        .expect("a group row for the status");
    assert_eq!(result.value_i64(row, "count"), Some(count));
}

#[then(regex = r"^the walk returns every latency_ms from (\d+) to (\d+) once$")]
fn walk_complete(world: &mut LaserWorld, first: i64, last: i64) {
    let expected: Vec<i64> = (first..=last).collect();
    assert_eq!(world.walked_latencies, expected);
}

fn answered(world: &LaserWorld) -> &QueryResult {
    world.last_query.as_ref().expect("a query ran")
}

fn latency(result: &QueryResult, row: &Row) -> i64 {
    result
        .value_i64(row, "latency_ms")
        .expect("a numeric latency_ms")
}
