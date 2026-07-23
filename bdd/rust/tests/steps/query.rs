use crate::common::world::LaserWorld;
use cucumber::{given, then, when};
use laser_bdd::query_engine::QueryEngine;
use laser_sdk::query::{
    AggCall, AggFunc, Aggregate, CmpOp, Dir, Filter, Predicate, Query, QueryResult, Sort, Value,
};

fn engine(world: &LaserWorld) -> &QueryEngine {
    world.query_engine.as_ref().expect("a seeded query index")
}

fn result(world: &LaserWorld) -> &QueryResult {
    world.last_query.as_ref().expect("a query was run")
}

#[given(regex = r#"^a query index "([^"]+)" seeded with sample api-call rows$"#)]
async fn seed_index(world: &mut LaserWorld, index: String) {
    let mut query_engine = QueryEngine::new();
    for (status, latency) in [("200", "10"), ("200", "550"), ("500", "900"), ("200", "30")] {
        query_engine.insert(
            &index,
            QueryEngine::row(&[("status", status), ("latency_ms", latency)]),
        );
    }
    world.query_engine = Some(query_engine);
}

#[when(regex = r#"^I query "([^"]+)" for latency_ms greater than (\d+)$"#)]
async fn query_filter(world: &mut LaserWorld, index: String, bound: i64) {
    let query = Query {
        index,
        filter: Some(Filter::Pred(Predicate {
            field: "latency_ms".to_owned(),
            op: CmpOp::Gt,
            value: Value::Int(bound),
        })),
        ..Default::default()
    };
    world.last_query = Some(engine(world).execute(&query));
}

#[when(regex = r#"^I query "([^"]+)" ordered by latency_ms descending$"#)]
async fn query_ordered(world: &mut LaserWorld, index: String) {
    let query = Query {
        index,
        order: vec![Sort {
            field: "latency_ms".to_owned(),
            dir: Dir::Desc,
        }],
        ..Default::default()
    };
    world.last_query = Some(engine(world).execute(&query));
}

#[when(regex = r#"^I query "([^"]+)" with limit (\d+)$"#)]
async fn query_limited(world: &mut LaserWorld, index: String, limit: usize) {
    let query = Query {
        index,
        limit,
        want_total: true,
        ..Default::default()
    };
    world.last_query = Some(engine(world).execute(&query));
}

#[when(regex = r#"^I count "([^"]+)" grouped by status$"#)]
async fn query_count(world: &mut LaserWorld, index: String) {
    let query = Query {
        index,
        aggregate: Some(Aggregate {
            group_by: vec!["status".to_owned()],
            funcs: vec![AggCall {
                func: AggFunc::Count,
                field: None,
                arg: None,
                alias: "count".to_owned(),
            }],
            window: None,
        }),
        ..Default::default()
    };
    world.last_query = Some(engine(world).execute(&query));
}

#[then(regex = r#"^the query returns (\d+) rows$"#)]
async fn then_returns_rows(world: &mut LaserWorld, expected: usize) {
    assert_eq!(result(world).rows.len(), expected, "row count");
}

#[then("every returned row has latency_ms greater than 500")]
async fn then_rows_exceed_bound(world: &mut LaserWorld) {
    for row in &result(world).rows {
        let latency: i64 = row.headers["latency_ms"].parse().expect("numeric latency");
        assert!(latency > 500, "row latency {latency} should exceed 500");
    }
}

#[then(regex = r#"^the returned latency_ms values are "([^"]+)" in order$"#)]
async fn then_values_in_order(world: &mut LaserWorld, expected: String) {
    let got: Vec<String> = result(world)
        .rows
        .iter()
        .map(|row| row.headers["latency_ms"].clone())
        .collect();
    let want: Vec<String> = expected.split(", ").map(str::to_owned).collect();
    assert_eq!(got, want, "ordered latency values");
}

#[then(regex = r#"^the page total is (\d+)$"#)]
async fn then_page_total(world: &mut LaserWorld, total: usize) {
    assert_eq!(
        result(world).page.total,
        Some(total as u64),
        "page total counts every match"
    );
}

#[then(regex = r#"^group "([^"]+)" has count (\d+)$"#)]
async fn then_group_count(world: &mut LaserWorld, status: String, count: usize) {
    let row = result(world)
        .rows
        .iter()
        .find(|row| row.headers.get("status") == Some(&status))
        .expect("a group row for the status");
    assert_eq!(row.headers["count"], count.to_string(), "group count");
}
