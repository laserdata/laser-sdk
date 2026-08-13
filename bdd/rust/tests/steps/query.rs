use crate::common::world::LaserWorld;
use cucumber::{given, then, when};
use laser_bdd::query_engine::{QueryEngine, query_on};
use laser_sdk::query::{AggCall, AggFunc, Aggregate, CmpOp, Dir, Filter, QueryResult, Sort};

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
    let mut query = query_on(&index);
    query.filter = Some(Filter::pred("latency_ms", CmpOp::Gt, bound));
    world.last_query = Some(engine(world).execute(&query));
}

#[when(regex = r#"^I query "([^"]+)" ordered by latency_ms descending$"#)]
async fn query_ordered(world: &mut LaserWorld, index: String) {
    let mut query = query_on(&index);
    query.order = vec![Sort {
        field: "latency_ms".to_owned(),
        dir: Dir::Desc,
    }];
    world.last_query = Some(engine(world).execute(&query));
}

#[when(regex = r#"^I query "([^"]+)" with limit (\d+)$"#)]
async fn query_limited(world: &mut LaserWorld, index: String, limit: usize) {
    let mut query = query_on(&index);
    query.page.limit = u32::try_from(limit).expect("BDD page limit fits u32");
    query.page.want_total = true;
    world.last_query = Some(engine(world).execute(&query));
}

#[when(regex = r#"^I count "([^"]+)" grouped by status$"#)]
async fn query_count(world: &mut LaserWorld, index: String) {
    let mut query = query_on(&index);
    query.aggregate = Some(Aggregate {
        group_by: vec!["status".to_owned()],
        funcs: vec![AggCall {
            func: AggFunc::Count,
            field: None,
            arg: None,
            alias: "count".to_owned(),
        }],
        window: None,
    });
    world.last_query = Some(engine(world).execute(&query));
}

#[then(regex = r#"^the query returns (\d+) rows$"#)]
async fn then_returns_rows(world: &mut LaserWorld, expected: usize) {
    assert_eq!(result(world).rows.len(), expected, "row count");
}

#[then("every returned row has latency_ms greater than 500")]
async fn then_rows_exceed_bound(world: &mut LaserWorld) {
    for row in &result(world).rows {
        let latency = result(world)
            .value_i64(row, "latency_ms")
            .expect("numeric latency");
        assert!(latency > 500, "row latency {latency} should exceed 500");
    }
}

#[then(regex = r#"^the returned latency_ms values are "([^"]+)" in order$"#)]
async fn then_values_in_order(world: &mut LaserWorld, expected: String) {
    let got: Vec<String> = result(world)
        .rows
        .iter()
        .map(|row| {
            result(world)
                .value_i64(row, "latency_ms")
                .expect("numeric latency")
                .to_string()
        })
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
        .find(|row| result(world).value_text(row, "status").as_deref() == Some(&status))
        .expect("a group row for the status");
    assert_eq!(
        result(world).value_i64(row, "count"),
        Some(count as i64),
        "group count"
    );
}
