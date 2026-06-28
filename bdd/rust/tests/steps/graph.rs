use crate::common::world::LaserWorld;
use cucumber::{given, then, when};
use laser_bdd::graph_engine::{Dir, GraphEngine};

fn engine(world: &mut LaserWorld) -> &mut GraphEngine {
    world.graph_engine.as_mut().expect("a graph was opened")
}

#[given("an empty graph")]
async fn open_graph(world: &mut LaserWorld) {
    world.graph_engine = Some(GraphEngine::new());
}

#[when(regex = r#"^I observe "([^"]+)" (\w+) "([^"]+)"$"#)]
async fn observe(world: &mut LaserWorld, from: String, edge_type: String, to: String) {
    let graph = engine(world);
    let from_id = graph.upsert_node(&from);
    let to_id = graph.upsert_node(&to);
    graph.add_edge(from_id, &edge_type, to_id);
}

#[when(regex = r#"^I observe "([^"]+)" (\w+) "([^"]+)" valid from (\d+)$"#)]
async fn observe_valid_from(
    world: &mut LaserWorld,
    from: String,
    edge_type: String,
    to: String,
    valid_from: u64,
) {
    let graph = engine(world);
    let from_id = graph.upsert_node(&from);
    let to_id = graph.upsert_node(&to);
    graph.add_edge_valid(from_id, &edge_type, to_id, Some(valid_from), None);
}

#[then(regex = r#"^the graph holds (\d+) nodes$"#)]
async fn node_count(world: &mut LaserWorld, count: usize) {
    assert_eq!(engine(world).node_count(), count, "distinct node count");
}

#[then(regex = r#"^traversing from "([^"]+)" out "(\w+)" then "(\w+)" reaches "([^"]+)"$"#)]
async fn traverse_two_out(
    world: &mut LaserWorld,
    start: String,
    first: String,
    second: String,
    target: String,
) {
    let hops = vec![(first, Dir::Out), (second, Dir::Out)];
    let reached = engine(world).traverse(&start, &hops);
    assert!(reached.contains(&target), "{target} should be reachable");
}

#[then(regex = r#"^traversing from "([^"]+)" out "(\w+)" reaches "([^"]+)"$"#)]
async fn traverse_out_reaches(world: &mut LaserWorld, start: String, edge: String, target: String) {
    let reached = engine(world).traverse(&start, &[(edge, Dir::Out)]);
    assert!(reached.contains(&target), "{target} should be reachable");
}

#[then(regex = r#"^traversing from "([^"]+)" out "(\w+)" does not reach "([^"]+)"$"#)]
async fn traverse_out_excludes(
    world: &mut LaserWorld,
    start: String,
    edge: String,
    target: String,
) {
    let reached = engine(world).traverse(&start, &[(edge, Dir::Out)]);
    assert!(
        !reached.contains(&target),
        "{target} should not be reachable"
    );
}

#[then(regex = r#"^traversing from "([^"]+)" incoming "(\w+)" reaches "([^"]+)"$"#)]
async fn traverse_in_reaches(world: &mut LaserWorld, start: String, edge: String, target: String) {
    let reached = engine(world).traverse(&start, &[(edge, Dir::In)]);
    assert!(reached.contains(&target), "{target} should be reachable");
}

#[then(regex = r#"^traversing from "([^"]+)" out "(\w+)" as of (\d+) reaches "([^"]+)"$"#)]
async fn traverse_as_of_reaches(
    world: &mut LaserWorld,
    start: String,
    edge: String,
    at: u64,
    target: String,
) {
    let reached = engine(world).traverse_as_of(&start, &[(edge, Dir::Out)], Some(at));
    assert!(reached.contains(&target), "{target} should be reachable");
}

#[then(regex = r#"^traversing from "([^"]+)" out "(\w+)" as of (\d+) does not reach "([^"]+)"$"#)]
async fn traverse_as_of_excludes(
    world: &mut LaserWorld,
    start: String,
    edge: String,
    at: u64,
    target: String,
) {
    let reached = engine(world).traverse_as_of(&start, &[(edge, Dir::Out)], Some(at));
    assert!(
        !reached.contains(&target),
        "{target} should not be reachable"
    );
}

#[when(regex = r#"^I observe "([^"]+)" (\w+) "([^"]+)" from "([^"]+)"$"#)]
async fn observe_with_source(
    world: &mut LaserWorld,
    from: String,
    edge_type: String,
    to: String,
    source: String,
) {
    let graph = engine(world);
    let from_id = graph.upsert_node_with_source(&from, &source);
    let to_id = graph.upsert_node_with_source(&to, &source);
    graph.add_edge_with_source(from_id, &edge_type, to_id, &source);
}

#[then(regex = r#"^the source of node "([^"]+)" is "([^"]+)"$"#)]
async fn node_source_is(world: &mut LaserWorld, value: String, source: String) {
    assert_eq!(engine(world).node_source(&value), Some(source.as_str()));
}

#[then(regex = r#"^the source of edge "([^"]+)" (\w+) "([^"]+)" is "([^"]+)"$"#)]
async fn edge_source_is(
    world: &mut LaserWorld,
    from: String,
    edge_type: String,
    to: String,
    source: String,
) {
    assert_eq!(
        engine(world).edge_source(&from, &edge_type, &to),
        Some(source.as_str())
    );
}
