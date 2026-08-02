use laser_examples::{init_tracing, laser, managed_feature_ready, phase, stream_for};
use laser_sdk::prelude::full::*;

// The Graph primitive: nodes and edges built from what your messages mention,
// traversable and time-aware. Managed by laser-plane.
const GRAPH: &str = "kg";
const CUSTOMER: &str = "customer:42";
const RELATION: &str = "purchased";

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    init_tracing();
    let laser = laser(&stream_for("graph"), Capabilities::OPEN).await?;
    if !laser.capabilities().await.graph {
        managed_feature_ready(false, "the knowledge graph", "graph");
        return Ok(());
    }

    phase("relate entities, then traverse from one of them");
    // `link` upserts both content-addressed entity nodes and the typed edge
    // between them, so re-linking the same triple converges instead of growing.
    for product in ["product:7", "product:9"] {
        laser.graph(GRAPH).link(CUSTOMER, RELATION, product).await?;
    }

    // The same id `link` derived, rebuilt locally: a node is addressed by its
    // content, never by a server-assigned key.
    let customer = GraphNode::entity("customer", "42").id;
    let purchases = laser
        .graph(GRAPH)
        .neighbors(customer, EdgeDir::Out, Some(RELATION.to_owned()), 1)
        .await?;

    println!("  {CUSTOMER} {RELATION}:");
    for node in &purchases.nodes {
        println!("    {}", entity_of(node));
    }
    Ok(())
}

// A node's `kind:value` form, the same spelling `link` accepted: the label it
// carries plus its content-addressed `value` attribute.
fn entity_of(node: &GraphNode) -> String {
    let kind = node.labels.first().map_or("entity", String::as_str);
    let value = node
        .attrs
        .iter()
        .find(|(key, _)| key.as_str() == "value")
        .map_or_else(|| "?".to_owned(), |(_, value)| value.to_string());
    format!("{kind}:{value}")
}
