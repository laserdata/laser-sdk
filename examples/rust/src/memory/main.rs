use laser_examples::{fork_feature_ready, init_tracing, laser, phase, stream_for};
use laser_sdk::prelude::*;
use std::collections::HashMap;
use tracing::info;

// Agentic memory, both halves. An agent gets better with use two ways, and this
// runs them over one incident-knowledge domain:
//
//   MEMORY (in-process, no server)   remember what you know, recall what is
//                                    relevant, improve recall from feedback, and
//                                    forget what is stale. The four verbs as one
//                                    loop over `VectorMemory`.
//
//   KNOWLEDGE GRAPH (managed)        the durable side: entities become
//                                    content-addressed nodes, relationships typed
//                                    edges, and a traversal answers how things
//                                    connect, what recall alone cannot.
//
//   cargo run --example memory
//
// The memory half needs no server. The graph half is a managed read model (AGDX
// A13): point the example at a LaserData Cloud deployment to run it, otherwise it
// prints how and exits clean. The named graph is browsable in the management
// console's graph explorer.

// What the assistant knows, each line one remembered fact.
const KNOWLEDGE: &[&str] = &[
    "checkout latency spikes are usually database connection pool exhaustion",
    "billing double-charges trace back to retries without an idempotency key",
    "search returning stale results means the nightly index rebuild failed",
    "checkout pages recover fastest by failing over to the read replica",
    "auth token errors after a deploy come from the rotated signing key",
    "cart abandonment climbs when the cache eviction rate is set too aggressive",
    "inventory drift is the message queue dropping stock-adjustment events",
    "recommendation gaps appear when the search index lags behind the catalog",
];

// A deterministic bag-of-words embedder, the model seam an app fills (here a real
// deployment would call an embedding model). Each token hashes into one of `DIMS`
// buckets and the vector is L2-normalized, so cosine similarity reflects shared
// vocabulary. Good enough to rank related facts, and reproducible.
const DIMS: usize = 64;

// The knowledge graph the second half builds and traverses.
const GRAPH: &str = "ops";

struct BagOfWords;

impl Embedder for BagOfWords {
    async fn embed(&self, text: &str) -> Result<Vec<f32>, LaserError> {
        let mut vector = vec![0.0f32; DIMS];
        for token in text.split_whitespace() {
            let token = token.trim_matches(|c: char| !c.is_alphanumeric());
            if token.is_empty() {
                continue;
            }
            let bucket = token.to_ascii_lowercase().bytes().fold(0u32, |hash, byte| {
                hash.wrapping_mul(31).wrapping_add(byte as u32)
            });
            vector[bucket as usize % DIMS] += 1.0;
        }
        let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
        if norm > 0.0 {
            for value in &mut vector {
                *value /= norm;
            }
        }
        Ok(vector)
    }
}

// One conversation holds the assistant's working memory for this session.
fn scope(conversation: ConversationId) -> MemoryScope {
    MemoryScope::builder().conversation(conversation).build()
}

// Recall the top `limit` facts closest to `question`, returning (id, text, score).
async fn recall(
    memory: &VectorMemory<BagOfWords>,
    conversation: ConversationId,
    question: &str,
    limit: usize,
) -> Result<Vec<(MemoryId, String, f32)>, LaserError> {
    let query = MemoryQuery::builder()
        .semantic(question.to_owned())
        .limit(limit)
        .build();
    let hits = Memory::recall(memory, &scope(conversation), &query).await?;
    Ok(hits
        .into_iter()
        .map(|hit| {
            let text = String::from_utf8_lossy(&hit.payload).into_owned();
            (hit.id, text, hit.score.unwrap_or(0.0))
        })
        .collect())
}

fn print_hits(label: &str, hits: &[(MemoryId, String, f32)]) {
    info!("{label}");
    for (rank, (_, text, score)) in hits.iter().enumerate() {
        info!("  {}. ({score:.3}) {text}", rank + 1);
    }
}

// Pull a node's `value` attribute back out for display.
fn value_of(node: &GraphNode) -> &str {
    node.attrs
        .iter()
        .find(|(key, _)| key == "value")
        .and_then(|(_, value)| match value {
            Value::Str(text) => Some(text.as_str()),
            _ => None,
        })
        .unwrap_or("?")
}

fn print_nodes(label: &str, nodes: &[GraphNode]) {
    let mut values: Vec<&str> = nodes.iter().map(value_of).collect();
    values.sort_unstable();
    info!("{label}: {}", values.join(", "));
}

// A traversal result is seeded with its start frontier, so the start nodes ride
// along in `nodes`. Narrow to one entity kind to print just what was reached.
fn print_nodes_of(label: &str, kind: &str, nodes: &[GraphNode]) {
    let mut values: Vec<&str> = nodes
        .iter()
        .filter(|node| node.labels.iter().any(|l| l == kind))
        .map(value_of)
        .collect();
    values.sort_unstable();
    info!("{label}: {}", values.join(", "));
}

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    init_tracing();
    let conversation = ConversationId::new();

    // PART 1 - MEMORY. The four agentic-memory verbs as one loop, in process over
    // `VectorMemory`, so this half needs no server.
    let memory = VectorMemory::new(BagOfWords);

    // REMEMBER. Store what the assistant knows, keeping the ids of the note the
    // operator upvotes and the one it later forgets.
    phase("Remember");
    let mut replica_note = None;
    let mut stale_note = None;
    for fact in KNOWLEDGE {
        let id = memory
            .remember(&scope(conversation), fact.as_bytes().to_vec())
            .await?;
        if fact.contains("read replica") {
            replica_note = Some(id);
        }
        if fact.contains("index rebuild") {
            stale_note = Some(id);
        }
    }
    info!("remembered {} facts", KNOWLEDGE.len());

    // RECALL. A question recalls the closest facts, ordered by similarity.
    phase("Recall");
    let question = "checkout is slow during the sale";
    let hits = recall(&memory, conversation, question, 3).await?;
    print_hits(&format!("recall for {question:?}:"), &hits);

    // IMPROVE. The read-replica failover resolved the incident, so the operator
    // upvotes it. Feedback reweights recall, and the upvoted note ranks first.
    phase("Improve");
    let replica_note = replica_note.expect("the read-replica note was remembered");
    memory
        .improve(&scope(conversation), Feedback::new(replica_note, 1.0))
        .await?;
    let hits = recall(&memory, conversation, question, 3).await?;
    print_hits(&format!("recall after feedback for {question:?}:"), &hits);
    if let Some((id, _, _)) = hits.first() {
        assert_eq!(
            *id, replica_note,
            "feedback should rank the upvoted note first"
        );
        info!("the upvoted note now ranks first");
    }

    // FORGET. The nightly-index note is superseded after the job is fixed, so the
    // assistant forgets it and it stops surfacing.
    phase("Forget");
    let stale_note = stale_note.expect("the index-rebuild note was remembered");
    memory.forget(&scope(conversation), stale_note).await?;
    let after = recall(&memory, conversation, "search results are stale", 3).await?;
    assert!(
        after.iter().all(|(id, _, _)| *id != stale_note),
        "a forgotten fact must not recall"
    );
    info!("forgot the superseded index-rebuild note, it no longer recalls");

    // PART 2 - KNOWLEDGE GRAPH. The durable side: model the same ops domain as a
    // graph and traverse how its entities connect. Managed, so it needs LaserData
    // Cloud. The connect is attempted here so the memory half above runs with no
    // server, and the graph half lights up only when a deployment is reachable.
    let graph_ready = match laser(&stream_for("memory"), Capabilities::OPEN.with_graph(true)).await
    {
        Ok(laser) => laser
            .capabilities()
            .await
            .graph
            .then_some(laser)
            .or_else(|| {
                fork_feature_ready(false, "the knowledge graph", "memory");
                None
            }),
        Err(_) => {
            fork_feature_ready(false, "the knowledge graph", "memory");
            None
        }
    };
    let Some(laser) = graph_ready else {
        return Ok(());
    };

    // BUILD. A realistic slice of a platform's operational knowledge: services own
    // teams, depend on components, fail over to mitigations, and incidents touch
    // both. `GraphNode::entity` content-addresses the id, so a component named by
    // many services is one node, which is what makes this a graph and not a pile
    // of pairs.
    phase("Build the knowledge graph");
    let services = [
        "checkout",
        "billing",
        "search",
        "cart",
        "auth",
        "recommendations",
        "inventory",
        "notifications",
    ];
    let components = [
        "orders-db",
        "db-pool",
        "read-replica",
        "search-index",
        "signing-key",
        "cache",
        "payment-gateway",
        "message-queue",
    ];
    let teams = ["payments", "search-platform", "core-platform"];
    let incidents = ["INC-101", "INC-102"];

    // Every entity as a node, kept by value so the relationships below can wire
    // the same node objects.
    let mut by_value: HashMap<&str, GraphNode> = HashMap::new();
    for value in services {
        by_value.insert(value, GraphNode::entity("Service", value));
    }
    for value in components {
        by_value.insert(value, GraphNode::entity("Component", value));
    }
    for value in teams {
        by_value.insert(value, GraphNode::entity("Team", value));
    }
    for value in incidents {
        by_value.insert(value, GraphNode::entity("Incident", value));
    }

    // (from, relationship, to) triples, resolved against the nodes above.
    let relationships: &[(&str, &str, &str)] = &[
        ("checkout", "depends_on", "orders-db"),
        ("checkout", "depends_on", "db-pool"),
        ("checkout", "depends_on", "payment-gateway"),
        ("checkout", "mitigated_by", "read-replica"),
        ("billing", "depends_on", "orders-db"),
        ("billing", "depends_on", "signing-key"),
        ("billing", "depends_on", "payment-gateway"),
        ("search", "depends_on", "search-index"),
        ("search", "depends_on", "cache"),
        ("search", "mitigated_by", "cache"),
        ("cart", "depends_on", "cache"),
        ("cart", "depends_on", "orders-db"),
        ("auth", "depends_on", "signing-key"),
        ("recommendations", "depends_on", "search-index"),
        ("recommendations", "depends_on", "cache"),
        ("inventory", "depends_on", "orders-db"),
        ("inventory", "depends_on", "message-queue"),
        ("notifications", "depends_on", "message-queue"),
        ("read-replica", "replicates", "orders-db"),
        ("payments", "owns", "checkout"),
        ("payments", "owns", "billing"),
        ("search-platform", "owns", "search"),
        ("search-platform", "owns", "recommendations"),
        ("core-platform", "owns", "auth"),
        ("core-platform", "owns", "cart"),
        ("core-platform", "owns", "inventory"),
        ("core-platform", "owns", "notifications"),
        ("INC-101", "affected", "checkout"),
        ("INC-101", "affected", "db-pool"),
        ("INC-102", "affected", "search"),
        ("INC-102", "affected", "search-index"),
    ];
    let edges: Vec<GraphEdge> = relationships
        .iter()
        .map(|(from, relationship, to)| {
            GraphEdge::relate(&by_value[from], *relationship, &by_value[to])
        })
        .collect();
    let nodes: Vec<GraphNode> = by_value.values().cloned().collect();
    let checkout = by_value["checkout"].clone();
    let incident = by_value["INC-101"].clone();
    info!("built {} nodes and {} edges", nodes.len(), edges.len());
    // Register the graph projection so the console explorer lists `ops`. A graph
    // built only by `upsert` (no projection) is reachable by name but not
    // discoverable. The entity schema is the projector path: bind it to a source
    // topic and the projector extracts the same nodes and edges this writes here.
    laser
        .projections()
        .register_graph(
            Projection::builder(format!("{GRAPH}.v1"))
                .name(GRAPH)
                .content_type(ContentType::Json)
                .graph(EntitySchema {
                    nodes: vec![
                        NodeExtract {
                            label: "Service".to_owned(),
                            value_pointer: "/service".to_owned(),
                            embedding_pointer: None,
                        },
                        NodeExtract {
                            label: "Component".to_owned(),
                            value_pointer: "/component".to_owned(),
                            embedding_pointer: None,
                        },
                    ],
                    edges: vec![EdgeExtract {
                        edge_type: "depends_on".to_owned(),
                        from_pointer: "/service".to_owned(),
                        to_pointer: "/component".to_owned(),
                    }],
                })
                .build(),
        )
        .await?;
    laser.graph(GRAPH).upsert(nodes, edges).await?;
    info!("registered and upserted the '{GRAPH}' graph, browsable in the console explorer");

    // NEIGHBORS. The cheap one-hop read: checkout and everything it points at, its
    // dependencies and its failover.
    phase("Read a node's neighbors");
    let around = laser
        .graph(GRAPH)
        .neighbors(checkout.id, EdgeDir::Out, None, 1)
        .await?;
    print_nodes("checkout's one-hop neighborhood", &around.nodes);

    // TRAVERSE. From every Service, follow `depends_on` to the components the
    // whole platform rests on, the structural view recall cannot give.
    phase("Traverse from a predicate");
    let dependencies = laser
        .graph(GRAPH)
        .start_match(Filter::pred("label", CmpOp::Eq, "Service"))
        .out("depends_on")
        .limit(100)
        .fetch()
        .await?;
    print_nodes_of(
        "components every Service depends on",
        "Component",
        &dependencies.nodes,
    );

    // BLAST RADIUS. From an incident, follow `affected` to everything it touched,
    // the question an on-call engineer actually asks.
    phase("Trace an incident's blast radius");
    let blast = laser
        .graph(GRAPH)
        .start_ids(vec![incident.id])
        .out("affected")
        .fetch()
        .await?;
    let mut touched: Vec<&str> = blast
        .nodes
        .iter()
        .filter(|node| node.id != incident.id)
        .map(value_of)
        .collect();
    touched.sort_unstable();
    info!("what INC-101 affected: {}", touched.join(", "));

    info!("done: memory recalls what is relevant, the graph shows how it connects");
    Ok(())
}
