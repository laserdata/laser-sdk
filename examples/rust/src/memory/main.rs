use laser_examples::{PARTITIONS, cloud_feature_ready, init_tracing, laser, phase, stream_for};
use laser_sdk::prelude::full::*;
use std::collections::HashMap;
use std::time::Duration;
use tracing::info;

// Agentic memory, three facets. An agent gets better with use, and this
// runs them over one incident-knowledge domain:
//
//   MEMORY (in-process, no server)   remember what you know, recall what is
//                                    relevant, improve recall from feedback, and
//                                    forget what is stale. The four verbs as one
//                                    loop over `VectorMemory`.
//
//   DURABLE MEMORY (managed)         the same verbs over the memory topic on the
//                                    log, so facts persist and replay. One model:
//                                    every remember publishes to the topic, and
//                                    the deployment materializes it for recall.
//
//   KNOWLEDGE GRAPH (managed)        entities become content-addressed nodes,
//                                    relationships typed edges, and a traversal
//                                    answers how things connect, what recall
//                                    alone cannot.
//
//   cargo run --example memory
//
// The first half needs no server. The durable-memory and graph halves are
// managed: point the example at a LaserData Cloud deployment to run them,
// otherwise it prints how and exits clean. The durable memories show in the
// console's Memory view and the graph in its explorer.

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

// Bag-of-words embedding width for the model seam below.
const DIMS: usize = 64;

// The knowledge graph the second half builds and traverses.
const GRAPH: &str = "ops";

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    init_tracing();
    let conversation = ConversationId::new();

    // PART 1: MEMORY. The four agentic-memory verbs as one loop, in process over
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

    // PART 2: DURABLE MEMORY AND THE KNOWLEDGE GRAPH. The durable side: persist
    // the same facts and model how the domain's entities connect. Managed, so it
    // needs LaserData Cloud. The connect is attempted here so the memory half
    // above runs with no server, and the hello probe decides whether the managed
    // half lights up: on raw Apache Iggy it prints one pointer and exits green.
    let managed_ready = match laser(&stream_for("memory"), Capabilities::OPEN).await {
        Ok(laser) => {
            let capabilities = laser.capabilities().await;
            (capabilities.graph && capabilities.kv.available)
                .then_some(laser)
                .or_else(|| {
                    cloud_feature_ready(
                        false,
                        "the durable memory and knowledge graph surface",
                        "memory",
                    );
                    None
                })
        }
        Err(_) => {
            cloud_feature_ready(
                false,
                "the durable memory and knowledge graph surface",
                "memory",
            );
            None
        }
    };
    let Some(laser) = managed_ready else {
        return Ok(());
    };

    // DURABLE MEMORY (managed). The `VectorMemory` above lives in this process and
    // is gone when it exits. Durable memory is the single model: every remember
    // publishes to the memory topic, so the facts persist and replay, browsable in
    // the console's Memory view (namespace `incidents`). Same four verbs.
    // `memory_topic` configures that topic up front: a partition count, and a
    // message-expiry window that bounds how long the history lives on the log.
    phase("Remember durable facts");
    let durable = laser
        .memory_topic("incidents")
        .partitions(PARTITIONS)
        .ttl(Duration::from_secs(86_400))
        .build()
        .await?;
    for fact in KNOWLEDGE {
        durable
            .remember(fact.as_bytes().to_vec())
            .scope(conversation)
            .send()
            .await?;
    }
    let durable_hits = durable.recall(conversation).limit(3).fetch().await?;
    info!(
        "stored {} durable facts, recalled {} most-recent",
        KNOWLEDGE.len(),
        durable_hits.len()
    );
    // Each recalled item points back to its origin log record, so a reader (or
    // the console) can fold from the read view to the source message.
    for hit in &durable_hits {
        if let Some(SourceRef::Message {
            stream,
            topic,
            partition,
            offset,
            ..
        }) = &hit.source
        {
            info!("  recalled from source {stream}/{topic} partition {partition} offset {offset}");
        }
    }

    // ONE SCOPE. The incident is one conversation. `laser.context(..)` binds it
    // once, so the same memory recalls without repeating the id, and the
    // conversation's own messages read back through the same handle. The session
    // (messages plus working memory) is one scope, while the knowledge graph
    // below stays cross-conversation on purpose.
    phase("Scope the session: messages and memory under one conversation");
    let session = laser.context(conversation);
    session
        .append(
            AgentTopic::Audit,
            b"incident opened: checkout slow".to_vec(),
        )
        .await?;
    let scoped_hits = session
        .memory("incidents")
        .recall()
        .limit(3)
        .fetch()
        .await?;
    let trail = session.fetch(vec![AgentTopic::Audit], 8).await?;
    info!(
        "one scope recalled {} durable facts and read back {} of the conversation's messages",
        scoped_hits.len(),
        trail.len()
    );

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
    // Provenance: register each entity as a real record in the `topology`
    // key-value namespace, then point its graph node at that record. The node's
    // source is then a live deep link, the console renders it as a click-through
    // to the actual KV entry. First-writer on the node.
    let registry = laser.kv("topology");
    for (value, node) in by_value.iter_mut() {
        let kind = node.labels.first().map(String::as_str).unwrap_or("Entity");
        registry
            .set(*value)
            .bytes(format!("{{\"kind\":\"{kind}\",\"value\":\"{value}\"}}"))
            .send()
            .await?;
        node.source = Some(SourceRef::Kv {
            namespace: "topology".to_owned(),
            key: (*value).to_owned(),
        });
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
    // A `mitigated_by` edge is a bitemporal fact: the mitigation became true when
    // it was applied. Stamp a valid-from so the edge records when, not just that,
    // it holds. The orthogonal system-time axis (when we observed it) is the log
    // offset of the upsert, which the substrate records for free, so a later
    // traversal can ask what was true at a given time. Other edges are open-ended.
    const MITIGATION_SINCE_US: u64 = 1_900_000_000_000_000;
    let edges: Vec<GraphEdge> = relationships
        .iter()
        .map(|(from, relationship, to)| {
            // The relationship is anchored on the `from` entity's record, so the
            // edge carries that source (last-writer on the edge).
            let edge = GraphEdge::relate(&by_value[from], *relationship, &by_value[to])
                .with_source(SourceRef::Kv {
                    namespace: "topology".to_owned(),
                    key: (*from).to_owned(),
                });
            if *relationship == "mitigated_by" {
                edge.valid(Some(MITIGATION_SINCE_US), None)
            } else {
                edge
            }
        })
        .collect();
    let mitigations = edges.iter().filter(|e| e.valid_from.is_some()).count();
    let nodes: Vec<GraphNode> = by_value.values().cloned().collect();
    let checkout = by_value["checkout"].clone();
    let incident = by_value["INC-101"].clone();
    info!(
        "built {} nodes and {} edges ({mitigations} bitemporal, carrying a valid-from)",
        nodes.len(),
        edges.len()
    );
    // Register the graph projection so the console explorer lists `ops`. The
    // entity schema is the extraction plan: bind it to a source topic and the
    // projector applies it per record. Here the demo writes the graph directly
    // with the `upsert` below, which is the same content-addressed write path.
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
                        valid_from_pointer: None,
                        valid_to_pointer: None,
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

    // PROVENANCE. Every node and edge records the source it was extracted from, so
    // a traversal is navigable back to its origin. Here each entity points at its
    // record in the `topology` key-value namespace. The console renders this
    // `source` as a live click-through to that KV entry (or to the message, for a
    // projector-built graph). Node source is first-writer, edge source last-writer.
    phase("Trace a node back to its source");
    if let Some(node) = around.nodes.iter().find(|node| node.id == checkout.id) {
        match &node.source {
            Some(SourceRef::Kv { namespace, key }) => {
                info!("checkout's source record is {namespace}/{key}")
            }
            Some(other) => info!("checkout came from {other:?}"),
            None => info!("checkout carries no source"),
        }
    }

    // BITEMPORAL. The `mitigated_by` edges carry a valid-from, so an "as of" read
    // sees the graph as it was then. Before the mitigation was applied, checkout
    // has no failover. After, the read-replica mitigation appears. Same query, two
    // points in valid-time.
    phase("Read the graph as of a point in time");
    let before = laser
        .graph(GRAPH)
        .start_ids(vec![checkout.id])
        .out("mitigated_by")
        .as_of(MITIGATION_SINCE_US - 1)
        .fetch()
        .await?;
    let after = laser
        .graph(GRAPH)
        .start_ids(vec![checkout.id])
        .out("mitigated_by")
        .as_of(MITIGATION_SINCE_US + 1)
        .fetch()
        .await?;
    let reached = |result: &laser_sdk::wire::graph::GraphResult| {
        result
            .nodes
            .iter()
            .filter(|node| node.id != checkout.id)
            .count()
    };
    info!(
        "checkout mitigations before the rollout: {}, after: {}",
        reached(&before),
        reached(&after)
    );

    // PATHS. The same traversal, asking for whole paths instead of a node set, so
    // a caller sees how an incident reaches a component, not just that it does.
    phase("Return whole paths");
    let paths = laser
        .graph(GRAPH)
        .start_ids(vec![incident.id])
        .out("affected")
        .return_paths()
        .fetch()
        .await?;
    info!(
        "INC-101 reaches {} components by a traced path",
        paths.paths.len()
    );

    info!("done: memory recalls what is relevant, the graph shows how it connects");
    Ok(())
}

// The deterministic bag-of-words embedder, the model seam an app fills (here a
// real deployment would call an embedding model). Each token hashes into one of
// `DIMS` buckets and the vector is L2-normalized, so cosine similarity reflects
// shared vocabulary. Good enough to rank related facts, and reproducible.
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
