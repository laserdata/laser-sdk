use laser_examples::init_tracing;
use laser_sdk::prelude::*;
use tracing::info;

// An agent that learns. The four agentic-memory verbs are one loop: remember
// what you know, recall what is relevant, improve the recall from feedback, and
// forget what is stale. This runs the whole loop in process over `VectorMemory`,
// so it needs no server: it is the smallest complete picture of how memory makes
// an agent better with use.
//
//   1. REMEMBER   the assistant stores what it knows, each item a fact.
//   2. RECALL     a question recalls the semantically closest facts, ranked.
//   3. IMPROVE    the operator upvotes the fact that actually helped, and the
//                 next recall ranks it higher: the agent learns from feedback.
//   4. FORGET     a superseded fact is forgotten and stops surfacing.
//
//   cargo run --release --example recall
//
// The same loop runs durably and at scale against a managed deployment by
// swapping `VectorMemory` for `Laser::memory()` (log- or KV-backed) or
// `QueryMemory` (vector recall over a materialized index). The knowledge graph
// that the durable memory builds is its own surface (AGDX A13), browsable in the
// management console's graph explorer.

// What the assistant knows. Each line becomes one remembered fact.
const KNOWLEDGE: &[&str] = &[
    "checkout latency spikes are usually database connection pool exhaustion",
    "billing double-charges trace back to retries without an idempotency key",
    "search returning stale results means the nightly index rebuild failed",
    "checkout pages recover fastest by failing over to the read replica",
    "auth token errors after a deploy come from the rotated signing key",
];

// A deterministic bag-of-words embedder, the model seam an app fills (here a real
// deployment would call an embedding model). Each token hashes into one of
// `DIMS` buckets and the vector is L2-normalized, so cosine similarity reflects
// shared vocabulary. Good enough to rank related facts, and reproducible.
const DIMS: usize = 64;

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

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    init_tracing();
    let memory = VectorMemory::new(BagOfWords);
    let conversation = ConversationId::new();

    // 1. REMEMBER. Store what the assistant knows, keeping the ids of two notes:
    //    the read-replica note the operator will upvote, and the stale search
    //    note it will later forget. (The durable facade, `Laser::memory()`, adds
    //    a fluent `.kind()` / `.durable()` / `.dedup()` builder over this call.)
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

    // 2. RECALL. A question recalls the closest facts. Two notes mention
    //    checkout, so both surface, ordered by similarity.
    let question = "checkout is slow during the sale";
    let hits = recall(&memory, conversation, question, 3).await?;
    print_hits(&format!("recall for {question:?}:"), &hits);

    // 3. IMPROVE. The read-replica failover resolved the incident, so the
    //    operator upvotes it. Feedback reweights recall.
    let replica_note = replica_note.expect("the read-replica note was remembered");
    memory
        .improve(&scope(conversation), Feedback::new(replica_note, 1.0))
        .await?;
    info!("upvoted the read-replica failover note");

    // The same question now ranks the upvoted note first: the agent learned.
    let hits = recall(&memory, conversation, question, 3).await?;
    print_hits(&format!("recall after feedback for {question:?}:"), &hits);
    if let Some((id, _, _)) = hits.first() {
        assert_eq!(
            *id, replica_note,
            "feedback should rank the upvoted note first"
        );
        info!("the upvoted note now ranks first");
    }

    // 4. FORGET. The nightly-index note is superseded after the job is fixed,
    //    so the assistant forgets it and it stops surfacing.
    let stale_note = stale_note.expect("the index-rebuild note was remembered");
    memory.forget(&scope(conversation), stale_note).await?;
    info!("forgot a superseded fact: the nightly index-rebuild note");
    let after = recall(&memory, conversation, "search results are stale", 3).await?;
    assert!(
        after.iter().all(|(id, _, _)| *id != stale_note),
        "a forgotten fact must not recall"
    );
    info!("the forgotten fact no longer recalls");

    info!("done: remember, recall, improve, forget, the loop that makes an agent learn");
    Ok(())
}
