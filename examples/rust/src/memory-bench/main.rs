// A LongMemEval-shaped recall benchmark over the agentic-memory facade: pinned
// needle facts are buried under distractor turns, then one question per needle
// is answered from each recall strategy. Per strategy the run scores accuracy
// (did the needle land in the recalled context, the memory system's own job,
// independent of answer quality), the recall tokens spent getting there, and
// the average recall latency.
//
// The default run stays deterministic and key-free on an in-process vector
// backend and the bag-of-words HashEmbedder, no model and no managed
// deployment. Against LaserData Cloud, swap `MemoryBackend::Vector` for `Auto`
// and the same protocol exercises the managed semantic, keyword, and hybrid
// strategies. The answer step rides the `LlmClient` seam (MockLlm by default),
// so a local run's scores reflect the stand-in embedder, not a real model. Wire
// a real embedder and model against a deployment to measure your own recall.
//
// Scale the pressure with the shared `LASER_MEMORY_BENCH_` knobs:
//
//   # default: 40 distractors per needle, recall up to 8 items
//   cargo run --release --example memory-bench
//
//   # heavier: more distractors, a tighter recall window
//   LASER_MEMORY_BENCH_DISTRACTORS=200 LASER_MEMORY_BENCH_RECALL_LIMIT=4 \
//     cargo run --release --example memory-bench
use laser_examples::{MockLlm, env_usize, init_tracing, laser, stream_for};
use laser_sdk::prelude::full::*;
use std::time::Instant;
use tracing::info;

const EMBEDDING_DIMS: usize = 64;

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    init_tracing();
    let config = Config::from_env();
    let laser = laser(&stream_for("memory-bench"), Capabilities::OPEN).await?;

    // The in-process vector backend keeps the run deterministic and key-free.
    // Against LaserData Cloud, swap `MemoryBackend::Vector` for `Auto` and the
    // same protocol exercises the managed query and keyword strategies.
    let memory = laser
        .memory_with("bench", MemoryBackend::Vector)
        .embedder(HashEmbedder);
    let conversation = ConversationId::new();

    info!(
        cases = CASES.len(),
        distractors = config.distractors,
        recall_limit = config.recall_limit,
        "seeding needles under distractor turns"
    );
    for (index, case) in CASES.iter().enumerate() {
        memory
            .remember(case.fact.as_bytes().to_vec())
            .scope(conversation)
            .send()
            .await?;
        for turn in 0..config.distractors {
            let filler = format!("routine turn {turn} of thread {index}: nothing notable");
            memory
                .remember(filler.into_bytes())
                .scope(conversation)
                .send()
                .await?;
        }
    }

    // The answer step rides the LLM seam: MockLlm by default, a real model by
    // building with `llm-anthropic` / `llm-openai` and a key. Accuracy below is
    // needle-in-context (did recall surface the fact), which is the memory
    // system's own responsibility. Answer quality belongs to the model.
    let llm = MockLlm;
    let strategies = [RecallStrategy::Recent, RecallStrategy::Semantic];
    for strategy in strategies {
        let mut report = StrategyReport::default();
        for case in CASES {
            let started = Instant::now();
            let items = memory
                .recall(conversation)
                .semantic(case.question)
                .strategy(strategy)
                .limit(config.recall_limit)
                .fetch()
                .await?;
            report.latency_micros += started.elapsed().as_micros();
            let block = to_context_block(&items, Some(config.token_budget));
            report.recalled_tokens += block.len() / 4;
            if block.contains(case.probe) {
                report.hits += 1;
            }
            let _answer = laser_examples::LlmClient::complete(
                &llm,
                &format!("context:\n{block}\n\nquestion: {}", case.question),
            )
            .await;
        }
        info!(
            strategy = ?strategy,
            accuracy = format!("{}/{}", report.hits, CASES.len()),
            recall_tokens = report.recalled_tokens,
            avg_latency_micros = report.latency_micros / CASES.len() as u128,
            "strategy scored"
        );
    }

    info!(
        "done. These scores are from the deterministic MockLlm on a local run. \
         Build with --features llm-anthropic or llm-openai and point at a managed \
         deployment for real accuracy numbers"
    );
    Ok(())
}

// The run's knobs, all environment-driven with the pinned defaults.
struct Config {
    // Distractor turns buried around each needle fact.
    distractors: usize,
    // Items each recall may return: the context window pressure.
    recall_limit: usize,
    // Advisory token budget handed to the context block.
    token_budget: usize,
}

impl Config {
    fn from_env() -> Self {
        Self {
            distractors: env_usize("LASER_MEMORY_BENCH_DISTRACTORS", 40).max(1),
            recall_limit: env_usize("LASER_MEMORY_BENCH_RECALL_LIMIT", 8).max(1),
            token_budget: env_usize("LASER_MEMORY_BENCH_TOKEN_BUDGET", 256).max(16),
        }
    }
}

// The examples' deterministic token-hash embedder: same text, same vector,
// no model, no key. Real deployments register their model embedder through
// the same seam.
struct HashEmbedder;

impl Embedder for HashEmbedder {
    async fn embed(&self, text: &str) -> Result<Vec<f32>, LaserError> {
        let mut vector = vec![0.0f32; EMBEDDING_DIMS];
        for token in text
            .split(|c: char| !c.is_alphanumeric())
            .filter(|token| !token.is_empty())
        {
            let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
            for byte in token.to_ascii_lowercase().bytes() {
                hash ^= u64::from(byte);
                hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
            }
            vector[hash as usize % EMBEDDING_DIMS] += 1.0;
        }
        Ok(vector)
    }
}

// One benchmark case: the fact to remember, and the question whose recall
// must surface it. The `probe` is the term the question shares with the fact.
struct Case {
    fact: &'static str,
    question: &'static str,
    probe: &'static str,
}

const CASES: &[Case] = &[
    Case {
        fact: "the customer Dana prefers refunds as store credit",
        question: "how does Dana want refunds handled?",
        probe: "refunds",
    },
    Case {
        fact: "the staging cluster lives in the eu-west region",
        question: "which region hosts the staging cluster?",
        probe: "staging",
    },
    Case {
        fact: "the invoice INV-77 was disputed over a duplicate charge",
        question: "what happened with invoice INV-77?",
        probe: "INV-77",
    },
    Case {
        fact: "the on-call rotation hands over every Tuesday at noon",
        question: "when does the on-call rotation hand over?",
        probe: "rotation",
    },
];

// What one strategy scored across the whole case set.
#[derive(Default)]
struct StrategyReport {
    hits: usize,
    recalled_tokens: usize,
    latency_micros: u128,
}
