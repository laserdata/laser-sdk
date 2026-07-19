# memory-bench

> Measure recall, not vibes: per-strategy accuracy, tokens, and latency over the memory facade.

## What it does

A LongMemEval-shaped protocol over the agentic-memory surface: pinned needle facts are buried under distractor turns, then one question per needle is answered from each recall strategy.

1. **Seed** each needle fact into memory, surrounded by `LASER_MEMORY_BENCH_DISTRACTORS` routine turns.
2. **Recall** each question through every strategy under test (`recent`, `semantic` on the default in-process run), capped at `LASER_MEMORY_BENCH_RECALL_LIMIT` items.
3. **Answer** through the examples' `LlmClient` seam (the deterministic `MockLlm` by default, a real model with `llm-anthropic` / `llm-openai` and a key).
4. **Score** per strategy: accuracy (did the needle land in the recalled context, the memory system's own responsibility, independent of answer quality), recall tokens (what the strategy spent to get there), and average recall latency.

## Run it

Every knob shares the SDK `LASER_` namespace under the `LASER_MEMORY_BENCH_` prefix.

| variable | default | meaning |
| --- | --- | --- |
| `LASER_MEMORY_BENCH_DISTRACTORS` | `40` | distractor turns buried around each needle |
| `LASER_MEMORY_BENCH_RECALL_LIMIT` | `8` | items each recall may return |
| `LASER_MEMORY_BENCH_TOKEN_BUDGET` | `256` | advisory token budget for the context block |

```sh
docker run -p 8090:8090 apache/iggy:latest
cargo run --release --example memory-bench

# heavier pressure: more distractors, tighter window
LASER_MEMORY_BENCH_DISTRACTORS=200 LASER_MEMORY_BENCH_RECALL_LIMIT=4 \
  cargo run --release --example memory-bench
```

The default run rides the in-process vector backend, deterministic and key-free. Against LaserData Cloud, switch `MemoryBackend::Vector` to `Auto` in `main.rs` and the same protocol exercises the managed semantic, keyword, and hybrid strategies.

## Where to look (LaserData Cloud)

Nothing lands in the console on the default in-process run. Against a managed deployment with `MemoryBackend::Auto`, the seeded items appear in the Memory view and the recall queries in the query surface.

## Highlights

- The needle-under-distractors protocol as plain code: change `CASES` and the knobs, keep the scoring.
- Accuracy is needle-in-context, so the memory system is measured on its own responsibility and the model seam stays swappable.
- `RecallStrategy` routing through one builder (`.strategy(..)`, or the `semantic` / `keyword` / `hybrid` sugar), with `MemoryItem.signals` carrying each item's per-signal attribution.
- A local run scores the deterministic stand-in embedder, so wire a real embedder and model against a deployment to measure your own recall.
