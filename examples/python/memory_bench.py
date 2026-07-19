"""memory-bench: measure recall, not vibes.

A LongMemEval-shaped protocol over the memory facade, the Python mirror of the
Rust `memory-bench` example: pinned needle facts are buried under distractor
turns, then one question per needle is answered from each recall strategy. Per
strategy the run reports accuracy (did the needle land in the recalled
context, the memory system's own responsibility), recall tokens, and average
recall latency.

Knobs share the SDK `LASER_` namespace under the `LASER_MEMORY_BENCH_` prefix:
LASER_MEMORY_BENCH_DISTRACTORS (40), LASER_MEMORY_BENCH_RECALL_LIMIT (8).

Run it:
    docker run -p 8090:8090 apache/iggy:latest
    python memory_bench.py
"""

from __future__ import annotations

import asyncio
import time

import _common
import laser_sdk as ls

DIMS = 64

# One benchmark case per line: the fact to remember, the question whose recall
# must surface it, and the probe term the two share.
CASES = [
    (
        "the customer Dana prefers refunds as store credit",
        "how does Dana want refunds handled?",
        "refunds",
    ),
    (
        "the staging cluster lives in the eu-west region",
        "which region hosts the staging cluster?",
        "staging",
    ),
    (
        "the invoice INV-77 was disputed over a duplicate charge",
        "what happened with invoice INV-77?",
        "INV-77",
    ),
    (
        "the on-call rotation hands over every Tuesday at noon",
        "when does the on-call rotation hand over?",
        "rotation",
    ),
]

DISTRACTORS = max(1, _common.env_int("LASER_MEMORY_BENCH_DISTRACTORS", 40))
RECALL_LIMIT = max(1, _common.env_int("LASER_MEMORY_BENCH_RECALL_LIMIT", 8))


async def main() -> None:
    laser = await _common.connect("memory-bench")
    memory = laser.vector_memory(embed)
    conversation = ls.new_conversation_id()

    print(f"seeding {len(CASES)} needles under {DISTRACTORS} distractors each")
    for index, (fact, _question, _probe) in enumerate(CASES):
        await memory.remember(fact, conversation=conversation)
        for turn in range(DISTRACTORS):
            filler = f"routine turn {turn} of thread {index}: nothing notable"
            await memory.remember(filler, conversation=conversation)

    for strategy in ("recent", "semantic"):
        hits = 0
        recalled_tokens = 0
        latency_micros = 0
        for _fact, question, probe in CASES:
            started = time.monotonic()
            items = await memory.recall(
                semantic=question,
                strategy=strategy,
                limit=RECALL_LIMIT,
                conversation=conversation,
            )
            latency_micros += int((time.monotonic() - started) * 1_000_000)
            block = "\n".join(item.text for item in items)
            recalled_tokens += len(block) // 4
            if probe in block:
                hits += 1
        print(
            f"strategy={strategy} accuracy={hits}/{len(CASES)} "
            f"recall_tokens={recalled_tokens} "
            f"avg_latency_micros={latency_micros // len(CASES)}"
        )

    print(
        "done. These scores use a deterministic stand-in embedder on a local run. "
        "Point at a managed deployment with a real embedding model for real numbers"
    )


def _fnv1a(text: str) -> int:
    hash_value = 0x811C9DC5
    for byte in text.encode():
        hash_value = ((hash_value ^ byte) * 0x01000193) & 0xFFFFFFFF
    return hash_value


# The deterministic bag-of-words embedder, the model seam an app fills.
async def embed(text: str) -> list[float]:
    vector = [0.0] * DIMS
    for token in text.lower().split():
        token = "".join(char for char in token if char.isalnum())
        if token:
            vector[_fnv1a(token) % DIMS] += 1.0
    return vector


if __name__ == "__main__":
    asyncio.run(main())
