"""recall (agentic memory): an agent that learns from feedback.

The four agentic-memory verbs are one loop: remember what you know, recall what
is relevant, improve the recall from feedback, and forget what is stale. This
runs the whole loop over an in-process vector memory, the smallest complete
picture of how memory makes an agent better with use.

  1. REMEMBER   the assistant stores what it knows.
  2. RECALL     a question recalls the semantically closest facts, ranked.
  3. IMPROVE    the operator upvotes the fact that helped, and the next recall
                ranks it higher: the agent learns from feedback.
  4. FORGET     a superseded fact is forgotten and stops surfacing.

The connection is only to obtain the client handle; the vector memory lives in
process, so this needs no managed plane. Swap `vector_memory` for
`laser.memory()` (log- or KV-backed) or `laser.query_memory(...)` (vector recall
over a materialized index) to run the same loop durably and at scale.

Run it:
    docker run -p 8090:8090 apache/iggy:latest
    python recall.py
"""

from __future__ import annotations

import asyncio

import _common
import laser_sdk as ls

# What the assistant knows. Each line becomes one remembered fact.
KNOWLEDGE = [
    "checkout latency spikes are usually database connection pool exhaustion",
    "billing double-charges trace back to retries without an idempotency key",
    "search returning stale results means the nightly index rebuild failed",
    "checkout pages recover fastest by failing over to the read replica",
    "auth token errors after a deploy come from the rotated signing key",
]

DIMS = 64


def _fnv1a(text: str) -> int:
    hash_value = 0x811C9DC5
    for byte in text.encode():
        hash_value = ((hash_value ^ byte) * 0x01000193) & 0xFFFFFFFF
    return hash_value


# A deterministic bag-of-words embedder, the model seam an app fills (a real
# deployment calls an embedding model here). Async because the SDK awaits it.
async def embed(text: str) -> list[float]:
    vector = [0.0] * DIMS
    for token in text.lower().split():
        token = "".join(char for char in token if char.isalnum())
        if token:
            vector[_fnv1a(token) % DIMS] += 1.0
    norm = sum(value * value for value in vector) ** 0.5
    if norm > 0.0:
        vector = [value / norm for value in vector]
    return vector


def print_hits(label: str, hits) -> None:
    print(label)
    for rank, hit in enumerate(hits, start=1):
        print(f"  {rank}. ({hit.score or 0.0:.3f}) {hit.text}")


async def main() -> None:
    laser = await _common.connect("recall")
    memory = laser.vector_memory(embed)
    conversation = ls.new_conversation_id()

    # 1. REMEMBER. Store what the assistant knows, keeping the ids of the note
    #    the operator will upvote and the one it will later forget.
    replica_note = None
    stale_note = None
    for fact in KNOWLEDGE:
        memory_id = await memory.remember(fact, conversation=conversation)
        if "read replica" in fact:
            replica_note = memory_id
        if "index rebuild" in fact:
            stale_note = memory_id
    print(f"remembered {len(KNOWLEDGE)} facts")

    # 2. RECALL. A question recalls the closest facts, ordered by similarity.
    question = "checkout is slow during the sale"
    hits = await memory.recall(semantic=question, limit=3, conversation=conversation)
    print_hits(f"recall for {question!r}:", hits)

    # 3. IMPROVE. The read-replica failover resolved the incident, so the
    #    operator upvotes it. Feedback reweights recall.
    await memory.improve(replica_note, 1.0, conversation=conversation)
    print("upvoted the read-replica failover note")

    hits = await memory.recall(semantic=question, limit=3, conversation=conversation)
    print_hits(f"recall after feedback for {question!r}:", hits)
    assert hits[0].id == replica_note, "feedback should rank the upvoted note first"
    print("the upvoted note now ranks first")

    # 4. FORGET. The nightly-index note is superseded after the job is fixed, so
    #    the assistant forgets it and it stops surfacing.
    await memory.forget(stale_note, conversation=conversation)
    print("forgot a superseded fact: the nightly index-rebuild note")
    after = await memory.recall(
        semantic="search results are stale", limit=3, conversation=conversation
    )
    assert all(hit.id != stale_note for hit in after), "a forgotten fact must not recall"
    print("the forgotten fact no longer recalls")

    print("done: remember, recall, improve, forget, the loop that makes an agent learn")


if __name__ == "__main__":
    asyncio.run(main())
