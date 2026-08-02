"""recall (Memory primitive): durable, auditable agent memory.

Four verbs: remember, recall, improve, forget. Every change is a message on
your log, so memory is versioned and auditable by construction. The durable
log path recalls by recency; the vector backend adds similarity ranking.

What it shows:
  - remember one fact under a conversation scope
  - recall the newest fact under that scope
  - improve it with feedback, then forget it

Named `recall` (one of the four verbs), not `memory`, since that name is
already the full deep-dive scenario next door. The accessor is still
`laser.memory(namespace)`. `folded=True` reads the memory topic in process
instead of the managed key-value read view, so this runs against plain
Apache Iggy with no Cloud deployment.

Run it:
    just up
    python3 recall.py

Docs: https://docs.laserdata.cloud/laser-sdk/memory
Full scenario: memory.py (durable memory, then the knowledge graph over the same domain)
"""

from __future__ import annotations

import asyncio

import _common
import laser_sdk as ls

EXAMPLE = "recall"
NAMESPACE = "customer:42"
FACT = "Prefers aisle seats, travels monthly"


async def main() -> None:
    laser = await _common.connect(EXAMPLE)
    # Memory records ride the well-known agent topics, created once here.
    await laser.bootstrap(_common.PARTITIONS)
    conversation = ls.new_conversation_id()

    _common.phase("all four verbs: remember, recall, improve, forget")
    memory = laser.memory(NAMESPACE)

    fact_id = await memory.remember(FACT, conversation=conversation)

    # Durable log memory recalls the newest matching facts. Similarity ranking
    # is the vector/reranker path shown in the full memory example.
    hits = await memory.recall(
        limit=5,
        conversation=conversation,
        strategy="recent",
        folded=True,
    )
    print("  newest recalled fact(s):")
    for hit in hits:
        print(f"    {hit.text}")

    # Reinforce what was useful, then retire it. Both are records on the memory
    # topic, so the store stays an auditable history, not a mutable cell.
    await memory.improve(fact_id, 1.0, conversation=conversation)
    await memory.forget(fact_id, conversation=conversation)
    print(f"  reinforced then forgot {fact_id}")


if __name__ == "__main__":
    asyncio.run(main())
