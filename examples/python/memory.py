"""memory (agentic memory): an agent that recalls and reasons over connections.

Agentic memory has three facets, run over one incident-knowledge domain:

  MEMORY (in-process)        remember what you know, recall what is relevant,
                             improve recall from feedback, and forget what is
                             stale. The four verbs as one loop over a vector
                             memory.

  DURABLE MEMORY (managed)   the same verbs over the memory topic on the log, so
                             facts persist and replay. One model: every remember
                             publishes, the deployment materializes it for recall.

  KNOWLEDGE GRAPH (managed)  entities become content-addressed nodes,
                             relationships typed edges, and a traversal answers
                             how things connect, what recall alone cannot.

The first facet runs against any connected server. The durable-memory and graph
facets are managed: against raw Apache Iggy they are skipped with a note. The
durable memories show in the console's Memory view and the named graph in its
explorer.

Run it:
    docker run -p 8090:8090 apache/iggy:latest
    python memory.py
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
    "cart abandonment climbs when the cache eviction rate is set too aggressive",
    "inventory drift is the message queue dropping stock-adjustment events",
    "recommendation gaps appear when the search index lags behind the catalog",
]

# Bag-of-words embedding width for the model seam below.
DIMS = 64

# The knowledge graph the second half builds and traverses.
GRAPH = "ops"


async def main() -> None:
    laser = await _common.connect("memory")
    conversation = ls.new_conversation_id()

    # PART 1: MEMORY, against any connected server.
    await run_memory(laser, conversation)

    caps = await laser.capabilities()

    # PART 2: DURABLE MEMORY, materialized from the memory topic on the log.
    if _common.managed_gate(caps.kv, "durable memory", "memory"):
        await run_durable(laser, conversation)

    # PART 3: KNOWLEDGE GRAPH, a managed read model.
    if _common.managed_gate(caps.graph, "the knowledge graph", "memory"):
        await run_graph(laser)

    print("done: memory recalls what is relevant, the graph shows how it connects")


async def run_memory(laser, conversation) -> None:
    """Part 1: the four memory verbs as one loop over an in-process vector memory."""
    memory = laser.vector_memory(embed)

    # REMEMBER. Store what the assistant knows, keeping the ids of the note the
    # operator upvotes and the one it later forgets.
    replica_note = None
    stale_note = None
    for fact in KNOWLEDGE:
        memory_id = await memory.remember(fact, conversation=conversation)
        if "read replica" in fact:
            replica_note = memory_id
        if "index rebuild" in fact:
            stale_note = memory_id
    print(f"remembered {len(KNOWLEDGE)} facts")

    # RECALL. A question recalls the closest facts, ordered by similarity.
    question = "checkout is slow during the sale"
    hits = await memory.recall(semantic=question, limit=3, conversation=conversation)
    print_hits(f"recall for {question!r}:", hits)

    # IMPROVE. The read-replica failover resolved the incident, so the operator
    # upvotes it and the next recall ranks it first.
    await memory.improve(replica_note, 1.0, conversation=conversation)
    hits = await memory.recall(semantic=question, limit=3, conversation=conversation)
    print_hits(f"recall after feedback for {question!r}:", hits)
    assert hits[0].id == replica_note, "feedback should rank the upvoted note first"
    print("the upvoted note now ranks first")

    # FORGET. The nightly-index note is superseded after the job is fixed.
    await memory.forget(stale_note, conversation=conversation)
    after = await memory.recall(
        semantic="search results are stale", limit=3, conversation=conversation
    )
    assert all(hit.id != stale_note for hit in after), "a forgotten fact must not recall"
    print("forgot the superseded index-rebuild note, it no longer recalls")


async def run_durable(laser, conversation) -> None:
    """Durable memory, the single model. Every remember publishes to the memory
    topic, so facts persist and replay. `memory_topic` configures that topic up
    front: a partition count and a stream message-expiry window."""
    durable = await laser.memory_topic("incidents", partitions=4, ttl_secs=86_400)
    for fact in KNOWLEDGE:
        await durable.remember(fact, conversation=conversation)
    hits = await durable.recall(limit=3, conversation=conversation)
    print(f"stored {len(KNOWLEDGE)} durable facts, recalled {len(hits)}")
    # Each recalled item points back to its origin log record, so a reader (or
    # the console) can fold from the read view to the source message.
    for hit in hits:
        if hit.source:
            stream, topic, partition, offset, _conversation = hit.source
            print(f"  recalled from source {stream}/{topic} partition {partition} offset {offset}")

    # ONE SCOPE. The incident is one conversation. `laser.context(..)` binds it
    # once, so the same memory recalls without repeating the id and the
    # conversation's own messages read back through the same handle. The session
    # (messages plus working memory) is one scope. The knowledge graph stays
    # cross-conversation on purpose.
    session = laser.context(conversation)
    await session.append("audit", b"incident opened: checkout slow")
    scoped_hits = await session.memory(laser.memory_on_topic("incidents")).recall(limit=3)
    trail = await session.fetch(topics=["audit"], last_n=8)
    print(
        f"one scope recalled {len(scoped_hits)} durable facts and read back "
        f"{len(trail)} of the conversation's messages"
    )


async def run_graph(laser) -> None:
    """Part 2: model the same ops domain as a graph and traverse how it connects."""
    # BUILD. A realistic slice of a platform's operational knowledge: services own
    # teams, depend on components, fail over to mitigations, and incidents touch
    # both. `graph_node` content-addresses the id, so a component named by many
    # services is one node, which is what makes this a graph and not a pile of pairs.
    entities = [
        ("Service", "checkout"),
        ("Service", "billing"),
        ("Service", "search"),
        ("Service", "cart"),
        ("Service", "auth"),
        ("Service", "recommendations"),
        ("Service", "inventory"),
        ("Service", "notifications"),
        ("Component", "orders-db"),
        ("Component", "db-pool"),
        ("Component", "read-replica"),
        ("Component", "search-index"),
        ("Component", "signing-key"),
        ("Component", "cache"),
        ("Component", "payment-gateway"),
        ("Component", "message-queue"),
        ("Team", "payments"),
        ("Team", "search-platform"),
        ("Team", "core-platform"),
        ("Incident", "INC-101"),
        ("Incident", "INC-102"),
    ]
    by_value = {value: ls.graph_node(label, value) for label, value in entities}
    # Provenance: register each entity as a real record in the `topology` key-value
    # namespace, then point its graph node at that record, so the node's source is a
    # live deep link the console renders as a click-through to the actual KV entry.
    # First-writer on the node.
    registry = laser.kv("topology")
    for value, node in by_value.items():
        kind = node["labels"][0] if node.get("labels") else "Entity"
        await registry.set(value).json({"kind": kind, "value": value}).send()
        node["source"] = {"kind": "kv", "namespace": "topology", "key": value}
    relationships = [
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
    ]
    nodes = list(by_value.values())
    # A `mitigated_by` edge is a bitemporal fact: stamp a `valid_from` so the edge
    # records when the mitigation became true, not just that it holds. The
    # orthogonal system-time axis (when we observed it) is the log offset of the
    # upsert, recorded by the substrate for free. Other edges stay open-ended.
    MITIGATION_SINCE_US = 1_900_000_000_000_000
    edges = []
    for src, rel, dst in relationships:
        edge = ls.graph_edge(by_value[src], rel, by_value[dst])
        # The relationship was asserted by the `src` entity's memory, so the edge
        # carries that source (last-writer on the edge).
        edge["source"] = {"kind": "kv", "namespace": "topology", "key": src}
        if rel == "mitigated_by":
            edge["valid_from"] = MITIGATION_SINCE_US
        edges.append(edge)

    # Register the graph projection so the console explorer lists `ops`. The
    # entity schema is the extraction plan: bind it to a source topic and the
    # projector applies it per record. This demo writes the graph directly with
    # the `upsert` below, the same content-addressed write path.
    await laser.register_graph(
        {
            "id": f"{GRAPH}.v1",
            "name": GRAPH,
            "version": 1,
            "content_type": "json",
            "extraction": {"fields": [], "inline_payload": False},
            "entity_schema": {
                "nodes": [
                    {"label": "Service", "value_pointer": "/service"},
                    {"label": "Component", "value_pointer": "/component"},
                ],
                "edges": [
                    {
                        "edge_type": "depends_on",
                        "from_pointer": "/service",
                        "to_pointer": "/component",
                    },
                ],
            },
        }
    )
    graph = laser.graph(GRAPH)
    await graph.upsert(nodes, edges)
    print(f"registered and upserted {len(nodes)} nodes, {len(edges)} edges in the {GRAPH!r} graph")

    # NEIGHBORS. The cheap one-hop read: checkout and everything it points at.
    around = await graph.neighbors(by_value["checkout"]["id"], direction="out", depth=1)
    print_nodes("checkout's one-hop neighborhood", around["nodes"])

    # TRAVERSE. From every Service, follow `depends_on` to the components the whole
    # platform rests on, the structural view recall cannot give.
    dependencies = await graph.query(match_label="Service", hops=[("depends_on", "out")], limit=100)
    print_nodes_of("components every Service depends on", "Component", dependencies["nodes"])

    # BLAST RADIUS. From an incident, follow `affected` to everything it touched,
    # the question an on-call engineer actually asks.
    incident_id = by_value["INC-101"]["id"]
    blast = await graph.query(start_ids=[incident_id], hops=[("affected", "out")])
    touched = sorted(
        node.get("attrs", {}).get("value", "?")
        for node in blast["nodes"]
        if node["id"] != incident_id
    )
    print(f"what INC-101 affected: {', '.join(touched)}")

    # PROVENANCE. Every node and edge records the source it was extracted from, so
    # a traversal is navigable back to its origin. Here each entity points at its
    # record in the `topology` key-value namespace. The console renders this
    # `source` as a live click-through to that KV entry (or to the message, for a
    # projector-built graph). Node source is first-writer, edge source last-writer.
    checkout_id = by_value["checkout"]["id"]
    checkout_node = next((n for n in around["nodes"] if n["id"] == checkout_id), None)
    source = (checkout_node or {}).get("source")
    if source and source.get("kind") == "kv":
        print(f"checkout's source record is {source['namespace']}/{source['key']}")

    # BITEMPORAL. The `mitigated_by` edges carry a valid-from, so an `as_of` read
    # sees the graph as it was then: no failover before the rollout, the
    # read-replica mitigation after. Same query, two points in valid-time.
    before = await graph.query(
        start_ids=[checkout_id], hops=[("mitigated_by", "out")], as_of=MITIGATION_SINCE_US - 1
    )
    after = await graph.query(
        start_ids=[checkout_id], hops=[("mitigated_by", "out")], as_of=MITIGATION_SINCE_US + 1
    )
    n_before = sum(1 for node in before["nodes"] if node["id"] != checkout_id)
    n_after = sum(1 for node in after["nodes"] if node["id"] != checkout_id)
    print(f"checkout mitigations before the rollout: {n_before}, after: {n_after}")

    # PATHS. The same traversal, asking for whole paths instead of a node set.
    paths = await graph.query(start_ids=[incident_id], hops=[("affected", "out")], returns="paths")
    print(f"INC-101 reaches {len(paths.get('paths', []))} components by a traced path")


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


def print_nodes(label: str, nodes) -> None:
    values = sorted(node.get("attrs", {}).get("value", "?") for node in nodes)
    print(f"{label}: {', '.join(values)}")


def print_nodes_of(label: str, kind: str, nodes) -> None:
    # A traversal result is seeded with its start frontier, so the start nodes ride
    # along. Narrow to one entity kind to print just what was reached.
    values = sorted(
        node.get("attrs", {}).get("value", "?") for node in nodes if kind in node.get("labels", [])
    )
    print(f"{label}: {', '.join(values)}")


if __name__ == "__main__":
    asyncio.run(main())
