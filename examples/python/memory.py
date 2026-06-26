"""memory (agentic memory): an agent that recalls and reasons over connections.

Agentic memory has two halves, and this runs both over one incident-knowledge
domain:

  MEMORY (in-process)        remember what you know, recall what is relevant,
                             improve recall from feedback, and forget what is
                             stale. The four verbs as one loop over a vector
                             memory.

  KNOWLEDGE GRAPH (managed)  the durable side: entities become content-addressed
                             nodes, relationships typed edges, and a traversal
                             answers how things connect, what recall alone cannot.

The memory half runs against any connected server. The graph half is a managed
read model (AGDX A13): against raw Apache Iggy it is skipped with a note. The
named graph is browsable in the management console's graph explorer.

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

DIMS = 64

GRAPH = "ops"


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
    edges = [ls.graph_edge(by_value[src], rel, by_value[dst]) for src, rel, dst in relationships]

    # Register the graph projection so the console explorer lists `ops`. A graph
    # built only by `upsert` (no projection) is reachable by name but not
    # discoverable. The entity schema is the projector path: bind it to a source
    # topic and the projector extracts the same nodes and edges this writes here.
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


async def main() -> None:
    laser = await _common.connect("memory")
    conversation = ls.new_conversation_id()

    # PART 1 - MEMORY, against any connected server.
    await run_memory(laser, conversation)

    # PART 2 - KNOWLEDGE GRAPH, a managed read model.
    caps = await laser.capabilities()
    if _common.managed_gate(caps.graph, "the knowledge graph", "memory"):
        await run_graph(laser)

    print("done: memory recalls what is relevant, the graph shows how it connects")


if __name__ == "__main__":
    asyncio.run(main())
