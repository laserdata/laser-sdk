"""graph (Graph primitive): the relationships your messages imply.

Nodes and edges built from what flows through your log - who bought what,
which agent said what, what depends on what. Traverse it, search it by
meaning, and ask what was true at any point in time.

What it shows:
  - relate entities in one call (`link` upserts both content-addressed nodes
    and the typed edge between them)
  - rebuild the same node id locally, since a node is addressed by its content
  - read the neighborhood a traversal reaches from that node

Graph is a managed feature: it needs Laser Stack or LaserData Cloud and skips
on Apache Iggy without a managed backend.

Run it:
    LASER_CONNECTION_STRING=user:pwd@your-host python3 graph.py

Docs: https://docs.laserdata.cloud/laser-sdk/graph
Full scenario: memory.py (a full ops knowledge graph, bitemporal edges, point-in-time reads)
"""

from __future__ import annotations

import asyncio

import _common
import laser_sdk as ls

EXAMPLE = "graph"
GRAPH = "kg"
CUSTOMER = "customer:42"
RELATION = "purchased"
PRODUCTS = ("product:7", "product:9")


def entity_of(node: dict) -> str:
    """A node's `kind:value` form, the same spelling `link` accepted: the label
    it carries plus its content-addressed `value` attribute."""
    kind = node["labels"][0] if node["labels"] else "entity"
    return f"{kind}:{node['attrs'].get('value', '?')}"


async def main() -> None:
    laser = await _common.connect(EXAMPLE)
    if not _common.managed_gate((await laser.capabilities()).graph, "the knowledge graph", EXAMPLE):
        return

    _common.phase("relate entities, then traverse from one of them")
    graph = laser.graph(GRAPH)
    for product in PRODUCTS:
        await graph.link(CUSTOMER, RELATION, product)

    customer_id = ls.node_id("customer", "42")
    purchases = await graph.neighbors(customer_id, direction="out", edge_type=RELATION, depth=1)

    print(f"  {CUSTOMER} {RELATION}:")
    for node in purchases["nodes"]:
        print(f"    {entity_of(node)}")


if __name__ == "__main__":
    asyncio.run(main())
