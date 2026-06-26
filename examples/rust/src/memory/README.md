# memory

> Agentic memory, both halves: recall what is relevant, and traverse how it connects.

## What it does

An agent gets better with use two ways, and this runs both over one
incident-knowledge domain.

**Memory** (in-process, needs no server): the four agentic-memory verbs as one
loop over `VectorMemory`.

1. **Remember** the assistant stores what it knows, each item a fact.
2. **Recall** a question recalls the semantically closest facts, ranked.
3. **Improve** the operator upvotes the fact that resolved the incident, and the
   next recall ranks it first. The agent learns.
4. **Forget** a superseded fact is forgotten and stops surfacing.

**Knowledge graph** (managed): the durable side. The same ops domain becomes a
graph of content-addressed nodes and typed edges, then a traversal answers how
the entities connect, which recall alone cannot.

5. **Build** upsert a realistic slice (services, components, teams, incidents)
   wired by `depends_on`, `mitigated_by`, `replicates`, `owns`, and `affected`.
6. **Neighbors** what sits one hop from `checkout`.
7. **Traverse** from every `Service`, follow `depends_on` to the components the
   whole platform rests on.
8. **Blast radius** from an incident, follow `affected` to everything it touched,
   the question an on-call engineer actually asks.

## Run it

```sh
cargo run --release --example memory
```

The memory half runs with no server. The graph half is a managed read model
(AGDX A13): point the example at a LaserData Cloud deployment to run it,
otherwise it prints how and exits clean.

```sh
LASER_CONNECTION_STRING=iggy://user:pwd@your-laserdata-cloud-host \
  cargo run --release --example memory
```

## Where to look (LaserData Cloud)

The example builds the `ops` graph, so it appears in the management console's
graph explorer. Open it there to browse the same nodes and edges, or walk them
from a start node.

## Highlights

- The agentic-memory verbs `remember` / `recall` / `improve` / `forget` on the
  `Memory` trait, one backend-agnostic surface, with feedback-weighted recall so
  the agent gets better with use.
- The model seam: `Embedder` is the one place a real embedding model plugs in,
  the same boundary as the `LlmClient` seam in the other examples.
- The graph surface on `Laser::graph(name)`: `upsert` writes nodes and edges,
  `neighbors` is the one-hop read, and the `start_match` + `out` builder runs a
  multi-hop traversal, all reusing the query `Filter` grammar.
- Content-addressed identity: `MemoryId::content`, `GraphNode::entity`, and
  `GraphEdge::relate` mint ids from the wire crate's one canonical `content_id`,
  so the same fact or entity converges across every SDK.
- The two halves swap to durable backends with no change to the loop:
  `Laser::memory()` (log- or KV-backed) or `QueryMemory` for managed recall, the
  knowledge graph for the relationship layer.
