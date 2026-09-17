# memory

This example stores incident facts, retrieves related facts, and records feedback. It also demonstrates durable memory and graph relationships on managed deployments.

## What it does

The example applies memory operations to one set of incident facts.

The local phase uses `VectorMemory` and needs no server:

1. Record each known fact.
2. Retrieve the facts most similar to a question.
3. Apply positive feedback and make sure that the relevant fact ranks first.
4. Forget a superseded fact and make sure that recall excludes it.

The durable phase publishes memory records to a topic and reads the managed view. `memory_topic("incidents")` configures the partition count and message expiry. The topic retains the history under that policy.

`laser.context(conversation)` selects the incident conversation. The scope appends and reads messages without repeating the ID. `scope.memory("incidents")` uses that conversation for session memory. Durable facts and graph relationships can span conversations.

The managed graph phase connects incident entities through typed relationships:

5. Upsert services, components, teams, and incidents with `depends_on`, `mitigated_by`, `replicates`, `owns`, and `affected` relationships.
6. Read the neighbors of `checkout`.
7. Follow `depends_on` from each `Service` to its components.
8. Follow `affected` from an incident to the entities it affected.
9. Follow source references from graph elements to their original records.
10. Read the graph at a selected valid time to exclude mitigations that started later.
11. Return complete paths through the graph.

## Run it

Run from `examples/rust`:

```sh
cargo run --release --example memory
```

The memory half needs no server. Durable memory and graph traversal require Laser Stack or LaserData Cloud.

```sh
LASER_CONNECTION_STRING=iggy:laser@127.0.0.1:8090 \
  cargo run --release --example memory
```

Connection failures return the original SDK error. A successful connection without the required capability skips only that phase.

## Where to look

The example builds the `ops` graph. Query it through the SDK or open it in the LaserData Cloud console.

## Highlights

- Use `remember`, `recall`, `improve`, and `forget` through `Memory`. Feedback changes recall ranking.
- The model seam: `Embedder` is the one place a real embedding model plugs in, the same boundary as the `LlmClient` seam in the other examples.
- Use `Laser::graph(name)` for `upsert`, `neighbors`, and traversal. `start_match` and `out` select a path through the graph using the shared `Filter` grammar.
- Content-addressed identity: `MemoryId::content`, `GraphNode::entity`, and `GraphEdge::relate` mint ids from the wire crate's one canonical `content_id`, so the same fact or entity converges across every SDK.
- `Laser::memory(namespace)` supplies log-based memory. `Laser::memory_topic(topic)` configures its stream, partitions, and expiry. The graph supplies relationship reads.
- Memory rows retain their source conversation. The Console can use it to filter memory, graph, and query views.
