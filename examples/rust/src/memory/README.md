# memory

> Agentic memory, three facets: remember and recall in process, persist durably, and traverse how it connects.

## What it does

An agent gets better with use, and this runs three facets over one incident-knowledge domain.

**Memory** (in-process, needs no server): the four agentic-memory verbs as one loop over `VectorMemory`.

1. **Remember** the assistant stores what it knows, each item a fact.
2. **Recall** a question recalls the semantically closest facts, ranked.
3. **Improve** the operator upvotes the fact that resolved the incident, and the next recall ranks it first. The agent learns.
4. **Forget** a superseded fact is forgotten and stops surfacing.

**Durable memory** (managed): the same four verbs over the memory topic on the log. Every remember publishes to the topic, so the facts persist and replay, and a deployment materializes them into the read view the console's Memory view shows. `memory_topic("incidents")` configures that topic up front: its partition count and a message-expiry window that bounds how long the history lives.

**One scope** (managed): the incident is one conversation, so `laser.context(conversation)` binds it once. The conversation's messages append and read back through the scope, and `scope.memory("incidents")` recalls the same durable facts without repeating the id. The session (messages plus working memory) is one scope, while durable facts and the graph stay cross-conversation on purpose.

**Knowledge graph** (managed): the same ops domain becomes a graph of content-addressed nodes and typed edges, then traversals answer how the entities connect, which recall alone cannot.

5. **Build** upsert a realistic slice (services, components, teams, incidents) wired by `depends_on`, `mitigated_by`, `replicates`, `owns`, and `affected`.
6. **Neighbors** what sits one hop from `checkout`.
7. **Traverse** from every `Service`, follow `depends_on` to the components the whole platform rests on.
8. **Blast radius** from an incident, follow `affected` to everything it touched, the question an on-call engineer actually asks.
9. **Provenance** every node and edge links back to the source record it was built from, a click-through in the console.
10. **Bitemporal** read the graph "as of" a point in valid-time, so a mitigation appears only after it was applied.
11. **Paths** return whole traced paths, not just the set of reached nodes.

## Run it

```sh
cargo run --release --example memory
```

The memory half runs with no server. The durable-memory and graph halves are managed: point the example at a LaserData Cloud deployment to run them, otherwise it prints how and exits clean.

```sh
LASER_CONNECTION_STRING=user:pwd@your-laserdata-cloud-host \
  cargo run --release --example memory
```

## Where to look (LaserData Cloud)

The example builds the `ops` graph, so it appears in the management console's graph explorer. Open it there to browse the same nodes and edges, or walk them from a start node.

## Highlights

- The agentic-memory verbs `remember` / `recall` / `improve` / `forget` on the `Memory` trait, one backend-agnostic surface, with feedback-weighted recall so the agent gets better with use.
- The model seam: `Embedder` is the one place a real embedding model plugs in, the same boundary as the `LlmClient` seam in the other examples.
- The graph surface on `Laser::graph(name)`: `upsert` writes nodes and edges, `neighbors` is the one-hop read, and the `start_match` + `out` builder runs a multi-hop traversal, all reusing the query `Filter` grammar.
- Content-addressed identity: `MemoryId::content`, `GraphNode::entity`, and `GraphEdge::relate` mint ids from the wire crate's one canonical `content_id`, so the same fact or entity converges across every SDK.
- One memory model, no backend to choose: `Laser::memory(namespace)` remembers to the log and recalls, and `Laser::memory_topic(topic)` configures the same durable memory (stream, partitions, message-expiry) with no change to the verbs. The knowledge graph carries the relationship layer alongside it.
- Durable memory is conversation-scoped: each row records the conversation that wrote it, so the LaserData console's Conversations page links a per-conversation lens into the memory, graph, and query surfaces filtered to that conversation.
