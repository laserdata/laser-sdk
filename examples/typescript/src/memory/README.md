# memory - remembered, durable, and connected knowledge

> Agentic memory in three facets: learn in process, persist by conversation, and traverse relationships that recall alone cannot answer.

## What it does

The example uses one incident-knowledge domain across three related surfaces.

**Vector memory** runs in process and demonstrates the four memory verbs over a deterministic 64-dimensional embedder.

1. **Remember** stores the same eight operational facts as the Rust and Python examples under one new conversation.
2. **Recall** returns the three closest facts for `checkout is slow during the sale`.
3. **Improve** upvotes the read-replica fact that resolved the incident and asserts that it ranks first.
4. **Forget** removes the superseded search-index note and asserts that recall no longer returns it.

**Durable memory** runs when the deployment advertises graph and KV support.

5. Configures the `incidents` memory topic with four partitions and a one-day message expiry.
6. Remembers the same eight facts durably under the original conversation and recalls the three most recent.
7. Prints each recalled fact's source stream, topic, partition, and offset.
8. Appends an audit message, binds the same durable memory handle to `laser.context(conversation)`, and reads both through one scope.

**Knowledge graph** runs when the deployment advertises graph and KV support.

9. Registers the `ops` graph projection.
10. Writes every entity to the `topology` key-value namespace and attaches that source record to its graph node.
11. Upserts content-addressed services, components, teams, incidents, and their typed relationships.
12. Reads the one-hop neighborhood around `checkout`.
13. Starts from every `Service`, follows `depends_on`, and prints the shared component layer.
14. Follows `affected` from `INC-101` to calculate its blast radius.
15. Traces `checkout` back to its source key-value record.
16. Reads the `mitigated_by` relationship before and after its valid-time boundary.
17. Returns whole paths from the incident to each affected entity.

## Run it

```sh
npm run example:memory
```

The vector phase needs no server. The example connects only after completing that phase. Durable memory and graph traversal are managed read models and print a precise skip reason when no deployment is available.

Run every phase against LaserData Cloud.

```sh
LASER_CONNECTION_STRING=user:pwd@your-laserdata-cloud-host \
  npm run example:memory
```

Set `LASER_STREAM` when the deployment has provisioned a specific stream.

## Where to look (LaserData Cloud)

- **Memory**: durable facts in the `incidents` topic and namespace, filtered by the run's incident conversation.
- **Conversations**: the incident audit message and its conversation-scoped memory lens.
- **Key-value**: the `topology` namespace that gives every graph entity a live source record.
- **Graph explorer**: the `ops` graph, including eight services, eight components, three teams, and two incidents.
- **Graph traversal**: `checkout` neighbors, shared service dependencies, the `INC-101` blast radius, valid-time mitigations, and traced paths.

## Highlights

- `MemoryHandle.vector(embedder)` is the model seam. Replace the deterministic embedder with a production embedding provider without changing memory calls.
- `remember`, `recall`, `improve`, and `forget` are one backend-independent vocabulary.
- `laser.memoryTopic("incidents").partitions(4).ttl(86_400_000).build()` mirrors Rust's configured durable memory topic with idiomatic millisecond duration input.
- `laser.context(conversation).memory(durable)` binds the exact durable handle and conversation once instead of substituting another namespace or topic.
- `graphNodeEntity` and `graphEdgeRelate` use content-addressed IDs, so the same entity or relationship converges across SDKs.
- `startMatch(...).out("depends_on")` uses the same typed query filter grammar as other managed surfaces.
- `source`, `validFrom`, `asOf`, and `returnPaths()` show provenance, bitemporal reads, and explainable traversal results without changing the graph model.
