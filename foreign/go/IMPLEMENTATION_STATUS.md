# Go SDK implementation status

The Go client uses the Apache Iggy Go SDK over TCP. The [parity matrix](../../docs/parity.md#the-go-client) lists the raw capabilities and their Go entry points. The matrix checks named declarations. The wire tests, shared behavior scenarios, and live tests check different aspects of interoperability.

| Area | Implementation and verification |
| --- | --- |
| Wire contract | The Go wire tests read the shared fixtures and check Rust declarations. The differential test sends Go-generated values through the Rust decoder and encoder. |
| Streaming | Stream and topic administration, synchronous direct producers with routing, confirmations and statistics, a size-and-time batching producer, governed batch publishing, replay, encryption, consumer groups with server-side policies, stored offsets, and filtered reads. A group reader walks its whole assignment with fenced acknowledgments, a local guard and server-held waits. Typed topics support JSON, CBOR and registered writer schemas. The live tests include Go-to-Rust and Rust-to-Go records. |
| Managed operations | Queries with bounded row iteration, full collection, typed payload decoding, vector search and aggregate windows, graphs, schemas, projections, bindings, materializations, change feeds, forks, authorization, filter catalog, KV with patch, copy, move and expiry, and fenced leases. `FencedLeaseClient` prepares and executes the lease verbs on a connection of its own. `ExecuteBatch` returns the replies to independent operations in order. `GetMany` batches reads. |
| Consistency and sessions | KV, graph, fork, and session reads send the applicable frontier. Session handles support layouts, leases, lifecycle, state, model and tool calls, budgets, checkpoints with turns and replay, operator control reads, linked memory, linked graphs, retrievals, and session-scoped KV. `Sessions.Watch` follows the session index. |
| Agents and workflows | Agent gates, routing to the advertised inbox with the card as fallback, contracts, scatter, input requests, chunk stream writers, durable workflow steps, compensation, fenced work, and the plain function workflow. The journal codec uses the Rust variant names and fields. Spawned agents enforce operator control and bound what they queue. Package `lasertest` builds handler inputs for unit tests. |
| Memory and context | Log-backed memory with managed read-after-write visibility, named items, caller-named memory topics, vector and custom backends, embedding and ranking interfaces, per-conversation summary and pruning, context assembly with selection reports, snapshots, and conversation replay. |
| Bridges | A2A, MCP, and AG-UI. Task-bound A2A callers support child-session submission. Agent-card signing and verification share the reference signature vectors. The request mappings are exported for an edge that builds the envelope itself. |
| Connection and errors | A connection string or an address with credentials, ops stream and topic overrides, and capability negotiation. Error predicates classify a failure from any surface. |
| Examples | The [Go examples](../../examples/go/README.md) have one program per scenario. The [quick start](../../README.md#quick-start) publishes and replays a record. |

## Transport constraints

The pinned Apache Iggy Go SDK supports TCP. Its producer API lacks producer idempotence and background sharding. Go provides synchronous direct sends and a size-and-time batching producer. The Rust producer's native asynchronous queue modes have no Go counterpart. Go users release a lease explicitly, typically with a deferred call.

## Verification

`just go-check` runs local checks across the client, examples, and BDD modules. `just go-differential` compares Go wire vectors with Rust. `scripts/run-bdd-tests.sh go` exercises the shared scenarios against the pinned server. `scripts/run-managed-bdd.sh go` runs the managed cases on a local stack. `--postgres` or `--doris` enables the corresponding remote SQL scenarios. The recovering-catalog case runs with `scripts/run-bdd-tests.sh go --recovering-catalog`.

Go CI runs the unit suite at the declared minimum, the previous stable and the current stable release. It checks the parity matrix, formatting, vet, module tidiness, unit and race tests, decoder fuzzing, known reachable vulnerabilities, cross-language wire compatibility, live interoperability against Apache Iggy, and the shared managed scenarios on Laser Stack. The aggregate gate combines the Apache Iggy, PostgreSQL-backed Laser Stack, and recovering-catalog reports and requires a passing result for every shared scenario. The remote SQL scenarios can also run on Doris.
