# LaserData - Laser SDK

Laser SDK provides streaming, queries, key-value state, forks, graphs, and agent coordination over one [Apache Iggy](https://iggy.apache.org) connection. [LaserData, Inc.](https://laserdata.com) maintains the SDK. A log stores records in append order. Managed services build query results and state from those records.

The log is the source of truth. A read model is data organized for a specific query. Services can rebuild these models from retained log records. A support task can record messages, keep working memory, and track dependencies through the same SDK.

Rust ([`laser-sdk` on crates.io](https://crates.io/crates/laser-sdk)) defines the reference SDK behavior. Python ([`laser-sdk` on PyPI](https://pypi.org/project/laser-sdk/), [source](foreign/python/README.md)) calls the Rust implementation. TypeScript ([`@laserdata/laser-sdk` on npm](https://www.npmjs.com/package/@laserdata/laser-sdk), [source](foreign/typescript/README.md)) provides a native Node client. Shared test data and behavior scenarios compare the clients. The [`laser-wire` crate](https://crates.io/crates/laser-wire) ([source](wire/README.md)) defines the data exchanged between them.

> [Laser Stack](https://github.com/laserdata/laser-stack) runs the Iggy fork and `laser-plane` for local development. It supports the managed SDK operations, including queries. In the Laser Stack checkout, run `./scripts/up` to start the services and obtain `LASER_CONNECTION_STRING`. Laser Stack excludes proprietary Cloud features such as the Stream and Data UI.

Read the three-language guides at [docs.laserdata.cloud/laser-sdk](https://docs.laserdata.cloud/laser-sdk). Start with the [quickstart](https://docs.laserdata.cloud/laser-sdk/quickstart) or the focused [`examples/`](examples/README.md).

After the focused examples, [Photon Market](https://github.com/laserdata/laser-example-photon-market) shows a more realistic Rust system built with Laser SDK across multiple modules and microservices.

## Quick start

Choose where to run:

- In [LaserData Cloud](https://laserdata.cloud), create a Free deployment. Copy its connection string from the Console Credentials tab. Export the value as `LASER_CONNECTION_STRING`.
- In the [Laser Stack](https://github.com/laserdata/laser-stack) checkout, run `./scripts/up`. Copy the printed `LASER_CONNECTION_STRING` export.
- Start Apache Iggy for streaming. Set `LASER_CONNECTION_STRING` to its connection string.

The repository examples read `LASER_CONNECTION_STRING`. Export it once or set it for one command:

```sh
export LASER_CONNECTION_STRING='<connection string>'
LASER_CONNECTION_STRING='<connection string>' cargo run --example log
```

The examples below connect to local Apache Iggy. The default TCP port is `:8090`, so the address can omit it. Laser SDK uses the transport that Iggy provides.

Rust ([crates.io](https://crates.io/crates/laser-sdk))

```sh
cargo add laser-sdk
```

```rust,no_run
use laser_sdk::prelude::*;

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    let laser = Laser::connect("iggy:iggy@127.0.0.1").await?;
    let topic = laser.stream("telemetry").topic("inferences");
    topic.ensure(4).await?;
    topic.publish().json(&serde_json::json!({ "latency_ms": 42 }))?.send().await?;

    let messages = topic.replay()?.poll().await?;
    println!("read {} message(s)", messages.len());
    Ok(())
}
```

Python ([PyPI](https://pypi.org/project/laser-sdk/))

```sh
pip install laser-sdk
```

```python
import asyncio
import laser_sdk as ls

async def main():
    laser = await ls.Laser.connect("iggy:iggy@127.0.0.1")
    topic = laser.stream("telemetry").topic("inferences")
    await topic.ensure(partitions=4)
    await topic.publish().json({"latency_ms": 42}).send()

    messages = await topic.replay().poll()
    print(f"read {len(messages)} message(s)")

asyncio.run(main())
```

TypeScript ([npm](https://www.npmjs.com/package/@laserdata/laser-sdk))

```sh
npm install @laserdata/laser-sdk
```

```ts
import { Laser } from "@laserdata/laser-sdk"

await using laser = await Laser.connect("iggy:iggy@127.0.0.1")
const topic = laser.stream("telemetry").topic("inferences")
await topic.ensure(4)
await topic.publish().json({ latency_ms: 42 }).send()

const messages = await (await topic.replay()).poll()
console.log(`read ${messages.length} message(s)`)
```

The examples use open streaming. Add `managed` for projections, queries, KV, forks, graphs, and the run registry on Laser Stack or LaserData Cloud. Add `agent` for handlers, memory, contracts, and workflows.

Streaming sends return Apache Iggy commit confirmations. A confirmation identifies the stream, topic, partition, and first offset in a batch. The list can be empty when the server does not report offsets. Completion follows the topic durability policy. Use the stream, topic, and partition together with the offset.

One connection can address every stream that the user can access. Use `laser.stream(name).topic(name)` to name the stream and topic. A default stream enables the shorter `laser.topic(name)` form. It does not restrict the connection to that stream.

## One grammar, every primitive

An accessor selects a feature on the connected client. A method performs an action on that feature. The calls follow this form: `object.verb(input).await`.

| Accessor | Primitive | Reach for it to |
| --- | --- | --- |
| `laser.stream(name).topic(name)` | Log | publish and consume records, replay by offset, batch |
| `laser.query(index)` / `laser.query_lakehouse(destination, generation)` | Views | filter, aggregate, page, inspect or cancel operational and lakehouse queries |
| `laser.destinations()` | Destinations | declare materialization targets, change desired state, inspect checkpoints and query routes |
| `laser.graph(name)` | Graph | link entities, traverse, find neighbors and nearest vectors |
| `laser.watch()` | Change feed | consume advancement records instead of re-querying blind |
| `laser.kv(namespace)` / `laser.fork(id)` | State | point reads and writes, CAS, leases, copy-on-write branches |
| `laser.memory(scope)` | Memory | remember, recall (semantic / keyword / hybrid), consolidate |
| `laser.context(id)` | Context | append and assemble one conversation's record, and scope its memory to that conversation |
| `laser.sessions().create(id)` | Session | typed conversation turns, context for a model, scoped memory, and replay from saved offsets |
| `laser.agent(id)` / `laser.contract(..)` / `laser.workflow(..)` / `laser.runs()` | Fabric | directed asks, deadline contracts, ordered workflows, the run registry |

The Rust form follows:

```rust,ignore
use std::time::Duration;

let laser = Laser::connect("iggy:iggy@127.0.0.1").await?;
let orders = laser.stream("app").topic("orders");
let audit = laser.stream("audit").topic("events");

// Log: streams group topics, topics carry your records.
orders.ensure(4).await?;
audit.ensure(4).await?;
orders.publish().json(&order)?.send().await?;
audit.publish().json(&event)?.send().await?;
let mut replay = orders.replay()?;

// Views: declared projections answer queries, the graph answers traversals.
let rows = laser
    .query("orders_v1")
    .where_eq("status", "paid")
    .limit(10)
    .fetch()
    .await?;
let nearby = laser.graph("kg").neighbors(node, EdgeDir::Out, None, 2).await?;
let mut feed = laser.watch().index("orders_v1").records()?; // await-then-query

// State: point reads and writes, optimistic concurrency, branches.
laser
    .kv("sessions")
    .set("user:42")
    .json(&session)?
    .ttl(Duration::from_secs(300))
    .send()
    .await?;
let draft = laser.fork("what-if");

// Fabric: identities, context, memory, coordination, runs.
let reply = laser.agent(id).ask(commands, replies, task, &prov, timeout).await?;

// Context: one task streams its messages, keeps its memory, resolves its deps.
let ctx = laser.context(conversation);
ctx.append(AgentTopic::Audit, b"step done").await?;
let facts = ctx.memory("support").recall().semantic("refund disputes").fetch().await?;
let deps = ctx.graph("services").neighbors(node, EdgeDir::Out, None, 2).await?;

// Session: the same conversation as typed turns, context, memory, and replay.
let session = laser.sessions().create("agent-42");
session.append(SessionTurnKind::Instruction, b"summarize the ticket").await?;
session.append(SessionTurnKind::ModelResponse, b"it is a login bug").await?;
let turns = session.context().await?; // kinds and payloads, last 50 turns within 4000 tokens
let facts = session.memory().search("login bug").await?;
let checkpoint = session.checkpoint().await?; // serializable, persist it anywhere
let later = session.replay(checkpoint, Vec::new(), |mut acc, turn| { acc.push(turn.text()); acc }).await?;

laser.memory("notes").set("current-plan", plan_json).await?; // named point state, an event on the memory topic
let run = laser.workflow("refund").registered().step(/* .. */).run().await?;
let page = laser.runs().list().state(AgentRunState::Running).fetch().await?;
```

The Python form follows:

```python
laser = await ls.Laser.connect("iggy:iggy@127.0.0.1")
orders = laser.stream("app").topic("orders")
audit = laser.stream("audit").topic("events")

# Log
await orders.publish().json(order).send()
await audit.publish().json(event).send()

# Views + graph + change feed
rows = await laser.query("orders_v1").where_eq("status", "paid").limit(10).fetch()
nearby = await laser.graph("kg").neighbors(node, direction="out", depth=2)
feed = laser.watch(index="orders_v1")

# State
await laser.kv("sessions").set("user:42").json(session).ttl(300).send()

# Fabric: one task streams its messages, keeps its memory, resolves its deps
ctx = laser.context(conversation)
await ctx.append("audit", b"step done")
facts = await ctx.memory(laser.memory()).recall(semantic="refund disputes")
turns = await ctx.fetch(last_n=20, token_budget=4_000)
deps = await ctx.graph("services").neighbors(node, direction="out", depth=2)

# Session: the same conversation as typed turns, context, memory, and replay
session = laser.sessions().create("agent-42")
await session.append("instruction", b"summarize the ticket")
await session.append("model.response", b"it is a login bug")
turns = await session.context()
facts = await session.memory().search("login bug")
checkpoint = await session.checkpoint()
later = await session.turns_since(checkpoint)
run = await laser.runs().submit("refund", task)
```

The TypeScript form uses camelCase method names:

```ts
const laser = await Laser.connect("iggy:iggy@127.0.0.1")
const orders = laser.stream("app").topic("orders")
const audit = laser.stream("audit").topic("events")

// Log
await orders.publish().json(order).send()
await audit.publish().json(event).send()

// Views + graph + change feed
const rows = await laser
  .query("orders_v1")
  .whereEq("status", "paid")
  .limit(10)
  .fetch()
const nearby = await laser.graph("kg").neighbors(node, "out", undefined, 2)
const feed = await laser.watch().index("orders_v1").records()

// State
await laser.kv("sessions").set(key).json(session).ttl(300_000_000n).send()
const draft = laser.fork("what-if")

// Fabric: one task streams its messages, keeps its memory, resolves its deps
const ctx = laser.context(conversation)
await ctx.append("audit", new TextEncoder().encode("step done"))
const facts = await ctx
  .memory("support")
  .recall()
  .semantic("refund disputes")
  .fetch()

// Session: the same conversation as typed turns, context, memory, and replay
const session = laser.sessions().create("agent-42")
await session.append("instruction", encode("summarize the ticket"))
await session.append("model.response", encode("it is a login bug"))
const turns = await session.context()
const hits = await session.memory().search("login bug")
const checkpoint = await session.checkpoint()
const later = await session.turnsSince(checkpoint)
const run = await laser.runs().submit("refund", task)
```

### Streaming contract

- Accessors are free to construct. IO starts at terminal verbs such as `.send()` and `.fetch()`.
- `topic.producer()` supports batching, linger, retries, and key or partition routing.
- `topic.consumer(..)` and `topic.consumer_group(..)` provide live reads, replay, polling control, and automatic or explicit offset commits.
- `ConsumerMessage` preserves the exact Apache Iggy headers and log position.
- `topic.iggy_producer()`, `topic.iggy_consumer_group()`, `laser.client()`, and `laser_sdk::iggy` expose Apache Iggy directly when the Laser surface is not enough.

All three SDKs use Apache Iggy VSR framing. Managed commands use the same connection and are dispatched according to the capabilities reported by the server.

Data platform (the core, stands on its own):

| Primitive | What you get |
| --- | --- |
| Publish / consume | Typed serde values or raw records onto topics, direct producer batching/linger/routing, and live async partition or consumer-group readers with server offsets and configurable commit policies. |
| Projections + query DSL | Filters, aggregates, time ranges, pagination, and vector recall over indexes you declare once per topic, with opt-in read-your-writes consistency, and a `conversation(id)` filter that narrows any query to the records one conversation wrote. |
| Key-value + forks | Working state with compare-and-swap, conditional ops, expiry, JSON merge-patch, and revocable holder-scoped leases, plus copy-on-write branches of the read model for speculative work. |
| Knowledge graph | Content-addressed nodes and edges, traversal / neighbor / nearest-vector / path reads, bitemporal valid-time edges, source back-links, and a `conversation(id)` filter that narrows a traversal to one conversation. |
| Governance (RBAC) | Capability grants over the managed surfaces: `effect feature:action [on resource]` assembled through roles bound to the unforgeable server-stamped user, deny-wins, default-deny. New users receive no managed capabilities unless roles are explicitly bound. `laser.whoami()` + the role/binding/history verbs, including revision-guarded role binding. Orthogonal to Iggy's own permissions, enforced server-side at the edge. |

Agent fabric (opt in with the `agent` feature):

| Primitive | What you get |
| --- | --- |
| Reliable runtime | A consumer with dedup, retry, and dead-letter, request/reply correlation, conversation and causality tracking, routing, sessions, and context assembly. |
| Agentic memory | One durable model: `remember` / `recall` / `improve` / `forget` publish to a memory topic (the versioned audit) that materializes to a versioned key-value read view and recalls by recency. The topic is configurable (`memory_topic(name).stream(..).partitions(n).ttl(d)`). The in-process vector backend and rerank seam add semantic / keyword / hybrid ranking. Consolidation, token-budgeted `to_context_block`, and content-addressed dedup compose above both. Vector memory created from a `Laser` inherits its action governor even though the index itself stays local. A scan over the read view narrows to one conversation with `conversation(id)`, the same lens the query and graph reads carry. |
| Discovery | Agents advertise a capability card and a live inbox, fused into one cached registry with health-aware resolution and reversible operator `quarantine` / `unquarantine`. One connection may advertise one agent. Sensitive routes can require the presence's server-authenticated principal. |
| Coordination | `contract` (a directed task with a deadline and a real consumed / completed / timed-out answer), `fan_out` / `scatter` (ask every capable agent, gather under a policy), and `approval_gate` (pause for a human). With signing enabled, terminals fail closed on unsigned or wrongly signed replies and expose the verified principal. |
| Workflow engine | `laser.workflow(..).step(..)`: dependency-ordered steps, budgets, verifier panels, saga compensation, crash-recovery replay from a journal, and per-step fenced leases. Use `.exclusive_in(namespace)` when the handler commits an external effect with `kv(target_namespace).cas_fenced(key, namespace, ..)`. The engine races bounded renewal against completion, keeps the lease through verification and completion journaling, then releases it before `OnTimeout::Reassign` gives a fresh holder a new fence. |
| Run registry | `laser.runs()`: submit a run, read its state, list runs (filtered, paged), record a cancel intent. A managed read model folded from the status records a `.registered()` workflow or contract stamps, so "what happened to that task" is one call, and the log stays the truth. |
| AGDX envelope | A typed, versioned, fixture-pinned agent message format on the log, with producer verbs, resumable token streams, and deterministic reassembly. ([notes](docs/agdx.md)) |
| Action governance | A pre-effect policy hook (`ActionGovernor`) over everything an agent publishes: allow, observe, block, step-up, modify, or defer each send, typed or raw topic publish, AGDX verb, and memory write before it runs. Enforce or shadow mode records every non-allow decision as digest-chained evidence. `QuorumGovernor` runs named governors concurrently under `All` / `Any` / `AtLeast(n)`. Every mandatory voter must affirm, invalid configurations and mandatory errors block, and conflicting body replacements block. `SwappableGovernor` changes the active policy without reconnecting. Defense in depth above server-owned RBAC. |
| Durable intent | SDK-level typed records for asynchronous effect approval, not an AGDX wire extension. Fallible `Intent::builder().build()` validates the frozen voter set, threshold, deadline, and body digest. Fallible `Vote::cast` binds an eligible voter to that digest and policy version. `decide` ignores invalid, early, late, and future ballots, then returns a canonical commit or abort. Mandatory voters must allow, conflicting repeats abort, and `Decision::authorizes` verifies the exact intent before an effect runs. Voter identity is trusted only under a signed-principal or topology-isolated deployment profile. |
| Swarm activity | A supervisor's replay-safe read model over governance evidence: `SwarmActivity::observe` deduplicates by decision id, `.agent(name)` reads one agent's counts and deterministic latest decision, and `.agents()` lists every folded agent busiest first. |
| Crash context | A recovery tool's one-call bundle over an already-read journal tail, dead-letter capsule, and latest governance decision. `.summarize()` emits a bounded deterministic digest with control characters escaped, so untrusted payloads cannot forge diagnostic lines. It performs no I/O and never invokes a model. |
| Edge bridges | A2A, MCP, and AG-UI mapped onto AGDX over the durable log, no SSE. ([interop](docs/interop.md)) |

Agent routing, contracts, fan-out, and workflows run as client-side state machines. They track offsets, deadlines, leases, and replies in the log. They do not require a separate orchestration server. Their durability and ordering depend on the underlying log and the selected policies.

## Why it is good to build on

- Streaming and managed operations use the same connection and record metadata.
- Read models can rebuild from retained records. A new projection, index, or agent can read earlier records by offset.
- `laser.stream("commerce").topic("orders").json::<Order>()` publishes and replays `Order` values. A schema-bound handle checks each value against its registered schema before sending. Decode failures include the record position. Producers and consumers support batches.
- Streaming, the agent runtime, log-based memory, and open coordination run on Apache Iggy. Managed operations use capabilities reported by LaserData Cloud or Laser Stack.
- A fence is an increasing token that identifies a lease holder. An `.exclusive_in(namespace)` step and `kv(namespace).cas_fenced(..)` use the same fence sequence. The protected state rejects writes from a replaced holder.
- Rust defines the reference behavior. Python calls that implementation. TypeScript uses the same encoded test data and shared behavior scenarios.

## Open core, managed surface

| Deployment | Available surfaces |
| --- | --- |
| Apache Iggy | Streaming, provenance, AGDX, the agent runtime, log-backed memory, contracts, and workflows |
| Laser Stack | Everything above, plus query, projections, KV, forks, graph, durable memory, the run registry, durable dedup, and fenced leases |
| LaserData Cloud | The complete SDK surface with managed deployment and UI services |

Capability negotiation runs during connection setup. A managed call against Apache Iggy without a managed backend returns `LaserError::Unsupported`. The underlying client remains available through `topic.iggy_producer()`, `topic.iggy_consumer(..)`, and `laser.client()`.

Laser Stack runs the LaserData Apache Iggy fork with `laser-plane`. Apache Iggy owns the durable log and VSR connection. `laser-plane` maintains the managed read models and handles query, projection, schema, KV, fork, graph, and run operations.

### Authorization

- Apache Iggy RBAC controls server, stream, and topic access.
- LaserData governance RBAC controls managed capabilities.
- The layers are independent. Creating a user does not grant a managed role.
- `LaserError::is_permission_denied()` and `is_stream_or_topic_not_found()` classify native access failures. Managed authorization uses the unified unauthorized result.

### TLS

Connections to `*.laserdata.cloud` and `*.laserdata.com` automatically use the public LaserData CA bundled with the SDK. `LASER_TLS_CERT=<path>` enables TLS with an explicit CA for any host or overrides the bundled CA. `LASER_NO_TLS=1` disables automatic TLS setup. Other hosts keep the TLS settings from their connection string when neither variable is set.

## Documentation

- [Tutorial](docs/tutorial.md): a progressive guide from one message to projections, queries, vector recall, codecs, multi-stream topologies, and the agent fabric.
- [Building agents](docs/building-agents.md): a recipe guide that works one multi-agent scenario end to end, including governed agents, managed-surface RBAC, and concrete SDK calls.
- [AGDX notes](docs/agdx.md): an in-repo development reference for the Agent Data Exchange Protocol the SDK implements (the envelope, Apache Iggy binding, the surfaces). The protocol home is [agdxprotocol.ai](https://agdxprotocol.ai).
- [Interop](docs/interop.md): the A2A / MCP / AG-UI edge bridges over AGDX.
- [Examples](examples/README.md): aligned Rust, Python, and TypeScript systems runnable against Apache Iggy, Laser Stack, or LaserData Cloud.
- [`wire/README.md`](wire/README.md): the contract crate and its compatibility rules.

## Workspace

| Crate | What it is |
| --- | --- |
| [`laser-wire`](wire/README.md) (`wire/`) | the wire contract: codes, envelopes, query IR, dictionaries, caps, the AGDX envelope, and the golden fixture corpus. Runtime-free and wasm-portable. |
| [`laser-sdk`](sdk/README.md) (`sdk/`) | the client and agent runtime, re-exporting the wire crate as `laser_sdk::wire`. |
| [`foreign/python`](foreign/python/README.md) | the Python SDK, PyO3 bindings over the Rust crate. |
| [`foreign/typescript`](foreign/typescript/README.md) | the native TypeScript SDK over Apache Iggy. |
| [`examples`](examples/README.md) | Eight focused examples cover `log`, `query`, `watch`, `kv`, `graph`, `recall`, `context`, and `agent` in Rust, Python, and TypeScript. Nine larger examples cover streaming, event analytics, an order book, load generation, support agents, memory, interoperability, `orchestra`, and governance. |

## Benchmarks

Run the maintained native campaign from the repository root:

```sh
just bench
just bench 15 3 8 # seconds per arm, repetitions, parallel lanes
```

- Matched raw Iggy and Laser streaming workloads use TCP VSR and one connection per producer or consumer lane.
- The maintained campaign covers streaming, AGDX, managed surfaces, MCP, startup, and recovery.
- The test runner runs in release mode and keeps progress output outside timed regions.
- Results include immutable JSON evidence, HDR histograms, CSV exports, and a standalone HTML report.
- `bench/.env` can select caller-provided native binaries. Otherwise the test runner resolves the maintained signed artifacts.

Use `just bench-smoke` to validate the test runner and `just bench-full` for the exhaustive matrix. Read [`bench/README.md`](bench/README.md) for workload definitions, result interpretation, and authoritative campaign requirements.

## Development

Run the repository gates from the root:

```sh
just lint    # fmt + sort + machete + clippy -D warnings
just test    # workspace unit tests
just test-it # integration tests against Apache Iggy
just bdd     # cross-SDK BDD conformance (needs Docker)
just ci      # the full gate (lint, test, wasm, deny, advisories, fixtures)
```

Feature profiles select Laser capabilities:

- Default: typed streaming and provenance.
- `--no-default-features --features streaming`: streaming only.
- `--features agent`: agent runtime and coordination.
- `--features managed`: every managed surface.

Every profile uses VSR.

## Delivery model

At-least-once with idempotent operations, per-conversation (per-partition) ordering, and replay-friendly throughout. Materialized indexes can rebuild from explicit source offsets and snapshots instead of making full replay a hot-path default.

## License

Apache-2.0. Copyright LaserData, Inc. Apache and Apache Iggy are trademarks of the Apache Software Foundation, and use does not imply endorsement.

## Publish recovery

Publish attempts default to 60 seconds with three retries. Retry delays start at 250 milliseconds, double after each failure, and stop increasing at 30 seconds. Configure these values through the client builder or connect arguments. The corresponding environment variables are `LASER_PUBLISH_TIMEOUT_MS`, `LASER_PUBLISH_MAX_RETRIES`, and `LASER_PUBLISH_RETRY_BACKOFF_MS`. Explicit configuration overrides these variables. Exhausted retries return an error for the application to handle.

See [publish recovery and outage handling](docs/publish-recovery.md).
