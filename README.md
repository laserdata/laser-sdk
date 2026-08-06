# Laser SDK

**Build agents and data-driven systems on one durable log.** Ultra-low-latency **streaming**, a **query** layer, **key-value** state, copy-on-write **forks**, a **knowledge graph**, and a full **agent fabric** (memory, discovery, contracts, workflows), all over a single [Apache Iggy](https://iggy.apache.org) connection. By [LaserData, Inc.](https://laserdata.com)

**One connection replaces four systems.** The stream you already publish to becomes the store you query, the state you coordinate on, and the fabric your agents discover, route, and reason over. No second database, no cache, no orchestration server, nothing to keep in sync. The **log is the single source of truth**, and every other surface is a read model you can rebuild from offset 0. A support task, say, streams its messages, keeps its working memory, and resolves the dependencies between them, all in one place.

**Rust** ([`laser-sdk` on crates.io](https://crates.io/crates/laser-sdk)) is the reference SDK. **Python** ([`laser-sdk` on PyPI](https://pypi.org/project/laser-sdk/), [source](foreign/python/README.md)) binds the same Rust core. **TypeScript** ([`@laserdata/laser-sdk` on npm](https://www.npmjs.com/package/@laserdata/laser-sdk), [source](foreign/typescript/README.md)) is a native Node client checked against the same fixture and BDD corpus. The standalone [`laser-wire` crate](https://crates.io/crates/laser-wire) ([source](wire/README.md)) owns the language-neutral contract.

> **Run locally with [Laser Stack](https://github.com/laserdata/laser-stack).** It runs the LaserData Apache Iggy fork and `laser-plane` as a complete local development stack, so every Laser SDK surface, including managed queries, can be developed, tested, and deployed without LaserData Cloud. Run `./scripts/up` from the Laser Stack checkout to start the infrastructure and receive `LASER_CONNECTION_STRING`. Laser Stack does not include proprietary LaserData Cloud features such as the Stream and Data UI.

Full primitive-by-primitive docs, in all three languages, are at [docs.laserdata.cloud/laser-sdk](https://docs.laserdata.cloud/laser-sdk) - start with the [quickstart](https://docs.laserdata.cloud/laser-sdk/quickstart), or jump straight to [`examples/`](examples/README.md) for the tiny per-primitive examples the docs are built from.

After the focused examples, [Photon Market](https://github.com/laserdata/laser-example-photon-market) shows a more realistic Rust system built with Laser SDK across multiple modules and microservices.

## Quick start

Choose where to run:

- **LaserData Cloud:** [create a Free deployment](https://laserdata.cloud), copy its connection string from the Console's Credentials tab, and export it as `LASER_CONNECTION_STRING`.
- **Laser Stack:** run `./scripts/up` in the [Laser Stack](https://github.com/laserdata/laser-stack) checkout. It starts the complete local SDK surface and prints a copyable `LASER_CONNECTION_STRING` export.
- **Apache Iggy:** use a VSR-enabled server for the open streaming path, then set `LASER_CONNECTION_STRING` to its connection string.

The repository examples read `LASER_CONNECTION_STRING`. Export it once or set it for one command:

```sh
export LASER_CONNECTION_STRING='<connection string>'
LASER_CONNECTION_STRING='<connection string>' cargo run --example log
```

The snippets below connect to local Apache Iggy. `:8090` is the default TCP port and can be omitted. Laser SDK always uses VSR and has no protocol flag.

**Rust** ([crates.io](https://crates.io/crates/laser-sdk))

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

**Python** ([PyPI](https://pypi.org/project/laser-sdk/))

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

**TypeScript** ([npm](https://www.npmjs.com/package/@laserdata/laser-sdk))

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

That is the complete open streaming path. Add `managed` for projections, query, KV, forks, graph, and the run registry on Laser Stack or LaserData Cloud. Add `agent` for reliable handlers, memory, contracts, and workflows.

Every direct or fluent streaming send returns Apache Iggy's commit confirmations. Each confirmation identifies the selected stream, topic, partition, and batch base offset. The list can be empty when a server cannot report offsets. A confirmation is an in-memory commit position, not an fsync guarantee, and its base offset is meaningful only with its stream, topic, and partition.

One connection addresses every stream on the server. The canonical path is `laser.stream(name).topic(name)`. Each SDK also offers a default-stream helper for applications that mostly use one stream, but that helper only enables the shorter `laser.topic(name)` accessor and never limits the connection.

## One grammar, every primitive

Every feature is a **primitive** you reach by one accessor on the connected client, and every action is a **verb** on that primitive. One shape, `object.verb(input).await`, across the whole platform:

| Accessor | Primitive | Reach for it to |
| --- | --- | --- |
| `laser.stream(name).topic(name)` | **Log** | publish and consume records, replay by offset, batch |
| `laser.query(index)` | **Views** | filter, aggregate, page, vector-search declared projections |
| `laser.graph(name)` | **Graph** | link entities, traverse, find neighbors and nearest vectors |
| `laser.watch()` | **Change feed** | consume advancement records instead of re-querying blind |
| `laser.kv(namespace)` / `laser.fork(id)` | **State** | point reads and writes, CAS, leases, copy-on-write branches |
| `laser.memory(scope)` | **Memory** | remember, recall (semantic / keyword / hybrid), consolidate |
| `laser.context(id)` | **Context** | append and assemble one conversation's record, and scope its memory to that conversation |
| `laser.agent(id)` / `laser.contract(..)` / `laser.workflow(..)` / `laser.runs()` | **Fabric** | directed asks, deadline contracts, ordered workflows, the run registry |

Learn the pattern once and the whole platform reads the same way. In Rust:

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

laser.memory("notes").set("current-plan", plan_json).await?; // named point state, an event on the memory topic
let run = laser.workflow("refund").registered().step(/* .. */).run().await?;
let page = laser.runs().list().state(AgentRunState::Running).fetch().await?;
```

The same grammar in Python, one-to-one with the Rust accessors:

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
run = await laser.runs().submit("refund", task)
```

And the same grammar in TypeScript, camelCase over the identical accessors:

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
const run = await laser.runs().submit("refund", task)
```

### Streaming contract

- Accessors are free to construct. IO starts at terminal verbs such as `.send()` and `.fetch()`.
- `topic.producer()` supports batching, linger, retries, and key or partition routing.
- `topic.consumer(..)` and `topic.consumer_group(..)` provide live reads, replay, polling control, and automatic or explicit offset commits.
- `ConsumerMessage` preserves the exact Apache Iggy headers and log position.
- `topic.iggy_producer()`, `topic.iggy_consumer_group()`, `laser.client()`, and `laser_sdk::iggy` expose Apache Iggy directly when the Laser surface is not enough.

All three SDKs use Apache Iggy VSR framing. Managed commands use the same connection and are dispatched according to the capabilities reported by the server.

**Data platform** (the core, stands on its own):

| Primitive | What you get |
| --- | --- |
| **Publish / consume** | Typed serde values or raw records onto topics, direct producer batching/linger/routing, and live async partition or consumer-group readers with server offsets and configurable commit policies. |
| **Projections + query DSL** | Filters, aggregates, time ranges, pagination, and vector recall over indexes you declare once per topic, with opt-in read-your-writes consistency, and a `conversation(id)` filter that narrows any query to the records one conversation wrote. |
| **Key-value + forks** | Working state with compare-and-swap, conditional ops, expiry, JSON merge-patch, and advisory leases, plus copy-on-write branches of the read model for speculative work. |
| **Knowledge graph** | Content-addressed nodes and edges, traversal / neighbor / nearest-vector / path reads, bitemporal valid-time edges, source back-links, and a `conversation(id)` filter that narrows a traversal to one conversation. |
| **Governance (RBAC)** | Capability grants over the managed surfaces: `effect feature:action [on resource]` assembled through roles bound to the unforgeable server-stamped user, deny-wins, default-deny. New users receive no managed capabilities unless roles are explicitly bound. `laser.whoami()` + the role/binding/history verbs, including revision-guarded role binding. Orthogonal to Iggy's own permissions, enforced server-side at the edge. |

**Agent fabric** (opt in with the `agent` feature):

| Primitive | What you get |
| --- | --- |
| **Reliable runtime** | A consumer with dedup, retry, and dead-letter, request/reply correlation, conversation and causality tracking, routing, sessions, and context assembly. |
| **Agentic memory** | One durable model: `remember` / `recall` / `improve` / `forget` publish to a memory topic (the versioned audit) that materializes to a versioned key-value read view and recalls by recency. The topic is configurable (`memory_topic(name).stream(..).partitions(n).ttl(d)`). The in-process vector backend and rerank seam add semantic / keyword / hybrid ranking. Consolidation, token-budgeted `to_context_block`, and content-addressed dedup compose above both. Vector memory created from a `Laser` inherits its action governor even though the index itself stays local. A scan over the read view narrows to one conversation with `conversation(id)`, the same lens the query and graph reads carry. |
| **Discovery** | Agents advertise a capability **card** and a live **inbox**, fused into one cached registry with health-aware resolution and reversible operator `quarantine` / `unquarantine`. One connection may advertise one agent. Sensitive routes can require the presence's server-authenticated principal. |
| **Coordination** | `contract` (a directed task with a deadline and a real consumed / completed / timed-out answer), `fan_out` / `scatter` (ask every capable agent, gather under a policy), and `approval_gate` (pause for a human). With signing enabled, terminals fail closed on unsigned or wrongly signed replies and expose the verified principal. |
| **Workflow engine** | `laser.workflow(..).step(..)`: dependency-ordered steps, budgets, verifier panels, saga compensation, crash-recovery replay from a journal, and per-step fenced leases. Use `.exclusive_in(namespace)` when the handler commits an external effect with `kv(namespace).cas_fenced(..)`. `OnTimeout::Reassign` hands a timed-out task to a fresh holder. |
| **Run registry** | `laser.runs()`: submit a run, read its state, list runs (filtered, paged), record a cancel intent. A managed read model folded from the status records a `.registered()` workflow or contract stamps, so "what happened to that task" is one call, and the log stays the truth. |
| **AGDX envelope** | A typed, versioned, fixture-pinned agent message format on the log, with producer verbs, resumable token streams, and deterministic reassembly. ([notes](docs/agdx.md)) |
| **Action governance** | A pre-effect policy hook (`ActionGovernor`) over everything an agent publishes: allow, observe, block, step-up, modify, or defer each send, typed or raw topic publish, AGDX verb, and memory write before it runs. Enforce or shadow mode records every non-allow decision as digest-chained evidence. `QuorumGovernor` runs named governors concurrently under `All` / `Any` / `AtLeast(n)`. Every mandatory voter must affirm, invalid configurations and mandatory errors block, and conflicting body replacements block. `SwappableGovernor` changes the active policy without reconnecting. Defense in depth above server-owned RBAC. |
| **Durable intent** | SDK-level typed records for asynchronous effect approval, not an AGDX wire extension. Fallible `Intent::builder().build()` validates the frozen voter set, threshold, deadline, and body digest. Fallible `Vote::cast` binds an eligible voter to that digest and policy version. `decide` ignores invalid, early, late, and future ballots, then returns a canonical commit or abort. Mandatory voters must allow, conflicting repeats abort, and `Decision::authorizes` verifies the exact intent before an effect runs. Voter identity is trusted only under a signed-principal or topology-isolated deployment profile. |
| **Swarm activity** | A supervisor's replay-safe read model over governance evidence: `SwarmActivity::observe` deduplicates by decision id, `.agent(name)` reads one agent's counts and deterministic latest decision, and `.agents()` lists every folded agent busiest first. |
| **Crash context** | A recovery tool's one-call bundle over an already-read journal tail, dead-letter capsule, and latest governance decision. `.summarize()` emits a bounded deterministic digest with control characters escaped, so untrusted payloads cannot forge diagnostic lines. It performs no I/O and never invokes a model. |
| **Edge bridges** | A2A, MCP, and AG-UI mapped onto AGDX over the durable log, no SSE. ([interop](docs/interop.md)) |

The agent fabric is the part most systems bolt on as a separate service. Here, routing, contracts, fan-out, and workflows are **conventions over the log**, thin client-side state machines over offsets, deadlines, leases, and replies. There is no orchestration server in the path. The substrate stays a log, which means your agents inherit its durability, replay, and ordering for free.

## Why it is good to build on

- **One connection, one mental model.** Everything is records on a log. Publish, query, KV, forks, graph, and coordination share the same connection and the same provenance, so there is nothing to wire together and nothing to keep consistent.
- **Replayable by construction.** Every read model rebuilds from offset 0. A bad projection, a new index, a fresh agent joining late: all just replay the log.
- **Typed end to end.** Serde in, codec stamped on the wire, decoded back to your struct. One typed handle per topic when you want the contract pinned: `laser.stream("commerce").topic("orders").json::<Order>()` publishes and replays `Order` values (a schema-bound form validates against the registered writer schema before a byte leaves the process), and a record that stops decoding surfaces with its exact log position. Batched both directions, so throughput is a flag, not a rewrite.
- **Open core, no lock-in.** Publish, consume, the agent runtime, memory, and all coordination run on Apache Iggy. The managed surfaces light up against LaserData Cloud or Laser Stack through capability negotiation, with the exact same code.
- **Fenced effects when it matters.** An `.exclusive_in(namespace)` step and the handler's `kv(namespace).cas_fenced(..)` commit share one monotonic fence sequence, so reassignment prevents a zombie worker from committing through the protected state boundary.
- **Three conforming clients.** Rust owns the reference behavior, Python binds that core, and native TypeScript consumes the same byte-pinned fixtures and shared BDD scenarios.

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

Connections to `*.laserdata.cloud` and `*.laserdata.com` automatically use the public LaserData CA bundled with the SDK. `LASER_TLS_CERT=<path>` overrides that CA, and `LASER_NO_TLS=1` disables automatic TLS setup. Other hosts keep the TLS settings from their connection string.

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
| [`examples`](examples/README.md) | eight tiny per-primitive examples (`log`, `query`, `watch`, `kv`, `graph`, `recall`, `context`, `agent`, one per accessor above, step-for-step identical in all three languages), plus nine runnable systems: a focused direct-streaming producer/consumer walkthrough, event analytics, an order book, a firehose load generator, an agentic support desk, an agentic-memory loop, an A2A/MCP/AG-UI interop gateway, the `orchestra` multi-agent orchestrator, and a governance scenario. |

## Benchmarks

Run the maintained native campaign from the repository root:

```sh
just bench
just bench 15 3 8 # seconds per arm, repetitions, parallel lanes
```

- Matched raw Iggy and Laser streaming workloads use TCP VSR and one connection per producer or consumer lane.
- The maintained campaign covers streaming, AGDX, managed surfaces, MCP, startup, and recovery.
- The harness runs in release mode and keeps progress output outside timed regions.
- Results include immutable JSON evidence, HDR histograms, CSV exports, and a standalone HTML report.
- `bench/.env` can select caller-provided native binaries. Otherwise the harness resolves the maintained signed artifacts.

Use `just bench-smoke` to validate the harness and `just bench-full` for the exhaustive matrix. Read [`bench/README.md`](bench/README.md) for workload definitions, result interpretation, and authoritative campaign requirements.

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
