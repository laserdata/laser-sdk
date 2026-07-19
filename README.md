# Laser SDK

**Build agents and data-driven systems on one durable log.** Ultra-low-latency **streaming**, a **query** layer, **key-value** state, copy-on-write **forks**, a **knowledge graph**, and a full **agent fabric** (memory, discovery, contracts, workflows), all over a single [Apache Iggy](https://iggy.apache.org) connection. By [LaserData, Inc.](https://laserdata.com)

**One connection replaces four systems.** The stream you already publish to becomes the store you query, the state you coordinate on, and the fabric your agents discover, route, and reason over. No second database, no cache, no orchestration server, nothing to keep in sync. The **log is the single source of truth**, and every other surface is a read model you can rebuild from offset 0. A support task, say, streams its messages, keeps its working memory, and resolves the dependencies between them, all in one place.

> **Prerelease (`0.0.1-rc.16`).** The wire contract and the public API may still change between release candidates, so pin an exact version.

**Rust** (the reference SDK) and **Python** ([`foreign/python`](foreign/python/README.md), native bindings over the same core). The wire contract is a standalone, language-neutral crate ([`laser-wire`](wire/README.md)) pinned byte-for-byte by a cross-language conformance suite, so more language SDKs follow without drift.

## Quick start

Start Apache Iggy:

```sh
docker run -p 8090:8090 apache/iggy:latest
```

**Rust** (`laser-sdk = "=0.0.1-rc.16"`)

```rust,no_run
use laser_sdk::prelude::*;

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    let laser = Laser::connect_with_stream("iggy:iggy@127.0.0.1:8090", "telemetry").await?;
    let topic = laser.topic("inferences");
    topic.ensure(4).await?;
    topic.publish().json(&serde_json::json!({ "latency_ms": 42 }))?.send().await?;

    let messages = topic.replay()?.poll().await?;
    println!("read {} message(s)", messages.len());
    Ok(())
}
```

**Python** (`pip install laser-sdk`)

```python
import asyncio
import laser_sdk as ls

async def main():
    laser = await ls.Laser.connect("iggy:iggy@127.0.0.1:8090", stream="telemetry")
    topic = laser.topic("inferences")
    await topic.ensure(partitions=4)
    await topic.publish().json({"latency_ms": 42}).send()

    messages = await topic.replay().poll()
    print(f"read {len(messages)} message(s)")

asyncio.run(main())
```

That is the complete open streaming path. Add `managed` for projections, query, KV, forks, graph, and the run registry on LaserData Cloud. Add `agent` for reliable handlers, memory, contracts, and workflows.

## One grammar, every primitive

Every feature is a **primitive** you reach by one accessor on the connected client, and every action is a **verb** on that primitive. One shape, `object.verb(input).await`, across the whole platform:

| Accessor | Primitive | Reach for it to |
| --- | --- | --- |
| `laser.topic(name)` / `laser.stream(name)` | **Log** | publish and consume records, replay by offset, batch |
| `laser.query(index)` | **Views** | filter, aggregate, page, vector-search declared projections |
| `laser.graph(name)` | **Graph** | link entities, traverse, find neighbors and nearest vectors |
| `laser.watch()` | **Change feed** | await a view's advance instead of polling it |
| `laser.kv(namespace)` / `laser.fork(id)` | **State** | point reads and writes, CAS, leases, copy-on-write branches |
| `laser.memory(scope)` | **Memory** | remember, recall (semantic / keyword / hybrid), consolidate |
| `laser.context(id)` | **Context** | append and assemble one conversation's record, and scope its memory to that conversation |
| `laser.agent(id)` / `laser.contract(..)` / `laser.workflow(..)` / `laser.runs()` | **Fabric** | directed asks, deadline contracts, ordered workflows, the run registry |

Learn the pattern once and the whole platform reads the same way. In Rust:

```rust,ignore
use std::time::Duration;

let laser = Laser::connect_with_stream("iggy:iggy@127.0.0.1:8090", "app").await?;

// Log: streams group topics, topics carry your records.
laser.topic("orders").ensure(4).await?;
laser.stream("audit").topic("events").ensure(4).await?;
laser.topic("orders").publish().json(&order)?.send().await?;
laser.stream("audit").topic("events").publish().json(&event)?.send().await?;
let mut replay = laser.topic("orders").replay()?;

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
laser = await ls.Laser.connect("iggy:iggy@127.0.0.1:8090", stream="app")

# Log
await laser.topic("orders").publish().json(order).send()
await laser.stream("audit").topic("events").publish().json(event).send()

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
deps = await ctx.graph("services").neighbors(node, direction="out", depth=2)
run = await laser.runs().submit("refund", task)
```

The accessors are free to construct, IO happens at the terminal verb (`.send()` writes, `.fetch()` reads), and options are fluent. Ordinary services use `topic.producer()`, `topic.consumer(..)`, and `topic.consumer_group(..)`: direct batching and linger, retries, key/partition routing, live `futures::Stream` reads, group creation, polling strategy, replay, and automatic or explicit server offset commits are all Laser APIs. `ConsumerMessage` preserves exact Apache Iggy headers and log positions. The substrate still stays one call away through `topic.iggy_producer()`, `topic.iggy_consumer_group()`, the complete `laser.client()`, and the exact-version `laser_sdk::iggy` re-export for encryption or administration outside the Laser surface.

Apache Iggy's VSR cluster client is available with `features = ["vsr"]`. Laser forwards that switch to `iggy/vsr` and keeps the same streaming and agent APIs. VSR currently covers standard Iggy commands. LaserData's managed query/KV/fork/graph/control surfaces use custom command codes, which upstream's VSR encoder does not yet admit, so a VSR build treats those surfaces as unavailable until that vocabulary lands upstream.

The first managed clustered deployment will use one global leader for every stream and partition, not per-partition leader routing. It is not active yet. Activation also requires server-ng AGDX bridge parity, an authoritative leader and VSR view in cluster metadata, plus typed `NotLeader` reconnect and retry.

**Data platform** (the core, stands on its own):

| Primitive | What you get |
| --- | --- |
| **Publish / consume** | Typed serde values or raw records onto topics, direct producer batching/linger/routing, and live async partition or consumer-group readers with server offsets and configurable commit policies. |
| **Projections + query DSL** | Filters, aggregates, time ranges, pagination, and vector recall over indexes you declare once per topic, with opt-in read-your-writes consistency, and a `conversation(id)` filter that narrows any query to the records one conversation wrote. |
| **Key-value + forks** | Working state with compare-and-swap, conditional ops, expiry, JSON merge-patch, and advisory leases, plus copy-on-write branches of the read model for speculative work. |
| **Knowledge graph** | Content-addressed nodes and edges, traversal / neighbor / nearest-vector / path reads, bitemporal valid-time edges, source back-links, and a `conversation(id)` filter that narrows a traversal to one conversation. |
| **Governance (RBAC)** | Capability grants over the managed surfaces: `effect feature:action [on resource]` assembled through roles bound to the unforgeable server-stamped user, deny-wins, default-deny. New users receive no managed capabilities unless roles are explicitly bound. `laser.whoami()` + the role/binding/history verbs, including revision-guarded role binding. Orthogonal to Iggy's own permissions, enforced fork-native at the edge. |

**Agent fabric** (opt in with the `agent` feature):

| Primitive | What you get |
| --- | --- |
| **Reliable runtime** | A consumer with dedup, retry, and dead-letter, request/reply correlation, conversation and causality tracking, routing, sessions, and context assembly. |
| **Agentic memory** | One model: `remember` / `recall` / `improve` / `forget` publish to a memory topic (the versioned audit) that materializes to a versioned key-value read view. The topic is configurable (`memory_topic(name).stream(..).partitions(n).ttl(d)`), with semantic / keyword / hybrid recall, a rerank seam, a consolidation pass, token-budgeted `to_context_block`, and content-addressed dedup on every built-in backend. In-process vector memory created from a `Laser` inherits its action governor even though the index itself stays local. A scan over the read view narrows to one conversation with `conversation(id)`, the same lens the query and graph reads carry. |
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
- **Typed end to end.** Serde in, codec stamped on the wire, decoded back to your struct. One typed handle per topic when you want the contract pinned: `laser.topic("orders").json::<Order>()` publishes and replays `Order` values (a schema-bound form validates against the registered writer schema before a byte leaves the process), and a record that stops decoding surfaces with its exact log position. Batched both directions, so throughput is a flag, not a rewrite.
- **Open core, no lock-in.** Publish, consume, the agent runtime, memory, and all coordination run on stock Apache Iggy. The managed surfaces light up against LaserData Cloud through capability negotiation, with the exact same code.
- **Fenced effects when it matters.** An `.exclusive_in(namespace)` step and the handler's `kv(namespace).cas_fenced(..)` commit share one monotonic fence sequence, so reassignment prevents a zombie worker from committing through the protected state boundary.
- **Rust and Python in lockstep.** The Python SDK is native bindings over the Rust core against one byte-pinned wire contract, so the two never diverge.

## Open core, managed surface

The **open** surface (publish, consume, the agent runtime, provenance, log-backed memory, AGDX, and all coordination) runs on raw Apache Iggy. The **managed** surface (query, projections, KV, forks, the knowledge graph, durable dedup, and the fenced leases behind `.exclusive()` / `.exclusive_in(..)`) needs [LaserData Cloud](https://laserdata.com) and returns `LaserError::Unsupported` on raw Iggy. The same code runs in both, and capability negotiation at connect decides what is available. The SDK never hides the Iggy client (`topic.iggy_producer()`, `topic.iggy_consumer(..)`, `laser.client()`).

Access is governed in two layers. Iggy's native RBAC grants global, stream, and topic permissions, so it decides whether a credential can create streams, send records, poll topics, and see a stream at all. LaserData's governance RBAC grants managed-surface capabilities, so it decides whether the same server-stamped user can call query, KV, graph, projection, fork, agent, workflow, and `authz` operations. The layers are independent: creating a user does not bind any governance role, and an operator must explicitly bind roles to grant managed access. `LaserError::is_permission_denied()` and `is_stream_or_topic_not_found()` classify native permission misses, and managed authorization failures return the unified unauthorized result.

`Laser::connect` and `connect_with_stream` recognize a `*.laserdata.cloud` or `*.laserdata.com` host in the connection string and auto-attach TLS with LaserData's public root CA, embedded in the SDK itself so a bare connection string is enough (no cert file to fetch or ship). `LASER_TLS_CERT=<path>` overrides it with any CA file (a rotated cert included), `LASER_NO_TLS=1` disables the auto-attach, and any other host is left untouched: bring your own `tls=true&tls_ca_file=<path>` for a self-hosted deployment.

## Documentation

- [Tutorial](docs/tutorial.md): a progressive guide from one message to projections, queries, vector recall, codecs, multi-stream topologies, and the agent fabric.
- [Building agents](docs/building-agents.md): a recipe guide that works one multi-agent scenario end to end, including governed agents, managed-surface RBAC, and concrete SDK calls.
- [AGDX notes](docs/agdx.md): an in-repo development reference for the Agent Data Exchange Protocol the SDK implements (the envelope, the Apache Iggy binding, the surfaces). The protocol home is [agdxprotocol.ai](https://agdxprotocol.ai).
- [Interop](docs/interop.md): the A2A / MCP / AG-UI edge bridges over AGDX.
- [Examples](examples/rust/README.md): complete runnable systems, each with its own README, runnable locally or against LaserData Cloud unchanged.
- [`wire/README.md`](wire/README.md): the contract crate and its compatibility rules.

## Workspace

| Crate | What it is |
| --- | --- |
| [`laser-wire`](wire/README.md) (`wire/`) | the wire contract: codes, envelopes, query IR, dictionaries, caps, the AGDX envelope, and the golden fixture corpus. Runtime-free and wasm-portable. |
| [`laser-sdk`](sdk/README.md) (`sdk/`) | the client and agent runtime, re-exporting the wire crate as `laser_sdk::wire`. |
| [`foreign/python`](foreign/python/README.md) | the Python SDK, PyO3 bindings over the Rust crate. |
| [`examples/rust`](examples/rust/README.md) | ten runnable systems: a focused direct-streaming producer/consumer walkthrough, event analytics, an order book, a firehose load generator, an agentic support desk, an agentic-memory loop, a memory benchmark harness, an A2A/MCP/AG-UI interop gateway, the `orchestra` multi-agent orchestrator, and a governance scenario. |

## Development

```sh
just lint    # fmt + sort + machete + clippy -D warnings
just test    # workspace unit tests
just test-it # integration tests against an Iggy testcontainer (needs Docker)
just bdd     # cross-SDK BDD conformance (needs Docker)
just ci      # the full gate (lint, test, wasm, deny, advisories, fixtures)
```

Build profiles: the default is typed streaming plus provenance, `--no-default-features --features streaming` selects streaming alone, `--features agent` adds the agent fabric, `--features managed` adds every managed platform surface, and `--all-features` compiles every additive feature including VSR. Because VSR and classic Iggy framing are compile-time alternatives, `just test-it` runs the classic real-server integration suite separately, and the all-feature unit/build gates still compile VSR.

## Delivery model

At-least-once with idempotent operations, per-conversation (per-partition) ordering, and replay-friendly throughout. Materialized indexes can rebuild from explicit source offsets and snapshots instead of making full replay a hot-path default.

## License

Apache-2.0. Copyright LaserData, Inc. Apache and Apache Iggy are trademarks of the Apache Software Foundation, and use does not imply endorsement.
