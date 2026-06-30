# Laser SDK

**One durable log. A whole data platform and a multi-agent fabric on top of it.** Streaming, queries, key-value, copy-on-write forks, a knowledge graph, and full agent coordination, all over a single [Apache Iggy](https://iggy.apache.org) connection. No second database to run, no orchestration server to operate, no read store to keep in sync. The log is the source of truth and every other surface is a read model you can rebuild from offset 0. By [LaserData, Inc.](https://laserdata.com)

That is the whole pitch: the thing you already stream through becomes the thing you query, the place you keep working state, and the substrate your agents discover, route, and coordinate over. One connection does the job a message bus, a database, a cache, and an orchestrator usually take four systems to do.

> **Prerelease (`0.0.1-rc.7`).** Pre-1.0 and moving fast. The wire contract, the AGDX notes, and the public API may break in any release. Pin an exact version.

**Rust** (the reference SDK) and **Python** ([`foreign/python`](foreign/python/README.md), native bindings over the same core). The wire contract is a standalone, language-neutral crate ([`laser-wire`](wire/README.md)) pinned byte-for-byte by a cross-language conformance suite, so more language SDKs follow without drift.

## Primitives at a glance

**Data platform** (the core, stands on its own):

| Primitive | What you get |
| --- | --- |
| **Publish / consume** | Typed serde values onto topics in one call (JSON, MessagePack, CBOR, BSON, Avro, Protobuf, or raw bytes), batched into one round-trip both sending and polling. |
| **Projections + query DSL** | Filters, aggregates, time ranges, pagination, and vector recall over indexes you declare once per topic, with opt-in read-your-writes consistency. |
| **Key-value + forks** | Working state with compare-and-swap, conditional ops, expiry, JSON merge-patch, and advisory leases, plus copy-on-write branches of the read model for speculative work. |
| **Knowledge graph** | Content-addressed nodes and edges, traversal / neighbor / nearest-vector / path reads, bitemporal valid-time edges, and source back-links. |

**Agent fabric** (opt in with the `agent` feature):

| Primitive | What you get |
| --- | --- |
| **Reliable runtime** | A consumer with dedup, retry, and dead-letter, request/reply correlation, conversation and causality tracking, routing, sessions, and context assembly. |
| **Agentic memory** | `remember` / `recall` / `improve` / `forget` over the log, KV, an in-process vector index, or query and graph. Semantic, keyword, and feedback recall, a rerank seam, a consolidation pass, and token-budgeted `to_context_block`. |
| **Discovery** | Agents advertise a capability **card** and a live **inbox**, fused into one cached registry with health-aware resolution and reversible operator `quarantine` / `unquarantine` (optionally signed and verified). |
| **Coordination** | `contract` (a directed task with a deadline and a real consumed / completed / timed-out answer), `fan_out` / `scatter` (ask every capable agent, gather under a policy), and `approval_gate` (pause for a human). |
| **Workflow engine** | `laser.workflow(..).step(..)`: dependency-ordered steps, budgets, verifier panels, saga compensation, crash-recovery replay from a journal, and a per-step `.exclusive()` for an at-most-once fenced effect, with `OnTimeout::Reassign` to hand a timed-out task to a fresh holder. |
| **AGDX envelope** | A typed, versioned, fixture-pinned agent message format on the log, with producer verbs, resumable token streams, and deterministic reassembly. ([notes](docs/agdx.md)) |
| **Edge bridges** | A2A, MCP, and AG-UI mapped onto AGDX over the durable log, no SSE. ([interop](docs/interop.md)) |

The agent fabric is the part most systems bolt on as a separate service. Here, routing, contracts, fan-out, and workflows are **conventions over the log**, thin client-side state machines over offsets, deadlines, leases, and replies. There is no orchestration server in the path. The substrate stays a log, which means your agents inherit its durability, replay, and ordering for free.

## Why it is good to build on

- **One connection, one mental model.** Everything is records on a log. Publish, query, KV, forks, graph, and coordination share the same connection and the same provenance, so there is nothing to wire together and nothing to keep consistent.
- **Replayable by construction.** Every read model rebuilds from offset 0. A bad projection, a new index, a fresh agent joining late: all just replay the log.
- **Typed end to end.** Serde in, codec stamped on the wire, decoded back to your struct. Batched both directions, so throughput is a flag, not a rewrite.
- **Open core, no lock-in.** Publish, consume, the agent runtime, memory, and all coordination run on stock Apache Iggy. The managed surfaces light up against LaserData Cloud through capability negotiation, with the exact same code.
- **At-most-once when it matters.** The `.exclusive()` fenced step gives a single-holder guarantee for an external effect, with reassignment on timeout, so a zombie worker cannot double-execute.
- **Rust and Python in lockstep.** The Python SDK is native bindings over the Rust core against one byte-pinned wire contract, so the two never diverge.

## Quick start

Run a server, then publish:

```sh
docker run -p 8090:8090 apache/iggy:latest
```

**Rust**

```toml
laser-sdk = { version = "0.0.1-rc.7", features = ["query"] }
```

```rust
use laser_sdk::prelude::*;

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    let laser = Laser::connect_with_stream("iggy:iggy@127.0.0.1:8090", "telemetry").await?;
    laser.publish("inferences").json(&serde_json::json!({ "model": "gpt-4o", "latency_ms": 420 }))?.send().await?;
    Ok(())
}
```

**Python** (`pip install laser-sdk`)

```python
import asyncio, laser_sdk as ls

async def main():
    laser = await ls.Laser.connect("iggy:iggy@127.0.0.1:8090", stream="telemetry")
    await laser.publish("inferences").json({"model": "gpt-4o", "latency_ms": 420}).send()

asyncio.run(main())
```

**Batch** is the throughput lever: `publish_batch` accumulates records and ships them in one round-trip, and a `reader` cursor drains everything new in one poll. `json` / `msgpack` are conveniences over `add_payload`, which takes raw bytes the SDK never inspects.

**An orchestrator over capability agents** (the `orchestra` example, Rust and Python, an interactive walk-through you can watch live in the console):

```rust
// Spawn diagnose agents that advertise a card. The orchestrator never hard-codes who can do what.
let reply = laser
    .contract(Router::to_capable("diagnose", RoutePolicy::Any))  // one capable agent
    .from("orchestrator".parse()?)
    .payload(b"checkout API latency spike")
    .deadline(Duration::from_secs(10))
    .send().await?;                                              // Completed / NotConsumed / TimedOut

laser.quarantine("operator".parse()?, &"bad-agent".parse()?).await?;    // pull a misbehaving agent
laser.unquarantine("operator".parse()?, &"bad-agent".parse()?).await?;  // and let it back in
```

## Open core, managed surface

The **open** surface (publish, consume, the agent runtime, provenance, log-backed memory, AGDX, and all coordination) runs on raw Apache Iggy. The **managed** surface (query, projections, KV, forks, the knowledge graph, durable dedup, and the fenced lease behind `.exclusive()`) needs [LaserData Cloud](https://laserdata.com) and returns `LaserError::Unsupported` on raw Iggy. The same code runs in both, and capability negotiation at connect decides what is available. The SDK never hides the Iggy client (`laser.iggy_producer`, `iggy_consumer`, `client()`).

## Documentation

- [Tutorial](docs/tutorial.md): a progressive guide from one message to projections, queries, vector recall, codecs, multi-stream topologies, and the agent fabric.
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
| [`examples/rust`](examples/rust/README.md) | seven runnable systems: event analytics, an order book, a firehose load generator, an agentic support desk, an agentic-memory loop, an A2A/MCP/AG-UI interop gateway, and the `orchestra` multi-agent orchestrator. |

## Development

```sh
just lint    # fmt + sort + machete + clippy -D warnings
just test    # workspace unit tests
just test-it # integration tests against an Iggy testcontainer (needs Docker)
just bdd     # cross-SDK BDD conformance (needs Docker)
just ci      # the full gate (lint, test, wasm, deny, advisories, fixtures)
```

Build profiles: `--no-default-features --features query` (substrate only), `--features query` (runtime + query), `--all-features` (everything).

## Delivery model

At-least-once with idempotent operations, per-conversation (per-partition) ordering, and replay-friendly throughout. The materialized index rebuilds from offset 0.

## License

Apache-2.0. Copyright LaserData, Inc. Apache and Apache Iggy are trademarks of the Apache Software Foundation, and use does not imply endorsement.
