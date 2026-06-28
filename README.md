# Laser SDK

> **Status: edge / prerelease (`0.0.1-rc.5`).** Pre-1.0 and moving fast. The wire contract, the AGDX spec, and the public API may change in any release, with no stability guarantee yet. Pin an exact version and expect breaking changes.

An open data platform by [LaserData, Inc.](https://laserdata.com) over [Apache Iggy](https://iggy.apache.org) for streaming, querying, and coordinating data on a durable log. Ultra-low-latency streaming is the foundation, and on top of it sit declared projections with a query DSL, a key-value store, and copy-on-write forks, all on one connection. The log is the source of truth and every other surface is a read model on it. A reliable agent runtime and the Agent Data Exchange Protocol (AGDX) layer come on top when you build agents, but they are an optional extension. The streaming and data core stands on its own.

This repository ships the **Rust** SDK, the first and reference implementation, and a **Python** SDK ([`foreign/python`](foreign/python/README.md)) built as native bindings over it. The wire contract is a standalone, language-neutral crate ([`laser-wire`](wire/README.md)) pinned byte-for-byte by a cross-language conformance suite, so SDKs for the other Apache Iggy languages can follow under `foreign/`. The rest are planned, not yet shipped.

The log is the source of truth. Messages are appended once and stay replayable from offset 0. Queries, projections, KV, and agent coordination are all read models built on top of it, never a second system to keep in sync.

## What it does

The data platform:

- **Typed publish and consume**: serde values onto Iggy topics in one call, JSON/MessagePack/CBOR/BSON, schema-first Avro/Protobuf, or any raw bytes the SDK never inspects, batched in one network round-trip on both the send and poll sides.
- **Declared projections and a query DSL**: filters, aggregates, time ranges, pagination, and vector recall over materialized indexes, declared once per topic like a database index, with an opt-in read-your-writes consistency level for reads that must see their own prior writes.
- **A key-value store and copy-on-write forks** of the materialized read model, for working state and speculative branches. The store offers compare-and-swap for lock-free optimistic concurrency, conditional reads and writes, expiry, JSON merge-patch, and advisory leases on a shared key.
- **A knowledge graph** as a managed read model: content-addressed nodes and edges with traversal, neighbor, nearest-vector, and whole-path reads, the relationship layer the agentic memory composes over. Edges carry an optional valid-time window for bitemporal facts, so a changed relationship is superseded without being destroyed, and an "as of" read traverses the graph as it was at any past instant.

The optional agent layer (opt in only when you build agents):

- **A reliable agent runtime**: consumer with dedup, retries, and dead-lettering, request/reply correlation, conversation and causality tracking, routing, sessions, context assembly, and an agentic memory facade (`remember` / `recall` / `improve` / `forget`) over a backend you pick: the append-only log, the key-value store, an in-process vector index, or the query and graph surfaces. Memory classes follow the episodic / semantic / procedural taxonomy (including reusable skills), recall fuses semantic, keyword, and feedback signals with an optional rerank seam, and an asynchronous consolidation seam ("memify") summarizes, reweights, and prunes over the log.
- **The Agent Data Exchange Protocol (AGDX)**: a typed, versioned, fixture-pinned envelope for agent messaging on the log, with typed producer verbs, token streams that resume from offsets, and deterministic reassembly. Specified in the [AGDX spec](docs/agdx.md).
- **Edge interoperability**: optional A2A, MCP, and AG-UI bridges that map the edge standards onto AGDX and ride the durable log (no SSE), so one internal agent is reachable as an A2A agent, an MCP tool server, or an AG-UI event stream. See [docs/interop.md](docs/interop.md).

## Open core, managed surface

The open surface (publish, consume, the agent runtime, provenance, log-backed memory, AGDX) runs against raw Apache Iggy. The managed surface (query, projections, KV, forks, the knowledge graph, durable dedup) works against LaserData Cloud and returns `LaserError::Unsupported` against raw Apache Iggy. The same code runs in both cases. Capability negotiation at connect decides what is available. Agentic memory is not a separate managed surface: it runs over the log (open) or the key-value, query, and graph surfaces (managed), so each backend lights up with the surface it rides. `Laser::memory` picks one by capability, and `memory_with` forces the choice.

The SDK is built on the Apache Iggy SDK and never hides it: `laser.iggy_producer`, `laser.iggy_consumer`, and `laser.client()` expose the full client when you want it directly.

## Quick start

```toml
[dependencies]
laser-sdk = { version = "0.0.1-rc.5", features = ["query"] }
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```sh
docker run -p 8090:8090 apache/iggy:latest
```

```rust
use laser_sdk::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Inference {
    model: String,
    latency_ms: u32,
    user_id: String,
}

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    let laser = Laser::connect_with_stream("iggy:iggy@127.0.0.1:8090", "telemetry").await?;
    laser
        .publish("inferences")
        .json(&Inference {
            model: "gpt-4o".into(),
            latency_ms: 420,
            user_id: "alice".into(),
        })?
        .send()
        .await?;
    Ok(())
}
```

### Batch and any payload

The single publish above is the simplest call, not the common one. `publish_batch` accumulates records and ships them in one network round-trip, the largest throughput lever the SDK gives you, and consuming mirrors it: a `reader` cursor drains every record that arrived since the last poll in one call. Batch both sides and the per-message overhead is gone.

The payload is yours, in any format. `json` / `msgpack` (and the batch `add_json` / `add_msgpack`) are conveniences over `add_payload`, which takes raw bytes the SDK never inspects, so Avro, Protobuf, a compressed blob, or your own framing ride unchanged.

```rust
let mut batch = laser.publish_batch("inferences");
for inference in &window {
    batch = batch.add_json(inference)?;   // or .add_payload(raw_bytes) for any format
}
batch.send().await?;                      // the whole window, one round-trip
```

From here the [tutorial](docs/tutorial.md) takes over: nine chapters building one observability pipeline from a single message to projections, queries, aggregates, vector recall, codecs, multi-stream topologies, and the agent runtime.

## Workspace

| Crate | What it is |
| --- | --- |
| [`laser-wire`](wire/README.md) (`wire/`) | the wire contract: codes, envelopes, the query IR, dictionaries, caps, the AGDX envelope, the golden fixture corpus. Runtime-free, wasm-portable |
| [`laser-sdk`](sdk/README.md) (`sdk/`) | the client and agent runtime on top, re-exporting the wire crate as `laser_sdk::wire` |
| [`examples/rust`](examples/rust/README.md) | six runnable systems: event analytics, an order book, a firehose load generator, an agentic support desk, an agentic-memory recall loop, and an A2A/MCP/AG-UI interop gateway, plus the cloud connection setup |

## Documentation

- [Tutorial](docs/tutorial.md): the progressive nine-chapter guide.
- [AGDX spec](docs/agdx.md): the authoritative Agent Data Exchange Protocol specification - the substrate-neutral core, the normative Apache Iggy binding, the agent envelope, the design rationale, and the roadmap. (An HTTP surface for UIs and an illustrative Kafka binding live inside it too.) The protocol's home is [agdxprotocol.ai](https://agdxprotocol.ai).
- [docs/interop.md](docs/interop.md): the A2A / MCP / AG-UI edge-interoperability guide (the bridges over AGDX).
- [Examples](examples/rust/README.md): each example is a complete system with its own README, runnable locally or against LaserData Cloud with no code change.
- [wire/README.md](wire/README.md): the contracts crate, its features, and its compatibility rules.

## Development

```sh
just lint           # fmt + sort + machete + clippy -D warnings
just test           # workspace unit tests
just test-it        # integration tests against an Iggy testcontainer (needs Docker)
just bdd            # cross-SDK BDD conformance scenarios, Rust runner (needs Docker)
just wasm           # laser-wire on wasm32-unknown-unknown
just deny-wire      # laser-wire dependency bans
just advisories     # workspace vulnerability / unmaintained-crate advisories
just fuzz           # fuzz the wire decode surface (nightly + cargo-fuzz)
just fixtures-regen # regenerate the golden corpus after an intentional wire change
just ci             # all of the above
```

Integration tests share one Iggy testcontainer and isolate each test on its own stream. The wasm and deny gates hold the wire crate to its portability guarantee. The [`bdd/`](bdd/README.md) harness is the cross-language conformance contract: one set of Gherkin scenarios every language SDK must pass, alongside the wire fixture corpus.

```sh
cargo build --no-default-features --features query   # generic substrate only
cargo build --features query                         # agent runtime + query
cargo build --all-features                           # everything
```

## Delivery model

At-least-once delivery with idempotent operations, per-conversation (per-partition) ordering, replay-friendly. The materialized index can always be rebuilt from offset 0.

## License

Apache-2.0. Copyright LaserData, Inc.

## Trademarks

Apache and Apache Iggy are trademarks of the Apache Software Foundation. Use of these marks does not imply endorsement by the Apache Software Foundation.
