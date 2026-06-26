# Laser SDK

[![crates.io](https://img.shields.io/crates/v/laser-sdk.svg)](https://crates.io/crates/laser-sdk) [![docs.rs](https://docs.rs/laser-sdk/badge.svg)](https://docs.rs/laser-sdk)

An open SDK by [LaserData, Inc.](https://laserdata.com) over [Apache Iggy](https://iggy.apache.org) for streaming, querying, and coordinating data on a durable log. This is the **Rust** SDK, the first and reference implementation. The wire contract is language-neutral and pinned by a cross-language conformance suite, so more Apache Iggy-language SDKs can follow (planned, not yet shipped). Typed publish and consume are the foundation. Declared projections turn the log into a queryable read model with a filter, aggregate, and vector query DSL, and a key-value store and copy-on-write forks hold working state and speculative branches. An optional agent runtime and the Agent Data Exchange Protocol (AGDX) layer on top. The same code runs against open Apache Iggy and the managed LaserData Cloud, which negotiates at connect which surfaces are available.

Laser SDK ships in two layers:

- **generic** (`query` feature), typed publish, declared `Projection` schemas, query DSL with filters / aggregates / vector recall. No agent concepts. Suitable for API observability, analytics, audit logs, market data, IoT.
- **agentic** (`agent` feature, default), reliable consumer + DLQ, conversation/causality spine, request/reply, `Router`, `Memory`, `Agent::builder` handlers, the typed AGDX verbs + envelope-aware consumer, and the edge bridges - A2A (`a2a-bridge`), MCP (`mcp-bridge`), AG-UI state sync + event rendering (`agui`). Built on the generic layer.

The SDK carries `gen_ai.*` provenance describing model calls but never makes them. It moves and coordinates messages only.

The wire contract underneath (CBOR envelopes, the query IR, the agent envelope, header and topic dictionaries, caps, the golden fixture corpus) is its own runtime-free, wasm-portable crate, [`laser-wire`](https://crates.io/crates/laser-wire), re-exported whole as `laser_sdk::wire` and under the historical module paths, so existing imports keep working.

## Install

```toml
[dependencies]
laser-sdk = { version = "0.0.1-rc.4", default-features = false, features = ["query"] }   # generic substrate
# or with the agent runtime layered on top:
laser-sdk = { version = "0.0.1-rc.4", features = ["query"] }
```

## Quick example

```rust,no_run
use laser_sdk::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct ApiCall {
    endpoint:   String,
    status:     u16,
    latency_ms: u32,
    user_id:    String,
}

# async fn run() -> Result<(), LaserError> {
let laser = Laser::connect_with_stream("iggy://iggy:iggy@127.0.0.1:8090", "api-metrics").await?;

laser.publish("api_calls")
    .json(&ApiCall {
        endpoint:   "/v1/items".into(),
        status:     200,
        latency_ms: 42,
        user_id:    "alice".into(),
    })?
    .send().await?;

let slow: Vec<ApiCall> = laser.query("api_calls")
    .filter_gte("latency_ms", 500)
    .order_desc("latency_ms")
    .limit(20)
    .fetch_typed().await?;
# Ok(()) }
```

## Batch and any payload

A single publish is the simplest call, not the common one. `publish_batch` accumulates records and sends them in one network round-trip, the largest throughput lever the SDK offers, and reads mirror it: a `reader` cursor drains every record that arrived since the last poll in one call. Batching on both sides is what makes the path efficient.

The body is opaque bytes in any format. `json` / `msgpack` and the batch `add_json` / `add_msgpack` are conveniences over `add_payload`, which takes raw bytes the SDK never inspects. Use `raw_bytes(bytes, ContentType::Avro)` for an already-encoded body, or `add_avro` for schema-first encoding, so Avro, Protobuf, a compressed blob, or your own framing all ride unchanged.

```rust,no_run
# use laser_sdk::prelude::*;
# use serde::Serialize;
# #[derive(Serialize)] struct ApiCall { status: u16 }
# async fn run(laser: Laser, window: Vec<ApiCall>) -> Result<(), LaserError> {
let mut batch = laser.publish_batch("api_calls");
for call in &window {
    batch = batch.add_json(call)?;        // or .add_payload(raw_bytes) for any format
}
batch.send().await?;                      // the whole window, one round-trip
# Ok(()) }
```

Schema lives on the projector side, NOT in the producer code above. A `Projection` (which fields are indexed, where to read them from in the payload, whether the body rides alongside the row for retrieval) is declared once through your control workflow:

```rust,no_run
use laser_sdk::prelude::*;

let api_call_v1 = Projection::builder("api.call.v1")
    .name("api.call").version(1)
    .content_type(ContentType::Json)
    .fields(["endpoint", "status", "latency_ms", "user_id"])
    .build();

let binding = ProjectionBinding::builder()
    .source("api-metrics", "api_calls")     // (data stream, topic)
    .allow("api.call.v1")
    .default_projection("api.call.v1")
    .build();
```

In production LaserData Cloud runs the projector for you. For local development the example crate ships a single-process projector you can spawn next to your code.

## Features

- `default = ["agent", "provenance"]`
- `provenance`, wire contract + provenance encoding/decoding
- `agent`, `Laser` facade, reliable consumer, `Agent::builder`, context, memory, state
- `query`, fluent publish/query client for LaserData Cloud, including the `read_your_writes` consistency level and the unified `ResultCode` (via `LaserError::code()`)
- `kv`, managed key-value store client (get/set/delete/scan, optional expiry, and compare-and-swap via `.expect_version`/`.expect_absent().commit()`) over the `AGDX_KV` managed commands, backed by LaserData Cloud's managed point-state store
- `a2a-bridge`, A2A JSON-RPC bridge over the agent topology (message/send + stream, tasks/get + cancel, Agent Card)
- `mcp-bridge`, MCP JSON-RPC bridge (initialize, tools, resources, prompts) mapping tool calls onto AGDX
- `agui`, AG-UI state sync and event rendering over the log

## Documentation

The full progressive tutorial (publish, projections, batches, heterogeneous topics, filters / aggregates / vector recall, codecs, user isolation, agentic runtime, open SDK vs LaserData Cloud) lives in the project repository under `docs/tutorial.md`, linked from the [repository README](https://github.com/laserdata/laser-sdk#readme). API reference is on [docs.rs](https://docs.rs/laser-sdk). The AGDX protocol's home is [agdxprotocol.ai](https://agdxprotocol.ai).

## License

Apache-2.0. Copyright LaserData, Inc.

Apache and Apache Iggy are trademarks of the Apache Software Foundation. Use of these marks does not imply endorsement by the Apache Software Foundation.
