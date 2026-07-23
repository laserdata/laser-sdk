# Laser SDK

[![crates.io](https://img.shields.io/crates/v/laser-sdk.svg)](https://crates.io/crates/laser-sdk) [![docs.rs](https://docs.rs/laser-sdk/badge.svg)](https://docs.rs/laser-sdk)

An open SDK by [LaserData, Inc.](https://laserdata.com) over [Apache Iggy](https://iggy.apache.org) for streaming, querying, and coordinating data on a durable log. This is the Rust reference implementation. Python binds this crate and TypeScript implements the same contract natively. All three consume the shared fixture and BDD corpus. Typed publish and consume are the foundation. Declared projections, query, key-value state, forks, and the optional AGDX agent runtime build on that log.

Laser SDK ships in independently adoptable layers:

- **streaming** (`streaming` feature, default), streams, topics, raw and typed publish, batches, resumable cursors, and JSON/CBOR/MessagePack codecs on Apache Iggy.
- **managed platform** (`managed` feature), projections, query, key-value state, forks, graph, watch, and the run registry against LaserData Cloud.
- **agentic** (`agent` feature), reliable consumer + DLQ, conversation and causality, request/reply, routing, memory, handlers, typed AGDX verbs, workflows, effect governance, and durable intent records.
- **edges**, the optional A2A, MCP, and AG-UI adapters.

The SDK carries `gen_ai.*` provenance describing model calls but never makes them. It moves and coordinates messages only.

The wire contract underneath (CBOR envelopes, the query IR, the agent envelope, header and topic dictionaries, caps, the golden fixture corpus) is its own runtime-free, wasm-portable crate, [`laser-wire`](https://crates.io/crates/laser-wire), re-exported whole as `laser_sdk::wire` and under the historical module paths, so existing imports keep working.

## Install

```toml
[dependencies]
laser-sdk = "=0.0.1-rc.18" # typed streaming plus provenance
# Add only the layers the application uses:
laser-sdk = { version = "=0.0.1-rc.18", features = ["agent", "managed"] }
```

## Quick example

```rust
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
let laser = Laser::connect("iggy:iggy@127.0.0.1:8090").await?;
let api_calls = laser
    .stream("api-metrics")
    .topic("api_calls")
    .json::<ApiCall>();
api_calls.topic().ensure(4).await?;

api_calls.publish(&ApiCall {
        endpoint:   "/v1/items".into(),
        status:     200,
        latency_ms: 42,
        user_id:    "alice".into(),
    })?.send().await?;

let mut records = api_calls.records("latency-dashboard")?; // your reader's name, offsets stay caller-owned
while let Some(record) = records.next().await {
    let call: ApiCall = record?.value;
    println!("{} returned {}", call.endpoint, call.status);
}
# Ok(()) }
```

This example uses only the default `streaming` and `provenance` features and runs against any Apache Iggy. One connection addresses every stream on the server. `Laser::connect_with_stream` only pins a default stream so `laser.topic(name)` can be used as a shortcut. It does not limit the connection to that stream. `Laser::connect_env()` reads `LASER_CONNECTION_STRING` and the optional `LASER_STREAM`, and `Laser::local()` targets the stock local container.

Apache Iggy TCP reconnection is enabled for both the initial handshake and a later dropped socket, with unlimited retries at one-second intervals by default. Tune it through `reconnection_retries=<count|unlimited>` and `reconnection_interval=<duration>` in the connection string. The client reapplies connection-string credentials after reconnecting, so a server restart does not leave the socket unauthenticated.

Pointed at a `*.laserdata.cloud` or `*.laserdata.com` host, `connect`/`connect_with_stream` auto-attach TLS with LaserData's public root CA, bundled in the SDK so a bare connection string is enough. `LASER_TLS_CERT=<path>` overrides it, `LASER_NO_TLS=1` disables it, and every other host is left untouched.

## Batch and any payload

A single publish is the simplest call, not the common one. `publish_batch` accumulates typed records into one network round-trip. For a continuously running service, `topic.producer()` adds direct batching, linger, retries, and routing. `.background(BackgroundConfig::builder()..)` switches to Apache Iggy's buffered, sharded async sends instead, call `Producer::shutdown()` before dropping it or unflushed messages are lost. `topic.consumer_group()` is a live async stream with server-managed offsets. A replay cursor remains the bounded, caller-checkpointed reader. Batching on both live and bounded paths is what makes the path efficient.

The body is opaque bytes in any format. `json` / `msgpack` and the batch `add_json` / `add_msgpack` are conveniences over `add_payload`, which takes raw bytes the SDK never inspects. Use `raw_bytes(bytes, ContentType::Avro)` for an already-encoded body, or `add_avro` for schema-first encoding, so Avro, Protobuf, a compressed blob, or your own framing all ride unchanged.

```rust
# use laser_sdk::prelude::*;
# use serde::Serialize;
# #[derive(Serialize)] struct ApiCall { status: u16 }
# async fn run(laser: Laser, window: Vec<ApiCall>) -> Result<(), LaserError> {
let api_calls = laser.stream("api-metrics").topic("api_calls");
let mut batch = api_calls.publish_batch();
for call in &window {
    batch = batch.add_json(call)?;        // or .add_payload(raw_bytes) for any format
}
batch.send().await?;                      // the whole window, one round-trip
# Ok(()) }
```

Schema lives on the projector side, NOT in the producer code above. A `Projection` (which fields are indexed, where to read them from in the payload, whether the body rides alongside the row for retrieval) is declared once through your control workflow:

```rust
use laser_sdk::query::{Projection, ProjectionBinding};
use laser_sdk::stream::ContentType;

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

LaserData Cloud runs the projector. Against raw Apache Iggy, projection and query calls return `LaserError::Unsupported`. The open streaming path above continues to run unchanged.

## Typed topics

One handle binds a topic to one body type. The serde forms need no registry, encoding on the way in and decoding with the log position attached on the way out.

```rust
# use laser_sdk::prelude::*;
# use serde::{Deserialize, Serialize};
# #[derive(Serialize, Deserialize)] struct Order { customer: String, amount: i64 }
# async fn run(laser: Laser, order: Order) -> Result<(), LaserError> {
let orders = laser.stream("commerce").topic("orders").json::<Order>(); // or .cbor::<Order>()
orders.publish(&order)?.send().await?;              // stamps agdx.ct, builder verbs still chain

let mut records = orders.records("billing")?;       // named cursor, not consumer-group delivery
while let Some(next) = records.next().await {
    let order: Order = next?.value;
}
# Ok(()) }
```

`laser.stream("commerce").topic("orders").schema::<Order>(id).await?` is the schema-bound form (feature `schema-codecs`, the registry lives on LaserData Cloud): it resolves and compiles the registered writer schema once per handle, validates every body client-side, and stamps `agdx.ct` + `agdx.sid`, so a body that stops matching fails at the producer instead of downstream in the projector. A record that does not decode never wedges a reader: `records` yields `TypedDecodeError { position, source }` naming the exact log position and keeps going, and the reliable path composes instead of duplicating (decode inside an `Agent` handler, undecodable records ride the existing dead-letter policy).

## Durable approval records

With the `agent` feature, effects that need asynchronous approval use ordinary typed topics. The SDK validates the intent and ballots, but your application owns the topic layout, replay cursor, and final effect:

```rust
use laser_sdk::intent::{decide, Intent, IntentPolicy, Vote, VoteChoice};
use laser_sdk::prelude::*;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

# async fn run(laser: Laser) -> Result<(), Box<dyn std::error::Error>> {
let deadline = (SystemTime::now() + Duration::from_secs(30))
    .duration_since(UNIX_EPOCH)?.as_micros() as u64;
let intent = Intent::builder()
    .conversation(ConversationId::new())
    .proposer("planner".parse()?)
    .body(b"reserve inventory".to_vec())
    .eligible_voters(vec!["safety".parse()?])
    .policy(IntentPolicy::All)
    .policy_version(7)
    .deadline_micros(deadline)
    .build()?;

laser.stream("governance").topic("intents").json::<Intent>().publish(&intent)?.send().await?;
let vote = Vote::cast(&intent, "safety".parse()?, VoteChoice::Allow)?;
let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros() as u64;
if let Some(decision) = decide(&intent, &[vote], now)? {
    if decision.authorizes(&intent)? {
        // Apply the idempotent, fenced effect, then persist the decision.
    }
}
# Ok(()) }
```

`Intent`, `Vote`, and `Decision` are SDK record conventions, not AGDX envelope types. Invalid configuration fails before publish, and deserialized intents are validated again before voting or folding. A voter name is still a record claim. Use signed principals or topic ACL isolation when authorship must be trusted.

## Addressing

Every primitive is an accessor on the connected client. The accessor is free and synchronous, IO happens at the terminal verb.

Identity and delivery topology are separate types: `AgentId` is the logical protocol identity used for routing and attribution, `ConsumerGroupName` selects which Apache Iggy replicas share work, and `PrincipalId` is the authenticated server identity used at trust and RBAC boundaries. `Agent::builder` derives the default group spelling from the agent id, and `.consumer_group(..)` overrides deployment topology without changing logical identity.

Live presence is connection-scoped, so one connection may advertise one agent. A second agent receives `LaserError::PresenceConflict` instead of overwriting the first. Claim-based capability routing remains available, while `Router::to_principal(..)` and `CapabilitySelector::principal(..)` require the selected live presence to match a server-authenticated `PrincipalId` and fail with `RoutePrincipalMismatch`. With a verifier enrolled, contract terminals accept only a valid signature from the route identity. `AgentMessage::verified_principal` records who actually answered, including every `ScatterReport` branch.

| Accessor | Scope | Serves |
| --- | --- | --- |
| `laser.stream(name).topic(name)` | any topic on any stream | the same verbs, explicitly addressed (streams are first-class and dynamic on Apache Iggy) |
| `laser.topic(name)` | a topic on the optional default stream | shorthand for the same verbs |
| `laser.query(index)` | a materialized index | filters, aggregates, vector recall, the bounded `.max_rows(n).rows()` walk |
| `laser.watch()` | the change feed | await a view's advance instead of polling it blind |
| `laser.kv(namespace)` | managed point state | get/set/delete/scan, compare-and-swap, leases |
| `laser.fork(id)` | a copy-on-write branch | speculative writes, overlay queries, promote or squash |
| `laser.graph(name)` | the knowledge graph | traversal, neighbors, upsert, link/unlink |
| `laser.memory(scope)` | agentic memory | remember / recall / improve / forget |
| `laser.context(conversation)` | one conversation's working record | append, bounded fetch, prompt block, state folds |
| `laser.agent(id)` / `laser.contract(..)` / `laser.workflow(name)` / `laser.runs()` | the fabric | directed asks, deadline-bound contracts, dependency-ordered workflows, the run registry |

One connection addresses every stream. `connect_with_stream` optionally pins a default stream and `laser.topic(name)` is shorthand against it. Without a default stream, that shortcut returns the typed `NoStream` error. Iggy RBAC is enforced at the stream and topic level, so a permission miss surfaces as a typed error (`is_permission_denied()` / `is_stream_or_topic_not_found()`), never a silent wrong answer.

Every managed read model records the conversation that wrote each row (from the record's `gen_ai.conversation.id` header), so a read can narrow to one conversation server-side: `laser.query(index).conversation(id)`, `laser.graph(name).conversation(id).neighbors(..)`, and `laser.kv(ns).scan().conversation(id)` (and `.delete_many().conversation(id)`) over a memory-view namespace. It is a read-side narrowing over provenance, not a new isolation boundary, so a generic key-value entry that carries no conversation is left out of a conversation-filtered scan.

## The read ladder

Reads are rungs, each buying more machinery for more cost. Take the lowest rung that answers your question.

| Rung | Call | You get |
| --- | --- | --- |
| live consumer | `topic.consumer(..)` / `consumer_group(..)` | a Laser async `Stream` over Apache Iggy with batching, polling, replay, retries, groups, automatic or explicit server offset commits, and `next_within(timeout)` for a bounded single-record wait |
| replay | `topic.replay()` | a resumable `Cursor` by explicit offsets: bounded, restartable, nothing consumed (`topic.json::<T>().records(reader_name)` is the same rung, typed) |
| change feed | `laser.watch()` | wakeups when a materialized view advances, so await-then-query replaces sleep-and-retry |
| reliable consumer | `Agent::builder` / `ReliableConsumer` | consumer-group delivery plus dedup, retry, deadline, and dead-lettering |
| query | `laser.query(index)` | the materialized read model: filters, aggregates, vector recall, consistency levels |

Writes climb the same way: `topic.send(..)` is the raw zero-overhead append, `publish()` / `publish_batch()` are the typed fluent forms, and `topic.producer()` is the long-lived direct producer with batching, linger, retries, topology, and per-send routing. `topic.batching()` remains the governed size-and-time batcher for typed agent paths, `contract(..)` is a directed task, and `workflow(name)` is the dependency-ordered engine. Nothing on a higher rung hides the rungs below: the raw Apache Iggy builders and client stay reachable for advanced configuration.

## Features

- `default = ["streaming", "provenance"]`
- `streaming`, the open Apache Iggy foundation: `Laser`, streams, topics, direct producers, live partition and consumer-group streams, server offsets, raw and typed publish, batches, explicit-offset cursors, and JSON/CBOR/MessagePack codecs
- `vsr`, switches the underlying Apache Iggy client to its VSR cluster protocol and implies `streaming`. The Laser producer/consumer API and raw Apache Iggy escape hatch stay unchanged. Standard streaming commands are supported. Managed LaserData commands remain unavailable until upstream VSR admits custom command codes. Run `native-streaming` with `--features vsr` against a VSR cluster. `just test-it` remains the separate classic-protocol container suite because both framings cannot coexist in one binary.
- `provenance`, wire contract + provenance encoding/decoding
- `agent`, reliable consumer, `Agent::builder`, context, memory, state, contracts, workflows, and the `ActionGovernor` effect-boundary policy hook
- `query`, the managed materialized-view query client, including `read_your_writes` consistency and the unified `ResultCode` via `LaserError::code()`
- `managed`, the managed tier in one word: everything LaserData Cloud serves over the command band, composing the granular `fork`, `graph`, `kv`, `projections`, `query`, `rbac`, `runs`, and `watch` features. Each is independently selectable when a program needs one surface without the rest. Open core stays the default: publish and consume use `streaming`, the agent runtime builds on it, and both run on stock Apache Iggy. Each managed surface lights up by capability negotiation, so the tiering is a deployment fact, not a build fork.
- `kv`, an independently selectable managed key-value client (get/set/delete/scan, optional expiry, compare-and-swap via `.expect_version`/`.expect_absent().commit()`, single-transaction `copy_to`/`move_to`, and the one-round-trip `get_many` over the mixed-operation batch) over the `AGDX_KV` managed commands, backed by LaserData Cloud's managed point-state store
- capability RBAC over the managed surfaces (`rbac` feature, `sdk/src/rbac/`): `laser.whoami()` + `list_roles`/`get_role`/`get_bindings`/`define_role`/`delete_role`/`bind_roles`/`bind_roles_expect_revision`/`authz_history`, plus the pure `grants_allow` / `delegated_allow` decision helpers. Grants are `effect feature:action [on resource-pattern]` assembled through roles bound to the server-stamped user (deny-wins, default-deny), gated on the `authz` capability. Role names pass the wire-owned `validate_role_name` (64-byte charset safelist) before any round-trip. The layer is orthogonal to Iggy's own permissions and enforced at the streaming edge.
- `a2a-bridge`, A2A v1.0 JSON-RPC bridge over the agent topology (SendMessage + streaming, GetTask + CancelTask, the supportedInterfaces Agent Card)
- `mcp-bridge`, MCP JSON-RPC bridge (initialize, tools, resources, prompts) mapping tool calls onto AGDX
- `agui`, AG-UI state sync and event rendering over the log
- `sign`, ed25519 envelope signing and verification: `Agent::builder().signing_key(..)` signs pickup and terminal replies, `LaserBuilder::verifier(..)` rejects unsigned, unknown, or wrong-identity contract replies and surfaces their verified principal, signed `quarantine`/`unquarantine` registry facts, detached-JWS A2A card signing over the JCS form (`A2aBridge::signed_card` / `sign::verify_card`), and the managed `KvKeyRegistry` (enroll and snapshot verifying keys through the platform instead of a side file)

## Observability

The SDK instruments its own verbs and runtime loops with `tracing` spans under the target `laser`: hot-path verbs (publish, consume polls, managed calls) at `debug` so a default `info` filter never taxes them, lifecycle (connect, agent spawn, workflow runs, contracts) at `info`. Span fields reuse the wire's provenance vocabulary (`conversation`, `correlation`, `agent`, `topic` / `index`, `operation`, and the command `code` on managed calls), so client spans join log-derived traces in a standard OpenTelemetry pipeline with no custom translation. The exact field-to-header mapping is pinned in the AGDX notes.

No exporter ships with the SDK: `tracing` is the seam and your subscriber bridges it, e.g. with `tracing-opentelemetry`:

```rust,ignore
use tracing_subscriber::layer::SubscriberExt;

let tracer = opentelemetry_otlp::new_pipeline().tracing().install_simple()?;
tracing::subscriber::set_global_default(
    tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer)),
)?;
```

## Prelude

`use laser_sdk::prelude::*` imports the slim set: the accessors and the handful of types nearly every program names (~35 items). `use laser_sdk::prelude::full::*` adds the long tail (bridge types, seam traits, projection-control shapes, every memory knob) for an example or test that touches many surfaces. Application code reads best on the slim prelude plus explicit imports.

## Documentation

The full progressive tutorial (publish, projections, batches, heterogeneous topics, filters / aggregates / vector recall, codecs, user isolation, agentic runtime, open SDK vs LaserData Cloud) lives in the project repository under `docs/tutorial.md`, linked from the [repository README](https://github.com/laserdata/laser-sdk#readme). API reference is on [docs.rs](https://docs.rs/laser-sdk). The AGDX protocol's home is [agdxprotocol.ai](https://agdxprotocol.ai).

## License

Apache-2.0. Copyright LaserData, Inc.

Apache and Apache Iggy are trademarks of the Apache Software Foundation. Use of these marks does not imply endorsement by the Apache Software Foundation.
