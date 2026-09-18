# LaserData - Laser SDK

[![crates.io](https://img.shields.io/crates/v/laser-sdk.svg)](https://crates.io/crates/laser-sdk) [![docs.rs](https://docs.rs/laser-sdk/badge.svg)](https://docs.rs/laser-sdk)

[LaserData, Inc.](https://laserdata.com) maintains Laser SDK for [Apache Iggy](https://iggy.apache.org). This crate defines the Rust reference implementation. Python calls it, and TypeScript implements the same contract in Node. The clients use shared reference data and behavior tests. Streaming records form the base for projections, queries, key-value state, forks, and the optional AGDX agent runtime.

Laser SDK ships in independently adoptable layers:

- streaming (`streaming` feature, default), streams, topics, raw and typed publish, batches, resumable cursors, and JSON/CBOR/MessagePack codecs on Apache Iggy.
- managed platform (`managed` feature), projections, query, key-value state, forks, graph, watch, and the run registry against LaserData Cloud or Laser Stack.
- agentic (`agent` feature), reliable consumer + DLQ, conversation and causality, request/reply, routing, memory, handlers, typed AGDX verbs, workflows, effect governance, and durable intent records.
- edges, the optional A2A, MCP, and AG-UI adapters.

The SDK carries `gen_ai.*` provenance describing model calls but never makes them. It moves and coordinates messages only.

The [`laser-wire`](https://crates.io/crates/laser-wire) crate defines encoded messages, schemas, command codes, limits, and reference test data. It supports WebAssembly and does not require an asynchronous runtime. Laser SDK exposes it as `laser_sdk::wire` and through the existing module paths.

## Install

```toml
[dependencies]
laser-sdk = "0.4.0" # typed streaming plus provenance
# Add only the layers the application uses:
laser-sdk = { version = "0.4.0", features = ["agent", "managed"] }
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

let committed = api_calls.publish(&ApiCall {
        endpoint:   "/v1/items".into(),
        status:     200,
        latency_ms: 42,
        user_id:    "alice".into(),
    })?.send().await?;
println!("commit confirmations: {}", committed.confirmations.len());

let mut records = api_calls.records("latency-dashboard")?; // your reader's name, offsets stay caller-owned
while let Some(record) = records.next().await {
    let call: ApiCall = record?.value;
    println!("{} returned {}", call.endpoint, call.status);
}
# Ok(()) }
```

This example uses only the default `streaming` and `provenance` features and runs against Apache Iggy. One connection addresses every stream on the server. `Laser::connect_with_stream` only pins a default stream so `laser.topic(name)` can be used as a shortcut. It does not limit the connection to that stream. `Laser::connect_env()` reads `LASER_CONNECTION_STRING` and the optional `LASER_STREAM`, and `Laser::local()` targets local Apache Iggy.

Direct producers, topic sends, and publish builders return `SendMessagesResponse`. Each confirmation identifies the stream, topic, partition, and first offset in a batch. A server that does not report offsets returns an empty list. Completion follows the topic durability policy. An offset identifies a position only within its stream, topic, and partition.

Apache Iggy retries initial TCP connections and reconnects dropped connections. The default is unlimited retries at one-second intervals. Use `reconnection_retries=<count|unlimited>` and `reconnection_interval=<duration>` to change this behavior. After reconnecting, the client reapplies the credentials from the connection string.

For `*.laserdata.cloud` and `*.laserdata.com`, `connect` and `connect_with_stream` enable TLS with the bundled LaserData root CA. A CA is a certificate authority used to establish trust. The SDK stores this certificate in a directory that only the current user can access. It reuses the file only when its bytes match the bundled certificate.

Set `LASER_TLS_CERT=<path>` to use an explicit CA with any host. Set `LASER_NO_TLS=1` to disable automatic TLS. Values `0` and `false` do not disable it. Other hosts retain their connection-string TLS configuration when neither variable is set.

## Batch and any payload

`publish_batch` groups typed records for sending. A service can use `topic.producer()` for batching, delays, retries, and routing. `.background(BackgroundConfig::builder()..)` selects buffered sends through Apache Iggy. Before dropping a background producer, call `Producer::shutdown()` to flush its messages. Otherwise, unflushed messages can be lost. `topic.consumer_group()` provides continuous reads with offsets stored on the server.

A replay cursor reads a bounded set of records and keeps its offsets in the client. Save those offsets to resume later. Failed or canceled polls do not advance the saved offsets. Each partition read returns at most 10,000 messages, even when the configured request batch is larger.

The message body contains bytes in the format that the application selects. `json`, `msgpack`, `add_json`, and `add_msgpack` provide encoding helpers. `add_payload` sends raw bytes without inspecting their format. Use `raw_bytes(bytes, ContentType::Avro)` for encoded data or `add_avro` to encode with a schema. Protobuf, compressed data, and application-defined formats can use the same path.

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
let committed = batch.send().await?;      // the whole window, one round-trip
println!("commit confirmations: {}", committed.confirmations.len());
# Ok(()) }
```

A projection describes how a service reads and indexes message fields. Declare a `Projection` through the control API. The declaration selects fields, their positions in the payload, and whether the result retains the body:

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

`laser-plane` runs the projector in Laser Stack and LaserData Cloud. Against Apache Iggy without a managed backend, projection and query calls return `LaserError::Unsupported`. The open streaming path above continues to run unchanged.

## Typed operational and lakehouse queries

Query results carry one ordered logical field schema and positional tagged values. Read values through the result so field lookup and value typing remain explicit:

```rust,ignore
let result = laser
    .query("orders_v1")
    .where_eq("customer_id", "alice")
    .filter_gte("total", 100_i64)
    .limit(100)
    .fetch()
    .await?;

for row in &result.rows {
    println!("{}", result.value_text(row, "total").unwrap_or_default());
}
```

`result.fields` gives the ordered schema for `row.values`. Tagged values preserve integer widths, decimal precision, timestamps, UUIDs, bytes, structs, lists, maps, and nullability. Each page contains at most 1000 rows. Use the server-provided `next_cursor` to continue. `has_more` is true exactly when that cursor is present.

Lakehouse queries name one destination generation and can select a retained snapshot:

```rust,ignore
let historical = laser
    .query_lakehouse(destination_id, destination_generation)
    .at_snapshot(snapshot_id)?
    .filter_eq("customer_id", "alice")
    .limit(100)
    .fetch()
    .await?;

println!("{:?}", historical.context.resolved_target);
```

The result context identifies the selected engine and target. Lakehouse pages also identify destination and backend generations, the table UUID, snapshot, schema, and partition-spec IDs. They include the materialization boundary, checkpoint revision, and global state revision.

Each query builder keeps one execution identity. Use `execution_id()` to obtain it. The `status()` and `cancel()` commands require the corresponding deployment capabilities. The SDK rejects unsupported calls before sending them. `fetch_all()` and bounded row iteration follow server-issued cursors.

## Destinations and Arrow IPC

Destination and explicit query-route state is available through `laser.destinations()`. Reads choose checkpoint consistency and remain bounded:

```rust,ignore
use laser_sdk::wire::checkpoint::{CheckpointReadConsistency, DestinationListFilter};

let page = laser
    .destinations()
    .list(
        DestinationListFilter::default(),
        None,
        50,
        CheckpointReadConsistency::Linearizable,
    )
    .await?;
println!("{} destination(s) at revision {}", page.destinations.len(), page.global_state_revision);
```

Registration and desired-state changes are revision-guarded. `register` takes one complete destination declaration. `set_desired_state` compares both global state and destination definition revisions before applying a change.

For analytical batches, publish one complete self-contained Arrow IPC stream per Apache Iggy message:

```rust,ignore
use laser_sdk::wire::arrow::{ArrowIpcMessageMetadata, ARROW_IPC_CONTRACT_VERSION};
use laser_sdk::wire::schema::SchemaFingerprint;

let metadata = ArrowIpcMessageMetadata {
    contract_version: ARROW_IPC_CONTRACT_VERSION,
    schema_fingerprint: SchemaFingerprint::new(schema_fingerprint),
    encoded_bytes: arrow_stream.len() as u64,
    field_count: 8,
    record_batch_count: 1,
    row_count: 10_000,
    dictionary_count: 0,
};
topic.publish().arrow_ipc(arrow_stream, metadata)?.send().await?;
```

Before sending, the SDK checks the metadata and exact payload length. Managed ingestion requires a self-contained Arrow stream with microsecond timestamps and stable dictionaries. Decimal widths cannot exceed 128 bits. Unions and extension types are not supported.

## Typed topics

A typed handle binds a topic to one body type. The serde forms encode published values and decode received values. They do not need a schema registry. Decoded records include their log positions.

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

`laser.stream("commerce").topic("orders").schema::<Order>(id).await?` uses a registered schema. This form requires `schema-codecs` and the registry served by `laser-plane`. The handle resolves and compiles the schema once, checks each body, and adds `agdx.ct` and `agdx.sid`. A body that does not match fails before publication.

The `records` reader reports `TypedDecodeError { position, source }` for a record that does not decode, then continues. An `Agent` handler can decode through the same API. Its existing dead-letter policy handles invalid records.

## Durable approval records

With the `agent` feature, effects that need asynchronous approval use ordinary typed topics. The SDK validates the intent and ballots, but your application owns the topic layout, replay cursor, and final effect:

```rust,ignore
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

`AgentId` identifies an agent for routing and attribution. `ConsumerGroupName` selects the Apache Iggy consumer group that shares work. `PrincipalId` identifies the authenticated user for access decisions. `Agent::builder` derives the default group name from the agent ID. `.consumer_group(..)` changes the group without changing the agent identity.

One connection can advertise one agent. A second advertisement receives `LaserError::PresenceConflict`. `Router::to_principal(..)` and `CapabilitySelector::principal(..)` require live presence to match the authenticated `PrincipalId`. A mismatch returns `RoutePrincipalMismatch`. With a verifier, contract replies require a valid signature from the selected identity. `AgentMessage::verified_principal` records the responder, including each `ScatterReport` branch.

| Accessor | Scope | Serves |
| --- | --- | --- |
| `laser.stream(name).topic(name)` | any topic on any stream | the same verbs, explicitly addressed (streams are first-class and dynamic on Apache Iggy) |
| `laser.topic(name)` | a topic on the optional default stream | shorthand for the same verbs |
| `laser.query(index)` | a materialized index | filters, aggregates, vector recall, the bounded `.max_rows(n).rows()` walk |
| `laser.watch()` | the change feed | consume advancement records instead of re-querying blind |
| `laser.kv(namespace)` | managed point state | get/set/delete/scan, compare-and-swap, leases |
| `laser.fork(id)` | a copy-on-write branch | speculative writes, overlay queries, promote or squash |
| `laser.graph(name)` | the knowledge graph | traversal, neighbors, upsert, link/unlink |
| `laser.memory(scope)` | agentic memory | remember / recall / improve / forget |
| `laser.context(conversation)` | one conversation's working record | append, bounded fetch, prompt block, state folds |
| `laser.sessions().create(id)` | one agent's conversation | typed turns, context for a model, scoped memory, saved offsets through `checkpoint`, reads through `turns_at` / `turns_since`, state through `state_at` / `replay` |
| `laser.agent(id)` / `laser.contract(..)` / `laser.workflow(name)` / `laser.runs()` | the fabric | directed asks, deadline-bound contracts, dependency-ordered workflows, the run registry |

A lease gives one holder temporary permission to coordinate an operation. A connection-backed `Laser` acquires it through a dedicated coordination connection. An application-supplied `IggyClient` uses an explicit `FencedLeaseClient` with its own transport. A timed-out attempt retires that connection. If the outcome is unknown, the call waits through the requested lifetime before returning an error.

Requested lifetimes must fall within `MIN_LEASE_TTL_MICROS ..= MAX_LEASE_TTL_MICROS`, from 1 second to 5 minutes. The SDK rejects other values before sending. The store can grant less time, but never more. A holder can renew before expiry. Exclusive workflows keep the lease through verification and the completion journal write. They release it after that record is durable.

`FencedLeaseClient` accepts a `ManagedKvTransport`. For a transport selected at runtime, use `SharedKvTransport`, which is `Arc<dyn DynManagedKvTransport>`. Each `ManagedKvTransport` implements the object-safe interface. The `Arc` also implements `ManagedKvTransport`. `FencedLeaseClient::new` therefore uses the same envelopes, retries, and decoding with either form.

One connection can address every stream that its user can access. `connect_with_stream` selects an optional default for `laser.topic(name)`. Without a default, this shortcut returns `NoStream`. Apache Iggy controls access to streams and topics. Use `is_permission_denied()` and `is_stream_or_topic_not_found()` to identify access failures.

Managed read models can retain the originating conversation from `gen_ai.conversation.id`. Use `laser.query(index).conversation(id)` or `laser.graph(name).conversation(id).neighbors(..)` to narrow reads. A memory-view namespace also supports `laser.kv(ns).scan().conversation(id)` and `.delete_many().conversation(id)`. These filters use record metadata and do not create an access boundary. A key-value entry without conversation metadata is excluded from a conversation-filtered scan.

## The read ladder

Choose the read API that provides the behavior your application needs.

| Rung | Call | You get |
| --- | --- | --- |
| live consumer | `topic.consumer(..)` / `consumer_group(..)` | a Laser async `Stream` over Apache Iggy with batching, polling, replay, retries, groups, automatic or explicit server offset commits, and `next_within(timeout)` for a bounded single-record wait |
| replay | `topic.replay()` | a resumable `Cursor` by explicit offsets: bounded, restartable, nothing consumed (`topic.json::<T>().records(reader_name)` is the same rung, typed) |
| change feed | `laser.watch()` | lightweight advancement records, so feed-poll-then-query replaces repeated blind queries |
| reliable consumer | `Agent::builder` / `ReliableConsumer` | consumer-group delivery plus dedup, retry, deadline, and dead-lettering |
| query | `laser.query(index)` | the materialized read model: filters, aggregates, vector recall, consistency levels |

Use `topic.send(..)` for raw publication. Use `publish()` and `publish_batch()` for typed builders. Use `topic.producer()` for a persistent producer with batching, retries, topology discovery, and routing. `topic.batching()` adds governed batches that flush by size or time. `contract(..)` sends a directed task, and `workflow(name)` runs steps in dependency order. Apache Iggy builders and the client remain available for detailed configuration.

## Features

- `default = ["streaming", "provenance"]`
- `streaming`, the open Apache Iggy foundation: `Laser`, streams, topics, direct producers, live partition and consumer-group streams, server offsets, raw and typed publish, batches, explicit-offset cursors, and JSON/CBOR/MessagePack codecs. The SDK uses Iggy's native VSR transport. Managed reads use the non-replicated extension path, and managed authorization writes use dedicated replicated operation codes.
- `provenance`, wire contract + provenance encoding/decoding
- `agent`, reliable consumer, `Agent::builder`, context, memory, state, contracts, workflows, and the `ActionGovernor` effect-boundary policy hook
- `query`, the managed materialized-view query client, including `read_your_writes` consistency and the unified `ResultCode` via `LaserError::code()`
- `managed` enables `fork`, `graph`, `kv`, `projections`, `query`, `rbac`, `runs`, and `watch`. Each can also be selected separately. Streaming and agents remain available on Apache Iggy. Managed operations require reported deployment capabilities.
- `kv` provides managed key-value reads, writes, scans, expiry, and compare-and-swap through `AGDX_KV`. Conditional writes use `.expect_version` or `.expect_absent().commit()`. `copy_to` and `move_to` use one transaction. `get_many` uses a mixed batch. `laser-plane` provides storage.
- capability RBAC over the managed surfaces (`rbac` feature, `sdk/src/rbac/`): `laser.whoami()` + `list_roles`/`get_role`/`get_bindings`/`define_role`/`delete_role`/`bind_roles`/`bind_roles_expect_revision`/`authz_history`, plus the pure `grants_allow` / `delegated_allow` decision helpers. Grants are `effect feature:action [on resource-pattern]` assembled through roles bound to the server-stamped user (deny-wins, default-deny), gated on the `authz` capability. Role names pass the wire-owned `validate_role_name` (64-byte charset safelist) before any round-trip. The layer is orthogonal to Iggy's own permissions and enforced at the streaming edge.
- `a2a-bridge`, A2A v1.0 JSON-RPC bridge over the agent topology (SendMessage + streaming, GetTask + CancelTask, the supportedInterfaces Agent Card)
- `mcp-bridge`, MCP JSON-RPC bridge (initialize, tools, resources, prompts) mapping tool calls onto AGDX
- `agui`, AG-UI state sync and event rendering over the log
- `sign` provides Ed25519 signing and verification. `Agent::builder().signing_key(..)` signs pickup and terminal replies, including `respond_input`. `Agent::builder().verifier(..)` rejects unsigned or invalid records before handling. `LaserBuilder::verifier(..)` applies the same requirement to correlated reply waits.

Signatures bind observed headers and are evaluated at the server-recorded timestamp. Signed `quarantine` and `unquarantine` facts require operator keys. `A2aBridge::signed_card` and `sign::verify_card` support detached JWS over the canonical card. `KvKeyRegistry` stores versioned keys in the managed platform.

## Observability

The SDK creates `tracing` spans under the `laser` target. Publication, polls, and managed calls use `debug`. Connections, agent startup, workflows, and contracts use `info`. Fields include `conversation`, `correlation`, `agent`, `topic`, `index`, `operation`, and managed command `code`. The AGDX specification defines their mapping to record headers. An OpenTelemetry subscriber can connect client spans with traces derived from records.

The SDK supplies spans through `tracing`. Your application supplies the subscriber and exporter. The following example uses `tracing-opentelemetry` to show the connection. It is illustrative and does not compile as part of this crate:

```rust,ignore
use tracing_subscriber::layer::SubscriberExt;

let tracer = opentelemetry_otlp::new_pipeline().tracing().install_simple()?;
tracing::subscriber::set_global_default(
    tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer)),
)?;
```

## Prelude

`use laser_sdk::prelude::*` imports the common accessors and types, about 35 items. `use laser_sdk::prelude::full::*` also imports bridge types, extension traits, projection types, and memory configuration. Prefer the smaller set and explicit imports for application code.

## Documentation

The [repository README](https://github.com/laserdata/laser-sdk#readme) links to `docs/tutorial.md`. The tutorial covers publication, projections, queries, batches, codecs, stream isolation, and agents. The API reference is on [docs.rs](https://docs.rs/laser-sdk). The protocol home is [agdxprotocol.ai](https://agdxprotocol.ai).

## License

Apache-2.0. Copyright LaserData, Inc.

Apache and Apache Iggy are trademarks of the Apache Software Foundation. Use of these marks does not imply endorsement by the Apache Software Foundation.

## Publish recovery

Publish attempts default to 60 seconds with three retries. Retry delays start at 250 milliseconds, double after each failure, and stop increasing at 30 seconds. Configure these values through the client builder or connect arguments. The corresponding environment variables are `LASER_PUBLISH_TIMEOUT_MS`, `LASER_PUBLISH_MAX_RETRIES`, and `LASER_PUBLISH_RETRY_BACKOFF_MS`. Explicit configuration overrides these variables. Exhausted retries return an error for the application to handle.

See [publish recovery and outage handling](../docs/publish-recovery.md).
