# LaserData - Laser SDK tutorial

This tutorial builds an observability application for model calls. It records calls and queries them by latency, result, model, and user. Chapters 1 through 8 cover publication, projections, queries, batches, codecs, and similarity reads. Chapter 9 adds agent coordination, Chapter 10 adds multi-agent orchestration, and Chapter 11 reads only matching records with a server-side filter. The snippets use Rust, the reference SDK. The [three-language guides](https://docs.laserdata.cloud/laser-sdk) show each feature in TypeScript, Rust, and Python, and the [parity matrix](parity.md) maps every Rust call to its Python and TypeScript spelling.

Prerequisites: the install snippet from the [README](../README.md) and a running Apache Iggy server. `just up` starts one on `127.0.0.1:8090` through Docker.

---

## Two layers, one connection

Laser SDK is a streaming substrate, a managed query layer over it, and an agentic runtime built on top:

| layer | what it is | when you need it |
| --- | --- | --- |
| streaming (`streaming` feature, default) | typed publish, direct producers, live async consumer groups with server offsets, and the resumable `Cursor`. No agent concepts, no managed backend. | anywhere you stream messages against Apache Iggy. |
| managed (`managed` feature, or the granular `query` / `projections` / `destinations` / `filters` / `kv` / `fork` / `graph` / `watch` / `rbac`) | declared projections, query DSL with filters / aggregates / vector recall, served by Laser Stack or LaserData Cloud. | agent / LLM observability, analytics, audit logs, market data, IoT, anywhere you want to query what you streamed. |
| agentic (`agent` feature) | reliable consumer + DLQ, sessions, conversation/causality, `Router`, `Memory`, `Agent::builder` handlers. Builds on the streaming layer. | When you are orchestrating LLM agents, not just observing traffic. |

Chapters 1-8 use streaming and managed data operations. Chapter 9 adds the agent runtime.

Apache Iggy provides the log and transport. Laser Stack or LaserData Cloud adds managed projections, queries, KV, and forks on the same connection. `Topic::producer()` and `consumer_group()` provide continuous streaming. `Topic::replay()` provides a cursor with client-owned offsets. `StateStore` can save checkpoints and duplicate-suppression keys.

For ordinary streaming, use `topic.producer()`, `topic.consumer(..)`, and `topic.consumer_group(..).consumer()`. They provide batching, delays, retries, routing, groups, and automatic or explicit commits. The [`native-streaming`](../examples/rust/src/native-streaming/README.md) example demonstrates both commit modes. For detailed Iggy configuration, use `topic.iggy_producer()`, `topic.iggy_consumer_group(..)`, or `laser.client()`. Import matching Iggy types through `laser_sdk::iggy`.

VSR is the only supported Rust, Python, and TypeScript transport. It requires no Cargo feature or TypeScript connection option. Standard commands, unknown managed codes, and dedicated replicated authorization operations all use the same client connection. The server remains authoritative for custom command classification.

---

## Chapter 1 - publish your first message

Connect to Apache Iggy and publish a typed record to a topic. The topic durability policy determines its acknowledgment guarantee. The record becomes queryable after a projection indexes it. Chapter 2 adds that projection.

```rust
use laser_sdk::prelude::*; // the slim prelude: accessors + the everyday types. `prelude::full::*` has everything.
use serde::{Deserialize, Serialize};

// An enum (with `strum::Display` + serde rename), not a stringly-typed field, so
// the indexed value and the JSON payload can never disagree.
#[derive(Debug, Serialize, Deserialize, strum::Display)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
enum Outcome { Ok, Error, Timeout }

#[derive(Debug, Serialize, Deserialize)]
struct Inference {
    model:      String,
    provider:   String,
    outcome:    Outcome,
    latency_ms: u32,
    user_id:    String,
    tokens:     u32,
}

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    let laser = Laser::connect("iggy:iggy@127.0.0.1:8090").await?;
    let inferences = laser.stream("agent-telemetry").topic("inferences");

    inferences.ensure(4).await?;
    inferences.publish()
        .json(&Inference {
            model:      "gpt-4o".into(),
            provider:   "openai".into(),
            outcome:    Outcome::Ok,
            latency_ms: 420,
            user_id:    "alice".into(),
            tokens:     1840,
        })?
        .send().await?;

    Ok(())
}
```

The example records each model call as an `Inference`. Publication, query, and agent operations share the connection created by `connect`.

A stream groups topics in Apache Iggy. Applications can organize streams by domain, environment, or access boundary. Use `Laser::connect(conn)` and `laser.stream(name).topic(name)` for explicit addressing. `Laser::connect_with_stream(conn, stream)` and `laser.with_default_stream(stream)` select a default for `laser.topic(name)` and agent helpers. They do not restrict the connection to one stream.

`.json(&body)` encodes the value and selects `agdx.ct` code `1` for JSON. The terminal send publishes it.

The log retains records under its retention policy. Readers can replay retained offsets. Without `.partition_key(..)`, balanced routing selects a partition. A stable key selects a consistent partition. Chapter 2 declares the projection used to index these records.

---

## Chapter 2 - declare a projection, query the topic

A `Projection` selects fields and their positions in the payload. It plays a similar role to `CREATE INDEX ON inferences(latency_ms)` in a database. The projector extracts those fields when it processes new records. The Chapter 1 producer stays unchanged.

```rust
use laser_sdk::query::{Projection, ProjectionBinding};
use laser_sdk::stream::ContentType;

let inference_v1 = Projection::builder("inference.v1")
    .name("inference")
    .version(1)
    .content_type(ContentType::Json)
    .fields(["model", "provider", "outcome", "latency_ms", "user_id", "tokens"])
    .build();

let binding = ProjectionBinding::builder()
    .source("agent-telemetry", "inferences")   // (data stream, topic)
    .allow("inference.v1")
    .default_projection("inference.v1")
    .build();
```

Register the projection, then apply the binding that routes `inferences` into it:

```rust
laser.projections().register(inference_v1).await?;
laser.bindings().apply(binding).await?;
```

The binding is rejected when it names a projection that is not registered. Both calls publish control commands that `laser-plane` applies asynchronously, so poll `laser.projections().get(id)` to observe the result. `laser-plane` then materializes records from `inferences`.

### Three storage tiers

A published record lives in up to three places, controlled by the projection:

1. The Iggy log retains the original encoded records and supports offset replay.
2. The projector extracts fields from `.fields([...])` or `.field_at(...)`. Queries use them for `where_eq`, `filter_*`, `order_*`, and aggregates.
3. An inline body copies the original payload into the row for `fetch_typed::<T>()`. Use `.index_only()` to omit that copy. Log retention remains independent.

The body can contain fields that are not indexed. Queries use declared fields. Other fields remain available through the inline body or retained log record.

> _With `.index_only()`, `fetch_typed::<T>()` cannot decode rows because the reserved original-payload field is absent. Callers either use `.fetch()` and read typed positional values through `QueryResult::value`, or replay from Iggy log. Plan the trade-off when you declare the projection, not at query time._

Now query it:

```rust
let slow: Vec<Inference> = laser.query("inferences")
    .filter_gte("latency_ms", 500)
    .order_desc("latency_ms")
    .limit(20)
    .fetch_typed().await?;

for call in slow {
    println!("{}/{} -> {} in {}ms", call.provider, call.model, call.outcome, call.latency_ms);
}
```

`laser.query("inferences")` runs across the same Iggy connection. Query is a managed feature served by Laser Stack or LaserData Cloud. Against Apache Iggy without `laser-plane`, a query terminal such as `.fetch()` returns `LaserError::Unsupported`. KV, forks, and registry browse behave the same way.

What runs on Apache Iggy is the open SDK's streaming, agent, provenance, dedup, cursor, and log-backed memory surfaces. The query, KV, fork, and projection surfaces require Laser Stack or LaserData Cloud. The typed result decodes straight back into your struct.

Laser Stack and LaserData Cloud serve two more read surfaces, both answering `LaserError::Unsupported` against Apache Iggy without a managed backend:

- The `kv` feature exposes `Laser::kv` when `Capabilities::kv.available` is true. It supports `get`, `set`, `delete`, and `scan` with opaque keys and values. Use `.bytes`, `.json`, `.msgpack`, or `.encode_with::<C>` to write. Use `get`, `get_typed`, or `get_as::<C, _>` to read. Managed grants control namespace access.
- Read projections through `laser.projections().get(id)` or `laser.projections().list().fetch()`. Read schemas through `laser.schemas().get(id)` or `laser.schemas().list()`. These calls require `Capabilities::managed`. Control-topic writes change projections and schemas.

### Projection retention, decoupled from topic expiry

The topic `message_expiry` controls raw-record retention. A projection normally follows the log and removes rows whose source expires. Set binding retention to select a different lifetime for the read model:

```rust
use laser_sdk::query::RetentionPolicy;

let binding = ProjectionBinding::builder()
    .source("agent-telemetry", "inferences")
    .allow("inference.v1")
    .default_projection("inference.v1")
    .retention(RetentionPolicy::Keep)   // index survives even after the log expires
    .build();
```

`RetentionPolicy` variants:

- `MirrorLog` (default) - follow the log, and also drops the projection when the source topic is deleted.
- `Keep` - rows live forever, regardless of log expiry or topic deletion.
- `KeepUntilSourceDeleted` - ignore message expiry (keep forever), but drop the projection when the source topic is deleted. For "permanent index, but it is meaningless once the topic is gone."
- `TimeToLive { ttl_micros }` - keep rows for a fixed age after they were materialized, independent of the log.
- `MaxRows { rows }` - keep only the newest N rows for the table.

Leave `.retention(...)` unset to inherit the deployment default. The policy is enforced by `laser-plane`.

### Why the producer does not stamp `.index(...)` per record

A projection defines the indexed fields. Stamping `.index("user_id", "alice")` on each record instead can:

- Repeat field names in every record.
- Couple producers to projector details.
- Allow producers to provide inconsistent field definitions.

The producer supplies data, and the projection defines extraction. An explicit `.index(..)` header wins over a value the projection schema extracts for the same field. A record with no indexed fields from headers or the schema produces no row.

> _Niche scenario: the producer needs to surface a queryable field on a payload the projector cannot decode (opaque binary, custom framing, or a schema-first body with no registered schema). For those, the producer stamps the field with `.index("trace_id", id)` and the projector indexes the `agdx.idx.*` header. `.header(..)` is different. It attaches metadata that stays on the log record and is not queryable. Not used in the rest of the tutorial._

---

## Chapter 3 - real-time batches

Use batches when applications publish many records. `publish_batch().send()` groups records before sending them through Iggy. The builder keeps records locally until `.send().await?`.

```rust
let drained: Vec<Inference> = drain_trace_buffer(Duration::from_secs(1));

let inferences = laser.stream("agent-telemetry").topic("inferences");
inferences.publish_batch()
    .extend_json(drained.iter())?    // N records, in memory
    .send().await?;                  // ONE send_messages, N records
```

Iggy limits the bytes in a `send_messages` request. Split a larger backlog of records into batches that fit that limit.

Partitioning composes with the batch:

- No `partition_key` (default), Iggy's balanced partitioner picks one partition for the whole `send_messages` call. Throughput-friendly.
- `.partition_key("alice")`, the entire batch is hashed to one partition, preserving per-user ordering across records.
- One-partition topic, global order across the whole topic, useful for the heterogeneous-message pattern in Chapter 4.

A query above `MAX_PAGE_SIZE` (1000 rows) fails with `LaserError::Invalid` before any round trip. Replies also have a 64 MiB size limit. Use `.max_rows(n).rows()` or `.fetch_all()` to read larger results through bounded pages (Chapter 5).

The projection from Chapter 2 covers every record in this batch. No new declaration needed.

---

## Chapter 4 - heterogeneous topic, mixed message shapes

One topic can contain different record types. This chapter uses inferences, tool calls, and errors in one partition. Declare a projection for each shape and bind them to the topic. `agdx.ref` selects the extraction rule for each record.

```rust
let tool_call_v1 = Projection::builder("tool.call.v1")
    .name("tool.call").version(1)
    .content_type(ContentType::Json)
    .fields(["tool", "outcome", "latency_ms", "user_id"])
    .build();

let agent_error_v1 = Projection::builder("agent.error.v1")
    .name("agent.error").version(1)
    .content_type(ContentType::Json)
    .fields(["agent", "kind", "user_id"])
    .build();

let trace = ProjectionBinding::builder()
    .source("agent-telemetry", "agent_trace")
    .allow("inference.v1")
    .allow("tool.call.v1")
    .allow("agent.error.v1")
    .default_projection("inference.v1")
    .build();
```

The producer stamps the `agdx.ref` projection-ref header per record so the projector knows which extraction plan to apply:

```rust
let trace = laser.stream("agent-telemetry").topic("agent_trace");
trace.publish_batch()
    .partition_key(&conversation_id)       // keep one run's messages in order
    .add_json_with_projection("inference.v1",   &inference)?
    .add_json_with_projection("tool.call.v1",   &tool_call)?
    .add_json_with_projection("agent.error.v1", &error)?
    .add_json_with_projection("inference.v1",   &next_inference)?
    .send().await?;                        // ONE send_messages, 4 records
```

A record arriving without a `projection_ref` uses the binding's `default_projection`. If none, the record is skipped.

---

## Chapter 5 - filter, aggregate, time-range, page

The query DSL exists so you do not write SQL.

```rust
use laser_sdk::query::{CmpOp, Filter};
use std::time::{SystemTime, UNIX_EPOCH};

let now_us = SystemTime::now().duration_since(UNIX_EPOCH)
    .expect("system time").as_micros() as u64;
let hour_us: u64 = 3_600_000_000;

// Top-N latency outliers in the last hour:
let outliers: Vec<Inference> = laser.query("inferences")
    .filter_gte("latency_ms", 5_000)
    .time_range(now_us - hour_us, now_us)
    .order_desc("latency_ms")
    .limit(50)
    .fetch_typed().await?;

// Error counts per model:
let errors_by_model = laser.query("inferences")
    .where_eq("outcome", "error")
    .count()
    .group_by(["model"])
    .fetch().await?;
// The result schema carries the count as the field "count". Read it with
// result.value(row, "count") or result.value_u64(row, "count").

// Several metrics in one pass, plus a HAVING on the count alias. Each metric
// lands under its alias (count -> "count", avg -> "avg", p95 -> "percentile"):
let hot_routes = laser.query("inferences")
    .count()
    .avg("latency_ms")
    .percentile("latency_ms", 0.95)
    .group_by(["model"])
    .having(Filter::pred("count", CmpOp::Gt, 1_000_i64))
    .fetch().await?;
// percentile / stddev are backend-gated: an embedded index returns
// LaserError::Unsupported, a columnar backend answers it.

// Filter tree: (outcome = error) OR latency over 10s.
let trouble: Vec<Inference> = laser.query("inferences")
    .filter(Filter::any([
        Filter::pred("outcome", CmpOp::Eq, "error"),
        Filter::pred("latency_ms", CmpOp::Gte, 10_000_i64),
    ]))
    .fetch_typed().await?;

// Page-walking under an explicit ceiling (the bounded-reads law):
let mut rows = laser.query("inferences")
    .where_eq("user_id", "alice")
    .order_desc("latency_ms")
    .max_rows(10_000)
    .rows()?;
while let Some(row) = rows.next().await? {
    process(row);
}

// Or materialize the whole result set in one call:
let all: Vec<Inference> = laser.query("inferences")
    .where_eq("provider", "anthropic")
    .fetch_all_typed().await?;
```

Exact-match and comparison methods are `where_eq`, `filter_eq`, `filter_ne`, `filter_gt`, `filter_gte`, `filter_lt`, `filter_lte`, and `filter_in`. Text predicates use `filter_contains` and `filter_prefix`. `filter(Filter)` composes `Any` and `Not` trees.

Result selection uses `time_range`, `order_asc`, `order_desc`, `limit`, `offset`, `with_payload`, `select_fields`, and `distinct`. Aggregates are `count`, `count_distinct`, `sum`, `avg`, `min`, `max`, `stddev`, and `percentile`. `agg_as`, `group_by`, `window`, and `having` configure aggregate output. `raw_sql` and `raw_sql_with` select SQL. `nearest` and `nearest_in` select vector search.

Terminals, `.fetch()` (paged), `.fetch_typed::<T>()` (`Vec<T>`), `.fetch_one::<T>()` (`Option<T>`), the bounded walks `.max_rows(n).rows()` / `.max_rows(n).rows_typed::<T>()` (explicit ceiling, then row-at-a-time), and the explicit full-result opt-ins `.fetch_all()` / `.fetch_all_typed::<T>()`.

Use `.conversation(conversation_id)` to restrict results by conversation. The deployment projects `gen_ai.conversation.id` into the reserved `conversation_id` field. The method adds an ordinary predicate on that field.

---

## Chapter 6 - vector recall

A vector projection selects the payload field that contains the embedding, with `/embedding` as the default. The projector stores it during materialization. Queries use the stored embedding.

```rust
let incident_v1 = Projection::builder("incident.v1")
    .name("incident").version(1)
    .content_type(ContentType::Json)
    .fields(["service", "severity"])
    .vector_field("/embedding")     // RFC-6901 JSON pointer into the body
    .build();

let incidents = ProjectionBinding::builder()
    .source("agent-telemetry", "incidents")
    .allow("incident.v1")
    .default_projection("incident.v1")
    .build();
```

Producer publishes a postmortem with its embedding inline:

```rust
#[derive(Serialize, Deserialize)]
struct Incident {
    service:   String,
    severity:  String,
    summary:   String,
    embedding: Vec<f32>,
}

laser.stream("agent-telemetry").topic("incidents").publish()
    .json(&Incident { /* ... */ })?
    .send().await?;
```

Consumer finds past incidents similar to a new one:

```rust
let nearest: Vec<Incident> = laser.query("incidents")
    .where_eq("service", "storage")
    .nearest(query_embedding, 5)
    .fetch_typed().await?;
```

`Laser::memory` provides the shared memory API: remember, recall, improve, and forget. Use `query().nearest(..)` for direct control of vector queries.

---

## Chapter 7 - codecs, JSON, MessagePack, Avro, Protobuf, your own

`ContentType::code` selects the `u8` value stored in `agdx.ct`. `Codec<T>` encodes a value and reports its content type. Built-in codecs are `Json`, `Msgpack`, `Cbor`, and `Bson`. Their formats carry field names, so they can be decoded without a writer schema. For other formats, implement the trait or supply `.raw_bytes(...)`.

```rust
// First-party shortcuts (JSON and MessagePack have builder sugar):
laser.stream("agent-telemetry").topic("inferences").publish().json(&inference)?.send().await?;
laser.stream("agent-telemetry").topic("inferences").publish().msgpack(&inference)?.send().await?;

// Generic dispatch works for every codec, including CBOR and BSON. Bson rides
// the `query` feature (it pulls in the wire crate's native BSON support), so
// it lives on `laser_sdk::query`, not `laser_sdk::stream` like the other three:
use laser_sdk::query::Bson;
use laser_sdk::stream::{Cbor, Json, Msgpack};
laser.stream("agent-telemetry").topic("inferences").publish()
    .encode_with::<Cbor, _>(&inference)?
    .send().await?;
laser.stream("agent-telemetry").topic("inferences").publish()
    .encode_with::<Bson, _>(&inference)?
    .send().await?;
```

`Codec` encodes values, and `Decoder` decodes them. `Json`, `Msgpack`, `Cbor`, and `Bson` implement both. `fetch_typed` uses JSON by default. `fetch_typed_with::<C, _>` and `fetch_one_with` select another codec:

```rust
let traces: Vec<Inference> = laser.query("inferences").fetch_typed_with::<Msgpack, _>().await?;
```

Payload bytes come back out of the public API as `Vec<u8>` for cursor messages, agent messages, and memory items. A live consumer record holds its payload as `Bytes`. Queries return positional tagged values. When payload selection is enabled, `fetch_typed` and `fetch_typed_with` read the reserved original-payload field from the result schema. Raw byte inputs on the hot chain accept `Vec<u8>`, `String`, and `&'static [u8]`.

### One typed handle instead of per-call codecs

Use `laser.stream("agent-telemetry").topic("inferences").json::<Inference>()` or `.cbor::<Inference>()` to select one body type. `TypedTopic.publish(&value)` encodes it, and `records(reader_name)` returns typed records. Decode failures include the log position. The `schema-codecs` form `.schema::<Inference>(id).await?` uses a registered schema, rejects invalid values, and adds `agdx.sid`.

### A custom codec (Avro example)

```rust
use laser_sdk::stream::{Codec, ContentType};
use laser_sdk::wire::error::DecodeError;

pub struct AvroCodec;
impl<T: my_avro::AvroSerialize> Codec<T> for AvroCodec {
    fn content_type() -> ContentType { ContentType::Avro }
    fn encode(value: &T) -> Result<Vec<u8>, DecodeError> {
        my_avro::to_bytes(value).map_err(|e| DecodeError::Encode(format!("avro: {e}")))
    }
}

laser.stream("agent-telemetry").topic("inferences").publish()
    .encode_with::<AvroCodec, _>(&inference)?
    .send().await?;
```

### Mixed codecs in one batch

```rust
let trace = laser.stream("agent-telemetry").topic("agent_trace");
trace.publish_batch()
    .add_encoded::<Json, _>(&inference)?                      // JSON
    .add_encoded::<Msgpack, _>(&tool_call)?                   // MessagePack
    .add_raw_bytes(embedding_bytes, ContentType::Avro)        // pre-encoded
    .add_encoded_with_projection::<Json, _>("agent.error.v1", &error)?
    .send().await?;
```

The `agdx.ct` header code on each record tells the consumer how to decode. The batch-wide `.content_type(...)` directive applies to records that do not carry their own. Without either, no `agdx.ct` is stamped at all and the payload rides as opaque bytes.

### Self-describing vs schema-first codecs

The four built-in codecs (`Json`, `Msgpack`, `Cbor`, `Bson`) are self-describing: the bytes carry their own field names, so the managed projector indexes them with nothing declared in advance.

Avro and Protobuf need a writer schema to interpret their bodies. Register the schema and attach its `u32` ID through `agdx.sid`. Without a registered schema, projection can use only explicit `agdx.idx.*` headers and leaves the body opaque.

`JsonSchema { schema }` uses draft 2020-12 to check self-describing payloads. A record with that schema ID is decoded and checked by `laser-plane`. A mismatch prevents body-field materialization and appears in health counters. The configured policy determines dead-letter publication.

Registration is synchronous and `laser-plane` allocates the id:

```rust
let schema_id = laser
    .schemas()
    .register(SchemaSource::Avro { schema: READING_AVRO_SCHEMA.to_owned() })
    .send()
    .await?;
```

`laser-plane` compiles the schema, allocates a free ID, appends the control record durably, and returns the ID. Apply that ID through `agdx.sid`. The record applies asynchronously, so read the registry before publishing with a new ID. `laser.schemas().drop(id)` records asynchronous removal.

`SchemaSource` supports `Avro { schema }`, `Protobuf { descriptor_set, message_type }`, and `JsonSchema { schema }`. Avro uses schema JSON. Protobuf uses a compiled `FileDescriptorSet` and fully qualified message type. The [`fleet-tape`](../examples/rust/src/fleet-tape/README.md) and [`event-analytics`](../examples/rust/src/event-analytics/README.md) examples demonstrate Avro and JSON Schema.

Schema IDs are permanent. Register a new ID for a changed definition instead of replacing an existing one. Dropped IDs remain reserved, and earlier records retain decoding support. Registering the same definition through the raw control topic can restore it. A different definition under that ID is rejected and dead-lettered.

`laser.schemas().list()` returns `Vec<SchemaInfo>`, where `SchemaInfo { schema, dropped }` includes lifecycle state. `laser.schemas().get(id)` returns `Option<SchemaInfo>`. These reads use the same managed path as `projections().get(id)` and `projections().list()`. Apache Iggy without a managed backend returns unsupported.

The schema registry lives in the managed runtime, and `agdx.sid` selects the registered writer schema used to decode a record. It works against LaserData Cloud or Laser Stack and returns an unsupported error on Apache Iggy without a managed backend.

`agdx.sid` (codec/decode dispatch) and `agdx.ref` (materialization routing) are separate concerns on separate headers. Producers can stamp either, both, or neither.

---

## Chapter 8 - many streams on one connection

A stream groups topics in Apache Iggy. A connection can address several streams, subject to its permissions. Select groups that fit the workload, such as data domains or environments:

```rust
let laser = Laser::connect("iggy:iggy@127.0.0.1:8090").await?;
laser.stream("gateway").topic("inferences").publish() /* ... */;
laser.stream("search").topic("inferences").publish() /* ... */;
```

Stream accessors share the connection and producer cache. They perform no I/O until an operation runs. `with_default_stream` changes the default used by `laser.topic(..)`. For application attribution within a stream, project fields such as `workspace_id` or `api_key_prefix` and filter them in queries.

---

## Chapter 9 - the agentic layer

The `agent` feature adds coordination to streaming. It supplies correlation, retries, duplicate suppression, deadlines, causality, context, and memory support. Application handlers implement the business logic.

### What the agent runtime gives you

| concern | open-SDK primitive | what it solves |
| --- | --- | --- |
| reliable consumption | `Agent::builder().handler(H).build().spawn(..)`, `ReliableConsumer` | at-least-once + idempotent. Dedup window on `agdx.idem`, retries with backoff for transient errors, dead-letter for permanent + undecodable + deadline-exceeded. `AgentId` is logical identity, `ConsumerGroupName` is replica topology and defaults from the agent id unless explicitly overridden. |
| reply correlation | `Laser::request(...).await`, `AgentCtx::respond(payload)` | request stamps a fresh `correlation_id` (Ulid) on `agdx.corr`, distinct from the business `idempotency_key` on `agdx.idem`. Responder echoes it back via `respond`. Reader filters on `agdx.corr`, so a forged reply that guesses the conversation id cannot hijack. |
| conversation + causality | `ConversationId`, `MessageId`, `Provenance.causal_parent`, `spawn_subconversation(&parent)` | a conversation is one partition (total order). Sub-conversations carry `agdx.parent_conv` + `agdx.root_conv`. Replies carry `agdx.cause`. Walk one partition for a chat. Walk the causality tree for a multi-agent flow. |
| routing | `Router::to(agent_id)` / `Router::broadcast()` | stamps `agdx.to` with the agent id, or `*` for every agent. The reliable consumer classifies each record and hands only work for its own operations to the handler. Replies, status, events, and records for other agents are skipped and committed. |
| session | `laser.sessions().create(label).agent(id).begin()` -> `(Session, SessionLease)`: `end` / `fail` / `cancel` / `run`, `model` / `tool` / `assemble`, `state()`, `context()`, `checkpoint()`, `turns_at` / `turns_since`, `state_at` / `replay` | a session is one conversation with a recorded lifecycle on `agent.sessions`. `SessionConfig` selects the stream, the layout, the idle timeout, and the heartbeat. A `Checkpoint` stores lane offsets for reads before or from that point. |
| session policy | `SessionPolicy::PerCall` / `SessionPolicy::PerUser` | per-user mode derives a stable `ConversationId` from the user key (versioned FNV-1a) so the same user keeps the same conversation across processes. |
| context assembly | `ContextAssembler::builder().conversation_id(c).policy(Box::new(LastN(20))).build().assemble(&laser)` | read one partition (or walk the causality tree with `across_subconversations`) and apply a `ContextPolicy` (`LastN`, `RoleFilter`, or your own) to feed an LLM call. Each partition read examines at most its newest 10,000 raw records before the conversation filter runs, see [client behavior](client-behavior.md). |
| log replay -> state | `ConversationState::load(laser, conv, topics, bound, init, fold)` | deterministic fold of the conversation back to current state, under an explicit `ReplayBound` (`FromOffsets` incremental, `Last(n)`, `Full` written out, or a `Checkpoint` bound through `FromCheckpoint` and `At`). `load_with(store, ..)` seeds from a `SnapshotStore` and folds only the tail past the snapshot. Same idea as event sourcing on the conversation partition. The same 10,000-record window applies, so `Full` covers a partition only when it holds no more than that. |
| memory | `Laser::memory(ns)` -> `MemoryHandle`, the one model: every `remember` / `recall` / `improve` / `forget` rides the memory topic (`agent.memory` by default) that materializes to a versioned key-value read view. `memory_topic(name).stream(..).partitions(n).ttl(d)` configures the topic. `memory_with(ns, MemoryBackend::Vector)` is the in-process similarity index for tests and offline recall. | one API, scope by agent / conversation. User isolation lives at the stream boundary. |
| state | `StateStore` trait (`get`/`set`/`delete`) + `InMemoryStore` / `FileStore`, and managed `Kv` (which implements `StateStore`) | one point-store seam for dedup persistence, checkpoints, per-agent state. `FileStore` does atomic `<file>.<ulid>.tmp` + rename. Swap in `laser.kv(ns)` for the managed durable backend, same trait. |
| stream cursor | `laser.stream(stream).topic(topic).replay()` -> `Cursor` (`poll` / `offsets` / `from_offsets` / `stream`) | resumable, offset-addressable read over the log. Checkpoint `offsets()` into any `StateStore` to resume after a restart. `stream()` drives it as a `futures::Stream` (draining then ending when caught up, the shape the Python binding exposes as `async for`). The open primitive the `Agent` runtime sits above. |
| A2A interop | `A2aBridge` (feature `a2a-bridge`, plus `a2a-http` for the axum router) | speaks Google's A2A JSON-RPC over the agent runtime. One axum route, the agent topology underneath. |

### Sessions: one unit of work

A session is one conversation with a recorded lifecycle. Its id is the conversation id, so every conversation read finds it. Bootstrap the agent topics once. `agent.sessions` needs an explicit retention, because it holds every session's records and there is no safe default.

```rust
use laser_sdk::agent::{ModelRequest, ModelResponse, TopicRetention};

laser
    .sessions()
    .bootstrap(4, TopicRetention::expire_after(Duration::from_secs(7 * 86_400)))
    .await?;

let (session, lease) = laser
    .sessions()
    .create("ticket-4821")
    .agent("planner".parse::<AgentId>()?)
    .begin()
    .await?;
let summary = session
    .run(lease, |session| async move {
        let assembled = session.assemble(Box::new(LastN(20))).await?;
        let call = session
            .model(ModelRequest::new("gpt-4o", assembled.text()), Some(&assembled))
            .await?;
        let answer = b"it is a login bug".to_vec(); // your provider call goes here
        call.complete(ModelResponse { body: answer.clone(), ..Default::default() })
            .await?;
        session.state().set("status", serde_json::json!("triaged")).await?;
        Ok(answer)
    })
    .await?;
```

`create(label)` derives the id from the stream, the namespace, and the label, so the same label reaches the same session. `start()` makes a fresh one. `begin` writes the start record and returns a `SessionLease`, which keeps the session in this process's heartbeat on `agent.heartbeats`. `run` ends the session as completed on success and as failed on an error or a panic. `end`, `fail`, and `cancel` do the same by hand. Every clone of a handle shares one terminal latch, so a retried `end` resends the same record and a later `cancel` is refused.

The SDK never calls a model. `model` writes the request, and the context manifest when you pass one, and `complete` writes the answer with its usage. `tool(name, args)` records a tool call the same way. Tool arguments and JSON request bodies pass a redactor first, which by default drops the values of keys such as `api_key` and `token`. `state()` keeps one JSON document per session as JSON Patch records on the lane.

`laser.sessions().submit(agent, input).from(me).send()` hands a new session to an agent. Inside the handler, `ctx.session()` returns that session, and `ctx.session().end()` completes it. An operator stops a session with `laser.sessions().control(stream, id).as_operator(op).cancel()`, which writes on `agent.control`. Only accounts with send permission on that topic can do it. `laser.sessions().open(id)` reads any session's lane: `context()`, `checkpoint()`, `turns_since(checkpoint)`, and `state().get()`.

On a deployment that announces the `sessions` capability, the factory also reads the managed session index: `laser.sessions().list().fetch()`, `get(id)`, `events(id).fetch()`, `state(id, 0)`, `links(id, None)`, `sources(id)`, and `watch(poll_every)`. `laser.read_at(&event.at)` fetches the record a timeline row points at, on open Apache Iggy too.

Each stream picks a layout through `SessionConfig::layout`. The default `SessionLayout::Shared` puts all work on `agent.sessions`, keyed by session. `PerAgentPartition` gives each declared agent its own partition for the work addressed to it, `PerAgentTopic` gives each agent its own topic, and `SinglePartition` keeps everything on one partition. Lifecycle and state always stay on the session's partition of `agent.sessions`. [Agents, groups, and layouts](building-agents.md#agents-groups-and-layouts) explains how to choose one.

### A handler that responds

```rust
struct Echo;

impl AgentHandler for Echo {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>)
        -> Result<(), LaserError>
    {
        ctx.respond(message.payload.clone()).await
    }
}

let mut handle = Agent::builder()
    .id("echo".parse()?)
    .listen_on(AgentTopic::Sessions)
    .respond_on(AgentTopic::Sessions)
    .handler(Echo)
    .build()
    .spawn(laser.clone());
handle.ready().await?;  // joined its group, so a publish now is delivered
```

`spawn` returns an `AgentHandle` that owns the running agent. Keep it for as long as the agent should run. Dropping the handle signals a graceful shutdown, so an agent spawned without binding the handle stops at once. Call `handle.shutdown().await?` to stop the agent and read its consumer result, or `handle.join().await?` to wait for it to finish.

### Request a reply, await the correlated response

The caller does not poll. `request` stamps the correlation key, waits on the reply topic, and returns the matching `AgentMessage`:

```rust
let reply = laser.request(
    AgentTopic::Sessions,
    AgentTopic::Sessions,
    b"summarize ticket #4821".to_vec(),
    &Provenance::builder()
        .conversation_id(ConversationId::new())
        .build(),
    Duration::from_secs(5),
).await?;

println!("got reply: {} bytes", reply.payload.len());
```

### Fan-out to sub-conversations, then aggregate

```rust
impl AgentHandler for Coordinator {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>)
        -> Result<(), LaserError>
    {
        // Enrich a request from several sources at once, each in its own
        // sub-conversation linked back to the root.
        for source in ["logs", "metrics", "traces"] {
            let child = ctx.spawn_subconversation();  // fresh conversation_id, links to root
            ctx.send(AgentTopic::Sessions, source.as_bytes().to_vec(), &child).await?;
        }
        Ok(())
    }
}
```

Each sub-conversation gets its own partition (= total order within that branch) and carries `parent_conversation_id` + `root_conversation_id` so a downstream context assembler can walk the whole tree. The `incident-desk` example's triage shows the fan-out half of this loop. It sends one deadline-bounded request per diagnostic angle and gathers the replies in the caller.

### Memory, semantic recall

`Laser::memory(namespace)` publishes memory changes to a topic. A managed deployment builds a versioned key-value view from those records. Default recall reads that view, while the topic retains history under its own policy.

```rust
// One handle per namespace, reused so recall stays incremental across calls.
let mem = laser.memory("assistant");
mem.remember(b"user prefers concise tone".to_vec())
    .scope(conv)
    .send()
    .await?;

let recent = mem.recall(conv).limit(10).fetch().await?;
```

`laser.memory_topic("assistant").stream("laser-agents").partitions(4).ttl(Duration::from_secs(7 * 86_400)).build()` configures the memory topic. Each scope maps to one partition. Topic expiry is separate from read-view retention.

`memory_with(ns, MemoryBackend::Vector).embedder(..)` provides local similarity memory without a server. It embeds records on write and ranks them by cosine similarity. The application supplies `Embedder`, as it supplies `LlmClient` for model calls.

Managed models can retain the source conversation from `gen_ai.conversation.id`. Filter with `laser.query(index).conversation(id)`, `laser.graph(name).conversation(id).neighbors(..)`, or `laser.kv(ns).scan().conversation(id)`. These filters narrow results without changing access rules. Entries without conversation metadata are excluded from a filtered KV scan. The Console can use these filters to connect conversation, memory, graph, and query views.

### Open SDK vs the managed runtime

Streaming, agents, provenance, duplicate suppression, `Cursor`, `StateStore`, and locally folded memory run on Apache Iggy. Queries, projections, KV, and forks require a managed backend. Without it, calls return `LaserError::Unsupported`.

Capabilities group support under `managed`, `query`, `destinations`, `kv`, `graph`, `forks`, `sessions`, `a2a_gateway`, `watch`, `authz`, and `filters`. The server sets `sessions` only when it serves the managed session reads. Query includes `available`, `consistency`, `keyword`, `cursor_paging`, `cancellation`, and `execution_status`. KV includes `available`, `cas`, `cas_fenced`, and `fenced_leases`. Memory combines query and graph capabilities rather than defining another group:

| concern | open SDK (this crate, Apache Iggy) | managed runtime (LaserData Cloud or Laser Stack) |
| --- | --- | --- |
| transport | one Iggy connection, publish + batch API | same connection, same wire. Adds capability negotiation at login + the query API |
| query / projections | not available, returns `LaserError::Unsupported` | picks up `Projection` + `ProjectionBinding` configuration and materializes read models served off the log |
| reliable consumption | `ReliableConsumer` with in-memory dedup + DLQ | the same `ReliableConsumer`. Effects that must happen once use a KV compare-and-swap or a fenced write |
| memory | `Laser::memory(ns)` runs here: remember publishes to the memory topic, and folded recall (`recall().folded()`, Python `recall(folded=True)`) rebuilds memory from the log in process. In-process `VectorMemory<E>` (cosine recall, bring your own `Embedder`) needs no server either | the same `Laser::memory(ns)` - a deployment materializes the topic into a versioned key-value read view for fast recall. Memory itself has no capability flag |
| sessions | `Laser::sessions` runs here: lifecycle, model and tool records, state, context, checkpoints, replay, and operator control over the session lane | the same `Laser::sessions`. A registered stream also gets a session index with lists, timelines, folded state, and links. Session memory recall reads the managed key-value view |
| forks | not available, returns `LaserError::Unsupported` | copy-on-write branches of the read model, surfaced through `Capabilities::forks` |
| A2A | `A2aBridge` axum route you self-host | managed A2A gateway with auth, streaming, persisted task store, agent-card metadata, surfaced through `Capabilities::a2a_gateway` |

Applications use the same imports for Apache Iggy, Laser Stack, and LaserData Cloud. Capability discovery identifies available operations. Managed calls return typed `Unsupported` errors when the server cannot serve them.

### Running examples for this chapter

The agentic demos under `examples/rust/src/` (run from `examples/rust` with Apache Iggy up via `just up`):

```sh
cargo run --example incident-desk  # the incident desk: triage fan-out + LLM synthesis,
                                   # semantic recall, effectively-once capacity grants behind
                                   # a durable approval, speculative fork, log-replayed audit
```

The general-purpose counterpart (`event-analytics`) lives, with per-example READMEs, in [`examples/rust/README.md`](../examples/rust/README.md).

---

## Chapter 10 - multi-agent orchestration

Chapter 9 selects sub-conversations directly. The orchestration API adds discovery and capability-based routing through the same log. The snippets in this chapter assume a glob import of `laser_sdk::prelude::full`.

### Agents advertise, the orchestrator resolves

Give an agent `capabilities` and it self-advertises a capability card on the registry when it spawns. The orchestrator folds those cards into a registry and resolves a skill to the agents that serve it.

```rust
use laser_sdk::wire::agent::{AgentCard, CapabilityDescriptor};

fn diagnose_card() -> AgentCard {
    AgentCard { capabilities: vec![CapabilityDescriptor { skill_id: "diagnose".into(), ..Default::default() }], ..Default::default() }
}

let worker = Agent::builder()
    .id("diag-alpha".parse()?)
    .listen_on(AgentTopic::Sessions)
    .respond_on(AgentTopic::Sessions)
    .capabilities(diagnose_card().capabilities)  // auto-advertises the card on spawn
    .ack_on_pickup(true)                          // emit a Working signal when a task is taken
    .handler(handler)
    .build()
    .spawn(laser.clone());
```

Keep `worker` alive while the agent should serve work, because dropping the handle stops the agent.

### A contract: one directed task with a deadline

`Laser::contract` hands a task to one capable agent and tells you whether it was consumed, completed, or timed out, with no hand-rolled correlation ids or timers. The deadline defaults to 30 seconds. `Contract::NotConsumed` is reported only when the contract sets `expire_if_not_consumed` and the target emits pickup acknowledgments through `ack_on_pickup`. `Contract::Failed` carries the target's terminal error reply.

```rust
let outcome = laser
    .contract(Router::to_capable("diagnose", RoutePolicy::Any))
    .from("orchestrator".parse()?)
    .payload(b"auth API latency spike".to_vec())
    .inbox_route(InboxRoute::Fixed(AgentTopic::Sessions))  // a managed deployment uses the default Advertised
    .deadline(Duration::from_secs(10))
    .send()
    .await?;
match outcome {
    Contract::Completed(reply) => { /* reply.body() is the finding */ }
    Contract::NotConsumed | Contract::TimedOut | Contract::Failed(_) => { /* surface it */ }
}
```

### A workflow: dependency-ordered steps, panels, and exclusivity

The workflow engine runs steps in dependency order and passes results between them. An `all_capable` step sends work to every matching agent. Budgets limit work, and the journal supports recovery. `.exclusive()` acquires a lease in `WORKFLOW_FENCE_NAMESPACE` for stale-holder rejection.

For protected external state, use `.exclusive_in(namespace)`. In the handler, use `kv(target_namespace).cas_fenced(key, namespace, run_id, token)` with the recorded token and run ID. This binds the effect to the same live lease and fence counter. Renewal starts halfway through the granted lifetime and remains bounded by lease expiry and the workflow deadline.

A workflow run is a session whose id is the run id, and every step and compensation is a child session of it. Between steps the engine checks `agent.control` for a cancel request, then compensates and returns `LaserError::Cancelled`. A contract can run as a child session too, through `ContractBuilder::parent(parent, root)`.

A completed step keeps its lease through verification and the durable journal write, then releases it. `.on_timeout(OnTimeout::Reassign)` acquires a new lease and fence before retrying with another holder. Reassignments are bounded. The default is `OnTimeout::Fail`.

```rust
let result = laser
    .workflow("incident")
    .inbox_route(InboxRoute::Fixed(AgentTopic::Sessions))
    .step("triage", Router::to_capable("triage", RoutePolicy::Any), |_ctx: &StepContext<'_>| b"incident".to_vec())
    .step("diagnose", Router::all_capable("diagnose", RoutePolicy::Any),
          |ctx: &StepContext<'_>| ctx.outputs.get("triage").cloned().unwrap_or_default())
        .after("triage")
    .run()
    .await?;
```

### Health and quarantine

Routing excludes agents that report `Unavailable`. An operator can exclude an agent with `quarantine` and restore it with `unquarantine`. Retention can also remove the recorded quarantine fact.

```rust
laser.quarantine("operator".parse()?, &"diag-alpha".parse()?).await?;
// later, once the agent is healthy again:
laser.unquarantine("operator".parse()?, &"diag-alpha".parse()?).await?;
```

Registry-topic permissions control publication. With `sign`, use `quarantine_signed` and `unquarantine_signed` for signed facts. A registry configured through `LaserBuilder::verifier(keys)` accepts those facts only with a valid operator signature. This adds a check above native topic permissions.

The `orchestra` example runs all of this end to end (a directed contract, a scatter panel, health exclusion, and quarantine), in Rust (`cargo run --example orchestra`), Python (`python orchestra.py`), and TypeScript (`npm run example:orchestra`).

---

## Chapter 11 - read only the records you need

**Configure a consumer group once, then consume using its ID.** A group-owned policy runs on the streaming server. Normal group consumers and advanced readers receive matching records. An unbound group receives every record without payload evaluation.

```rust
use laser_sdk::filters::{ConsumerFilter, FilterExpr};
use laser_sdk::prelude::CommitPolicy;
use laser_sdk::query::CmpOp;

let topic = laser.stream("agent-telemetry").topic("inferences");
let group = topic.consumer_group("on-call-review");
let policy = ConsumerFilter::json(FilterExpr::pred("latency_ms", CmpOp::Gt, 2_000));
let info = group.create().filter(policy).build().await?;

let mut consumer = topic.consumer_group_id(u64::from(info.id))
    .consumer()
    .batch_length(100)
    .commit_policy(CommitPolicy::Disabled)
    .build()
    .await?;
while let Some(record) = consumer.next().await {
    let record = record?;
    println!("{} {}", record.partition_id, record.position.offset);
    consumer.commit(&record).await?;
}
consumer.shutdown().await?;
```

The setup can run separately from every consumer instance. The consumer needs no filter definition, ID or revision. Several instances in the group share native partition assignments. Different groups keep independent progress.

**Batch length 100 examines at most 100 source records per partition request.** It can deliver zero through 100 records. Empty selections retain progress and do not mean end of stream. The advanced `group.reader()` uses a separate matching-record `count` and `max_examined` scan budget. It also supports unbound groups. Its acknowledgment includes skipped records only through completed work.

Use `group.filter().configure(...)` for later setup and its `preview`, `test`, `revisions`, `revise` and `set_revision_enabled` methods for diagnostics and administration. A draft revision does not replace a group's active policy. Use another group for a different A/B policy.

**The recorded CDC example saves 98.5% of payload transfer:** 4 of 240 records and 424 of 27,953 payload bytes. The example exists in Rust, Python and TypeScript. It demonstrates change evidence, numeric headers and JSON, CBOR, Avro and Protobuf policies.

This uses standard Iggy transport and native source reads through the optional group-aware command. Raw Iggy polling retains its standard behavior. Failed policy resolution never silently broadens a group read. Server CPU, memory and latency costs must be measured with the traffic savings.

Run the Rust example:

```sh
cargo run --example cdc
```

---

## Running locally

Use `just up` to start Apache Iggy for streaming, agents, provenance, cursors, and locally folded memory. The open integration tests use those features without a managed backend.

Queries, projections, KV, and forks require Laser Stack or LaserData Cloud. A managed backend consumes the log and serves the resulting views. Apache Iggy without that backend returns `LaserError::Unsupported`. Capability discovery identifies available features when the client connects.

---

## What the SDK ships vs what the managed runtime runs

| ships in this workspace | runs in Laser Stack or LaserData Cloud |
| --- | --- |
| the `laser-wire` contract crate (codes, envelopes, dictionaries, caps, the agent envelope, the golden fixture corpus) | the same crate, consumed as the one typed source of truth |
| publish / batch / query API | one Iggy connection, public |
| `Projection` + `ProjectionBinding` types | resolved from the cloud's deployment snapshots |
| query DSL + request/reply envelope | served over the managed command band on the same connection |
| managed KV client (`kv` feature, `Laser::kv`) + registry browse (projections via `projections().get` / `projections().list`, writer schemas via `schemas().get` / `schemas().list`) | the `AGDX_KV_*` / `AGDX_*_PROJECTION` / `AGDX_*_SCHEMA` managed commands, served by Laser Stack or LaserData Cloud |
| group-owned consumer policies: normal and advanced readers with fenced acknowledgments, preview/test and revision management, with optional local evaluation under `filters` | the `AGDX_FILTER*` commands evaluated by the LaserData Iggy fork next to the data, and the saved-filter catalog with group bindings kept by `laser-plane` |
| `Codec<T>` trait + `Json` + `Msgpack` + `Cbor` + `Bson` | identical wire. Codecs run on the producer side. Schema-first codecs resolve their writer schema from the managed registry |
| reliable agent runtime | same agent runtime can run inside cloud services |
| projection, binding, and schema registry control (`laser.projections()`, `laser.bindings()`, `laser.schemas()`) | the long-running managed projector applies the control commands and materializes the read models |

## Handling cluster outages

All three clients support configurable limits on publish retries. A long-running application must handle exhausted retries and retain work for a later attempt. See [publish recovery](publish-recovery.md) for defaults, language examples, and delivery behavior.
