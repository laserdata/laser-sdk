# Laser SDK tutorial

A progressive, hands-on guide to the SDK. Each chapter builds on the last using one running scenario: a real-time agent/LLM observability pipeline capturing every model call your agents make, queryable by latency, outcome, model, and user. By Chapter 8 you can publish a heterogeneous batch in one network round-trip, query it by user/model/outcome, range-aggregate latency and tokens, and recall the nearest past incidents by embedding similarity, all over the same Iggy connection. Chapter 9 layers the agentic runtime on top of the same substrate.

Prerequisites: the install snippet from the [README](../README.md) and a local Apache Iggy (`docker run -p 8090:8090 apache/iggy:latest`, or `just up` from the repo root).

---

## Two layers, one connection

Laser SDK is a **generic** publish/query substrate plus an **agentic** runtime built on top:

| layer | what it is | when you need it |
| --- | --- | --- |
| **generic** (`query` feature) | typed publish, declared projections, query DSL with filters / aggregates / vector recall. No agent concepts. | agent / LLM observability, analytics, audit logs, market data, IoT, anywhere you stream typed messages and want to query them. |
| **agentic** (`agent` feature, default) | reliable consumer + DLQ, conversation/causality, `Router`, `Memory`, `Agent::builder` handlers. Builds on the generic layer. | When you are orchestrating LLM agents, not just observing traffic. |

Chapters 1-8 use ONLY the generic layer. Chapter 9 adds the agentic layer for those who graduate from observation to coordination.

Both layers stand on one foundation: **Apache Iggy**, low-latency message streaming. The log is the source of truth. Writes ride it, and the LaserData Cloud features (projections, query, KV, forks) serve ephemeral reads *off* it. Two SDK-side helpers carry no new wire and work against any Apache Iggy: `Laser::reader(topic)` gives a resumable, offset-addressable `Cursor` over the log, and the `StateStore` seam (`get`/`set`/`delete`: `InMemoryStore`, `FileStore`, or managed `Kv`) holds point state like cursor checkpoints and dedup keys.

laser-sdk is built **on** the Apache Iggy SDK and never hides it. When you want Iggy's own producer/consumer power directly - the rich builders and the async `Stream` you loop over with `StreamExt::next` - reach for `laser.iggy_producer`, `laser.iggy_consumer`, and `laser.iggy_consumer_group` (or `laser.client()` for the whole `IggyClient`). The query / KV / agentic layers build on top of that, they do not replace it. See the [`order-book`](../examples/rust/src/order-book/README.md) example's live book and the [`event-analytics`](../examples/rust/src/event-analytics/README.md) hot path.

---

## Chapter 1 - publish your first message

Connect to Apache Iggy, push a single typed message onto a topic. That is it. At this point Iggy has the bytes durably on the log. Nothing is queryable yet because no index exists for that topic. Chapter 2 makes it queryable.

```rust
use laser_sdk::prelude::*;
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
    // Connect, and pin "agent-telemetry" as the default stream so the calls
    // below take just a topic. `Laser::connect(conn)` (no stream) plus
    // `publish_on(stream, topic)` is the form for talking to many streams on one
    // connection. No scheme needed - the SDK defaults to Iggy over TCP, and
    // against LaserData Cloud TLS is auto-attached.
    let laser = Laser::connect_with_stream("iggy:iggy@127.0.0.1:8090", "agent-telemetry").await?;

    laser.publish("inferences")
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

The running example is an **agent/LLM observability** stream: every model call your agents make is one `Inference` message. The connection string is the only thing `connect` needs - one Iggy connection that publish, query, and (later) agent traffic share.

A **stream** is the Iggy namespace one layer above a topic. You can use anywhere from one to thousands of streams on a single connection (by data domain, by environment, or just one), exactly as Apache Iggy does. Two equivalent ways to say which stream:

- `Laser::connect(conn)` (connection only) then `laser.publish_on(stream, topic)`
  - name the stream per call. Best when one connection talks to many streams.
- `Laser::connect_with_stream(conn, stream)` (or `laser.with_stream(stream)` to re-scope any connection) pins a **default** stream, so the shorter `laser.publish(topic)` and the agentic helpers take just a topic. This chapter uses that form.

`.json(&body)` encodes the value, stamps the compact `agdx.ct` codec code (`json` = `1`), and sends the message on the topic.

The bytes are on the Iggy log forever (or until retention rotates them out), partitioned by hash of the conversation/key and replayable from offset 0. They are not yet indexed. A `laser.query("inferences")` right now would return nothing because LaserData Cloud has never been told what to materialize from this topic. That is the next chapter.

---

## Chapter 2 - declare a projection, query the topic

In a database you run `CREATE INDEX ON inferences(latency_ms)` once, then `INSERT` rows and the engine extracts the indexed columns from each row automatically. Same model here. A `Projection` declares which fields are indexed and where to find them in the payload. The producer code from Chapter 1 does not change.

```rust
use laser_sdk::prelude::*;

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

This declaration lives once, in your infra repo, and ships to LaserData Cloud through your control workflow. The cloud's managed projector picks it up and starts materializing rows. From that point on, every message your producer publishes to `inferences` lands on the queryable index.

### Three storage tiers

A published record lives in up to three places, controlled by the projection:

1. **Iggy log** (always): the original wire bytes, partitioned, replayable from offset 0. The source of truth.
2. **Indexed columns** (always): the scalar fields you declared via `.fields([...])` / `.field_at(...)`, extracted from the payload at materialize time. These drive `where_eq` / `filter_*` / `order_*` / aggregates.
3. **Inline body** (default ON): a copy of the full original payload alongside the row, so `fetch_typed::<T>()` decodes back into your struct without going back to the Iggy log. Opt out per projection with `.index_only()` when the body is large or already stored elsewhere. The Iggy log keeps the bytes either way.

The body may carry fields that are NOT indexed. Only the declared fields are queryable. Everything else is retrievable through the inline body (when on) or by Iggy replay (when off).

> *With `.index_only()`, `fetch_typed::<T>()` cannot decode rows because their `payload` is `None`. Callers either use `.fetch()` and decode indexed columns directly off `Row.headers` / `Row.metadata`, or replay from the Iggy log. Plan the trade-off when you declare the projection, not at query time.*

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

`laser.query("inferences")` runs across the same Iggy connection. Query is a **LaserData Cloud feature**. Against raw Apache Iggy `laser.query(...)` returns `LaserError::Unsupported`: the default `OPEN` capability set leaves `managed_query` false, so nothing answers the request and the SDK never silently fakes a result off the log. KV, forks, and registry browse behave the same way.

What runs on raw Apache Iggy is the open SDK's streaming, agent, provenance, dedup, cursor, and log-backed memory surfaces. The query / KV / fork / projection surfaces require LaserData Cloud. The typed result decodes straight back into your struct.

LaserData Cloud serves two more read surfaces, both answering `LaserError::Unsupported` against raw Apache Iggy:

- **Managed key-value store** (`kv` feature, `Laser::kv`, gated on `Capabilities::managed_kv`): `get` / `set` / `delete` / `scan` with optional expiry, arbitrary opaque byte keys/values, namespaced and user-scoped, backed by LaserData Cloud's managed point-state store. Values take the same codecs as publish - `.bytes` (raw), `.json`, `.msgpack`, `.encode_with::<C>` - and read back with `get` (bytes), `get_typed` (JSON), or `get_as::<C, _>` (any codec). See `the AGDX spec` §14.
- **Registry browse** (`laser.projections().get(id)` / `laser.projections().list().fetch()` for projections, `laser.schemas().get(id)` / `laser.schemas().list()` for writer schemas, gated on `Capabilities::managed_host`): read back which projections and registered writer schemas (Avro/Protobuf/JSON Schema) exist and their full shape. Projection and schema CRUD stay writes on the control topic. See `the AGDX spec` §15.

> *Running this tutorial's snippets locally, without the cloud? The example crate ships a single-process projector you can spawn next to your code. See [Running locally](#running-locally) at the bottom of the reference section.*

### Projection retention, decoupled from topic expiry

A topic's Iggy `message_expiry` controls how long the **log** keeps the raw bytes. By default a projection mirrors that: when Iggy drops a message, LaserData Cloud prunes the row it produced. But the log and the read-model are different products with different lifetimes - you often want short-lived partitions (cheap storage, fast replay) feeding a **permanent** index. Set the binding's retention to decouple them:

```rust
let binding = ProjectionBinding::builder()
    .source("agent-telemetry", "inferences")
    .allow("inference.v1")
    .default_projection("inference.v1")
    .retention(RetentionPolicy::Keep)   // index survives even after the log expires
    .build();
```

`RetentionPolicy` variants:

- `MirrorLog` (default) - follow the log, and also drops the projection when the source **topic is deleted**.
- `Keep` - rows live forever, regardless of log expiry **or** topic deletion.
- `KeepUntilSourceDeleted` - ignore message expiry (keep forever), **but** drop the projection when the source topic is deleted. For "permanent index, but it's meaningless once the topic is gone."
- `TimeToLive { ttl_micros }` - keep rows for a fixed age after they were materialized, independent of the log.
- `MaxRows { rows }` - keep only the newest N rows for the table.

Leave `.retention(...)` unset to inherit LaserData Cloud's fleet-wide default. The policy is enforced by LaserData Cloud. See `the AGDX spec` §8.

### Why the producer does not stamp `.index(...)` per record

A projection is a **read-model contract**. If producers were stamping `.index("user_id", "alice")` per record they would be:

- duplicating field names on every message (wire cost),
- coupled to projector internals (refactor pain),
- able to disagree with each other on what the schema is.

The projection avoids all three. The producer ships data. The projection is the schema.

> *Niche scenario, the producer needs to surface a queryable field on a payload the projector cannot decode (opaque binary, custom framing). For those, the projection can declare a header-source field and the producer stamps it via `.header("trace_id", id)` as ride-along metadata. Same "schema lives on the projector side" principle, header instead of JSON pointer. Not used in the rest of the tutorial.*

---

## Chapter 3 - real-time batches

A busy agent fleet emits thousands of inferences per second. Per-message publishes are not the path. Batches are. **One `publish_batch().send()` is one Iggy `send_messages` network call.** The fluent chain composes records in memory. Nothing leaves the process until `.send().await?`.

```rust
let drained: Vec<Inference> = drain_trace_buffer(Duration::from_secs(1));

laser.publish_batch("inferences")
    .extend_json(drained.iter())?    // N records, in memory
    .send().await?;                  // ONE send_messages, N records
```

Batch size is bounded by what Iggy will accept on a single `send_messages` call (Apache Iggy's max-message-size budget summed across the records), not by an arbitrary record count cap. Drain larger windows by splitting the producer-side queue into multiple batches.

Partitioning composes with the batch:

- **No `partition_key`** (default), Iggy's balanced partitioner picks one partition for the whole `send_messages` call. Throughput-friendly.
- **`.partition_key("alice")`**, the entire batch is hashed to one partition, preserving per-user ordering across records.
- **One-partition topic**, global order across the whole topic, useful for the heterogeneous-message pattern in Chapter 4.

A query for more than `MAX_PAGE_SIZE` (1000 rows) is rejected with `QueryError::TooLarge` rather than silently truncated, and the reply is bounded to 64 MiB as it is built, so a runaway query cannot blow up the wire or be mistaken for the whole answer. Walk larger result sets with the auto-paginating `.stream()` / `.fetch_all()` covered in Chapter 5.

The projection from Chapter 2 covers every record in this batch. No new declaration needed.

---

## Chapter 4 - heterogeneous topic, mixed message shapes

Real agents produce more than one shape of message on the same stream. Same topic, same partition (for ordering), three shapes: inferences, tool calls, errors. Declare each shape as its own projection. Bind all three to the same topic. The projector routes per record by `agdx.ref` (the projection-ref header).

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
laser.publish_batch("agent_trace")
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
// Each group row carries the count under headers["count"].

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

// Page-walking with the auto-paginating stream:
let mut rows = laser.query("inferences")
    .where_eq("user_id", "alice")
    .order_desc("latency_ms")
    .stream();
while let Some(row) = rows.next().await? {
    process(row);
}

// Or materialize the whole result set in one call:
let all: Vec<Inference> = laser.query("inferences")
    .where_eq("provider", "anthropic")
    .fetch_all_typed().await?;
```

Fluent surface, `where_eq` / `filter_eq` / `filter_ne` / `filter_gt` / `filter_gte` / `filter_lt` / `filter_lte` / `filter_in` / `filter_contains` / `filter_prefix` / `filter(Filter)` (compose `Any`/`Not` trees) / `time_range` / `order_asc` / `order_desc` / `limit` / `offset` / `with_payload` / `select_fields` / `distinct` / `count` / `count_distinct` / `sum` / `avg` / `min` / `max` / `stddev` / `percentile` / `agg_as` / `group_by` / `window` / `having` / `raw_sql` / `raw_sql_with` / `nearest` / `nearest_in`.

Terminals, `.fetch()` (paged), `.fetch_typed::<T>()` (`Vec<T>`), `.fetch_one::<T>()` (`Option<T>`), `.stream()` / `.stream_typed::<T>()`, `.fetch_all()` / `.fetch_all_typed::<T>()`.

---

## Chapter 6 - vector recall

Same wire, same DSL. A new projection declares which payload field carries the embedding (default `/embedding`). The projector extracts it at materialize time so queries never re-embed.

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

laser.publish("incidents")
    .json(&Incident { /* ... */ })?
    .send().await?;
```

Consumer finds past incidents similar to a new one:

```rust
let nearest: Vec<Incident> = laser.query("incidents")
    .where_eq("service", "payments")
    .nearest(query_embedding, 5)
    .fetch_typed().await?;
```

The `Memory` trait in the SDK wraps this same path. Reach for `QueryMemory` when you want a single API for remember / recall across many topics. Reach for `query().nearest(..)` when you want full control.

---

## Chapter 7 - codecs, JSON, MessagePack, Avro, Protobuf, your own

`ContentType` is the wire tag stamped on `agdx.ct` as a compact `u8` code (`ContentType::code`). `Codec<T>` is the trait that abstracts "encode this `T` + tell me the tag". Four first-party codecs ship: `Json`, `Msgpack`, `Cbor`, and `Bson`. All four are self-describing, so LaserData Cloud's projector can index their fields with no schema declared up front. For a schema-first format (Avro, Protobuf), Arrow, or your own framing, implement the trait once or hand bytes via `.raw_bytes(...)`.

```rust
// First-party shortcuts (JSON and MessagePack have builder sugar):
laser.publish("inferences").json(&inference)?.send().await?;
laser.publish("inferences").msgpack(&inference)?.send().await?;

// Generic dispatch works for every codec, including CBOR and BSON:
use laser_sdk::query::{Bson, Cbor, Json, Msgpack};
laser.publish("inferences")
    .encode_with::<Cbor, _>(&inference)?
    .send().await?;
laser.publish("inferences")
    .encode_with::<Bson, _>(&inference)?
    .send().await?;
```

Reading is symmetric. `Codec` encodes. `Decoder` decodes. All four built-in codecs (`Json`, `Msgpack`, `Cbor`, `Bson`) implement both halves. `fetch_typed` defaults to JSON. `fetch_typed_with::<C, _>` (and `fetch_one_with`, `Row::decode_with`) takes any codec, so a topic written with MessagePack reads back with MessagePack:

```rust
let traces: Vec<Inference> = laser.query("inferences").fetch_typed_with::<Msgpack, _>().await?;
```

Payload bytes cross the public API as `Vec<u8>` (`Row.payload`, `Message.payload`, `MemoryItem.payload`), and byte inputs take `impl AsRef<[u8]>` / `impl Into<Vec<u8>>` - the SDK never leaks the `bytes` crate into your signatures.

### A custom codec (Avro example)

```rust
use laser_sdk::query::{Codec, ContentType};
use laser_sdk::LaserError;

pub struct AvroCodec;
impl<T: my_avro::AvroSerialize> Codec<T> for AvroCodec {
    fn content_type() -> ContentType { ContentType::Avro }
    fn encode(value: &T) -> Result<Vec<u8>, LaserError> {
        my_avro::to_bytes(value).map_err(|e| LaserError::Query(format!("avro: {e}")))
    }
}

laser.publish("inferences")
    .encode_with::<AvroCodec, _>(&inference)?
    .send().await?;
```

### Mixed codecs in one batch

```rust
laser.publish_batch("agent_trace")
    .add_encoded::<Json, _>(&inference)?                      // JSON
    .add_encoded::<Msgpack, _>(&tool_call)?                   // MessagePack
    .add_raw_bytes(embedding_bytes, ContentType::Avro)        // pre-encoded
    .add_encoded_with_projection::<Json, _>("agent.error.v1", &error)?
    .send().await?;
```

The `agdx.ct` header code on each record tells the consumer how to decode. The batch-wide `.content_type(...)` directive applies to records that do not carry their own. Without either, no `agdx.ct` is stamped at all and the payload rides as opaque bytes.

### Self-describing vs schema-first codecs

The four built-in codecs (`Json`, `Msgpack`, `Cbor`, `Bson`) are self-describing: the bytes carry their own field names, so LaserData Cloud's projector indexes them with nothing declared in advance.

Schema-first formats (Avro, Protobuf) carry no field names in the body, so the projector needs the writer schema to decode them. LaserData Cloud keeps registered writer schemas for exactly this. You register a writer schema once, keyed by a `u32` id, then a producer stamps that id on the `agdx.sid` header (`u32` typed value) so LaserData Cloud resolves the schema and decodes the body. A record whose schema-first codec has no registered schema is indexed only from its `agdx.idx.*` headers (the body is left opaque). A third source kind, `JsonSchema { schema }` (draft 2020-12), covers the self-describing codecs: they decode without a schema, but a record stamping a JSON Schema's id has its decoded payload validated by LaserData Cloud - a mismatch never materializes body fields and shows up in LaserData Cloud's `/health` counters (and the DLQ when the policy says so).

Registration is synchronous and LaserData Cloud allocates the id:

```rust
let schema_id = laser
    .schemas()
    .register(SchemaSource::Avro { schema: ORDER_AVRO_SCHEMA.to_owned() })
    .send()
    .await?;
```

LaserData Cloud validates that the definition compiles, allocates the next free id (concurrent callers never collide), durably appends the control event, and returns the id - producers then stamp it on `agdx.sid`. `laser.schemas().drop(id)` tombstones it asynchronously. `SchemaSource` is `Avro { schema }` (the canonical Avro JSON text), `Protobuf { descriptor_set, message_type }` (a compiled `FileDescriptorSet` plus the fully-qualified message type to decode), or `JsonSchema { schema }`. The returned id is durable but applies asynchronously, so read back (below) before the first publish against a fresh id. The [`order-book`](../examples/rust/src/order-book/README.md) Avro tape and the [`event-analytics`](../examples/rust/src/event-analytics/README.md) JSON Schema guard walk both paths end to end.

Ids are permanent. A schema change is always a NEW register (and producers move to stamping the new id), never an in-place replacement: re-keying an id would change how every record already stamped with it decodes. Dropping tombstones the id - records stamped with it keep decoding and the id stays reserved (re-registering the identical definition on the raw control topic revives it. A different definition is rejected and dead-lettered).

To read back, `laser.schemas().list()` returns every known writer schema as `Vec<SchemaInfo>` (`SchemaInfo { schema, dropped }` carries the lifecycle flag) and `laser.schemas().get(id)` returns the `Option<SchemaInfo>` occupying an id. Both are read-only browse calls over the same managed bridge as projection browse (`projections().get(id)` / `projections().list()`), and behave the same way off the cloud: they answer an unsupported error against raw Apache Iggy.

The registry is a bridge: it lives in LaserData Cloud until Iggy gains native schema support, at which point `agdx.sid` becomes an infrastructure-native dispatch key. It works against LaserData Cloud, or returns an unsupported error elsewhere.

`agdx.sid` (codec/decode dispatch) and `agdx.ref` (materialization routing) are separate concerns on separate headers. Producers can stamp either, both, or neither.

---

## Chapter 8 - many streams on one connection

A stream is the Iggy namespace one layer above topics. Scope a connection to any stream and its topics, consumer groups, replay, and projections all stay inside that boundary. You pick the grouping - from one to thousands of streams, all sharing one connection. Group by whatever fits the workload (data domain, environment, or not at all):

```rust
let laser     = Laser::connect("iggy:iggy@127.0.0.1:8090").await?;
let checkout  = laser.with_stream("checkout");
let search    = laser.with_stream("search");
// checkout.publish("inferences")...  /  search.publish("inferences")...
```

`with_stream` is a cheap re-scope: the connection and producer cache are shared, so spinning up a handle per stream is free. If you'd rather attribute inside one stream, stamp it via an indexed projection field (`workspace_id`, `api_key_prefix`, etc.). The query DSL filters on it like any other.

---

## Chapter 9 - the agentic layer

Everything above moves typed messages and queries them. This chapter turns the SDK into a coordination layer for **LLM agents** built on the same one-connection-to-Iggy substrate. The agentic feature is the default for a reason. Once you graduate from observing traffic to making decisions about it, the runtime below is what catches the hard parts (correlation, retries, dedup, deadlines, causality, context, memory) so your handler code stays a function from input to output.

### What the agent runtime gives you

| concern | open-SDK primitive | what it solves |
| --- | --- | --- |
| reliable consumption | `Agent::builder().handler(H).spawn(..)`, `ReliableConsumer` | at-least-once + idempotent. Dedup window on `agdx.idem`, retries with backoff for transient errors, dead-letter for permanent + undecodable + deadline-exceeded. One agent = one Iggy consumer group. |
| reply correlation | `Laser::request(...).await`, `AgentCtx::respond(payload)` | request stamps a fresh `idempotency_key` (Ulid). Responder echoes it back via `respond`. Reader filters on `agdx.idem`, so a forged reply that guesses the conversation id cannot hijack. |
| conversation + causality | `ConversationId`, `MessageId`, `Provenance.causal_parent`, `spawn_subconversation(&parent)` | a conversation is one partition (total order). Sub-conversations carry `agdx.parent_conv` + `agdx.root_conv`. Replies carry `agdx.cause`. Walk one partition for a chat. Walk the causality tree for a multi-agent flow. |
| routing | `Router::to(agent_id)` / `Router::broadcast()` | stamps / clears `agdx.to`. Defensive filter at the consumer side, see the consumer-group note above. |
| sessions | `SessionPolicy::PerCall` / `SessionPolicy::PerUser` | per-user mode derives a stable `ConversationId` from the user key (versioned FNV-1a) so the SAME user keeps the SAME conversation across processes. |
| context assembly | `ContextAssembler::builder().conversation_id(c).policy(LastN(20)).assemble()` | read one partition (or walk the causality tree with `across_subconversations`) and apply a `ContextPolicy` (`LastN`, `RoleFilter`, or your own) to feed an LLM call. |
| log replay -> state | `ConversationState::load(laser, conv, topics, init, fold)` | deterministic fold of the conversation back to current state. Same idea as event sourcing on the conversation partition. |
| memory | `Memory` trait + `LogMemory` (log-backed, incremental recall), `VectorMemory<E: Embedder>` (semantic), `QueryMemory` (over LaserData Cloud), `KvMemory` (durable point state + TTL, feature `kv`) | the same `remember` / `recall` / `forget` API across all four. Scope by agent / conversation. User isolation lives at the stream boundary. |
| state | `StateStore` trait (`get`/`set`/`delete`) + `InMemoryStore` / `FileStore`, and managed `Kv` (which implements `StateStore`) | one point-store seam for dedup persistence, checkpoints, per-agent state. `FileStore` does atomic `<file>.<ulid>.tmp` + rename. Swap in `laser.kv(ns)` for the managed durable backend, same trait. |
| stream cursor | `Laser::reader(topic)` -> `Cursor` (`poll` / `offsets` / `from_offsets`) | resumable, offset-addressable read over the log. Checkpoint `offsets()` into any `StateStore` to resume after a restart. The open primitive the `Agent` runtime sits above. |
| A2A interop | `A2aBridge` (feature `a2a-bridge`) | speaks Google's A2A JSON-RPC over the agent runtime. One axum route, the agent topology underneath. |

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

Agent::builder()
    .id("echo".parse()?)
    .listen_on(AgentTopic::Commands)
    .respond_on(AgentTopic::Responses)
    .handler(Echo)
    .build()
    .spawn(laser.clone());
```

### Request a reply, await the correlated response

The caller does not poll. `request` stamps the correlation key, waits on the reply topic, and returns the matching `AgentMessage`:

```rust
let reply = laser.request(
    AgentTopic::Commands,
    AgentTopic::Responses,
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
        for source in ["crm", "billing", "support"] {
            let child = ctx.spawn_subconversation();  // fresh conversation_id, links to root
            ctx.send(AgentTopic::Commands, source.as_bytes().to_vec(), &child).await?;
        }
        Ok(())
    }
}
```

Each sub-conversation gets its own partition (= total order within that branch) and carries `parent_conversation_id` + `root_conversation_id` so a downstream context assembler can walk the whole tree. The `concierge` example's triage fan-out shows the full loop, including aggregating the replies at the root.

### Memory, semantic recall

```rust
// LogMemory writes to the Audit topic, scoped by agent + conversation. Recall is
// incremental - hold one instance and it folds only what is new since the last
// call, never from offset 0. It owns the Laser (cheap to clone, shared connection).
let mem = LogMemory::new(laser.clone());
let scope = MemoryScope::builder()
    .conversation(conv)
    .agent("assistant".parse()?)
    .build();
mem.remember(&scope, b"user prefers concise tone".to_vec()).await?;

let recent = mem.recall(&scope, &MemoryQuery::builder().limit(10).build()).await?;
```

Switch to `VectorMemory<E>` / `QueryMemory<E>` for cosine-similarity recall (the `Embedder` trait is the model seam, the same way `LlmClient` keeps model calls out of the SDK), or `KvMemory` for durable, TTL-able point-state memory on LaserData Cloud's KV store. All four are the same `remember` / `recall` / `forget` API.

### Open SDK vs LaserData Cloud (the managed runtime)

The open streaming surface above - publish, batch, consume, the agentic runtime, provenance, dedup, the `Cursor`, `StateStore`, and log-backed memory - runs against raw Apache Iggy, and is what you copy out of this repo. The query / projection / KV / fork surface is **not** open: it returns `LaserError::Unsupported` against raw Apache Iggy and only works against LaserData Cloud. The same `Laser` handle keeps working either way - the managed capabilities light up when the connected streaming infrastructure advertises a richer set. Capabilities are grouped, not flat: a root `managed` flag, then nested surfaces (`query` with its `available`/`projections`/`schemas`/`consistency`, `kv` with `available`/`cas`, `graph`, `forks`) plus `sessions`, `durable_dedup`, and `a2a_gateway`. They map to the **LaserData Cloud** managed runtime (LaserData's managed streaming runtime). Agentic memory has no capability of its own: it composes `query` and `graph`. The split:

| concern | open SDK (this crate, raw Apache Iggy) | LaserData Cloud (managed runtime) |
| --- | --- | --- |
| transport | one Iggy connection, publish + batch API | same connection, same wire. Adds capability negotiation at login + the query API |
| query / projections | not available, returns `LaserError::Unsupported` | the long-running LaserData Cloud, picks up `Projection` + `ProjectionBinding` config and materializes per-user read models, served off the log |
| reliable consumption | `ReliableConsumer` with in-memory dedup + DLQ | infrastructure-side durable dedup primitives surfaced through `Capabilities::durable_dedup` |
| memory | `LogMemory` (folds the log) + `VectorMemory<E>` (in-memory cosine recall, bring your own `Embedder`) | `QueryMemory<E>` (durable vector recall, gated on `query.available`) + `KvMemory` (durable point-state, gated on `kv.available`). The graph-backed memory composes `query` + `graph`. Memory itself has no capability flag |
| sessions / forks | not available, returns `LaserError::Unsupported` | infrastructure-native session start + fork-from primitives, surfaced through `Capabilities::sessions` + `Capabilities::forks` |
| A2A | `A2aBridge` axum route customers self-host | managed A2A gateway with auth, streaming, persisted task store, agent-card metadata, surfaced through `Capabilities::a2a_gateway` |

The contract: **your app's imports do not change** when you move from raw Apache Iggy to LaserData Cloud. Capability negotiation flips the internal seams, and a managed call against raw Apache Iggy is a typed `Unsupported`. The managed runtime is LaserData Cloud, never a separate client crate the app has to import.

### Running examples for this chapter

The agentic demos under `examples/rust/src/` (run from `examples/rust` with Apache Iggy up via `just up`):

```sh
cargo run --example concierge   # the AI support desk: triage fan-out + LLM synthesis,
                                # semantic recall, effectively-once credits behind a
                                # durable approval, speculative fork, log-replayed audit
```

The general-purpose counterpart (`event-analytics`) lives, with per-example READMEs, in [`examples/rust/README.md`](../examples/rust/README.md).

---

---

## Running locally

The streaming core (publish, consume, the agent runtime, provenance, the `Cursor`, log-backed memory) runs against a raw Apache Iggy you start with `just up`, and every example and integration test exercises it there with no managed backend.

Query, projections, key-value, and forks are the managed surface. They run against LaserData Cloud, which materializes the projection and serves the query off the log. There is no in-process query worker and no local query path. Against raw Apache Iggy these calls return `LaserError::Unsupported`, so to run the query chapters point the example at a LaserData Cloud deployment (set the connection variables in the example README). The same code runs unchanged in both cases. The capability handshake at connect decides what is available.

---

## What the SDK ships vs what LaserData Cloud runs

| ships in this workspace | runs in LaserData Cloud |
| --- | --- |
| the `laser-wire` contract crate (codes, envelopes, dictionaries, caps, the agent envelope, the golden fixture corpus) | the same crate, consumed as the one typed source of truth |
| publish / batch / query API | one Iggy connection, customer-facing |
| `Projection` + `ProjectionBinding` types | resolved from the cloud's deployment snapshots |
| query DSL + request/reply envelope | served from the `_agdx` internal stream |
| managed KV client (`kv` feature, `Laser::kv`) + registry browse (projections via `projections().get` / `projections().list`, writer schemas via `schemas().get` / `schemas().list`) | the `AGDX_KV_*` / `AGDX_*_PROJECTION` / `AGDX_*_SCHEMA` managed commands, served by LaserData Cloud |
| `Codec<T>` trait + `Json` + `Msgpack` + `Cbor` + `Bson` | identical wire. Codecs run on the producer side. Schema-first codecs resolve their writer schema from LaserData Cloud's registry |
| reliable agent runtime | same agent runtime can run inside cloud services |
| example projector (header path) + test projector (registry path) | the long-running managed projector under Operator |
