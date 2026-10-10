# LaserData - Laser SDK

This package provides the Python Laser SDK for Apache Iggy. [LaserData, Inc.](https://laserdata.com) maintains it. PyO3 exposes the Rust SDK to Python, so both clients use the same data contract, codecs, and runtime.

Rust and Python share the data contract and Rust implementation. The bindings expose Python forms of the SDK operations, configuration, and errors. Shared examples and behavior scenarios cover language-neutral behavior.

> The current release is `0.7.0`. The wire contract and public API use semantic versioning. Before `1.0.0`, minor releases can contain breaking changes.

`spawn_agent(agent_id, ..., consumer_group=None)` separates agent identity from its consumer group. The default group uses the agent ID spelling. Set `consumer_group` when the deployment needs a different group.

One Apache Iggy connection supports streaming and managed operations for queries, key-value state, graphs, and forks. The optional AGDX agent runtime sends messages and runs asynchronous handlers. It supports at-least-once delivery, per-conversation ordering, duplicate suppression, retries, and dead letters.

Apache Iggy is the underlying streaming core. Projections, the query layer, the key-value store, the knowledge graph, and forks are served by Laser Stack or LaserData Cloud over that same connection. Against Apache Iggy without a managed backend those calls raise `UnsupportedError`.

## Install

```bash
pip install laser-sdk
```

Wheels ship for Linux (x86_64, aarch64) and macOS (Intel, Apple Silicon), Python 3.10 and later.

Every wheel and source build uses Apache Iggy's native VSR transport. Standard streaming commands, LaserData's non-replicated managed command band, and dedicated replicated authorization operations use the same connection.

## Connect

```python
import asyncio
from laser_sdk import Laser


async def main():
    laser = await Laser.connect("iggy:iggy@127.0.0.1:8090")
    readings = laser.stream("fleet").topic("readings")
    await readings.ensure(partitions=4)
    caps = await laser.capabilities()
    print(caps.query.available, caps.kv.cas, caps.graph)


asyncio.run(main())
```

Use a `user:password@host[:port]` or `token@host[:port]` connection string, or pass `address=` with `credentials=(user, password)` instead. The port defaults to 8090. Only the `iggy://` and `iggy+tcp://` schemes and Apache Iggy's TCP options are accepted, credentials are required, and an unknown or repeated option raises `ConfigError` before dialing. `tls_ca_file=` alone turns TLS on. `Laser.connect_env()` reads `LASER_CONNECTION_STRING` and an optional `LASER_STREAM`, `Laser.local()` connects to `iggy:iggy@127.0.0.1:8090`, and `Laser.connect_with_stream(conn, stream)` pins a default stream. The SDK supplies the Apache Iggy TCP scheme. Select a stream with `laser.stream(name)` and a topic with `.topic(name)`. The optional `stream=` selects a default for the shorter `laser.topic(name)` form. It does not restrict access to other streams. Accessors select objects, and operations such as `publish`, `replay`, and `ensure` perform I/O.

Python uses the Rust client reconnect policy. TCP connections retry a failed dial and reconnect dropped sockets, by default without limit at one-second intervals. The 30-second connect budget stops the retries of the initial connection. Set `reconnection_retries=<count|unlimited>` and `reconnection_interval=<duration>` in the connection string. After reconnecting, the client reapplies those credentials.

`Laser.connect` calls Rust `Laser::connect`. Hosts under `*.laserdata.cloud` and `*.laserdata.com` use TLS with the bundled LaserData root CA. `LASER_TLS_CERT=<path>` selects an explicit certificate. `LASER_NO_TLS=1` disables automatic TLS. Other hosts retain their connection-string configuration.

LaserData Cloud and Laser Stack enable managed surfaces only when their backend announcement reports ready. `await laser.refresh_capabilities()` re-probes a long-lived connection after startup or a backend restart. The returned `Capabilities` groups each managed surface: `caps.query` is a `QueryCaps` (`available`, `consistency`), `caps.kv` a `KvCaps` (`available`, `cas`, `fenced_leases`), `caps.filters` a `FilterCaps`, and `caps.destinations` a `DestinationCaps`. It also carries `versions: OpVersions | None` and the advertised backends. Apache Iggy keeps every managed surface off and reports no operation versions.

## Connect timeout

`Laser.connect` gives up after 30 seconds. The budget covers the dial, the TLS handshake, the login, and the capability probe. Pass `connect_timeout_ms` to use another budget, or set `LASER_CONNECT_TIMEOUT_MS`. The argument overrides the variable. An expired budget raises `TimeoutError` that says whether the server never accepted the connection or never answered the login. `laser.stream(name).delete()` removes a stream you no longer need, and `await laser.close()` ends the shared connection. See [connect timeout and cleanup](../../docs/connect-timeout.md).

Every relative duration in the Python SDK is in milliseconds and its keyword ends in `_ms`, for example `timeout_ms`, `ttl_ms`, `linger_ms`, `wait_ms`, and `TopicRetention.expire_after(age_ms)`, matching the TypeScript SDK. A fraction of a millisecond is kept, and a negative or non-finite value raises `InvalidError`. Absolute times and wire fields stay in epoch microseconds (`*_micros`).

## Publish and consume

```python
readings = laser.stream("fleet").topic("readings")
await readings.ensure(partitions=4)

committed = await (
    readings.publish()
    .index("host_id", "node-7")
    .index("cpu", "82")
    .inline_payload()
    .json({"host": "node-7", "cpu": 82})
    .send()
)
print(committed.confirmations)
```

Producers and publish builders return `SendMessagesResponse`. Each `SendMessagesConfirmationResponse` identifies a committed batch by stream, topic, partition, and first offset. The list can be empty when the server does not report offsets. Completion follows the topic durability policy.

## Batch and any payload

`publish_batch` groups records for sending. A `topic(..).replay()` cursor reads retained records and saves the next offset for each partition. Each poll reads at most 10,000 messages per partition. Later polls resume from the saved offsets. Failed or canceled polls leave those offsets unchanged.

The payload contains bytes in the application-selected format. `add_json`, `add_msgpack`, and `extend_json` provide encoding helpers. `add_payload` sends raw `bytes` without inspecting their format. `topic.send(payload, headers=, partition_key=)` and `topic.batch(messages, partition_key=)` are the zero-overhead raw paths, and `topic.batching(max_records=, max_bytes=, linger_ms=, partition_key=)` returns a size-and-time `BatchingProducer` with `send`, `flush`, and `close`. Compressed data and application-defined formats use the same path. The following sections cover Avro and Protobuf.

```python
batch = readings.publish_batch().inline_payload()
batch.extend_json([{"host": "node-7", "cpu": 82}, {"host": "node-8", "cpu": 40}])
batch.add_payload(b"\x00any-bytes-any-format")  # raw bytes, untouched by the SDK
committed = await batch.send()  # the whole batch, one round-trip
```

## Live producer and consumer

Use `Topic.producer`, `Topic.consumer`, and `Topic.consumer_group` for continuous streaming through Apache Iggy. Producers support batches, delays, retries, resource creation, expiry, size, and routing. Consumers support first, last, next, offset, and timestamp reads. They also support groups, retries, automatic commits, explicit offset storage, and offset inspection.

```python
topic = laser.stream("fleet").topic("events")
producer = topic.producer(
    batch_length=1000,
    linger_ms=5,
    retries=3,
    partitions=4,
)
await producer.init()
committed = await producer.send(b"one", headers={"type": ("uint16", 7)}, key=b"node-42")
batch_committed = await producer.send_batch(
    [(b"two", {"type": 8}), b"three"],
    key=b"node-42",
)

consumer = topic.consumer_group("workers").consumer(
    batch_length=1000,
    poll_interval_ms=5,
    auto_commit="disabled",
)
await consumer.init()
try:
    message = await consumer.next()
    if message is not None:
        await handle(message.payload, message.headers)
        await consumer.commit(message)
finally:
    await consumer.shutdown()
```

Header values accept ordinary Python scalar values. For an exact Apache Iggy numeric type, pass `(kind, value)`. `ConsumerMessage.header_kinds` reports the received types. The default `auto_commit="polling"` with `commit_interval_ms=0` matches the Rust default `Polling` policy. The other modes are `"all"`, `"each"`, `"every"` with `commit_every=`, `"interval"`, and `"disabled"`. A `commit_interval_ms` above zero adds a timer to the chosen mode, so `auto_commit="each"` with `commit_interval_ms=1000` stores on each message or each second. `"interval"` requires `commit_interval_ms` above zero. `next_within(wait_secs)` waits a bounded time and raises `TimeoutError` when nothing arrives, like Rust `next_within`.

With automatic commits disabled, call `commit(message)` after successful handling. `shutdown()` does not advance the offset past that commit. `Consumer` waits for new records. A `replay()` cursor reads retained records in bounded polls and stops its iterator when caught up.

## Publish recovery

By default, each publish attempt has a 60-second timeout and a failed attempt retries up to three times. Retry delays start at 250 milliseconds, double after each failure, and stop growing at 30 seconds. Pass `publish_timeout_ms`, `publish_max_retries`, and `publish_retry_backoff_ms` to `Laser.connect`, or set `LASER_PUBLISH_TIMEOUT_MS`, `LASER_PUBLISH_MAX_RETRIES`, and `LASER_PUBLISH_RETRY_BACKOFF_MS`. The arguments override the variables. Direct producers inherit this configuration while `retries` and `retry_interval_ms` stay `None`, and `retries=0` disables resends. A publish that gives up raises `PublishFailedError` with `committed`, the confirmed ranges, and `unconfirmed`, the records without a confirmation. Its `__cause__` is the original error. Inspect them before retrying, because unconfirmed records can already be on the server. A batching producer keeps a failed timer flush until the next `send()`, `flush()`, or `close()` reports it. See [publish recovery and outage handling](../../docs/publish-recovery.md).

## Typed topics

`topic.json(Reading)` binds a topic to a dataclass or pydantic model. `publish(reading)` encodes an instance as JSON, and `publish_batch(readings)` sends several in one round trip. `records(reader_name)` reads typed records with the same client-owned offsets as `replay()`. `next()` returns a decoded record or `None` when caught up. If decoding fails, it raises `TypedDecodeError` with the log position. The next read continues past that record.

```python
from dataclasses import dataclass


@dataclass
class Reading:
    host: str
    cpu: int


readings = laser.stream("fleet").topic("readings").json(Reading)
await readings.publish(Reading(host="node-7", cpu=82)).send()

records = readings.records("metrics-export")
while (record := await records.next()) is not None:
    reading: Reading = record.value  # record.position names the log slot
```

`topic.cbor(Reading)` and `await topic.schema(schema_id, Reading)` bind CBOR and registered-schema bodies the same way. Use the plain `laser.topic(name).publish()` builder for explicit raw or codec-specific publication. Schema handles resolve and compile their writer schema once and share it between publication and typed reads. Typed Protobuf handles decode records, while publication requires encoded bytes through the plain builder.

## Schema-first bodies (Avro / Protobuf)

Compile the registered writer schema before publishing records that use it. The client encodes each value and rejects values that do not match. The managed plane resolves the schema ID to extract indexed columns.

```python
from laser_sdk import CompiledSchema

source = {"kind": "avro", "schema": reading_avro_schema}
schema_id = await laser.schemas().register(source, name="fleet_reading")
compiled = CompiledSchema.compile(source, id=schema_id)

batch = laser.stream("fleet").topic("readings_avro").publish_batch().inline_payload()
for reading in samples:
    batch = batch.add_avro(compiled, schema_id, reading)
await batch.send()
```

`CompiledSchema` provides `validate`, `validate_value`, `decode`, and `encode_avro`. `laser.schemas()` also provides `get`, `list`, and `drop`. The publish builder provides `.avro(compiled, schema_id, value)`. For an encoded Protobuf body, use `.raw_bytes(bytes, "protobuf")` or batch `.add_raw_bytes(..)`. Schema registration requires the `laser-plane` registry in Laser Stack or LaserData Cloud.

## Query (managed)

```python
result = await (
    laser.query("readings_v1")
    .where_eq("host_id", "node-7")
    .filter_gte("cpu", 90)
    .order_desc("cpu")
    .limit(10)
    .fetch()
)
for row in result.rows:
    print(result.value_text(row, "host_id"), result.value(row, "cpu"))
```

`result.fields` defines the ordered result schema. Each row contains tagged values in that order. Use `value()` to retain the value type or `value_text()` for stable display text. Values preserve integer widths, decimal precision, timestamps, UUIDs, bytes, structs, lists, maps, and nullability.

Filters and parameters accept `bool`, `int`, `float`, `str`, `bytes`, `uuid.UUID`, `decimal.Decimal`, date and time types, `None`, and lists. `datetime.date` and `datetime.time` retain their types. `datetime.datetime` with a timezone becomes a UTC instant, and a naive value retains no timezone. Non-finite numbers, out-of-range integers, and a `time` with `tzinfo` raise `InvalidError`.

`fetch()` returns one bounded page, and `result.page` is its `Page`. `page.has_more` is true exactly when `page.next_cursor` is present, and `query.cursor(next_cursor)` continues from it. `fetch_all()` follows the server-provided cursor. Request an exact match count only when needed. It requires a separate count over the full filter:

```python
result = await laser.query("readings_v1").where_eq("host_id", "node-7").with_total().fetch()
print(result.page.total, result.page.has_more)
```

`filter(Filter...)` adds any predicate tree (`Filter.pred`, `all`, `any`, `negate`), `having(Filter...)` filters aggregate groups, and `agg_as(func, alias, field=, fraction=)` names an aggregate column. `rows()` and `rows_typed()` return asynchronous iterators that walk pages up to an explicit `max_rows(n)` ceiling and raise `InvalidError` without one, like Rust `rows()`.

Each query keeps one execution identity and an absolute deadline. Use the same builder to inspect or cancel a running query:

```python
request = laser.query("readings_v1").filter_gte("cpu", 90).deadline(30_000)
result = await request.fetch()
status = await request.status()
if status["state"] == "running":
    await request.cancel()
```

Lakehouse queries always name one destination generation and can select a retained snapshot explicitly:

```python
result = await (
    laser.query_lakehouse(destination_id, destination_generation)
    .at_snapshot(snapshot_id)
    .filter_eq("host_id", "node-7")
    .limit(100)
    .fetch()
)
print(result.context)
```

The returned context proves the resolved engine and target. Lakehouse pages also carry destination, table, snapshot, schema, partition-spec, materialization-boundary, checkpoint, and global-state evidence.

## Destinations and Arrow IPC

Destination reads and query routes are available from one managed accessor. Reads choose potentially stale or linearizable checkpoint state explicitly:

```python
destinations = laser.destinations()
page = await destinations.list(limit=50, consistency="linearizable")
routes = await destinations.query_routes(
    consistency="potentially_stale", name_contains="readings", limit=50
)
current = await destinations.get(destination_id, consistency="linearizable")
```

Registration and desired-state changes are revision-guarded. `register()` takes one complete destination declaration and `set_desired_state()` compares both the global state revision and destination definition revision before applying a change.

For analytical batches, publish one complete self-contained Arrow IPC stream per message. The SDK validates the contract version, schema fingerprint, counts, bounds, and exact byte length before transport I/O:

```python
metadata = {
    "contract_version": 1,
    "schema_fingerprint": schema_fingerprint,
    "encoded_bytes": len(arrow_stream),
    "field_count": 8,
    "record_batch_count": 1,
    "row_count": 10_000,
    "dictionary_count": 0,
}
await readings.publish().arrow_ipc(arrow_stream, metadata).send()
```

Arrow input must use stream format, be self-contained, use microsecond timestamps, avoid dictionary replacement and deltas, keep decimals within 128 bits, and contain no unions or extension types.

Query, the key-value store, the knowledge graph, and forks are managed features served by LaserData Cloud or Laser Stack. Against Apache Iggy without a managed backend they raise `UnsupportedError`.

## Key-value

```python
kv = laser.kv("sessions")
await kv.set("user:42").json({"state": "online"}).ttl(300_000).send()
state = await kv.get_typed("user:42")
entry = await kv.get_entry("user:42")
print(entry.source())  # origin stream/topic ids, partition, and offset when stamped
values = await kv.get_many(["user:42", "user:43"])  # one round trip (the mixed-operation batch)
await kv.copy_to("user:42", "user:42:2026", to_namespace="archive")  # one backend transaction
await kv.move_to("plan:draft", "plan:current")  # copy plus source delete
lease = await kv.lease("source-owner", "worker-1", 30_000)
state = await kv.get_entry_at_least("source-state", lease.position)
lease = await kv.renew_lease("source-owner", "worker-1", lease.token, 30_000)
await kv.release("source-owner", "worker-1", lease.token)
await kv.delete("user:42")
```

`cas_fenced`, `copy_to`, and `move_to` return request builders. Await one directly, or finish it with `commit()` for `cas_fenced` and `send()` for the copies.

`Lease` exposes `token`, `granted_ttl_ms`, and `position`, a `MutationPosition`. After takeover, pass that position to `get_entry_at_least` to exclude state older than the grant. Request a lifetime from 1,000 to 300,000 milliseconds. The store can grant less time, but never more. Values outside the range fail before sending.

Renew a lease before its granted lifetime expires. Reacquisition does not extend a live lease. Acquisition uses a dedicated coordination connection. If the outcome is unknown, the SDK retires the connection and waits through the requested lifetime. It then raises an ambiguous-mutation error that requires operation-specific recovery.

### Prepared coordination requests

`FencedLeaseClient` exposes the same prepared mutation flow as Rust. It accepts `DedicatedKvTransport` or an object with `send(code, frame)` and `reset()` methods. Reset must stop all in-flight requests before it returns. A custom `close()` performs terminal retirement when provided. Otherwise client close calls `reset()`. Each attempt times out after 10 seconds, and `with_attempt_timeout(timeout_ms)` changes that.

```python
from laser_sdk import FencedLeaseClient

async with FencedLeaseClient.connect_dedicated("iggy:laser@127.0.0.1:8090") as coordination:
    operation = coordination.prepare_release(
        {
            "v": 1,
            "namespace": "leases",
            "key": b"source-owner",
            "holder_id": "worker-1",
            "lease_token": lease.token,
        }
    )
    released = await coordination.release(operation)
```

Prepare a mutation once. The returned `PreparedMutation` holds its stable `operation_id` and exact request bytes. After an ambiguous result, read `ambiguous_recovery`. Renew and release repeat the same prepared request. Acquisition waits through the requested lifetime before a fresh acquisition. Fenced compare-and-swap requires a target-precondition read before deciding how to recover. These are caller actions on this low-level client.

Call `close()` or use `async with` to retire the client. Later execution fails before connection or send. A dedicated transport also exposes terminal `close()`. Its `reset()` retires a connection and permits later reuse.

## Consumer filters

**Filter before the network.** Consumer filters select records on the server so each reader receives only its matching subset of a topic and its partitions. The shared CDC example delivers **4 of 240 records** and saves **98.5% of payload transfer**. It preserves original payloads and offsets, supports exact-width typed headers, and acknowledges only completed work. See the [Consumer Filters guide](https://docs.laserdata.cloud/laser-sdk/consumer-filters) and the [three-language examples](https://github.com/laserdata/laser-sdk/tree/main/examples).

Configure a consumer group's policy once. Its ordinary consumers and page readers then use that saved policy. An unbound group receives all records without payload decoding.

```python
import laser_sdk as ls

safe_mode = ls.ConsumerFilter.json(
    ls.FilterExpr.all(
        [
            ls.FilterExpr.pred("table", "eq", "satellites"),
            ls.FilterExpr.pred("changed", "contains", "mode"),
            ls.FilterExpr.pred("after.mode", "eq", "safe"),
        ]
    )
)
topic = laser.stream("orbit").topic("fleet_changes")
group = topic.consumer_group("anomaly-desk")
info = await group.create(filter=safe_mode)  # Run once during setup.

# Every consumer instance needs only the group name or info.id.
consumer = group.consumer(batch_length=100, auto_commit="disabled")
try:
    record = await consumer.next()
    print(record.position.partition_id, record.position.offset, record.payload)
    await consumer.commit(record)
finally:
    await consumer.shutdown()
```

`batch_length=100` examines at most 100 source records per partition request. It may deliver fewer matches, including none. An empty scan advances over rejected records and does not mean end of stream. The consumer continues bounded scans and waits only when caught up. Manual `commit(record)` stores safe contiguous progress after processing. Automatic policies commit the delivered prefix at their configured cadence and at shutdown. The native Apache Iggy path retains its own documented commit timing.

For batch handling, build `await group.reader(count=100, max_examined=1000)`, then use `next_page()` and `ack_page()`. `start=` takes `"next"`, `"first"`, `"last"`, `{"offset": n}`, or `{"timestamp": micros}`. Here `count` limits returned records, while `max_examined` independently limits the scan. Each reader joins as a member, and Iggy distributes partitions across instances. Acknowledgments preserve pending earlier work and reject stale source or policy generations. A reader keeps at most 1024 unacknowledged record-bearing pages per partition by default, so a reader that never acknowledges stops at that bound instead of growing without limit. Set `max_unacked_pages=` to change it.

For an application checkpoint inside a filtered page, call `await reader.ack_through(record)` after persisting the checkpoint and processing all preceding records on that partition. Later records in the same page stay pending. Use `ack_page` when the whole page is complete.

Use separate groups for A/B revisions so their offsets remain independent, and pause or resume a revision with `set_revision_enabled`. A fresh named consumer with the default `polling="next"` starts at the first retained record, and a stable name resumes durable progress. On the native Apache Iggy path the default `auto_commit="polling"` commits each polled batch before delivery, so a later consumer can resume after records the application did not process. Disable auto-commit and commit after successful processing when that matters.

`group.filter()` provides `configure`, `get`, `revisions`, `revise`, `set_revision_enabled`, `release`, `delete`, `preview` and `test`. `delete()` removes the group's own filter with every revision and releases the group first. Pass `operation_id` to configuration when setup must resume with the same operation ID after a lost reply. `topic.consumer_group_id(id)` addresses the saved group by numeric ID. Catalog failures, denied reads and paused revisions never broaden into unfiltered delivery. Original Apache Iggy uses native group consumption, while an older managed server without group-aware reads returns an explicit upgrade error.

Filters support JSON, CBOR, Avro, Protobuf and typed headers. Avro and Protobuf filters use registered writer schemas, immutable schema IDs in the filter, and the `agdx.sid` header on each record. Headers-only filters work with any payload format and never decode the payload. `CompiledFilter.compile(filter).evaluate(payload)` runs the evaluator locally, and `explain(payload)` returns its verdict tree. `FilterExpr.text(field, kind, pattern)` matches case-sensitively, and chaining `.case_insensitive()` ignores case. `with_fault_policy`, `with_foreign_policy` and `with_mismatch_policy` control invalid records. Passed invalid records and unfiltered records have `evaluated` false. Failures raise `FilterError` with a `reason`, and every `LaserError` carries `filter_reason`. A fault or oversized stop also sets `partition_id` and `offset`. Unknown header value kinds remain raw bytes. Malformed entries set `headers_malformed` while valid headers and the payload remain readable. A structurally truncated block has empty headers. A `next_record` call cancelled after it read a record yields that record again on the next call.

`group.reader(local_guard=True)` checks delivered records against the filter with the same evaluator the server runs. It verifies records marked unevaluated under the reported decoder bounds and requires the applicable pass policy.

## Knowledge graph

```python
import laser_sdk as ls

auth = ls.graph_node_entity("Service", "auth")
pool = ls.graph_node_entity("Component", "db-pool")

graph = laser.graph("ops")
await graph.upsert([auth, pool], [ls.graph_edge_relate(auth, "depends_on", pool)])
await graph.link("service:auth", "mitigated_by", "component:read-replica")

around = await graph.neighbors(auth["id"], "out", None, 2)
deps = await (
    graph.start_match(ls.Filter.pred("label", "eq", "Service")).out("depends_on").limit(50).fetch()
)
print(around.get("nodes", []), deps.get("nodes", []))
```

`graph_node_entity` derives an ID from label and value, so repeating the same entity preserves its ID. Nodes, edges, and results are dicts, with attributes as `[name, value]` pairs. `link(from, relation, to)` connects two `kind:value` entities. `relink` closes active edges of the same single-valued relation before recording the new value. `unlink` closes an edge valid-time window and retains its nodes.

A traversal starts from `start_ids`, from every node matching `start_match(Filter)`, or from `start_nearest(embedding, k)`. `out`, `incoming`, and `both` add hops. `return_edges`, `return_triplets`, and `return_paths` select the result shape, nodes by default. `as_of` uses epoch microseconds to select edges valid at that instant, and `conversation` scopes the read. `fetch()` runs it and `neighbors(node, dir, edge_type, depth)` is the one-hop shorthand. `node_id_content(label, value)` and `edge_id_content(from, edge_type, to)` compute the content IDs every SDK agrees on.

## Agents

```python
from laser_sdk import Laser


async def handle(ctx, message):
    text = bytes(message.payload).decode()
    await ctx.respond(f"echo: {text}".encode())


laser = await Laser.connect("iggy:iggy@127.0.0.1:8090", stream="agents")
await laser.bootstrap(partitions=4, retention=ls.TopicRetention.expire_after(86_400_000))

handle_agent = laser.spawn_agent("echo", "agent.sessions", handle, respond_on="agent.sessions")
await handle_agent.ready()

from laser_sdk import Provenance

reply = await laser.request(
    "agent.sessions",
    "agent.sessions",
    b"hello",
    Provenance(agent="caller"),
    timeout_ms=10_000,
)
print(bytes(reply.payload).decode())

await handle_agent.shutdown()
```

Keep the handle until `shutdown()` or `join()`. Dropping it signals a graceful stop and cancels periodic consolidation, and `abort()` stops the agent immediately. `async with laser.spawn_agent(...) as handle:` waits for readiness and stops the agent on exit.

Each agent id reads through its own consumer group, named after the agent id unless `consumer_group=` names another, so every instance of one agent shares its work. The handler only sees work for this agent: commands addressed to it or to every agent. Replies, status records, and records for other agents are committed without calling it. On `agent.sessions` and `agent.control`, when the server resolves group policies and serves filtered reads and the filter catalog, the group is bound to the addressee filter `agdx.to In [<agent id>, "*"]` before the first read, so the server sends only those records. Otherwise the agent reads every record and sorts them itself. [Agents, groups, and layouts](../../docs/building-agents.md#agents-groups-and-layouts) explains how to pick a layout.

### Agent memory and configuration

`MemoryHandler(inner, memory).auto_remember("message")` remembers each successful turn under its conversation and source agent. A failed handler writes no memory. A failed memory write does not repeat an effect that the handler completed.

`spawn_agent` accepts capability descriptor dicts as well as skill names. `fixed_inbox` sets the handler's default fan-out route. `governor_retention=(capacity, idle_ttl_ms)` bounds the process-local evidence heads for an agent governor. `dedup_window` sets the default in-memory capacity in keys. A custom `dedup` callback replaces that window, and passing both raises `InvalidError`.

Periodic consolidation requires both `consolidate_every_ms` and a callable or an object with `consolidate(scope)`. When the agent has an ID, each pass receives that agent in the scope dict. Otherwise the scope is empty. Pass failures are logged and later passes continue. Shutdown and context exit stop new passes and request cancellation of an active asynchronous callback. `join()` keeps consolidation active until the agent stops.

### AGDX send refinements

`command`, `respond`, `emit`, `status`, and `fail` accept `cause`, `cause_at`, `deadline_micros`, `idempotency_key`, `metadata`, `tool`, `usage`, and `claim_check`. A causal position is `ls.LogPosition(stream_id, topic_id, partition_id, offset)` and requires a cause record ID. The returned envelope retains the Rust packed 20-byte position capsule. `claim_check=(store, threshold_bytes)` moves a large body to the supplied blob store.

An AGDX producer's `signing_key` signs those five send verbs. Chunk streams and `request_input` use the unsigned Rust helpers. `request_input` accepts the reply topic, prompt, timeout, and an optional `target` agent.

### Signed, principal-bound contracts

`Laser.agent(id).contract(...)` uses the same contract refinements as `Laser.contract(...)`. Its source identity comes from the scope. Both accept reply-topic, conversation, fence, pickup-expiry, principal, fixed-inbox, and registration settings.

Rust and Python use the same Ed25519 verifier and routing rules. Enroll trusted keys before connecting. Give each signing agent its key. For sensitive routes, require an authenticated principal. One connection can advertise one agent. A second identity raises a conflict without replacing the first.

```python
from laser_sdk import Contract, KeyRegistry, Laser, SigningKey

risk_key = SigningKey(bytes(range(32)))
keys = KeyRegistry()
keys.enroll("42", risk_key.verifying_key)

laser = await Laser.connect(connection, stream="agents", verifier=keys)
risk = laser.spawn_agent(
    "risk",
    "risk.commands",
    handle,
    capabilities=["screen-change"],
    signing_key=risk_key,
    verifier=keys,
)
await risk.ready()

result = await laser.contract(
    "screen-change",
    b'{"change":"rotate the storage credentials"}',
    source="planner",
    principal=42,
)
match result:
    case Contract.Completed(reply):
        assert reply.verified_principal == "42"
    case Contract.Failed(reply):
        print("refused:", bytes(reply.body()).decode())
    case Contract.NotConsumed() | Contract.TimedOut():
        print("no reply")
```

`contract` defaults to a 30-second deadline, like Rust and TypeScript, and returns a `Contract`: `Contract.Completed(reply)`, `Contract.Failed(reply)`, `Contract.NotConsumed()`, or `Contract.TimedOut()`. Route to one named agent with `agent=` and `skill=None`. `expire_if_not_consumed_ms` lets an unpicked task end as `not_consumed`, and `reply_on`, `conversation`, `fence`, and `registered` mirror the Rust contract builder. `laser.agent(id)` returns an `AgentScope` with `send`, `ask`, `contract`, `publish_card`, and `advertise`. `laser.agent_registry()` reads folded cards, quarantine facts, and live presence. `scatter` returns the reply bodies of the agents that completed. `scatter`, `scatter_report`, and `AgentCtx.fan_out` take a required `deadline_ms`, as in Rust and TypeScript. `scatter_report` returns a `ScatterReport` whose `outcomes` hold each agent's `Contract` or failure, with `completed()` and `failures()` views. Read `reply.verified_principal` when policy or UI code must inspect the signer. With a verifier configured, unsigned, invalid, and wrong-principal replies are ignored rather than returned with an empty identity.

For a human-in-the-loop pause, the typed AGDX producer's `request_input` publishes a prompt and blocks on the human's correlated reply, which a handler resolves with `AgentCtx.respond_input`:

```python
decision = await laser.agdx("agent.sessions", "orchestrator", conversation_id).request_input(
    "agent.sessions", b"approve draining node-7?", timeout_ms=15_000, target="approver"
)
```

`target` addresses the prompt to one agent. Without it every agent listening on a shared session topic receives the prompt.

The same identity rules as contracts apply. A caller connected with `verifier=` accepts only signed responses, so an approver spawned with `signing_key=` resumes it and nothing else can. A producer signs its own sends the same way: `laser.agdx(..., signing_key=key)` signs every envelope it publishes (`command`/`respond`/`emit`/`status`/`fail`), the Python spelling of the per-send `.signed_by` builder in Rust and TypeScript.

### Fan-out and human approval from a handler

`AgentCtx` (the `ctx` a handler receives) carries two more coordination verbs beyond `respond`/`send`/`request`, mirroring the Rust `AgentCtx`:

```python
async def orchestrate(ctx, message):
    # Fan a task out to every agent advertising "diagnose", gathering
    # replies on this handler's own respond_on topic.
    gather = await ctx.fan_out("diagnose", b"scan", deadline_ms=10_000)
    for agent, reply in gather.ok:
        print(agent, bytes(reply.body()).decode())
    for agent, error in gather.failures:
        print(agent, error)


async def gatekeeper(ctx, message):
    # Pause on a human decision before continuing, the ctx-scoped sibling
    # of the top-level request_input above.
    decision = await ctx.approval_gate(
        "agent.sessions", b"approve draining node-7?", timeout_ms=15_000
    )
    await ctx.respond(decision)
```

`fan_out`'s `policy` is `"require_all"` (default, wait for every branch), `"quorum"` (pass `quorum=<n>` to stop once that many succeed), or `"best_effort"` (take whatever landed by `deadline_ms`). Unavailable and quarantined agents are excluded, and a target that resolves no inbox is a `failures` entry, never silently rerouted. `fixed_inbox` routes every branch to a fixed topic instead of each agent's advertised inbox, the same knob `contract`/`scatter` take. Presence is connection-scoped (one connection may advertise one agent), so capability-advertising workers under test each need their own connection.

### Unit-testing a handler

`agent_message` and `agent_ctx` build a message and a ctx directly, with no live consumer or server involved, so a handler function is testable like any other callable:

```python
from laser_sdk import agent_ctx, agent_message, Provenance

message = agent_message(b"hello", Provenance(agent="tester"))
ctx = agent_ctx(laser, message, agent="tester", respond_on="agent.sessions")
await handle(ctx, message)  # call your handler function directly
```

`laser` only needs to be live for whatever ctx helpers the handler actually calls (`respond`/`fan_out`/...). A handler that only reads its message needs no server at all.

A policy decides before an SDK effect. It can allow, observe, block, require approval, modify, or defer the action. Enforce mode applies the decision, while observe mode records it. Decisions that are not allow produce linked evidence on the audit topic. Typed refusals include `PolicyBlockedError`, `StepUpRequiredError`, and `PolicyDeferredError`:

```python
from laser_sdk import ActionDecision, PolicyBlockedError, PolicyRef


class NoDrains:
    async def decide(self, action):
        if action.payload.startswith(b"drain-node"):
            return ActionDecision.block("drains need approval").with_policy(
                PolicyRef("ops", "3", ["no-drains"])
            )
        return ActionDecision.allow()


governed = laser.with_governor(NoDrains(), mode="enforce")
try:
    await governed.send_agent("agent.sessions", b"drain-node node-7", provenance)
except PolicyBlockedError as refused:
    print(refused)  # policy blocked: drains need approval

# Per-agent: everything the handler publishes is governed too.
handle_agent = laser.spawn_agent("operator", "agent.sessions", handle, governor=NoDrains())
```

The governor receives a `GovernedAction` whose `counters` (`ActionCounters`) report the sends, requests, and bytes already spent in that scope. `decision.verdict` is a `Verdict`. Compare it with `Verdict.block()` or read `verdict.as_str()`, and read a step-up scope or modified body from `verdict.scope` and `verdict.body`. `decision.policy` is a `PolicyRef`. `laser.with_governor_retention(governor, mode, GovernorRetention(capacity=, idle_ttl_ms=))` bounds the process-local evidence heads.

`QuorumGovernor` combines named voters through `all`, `any`, or `at_least(n)`. A voter can implement deterministic rules or call a model. Every `mandatory` voter must return `allow`, `observe`, or `modify`. A mandatory denial or error blocks the operation under every policy:

```python
from laser_sdk import QuorumGovernor, QuorumPolicy

quorum = QuorumGovernor(QuorumPolicy.at_least(2))
quorum.voter("safety", NoDrains(), mandatory=True)
quorum.voter("llm_reviewer", llm_voter, mandatory=False)

governed = laser.with_governor(quorum, mode="enforce")
```

`SwappableGovernor` replaces the active policy without reconnecting or dropping existing handles. An operator, configuration reload, or recorded update can trigger the replacement. It affects the next decision and leaves recorded decisions unchanged:

```python
from laser_sdk import SwappableGovernor

swappable = SwappableGovernor(NoDrains())
governed = laser.with_governor(swappable, mode="enforce")
...
previous = swappable.swap(a_stricter_policy)  # returns the replaced policy
```

Durable approvals are native typed records. They publish and replay directly, while the SDK keeps log ownership explicit:

```python
import time
from laser_sdk import Decision, Intent, IntentPolicy, Vote, decide

intent = Intent(
    conversation=conversation_id,
    proposer="planner",
    body=b"rotate the storage credentials",
    eligible_voters=["safety"],
    policy=IntentPolicy.all(),
    policy_version=7,
    deadline_micros=time.time_ns() // 1_000 + 30_000_000,
)
await laser.stream("agents").topic("intents").json(Intent).publish(intent).send()
vote = Vote.cast(intent, "safety", "allow")
decision = decide(intent, [vote], time.time_ns() // 1_000)
if decision and decision.authorizes(intent):
    await laser.stream("agents").topic("decisions").json(Decision).publish(decision).send()
```

Construction, casting, and folding raise `IntentError` on malformed state. It subclasses `InvalidError`, and its `kind` names the broken rule, such as `IntentError.INVALID_DEADLINE`. Mandatory voters must affirm, and ballots outside the intent's time window never count. A voter name remains a record claim unless signing or topic ACLs bind it to an authenticated principal.

`SwarmActivity` builds a read model from governance evidence. Read `PolicyEvidence` records from the audit topic, then apply them to inspect each agent activity:

```python
from laser_sdk import AgentTopic, PolicyEvidence, SwarmActivity

swarm = SwarmActivity()
for message in await laser.assemble_context(conversation_id, topics=[AgentTopic.Audit]):
    envelope = message.envelope
    if envelope and envelope.get("operation") == "policy_decision":
        swarm.observe(PolicyEvidence.decode(bytes(envelope["body"])))

activity = swarm.agent("planner")
if activity:
    print(activity.decisions, activity.count("block"))
```

`CrashContext` combines a journal tail, an optional dead-letter record, and the latest available decision for the conversation. Its summary is deterministic. It does not call a model:

```python
from laser_sdk import CrashContext

journal = await laser.assemble_context(conversation_id, topics=[AgentTopic.Sessions])
context = CrashContext(journal=journal, dead_letter=None, last_decision=activity.last_decision)
print(context.summarize())
```

## Sessions

A session is one conversation with a recorded lifecycle. Its id is the conversation id, so every conversation read finds it. Bootstrap the agent topics once. `agent.sessions` needs an explicit retention, because it holds every session's records.

```python
import laser_sdk as ls

sessions = laser.sessions()
await sessions.bootstrap(4, ls.TopicRetention.expire_after(7 * 86_400_000))

# `async with` begins the session, ends it when the block finishes, and fails
# it with the exception type and traceback when the block raises.
async with sessions.create("ticket-42").agent("triage") as session:
    assembled = await session.assemble(ls.LastN(20))
    call = await session.model(
        ls.ModelRequest("gpt-4o", assembled.text(), provider="openai"), assembled
    )
    answer = await my_model(assembled.text())  # the SDK never calls a model
    await call.complete(ls.ModelResponse(answer, usage={"input_tokens": 812, "output_tokens": 64}))
    tool = await session.tool("lookup", {"ticket": 42, "api_key": "k"})  # api_key is redacted
    await tool.complete(b"found")
    await session.state().set("status", "triaged")
    await session.state().patch([{"op": "add", "path": "/owner", "value": "ops"}])
```

`create(label)` derives the id from the stream, the namespace, and the label. `start()` makes a fresh id. `await builder.begin()` returns a `(Session, SessionLease)` pair when a block does not fit, and `session.run(lease, work)` ends the session by the outcome of `work`. `end()`, `fail(error)` (an `AgentErrorBody` dict such as `{"code": 4, "message": "deadline passed"}`), and `cancel()` write the terminal record. Every handle copy shares one terminal latch, so a retried `end()` resends the same record and a later `cancel()` raises `InvalidError`. A held lease lists the session in the process heartbeat on `agent.heartbeats`. Call `lease.release()` when the process stops working on the session.

Hand a session to an agent with `submit`. The agent's handler reaches it through `ctx.session()`:

```python
async def handle(ctx, message):
    session = ctx.session()
    await session.state().set("seen", True)
    await session.end()


worker = laser.spawn_agent("worker", ls.AgentTopic.Sessions, handle)
submitted = await (
    laser.sessions()
    .submit("worker", b'{"ticket": 42}')
    .from_("intake")
    .label("ticket-42")
    .budget(ls.Budget(tokens=4_000))
    .send()
)
turns = await laser.sessions().open(submitted.session).context()
print([turn.display for turn in turns])  # session.submitted, session.resumed, ...
```

An operator stops a session through `agent.control`, which only accounts with send permission on that topic can write:

```python
control = laser.sessions().control(laser.default_stream, submitted.session).as_operator("ops")
await control.cancel()  # the agents end it at their next boundary
await control.force_cancel()  # ends a session whose agent is gone
```

`laser.sessions(stream=, layout=, idle_timeout_ms=, heartbeat_ms=, register_source=, fail_on_dead_letter=, memory_namespace=, context_turns=, context_tokens=, sdk=)` configures the factory. The defaults are a 300-second idle timeout and a 60-second heartbeat. `layout=ls.SessionLayout.PerAgentPartition({"planner": 0, "worker": 2})` puts the work addressed to each declared agent on its own partition of `agent.sessions`. `SessionLayout.PerAgentTopic(topics={"planner": "planner.inbox", "worker": "worker.inbox"})` routes the work addressed to each declared agent onto its own topic, `SessionLayout.SinglePartition()` puts everything on one partition, and `SessionLayout.Shared()` is the default. Lifecycle and state always stay on the session's partition. `sessions.bootstrap(partitions, retention)` also registers the stream as a session source when the server announces `sessions`, and reports `registered`.

On a deployment that announces `sessions`, the factory reads the managed session index. Replies are dicts, and a failed read raises `SessionError`:

```python
page = await laser.sessions().list(status="active", limit=20)
info = await laser.sessions().get(submitted.session)
events = await laser.sessions().events(submitted.session, limit=100)
record = await laser.read_at(events["items"][0]["at"])  # works on Apache Iggy too
watch = laser.sessions().watch(1_000)
change = await watch.next()
```

Inside a handler, `await ctx.session().pending_control()` returns the pause and cancel requests the agent recorded from `agent.control`. The runtime never interrupts a handler. `control(...).signed_by(key)` signs control records. `spawn_agent(..., sessions=laser.sessions(fail_on_dead_letter=True))` fails the session of a dead-lettered record.

An operator can pause a session. `pause()` names the agents that must acknowledge, set them with `participants([...])` or let the SDK read them from the session. Agents hold work that arrives while the session is paused and handle it after `resume()`. `await session.parked()` lists held work that was never handled, for example after a cancel while paused.

`await session.over_budget()` tells you whether the session's recorded usage passed the budget in its start record, by tokens or by cost. A deployment that indexes sessions answers from its index, and open Apache Iggy folds the retained lane. On a deployment that indexes sessions, an agent with an id checks each session before its handler runs. When the session is over its budget, the agent fails it once with reason `budget` and commits the work without handling it. A workflow checks its run at every step boundary, compensates, raises `BudgetExceededError`, and ends the run session failed with reason `budget`. Budget reads are eventually consistent, so a budget is a cooperative limit, not a hard spending cap.

Workflows run as sessions too. A run is a root session and each step and compensation is a child session. `contract(..., parent=, root=)`, `A2aBridge.submit_in(parent, params, root=)`, and `McpBridge.call_tool_in(parent, name, params, root=)` run as child sessions of `parent`. The bridge calls also take `target=` to address one agent.

```python
wf = laser.workflow("incident-response")
# A fenced external effect must use the same namespace in the workflow lease
# and in the handler's kv("storage").cas_fenced(...) commit.
wf.step(
    "rotate",
    to="rotator",
    build=lambda outputs: b'{"credential":"storage"}',
    fence_namespace="storage",
    on_timeout="reassign",
)
```

## Change feed (managed)

Await a view's advance instead of polling it blind. A projection binding built with notify makes the plane publish one change record per committed batch, and `laser.watch()` reads that feed. Gated on the `watch` capability: `laser.watch()` raises `UnsupportedError` elsewhere. Resume with `watch(from_offsets=)` or `reader.from_offsets(saved)`.

```python
feed = laser.watch(index="readings_v1")
for change in await feed.poll():
    print(change.index, change.from_offset, change.to_offset, change.rows)
    rows = await laser.query(
        "readings_v1"
    ).fetch_all()  # the record is a wakeup, the rows come from query
saved = feed.offsets  # persist to resume after a restart
```

## Consume and replay

```python
import laser_sdk as ls

# A resumable reader over a topic. Each poll drains what is new. Persist the
# offsets to resume after a restart.
cursor = laser.stream("fleet").topic("readings").replay()
for message in await cursor.poll():
    print(message.json())
saved = cursor.offsets

# Replay a conversation's history off the log (agent runtime). `token_budget`
# trims the selection to an estimated token count, applied after `last_n`.
history = await laser.assemble_context(conversation_id, last_n=50, token_budget=4_000)

# One session's lane: a model-ready context and checkpointed replay.
session = laser.sessions().open(session_id)
for turn in await session.context():
    print(turn.display, turn.text())
checkpoint = await session.checkpoint()
saved = checkpoint.to_json()
later = await session.turns_since(ls.Checkpoint.from_json(saved))
```

`laser.context(conversation)` returns a `ContextScope`. Its `fetch(n=50, token_budget=)` and `block` read `agent.sessions` unless `topics=` names others, and return `ContextMessage` records (`id`, `provenance`, `payload`, `envelope`, `topic`). `fetch_with(topics, Chain([LastN(20), TokenBudget(4_000)]))` takes the policy objects `LastN`, `TokenBudget(max_tokens, estimator=None)`, `RoleFilter(agents)`, and `Chain`. `state(topics, init, fold, last_n=|from_offsets=|from_checkpoint=|at=|full=True)`, `state_with(store, topics, init, fold)`, and `checkpoint(topics)` fold the conversation log like Rust. `context_checkpoint(laser, topics)` captures a checkpoint without a conversation, and `ConversationState.load(laser, conversation, topics, init, fold)` folds one directly with the same bounds. Each read covers at most the newest `CONTEXT_READ_WINDOW` (10,000) messages per partition.

`ContextMessage` also carries `timestamp_micros`, the broker append time, and the numeric `stream_id` and `topic_id`. `LastN`, `TokenBudget`, `RoleFilter`, and `Chain` report `name`, `version`, and `selection(history)`, which a context manifest records. A custom policy can set `name` and `version` attributes. Every SDK estimates tokens the same way: the byte count divided by four, rounded up.

## Memory and state

Agent memory is a `MemoryHandle` with `remember`, `recall`, `forget(id)`, and `improve(target, weight)` over a log-based backend, a local vector backend, or a custom one. `laser.memory(namespace)` returns the log-based handle, and the `LogMemory(laser)`, `VectorMemory(embedder)`, and `RerankedMemory(inner, reranker)` subclasses build each backend directly. The log-based handle also supports named state through `set(key, value)`, `fetch(key)`, `update(key, patch)`, and `remove(key)`, which `LogMemory` also spells `set_named`, `fetch_named`, `update_named`, and `forget_named`. These named operations raise `UnsupportedError` on the vector and custom backends.

Log-based writes publish to the memory topic and work on Apache Iggy. Default `recall` and `fetch` reads use a managed key-value view, so they need Laser Stack or LaserData Cloud. Use `recall(folded=True)` or `fetch_folded` to build the view locally from the topic. `remember(payload, kind=, durable=, dedup=, user=, application=, stream=)` mirrors the Rust remember builder, `recall(..., block=True)` and `context(conversation, token_budget=)` render a prompt block, and `consolidate(max_items, ...)` prunes a scope. `laser.memory_with(namespace, "vector", embedder=...)` selects the backend explicitly. `ContextScope.memory(..)` and `Session.memory(..)` return a `ScopedMemory` with `recall(folded=)`, `search(folded=)`, `remember`, `block`, `consolidate`, `forget`, and `improve`, and `Session.memory_in(namespace)` picks another namespace. The local vector backend ranks records by similarity. Its embedder can return `list[float]` or an awaitable for an external model call.

```python
from laser_sdk import VectorMemory


async def embed(text: str) -> list[float]: ...  # your model, or a deterministic stand-in


memory = VectorMemory.governed(laser, embed)  # or VectorMemory(embed) without a governor
await memory.remember("auth latency traces to the database pool", conversation=cid)
hits = await memory.recall(conversation=cid, semantic="why is auth slow", limit=3)
print([item.text() for item in hits])

# A durable key/value seam for agent state, the same vocabulary as the managed store.
from laser_sdk import InMemoryStore  # or FileStore("/var/lib/agent")

store = InMemoryStore()
await store.set("cursor", saved_bytes)
value = await store.get("cursor")
```

`VectorMemory.governed(laser, embedder)` inherits the governor enrolled on that `Laser`. A blocked write never mutates the local index, and a modified decision replaces the proposed memory body before embedding. Rust and Python therefore apply the same effect-boundary policy to local semantic memory.

## Edge interop (A2A / MCP / AG-UI)

Reach an agent as an A2A task source or an MCP tool server, and render a conversation as AG-UI events, all over the durable log:

```python
from laser_sdk import A2aBridge, McpBridge

# A2A: submit a task to the "assistant" agent, poll for the result.
a2a = A2aBridge(laser, "a2a-gateway", "agent.sessions", "agent.sessions")
task = await a2a.submit(
    {"message": {"role": "user", "parts": [{"kind": "text", "text": "hi"}]}},
    target="assistant",
)
status = await a2a.task(task["id"])

# MCP: advertise tools, route tools/call to the agent.
mcp = McpBridge(
    laser,
    "mcp-gateway",
    "agent.sessions",
    "agent.sessions",
    "laser-mcp",
    tools=[{"name": "ask", "input_schema": {"type": "object"}}],
)
tools = mcp.list_tools()
result = await mcp.call_tool("ask", {"q": "what is AGDX?"}, target="assistant")


# An agent answers a bridge request from its handler:
async def handle(ctx, message):
    await ctx.respond_input("agent.sessions", b"the answer")


# AG-UI: snapshot + deltas reconstruct shared state off the log.
await laser.publish_state_snapshot("ui", conversation_id, {"count": 1})
state = await laser.reconstruct_state(conversation_id)
events = await laser.agui_events(conversation_id, "agent.sessions")
```

`target` addresses a bridge call to one agent. Without it every agent listening on a shared session topic receives the call. The JSON-RPC `handle_rpc` path sends unaddressed.

Host the actual HTTP endpoint with your Python web framework over these adapter methods.

## Errors

Every failure raises a subclass of `LaserError`. The classes are `ConfigError` (with `NoStreamError`, `NoRespondTopicError`, and `HandlerConfigError` below it), `HandlerError`, `RejectedError`, `TimeoutError`, `AmbiguousMutationError`, `StateStoreError`, `QueryError`, `KvError`, `ForkError`, `GraphError`, `AuthzError`, `CheckpointError`, `FilterError` (with `FilterFaultError`, `FilterOversizedRecordError`, and `ConsumerGroupSetupError`), `SignatureError`, `UnsupportedError`, `InvalidError` (with `IntentError`, `ValidateError`, and `IdError`), `CodecError` (with `TypedDecodeError`), `ProtocolError`, `TransportError`, `PublishFailedError`, `IntegrityError`, `PolicyBlockedError`, `StepUpRequiredError`, `PolicyDeferredError`, `RoutingError` (with `NoCapableAgentError`, `NoInboxError`, and `RoutePrincipalMismatchError`), `PresenceConflictError`, `FenceViolationError`, `BudgetExceededError`, `CancelledError`, `QuarantinedError`, and `ProvenanceError`. Each instance carries `code`, `retryable`, `iggy_error_code`, `filter_reason`, `unavailable`, `unsupported`, `not_found`, `version_skew`, `version_conflict`, `ambiguous_mutation`, `stale`, `permission_denied`, `stream_or_topic_not_found`, `no_capable_agent`, `lease_lost`, `fence_violation`, `budget_exceeded`, `quarantined`, and `not_leader` attributes so you can branch without matching on the type. `IntentError`, `ValidateError`, `IdError`, and `ProvenanceError` add a `kind` with class constants such as `IdError.INVALID_ULID`. A publish that gives up raises `PublishFailedError`, whose `committed` and `unconfirmed` report what it left. A handler rejection raises `RejectedError`. `TimeoutError` also subclasses the builtin `TimeoutError`, `InvalidError` also subclasses `ValueError`, and `CancelledError` also subclasses `asyncio.CancelledError`, so stdlib-style `except TimeoutError` and `except asyncio.CancelledError` catch them too.

## Reading

Use `async for message in laser.stream("fleet").topic("events").replay()` to read raw records. `WatchReader` and `topic.records(reader_name)` also support asynchronous iteration. They stop when caught up. A later iteration resumes from the same offsets. Use `poll()` for a batch of records.

## Lifecycle

Use `async with await Laser.connect(conn) as laser:` to manage a connection. The shared connection closes when its last handle is dropped. `with_default_stream` and `with_ops_stream` return handles that share that connection. `await laser.close()` ends the shared connection for every handle.

## Resource names

A `Laser` with a default stream scopes every managed name to that stream, `stream:<stream>/<name>`: KV and memory namespaces, leases and fences, the key registry, graph names, projection and index ids, query indexes, fork ids, and the watch index filter. Listings return local names, and schema calls carry the stream. `laser.resource_name(name)`, `Kv.resource_namespace`, and `ForkHandle.resource_id` show the scoped name. `Laser.connect(..., resource_naming="bare")` or `laser.with_resource_naming("bare")` sends every name as written. `Capabilities.stream_tenancy` reports a deployment that enforces the scoping, and `ConsumerFilter.with_schema_stream(stream)` names a filter's schema registry.

## More APIs

These calls match the Rust and TypeScript surfaces:

- `Topic.producer(background=True)` buffers sends through Iggy's background mode with its defaults, and `background=BackgroundConfig(...)` sets shards, flush limits, the failure mode, and an `error_callback`. `await producer.shutdown()` waits for sends in flight, flushes, and closes.
- `Topic.cbor(cls)` and `await Topic.schema(schema_id, cls)` add typed topics. `PublishRequest.claim_check(store, threshold_bytes)` stores large bodies by reference.
- `Laser.memory_custom(backend)` plugs in a custom memory backend. `MemoryHandle.reranker(callable)` reranks semantic recall.
- `Session.context_with(policy)` and `Session.graph(name)`.
- `ForkHandle.create(continuous=True)`, `Workflow.run_id(id)`, `Intent.validate()`.
- `Capabilities.is_open_only()` and `serves_consistency(level)`.
- `AgdxStream` options (`with_deadline_micros`, `with_target`, `content_type`, `buffered`, and `flush`), `SigningKey.sign`, and the `KeyRegistry` verify family.
- `BatchPublishRequest.add_json`, `add_msgpack`, `add_payload`, and `add_raw_bytes` take `projection_ref=` for one record.
- `Laser.contract`, `scatter`, `scatter_report`, `AgentScope.contract`, and `Workflow.step` take `policy=` with a route policy word: `any`, `cheapest`, `fastest`, `least_loaded`, or `sticky:<agent>`. `AgentCtx.fan_out` takes the same word as `route_policy=`. `AgentScope.contract(None, payload, skill=...)` routes by capability.
- The `Json`, `Cbor`, `Msgpack`, and `Bson` codec classes carry `content_type`, `encode`, and `decode`, and plug into `encode_with`, `Kv.get_as`, and `fetch_typed_with`.
- `Message.id` and `ConsumerMessage.position` are a `MessageId` (`partition_id`, `offset`, and `str()` as `p:o`).
- `laser.consumed(ConsumerRef.Group(name), LogPosition(...))` returns `ConsumptionStatus.Consumed(committed, head)` or `ConsumptionStatus.NotYetConsumed(behind_by)`.
- `ConsumerGroup.create()` and `info()` return a `ConsumerGroupInfo` (`id`, `name`, `identity`, `filter`), and `Kv.exists` returns a `KvMetadata` or `None`.
- `Checkpoint.topic_offsets(topic)` returns a dict of partition to offset, or `None`.
- `PolicyEvidence.encode()` returns the evidence body that `decode` reads.
- `laser.projections().list(topics=[...], search=...)` narrows by several source topics and by a substring of the id or name. `laser.bindings()` and `laser.schemas()` group the binding and schema registry calls the same way.
- `MemoryHandle.consolidate` and `ScopedMemory.consolidate` take `summarizer=` (a sync or async callable from `list[bytes]` to `bytes`) and `prune_summarized=True`.
- `memory_id_content(kind, body, stream=, agent=, user=, application=)` returns the content-derived memory id. `memory_kind_class(kind)` returns `episodic`, `semantic`, or `procedural`.
- `Sessions.turn_topic(kind)` and `Sessions.turn_kind(topic)` map a session turn kind to its default topic and back.
- `AgentRegistry.agents`, `lookup`, and `resolve` return `RegisteredCard` objects (`agent`, `card`, `observed_at_micros`) with `is_fresh(now_micros)`, `serves(skill)`, and `available_for(skill)`.

`Laser.assemble_context` accepts `across_subconversations`, `from_offsets`, `from_checkpoint`, `to_checkpoint`, and an explicit context `policy`. A start-offset map applies to every topic. A start checkpoint replaces the raw offset map. Topics absent from it start at offset zero. An end checkpoint bounds the read to saved offsets. An explicit policy replaces `roles`, `last_n`, and `token_budget` and cannot be combined with them.

`MemoryHandle.recall` and `ScopedMemory.recall` accept `stream`, `durable`, and `token_budget`. The full scope and query reach the selected backend. `durable` sets the scope lifetime for custom callbacks. Built-ins do not persist or filter that value. Omit `conversation` to read across conversations. A token budget in the query is advisory and does not trim the returned item list. Use `context` or `block` to render a budgeted text block. `MemoryHandle.improve`, `forget`, and `consolidate` accept the full scope. Feedback can include a `note`.

`Provenance(fence_token=...)` stamps the fence used by the reliable consumer. Received messages expose it through `message.provenance.fence_token`.

Context policies also accept a synchronous callable or an object with `select(messages)`. It returns the selected `ContextMessage` objects. A callback failure raises after the read. An asynchronous policy is rejected because the Rust policy seam is synchronous.

`spawn_agent(None, ..., consumer_group="name")` creates an unscoped reliable consumer. It has no agent routing identity and cannot advertise capabilities. A named agent keeps its own default consumer group.

Use `to_context_block(items, token_budget=None)` to render the exact result of a scoped, ranked, or folded recall. It keeps the supplied order. The Rust formatter always retains the first item and adds an omission marker when the budget excludes later items.

`BatchPublishRequest.add_record(payload, content_type=, index=, headers=, projection_ref=, schema_id=, inline_payload=, logical_schema_fingerprint=)` sets every option of one record and does not inherit the batch defaults. A fingerprint must contain 32 bytes and requires `content_type="arrow"`. This path stamps metadata without parsing the payload, so supply a complete Arrow IPC stream. Use `add_arrow_ipc(payload, metadata)` when the SDK must validate the declared message metadata and payload length.

Agent abort and an expired shutdown grace request cancellation of the active Python handler. Cancellation of a memory call also requests cancellation of its active asynchronous callback. Callbacks must propagate `asyncio.CancelledError`. A synchronous callback completes before cancellation takes effect.

A custom memory backend can add `append(scope, id, kind, payload)` to preserve an explicit memory kind and content ID. It returns the stored ID. The SDK falls back to the original `remember(scope, payload)` callback when `append` is absent. Consolidation passes a durable scope and the `summary` kind through this optional hook.

`MemoryHandle.append(id, payload, kind=, ...)` writes with a supplied ID and memory kind under the full scope. Built-in backends preserve both.

Workflow `build`, `verify`, and `compensate` callbacks can return directly or through an awaitable. Objects with `build` or `verify` methods also work. SDK exceptions keep their error class. A build must produce bytes, and a verifier must produce a boolean. Cancelling a running workflow retires its active Python callback task.

`SnapshotStore(backend)` wraps custom `latest(conversation)` and `save(snapshot)` methods. Both can return directly or through an awaitable. A snapshot is a dict with `conversation`, inclusive per-partition `as_of` offsets, and opaque `state` bytes. `ContextScope.state_with` also accepts the backend object directly. It JSON-decodes the saved state and resumes after those offsets.

Python middleware and dead-letter observers receive full outcomes. `after_handle(message, result, attempt)` receives `{"ok": bool, "error": typed_exception_or_none}`. `dead_letter(message, capsule, publish_error)` receives the full wire capsule and a typed publish error or `None`. Both can return directly or through an awaitable.

Custom route policies accept `None`, a built-in word, or a synchronous callable or `select(skill_id, candidates)` object. Each candidate is a `RouteCandidate` with `agent`, `card`, and `capability`. Return a candidate index or `None`. Async scorers are rejected.

Portable native helpers include `encode_snapshot`, `decode_snapshot`, `resume_offsets`, and `fuse_reciprocal_rank`. Snapshot bytes match the Rust fixture. Pure request conversions preserve the original bytes through `command_from_message_send` and `tool_call_from_request`. Their outbound counterparts are `task_from_envelope` and `tool_result_from_envelope`. Card and delegation helpers are `sign_card_value`, `verify_card`, and `verify_delegation`. `check_in` and `resolve_body` share claim-check thresholds and digest checks with publication and consumption.

`SystemClock` and `TestClock` subclass `Clock`. `SystemClock().now_micros()` returns epoch microseconds. `TestClock(start_micros=0)` exposes `now_micros`, `set`, and `advance`. Inputs fit unsigned 64-bit values, and anything else raises `InvalidError`. Advancement wraps at the native limit.

Both bridges expose `handle_rpc(request)`, an awaitable that returns a JSON-RPC response dict. It supports malformed-request errors without requiring an HTTP framework. `KvKeyRegistry.enroll(principal, verifying)` enrolls an agent key, and `enroll_record(record)` returns the lifecycle-aware record version.

## License

Apache-2.0. Copyright LaserData, Inc.

Apache and Apache Iggy are trademarks of the Apache Software Foundation. Use of these marks does not imply endorsement by the Apache Software Foundation.
