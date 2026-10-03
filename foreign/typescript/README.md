# LaserData - Laser SDK

This package provides the native TypeScript Laser SDK for Apache Iggy. [LaserData, Inc.](https://laserdata.com) maintains it. One connection supports streaming and managed operations for queries, state, forks, graphs, and agents.

This prerelease targets Node 22.14 or later. Bun, Deno, and browsers are not supported because the Apache Iggy transport uses Node TCP and TLS APIs.

> The current release is `0.5.2`. The wire contract and public API use semantic versioning. Before `1.0.0`, minor releases can contain breaking changes.

**Filter before the network.** Consumer filters select records on the server so each reader receives only its matching subset of a topic and its partitions. The shared CDC example delivers **4 of 240 records** and saves **98.5% of payload transfer**. It preserves original payloads and offsets, supports exact-width typed headers, and acknowledges only completed work. See the [Consumer Filters guide](https://docs.laserdata.cloud/laser-sdk/consumer-filters) and the [three-language examples](https://github.com/laserdata/laser-sdk/tree/main/examples).

## Install

```sh
npm install @laserdata/laser-sdk
```

The package is ESM-first. A CommonJS application can load it with dynamic `import()`.

## Connect and stream

```ts
import { Laser } from "@laserdata/laser-sdk"

await using laser = await Laser.connect(
  process.env.LASER_CONNECTION_STRING ?? "iggy:iggy@127.0.0.1:8090"
)
const topic = laser.stream("commerce").topic("orders")
await topic.ensure(4)
const committed = await topic.publish().json({ id: "order-1", total: 42 }).send()
console.log(committed.confirmations)

const records = await (await topic.replay()).poll()
console.log(`read ${records.length} order(s)`)
```

Use a `user:password@host:port` connection string. The SDK supplies the Apache Iggy TCP scheme. Select a stream with `laser.stream(name)` and a topic with `.topic(name)`. `Laser.connectWithStream()` selects a default for the shorter `laser.topic(name)` form. It does not restrict stream access.

`Laser.connectEnv()` reads `LASER_CONNECTION_STRING` and optional `LASER_STREAM`. `Laser.local()` uses the default local server. `Laser.builder()` accepts a connection string, separate credentials, or an Apache Iggy client. For an injected client, select whether Laser SDK owns or borrows it.

For owned connections, the SDK retries initial connections and reconnects dropped sockets. The default is unlimited retries at one-second intervals. Set `reconnection_retries` to a non-negative integer to limit retries. Set `reconnection_interval` to a duration such as `250ms`, `1s`, or `1m`. The caller controls the lifecycle and reconnect policy of an injected client.

The TypeScript SDK uses Iggy's native VSR transport for owned and injected clients.

## Streaming model

`laser.stream(name).topic(name)` addresses any stream. `laser.topic(name)` is the default-stream shortcut. Topics support:

- raw, JSON, CBOR, MessagePack, Avro, Protobuf, and JSON Schema records
- exact headers, metadata, indexes, projection and schema IDs, inline payload, claim-check, routing keys, explicit partitions, and heterogeneous batches
- direct producers with bounded retry and explicit batching
- standalone and consumer-group readers with first, last, next, offset, or timestamp starts
- automatic or explicit offset commits, replay, cancellation, and bounded `nextWithin()` waits

Delivery is at least once, so records can repeat. Ordering applies within each selected partition. Make external effects idempotent, which means safe to repeat. For operations that require a lease holder token, use fenced managed coordination.

Producers and publish builders return `SendMessagesResponse`. A confirmation identifies a committed batch. It contains the stream, topic, partition, and first offset. The list can be empty when the server does not report offsets. Completion follows the topic durability policy.

## Runtime-checked records

TypeScript types do not exist at runtime. A typed topic therefore takes a codec that checks decoded values. A generic type parameter alone cannot check incoming bytes.

```ts
import { jsonCodec } from "@laserdata/laser-sdk"

interface Order {
  readonly id: string
  readonly total: number
}

const orderCodec = jsonCodec<Order>((value) => {
  if (typeof value !== "object" || value === null) throw new TypeError("order must be an object")
  const order = value as Record<string, unknown>
  if (typeof order.id !== "string" || typeof order.total !== "number") {
    throw new TypeError("order fields are invalid")
  }
  return { id: order.id, total: order.total }
})

const orders = laser.stream("commerce").topic("orders").json(orderCodec)
await orders.publish({ id: "order-1", total: 42 })

const reader = await orders.records("orders-export")
const record = await reader.nextWithin(1_000)
console.log(record?.value)
```

Registered Avro, Protobuf, and JSON Schema topics compile their writer schema once. They reject invalid values before sending and attach the schema ID. Reads decode through the same schema.

## Managed data surfaces

LaserData Cloud and Laser Stack report their capabilities at connection time and through `laser.refreshCapabilities()`. A backend that is not ready leaves its managed operations unavailable. The SDK repeats discovery without reconnecting. Apache Iggy without a managed backend returns `UnsupportedError` for managed calls. Use `laser.capabilities()` to inspect support.

The root client provides queries, projections, schemas, key-value state, forks, graphs, access roles, runs, change feeds, and managed batches. Query calls use managed commands. They do not use a request topic. Reference data from the Rust implementation defines the expected AGDX encoding for each client.

```ts
import { graphNodeEntity, queryResultValue, typedValueDiagnosticText } from "@laserdata/laser-sdk"

const paid = await laser.query("orders_v1").whereEq("status", "paid").limit(20).fetch()
for (const row of paid.rows) {
  const total = queryResultValue(paid, row, "total")
  console.log(total === undefined ? "missing" : typedValueDiagnosticText(total))
}

const key = new TextEncoder().encode("user:42")
const session = { cart: ["sku-1"] }
await laser.kv("sessions").set(key).json(session).ttl(300_000_000n).send()
const stored = await laser.kv("sessions").get(key)

const checkout = graphNodeEntity("Service", "checkout")
const nearby = await laser.graph("ops").neighbors(checkout.id, "out", undefined, 2)
```

`result.fields` defines the ordered result schema. Each `row.values` entry matches the field at the same position. Tagged values preserve numeric widths, decimal precision, timestamps, UUIDs, bytes, nested values, and nullability. Use `queryResultValue()` to select a field by name. Use `typedValueDiagnosticText()` for stable display text.

The first page can use an offset. Later pages use the `nextCursor` supplied by the server. `hasMore` is true exactly when that cursor is present. `fetchAll()` follows these cursors. Each page contains at most 1000 rows.

A query has a stable execution identity and absolute deadline. `status()` and `cancel()` use dedicated managed commands and fail locally when the deployment does not advertise those capabilities:

```ts
const request = laser.query("orders_v1").filterGte("total", 100).deadlineMicros(deadlineMicros)
const page = await request.fetch()
const status = await request.status()
if (status.state === "running") await request.cancel()
```

Lakehouse queries name one destination generation and can select a retained snapshot:

```ts
const historical = await laser
  .queryLakehouse(destinationId, destinationGeneration)
  .atSnapshot(snapshotId)
  .filterEq("customer_id", "alice")
  .limit(100)
  .fetch()

console.log(historical.context.resolvedTarget, historical.context.boundary)
```

The result context identifies the engine and target. Lakehouse pages also identify destination and backend generations, the table UUID, snapshot, schema, and partition-spec IDs. They include the materialization boundary, checkpoint revision, and global state revision.

Use `laser.destinations()` for destination declarations and explicit query routes. Reads select either potentially stale state or state ordered with completed writes. Writes compare the expected global and definition revisions:

```ts
const destinations = laser.destinations()
const page = await destinations.list("linearizable", {}, undefined, 50)
const routes = await destinations.queryRoutes("potentially_stale", "orders", undefined, 50)
const current = await destinations.get(destinationId, "linearizable")
```

For analytical batches, publish one self-contained Arrow IPC stream per message. Before sending, the SDK checks the metadata and exact byte length:

```ts
await topic
  .publish()
  .arrowIpc(arrowStream, {
    contractVersion: 1,
    schemaFingerprint,
    encodedBytes: BigInt(arrowStream.length),
    fieldCount: 8,
    recordBatchCount: 1,
    rowCount: 10_000n,
    dictionaryCount: 0
  })
  .send()
```

Arrow input must use a self-contained stream with microsecond timestamps and stable dictionaries. Dictionary replacement and deltas are not allowed. Decimal widths cannot exceed 128 bits. Unions and extension types are not supported.

Accessors select operations without performing I/O. Methods such as `send()`, `fetch()`, `poll()`, and `nextWithin()` perform the work.

A replay cursor saves offsets only after all partition reads succeed. A failed or canceled poll leaves them unchanged. Each request reads at most 10,000 messages. Further polls resume from the saved offsets. The TypeScript cursor stream continues waiting for new records until it is stopped.

Durable memory can use the default audit topic through `laser.memory(namespace)`, an existing isolated topic through `laser.memoryOnTopic(topic)`, or a configured topic:

```ts
const incidents = await laser.memoryTopic("incidents").partitions(4).ttl(86_400_000).build()

await incidents.remember(new TextEncoder().encode("checkout uses the read replica")).send()
```

TypeScript duration inputs use milliseconds. `noExpiry()` keeps the raw memory history until ordinary topic retention removes it.

**Filter before the network.** Configure a consumer group's policy once. Its ordinary consumers and page readers then receive the selected records with their original payloads, headers and offsets. The shared CDC feed delivers **4 of 240 records**, saving **98.5% of payload transfer**. An unbound group receives all records without payload decoding.

```ts
const safeMode = ConsumerFilter.json(
  FilterExpr.all([
    FilterExpr.pred("table", "eq", "satellites"),
    FilterExpr.pred("changed", "contains", "mode"),
    FilterExpr.pred("after.mode", "eq", "safe")
  ])
)
const topic = laser.stream("orbit").topic("fleet_changes")
const group = topic.consumerGroup("anomaly-desk")
await group.create({ filter: safeMode }) // Run once during setup.

// Every consumer instance needs only the group name or the returned group ID.
const consumer = await group.consumer({ batchLength: 100, autoCommit: false })
try {
  const record = await consumer.nextWithin(15_000)
  if (record !== null) {
    console.log(record.partitionId, record.offset, record.payload)
    await consumer.commit(record)
  }
} finally {
  await consumer.shutdown()
}
```

**`batchLength: 100` examines at most 100 source records per partition request.** It may deliver fewer matches, including none. An empty scan advances over rejected records and does not mean end of stream. The consumer continues bounded scans and waits only when caught up. Manual `commit(record)` stores safe contiguous progress after processing. The default automatic mode stores the delivered prefix before the next poll and at shutdown.

For batch handling, build `await group.reader().count(100).maxExamined(1000).build()`, then use `nextPage()` and `ackPage()`. Here `count` limits returned records, while `maxExamined` independently limits the scan. An unbound group returns all records. Each reader joins as a member, and Iggy distributes partitions across instances. Acknowledgments preserve pending earlier work and reject stale source or policy generations.

`group.filter()` provides `configure`, `get`, `revisions`, `revise`, `setRevisionEnabled`, `release`, `delete`, `preview` and `test`. `delete()` removes the group's own filter with every revision and releases the group first. Use `configureAs(operationId, policy)` when setup must resume with the same operation ID after a lost reply. `topic.consumerGroupId(id)` addresses the same saved group by numeric ID. Catalog failures, denied reads and paused revisions never broaden into unfiltered delivery. Original Apache Iggy uses native group consumption, while an older managed server without group-aware reads returns an explicit upgrade error.

Filters support JSON, CBOR, Avro, Protobuf and typed headers. `CompiledFilter.compile(filter).evaluate(record)` checks records locally with exact integers. `ConsumerFilter.withFaultPolicy`, `withForeignPolicy` and `withMismatchPolicy` control invalid records. Passed invalid records and unfiltered records have `evaluated` false. Malformed headers set `headersMalformed` without discarding valid entries or the payload. Server refusals throw `FilterExecutionError`, and fault or oversized stops throw `FilterStopError` with the stop reason. See the [Consumer Filters guide](https://docs.laserdata.cloud/laser-sdk/consumer-filters) and the [three-language examples](https://github.com/laserdata/laser-sdk/tree/main/examples).

## Agents and coordination

The agent layer adds record origins, typed AGDX messages, routing, discovery, retries, dead letters, contracts, and workflows. Consumers commit offsets after handling records. Workflow journals support replay, budgets, compensation, and fenced steps. Lease acquisition is not retried automatically after reconnect. If its outcome is unknown, it waits through the requested lifetime before returning `AmbiguousMutationError`. Workflows retain leases through verification and the completion journal write.

Sessions group one agent's conversation through `laser.sessions().create(id)`. `append(kind, data)` records a turn, and `context()` returns typed turns for a model. Turn kinds are `instruction`, `response`, `model.response`, `tool.call`, `tool.result`, and `human.input`. `memory().search(query)` searches memory scoped to the conversation.

A checkpoint records the next offset for each topic partition. `checkpoint()`, `turnsAt`, `turnsSince`, `stateAt`, and `replay` support reads and state reconstruction around those saved offsets. `Checkpoint.fromJSON(JSON.stringify(checkpoint))` restores a saved checkpoint. `laser.sessions({ stream, topics, memoryNamespace, contextTurns, contextTokens })` configures the stream, topics, memory namespace, and context limits.

The root package provides context, snapshots, memory, governance, intent records, signing, delegation, A2A, MCP, AG-UI, and edge authorization. A configured verifier rejects unsigned or invalid replies for contracts, shared reply readers, and `requestInput`. It binds signatures to the observed headers and server timestamp. An agent with a signing key signs its `respond` and `respondInput` replies. `KvKeyRegistry` manages versioned keys through the platform. The SDK records model-call metadata but does not call a model.

```ts
import { Agent, AgentId, AgentTopic } from "@laserdata/laser-sdk"

await using handle = Agent.builder()
  .id(AgentId.new("support"))
  .listenOn(AgentTopic.Commands)
  .respondOn(AgentTopic.Responses)
  .handler({
    handle(message, context) {
      return context.respond(message.payload)
    }
  })
  .spawn(laser)

await handle.ready()
// The agent now consumes with commit-after-handle delivery.
```

Waiting operations accept `AbortSignal` or an explicit timeout where their contract permits one. Owned roots and spawned handles must be closed or shut down. Scoped views do not own the shared connection.

## Errors and ownership

SDK failures extend `LaserError` and carry a stable `kind`. Separate subclasses identify configuration, timeout, cancellation, unknown mutation outcomes, unsupported operations, encoding, transport, policy, and signature failures. Catch a specific subclass when it needs different recovery. Otherwise, report the base error and its cause.

`Laser.connect*()` owns its Apache Iggy client. `Laser.builder()` can own or borrow an injected client. `Laser`, `Producer`, `Consumer`, and `AgentHandle` support `await using`. Their `close()` and `shutdown()` methods are safe to repeat. `close()` on any `Laser` view ends the shared connection for every view. Disposing a scoped view with `await using` leaves the root connection open.

## Package exports

- `@laserdata/laser-sdk` is the ordinary application surface
- `@laserdata/laser-sdk/full` adds the complete native wire namespace
- `@laserdata/laser-sdk/testing` provides clocks, stores, fake transports, factories, observers, and bounded eventually checks
- `@laserdata/laser-sdk/opentelemetry` adapts the observer seam to OpenTelemetry

`laser.iggyClient()` is the Apache Iggy escape hatch for native administrative or transport operations that Laser does not wrap.

## Examples and verification

The examples in [`examples/typescript`](../../examples/typescript/README.md) cover nine focused operations and nine larger applications. The shared scenarios under [`bdd/scenarios`](../../bdd/scenarios) describe behavior across clients.

```sh
npm ci
npm run verify
```

`verify` runs style, formatting, lint, dependency-boundary, type, build, API, unit, wire, coverage, license, and package tests. Integration and shared BDD tests run separately against the versioned native Iggy server.

## Security and license

Report security issues through the repository security policy. The package is Apache-2.0 licensed. Apache and Apache Iggy are trademarks of the Apache Software Foundation.

## Connect timeout

Connecting gives up after 30 seconds. Set another budget with the Rust `connect_timeout` builder method, the Python `connect_timeout_ms` connect argument, or the TypeScript `connectTimeout` builder method. The environment variable is `LASER_CONNECT_TIMEOUT_MS`, and explicit configuration overrides it. The budget covers the TCP dial, the TLS handshake, the login, and the capability probe. An expired budget returns a timeout error that says whether the server never accepted the connection or never answered the login. `stream(name).delete()` removes a stream you no longer need, and `close()` ends the shared connection.

See [connect timeout and cleanup](../../docs/connect-timeout.md).

## Publish recovery

Publish attempts default to 60 seconds with three retries. Retry delays start at 250 milliseconds, double after each failure, and stop increasing at 30 seconds. Configure these values through the client builder or connect arguments. The corresponding environment variables are `LASER_PUBLISH_TIMEOUT_MS`, `LASER_PUBLISH_MAX_RETRIES`, and `LASER_PUBLISH_RETRY_BACKOFF_MS`. Explicit configuration overrides these variables. Exhausted retries return an error for the application to handle.

See [publish recovery and outage handling](../../docs/publish-recovery.md).

## Consumer filter groups and offsets

**Provision a filtered group once, then consume by its ID.** The filter API supports create-and-bind setup, numeric group selection, and revision pause/resume. Use separate groups for A/B revisions so their offsets remain independent. A fresh named consumer using `Next` starts at the first retained record. Ordinary consumers auto-commit each polled batch before delivery by default, so a later consumer can resume after records the application did not process. Disable auto-commit and commit after successful processing when that matters. Filtered readers use explicit acknowledgments, and keep at most 1024 unacknowledged record-bearing pages per partition by default, set with `maxUnackedPages`, so a reader that never acknowledges stops at that bound instead of growing without limit. Unnamed TypeScript consumers have isolated identities and default to no automatic commit. Use a stable name to resume durable progress. See the [Consumer Filters guide](https://docs.laserdata.cloud/laser-sdk/consumer-filters).

**Filter fields inside JSON, CBOR, Avro, and Protobuf payloads on the server.** Avro and Protobuf use registered writer schemas, immutable schema IDs in the filter, and the `agdx.sid` header on each record. **Headers-only filters work with any payload format.** Filtering preserves original bytes and offsets. See the [Consumer Filters guide](https://docs.laserdata.cloud/laser-sdk/consumer-filters) for codec profiles and examples.

For an application checkpoint inside a filtered page, call `reader.ackThrough(record)` after persisting the checkpoint and processing all preceding records on that partition. Later records in the same page stay pending. Use `ackPage` when the whole page is complete. Regex predicates reject an enabled TypeScript local guard instead of silently skipping verification.

Consumer-filter patterns use the server's bounded Rust regex engine. TypeScript validates structure and lets the server decide regex syntax. A local `CompiledFilter` refuses regex predicates, because only the server's engine defines their meaning. A filter permits four compiled glob or regex predicates, with a 256 KiB program budget each. Local guards reject regex filters explicitly. Guards verify unevaluated records under the server's reported decoder bounds and the applicable pass policy.
