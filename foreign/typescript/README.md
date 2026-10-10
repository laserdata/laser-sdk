# LaserData - Laser SDK

This package provides the native TypeScript Laser SDK for Apache Iggy. [LaserData, Inc.](https://laserdata.com) maintains it. One connection supports streaming and managed operations for queries, state, forks, graphs, and agents.

This prerelease targets Node 22.14 or later. Bun, Deno, and browsers are not supported because the Apache Iggy transport uses Node TCP and TLS APIs.

> The current release is `0.7.0`. The wire contract and public API use semantic versioning. Before `1.0.0`, minor releases can contain breaking changes.

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
const topic = laser.stream("fleet").topic("readings")
await topic.ensure(4)
const committed = await topic.publish().json({ host: "node-7", cpu: 82 }).send()
console.log(committed.confirmations)

const records = await (await topic.replay()).poll()
console.log(`read ${records.length} reading(s)`)
```

Use a `user:password@host:port` or `token@host:port` connection string. The scheme is optional and may only be `iggy://` or `iggy+tcp://`. The port defaults to 8090. Credentials are required and taken verbatim, without percent-decoding. The options are exactly Apache Iggy's TCP set: `tls`, `tls_domain`, `tls_ca_file`, `reconnection_retries`, `reconnection_interval`, `reestablish_after`, `heartbeat_interval`, and `nodelay`. An unknown, repeated, or malformed option, an IPv6 literal, or a path fails with `ConfigError` before any dial, as in Rust and Python. `tls_ca_file` alone turns TLS on, even under `LASER_NO_TLS=1`. `tls=false` turns off the automatic TLS for LaserData hosts and `LASER_TLS_CERT`, and combining it with `tls_ca_file` is a `ConfigError`. The Node transport validates `reestablish_after` but reconnects on its own schedule. Select a stream with `laser.stream(name)` and a topic with `.topic(name)`. `Laser.connectWithStream()` selects a default for the shorter `laser.topic(name)` form. It does not restrict stream access.

`Laser.connectEnv()` reads `LASER_CONNECTION_STRING` and optional `LASER_STREAM`. `Laser.local()` uses the default local server. `Laser.builder()` accepts a connection string, `address(host, port?)` with the required `credentials(username, password)`, or an Apache Iggy client. For an injected client, select whether Laser SDK owns or borrows it.

For owned connections, the SDK retries a failed dial and reconnects dropped sockets, by default without limit at one-second intervals. The 30-second connect budget stops the retries of the initial connection. Add `reconnection_retries` to the connection string with a non-negative integer to limit retries, and `reconnection_interval` with a duration such as `250ms`, `1s`, or `1m`, for example `iggy:iggy@127.0.0.1:8090?reconnection_retries=5&reconnection_interval=250ms`. The caller controls the lifecycle and reconnect policy of an injected client.

The TypeScript SDK uses Iggy's native VSR transport for owned and injected clients.

## Streaming model

`laser.stream(name).topic(name)` addresses any stream. `laser.topic(name)` is the default-stream shortcut. Topics support:

- raw, JSON, CBOR, MessagePack, Avro, Protobuf, and JSON Schema records
- exact headers, indexes, projection and schema IDs, inline payload, claim-check, routing keys, explicit partitions, and heterogeneous batches
- direct producers with bounded retry, explicit batching, and stream and topic creation on the first send (`createStream`, `createTopic`, `partitions`)
- a size-and-time batching publisher through `topic.batching()`
- standalone and consumer-group readers with first, last, next, offset, or timestamp starts
- the ten Rust commit policies through `commitPolicy`, such as `{ kind: "disabled" }` for manual commits, explicit `commit`, `storeOffset`, `deleteOffset`, and `lastStoredOffset`, replay, cancellation, and bounded `nextWithin()` waits that fail with `TimeoutError`

A claim-checked agent record comes back through `agentMessageResolveBody(message, store)`: it resolves the AGDX envelope body (or the plain payload) when the content type is `ref`, checks the fetched bytes against the capsule's size and SHA-256, and throws `IntegrityError` on a mismatch. Any other record returns its body unchanged.

Delivery is at least once, so records can repeat. Ordering applies within each selected partition. Make external effects idempotent, which means safe to repeat. For operations that require a lease holder token, use fenced managed coordination.

Producers and publish builders return `SendMessagesResponse`. A confirmation identifies a committed batch. It contains the stream, topic, partition, and first offset. The list can be empty when the server does not report offsets. Completion follows the topic durability policy.

## Runtime-checked records

TypeScript types do not exist at runtime. A typed topic therefore takes a codec that checks decoded values. A generic type parameter alone cannot check incoming bytes. The `Json`, `Cbor`, `Msgpack`, and `Bson` codec classes take an optional decode check, and a custom `Codec<T>` declares its `contentType` with `encode` and `decode`.

```ts
import { Json } from "@laserdata/laser-sdk"

interface Reading {
  readonly host: string
  readonly cpu: number
}

const readingCodec = new Json<Reading>((value) => {
  if (typeof value !== "object" || value === null)
    throw new TypeError("a reading must be an object")
  const reading = value as Record<string, unknown>
  if (typeof reading.host !== "string" || typeof reading.cpu !== "number") {
    throw new TypeError("reading fields are invalid")
  }
  return { host: reading.host, cpu: reading.cpu }
})

const readings = laser.stream("fleet").topic("readings").json(readingCodec)
await readings.publish({ host: "node-7", cpu: 82 })

const reader = await readings.records("readings-export")
for await (const item of reader.stream()) {
  if (item.kind === "record") console.log(item.record.value)
}
```

`reader.next()` returns one decoded item and `undefined` once the reader is caught up. `reader.poll()` returns one bounded read, and `reader.stream()` ends once caught up, like Rust. A record that does not decode comes back as an error item with its log position, and the reader moves past it. A failed poll comes back from `next()` as an error item with no position. Each server request reads at most `batch(n)` records per partition (default 1000), and one poll drains each partition up to 10,000 records. `reader.offsets` is empty before the first poll unless `fromOffsets` seeded it. Persist it after a poll and pass it to `fromOffsets` to resume. `topic.replay()` returns a `Cursor` with the same offset, batch, and stream semantics.

Registered Avro, Protobuf, and JSON Schema topics compile their writer schema once. They reject invalid values before sending and attach the schema ID. Reads decode through the same schema.

## Managed data surfaces

LaserData Cloud and Laser Stack report their capabilities at connection time and through `laser.refreshCapabilities()`. A backend that is not ready leaves its managed operations unavailable. The SDK repeats discovery without reconnecting. Apache Iggy without a managed backend returns `UnsupportedError` for managed calls. Use `laser.capabilities()` to inspect support.

The root client provides queries, projections, schemas, key-value state, forks, graphs, access roles, change feeds, and managed batches. Query calls use managed commands. They do not use a request topic. Reference data from the Rust implementation defines the expected AGDX encoding for each client.

```ts
import { graphNodeEntity, queryResultValue, typedValueDiagnosticText } from "@laserdata/laser-sdk"

const degraded = await laser.query("readings_v1").whereEq("status", "degraded").limit(20).fetch()
for (const row of degraded.rows) {
  const cpu = queryResultValue(degraded, row, "cpu")
  console.log(cpu === undefined ? "missing" : typedValueDiagnosticText(cpu))
}

const key = new TextEncoder().encode("service:auth")
await laser.kv("config").set(key).json({ log_level: "debug" }).ttl(300_000).send()
const stored = await laser.kv("config").get(key)

const auth = graphNodeEntity("Service", "auth")
const nearby = await laser.graph("ops").neighbors(auth.id, "out", undefined, 2)
```

A `Laser` with a default stream names every managed resource it sends inside that stream, as `stream:<stream>/<name>`: KV, memory, lease, and fence namespaces, the key registry, graph names, projection ids and index names, fork ids, and the operational index of a query. A name that already starts with `stream:` is sent as is, and listings return only the stream's own names under the names the caller gave them. `laser.resourceName(name)` shows the name sent, `kv.resourceNamespace` and `fork.resourceId` the sent forms of a handle, and `Laser.builder().resourceNaming("bare")` or `laser.withResourceNaming("bare")` sends names exactly as written. Under stream tenancy, `laser.watch()` reads the stream's own change feed.

Graph ids are content-addressed like Rust and Python: `NodeId.content(label, valueBytes)` and `EdgeId.content(from, edgeType, to)` mint the same ids in every SDK. `graphNodeEntity`, `graphEdgeRelate`, `graphEdgeValid(edge, from?, to?)`, and `graphEdgeWithSource(edge, source)` build the node and edge values `upsert` takes. A window or a source never changes the edge id.

`result.fields` defines the ordered result schema. Each `row.values` entry matches the field at the same position. Tagged values preserve numeric widths, decimal precision, timestamps, UUIDs, bytes, nested values, and nullability. Use `queryResultValue()` to select a field by name. Use `typedValueDiagnosticText()` for stable display text.

The first page can use an offset. Later pages use the `result.page.nextCursor` supplied by the server. `result.page.hasMore` is true exactly when that cursor is present. `fetchAll()` follows these cursors. Each page contains at most 1000 rows.

A query has a stable execution identity and an absolute deadline, 30 seconds after the request was built unless you set `deadlineMicros(epochMicros)` or the relative `deadline(timeoutMs)`. `status()` and `cancel()` use dedicated managed commands and fail locally when the deployment does not advertise those capabilities. `laser.executeQuery(query)`, `queryPage(executionId, cursor, deadlineMicros)`, `queryStatus(executionId)`, and `cancelQuery(executionId)` are the lower-level forms:

```ts
const request = laser
  .query("readings_v1")
  .filterGte("cpu", { kind: "int", value: 90 })
  .deadline(10_000)
const page = await request.fetch()
const status = await request.status()
if (status.state === "running") await request.cancel()
```

`new QueryBuilder()` builds the `Query` that `laser.executeQuery(query)` takes, field for field like Rust `Query::builder()` and Python `QueryBuilder`. `executionId`, `target`, and `deadlineMicros` are required, and `build()` throws `InvalidError` when one is unset:

```ts
import { MintUlid, QueryBuilder, QueryExecutionId, operationalTarget } from "@laserdata/laser-sdk"

const query = new QueryBuilder()
  .executionId(MintUlid.mint(QueryExecutionId))
  .target(operationalTarget("readings_v1"))
  .deadlineMicros(BigInt(Date.now() + 10_000) * 1000n)
  .consistency("strong")
  .build()
const result = await laser.executeQuery(query)
```

Lakehouse queries name one destination generation and can select a retained snapshot:

```ts
const historical = await laser
  .queryLakehouse(destinationId, destinationGeneration)
  .atSnapshot(snapshotId)
  .filterEq("host_id", { kind: "string", value: "node-7" })
  .limit(100)
  .fetch()

console.log(historical.context.resolvedTarget, historical.context.boundary)
```

The result context identifies the engine and target. Lakehouse pages also identify destination and backend generations, the table UUID, snapshot, schema, and partition-spec IDs. They include the materialization boundary, checkpoint revision, and global state revision.

Use `laser.destinations()` for destination declarations and explicit query routes. Reads select either potentially stale state or state ordered with completed writes. Writes compare the expected global and definition revisions:

```ts
const destinations = laser.destinations()
const page = await destinations.list("linearizable", {}, undefined, 50)
const routes = await destinations.queryRoutes("potentially_stale", "readings", undefined, 50)
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

Durable memory can use the default `agent.memory` topic through `laser.memory(namespace)`, an existing isolated topic through `laser.memoryOnTopic(topic)`, or a configured topic. Each form needs a stream, from `Laser.connectWithStream()` or `withDefaultStream()`. `memoryTopic(topic).stream(name)` and `memoryOnTopic(topic, stream)` name one directly:

```ts
const incidents = await laser.memoryTopic("incidents").partitions(4).ttl(86_400_000).build()

await incidents.remember(new TextEncoder().encode("auth uses the read replica")).send()
```

`new KvStore(laser.kv(namespace))` is the managed `Kv` as a `StateStore`, beside `InMemoryStore` and `FileStore`, like Rust's `impl StateStore for Kv` and Python `KvStore`. Its `set` never expires.

Relative durations are milliseconds as `number` across the SDK: memory topic `ttl`, consumer intervals, KV `ttl`, `expire`, `lease`, and `renewLease`, and the producer `expireAfterMs`. Absolute times, such as KV `expireAt` and `expiresAt`, are epoch microseconds as `bigint`. `noExpiry()` keeps the raw memory history until ordinary topic retention removes it.

Configure a consumer group's policy once. Its ordinary consumers and page readers then receive the selected records with their original payloads, headers and offsets. An unbound group receives all records without payload decoding.

```ts
import { ConsumerFilter, FilterExpr } from "@laserdata/laser-sdk"

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
const consumer = await group.consumer({ batchLength: 100, commitPolicy: { kind: "disabled" } })
try {
  const record = await consumer.nextWithin(15_000) // TimeoutError when nothing matched in time
  console.log(record.position.partitionId, record.position.offset, record.json())
  await consumer.commit(record)
} finally {
  await consumer.shutdown()
}
```

**`batchLength: 100` examines at most 100 source records per partition request.** It may deliver fewer matches, including none. An empty scan advances over rejected records and does not mean end of stream. The consumer continues bounded scans and waits only when caught up. Manual `commit(record)` stores safe contiguous progress after processing. The default automatic mode stores the delivered prefix before the next poll and at shutdown.

For batch handling, build `await group.reader().count(100).maxExamined(1000).build()`, then use `nextPage()` and `ackPage()`. Here `count` limits returned records, while `maxExamined` independently limits the scan. An unbound group returns all records. Each reader joins as a member, and Iggy distributes partitions across instances. Acknowledgments preserve pending earlier work and reject stale source or policy generations.

`group.filter()` provides `configure`, `get`, `revisions`, `revise`, `setRevisionEnabled`, `release`, `delete`, `preview` and `test`. `delete()` removes the group's own filter with every revision and releases the group first. Use `configureAs(operationId, policy)` when setup must resume with the same operation ID after a lost reply. `topic.consumerGroupId(id)` addresses the same saved group by numeric ID. Catalog failures, denied reads and paused revisions never broaden into unfiltered delivery. Original Apache Iggy uses native group consumption, while an older managed server without group-aware reads returns an explicit upgrade error.

Filters support JSON, CBOR, Avro, Protobuf and typed headers. `CompiledFilter.compile(filter).evaluate(record)` checks records locally with exact integers. `ConsumerFilter.withFaultPolicy`, `withForeignPolicy` and `withMismatchPolicy` control invalid records. Passed invalid records and unfiltered records have `evaluated` false. Malformed headers set `headersMalformed` without discarding valid entries or the payload. Server refusals throw `FilterExecutionError`. A fault stop throws `FilterFaultError` with its `reason`, and an oversized stop throws `FilterOversizedRecordError`. `filterReason(error)` reads the reason from any of them. See the [Consumer Filters guide](https://docs.laserdata.cloud/laser-sdk/consumer-filters) and the [three-language examples](https://github.com/laserdata/laser-sdk/tree/main/examples).

## Agents and coordination

The agent layer adds record origins, typed AGDX messages, routing, discovery, retries, dead letters, contracts, and workflows. Consumers commit offsets after handling records. Workflow journals support replay, budgets, compensation, and fenced steps. Lease acquisition is not retried automatically after reconnect. If its outcome is unknown, it waits through the requested lifetime before returning `AmbiguousMutationError`. Workflows retain leases through verification and the completion journal write. A workflow run is a root session on `agent.sessions` whose id is the run id, and each step and compensation is a child session written by `ContractBuilder.parent(parent, root)`. The run ends completed, failed, or canceled, and a cancel request on `agent.control` stops it between steps with `CancelledError`, which names the run. `session.cancelRequested()` reads that request.

Every agent record of a stream rides one `agent.sessions` topic, keyed by session. `laser.bootstrap(partitions, retention)` creates it under a required `TopicRetention`, such as `TopicRetention.expireAfter(86_400_000)` for one day. A retention that never expires and has no size bound is refused. Bootstrap also creates `agent.heartbeats` with a one-hour expiry and the `agent.streams`, `agent.memory`, `agent.dlq`, `agent.audit`, and `agent.workflow_journal` topics. It never creates `agent.control`, which only operators may write. `laser.request` completes only on a correlated `response` or `error` in the request's conversation, addressed to the requesting agent when the request named one. A reliable consumer hands its handler only work: commands for an operation it serves and plain records, addressed to it or to every agent. Replies, status records, events, and records for other agents are skipped and committed. The `operations` option narrows the served operations. The consumer reads its group through the group-aware engine when the server resolves group policies, and natively otherwise, such as for a `Laser` built from a client you brought. Each agent id reads through its own group, named after the agent id unless `consumerGroup(..)` names another, so every instance of one agent shares its work. On `agent.sessions` and `agent.control`, a server that serves filtered reads and the filter catalog gets that group bound to the addressee filter `agdx.to In [<agent id>, "*"]` before the first read, and a group bound to another filter is refused with `ConsumerGroupSetupError`. A client brought without a connection string reads natively and binds no filter, with a `LaserWarning` when a managed plane is ready. `AgentHandle.ready()` rejects with the runtime's own error when it stopped before ready, and `ctx.requestAt()` returns the handled record's log position. A record that does not decode is dead-lettered under the conversation its header names, or one derived from its log position. `Agent.builder().sessions(config)` sets the `SessionConfig` the runtime applies, and under `failOnDeadLetter(true)` a dead-lettered record fails its session.

`laser.sessions().create(label)` derives a stable session id from the stream, an optional `namespace`, and the label, and `start()` mints a fresh one. Both return a builder that needs an `agent` and accepts `parent`, `withId`, `idleTimeout`, `budget`, and `tag`. `begin()` writes the session start and returns `{ session, lease }`. While a lease is held, the process lists the session in a heartbeat on `agent.heartbeats`. Release it when the work stops. `session.end()`, `fail(error)`, and `cancel()` write the terminal record once. A retry repeats the same record and a different verb is refused. `session.run(lease, work)` ends the session on success, fails it when `work` throws or rejects, rethrows that error, and releases the lease. `append(envelope)` writes one typed envelope of this session. `open(id)` reads any session without writing.

```ts
import { AgentId, TopicRetention } from "@laserdata/laser-sdk"

await laser.bootstrap(4, TopicRetention.expireAfter(86_400_000))
const { session, lease } = await laser
  .sessions()
  .create("incident")
  .agent(AgentId.new("planner"))
  .begin()
await session.run(lease, async () => {
  // The agent's work for this session.
})
```

The SDK never calls a model. `session.model(new ModelRequest(model, body), assembled)` records the request and returns a call to `complete` with the response or `fail`, and `recordModelCall` records a call that already happened. `session.tool(name, args)` does the same for a tool. Request bodies and tool arguments pass through `defaultRedact`, or the function given to `session.redact`. `session.assemble(policy)` returns the context fragments with a manifest, and `recordRetrieval(query, items)` and `recordCompaction(compaction)` record recalled items and summaries on the lane. Each `ContextMessage` carries its broker `timestampMicros` and the numeric `streamId` and `topicId` it was read from. `session.state()` keeps a JSON document as patches on the session lane with periodic snapshots, and `end()` snapshots changed state first. A handle's first write starts from the lane's current revision, read from the lane fold, or from the managed state view when the fold is incomplete and the view is not behind it, and `replace(document)` writes one patch that drops the missing keys and sets the rest. `sessions.submit(agent, input).from(submitter).send()` hands a new session to an agent, which marks it working when it picks the command up, and `ctx.session()` reaches that session from the handler. `sessions.control(stream, id).asOperator(operator)` sends `pause`, `resume`, `cancel`, and `forceCancel` on `agent.control`, which needs operator send rights, and `signedBy(key)` signs them. `session.pendingControl()` returns the pause and cancel requests sent to the session. Inside a handler the runtime follows every `agent.control` partition and keeps them current without interrupting the handler, which decides itself when to stop. A pause names the agents whose acknowledgments complete it, given by `participants(agents)` or read from the session lane. An agent with an id acknowledges each pause and resume it takes part in on the lane, parks work for a paused session as a `session_parked` event before committing it, and handles the held work once after the resume, before new work for the session. A cancel while paused ends the session canceled and leaves the held work unprocessed. After a restart, a rebalance, or a reopened consumer the runtime rebuilds this from a bounded read of the log. `session.parked()` lists the held records no agent reported handled, with `complete` false when the read could not prove the list whole. `session.overBudget()` says whether the session's recorded usage passed its budget: the session index answers on a deployment that indexes sessions, and the retained lane is folded otherwise. There, an agent with an id ends a session over its budget failed with reason `budget`, once, and commits its work without handling it, and a workflow whose run session passes its budget compensates and throws `BudgetExceededError`. A budget is a cooperative limit, not a hard spending cap. `session.signedBy(key)` signs the terminal record written by `end`, `fail`, and `cancel`. On a deployment that indexes sessions, `sessions.get(id)`, `list()` (narrowed by `root(id)` and `labelPrefix(prefix)`), `events(id)`, `state(id, historyLimit)`, `links(id)`, `sources(id)`, `changes(after, limit)`, `await watch(pollEveryMs)` (anchored when it resolves, so no later change is missed), and `session.status()` read the session index, and a typed failure throws `SessionError`. Open Apache Iggy refuses them with `UnsupportedError`. `laser.readAt(ref)` reads the one record a message reference names, or `undefined` once the record is gone or the topic was recreated.

`context()` returns the session's records as `SessionTurn` values, each with a timeline `display` type such as `session.started`, `tool.call`, or `model.response`. `memory().search(query)` searches memory scoped to the session. A checkpoint records the next offset for each partition of the session topic. `checkpoint()`, `turnsAt`, `turnsSince`, `stateAt`, and `replay` support reads and state reconstruction around those saved offsets. `Checkpoint.fromJSON(JSON.stringify(checkpoint))` restores a saved checkpoint. `new SessionConfig()` sets the stream, layout, heartbeat interval, idle timeout, memory namespace, and context limits. Under a `{ kind: "perAgentPartition", partitions }` layout, a command on `agent.sessions` lands on its addressee's declared partition and a reply on its requester's, while every other record stays on the session's partition. A `singlePartition` layout bootstraps one partition. Under `{ kind: "perAgentTopic", topics: new Map([["planner", "planner.inbox"]]) }`, a command, reply, or chunk on `agent.sessions` addressed to a declared agent lands on that agent's topic keyed by session, and lifecycle, state, broadcast records, and records for undeclared agents stay on the lane. `Sessions.bootstrap` creates the declared topics with the lane's partition count and retention and refuses `agent.sessions` or `agent.control` as a declared topic. A declared agent spawned on `agent.sessions` reads its own topic, and a declared requester waits for replies on its own topic.

The root package provides context, snapshots, memory, governance, intent records, signing, delegation, A2A, MCP, AG-UI, and edge authorization. A configured verifier rejects unsigned or invalid replies for contracts, shared reply readers, and `requestInput`. It binds signatures to the observed headers and server timestamp. An agent with a signing key signs its `respond` and `respondInput` replies. `KvKeyRegistry` manages versioned keys through the platform. The SDK records model-call metadata but does not call a model. AG-UI state is the session state document: `laser.publishStateSnapshot(source, conversation, state)` replaces and snapshots it, `publishStateDelta(source, conversation, patch)` patches it, and `reconstructState(conversation)` reads it from the session lane, or from the managed state view when the lane no longer holds the document's baseline. `aguiEvents` renders state records as `STATE_SNAPSHOT` with the document and `STATE_DELTA` with the patch.

```ts
import { Agent, AgentId, AgentTopic } from "@laserdata/laser-sdk"

await using handle = Agent.builder()
  .id(AgentId.new("support"))
  .listenOn(AgentTopic.Sessions)
  .respondOn(AgentTopic.Sessions)
  .handler({
    handle(message, ctx) {
      return ctx.respond(message.payload)
    }
  })
  .build()
  .spawn(laser)

await handle.ready()
// The agent now consumes with commit-after-handle delivery.
```

Waiting operations accept `AbortSignal` or an explicit timeout where their contract permits one. Owned roots and spawned handles must be closed or shut down. Scoped views do not own the shared connection.

## Errors and ownership

SDK failures extend `LaserError` and carry a stable `kind`. Separate subclasses identify configuration, timeout, cancellation, unknown mutation outcomes, unsupported operations, encoding, transport, policy, and signature failures. Catch a specific subclass when it needs different recovery. Otherwise, report the base error and its cause.

A failed publish throws `PublishFailedError` with the `stream`, `topic`, the `committed` confirmations, and the `unconfirmed` records. `publishCause()` reaches the original failure, so an `instanceof TransportError` check around a publish no longer matches. The classifiers mirror the Rust `LaserError` methods and answer through any publish wrapping: `isPermissionDenied`, `isUnsupported`, `isNotFound`, `isUnavailable`, `isNotLeader`, `isStale`, `isVersionSkew`, `isVersionConflict`, `isAmbiguousMutation`, `isStreamOrTopicNotFound`, `isNoCapableAgent`, `isLeaseLost`, `isFenceViolation`, `isBudgetExceeded`, `isQuarantined`, `isRetryable`, `filterReason`, `iggyErrorCode`, and `code`, which returns the unified result code. Routing failures throw `NoCapableAgentError`, `NoInboxError`, or `RoutePrincipalMismatchError`. Invalid identifiers throw `IdError` and malformed provenance headers throw `ProvenanceError`. Payloads are `Uint8Array`, so `TextEncoder` and `TextDecoder` convert text.

`Laser.connect*()` owns its Apache Iggy client. `Laser.builder()` can own or borrow an injected client. `Laser`, `Producer`, `Consumer`, and `AgentHandle` support `await using`. Their `close()` and `shutdown()` methods are safe to repeat. `close()` on any `Laser` view ends the shared connection for every view. Disposing a view from `withDefaultStream()` or another `with*` method with `await using` leaves the root connection open, so a process that disposes only the view keeps running on the open socket. Dispose or close the root. Rust and Python close the connection when the last handle is dropped, which JavaScript cannot observe, so in TypeScript the root `Laser` owns it.

## Connect timeout

Connecting gives up after 30 seconds. Set another budget in milliseconds with `Laser.builder().connectTimeout()` or the `LASER_CONNECT_TIMEOUT_MS` environment variable. Explicit configuration overrides the variable. The budget covers the TCP dial, the TLS handshake, the login, and the capability probe. An expired budget rejects with a `TimeoutError` that says whether the server never accepted the connection or never answered the login. `laser.stream(name).delete()` removes a stream you no longer need, and `laser.close()` ends the shared connection. See [connect timeout and cleanup](../../docs/connect-timeout.md).

## Publish recovery

Publish attempts default to 60 seconds with three retries. Retry delays start at 250 milliseconds, double after each failure, and stop increasing at 30 seconds. Set them with `publishTimeout`, `publishMaxRetries`, and `publishRetryBackoff` on `Laser.builder()`, or with `LASER_PUBLISH_TIMEOUT_MS`, `LASER_PUBLISH_MAX_RETRIES`, and `LASER_PUBLISH_RETRY_BACKOFF_MS`. Explicit configuration overrides the variables. Producer setup (creating the stream and topic) runs under the same timeout and retry budget, and a lost creation race against another producer counts as success. A background producer resends a failed write at once, then waits the configured backoff before each later resend without doubling it, and resends after any error except a lost confirmation, like Rust and Python. Exhausted retries reject with a `PublishFailedError`, so check `committed` before resending. A batching publisher from `topic.batching()` keeps a failed timer flush until `send()`, `flush()`, or `close()` reports it. See [publish recovery and outage handling](../../docs/publish-recovery.md).

## Consumer filter groups and offsets

**Provision a filtered group once, then consume by its ID.** The filter API supports create-and-bind setup, numeric group selection, and revision pause/resume. Use separate groups for A/B revisions so their offsets remain independent. A fresh named consumer using `Next` starts at the first retained record. Ordinary consumers auto-commit each polled batch before delivery by default, so a later consumer can resume after records the application did not process. Disable auto-commit and commit after successful processing when that matters. Filtered readers use explicit acknowledgments, and keep at most 1024 unacknowledged record-bearing pages per partition by default, set with `maxUnackedPages`, so a reader that never acknowledges stops at that bound instead of growing without limit. Unnamed TypeScript consumers have isolated identities and default to no automatic commit. Use a stable name to resume durable progress. See the [Consumer Filters guide](https://docs.laserdata.cloud/laser-sdk/consumer-filters).

Avro and Protobuf filters use registered writer schemas, immutable schema IDs in the filter, and the `agdx.sid` header on each record. Headers-only filters work with any payload format. Filtering preserves original bytes and offsets.

For an application checkpoint inside a filtered page, call `reader.ackThrough(record)` after persisting the checkpoint and processing all preceding records on that partition. Later records in the same page stay pending. Use `ackPage` when the whole page is complete.

Consumer-filter regex predicates use the Rust `regex` syntax the server compiles. TypeScript ports that syntax and runs it in time linear in the value, so validation refuses what the server refuses and a local `CompiledFilter` or `localGuard(true)` evaluates regex predicates with the same verdicts. Unicode properties and case folding come from Node's Unicode tables. `\p{Age=..}` and the grapheme, word, and sentence break properties are refused because JavaScript has no equivalent. The server measures its 256 KiB program budget on its own compiled program, and TypeScript checks a lower estimate, so a pattern near the limit can pass local validation and still be refused by the server. A filter permits four compiled glob or regex predicates. Guards verify unevaluated records under the server's reported decoder bounds and the applicable pass policy.

## Resource names

A `Laser` with a default stream scopes every managed name to that stream, `stream:<stream>/<name>`: KV and memory namespaces, leases and fences, the key registry, graph names, projection and index ids, query indexes, fork ids, and the watch index filter. Listings return local names, and schema calls carry the stream. `laser.resourceName(name)`, `Kv.resourceNamespace`, and `ForkHandle.resourceId` show the scoped name. `resourceNaming("bare")` on the builder or `laser.withResourceNaming("bare")` sends every name as written. `capabilities.streamTenancy` reports a deployment that enforces the scoping, and `ConsumerFilter.withSchemaStream(filter, stream)` names a filter's schema registry.

[Agents, groups, and layouts](../../docs/building-agents.md#agents-groups-and-layouts) explains how to pick a session layout.

## More APIs

These match the Rust and Python surfaces:

- Producer options `batchLength`, `lingerMs`, `maxTopicBytes`, `unlimitedTopicSize`, and `background`. In background mode a send returns once its records are buffered, and `shutdown()` drains the buffer, closes, and reports a background send failure.
- `QueryRequest.atSnapshot(id)`, `atTimestampMicros(ts)`, and `rowsTyped(codec)`.
- `MemoryHandle.backend`, `AgentScope.contract(router)`, and `SwappableGovernor.current()`.
- Capability helpers `isOpenOnly`, `servesConsistency`, `isReady`, `readinessReasons`, `enabledBackends`, and `unreadyBackends`.
- `AgdxSend.claimCheck(store, thresholdBytes)` moves a body at or over the threshold to the blob store before signing.
- `ConversationId.asU128()` returns the raw 128-bit ULID value.
- `MemoryHandle.consolidate` and `ScopedMemory.consolidate` take `{ summarizer, pruneSummarized }` to fold `message` items into one summary per conversation and to forget the folded items.
- `ScopedMemory.search(query, { limit, folded })` recalls up to 50 items by default, and `folded` folds the memory topic in process. `ContextScope.fetch` and `block` take an optional token budget applied after the count. `ContextScope.memoryWith(namespace, backend, embedder?)` matches Rust `memory_with`, and a vector backend without an embedder, or another backend with one, fails with `InvalidError` at open.
- `filterCapsEvaluates(filters, evaluatorVersion, codec)` tells whether the server evaluates a filter exactly as this build does.
- `KeyRecord.keyId()` returns the 8-byte identifier of the record's public key.
- `MemoryHandler` wraps an agent handler, and `autoRemember(kind)` remembers each handled message under its conversation.
- `cardIsFresh`, `cardServes`, and `cardAvailableFor` check the freshness, the skills, and the advertised health of a registered card.
- `new ProjectionBuilder(id)`, `new ProjectionBindingBuilder()`, and `new IndexSchemaBuilder()` build projection declarations with the Rust defaults.
- `laser.fork(id)` returns a `ForkHandle` whose `id` is the caller's name for the fork and whose `resourceId` is the id it sends.

## Package exports

- `@laserdata/laser-sdk` is the ordinary application surface
- `@laserdata/laser-sdk/full` adds the native `wire` namespace
- `@laserdata/laser-sdk/testing` provides `TestClock`, `InMemoryStore`, and the `agentMessage` and `agentCtx` factories for handler tests
- `@laserdata/laser-sdk/opentelemetry` adapts the observer seam to OpenTelemetry

`laser.client` (a property) is the Apache Iggy escape hatch for native administrative or transport operations that Laser does not wrap.

## Examples and verification

The examples in [`examples/typescript`](../../examples/typescript/README.md) cover nine focused operations and nine larger applications. The shared scenarios under [`bdd/scenarios`](../../bdd/scenarios) describe behavior across clients.

```sh
npm ci
npm run verify
```

`verify` runs formatting, lint, dependency-boundary, type, build, API, unit, wire, coverage, license, and package tests. Integration and shared BDD tests run separately against the versioned native Iggy server.

## Security and license

Report security issues through the repository security policy. The package is Apache-2.0 licensed. Apache and Apache Iggy are trademarks of the Apache Software Foundation.
