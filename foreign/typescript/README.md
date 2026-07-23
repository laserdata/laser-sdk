# Laser SDK for TypeScript

The native TypeScript client for LaserData over Apache Iggy. It provides typed streaming, declared projections and query, key-value state, copy-on-write forks, a knowledge graph, and an optional agent runtime over one connection.

This prerelease targets Node 22.14 or later. Bun, Deno, and browsers are not supported because the Apache Iggy transport uses Node TCP and TLS APIs.

> **Prerelease (`0.0.1-rc.2`).** Pin an exact version while the public API and wire contract are still in release-candidate development.

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
await topic.publish().json({ id: "order-1", total: 42 }).send()

const records = await (await topic.replay()).poll()
console.log(`read ${records.length} order(s)`)
```

Use the bare `user:password@host:port` connection string. The SDK supplies the Apache Iggy TCP scheme internally. One connection addresses every stream on the server. Select a stream explicitly with `laser.stream(name)`, then a topic with `.topic(name)`. `Laser.connectWithStream()` only pins a default stream so `laser.topic(name)` can be used as a shortcut. It does not limit the connection to that stream. `Laser.connectEnv()` reads `LASER_CONNECTION_STRING` and the optional `LASER_STREAM`. `Laser.local()` targets the default local server. `Laser.builder()` accepts a connection string, an address and credentials, or an injected Apache Iggy client with explicit owned or borrowed lifetime.

Owned connections retry the initial handshake and reconnect after a dropped socket, with unlimited retries at one-second intervals by default. Set `reconnection_retries` to a non-negative integer and `reconnection_interval` to `250ms`, `1s`, or `1m` in the connection string to tune the policy. Laser owns this loop because the upstream Node reconnect path can leave its connect promise pending. An injected client remains under its caller's lifecycle and reconnect policy.

LaserData Cloud hostnames automatically use TLS with the public LaserData root CA embedded in the package. `LASER_TLS_CERT` selects another CA file and `LASER_NO_TLS` disables automatic TLS. Other hosts keep the connection string's TLS settings unchanged.

## Streaming model

`laser.stream(name).topic(name)` addresses any stream. `laser.topic(name)` is the default-stream shortcut. Topics support:

- raw, JSON, CBOR, MessagePack, Avro, Protobuf, and JSON Schema records
- exact headers, metadata, indexes, projection and schema IDs, inline payload, claim-check, routing keys, explicit partitions, and heterogeneous batches
- direct producers with bounded retry and explicit batching
- standalone and consumer-group readers with first, last, next, offset, or timestamp starts
- automatic or explicit offset commits, replay, cancellation, and bounded `nextWithin()` waits

Delivery is at least once. Ordering is per selected partition. A handler should make external effects idempotent, or use the fenced managed coordination path when a monotonic holder token is required.

## Runtime-checked records

TypeScript types disappear at runtime. A typed topic therefore takes an explicit codec that validates decoded values instead of pretending a generic parameter can validate bytes.

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

Registered Avro, Protobuf, and JSON Schema topics compile the writer schema once, validate before transport I/O, stamp its schema ID, and decode through the same schema on reads.

## Managed data surfaces

LaserData Cloud announces its capabilities during connection setup. Stock Apache Iggy serves streaming and returns `UnsupportedError` for managed calls. The same client code can feature-detect through `laser.capabilities()`.

The root client exposes query, projections, bindings, schemas, key-value and CAS, forks, graph traversal, RBAC, runs, watch feeds, and independent managed command batches. Query uses the managed command path, not a request topic. Every command is encoded through the same Rust-owned AGDX fixtures consumed by the other SDKs.

```ts
import { graphNodeEntity } from "@laserdata/laser-sdk"

const paid = await laser.query("orders_v1").filterEq("status", "paid").limit(20).fetch()

const key = new TextEncoder().encode("user:42")
const session = { cart: ["sku-1"] }
await laser.kv("sessions").set(key).json(session).ttl(300_000_000n).send()
const stored = await laser.kv("sessions").get(key)

const checkout = graphNodeEntity("Service", "checkout")
const nearby = await laser.graph("ops").neighbors(checkout.id, "out", undefined, 2)
```

Accessors are cheap to construct. I/O happens at terminal verbs such as `send()`, `fetch()`, `poll()`, and `nextWithin()`.

Durable memory can use the default audit topic through `laser.memory(namespace)`, an existing isolated topic through `laser.memoryOnTopic(topic)`, or a configured topic:

```ts
const incidents = await laser.memoryTopic("incidents").partitions(4).ttl(86_400_000).build()

await incidents.remember(new TextEncoder().encode("checkout uses the read replica")).send()
```

TypeScript duration inputs use milliseconds. `noExpiry()` keeps the raw memory history until ordinary topic retention removes it.

## Agents and coordination

The agent layer adds provenance, typed AGDX commands and responses, chunked streams, registry and presence, routing, reliable commit-after-handle delivery, deduplication, retry and dead-letter handling, contracts, scatter, and workflow execution. Workflow journals support replay and resume, verifier panels, budgets, compensation, and fenced steps.

Context, snapshots, log and vector memory, action governance, replayable intent decisions, Ed25519 signing, delegation, A2A, MCP, AG-UI, and edge authorization are available from the root package. The SDK transports model provenance but does not invoke a model.

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

All SDK failures extend `LaserError` and carry a stable `kind`. Configuration, timeout, cancellation, unsupported managed capability, codec, transport, policy, and signature failures have dedicated subclasses. Catch the narrow subclass when recovery differs, otherwise report the base error with its cause.

`Laser.connect*()` owns its Apache Iggy client. `Laser.builder()` can instead borrow or own an injected client explicitly. `Laser`, `Producer`, `Consumer`, and `AgentHandle` support `await using`. Their explicit `close()` and `shutdown()` methods remain idempotent. Closing a scoped view never closes the root connection.

## Package exports

- `@laserdata/laser-sdk` is the ordinary application surface
- `@laserdata/laser-sdk/full` adds the complete native wire namespace
- `@laserdata/laser-sdk/testing` provides clocks, stores, fake transports, factories, observers, and bounded eventually checks
- `@laserdata/laser-sdk/opentelemetry` adapts the observer seam to OpenTelemetry

`laser.iggyClient()` is the Apache Iggy escape hatch for native administrative or transport operations that Laser does not wrap.

## Examples and verification

The nine non-benchmark examples live in [`examples/typescript`](../../examples/typescript/README.md). Shared behavior is covered by every feature under [`bdd/scenarios`](../../bdd/scenarios).

```sh
npm ci
npm run verify
```

`verify` runs style, formatting, lint, dependency boundaries, strict types, builds, API reports, unit and wire tests, robustness, coverage, licenses, and packed-consumer checks. Live Apache Iggy integration and shared BDD are separate Docker-backed gates.

## Security and license

Report security issues through the repository security policy. The package is Apache-2.0 licensed. Apache and Apache Iggy are trademarks of the Apache Software Foundation.
