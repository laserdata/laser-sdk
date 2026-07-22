# Laser SDK for TypeScript

The native TypeScript client for LaserData over Apache Iggy. It provides typed
streaming, declared projections and query, key-value state, copy-on-write
forks, a knowledge graph, and an optional agent runtime over one connection.

This prerelease targets Node 22.14 or later. Bun, Deno, and browsers are not
supported because the Apache Iggy transport uses Node TCP and TLS APIs.

## Install

```sh
npm install @laserdata/laser-sdk
```

The package is ESM-first. A CommonJS application can load it with dynamic
`import()`.

## Connect and stream

```ts
import { Laser, jsonCodec } from "@laserdata/laser-sdk"

interface Order {
  readonly id: string
  readonly total: number
}

const orderCodec = jsonCodec<Order>((value) => {
  if (typeof value !== "object" || value === null) throw new Error("order must be an object")
  const order = value as Record<string, unknown>
  if (typeof order.id !== "string" || typeof order.total !== "number") {
    throw new Error("order has invalid fields")
  }
  return { id: order.id, total: order.total }
})

const laser = await Laser.connectWithStream(
  process.env.LASER_CONNECTION_STRING ?? "iggy://iggy:iggy@127.0.0.1:8090",
  "commerce"
)

try {
  const topic = laser.topic("orders")
  await topic.ensure(4)
  await topic.publish().json({ id: "order-1", total: 42 }, orderCodec).send()

  const record = await topic.json(orderCodec).records("orders-export").nextWithin(1_000)
  console.log(record?.value)
} finally {
  await laser.close()
}
```

Use `Laser.connect()` when no default stream is needed, `Laser.connectEnv()`
for `LASER_CONNECTION_STRING` and `LASER_STREAM`, or `Laser.local()` for the
default local server. `Laser.builder()` accepts a connection string, an address
and credentials, or an injected Apache Iggy client with explicit owned or
borrowed lifetime.

LaserData Cloud hostnames automatically use TLS with the public LaserData root
CA embedded in the package. `LASER_TLS_CERT` selects another CA file and
`LASER_NO_TLS` disables automatic TLS. Other hosts keep the connection string's
TLS settings unchanged.

## Streaming model

`laser.stream(name).topic(name)` addresses any stream. `laser.topic(name)` is
the default-stream shortcut. Topics support:

- raw, JSON, CBOR, MessagePack, Avro, Protobuf, and JSON Schema records
- exact headers, metadata, indexes, projection and schema IDs, inline payload,
  claim-check, routing keys, explicit partitions, and heterogeneous batches
- direct producers with bounded retry and explicit batching
- standalone and consumer-group readers with first, last, next, offset, or
  timestamp starts
- automatic or explicit offset commits, replay, cancellation, and bounded
  `nextWithin()` waits

Delivery is at least once. Ordering is per selected partition. A handler should
make external effects idempotent, or use the fenced managed coordination path
when a monotonic holder token is required.

## Managed data surfaces

LaserData Cloud announces its capabilities during connection setup. Stock
Apache Iggy serves streaming and returns `UnsupportedError` for managed calls.
The same client code can feature-detect through `laser.capabilities()`.

The root client exposes query, projections, bindings, schemas, key-value and
CAS, forks, graph traversal, RBAC, runs, watch feeds, and independent managed
command batches. Query uses the managed command path, not a request topic.
Every command is encoded through the same Rust-owned AGDX fixtures consumed by
the other SDKs.

## Agents and coordination

The agent layer adds provenance, typed AGDX commands and responses, chunked
streams, registry and presence, routing, reliable commit-after-handle delivery,
deduplication, retry and dead-letter handling, contracts, scatter, and workflow
execution. Workflow journals support replay and resume, verifier panels,
budgets, compensation, and fenced steps.

Context, snapshots, log and vector memory, action governance, replayable
intent decisions, Ed25519 signing, delegation, A2A, MCP, AG-UI, and edge
authorization are available from the root package. The SDK transports model
provenance but does not invoke a model.

Waiting operations accept `AbortSignal` or an explicit timeout where their
contract permits one. Owned roots and spawned handles must be closed or shut
down. Scoped views do not own the shared connection.

## Package exports

- `@laserdata/laser-sdk` is the ordinary application surface
- `@laserdata/laser-sdk/full` adds the complete native wire namespace
- `@laserdata/laser-sdk/testing` provides clocks, stores, fake transports,
  factories, observers, and bounded eventually checks
- `@laserdata/laser-sdk/opentelemetry` adapts the observer seam to OpenTelemetry

`laser.iggyClient()` is the Apache Iggy escape hatch for native administrative
or transport operations that Laser does not wrap.

## Examples and verification

The nine non-benchmark examples live in
[`examples/typescript`](../../examples/typescript/README.md). Shared behavior is
covered by every feature under [`bdd/scenarios`](../../bdd/scenarios).

```sh
npm ci
npm run verify
```

`verify` runs style, formatting, lint, dependency boundaries, strict types,
builds, API reports, unit and wire tests, robustness, coverage, licenses, and
packed-consumer checks. Live Apache Iggy integration and shared BDD are separate
Docker-backed gates.

## Security and license

Report security issues through the repository security policy. The package is
Apache-2.0 licensed. Apache and Apache Iggy are trademarks of the Apache
Software Foundation.
