---
name: typescript-sdk
description: Work on the native TypeScript Laser SDK, its wire fixture port, Apache Iggy adapter, Node runtime, package exports, tests, BDD, examples, CI, and npm release gates under foreign/typescript, bdd/typescript, and examples/typescript.
---

# TypeScript SDK

Read [AGENTS.md](../../../AGENTS.md) and [laser-sdk-overview](../laser-sdk-overview/SKILL.md) first. Rust `laser-wire` types and fixtures remain authoritative. Never invent a TypeScript-only wire shape.

## Layout

- `foreign/typescript/src/wire`: native codecs and validators
- `foreign/typescript/src/iggy/apache-iggy.ts`: the only Apache Iggy and Node `Buffer` adaptation boundary
- `foreign/typescript/src/stream`, `managed`, `agent`, `memory`, `bridges`: public behavior by layer
- `foreign/typescript/test`: unit, wire, robustness, and real-Iggy integration
- `bdd/typescript`: every shared Gherkin scenario, no copied features
- `examples/typescript`: eight primitive and nine deep-dive mirrors

Public bytes are `Uint8Array`. Wire-sized u64 and u128 values are `bigint`. Public JSON is `unknown` until validated. Source is strict ESM, semicolon-free, has no public `any`, and uses no default exports. Managed operations negotiate capabilities and return `UnsupportedError` on Apache Iggy.

Lease acquisition is the managed-transport retry exception. `Kv.lease` disables reconnect replay for the acquisition request, waits through the requested TTL after an ambiguous transport result, then raises `AmbiguousMutationError`. Exclusive workflows keep their lease through verification and completion journaling, race an in-flight renewal against contract completion, and bound renewal by lease expiry and the workflow deadline.

Capability negotiation must match Rust and Python. `BackendAnnounce.ready !== true` cannot enable plane-served surfaces or expose stale backends. `refreshCapabilities()` and the one-second unavailable retry re-probe without reconnecting, preserve the builder's configured capability seed, and adopt announced topology except where the builder explicitly overrode a topology field. `withCapabilities()` remains an authoritative handle-local override and refresh must not replace it.

`src/iggy/apache-iggy.ts` uses Iggy's native VSR transport. Injected clients use the same transport, and `fromClient` only probes the injected client for liveness. LaserData hosts use TLS with the bundled root CA and explicit SNI. The Apache Iggy Node SDK supports VSR over TLS through its normal `getRawClient` path, with no Laser-side transport workaround. Synchronous client configuration failures must fail immediately and never enter the unlimited connection retry loop. This module is also the allocation boundary: a `Uint8Array` becomes a Node `Buffer` view over the same backing store rather than a copied buffer.

Direct and fluent streaming sends return Apache Iggy's `SendMessagesResponse`. Re-export its confirmation types, preserve all confirmations through the transport layer, and allow an empty list when the server cannot report offsets. A confirmation is an in-memory commit position, not an fsync guarantee.

Memory mirrors the Rust topology. `laser.memory(namespace)` uses the default audit topic, `laser.memoryOnTopic(topic, stream?)` opens an isolated existing topic, and `laser.memoryTopic(topic).stream(name).partitions(n).ttl(milliseconds).build()` configures one with message expiry. `noExpiry()` writes zero expiry. `laser.context(conversation).memory(handle)` must retain the exact topic-backed handle instead of substituting another namespace.

## Data stack parity

The native modules `wire/schema.ts`, `source.ts`, `destination.ts`, `checkpoint.ts`, `arrow.ts`, and Query in `wire/query.ts` mirror Rust field-for-field. Successful query replies must validate result fields, positional row width, tagged values against logical types, page cursor agreement, delivered consistency, and operational or lakehouse evidence before reaching application code.

`Laser.query()` and `queryLakehouse()` build explicit targets. `QueryRequest` supports snapshot selection, typed raw SQL parameters, cursor continuation, status, and cancellation. `Laser.destinations()` exposes public checkpoint mutations and bounded destination and route reads. Public checkpoint decoding must never accept replicated transitions.

Every managed command descriptor carries its capability surface and correct `OpVersions` field. Query commands use `versions.query`. Destination and checkpoint commands use `versions.checkpoint`. Cursor paging, execution status, and cancellation also check their dedicated query capability flags.

`PublishRequest.arrowIpc` and the batch peer validate metadata and exact payload length before I/O. Use `Uint8Array` for bytes and `bigint` for wire u64 and u128 values.

Any public change updates root and full exports, both API reports, unit and wire tests, robustness tests, fixture manifest, README examples, and the shared `data_stack.feature` steps. Run `build:test` before test runners so they execute the current source.

## Exports

- root: ordinary application API
- `./full`: root plus the complete wire namespace
- `./testing`: deterministic seams and factories
- `./opentelemetry`: optional observer adapter

Do not add deep package exports. Review generated API reports after every public change.

## Verification

From `foreign/typescript` run:

```sh
npm run verify
npm run test:integration
```

Then run `scripts/run-bdd-tests.sh typescript` and the example package tests against Apache Iggy. `verify` includes style, format, lint, emitted dependency cycles, strict types, builds, API reports, fixture and robustness tests, coverage, licenses, and packed ESM/CommonJS-interoperating consumers.

Node 22.14 and Node 24 are supported. Bun, Deno, and browsers are unsupported until their transport and complete gates pass. Release tags use `ts-v*` and publish the exact CI-produced tarball through npm OIDC.
