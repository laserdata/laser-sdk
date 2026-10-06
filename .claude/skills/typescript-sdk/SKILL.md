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
- `examples/typescript`: nine primitive and nine deep-dive mirrors

Public bytes are `Uint8Array`. Wire-sized u64 and u128 values are `bigint`. Public JSON is `unknown` until validated. Source is strict ESM, semicolon-free, has no public `any`, and uses no default exports. Managed operations negotiate capabilities and return `UnsupportedError` on Apache Iggy.

Lease acquisition is the managed-transport retry exception. `Kv.lease` disables reconnect replay for the acquisition request, waits through the requested TTL after an ambiguous transport result, then raises `AmbiguousMutationError`. Exclusive workflows keep their lease through verification and completion journaling, race an in-flight renewal against contract completion, and bound renewal by lease expiry and the workflow deadline.

Capability negotiation must match Rust and Python. `BackendAnnounce.ready === false` cannot enable plane-served surfaces or expose stale backends. An omitted ready field uses the wire's compatibility default. `refreshCapabilities()` and the one-second unavailable retry re-probe without reconnecting, preserve the builder's configured capability seed, and adopt announced topology except where the builder explicitly overrode a topology field. `withCapabilities()` remains an authoritative handle-local override and refresh must not replace it.

`src/iggy/apache-iggy.ts` uses Iggy's native VSR transport. Injected clients use the same transport, and `fromClient` only probes the injected client for liveness. LaserData hosts use TLS with the bundled root CA and explicit SNI. The Apache Iggy Node SDK supports VSR over TLS through its normal `getRawClient` path, with no Laser-side transport workaround. Synchronous client configuration failures must fail immediately and never enter the unlimited connection retry loop. This module is also the allocation boundary: a `Uint8Array` becomes a Node `Buffer` view over the same backing store rather than a copied buffer.

Streaming sends return Apache Iggy `SendMessagesResponse`. Re-export the confirmation types and preserve all confirmations through transport. An empty list is valid when the server does not report offsets. Completion follows the topic durability policy.

`laser.sessions(config?)` takes a `SessionConfig` and returns `Sessions`, with `create(id)`, `start()`, and `open(conversation)` methods that return a `Session`. `append(kind, data)` records a turn. `context()` and `contextWith(policy)` return `SessionTurn[]`. Each turn contains its `kind` and a `ContextMessage` with the source `topic`. `memory(namespace?)` returns `ScopedMemory`, which provides `remember`, `recall`, `search(query)`, `block(tokenBudget)`, `consolidate(maxItems)`, `forget`, and `improve`.

`checkpoint()` returns a `Checkpoint`, with `toJSON` and `Checkpoint.fromJSON` for persistence. `turnsAt`, `turnsSince`, `stateAt`, and `replay` read around its saved offsets. `new SessionConfig().stream(name).topic(kind, topic).memoryNamespace(ns).contextTurns(n).contextTokens(n)` chains the settings, and its getters are `streamName`, `memoryNamespaceName`, `contextTurnBound`, `contextTokenBound`, and `topics()`. Each kind needs a distinct topic. `laser.sessions` raises `InvalidError` for shared topics.

The context assembler reads each partition through the Rust window: at most the newest `CONTEXT_READ_WINDOW` (10,000) raw records, tail anchored, or the window ending at a `toCheckpoint`, then filters by conversation. A checkpoint resume ignores `fromOffsets`, as in Rust. `contextCheckpoint(laser, topics)` captures a checkpoint. `ContextAssembler.builder().conversationId(id)` starts an assembly, and `fromCheckpoint` and `toCheckpoint` read around a checkpoint, while state replay uses the whole range for `from-checkpoint` and `at` bounds. A topic without records assembles as empty.

Memory follows the Rust structure. `laser.memory(namespace)` uses the default audit topic. `laser.memoryOnTopic(topic, stream?)` opens an isolated existing topic. `laser.memoryTopic(topic).stream(name).partitions(n).ttl(milliseconds).build()` configures a topic with message expiry. `noExpiry()` creates the topic with never-expire retention. `laser.context(conversation).memory(handle)` must retain the exact topic-backed handle instead of substituting another namespace. `MemoryHandle.set`, `fetch`, `fetchFolded`, `update`, and `remove` address named facts like Rust and throw `UnsupportedError` on a non-log backend. `LogMemory` spells them `setNamed`, `fetchNamed`, `fetchNamedFolded`, `updateNamed`, and `forgetNamed`. `memory.remember(payload).scope(conversation)` and `memory.recall(conversation)` scope a call, and `RecallBuilder.block(tokenBudget?)` renders a block.

## Streaming and error parity

0.6.0 parity additions: producer options `batchLength` (1000, splits a direct batch), `lingerMs`, `maxTopicBytes`, `unlimitedTopicSize`, and `background` (`ProducerBackgroundOptions` with Iggy's `BackgroundConfig` defaults, one ordered worker, a send returns once queued, `onError`, and `shutdown()` drains, closes, and reports a background failure). `Producer.flush` is internal. `QueryRequest.atSnapshot`, `atTimestampMicros` (lakehouse only), `rowsTyped(codec)` (needs `maxRows`). `MemoryHandle.backend` reports `log`, `vector`, or `custom`. `AgentScope.contract(router)`. Capability helpers `isOpenOnly`, `servesConsistency`, `isReady`, `readinessReasons`, `enabledBackends`, `unreadyBackends`, `backend`, `OPEN_CAPABILITIES` are exported. `SwappableGovernor.current()`, and `swap()` returns the previous governor.

- `Consumer.nextWithin(ms, { signal })` returns a `ConsumerMessage` and throws `TimeoutError` on deadline and `InvalidError` after shutdown, like Rust `next_within`. `ConsumerOptions` carries `commitPolicy` (the ten Rust variants, such as `{ kind: "disabled" }`), `autoJoinGroup`, `createGroup`, `pollingRetryIntervalMs`, `initRetries`, and `allowReplay`. The default `batchLength` is 1000. `storeOffset`, `deleteOffset`, `lastStoredOffset`, and `storedOffset` mirror Rust.
- `Producer` creates its stream and topic before the first send unless `createStream` or `createTopic` is false, and takes `partitions`, `expireAfterMicros`, and `neverExpire`. `Topic.batching()` returns `BatchingProducerBuilder` (`maxRecords`, `maxBytes`, `linger`, `partitionKey`) in `src/stream/batching.ts`.
- A failed send throws `PublishFailedError` with `stream`, `topic`, `committed`, `unconfirmed`, and `publishCause()`. `src/client/error-classify.ts` holds the Rust classifier family (`isPermissionDenied`, `isUnsupported`, `isNotFound`, `isUnavailable`, `isNotLeader`, `isStale`, `isVersionSkew`, `isVersionConflict`, `isAmbiguousMutation`, `isStreamOrTopicNotFound`, `isNoCapableAgent`, `isLeaseLost`, `isFenceViolation`, `isBudgetExceeded`, `isQuarantined`, `filterReason`, `iggyErrorCode`, `code`). Each one answers through publish wrapping. `FenceViolationError`, `QuarantinedError`, `CheckpointExecutionError`, and `NoRespondTopicError` mirror the Rust variants.
- `Kv.expire(key, ttlMicros?, nowMicros?)` takes a TTL like Rust and Python, and `expireAt` takes an absolute time. `KvSetRequest.send()` throws `InvalidError` when a precondition was set. `msgpack` is available on the set and fenced CAS builders. Fork `embedding` takes `Iterable<number>`. Payload text goes through `TextEncoder` and `TextDecoder`, since the root exports no UTF-8 helpers.
- Names follow Rust: `Laser.client`, `Laser.fromClient`, `dlqTopic`, `withDlqTopic`, `aggAs`, `stddev`, `asStr`, and `ForkHandle.id`. Deliberate idioms recorded in the parity matrix are microsecond `ttl(ttlMicros)` and `Map` offsets.
- Handles have no public constructor where Rust has none. `Topic`, `Stream`, `Producer`, `Consumer`, `QueryRequest`, `Sessions`, `MemoryHandle`, `AgentHandle`, `AgentRegistry`, `ContextScope`, and the builders come from `Laser` or `Agent.builder()...build().spawn(laser)`. `laser.advertisePresence` takes a wire `AgentPresence` from `newAgentPresence(agent.wireId(), inbox)`. Route policies and gather policies are tagged objects such as `{ kind: "any" }` and `{ kind: "requireAll" }`.

## Data stack parity

The native modules `wire/schema.ts`, `source.ts`, `destination.ts`, `checkpoint.ts`, `arrow.ts`, and Query in `wire/query.ts` mirror Rust field-for-field. Successful query replies must validate result fields, positional row width, tagged values against logical types, page cursor agreement, delivered consistency, and operational or lakehouse evidence before reaching application code.

`Laser.query()` and `queryLakehouse()` build explicit targets. `QueryRequest` supports snapshot selection, typed raw SQL parameters, cursor continuation, status, and cancellation. `Laser.destinations()` exposes public checkpoint mutations and bounded destination and route reads. Public checkpoint decoding must never accept replicated transitions.

Every managed command descriptor carries its capability surface and correct `OpVersions` field. Query commands use `versions.query`. Destination and checkpoint commands use `versions.checkpoint`. Cursor paging, execution status, and cancellation also check their dedicated query capability flags.

`PublishRequest.arrowIpc` and the batch peer validate metadata and exact payload length before I/O. Use `Uint8Array` for bytes and `bigint` for wire u64 and u128 values.

Any public change updates root and full exports, both API reports, unit and wire tests, robustness tests, fixture manifest, README examples, and the shared `data_stack.feature` steps. Run `build:test` before test runners so they execute the current source. Then regenerate `docs/parity.md` with `python3 scripts/check-parity.py --write` and run `just parity-check`.

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

Then run `scripts/run-bdd-tests.sh typescript` and the example package tests against Apache Iggy. `verify` includes format, lint, emitted dependency cycles, strict types, builds, API reports, fixture and robustness tests, coverage, licenses, and packed ESM/CommonJS-interoperating consumers.

Node 22.14 and Node 24 are supported. Bun, Deno, and browsers are unsupported until their transport and complete gates pass. Release tags use `ts-v*` and publish the exact CI-produced tarball through npm OIDC.

## Publish recovery

Defaults, failure reports, and outage handling are in [publish recovery](../../../docs/publish-recovery.md) and [connect timeout and cleanup](../../../docs/connect-timeout.md).

In `foreign/typescript/src/iggy/apache-iggy.ts`, a socket replacement during a leader change does not count as a lost connection. Allow the current send to finish. Run only one publish attempt per connection at a time. The Apache Iggy client resends all queued commands when it changes nodes. A second queued send can return the connection to the metadata leader. The three-node reconnect test depends on both rules.

## Consumer-group filters

Group handles, filter setup, scan budgets, progress, and cross-language test parity follow the [consumer-filters](../consumer-filters/SKILL.md) skill.

## Coordination and lifecycle

`src/managed/coordination.ts` holds `FencedLeaseClient`, `PreparedMutation`, and `DedicatedKvTransport`: prepared operations with stable bytes, readiness checked before a request is sent, operation-specific recovery, and a terminal `close`. Background `onError` callbacks can return promises. Low-level reliable consumers take `shutdownGraceMs`. `ConsolidationReport` uses `summarized`, `reweighted`, `pruned`, and `derived`. The upgrade steps from 0.5 are in [client behavior](../../../docs/client-behavior.md).

Native partition and group polls preserve microsecond timestamps, message IDs, checksums, current offsets, and raw user headers. Malformed header blocks are marked on each record. Native polling defaults to no delay. Lease attempts use the dedicated coordination transport and its 10-second attempt bound. An injected client cannot create that dedicated connection.
