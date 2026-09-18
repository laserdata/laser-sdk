---
name: laser-sdk-overview
description: Entry point for the Rust reference SDK, Python bindings, native TypeScript SDK, and laser-wire contract over Apache Iggy. Load this first for changes under wire, sdk, foreign, bdd, or examples, then route to the focused area skill.
---

# LaserData - Laser SDK - Overview

Repo-wide rules (verification order, idiomatic-traits, no `cargo install`, no em dashes, BDD test naming) live in [AGENTS.md](../../../AGENTS.md). This file owns SDK-specific routing and the shared mental model.

## Contents

- [What this crate is](#what-this-crate-is)
- [Which skill to load](#which-skill-to-load)
- [The spine in one paragraph](#the-spine-in-one-paragraph)
- [Hard invariants](#hard-invariants)
- [Module map](#module-map)
- [Shipped vs planned](#shipped-vs-planned)
- [Cite by symbol, not line number](#cite-by-symbol-not-line-number)

## What this crate is

Laser SDK uses an append-only log for source records. Projections, queries, key-value state, and forks provide views over that data. AGDX defines the shared exchange contract.

The workspace publishes `laser-wire`, the Rust `laser-sdk`, Python bindings, and a native TypeScript client. Rust defines the codes, envelopes, dictionaries, limits, and reference files. Python calls the Rust implementation. TypeScript uses the same encoded files and behavior scenarios. The SDKs do not call language models.

`laser_sdk::prelude::*` imports common accessors and types, about 35 items. `laser_sdk::prelude::full::*` also imports bridge, projection, and memory types. Examples and integration tests use `full`. Application code can use the smaller prelude with explicit imports. `ReliableConsumer` is the public consumer, and `ReliableWorker` is its private message adapter.

`Laser::connect(connection_string)` opens a connection. `laser.stream(name)` selects a stream with `ensure()` and `topic(name)` methods. `laser.stream(name).topic(name)` addresses a topic explicitly. `laser.topic(name)` uses the default selected by `connect_with_stream` or `with_default_stream`. Without a default, it returns `NoStream`.

For `*.laserdata.cloud` and `*.laserdata.com`, Rust and Python enable TLS unless `tls_ca_file=` already supplies a certificate. `sdk/certs/laserdata.crt` supplies the bundled CA through `include_bytes!`. `resolve_tls` and `is_laserdata_host` in `sdk/src/laser.rs` implement selection. The cached certificate must match the bundled bytes and reside in an owner-only directory. `LASER_TLS_CERT=<path>` selects a CA for any host. `LASER_NO_TLS=1` disables automatic TLS, while `0` and `false` do not.

Other hosts retain their connection-string configuration. `Laser::connect_env()` reads `LASER_CONNECTION_STRING` and optional `LASER_STREAM`, with a typed `Config` error for missing required input. `Laser::local()` targets `iggy:iggy@127.0.0.1:8090`.

The `streaming` feature provides `publish()`, `publish_batch()`, `send(payload, headers, key)`, and `batch(messages, key)`. Typed and one-shot sends accept `impl Into<Vec<u8>>`. Continuous streaming uses `producer()`, `consumer(name, partition)`, and `consumer_group(group)`. `ProducerMessage` and `ConsumerMessage` retain `bytes::Bytes` for shared payload storage. These APIs support batching, delays, retries, discovery, routing, replay, groups, and commits.

`commit(&message)` stores an offset explicitly, and `next_within(timeout)` bounds a single-record wait. `replay()` creates a client-offset cursor, and `ensure(partitions)` creates the topic when absent. In `sdk/src/typed.rs`, `.json::<T>()` and `.cbor::<T>()` select codecs. `.schema::<T>(id)` compiles a registered schema, rejects invalid values, and adds `agdx.ct` and `agdx.sid`. `records(reader_name)` reports `TypedDecodeError { position, source }` and continues past an invalid record.

Use `iggy_producer()`, `iggy_consumer()`, `iggy_consumer_group()`, `Laser::client()`, and `laser_sdk::iggy` for direct Iggy access. Examples include `native-streaming`, `event-analytics`, and `order-book`. Default features are `streaming` and `provenance`. Agent and managed features are optional.

Every client uses standard Iggy transport. Managed reads use the non-replicated extension, and the three authorization writes use dedicated replicated operations. Managed calls return `LaserError::Unsupported` without a supporting backend. `BlobStore::put`, `BlobStore::get`, and one-shot managed calls use `Vec<u8>`. Conversion to Iggy `Bytes` occurs at the client-call boundary. `sdk/src/blob.rs` and `send_raw_with_response` define those paths.

## Which skill to load

- Wire contract types, codes, dictionaries, fixtures, the agent envelope (Agent Data Exchange Protocol), framing -> [wire-contract](../wire-contract/SKILL.md)
- Provenance runtime: `AgentTopic`, `Provenance` encode/decode, OTel/`agdx.*` aliasing, caps -> [provenance](../provenance/SKILL.md)
- `Laser` facade, reliable consumer, `Agent` builder, router, sessions, request/reply, shutdown -> [agent-runtime](../agent-runtime/SKILL.md)
- Reading the log back: `ContextAssembler`/policies, `ConversationState`, `Memory`/`LogMemory` -> [context-and-memory](../context-and-memory/SKILL.md)
- Example crate, the `LlmClient` seam, `TestIggy`, integration-test conventions -> [examples-and-testing](../examples-and-testing/SKILL.md)
- Typed records and publish live under the `streaming` feature and `laser_sdk::stream`. Queryable indexing directives stay on those records because they are written at append time.
- Preserve all `SendMessagesResponse` confirmations across retries. Each identifies a stream, topic, partition, and base offset. The server can return none when it does not report offsets. Completion follows the topic durability policy.
- Query DSL and `query()` (managed deployment only: the `AGDX_QUERY` managed command off the log via `send_raw_with_response`, Apache Iggy without a managed backend returns `Unsupported`. No topic request/reply query path) -> [query](../query/SKILL.md)
- Logical schemas, tagged positional query results, explicit lakehouse targets, destination and checkpoint state, Arrow IPC publishing, and the matching HTTP surface -> [query](../query/SKILL.md) and [wire-contract](../wire-contract/SKILL.md)
- Managed key-value store (`Laser::kv`, get/set/delete/scan, optional expiry) over the `AGDX_KV_*` managed commands, backed by `laser-plane` in Laser Stack or LaserData Cloud (`kv` feature, independently selectable from `query`. Client-only, backend is managed-side) -> [kv](../kv/SKILL.md) (wire the AGDX spec)
- `Laser::runs()` in `sdk/src/runs.rs` provides submission, status, cancellation intent, and paged listing through `AGDX_AGENT_*`. It requires `runs` and the `agent_workflow` capability. `submit_budgeted` uses `RunBudget` for events, calls, patches, depth, time, and cost. Exceeding a limit returns `LaserError::BudgetExceeded`. `.registered()` records the `run` metadata needed for the registry fold. Read [agent-runtime](../agent-runtime/SKILL.md).
- Capability RBAC over the managed surfaces (`Laser::whoami` + the role/binding/history verbs in `sdk/src/rbac/`, `rbac` feature: `effect feature:action [on resource]` grants assembled through roles bound to the server-stamped user, deny-wins, default-deny). Fork-native and journalled: the streaming server enforces feature+action+keyed-resource at the edge (`AGDX_AUTHZ_*` band, `authz` capability), orthogonal to Iggy's own `Permissions`. See [wire-contract](../wire-contract/SKILL.md) for the `authz` module.
- A2A, MCP, and AG-UI map external interfaces to AGDX. Features are `a2a-bridge`, `mcp-bridge`, and `agui`. Read [a2a-bridge](../a2a-bridge/SKILL.md) for task operations, cards, tools, and event rendering.
- The Python SDK under `foreign/python/` (PyO3 bindings over this crate), its maturin packaging, the `.pyi` stubs, the pytest suite, or the Python BDD runner -> [python-bindings](../python-bindings/SKILL.md)
- The native TypeScript SDK under `foreign/typescript/`, its Node transport, npm package, fixture port, Cucumber runner, and examples -> [typescript-sdk](../typescript-sdk/SKILL.md)

## The spine in one paragraph

`Provenance` requires `conversation_id` and encodes its fields into Iggy headers. The conversation ID selects the partition for agent records. `spawn_subconversation` preserves `parent_conversation_id` and `root_conversation_id`. Replies identify the source `MessageId` through `causal_parent`. These fields support causal reconstruction.

## Hard invariants

- Ordering is per-conversation only. No cross-conversation/-topic order. Context assembly orders best-effort by Iggy timestamp, deterministic on ties, not strictly chronological across topics (Apache Iggy cannot total-order).
- At-least-once + idempotent, never exactly-once. Handlers must tolerate redelivery. Dedup is a best-effort in-memory sliding window.
- The header set is a wire contract. Changing keys or the encode/decode breaks messages already on the log. Keys live in `wire/src/headers.rs`, and the encode/decode lives in [provenance](../provenance/SKILL.md).
- The fixture corpus pins the bytes. Any change under `wire/src/` that alters encoded bytes fails `wire/tests/wire_fixtures.rs`, and an intentional change regenerates (`just fixtures-regen`) and rides the release process.
- `ConversationId::derive` is versioned (FNV-1a + `DERIVE_VERSION`). Changing the algorithm without bumping the version remaps every `PerUser` conversation.

## Module map

The Structure section in [AGENTS.md](../../../AGENTS.md) lists modules. `stream.rs`, `typed.rs`, and `cursor.rs` provide streaming and replay. `provenance/` provides headers, and `agent/` provides the runtime. `context.rs`, `memory.rs`, and `agent/state.rs` read recorded state. `poll.rs` contains the shared bounded partition reader.

`govern.rs` defines `ActionGovernor`. `state_store.rs` defines `InMemoryStore`, `FileStore`, and KV state operations. `capabilities.rs` represents discovered support. `types/ids.rs` defines IDs and `MintUlid`.

`query/` provides operational and lakehouse queries, and `destinations.rs` provides declarations, checkpoints, and routes. Their workers run in the managed deployment. Read [query](../query/SKILL.md) and [wire-contract](../wire-contract/SKILL.md) for shared data types. Read [agent-runtime](../agent-runtime/SKILL.md) for `Deduplicator`, `Laser::capabilities`, and agent behavior.

## Shipped vs planned

The one canonical inventory lives in [AGENTS.md](../../../AGENTS.md#what-is-shipped-vs-planned), organized by area (core, RBAC, AGDX wire surface, dead letters, edge bridges, orchestration, planned). Read it there rather than a copy here, kept in one place so it does not drift.

## Cite by symbol, not line number

Refer to `Laser::send_agent`, `ReliableConsumer::consume`, `keys::CONVERSATION_ID`, not line numbers. Lines drift, symbols do not.

## Publish recovery

Publish attempts default to 60 seconds with three retries. Retry delays start at 250 ms, double after each failure, and stop increasing at 30 seconds.

Rust builder methods are `publish_timeout`, `publish_max_retries`, and `publish_retry_backoff`. Python `Laser.connect` keywords are `publish_timeout_ms`, `publish_max_retries`, and `publish_retry_backoff_ms`. TypeScript builder methods are `publishTimeout`, `publishMaxRetries`, and `publishRetryBackoff`. Explicit configuration overrides `LASER_PUBLISH_TIMEOUT_MS`, `LASER_PUBLISH_MAX_RETRIES`, and `LASER_PUBLISH_RETRY_BACKOFF_MS`.

Exhausted retries return an error for the application to handle. They do not exit the process. Preserve message identity and confirmed chunks across retries. See [publish recovery](../../../docs/publish-recovery.md).
