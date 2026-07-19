---
name: laser-sdk-overview
description: Entry point and index for the laser-sdk workspace (the laser-wire contract crate plus the laser-sdk agentic data-platform SDK over Apache Iggy: streaming foundation plus projections/query, key-value, and forks). Load this first for any change under `wire/`, `sdk/`, or `examples/`, then route to the focused per-area skill. Covers the module map, the shipped-vs-planned boundary, and SDK-wide conventions.
---

# Laser SDK - Overview

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

This is a data platform on a durable log: ultra-low-latency streaming is the foundation, and projections with a query DSL, a key-value store, and copy-on-write forks are read models on top of the one source-of-truth log. The wire contract is the Agent Data Exchange Protocol (AGDX).

One workspace, two published crates. **laser-wire** (`wire/`) is the LaserData wire contract: every command code, envelope, header/topic dictionary, cap, the Agent Data Exchange Protocol envelope, and the golden fixture corpus, as data and pure functions only (runtime-free, wasm-portable, dependency-banned by CI). The SDK and LaserData Cloud both consume it, so the wire is one definition instead of hand mirrors. **laser-sdk** (`sdk/`) is a thin, ergonomic layer over the Iggy client on top. It does not hide Iggy and it never calls an LLM. It moves provenance-tagged messages, reconstructs conversations and causality from the log, and wraps Iggy's consumer with at-least-once + idempotent delivery. The SDK re-exports the wire crate as `laser_sdk::wire` and under its historical module paths (`laser_sdk::query::Query` keeps working).

The prelude is two-tier: `laser_sdk::prelude::*` is the slim set (the accessors plus the everyday types, ~35 items), `laser_sdk::prelude::full::*` adds the long tail (bridge types, seam traits, projection-control shapes, memory knobs). Examples and integration tests use `full`. Application code reads best on slim plus explicit imports. The reliable consumer's public type is `ReliableConsumer` (renamed from `AgentConsumer`, the private per-message adapter is `ReliableWorker`).

Connection vs stream: `Laser::connect(connection_string)` takes only the connection. For a `*.laserdata.cloud`/`*.laserdata.com` host with no `tls_ca_file=` already set, it auto-attaches TLS with LaserData's public root CA, embedded in the SDK itself (`sdk/certs/laserdata.crt` via `include_bytes!`, `resolve_tls`/`is_laserdata_host` in `sdk/src/laser.rs`) so a bare connection string is enough. `LASER_TLS_CERT=<path>` overrides the bundled cert (a rotated CA included), `LASER_NO_TLS=1` disables the check, and every other host passes through untouched (bring your own `tls=true&tls_ca_file=<path>`). `Laser::connect_env()` reads `LASER_CONNECTION_STRING` + optional `LASER_STREAM` (typed `Config` error when unset), `Laser::local()` is the stock local container (`iggy://iggy:iggy@127.0.0.1:8090`), both three-line delegations. A stream is the real Iggy layer grouping topics, reached through the accessors: `laser.stream(name)` is the stream handle (`ensure()`, `topic(name)`), `laser.stream(name).topic(name)` addresses any topic on any stream, and `laser.topic(name)` is the one-word shortcut against the default stream (pinned via `connect_with_stream` / `with_default_stream`, without one it returns the typed `NoStream`). `Topic` carries the `streaming` feature's verbs: `publish()` / `publish_batch()` and `send(payload, headers, key)` / `batch(messages, key)` (`impl Into<Vec<u8>>`, the typed/one-shot publish path), `producer()` / `consumer(name, partition)` / `consumer_group(group)` (the live hot-loop path: `ProducerMessage`/`ConsumerMessage` keep `bytes::Bytes` here specifically for a zero-copy clone under high message-rate throughput, direct batching/linger/retries/topology and balanced/key/partition routing, polling/replay, group lifecycle, retries, automatic commit policies, explicit `commit(&message)`, `next_within(timeout)` for a bounded single-record wait, committed offsets, exact headers), `replay()` (the caller-offset Cursor), `ensure(partitions)`, and the typed handle (`sdk/src/typed.rs`: `.json::<T>()` / `.cbor::<T>()` serde forms, `.schema::<T>(id)` resolves + compiles the registered writer schema once and validates before send stamping `agdx.ct` + `agdx.sid`, `records(reader_name)` the Cursor-backed typed reader yielding `TypedDecodeError { position, source }` for a record that does not decode and moving past it). The raw Apache Iggy escape hatch is `iggy_producer()` / `iggy_consumer()` / `iggy_consumer_group()`, `Laser::client()`, and the exact-version `laser_sdk::iggy` re-export. See `native-streaming`, `event-analytics`, and `order-book`. The default feature set is `streaming` plus `provenance`, and the agent and managed layers opt in. `vsr` implies `streaming` and forwards to `iggy/vsr`, switching standard commands without changing Laser APIs, and managed custom command codes remain unavailable until upstream VSR admits them. Managed features (query/KV/forks) are LaserData Cloud only and return `LaserError::Unsupported` on raw Apache Iggy. `BlobStore::put`/`get` (claim-check, `sdk/src/blob.rs`) and the one-shot managed commands (query/KV/RBAC/batch/runs, `send_raw_with_response`) are also `Vec<u8>`, converting to iggy's own `Bytes` only at the literal client call.

## Which skill to load

- Wire contract types, codes, dictionaries, fixtures, the agent envelope (Agent Data Exchange Protocol), framing -> [wire-contract](../wire-contract/SKILL.md)
- Provenance runtime: `AgentTopic`, `Provenance` encode/decode, OTel/`agdx.*` aliasing, caps -> [provenance](../provenance/SKILL.md)
- `Laser` facade, reliable consumer, `Agent` builder, router, sessions, request/reply, shutdown -> [agent-runtime](../agent-runtime/SKILL.md)
- Reading the log back: `ContextAssembler`/policies, `ConversationState`, `Memory`/`LogMemory` -> [context-and-memory](../context-and-memory/SKILL.md)
- Example crate, the `LlmClient` seam, `TestIggy`, integration-test conventions -> [examples-and-testing](../examples-and-testing/SKILL.md)
- Typed records and publish live under the `streaming` feature and `laser_sdk::stream`. Queryable indexing directives stay on those records because they are written at append time.
- Query DSL and `query()` (LaserData Cloud only: the `AGDX_QUERY` managed command off the log via `send_raw_with_response`, raw Apache Iggy returns `Unsupported`. No topic request/reply query path) -> [query](../query/SKILL.md)
- Managed key-value store (`Laser::kv`, get/set/delete/scan, optional expiry) over the `AGDX_KV_*` managed commands, backed by LaserData Cloud's managed point-state store (`kv` feature, independently selectable from `query`. Client-only, backend is managed-side) -> [kv](../kv/SKILL.md) (wire the AGDX spec)
- The managed run registry (`Laser::runs()` in `sdk/src/runs.rs`, `runs` feature: submit / `submit_budgeted` (a multi-dimensional per-run `RunBudget`: events/model_calls/tool_calls/patches/depth/wall_clock/cost, plane-accumulated in the run fold, `LaserError::BudgetExceeded` on a crossed cap) / status / cancel-as-intent / fluent paged list over the `AGDX_AGENT_*` band, gated on the `agent_workflow` capability. `.registered()` on workflows and contracts stamps the pinned `run` metadata key and the plane folds run state from those status records) -> [agent-runtime](../agent-runtime/SKILL.md)
- Capability RBAC over the managed surfaces (`Laser::whoami` + the role/binding/history verbs in `sdk/src/rbac/`, `rbac` feature: `effect feature:action [on resource]` grants assembled through roles bound to the server-stamped user, deny-wins, default-deny). Fork-native and journalled: the streaming server enforces feature+action+keyed-resource at the edge (`AGDX_AUTHZ_*` band, `authz` capability), orthogonal to Iggy's own `Permissions`. See [wire-contract](../wire-contract/SKILL.md) for the `authz` module.
- A2A JSON-RPC bridge (v1.0: SendMessage + streaming, GetTask + CancelTask, the supportedInterfaces Agent Card with optional JWS signing), the sibling MCP bridge (initialize, tools, resources, prompts), and AG-UI state sync + event rendering (`agui`), all over the AGDX verbs (`a2a-bridge` / `mcp-bridge` / `agui` features) -> [a2a-bridge](../a2a-bridge/SKILL.md)
- The Python SDK under `foreign/python/` (PyO3 bindings over this crate), its maturin packaging, the `.pyi` stubs, the pytest suite, or the Python BDD runner -> [python-bindings](../python-bindings/SKILL.md)

## The spine in one paragraph

Every message carries a `Provenance` (required `conversation_id`, the rest optional) serialized into Iggy user-headers. `conversation_id` is the partition key, so a conversation is totally ordered on its partition. Sub-conversations (`spawn_subconversation`) carry `parent_conversation_id` + a stable `root_conversation_id`, and replies carry `causal_parent` (the source `MessageId`). That is the whole causality spine: read one partition for one conversation. Walk `root_conversation_id` + `causal_parent` for a tree.

## Hard invariants

- **Ordering is per-conversation only.** No cross-conversation/-topic order. Context assembly orders best-effort by Iggy timestamp, deterministic on ties, not strictly chronological across topics (Apache Iggy cannot total-order).
- **At-least-once + idempotent**, never exactly-once. Handlers must tolerate redelivery. Dedup is a best-effort in-memory sliding window.
- **The header set is a wire contract.** Changing keys or the encode/decode breaks messages already on the log. Keys live in `wire/src/headers.rs`, and the encode/decode lives in [provenance](../provenance/SKILL.md).
- **The fixture corpus pins the bytes.** Any change under `wire/src/` that alters encoded bytes fails `wire/tests/wire_fixtures.rs`, and an intentional change regenerates (`just fixtures-regen`) and rides the release process.
- **`ConversationId::derive` is versioned** (FNV-1a + `DERIVE_VERSION`). Changing the algorithm without bumping the version remaps every `PerUser` conversation.

## Module map

See the Structure section of [AGENTS.md](../../../AGENTS.md). Quick pointers: `stream.rs` + `typed.rs` + `cursor.rs` = the streaming primitive and its typed, resumable layers. `provenance/` = causality headers. `agent/` = facade + runtime (incl. the `Deduplicator` seam and `Laser::capabilities`). `context.rs` + `memory.rs` + `agent/state.rs` = read-the-log. `govern.rs` = the `ActionGovernor` effect-boundary policy hook (decide before agent sends / AGDX verbs / memory writes, evidence on the audit topic, see [agent-runtime](../agent-runtime/SKILL.md)). `state_store.rs` = the point-store seam (`get`/`set`/`delete`: `InMemoryStore`/`FileStore`, and `Kv`). `capabilities.rs` = premium negotiation. `poll.rs` = the one shared partition-drain helper. `types/ids.rs` = the id types plus the AGDX id bridge (`MintUlid`, with `AgentId::wire_id` = the name verbatim, since wire agent ids are strings). `query/` = the managed materialized-view client (`Laser::query`, projections, bindings, and schemas). The worker and backends are test-only, see [query](../query/SKILL.md). `wire/` = the contract crate, see [wire-contract](../wire-contract/SKILL.md).

## Shipped vs planned

The one canonical inventory lives in [AGENTS.md](../../../AGENTS.md#what-is-shipped-vs-planned), organized by area (core, RBAC, AGDX wire surface, dead letters, edge bridges, orchestration, planned). Read it there rather than a copy here, kept in one place so it doesn't drift.

## Cite by symbol, not line number

Refer to `Laser::send_agent`, `ReliableConsumer::consume`, `keys::CONVERSATION_ID`, not line numbers. Lines drift, symbols do not.
