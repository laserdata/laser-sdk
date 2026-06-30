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

Connection vs stream: `Laser::connect(connection_string)` takes only the connection (defaults to Iggy over TCP, TLS auto against LaserData Cloud). A stream is the Iggy namespace one layer above topics: name it per call (`publish_on(stream, topic)`, `reader_on`, `ensure_topic_on`) or pin a default (`connect_with_stream` / `with_stream`) for the shorter `publish(topic)` and the agentic helpers. One connection drives any number of streams. Raw Iggy is exposed, not hidden: `iggy_producer` / `iggy_consumer` / `iggy_consumer_group` return the Iggy SDK builders (`IggyConsumer` is a `futures::Stream`), see `event-analytics` example. Managed features (query/KV/forks) are LaserData Cloud only and return `LaserError::Unsupported` on raw Apache Iggy.

## Which skill to load

- Wire contract types, codes, dictionaries, fixtures, the agent envelope (Agent Data Exchange Protocol), framing -> [wire-contract](../wire-contract/SKILL.md)
- Provenance runtime: `AgentTopic`, `Provenance` encode/decode, OTel/`agdx.*` aliasing, caps -> [provenance](../provenance/SKILL.md)
- `Laser` facade, reliable consumer, `Agent` builder, router, sessions, request/reply, shutdown -> [agent-runtime](../agent-runtime/SKILL.md)
- Reading the log back: `ContextAssembler`/policies, `ConversationState`, `Memory`/`LogMemory` -> [context-and-memory](../context-and-memory/SKILL.md)
- Example crate, the `LlmClient` seam, `TestIggy`, integration-test conventions -> [examples-and-testing](../examples-and-testing/SKILL.md)
- Query DSL, `Record`/`publish`, `query()` (LaserData Cloud only: the `AGDX_QUERY` managed command off the log via `send_raw_with_response`, raw Apache Iggy returns `Unsupported`. No topic request/reply query path), the `agdx.idx.*` indexing contract -> [query](../query/SKILL.md)
- Managed key-value store (`Laser::kv`, get/set/delete/scan, optional expiry) over the `AGDX_KV_*` managed commands, backed by LaserData Cloud's managed point-state store (`kv` feature, builds on `query`. Client-only, backend is managed-side) -> [kv](../kv/SKILL.md) (wire the AGDX spec)
- A2A JSON-RPC bridge (message/send + stream, tasks/get + cancel, Agent Card), the sibling MCP bridge (initialize, tools, resources, prompts), and AG-UI state sync + event rendering (`agui`), all over the AGDX verbs (`a2a-bridge` / `mcp-bridge` / `agui` features) -> [a2a-bridge](../a2a-bridge/SKILL.md)
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

See the Structure section of [AGENTS.md](../../../AGENTS.md). Quick pointers: `provenance/` = wire. `agent/` = facade + runtime (incl. the `Deduplicator` seam and `Laser::capabilities`). `context.rs` + `cursor.rs` + `memory.rs` + `agent/state.rs` = read-the-log (`cursor.rs` = `Laser::reader` resumable stream cursor). `state_store.rs` = the point-store seam (`get`/`set`/`delete`: `InMemoryStore`/`FileStore`, and `Kv`). `capabilities.rs` = premium negotiation. `poll.rs` = the one shared partition-drain helper. `types/ids.rs` = the id types plus the AGDX id bridge (`MintUlid`, with `AgentId::wire_id` = the name verbatim, since wire agent ids are strings). `query/` = the query **client** (`Record`, `Laser::publish`/`query`, feature `query`, the wire types re-export from laser-wire). The worker and backends are test-only, see [query](../query/SKILL.md). `wire/` = the contract crate, see [wire-contract](../wire-contract/SKILL.md).

## Shipped vs planned

Shipped (current `0.0.1-rc.7` scope): provenance, causality, reliable consumer, context seam, memory seam, builder/router/session/state, the `respond_on` + `AgentCtx` handler seam, opt-in warm dedup, a semantic `VectorMemory`, the **agentic-memory facade** (`Laser::memory()` with `remember`/`recall`/`improve`/`forget`, content-addressed dedup via `content_id`), the **managed knowledge-graph surface** (`Laser::graph()` traversal, neighbor reads, and node/edge `upsert`, graph projections via `Laser::projections().register_graph()`, content-addressed `GraphNode::entity` / `GraphEdge::relate` ids, with an optional `SourceRef` provenance pointer on each node and edge linking it back to its origin record), the `a2a-bridge`/`mcp-bridge`/`agui` features, and the **query client** (`query` feature: DSL, `Record`, `Laser::publish`/`query`). Spec capabilities are shipped in the contract + client and the capability set is **grouped** (A12: `managed`, `query{available,projections,schemas,consistency}`, `kv{available,cas}`, `graph`, `fork`, never a flat list): the **unified `ResultCode` space** (A7, `laser_wire::result`, `LaserError::code()`), **key-value compare-and-swap** (`KvCas`/`CasExpect`, entry `version`, `.expect_version`/`.expect_absent().commit()`, `kv.cas` capability) plus the **extended key-value ops** (`exists`/`expire`/`patch`/`lease`/`release` and conditional get/delete, AGDX A10/C6), the **served `query.consistency` level** (`Query.consistency`, `QueryError::Stale`), the **knowledge-graph ops** (`graph` capability, A13), and the **`must_understand` marker** on the agent envelope, the managed-side execution landing in the managed backend. The `concierge` example runs on real Claude (`llm-anthropic`) or OpenAI (`llm-openai`). Planned, not present: durable infrastructure-side dedup, a durable `VectorMemory` backed by an external relational store, a managed A2A gateway, and the **production query worker** (operator-managed). The worker + backends currently exist only as test support, see [query](../query/SKILL.md)). The **Agent Data Exchange Protocol wire surface** is shipped in laser-wire (envelope, ids, dictionaries, the validity matrix with closed operation vocabularies, the pinned chunk-stream / state-sync / metadata-key conventions, `BodyRef`, `AgentCard`, `AgentPresence` (the live connection-metadata body carrying an agent's current inbox topic, resolved by `InboxRoute` so fan-out routes to a per-agent inbox, never a shared topic), draft fixtures embedded in the corpus, the AGDX spec), and so are the SDK's typed producer verbs (`Laser::agdx` -> `Agdx` + `AgdxStream`), the `ChunkAssembler` reassembly state machine, the **envelope-aware reliable consumer + read path** (`AgentMessage`/`ContextMessage` carry the decoded envelope), the **A2A bridge** (`A2aBridge`: message/send + stream, tasks/get + cancel, Agent Card, v0.3.0 schema) and **MCP bridge** (`McpBridge`: initialize, tools, resources, prompts, 2025-11-25 schema) on the AGDX verbs, `Laser::reassemble_channel` (log-native stream replay), the `AgentDeadLetter`/`redrive_dead_letter` DLQ path, AG-UI state sync + event rendering (`agui` feature: `publish_state_snapshot`/`publish_state_delta`/`reconstruct_state`/`agui_events`), and the complete AGDX record fixture. Still outstanding: the byte/latency benchmark suite (needs a real Iggy environment) and the niche AG-UI event types with no AGDX source (`MESSAGES_SNAPSHOT`, `ACTIVITY_*`, `RAW`/`CUSTOM`/`META`). Do not document planned API as if shipped.

## Cite by symbol, not line number

Refer to `Laser::send_agent`, `ReliableConsumer::consume`, `keys::CONVERSATION_ID`, not line numbers. Lines drift, symbols do not.
