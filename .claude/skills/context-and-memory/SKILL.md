---
name: context-and-memory
description: Reading the log back - `sdk/src/context.rs`, `sdk/src/cursor.rs`, `sdk/src/memory.rs`, `sdk/src/state_store.rs`, `sdk/src/agent/state.rs`, `sdk/src/poll.rs`. Use when changing context assembly, a `ContextPolicy` (LastN/RoleFilter), the `Cursor` stream reader, the `StateStore` seam (`InMemoryStore`/`FileStore`/`Kv`), the `Memory` trait, `LogMemory` or the semantic `VectorMemory`/`Embedder` seam, memory scoping (stream/agent/conversation), tombstone/forget, or `ConversationState` replay.
---

# Context and memory

These modules reconstruct state from the durable log: assemble an ordered window of a conversation, fold it into state, or recall scoped memory items. Load [laser-sdk-overview](../laser-sdk-overview/SKILL.md) first. Repo rules in [AGENTS.md](../../../AGENTS.md).

## STOP and ask the user before

- Changing the cross-topic ordering key in `ContextAssembler::assemble` (`(timestamp, topic_index, partition, offset)`) - it defines what an LLM is fed.
- Changing the `LogMemory` tombstone framing (`FORGET_PREFIX`) - it is the wire format of a forget record on the audit topic.
- Changing how `recall` scopes results (agent filter, multi-tenancy is at the Iggy stream boundary, not a memory scope field).

## Key files and symbols

- `poll.rs` - `drain_partition(client, stream, topic, consumer, partition, from_offset, batch) -> PartitionBatch { messages, next_offset }`. The single shared batched-drain used by both context and the reply scanner. Reuse it, do not hand-roll another poll loop.
- `context.rs` - `ContextAssembler` (bon builder: `conversation_id`, `across_subconversations`, `topics`, `policy`). `assemble` reads each topic's partitions via `drain_partition`, filters by `matches` (conversation, or root/parent when `across_subconversations`), orders by Iggy timestamp with a deterministic tie-break, then applies the `ContextPolicy`. Policies: `LastN`, `RoleFilter`. Read paths use `AgentTopic::as_identifier()`. NB: `assemble` is **stateless** - it uses a throwaway consumer and reads each partition from offset 0 on every call (`drain_partition(.., from_offset: 0, ..)`), so cost scales with topic size. That is a choice, not a limit: a reader that committed a consumer offset and folded incrementally reads only new messages - which is exactly what `LogMemory` (below) and the public `Cursor` (`cursor.rs`) now do. Keep this in mind before claiming any log-backed recall is inherently O(history).
- `memory.rs` - `Memory` trait (`remember`/`recall`/`forget`), one substrate per backend (durability x recall-type. Not all are Liskov-substitutable - semantic vs recency, and `QueryMemory::forget` is `Unsupported`):
  - `LogMemory` (log-backed on `AgentTopic::Audit`): `remember` stamps `scope.agent` and an idempotency key = the `MemoryId`. `forget` appends a tombstone. **Incremental recall**: holds a folded `Projection` (items + tombstoned ids) + a per-partition cursor behind a `tokio::sync::Mutex`, `catch_up` drains only new audit messages via `poll::drain_partition` (the same primitive the public `Cursor` rides), then `recall` filters by conversation + agent (`query.agent` overrides `scope.agent`), drops tombstoned, takes the last `limit`. No from-0 rescan and no side checkpoint - the log is the truth, so a fresh instance rebuilds the projection once on first recall. The fold (`Projection::absorb`) is a pure, unit-tested fn. Isolation is at the Iggy stream level. Recency only.
  - `VectorMemory<E: Embedder>`: semantic, in-memory. `remember` embeds the payload, `recall` ranks by cosine similarity to `query.semantic` (or falls back to recency). The `Embedder` trait is the model seam (impl lives in app code, like `LlmClient`). A durable vector store backed by an external relational store is a future drop-in. Embed the query *before* taking the items lock (no model call under the lock).
  - `QueryMemory<E: Embedder>` (feature `query`): semantic + durable. `remember` publishes payload+embedding to a topic. `recall` runs a vector/nearest (or recency) query over the materialized index. `forget` is `Unsupported` (the query layer is append-only until projector tombstones land).
  - `KvMemory` (feature `kv`, needs the Iggy server): durable point state, recency only. One KV entry per item keyed `"<conversation>/<id>"`. `recall` prefix-scans the conversation, `forget` is an O(1) delete that reclaims, `with_ttl` expires entries on their own. The edge over `LogMemory` is the **data model, not recall cost** (a stateful offset-committing reader makes log recall incremental too): real `forget` vs a replayed tombstone, and per-key TTL vs coarse log retention. The complete backend (all three ops + expiry). Not semantic (`query.semantic` ignored).
- `agent/state.rs` - `ConversationState::load(laser, conversation, topics, init, fold)` replays the topics and folds them from `init`.
- `cursor.rs` - `Laser::reader(topic)` -> `Cursor`: resumable, offset-addressable read over a topic. `poll` drains every partition from where it stopped (only new messages), `offsets()` exposes the per-partition cursor and `from_offsets(..)` resumes it. Offsets are caller-owned: checkpoint them into any `StateStore` to survive a restart. Yields `Message` (raw payload + headers, no `Provenance` coupling - works on any topic). This is the open primitive the `Agent` runtime (consumer groups) sits above. Reuses `poll::drain_partition`.
- `state_store.rs` - `StateStore` trait (`get`/`set`/`delete`), the one point-store seam for dedup persistence, conversation/cursor checkpoints, or arbitrary agent state. `get`/`set`/`delete` is shared with `Kv`, which **implements `StateStore`** (so the managed KV store is the durable drop-in). Self-contained defaults: `InMemoryStore` (in-mem) and `FileStore` (a local directory). `FileStore` hex-encodes keys into file names (no path traversal) and writes through a unique `<file>.<ulid>.tmp` staging file + `rename` so a crash mid-write does not leave a zero-length file and concurrent writers do not race on the same staging path.

## Rules specific to this area

- Ordering across topics is best-effort: deterministic but not strictly chronological for same-microsecond messages on different topics (Apache Iggy has independent offset spaces). State this. Do not pretend it is total order.
- `recall` honors the scope it was stored under. If you add a scope dimension, stamp it in `remember`'s provenance and filter it in `recall` - do not leave a scope field unused.
- `LogMemory.recall` returns the last `query.limit` items for the conversation. It folds the audit log **incrementally** (a maintained `Projection` + per-partition cursor, draining only new messages), not from offset 0 each call. Preserve those results-equal-to-a-full-fold semantics. The incrementality is an optimization, not a behavior change. `ContextAssembler` (one-shot folds) stays stateless by design.

## Review smells

- A second copy of the partition-drain loop instead of `poll::drain_partition`.
- A `MemoryScope` field written on `remember` but ignored by `recall` (or vice versa) - the original scope/query mismatch bug.
- Ordering code that assumes cross-topic chronological order.
