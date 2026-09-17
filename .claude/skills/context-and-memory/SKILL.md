---
name: context-and-memory
description: Reading the log back - `sdk/src/context.rs`, `sdk/src/cursor.rs`, `sdk/src/memory.rs`, `sdk/src/state_store.rs`, `sdk/src/agent/state.rs`, `sdk/src/poll.rs`. Use when changing context assembly, a `ContextPolicy` (LastN/RoleFilter), the `Cursor` stream reader, the `StateStore` seam (`InMemoryStore`/`FileStore`/`Kv`), the `Memory` trait, `LogMemory` or the semantic `VectorMemory`/`Embedder` seam, memory scoping (stream/agent/conversation), tombstone/forget, or `ConversationState` replay.
---

# Context and memory

The TypeScript peer lives under `foreign/typescript/src/context*`, `conversation-state.ts`, `snapshot.ts`, and `memory/`.

These modules reconstruct state from the durable log: assemble an ordered window of a conversation, fold it into state, or recall scoped memory items. Load [laser-sdk-overview](../laser-sdk-overview/SKILL.md) first. Repo rules in [AGENTS.md](../../../AGENTS.md).

Iggy provides the VSR transport and AGDX command classifier. Context assembly, cursors, conversation-state replay, topic snapshots, and folded `LogMemory` use standard append and poll commands. KV-backed recall, query, graph, and other managed memory composition use the non-replicated extension path. Callers must keep capability and `Unsupported` handling for an Iggy server that does not serve AGDX.

## STOP and ask the user before

- Changing the cross-topic ordering key in `ContextAssembler::assemble` (`(timestamp, topic_index, partition, offset)`) - it defines what an LLM is fed.
- Changing the `MemoryLogEntry` framing (the typed `Item`/`Forget`/`Feedback` records `LogMemory` writes to the audit topic) - it is the wire format of a memory record on the log.
- Changing how `recall` scopes results (agent filter, multi-tenancy is at Iggy stream boundary, not a memory scope field).

## Key files and symbols

- `poll.rs` - `drain_partition(client, stream, topic, consumer, partition, from_offset, batch) -> PartitionBatch { messages, next_offset }`. The single shared batched-drain used by both context and the reply scanner. Reuse it, do not hand-roll another poll loop.
- `ContextAssembler` uses a bon builder for `conversation_id`, `across_subconversations`, `topics`, and `policy`. It reads topic partitions through the shared bounded reader, filters conversation or ancestry, orders results deterministically, and applies `ContextPolicy`. Policies include `LastN` and `RoleFilter`. Use `AgentTopic::as_identifier()` on read paths.

Context assembly is stateless between calls. It uses `tail_anchored_offset` for a recent bounded window and can accept `from_offsets`. `Cursor` and folded `LogMemory` retain offsets for incremental reads. Do not describe every read as a full history scan.
- `memory.rs` defines `Memory` with `remember`, `recall`, `improve`, and `forget`. `LogMemory` records durable changes, and `VectorMemory` supports local similarity reads. `MemoryKind`, `MemoryClass`, `Lifetime`, `RecallStrategy`, `Feedback`, and `MemoryScope` define SDK behavior. Scope includes user, agent, conversation, app, and stream. `MemoryId::content` uses `laser_wire::hashing::content_id` for matching IDs across clients.

Applications supply `Embedder`, `Reranker`, `Summarizer`, and `Consolidator`. `RerankedMemory` adds a second ranking stage. Shared forms are `SharedEmbedder`, `SharedReranker`, and `SharedConsolidator`. `MaybeEmbedder` reports missing configuration only when an embedding is needed. `RecallSignal` and `MemoryItem.signals` retain strategy, rank, and original score. `fuse_reciprocal_rank` combines hybrid results.

Memory adds no separate wire command group. It combines publication, query, and graph operations. Read [wire-contract](../wire-contract/SKILL.md) and the memory section in `docs/agdx.md`.
  - `LogMemory` writes `MemoryLogEntry::Item`, `Feedback`, and `Forget` records to the memory topic, with `AgentTopic::Audit` as the default. It uses named-field CBOR through `laser_wire::framing`. Default `recall` and `fetch_named` read the managed KV view. Without that view, these calls return `Unsupported`.

`recall_kv` decodes `KvEntry.scope`, filters the requested scope, and orders items by their IDs. `MemoryKind::from_word` and typed ID parsers check the stored metadata. `update_named` folds locally before a merge.

Local folding is explicit through `RecallBuilder::folded()`, `MemoryHandle::recall_folded`, `fetch_folded`, `LogMemory::recall_folded`, or `fetch_named_folded`. It retains a `Projection` and partition offsets under a Tokio mutex. `catch_up` reads only new records through `poll::drain_partition`. Topic access remains controlled by Iggy permissions.
  - `VectorMemory<E: Embedder>` stores local vectors. `remember` embeds a payload. `recall` combines cosine similarity, `keyword_score`, and feedback according to `query.strategy`. `Keyword` needs no embedding, and `Hybrid` combines active signals. Scope filters include conversation, agent, user, and app. Without a signal, it returns recent items.

`Laser::memory_with(.., Vector)` and Python `laser.vector_memory(..)` retain the creating Laser governor. Apply policy before local mutations and present the proposed body to that policy. Preserve content IDs from `.dedup()`. `VectorMemory::new` is standalone, and `VectorMemory::governed` selects explicit low-level governance. Use `RerankedMemory` for another ranking stage. Embed the query before taking the item lock.
  - `MemoryHandle` is the common memory interface. `Laser::memory(ns)` selects log-based memory. `Laser::memory_on_topic(name)` selects an existing topic. `Laser::memory_topic(name).stream(s).partitions(n).ttl(d)` and `.no_expiry().build()` configure topic storage. The default `DEFAULT_MEMORY_TOPIC_TTL` is 30 days, separate from read-view retention.

`MemoryBackend` variants are `Auto`, `Log`, and `Vector`. `memory_with(ns, MemoryBackend::Vector).embedder(..)` selects local vectors, and `.reranker(..)` adds reranking. Builders include `remember(payload).scope(c).kind(..).dedup().send()` and `recall(c).semantic(..)`, `.keyword(..)`, `.hybrid(..)`, `.strategy(..)`, `.limit(n)`, and `.fetch()`. `context(conversation, token_budget)` renders memory for a context. `consolidate(scope, max_items)` runs consolidation.
  - TypeScript keeps the same topology with language-specific names: `laser.memory(namespace)`, `laser.memoryOnTopic(topic, stream?)`, and `laser.memoryTopic(topic).stream(name).partitions(n).ttl(milliseconds)/.noExpiry().build()`. `laser.context(conversation).memory(handle)` binds the exact topic-backed handle. A recalled log-memory item carries its message source when the transport resolves stream and topic IDs.
  - `DefaultConsolidator<M, S>::new(memory, max_items)` keeps the newest items by ULID `MemoryId`. `.with_summarizer(..)` adds summaries of Message-kind records, and `.prune_summarized()` forgets those source turns. `Agent::builder().consolidate_every(d)` with `.consolidator(SharedConsolidator::new(..))` runs consolidation outside the handler loop. The task stops with the agent. Further fact-invalidation behavior remains application policy.
  - `MemoryQuery` supports `token_budget` and `limit`. `to_context_block(items, token_budget)` renders items in rank order and adds `[..., N more omitted ...]` when needed. `estimate_tokens` uses about four bytes per token and does not invoke a tokenizer. The first item always renders, so this estimate is not a strict model-token limit.
  - `GraphHandle` in `sdk/src/graph.rs` requires `graph` and is returned by `Laser::graph(name)`. `upsert` writes content-addressed nodes and edges. `GraphEdge::relate(..).valid(from,to)` adds a valid-time window. `neighbors(node, dir, edge_type, depth)` reads adjacent paths. Traversal builders combine `start_ids`, `start_match`, `start_nearest`, `out`, `incoming`, `both`, `return_edges`, `return_paths`, `.as_of(micros)`, and `fetch`.

The managed engine supports nodes, edges, paths, and triplets with ID, predicate, or nearest-vector starts. `GraphNode::entity`, `GraphEdge::relate`, `NodeId::content`, and `EdgeId::content` use the wire `content_id`. `SourceRef` records origin without changing identity. Nodes retain the first source, and edges retain the latest. `GraphEdge::with_source` sets that metadata.

`SourceRef::Message` can carry conversation identity. `.conversation(id)` restricts graph reads by that metadata. `Laser::projections().register_graph(..)` and `drop_graph(..)` manage graph projections. Unsupported deployments return `Unsupported`. Missing managed grants return `GraphError::Unauthorized`.
- `MemoryHandle::{set, fetch, update, remove}` addresses items by a caller-provided key. `set` publishes `laser_wire::memory::MemoryRecord::Item` under `"<namespace>/<key>"`, and `remove` publishes `Forget`. `update` reads the current value, applies RFC-7386 through `merge_named`, and republishes. Default `fetch` reads the managed KV view. `fetch_folded` reads locally from the retained topic. Named operations do not write directly to KV.
- `ConversationState::load(laser, conversation, topics, bound, init, fold)` in `agent/state.rs` rebuilds state under an explicit `ReplayBound`. Bounds include `FromOffsets(map)`, `FromCheckpoint(checkpoint)`, `At(checkpoint)`, `Last(n)`, and `Full`. `load_with(store, ..)` can seed state from `SnapshotStore`. Keep full-history and bounded-replay behavior distinct.

A `Checkpoint` in `context.rs` records the next offset per topic partition. `ContextScope::checkpoint(topics)` captures it with topic keys from `AgentTopic::topic_string`, including custom topics. `to_checkpoint` excludes the recorded offset and treats absent partitions as empty. `from_checkpoint` resumes at the recorded offset. `ContextMessage.topic` identifies the source topic.
- `cursor.rs` - `Topic::replay()` -> `Cursor`: resumable, offset-addressable read over a topic. `poll` drains every partition from where it stopped (only new messages), `offsets()` exposes the per-partition cursor and `from_offsets(..)` resumes it. Offsets are caller-owned: checkpoint them into any `StateStore` to survive a restart. Yields `Message` (raw payload + headers, no `Provenance` coupling - works on any topic). This is the open primitive the `Agent` runtime (consumer groups) sits above. Reuses `poll::drain_partition`.
- `StateStore` defines `get`, `set`, and `delete` for offsets, duplicate keys, and application state. `Kv` implements the same interface. Local implementations are `InMemoryStore` and `FileStore`. `FileStore` encodes keys as hexadecimal filenames and writes through unique `<file>.<ulid>.tmp` files before rename. This prevents path traversal and partial-file reads during replacement.

## Rules specific to this area

- Ordering across topics is best-effort: deterministic but not strictly chronological for same-microsecond messages on different topics (Apache Iggy has independent offset spaces). State this. Do not pretend it is total order.
- `recall` honors the scope it was stored under. If you add a scope dimension, stamp it in `remember`'s provenance and filter it in `recall` - do not leave a scope field unused.
- `LogMemory::recall_folded` returns the last `query.limit` matching items. It retains a `Projection` and partition offsets and reads only new records. Keep its result equivalent to applying the same records in a full fold. Default `LogMemory::recall` reads the managed KV view. `ContextAssembler` remains stateless between calls.

## Review smells

- A second copy of the partition-drain loop instead of `poll::drain_partition`.
- A `MemoryScope` field written on `remember` but ignored by `recall` (or vice versa) - the original scope/query mismatch bug.
- Ordering code that assumes cross-topic chronological order.
