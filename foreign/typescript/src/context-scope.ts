import type { Laser } from "./client/laser.js"
import type { GraphHandle } from "./managed/graph.js"
import {
  type Checkpoint,
  Chain,
  ContextAssembler,
  contextCheckpoint,
  LastN,
  type ContextMessage,
  type ContextPolicy,
  TokenBudget
} from "./context.js"
import { ConversationState, type ReplayBound } from "./conversation-state.js"
import type { SnapshotStore } from "./snapshot.js"
import type { BytesLike } from "./client/bytes.js"
import type { ConversationId } from "./types/ids.js"
import type { ProducerInfo, SourceRef } from "./wire/graph.js"
import type { MemoryBackend, MemoryHandle } from "./memory/handle.js"
import type { Embedder } from "./memory/types.js"
import type {
  ConsolidateOptions,
  ConsolidationReport,
  Feedback,
  MemoryId,
  MemoryItem
} from "./memory/types.js"

const DEFAULT_SEARCH_LIMIT = 50

export class ContextScope {
  /** `laser` is the client this scope reads and writes through. */
  private constructor(
    readonly laser: Laser,
    readonly conversation: ConversationId
  ) {}

  /** @internal */
  static create(laser: Laser, conversation: ConversationId): ContextScope {
    return new ContextScope(laser, conversation)
  }

  append(topic: string, payload: BytesLike): Promise<void> {
    return this.laser.sendAgent(topic, payload, { conversationId: this.conversation })
  }

  /** The newest `count` messages of this conversation on `topics`, then
   * trimmed to `tokenBudget` estimated tokens when given, like Rust
   * `Chain(LastN, TokenBudget)`. */
  fetch(
    topics: readonly string[],
    count: number,
    tokenBudget?: number
  ): Promise<readonly ContextMessage[]> {
    return this.fetchWith(
      topics,
      tokenBudget === undefined
        ? new LastN(count)
        : new Chain([new LastN(count), new TokenBudget(tokenBudget)])
    )
  }

  fetchWith(topics: readonly string[], policy: ContextPolicy): Promise<readonly ContextMessage[]> {
    return ContextAssembler.builder()
      .conversationId(this.conversation)
      .topics(topics)
      .policy(policy)
      .build()
      .assemble(this.laser)
  }

  /** `fetch` rendered as one newline-joined text block. */
  async block(topics: readonly string[], count: number, tokenBudget?: number): Promise<string> {
    const messages = await this.fetch(topics, count, tokenBudget)
    return messages.map((message) => new TextDecoder().decode(message.payload)).join("\n")
  }

  /** The current tail of `topics` as a `Checkpoint`: fold up to it with the
   * `at` replay bound or resume after it with `from-checkpoint`. */
  checkpoint(topics: readonly string[]): Promise<Checkpoint> {
    return contextCheckpoint(this.laser, topics)
  }

  /** Binds a memory namespace or existing handle to this conversation. */
  memory(namespaceOrHandle: string | MemoryHandle): ScopedMemory {
    const handle =
      typeof namespaceOrHandle === "string"
        ? this.laser.memory(namespaceOrHandle)
        : namespaceOrHandle
    return ScopedMemory.create(handle, this.conversation)
  }

  /** `memory` on an explicit backend, like Rust `memory_with`. The vector
   * backend needs `embedder`, and any other backend refuses one. */
  memoryWith(namespace: string, backend: MemoryBackend, embedder?: Embedder): ScopedMemory {
    return ScopedMemory.create(
      this.laser.memoryWith(namespace, backend, embedder),
      this.conversation
    )
  }

  /** The knowledge graph `name`, reached from this scope so one conversation's
   * messages, memory, and the relationships between them share an entry point.
   * Returned unnarrowed on purpose: a dependency edge holds no matter which
   * conversation asserted it, so scoping the graph to one would hide the
   * relationships the caller came for. Narrow explicitly with the graph's own
   * `conversation(id)` lens for only what this conversation wrote. */
  graph(name: string): GraphHandle {
    return this.laser.graph(name)
  }

  state<State>(
    topics: readonly string[],
    bound: ReplayBound,
    initial: State,
    fold: (state: State, message: ContextMessage) => State
  ): Promise<State> {
    return ConversationState.load(this.laser, this.conversation, topics, bound, initial, fold)
  }

  /** Like `state` but seeded through a `SnapshotStore`: the newest snapshot's
   * state, JSON-decoded unless `decodeState` is given, plus a replay of only
   * the tail past it. */
  stateWith<State>(
    store: SnapshotStore,
    topics: readonly string[],
    init: State,
    fold: (state: State, message: ContextMessage) => State,
    decodeState?: (bytes: Uint8Array) => State
  ): Promise<State> {
    return ConversationState.loadWith(
      this.laser,
      store,
      this.conversation,
      topics,
      init,
      fold,
      decodeState
    )
  }
}

export class ScopedMemory {
  /** `handle` is the underlying memory, for the cross-conversation verbs this
   * scoped face does not narrow. */
  private constructor(
    readonly handle: MemoryHandle,
    readonly conversation: ConversationId,
    private readonly originValue?: SourceRef,
    private readonly producerValue?: ProducerInfo
  ) {}

  /** @internal */
  static create(handle: MemoryHandle, conversation: ConversationId): ScopedMemory {
    return new ScopedMemory(handle, conversation)
  }

  remember(payload: Uint8Array) {
    const builder = this.handle.remember(payload).scope(this.conversation)
    if (this.originValue !== undefined) builder.origin(this.originValue)
    if (this.producerValue !== undefined) builder.producer(this.producerValue)
    return builder
  }

  /** The origin stamped on every remembered item, if any. */
  origin(): SourceRef | undefined {
    return this.originValue
  }

  /** The producer stamped on every remembered item, if any. */
  producer(): ProducerInfo | undefined {
    return this.producerValue
  }

  /** This scope with `origin` and `producer` stamped on every remembered item. */
  withLineage(origin?: SourceRef, producer?: ProducerInfo): ScopedMemory {
    return new ScopedMemory(this.handle, this.conversation, origin, producer)
  }

  recall() {
    return this.handle.recall(this.conversation)
  }

  /** Keyword recall for `query` within this conversation, up to `limit`
   * items (default 50). Needs no embedder, so it works on the default
   * log-backed memory. `folded` folds the memory topic in process instead of
   * reading the managed view. */
  search(
    query: string,
    options: { readonly limit?: number; readonly folded?: boolean } = {}
  ): Promise<readonly MemoryItem[]> {
    const recall = this.recall()
      .keyword(query)
      .limit(options.limit ?? DEFAULT_SEARCH_LIMIT)
    return (options.folded === true ? recall.folded() : recall).fetch()
  }

  /** This conversation's recalled items as one prompt-ready block, trimmed to
   * `tokenBudget` estimated tokens when given. */
  block(tokenBudget?: number): Promise<string> {
    return this.handle.context(
      { conversation: this.conversation },
      tokenBudget === undefined ? {} : { tokenBudget }
    )
  }

  /** One consolidation pass over this conversation, keeping the newest
   * `maxItems`. `options` adds the summarize pass. */
  consolidate(maxItems: number, options?: ConsolidateOptions): Promise<ConsolidationReport> {
    return this.handle.consolidate({ conversation: this.conversation }, maxItems, options)
  }

  /** Forgets the item `id` when it was remembered in this conversation. The
   * tombstone carries the conversation, and both the log fold and the managed
   * view leave an item of another conversation untouched. */
  forget(id: MemoryId): Promise<void> {
    return this.handle.forget({ conversation: this.conversation }, id)
  }

  /** Records `feedback` on the item it targets when that item was remembered
   * in this conversation, scoped like `forget`. */
  improve(feedback: Feedback): Promise<MemoryId> {
    return this.handle.improve({ conversation: this.conversation }, feedback)
  }
}
