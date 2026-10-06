import type { Laser } from "./client/laser.js"
import type { GraphHandle } from "./managed/graph.js"
import {
  type Checkpoint,
  ContextAssembler,
  contextCheckpoint,
  LastN,
  type ContextMessage,
  type ContextPolicy
} from "./context.js"
import { ConversationState, type ReplayBound } from "./conversation-state.js"
import type { SnapshotStore } from "./snapshot.js"
import type { BytesLike } from "./client/bytes.js"
import type { ConversationId } from "./types/ids.js"
import type { MemoryHandle } from "./memory/handle.js"
import type {
  ConsolidateOptions,
  ConsolidationReport,
  Feedback,
  MemoryId,
  MemoryItem
} from "./memory/types.js"

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

  fetch(topics: readonly string[], count: number): Promise<readonly ContextMessage[]> {
    return this.fetchWith(topics, new LastN(count))
  }

  fetchWith(topics: readonly string[], policy: ContextPolicy): Promise<readonly ContextMessage[]> {
    return ContextAssembler.builder()
      .conversationId(this.conversation)
      .topics(topics)
      .policy(policy)
      .build()
      .assemble(this.laser)
  }

  async block(topics: readonly string[], count: number): Promise<string> {
    const messages = await this.fetch(topics, count)
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
    readonly conversation: ConversationId
  ) {}

  /** @internal */
  static create(handle: MemoryHandle, conversation: ConversationId): ScopedMemory {
    return new ScopedMemory(handle, conversation)
  }

  remember(payload: Uint8Array) {
    return this.handle.remember(payload).scope(this.conversation)
  }

  recall() {
    return this.handle.recall(this.conversation)
  }

  /** Keyword recall for `query` within this conversation. Needs no embedder,
   * so it works on the default log-backed memory. */
  search(query: string, limit?: number): Promise<readonly MemoryItem[]> {
    const recall = this.recall().keyword(query)
    return (limit === undefined ? recall : recall.limit(limit)).fetch()
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

  /** Forgets the item `id`. The item is addressed by id alone, so an item
   * remembered in another conversation is forgotten too. The conversation
   * only stamps the tombstone's provenance. */
  forget(id: MemoryId): Promise<void> {
    return this.handle.forget({ conversation: this.conversation }, id)
  }

  /** Records `feedback` on the item it targets, addressed by id alone like
   * `forget`. The conversation only stamps the feedback record's provenance. */
  improve(feedback: Feedback): Promise<MemoryId> {
    return this.handle.improve({ conversation: this.conversation }, feedback)
  }
}
