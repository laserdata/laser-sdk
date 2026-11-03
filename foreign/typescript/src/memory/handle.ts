import { InvalidError, UnsupportedError } from "../client/errors.js"
import type { Laser } from "../client/laser.js"
import { type AgentId, ConversationId } from "../types/ids.js"
import { LogMemory } from "./log-memory.js"
import {
  Lifetime,
  MemoryId,
  MemoryKind,
  RecallStrategy,
  toContextBlock,
  type Embedder,
  type Feedback,
  type ConsolidateOptions,
  type ConsolidationReport,
  type Memory,
  type MemoryItem,
  type MemoryQuery,
  type MemoryScope,
  type Reranker
} from "./types.js"
import { VectorMemory, WITH_EMBEDDER } from "./vector-memory.js"
import type { ProducerInfo, SourceRef } from "../wire/graph.js"

// How many items one consolidation pass recalls and operates over.
const CONSOLIDATION_WINDOW = 10_000

export const MemoryBackend = { Auto: "auto", Log: "log", Vector: "vector" } as const
export type MemoryBackend = (typeof MemoryBackend)[keyof typeof MemoryBackend]

export class MemoryHandle implements Memory {
  private constructor(private readonly store: Memory) {}

  /** @internal */
  static create(store: Memory): MemoryHandle {
    return new MemoryHandle(store)
  }

  /** Which backend this handle resolved to: `log` for the durable stream
   * model, `vector` for the in-process similarity index, `custom` for a
   * caller-supplied backend. A reranked handle reports its inner backend. */
  get backend(): MemoryBackendKind {
    return backendKindOf(this.store)
  }

  /** @internal */
  static log(laser: Laser, namespace: string): MemoryHandle {
    return new MemoryHandle(new LogMemory(laser, namespace))
  }

  /** @internal */
  static logTopic(laser: Laser, topic: string, stream?: string): MemoryHandle {
    return new MemoryHandle(new LogMemory(laser, topic, topic, stream))
  }

  /** A standalone in-process vector handle over `embedder`, with no `Laser`
   * and no governance. */
  static vector(embedder: Embedder): MemoryHandle {
    return new MemoryHandle(new VectorMemory(embedder))
  }

  /** @internal */
  static governedVector(laser: Laser, embedder?: Embedder): MemoryHandle {
    return new MemoryHandle(VectorMemory.governed(laser, embedder))
  }

  /** @internal */
  static custom(memory: Memory): MemoryHandle {
    const append = memory.append?.bind(memory)
    return new MemoryHandle({
      ...(append === undefined ? {} : { append }),
      remember: (scope, payload) => memory.remember(scope, payload),
      recall: (scope, query) => memory.recall(scope, query),
      improve: (scope, feedback) => memory.improve(scope, feedback),
      forget: (scope, id) => memory.forget(scope, id)
    })
  }

  remember(payload: Uint8Array): RememberBuilder
  remember(scope: MemoryScope, payload: Uint8Array): Promise<MemoryId>
  remember(
    scopeOrPayload: MemoryScope | Uint8Array,
    payload?: Uint8Array
  ): RememberBuilder | Promise<MemoryId> {
    if (scopeOrPayload instanceof Uint8Array) return RememberBuilder.create(this, scopeOrPayload)
    return this.store.remember(scopeOrPayload, required(payload))
  }

  /** A recall builder scoped to `conversation`, or to every conversation
   * when it is omitted. */
  recall(conversation?: ConversationId): RecallBuilder
  recall(scope: MemoryScope, query: MemoryQuery): Promise<readonly MemoryItem[]>
  recall(
    scope?: MemoryScope | ConversationId,
    query?: MemoryQuery
  ): RecallBuilder | Promise<readonly MemoryItem[]> {
    if (scope === undefined) return RecallBuilder.create(this)
    if (scope instanceof ConversationId) return RecallBuilder.create(this, scope)
    return this.store.recall(scope, query ?? {})
  }

  /** Recall by folding the memory topic in process instead of reading the
   * managed view. The log backend folds, the vector backend is already in
   * process, and a reranked backend folds its inner backend then reranks. */
  recallFolded(scope: MemoryScope, query: MemoryQuery): Promise<readonly MemoryItem[]> {
    return recallFoldedOn(this.store, scope, query)
  }

  /** Records `feedback` on the item it targets. With a conversation in
   * `scope`, the built-in backends apply it only to an item remembered in that
   * conversation, without one to the item in any conversation. */
  improve(scope: MemoryScope, feedback: Feedback): Promise<MemoryId> {
    return this.store.improve(scope, feedback)
  }

  /** Forgets the item `id`. With a conversation in `scope`, the built-in
   * backends forget it only when it was remembered in that conversation,
   * without one in any conversation. */
  forget(scope: MemoryScope, id: MemoryId): Promise<void> {
    return this.store.forget(scope, id)
  }

  async context(scope: MemoryScope, query: MemoryQuery = {}): Promise<string> {
    return toContextBlock(await this.store.recall(scope, query), query.tokenBudget)
  }

  /** One consolidation pass over `scope`: keeps the newest `maxItems` and
   * forgets the rest. With a `summarizer`, the `message` items are first folded
   * into one durable summary per conversation, stored under `scope` narrowed
   * to that conversation, and `pruneSummarized` forgets the folded items. */
  async consolidate(
    scope: MemoryScope,
    maxItems: number,
    options: ConsolidateOptions = {}
  ): Promise<ConsolidationReport> {
    if (!Number.isSafeInteger(maxItems) || maxItems < 0)
      throw new InvalidError("consolidation maxItems must be a non-negative safe integer")
    const items = await this.store.recall(scope, {
      limit: CONSOLIDATION_WINDOW,
      strategy: RecallStrategy.Recent
    })
    let remaining = items
    let summarized = 0
    let pruned = 0
    if (options.summarizer !== undefined) {
      const sessions = items.filter((item) => item.kind === MemoryKind.Message)
      if (sessions.length > 0) {
        // One summary per conversation, so each stays recallable in the
        // conversation it distills.
        for (const [conversation, turns] of byConversation(sessions)) {
          const summary = await options.summarizer.summarize(
            turns.map((item) => item.payload.slice())
          )
          await appendOn(
            this.store,
            { ...scope, conversation, lifetime: Lifetime.Durable },
            MemoryId.new(),
            MemoryKind.Summary,
            summary
          )
        }
        summarized = sessions.length
        if (options.pruneSummarized === true) {
          for (const item of sessions) {
            try {
              await this.store.forget(scope, item.id)
              pruned += 1
            } catch {
              // Failed forgets stay outside the successful prune count.
            }
          }
          remaining = items.filter((item) => item.kind !== MemoryKind.Message)
        }
      }
    }
    const oldestFirst = [...remaining].reverse()
    for (const item of oldestFirst.slice(0, Math.max(0, oldestFirst.length - maxItems))) {
      try {
        await this.store.forget(scope, item.id)
        pruned += 1
      } catch {
        // Failed forgets stay outside the successful prune count.
      }
    }
    return { summarized, reweighted: 0, pruned, derived: 0 }
  }

  reranker(reranker: Reranker): MemoryHandle {
    return new MemoryHandle(new RerankedMemory(this.store, reranker))
  }

  /** Registers the embedding seam for similarity recall. It configures the
   * in-process vector index, so a vector handle starts a fresh index over
   * `embedder` with the same governance. The durable log model recalls by
   * recency and a custom backend owns its retrieval, so both are returned
   * unchanged. */
  embedder(embedder: Embedder): MemoryHandle {
    const store = withEmbedder(this.store, embedder)
    return store === this.store ? this : new MemoryHandle(store)
  }

  /** @internal */
  logBackend(): LogMemory | undefined {
    return namedMemoryOf(this.store)
  }

  /** Writes named point state under your own `key`. Every write is a durable
   * event on the memory topic. Only the log-backed handle has a key space, so
   * the vector handle refuses with `UnsupportedError`. */
  set(key: string, payload: Uint8Array): Promise<void> {
    return this.namedMemory("set").setNamed(key, payload)
  }

  /** Point-reads the named item written by `set`, from the managed key-value
   * view, or `undefined`. */
  fetch(key: string): Promise<Uint8Array | undefined> {
    return this.namedMemory("fetch").fetchNamed(key)
  }

  /** Point-reads the named item by folding the memory topic in process. */
  fetchFolded(key: string): Promise<Uint8Array | undefined> {
    return this.namedMemory("fetchFolded").fetchNamedFolded(key)
  }

  /** Merge-patches the named item (RFC 7386 over a JSON value): fields in
   * `patch` overwrite, `null` removes. */
  update(key: string, patch: Uint8Array): Promise<void> {
    return this.namedMemory("update").updateNamed(key, patch)
  }

  /** Deletes the named item. Removing an absent key is fine. */
  remove(key: string): Promise<void> {
    return this.namedMemory("remove").forgetNamed(key)
  }

  private namedMemory(verb: string): LogMemory {
    const memory = namedMemoryOf(this.store)
    if (memory === undefined) {
      throw new UnsupportedError(
        `${verb}(key) is the named-item altitude and needs the durable memory handle`,
        { cause: { surface: "memory" } }
      )
    }
    return memory
  }

  append(
    scope: MemoryScope,
    id: MemoryId,
    kind: MemoryKind,
    payload: Uint8Array
  ): Promise<MemoryId> {
    return appendOn(this.store, scope, id, kind, payload)
  }
}

export class RememberBuilder {
  private memoryScope: MemoryScope = {}
  private memoryKind: MemoryKind = MemoryKind.Fact
  private deduplicate = false

  private constructor(
    private readonly handle: MemoryHandle,
    private readonly payload: Uint8Array
  ) {}

  /** @internal */
  static create(handle: MemoryHandle, payload: Uint8Array): RememberBuilder {
    return new RememberBuilder(handle, payload)
  }

  /** Records the session record that motivated this item. */
  origin(origin: SourceRef): this {
    this.memoryScope = { ...this.memoryScope, origin }
    return this
  }

  /** Records the component that produced this item. */
  producer(producer: ProducerInfo): this {
    this.memoryScope = { ...this.memoryScope, producer }
    return this
  }

  /** Scopes the item to a conversation. */
  scope(conversation: ConversationId): this {
    this.memoryScope = { ...this.memoryScope, conversation }
    return this
  }

  user(user: string): this {
    this.memoryScope = { ...this.memoryScope, user }
    return this
  }

  agent(agent: AgentId): this {
    this.memoryScope = { ...this.memoryScope, agent }
    return this
  }

  application(application: string): this {
    this.memoryScope = { ...this.memoryScope, app: application }
    return this
  }

  stream(stream: string): this {
    this.memoryScope = { ...this.memoryScope, stream }
    return this
  }

  durable(): this {
    this.memoryScope = { ...this.memoryScope, lifetime: Lifetime.Durable }
    return this
  }

  kind(kind: MemoryKind): this {
    this.memoryKind = kind
    return this
  }

  dedup(): this {
    this.deduplicate = true
    return this
  }

  send(): Promise<MemoryId> {
    const id = this.deduplicate
      ? MemoryId.content(this.memoryScope, this.memoryKind, this.payload)
      : MemoryId.new()
    return this.handle.append(this.memoryScope, id, this.memoryKind, this.payload)
  }
}

export class RecallBuilder {
  private scope: MemoryScope = {}
  private query: MemoryQuery = {}
  private foldInProcess = false

  private constructor(
    private readonly handle: MemoryHandle,
    conversation?: ConversationId
  ) {
    if (conversation !== undefined) this.scope = { conversation }
  }

  /** @internal */
  static create(handle: MemoryHandle, conversation?: ConversationId): RecallBuilder {
    return new RecallBuilder(handle, conversation)
  }

  user(user: string): this {
    this.scope = { ...this.scope, user }
    return this
  }

  agent(agent: AgentId): this {
    this.scope = { ...this.scope, agent }
    return this
  }

  application(application: string): this {
    this.scope = { ...this.scope, app: application }
    return this
  }

  stream(stream: string): this {
    this.scope = { ...this.scope, stream }
    return this
  }

  recent(): this {
    this.query = { ...this.query, strategy: RecallStrategy.Recent }
    return this
  }

  semantic(text: string): this {
    this.query = { ...this.query, semantic: text, strategy: RecallStrategy.Semantic }
    return this
  }

  keyword(text: string): this {
    this.query = { ...this.query, semantic: text, strategy: RecallStrategy.Keyword }
    return this
  }

  hybrid(text: string): this {
    this.query = { ...this.query, semantic: text, strategy: RecallStrategy.Hybrid }
    return this
  }

  strategy(strategy: RecallStrategy): this {
    this.query = { ...this.query, strategy }
    return this
  }

  limit(limit: number): this {
    this.query = { ...this.query, limit }
    return this
  }

  /** Fold the memory topic in process instead of reading the managed key-value
   * view, for a deployment that materializes no read view. */
  folded(): this {
    this.foldInProcess = true
    return this
  }

  fetch(): Promise<readonly MemoryItem[]> {
    return this.foldInProcess
      ? this.handle.recallFolded(this.scope, this.query)
      : this.handle.recall(this.scope, this.query)
  }

  /** Runs the recall and renders the items as one prompt-ready block under
   * `tokenBudget` estimated tokens when given. */
  async block(tokenBudget?: number): Promise<string> {
    return toContextBlock(await this.fetch(), tokenBudget)
  }
}

// The fold dispatch, mirroring Rust's `recall_folded`: the log backend folds its
// topic, the vector backend is already in process, a reranked backend folds its
// inner backend and reranks that, and a custom backend has no separate fold.
async function recallFoldedOn(
  backend: Memory,
  scope: MemoryScope,
  query: MemoryQuery
): Promise<readonly MemoryItem[]> {
  if (backend instanceof LogMemory) return backend.recallFolded(scope, query)
  if (backend instanceof RerankedMemory) {
    const { inner, reranker } = backend[RERANKED]()
    const items = await recallFoldedOn(inner, scope, query)
    return query.semantic === undefined ? items : reranker.rerank(query.semantic, items)
  }
  return backend.recall(scope, query)
}

function appendOn(
  memory: Memory,
  scope: MemoryScope,
  id: MemoryId,
  kind: MemoryKind,
  payload: Uint8Array
): Promise<MemoryId> {
  if (memory instanceof VectorMemory || memory instanceof LogMemory)
    return memory.append(scope, id, kind, payload)
  if (memory instanceof RerankedMemory) return memory.append(scope, id, kind, payload)
  return memory.append?.(scope, id, kind, payload) ?? memory.remember(scope, payload)
}

// Groups summarized turns by conversation, in the order each conversation first
// appears in the recall, keeping each group's recall order.
function byConversation(
  items: readonly MemoryItem[]
): readonly (readonly [ConversationId, readonly MemoryItem[]])[] {
  const groups = new Map<string, [ConversationId, MemoryItem[]]>()
  for (const item of items) {
    const conversation = item.provenance.conversationId
    const group = groups.get(conversation.toString())
    if (group === undefined) groups.set(conversation.toString(), [conversation, [item]])
    else group[1].push(item)
  }
  return [...groups.values()]
}

function namedMemoryOf(memory: Memory): LogMemory | undefined {
  if (memory instanceof LogMemory) return memory
  return memory instanceof RerankedMemory ? namedMemoryOf(memory[RERANKED]().inner) : undefined
}

export type MemoryBackendKind = "log" | "vector" | "custom"

function backendKindOf(memory: Memory): MemoryBackendKind {
  if (memory instanceof LogMemory) return "log"
  if (memory instanceof VectorMemory) return "vector"
  if (memory instanceof RerankedMemory) return backendKindOf(memory[RERANKED]().inner)
  return "custom"
}

// The wrapped backend and the rerank stage, read by the handle's fold, append,
// and backend dispatch.
const RERANKED = Symbol("laser.internal.reranked")

/** Wraps any {@link Memory} with a {@link Reranker} second stage: `recall` runs
 * the inner backend's retrieval, then reorders the candidates through the
 * reranker when the query carries a `semantic` string. `remember`, `append`,
 * `improve`, and `forget` delegate unchanged. */
export class RerankedMemory implements Memory {
  constructor(
    private readonly inner: Memory,
    private readonly reranker: Reranker
  ) {}

  [RERANKED](): { readonly inner: Memory; readonly reranker: Reranker } {
    return { inner: this.inner, reranker: this.reranker }
  }

  remember(scope: MemoryScope, payload: Uint8Array): Promise<MemoryId> {
    return this.inner.remember(scope, payload)
  }

  append(
    scope: MemoryScope,
    id: MemoryId,
    kind: MemoryKind,
    payload: Uint8Array
  ): Promise<MemoryId> {
    return appendOn(this.inner, scope, id, kind, payload)
  }

  async recall(scope: MemoryScope, query: MemoryQuery): Promise<readonly MemoryItem[]> {
    const items = await this.inner.recall(scope, query)
    return query.semantic === undefined ? items : this.reranker.rerank(query.semantic, items)
  }

  improve(scope: MemoryScope, feedback: Feedback): Promise<MemoryId> {
    return this.inner.improve(scope, feedback)
  }

  forget(scope: MemoryScope, id: MemoryId): Promise<void> {
    return this.inner.forget(scope, id)
  }
}

// The embedder dispatch, mirroring Rust: a vector backend rebuilds over the new
// embedder, a reranked backend reconfigures its inner backend, and the log and
// custom backends ignore it.
function withEmbedder(memory: Memory, embedder: Embedder): Memory {
  if (memory instanceof VectorMemory) return memory[WITH_EMBEDDER](embedder)
  if (memory instanceof RerankedMemory) {
    const { inner, reranker } = memory[RERANKED]()
    const configured = withEmbedder(inner, embedder)
    return configured === inner ? memory : new RerankedMemory(configured, reranker)
  }
  return memory
}

function required(payload: Uint8Array | undefined): Uint8Array {
  if (payload === undefined) throw new TypeError("memory payload is required")
  return payload
}
