import { InvalidError, UnsupportedError } from "../client/errors.js"
import type { Laser } from "../client/laser.js"
import type { AgentId, ConversationId } from "../types/ids.js"
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
import { VectorMemory } from "./vector-memory.js"

export const MemoryBackend = { Auto: "auto", Log: "log", Vector: "vector" } as const
export type MemoryBackend = (typeof MemoryBackend)[keyof typeof MemoryBackend]

export class MemoryHandle implements Memory {
  constructor(private readonly store: Memory) {}

  /** Which backend this handle resolved to: `log` for the durable stream
   * model, `vector` for the in-process similarity index, `custom` for a
   * caller-supplied backend. A reranked handle reports its inner backend. */
  get backend(): MemoryBackendKind {
    return backendKindOf(this.store)
  }

  static log(laser: Laser, namespace: string): MemoryHandle {
    return new MemoryHandle(new LogMemory(laser, namespace))
  }

  /** Opens durable memory on a caller-named topic. */
  static logTopic(laser: Laser, topic: string, stream?: string): MemoryHandle {
    return new MemoryHandle(new LogMemory(laser, topic, topic, stream))
  }

  static vector(embedder?: Embedder): MemoryHandle {
    return new MemoryHandle(new VectorMemory(embedder))
  }

  static governedVector(laser: Laser, embedder?: Embedder): MemoryHandle {
    return new MemoryHandle(VectorMemory.governed(laser, embedder))
  }

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
    if (scopeOrPayload instanceof Uint8Array) return new RememberBuilder(this, scopeOrPayload)
    return this.store.remember(scopeOrPayload, required(payload))
  }

  recall(): RecallBuilder
  recall(scope: MemoryScope, query: MemoryQuery): Promise<readonly MemoryItem[]>
  recall(scope?: MemoryScope, query?: MemoryQuery): RecallBuilder | Promise<readonly MemoryItem[]> {
    if (scope === undefined) return new RecallBuilder(this)
    return this.store.recall(scope, query ?? {})
  }

  /** Recall by folding the memory topic in process instead of reading the
   * managed view. The log backend folds, the vector backend is already in
   * process, and a reranked backend folds its inner backend then reranks. */
  recallFolded(scope: MemoryScope, query: MemoryQuery): Promise<readonly MemoryItem[]> {
    return recallFoldedOn(this.store, scope, query)
  }

  improve(scope: MemoryScope, feedback: Feedback): Promise<MemoryId> {
    return this.store.improve(scope, feedback)
  }

  forget(scope: MemoryScope, id: MemoryId): Promise<void> {
    return this.store.forget(scope, id)
  }

  async context(scope: MemoryScope, query: MemoryQuery = {}): Promise<string> {
    return toContextBlock(await this.store.recall(scope, query), query.tokenBudget)
  }

  /** One consolidation pass over `scope`: keeps the newest `maxItems` and
   * forgets the rest. With a `summarizer`, the `message` items are first folded
   * into one durable summary, and `pruneSummarized` forgets the folded items. */
  async consolidate(
    scope: MemoryScope,
    maxItems: number,
    options: ConsolidateOptions = {}
  ): Promise<ConsolidationReport> {
    if (!Number.isSafeInteger(maxItems) || maxItems < 0)
      throw new InvalidError("consolidation maxItems must be a non-negative safe integer")
    const items = await this.store.recall(scope, { limit: 10_000 })
    let remaining = items
    let summarized = 0
    let pruned = 0
    if (options.summarizer !== undefined) {
      const sessions = items.filter((item) => item.kind === MemoryKind.Message)
      if (sessions.length > 0) {
        const summary = await options.summarizer.summarize(
          sessions.map((item) => item.payload.slice())
        )
        await appendOn(
          this.store,
          { ...scope, lifetime: Lifetime.Durable },
          MemoryId.new(),
          MemoryKind.Summary,
          summary
        )
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
    const oldestFirst = [...remaining].sort((left, right) =>
      left.id.asU128() < right.id.asU128() ? -1 : left.id.asU128() > right.id.asU128() ? 1 : 0
    )
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

  logBackend(): LogMemory | undefined {
    return namedMemoryOf(this.store)
  }

  /** Writes named point state under your own `key`. Every write is a durable
   * event on the memory topic. Only the log-backed handle has a key space, so
   * the vector handle refuses with `UnsupportedError`. */
  set(key: string, payload: Uint8Array): Promise<void> {
    return this.namedMemory("set").set(key, payload)
  }

  /** Point-reads the named item written by `set`, from the managed key-value
   * view, or `undefined`. */
  fetch(key: string): Promise<Uint8Array | undefined> {
    return this.namedMemory("fetch").fetch(key)
  }

  /** Point-reads the named item by folding the memory topic in process. */
  fetchFolded(key: string): Promise<Uint8Array | undefined> {
    return this.namedMemory("fetchFolded").fetchFolded(key)
  }

  /** Merge-patches the named item (RFC 7386 over a JSON value): fields in
   * `patch` overwrite, `null` removes. */
  update(key: string, patch: Uint8Array): Promise<void> {
    return this.namedMemory("update").update(key, patch)
  }

  /** Deletes the named item. Removing an absent key is fine. */
  remove(key: string): Promise<void> {
    return this.namedMemory("remove").remove(key)
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
  private scope: MemoryScope = {}
  private memoryKind: MemoryKind = MemoryKind.Fact
  private deduplicate = false

  constructor(
    private readonly handle: MemoryHandle,
    private readonly payload: Uint8Array
  ) {}

  conversation(conversation: ConversationId): this {
    this.scope = { ...this.scope, conversation }
    return this
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
    this.scope = { ...this.scope, application }
    return this
  }

  stream(stream: string): this {
    this.scope = { ...this.scope, stream }
    return this
  }

  durable(): this {
    this.scope = { ...this.scope, lifetime: Lifetime.Durable }
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
      ? MemoryId.content(this.scope, this.memoryKind, this.payload)
      : MemoryId.new()
    return this.handle.append(this.scope, id, this.memoryKind, this.payload)
  }
}

export class RecallBuilder {
  private scope: MemoryScope = {}
  private query: MemoryQuery = {}
  private foldInProcess = false

  constructor(private readonly handle: MemoryHandle) {}

  conversation(conversation: ConversationId): this {
    this.scope = { ...this.scope, conversation }
    return this
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
    this.scope = { ...this.scope, application }
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

  tokenBudget(tokenBudget: number): this {
    this.query = { ...this.query, tokenBudget }
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

  async block(): Promise<string> {
    return toContextBlock(await this.fetch(), this.query.tokenBudget)
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
  if (backend instanceof RerankedMemory) return backend.recallFolded(scope, query)
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
  if (memory instanceof RerankedMemory) return appendOn(memory.inner, scope, id, kind, payload)
  return memory.append?.(scope, id, kind, payload) ?? memory.remember(scope, payload)
}

function namedMemoryOf(memory: Memory): LogMemory | undefined {
  if (memory instanceof LogMemory) return memory
  return memory instanceof RerankedMemory ? namedMemoryOf(memory.inner) : undefined
}

export type MemoryBackendKind = "log" | "vector" | "custom"

function backendKindOf(memory: Memory): MemoryBackendKind {
  if (memory instanceof LogMemory) return "log"
  if (memory instanceof VectorMemory) return "vector"
  if (memory instanceof RerankedMemory) return backendKindOf(memory.inner)
  return "custom"
}

class RerankedMemory implements Memory {
  constructor(
    readonly inner: Memory,
    private readonly reranker: Reranker
  ) {}

  async recallFolded(scope: MemoryScope, query: MemoryQuery): Promise<readonly MemoryItem[]> {
    const items = await recallFoldedOn(this.inner, scope, query)
    return query.semantic === undefined ? items : this.reranker.rerank(query.semantic, items)
  }

  remember(scope: MemoryScope, payload: Uint8Array): Promise<MemoryId> {
    return this.inner.remember(scope, payload)
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

function required(payload: Uint8Array | undefined): Uint8Array {
  if (payload === undefined) throw new TypeError("memory payload is required")
  return payload
}
