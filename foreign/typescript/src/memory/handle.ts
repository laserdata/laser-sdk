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
  constructor(private readonly backend: Memory) {}

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
    return new MemoryHandle(memory)
  }

  remember(payload: Uint8Array): RememberBuilder
  remember(scope: MemoryScope, payload: Uint8Array): Promise<MemoryId>
  remember(
    scopeOrPayload: MemoryScope | Uint8Array,
    payload?: Uint8Array
  ): RememberBuilder | Promise<MemoryId> {
    if (scopeOrPayload instanceof Uint8Array) return new RememberBuilder(this, scopeOrPayload)
    return this.backend.remember(scopeOrPayload, required(payload))
  }

  recall(): RecallBuilder
  recall(scope: MemoryScope, query: MemoryQuery): Promise<readonly MemoryItem[]>
  recall(scope?: MemoryScope, query?: MemoryQuery): RecallBuilder | Promise<readonly MemoryItem[]> {
    if (scope === undefined) return new RecallBuilder(this)
    return this.backend.recall(scope, query ?? {})
  }

  improve(scope: MemoryScope, feedback: Feedback): Promise<MemoryId> {
    return this.backend.improve(scope, feedback)
  }

  forget(scope: MemoryScope, id: MemoryId): Promise<void> {
    return this.backend.forget(scope, id)
  }

  async context(scope: MemoryScope, query: MemoryQuery = {}): Promise<string> {
    return toContextBlock(await this.backend.recall(scope, query), query.tokenBudget)
  }

  async consolidate(scope: MemoryScope, maxItems: number): Promise<ConsolidationReport> {
    const items = await this.backend.recall(scope, {
      limit: 10_000,
      strategy: RecallStrategy.Recent
    })
    const stale = items.slice(Math.max(0, maxItems))
    for (const item of stale) await this.backend.forget(scope, item.id)
    return { scanned: items.length, kept: items.length - stale.length, forgotten: stale.length }
  }

  reranker(reranker: Reranker): MemoryHandle {
    return new MemoryHandle(new RerankedMemory(this.backend, reranker))
  }

  logBackend(): LogMemory | undefined {
    return this.backend instanceof LogMemory ? this.backend : undefined
  }

  append(
    scope: MemoryScope,
    id: MemoryId,
    kind: MemoryKind,
    payload: Uint8Array
  ): Promise<MemoryId> {
    if (this.backend instanceof VectorMemory || this.backend instanceof LogMemory) {
      return this.backend.append(scope, id, kind, payload)
    }
    return this.backend.remember(scope, payload)
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

  fetch(): Promise<readonly MemoryItem[]> {
    return this.handle.recall(this.scope, this.query)
  }

  async block(): Promise<string> {
    return toContextBlock(await this.fetch(), this.query.tokenBudget)
  }
}

class RerankedMemory implements Memory {
  constructor(
    private readonly inner: Memory,
    private readonly reranker: Reranker
  ) {}

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
