import { utf8 } from "../client/bytes.js"
import { type CallOptions, abortable, cancellable, throwIfAborted } from "../client/call-options.js"
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

/** Agent memory, from `laser.memory(namespace)` without `await`. Every verb
 * ends with an awaited `send()` or `fetch()`:
 *
 * ```ts
 * const memory = laser.memory("assistant")
 * const city = await memory.remember("Lives in Gdansk").scope(first).user("user-42").send()
 * await memory.supersede(city, "Lives in Lisbon").scope(second).user("user-42").send()
 * const facts = await memory.recall().user("user-42").semantic("Where does the user live?").fetch()
 * console.log(facts.map(memoryItemText)) // ["Lives in Lisbon"]
 * ```
 *
 * `remember` resolves to the new item's id, `supersede` replaces a fact in
 * every conversation, `recall` resolves to `MemoryItem`s, and
 * `forget(scope, id)` drops one. Scope a call to a user, an agent, a conversation, or an
 * application. The backend decides how recall ranks: the durable log backend
 * by the words an item shares with `semantic`, the vector backend by
 * embedding similarity. */
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
      remember: (scope, payload, options) => memory.remember(scope, payload, options),
      recall: (scope, query, options) => memory.recall(scope, query, options),
      improve: (scope, feedback, options) => memory.improve(scope, feedback, options),
      forget: (scope, id, options) => memory.forget(scope, id, options)
    })
  }

  /** Starts a remember of `payload`. Text is stored as its UTF-8 bytes. Chain
   * `scope`, `user`, `agent`, then await `send()` for the new item's id, the
   * id `supersede` and `forget` take. The first write creates the memory
   * topic. A conversation id is a ULID, `ConversationId.new()` makes one. */
  remember(payload: Uint8Array | string): RememberBuilder
  remember(
    scope: MemoryScope,
    payload: Uint8Array | string,
    options?: CallOptions
  ): Promise<MemoryId>
  remember(
    scopeOrPayload: MemoryScope | Uint8Array | string,
    payload?: Uint8Array | string,
    options: CallOptions = {}
  ): RememberBuilder | Promise<MemoryId> {
    if (scopeOrPayload instanceof Uint8Array || typeof scopeOrPayload === "string")
      return RememberBuilder.create(this, payloadBytes(scopeOrPayload))
    const body = payloadBytes(required(payload))
    return cancellable(options.signal, "memory remember", () =>
      this.store.remember(scopeOrPayload, body, options)
    )
  }

  /** Starts a correction: `send` remembers `payload` under the builder's scope,
   * then forgets `oldId` in every conversation, so no recall returns the old
   * fact, and resolves to the new item's id. A failure between the two leaves
   * both facts. `oldId` that is not a memory id (as `remember` resolved to)
   * throws `InvalidError` before anything is written. A conversation id is a
   * ULID, `ConversationId.new()` makes one. */
  supersede(oldId: MemoryId | string, payload: Uint8Array | string): SupersedeBuilder {
    const old = typeof oldId === "string" ? MemoryId.parse(oldId) : oldId
    return SupersedeBuilder.create(this, old, payloadBytes(payload))
  }

  /** A recall builder scoped to `conversation`, or to every conversation
   * when it is omitted. Chain `user`, `semantic`, `limit`, then await
   * `fetch()` for up to `limit` (50) `MemoryItem`s, newest first, without the
   * forgotten and superseded ones. `memoryItemText(item)` reads one as text.
   * On the log backend `semantic(query)` ranks every item by the words it
   * shares with the query, newest first among equals, and drops none. A
   * recall reads the managed view where the deployment serves one, waiting
   * for this connection's own writes, and folds the topic in process
   * elsewhere. A conversation id is a ULID, `ConversationId.new()` makes one. */
  recall(conversation?: ConversationId): RecallBuilder
  recall(
    scope: MemoryScope,
    query: MemoryQuery,
    options?: CallOptions
  ): Promise<readonly MemoryItem[]>
  recall(
    scope?: MemoryScope | ConversationId,
    query?: MemoryQuery,
    options: CallOptions = {}
  ): RecallBuilder | Promise<readonly MemoryItem[]> {
    if (scope === undefined) return RecallBuilder.create(this)
    if (scope instanceof ConversationId) return RecallBuilder.create(this, scope)
    return cancellable(options.signal, "memory recall", async () => {
      if (query?.strategy !== undefined) knownStrategy(query.strategy)
      return this.store.recall(scope, query ?? {}, options)
    })
  }

  /** Recall by folding the memory topic in process instead of reading the
   * managed view. The log backend folds, the vector backend is already in
   * process, and a reranked backend folds its inner backend then reranks. */
  recallFolded(
    scope: MemoryScope,
    query: MemoryQuery,
    options: CallOptions = {}
  ): Promise<readonly MemoryItem[]> {
    return cancellable(options.signal, "memory recall", () =>
      recallFoldedOn(this.store, scope, query, options)
    )
  }

  /** Records `feedback` on the item it targets. With a conversation in
   * `scope`, the built-in backends apply it only to an item remembered in that
   * conversation, without one to the item in any conversation. */
  improve(scope: MemoryScope, feedback: Feedback, options: CallOptions = {}): Promise<MemoryId> {
    return cancellable(options.signal, "memory improve", () =>
      this.store.improve(scope, feedback, options)
    )
  }

  /** Forgets the item `id`. With a conversation in `scope`, the built-in
   * backends forget it only when it was remembered in that conversation,
   * without one in any conversation. Remembering it again later brings it
   * back. To correct a fact use `supersede`, which also stores the new one. */
  forget(scope: MemoryScope, id: MemoryId, options: CallOptions = {}): Promise<void> {
    return cancellable(options.signal, "memory forget", () => this.store.forget(scope, id, options))
  }

  /** Recalls `conversation`'s most relevant items and renders them as one
   * prompt-ready block, trimmed to `tokenBudget` estimated tokens when given. */
  context(
    conversation: ConversationId,
    tokenBudget?: number,
    options: CallOptions = {}
  ): Promise<string> {
    return RecallBuilder.create(this, conversation).block(tokenBudget, options)
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
    const signal = options.signal
    throwIfAborted(signal, "memory consolidate")
    if (!Number.isSafeInteger(maxItems) || maxItems < 0)
      throw new InvalidError("consolidation maxItems must be a non-negative safe integer")
    const items = await abortable(
      this.store.recall(
        scope,
        { limit: CONSOLIDATION_WINDOW, strategy: RecallStrategy.Recent },
        { signal }
      ),
      signal,
      "memory consolidate"
    )
    let remaining = items
    let summarized = 0
    let pruned = 0
    if (options.summarizer !== undefined) {
      const sessions = items.filter((item) => item.kind === MemoryKind.Message)
      if (sessions.length > 0) {
        // One summary per conversation, so each stays recallable in the
        // conversation it distills. Its id covers the turns it distills, so a
        // pass over turns that already have their summary skips the summarizer.
        for (const [conversation, turns] of byConversation(sessions)) {
          const summaryScope = { ...scope, conversation, lifetime: Lifetime.Durable }
          const summaryId = MemoryId.content(summaryScope, MemoryKind.Summary, coveredIds(turns))
          if (items.some((item) => item.id.equals(summaryId))) continue
          const summary = await abortable(
            options.summarizer.summarize(turns.map((item) => item.payload.slice())),
            signal,
            "memory consolidate"
          )
          await abortable(
            appendOn(this.store, summaryScope, summaryId, MemoryKind.Summary, summary, { signal }),
            signal,
            "memory consolidate"
          )
          summarized += turns.length
        }
        if (options.pruneSummarized === true) {
          for (const item of sessions) {
            try {
              await abortable(
                this.store.forget(scope, item.id, { signal }),
                signal,
                "memory consolidate"
              )
              pruned += 1
            } catch {
              throwIfAborted(signal, "memory consolidate")
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
        await abortable(this.store.forget(scope, item.id, { signal }), signal, "memory consolidate")
        pruned += 1
      } catch {
        throwIfAborted(signal, "memory consolidate")
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
   * `embedder` with the same governance. The durable log model ranks by word
   * overlap and feedback and a custom backend owns its retrieval, so both are
   * returned unchanged. */
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
  set(key: string, payload: Uint8Array | string, options: CallOptions = {}): Promise<void> {
    const memory = this.namedMemory("set")
    return cancellable(options.signal, "memory set", () =>
      memory.setNamed(key, payloadBytes(payload), options)
    )
  }

  /** Point-reads the named item written by `set`, from the managed key-value
   * view, or `undefined`. */
  fetch(key: string, options: CallOptions = {}): Promise<Uint8Array | undefined> {
    const memory = this.namedMemory("fetch")
    return cancellable(options.signal, "memory fetch", () => memory.fetchNamed(key, options))
  }

  /** Point-reads the named item by folding the memory topic in process. */
  fetchFolded(key: string, options: CallOptions = {}): Promise<Uint8Array | undefined> {
    const memory = this.namedMemory("fetchFolded")
    return cancellable(options.signal, "memory fetch", () => memory.fetchNamedFolded(key))
  }

  /** Merge-patches the named item (RFC 7386 over a JSON value): fields in
   * `patch` overwrite, `null` removes. */
  update(key: string, patch: Uint8Array | string, options: CallOptions = {}): Promise<void> {
    const memory = this.namedMemory("update")
    return cancellable(options.signal, "memory update", () =>
      memory.updateNamed(key, payloadBytes(patch), options)
    )
  }

  /** Deletes the named item. Removing an absent key is fine. */
  remove(key: string, options: CallOptions = {}): Promise<void> {
    const memory = this.namedMemory("remove")
    return cancellable(options.signal, "memory remove", () => memory.forgetNamed(key, options))
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
    payload: Uint8Array | string,
    options: CallOptions = {}
  ): Promise<MemoryId> {
    return cancellable(options.signal, "memory append", () =>
      appendOn(this.store, scope, id, kind, payloadBytes(payload), options)
    )
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

  send(options: CallOptions = {}): Promise<MemoryId> {
    const id = this.deduplicate
      ? MemoryId.content(this.memoryScope, this.memoryKind, this.payload)
      : MemoryId.new()
    return this.handle.append(this.memoryScope, id, this.memoryKind, this.payload, options)
  }
}

/** Builds a correction: the new fact takes the remember scope setters, and
 * `send` forgets the old fact afterwards. */
export class SupersedeBuilder {
  private readonly replacement: RememberBuilder
  private forgetScope: MemoryScope = {}

  private constructor(
    private readonly handle: MemoryHandle,
    private readonly oldId: MemoryId,
    payload: Uint8Array
  ) {
    this.replacement = RememberBuilder.create(handle, payload)
  }

  /** @internal */
  static create(handle: MemoryHandle, oldId: MemoryId, payload: Uint8Array): SupersedeBuilder {
    return new SupersedeBuilder(handle, oldId, payload)
  }

  /** Records the session record that motivated the new fact. */
  origin(origin: SourceRef): this {
    this.replacement.origin(origin)
    return this
  }

  /** Records the component that produced the new fact. */
  producer(producer: ProducerInfo): this {
    this.replacement.producer(producer)
    return this
  }

  /** Scopes the new fact to a conversation. The old one is forgotten in every conversation. */
  scope(conversation: ConversationId): this {
    this.replacement.scope(conversation)
    return this
  }

  user(user: string): this {
    this.replacement.user(user)
    this.forgetScope = { ...this.forgetScope, user }
    return this
  }

  agent(agent: AgentId): this {
    this.replacement.agent(agent)
    this.forgetScope = { ...this.forgetScope, agent }
    return this
  }

  application(application: string): this {
    this.replacement.application(application)
    this.forgetScope = { ...this.forgetScope, app: application }
    return this
  }

  stream(stream: string): this {
    this.replacement.stream(stream)
    this.forgetScope = { ...this.forgetScope, stream }
    return this
  }

  durable(): this {
    this.replacement.durable()
    return this
  }

  kind(kind: MemoryKind): this {
    this.replacement.kind(kind)
    return this
  }

  dedup(): this {
    this.replacement.dedup()
    return this
  }

  /** Remembers the new fact, then forgets the old one, and resolves the new id. */
  async send(options: CallOptions = {}): Promise<MemoryId> {
    const id = await this.replacement.send(options)
    await this.handle.forget(this.forgetScope, this.oldId, options)
    return id
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
    this.query = { ...this.query, strategy: knownStrategy(strategy) }
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

  fetch(options: CallOptions = {}): Promise<readonly MemoryItem[]> {
    return this.foldInProcess
      ? this.handle.recallFolded(this.scope, this.query, options)
      : this.handle.recall(this.scope, this.query, options)
  }

  /** Runs the recall and renders the items as one prompt-ready block under
   * `tokenBudget` estimated tokens when given. */
  async block(tokenBudget?: number, options: CallOptions = {}): Promise<string> {
    return toContextBlock(await this.fetch(options), tokenBudget)
  }
}

// The fold dispatch, mirroring Rust's `recall_folded`: the log backend folds its
// topic, the vector backend is already in process, a reranked backend folds its
// inner backend and reranks that, and a custom backend has no separate fold.
async function recallFoldedOn(
  backend: Memory,
  scope: MemoryScope,
  query: MemoryQuery,
  options: CallOptions
): Promise<readonly MemoryItem[]> {
  if (backend instanceof LogMemory) return backend.recallFolded(scope, query)
  if (backend instanceof RerankedMemory) {
    const { inner, reranker } = backend[RERANKED]()
    const items = await recallFoldedOn(inner, scope, query, options)
    return query.semantic === undefined ? items : reranker.rerank(query.semantic, items)
  }
  return backend.recall(scope, query, options)
}

function appendOn(
  memory: Memory,
  scope: MemoryScope,
  id: MemoryId,
  kind: MemoryKind,
  payload: Uint8Array,
  options: CallOptions = {}
): Promise<MemoryId> {
  if (memory instanceof VectorMemory) return memory.append(scope, id, kind, payload)
  if (memory instanceof LogMemory || memory instanceof RerankedMemory)
    return memory.append(scope, id, kind, payload, options)
  return (
    memory.append?.(scope, id, kind, payload, options) ?? memory.remember(scope, payload, options)
  )
}

// The turns a summary covers as its content: each id as 16 big-endian bytes,
// in ascending order, the bytes every SDK hashes.
function coveredIds(turns: readonly MemoryItem[]): Uint8Array {
  const ids = turns
    .map((item) => item.id.asU128())
    .sort((left, right) => (left < right ? -1 : left > right ? 1 : 0))
  const bytes = new Uint8Array(ids.length * 16)
  ids.forEach((id, index) => {
    for (let byte = 0; byte < 16; byte += 1) {
      bytes[index * 16 + byte] = Number((id >> BigInt(8 * (15 - byte))) & 0xffn)
    }
  })
  return bytes
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

  remember(scope: MemoryScope, payload: Uint8Array, options: CallOptions = {}): Promise<MemoryId> {
    return this.inner.remember(scope, payload, options)
  }

  append(
    scope: MemoryScope,
    id: MemoryId,
    kind: MemoryKind,
    payload: Uint8Array,
    options: CallOptions = {}
  ): Promise<MemoryId> {
    return appendOn(this.inner, scope, id, kind, payload, options)
  }

  async recall(
    scope: MemoryScope,
    query: MemoryQuery,
    options: CallOptions = {}
  ): Promise<readonly MemoryItem[]> {
    const items = await this.inner.recall(scope, query, options)
    return query.semantic === undefined ? items : this.reranker.rerank(query.semantic, items)
  }

  improve(scope: MemoryScope, feedback: Feedback, options: CallOptions = {}): Promise<MemoryId> {
    return this.inner.improve(scope, feedback, options)
  }

  forget(scope: MemoryScope, id: MemoryId, options: CallOptions = {}): Promise<void> {
    return this.inner.forget(scope, id, options)
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

function required(payload: Uint8Array | string | undefined): Uint8Array | string {
  if (payload === undefined) throw new TypeError("memory payload is required")
  return payload
}

// A strategy word from untyped code, refused with the valid words.
function knownStrategy(strategy: RecallStrategy): RecallStrategy {
  const words: readonly string[] = Object.values(RecallStrategy)
  if (!words.includes(strategy)) {
    throw new InvalidError(`unknown recall strategy '${strategy}': use one of ${words.join(", ")}`)
  }
  return strategy
}

function payloadBytes(payload: Uint8Array | string): Uint8Array {
  return typeof payload === "string" ? utf8(payload) : payload
}
