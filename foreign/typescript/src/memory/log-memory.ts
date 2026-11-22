import { type CallOptions, delay } from "../client/call-options.js"
import {
  CodecError,
  InvalidError,
  NoStreamError,
  ProtocolError,
  TimeoutError
} from "../client/errors.js"
import { INTERNAL_GOVERN, INTERNAL_MEMORY_WRITES, INTERNAL_TRANSPORT } from "../client/internals.js"
import type { Laser } from "../client/laser.js"
import { ActionKind } from "../govern.js"
import type { HeaderValue } from "../stream/header-value.js"
import { AgentTopic } from "../provenance/agent-topic.js"
import {
  decodeProvenanceHeaders,
  encodeProvenanceHeaders,
  type Provenance
} from "../provenance/provenance.js"
import { Mutex } from "../runtime/mutex.js"
import { Cursor } from "../stream/cursor.js"
import { AgentId, ConversationId } from "../types/ids.js"
import { MEMORY_APP, MEMORY_NAMESPACE, MEMORY_USER } from "../wire/headers.js"
import { decodeOne } from "../wire/cbor.js"
import { decodeMemoryRecord, encodeMemoryRecordFrame, type MemoryRecord } from "../wire/memory.js"
import type { ProducerInfo, SourceRef } from "../wire/graph.js"
import type { KvEntry } from "../wire/kv.js"
import {
  MemoryId,
  MemoryKind,
  RecallStrategy,
  type Feedback,
  type Memory,
  type MemoryItem,
  type MemoryQuery,
  type MemoryScope
} from "./types.js"
import { keywordScore, tokenize } from "./vector-memory.js"
import type { MemoryWrite } from "./writes.js"

/** How long a memory topic keeps records, the expiry of a topic a memory
 * write or `memoryTopic` creates: 30 days. */
export const DEFAULT_MEMORY_TOPIC_TTL_MS = 30 * 24 * 60 * 60 * 1_000

const READ_BATCH = 1000
// The settings of a memory topic a write creates because it was missing.
const CREATED_TOPIC = {
  messageExpiryMicros: BigInt(DEFAULT_MEMORY_TOPIC_TTL_MS) * 1_000n
} as const
const OWN_WRITES_FIRST_BACKOFF_MS = 50
const OWN_WRITES_MAX_BACKOFF_MS = 500
const OWN_WRITES_TIMEOUT =
  "timed out waiting for the memory view to apply this connection's writes: a folded recall reads the topic directly"

// The ids one view walk held, each with the conversations it held it in.
type Held = ReadonlyMap<string, ReadonlySet<string>>

interface FoldedItem {
  readonly item: MemoryItem
  readonly scope: MemoryScope
  readonly timestampMicros: bigint
}

// Where one memory record lands, and the identity the governor sees for it.
interface RecordTarget {
  readonly stream: string
  readonly conversation: ConversationId
  readonly agent?: AgentId
  readonly idempotencyKey: string
}

export class LogMemory implements Memory {
  private readonly items = new Map<string, FoldedItem>()
  // Tombstones and feedback keyed by target and the conversation they are
  // limited to, with an empty conversation for any. A tombstone keeps the
  // latest forget's append time, so it drops older writes from any partition
  // and keeps later ones. Feedback stays apart from the items so a weight
  // folded before its item (a cross-partition reorder) still applies.
  private readonly forgotten = new Map<string, bigint>()
  private readonly feedback = new Map<string, number>()
  private readonly named = new Map<string, Uint8Array>()
  private readonly offsets = new Map<number, bigint>()
  private readonly fold = new Mutex()

  /** @internal */
  readonly namespace: string
  /** @internal */
  readonly topic: string
  /** @internal */
  readonly stream: string | undefined

  /** A log-backed memory on `topic` (the memory topic by default). The named
   * items key on `namespace`, which defaults to the topic's name. */
  constructor(
    private readonly laser: Laser,
    namespace?: string,
    topic: string = AgentTopic.Memory,
    stream: string | undefined = laser.defaultStream
  ) {
    this.topic = topic
    this.stream = stream
    this.namespace = namespace ?? topic
  }

  async remember(
    scope: MemoryScope,
    payload: Uint8Array,
    options: CallOptions = {}
  ): Promise<MemoryId> {
    return this.append(scope, MemoryId.new(), MemoryKind.Fact, payload, options)
  }

  async append(
    scope: MemoryScope,
    id: MemoryId,
    kind: MemoryKind,
    payload: Uint8Array,
    options: CallOptions = {}
  ): Promise<MemoryId> {
    this.checkStream(scope)
    const target = this.target(scope, id.toString())
    // The governor sees the item body, not the record frame around it.
    const body = await this.govern(target, payload.slice())
    await this.send(
      target,
      scope,
      encodeMemoryRecordFrame({
        kind: "item",
        id: id.toString(),
        memoryKind: kind,
        body,
        ...lineage(scope)
      }),
      options
    )
    this.laser[INTERNAL_MEMORY_WRITES]().record({
      view: this.viewKey(),
      id: id.toString(),
      forgotten: false,
      conversation: target.conversation.toString(),
      ...writeLenses(scope)
    })
    return id
  }

  /** Reads the managed key-value view the deployment materializes from the
   * memory topic, so a large topic never folds in process. Folding is the
   * opt-in {@link LogMemory.recallFolded}, never the default, matching Rust and
   * Python. The view applies writes in the background, so a recall waits until
   * it reflects every remember this connection made inside the recall's lenses
   * and every forget it made, scanning again after 50 ms doubling to 500 ms,
   * and throws a retryable `TimeoutError` after 10 seconds. A deployment that serves no
   * key-value view folds the topic instead, like {@link LogMemory.recallFolded}. */
  async recall(
    scope: MemoryScope,
    query: MemoryQuery,
    options: CallOptions = {}
  ): Promise<readonly MemoryItem[]> {
    const limit = recallLimit(query)
    if (limit === 0 || (scope.stream !== undefined && scope.stream !== this.stream)) return []
    if (!(await this.viewServed())) return this.recallFolded(scope, query)
    const agent = query.agent ?? scope.agent
    const writes = this.laser[INTERNAL_MEMORY_WRITES]()
    const lenses = {
      ...(scope.conversation !== undefined ? { conversation: scope.conversation.toString() } : {}),
      ...(scope.user !== undefined ? { user: scope.user } : {}),
      ...(agent !== undefined ? { agent: agent.asStr() } : {}),
      ...(scope.app !== undefined ? { app: scope.app } : {})
    }
    const deadline = performance.now() + writes.waitMs
    let backoffMs = OWN_WRITES_FIRST_BACKOFF_MS
    for (;;) {
      const { items, held } = await this.scanView(scope, query, limit, agent, options)
      const waited = writes.within(this.viewKey(), lenses)
      const pending = waited.filter((write) => !reflected(write, held))
      writes.settle(waited.filter((write) => reflected(write, held)))
      if (pending.length === 0) return items
      const remainingMs = deadline - performance.now()
      if (remainingMs <= 0) {
        writes.settle(pending)
        throw new TimeoutError(OWN_WRITES_TIMEOUT)
      }
      await delay(Math.min(backoffMs, remainingMs), options.signal, "memory recall")
      backoffMs = Math.min(backoffMs * 2, OWN_WRITES_MAX_BACKOFF_MS)
    }
  }

  // One walk of the view: the ranked items, and every id it holds inside the
  // scope's lenses with the conversations it holds it in.
  private async scanView(
    scope: MemoryScope,
    query: MemoryQuery,
    limit: number,
    agent: AgentId | undefined,
    options: CallOptions
  ): Promise<{ readonly items: readonly MemoryItem[]; readonly held: Held }> {
    // Feedback ranks every strategy except recent, a ranking strategy adds the
    // query's word overlap, then arrival breaks ties.
    const weighted = query.strategy !== RecallStrategy.Recent
    const tokens = rankingTokens(query)
    const selected = new Map<string, Selected>()
    const held = new Map<string, Set<string>>()
    let requested = 1
    for (;;) {
      // The server narrows by the scope's lenses, and the rows are checked again
      // here, so a server without them returns the same items.
      const scan = this.laser
        .kv(this.resourceNamespace())
        .scan()
        .memoryLens(scope.user, agent?.asStr(), scope.app)
        .page(requested)
      if (scope.conversation !== undefined) {
        scan.conversation(scope.conversation.toString()).lensStream(this.resourceStream())
      }
      const page = await scan.fetch(options)
      if (page.page !== requested)
        throw new ProtocolError(
          `memory scan asked for page ${String(requested)} and was answered page ${String(page.page)}`
        )
      for (const entry of page.items) {
        const stored = entry.scope
        if (
          stored === undefined ||
          (scope.user !== undefined && stored.user !== scope.user) ||
          (scope.app !== undefined && stored.app !== scope.app) ||
          (agent !== undefined && stored.agent !== agent.asStr())
        )
          continue
        const item = itemFromEntry(scope.conversation, entry)
        if (
          item === undefined ||
          (scope.conversation !== undefined &&
            !item.provenance.conversationId.equals(scope.conversation))
        )
          continue
        const id = item.id.toString()
        const conversations = held.get(id) ?? new Set<string>()
        conversations.add(item.provenance.conversationId.toString())
        held.set(id, conversations)
        if (!matchesQueryText(query, item.payload)) continue
        const weight = weighted ? stored.weight : undefined
        const overlap = tokens === undefined ? undefined : keywordScore(tokens, item.payload)
        selected.set(id, {
          arrival: arrivalKey(entry, item.id),
          item,
          weight,
          overlap,
          score: (overlap ?? 0) + (weight ?? 0)
        })
        if (selected.size > limit) {
          const lowest = [...selected].sort(([, left], [, right]) => compareRank(left, right))[0]
          if (lowest !== undefined) selected.delete(lowest[0])
        }
      }
      if (page.items.length === 0 || page.page >= page.totalPages) break
      requested += 1
    }
    const items = [...selected.values()]
      .sort((left, right) => compareRank(right, left))
      .map(({ item, overlap, weight }, rank) => rankedItem(item, rank, overlap, weight))
    return { items, held }
  }

  /** Recall by folding the memory topic in process, the opt-in path for a small
   * deployment with no materialized read view. Items come newest first, ranked
   * like the view recall: feedback reorders every strategy except `Recent`
   * with a `RecallStrategy.Auto` signal, and a semantic, hybrid, or auto query
   * with text adds its word overlap with a `RecallStrategy.Keyword` signal. */
  async recallFolded(scope: MemoryScope, query: MemoryQuery): Promise<readonly MemoryItem[]> {
    const limit = recallLimit(query)
    if (limit === 0 || (scope.stream !== undefined && scope.stream !== this.stream)) return []
    await this.catchUp()
    const agent = query.agent ?? scope.agent
    const newestFirst = [...this.items.values()]
      .filter((entry) => matchesScope(entry.scope, scope, agent))
      .map((entry) => entry.item)
      .filter((item) => matchesQueryText(query, item.payload))
      .reverse()
    const tokens = rankingTokens(query)
    if (
      query.strategy === RecallStrategy.Recent ||
      (tokens === undefined && this.feedback.size === 0)
    )
      return newestFirst.slice(0, limit).map(copyItem)
    // A stable sort keeps newest first among equal scores.
    return newestFirst
      .map((item) => {
        const overlap = tokens === undefined ? undefined : keywordScore(tokens, item.payload)
        const weight = this.feedbackFor(item)
        return { item, overlap, weight, score: (overlap ?? 0) + (weight ?? 0) }
      })
      .sort((left, right) => right.score - left.score)
      .slice(0, limit)
      .map(({ item, overlap, weight }, rank) => rankedItem(copyItem(item), rank, overlap, weight))
  }

  async improve(
    scope: MemoryScope,
    feedback: Feedback,
    options: CallOptions = {}
  ): Promise<MemoryId> {
    this.checkStream(scope)
    const id = MemoryId.new()
    await this.publish(
      scope,
      id.toString(),
      {
        kind: "feedback",
        target: feedback.target.toString(),
        weight: feedback.weight,
        ...(scope.conversation !== undefined ? { conversation: scope.conversation.toString() } : {})
      },
      options
    )
    return id
  }

  async forget(scope: MemoryScope, id: MemoryId, options: CallOptions = {}): Promise<void> {
    this.checkStream(scope)
    await this.publish(
      scope,
      id.toString(),
      {
        kind: "forget",
        target: id.toString(),
        ...(scope.conversation !== undefined ? { conversation: scope.conversation.toString() } : {})
      },
      options
    )
    this.laser[INTERNAL_MEMORY_WRITES]().record({
      view: this.viewKey(),
      id: id.toString(),
      forgotten: true,
      ...(scope.conversation !== undefined ? { conversation: scope.conversation.toString() } : {})
    })
  }

  /** Writes named point state: a keyed item on the topic, reflected in the
   * fold for read-your-writes. */
  async setNamed(key: string, payload: Uint8Array, options: CallOptions = {}): Promise<void> {
    const id = this.namedKey(key)
    await this.publish(
      {},
      id,
      {
        kind: "item",
        id,
        memoryKind: MemoryKind.Fact,
        body: payload.slice()
      },
      options
    )
    this.named.set(id, payload.slice())
  }

  /** Reads named point state from the managed key-value view, like {@link LogMemory.recall},
   * or folds the topic on a deployment that serves no view. Folding is the
   * explicit opt-in {@link LogMemory.fetchNamedFolded}. */
  async fetchNamed(key: string, options: CallOptions = {}): Promise<Uint8Array | undefined> {
    if (!(await this.viewServed())) return this.fetchNamedFolded(key)
    return this.laser.kv(this.resourceNamespace()).get(this.namedKey(key), options)
  }

  /** Reads named point state by folding the topic in process. */
  async fetchNamedFolded(key: string): Promise<Uint8Array | undefined> {
    await this.catchUp()
    return this.named.get(this.namedKey(key))?.slice()
  }

  /** Merge-patches the named item (RFC 7386). A patch or a stored value that
   * is not JSON throws `CodecError`. */
  async updateNamed(key: string, patch: Uint8Array, options: CallOptions = {}): Promise<void> {
    const patchValue = parseJson(patch, "update patch is not JSON")
    const current = await this.fetchNamedFolded(key)
    const base = current === undefined ? null : parseJson(current, "stored value is not JSON")
    const merged = mergePatch(base, patchValue)
    await this.setNamed(key, new TextEncoder().encode(JSON.stringify(merged)), options)
  }

  /** Deletes named point state with a keyed tombstone. */
  async forgetNamed(key: string, options: CallOptions = {}): Promise<void> {
    const id = this.namedKey(key)
    await this.publish({}, id, { kind: "forget", target: id }, options)
    this.named.delete(id)
  }

  private async publish(
    scope: MemoryScope,
    idempotencyKey: string,
    record: MemoryRecord,
    options: CallOptions
  ): Promise<void> {
    const target = this.target(scope, idempotencyKey)
    const payload = await this.govern(target, encodeMemoryRecordFrame(record))
    await this.send(target, scope, payload, options)
  }

  private target(scope: MemoryScope, idempotencyKey: string): RecordTarget {
    return {
      stream: this.requireStream(),
      conversation: scope.conversation ?? ConversationId.derive(idempotencyKey),
      ...(scope.agent !== undefined ? { agent: scope.agent } : {}),
      idempotencyKey
    }
  }

  private govern(target: RecordTarget, payload: Uint8Array): Promise<Uint8Array> {
    return this.laser[INTERNAL_GOVERN]({
      kind: ActionKind.MemoryWrite,
      stream: target.stream,
      topic: this.topic,
      ...(target.agent !== undefined ? { source: target.agent.asStr() } : {}),
      conversation: target.conversation,
      payload,
      signed: false
    })
  }

  private async send(
    target: RecordTarget,
    scope: MemoryScope,
    payload: Uint8Array,
    options: CallOptions
  ): Promise<void> {
    const headers = new Map(
      encodeProvenanceHeaders({
        conversationId: target.conversation,
        ...(target.agent !== undefined ? { agent: target.agent } : {}),
        idempotencyKey: target.idempotencyKey
      })
    )
    headers.set(MEMORY_NAMESPACE, stringHeader(this.resourceNamespace()))
    if (scope.user !== undefined) headers.set(MEMORY_USER, stringHeader(scope.user))
    if (scope.app !== undefined) headers.set(MEMORY_APP, stringHeader(scope.app))
    await this.laser[INTERNAL_TRANSPORT]().sendMessagesWithHeaders(
      target.stream,
      this.topic,
      [{ payload, headers }],
      target.conversation.toString(),
      undefined,
      {
        ...(options.signal === undefined ? {} : { signal: options.signal }),
        createTopic: CREATED_TOPIC
      }
    )
  }

  // One fold at a time, so two concurrent recalls never absorb the same
  // records twice and double a feedback weight.
  private catchUp(): Promise<void> {
    return this.fold.runExclusive(() => this.drain())
  }

  private async drain(): Promise<void> {
    const stream = this.requireStream()
    const namespace = this.resourceNamespace()
    const transport = this.laser[INTERNAL_TRANSPORT]()
    const partitions = await transport.findTopicPartitionCount(stream, this.topic)
    // A memory topic that does not exist yet folds as empty.
    if (partitions === undefined) return
    const topicIds = await transport.resolveStreamTopicIds?.(stream, this.topic)
    const cursor = Cursor.create(
      transport,
      stream,
      this.topic,
      Array.from({ length: partitions }, (_, partition) => partition)
    )
      .batch(READ_BATCH)
      .fromOffsets(this.offsets)
    for (;;) {
      const messages = await cursor.pollRecords()
      if (messages.length === 0) break
      for (const message of messages) {
        this.offsets.set(message.partitionId, message.offset + 1n)
        if (headerString(message.headers, MEMORY_NAMESPACE) !== namespace) continue
        let record: MemoryRecord
        let provenance: Provenance
        try {
          record = decodeMemoryRecord(decodeOne(message.payload, "memory record"), "memory record")
          provenance = decodeProvenanceHeaders(message.headers)
        } catch {
          continue
        }
        this.absorb(
          record,
          provenance,
          message.headers,
          message.timestampMicros ?? 0n,
          topicIds === undefined
            ? undefined
            : {
                kind: "message",
                stream: topicIds.streamId,
                topic: topicIds.topicId,
                partition: message.partitionId,
                offset: message.offset
              }
        )
      }
    }
  }

  private absorb(
    record: MemoryRecord,
    provenance: Provenance,
    headers: ReadonlyMap<string, HeaderValue>,
    timestampMicros: bigint,
    source?: SourceRef
  ): void {
    if (record.kind === "forget") {
      const target = canonicalId(record.target)
      const conversation = canonicalConversation(record.conversation)
      const existing = this.items.get(target)
      if (
        existing !== undefined &&
        existing.timestampMicros <= timestampMicros &&
        (conversation === "" || existing.item.provenance.conversationId.toString() === conversation)
      )
        this.items.delete(target)
      const key = scopedKey(target, conversation)
      if ((this.forgotten.get(key) ?? -1n) < timestampMicros)
        this.forgotten.set(key, timestampMicros)
      if (conversation === "") this.named.delete(record.target)
      return
    }
    if (record.kind === "feedback") {
      const key = scopedKey(canonicalId(record.target), canonicalConversation(record.conversation))
      this.feedback.set(key, (this.feedback.get(key) ?? 0) + record.weight)
      return
    }
    const id = parseMemoryId(record.id)
    // A record id that is not a memory id is a named key.
    if (id === undefined) {
      this.named.set(record.id, record.body.slice())
      return
    }
    const key = id.toString()
    if (
      this.isForgotten(key, provenance.conversationId.toString(), timestampMicros) ||
      this.items.has(key)
    )
      return
    const user = headerString(headers, MEMORY_USER)
    const app = headerString(headers, MEMORY_APP)
    const scope: MemoryScope = {
      ...(user !== undefined ? { user } : {}),
      ...(provenance.agent !== undefined ? { agent: provenance.agent } : {}),
      conversation: provenance.conversationId,
      ...(app !== undefined ? { app } : {}),
      ...(this.stream !== undefined ? { stream: this.stream } : {})
    }
    this.items.set(key, {
      item: {
        id,
        payload: record.body.slice(),
        provenance,
        kind: parseKind(record.memoryKind),
        signals: [],
        ...(record.origin !== undefined ? { origin: record.origin } : {}),
        ...(record.producer !== undefined ? { producer: record.producer } : {}),
        ...(source === undefined
          ? {}
          : {
              source: {
                ...source,
                ...(source.kind === "message"
                  ? { conversation: provenance.conversationId.toString() }
                  : {})
              }
            })
      },
      scope,
      timestampMicros
    })
  }

  // A write at or before a tombstone's time stays forgotten.
  private isForgotten(id: string, conversation: string, timestampMicros: bigint): boolean {
    return [scopedKey(id, ""), scopedKey(id, conversation)].some(
      (key) => (this.forgotten.get(key) ?? -1n) >= timestampMicros
    )
  }

  private feedbackFor(item: MemoryItem): number | undefined {
    const id = item.id.toString()
    const any = this.feedback.get(scopedKey(id, ""))
    const scoped = this.feedback.get(scopedKey(id, item.provenance.conversationId.toString()))
    return any === undefined && scoped === undefined ? undefined : (any ?? 0) + (scoped ?? 0)
  }

  // The namespace the records' header and the key-value read view carry,
  // scoped to the stream the records ride, so the fold writes where recall
  // reads.
  private resourceNamespace(): string {
    return this.laser.resourceNameIn(this.stream, this.namespace)
  }

  private resourceStream(): string | undefined {
    return this.laser.resourceScope(this.stream)
  }

  // Whether the deployment materializes the memory view, which rides the
  // managed key-value store.
  private async viewServed(): Promise<boolean> {
    return (await this.laser.capabilities()).kv.available
  }

  // The view a write lands in and a recall reads: stream, topic, and namespace.
  private viewKey(): string {
    return `${this.stream ?? ""}\0${this.topic}\0${this.resourceNamespace()}`
  }

  private namedKey(key: string): string {
    return `${this.namespace}/${key}`
  }

  // A handle writes to one stream, so a scope naming another is refused
  // rather than written where no recall of that stream reads.
  private checkStream(scope: MemoryScope): void {
    if (scope.stream !== undefined && scope.stream !== this.requireStream()) {
      throw new InvalidError(
        `memory scope names stream \`${scope.stream}\`, but this memory writes to \`${this.requireStream()}\``
      )
    }
  }

  private requireStream(): string {
    if (this.stream === undefined) {
      throw new NoStreamError("log memory requires a default stream")
    }
    return this.stream
  }
}

// Whether one view walk reflects `write`: a remembered id is held, a forgotten
// one is not held in the forget's conversation, or anywhere without one.
function reflected(write: MemoryWrite, held: Held): boolean {
  const conversations = held.get(write.id)
  if (!write.forgotten) return conversations !== undefined
  if (conversations === undefined) return true
  return write.conversation !== undefined && !conversations.has(write.conversation)
}

// The user, agent, and application lenses a write is recorded under.
function writeLenses(scope: MemoryScope): Pick<MemoryWrite, "user" | "agent" | "app"> {
  return {
    ...(scope.user !== undefined ? { user: scope.user } : {}),
    ...(scope.agent !== undefined ? { agent: scope.agent.asStr() } : {}),
    ...(scope.app !== undefined ? { app: scope.app } : {})
  }
}

function recallLimit(query: MemoryQuery): number {
  const limit = query.limit ?? 50
  if (!Number.isSafeInteger(limit) || limit < 0)
    throw new InvalidError("memory recall limit must be a non-negative safe integer")
  return limit
}

function copyItem(item: MemoryItem): MemoryItem {
  return { ...item, payload: item.payload.slice(), signals: [...item.signals] }
}

function parseMemoryId(text: string): MemoryId | undefined {
  try {
    return MemoryId.parse(text)
  } catch {
    return undefined
  }
}

function canonicalId(text: string): string {
  return parseMemoryId(text)?.toString() ?? text
}

function canonicalConversation(text: string | undefined): string {
  if (text === undefined) return ""
  try {
    return ConversationId.parse(text).toString()
  } catch {
    return text
  }
}

function scopedKey(id: string, conversation: string): string {
  return `${id}\0${conversation}`
}

function stringHeader(value: string): HeaderValue {
  return { kind: "string", value }
}

function headerString(headers: ReadonlyMap<string, HeaderValue>, key: string): string | undefined {
  const value = headers.get(key)
  return value?.kind === "string" ? value.value : undefined
}

function parseKind(word: string): MemoryKind {
  return Object.values(MemoryKind).includes(word as MemoryKind)
    ? (word as MemoryKind)
    : MemoryKind.Fact
}

// Rebuild a memory item from a read-view row, or `undefined` when the key is a
// named-item key rather than a recall id, or the row carries no memory scope.
// Arrival order of a memory view row across partitions: the server append time,
// then the source address, then the id. Offsets from different topics or
// partitions are never compared alone. A row without an append time sorts as the
// oldest.
type ArrivalKey = readonly [bigint, number, number, bigint, bigint]

interface Selected {
  readonly arrival: ArrivalKey
  readonly item: MemoryItem
  readonly weight: number | undefined
  readonly overlap: number | undefined
  readonly score: number
}

function arrivalKey(entry: KvEntry, id: MemoryId): ArrivalKey {
  const source = entry.scope?.source
  const timestamp = entry.scope?.timestampMicros ?? 0n
  return source?.kind === "message"
    ? [timestamp, source.topic, source.partition, source.offset, id.asU128()]
    : [timestamp, 0, 0, 0n, id.asU128()]
}

function compareArrival(left: ArrivalKey, right: ArrivalKey): number {
  for (let index = 0; index < left.length; index += 1) {
    const a = left[index] ?? 0
    const b = right[index] ?? 0
    if (a !== b) return a < b ? -1 : 1
  }
  return 0
}

function compareRank(left: Selected, right: Selected): number {
  const score = left.score - right.score
  return score === 0 ? compareArrival(left.arrival, right.arrival) : Math.sign(score)
}

// The item with its ranked score and signals, unchanged when nothing ranked it.
function rankedItem(
  item: MemoryItem,
  rank: number,
  overlap: number | undefined,
  weight: number | undefined
): MemoryItem {
  if (overlap === undefined && weight === undefined) return item
  return {
    ...item,
    score: (overlap ?? 0) + (weight ?? 0),
    signals: [
      ...(overlap === undefined
        ? []
        : [{ strategy: RecallStrategy.Keyword, rank, score: overlap }]),
      ...(weight === undefined ? [] : [{ strategy: RecallStrategy.Auto, rank, score: weight }])
    ]
  }
}

function itemFromEntry(
  conversation: ConversationId | undefined,
  entry: KvEntry
): MemoryItem | undefined {
  const { scope } = entry
  if (scope === undefined) return undefined
  let id: MemoryId
  try {
    id = MemoryId.parse(new TextDecoder().decode(entry.key))
  } catch {
    return undefined
  }
  let storedConversation: ConversationId
  let agent: AgentId | undefined
  try {
    if (scope.conversation === undefined) {
      if (conversation === undefined) return undefined
      storedConversation = conversation
    } else {
      storedConversation = ConversationId.parse(scope.conversation)
    }
    agent = scope.agent === undefined ? undefined : AgentId.new(scope.agent)
  } catch {
    return undefined
  }
  return {
    id,
    payload: entry.value.slice(),
    provenance: {
      conversationId: storedConversation,
      ...(agent === undefined ? {} : { agent })
    },
    kind: kindFromWord(scope.kind),
    ...(scope.source === undefined ? {} : { source: scope.source }),
    signals: []
  }
}

function kindFromWord(word: string | undefined): MemoryKind {
  const kinds: readonly MemoryKind[] = Object.values(MemoryKind)
  return kinds.find((kind) => kind === word) ?? MemoryKind.Fact
}

function matchesScope(
  stored: MemoryScope,
  requested: MemoryScope,
  agent: MemoryScope["agent"]
): boolean {
  return (
    (requested.stream === undefined || stored.stream === requested.stream) &&
    (requested.user === undefined || stored.user === requested.user) &&
    (agent === undefined || stored.agent?.equals(agent) === true) &&
    (requested.conversation === undefined ||
      stored.conversation?.equals(requested.conversation) === true) &&
    (requested.app === undefined || stored.app === requested.app)
  )
}

// The query's words when they rank the scope instead of filtering it: a
// semantic, hybrid, or auto query with text keeps every item.
function rankingTokens(query: MemoryQuery): ReadonlySet<string> | undefined {
  const strategy = query.strategy ?? RecallStrategy.Auto
  const ranks =
    strategy === RecallStrategy.Semantic ||
    strategy === RecallStrategy.Hybrid ||
    strategy === RecallStrategy.Auto
  if (!ranks || query.semantic === undefined) return undefined
  // Text without a word ranks nothing, so the recall stays newest first.
  const tokens = tokenize(query.semantic)
  return tokens.size === 0 ? undefined : tokens
}

// Keyword, graph, and temporal queries keep only the items that share a word.
function matchesQueryText(query: MemoryQuery, payload: Uint8Array): boolean {
  if (
    query.strategy === RecallStrategy.Recent ||
    query.semantic === undefined ||
    rankingTokens(query) !== undefined
  )
    return true
  const tokens = tokenize(query.semantic)
  return tokens.size === 0 || keywordScore(tokens, payload) > 0
}

function parseJson(bytes: Uint8Array, what: string): unknown {
  try {
    return JSON.parse(new TextDecoder().decode(bytes))
  } catch (error) {
    throw new CodecError(
      `${what}: ${error instanceof Error ? error.message : String(error)}`,
      "memory",
      "update",
      { cause: error }
    )
  }
}

// RFC 7386 over parsed JSON. The merged object has no prototype, so every key
// a patch carries, `__proto__` included, lands as an own field and serializes
// back out.
function mergePatch(base: unknown, patch: unknown): unknown {
  if (!isObject(patch)) return patch
  const output = Object.create(null) as Record<string, unknown>
  if (isObject(base)) for (const [key, value] of Object.entries(base)) output[key] = value
  for (const [key, value] of Object.entries(patch)) {
    if (value === null) Reflect.deleteProperty(output, key)
    else output[key] = mergePatch(output[key], value)
  }
  return output
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

function lineage(scope: MemoryScope): {
  readonly origin?: SourceRef
  readonly producer?: ProducerInfo
} {
  return {
    ...(scope.origin !== undefined ? { origin: scope.origin } : {}),
    ...(scope.producer !== undefined ? { producer: scope.producer } : {})
  }
}
