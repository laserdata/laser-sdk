import { CodecError, InvalidError, NoStreamError, ProtocolError } from "../client/errors.js"
import { INTERNAL_GOVERN, INTERNAL_TRANSPORT } from "../client/internals.js"
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
import type { SourceRef } from "../wire/graph.js"
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

const READ_BATCH = 1000

interface FoldedItem {
  readonly item: MemoryItem
  readonly scope: MemoryScope
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
  private readonly forgotten = new Set<string>()
  // Feedback weight per target, kept apart from the items so a weight folded
  // before its item (a cross-partition reorder) still applies.
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

  /** A log-backed memory on `topic` (the audit topic by default). The named
   * items key on `namespace`, which defaults to the topic's name. */
  constructor(
    private readonly laser: Laser,
    namespace?: string,
    topic: string = AgentTopic.Audit,
    stream: string | undefined = laser.defaultStream
  ) {
    this.topic = topic
    this.stream = stream
    this.namespace = namespace ?? topic
  }

  async remember(scope: MemoryScope, payload: Uint8Array): Promise<MemoryId> {
    return this.append(scope, MemoryId.new(), MemoryKind.Fact, payload)
  }

  async append(
    scope: MemoryScope,
    id: MemoryId,
    kind: MemoryKind,
    payload: Uint8Array
  ): Promise<MemoryId> {
    const target = this.target(scope, id.toString())
    // The governor sees the item body, not the record frame around it.
    const body = await this.govern(target, payload.slice())
    await this.send(
      target,
      scope,
      encodeMemoryRecordFrame({ kind: "item", id: id.toString(), memoryKind: kind, body })
    )
    return id
  }

  /** Reads the managed key-value view the deployment materializes from the
   * memory topic, so a large topic never folds in process. Folding is the
   * opt-in {@link LogMemory.recallFolded}, never the default, matching Rust and Python. */
  async recall(scope: MemoryScope, query: MemoryQuery): Promise<readonly MemoryItem[]> {
    const limit = recallLimit(query)
    if (limit === 0 || (scope.stream !== undefined && scope.stream !== this.stream)) return []
    const selected = new Map<string, MemoryItem>()
    const agent = query.agent ?? scope.agent
    let cursor: Uint8Array | undefined
    for (;;) {
      const scan = this.laser.kv(this.namespace).scan()
      if (scope.conversation !== undefined) scan.conversation(scope.conversation.toString())
      if (cursor !== undefined) scan.cursor(cursor)
      const page = await scan.fetch()
      for (const entry of page.entries) {
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
        selected.set(item.id.toString(), item)
        if (selected.size > limit) {
          const oldest = [...selected.keys()].sort()[0]
          if (oldest !== undefined) selected.delete(oldest)
        }
      }
      if (page.cursor === undefined) break
      if (
        page.cursor.length === cursor?.length &&
        page.cursor.every((byte, index) => byte === cursor?.[index])
      )
        throw new ProtocolError("memory scan cursor did not advance")
      cursor = page.cursor
    }
    return [...selected.values()].sort((left, right) =>
      left.id.asU128() > right.id.asU128() ? -1 : left.id.asU128() < right.id.asU128() ? 1 : 0
    )
  }

  /** Recall by folding the memory topic in process, the opt-in path for a small
   * deployment with no materialized read view. Items come newest first.
   * Feedback reorders every strategy except `Recent`, and its signal carries
   * `RecallStrategy.Auto`. */
  async recallFolded(scope: MemoryScope, query: MemoryQuery): Promise<readonly MemoryItem[]> {
    const limit = recallLimit(query)
    if (limit === 0 || (scope.stream !== undefined && scope.stream !== this.stream)) return []
    await this.catchUp()
    const agent = query.agent ?? scope.agent
    const newestFirst = [...this.items.values()]
      .filter((entry) => matchesScope(entry.scope, scope, agent))
      .map((entry) => entry.item)
      .reverse()
    if (query.strategy === RecallStrategy.Recent || this.feedback.size === 0)
      return newestFirst.slice(0, limit).map(copyItem)
    return newestFirst
      .map((item) => ({ item, weight: this.feedback.get(item.id.toString()) }))
      .sort((left, right) => (right.weight ?? 0) - (left.weight ?? 0))
      .slice(0, limit)
      .map(({ item, weight }, rank) =>
        weight === undefined
          ? copyItem(item)
          : {
              ...copyItem(item),
              score: weight,
              signals: [{ strategy: RecallStrategy.Auto, rank, score: weight }]
            }
      )
  }

  async improve(scope: MemoryScope, feedback: Feedback): Promise<MemoryId> {
    const id = MemoryId.new()
    await this.publish(scope, id.toString(), {
      kind: "feedback",
      target: feedback.target.toString(),
      weight: feedback.weight
    })
    return id
  }

  async forget(scope: MemoryScope, id: MemoryId): Promise<void> {
    await this.publish(scope, id.toString(), { kind: "forget", target: id.toString() })
  }

  /** Writes named point state: a keyed item on the topic, reflected in the
   * fold for read-your-writes. */
  async setNamed(key: string, payload: Uint8Array): Promise<void> {
    const id = this.namedKey(key)
    await this.publish({}, id, {
      kind: "item",
      id,
      memoryKind: MemoryKind.Fact,
      body: payload.slice()
    })
    this.named.set(id, payload.slice())
  }

  /** Reads named point state from the managed key-value view, like {@link LogMemory.recall}.
   * Folding the topic is the opt-in {@link LogMemory.fetchNamedFolded}. */
  async fetchNamed(key: string): Promise<Uint8Array | undefined> {
    return this.laser.kv(this.namespace).get(new TextEncoder().encode(this.namedKey(key)))
  }

  /** Reads named point state by folding the topic in process. */
  async fetchNamedFolded(key: string): Promise<Uint8Array | undefined> {
    await this.catchUp()
    return this.named.get(this.namedKey(key))?.slice()
  }

  /** Merge-patches the named item (RFC 7386). A patch or a stored value that
   * is not JSON throws `CodecError`. */
  async updateNamed(key: string, patch: Uint8Array): Promise<void> {
    const patchValue = parseJson(patch, "update patch is not JSON")
    const current = await this.fetchNamedFolded(key)
    const base = current === undefined ? null : parseJson(current, "stored value is not JSON")
    const merged = mergePatch(base, patchValue)
    await this.setNamed(key, new TextEncoder().encode(JSON.stringify(merged)))
  }

  /** Deletes named point state with a keyed tombstone. */
  async forgetNamed(key: string): Promise<void> {
    const id = this.namedKey(key)
    await this.publish({}, id, { kind: "forget", target: id })
    this.named.delete(id)
  }

  private async publish(
    scope: MemoryScope,
    idempotencyKey: string,
    record: MemoryRecord
  ): Promise<void> {
    const target = this.target(scope, idempotencyKey)
    const payload = await this.govern(target, encodeMemoryRecordFrame(record))
    await this.send(target, scope, payload)
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

  private async send(target: RecordTarget, scope: MemoryScope, payload: Uint8Array): Promise<void> {
    const headers = new Map(
      encodeProvenanceHeaders({
        conversationId: target.conversation,
        ...(target.agent !== undefined ? { agent: target.agent } : {}),
        idempotencyKey: target.idempotencyKey
      })
    )
    headers.set(MEMORY_NAMESPACE, stringHeader(this.namespace))
    if (scope.user !== undefined) headers.set(MEMORY_USER, stringHeader(scope.user))
    if (scope.app !== undefined) headers.set(MEMORY_APP, stringHeader(scope.app))
    await this.laser[INTERNAL_TRANSPORT]().sendMessageWithHeaders(
      target.stream,
      this.topic,
      payload,
      headers,
      target.conversation.toString()
    )
  }

  // One fold at a time, so two concurrent recalls never absorb the same
  // records twice and double a feedback weight.
  private catchUp(): Promise<void> {
    return this.fold.runExclusive(() => this.drain())
  }

  private async drain(): Promise<void> {
    const stream = this.requireStream()
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
        if (headerString(message.headers, MEMORY_NAMESPACE) !== this.namespace) continue
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
    source?: SourceRef
  ): void {
    if (record.kind === "forget") {
      const target = canonicalId(record.target)
      this.items.delete(target)
      this.forgotten.add(target)
      this.named.delete(record.target)
      return
    }
    if (record.kind === "feedback") {
      const target = canonicalId(record.target)
      this.feedback.set(target, (this.feedback.get(target) ?? 0) + record.weight)
      return
    }
    const id = parseMemoryId(record.id)
    // A record id that is not a memory id is a named key.
    if (id === undefined) {
      this.named.set(record.id, record.body.slice())
      return
    }
    const key = id.toString()
    if (this.forgotten.has(key) || this.items.has(key)) return
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
      scope
    })
  }

  private namedKey(key: string): string {
    return `${this.namespace}/${key}`
  }

  private requireStream(): string {
    if (this.stream === undefined) {
      throw new NoStreamError("log memory requires a default stream")
    }
    return this.stream
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
