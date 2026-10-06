import { InvalidError, NoStreamError, ProtocolError } from "../client/errors.js"
import { INTERNAL_GOVERN, INTERNAL_TRANSPORT } from "../client/internals.js"
import type { Laser } from "../client/laser.js"
import { ActionKind } from "../govern.js"
import type { IggyHeaderValue } from "../iggy/apache-iggy.js"
import { AgentTopic } from "../provenance/agent-topic.js"
import { decodeProvenanceHeaders, encodeProvenanceHeaders } from "../provenance/provenance.js"
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

interface FoldedItem {
  readonly item: MemoryItem
  readonly scope: MemoryScope
  readonly sequence: number
  feedback: number
}

export class LogMemory implements Memory {
  private readonly items = new Map<string, FoldedItem>()
  private readonly forgotten = new Set<string>()
  private readonly named = new Map<string, Uint8Array>()
  private readonly offsets = new Map<number, bigint>()
  private sequence = 0

  constructor(
    private readonly laser: Laser,
    readonly namespace: string,
    readonly topic: string = AgentTopic.Audit,
    readonly stream: string | undefined = laser.defaultStream
  ) {}

  async remember(scope: MemoryScope, payload: Uint8Array): Promise<MemoryId> {
    return this.append(scope, MemoryId.new(), MemoryKind.Fact, payload)
  }

  async append(
    scope: MemoryScope,
    id: MemoryId,
    kind: MemoryKind,
    payload: Uint8Array
  ): Promise<MemoryId> {
    await this.publish(scope, id.toString(), {
      kind: "item",
      id: id.toString(),
      memoryKind: kind,
      body: payload.slice()
    })
    return id
  }

  /** Reads the managed key-value view the deployment materializes from the
   * memory topic, so a large topic never folds in process. Folding is the
   * opt-in {@link LogMemory.recallFolded}, never the default, matching Rust and Python. */
  async recall(scope: MemoryScope, query: MemoryQuery): Promise<readonly MemoryItem[]> {
    const limit = query.limit ?? 50
    if (!Number.isSafeInteger(limit) || limit < 0)
      throw new InvalidError("memory recall limit must be a non-negative safe integer")
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
          (scope.application !== undefined && stored.app !== scope.application) ||
          (agent !== undefined && stored.agent !== agent.asString())
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
   * deployment with no materialized read view. */
  async recallFolded(scope: MemoryScope, query: MemoryQuery): Promise<readonly MemoryItem[]> {
    await this.catchUp()
    const agent = query.agent ?? scope.agent
    const matched = [...this.items.values()]
      .filter((entry) => matchesScope(entry.scope, scope, agent))
      .sort((left, right) => right.feedback - left.feedback || right.sequence - left.sequence)
      .slice(0, query.limit ?? 50)
    const strategy = query.strategy ?? RecallStrategy.Auto
    return matched.map((entry, rank) => ({
      ...entry.item,
      payload: entry.item.payload.slice(),
      ...(entry.feedback !== 0 ? { score: entry.feedback } : {}),
      signals: entry.feedback === 0 ? [] : [{ strategy, rank, score: entry.feedback }]
    }))
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

  async set(key: string, payload: Uint8Array): Promise<void> {
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
   * Folding the topic is the opt-in {@link LogMemory.fetchFolded}. */
  async fetch(key: string): Promise<Uint8Array | undefined> {
    return this.laser.kv(this.namespace).get(new TextEncoder().encode(this.namedKey(key)))
  }

  async fetchFolded(key: string): Promise<Uint8Array | undefined> {
    await this.catchUp()
    return this.named.get(this.namedKey(key))?.slice()
  }

  async update(key: string, patch: Uint8Array): Promise<void> {
    const current = await this.fetchFolded(key)
    const base: unknown =
      current === undefined ? null : JSON.parse(new TextDecoder().decode(current))
    const patchValue: unknown = JSON.parse(new TextDecoder().decode(patch))
    const merged = mergePatch(base, patchValue)
    await this.set(key, new TextEncoder().encode(JSON.stringify(merged)))
  }

  async remove(key: string): Promise<void> {
    const id = this.namedKey(key)
    await this.publish({}, id, { kind: "forget", target: id })
    this.named.delete(id)
  }

  private async publish(
    scope: MemoryScope,
    idempotencyKey: string,
    record: MemoryRecord
  ): Promise<void> {
    const stream = this.requireStream()
    const conversation = scope.conversation ?? ConversationId.derive(idempotencyKey)
    const headers = new Map(
      encodeProvenanceHeaders({
        conversationId: conversation,
        ...(scope.agent !== undefined ? { agent: scope.agent } : {}),
        idempotencyKey
      })
    )
    headers.set(MEMORY_NAMESPACE, stringHeader(this.namespace))
    if (scope.user !== undefined) headers.set(MEMORY_USER, stringHeader(scope.user))
    if (scope.application !== undefined) headers.set(MEMORY_APP, stringHeader(scope.application))
    const encoded = encodeMemoryRecordFrame(record)
    const payload = await this.laser[INTERNAL_GOVERN]({
      kind: ActionKind.MemoryWrite,
      stream,
      topic: this.topic,
      ...(scope.agent !== undefined ? { source: scope.agent.asString() } : {}),
      conversation,
      payload: encoded,
      signed: false
    })
    await this.laser[INTERNAL_TRANSPORT]().sendMessageWithHeaders(
      stream,
      this.topic,
      payload,
      headers,
      conversation.toString()
    )
  }

  private async catchUp(): Promise<void> {
    const stream = this.requireStream()
    const cursor = await this.laser.stream(stream).topic(this.topic).replay({ batchSize: 1000 })
    const topicIds = await this.laser[INTERNAL_TRANSPORT]().resolveStreamTopicIds?.(
      stream,
      this.topic
    )
    cursor.fromOffsets(this.offsets)
    for (;;) {
      const messages = await cursor.poll()
      if (messages.length === 0) break
      for (const message of messages) {
        this.offsets.set(message.partitionId, message.offset + 1n)
        if (headerString(message.headers, MEMORY_NAMESPACE) !== this.namespace) continue
        let record: MemoryRecord
        try {
          record = decodeMemoryRecord(decodeOne(message.payload, "memory record"), "memory record")
        } catch {
          continue
        }
        this.absorb(
          record,
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
    headers: ReadonlyMap<string, IggyHeaderValue>,
    source?: SourceRef
  ): void {
    if (record.kind === "forget") {
      this.items.delete(record.target)
      this.forgotten.add(record.target)
      this.named.delete(record.target)
      return
    }
    if (record.kind === "feedback") {
      const entry = this.items.get(record.target)
      if (entry !== undefined) entry.feedback += record.weight
      return
    }
    if (record.id.startsWith(`${this.namespace}/`)) {
      this.named.set(record.id, record.body.slice())
      return
    }
    if (this.forgotten.has(record.id) || this.items.has(record.id)) return
    const id = MemoryId.parse(record.id)
    const provenance = decodeProvenanceHeaders(headers)
    const user = headerString(headers, MEMORY_USER)
    const application = headerString(headers, MEMORY_APP)
    const scope: MemoryScope = {
      ...(user !== undefined ? { user } : {}),
      ...(provenance.agent !== undefined ? { agent: provenance.agent } : {}),
      conversation: provenance.conversationId,
      ...(application !== undefined ? { application } : {}),
      ...(this.stream !== undefined ? { stream: this.stream } : {})
    }
    this.items.set(record.id, {
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
      scope,
      sequence: this.sequence,
      feedback: 0
    })
    this.sequence += 1
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

function stringHeader(value: string): IggyHeaderValue {
  return { kind: "string", value }
}

function headerString(
  headers: ReadonlyMap<string, IggyHeaderValue>,
  key: string
): string | undefined {
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
    (requested.application === undefined || stored.application === requested.application)
  )
}

function mergePatch(base: unknown, patch: unknown): unknown {
  if (!isObject(patch)) return patch
  const output: Record<string, unknown> = isObject(base) ? { ...base } : {}
  for (const [key, value] of Object.entries(patch)) {
    // `JSON.parse` produces `__proto__` as an own key, and assigning it would
    // hit the prototype setter: the key is silently lost and the merged
    // object's prototype is rewired. Reserved keys are skipped instead.
    if (key === "__proto__" || key === "constructor" || key === "prototype") continue
    if (value === null) Reflect.deleteProperty(output, key)
    else output[key] = mergePatch(output[key], value)
  }
  return output
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}
