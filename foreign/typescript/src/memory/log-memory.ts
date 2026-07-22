import { NoStreamError } from "../client/errors.js"
import { INTERNAL_GOVERN, INTERNAL_TRANSPORT } from "../client/internals.js"
import type { Laser } from "../client/laser.js"
import { ActionKind } from "../govern.js"
import type { IggyHeaderValue } from "../iggy/apache-iggy.js"
import { AgentTopic } from "../provenance/agent-topic.js"
import { decodeProvenanceHeaders, encodeProvenanceHeaders } from "../provenance/provenance.js"
import { ConversationId } from "../types/ids.js"
import { MEMORY_APP, MEMORY_NAMESPACE, MEMORY_USER } from "../wire/headers.js"
import { decodeOne } from "../wire/cbor.js"
import { decodeMemoryRecord, encodeMemoryRecordFrame, type MemoryRecord } from "../wire/memory.js"
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

  async recall(scope: MemoryScope, query: MemoryQuery): Promise<readonly MemoryItem[]> {
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
        this.absorb(record, message.headers)
      }
    }
  }

  private absorb(record: MemoryRecord, headers: ReadonlyMap<string, IggyHeaderValue>): void {
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
        signals: []
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
    if (value === null) Reflect.deleteProperty(output, key)
    else output[key] = mergePatch(output[key], value)
  }
  return output
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}
