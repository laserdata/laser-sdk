import {
  Lifetime,
  MemoryId,
  MemoryKind,
  RecallStrategy,
  ZERO_CONVERSATION,
  type Embedder,
  type Feedback,
  type Memory,
  type MemoryItem,
  type MemoryQuery,
  type MemoryScope,
  type RecallSignal
} from "./types.js"

interface VectorEntry {
  readonly id: MemoryId
  readonly scope: MemoryScope
  readonly embedding: readonly number[]
  readonly item: MemoryItem
  feedback: number
}

export class ZeroEmbedder implements Embedder {
  embed(): Promise<readonly number[]> {
    return Promise.resolve([0])
  }
}

export class VectorMemory implements Memory {
  private readonly items: VectorEntry[] = []

  constructor(
    private readonly embedder: Embedder = new ZeroEmbedder(),
    private readonly laser?: Laser
  ) {}

  static governed(laser: Laser, embedder?: Embedder): VectorMemory {
    return new VectorMemory(embedder, laser)
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
    if (this.items.some((entry) => entry.id.equals(id))) return id
    const governed = await this.govern(scope, {
      kind: "item",
      id: id.toString(),
      memoryKind: kind,
      body: payload
    })
    if (governed.kind !== "item") throw new TypeError("governed memory item changed record kind")
    const embedding = await this.embedder.embed(new TextDecoder().decode(governed.body))
    const provenance = {
      conversationId: scope.conversation ?? ZERO_CONVERSATION,
      ...(scope.agent !== undefined ? { agent: scope.agent } : {}),
      idempotencyKey: id.toString()
    }
    this.items.push({
      id,
      scope: normalizedScope(scope),
      embedding: [...embedding],
      item: {
        id,
        payload: governed.body.slice(),
        provenance,
        kind,
        signals: []
      },
      feedback: 0
    })
    return id
  }

  async recall(scope: MemoryScope, query: MemoryQuery): Promise<readonly MemoryItem[]> {
    const limit = query.limit ?? 50
    const strategy = query.strategy ?? RecallStrategy.Auto
    const text = query.semantic
    const wantsSemantic =
      text !== undefined &&
      strategy !== RecallStrategy.Keyword &&
      strategy !== RecallStrategy.Recent
    const wantsKeyword =
      text !== undefined &&
      (strategy === RecallStrategy.Keyword || strategy === RecallStrategy.Hybrid)
    const queryEmbedding = wantsSemantic ? await this.embedder.embed(text) : undefined
    const queryTokens = wantsKeyword ? tokenize(text) : undefined
    const agent = query.agent ?? scope.agent
    const matched = this.items.filter((entry) => matchesScope(entry.scope, scope, agent))

    if (queryEmbedding === undefined && queryTokens === undefined && !matched.some(hasFeedback)) {
      return matched
        .slice()
        .reverse()
        .slice(0, limit)
        .map((entry) => copyItem(entry.item))
    }

    return matched
      .map((entry) => {
        const semantic =
          queryEmbedding === undefined ? undefined : cosine(queryEmbedding, entry.embedding)
        const keyword =
          queryTokens === undefined ? undefined : keywordScore(queryTokens, entry.item.payload)
        const score = entry.feedback + (semantic ?? 0) + (keyword ?? 0)
        const signals: RecallSignal[] = []
        if (semantic !== undefined)
          signals.push({ strategy: RecallStrategy.Semantic, rank: 0, score: semantic })
        if (keyword !== undefined)
          signals.push({ strategy: RecallStrategy.Keyword, rank: 0, score: keyword })
        if (entry.feedback !== 0) {
          signals.push({ strategy: RecallStrategy.Auto, rank: 0, score: entry.feedback })
        }
        return { entry, score, signals }
      })
      .sort((left, right) => right.score - left.score)
      .slice(0, limit)
      .map(({ entry, score, signals }) => ({ ...copyItem(entry.item), score, signals }))
  }

  async improve(scope: MemoryScope, feedback: Feedback): Promise<MemoryId> {
    const governed = await this.govern(scope, {
      kind: "feedback",
      target: feedback.target.toString(),
      weight: feedback.weight
    })
    if (governed.kind !== "feedback") {
      throw new TypeError("governed memory feedback changed record kind")
    }
    const entry = this.items.find((candidate) => candidate.id.toString() === governed.target)
    if (entry !== undefined) entry.feedback += governed.weight
    return MemoryId.new()
  }

  async forget(scope: MemoryScope, id: MemoryId): Promise<void> {
    const governed = await this.govern(scope, { kind: "forget", target: id.toString() })
    if (governed.kind !== "forget")
      throw new TypeError("governed memory forget changed record kind")
    const index = this.items.findIndex((entry) => entry.id.toString() === governed.target)
    if (index !== -1) this.items.splice(index, 1)
  }

  size(): number {
    return this.items.length
  }

  private async govern(scope: MemoryScope, record: MemoryRecord): Promise<MemoryRecord> {
    if (this.laser === undefined) return record
    const stream = this.laser.defaultStream
    if (stream === undefined) throw new NoStreamError("governed vector memory requires a stream")
    const encoded = record.kind === "item" ? record.body : encodeMemoryRecordFrame(record)
    const payload = await this.laser[INTERNAL_GOVERN]({
      kind: ActionKind.MemoryWrite,
      stream,
      topic: AgentTopic.Audit,
      ...(scope.agent !== undefined ? { source: scope.agent.asString() } : {}),
      ...(scope.conversation !== undefined ? { conversation: scope.conversation } : {}),
      payload: encoded,
      signed: false
    })
    if (record.kind === "item") return { ...record, body: payload }
    return decodeMemoryRecord(
      decodeOne(payload, "governed memory record"),
      "governed memory record"
    )
  }
}

function normalizedScope(scope: MemoryScope): MemoryScope {
  return { ...scope, lifetime: scope.lifetime ?? Lifetime.Session }
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
    (requested.application === undefined || stored.application === requested.application) &&
    (requested.lifetime === undefined ||
      (stored.lifetime ?? Lifetime.Session) === requested.lifetime)
  )
}

function hasFeedback(entry: VectorEntry): boolean {
  return entry.feedback !== 0
}

function copyItem(item: MemoryItem): MemoryItem {
  return { ...item, payload: item.payload.slice(), signals: [...item.signals] }
}

function cosine(left: readonly number[], right: readonly number[]): number {
  let dot = 0
  let leftNorm = 0
  let rightNorm = 0
  const length = Math.min(left.length, right.length)
  for (let index = 0; index < length; index += 1) {
    const leftValue = left[index] ?? 0
    const rightValue = right[index] ?? 0
    dot += leftValue * rightValue
    leftNorm += leftValue * leftValue
    rightNorm += rightValue * rightValue
  }
  return leftNorm === 0 || rightNorm === 0 ? 0 : dot / Math.sqrt(leftNorm * rightNorm)
}

function tokenize(text: string): ReadonlySet<string> {
  return new Set(
    text
      .toLowerCase()
      .split(/[^\p{L}\p{N}]+/u)
      .filter(Boolean)
  )
}

function keywordScore(query: ReadonlySet<string>, payload: Uint8Array): number {
  if (query.size === 0) return 0
  const body = tokenize(new TextDecoder().decode(payload))
  let hits = 0
  for (const token of query) if (body.has(token)) hits += 1
  return hits / query.size
}
import { NoStreamError } from "../client/errors.js"
import { INTERNAL_GOVERN } from "../client/internals.js"
import type { Laser } from "../client/laser.js"
import { ActionKind } from "../govern.js"
import { AgentTopic } from "../provenance/agent-topic.js"
import { decodeOne } from "../wire/cbor.js"
import { decodeMemoryRecord, encodeMemoryRecordFrame, type MemoryRecord } from "../wire/memory.js"
