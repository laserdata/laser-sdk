import { InvalidError } from "../client/errors.js"
import { mintUlidValue, type UlidSource } from "../runtime/ulid.js"
import { ConversationId } from "../types/ids.js"
import type { AgentId } from "../types/ids.js"
import { contentId } from "../wire/hashing.js"
import { crockfordDecode, crockfordEncode } from "../wire/ids.js"
import type { SourceRef } from "../wire/graph.js"

export const MemoryKind = {
  Fact: "fact",
  Message: "message",
  Summary: "summary",
  Entity: "entity",
  Feedback: "feedback",
  Procedure: "procedure"
} as const

export type MemoryKind = (typeof MemoryKind)[keyof typeof MemoryKind]

export const MemoryClass = {
  Episodic: "episodic",
  Semantic: "semantic",
  Procedural: "procedural"
} as const

export type MemoryClass = (typeof MemoryClass)[keyof typeof MemoryClass]

export const Lifetime = { Session: "session", Durable: "durable" } as const
export type Lifetime = (typeof Lifetime)[keyof typeof Lifetime]

export const RecallStrategy = {
  Auto: "auto",
  Recent: "recent",
  Semantic: "semantic",
  Keyword: "keyword",
  Graph: "graph",
  Temporal: "temporal",
  Hybrid: "hybrid"
} as const

export type RecallStrategy = (typeof RecallStrategy)[keyof typeof RecallStrategy]

const KIND_CODES: Readonly<Record<MemoryKind, number>> = {
  [MemoryKind.Fact]: 1,
  [MemoryKind.Message]: 2,
  [MemoryKind.Summary]: 3,
  [MemoryKind.Entity]: 4,
  [MemoryKind.Feedback]: 5,
  [MemoryKind.Procedure]: 6
}

export function memoryClass(kind: MemoryKind): MemoryClass {
  if (kind === MemoryKind.Message) return MemoryClass.Episodic
  if (kind === MemoryKind.Procedure) return MemoryClass.Procedural
  return MemoryClass.Semantic
}

export class MemoryId {
  private constructor(private readonly value: bigint) {}

  static new(source?: UlidSource): MemoryId {
    return new MemoryId(mintUlidValue(source))
  }

  static fromU128(value: bigint): MemoryId {
    if (value < 0n || value >= 1n << 128n) {
      throw new InvalidError("memory id must fit in 128 bits", { value: value.toString() })
    }
    return new MemoryId(value)
  }

  static parse(text: string): MemoryId {
    return MemoryId.fromU128(crockfordDecode(text))
  }

  static content(owner: MemoryScope, kind: MemoryKind, body: Uint8Array): MemoryId {
    const encoder = new TextEncoder()
    const segments = [
      encoder.encode(owner.stream ?? ""),
      Uint8Array.of(0),
      encoder.encode(owner.agent?.asString() ?? ""),
      Uint8Array.of(0),
      Uint8Array.of(KIND_CODES[kind]),
      body
    ]
    return MemoryId.fromU128(contentId(segments))
  }

  asU128(): bigint {
    return this.value
  }

  equals(other: MemoryId): boolean {
    return this.value === other.value
  }

  toString(): string {
    return crockfordEncode(this.value)
  }
}

export interface MemoryScope {
  readonly stream?: string
  readonly user?: string
  readonly agent?: AgentId
  readonly conversation?: ConversationId
  readonly application?: string
  readonly lifetime?: Lifetime
}

export interface RecallSignal {
  readonly strategy: RecallStrategy
  readonly rank: number
  readonly score?: number
}

export interface MemoryItem {
  readonly id: MemoryId
  readonly payload: Uint8Array
  readonly provenance: {
    readonly conversationId: ConversationId
    readonly agent?: AgentId
    readonly idempotencyKey?: string
  }
  readonly kind: MemoryKind
  readonly score?: number
  readonly signals: readonly RecallSignal[]
  readonly source?: SourceRef
}

export interface MemoryQuery {
  readonly limit?: number
  readonly tokenBudget?: number
  readonly agent?: AgentId
  readonly semantic?: string
  readonly strategy?: RecallStrategy
}

export interface Feedback {
  readonly target: MemoryId
  readonly weight: number
  readonly note?: string
}

export interface Memory {
  remember(scope: MemoryScope, payload: Uint8Array): Promise<MemoryId>
  recall(scope: MemoryScope, query: MemoryQuery): Promise<readonly MemoryItem[]>
  improve(scope: MemoryScope, feedback: Feedback): Promise<MemoryId>
  forget(scope: MemoryScope, id: MemoryId): Promise<void>
}

export interface Embedder {
  embed(text: string): Promise<readonly number[]>
}

export interface Reranker {
  rerank(query: string, items: readonly MemoryItem[]): Promise<readonly MemoryItem[]>
}

export interface ConsolidationReport {
  readonly scanned: number
  readonly kept: number
  readonly forgotten: number
}

export interface Consolidator {
  consolidate(memory: Memory, scope: MemoryScope): Promise<ConsolidationReport>
}

export function toContextBlock(items: readonly MemoryItem[], tokenBudget?: number): string {
  let spent = 0
  let rendered = 0
  const blocks: string[] = []
  for (const item of items) {
    const text = new TextDecoder().decode(item.payload)
    const cost = Math.max(1, Math.ceil(new TextEncoder().encode(text).byteLength / 4))
    if (tokenBudget !== undefined && rendered > 0 && spent + cost > tokenBudget) break
    blocks.push(text)
    spent += cost
    rendered += 1
  }
  const omitted = items.length - rendered
  if (omitted > 0) blocks.push(`[... ${String(omitted)} more recalled item(s) omitted ...]`)
  return blocks.join("\n\n")
}

export function fuseReciprocalRank(
  signals: readonly (readonly MemoryItem[])[],
  limit: number
): readonly MemoryItem[] {
  const byId = new Map<string, MemoryItem>()
  for (const ranked of signals) {
    ranked.forEach((candidate, rank) => {
      const key = candidate.id.toString()
      const contribution = 1 / (60 + rank)
      const held = byId.get(key)
      const signal: RecallSignal = {
        strategy: candidate.signals[0]?.strategy ?? RecallStrategy.Auto,
        rank,
        ...(candidate.score !== undefined ? { score: candidate.score } : {})
      }
      byId.set(
        key,
        held === undefined
          ? { ...candidate, score: contribution, signals: [signal] }
          : {
              ...held,
              score: (held.score ?? 0) + contribution,
              signals: [...held.signals, signal]
            }
      )
    })
  }
  return [...byId.values()]
    .sort((left, right) => (right.score ?? 0) - (left.score ?? 0))
    .slice(0, limit)
}

export const ZERO_CONVERSATION = ConversationId.parse("00000000000000000000000000")
