import { setWorldConstructor, type IWorldOptions } from "@cucumber/cucumber"
import {
  ConversationId,
  Laser,
  MemoryHandle,
  type Capabilities,
  type ContextMessage,
  type LaserError,
  type MemoryId
} from "@laserdata/laser-sdk"
import { randomUUID } from "node:crypto"

export class LaserWorld {
  readonly endpoint =
    process.env["LASER_BDD_URL"] ??
    (process.env["LASER_BDD_ADDR"] !== undefined
      ? `iggy:iggy@${process.env["LASER_BDD_ADDR"]}`
      : "iggy:iggy@127.0.0.1:8090")
  readonly abort = new AbortController()
  laser?: Laser
  stream?: string
  conversation?: ConversationId
  assembled: readonly ContextMessage[] = []
  error: LaserError | undefined
  capabilities?: Capabilities
  published = 0
  bootstrapped = false
  lastTopic?: string
  understood?: boolean
  memory?: MemoryHandle
  readonly memoryIds = new Map<string, MemoryId>()
  query = new QueryEngine()
  queryResult?: QueryResult
  kv = new KvEngine()
  cas?: CasResult
  graph = new GraphEngine()
  bridgeHops: readonly string[] = []
  bridgeLoopRejected = false
  bridgeTaskState?: string
  reconstructedState: unknown
  aguiEventTypes: readonly string[] = []

  constructor(_options: IWorldOptions) {}

  async connect(): Promise<void> {
    this.stream = `bdd-ts-${randomUUID().slice(0, 12)}`
    this.laser = await Laser.connectWithStream(this.endpoint, this.stream)
  }

  newConversation(): void {
    this.conversation = ConversationId.new()
  }

  requireLaser(): Laser {
    if (this.laser === undefined) throw new Error("scenario has no Laser connection")
    return this.laser
  }

  requireConversation(): ConversationId {
    if (this.conversation === undefined) throw new Error("scenario has no conversation")
    return this.conversation
  }

  async capture(effect: () => Promise<unknown>): Promise<void> {
    try {
      await effect()
      this.error = undefined
    } catch (error) {
      this.error = error as LaserError
    }
  }

  async close(): Promise<void> {
    this.abort.abort("scenario cleanup")
    if (this.laser !== undefined) await this.laser.close()
  }
}

export interface QueryResult {
  readonly rows: readonly Readonly<Record<string, string>>[]
  readonly total: number
}

export class QueryEngine {
  private readonly indexes = new Map<string, Readonly<Record<string, string>>[]>()

  seed(index: string, rows: readonly Readonly<Record<string, string>>[]): void {
    this.indexes.set(index, rows.map((row) => ({ ...row })))
  }

  execute(
    index: string,
    options: {
      readonly filter?: readonly [string, number]
      readonly orderDesc?: string
      readonly limit?: number
      readonly groupBy?: string
    } = {}
  ): QueryResult {
    let rows = [...(this.indexes.get(index) ?? [])]
    if (options.filter !== undefined) {
      const [field, threshold] = options.filter
      rows = rows.filter((row) => Number(row[field]) > threshold)
    }
    if (options.orderDesc !== undefined) {
      const field = options.orderDesc
      rows.sort((left, right) => Number(right[field]) - Number(left[field]))
    }
    if (options.groupBy !== undefined) {
      const groups = new Map<string, number>()
      for (const row of rows) {
        const key = row[options.groupBy] ?? ""
        groups.set(key, (groups.get(key) ?? 0) + 1)
      }
      rows = [...groups].sort().map(([key, count]) => ({ [options.groupBy ?? ""]: key, count: String(count) }))
    }
    const total = rows.length
    return { rows: rows.slice(0, options.limit), total }
  }
}

interface KvEntry {
  readonly value: string
  readonly version: number
  readonly expiresAt?: number
}

export type CasResult =
  | { readonly kind: "committed"; readonly version: number }
  | { readonly kind: "conflict"; readonly current?: number }

export class KvEngine {
  private readonly entries = new Map<string, KvEntry>()

  set(key: string, value: string, now: number, expiresAt?: number): number {
    const version = (this.live(key, now)?.version ?? 0) + 1
    this.entries.set(key, { value, version, ...(expiresAt !== undefined ? { expiresAt } : {}) })
    return version
  }

  cas(key: string, value: string, expected: "absent" | number, now: number): CasResult {
    const live = this.live(key, now)
    if ((expected === "absent" && live !== undefined) || (expected !== "absent" && live?.version !== expected)) {
      return { kind: "conflict", ...(live !== undefined ? { current: live.version } : {}) }
    }
    return { kind: "committed", version: this.set(key, value, now) }
  }

  private live(key: string, now: number): KvEntry | undefined {
    const entry = this.entries.get(key)
    return entry?.expiresAt !== undefined && now >= entry.expiresAt ? undefined : entry
  }
}

interface Edge {
  readonly from: string
  readonly kind: string
  readonly to: string
  readonly validFrom?: number
  readonly source?: string
}

export class GraphEngine {
  readonly nodes = new Map<string, string | undefined>()
  readonly edges: Edge[] = []

  observe(from: string, kind: string, to: string, validFrom?: number, source?: string): void {
    if (!this.nodes.has(from)) this.nodes.set(from, source)
    if (!this.nodes.has(to)) this.nodes.set(to, source)
    const edge = this.edges.find((candidate) => candidate.from === from && candidate.kind === kind && candidate.to === to)
    if (edge === undefined) this.edges.push({ from, kind, to, ...(validFrom !== undefined ? { validFrom } : {}), ...(source !== undefined ? { source } : {}) })
    else if (source !== undefined) Object.assign(edge, { source })
  }

  traverse(start: string, direction: "out" | "incoming", kinds: readonly string[], asOf?: number): ReadonlySet<string> {
    let frontier = new Set([start])
    for (const kind of kinds) {
      const next = new Set<string>()
      for (const edge of this.edges) {
        if (edge.kind !== kind || (asOf !== undefined && (edge.validFrom ?? 0) > asOf)) continue
        if (direction === "out" && frontier.has(edge.from)) next.add(edge.to)
        if (direction === "incoming" && frontier.has(edge.to)) next.add(edge.from)
      }
      frontier = next
    }
    return frontier
  }
}

setWorldConstructor(LaserWorld)
