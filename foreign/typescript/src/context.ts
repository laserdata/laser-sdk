import { ConfigError, InvalidError } from "./client/errors.js"
import type { Laser } from "./client/laser.js"
import { decodeAgentMessage } from "./agent/reliable-consumer.js"
import { AgentTopic } from "./provenance/agent-topic.js"
import type { Provenance } from "./provenance/provenance.js"
import type { AgentId, ConversationId, MessageId } from "./types/ids.js"
import type { AgentEnvelope } from "./wire/agent.js"

const READ_BATCH = 1_000

/**
 * The most raw records one context read examines in each partition, the same
 * window the Rust and Python SDKs use. An open read takes the newest records
 * before the current tail, and a point-in-time read the newest records before
 * the checkpoint, then keeps the ones of the conversation. Turns older than the
 * window on a busy shared partition are outside the read, so a long
 * conversation saves state with a checkpoint and replays from it.
 */
export const CONTEXT_READ_WINDOW = 10_000

const WINDOW = BigInt(CONTEXT_READ_WINDOW)
const U32_MAX = 0xffff_ffff
const U64_MAX = 0xffff_ffff_ffff_ffffn

// Node 22 writes and reads JSON integers beyond 2^53 exactly through these.
const EXACT_JSON = JSON as unknown as { rawJSON(text: string): unknown }

export interface ContextMessage {
  readonly id: MessageId
  readonly provenance: Provenance
  readonly payload: Uint8Array
  readonly envelope?: AgentEnvelope
  /** The name of the topic the message was read from. */
  readonly topic: string
}

/** A point in a conversation's log: the next offset each partition of each
 * topic will write, as captured by `ContextScope.checkpoint`. A client-side
 * bookmark keyed by topic name, never a record on the log. `toJSON` and
 * `Checkpoint.fromJSON` persist it. */
export class Checkpoint {
  private constructor(
    private readonly perTopic: ReadonlyMap<string, ReadonlyMap<number, bigint>>
  ) {}

  /** @internal */
  static async capture(laser: Laser, topics: readonly string[]): Promise<Checkpoint> {
    const entries = await Promise.all(
      topics.map(async (topic) => [topic, await laser.topic(topic).tailOffsets()] as const)
    )
    return new Checkpoint(new Map(entries))
  }

  /** The per-partition offsets for `topic`, or `undefined` when the checkpoint
   * was not taken over that topic. */
  topicOffsets(topic: string): ReadonlyMap<number, bigint> | undefined {
    return this.perTopic.get(topic)
  }

  isEmpty(): boolean {
    return this.perTopic.size === 0
  }

  /** The Rust and Python JSON shape, `{"per_topic": {topic: {partition: offset}}}`,
   * with every offset an exact JSON integer. */
  toJSON(): {
    readonly per_topic: Readonly<Record<string, Readonly<Record<string, unknown>>>>
  } {
    return {
      per_topic: Object.fromEntries(
        [...this.perTopic].map(([topic, offsets]) => [
          topic,
          Object.fromEntries(
            [...offsets].map(([partition, offset]) => [String(partition), jsonInteger(offset)])
          )
        ])
      )
    }
  }

  /** Reads `toJSON` output, as a JSON string or an already parsed value. */
  static fromJSON(value: unknown): Checkpoint {
    let parsed: unknown = value
    if (typeof value === "string") {
      try {
        parsed = JSON.parse(value, exactIntegers)
      } catch (cause) {
        throw new InvalidError("a checkpoint must be JSON", undefined, { cause })
      }
    }
    const perTopic = new Map<string, ReadonlyMap<number, bigint>>()
    for (const [topic, offsets] of Object.entries(
      jsonObject(jsonObject(parsed, "a checkpoint")["per_topic"], "checkpoint per_topic")
    )) {
      const partitions = new Map<number, bigint>()
      for (const [partition, offset] of Object.entries(
        jsonObject(offsets, `checkpoint topic ${topic}`)
      )) {
        const partitionId = Number(partition)
        if (!/^\d+$/.test(partition) || partitionId > U32_MAX) {
          throw new InvalidError(`checkpoint topic ${topic} has partition ${partition}, not a u32`)
        }
        partitions.set(partitionId, offsetOf(offset, topic))
      }
      perTopic.set(topic, partitions)
    }
    return new Checkpoint(perTopic)
  }
}

/** The current tail of `topics` on `laser`'s default stream, one entry per
 * topic. Fold up to it with the `at` replay bound or resume after it with
 * `from-checkpoint`. */
export function contextCheckpoint(laser: Laser, topics: readonly string[]): Promise<Checkpoint> {
  return Checkpoint.capture(laser, topics)
}

export interface ContextPolicy {
  select(history: readonly ContextMessage[]): readonly ContextMessage[]
}

export class LastN implements ContextPolicy {
  constructor(readonly count: number) {}

  select(history: readonly ContextMessage[]): readonly ContextMessage[] {
    return history.slice(Math.max(0, history.length - Math.max(0, this.count)))
  }
}

export class RoleFilter implements ContextPolicy {
  private readonly agents: ReadonlySet<string>

  constructor(agents: Iterable<AgentId>) {
    this.agents = new Set([...agents].map((agent) => agent.asStr()))
  }

  select(history: readonly ContextMessage[]): readonly ContextMessage[] {
    return history.filter((message) => {
      const agent = message.provenance.agent
      return agent !== undefined && this.agents.has(agent.asStr())
    })
  }
}

export class Chain implements ContextPolicy {
  constructor(readonly policies: readonly ContextPolicy[]) {}

  select(history: readonly ContextMessage[]): readonly ContextMessage[] {
    return this.policies.reduce<readonly ContextMessage[]>(
      (selected, policy) => policy.select(selected),
      history
    )
  }
}

export class TokenBudget implements ContextPolicy {
  constructor(
    private readonly maxTokens: number,
    private readonly estimate: (message: ContextMessage) => number = (message) =>
      Math.ceil(message.payload.byteLength / 4)
  ) {}

  select(history: readonly ContextMessage[]): readonly ContextMessage[] {
    const kept: ContextMessage[] = []
    let total = 0
    for (let index = history.length - 1; index >= 0; index -= 1) {
      const message = history[index]
      if (message === undefined) continue
      const cost = Math.max(0, this.estimate(message))
      if (kept.length > 0 && total + cost > this.maxTokens) break
      total += cost
      kept.push(message)
    }
    kept.reverse()
    return kept
  }
}

export interface ContextAssemblerOptions {
  readonly conversation: ConversationId
  readonly acrossSubconversations: boolean
  readonly topics: readonly string[]
  readonly policy: ContextPolicy
  readonly fromOffsets: ReadonlyMap<number, bigint>
  readonly fromCheckpoint?: Checkpoint
  readonly toCheckpoint?: Checkpoint
}

export class ContextAssemblerBuilder {
  private conversation: ConversationId | undefined
  private acrossChildren = false
  private selectedTopics: readonly string[] = [AgentTopic.Commands, AgentTopic.Responses]
  private selectedPolicy: ContextPolicy = new LastN(50)
  private offsets: ReadonlyMap<number, bigint> = new Map()
  private resumeFrom: Checkpoint | undefined
  private stopAt: Checkpoint | undefined

  /** The conversation whose history is assembled. Required. */
  conversationId(conversation: ConversationId): this {
    this.conversation = conversation
    return this
  }

  acrossSubconversations(value = true): this {
    this.acrossChildren = value
    return this
  }

  topics(topics: readonly string[]): this {
    this.selectedTopics = [...topics]
    return this
  }

  policy(policy: ContextPolicy): this {
    this.selectedPolicy = policy
    return this
  }

  /** One offset map shared by every topic. If `fromCheckpoint` is set, it
   * replaces this map for every topic. Missing checkpoint positions start at zero. */
  fromOffsets(offsets: ReadonlyMap<number, bigint>): this {
    this.offsets = new Map(offsets)
    return this
  }

  /** Resume after `checkpoint`, per topic and partition, and read to the tail. */
  fromCheckpoint(checkpoint: Checkpoint): this {
    this.resumeFrom = checkpoint
    return this
  }

  /** Stop at `checkpoint`, per topic and partition: a point-in-time read. */
  toCheckpoint(checkpoint: Checkpoint): this {
    this.stopAt = checkpoint
    return this
  }

  build(): ContextAssembler {
    if (this.conversation === undefined) {
      throw new ConfigError("ContextAssembler.builder().conversationId() is required")
    }
    return ContextAssembler.create({
      conversation: this.conversation,
      acrossSubconversations: this.acrossChildren,
      topics: this.selectedTopics,
      policy: this.selectedPolicy,
      fromOffsets: this.offsets,
      ...(this.resumeFrom === undefined ? {} : { fromCheckpoint: this.resumeFrom }),
      ...(this.stopAt === undefined ? {} : { toCheckpoint: this.stopAt })
    })
  }
}

export class ContextAssembler {
  static builder(): ContextAssemblerBuilder {
    return new ContextAssemblerBuilder()
  }

  private constructor(private readonly options: ContextAssemblerOptions) {}

  /** @internal */
  static create(options: ContextAssemblerOptions): ContextAssembler {
    return new ContextAssembler(options)
  }

  assemble(laser: Laser): Promise<readonly ContextMessage[]> {
    return readContext(this.options, laser, false)
  }
}

/** Every record of each partition's range instead of the newest
 * `CONTEXT_READ_WINDOW`, for state replay. An open range ends at the tail
 * seen when the replay starts. Not exported from the package. */
export function replayContext(
  options: ContextAssemblerOptions,
  laser: Laser
): Promise<readonly ContextMessage[]> {
  return readContext(options, laser, true)
}

async function readContext(
  options: ContextAssemblerOptions,
  laser: Laser,
  whole: boolean
): Promise<readonly ContextMessage[]> {
  const perTopic = await Promise.all(
    options.topics.map(async (topic, topicIndex) => {
      // The log timestamp only orders the merged read, like in Rust.
      const collected: (ContextMessage & {
        readonly timestampMicros: bigint
        readonly topicIndex: number
      })[] = []
      // A topic nobody has written yet has no history, same as in Rust.
      if ((await laser.topic(topic).partitionCount()) === undefined) return collected
      const handle = laser.topic(topic)
      const cursor = (await handle.replay()).batch(READ_BATCH)
      const partitions = [...cursor.offsets.keys()]
      const resume = options.fromCheckpoint
      const from = (partition: number): bigint =>
        resume === undefined
          ? (options.fromOffsets.get(partition) ?? 0n)
          : (resume.topicOffsets(topic)?.get(partition) ?? 0n)
      // A checkpoint holds the next offset to write, so a point-in-time read
      // ends before it, and a partition it does not name existed only after
      // it and reads as empty. An open read ends at the tail seen now.
      const stop = options.toCheckpoint
      const ends =
        stop === undefined
          ? await handle.tailOffsets()
          : (stop.topicOffsets(topic) ?? new Map<number, bigint>())
      const starts = new Map<number, bigint>()
      const bounds = new Map<number, bigint>()
      for (const partition of partitions) {
        const end = ends.get(partition) ?? 0n
        const windowStart = end > WINDOW ? end - WINDOW : 0n
        const start = from(partition)
        starts.set(partition, whole || start > windowStart ? start : windowStart)
        bounds.set(partition, end)
      }
      cursor.fromOffsets(starts).until(bounds)
      for (;;) {
        const records = await cursor.pollRecords()
        if (records.length === 0) break
        for (const record of records) {
          const decoded = decodeAgentMessage(record)
          if (decoded.kind !== "message" || !inConversation(options, decoded.message.provenance))
            continue
          collected.push({
            id: decoded.message.id,
            provenance: decoded.message.provenance,
            payload: decoded.message.payload,
            ...(decoded.message.envelope !== undefined
              ? { envelope: decoded.message.envelope }
              : {}),
            timestampMicros: record.timestampMicros ?? 0n,
            topic,
            topicIndex
          })
        }
      }
      return collected
    })
  )
  const ordered = perTopic.flat().sort((left, right) => {
    if (left.timestampMicros !== right.timestampMicros) {
      return left.timestampMicros < right.timestampMicros ? -1 : 1
    }
    if (left.topicIndex !== right.topicIndex) return left.topicIndex - right.topicIndex
    if (left.id.partitionId !== right.id.partitionId) {
      return left.id.partitionId - right.id.partitionId
    }
    return left.id.offset < right.id.offset ? -1 : left.id.offset > right.id.offset ? 1 : 0
  })
  return options.policy.select(ordered)
}

function inConversation(options: ContextAssemblerOptions, provenance: Provenance): boolean {
  if (provenance.conversationId.equals(options.conversation)) return true
  return (
    options.acrossSubconversations &&
    (provenance.rootConversationId?.equals(options.conversation) === true ||
      provenance.parentConversationId?.equals(options.conversation) === true)
  )
}

function jsonInteger(value: bigint): unknown {
  return value <= BigInt(Number.MAX_SAFE_INTEGER)
    ? Number(value)
    : EXACT_JSON.rawJSON(String(value))
}

function exactIntegers(
  _key: string,
  value: unknown,
  context?: { readonly source?: string }
): unknown {
  return typeof value === "number" &&
    context?.source !== undefined &&
    /^-?\d+$/.test(context.source)
    ? BigInt(context.source)
    : value
}

function jsonObject(value: unknown, what: string): Readonly<Record<string, unknown>> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new InvalidError(`${what} must be a JSON object`)
  }
  return value as Readonly<Record<string, unknown>>
}

function offsetOf(value: unknown, topic: string): bigint {
  const offset =
    typeof value === "bigint"
      ? value
      : typeof value === "number" && Number.isSafeInteger(value)
        ? BigInt(value)
        : undefined
  if (offset === undefined || offset < 0n || offset > U64_MAX) {
    throw new InvalidError(`checkpoint topic ${topic} has an offset that is not a u64`)
  }
  return offset
}
