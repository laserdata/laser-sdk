import type { Laser } from "./client/laser.js"
import { decodeAgentMessage } from "./agent/reliable-consumer.js"
import { AgentTopic } from "./provenance/agent-topic.js"
import type { Provenance } from "./provenance/provenance.js"
import type { AgentId, ConversationId, MessageId } from "./types/ids.js"
import type { AgentEnvelope } from "./wire/agent.js"

const READ_BATCH = 1_000

// Ceiling on the records one assembly holds in memory per topic. A conversation
// topic grows without bound, and this read is reachable from bridge requests, so
// the window keeps the most recent records rather than materializing the whole
// history. Selection policies want the newest records, so the oldest are the
// ones dropped.
const MAX_CONTEXT_RECORDS = 10_000

export interface ContextMessage {
  readonly id: MessageId
  readonly provenance: Provenance
  readonly payload: Uint8Array
  readonly envelope?: AgentEnvelope
  readonly timestampMicros: bigint
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

  static empty(): Checkpoint {
    return new Checkpoint(new Map())
  }

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

  get topics(): readonly string[] {
    return [...this.perTopic.keys()]
  }

  isEmpty(): boolean {
    return this.perTopic.size === 0
  }

  toJSON(): Record<string, Record<string, string>> {
    return Object.fromEntries(
      [...this.perTopic].map(([topic, offsets]) => [
        topic,
        Object.fromEntries(
          [...offsets].map(([partition, offset]) => [String(partition), String(offset)])
        )
      ])
    )
  }

  static fromJSON(value: unknown): Checkpoint {
    const parsed: unknown = typeof value === "string" ? JSON.parse(value) : value
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
      throw new TypeError("a checkpoint is an object keyed by topic name")
    }
    const perTopic = new Map<string, ReadonlyMap<number, bigint>>()
    for (const [topic, offsets] of Object.entries(parsed as Record<string, unknown>)) {
      if (typeof offsets !== "object" || offsets === null || Array.isArray(offsets)) {
        throw new TypeError(`checkpoint topic ${topic} is not an object keyed by partition`)
      }
      perTopic.set(
        topic,
        new Map(
          Object.entries(offsets as Record<string, unknown>).map(([partition, offset]) => {
            if (
              !/^\d+$/.test(partition) ||
              (typeof offset !== "string" && typeof offset !== "number")
            ) {
              throw new TypeError(`checkpoint topic ${topic} has an invalid partition entry`)
            }
            return [Number(partition), BigInt(offset)]
          })
        )
      )
    }
    return new Checkpoint(perTopic)
  }
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
    this.agents = new Set([...agents].map((agent) => agent.asString()))
  }

  select(history: readonly ContextMessage[]): readonly ContextMessage[] {
    return history.filter((message) => {
      const agent = message.provenance.agent
      return agent !== undefined && this.agents.has(agent.asString())
    })
  }
}

export class ContextChain implements ContextPolicy {
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
    readonly maxTokens: number,
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

interface ContextAssemblerOptions {
  readonly conversation: ConversationId
  readonly acrossSubconversations: boolean
  readonly topics: readonly string[]
  readonly policy: ContextPolicy
  readonly fromOffsets: ReadonlyMap<number, bigint>
  readonly fromCheckpoint?: Checkpoint
  readonly toCheckpoint?: Checkpoint
}

export class ContextAssemblerBuilder {
  private acrossChildren = false
  private selectedTopics: readonly string[] = [AgentTopic.Commands, AgentTopic.Responses]
  private selectedPolicy: ContextPolicy = new LastN(50)
  private offsets: ReadonlyMap<number, bigint> = new Map()
  private resumeFrom: Checkpoint | undefined
  private stopAt: Checkpoint | undefined

  constructor(private readonly conversation: ConversationId) {}

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

  /** One offset map shared by every topic. `fromCheckpoint` takes precedence
   * for the topics it names. */
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
    return new ContextAssembler({
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
  static builder(conversation: ConversationId): ContextAssemblerBuilder {
    return new ContextAssemblerBuilder(conversation)
  }

  constructor(private readonly options: ContextAssemblerOptions) {}

  async assemble(laser: Laser): Promise<readonly ContextMessage[]> {
    const perTopic = await Promise.all(
      this.options.topics.map(async (topic, topicIndex) => {
        const collected: (ContextMessage & { readonly topicIndex: number })[] = []
        // A topic nobody has written yet has no history, same as in Rust.
        if ((await laser.topic(topic).partitionCount()) === undefined) return collected
        const cursor = await laser.topic(topic).replay({ batchSize: READ_BATCH })
        const resume = this.options.fromCheckpoint?.topicOffsets(topic)
        cursor.fromOffsets(resume ?? this.options.fromOffsets)
        const stop = this.options.toCheckpoint
        if (stop !== undefined) {
          // A checkpoint holds the next offset to write, so a partition it does
          // not name existed only after it and reads as empty.
          const ends = stop.topicOffsets(topic) ?? new Map<number, bigint>()
          cursor.until(
            new Map(
              [...cursor.offsets.keys()].map((partition) => [partition, ends.get(partition) ?? 0n])
            )
          )
        }
        for (;;) {
          const records = await cursor.poll()
          if (records.length === 0) break
          for (const record of records) {
            const decoded = decodeAgentMessage(record)
            if (decoded.kind !== "message" || !this.matches(decoded.message.provenance)) continue
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
            if (collected.length > MAX_CONTEXT_RECORDS) collected.shift()
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
    return this.options.policy.select(ordered)
  }

  private matches(provenance: Provenance): boolean {
    if (provenance.conversationId.equals(this.options.conversation)) return true
    return (
      this.options.acrossSubconversations &&
      (provenance.rootConversationId?.equals(this.options.conversation) === true ||
        provenance.parentConversationId?.equals(this.options.conversation) === true)
    )
  }
}
