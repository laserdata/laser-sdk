import type { Laser } from "./client/laser.js"
import { decodeAgentMessage } from "./agent/reliable-consumer.js"
import { AgentTopic } from "./provenance/agent-topic.js"
import type { Provenance } from "./provenance/provenance.js"
import type { AgentId, ConversationId, MessageId } from "./types/ids.js"
import type { AgentEnvelope } from "./wire/agent.js"

const READ_BATCH = 1_000

export interface ContextMessage {
  readonly id: MessageId
  readonly provenance: Provenance
  readonly payload: Uint8Array
  readonly envelope?: AgentEnvelope
  readonly timestampMicros: bigint
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
}

export class ContextAssemblerBuilder {
  private acrossChildren = false
  private selectedTopics: readonly string[] = [AgentTopic.Commands, AgentTopic.Responses]
  private selectedPolicy: ContextPolicy = new LastN(50)
  private offsets: ReadonlyMap<number, bigint> = new Map()

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

  fromOffsets(offsets: ReadonlyMap<number, bigint>): this {
    this.offsets = new Map(offsets)
    return this
  }

  build(): ContextAssembler {
    return new ContextAssembler({
      conversation: this.conversation,
      acrossSubconversations: this.acrossChildren,
      topics: this.selectedTopics,
      policy: this.selectedPolicy,
      fromOffsets: this.offsets
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
        const cursor = await laser.topic(topic).replay({ batchSize: READ_BATCH })
        cursor.fromOffsets(this.options.fromOffsets)
        const collected: (ContextMessage & { readonly topicIndex: number })[] = []
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
