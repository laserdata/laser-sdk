import type { Laser } from "./client/laser.js"
import type { BytesLike } from "./client/bytes.js"
import type { GraphHandle } from "./managed/graph.js"
import {
  type Checkpoint,
  Chain,
  LastN,
  TokenBudget,
  type ContextMessage,
  type ContextPolicy
} from "./context.js"
import { ContextScope, type ScopedMemory } from "./context-scope.js"
import type { ReplayBound } from "./conversation-state.js"
import { InvalidError } from "./client/errors.js"
import { AgentTopic } from "./provenance/agent-topic.js"
import { ConversationId } from "./types/ids.js"

/** What a `Session.append` records. Each kind rides exactly one
 * conversation-level agent topic, so a session turn is an ordinary agent
 * message. The names are shared with the Rust and Python SDKs. */
export type SessionTurnKind =
  "instruction" | "response" | "model.response" | "tool.call" | "tool.result" | "human.input"

const TURN_TOPICS: Readonly<Record<SessionTurnKind, string>> = {
  instruction: AgentTopic.Commands,
  response: AgentTopic.Responses,
  "model.response": AgentTopic.LlmIo,
  "tool.call": AgentTopic.ToolCalls,
  "tool.result": AgentTopic.ToolResults,
  "human.input": AgentTopic.HumanInput
}

/** The topic a turn kind rides. */
export function sessionTurnTopic(kind: SessionTurnKind): string {
  return TURN_TOPICS[kind]
}

/** The kind that rides `topic`, or `undefined` outside `DEFAULT_SESSION_TOPICS`. */
export function sessionTurnKind(topic: string): SessionTurnKind | undefined {
  return (Object.keys(TURN_TOPICS) as SessionTurnKind[]).find((kind) => TURN_TOPICS[kind] === topic)
}

/** The topics a `Session` reads and writes: one per `SessionTurnKind`. */
export const DEFAULT_SESSION_TOPICS: readonly string[] = Object.values(TURN_TOPICS)

/** The memory namespace a `Session` uses by default. Conversation scoping
 * keeps one session's memory apart from another's. */
export const DEFAULT_SESSION_MEMORY_NAMESPACE = "agent.session"

export const DEFAULT_SESSION_CONTEXT_TURNS = 50
export const DEFAULT_SESSION_CONTEXT_TOKENS = 4000

/** One turn read back from a `Session`: the message plus the kind its topic implies. */
export interface SessionTurn {
  readonly kind: SessionTurnKind
  readonly message: ContextMessage
}

/** The payload of `turn` as UTF-8, lossy. */
export function sessionTurnText(turn: SessionTurn): string {
  return new TextDecoder().decode(turn.message.payload)
}

// How a `Sessions` factory lays its sessions out on the log.
interface SessionLayout {
  readonly stream?: string
  readonly topics: Readonly<Record<SessionTurnKind, string>>
  readonly memoryNamespace: string
  readonly contextTurns: number
  readonly contextTokens: number
}

/** How a `Sessions` factory lays its sessions out on the log. The defaults
 * are the conversation-level agent topics on the connection's default stream.
 * A fleet of agents that must not share a topic's partitions with another
 * fleet points its kinds at its own topics, or the whole factory at its own
 * stream. Every kind needs a topic of its own: a turn's kind is its topic.
 * The config is a value: each setter returns a new config, so a config shared
 * by a `Sessions` factory never changes under it. */
export class SessionConfig {
  private layout: SessionLayout = {
    topics: TURN_TOPICS,
    memoryNamespace: DEFAULT_SESSION_MEMORY_NAMESPACE,
    contextTurns: DEFAULT_SESSION_CONTEXT_TURNS,
    contextTokens: DEFAULT_SESSION_CONTEXT_TOKENS
  }

  /** Lay sessions out on `stream` instead of the connection's default stream. */
  stream(stream: string): SessionConfig {
    return this.with({ stream })
  }

  /** Ride `kind` on `topic`. A turn's kind is its topic, so every kind needs a
   * topic of its own: `laser.sessions` rejects a layout where two kinds share one. */
  topic(kind: SessionTurnKind, topic: string): SessionConfig {
    return this.with({ topics: { ...this.layout.topics, [kind]: topic } })
  }

  /** The memory namespace `Session.memory` opens. */
  memoryNamespace(namespace: string): SessionConfig {
    return this.with({ memoryNamespace: namespace })
  }

  /** The turn bound of `Session.context`. */
  contextTurns(turns: number): SessionConfig {
    return this.with({ contextTurns: turns })
  }

  /** The estimated token bound of `Session.context`. */
  contextTokens(tokens: number): SessionConfig {
    return this.with({ contextTokens: tokens })
  }

  /** The stream sessions ride, or `undefined` for the connection's default stream. */
  get streamName(): string | undefined {
    return this.layout.stream
  }

  /** The memory namespace `Session.memory` opens. */
  get memoryNamespaceName(): string {
    return this.layout.memoryNamespace
  }

  /** The turn bound of `Session.context`. */
  get contextTurnBound(): number {
    return this.layout.contextTurns
  }

  /** The estimated token bound of `Session.context`. */
  get contextTokenBound(): number {
    return this.layout.contextTokens
  }

  /** The topic `kind` rides. */
  topicFor(kind: SessionTurnKind): string {
    return this.layout.topics[kind]
  }

  /** The kind that rides `topic`, or `undefined` outside this layout. */
  kindFor(topic: string): SessionTurnKind | undefined {
    const topics = this.layout.topics
    return (Object.keys(topics) as SessionTurnKind[]).find((kind) => topics[kind] === topic)
  }

  /** Every topic this layout reads, in kind order. */
  topics(): readonly string[] {
    return Object.values(this.layout.topics)
  }

  private with(change: Partial<SessionLayout>): SessionConfig {
    const next = new SessionConfig()
    next.layout = { ...this.layout, ...change }
    return next
  }
}

/** The session factory. Build it with `laser.sessions()`. */
export class Sessions {
  readonly config: SessionConfig

  private constructor(
    private readonly laser: Laser,
    config: SessionConfig = new SessionConfig()
  ) {
    this.config = config
    validateLayout(this.config)
    const stream = this.config.streamName
    this.laser = stream === undefined ? laser : laser.withDefaultStream(stream)
  }

  /** @internal */
  static create(laser: Laser, config?: SessionConfig): Sessions {
    return new Sessions(laser, config)
  }

  /** The durable session named `id`. The conversation derives from `id`, so
   * the same id always reaches the same history, and nothing is created on the
   * server until a turn is appended. */
  create(id: string): Session {
    return this.open(ConversationId.derive(id))
  }

  /** A fresh anonymous session. Keep `session.conversation` to `open` it again. */
  start(): Session {
    return this.open(ConversationId.new())
  }

  /** The session over an existing conversation: one minted by `start`, carried
   * by an inbound message's provenance, or a sub-conversation. */
  open(conversation: ConversationId): Session {
    return Session.create(ContextScope.create(this.laser, conversation), this.config)
  }
}

/** One agent session over a conversation. Turns are agent messages on the
 * conversation-level topics, the context is a bounded assembly of them, memory
 * is the conversation's scoped memory, and a `Checkpoint` bounds point-in-time
 * and incremental replay. Build it with `laser.sessions()`. */
export class Session {
  private constructor(
    readonly scope: ContextScope,
    readonly config: SessionConfig = new SessionConfig()
  ) {}

  /** @internal */
  static create(scope: ContextScope, config: SessionConfig = new SessionConfig()): Session {
    return new Session(scope, config)
  }

  get conversation(): ConversationId {
    return this.scope.conversation
  }

  append(kind: SessionTurnKind, data: BytesLike): Promise<void> {
    return this.scope.append(this.config.topicFor(kind), data)
  }

  /** The model-ready context: the configured last turns across the session's
   * topics, trimmed to the configured estimated token bound. */
  context(): Promise<readonly SessionTurn[]> {
    return this.contextWith(
      new Chain([
        new LastN(this.config.contextTurnBound),
        new TokenBudget(this.config.contextTokenBound)
      ])
    )
  }

  async contextWith(policy: ContextPolicy): Promise<readonly SessionTurn[]> {
    return this.turnsOf(await this.scope.fetchWith(this.config.topics(), policy))
  }

  /** This session's memory in the configured namespace, scoped to the conversation. */
  memory(namespace: string = this.config.memoryNamespaceName): ScopedMemory {
    return this.scope.memory(namespace)
  }

  /** The knowledge graph `name`, the same graph `laser.graph` returns. */
  graph(name: string): GraphHandle {
    return this.scope.graph(name)
  }

  /** Where this session's topics end right now. Persist it and hand it to
   * `turnsAt`, `turnsSince`, `stateAt`, or `replay`. */
  checkpoint(): Promise<Checkpoint> {
    return this.scope.checkpoint(this.config.topics())
  }

  /** The turns up to `checkpoint`. */
  turnsAt(checkpoint: Checkpoint): Promise<readonly SessionTurn[]> {
    return this.turns({ kind: "at", checkpoint })
  }

  /** The turns appended after `checkpoint`. */
  turnsSince(checkpoint: Checkpoint): Promise<readonly SessionTurn[]> {
    return this.turns({ kind: "from-checkpoint", checkpoint })
  }

  /** Fold the turns up to `checkpoint`: state as it stood then. */
  async stateAt<State>(
    checkpoint: Checkpoint,
    initial: State,
    fold: (state: State, turn: SessionTurn) => State
  ): Promise<State> {
    return (await this.turnsAt(checkpoint)).reduce(fold, initial)
  }

  /** Fold the turns appended after `checkpoint`: bring state saved there up to date. */
  async replay<State>(
    checkpoint: Checkpoint,
    initial: State,
    fold: (state: State, turn: SessionTurn) => State
  ): Promise<State> {
    return (await this.turnsSince(checkpoint)).reduce(fold, initial)
  }

  private async turns(bound: ReplayBound): Promise<readonly SessionTurn[]> {
    return this.turnsOf(
      await this.scope.state(
        this.config.topics(),
        bound,
        [] as ContextMessage[],
        (acc, message) => {
          acc.push(message)
          return acc
        }
      )
    )
  }

  private turnsOf(messages: readonly ContextMessage[]): readonly SessionTurn[] {
    return messages.flatMap((message) => {
      const kind = this.config.kindFor(message.topic)
      return kind === undefined ? [] : [{ kind, message }]
    })
  }
}

function validateLayout(config: SessionConfig): void {
  const owners = new Map<string, SessionTurnKind>()
  for (const kind of Object.keys(TURN_TOPICS) as SessionTurnKind[]) {
    const topic = config.topicFor(kind)
    const other = owners.get(topic)
    if (other !== undefined) {
      throw new InvalidError(
        `session turn kinds ${other} and ${kind} share topic ${topic}, each kind needs its own topic`
      )
    }
    owners.set(topic, kind)
  }
}
