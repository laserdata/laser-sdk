import type { Laser } from "./client/laser.js"
import type { BytesLike } from "./client/bytes.js"
import type { GraphHandle } from "./managed/graph.js"
import {
  type Checkpoint,
  ContextChain,
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

/** How a `Sessions` factory lays its sessions out on the log. The defaults
 * are the conversation-level agent topics on the connection's default stream.
 * A fleet of agents that must not share a topic's partitions with another
 * fleet points its kinds at its own topics, or the whole factory at its own
 * stream. Every kind needs a topic of its own: a turn's kind is its topic. */
export interface SessionOptions {
  readonly stream?: string
  readonly topics?: Partial<Readonly<Record<SessionTurnKind, string>>>
  readonly memoryNamespace?: string
  readonly contextTurns?: number
  readonly contextTokens?: number
}

export class SessionConfig {
  readonly stream: string | undefined
  readonly topics: Readonly<Record<SessionTurnKind, string>>
  readonly memoryNamespace: string
  readonly contextTurns: number
  readonly contextTokens: number

  constructor(options: SessionOptions = {}) {
    this.stream = options.stream
    this.topics = { ...TURN_TOPICS, ...options.topics }
    this.memoryNamespace = options.memoryNamespace ?? DEFAULT_SESSION_MEMORY_NAMESPACE
    this.contextTurns = options.contextTurns ?? DEFAULT_SESSION_CONTEXT_TURNS
    this.contextTokens = options.contextTokens ?? DEFAULT_SESSION_CONTEXT_TOKENS
    const owners = new Map<string, SessionTurnKind>()
    for (const kind of Object.keys(this.topics) as SessionTurnKind[]) {
      const topic = this.topics[kind]
      const other = owners.get(topic)
      if (other !== undefined) {
        throw new InvalidError(
          `session turn kinds ${other} and ${kind} share topic ${topic}, each kind needs its own topic`
        )
      }
      owners.set(topic, kind)
    }
  }

  /** The topic `kind` rides. */
  topicFor(kind: SessionTurnKind): string {
    return this.topics[kind]
  }

  /** The kind that rides `topic`, or `undefined` outside this layout. */
  kindFor(topic: string): SessionTurnKind | undefined {
    return (Object.keys(this.topics) as SessionTurnKind[]).find(
      (kind) => this.topics[kind] === topic
    )
  }

  /** Every topic this layout reads, in kind order. */
  get topicList(): readonly string[] {
    return Object.values(this.topics)
  }
}

/** The session factory. Build it with `laser.sessions()`. */
export class Sessions {
  readonly config: SessionConfig

  constructor(
    private readonly laser: Laser,
    options: SessionOptions = {}
  ) {
    this.config = new SessionConfig(options)
    this.laser =
      this.config.stream === undefined ? laser : laser.withDefaultStream(this.config.stream)
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
    return new Session(new ContextScope(this.laser, conversation), this.config)
  }
}

/** One agent session over a conversation. Turns are agent messages on the
 * conversation-level topics, the context is a bounded assembly of them, memory
 * is the conversation's scoped memory, and a `Checkpoint` bounds point-in-time
 * and incremental replay. Build it with `laser.sessions()`. */
export class Session {
  constructor(
    readonly scope: ContextScope,
    readonly config: SessionConfig = new SessionConfig()
  ) {}

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
      new ContextChain([
        new LastN(this.config.contextTurns),
        new TokenBudget(this.config.contextTokens)
      ])
    )
  }

  async contextWith(policy: ContextPolicy): Promise<readonly SessionTurn[]> {
    return this.turnsOf(await this.scope.fetchWith(this.config.topicList, policy))
  }

  /** This session's memory in the configured namespace, scoped to the conversation. */
  memory(namespace: string = this.config.memoryNamespace): ScopedMemory {
    return this.scope.memory(namespace)
  }

  /** The knowledge graph `name`, the same graph `laser.graph` returns. */
  graph(name: string): GraphHandle {
    return this.scope.graph(name)
  }

  /** Where this session's topics end right now. Persist it and hand it to
   * `turnsAt`, `turnsSince`, `stateAt`, or `replay`. */
  checkpoint(): Promise<Checkpoint> {
    return this.scope.checkpoint(this.config.topicList)
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
        this.config.topicList,
        bound,
        [] as ContextMessage[],
        (acc, message) => [...acc, message]
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
