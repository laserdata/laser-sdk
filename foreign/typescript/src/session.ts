import { millisToMicros } from "./client/duration.js"
import type { Laser } from "./client/laser.js"
import { isUnavailable } from "./client/error-classify.js"
import { publishOptions } from "./client/publish-options.js"
import { warn } from "./runtime/warn.js"
import { type BytesLike, ownedBytes } from "./client/bytes.js"
import {
  InvalidError,
  LaserError,
  NoStreamError,
  SessionError,
  UnsupportedError
} from "./client/errors.js"
import {
  INTERNAL_ENSURE_RETAINED,
  INTERNAL_LAYOUTS,
  INTERNAL_PUBLISH_CONTROL,
  INTERNAL_SESSION_LEASES,
  INTERNAL_TRANSPORT
} from "./client/internals.js"
import type { Agdx, AgdxReceipt, AgdxSend } from "./agent/agdx.js"
import type { SessionLease } from "./agent/lease.js"
import type { GraphHandle } from "./managed/graph.js"
import type { Kv } from "./managed/kv.js"
import type { SigningKey } from "./signing.js"
import {
  type ControlBook,
  type ControlState,
  type PendingControl,
  controlRequest,
  foldControl
} from "./agent/control.js"
import {
  BUDGET_END_REASON,
  type BudgetBreach,
  breachErrorBody,
  breachOfInfo,
  laneBreach,
  unknownToIndex
} from "./agent/budget.js"
import { type ParkedRecords, laneParticipants, readParked } from "./agent/lane-scan.js"
import type { MemoryItem } from "./memory/types.js"
import {
  type Checkpoint,
  Chain,
  LastN,
  TokenBudget,
  type ContextMessage,
  type ContextPolicy,
  contextPolicyName,
  contextPolicyVersion
} from "./context.js"
import { ContextScope, type ScopedMemory } from "./context-scope.js"
import type { ReplayBound } from "./conversation-state.js"
import { mintUlidValue } from "./runtime/ulid.js"
import {
  AssembledContext,
  ModelCall,
  type ModelRequest,
  type ModelResponse,
  SessionState,
  type StateCursor,
  ToolCall,
  defaultRedact
} from "./session-ops.js"
import { AgentTopic } from "./provenance/agent-topic.js"
import { AgentId, ConversationId, MintUlid } from "./types/ids.js"
import {
  type AgentEnvelope,
  type AgentErrorBody,
  type Budget,
  type Fragment,
  METADATA_DURATION_MICROS,
  METADATA_PROVIDER_NAME,
  METADATA_REQUEST_MODEL,
  METADATA_RESPONSE_MODEL,
  METADATA_SUBMITTED,
  OPERATION_SESSION,
  type SdkInfo,
  type SessionEnd,
  type SessionStart,
  TaskStateName,
  type ContextCompaction,
  encodeContextCompaction,
  encodeContextManifest,
  encodeContextRetrieval,
  encodeSessionEnd,
  encodeSessionPauseRequestJson,
  encodeSessionStart,
  encodeSessionTransition,
  estimateTokens,
  taskStateFromCode,
  type AgentId as WireAgentId
} from "./wire/agent.js"
import { encodeNamed } from "./wire/cbor.js"
import { ContentType } from "./wire/content.js"
import {
  type DisplayType,
  OPERATION_CONTEXT_ASSEMBLED,
  OPERATION_CONTEXT_COMPACTED,
  OPERATION_CONTEXT_RETRIEVED,
  OPERATION_EXECUTE_TOOL,
  OPERATION_INVOKE_AGENT,
  OPERATION_SESSION_CANCEL,
  OPERATION_SESSION_PAUSE,
  OPERATION_SESSION_RESUME,
  displayType
} from "./wire/dispatch.js"
import type { ProducerInfo, SourceRef } from "./wire/graph.js"
import {
  ConversationId as WireConversationId,
  CorrelationId,
  RecordId,
  type SessionRef,
  crockfordEncode
} from "./wire/ids.js"
import { AGENT_CONTROL, AGENT_SESSIONS } from "./wire/topics.js"
import { SDK_VERSION } from "./version.js"
import {
  SessionEventsRequest,
  SessionListRequest,
  SessionWatch,
  readSession,
  unexpected
} from "./session-reads.js"
import {
  SessionChangesCommand,
  SessionGetCommand,
  SessionLinksCommand,
  SessionSourcesCommand,
  SessionStateCommand
} from "./wire/commands.js"
import type {
  LinkSurface,
  SessionChangesView,
  SessionInfo,
  SessionLinksView,
  SessionSourcesView,
  SessionStateView
} from "./wire/session.js"

// How often bootstrap checks that the session source registration is visible.
const REGISTRATION_POLL_INTERVAL_MS = 100

/** The idle timeout a session start records unless it sets its own, in milliseconds. */
export const DEFAULT_SESSION_IDLE_TIMEOUT_MS = 300_000

/** The heartbeat interval of a process that holds session leases, in milliseconds. */
export const DEFAULT_SESSION_HEARTBEAT_MS = 60_000

/** The memory namespace a `Session` uses by default. Conversation scoping
 * keeps one session's memory apart from another's. */
export const DEFAULT_SESSION_MEMORY_NAMESPACE = "agent.session"

export const DEFAULT_SESSION_CONTEXT_TURNS = 50
export const DEFAULT_SESSION_CONTEXT_TOKENS = 4000

const MIN_LEASE_INTERVAL_MS = 10
const SHARED_LAYOUT: SessionLayout = { kind: "shared" }
const U64_MAX = 0xffff_ffff_ffff_ffffn
const U32_MAX = 0xffff_ffffn

/** How long `agent.sessions` keeps records. Bootstrap needs one, because the
 * session topic holds every session's lane and there is no safe default. The
 * expiry is in milliseconds, and leaving it out never expires. The size bound
 * is in bytes, and leaving it out keeps the server default. A policy that
 * never expires and has no size bound is refused. */
export class TopicRetention {
  private constructor(
    private readonly expiryMs: number | undefined,
    private readonly maxSizeBytes: bigint | undefined
  ) {}

  /** A retention policy, refused when it would keep records forever with no
   * size bound. */
  static new(expiryMs?: number, maxSizeBytes?: bigint): TopicRetention {
    if (expiryMs !== undefined) duration(expiryMs, "retention expiry")
    if (maxSizeBytes !== undefined && (maxSizeBytes <= 0n || maxSizeBytes > U64_MAX)) {
      throw new InvalidError("a retention size bound must be a positive unsigned 64-bit integer")
    }
    if (expiryMs === undefined && maxSizeBytes === undefined) {
      throw new InvalidError("agent.sessions retention must set an expiry or a size bound")
    }
    return new TopicRetention(expiryMs, maxSizeBytes)
  }

  /** Expire records after `ageMs` milliseconds, with the server's default size
   * bound. A zero age is raised to one microsecond. */
  static expireAfter(ageMs: number): TopicRetention {
    return new TopicRetention(Math.max(duration(ageMs, "retention age"), 0.001), undefined)
  }

  /** The message expiry in milliseconds, or `undefined` to never expire. */
  expiry(): number | undefined {
    return this.expiryMs
  }

  /** The topic size bound in bytes, or `undefined` for the server default. */
  maxSize(): bigint | undefined {
    return this.maxSizeBytes
  }
}

/** Where a stream's agents put their work. Lifecycle and state always ride the
 * session's lane on `agent.sessions` in every layout. */
export type SessionLayout =
  /** One `agent.sessions` topic keyed by session. The default. */
  | { readonly kind: "shared" }
  /** Each declared agent reads its own topic, keyed by session. Commands,
   * replies, and chunks addressed to a declared agent on `agent.sessions`
   * land on its topic, everything else stays on the lane. */
  | { readonly kind: "perAgentTopic"; readonly topics: ReadonlyMap<string, string> }
  /** Work rides `agent.sessions` on a declared partition per agent. */
  | { readonly kind: "perAgentPartition"; readonly partitions: ReadonlyMap<string, number> }
  /** Everything on one partition. */
  | { readonly kind: "singlePartition" }

interface SessionSettings {
  readonly stream?: string
  // Unset until a layout is chosen, so only an explicit layout is declared.
  readonly layout?: SessionLayout
  readonly idleTimeoutMs: number
  readonly heartbeatMs: number
  readonly registerSource: boolean
  readonly failOnDeadLetter: boolean
  readonly memoryNamespace: string
  readonly contextTurns: number
  readonly contextTokens: number
  readonly sdk: SdkInfo
}

/** The layout and defaults of `laser.sessions`. The config is a value: each
 * setter returns a new config, so a config shared by a `Sessions` factory
 * never changes under it. */
export class SessionConfig {
  private settings: SessionSettings = {
    idleTimeoutMs: DEFAULT_SESSION_IDLE_TIMEOUT_MS,
    heartbeatMs: DEFAULT_SESSION_HEARTBEAT_MS,
    registerSource: true,
    failOnDeadLetter: false,
    memoryNamespace: DEFAULT_SESSION_MEMORY_NAMESPACE,
    contextTurns: DEFAULT_SESSION_CONTEXT_TURNS,
    contextTokens: DEFAULT_SESSION_CONTEXT_TOKENS,
    sdk: { language: "typescript", version: SDK_VERSION }
  }

  /** Lay sessions out on `stream` instead of the connection's default stream. */
  stream(stream: string): SessionConfig {
    return this.with({ stream })
  }

  /** Where agents put their work. */
  layout(layout: SessionLayout): SessionConfig {
    return this.with({ layout })
  }

  /** The idle timeout a session start records unless it sets its own. */
  idleTimeout(timeoutMs: number): SessionConfig {
    return this.with({ idleTimeoutMs: duration(timeoutMs, "idle timeout") })
  }

  /** How often a process with session leases publishes its heartbeat. */
  heartbeat(intervalMs: number): SessionConfig {
    return this.with({ heartbeatMs: duration(intervalMs, "heartbeat interval") })
  }

  /** Whether bootstrap registers the stream as a session source on a
   * deployment that serves sessions. On by default. */
  registerSource(register: boolean): SessionConfig {
    return this.with({ registerSource: register })
  }

  /** Fail a session when one of its records is dead-lettered. Off by default:
   * a dead letter counts as an error and never ends a session. */
  failOnDeadLetter(fail: boolean): SessionConfig {
    return this.with({ failOnDeadLetter: fail })
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

  /** The SDK a session start names. */
  sdk(language: string, version: string): SessionConfig {
    return this.with({ sdk: { language, version } })
  }

  /** The stream sessions ride, or `undefined` for the connection's default stream. */
  get streamName(): string | undefined {
    return this.settings.stream
  }

  /** The layout. */
  get layoutKind(): SessionLayout {
    return this.settings.layout ?? SHARED_LAYOUT
  }

  /** @internal */
  get declaredLayout(): SessionLayout | undefined {
    return this.settings.layout
  }

  /** The default idle timeout, in milliseconds. */
  get idleTimeoutValue(): number {
    return this.settings.idleTimeoutMs
  }

  /** The heartbeat interval, in milliseconds. */
  get heartbeatValue(): number {
    return this.settings.heartbeatMs
  }

  /** Whether bootstrap registers the session source. */
  get registersSource(): boolean {
    return this.settings.registerSource
  }

  /** Whether a dead letter fails its session. */
  get failsOnDeadLetter(): boolean {
    return this.settings.failOnDeadLetter
  }

  /** The memory namespace `Session.memory` opens. */
  get memoryNamespaceName(): string {
    return this.settings.memoryNamespace
  }

  /** The turn bound of `Session.context`. */
  get contextTurnBound(): number {
    return this.settings.contextTurns
  }

  /** The estimated token bound of `Session.context`. */
  get contextTokenBound(): number {
    return this.settings.contextTokens
  }

  /** The SDK a session start names. */
  get sdkInfo(): SdkInfo {
    return this.settings.sdk
  }

  private with(change: Partial<SessionSettings>): SessionConfig {
    const next = new SessionConfig()
    next.settings = { ...this.settings, ...change }
    return next
  }
}

/** The session id `label` derives to in `stream` under `namespace`. The same
 * three always reach the same session, so two applications that share a
 * stream share a session on purpose only when they pick the same namespace and
 * label. */
export function deriveSessionId(stream: string, namespace: string, label: string): ConversationId {
  return ConversationId.derive(`${stream}\u001f${namespace}\u001f${label}`)
}

/** What `Sessions.bootstrap` set up. */
export interface SessionBootstrap {
  /** Whether the stream is now registered as a session source. */
  readonly registered: boolean
}

interface LaneIdentity {
  readonly streamId: number
  readonly streamGeneration: bigint
  readonly topicId: number
  readonly topicGeneration: bigint
  readonly partitions: number
}

class LaneGuard {
  private identity: LaneIdentity | undefined
  private registered = false

  async observe(laser: Laser): Promise<LaneIdentity> {
    const transport = laser[INTERNAL_TRANSPORT]()
    if (transport.findSnapshotStream === undefined || transport.findSnapshotTopic === undefined)
      throw new UnsupportedError("the session lane identity is unavailable")
    const stream = requireStream(laser)
    const details = await transport.findSnapshotStream(stream)
    const topic = await transport.findSnapshotTopic(stream, AgentTopic.Sessions)
    if (details === undefined) throw staleLane("the session stream does not exist")
    if (topic === undefined) throw staleLane("agent.sessions does not exist")
    const current: LaneIdentity = {
      streamId: details.id,
      streamGeneration: details.createdAtMicros,
      topicId: topic.id,
      topicGeneration: topic.createdAtMicros,
      partitions: topic.partitions
    }
    if (current.partitions === 0) throw staleLane("agent.sessions has no partitions")
    const pinned = this.identity
    if (
      pinned !== undefined &&
      (pinned.streamId !== current.streamId ||
        pinned.streamGeneration !== current.streamGeneration ||
        pinned.topicId !== current.topicId ||
        pinned.topicGeneration !== current.topicGeneration ||
        pinned.partitions !== current.partitions)
    )
      throw staleLane("the session lane identity or partition count changed")
    this.identity ??= current
    return this.identity
  }

  async check(laser: Laser, conversation: ConversationId): Promise<LaneIdentity> {
    const identity = await this.observe(laser)
    if (!this.registered && (await laser.capabilities()).sessions) {
      try {
        const lane = await laser.sessions().registeredLane(conversation)
        if (
          lane?.[0] !== identity.topicId ||
          lane[1] !== identity.topicGeneration ||
          lane[2] !== identity.partitions
        )
          throw staleLane("the registered session lane does not match the current source")
        this.registered = true
      } catch (error) {
        if (!(error instanceof SessionError && error.detail.kind === "notRegistered")) throw error
      }
    }
    return identity
  }
}

function staleLane(message: string): SessionError {
  return new SessionError(`stale session generation: ${message}`, { kind: "stale", message })
}

/** The session factory. Build it with `laser.sessions()`. */
export class Sessions {
  readonly config: SessionConfig
  private readonly laser: Laser
  private readonly laneGuard = new LaneGuard()

  private constructor(laser: Laser, config: SessionConfig = new SessionConfig()) {
    this.config = config
    const stream = this.config.streamName
    this.laser = stream === undefined ? laser : laser.withDefaultStream(stream)
    // A declared layout holds for every later send on the stream through this
    // connection, so agents and lenses route the same way.
    const layout = this.config.declaredLayout
    const resolved = this.laser.defaultStream
    if (layout !== undefined && resolved !== undefined)
      this.laser[INTERNAL_LAYOUTS]().set(resolved, layout)
  }

  /** @internal */
  static create(laser: Laser, config?: SessionConfig): Sessions {
    return new Sessions(laser, config)
  }

  /** A session named `label`. Its id derives from the stream, the namespace,
   * and the label, so the same label reaches the same session. */
  create(label: string): SessionBuilder {
    return SessionBuilder.create(this, this.laser, label)
  }

  /** A fresh session with a new id. */
  start(): SessionBuilder {
    return SessionBuilder.create(this, this.laser, undefined)
  }

  /** The session over an existing `conversation`, as a lens: no IO, no lease,
   * and no author until `Session.asAgent` names one. */
  open(conversation: ConversationId): Session {
    return Session.lens(this.laser, this.config, conversation).withLaneGuard(this.laneGuard)
  }

  /** Create the agent topics with `laser.bootstrap`, then, when the deployment
   * serves sessions and `SessionConfig.registerSource` is on, register the
   * stream as a session source. A refused registration is reported as
   * `registered: false`, never an error, because an account without session
   * administration rights relies on provisioning to register the stream. */
  async bootstrap(partitions: number, retention: TopicRetention): Promise<SessionBootstrap> {
    const layout = this.config.layoutKind
    const count = layout.kind === "singlePartition" ? 1 : partitions
    const declared = layout.kind === "perAgentTopic" ? declaredTopics(layout.topics) : []
    await this.laser.bootstrap(count, retention)
    // Each declared agent topic carries a share of the session's work, so it
    // takes the lane's partition count and retention.
    for (const topic of declared) {
      await this.laser[INTERNAL_ENSURE_RETAINED](topic, count, retention)
    }
    await this.laneGuard.observe(this.laser)
    if (!this.config.registersSource || !(await this.laser.capabilities()).sessions) {
      return { registered: false }
    }
    try {
      await this.laser[INTERNAL_PUBLISH_CONTROL]({
        kind: "registerSessionSource",
        stream: this.stream(),
        topics: { kind: "all" }
      })
    } catch (error) {
      warn(`session source registration refused: ${String(error)}`)
      return { registered: false }
    }
    return { registered: await this.registrationVisible() }
  }

  // Wait until the deployment serves reads for the registered stream, up to
  // the publish timeout. Registration applies after its control record folds,
  // so reads and linked writes right after bootstrap would otherwise race it.
  private async registrationVisible(): Promise<boolean> {
    const transport = this.laser[INTERNAL_TRANSPORT]()
    const deadline = Date.now() + (transport.publishTimeoutMs?.() ?? publishOptions().timeoutMs)
    let lastUnavailable: string | undefined
    for (;;) {
      try {
        await this.changes(0n, 1)
        return true
      } catch (error) {
        if (isUnavailable(error)) {
          lastUnavailable = String(error)
        } else if (!(
          error instanceof SessionError &&
          (error.detail.kind === "notRegistered" || error.detail.kind === "stale")
        )) {
          warn(`session source registration not confirmed: ${String(error)}`)
          return false
        }
      }
      if (Date.now() >= deadline) {
        warn(
          `session source registration not visible before the publish timeout${lastUnavailable === undefined ? "" : `: ${lastUnavailable}`}`
        )
        return false
      }
      await new Promise((resolve) => setTimeout(resolve, REGISTRATION_POLL_INTERVAL_MS))
    }
  }

  /** Whether the deployment indexes sessions.
   * @internal */
  async indexesSessions(): Promise<boolean> {
    return (await this.laser.capabilities()).sessions
  }

  /** The stream the sessions ride. */
  stream(): string {
    return requireStream(this.laser)
  }

  /** Hand a new session to `agent` with `input` as its first command. */
  submit(agent: AgentId, input: BytesLike): SubmitBuilder {
    return SubmitBuilder.create(this, this.laser, agent, ownedBytes(input))
  }

  /** One session's summary from the deployment's session index. */
  async get(id: ConversationId): Promise<SessionInfo> {
    const outcome = await readSession(this.laser, SessionGetCommand, {
      stream: this.stream(),
      id: wireConversation(id)
    })
    if (outcome.kind !== "info") throw unexpected("get")
    return outcome.info
  }

  /** A page of this stream's sessions, newest first. Chain the filters and
   * finish with `fetch`. */
  list(): SessionListRequest {
    return SessionListRequest.create(this.laser, this.stream())
  }

  /** A page of one session's events in broker time order. Chain the paging
   * and finish with `fetch`. */
  events(id: ConversationId): SessionEventsRequest {
    return SessionEventsRequest.create(this.laser, this.stream(), wireConversation(id))
  }

  /** One session's folded state document with up to `historyLimit` history
   * rows. Zero leaves the history size to the server. */
  async state(id: ConversationId, historyLimit: number): Promise<SessionStateView> {
    const outcome = await readSession(this.laser, SessionStateCommand, {
      stream: this.stream(),
      id: wireConversation(id),
      historyLimit
    })
    if (outcome.kind !== "state") throw unexpected("state")
    return outcome.state
  }

  /** The resources one session wrote, recalled, or touched, narrowed to
   * `surface` when given. */
  async links(id: ConversationId, surface?: LinkSurface): Promise<SessionLinksView> {
    const outcome = await readSession(this.laser, SessionLinksCommand, {
      stream: this.stream(),
      id: wireConversation(id),
      ...(surface !== undefined ? { surface } : {})
    })
    if (outcome.kind !== "links") throw unexpected("links")
    return outcome.links
  }

  /** The source partitions that hold one session's records. */
  async sources(id: ConversationId): Promise<SessionSourcesView> {
    const outcome = await readSession(this.laser, SessionSourcesCommand, {
      stream: this.stream(),
      id: wireConversation(id)
    })
    if (outcome.kind !== "sources") throw unexpected("sources")
    return outcome.sources
  }

  /** @internal */
  async registeredLane(id: ConversationId): Promise<readonly [number, bigint, number] | undefined> {
    const outcome = await readSession(this.laser, SessionSourcesCommand, {
      stream: this.stream(),
      id: wireConversation(id),
      laneOnly: true
    })
    if (outcome.kind !== "sources") throw unexpected("sources")
    return outcome.sources.lane
  }

  /** The change rows of this stream after `after`, up to `limit`. Zero leaves
   * the page size to the server. */
  async changes(after: bigint, limit: number): Promise<SessionChangesView> {
    const outcome = await readSession(this.laser, SessionChangesCommand, {
      stream: this.stream(),
      after,
      limit
    })
    if (outcome.kind !== "changes") throw unexpected("changes")
    return outcome.changes
  }

  /** Follow this stream's session changes from now, polling every
   * `pollEveryMs` and coalescing the changed session ids of each poll. The
   * watch starts at the change rows retained when this call resolves, so a
   * change made after it is never missed. */
  watch(pollEveryMs: number): Promise<SessionWatch> {
    return SessionWatch.create(this.laser, this.stream(), pollEveryMs)
  }

  /** Operator control of the session `id` in `stream`. */
  control(stream: string, id: ConversationId): SessionControl {
    return SessionControl.create(this.laser.withDefaultStream(stream), id)
  }
}

/** What a submission wrote: the session and the command's correlation. */
export interface Submitted {
  readonly session: ConversationId
  readonly correlation: CorrelationId
}

/** A session handed to an agent: written as submitted on the lane, with a
 * command in the agent's inbox. The agent marks it working when it picks the
 * command up. */
export class SubmitBuilder {
  private submitter: AgentId | undefined
  private operationName: string = OPERATION_INVOKE_AGENT
  private labelValue: string | undefined
  private namespaceName: string | undefined
  private budgetValue: Budget | undefined
  private readonly tags: string[] = []

  private constructor(
    private readonly sessions: Sessions,
    private readonly laser: Laser,
    private readonly agent: AgentId,
    private readonly input: Uint8Array
  ) {}

  /** @internal */
  static create(
    sessions: Sessions,
    laser: Laser,
    agent: AgentId,
    input: Uint8Array
  ): SubmitBuilder {
    return new SubmitBuilder(sessions, laser, agent, input)
  }

  /** The agent that submits the session. Required. */
  from(submitter: AgentId): this {
    this.submitter = submitter
    return this
  }

  /** The command operation, `invoke_agent` unless set. */
  operation(operation: string): this {
    this.operationName = operation
    return this
  }

  /** Name the session, deriving its id from the stream, namespace, and label. */
  label(label: string): this {
    this.labelValue = label
    return this
  }

  /** The namespace a labeled session id derives under. */
  namespace(namespace: string): this {
    this.namespaceName = namespace
    return this
  }

  /** The token and cost ceiling a reader compares the session's usage with. */
  budget(budget: Budget): this {
    this.budgetValue = budget
    return this
  }

  /** One searchable tag. */
  tag(tag: string): this {
    this.tags.push(tag)
    return this
  }

  /** Write the submitted status and the command. */
  async send(): Promise<Submitted> {
    const submitter = this.submitter
    if (submitter === undefined) throw new InvalidError("a submission needs `.from(submitter)`")
    const stream = this.sessions.stream()
    const session =
      this.labelValue === undefined
        ? ConversationId.new()
        : deriveSessionId(stream, this.namespaceName ?? "", this.labelValue)
    const config = this.sessions.config
    const start: SessionStart = {
      ...(this.labelValue !== undefined ? { label: this.labelValue } : {}),
      ...(this.namespaceName !== undefined ? { namespace: this.namespaceName } : {}),
      agent: this.agent.wireId(),
      sdk: config.sdkInfo,
      idleTimeoutMicros: micros(config.idleTimeoutValue),
      ...(this.budgetValue !== undefined ? { budget: this.budgetValue } : {}),
      tags: [...this.tags]
    }
    const body = encodeNamed(encodeSessionStart(start))
    const lane = this.sessions.open(session).asAgent(submitter).lane()
    await lane
      .status(OPERATION_SESSION)
      .withTaskState(taskStateFromCode(TaskStateName.Submitted))
      .body(body)
      .contentType(ContentType.Cbor)
      .send()
    const correlation = MintUlid.mint(CorrelationId)
    await lane
      .command(correlation, this.input)
      .withOperation(this.operationName)
      .withTarget(this.agent)
      .withMetadata(METADATA_SUBMITTED, { kind: "bool", value: true })
      .send()
    return { session, correlation }
  }
}

/** Operator control of one session, sent on `agent.control`. Only accounts
 * with send permission on that topic can use it. */
export class SessionControl {
  private constructor(
    private readonly laser: Laser,
    private readonly session: ConversationId,
    private readonly operator?: AgentId,
    private readonly key?: SigningKey,
    private readonly named?: readonly WireAgentId[]
  ) {}

  /** @internal */
  static create(laser: Laser, session: ConversationId): SessionControl {
    return new SessionControl(laser, session)
  }

  /** The operator identity the control records carry. Required. */
  asOperator(operator: AgentId): SessionControl {
    return new SessionControl(this.laser, this.session, operator, this.key, this.named)
  }

  /** Sign every control record with `key`, so a verifying agent can prove
   * which operator sent it. */
  signedBy(key: SigningKey): SessionControl {
    return new SessionControl(this.laser, this.session, this.operator, key, this.named)
  }

  /** The agents whose acknowledgments complete a pause, instead of the
   * agents the session lane shows working on the session. */
  participants(participants: Iterable<AgentId>): SessionControl {
    return new SessionControl(
      this.laser,
      this.session,
      this.operator,
      this.key,
      [...participants].map((agent) => agent.wireId())
    )
  }

  /** Ask the session's agents to pause. The request names the agents whose
   * acknowledgments complete the pause: the set given to `participants`, or
   * else the agents the session lane shows working on the session, the
   * addressees of its work commands and the agents that picked it up. Every
   * agent that receives work for the session holds it until the resume, named
   * or not. */
  async pause(): Promise<AgdxReceipt> {
    const participants = this.named ?? (await laneParticipants(this.laser, this.session))
    return this.request(OPERATION_SESSION_PAUSE, encodeSessionPauseRequestJson({ participants }))
  }

  /** Ask a paused session's agents to resume. */
  async resume(): Promise<AgdxReceipt> {
    return this.request(OPERATION_SESSION_RESUME)
  }

  /** Ask the session's agents to end it as canceled at their next boundary. */
  async cancel(): Promise<AgdxReceipt> {
    return this.request(OPERATION_SESSION_CANCEL)
  }

  /** End the session as canceled without its agents, for a session whose agent
   * is gone. */
  async forceCancel(): Promise<AgdxReceipt> {
    return this.sign(
      this.producer()
        .status(OPERATION_SESSION)
        .withTaskState(taskStateFromCode(TaskStateName.Canceled))
        .body(encodeNamed(encodeSessionEnd({ reason: "forced" })))
        .contentType(ContentType.Cbor)
        .last()
    ).sendReceipt()
  }

  private request(operation: string, body = "{}"): Promise<AgdxReceipt> {
    return this.sign(
      this.producer()
        .command(MintUlid.mint(CorrelationId), new TextEncoder().encode(body))
        .withOperation(operation)
        .contentType(ContentType.Json)
    ).sendReceipt()
  }

  private sign(send: AgdxSend): AgdxSend {
    return this.key === undefined ? send : send.signedBy(this.key)
  }

  private producer(): Agdx {
    if (this.operator === undefined)
      throw new InvalidError("session control needs `.asOperator(id)`")
    return this.laser.agdx(AgentTopic.Control, this.operator, this.session)
  }
}

/** A session about to start. Chain the optional settings, then `begin`. */
export class SessionBuilder {
  private agentId: AgentId | undefined
  private namespaceName: string | undefined
  private parentId: ConversationId | undefined
  private rootId: ConversationId | undefined
  private explicitId: ConversationId | undefined
  private idleTimeoutMs: number | undefined
  private budgetValue: Budget | undefined
  private readonly tags: string[] = []

  private constructor(
    private readonly sessions: Sessions,
    private readonly laser: Laser,
    private readonly label: string | undefined
  ) {}

  /** @internal */
  static create(sessions: Sessions, laser: Laser, label: string | undefined): SessionBuilder {
    return new SessionBuilder(sessions, laser, label)
  }

  /** The agent that owns and writes the session. Required. */
  agent(agent: AgentId): this {
    this.agentId = agent
    return this
  }

  /** The namespace a labeled session id derives under. */
  namespace(namespace: string): this {
    this.namespaceName = namespace
    return this
  }

  /** Make this a child of `parent`, whose tree is rooted at `root` (the parent
   * itself when it has no parent). */
  parent(parent: ConversationId, root: ConversationId): this {
    this.parentId = parent
    this.rootId = root
    return this
  }

  /** Start the session under an explicit id instead of a label or a fresh id. */
  withId(id: ConversationId): this {
    this.explicitId = id
    return this
  }

  /** The idle timeout, instead of the configured default. */
  idleTimeout(timeoutMs: number): this {
    this.idleTimeoutMs = duration(timeoutMs, "idle timeout")
    return this
  }

  /** The token and cost ceiling a reader compares the session's usage with. */
  budget(budget: Budget): this {
    this.budgetValue = budget
    return this
  }

  /** One searchable tag. */
  tag(tag: string): this {
    this.tags.push(tag)
    return this
  }

  /** The id this builder starts: the explicit id, the label's derived id, or a
   * fresh one on each call for an unlabeled session. */
  id(): ConversationId {
    if (this.explicitId !== undefined) return this.explicitId
    if (this.label === undefined) return ConversationId.new()
    return deriveSessionId(this.sessions.stream(), this.namespaceName ?? "", this.label)
  }

  /** Write the session start on the lane and take a lease that keeps the
   * session listed in this process's heartbeat. */
  async begin(): Promise<{ readonly session: Session; readonly lease: SessionLease }> {
    const agent = this.agentId
    if (agent === undefined) throw new InvalidError("a session needs an agent")
    const id = this.id()
    const config = this.sessions.config
    const idleTimeoutMs = this.idleTimeoutMs ?? config.idleTimeoutValue
    const start: SessionStart = {
      ...(this.label !== undefined ? { label: this.label } : {}),
      ...(this.namespaceName !== undefined ? { namespace: this.namespaceName } : {}),
      agent: agent.wireId(),
      sdk: config.sdkInfo,
      ...(this.parentId !== undefined ? { parent: wireConversation(this.parentId) } : {}),
      ...(this.rootId !== undefined ? { root: wireConversation(this.rootId) } : {}),
      idleTimeoutMicros: micros(idleTimeoutMs),
      ...(this.budgetValue !== undefined ? { budget: this.budgetValue } : {}),
      tags: [...this.tags]
    }
    const body = encodeNamed(encodeSessionStart(start))
    const session = this.sessions.open(id).asAgent(agent).withAncestry(this.parentId, this.rootId)
    const generation = await session.streamGeneration()
    await session
      .withEnvelopeAncestry(
        session
          .lane()
          .status(OPERATION_SESSION)
          .withTaskState(taskStateFromCode(TaskStateName.Working))
          .body(body)
          .contentType(ContentType.Cbor)
      )
      .send()
    return { session, lease: session.lease(generation, idleTimeoutMs) }
  }
}

type TerminalState = "Completed" | "Failed" | "Canceled"

// The first terminal a session handle wrote or tried to write. Every handle
// derived from one session shares it, so a retry repeats the same record and a
// different verb is refused.
interface TerminalLatch {
  intent?: { readonly state: TerminalState; readonly end: SessionEnd; readonly record: RecordId }
}

// What one session handle carries. Handles derived from one session share the
// terminal latch and the state cursor.
interface SessionParts {
  readonly agent?: AgentId
  readonly parent?: ConversationId
  readonly root?: ConversationId
  readonly laneGuard: LaneGuard
  readonly terminal: TerminalLatch
  readonly state: StateCursor
  readonly redactor: (value: unknown) => unknown
  readonly current?: SourceRef
  // The control state of the runtime that handed this lens to a handler.
  readonly control?: ControlBook
  readonly key?: SigningKey
}

/** One session. Handles derived from one session share its terminal latch and
 * state cursor. Build it with `Sessions.create`, `Sessions.start`, or
 * `Sessions.open`. */
export class Session {
  /** The underlying `ContextScope`, for a topic outside the session lane or an
   * explicit `ContextPolicy`. */
  readonly scope: ContextScope

  private constructor(
    private readonly laser: Laser,
    readonly config: SessionConfig,
    conversation: ConversationId,
    private readonly parts: SessionParts
  ) {
    this.scope = ContextScope.create(laser, conversation)
  }

  /** @internal */
  static lens(laser: Laser, config: SessionConfig, conversation: ConversationId): Session {
    return new Session(laser, config, conversation, {
      laneGuard: new LaneGuard(),
      terminal: {},
      state: { revision: 0n, document: undefined, sinceSnapshot: 0 },
      redactor: defaultRedact
    })
  }

  /** @internal */
  withLaneGuard(laneGuard: LaneGuard): Session {
    return this.with({ laneGuard })
  }

  /** This handle writing as `agent`. */
  asAgent(agent: AgentId): Session {
    return this.with({ agent })
  }

  /** @internal */
  withAncestry(parent: ConversationId | undefined, root: ConversationId | undefined): Session {
    const { parent: _parent, root: _root, ...rest } = this.parts
    return new Session(this.laser, this.config, this.conversation, {
      ...rest,
      ...(parent !== undefined ? { parent } : {}),
      ...(root !== undefined ? { root } : {})
    })
  }

  /** Replace the redaction applied to tool arguments and model request bodies
   * before they are published. The redactor returns the value to publish. The
   * default drops the values of common secret keys. */
  redact(redactor: (value: unknown) => unknown): Session {
    return this.with({ redactor })
  }

  /** This handle signing its terminal record with `key`, so a verifying
   * reader can prove which agent ended the session. */
  signedBy(key: SigningKey): Session {
    return this.with({ key })
  }

  /** @internal */
  withControl(control: ControlBook): Session {
    return this.with({ control })
  }

  /** This handle with `source` as the record it acts on, stamped as the source
   * of graph writes and the origin of remembered items. */
  actingOn(source: SourceRef): Session {
    return this.with({ current: source })
  }

  /** This session's id. */
  get conversation(): ConversationId {
    return this.scope.conversation
  }

  /** The agent this handle writes as. */
  get agent(): AgentId | undefined {
    return this.parts.agent
  }

  /** The parent session, for a child session. */
  get parent(): ConversationId | undefined {
    return this.parts.parent
  }

  /** The root of this session's tree, for a child session. */
  get root(): ConversationId | undefined {
    return this.parts.root
  }

  /** The stream this session lives in. */
  stream(): string {
    return requireStream(this.laser)
  }

  /** Append one typed envelope to the session lane. The envelope must name this
   * session as its conversation. */
  append(envelope: AgentEnvelope): Promise<AgdxReceipt> {
    if (envelope.conversation.toString() !== this.conversation.toString()) {
      return Promise.reject(new InvalidError("an appended envelope must belong to this session"))
    }
    return this.laser
      .agdx(AgentTopic.Sessions, AgentId.new(envelope.source), this.conversation)
      .withLaneGuard(() => this.parts.laneGuard.check(this.laser, this.conversation))
      .publishEnvelope(envelope)
  }

  /** End the session as completed. State changed since the last snapshot is
   * snapshotted first, so a reader can start from the final document. */
  async end(): Promise<void> {
    await this.state().snapshotIfChanged()
    await this.terminate("Completed", {})
  }

  /** End the session as failed with `error`. */
  fail(error: AgentErrorBody): Promise<void> {
    return this.terminate("Failed", { error })
  }

  /** End the session as canceled. */
  cancel(): Promise<void> {
    return this.terminate("Canceled", {})
  }

  /** Run `work` inside the session and end it by the outcome: completed on
   * success, failed on a throw or a rejection, which is rethrown unchanged
   * after the failure is written. A thrown value that is not a `LaserError`
   * marks the failure as a panic. When the terminal write also fails, the
   * error from `work` is what surfaces. The lease is released either way. A
   * process that dies mid-run is not captured, and the session then shows idle
   * once its heartbeat stops. */
  async run<T>(lease: SessionLease, work: (session: Session) => T | Promise<T>): Promise<T> {
    try {
      let value: T
      try {
        value = await work(this)
      } catch (error) {
        try {
          await this.fail(failureBody(error))
        } catch {
          // The work error is the one that surfaces.
        }
        throw error
      }
      await this.end()
      return value
    } finally {
      lease.release()
    }
  }

  private async terminate(state: TerminalState, end: SessionEnd): Promise<void> {
    let intent = this.parts.terminal.intent
    if (intent !== undefined && intent.state !== state) {
      throw new InvalidError(`session already ending as ${intent.state.toLowerCase()}`)
    }
    if (intent === undefined) {
      intent = { state, end, record: MintUlid.mint(RecordId) }
      this.parts.terminal.intent = intent
    }
    const status = this.lane()
      .status(OPERATION_SESSION)
      .withTaskState(taskStateFromCode(TaskStateName[intent.state]))
      .withRecord(intent.record)
      .body(encodeNamed(encodeSessionEnd(intent.end)))
      .contentType(ContentType.Cbor)
      .last()
    const key = this.parts.key
    await this.withEnvelopeAncestry(key === undefined ? status : status.signedBy(key)).send()
  }

  /** @internal */
  async pickUp(): Promise<SessionLease> {
    const generation = await this.streamGeneration()
    const agent = this.parts.agent
    const transition = encodeNamed(
      encodeSessionTransition(agent === undefined ? {} : { actor: agent.wireId() })
    )
    await this.withEnvelopeAncestry(
      this.lane()
        .status(OPERATION_SESSION)
        .withTaskState(taskStateFromCode(TaskStateName.Working))
        .body(transition)
        .contentType(ContentType.Cbor)
    ).send()
    return this.lease(generation, this.config.idleTimeoutValue)
  }

  /** This session as a stream-scoped reference, for writes that land outside
   * the session's own stream. */
  reference(): SessionRef {
    return { stream: this.stream(), session: wireConversation(this.conversation) }
  }

  /** The session's state document. */
  state(): SessionState {
    return SessionState.create(this, this.parts.state)
  }

  /** The managed state view of this session, `undefined` on a deployment
   * that does not index sessions.
   * @internal */
  async managedStateView(): Promise<SessionStateView | undefined> {
    if (!(await this.laser.capabilities()).sessions) return undefined
    return this.laser.sessions(this.config).state(this.conversation, 0)
  }

  /** Assemble the model context under `policy` from the session lane and
   * describe it in a manifest. Nothing is written. */
  async assemble(policy: ContextPolicy): Promise<AssembledContext> {
    const name = contextPolicyName(policy)
    const version = contextPolicyVersion(policy)
    const fragments = await this.scope.fetchWith(LANE_TOPICS, policy)
    const lane = await this.laneIdentity()
    let tokens = 0n
    let bytes = 0n
    const frontier = new Map<number, bigint>()
    const manifestFragments = fragments.map((message): Fragment => {
      const size = message.payload.byteLength
      const estimate = estimateTokens(size)
      tokens += estimate
      bytes += BigInt(size)
      const seen = frontier.get(message.id.partitionId)
      if (seen === undefined || message.id.offset > seen)
        frontier.set(message.id.partitionId, message.id.offset)
      return {
        kind: "message",
        at: {
          kind: "message",
          stream: lane.streamId,
          topic: lane.topicId,
          partition: message.id.partitionId,
          offset: message.id.offset,
          generation: lane.generation,
          conversation: this.conversation.toString()
        },
        tokens: Number(estimate > U32_MAX ? U32_MAX : estimate),
        bytes: Math.min(size, Number(U32_MAX))
      }
    })
    return AssembledContext.create(fragments, {
      policy: name,
      policyVersion: version,
      fragments: manifestFragments,
      tokens,
      bytes,
      frontier: [...frontier]
        .sort(([left], [right]) => left - right)
        .map(([partition, offset]) => [lane.topicId, partition, offset] as const)
    })
  }

  /** Record a model request addressed to this agent, and the context it
   * received when `assembled` is given. Finish the returned call when the
   * application's provider answers. */
  async model(request: ModelRequest, assembled?: AssembledContext): Promise<ModelCall> {
    const correlation = MintUlid.mint(CorrelationId)
    await this.writeModelRequest(correlation, request, assembled)
    return ModelCall.create(this, correlation, request.operation)
  }

  /** Record a tool call addressed to this agent, with `args` redacted. */
  async tool(name: string, args: unknown): Promise<ToolCall> {
    const agent = this.requireAgent()
    const correlation = MintUlid.mint(CorrelationId)
    await this.lane()
      .command(correlation, new TextEncoder().encode(JSON.stringify(this.parts.redactor(args))))
      .withOperation(OPERATION_EXECUTE_TOOL)
      .withTool(name)
      .withTarget(agent)
      .contentType(ContentType.Json)
      .send()
    return ToolCall.create(this, correlation, name)
  }

  /** Record a model call that already happened: the request, the context when
   * given, and the response. The response duration is the measured call time.
   * A response without `durationMs` records a duration of 0. */
  async recordModelCall(
    request: ModelRequest,
    response: ModelResponse,
    assembled?: AssembledContext
  ): Promise<AgdxReceipt> {
    const correlation = MintUlid.mint(CorrelationId)
    await this.writeModelRequest(correlation, request, assembled)
    return this.writeResult(
      correlation,
      request.operation,
      undefined,
      response,
      response.durationMs ?? 0
    )
  }

  /** The key-value namespace `namespace` with every write linked to this session. */
  kv(namespace: string): Kv {
    return this.laser.kv(namespace).inSession(this.reference())
  }

  /** The knowledge graph `name` with every upsert linked to this session,
   * stamped with this agent as producer and the current record as source. */
  linkedGraph(name: string): GraphHandle {
    const graph = this.laser.graph(name).inSession(this.reference()).producedBy(this.producer())
    return this.parts.current === undefined ? graph : graph.sourcedFrom(this.parts.current)
  }

  /** This session's summary from the deployment's session index: status,
   * participants, counters, and the derived idle and over-budget flags. */
  status(): Promise<SessionInfo> {
    return this.laser.sessions().get(this.conversation)
  }

  /** The pause and cancel requests operators sent this session on
   * `agent.control`, addressed to this handle's agent or to every agent.
   * Inside a handler the runtime's control subscription keeps them current,
   * and the first read of a session folds its retained control records.
   * Outside a runtime every call reads the retained records. The runtime only
   * records requests: the handler decides when to stop. */
  async pendingControl(): Promise<PendingControl> {
    return (await this.controlState()).flags
  }

  /** The control state of this session, from the runtime's control book when
   * it is current, otherwise folded from the retained control records.
   * @internal */
  async controlState(): Promise<ControlState> {
    const conversation = this.conversation.toString()
    const known = this.parts.control?.state(conversation)
    if (known !== undefined) return known
    const me = this.parts.agent?.wireId()
    const records = [
      ...(await this.scope.fetchWith([AgentTopic.Control], new LastN(Number.MAX_SAFE_INTEGER)))
    ].sort(
      (left, right) =>
        compareBigint(left.timestampMicros, right.timestampMicros) ||
        left.id.partitionId - right.id.partitionId ||
        compareBigint(left.id.offset, right.id.offset)
    )
    const [state, last] = foldControl(
      records.flatMap((record) => {
        const request =
          record.envelope === undefined ? undefined : controlRequest(record.envelope, me)
        return request === undefined
          ? []
          : [[[record.id.partitionId, record.id.offset] as const, request] as const]
      })
    )
    return this.parts.control?.install(conversation, state, last) ?? state
  }

  /** Whether this session's usage has passed its budget: the summed input
   * and output tokens of its records over the token ceiling, or their summed
   * cost over the cost ceiling. A deployment that indexes sessions answers
   * from its index. On open Apache Iggy, or for a session the index does not
   * know yet, the retained session lane is folded: the budget of the first
   * start record against the `usage` of every record. A session without a
   * budget is never over it. The answer is eventually consistent, so a
   * budget is a cooperative limit, not a hard spending cap. */
  async overBudget(): Promise<boolean> {
    return (await this.budgetBreach()) !== undefined
  }

  /** How this session passed its budget, `undefined` while within it.
   * @internal */
  async budgetBreach(): Promise<BudgetBreach | undefined> {
    if ((await this.laser.capabilities()).sessions) {
      try {
        return breachOfInfo(await this.status())
      } catch (error) {
        if (!unknownToIndex(error)) throw error
      }
    }
    const records = await this.scope.fetchWith(
      [AgentTopic.Sessions],
      new LastN(Number.MAX_SAFE_INTEGER)
    )
    return laneBreach(
      records.flatMap((record) => (record.envelope === undefined ? [] : [record.envelope]))
    )
  }

  /** End the session failed with reason `budget`. The terminal latch makes a
   * repeated call on this handle or its clones write the same record.
   * @internal */
  failOverBudget(breach: BudgetBreach): Promise<void> {
    return this.terminate("Failed", {
      reason: BUDGET_END_REASON,
      error: breachErrorBody(breach)
    })
  }

  /** The work records this session's agents held while it was paused and
   * have not reported handled. A session canceled while paused keeps its held
   * records here unprocessed. The read covers the newest
   * `CONTEXT_READ_WINDOW` records of each `agent.sessions` partition. */
  parked(): Promise<ParkedRecords> {
    return readParked(this.laser, this.conversation.toString())
  }

  /** Whether an operator asked to cancel this session on `agent.control`, by a
   * cancel request or a forced cancel. Reads the retained control records of
   * this session, so it answers on open Apache Iggy too. */
  async cancelRequested(): Promise<boolean> {
    const records = await this.scope.fetchWith(
      [AgentTopic.Control],
      new LastN(Number.MAX_SAFE_INTEGER)
    )
    return records.some(
      (record) =>
        record.envelope?.operation === OPERATION_SESSION_CANCEL ||
        (record.envelope?.taskState?.kind === "known" &&
          record.envelope.taskState.name === "Canceled")
    )
  }

  /** Record that `items`, recalled for `query` when there was one, entered
   * the session's context, with each item's id and score. */
  recordRetrieval(query: string | undefined, items: readonly MemoryItem[]): Promise<AgdxReceipt> {
    return this.writeEvent(
      OPERATION_CONTEXT_RETRIEVED,
      encodeNamed(
        encodeContextRetrieval({
          ...(query !== undefined ? { query } : {}),
          items: items.map((item) => [item.id.toString(), item.score ?? 0] as const)
        }),
        { forceFloatNumbers: true }
      )
    )
  }

  /** Record that a summary replaced the covered records of the session's
   * context. */
  recordCompaction(compaction: ContextCompaction): Promise<AgdxReceipt> {
    return this.writeEvent(
      OPERATION_CONTEXT_COMPACTED,
      encodeNamed(encodeContextCompaction(compaction))
    )
  }

  /** This session's memory with every remembered item stamped with this agent
   * as producer and the current record as origin. */
  linkedMemory(): ScopedMemory {
    return this.memory().withLineage(this.parts.current, this.producer())
  }

  /** @internal */
  async writeResult(
    correlation: CorrelationId,
    operation: string,
    tool: string | undefined,
    response: Omit<ModelResponse, "duration">,
    durationMs: number
  ): Promise<AgdxReceipt> {
    const agent = this.requireAgent()
    let send = this.lane()
      .respond(correlation, response.body)
      .withOperation(operation)
      .withTarget(agent)
      .withMetadata(METADATA_DURATION_MICROS, {
        kind: "uint",
        value: millisToMicros(Math.max(0, durationMs))
      })
    if (tool !== undefined) send = send.withTool(tool)
    if (response.usage !== undefined) send = send.withUsage(response.usage)
    if (response.model !== undefined)
      send = send.withMetadata(METADATA_RESPONSE_MODEL, { kind: "str", value: response.model })
    if (response.finishReason !== undefined) send = send.withFinishReason(response.finishReason)
    return send.sendReceipt()
  }

  /** @internal */
  writeFailure(
    correlation: CorrelationId,
    operation: string,
    tool: string | undefined,
    error: AgentErrorBody
  ): Promise<AgdxReceipt> {
    const agent = this.requireAgent()
    let send = this.lane().fail(correlation, error).withOperation(operation).withTarget(agent)
    if (tool !== undefined) send = send.withTool(tool)
    return send.sendReceipt()
  }

  /** @internal */
  writeEvent(
    operation: string,
    body: Uint8Array,
    correlation?: CorrelationId
  ): Promise<AgdxReceipt> {
    let send = this.lane().emit(body).withOperation(operation).contentType(ContentType.Cbor)
    if (correlation !== undefined) send = send.withCorrelation(correlation)
    return send.sendReceipt()
  }

  /** @internal */
  mintOpId(): string {
    return crockfordEncode(mintUlidValue())
  }

  private async writeModelRequest(
    correlation: CorrelationId,
    request: ModelRequest,
    assembled: AssembledContext | undefined
  ): Promise<void> {
    const agent = this.requireAgent()
    let body = request.body
    try {
      const json: unknown = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(body))
      body = new TextEncoder().encode(JSON.stringify(this.parts.redactor(json)))
    } catch {
      // A body that is not JSON is published as it is.
    }
    let send = this.lane()
      .command(correlation, body)
      .withOperation(request.operation)
      .withTarget(agent)
      .withMetadata(METADATA_REQUEST_MODEL, { kind: "str", value: request.model })
    if (request.provider !== undefined)
      send = send.withMetadata(METADATA_PROVIDER_NAME, { kind: "str", value: request.provider })
    await send.send()
    if (assembled !== undefined) {
      await this.writeEvent(
        OPERATION_CONTEXT_ASSEMBLED,
        encodeNamed(encodeContextManifest({ ...assembled.manifest, correlation })),
        correlation
      )
    }
  }

  private producer(): ProducerInfo {
    const agent = this.parts.agent
    return {
      name: agent === undefined ? "sdk" : `sdk:${agent.asStr()}`,
      version: this.config.sdkInfo.version
    }
  }

  private requireAgent(): AgentId {
    const agent = this.parts.agent
    if (agent === undefined) throw new InvalidError("this session handle has no agent")
    return agent
  }

  private async laneIdentity(): Promise<{
    readonly streamId: number
    readonly topicId: number
    readonly generation: bigint
  }> {
    const stream = this.stream()
    const transport = this.laser[INTERNAL_TRANSPORT]()
    if (transport.findSnapshotStream === undefined || transport.findSnapshotTopic === undefined)
      throw new UnsupportedError("the session lane identity is unavailable")
    const details = await transport.findSnapshotStream(stream)
    if (details === undefined) throw new InvalidError("the session stream does not exist")
    const topic = await transport.findSnapshotTopic(stream, AgentTopic.Sessions)
    if (topic === undefined) throw new InvalidError("agent.sessions does not exist")
    return { streamId: details.id, topicId: topic.id, generation: topic.createdAtMicros }
  }

  private with(change: Partial<SessionParts>): Session {
    return new Session(this.laser, this.config, this.conversation, { ...this.parts, ...change })
  }

  /** @internal */
  lane(): Agdx {
    if (this.parts.agent === undefined) throw new InvalidError("this session handle has no agent")
    return this.laser
      .agdx(AgentTopic.Sessions, this.parts.agent, this.conversation)
      .withLaneGuard(() => this.parts.laneGuard.check(this.laser, this.conversation))
  }

  /** @internal */
  withEnvelopeAncestry(send: AgdxSend): AgdxSend {
    return send.withAncestry(
      this.parts.parent === undefined ? undefined : wireConversation(this.parts.parent),
      this.parts.root === undefined ? undefined : wireConversation(this.parts.root)
    )
  }

  /** @internal */
  async streamGeneration(): Promise<bigint> {
    return (await this.parts.laneGuard.check(this.laser, this.conversation)).streamGeneration
  }

  /** @internal */
  lease(generation: bigint, idleTimeoutMs: number): SessionLease {
    const agent = this.parts.agent
    if (agent === undefined) throw new InvalidError("a leased session needs an agent")
    const interval = Math.max(
      Math.min(this.config.heartbeatValue, idleTimeoutMs / 5),
      MIN_LEASE_INTERVAL_MS
    )
    return this.laser[INTERNAL_SESSION_LEASES]().acquire(
      this.laser,
      { stream: this.stream(), streamGeneration: generation, session: this.conversation },
      agent,
      interval
    )
  }

  /** The model-ready context: the configured last records on the session lane,
   * trimmed to the configured estimated token bound. */
  context(): Promise<readonly SessionTurn[]> {
    return this.contextWith(
      new Chain([
        new LastN(this.config.contextTurnBound),
        new TokenBudget(this.config.contextTokenBound)
      ])
    )
  }

  /** `context` under an explicit policy. */
  async contextWith(policy: ContextPolicy): Promise<readonly SessionTurn[]> {
    return turnsOf(await this.scope.fetchWith(LANE_TOPICS, policy))
  }

  /** This session's memory in the configured namespace, scoped to the session. */
  memory(namespace: string = this.config.memoryNamespaceName): ScopedMemory {
    return this.scope.memory(namespace)
  }

  /** The knowledge graph `name`, the same graph `laser.graph` returns. */
  graph(name: string): GraphHandle {
    return this.scope.graph(name)
  }

  /** Where the session lane ends right now. Persist it and hand it to
   * `turnsAt`, `turnsSince`, `stateAt`, or `replay`. */
  checkpoint(): Promise<Checkpoint> {
    return this.scope.checkpoint(LANE_TOPICS)
  }

  /** The records up to `checkpoint`. */
  turnsAt(checkpoint: Checkpoint): Promise<readonly SessionTurn[]> {
    return this.turns({ kind: "at", checkpoint })
  }

  /** The records appended after `checkpoint`. */
  turnsSince(checkpoint: Checkpoint): Promise<readonly SessionTurn[]> {
    return this.turns({ kind: "from-checkpoint", checkpoint })
  }

  /** Fold the records up to `checkpoint`: state as it stood then. */
  async stateAt<State>(
    checkpoint: Checkpoint,
    initial: State,
    fold: (state: State, turn: SessionTurn) => State
  ): Promise<State> {
    return (await this.turnsAt(checkpoint)).reduce(fold, initial)
  }

  /** Fold the records appended after `checkpoint`: bring state saved there up to date. */
  async replay<State>(
    checkpoint: Checkpoint,
    initial: State,
    fold: (state: State, turn: SessionTurn) => State
  ): Promise<State> {
    return (await this.turnsSince(checkpoint)).reduce(fold, initial)
  }

  private async turns(bound: ReplayBound): Promise<readonly SessionTurn[]> {
    return turnsOf(
      await this.scope.state(LANE_TOPICS, bound, [] as ContextMessage[], (acc, message) => {
        acc.push(message)
        return acc
      })
    )
  }
}

/** One record read back from a `Session`: the message off the log plus how a
 * timeline shows it. */
export interface SessionTurn {
  readonly display: DisplayType
  readonly message: ContextMessage
}

/** The envelope body of `turn`, or its payload for a plain record, as UTF-8,
 * lossy. */
export function sessionTurnText(turn: SessionTurn): string {
  return new TextDecoder().decode(turn.message.envelope?.body ?? turn.message.payload)
}

const LANE_TOPICS: readonly string[] = [AgentTopic.Sessions]

function turnsOf(messages: readonly ContextMessage[]): readonly SessionTurn[] {
  return messages.map((message) => ({
    display:
      message.envelope === undefined
        ? "agent.message"
        : displayType({ kind: "envelope", envelope: message.envelope }, AGENT_SESSIONS),
    message
  }))
}

function failureBody(error: unknown): AgentErrorBody {
  const message = error instanceof Error ? error.message : String(error)
  return {
    code: { kind: "known", name: "Internal" },
    message,
    retryable: false,
    ...(error instanceof LaserError
      ? {}
      : { detail: new Map([["panic", { kind: "bool" as const, value: true }]]) })
  }
}

function requireStream(laser: Laser): string {
  const stream = laser.defaultStream
  if (stream === undefined) {
    throw new NoStreamError(
      "sessions require a default stream, use connectWithStream() or SessionConfig.stream()"
    )
  }
  return stream
}

function wireConversation(id: ConversationId): WireConversationId {
  return WireConversationId.parse(id.toString())
}

function micros(ms: number): bigint {
  return millisToMicros(ms)
}

function duration(ms: number, name: string): number {
  if (!Number.isFinite(ms) || ms < 0) {
    throw new InvalidError(`a session ${name} must be a non-negative finite number of milliseconds`)
  }
  return ms
}

function compareBigint(left: bigint, right: bigint): number {
  return left < right ? -1 : left > right ? 1 : 0
}

// The distinct topics a per-agent topic layout declares, each a valid topic
// name that is not one of the topics the layout routes around.
function declaredTopics(topics: ReadonlyMap<string, string>): readonly string[] {
  const declared: string[] = []
  for (const topic of topics.values()) {
    const bytes = new TextEncoder().encode(topic).byteLength
    if (bytes === 0 || bytes > 255) {
      throw new InvalidError(`a declared agent topic must be 1 to 255 bytes, got \`${topic}\``)
    }
    if (topic === AGENT_SESSIONS || topic === AGENT_CONTROL) {
      throw new InvalidError(`a per-agent topic layout cannot declare \`${topic}\``)
    }
    if (!declared.includes(topic)) declared.push(topic)
  }
  return declared
}
