import {
  type CallOptions,
  abortable,
  cancellable,
  delay,
  throwIfAborted
} from "./client/call-options.js"
import { millisToMicros } from "./client/duration.js"
import type { Laser } from "./client/laser.js"
import { isStale, isUnavailable } from "./client/error-classify.js"
import { publishOptions, publishWithin } from "./client/publish-options.js"
import { warn } from "./runtime/warn.js"
import { type BytesLike, ownedBytes } from "./client/bytes.js"
import {
  InvalidError,
  NoStreamError,
  SessionEndedError,
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
  controlPartition,
  foldControl
} from "./agent/control.js"
import {
  BUDGET_END_REASON,
  type BudgetBreach,
  LaneLifecycle,
  breachErrorBody,
  breachOfInfo,
  indexUnusable,
  laneBreach
} from "./agent/budget.js"
import { type ParkedRecords, laneParticipants, readParked } from "./agent/lane-scan.js"
import type { MemoryItem } from "./memory/types.js"
import {
  type Checkpoint,
  Chain,
  ContextAssembler,
  LastN,
  TokenBudget,
  type ContextMessage,
  type ContextPolicy,
  contextCheckpoint,
  contextMessageBody,
  contextPolicyName,
  contextPolicyVersion,
  replayContext
} from "./context.js"
import { ContextScope, type ScopedMemory } from "./context-scope.js"
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
  AgentKind,
  type Budget,
  type Fragment,
  METADATA_DURATION_MICROS,
  METADATA_PROVIDER_NAME,
  METADATA_RECORDED,
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
    return this.with({ idleTimeoutMs: positiveDuration(timeoutMs, "idle timeout") })
  }

  /** How often a process with session leases publishes its heartbeat. */
  heartbeat(intervalMs: number): SessionConfig {
    return this.with({ heartbeatMs: positiveDuration(intervalMs, "heartbeat interval") })
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

  async check(
    laser: Laser,
    conversation: ConversationId,
    options: CallOptions = {}
  ): Promise<LaneIdentity> {
    throwIfAborted(options.signal, "session lane check")
    const identity = await abortable(this.observe(laser), options.signal, "session lane check")
    if (
      !this.registered &&
      (await abortable(laser.capabilities(), options.signal, "session lane check")).sessions
    ) {
      try {
        const lane = await laser.sessions().registeredLane(conversation, options)
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
  async bootstrap(
    partitions: number,
    retention: TopicRetention,
    options: CallOptions = {}
  ): Promise<SessionBootstrap> {
    const signal = options.signal
    throwIfAborted(signal, "session bootstrap")
    const layout = this.config.layoutKind
    const count = layout.kind === "singlePartition" ? 1 : partitions
    const declared = layout.kind === "perAgentTopic" ? declaredTopics(layout.topics) : []
    await this.laser.bootstrap(count, retention, options)
    // Each declared agent topic carries a share of the session's work, so it
    // takes the lane's partition count and retention.
    for (const topic of declared) {
      await abortable(
        this.laser[INTERNAL_ENSURE_RETAINED](topic, count, retention),
        signal,
        "session bootstrap"
      )
    }
    await abortable(this.laneGuard.observe(this.laser), signal, "session bootstrap")
    if (
      !this.config.registersSource ||
      !(await abortable(this.laser.capabilities(), signal, "session bootstrap")).sessions
    ) {
      return { registered: false }
    }
    try {
      await this.laser[INTERNAL_PUBLISH_CONTROL](
        { kind: "registerSessionSource", stream: this.stream(), topics: { kind: "all" } },
        options
      )
    } catch (error) {
      throwIfAborted(signal, "session bootstrap")
      warn(`session source registration refused: ${String(error)}`)
      return { registered: false }
    }
    return { registered: await this.registrationVisible(options) }
  }

  /** Stop the deployment from indexing this stream's sessions, through the
   * authenticated managed control path, as Rust `Sessions::unregister`. A later
   * `bootstrap` registers the stream again. */
  async unregister(options: CallOptions = {}): Promise<void> {
    throwIfAborted(options.signal, "session unregister")
    if (
      !(await abortable(this.laser.capabilities(), options.signal, "session unregister")).sessions
    ) {
      throw new UnsupportedError("this deployment does not index sessions", { surface: "sessions" })
    }
    await this.laser[INTERNAL_PUBLISH_CONTROL](
      { kind: "removeSessionSource", stream: this.stream() },
      options
    )
  }

  // Wait until the deployment serves reads for the registered stream, up to
  // the publish timeout. Registration applies after its control record folds,
  // so reads and linked writes right after bootstrap would otherwise race it.
  private async registrationVisible(options: CallOptions): Promise<boolean> {
    const signal = options.signal
    const transport = this.laser[INTERNAL_TRANSPORT]()
    const deadline = Date.now() + (transport.publishTimeoutMs?.() ?? publishOptions().timeoutMs)
    let lastUnavailable: string | undefined
    while (Date.now() < deadline) {
      try {
        await publishWithin(
          this.readChanges(0, 1, { ...options, deadline }),
          Math.max(0, deadline - Date.now())
        )
        return true
      } catch (error) {
        throwIfAborted(signal, "session bootstrap")
        if (Date.now() >= deadline) break
        if (isUnavailable(error) || isStale(error)) {
          lastUnavailable = String(error)
        } else if (!(
          error instanceof SessionError &&
          (error.detail.kind === "notRegistered" || error.detail.kind === "stale")
        )) {
          warn(`session source registration not confirmed: ${String(error)}`)
          return false
        }
      }
      await delay(
        Math.min(REGISTRATION_POLL_INTERVAL_MS, Math.max(0, deadline - Date.now())),
        signal,
        "session bootstrap"
      )
    }
    warn(
      `session source registration not visible before the publish timeout${lastUnavailable === undefined ? "" : `: ${lastUnavailable}`}`
    )
    return false
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
  async get(id: ConversationId, options: CallOptions = {}): Promise<SessionInfo> {
    const outcome = await readSession(
      this.laser,
      SessionGetCommand,
      { stream: this.stream(), id: wireConversation(id) },
      options
    )
    if (outcome.kind !== "info") throw unexpected("get")
    return outcome.info
  }

  /** A page of this stream's sessions, newest first. Chain the filters and
   * finish with `fetch`. */
  list(): SessionListRequest {
    return SessionListRequest.create(this.laser, this.stream())
  }

  /** A page of one session's events in server append time order. Chain the paging
   * and finish with `fetch`. */
  events(id: ConversationId): SessionEventsRequest {
    return SessionEventsRequest.create(this.laser, this.stream(), wireConversation(id))
  }

  /** One session's folded state document with up to `historyLimit` history
   * rows. Zero leaves the history size to the server. */
  async state(
    id: ConversationId,
    historyLimit: number,
    options: CallOptions = {}
  ): Promise<SessionStateView> {
    const outcome = await readSession(
      this.laser,
      SessionStateCommand,
      { stream: this.stream(), id: wireConversation(id), historyLimit },
      options
    )
    if (outcome.kind !== "state") throw unexpected("state")
    return outcome.state
  }

  /** The resources one session wrote, recalled, or touched, narrowed to
   * `surface` when given. */
  async links(
    id: ConversationId,
    surface?: LinkSurface,
    options: CallOptions = {}
  ): Promise<SessionLinksView> {
    const outcome = await readSession(
      this.laser,
      SessionLinksCommand,
      {
        stream: this.stream(),
        id: wireConversation(id),
        ...(surface !== undefined ? { surface } : {})
      },
      options
    )
    if (outcome.kind !== "links") throw unexpected("links")
    return outcome.links
  }

  /** The source partitions that hold one session's records. */
  async sources(id: ConversationId, options: CallOptions = {}): Promise<SessionSourcesView> {
    const outcome = await readSession(
      this.laser,
      SessionSourcesCommand,
      { stream: this.stream(), id: wireConversation(id) },
      options
    )
    if (outcome.kind !== "sources") throw unexpected("sources")
    return outcome.sources
  }

  /** @internal */
  async registeredLane(
    id: ConversationId,
    options: CallOptions = {}
  ): Promise<readonly [number, bigint, number] | undefined> {
    const outcome = await readSession(
      this.laser,
      SessionSourcesCommand,
      { stream: this.stream(), id: wireConversation(id), laneOnly: true },
      options
    )
    if (outcome.kind !== "sources") throw unexpected("sources")
    return outcome.sources.lane
  }

  /** One page of this stream's change feed in sequence order. Page `page` of
   * size `results` holds the changes with sequence `(page - 1) * results + 1`
   * to `page * results`, so appends and pruning never shift it. Zero `results`
   * leaves the page size to the server, and the reply names the size it used. */
  async changes(
    page: number,
    results: number,
    options: CallOptions = {}
  ): Promise<SessionChangesView> {
    return this.readChanges(page, results, options)
  }

  private async readChanges(
    page: number,
    results: number,
    options: CallOptions & { readonly deadline?: number }
  ): Promise<SessionChangesView> {
    const outcome = await readSession(
      this.laser,
      SessionChangesCommand,
      {
        stream: this.stream(),
        page,
        results
      },
      options
    )
    if (outcome.kind !== "changes") throw unexpected("changes")
    return outcome.changes
  }

  /** Follow this stream's session changes from now, polling every
   * `pollEveryMs` and coalescing the changed session ids of each poll. The
   * watch starts in one read at the newest change when this call resolves, so
   * a change made after it is never missed and no retained change replays. */
  watch(pollEveryMs: number, options: CallOptions = {}): Promise<SessionWatch> {
    return SessionWatch.create(this.laser, this.stream(), pollEveryMs, options)
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

  // The id of an unlabeled submission, minted once so `id` names the
  // session `send` writes.
  private readonly fresh = ConversationId.new()

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

  /** The id of the session `send` writes: the label's derived id, or one fresh
   * id minted for this submission. */
  id(): ConversationId {
    return this.labelValue === undefined
      ? this.fresh
      : deriveSessionId(this.sessions.stream(), this.namespaceName ?? "", this.labelValue)
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

  /** Write the submitted status and the command. A labeled session that ended
   * completed or failed resumes, and one an agent or an operator canceled is
   * refused with `SessionEndedError`. When the command cannot be published
   * after the status was written, the session is ended failed, so it never
   * stays submitted with no work, and the publish error is rethrown. */
  async send(options: CallOptions = {}): Promise<Submitted> {
    throwIfAborted(options.signal, "session submit")
    const submitter = this.submitter
    if (submitter === undefined) throw new InvalidError("a submission needs `.from(submitter)`")
    const session = this.id()
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
    const handle = this.sessions.open(session).asAgent(submitter)
    if (this.labelValue !== undefined && (await handle.endedAs(options)) === "canceled") {
      throw new SessionEndedError(session.toString(), "canceled")
    }
    // An abort after the status write starts leaves the command to follow, so
    // the session never stays submitted with no work.
    return cancellable(options.signal, "session submit", () => this.write(handle, session, body))
  }

  private async write(
    handle: Session,
    session: ConversationId,
    body: Uint8Array
  ): Promise<Submitted> {
    const lane = handle.lane()
    await lane
      .status(OPERATION_SESSION)
      .withTaskState(taskStateFromCode(TaskStateName.Submitted))
      .body(body)
      .contentType(ContentType.Cbor)
      .send()
    const correlation = MintUlid.mint(CorrelationId)
    try {
      await lane
        .command(correlation, this.input)
        .withOperation(this.operationName)
        .withTarget(this.agent)
        .withMetadata(METADATA_SUBMITTED, { kind: "bool", value: true })
        .send()
    } catch (error) {
      try {
        await handle.fail({
          code: { kind: "known", name: "Internal" },
          message: `the submitted command was not published: ${error instanceof Error ? error.message : String(error)}`,
          retryable: true
        })
      } catch (ended) {
        warn(
          `Ending submitted session ${session.toString()} after its command failed to publish also failed: ${String(ended)}`
        )
      }
      throw error
    }
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
  async pause(options: CallOptions = {}): Promise<AgdxReceipt> {
    throwIfAborted(options.signal, "session control")
    const participants =
      this.named ??
      (await abortable(
        laneParticipants(this.laser, this.session),
        options.signal,
        "session control"
      ))
    return this.request(
      OPERATION_SESSION_PAUSE,
      options,
      encodeSessionPauseRequestJson({ participants })
    )
  }

  /** Ask a paused session's agents to resume. */
  async resume(options: CallOptions = {}): Promise<AgdxReceipt> {
    return this.request(OPERATION_SESSION_RESUME, options)
  }

  /** Ask the session's agents to end it as canceled at their next boundary. */
  async cancel(options: CallOptions = {}): Promise<AgdxReceipt> {
    return this.request(OPERATION_SESSION_CANCEL, options)
  }

  /** End the session as canceled without its agents, for a session whose agent
   * is gone. */
  async forceCancel(options: CallOptions = {}): Promise<AgdxReceipt> {
    return cancellable(options.signal, "session control", () =>
      this.sign(
        this.producer()
          .status(OPERATION_SESSION)
          .withTaskState(taskStateFromCode(TaskStateName.Canceled))
          .body(encodeNamed(encodeSessionEnd({ reason: "forced" })))
          .contentType(ContentType.Cbor)
          .last()
      ).sendReceipt(options)
    )
  }

  private request(operation: string, options: CallOptions, body = "{}"): Promise<AgdxReceipt> {
    return cancellable(options.signal, "session control", () =>
      this.sign(
        this.producer()
          .command(MintUlid.mint(CorrelationId), new TextEncoder().encode(body))
          .withOperation(operation)
          .contentType(ContentType.Json)
      ).sendReceipt(options)
    )
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
    this.idleTimeoutMs = positiveDuration(timeoutMs, "idle timeout")
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
   * session listed in this process's heartbeat. A labeled session or one with
   * an explicit id that already ended is refused with `SessionEndedError`
   * before anything is written. Reopen a completed or failed one with
   * `resume`. */
  begin(
    options: CallOptions = {}
  ): Promise<{ readonly session: Session; readonly lease: SessionLease }> {
    return this.open(false, options)
  }

  /** `begin` that also reopens a session that ended completed or failed: its
   * new start record follows the earlier terminal, and the next `end`, `fail`,
   * or `cancel` ends it again. A session an agent or an operator canceled
   * stays ended and is refused with `SessionEndedError`. */
  resume(
    options: CallOptions = {}
  ): Promise<{ readonly session: Session; readonly lease: SessionLease }> {
    return this.open(true, options)
  }

  // The lease is taken only after every wait returned, so an aborted begin
  // leaves no lease behind.
  private async open(
    resume: boolean,
    options: CallOptions
  ): Promise<{ readonly session: Session; readonly lease: SessionLease }> {
    throwIfAborted(options.signal, "session begin")
    const agent = this.agentId
    if (agent === undefined) throw new InvalidError("a session needs an agent")
    const named = this.explicitId !== undefined || this.label !== undefined
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
    if (named) {
      const ended = await session.endedAs(options)
      if (ended !== undefined && (!resume || ended === "canceled"))
        throw new SessionEndedError(id.toString(), ended)
    }
    const generation = await session.streamGeneration(options)
    await abortable(
      session
        .withEnvelopeAncestry(
          session
            .lane(options)
            .status(OPERATION_SESSION)
            .withTaskState(taskStateFromCode(TaskStateName.Working))
            .body(body)
            .contentType(ContentType.Cbor)
        )
        .send(options),
      options.signal,
      "session begin"
    )
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
      state: {
        revision: 0n,
        head: undefined,
        document: undefined,
        sinceSnapshot: 0,
        tail: Promise.resolve()
      },
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
  append(envelope: AgentEnvelope, options: CallOptions = {}): Promise<AgdxReceipt> {
    return cancellable(options.signal, "session append", () => {
      if (envelope.conversation.toString() !== this.conversation.toString()) {
        return Promise.reject(new InvalidError("an appended envelope must belong to this session"))
      }
      return this.laser
        .agdx(AgentTopic.Sessions, AgentId.new(envelope.source), this.conversation)
        .withLaneGuard(() => this.parts.laneGuard.check(this.laser, this.conversation, options))
        .publishEnvelope(envelope, options)
    })
  }

  /** End the session as completed. State changed since the last snapshot is
   * snapshotted first, so a reader can start from the final document, unless
   * another writer changed the document since this handle's last write, in
   * which case the session ends without writing state. */
  async end(options: CallOptions = {}): Promise<void> {
    await this.state().snapshotIfChanged(options)
    await this.terminate("Completed", {}, options)
  }

  /** End the session as failed with `error`. */
  fail(error: AgentErrorBody, options: CallOptions = {}): Promise<void> {
    return this.terminate("Failed", { error }, options)
  }

  /** End the session as canceled. */
  cancel(options: CallOptions = {}): Promise<void> {
    return this.terminate("Canceled", {}, options)
  }

  /** Run `work` inside the session and end it by the outcome: completed on
   * success, failed on a throw or a rejection, which is rethrown unchanged
   * after the failure is written. A thrown `Error` records its class name and
   * stack. When the terminal write also fails, the error from `work` is what
   * surfaces. The lease is released either way. A
   * process that dies mid-run is not captured, and the session then shows idle
   * once its heartbeat stops. */
  async run<T>(
    lease: SessionLease,
    work: (session: Session) => T | Promise<T>,
    options: CallOptions = {}
  ): Promise<T> {
    try {
      throwIfAborted(options.signal, "session run")
      let value: T
      try {
        value = await abortable(Promise.resolve(work(this)), options.signal, "session run")
      } catch (error) {
        // An aborted run stops waiting and writes no terminal, as a run whose
        // process stopped, so the session shows idle once the lease is gone.
        throwIfAborted(options.signal, "session run")
        try {
          await this.fail(failureBody(error), options)
        } catch {
          // The work error is the one that surfaces.
        }
        throw error
      }
      await this.end(options)
      return value
    } finally {
      lease.release()
    }
  }

  private async terminate(
    state: TerminalState,
    end: SessionEnd,
    options: CallOptions = {}
  ): Promise<void> {
    throwIfAborted(options.signal, "session end")
    let intent = this.parts.terminal.intent
    if (intent !== undefined && intent.state !== state) {
      throw new InvalidError(`session already ending as ${intent.state.toLowerCase()}`)
    }
    if (intent === undefined) {
      intent = { state, end, record: MintUlid.mint(RecordId) }
      this.parts.terminal.intent = intent
    }
    const status = this.lane(options)
      .status(OPERATION_SESSION)
      .withTaskState(taskStateFromCode(TaskStateName[intent.state]))
      .withRecord(intent.record)
      .body(encodeNamed(encodeSessionEnd(intent.end)))
      .contentType(ContentType.Cbor)
      .last()
    const key = this.parts.key
    await abortable(
      this.withEnvelopeAncestry(key === undefined ? status : status.signedBy(key)).send(options),
      options.signal,
      "session end"
    )
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
  async managedStateView(options: CallOptions = {}): Promise<SessionStateView | undefined> {
    throwIfAborted(options.signal, "session state read")
    if (
      !(await abortable(this.laser.capabilities(), options.signal, "session state read")).sessions
    )
      return undefined
    return this.laser.sessions(this.config).state(this.conversation, 0, options)
  }

  /** Assemble the model context under `policy` from the conversation records
   * of the session and describe it in a manifest. `policy` selects from the
   * user, agent, tool, and model response records only: lifecycle, state,
   * context bookkeeping, and the model requests the session recorded never
   * reach a prompt. Nothing is written, and a selection over the manifest's
   * fragment cap is refused before any write. */
  async assemble(policy: ContextPolicy, options: CallOptions = {}): Promise<AssembledContext> {
    throwIfAborted(options.signal, "session assemble")
    const name = contextPolicyName(policy)
    const version = contextPolicyVersion(policy)
    const fragments = await abortable(
      this.laneContext(new ConversationRecords(policy)),
      options.signal,
      "session assemble"
    )
    const generations = await abortable(
      this.topicGenerations(fragments),
      options.signal,
      "session assemble"
    )
    let tokens = 0n
    let bytes = 0n
    const frontier = new Map<string, readonly [number, number, bigint]>()
    const manifestFragments = fragments.map((message): Fragment => {
      const size = contextMessageBody(message).byteLength
      const estimate = estimateTokens(size)
      tokens += estimate
      bytes += BigInt(size)
      const key = `${String(message.topicId)}:${String(message.id.partitionId)}`
      const seen = frontier.get(key)
      if (seen === undefined || message.id.offset > seen[2])
        frontier.set(key, [message.topicId, message.id.partitionId, message.id.offset])
      const generation = generations.get(message.topic)
      return {
        kind: "message",
        at: {
          kind: "message",
          stream: message.streamId,
          topic: message.topicId,
          partition: message.id.partitionId,
          offset: message.id.offset,
          ...(generation !== undefined ? { generation } : {}),
          conversation: this.conversation.toString()
        },
        tokens: Number(estimate > U32_MAX ? U32_MAX : estimate),
        bytes: Math.min(size, Number(U32_MAX))
      }
    })
    const manifest = {
      policy: name,
      policyVersion: version,
      fragments: manifestFragments,
      tokens,
      bytes,
      frontier: [...frontier.values()].sort(
        (left, right) => left[0] - right[0] || left[1] - right[1]
      )
    }
    // Encoding validates the manifest, so an oversized selection fails here.
    encodeContextManifest(manifest)
    return AssembledContext.create(fragments, manifest)
  }

  /** Record a model request addressed to this agent, and the context it
   * received when `assembled` is given. Finish the returned call when the
   * application's provider answers. The request is marked recorded, so the
   * agent's own runtime never takes it as new work. */
  async model(
    request: ModelRequest,
    assembled?: AssembledContext,
    options: CallOptions = {}
  ): Promise<ModelCall> {
    throwIfAborted(options.signal, "session model call")
    const correlation = MintUlid.mint(CorrelationId)
    await this.writeModelRequest(correlation, request, assembled, options)
    return ModelCall.create(this, correlation, request.operation)
  }

  /** Record a tool call addressed to this agent, with `args` redacted. The
   * call is marked recorded, so the agent's own runtime never takes it as new
   * work. */
  async tool(name: string, args: unknown, options: CallOptions = {}): Promise<ToolCall> {
    throwIfAborted(options.signal, "session tool call")
    const agent = this.requireAgent()
    const correlation = MintUlid.mint(CorrelationId)
    await abortable(
      this.lane(options)
        .command(correlation, new TextEncoder().encode(JSON.stringify(this.parts.redactor(args))))
        .withOperation(OPERATION_EXECUTE_TOOL)
        .withTool(name)
        .withTarget(agent)
        .withMetadata(METADATA_RECORDED, { kind: "bool", value: true })
        .contentType(ContentType.Json)
        .send(options),
      options.signal,
      "session tool call"
    )
    return ToolCall.create(this, correlation, name)
  }

  /** Record a model call that already happened: the request, the context when
   * given, and the response. The response duration is the measured call time.
   * A response without `durationMs` records a duration of 0. */
  async recordModelCall(
    request: ModelRequest,
    response: ModelResponse,
    assembled?: AssembledContext,
    options: CallOptions = {}
  ): Promise<AgdxReceipt> {
    throwIfAborted(options.signal, "session model call")
    const correlation = MintUlid.mint(CorrelationId)
    await this.writeModelRequest(correlation, request, assembled, options)
    return this.writeResult(
      correlation,
      request.operation,
      undefined,
      response,
      response.durationMs ?? 0,
      options
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
  status(options: CallOptions = {}): Promise<SessionInfo> {
    return this.laser.sessions().get(this.conversation, options)
  }

  /** The pause and cancel requests operators sent this session on
   * `agent.control`, addressed to this handle's agent or to every agent.
   * Inside a handler the runtime's control subscription keeps them current,
   * and the first read of a session folds its retained control records.
   * Outside a runtime every call reads the retained records. The runtime only
   * records requests: the handler decides when to stop. */
  async pendingControl(options: CallOptions = {}): Promise<PendingControl> {
    return (await this.controlState(options)).flags
  }

  /** The control state of this session, from the runtime's control book when
   * it is current, otherwise folded from the retained control records.
   * @internal */
  async controlState(options: CallOptions = {}): Promise<ControlState> {
    throwIfAborted(options.signal, "session control read")
    const conversation = this.conversation.toString()
    const known = this.parts.control?.state(conversation)
    if (known !== undefined) return known
    const me = this.parts.agent?.wireId()
    const retained = await abortable(this.controlRecords(), options.signal, "session control read")
    const records = [...retained].sort(
      (left, right) =>
        compareBigint(left.timestampMicros, right.timestampMicros) ||
        left.id.partitionId - right.id.partitionId ||
        compareBigint(left.id.offset, right.id.offset)
    )
    const stream = this.laser.defaultStream
    const partitions =
      stream === undefined
        ? undefined
        : await abortable(
            this.laser[INTERNAL_TRANSPORT]().findTopicPartitionCount(stream, AGENT_CONTROL),
            options.signal,
            "session control read"
          )
    const [state, last] = foldControl(
      records.flatMap((record) => {
        const request =
          record.envelope === undefined ? undefined : controlRequest(record.envelope, me)
        return request === undefined
          ? []
          : [[[record.id.partitionId, record.id.offset] as const, request] as const]
      }),
      partitions === undefined ? undefined : controlPartition(conversation, partitions)
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
  async overBudget(options: CallOptions = {}): Promise<boolean> {
    return (await this.budgetBreach(options)) !== undefined
  }

  /** How this session passed its budget, `undefined` while within it.
   * @internal */
  async budgetBreach(options: CallOptions = {}): Promise<BudgetBreach | undefined> {
    throwIfAborted(options.signal, "session budget read")
    if (
      (await abortable(this.laser.capabilities(), options.signal, "session budget read")).sessions
    ) {
      try {
        return breachOfInfo(await this.status(options))
      } catch (error) {
        if (!indexUnusable(error)) throw error
      }
    }
    const records = await abortable(this.laneRecords(), options.signal, "session budget read")
    const onLane = new Set(
      records.flatMap((record) =>
        record.topic === AGENT_SESSIONS && record.envelope !== undefined ? [record.envelope] : []
      )
    )
    const breach = laneBreach(
      records.flatMap((record) => (record.envelope === undefined ? [] : [record.envelope])),
      (envelope) => onLane.has(envelope)
    )
    if (!breach.started && records.length > 0) {
      warn(
        `The retained lane of session ${this.conversation.toString()} no longer holds its start record, so its budget is unknown and it reads as within budget`
      )
    }
    return breach.breach
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
  parked(options: CallOptions = {}): Promise<ParkedRecords> {
    return cancellable(options.signal, "session parked read", () =>
      readParked(this.laser, this.conversation.toString())
    )
  }

  /** Whether an operator asked to cancel this session on `agent.control`, by a
   * cancel request or a forced cancel addressed to this handle's agent or to
   * every agent, on the session's control route: the `cancelRequested` flag of
   * `pendingControl`. It answers on open Apache Iggy too. */
  async cancelRequested(options: CallOptions = {}): Promise<boolean> {
    return (await this.controlState(options)).flags.cancelRequested
  }

  /** Record that `items`, recalled for `query` when there was one, entered
   * the session's context, with each item's id and score. */
  recordRetrieval(
    query: string | undefined,
    items: readonly MemoryItem[],
    options: CallOptions = {}
  ): Promise<AgdxReceipt> {
    return this.writeEvent(
      OPERATION_CONTEXT_RETRIEVED,
      encodeNamed(
        encodeContextRetrieval({
          ...(query !== undefined ? { query } : {}),
          items: items.map((item) => [item.id.toString(), item.score ?? 0] as const)
        }),
        { forceFloatNumbers: true }
      ),
      undefined,
      options
    )
  }

  /** Record that a summary replaced the covered records of the session's
   * context. */
  recordCompaction(compaction: ContextCompaction, options: CallOptions = {}): Promise<AgdxReceipt> {
    return this.writeEvent(
      OPERATION_CONTEXT_COMPACTED,
      encodeNamed(encodeContextCompaction(compaction)),
      undefined,
      options
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
    durationMs: number,
    options: CallOptions = {}
  ): Promise<AgdxReceipt> {
    throwIfAborted(options.signal, "session write")
    const agent = this.requireAgent()
    let send = this.lane(options)
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
    return abortable(send.sendReceipt(options), options.signal, "session write")
  }

  /** @internal */
  writeFailure(
    correlation: CorrelationId,
    operation: string,
    tool: string | undefined,
    error: AgentErrorBody,
    options: CallOptions = {}
  ): Promise<AgdxReceipt> {
    return cancellable(options.signal, "session write", () => {
      const agent = this.requireAgent()
      let send = this.lane(options)
        .fail(correlation, error)
        .withOperation(operation)
        .withTarget(agent)
      if (tool !== undefined) send = send.withTool(tool)
      return send.sendReceipt(options)
    })
  }

  /** @internal */
  writeEvent(
    operation: string,
    body: Uint8Array,
    correlation?: CorrelationId,
    options: CallOptions = {}
  ): Promise<AgdxReceipt> {
    return cancellable(options.signal, "session write", () => {
      let send = this.lane(options)
        .emit(body)
        .withOperation(operation)
        .contentType(ContentType.Cbor)
      if (correlation !== undefined) send = send.withCorrelation(correlation)
      return send.sendReceipt(options)
    })
  }

  /** @internal */
  mintOpId(): string {
    return crockfordEncode(mintUlidValue())
  }

  private async writeModelRequest(
    correlation: CorrelationId,
    request: ModelRequest,
    assembled: AssembledContext | undefined,
    options: CallOptions
  ): Promise<void> {
    const agent = this.requireAgent()
    let body = request.body
    try {
      const json: unknown = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(body))
      body = new TextEncoder().encode(JSON.stringify(this.parts.redactor(json)))
    } catch {
      // A body that is not JSON is published as it is.
    }
    let send = this.lane(options)
      .command(correlation, body)
      .withOperation(request.operation)
      .withTarget(agent)
      .withMetadata(METADATA_RECORDED, { kind: "bool", value: true })
      .withMetadata(METADATA_REQUEST_MODEL, { kind: "str", value: request.model })
    if (request.provider !== undefined)
      send = send.withMetadata(METADATA_PROVIDER_NAME, { kind: "str", value: request.provider })
    await abortable(send.send(options), options.signal, "session model call")
    if (assembled !== undefined) {
      await this.writeEvent(
        OPERATION_CONTEXT_ASSEMBLED,
        encodeNamed(encodeContextManifest({ ...assembled.manifest, correlation })),
        correlation,
        options
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

  // The creation time of each topic the fragments were read from, the
  // generation a fragment's source reference proves its numeric ids with.
  private async topicGenerations(
    fragments: readonly ContextMessage[]
  ): Promise<ReadonlyMap<string, bigint>> {
    const stream = this.stream()
    const transport = this.laser[INTERNAL_TRANSPORT]()
    if (transport.findSnapshotTopic === undefined)
      throw new UnsupportedError("the session lane identity is unavailable")
    const generations = new Map<string, bigint>()
    for (const message of fragments) {
      if (generations.has(message.topic)) continue
      const topic = await transport.findSnapshotTopic(stream, message.topic)
      if (topic === undefined)
        throw new InvalidError(`the topic \`${message.topic}\` does not exist`)
      generations.set(message.topic, topic.createdAtMicros)
    }
    return generations
  }

  private with(change: Partial<SessionParts>): Session {
    return new Session(this.laser, this.config, this.conversation, { ...this.parts, ...change })
  }

  /** @internal */
  lane(options: CallOptions = {}): Agdx {
    if (this.parts.agent === undefined) throw new InvalidError("this session handle has no agent")
    return this.laser
      .agdx(AgentTopic.Sessions, this.parts.agent, this.conversation)
      .withLaneGuard(() => this.parts.laneGuard.check(this.laser, this.conversation, options))
  }

  /** @internal */
  withEnvelopeAncestry(send: AgdxSend): AgdxSend {
    return send.withAncestry(
      this.parts.parent === undefined ? undefined : wireConversation(this.parts.parent),
      this.parts.root === undefined ? undefined : wireConversation(this.parts.root)
    )
  }

  /** @internal */
  async streamGeneration(options: CallOptions = {}): Promise<bigint> {
    return (await this.parts.laneGuard.check(this.laser, this.conversation, options))
      .streamGeneration
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

  /** The configured last records of the session, every kind, trimmed to the
   * configured estimated token bound of their bodies. A model prompt reads
   * `assemble`, which keeps the conversation records only. */
  async context(options: CallOptions = {}): Promise<readonly SessionTurn[]> {
    return turnsOf(
      await cancellable(options.signal, "session context", () =>
        this.laneContext(
          new Chain([
            new LastN(this.config.contextTurnBound),
            new TokenBudget(this.config.contextTokenBound)
          ])
        )
      )
    )
  }

  /** `context` under an explicit policy. */
  async contextWith(
    policy: ContextPolicy,
    options: CallOptions = {}
  ): Promise<readonly SessionTurn[]> {
    return turnsOf(
      await cancellable(options.signal, "session context", () => this.laneContext(policy))
    )
  }

  /** The topics this session's records ride: `agent.sessions` and the declared
   * topics of the stream's per-agent topic layout.
   * @internal */
  laneTopics(): readonly string[] {
    const topics: string[] = [AGENT_SESSIONS]
    const layout = this.laser[INTERNAL_LAYOUTS]().get(this.stream())
    if (layout?.kind === "perAgentTopic") {
      for (const topic of layout.topics.values()) if (!topics.includes(topic)) topics.push(topic)
    }
    return topics
  }

  /** The newest records of this session under `policy`, within the context
   * window of each partition its records land on.
   * @internal */
  laneContext(policy: ContextPolicy): Promise<readonly ContextMessage[]> {
    return ContextAssembler.create({
      conversation: this.conversation,
      acrossSubconversations: false,
      topics: this.laneTopics(),
      policy,
      fromOffsets: new Map(),
      conversationPartitions: true
    }).assemble(this.laser)
  }

  /** Every retained record of this session, read whole from the partitions
   * its records land on, for a fold that must see every record.
   * @internal */
  laneRecords(): Promise<readonly ContextMessage[]> {
    return this.replayLane(this.laneTopics(), {})
  }

  /** How this session ended, `undefined` while it is open or never started.
   * The retained lane and control route decide, as the session index folds
   * them, since they are never behind a terminal already written.
   * @internal */
  async endedAs(
    options: CallOptions = {}
  ): Promise<"completed" | "failed" | "canceled" | undefined> {
    throwIfAborted(options.signal, "session read")
    const lifecycle = new LaneLifecycle()
    for (const record of await abortable(this.laneRecords(), options.signal, "session read")) {
      if (record.topic === AGENT_SESSIONS && record.envelope !== undefined)
        lifecycle.add(record.envelope)
    }
    const forced = (await abortable(this.controlRecords(), options.signal, "session read")).some(
      (record) =>
        record.envelope?.kind === AgentKind.Status &&
        record.envelope.taskState?.kind === "known" &&
        record.envelope.taskState.name === "Canceled"
    )
    return forced ? "canceled" : lifecycle.ended
  }

  /** Every retained control record of this session, read whole from its
   * control route, the one partition of `agent.control` its requests apply
   * on.
   * @internal */
  controlRecords(): Promise<readonly ContextMessage[]> {
    return this.replayLane([AGENT_CONTROL], {})
  }

  private replayLane(
    topics: readonly string[],
    bounds: { readonly fromCheckpoint?: Checkpoint; readonly toCheckpoint?: Checkpoint }
  ): Promise<readonly ContextMessage[]> {
    return replayContext(
      {
        conversation: this.conversation,
        acrossSubconversations: false,
        topics,
        policy: new LastN(Number.MAX_SAFE_INTEGER),
        fromOffsets: new Map(),
        conversationPartitions: true,
        ...bounds
      },
      this.laser
    )
  }

  /** This session's memory in the configured namespace, scoped to the session. */
  memory(namespace: string = this.config.memoryNamespaceName): ScopedMemory {
    return this.scope.memory(namespace)
  }

  /** The knowledge graph `name`, the same graph `laser.graph` returns. */
  graph(name: string): GraphHandle {
    return this.scope.graph(name)
  }

  /** Where the session lane and the declared agent topics end right now.
   * Persist it and hand it to `turnsAt`, `turnsSince`, `stateAt`, or
   * `replay`. */
  checkpoint(options: CallOptions = {}): Promise<Checkpoint> {
    return cancellable(options.signal, "session checkpoint", () =>
      contextCheckpoint(this.laser, this.laneTopics())
    )
  }

  /** The records up to `checkpoint`. */
  async turnsAt(
    checkpoint: Checkpoint,
    options: CallOptions = {}
  ): Promise<readonly SessionTurn[]> {
    return turnsOf(
      await cancellable(options.signal, "session replay", () =>
        this.replayLane(this.laneTopics(), { toCheckpoint: checkpoint })
      )
    )
  }

  /** The records appended after `checkpoint`. */
  async turnsSince(
    checkpoint: Checkpoint,
    options: CallOptions = {}
  ): Promise<readonly SessionTurn[]> {
    return turnsOf(
      await cancellable(options.signal, "session replay", () =>
        this.replayLane(this.laneTopics(), { fromCheckpoint: checkpoint })
      )
    )
  }

  /** Fold the records up to `checkpoint`: state as it stood then. */
  async stateAt<State>(
    checkpoint: Checkpoint,
    initial: State,
    fold: (state: State, turn: SessionTurn) => State,
    options: CallOptions = {}
  ): Promise<State> {
    return (await this.turnsAt(checkpoint, options)).reduce(fold, initial)
  }

  /** Fold the records appended after `checkpoint`: bring state saved there up to date. */
  async replay<State>(
    checkpoint: Checkpoint,
    initial: State,
    fold: (state: State, turn: SessionTurn) => State,
    options: CallOptions = {}
  ): Promise<State> {
    return (await this.turnsSince(checkpoint, options)).reduce(fold, initial)
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

// Narrows a history to the conversation records of a session before the inner
// policy selects from them: no lifecycle, state, or context bookkeeping, and no
// model request the session recorded, whose body repeats an earlier context.
class ConversationRecords implements ContextPolicy {
  constructor(private readonly inner: ContextPolicy) {}

  name(): string {
    return contextPolicyName(this.inner)
  }

  version(): string {
    return contextPolicyVersion(this.inner)
  }

  select(history: readonly ContextMessage[]): readonly ContextMessage[] {
    return this.inner.select(history.filter(isConversation))
  }
}

// The timeline types of the records a model prompt holds.
const CONVERSATION_DISPLAYS: ReadonlySet<DisplayType> = new Set<DisplayType>([
  "user.message",
  "agent.handoff",
  "agent.message",
  "tool.call",
  "tool.result",
  "model.response",
  "error"
])

function isConversation(message: ContextMessage): boolean {
  const envelope = message.envelope
  if (envelope === undefined) return true
  if (envelope.kind === AgentKind.Status || envelope.kind === AgentKind.Chunk) return false
  const display = displayType({ kind: "envelope", envelope }, AGENT_SESSIONS)
  if (display === "model.request") {
    const recorded = envelope.metadata?.get(METADATA_RECORDED)
    return !(recorded?.kind === "bool" && recorded.value)
  }
  return CONVERSATION_DISPLAYS.has(display)
}

function turnsOf(messages: readonly ContextMessage[]): readonly SessionTurn[] {
  return messages.map((message) => ({
    display:
      message.envelope === undefined
        ? "agent.message"
        : displayType({ kind: "envelope", envelope: message.envelope }, AGENT_SESSIONS),
    message
  }))
}

// A failed session's error body from a thrown value: its message, and for an
// `Error` its class name and stack, as Python records an exception's type and
// traceback.
function failureBody(error: unknown): AgentErrorBody {
  const message = error instanceof Error ? error.message : String(error)
  return {
    code: { kind: "known", name: "Internal" },
    message,
    retryable: false,
    ...(error instanceof Error
      ? {
          detail: new Map([
            ["exception", { kind: "str" as const, value: error.name }],
            ["traceback", { kind: "str" as const, value: error.stack ?? "" }]
          ])
        }
      : {})
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

// A zero idle timeout or heartbeat would drive the process heartbeat at its
// floor for every session of the stream.
function positiveDuration(ms: number, name: string): number {
  if (!Number.isFinite(ms) || ms <= 0) {
    throw new InvalidError(`a session ${name} must be a positive finite number of milliseconds`)
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
