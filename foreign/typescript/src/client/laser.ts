import { Materialization, MaterializeRequest } from "../managed/materialize.js"
import { millisToMicros } from "./duration.js"
import { type CallOptions, abortable, cancellable, delay, throwIfAborted } from "./call-options.js"
import { closeProducerStatistics } from "../stream/producer-statistics.js"
import type { GroupContext } from "../stream/consumer-group.js"
import { connectOptions, type ConnectOptions } from "./connect-options.js"
import { publishOptions, type PublishOptions } from "./publish-options.js"
import {
  ApacheIggyTransport,
  type ClientOwnership,
  type IggyClient,
  type LaserTransport,
  type ManagedAnswer,
  type ManagedSendOptions,
  type ManagedTarget,
  NEVER_EXPIRE,
  type SendMessagesResponse,
  serverErrorCode
} from "../iggy/apache-iggy.js"
import { PRESENCE_ATTEMPT_MS } from "../retry.js"
import { isRetryable } from "./error-classify.js"
import { Mutex } from "../runtime/mutex.js"
import { createAgdx, type Agdx } from "../agent/agdx.js"
import { replyTopicFor, resolveAgentTopic } from "../agent/partitioning.js"
import { decodeAgentMessage, type AgentMessage } from "../agent/reliable-consumer.js"
import { type Dispatch, classify } from "../wire/dispatch.js"
import type { AppliedFrontier } from "../wire/mutation.js"
import { ReplyHub } from "../agent/replies.js"
import { ContractBuilder, scatter, scatterReport, type ScatterReport } from "../agent/contract.js"
import type { GatherPolicy } from "../agent/context.js"
import { Workflow } from "../agent/workflow.js"
import {
  aguiEvents,
  publishStateDelta,
  publishStateSnapshot,
  reconstructState,
  type AgUiEvent
} from "../bridges/agui.js"
import type { CapabilitySelector, InboxRoute, Router } from "../agent/router.js"
import { AgentScope } from "../agent/scope.js"
import { ChunkAssembler, type StreamEvent } from "../agent/assembler.js"
import {
  AgentRegistry,
  ClientMetadataRequest,
  encodePresence,
  newRegistryCache,
  type RegistryCache
} from "../agent/registry.js"
import { AsyncOnce } from "../runtime/async-once.js"
import { Stream } from "../stream/stream.js"
import { ContextScope } from "../context-scope.js"
import {
  Sessions,
  type SessionConfig,
  type SessionLayout,
  type TopicRetention
} from "../session.js"
import { LeaseRegistry } from "../agent/lease.js"
import {
  ActionKind,
  GovernorState,
  encodePolicyEvidence,
  POLICY_DECISION_OPERATION,
  type ActionGovernor,
  type GovernorMode,
  type GovernorRetention,
  type PolicyEvidence
} from "../govern.js"
import { MemoryBackend, MemoryHandle } from "../memory/handle.js"
import { MemoryTopicBuilder } from "../memory/topic.js"
import type { Embedder, Memory } from "../memory/types.js"
import { NOOP_OBSERVER, type LaserObserver } from "../observe.js"
import type { KeyRegistry, SigningKey } from "../signing.js"
import type { Topic } from "../stream/topic.js"
import { executeBatch } from "../managed/batch.js"
import { ForkHandle } from "../managed/forks.js"
import { Kv, type LeaseCalls } from "../managed/kv.js"
import { LeaseCoordinator } from "../managed/coordination.js"
import { Bindings, Projections, Schemas } from "../managed/projections.js"
import { QueryRequest, queryReplyWaitMs } from "../managed/query.js"
import {
  authzHistory,
  bindRoles,
  defineRole,
  deleteRole,
  getBindings,
  getRole,
  listRoles,
  whoami
} from "../managed/rbac.js"
import { GraphHandle } from "../managed/graph.js"
import { Watch } from "../managed/watch.js"
import {
  type AuthzHistoryReply,
  type AuthzSubject,
  type BindingsReply,
  type Role,
  type WhoamiReply,
  splitScopedResource,
  streamResource
} from "../wire/authz.js"
import type { BatchItem } from "../wire/batch.js"
import {
  AGDX_HELLO_CODE,
  AGDX_CONTROL_PUBLISH_CODE,
  AGDX_KV_LEASE_CODE,
  AGDX_KV_LEASE_RENEW_CODE,
  AGDX_SET_CLIENT_METADATA_CODE,
  CONTROL_OP_VERSION,
  QUERY_OP_VERSION
} from "../wire/codes.js"
import { QueryCommand } from "../wire/commands.js"
import {
  encodeControlEnvelope,
  type ControlCommand,
  validateControlCommand
} from "../wire/control.js"
import type { ForkInfo } from "../wire/fork.js"
import { decodeBackendAnnounce } from "../wire/hello.js"
import type { KvNamespaceInfo } from "../wire/kv.js"
import { REGISTRY_FACT_TTL_MICROS } from "../wire/limits.js"
import {
  describeQueryError,
  type Query,
  type QueryResult,
  type QueryTarget
} from "../wire/query.js"
import { AgentId, ConversationId } from "../types/ids.js"
import { redriveKey, redrivenEnvelope } from "../agent/redrive.js"
import { ownedBytes, type BytesLike } from "./bytes.js"
import {
  OPERATION_CARD,
  OPERATION_QUARANTINE,
  OPERATION_UNQUARANTINE,
  METADATA_DATA_CLASSIFICATION,
  METADATA_DELEGATED_BY,
  METADATA_PURPOSE,
  encodeAgentCard,
  validateAgentCard,
  AgentKind,
  parseAgentId,
  type AgentEnvelope,
  type AgentCard,
  type AgentId as WireAgentId,
  type AgentPresence
} from "../wire/agent.js"
import { ContentType, contentTypeCode } from "../wire/content.js"
import { AGENT_VERSION, CONTENT_TYPE } from "../wire/headers.js"
import { IDEMPOTENCY_KEY, TARGET_AGENT_ID } from "../wire/headers.js"
import type { HeaderValue } from "../stream/header-value.js"
import { AgentTopic } from "../provenance/agent-topic.js"
import {
  encodeProvenanceHeaders,
  provenancePartitionKey,
  type Provenance
} from "../provenance/provenance.js"
import {
  AGENT_CONTROL,
  AGENT_SESSIONS,
  CHANGES_TOPIC,
  CONTROL_TOPIC,
  DLQ_TOPIC,
  OPS_STREAM,
  streamOpsTopic
} from "../wire/topics.js"
import { decodeOne, encodeNamed } from "../wire/cbor.js"
import type { ChannelId } from "../wire/ids.js"
import type { SourceRef } from "../wire/graph.js"
import type { ContextMessage } from "../context.js"
import type { LogPosition } from "../wire/ids.js"
import type { AgentDeadLetter, PatchOp } from "../wire/agent.js"
import type { ConsumerGroupName } from "../types/ids.js"
import {
  CommandExecutionError,
  ConfigError,
  NoStreamError,
  PresenceConflictError,
  QueryExecutionError,
  ProtocolError,
  InvalidError,
  LaserError,
  TimeoutError,
  TransportError,
  UnsupportedError
} from "./errors.js"
import { decodeManagedReply, executeManaged, type ManagedTransport } from "./managed.js"
import { BARE_SCOPE, type ResourceScope } from "./resource-scope.js"
import {
  INTERNAL_DIALED,
  INTERNAL_GOVERN,
  INTERNAL_ENSURE_RETAINED,
  INTERNAL_LAYOUTS,
  INTERNAL_PUBLISH_CONTROL,
  INTERNAL_REPLY_HUB,
  INTERNAL_SESSION_LEASES,
  INTERNAL_TRANSPORT,
  INTERNAL_VERIFIER
} from "./internals.js"
import {
  type Capabilities,
  OPEN_CAPABILITIES,
  advertisedTopology,
  capabilityReprobeDue,
  managedCapabilitiesFrom,
  mergeCapabilities,
  isReady,
  servesConsistency
} from "./capabilities.js"

const LOCAL_CONNECTION_STRING = "iggy:iggy@127.0.0.1:8090"
// The Iggy error code a server answers an unknown command with.
const INVALID_COMMAND = 3

export interface LaserTopology {
  readonly opsStream: string
  readonly controlTopic: string
  readonly dlqTopic: string
  readonly changesTopic: string
}

export interface TopologyOverrides {
  readonly opsStream: boolean
  readonly controlTopic: boolean
  readonly dlqTopic: boolean
  readonly changesTopic: boolean
}

const DEFAULT_TOPOLOGY: LaserTopology = {
  opsStream: OPS_STREAM,
  controlTopic: CONTROL_TOPIC,
  dlqTopic: DLQ_TOPIC,
  changesTopic: CHANGES_TOPIC
}

const NO_TOPOLOGY_OVERRIDES: TopologyOverrides = {
  opsStream: false,
  controlTopic: false,
  dlqTopic: false,
  changesTopic: false
}

/** How a `Laser` names the managed resources it sends: KV, memory, lease, and
 * fence namespaces, the key registry, graph names, projection ids and index
 * names, and fork ids. `stream`, the default, scopes every name to the default
 * stream as `stream:<stream>/<name>`, sends a name that already starts with
 * `stream:` as is, and sends bare names from a handle without a default
 * stream. `bare` sends every name exactly as written, deployment wide. */
export type ResourceNaming = "stream" | "bare"

export interface LaserBuildOptions {
  readonly connectOptions: ConnectOptions
  readonly publishOptions: PublishOptions
  readonly connectionString?: string
  readonly address?: { readonly host: string; readonly port: number }
  readonly credentials?: { readonly username: string; readonly password: string }
  readonly client?: IggyClient
  readonly ownership: ClientOwnership
  readonly defaultStream?: string
  readonly capabilities?: Capabilities
  readonly governor?: {
    readonly policy: ActionGovernor
    readonly mode: GovernorMode
    readonly retention: Partial<GovernorRetention>
  }
  readonly observer: LaserObserver
  readonly verifier?: KeyRegistry
  readonly topology: LaserTopology
  readonly topologyOverrides: TopologyOverrides
  readonly resourceNaming?: ResourceNaming
}

export class LaserBuilder {
  private connectOptionsValue: Partial<ConnectOptions> = {}
  private publishOptionsValue: Partial<PublishOptions> = {}
  private connectionStringValue: string | undefined
  private addressValue: { readonly host: string; readonly port: number } | undefined
  private credentialsValue: LaserBuildOptions["credentials"]
  private clientValue: IggyClient | undefined
  private ownershipValue: ClientOwnership = "borrowed"
  private defaultStreamValue: string | undefined
  private resourceNamingValue: ResourceNaming | undefined
  private capabilitiesValue: Capabilities | undefined
  private governorValue: LaserBuildOptions["governor"]
  private observerValue: LaserObserver = NOOP_OBSERVER
  private verifierValue: KeyRegistry | undefined
  private topologyValue: LaserTopology = DEFAULT_TOPOLOGY
  private topologyOverridesValue: TopologyOverrides = NO_TOPOLOGY_OVERRIDES

  private constructor(
    private readonly connectWith: (
      options: LaserBuildOptions,
      signal: AbortSignal | undefined
    ) => Promise<Laser>
  ) {}

  /** @internal */
  static create(
    connectWith: (options: LaserBuildOptions, signal: AbortSignal | undefined) => Promise<Laser>
  ): LaserBuilder {
    return new LaserBuilder(connectWith)
  }

  /**
   * Budget for the initial connect: TCP dial, TLS handshake, login, and the managed capability probe. An expired budget rejects with `TimeoutError` naming the stage that stalled. Default: 30 seconds, or `LASER_CONNECT_TIMEOUT_MS`.
   */
  connectTimeout(timeoutMs: number): this {
    this.connectOptionsValue = { timeoutMs: timeoutMs }
    return this
  }

  publishTimeout(timeoutMs: number): this {
    this.publishOptionsValue = { ...this.publishOptionsValue, timeoutMs: timeoutMs }
    return this
  }

  publishMaxRetries(value: number): this {
    this.publishOptionsValue = { ...this.publishOptionsValue, maxRetries: value }
    return this
  }

  publishRetryBackoff(backoffMs: number): this {
    this.publishOptionsValue = { ...this.publishOptionsValue, retryBackoffMs: backoffMs }
    return this
  }

  connectionString(value: string): this {
    this.connectionStringValue = value
    return this
  }

  address(host: string, port = 8090): this {
    this.addressValue = { host, port }
    return this
  }

  credentials(username: string, password: string): this {
    this.credentialsValue = { username, password }
    return this
  }

  client(client: IggyClient, options: { readonly ownership?: ClientOwnership } = {}): this {
    this.clientValue = client
    this.ownershipValue = options.ownership ?? "borrowed"
    return this
  }

  stream(value: string): this {
    this.defaultStreamValue = value
    return this
  }

  /** How the client names the managed resources it sends. The default
   * `stream` scopes them to the default stream, `bare` sends names exactly as
   * written. */
  resourceNaming(value: ResourceNaming): this {
    this.resourceNamingValue = value
    return this
  }

  capabilities(value: Capabilities): this {
    this.capabilitiesValue = value
    return this
  }

  governor(
    policy: ActionGovernor,
    mode: GovernorMode,
    retention: Partial<GovernorRetention> = {}
  ): this {
    this.governorValue = { policy, mode, retention }
    return this
  }

  observer(value: LaserObserver): this {
    this.observerValue = value
    return this
  }

  verifier(value: KeyRegistry): this {
    this.verifierValue = value
    return this
  }

  opsStream(value: string): this {
    this.topologyValue = { ...this.topologyValue, opsStream: value }
    this.topologyOverridesValue = { ...this.topologyOverridesValue, opsStream: true }
    return this
  }

  controlTopic(value: string): this {
    this.topologyValue = { ...this.topologyValue, controlTopic: value }
    this.topologyOverridesValue = { ...this.topologyOverridesValue, controlTopic: true }
    return this
  }

  dlqTopic(value: string): this {
    this.topologyValue = { ...this.topologyValue, dlqTopic: value }
    this.topologyOverridesValue = { ...this.topologyOverridesValue, dlqTopic: true }
    return this
  }

  changesTopic(value: string): this {
    this.topologyValue = { ...this.topologyValue, changesTopic: value }
    this.topologyOverridesValue = { ...this.topologyOverridesValue, changesTopic: true }
    return this
  }

  /** Connects with the configured settings. A configuration conflict
   * rejects before any I/O, like every other failure of the connect. */
  async connect(options: CallOptions = {}): Promise<Laser> {
    const modes =
      Number(this.connectionStringValue !== undefined) +
      Number(this.addressValue !== undefined) +
      Number(this.clientValue !== undefined)
    if (modes > 1) {
      throw new ConfigError("connectionString(), address(), and client() are mutually exclusive")
    }
    if (modes === 0 && this.credentialsValue === undefined) {
      throw new ConfigError("connectionString, address+credentials, or client is required")
    }
    if (this.credentialsValue !== undefined && this.addressValue === undefined) {
      throw new ConfigError("credentials() requires address()")
    }
    if (this.addressValue !== undefined && this.credentialsValue === undefined) {
      throw new ConfigError("address() requires credentials()")
    }
    if (this.addressValue !== undefined) {
      const { host, port } = this.addressValue
      if (host.length === 0 || !Number.isInteger(port) || port <= 0 || port > 65_535) {
        throw new ConfigError("address() requires a non-empty host and a valid TCP port")
      }
    }
    const topologyEntries: readonly (readonly [string, string])[] = [
      ["opsStream", this.topologyValue.opsStream],
      ["controlTopic", this.topologyValue.controlTopic],
      ["dlqTopic", this.topologyValue.dlqTopic],
      ["changesTopic", this.topologyValue.changesTopic]
    ]
    for (const [name, value] of topologyEntries) {
      if (value.length === 0) throw new ConfigError(`${name} must not be empty`)
    }
    const settings: LaserBuildOptions = {
      connectOptions: connectOptions(this.connectOptionsValue),
      publishOptions: publishOptions(this.publishOptionsValue),
      ...(this.connectionStringValue !== undefined
        ? { connectionString: this.connectionStringValue }
        : {}),
      ...(this.addressValue !== undefined ? { address: this.addressValue } : {}),
      ...(this.credentialsValue !== undefined ? { credentials: this.credentialsValue } : {}),
      ...(this.clientValue !== undefined ? { client: this.clientValue } : {}),
      ownership: this.ownershipValue,
      ...(this.defaultStreamValue !== undefined ? { defaultStream: this.defaultStreamValue } : {}),
      ...(this.resourceNamingValue !== undefined
        ? { resourceNaming: this.resourceNamingValue }
        : {}),
      ...(this.capabilitiesValue !== undefined ? { capabilities: this.capabilitiesValue } : {}),
      ...(this.governorValue !== undefined ? { governor: this.governorValue } : {}),
      observer: this.observerValue,
      ...(this.verifierValue !== undefined ? { verifier: this.verifierValue } : {}),
      topology: this.topologyValue,
      topologyOverrides: this.topologyOverridesValue
    }
    return connectUnless(options.signal, (signal) => this.connectWith(settings, signal))
  }
}

export {
  INTERNAL_DIALED,
  INTERNAL_GOVERN,
  INTERNAL_LAYOUTS,
  INTERNAL_PUBLISH_CONTROL,
  INTERNAL_REPLY_HUB,
  INTERNAL_SESSION_LEASES,
  INTERNAL_TRANSPORT,
  INTERNAL_VERIFIER
} from "./internals.js"

interface LaserSharedState {
  readonly registryCaches: Map<string, RegistryCache>
  readonly replyHubs: Map<string, Promise<ReplyHub>>
  // Lease acquisition over a dedicated coordination connection, one per
  // root client and shared by its clones.
  readonly leases: LeaseCoordinator
  // Session liveness leases and their heartbeat timer, shared by clones.
  readonly sessionLeases: LeaseRegistry
  // The session layout declared per stream, read at every agent send.
  readonly layouts: Map<string, SessionLayout>
  // Whether this client dialed a connection string, so it can open further
  // connections of its own.
  readonly dialed: boolean
  advertisedAgent?: string
  // The replica that holds this connection's advertised presence, pinned by
  // the first advertise and released by a clear that reached it.
  presenceReplica?: number
  // Serializes advertise and clear, so one cannot overtake the other.
  readonly presenceLock: Mutex
  capabilityProbeAtMs?: number
  announcedTopology: LaserTopology
  closing?: Promise<void>
}

export type ConsumerRef =
  | { readonly kind: "group"; readonly name: ConsumerGroupName | string }
  | { readonly kind: "consumer"; readonly name: string }

export type ConsumptionStatus =
  | { readonly kind: "notYetConsumed"; readonly behindBy: bigint }
  | { readonly kind: "consumed"; readonly committed: bigint; readonly head: bigint }
  /** The agent's group committed past the record without handling it. */
  | {
      readonly kind: "skipped"
      readonly committed: bigint
      readonly head: bigint
      readonly dispatch: Dispatch
    }

// `connectionString` is the string the client dialed, absent for an injected
// client, which has nothing to dial a coordination connection with.
function newSharedState(connectionString: string | undefined): LaserSharedState {
  return {
    registryCaches: new Map(),
    replyHubs: new Map(),
    presenceLock: new Mutex(),
    leases: LeaseCoordinator.forConnection(connectionString),
    sessionLeases: new LeaseRegistry(),
    layouts: new Map(),
    dialed: connectionString !== undefined,
    announcedTopology: DEFAULT_TOPOLOGY
  }
}

// The satellites bootstrap creates beside `agent.sessions`. `agent.control`
// is never among them: provisioning creates it with an operator send grant.
// `agent.registry` is created by the first card or registry fact.
const SATELLITE_TOPICS: readonly string[] = [
  AgentTopic.Streams,
  AgentTopic.Memory,
  AgentTopic.Dlq,
  AgentTopic.Audit,
  AgentTopic.WorkflowJournal
]

// Heartbeats only prove liveness, so they expire after an hour.
const HEARTBEAT_EXPIRY_MICROS = 3_600_000_000n

function retentionSettings(retention: TopicRetention): {
  readonly messageExpiryMicros: bigint
  readonly maxTopicSize?: bigint
} {
  const expiry = retention.expiry()
  const maxSize = retention.maxSize()
  return {
    messageExpiryMicros:
      expiry === undefined
        ? NEVER_EXPIRE
        : ((micros) => (micros > 1n ? micros : 1n))(millisToMicros(expiry)),
    ...(maxSize !== undefined ? { maxTopicSize: maxSize } : {})
  }
}

function actionKind(kind: AgentKind): ActionKind {
  switch (kind) {
    case AgentKind.Command:
      return ActionKind.Command
    case AgentKind.Response:
      return ActionKind.Response
    case AgentKind.Event:
      return ActionKind.Event
    case AgentKind.Status:
      return ActionKind.Status
    case AgentKind.Error:
      return ActionKind.Error
    case AgentKind.Chunk:
      return ActionKind.Send
  }
}

function metadataString(envelope: AgentEnvelope, key: string): string | undefined {
  const value = envelope.metadata?.get(key)
  return value?.kind === "str" ? value.value : undefined
}

function metadataActionFields(envelope: AgentEnvelope): {
  readonly onBehalfOf?: string
  readonly purpose?: string
  readonly dataClassification?: string
} {
  const onBehalfOf = metadataString(envelope, METADATA_DELEGATED_BY)
  const purpose = metadataString(envelope, METADATA_PURPOSE)
  const dataClassification = metadataString(envelope, METADATA_DATA_CLASSIFICATION)
  return {
    ...(onBehalfOf !== undefined ? { onBehalfOf } : {}),
    ...(purpose !== undefined ? { purpose } : {}),
    ...(dataClassification !== undefined ? { dataClassification } : {})
  }
}

// A server that answers without a managed announcement, Apache Iggy refusing
// the command or an older server's empty body, positively lacks the managed
// surfaces. A transport failure or a reply that does not decode established
// nothing, so a group consumer must not treat the deployment as unfiltered.
async function probeCapabilities(transport: ManagedTransport): Promise<Capabilities> {
  let reply: Uint8Array
  try {
    reply = await transport.sendManaged(AGDX_HELLO_CODE, new Uint8Array())
  } catch (error) {
    return {
      ...OPEN_CAPABILITIES,
      hello: serverErrorCode(error) === INVALID_COMMAND ? "rejected" : "failed"
    }
  }
  if (reply.byteLength === 0) {
    return { ...OPEN_CAPABILITIES, hello: "rejected" }
  }
  try {
    return managedCapabilitiesFrom(decodeBackendAnnounce(reply))
  } catch {
    return { ...OPEN_CAPABILITIES, hello: "failed" }
  }
}

// A connection that opens after `signal` aborted is closed, so an abandoned
// connect leaks no socket.
async function connectUnless(
  signal: AbortSignal | undefined,
  open: (signal: AbortSignal | undefined) => Promise<Laser>
): Promise<Laser> {
  throwIfAborted(signal, "connect")
  // The signal also ends the dial sweep, so an aborted connect dials no more.
  const opening = open(signal)
  try {
    return await abortable(opening, signal, "connect")
  } catch (error) {
    if (signal?.aborted === true) void opening.then((laser) => laser.close()).catch(() => undefined)
    throw error
  }
}

/** Owns one Apache Iggy connection and addresses every stream available through it. */
export class Laser implements AsyncDisposable {
  private constructor(
    private readonly transport: LaserTransport,
    readonly defaultStream: string | undefined,
    private readonly capabilitiesOnce: AsyncOnce<Capabilities>,
    private readonly shared: LaserSharedState,
    private readonly observer: LaserObserver = NOOP_OBSERVER,
    private readonly governor?: GovernorState,
    private readonly verifier?: KeyRegistry,
    private readonly configuredTopology: LaserTopology = DEFAULT_TOPOLOGY,
    private readonly topologyOverrides: TopologyOverrides = NO_TOPOLOGY_OVERRIDES,
    private readonly configuredCapabilities: Capabilities = OPEN_CAPABILITIES,
    private readonly capabilityOverride?: Capabilities,
    private readonly ownsClosure = true
  ) {}

  // How this handle names its managed resources. Clones keep it.
  private naming: ResourceNaming = "stream"

  private namedLike(other: Laser): this {
    this.naming = other.naming
    return this
  }

  /** A clone that names managed resources under `naming`, sharing the one
   * connection. `bare` opts out of the default stream scoping. */
  withResourceNaming(naming: ResourceNaming): Laser {
    const renamed = this.withObserver(this.observer)
    renamed.naming = naming
    return renamed
  }

  /** How this handle names the managed resources it sends. */
  resourceNaming(): ResourceNaming {
    return this.naming
  }

  /** The name this handle sends for the managed resource `name`:
   * `stream:<default stream>/<name>` under `stream` naming with a default
   * stream, else `name` unchanged. A name already scoped to the default stream
   * is returned unchanged, and a `stream:` name for any other stream is scoped
   * like a local name. Re-scope with `withDefaultStream` or use `bare` naming
   * to address another stream. */
  resourceName(name: string): string {
    return this.resourceNameIn(this.defaultStream, name)
  }

  /** The default stream when this handle scopes its resources to it.
   * @internal */
  resourceStream(): string | undefined {
    return this.resourceScope(this.defaultStream)
  }

  /** `stream` when this handle scopes its resources, the gate every scoped
   * name and every lens `stream` field passes through.
   * @internal */
  resourceScope(stream: string | undefined): string | undefined {
    return this.naming === "stream" && stream !== undefined && stream.length > 0
      ? stream
      : undefined
  }

  /** `name` scoped to `stream` under this handle's naming. An empty name stays
   * empty so the caller's validation still rejects it.
   * @internal */
  resourceNameIn(stream: string | undefined, name: string): string {
    const scope = this.resourceScope(stream)
    if (scope === undefined || name.length === 0) return name
    return splitScopedResource(name)?.[0] === scope ? name : `${streamResource(scope)}/${name}`
  }

  // A handle that scopes its names to a default stream containing `/` would
  // send names every server reads as belonging to the stream before the first
  // slash, so its managed calls are refused.
  private ensureScopable(commandCode: number): void {
    const stream = this.resourceStream()
    if (stream !== undefined && stream.includes("/") && commandCode !== AGDX_HELLO_CODE) {
      throw new InvalidError(
        `default stream \`${stream}\` contains \`/\`, so managed names cannot be scoped to it, use bare resource naming`
      )
    }
  }

  /** This handle's naming, for the managed handles it builds.
   * @internal */
  resourceScopeOf(stream: string | undefined = this.defaultStream): ResourceScope {
    const scope = this.resourceScope(stream)
    if (scope === undefined) return BARE_SCOPE
    return {
      name: (local) => this.resourceNameIn(scope, local),
      local: (name) => {
        const scoped = splitScopedResource(name)
        return scoped?.[0] === scope ? scoped[1] : undefined
      },
      stream: scope
    }
  }

  /** The caller's name for `name` returned by a listing: the local part when
   * it sits under this handle's own prefix, `undefined` when it belongs to
   * another stream or, while scoping is active, to no stream. A handle that
   * does not scope keeps every name unchanged.
   * @internal */
  localResourceName(name: string): string | undefined {
    const stream = this.resourceStream()
    if (stream === undefined) return name
    const scoped = splitScopedResource(name)
    return scoped?.[0] === stream ? scoped[1] : undefined
  }

  get opsStream(): string {
    return this.topologyOverrides.opsStream
      ? this.configuredTopology.opsStream
      : this.shared.announcedTopology.opsStream
  }

  get controlTopic(): string {
    return this.topologyOverrides.controlTopic
      ? this.configuredTopology.controlTopic
      : this.shared.announcedTopology.controlTopic
  }

  get dlqTopic(): string {
    return this.topologyOverrides.dlqTopic
      ? this.configuredTopology.dlqTopic
      : this.shared.announcedTopology.dlqTopic
  }

  get changesTopic(): string {
    return this.topologyOverrides.changesTopic
      ? this.configuredTopology.changesTopic
      : this.shared.announcedTopology.changesTopic
  }

  static builder(): LaserBuilder {
    return LaserBuilder.create(async (options, signal) => {
      const deadline = Date.now() + options.connectOptions.timeoutMs
      let transport: ApacheIggyTransport
      let dialed: string | undefined
      if (options.client !== undefined) {
        transport = await ApacheIggyTransport.fromClient(
          options.client,
          options.ownership,
          options.publishOptions,
          deadline
        )
      } else if (options.address !== undefined) {
        // Percent-encoded, so the parser hands the exact secret to the login
        // and a dedicated connection dialed from the same string gets it too.
        const { username, password } = options.credentials ?? { username: "", password: "" }
        const { host, port } = options.address
        // Only an IPv6 address holds a colon, and its authority is bracketed.
        const authority = host.includes(":") && !host.startsWith("[") ? `[${host}]` : host
        dialed = `iggy+tcp://${encodeURIComponent(username)}:${encodeURIComponent(password)}@${authority}:${String(port)}`
        transport = await ApacheIggyTransport.connect(
          dialed,
          options.publishOptions,
          deadline,
          signal
        )
      } else if (options.connectionString !== undefined) {
        dialed = options.connectionString
        transport = await ApacheIggyTransport.connect(
          dialed,
          options.publishOptions,
          deadline,
          signal
        )
      } else {
        throw new ConfigError("connectionString, address+credentials, or client is required")
      }
      const laser = new Laser(
        transport,
        options.defaultStream,
        options.capabilities === undefined
          ? new AsyncOnce()
          : AsyncOnce.resolved(options.capabilities),
        newSharedState(dialed),
        options.observer,
        options.governor === undefined
          ? undefined
          : new GovernorState(
              options.governor.policy,
              options.governor.mode,
              undefined,
              options.governor.retention
            ),
        options.verifier,
        options.topology,
        options.topologyOverrides,
        options.capabilities ?? OPEN_CAPABILITIES
      )
      laser.naming = options.resourceNaming ?? "stream"
      if (options.capabilities === undefined) await laser.probeCapabilitiesBefore(deadline)
      return laser
    })
  }

  /**
   * Wrap a connected, logged-in Apache Iggy client with no default stream.
   * Chain `withDefaultStream`, `withCapabilities`, or `withObserver` to
   * configure the handle. A `borrowed` client (the default) stays open when
   * the handle closes, an `owned` one closes with it.
   */
  static fromClient(
    client: IggyClient,
    options: CallOptions & { readonly ownership?: ClientOwnership } = {}
  ): Promise<Laser> {
    return Laser.builder().client(client, options).capabilities(OPEN_CAPABILITIES).connect(options)
  }

  /**
   * Connect using an Iggy connection string. Connecting gives up after 30 seconds, or `LASER_CONNECT_TIMEOUT_MS`, with a `TimeoutError` that says whether the server never accepted the connection or never answered the login. Use `Laser.builder().connectTimeout()` for another budget.
   */
  static connect(connectionString: string, options: CallOptions = {}): Promise<Laser> {
    return connectUnless(options.signal, (signal) =>
      Laser.open(connectionString, undefined, signal)
    )
  }

  static connectWithStream(
    connectionString: string,
    stream: string,
    options: CallOptions = {}
  ): Promise<Laser> {
    return connectUnless(options.signal, (signal) => Laser.open(connectionString, stream, signal))
  }

  static async connectEnv(
    env: Readonly<Record<string, string | undefined>> = process.env,
    options: CallOptions = {}
  ): Promise<Laser> {
    const connectionString = env["LASER_CONNECTION_STRING"]
    if (connectionString === undefined || connectionString.length === 0) {
      throw new ConfigError("LASER_CONNECTION_STRING is not set")
    }
    const stream = env["LASER_STREAM"]
    return stream === undefined || stream.length === 0
      ? Laser.connect(connectionString, options)
      : Laser.connectWithStream(connectionString, stream, options)
  }

  static local(options: CallOptions = {}): Promise<Laser> {
    return Laser.connect(LOCAL_CONNECTION_STRING, options)
  }

  private static async open(
    connectionString: string,
    stream: string | undefined,
    signal: AbortSignal | undefined
  ): Promise<Laser> {
    const deadline = Date.now() + connectOptions().timeoutMs
    const transport = await ApacheIggyTransport.connect(
      connectionString,
      undefined,
      deadline,
      signal
    )
    const laser = new Laser(transport, stream, new AsyncOnce(), newSharedState(connectionString))
    await laser.probeCapabilitiesBefore(deadline)
    return laser
  }

  /**
   * A view pinned to a default data `stream` that shares this connection.
   * Disposing the view with `await using` leaves the connection open, so keep
   * the root `Laser` in scope and dispose or `close()` it, or the open socket
   * keeps the process alive. `close()` on the view closes the shared connection
   * for every view, as Rust and Python `close` do.
   */
  withDefaultStream(stream: string): Laser {
    return new Laser(
      this.transport,
      stream,
      this.capabilitiesOnce,
      this.shared,
      this.observer,
      this.governor,
      this.verifier,
      this.configuredTopology,
      this.topologyOverrides,
      this.configuredCapabilities,
      this.capabilityOverride,
      false
    ).namedLike(this)
  }

  /** A clone whose managed operations use `opsStream` instead of the ops
   * stream the deployment announced. The connection is shared. */
  withOpsStream(opsStream: string): Laser {
    return this.withTopology({ opsStream })
  }

  /** A clone whose control commands publish to `controlTopic` on the ops
   * stream. The connection is shared. */
  withControlTopic(controlTopic: string): Laser {
    return this.withTopology({ controlTopic })
  }

  /** A clone whose dead-letter capsules publish to `dlqTopic` on the ops
   * stream. The connection is shared. */
  withDlqTopic(dlqTopic: string): Laser {
    return this.withTopology({ dlqTopic })
  }

  /** A clone whose change-feed records are read from `changesTopic` on the ops
   * stream. The connection is shared. */
  withChangesTopic(changesTopic: string): Laser {
    return this.withTopology({ changesTopic })
  }

  private withTopology(override: Partial<LaserTopology>): Laser {
    const topology: LaserTopology = {
      opsStream: this.opsStream,
      controlTopic: this.controlTopic,
      dlqTopic: this.dlqTopic,
      changesTopic: this.changesTopic,
      ...override
    }
    const overrides: TopologyOverrides = {
      opsStream: this.topologyOverrides.opsStream || override.opsStream !== undefined,
      controlTopic: this.topologyOverrides.controlTopic || override.controlTopic !== undefined,
      dlqTopic: this.topologyOverrides.dlqTopic || override.dlqTopic !== undefined,
      changesTopic: this.topologyOverrides.changesTopic || override.changesTopic !== undefined
    }
    return new Laser(
      this.transport,
      this.defaultStream,
      this.capabilitiesOnce,
      this.shared,
      this.observer,
      this.governor,
      this.verifier,
      topology,
      overrides,
      this.configuredCapabilities,
      this.capabilityOverride,
      false
    ).namedLike(this)
  }

  withCapabilities(capabilities: Capabilities): Laser {
    return new Laser(
      this.transport,
      this.defaultStream,
      this.capabilitiesOnce,
      this.shared,
      this.observer,
      this.governor,
      this.verifier,
      this.configuredTopology,
      this.topologyOverrides,
      this.configuredCapabilities,
      capabilities,
      false
    ).namedLike(this)
  }

  withGovernor(
    governor: ActionGovernor,
    mode: GovernorMode,
    retention: Partial<GovernorRetention> = {}
  ): Laser {
    return new Laser(
      this.transport,
      this.defaultStream,
      this.capabilitiesOnce,
      this.shared,
      this.observer,
      new GovernorState(governor, mode, undefined, retention),
      this.verifier,
      this.configuredTopology,
      this.topologyOverrides,
      this.configuredCapabilities,
      this.capabilityOverride,
      false
    ).namedLike(this)
  }

  withObserver(observer: LaserObserver): Laser {
    return new Laser(
      this.transport,
      this.defaultStream,
      this.capabilitiesOnce,
      this.shared,
      observer,
      this.governor,
      this.verifier,
      this.configuredTopology,
      this.topologyOverrides,
      this.configuredCapabilities,
      this.capabilityOverride,
      false
    ).namedLike(this)
  }

  private async observe<T>(
    operation: string,
    attributes: Readonly<Record<string, unknown>>,
    effect: () => Promise<T>
  ): Promise<T> {
    const span = this.observer.start(operation, attributes)
    try {
      const value = await effect()
      span.end()
      return value
    } catch (error) {
      span.end(error)
      throw error
    }
  }

  private managedTransport(): ManagedTransport {
    return {
      sendManaged: (commandCode, payload, options) =>
        this.observe("laser.managed", { operation: "managed", commandCode }, () => {
          this.ensureScopable(commandCode)
          return this.transport.sendManaged(commandCode, payload, options)
        })
    }
  }

  // The managed transport whose replies name the replica that answered.
  private answeringTransport(): {
    readonly sendManagedTo: (
      commandCode: number,
      payload: Uint8Array,
      target: ManagedTarget,
      options?: ManagedSendOptions
    ) => Promise<ManagedAnswer>
  } {
    return {
      sendManagedTo: (commandCode, payload, target, options) =>
        this.observe("laser.managed", { operation: "managed", commandCode }, () => {
          this.ensureScopable(commandCode)
          const transport = this.transport
          return transport.sendManagedTo === undefined
            ? transport
                .sendManaged(commandCode, payload, { ...options, target })
                .then((reply): ManagedAnswer => ({ reply }))
            : transport.sendManagedTo(commandCode, payload, target, options)
        })
    }
  }

  /**
   * The highest mutation position this client has seen per managed surface,
   * as Rust `Laser::seen_positions`. Every KV, fork and graph reply raises it,
   * and every later read of that surface sends it, so whichever node answers
   * has applied at least what this client wrote or read before.
   */
  seenPositions(): AppliedFrontier {
    return this.transport.seenPositions?.() ?? {}
  }

  capabilities(options: CallOptions = {}): Promise<Capabilities> {
    return cancellable(options.signal, "capabilities", () => this.probedCapabilities())
  }

  private async probedCapabilities(): Promise<Capabilities> {
    if (this.capabilityOverride !== undefined) return this.capabilityOverride
    const probe = async (): Promise<Capabilities> => {
      const probed = mergeCapabilities(
        this.configuredCapabilities,
        await probeCapabilities(this.managedTransport())
      )
      this.shared.capabilityProbeAtMs = Date.now()
      return probed
    }
    let capabilities = await this.capabilitiesOnce.get(probe)
    if (capabilityReprobeDue(capabilities, this.shared.capabilityProbeAtMs, Date.now())) {
      this.capabilitiesOnce.invalidate(capabilities)
      capabilities = await this.capabilitiesOnce.get(probe)
    }
    this.applyAdvertisedTopology(capabilities)
    return capabilities
  }

  private async probeCapabilitiesBefore(deadline: number): Promise<void> {
    await this.capabilitiesOnce.get(async () => {
      let timer: ReturnType<typeof setTimeout> | undefined
      const expired = new Promise<Capabilities>((resolve) => {
        timer = setTimeout(
          () => {
            resolve({ ...OPEN_CAPABILITIES, hello: "failed" })
          },
          Math.max(0, deadline - Date.now())
        )
      })
      try {
        const probed = mergeCapabilities(
          this.configuredCapabilities,
          await Promise.race([probeCapabilities(this.managedTransport()), expired])
        )
        this.shared.capabilityProbeAtMs = Date.now()
        return probed
      } finally {
        clearTimeout(timer)
      }
    })
    await this.capabilities()
  }

  async refreshCapabilities(options: CallOptions = {}): Promise<Capabilities> {
    throwIfAborted(options.signal, "capability refresh")
    if (this.capabilityOverride !== undefined) return this.capabilityOverride
    this.capabilitiesOnce.clear()
    return this.capabilities(options)
  }

  async waitUntilReady(timeoutMs: number, options: CallOptions = {}): Promise<Capabilities> {
    const signal = options.signal
    if (!Number.isFinite(timeoutMs) || timeoutMs < 0) {
      throw new InvalidError("timeoutMs must be finite and non-negative")
    }
    const deadline = Date.now() + timeoutMs
    for (;;) {
      throwIfAborted(signal, "readiness wait")
      const capabilities = await this.refreshCapabilities(options)
      if (isReady(capabilities)) return capabilities
      if (capabilities.backends.length === 0) {
        throw new UnsupportedError("server has no managed backend descriptors", {
          surface: "readiness"
        })
      }
      if (Date.now() >= deadline) {
        throw new TimeoutError("timed out waiting for managed backend readiness", {
          cause: capabilities.backends.flatMap((candidate) => candidate.readiness.reasons)
        })
      }
      await delay(100, signal, "readiness wait")
    }
  }

  private applyAdvertisedTopology(capabilities: Capabilities): void {
    const topology = advertisedTopology(capabilities)
    if (topology === undefined) return
    this.shared.announcedTopology = {
      opsStream: topology.opsStream,
      controlTopic: topology.controlTopic,
      dlqTopic: topology.dlqTopic,
      changesTopic: topology.changesTopic
    }
  }

  /** @internal */
  [INTERNAL_TRANSPORT](): LaserTransport {
    return this.transport
  }

  /** @internal */
  [INTERNAL_VERIFIER](): KeyRegistry | undefined {
    return this.verifier
  }

  /** @internal */
  [INTERNAL_LAYOUTS](): Map<string, SessionLayout> {
    return this.shared.layouts
  }

  /** @internal */
  [INTERNAL_DIALED](): boolean {
    return this.shared.dialed
  }

  /** @internal */
  [INTERNAL_SESSION_LEASES](): LeaseRegistry {
    return this.shared.sessionLeases
  }

  /** @internal */
  [INTERNAL_PUBLISH_CONTROL](command: ControlCommand, options: CallOptions = {}): Promise<void> {
    return this.publishControl(command, undefined, undefined, options)
  }

  /** @internal */
  /** The reply hub for replies sent to `topic`, on `requester`'s declared
   * topic when a per-agent topic layout moves its replies off the lane.
   * @internal */
  [INTERNAL_REPLY_HUB](topic: string, requester?: AgentId): Promise<ReplyHub> {
    return this.replyHub(this.replyTopicFor(topic, requester))
  }

  // Where `requester` waits for replies sent to `topic` under the default
  // stream's declared layout.
  private replyTopicFor(topic: string, requester: AgentId | undefined): string {
    const stream = this.defaultStream
    return replyTopicFor(
      stream === undefined ? undefined : this.shared.layouts.get(stream),
      topic,
      requester?.asStr()
    )
  }

  /** @internal */
  [INTERNAL_GOVERN](
    action: Omit<Parameters<GovernorState["govern"]>[0], "counters">
  ): Promise<Uint8Array> {
    if (this.governor === undefined) return Promise.resolve(action.payload.slice())
    return this.governor.govern(action, (evidence) =>
      this.emitPolicyEvidence(action.stream, evidence)
    )
  }

  get client(): IggyClient {
    return this.transport.iggyClient
  }

  stream(name: string): Stream {
    return Stream.create(
      this.transport,
      name,
      (stream, topic, payload, provenance) =>
        this[INTERNAL_GOVERN]({
          kind: ActionKind.Publish,
          stream,
          topic,
          ...(provenance?.agent !== undefined ? { source: provenance.agent.asStr() } : {}),
          ...(provenance?.targetAgentId !== undefined
            ? { target: provenance.targetAgentId.asStr() }
            : {}),
          ...(provenance !== undefined ? { conversation: provenance.conversationId } : {}),
          ...(provenance?.correlationId !== undefined
            ? { correlation: provenance.correlationId }
            : {}),
          payload,
          signed: false
        }),
      async (schemaId, options) => (await this.schemas().get(schemaId, options))?.schema,
      (operation, attributes, effect) => this.observe(operation, attributes, effect),
      () => {
        this.shared.registryCaches.delete(name)
        for (const [key, hub] of this.shared.replyHubs) {
          if (!key.startsWith(`${name}\u001f`)) continue
          this.shared.replyHubs.delete(key)
          void hub.then(
            (value) => {
              value.stop()
            },
            () => undefined
          )
        }
      },
      this.groupContext(),
      (topic) => {
        const scoped = this.withDefaultStream(name)
        return MaterializeRequest.create(
          {
            scope: scoped.resourceScopeOf(),
            capabilities: () => scoped.capabilities(),
            publish: (command, stream, operationId, options) =>
              scoped.publishControl(command, stream, operationId, options),
            statusInfo: (id, options) => scoped.projections().statusInfo(id, options)
          },
          name,
          topic
        )
      }
    )
  }

  topic(name: string): Topic {
    if (this.defaultStream === undefined) {
      throw new NoStreamError(
        "topic() requires a default stream, use stream(name).topic(name) or withDefaultStream(name)"
      )
    }
    return this.stream(this.defaultStream).topic(name)
  }

  agdx(topic: string, source: AgentId, conversation: ConversationId): Agdx {
    if (this.defaultStream === undefined) {
      throw new NoStreamError(
        "agdx() requires a default stream, use connectWithStream() or withDefaultStream()"
      )
    }
    const stream = this.defaultStream
    return createAgdx(
      this.transport,
      stream,
      topic,
      source,
      conversation,
      undefined,
      (envelope, willSign, approval) =>
        this[INTERNAL_GOVERN]({
          kind: actionKind(envelope.kind),
          stream,
          topic,
          source: envelope.source,
          ...(envelope.target !== undefined ? { target: envelope.target } : {}),
          conversation,
          ...(envelope.correlation !== undefined
            ? { correlation: envelope.correlation.toString() }
            : {}),
          ...(envelope.operation !== undefined ? { operation: envelope.operation } : {}),
          ...(envelope.tool !== undefined ? { tool: envelope.tool } : {}),
          ...metadataActionFields(envelope),
          payload: envelope.body,
          signed: willSign,
          ...(approval !== undefined ? { approval } : {})
        }),
      this.verifier,
      () => this.shared.layouts.get(stream)
    )
  }

  agent(id: AgentId): AgentScope {
    return AgentScope.create(this, id)
  }

  contract(router: Router): ContractBuilder {
    return ContractBuilder.create(this, router)
  }

  workflow(name: string): Workflow {
    return Workflow.create(this, name)
  }

  /** Replace the session state document of `conversation` with `state`, a
   * JSON object, then snapshot it, on the session lane. */
  publishStateSnapshot(
    source: AgentId,
    conversation: ConversationId,
    state: Readonly<Record<string, unknown>>,
    options: CallOptions = {}
  ): Promise<void> {
    return cancellable(options.signal, "state snapshot", () =>
      publishStateSnapshot(this, source, conversation, state)
    )
  }

  /** Apply `patch`, an RFC 6902 JSON Patch array, to the session state
   * document of `conversation`. */
  publishStateDelta(
    source: AgentId,
    conversation: ConversationId,
    patch: readonly PatchOp[],
    options: CallOptions = {}
  ): Promise<void> {
    return cancellable(options.signal, "state delta", () =>
      publishStateDelta(this, source, conversation, patch)
    )
  }

  /** The session state document of `conversation`, `undefined` until a state
   * record exists. */
  reconstructState(conversation: ConversationId, options: CallOptions = {}): Promise<unknown> {
    return cancellable(options.signal, "state read", () => reconstructState(this, conversation))
  }

  aguiEvents(
    conversation: ConversationId,
    topic: string,
    options: CallOptions = {}
  ): Promise<readonly AgUiEvent[]> {
    return cancellable(options.signal, "AG-UI rendering", () =>
      aguiEvents(this, conversation, topic)
    )
  }

  /** Scatter a contract to every agent advertising the selector's skill,
   * gather the branches under `policy` within `deadlineMs`, and return the
   * reply body of each that completed. */
  scatter(
    source: AgentId,
    selector: CapabilitySelector,
    payload: BytesLike,
    inboxRoute: InboxRoute,
    policy: GatherPolicy,
    deadlineMs: number,
    options: CallOptions = {}
  ): Promise<readonly Uint8Array[]> {
    return scatter(
      this,
      source,
      selector,
      payload,
      inboxRoute,
      policy,
      deadlineMs,
      undefined,
      options
    )
  }

  /** Like `scatter`, but every finished branch's outcome, the branches the
   * policy cancelled, and whether the policy was met. */
  scatterReport(
    source: AgentId,
    selector: CapabilitySelector,
    payload: BytesLike,
    inboxRoute: InboxRoute,
    policy: GatherPolicy,
    deadlineMs: number,
    options: CallOptions = {}
  ): Promise<ScatterReport> {
    return scatterReport(
      this,
      source,
      selector,
      payload,
      inboxRoute,
      policy,
      deadlineMs,
      undefined,
      options
    )
  }

  context(conversation: ConversationId): ContextScope {
    return ContextScope.create(this, conversation)
  }

  /** The session accessor under `config`, or the defaults. Free and
   * synchronous, IO happens at the verbs. */
  sessions(config?: SessionConfig): Sessions {
    return Sessions.create(this, config)
  }

  memory(namespace: string): MemoryHandle {
    return this.memoryWith(namespace, MemoryBackend.Auto)
  }

  /** Opens durable memory on an existing caller-named topic. */
  memoryOnTopic(topic: string, stream?: string): MemoryHandle {
    return MemoryHandle.logTopic(this, topic, stream)
  }

  /** Configures a durable memory topic before opening it. */
  memoryTopic(topic: string): MemoryTopicBuilder {
    return MemoryTopicBuilder.create(this, topic)
  }

  /** Memory on an explicit backend. The vector backend needs `embedder` and
   * any other backend refuses one, both with `InvalidError` at open. */
  memoryWith(namespace: string, backend: MemoryBackend, embedder?: Embedder): MemoryHandle {
    if (backend === MemoryBackend.Vector) {
      if (embedder === undefined)
        throw new InvalidError("the vector memory backend needs an embedder")
      return MemoryHandle.governedVector(this, embedder)
    }
    if (embedder !== undefined) {
      throw new InvalidError(`the ${backend} memory backend takes no embedder`)
    }
    return MemoryHandle.log(this, namespace)
  }

  memoryCustom(memory: Memory): MemoryHandle {
    return MemoryHandle.custom(memory)
  }

  /** Creates the agent topics on the default stream, `partitions` each:
   * `agent.sessions` under `retention`, `agent.heartbeats` with a one-hour
   * expiry, and the satellites `agent.streams`, `agent.memory`, `agent.dlq`,
   * `agent.audit`, and `agent.workflow_journal`. Idempotent. A stream that
   * already exists is used as it is. `agent.control` is not created here,
   * because only operators may send to it, and the registry topic is created
   * by the first card or registry fact. The seven topics hold
   * `7 * partitions` partitions on the server, so keep `partitions` small on
   * small tiers. */
  async bootstrap(
    partitions: number,
    retention: TopicRetention,
    options: CallOptions = {}
  ): Promise<void> {
    throwIfAborted(options.signal, "bootstrap")
    const stream = this.requireDefaultStream("bootstrap()")
    await abortable(
      this.bootstrapTopics(stream, partitions, retention),
      options.signal,
      "bootstrap"
    )
  }

  private async bootstrapTopics(
    stream: string,
    partitions: number,
    retention: TopicRetention
  ): Promise<void> {
    await this.observe(
      "laser.bootstrap",
      { operation: "bootstrap", stream, partitions },
      async () => {
        await this.transport.ensureStream(stream)
        await Promise.all([
          this.ensureRetainedTopic(
            stream,
            AgentTopic.Sessions,
            partitions,
            retentionSettings(retention)
          ),
          this.ensureRetainedTopic(stream, AgentTopic.Heartbeats, partitions, {
            messageExpiryMicros: HEARTBEAT_EXPIRY_MICROS
          }),
          ...SATELLITE_TOPICS.map((topic) => this.transport.ensureTopic(stream, topic, partitions))
        ])
      }
    )
  }

  /** Creates `topic` on the default stream with `partitions` and the lane's
   * `retention`, the way bootstrap creates `agent.sessions`.
   * @internal */
  async [INTERNAL_ENSURE_RETAINED](
    topic: string,
    partitions: number,
    retention: TopicRetention
  ): Promise<void> {
    const stream = this.requireDefaultStream("bootstrap()")
    await this.ensureRetainedTopic(stream, topic, partitions, retentionSettings(retention))
  }

  private async ensureRetainedTopic(
    stream: string,
    topic: string,
    partitions: number,
    settings: { readonly messageExpiryMicros: bigint; readonly maxTopicSize?: bigint }
  ): Promise<void> {
    if (this.transport.createTopicIfAbsent !== undefined) {
      await this.transport.createTopicIfAbsent(stream, topic, partitions, settings)
    } else if (this.transport.ensureTopicWithExpiry !== undefined) {
      await this.transport.ensureTopicWithExpiry(
        stream,
        topic,
        partitions,
        settings.messageExpiryMicros
      )
    } else {
      await this.transport.ensureTopic(stream, topic, partitions)
    }
  }

  async sendAgent(
    topic: string,
    payload: BytesLike,
    provenance: Provenance,
    options: CallOptions & { readonly contentType?: ContentType } = {}
  ): Promise<void> {
    throwIfAborted(options.signal, "agent send")
    await abortable(
      this.sendAgentWithKind(topic, payload, provenance, options, ActionKind.Send),
      options.signal,
      "agent send"
    )
  }

  private async sendAgentWithKind(
    topic: string,
    payload: BytesLike,
    provenance: Provenance,
    options: CallOptions & { readonly contentType?: ContentType },
    kind: ActionKind
  ): Promise<SendMessagesResponse> {
    const stream = this.requireDefaultStream("sendAgent()")
    return this.observe(
      "laser.agent.send",
      {
        operation: kind,
        stream,
        topic,
        conversation: provenance.conversationId.toString(),
        ...(provenance.correlationId !== undefined
          ? { correlation: provenance.correlationId }
          : {}),
        ...(provenance.agent !== undefined ? { agent: provenance.agent.asStr() } : {})
      },
      async () => {
        const governedPayload = await this[INTERNAL_GOVERN]({
          kind,
          stream,
          topic,
          ...(provenance.agent !== undefined ? { source: provenance.agent.asStr() } : {}),
          ...(provenance.targetAgentId !== undefined
            ? { target: provenance.targetAgentId.asStr() }
            : {}),
          conversation: provenance.conversationId,
          ...(provenance.correlationId !== undefined
            ? { correlation: provenance.correlationId }
            : {}),
          payload: ownedBytes(payload),
          signed: false
        })
        const headers = new Map(encodeProvenanceHeaders(provenance))
        stampBroadcastAddressee(headers, topic)
        if (options.contentType !== undefined) {
          headers.set(CONTENT_TYPE, {
            kind: "uint8",
            value: contentTypeCode(options.contentType)
          })
        }
        return this.transport.sendMessageWithHeaders(
          stream,
          this.agentTopicFor(stream, topic, provenance),
          governedPayload,
          headers,
          provenancePartitionKey(provenance),
          undefined,
          { signal: options.signal }
        )
      }
    )
  }

  // A plain agent record addressed to a declared agent on `agent.sessions`
  // lands on that agent's topic under a per-agent topic layout.
  private agentTopicFor(stream: string, topic: string, provenance: Provenance): string {
    return (
      resolveAgentTopic(
        this.shared.layouts.get(stream),
        topic,
        undefined,
        provenance.targetAgentId?.asStr()
      ) ?? topic
    )
  }

  private async emitPolicyEvidence(stream: string, evidence: PolicyEvidence): Promise<void> {
    const conversation =
      evidence.conversation === undefined
        ? ConversationId.parse("00000000000000000000000000")
        : ConversationId.parse(evidence.conversation)
    const source = AgentId.new(evidence.source ?? "governor")
    await createAgdx(this.transport, stream, AgentTopic.Audit, source, conversation)
      .emit(encodePolicyEvidence(evidence))
      .withOperation(POLICY_DECISION_OPERATION)
      .contentType(ContentType.Cbor)
      .send()
  }

  spawnSubconversation(parent: Provenance, author: AgentId): Provenance {
    return {
      conversationId: ConversationId.new(),
      parentConversationId: parent.conversationId,
      rootConversationId: parent.rootConversationId ?? parent.conversationId,
      agent: author
    }
  }

  /** Read the one record `at` names, checking that the topic still has the
   * generation the reference recorded and that the returned record sits at the
   * named offset. `undefined` when the record is gone or the topic was
   * recreated. Only a message reference names a log record. */
  readAt(at: SourceRef, options: CallOptions = {}): Promise<ContextMessage | undefined> {
    return cancellable(options.signal, "record read", () => this.readRecordAt(at))
  }

  private async readRecordAt(at: SourceRef): Promise<ContextMessage | undefined> {
    if (at.kind !== "message") {
      throw new InvalidError("only a message reference names a log record")
    }
    const names = await this.transport.resolveStreamTopicNames?.(at.stream, at.topic)
    if (names === undefined) return undefined
    const details = await this.transport.findSnapshotTopic?.(names.stream, names.topic)
    if (details?.id !== at.topic) return undefined
    if (at.generation !== undefined && at.generation !== details.createdAtMicros) return undefined
    const polled = await this.transport.pollMessages(
      names.stream,
      names.topic,
      { kind: "single", partitionId: at.partition },
      { kind: "offset", value: at.offset },
      1,
      false
    )
    const message = polled.find((candidate) => candidate.offset === at.offset)
    if (message === undefined) return undefined
    const currentNames = await this.transport.resolveStreamTopicNames?.(at.stream, at.topic)
    if (currentNames?.stream !== names.stream || currentNames.topic !== names.topic) {
      return undefined
    }
    const currentDetails = await this.transport.findSnapshotTopic?.(
      currentNames.stream,
      currentNames.topic
    )
    if (
      currentDetails?.id !== details.id ||
      currentDetails.createdAtMicros !== details.createdAtMicros
    ) {
      return undefined
    }
    const decoded = decodeAgentMessage({ ...message, partitionId: at.partition })
    if (decoded.kind === "error") throw decoded.error
    return {
      id: { partitionId: at.partition, offset: at.offset },
      provenance: decoded.message.provenance,
      payload: message.payload,
      ...(decoded.message.envelope !== undefined ? { envelope: decoded.message.envelope } : {}),
      topic: names.topic,
      timestampMicros: message.timestampMicros ?? 0n,
      streamId: at.stream,
      topicId: at.topic
    }
  }

  async reassembleChannel(
    conversation: ConversationId,
    topic: string,
    channel: ChannelId,
    options: CallOptions = {}
  ): Promise<readonly StreamEvent[]> {
    const signal = options.signal
    throwIfAborted(signal, "channel reassembly")
    const cursor = (
      await abortable(this.topic(topic).replay(), signal, "channel reassembly")
    ).batch(1_000)
    const envelopes: AgentEnvelope[] = []
    for (;;) {
      const records = await cursor.pollRecords(signal === undefined ? {} : { signal })
      if (records.length === 0) break
      for (const record of records) {
        const decoded = decodeAgentMessage(record)
        if (decoded.kind !== "message") continue
        const envelope = decoded.message.envelope
        if (
          envelope?.kind === AgentKind.Chunk &&
          envelope.conversation.toString() === conversation.toString() &&
          envelope.channel?.equals(channel) === true
        ) {
          envelopes.push(envelope)
        }
      }
    }
    envelopes.sort((left, right) => {
      const a = left.sequence ?? 0n
      const b = right.sequence ?? 0n
      return a < b ? -1 : a > b ? 1 : 0
    })
    // The opening chunk's bound ends a stream its producer never finished.
    const opening = envelopes[0]
    const abandonAt = opening?.sequence === 0n ? opening.deadlineMicros : undefined
    const assembler = new ChunkAssembler()
    const events = envelopes.flatMap((envelope) => assembler.feed(envelope))
    if (abandonAt !== undefined && BigInt(Date.now()) * 1_000n > abandonAt) {
      const abandoned = assembler.abandon()
      if (abandoned !== undefined) events.push(abandoned)
    }
    return events
  }

  consumed(
    target: ConsumerRef,
    at: LogPosition,
    options: CallOptions = {}
  ): Promise<ConsumptionStatus> {
    return cancellable(options.signal, "consumption check", () => this.consumedAt(target, at))
  }

  private async consumedAt(target: ConsumerRef, at: LogPosition): Promise<ConsumptionStatus> {
    if (
      this.transport.resolveStreamTopicNames === undefined ||
      this.transport.getConsumerOffset === undefined
    ) {
      throw new UnsupportedError("the active Iggy transport cannot resolve consumer offsets", {
        surface: "stream"
      })
    }
    const names = await this.transport.resolveStreamTopicNames(at.streamId, at.topicId)
    if (names === undefined) {
      return { kind: "notYetConsumed", behindBy: at.offset + 1n }
    }
    const consumer =
      target.kind === "group"
        ? {
            kind: "group" as const,
            name: typeof target.name === "string" ? target.name : target.name.asStr()
          }
        : { kind: "consumer" as const, name: target.name }
    const offset = await this.transport.getConsumerOffset(
      names.stream,
      names.topic,
      consumer,
      at.partitionId
    )
    if (offset === undefined) return { kind: "notYetConsumed", behindBy: at.offset + 1n }
    if (offset.storedOffset < at.offset)
      return { kind: "notYetConsumed", behindBy: at.offset - offset.storedOffset }
    const dispatch =
      consumer.kind === "group" ? await this.dispatchAt(consumer.name, names, at) : undefined
    return dispatch !== undefined && dispatch !== "work"
      ? { kind: "skipped", committed: offset.storedOffset, head: offset.currentOffset, dispatch }
      : { kind: "consumed", committed: offset.storedOffset, head: offset.currentOffset }
  }

  // How the agent named by `group` classifies the record at `at`, or
  // `undefined` when the record is gone, does not decode, or the group is not
  // an agent.
  private async dispatchAt(
    group: string,
    names: { readonly stream: string; readonly topic: string },
    at: LogPosition
  ): Promise<Dispatch | undefined> {
    let me: WireAgentId
    try {
      me = parseAgentId(group)
    } catch {
      return undefined
    }
    const records = await this.transport.pollMessages(
      names.stream,
      names.topic,
      { kind: "single", partitionId: at.partitionId },
      { kind: "offset", value: at.offset },
      1,
      false
    )
    const record = records.find((candidate) => candidate.offset === at.offset)
    if (record === undefined) return undefined
    const decoded = decodeAgentMessage(record, (1n << 64n) - 1n)
    if (decoded.kind !== "message") return undefined
    const { envelope, provenance } = decoded.message
    if (envelope !== undefined) return classify(envelope, names.topic, me, "any")
    const target = provenance.targetAgentId?.asStr()
    return target !== undefined && target !== me ? "foreign" : "work"
  }

  redriveDeadLetter(capsule: AgentDeadLetter, options: CallOptions = {}): Promise<void> {
    return cancellable(options.signal, "dead-letter redrive", () => this.redrive(capsule, options))
  }

  private async redrive(capsule: AgentDeadLetter, options: CallOptions): Promise<void> {
    if (this.transport.resolveStreamTopicNames === undefined) {
      throw new UnsupportedError("the active Iggy transport cannot resolve dead-letter sources", {
        surface: "stream"
      })
    }
    const source = capsule.source
    const names = await this.transport.resolveStreamTopicNames(source.streamId, source.topicId)
    if (names === undefined) {
      throw new InvalidError("dead-letter source stream or topic no longer exists")
    }
    // Any writer of the dead-letter topic can forge a capsule, so the redrive
    // stays inside this connection's default stream.
    const expected = this.requireDefaultStream("redriveDeadLetter()")
    if (names.stream !== expected) {
      throw new InvalidError(
        `dead-letter capsule names stream \`${names.stream}\`, outside this connection's stream \`${expected}\``
      )
    }
    const records = await this.transport.pollMessages(
      names.stream,
      names.topic,
      { kind: "single", partitionId: source.partitionId },
      { kind: "offset", value: source.offset },
      1,
      false
    )
    const original = records.find((record) => record.offset === source.offset)
    if (original === undefined) {
      throw new InvalidError(
        `dead-letter source record at offset ${source.offset.toString()} is no longer on the log`
      )
    }
    const headers = new Map(original.headers)
    const idempotency = headers.get(IDEMPOTENCY_KEY)
    if (idempotency?.kind === "string") {
      headers.set(IDEMPOTENCY_KEY, {
        kind: "string",
        value: redriveKey(idempotency.value, source.partitionId, source.offset)
      })
    }
    const payload = headers.has(AGENT_VERSION)
      ? redrivenEnvelope(
          original.payload,
          source.partitionId,
          source.offset,
          BigInt(Date.now()) * 1000n
        )
      : original.payload
    const decoded = decodeAgentMessage(original)
    const partitionKey =
      decoded.kind === "message" ? provenancePartitionKey(decoded.message.provenance) : undefined
    await this.transport.sendMessageWithHeaders(
      names.stream,
      names.topic,
      payload,
      headers,
      partitionKey,
      undefined,
      { signal: options.signal }
    )
  }

  async request(
    requestTopic: string,
    replyTopic: string,
    payload: BytesLike,
    provenance: Provenance,
    timeoutMs: number,
    options: CallOptions = {}
  ): Promise<AgentMessage> {
    const signal = options.signal
    if (!Number.isFinite(timeoutMs) || timeoutMs < 0) {
      throw new InvalidError("request() timeout must be a non-negative finite number")
    }
    throwIfAborted(signal, "request")
    const correlationId = provenance.correlationId ?? ConversationId.new().toString()
    const correlated = { ...provenance, correlationId }
    const listenTopic = this.replyTopicFor(replyTopic, correlated.agent)
    const sendTopic =
      this.defaultStream === undefined
        ? requestTopic
        : this.agentTopicFor(this.defaultStream, requestTopic, correlated)
    const hub = await abortable(this.replyHub(listenTopic), signal, "request")
    const ticket = hub.subscribe(correlationId, correlated.targetAgentId?.asStr(), true, {
      session: correlated.conversationId,
      ...(correlated.agent !== undefined ? { requester: correlated.agent.asStr() } : {})
    })
    try {
      const sent = await abortable(
        this.sendAgentWithKind(requestTopic, payload, correlated, { signal }, ActionKind.Request),
        signal,
        "request"
      )
      const confirmation = sent.confirmations[0]
      if (confirmation === undefined)
        throw new ProtocolError("request send had no committed message address")
      ticket.arm(
        sendTopic === listenTopic
          ? { partitionId: confirmation.partitionId, offset: confirmation.baseOffset }
          : undefined
      )
    } catch (error) {
      ticket.cancel()
      throw error
    }
    return ticket.wait(timeoutMs, signal)
  }

  clientMetadata(): ClientMetadataRequest {
    return ClientMetadataRequest.create(this.transport)
  }

  /**
   * Advertises this connection's live presence. The presence lives on the
   * replica that received it: the first advertise pins that replica, and every
   * later advertise or clear goes there, so a client whose managed requests
   * moved to another node never leaves a stale presence behind or advertises a
   * second one elsewhere. While the pinned replica does not answer within
   * `PRESENCE_ATTEMPT_MS`, the call fails as unavailable and keeps the pin.
   */
  advertisePresence(presence: AgentPresence, options: CallOptions = {}): Promise<void> {
    return cancellable(options.signal, "presence advertise", () =>
      this.advertiseOwnPresence(presence)
    )
  }

  private async advertiseOwnPresence(presence: AgentPresence): Promise<void> {
    const requested = AgentId.new(presence.agent).asStr()
    const advertised = this.shared.advertisedAgent
    if (advertised !== undefined && advertised !== requested) {
      throw new PresenceConflictError(advertised, requested)
    }
    this.shared.advertisedAgent = requested
    await this.shared.presenceLock.runExclusive(async () => {
      const replica = await this.sendPresence(encodePresence(presence))
      if (replica !== undefined) this.shared.presenceReplica = replica
    })
  }

  /**
   * Withdraws this connection's advertised presence. The clear goes to the
   * replica that holds the presence, and while that replica does not answer
   * the call fails as unavailable and keeps the pin, never reporting a
   * presence cleared that is still advertised.
   */
  clearPresence(options: CallOptions = {}): Promise<void> {
    return cancellable(options.signal, "presence clear", () => this.clearOwnPresence())
  }

  private async clearOwnPresence(): Promise<void> {
    await this.shared.presenceLock.runExclusive(async () => {
      await this.sendPresence(new Uint8Array())
      delete this.shared.presenceReplica
      delete this.shared.advertisedAgent
    })
  }

  // One presence frame to the pinned replica, else the caller's node, naming
  // the replica that answered. A pinned replica that does not answer within the
  // attempt bound is reported unavailable, and the pin is the caller's.
  private async sendPresence(payload: Uint8Array): Promise<number | undefined> {
    const pinned = this.shared.presenceReplica
    const target: ManagedTarget =
      pinned === undefined ? { kind: "caller" } : { kind: "replica", replica: pinned }
    try {
      const answer = await this.answeringTransport().sendManagedTo(
        AGDX_SET_CLIENT_METADATA_CODE,
        payload,
        target,
        { deadline: Date.now() + PRESENCE_ATTEMPT_MS }
      )
      return answer.replica
    } catch (error) {
      if (pinned === undefined || !(error instanceof LaserError) || !isRetryable(error)) throw error
      throw new TransportError(
        `replica ${String(pinned)} holds this connection's presence and did not answer within ${String(PRESENCE_ATTEMPT_MS / 1000)}s, the presence is kept there`,
        true,
        { cause: error }
      )
    }
  }

  agentRegistry(options: CallOptions = {}): Promise<AgentRegistry> {
    return cancellable(options.signal, "agent registry", () => this.openAgentRegistry())
  }

  private async openAgentRegistry(): Promise<AgentRegistry> {
    const stream = this.requireDefaultStream("agentRegistry()")
    await this.topic(AgentTopic.Registry).ensure(1)
    let cache = this.shared.registryCaches.get(stream)
    if (cache === undefined) {
      cache = newRegistryCache()
      this.shared.registryCaches.set(stream, cache)
    }
    const cursor = await this.topic(AgentTopic.Registry).replay()
    return AgentRegistry.create(
      cursor,
      cache,
      () => this.clientMetadata(),
      undefined,
      this.verifier,
      async () => {
        const hello = (await this.capabilities()).hello
        return hello === "answered" ? "yes" : hello === "rejected" ? "no" : "unknown"
      }
    )
  }

  async publishCard(source: AgentId, card: AgentCard, options: CallOptions = {}): Promise<void> {
    const signal = options.signal
    throwIfAborted(signal, "card publish")
    validateAgentCard(card)
    const body = encodeNamed(encodeAgentCard(card))
    await abortable(this.topic(AgentTopic.Registry).ensure(1), signal, "card publish")
    await abortable(
      this.agdx(AgentTopic.Registry, source, ConversationId.new())
        .status(OPERATION_CARD)
        .body(body)
        .contentType(ContentType.Cbor)
        .send({ signal }),
      signal,
      "card publish"
    )
  }

  quarantine(operator: AgentId, agent: AgentId, options: CallOptions = {}): Promise<void> {
    return cancellable(options.signal, "quarantine", () =>
      this.publishRegistryFact(OPERATION_QUARANTINE, operator, agent, undefined, options)
    )
  }

  unquarantine(operator: AgentId, agent: AgentId, options: CallOptions = {}): Promise<void> {
    return cancellable(options.signal, "unquarantine", () =>
      this.publishRegistryFact(OPERATION_UNQUARANTINE, operator, agent, undefined, options)
    )
  }

  quarantineSigned(
    operator: AgentId,
    agent: AgentId,
    key: SigningKey,
    options: CallOptions = {}
  ): Promise<void> {
    return cancellable(options.signal, "quarantine", () =>
      this.publishRegistryFact(OPERATION_QUARANTINE, operator, agent, key, options)
    )
  }

  unquarantineSigned(
    operator: AgentId,
    agent: AgentId,
    key: SigningKey,
    options: CallOptions = {}
  ): Promise<void> {
    return cancellable(options.signal, "unquarantine", () =>
      this.publishRegistryFact(OPERATION_UNQUARANTINE, operator, agent, key, options)
    )
  }

  private async publishRegistryFact(
    operation: string,
    operator: AgentId,
    agent: AgentId,
    key: SigningKey | undefined,
    options: CallOptions
  ): Promise<void> {
    await this.topic(AgentTopic.Registry).ensure(1)
    let fact = this.agdx(AgentTopic.Registry, operator, ConversationId.new())
      .status(operation)
      .withDeadlineAtMicros(BigInt(Date.now()) * 1000n + BigInt(REGISTRY_FACT_TTL_MICROS))
      .body(new TextEncoder().encode(agent.asStr()))
    if (key !== undefined) fact = fact.signedBy(key)
    await fact.send(options)
  }

  private requireDefaultStream(operation: string): string {
    if (this.defaultStream === undefined) {
      throw new NoStreamError(
        `${operation} requires a default stream, use connectWithStream() or withDefaultStream()`
      )
    }
    return this.defaultStream
  }

  private async replyHub(topic: string): Promise<ReplyHub> {
    const stream = this.requireDefaultStream("request()")
    const key = `${stream}\u001f${topic}`
    let hub = this.shared.replyHubs.get(key)
    if (hub === undefined) {
      hub = ReplyHub.create(this.transport, stream, topic, this.observer, this.verifier)
      this.shared.replyHubs.set(key, hub)
      void hub.catch(() => {
        if (this.shared.replyHubs.get(key) === hub) this.shared.replyHubs.delete(key)
      })
    }
    return hub
  }

  /** Start a query against an operational index, sent as `resourceName`
   * names it. */
  query(index: string): QueryRequest {
    return this.queryTarget({ kind: "operational", index })
  }

  /** Start a query against an explicit target. An operational index is sent
   * as `resourceName` names it. */
  queryTarget(target: QueryTarget): QueryRequest {
    const scope = this.resourceScopeOf()
    return QueryRequest.create(
      { ...target, index: scope.name(target.index) },
      (query, options) => this.executeQuery(query, options),
      scope
    )
  }

  // What a consumer group needs to reach its filter policy and read through
  // the partition primaries.
  private groupContext(): GroupContext {
    const transport = this.transport
    return {
      transport: {
        sendManaged: this.managedTransport().sendManaged,
        joinConsumerGroup: (streamId, topicId, name) =>
          transport.joinConsumerGroup(streamId, topicId, name),
        leaveConsumerGroup: (streamId, topicId, name) =>
          transport.leaveConsumerGroup(streamId, topicId, name),
        ...(transport.joinExistingConsumerGroup !== undefined
          ? { joinExistingConsumerGroup: transport.joinExistingConsumerGroup.bind(transport) }
          : {}),
        ...(transport.sendManagedTo !== undefined
          ? { sendManagedTo: this.answeringTransport().sendManagedTo }
          : {}),
        ...(transport.partitionPrimary !== undefined
          ? { partitionPrimary: transport.partitionPrimary.bind(transport) }
          : {}),
        ...(transport.storePosition !== undefined
          ? { storePosition: transport.storePosition.bind(transport) }
          : {}),
        storeOffset: transport.storeOffset.bind(transport),
        ...(transport.getConsumerOffset !== undefined
          ? { getConsumerOffset: transport.getConsumerOffset.bind(transport) }
          : {}),
        ...(transport.openCoordinator !== undefined
          ? { openCoordinator: transport.openCoordinator.bind(transport) }
          : {}),
        ...(transport.connectsNodes !== undefined ? { connectsNodes: transport.connectsNodes } : {})
      },
      capabilities: () => this.capabilities(),
      refreshCapabilities: () => this.refreshCapabilities(),
      scope: this.resourceScopeOf()
    }
  }

  /** A handle to the managed key-value store, scoped to `namespace`. The
   * namespace is sent as `resourceName` names it. */
  kv(namespace: string): Kv {
    return Kv.create(
      this.managedTransport(),
      () => this.capabilities(),
      namespace,
      this.scopedLeases(),
      this.resourceScopeOf()
    )
  }

  // The shared lease coordinator behind this handle's own scope check, as
  // every other managed call of the handle runs behind it.
  private scopedLeases(): LeaseCalls {
    const leases = this.shared.leases
    return {
      acquire: (request, options) => {
        this.ensureScopable(AGDX_KV_LEASE_CODE)
        return leases.acquire(request, options)
      },
      renew: (request, deadlineMs, options) => {
        this.ensureScopable(AGDX_KV_LEASE_RENEW_CODE)
        return leases.renew(request, deadlineMs, options)
      }
    }
  }

  /** Every KV namespace that holds at least one entry for this caller. A
   * handle that scopes its resources to a stream lists only that stream's
   * namespaces, under the names the caller gave them. */
  kvNamespaces(options: CallOptions = {}): Promise<readonly KvNamespaceInfo[]> {
    return Kv.namespaces(
      this.managedTransport(),
      () => this.capabilities(),
      this.resourceScopeOf(),
      options
    )
  }

  executeBatch(
    ops: readonly BatchItem[],
    options: CallOptions = {}
  ): Promise<readonly Uint8Array[]> {
    return cancellable(options.signal, "batch", async () =>
      executeBatch(this.managedTransport(), await this.capabilities(), ops, options)
    )
  }

  /** A handle to one fork by id. The id is sent as `resourceName` names it. */
  fork(forkId: string): ForkHandle {
    return ForkHandle.create(
      this.managedTransport(),
      () => this.capabilities(),
      forkId,
      this.resourceScopeOf()
    )
  }

  /** Every open fork for the authenticated user. A handle that scopes its
   * resources to a stream lists only that stream's forks, under the ids the
   * caller gave them. */
  forks(options: CallOptions = {}): Promise<readonly ForkInfo[]> {
    return ForkHandle.forks(
      this.managedTransport(),
      () => this.capabilities(),
      this.resourceScopeOf(),
      options
    )
  }

  materialization(projectionId: string): Materialization {
    return Materialization.create((options) => this.projections().status(projectionId, options))
  }

  projections(): Projections {
    return Projections.create(
      this.managedTransport(),
      () => this.capabilities(),
      (command, stream, options) => this.publishControl(command, stream, undefined, options),
      this.resourceScopeOf()
    )
  }

  bindings(): Bindings {
    return Bindings.create(
      (command, stream, options) => this.publishControl(command, stream, undefined, options),
      this.resourceScopeOf()
    )
  }

  schemas(): Schemas {
    return Schemas.create(
      this.managedTransport(),
      () => this.capabilities(),
      (command, stream, options) => this.publishControl(command, stream, undefined, options),
      this.resourceScopeOf()
    )
  }

  watch(): Watch {
    return Watch.create(
      () => this.capabilities(),
      async () => {
        // Under stream tenancy each stream's change feed rides its own ops
        // topic, so a scoped handle reads its stream's topic.
        const stream = this.resourceStream()
        const topic =
          stream !== undefined && (await this.capabilities()).streamTenancy
            ? streamOpsTopic(stream, CHANGES_TOPIC)
            : this.changesTopic
        return this.stream(this.opsStream).topic(topic)
      },
      this.resourceScopeOf()
    )
  }

  graph(name: string): GraphHandle {
    return GraphHandle.create(
      this.managedTransport(),
      () => this.capabilities(),
      name,
      undefined,
      this.resourceScopeOf()
    )
  }

  whoami(options: CallOptions = {}): Promise<WhoamiReply> {
    return cancellable(options.signal, "whoami", async () =>
      whoami(this.managedTransport(), await this.capabilities(), options)
    )
  }

  listRoles(
    options: CallOptions & { readonly namePrefix?: string; readonly search?: string } = {}
  ): Promise<readonly Role[]> {
    return cancellable(options.signal, "role list", async () =>
      listRoles(this.managedTransport(), await this.capabilities(), options)
    )
  }

  getRole(name: string, options: CallOptions = {}): Promise<Role | undefined> {
    return cancellable(options.signal, "role read", async () =>
      getRole(this.managedTransport(), await this.capabilities(), name, options)
    )
  }

  getBindings(userId: number, options: CallOptions = {}): Promise<BindingsReply> {
    return cancellable(options.signal, "binding read", async () =>
      getBindings(this.managedTransport(), await this.capabilities(), userId, options)
    )
  }

  defineRole(role: Role, options: CallOptions = {}): Promise<void> {
    return cancellable(options.signal, "role define", async () =>
      defineRole(this.managedTransport(), await this.capabilities(), role, undefined, options)
    )
  }

  /** `defineRole` under a caller-chosen `mutationId`, the receipt identity of
   * the request. The server keeps the outcome of an applied mutation under
   * the account that sent it, so a repeat with the same id and content after
   * a lost reply returns the original outcome without a second effect, and the
   * same id with other content is a conflict. */
  defineRoleAs(role: Role, mutationId: string, options: CallOptions = {}): Promise<void> {
    return cancellable(options.signal, "role define", async () =>
      defineRole(this.managedTransport(), await this.capabilities(), role, mutationId, options)
    )
  }

  deleteRole(name: string, options: CallOptions = {}): Promise<void> {
    return cancellable(options.signal, "role delete", async () =>
      deleteRole(this.managedTransport(), await this.capabilities(), name, undefined, options)
    )
  }

  /** `deleteRole` under a caller-chosen `mutationId`, with the receipt
   * semantics of `defineRoleAs`. */
  deleteRoleAs(name: string, mutationId: string, options: CallOptions = {}): Promise<void> {
    return cancellable(options.signal, "role delete", async () =>
      deleteRole(this.managedTransport(), await this.capabilities(), name, mutationId, options)
    )
  }

  bindRoles(
    userId: number,
    roles: readonly string[],
    expectRevision?: bigint,
    options: CallOptions = {}
  ): Promise<BindingsReply> {
    return cancellable(options.signal, "role bind", async () =>
      bindRoles(
        this.managedTransport(),
        await this.capabilities(),
        userId,
        roles,
        expectRevision,
        undefined,
        options
      )
    )
  }

  /** `bindRoles`, with an optional expected binding revision, under a
   * caller-chosen `mutationId`, with the receipt semantics of `defineRoleAs`.
   * An administrator that revokes its own administration keeps that request's
   * receipt, so its exact repeat after a lost reply returns the original
   * success and the binding revision moves once. */
  bindRolesAs(
    userId: number,
    roles: readonly string[],
    mutationId: string,
    expectRevision?: bigint,
    options: CallOptions = {}
  ): Promise<BindingsReply> {
    return cancellable(options.signal, "role bind", async () =>
      bindRoles(
        this.managedTransport(),
        await this.capabilities(),
        userId,
        roles,
        expectRevision,
        mutationId,
        options
      )
    )
  }

  authzHistory(
    subject: AuthzSubject,
    limit: number,
    afterRevision?: bigint,
    options: CallOptions = {}
  ): Promise<AuthzHistoryReply> {
    return cancellable(options.signal, "authorization history", async () =>
      authzHistory(
        this.managedTransport(),
        await this.capabilities(),
        subject,
        limit,
        afterRevision,
        options
      )
    )
  }

  private async publishControl(
    command: ControlCommand,
    stream?: string,
    operationId?: bigint,
    options: CallOptions = {}
  ): Promise<void> {
    const signal = options.signal
    throwIfAborted(signal, "control publish")
    const capabilities = await abortable(this.capabilities(), signal, "control publish")
    if (!capabilities.managed) {
      throw new UnsupportedError("control commands require a managed backend", {
        surface: "control"
      })
    }
    // The same refusal a server of another control version answers.
    if (
      capabilities.versions !== undefined &&
      capabilities.versions.control !== CONTROL_OP_VERSION
    ) {
      const message = `control wire version must be ${String(CONTROL_OP_VERSION)}, got ${String(capabilities.versions.control)}`
      throw new CommandExecutionError(message, {
        code: { kind: "known", name: "VersionSkew" },
        message
      })
    }
    validateControlCommand(command)
    const envelope = {
      v: CONTROL_OP_VERSION,
      timestampMicros: 0n,
      command,
      ...(stream !== undefined ? { stream } : {})
    }
    const payload = encodeNamed(encodeControlEnvelope(envelope))
    const reply = await abortable(
      this.managedTransport().sendManaged(
        AGDX_CONTROL_PUBLISH_CODE,
        payload,
        operationId === undefined ? undefined : { operationId }
      ),
      signal,
      "control publish"
    )
    decodeManagedReply((bytes) => {
      const value = decodeOne(bytes, "control reply")
      if (
        value instanceof Map &&
        value.get("code") === "ok" &&
        typeof value.get("message") === "string"
      )
        return
      throw new ProtocolError("control reply did not report an accepted command")
    }, reply)
  }

  /** Executes a pre-built `Query` and returns the raw paged result. Most
   * callers want `query(index)...fetch()` instead. Needs the managed backend
   * of Laser Stack or LaserData Cloud, otherwise it fails with
   * `UnsupportedError`. */
  async executeQuery(query: Query, options: CallOptions = {}): Promise<QueryResult> {
    const signal = options.signal
    throwIfAborted(signal, "query")
    const capabilities = await abortable(this.capabilities(), signal, "query")
    const rawSql = query.rawSql
    if (
      rawSql !== undefined &&
      !capabilities.backends.some(
        (backend) =>
          backend.desiredState === "enabled" &&
          backend.query?.rawSql === true &&
          backend.query.dialects.includes(rawSql.dialect)
      )
    ) {
      throw new UnsupportedError("raw SQL dialect is not served by an enabled backend", {
        surface: "query"
      })
    }

    if (query.text !== undefined && !capabilities.query.keyword) {
      throw new UnsupportedError("keyword query is not served by this deployment", {
        surface: "query",
        feature: "keyword"
      })
    }
    if (!servesConsistency(capabilities, query.consistency)) {
      throw new UnsupportedError(
        `${query.consistency} query consistency is not served by this deployment`,
        { surface: "query", feature: "consistency" }
      )
    }
    const reply = await sendQueryUntil(query.deadlineMicros, () =>
      executeManaged(
        this.managedTransport(),
        capabilities,
        QueryCommand,
        { v: QUERY_OP_VERSION, query },
        { signal }
      )
    )
    if (reply.kind === "ok") {
      if (reply.result.context.executionId.asU128() !== query.executionId.asU128()) {
        throw new ProtocolError("query reply execution id does not match the request")
      }
      return reply.result
    }
    throw new QueryExecutionError(describeQueryError(reply.error), reply.error)
  }

  /** Closes owned transport resources. Safe to call more than once. */
  async close(): Promise<void> {
    this.shared.closing ??= (async () => {
      for (const hub of this.shared.replyHubs.values()) {
        void hub.then(
          (value) => {
            value.stop()
          },
          () => undefined
        )
      }
      this.shared.replyHubs.clear()
      this.shared.sessionLeases.close()
      await this.shared.leases.close()
      await closeProducerStatistics(this.transport)
      await this.observe("laser.close", { operation: "close" }, () => this.transport.close())
    })()
    await this.shared.closing
  }

  /**
   * Closes the connection when this is the root `Laser` from `connect*()` or
   * `builder()`. A view from `withDefaultStream` or another `with*` method only
   * borrows the connection, so disposing it is a no-op, as with Python `async with`.
   * Rust closes when the last handle is dropped, which JavaScript cannot
   * observe, so the root owns the connection here.
   */
  [Symbol.asyncDispose](): Promise<void> {
    return this.ownsClosure ? this.close() : Promise.resolve()
  }
}

/** Every record on a shared session topic carries `agdx.to`, `*` when the
 * caller named no target, so a role group bound to the addressee filter on a
 * deployment that serves group-aware reads still receives it. Open Apache Iggy
 * classifies on the client and already delivered untargeted records. Other
 * topics keep the caller's headers verbatim. */
function stampBroadcastAddressee(headers: Map<string, HeaderValue>, topic: string): void {
  if (topic !== AGENT_SESSIONS && topic !== AGENT_CONTROL) return
  if (!headers.has(TARGET_AGENT_ID)) headers.set(TARGET_AGENT_ID, { kind: "string", value: "*" })
}

/** Send one query request and wait for its reply no longer than the query's
 * deadline allows. A deadline further out than `QUERY_DEADLINE_MS` is refused,
 * because the reply could not come back before the transport stops waiting for
 * it. A timed out wait does not mean the query stopped. The server ends it at
 * its deadline. */
async function sendQueryUntil<T>(deadlineMicros: bigint, send: () => Promise<T>): Promise<T> {
  const waitMs = queryReplyWaitMs(deadlineMicros, BigInt(Date.now()) * 1000n)
  let timer: ReturnType<typeof setTimeout> | undefined
  const expired = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      reject(new TimeoutError("no query reply by the query deadline"))
    }, waitMs)
  })
  try {
    return await Promise.race([send(), expired])
  } finally {
    clearTimeout(timer)
  }
}
