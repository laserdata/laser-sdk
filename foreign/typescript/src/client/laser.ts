import { closeProducerStatistics } from "../stream/producer-statistics.js"
import type { GroupContext } from "../stream/consumer-group.js"
import { connectOptions, type ConnectOptions } from "./connect-options.js"
import { publishOptions, type PublishOptions } from "./publish-options.js"
import {
  ApacheIggyTransport,
  type ClientOwnership,
  type IggyClient,
  type LaserTransport,
  serverErrorCode
} from "../iggy/apache-iggy.js"
import { createAgdx, type Agdx } from "../agent/agdx.js"
import { decodeAgentMessage, type AgentMessage } from "../agent/reliable-consumer.js"
import { ReplyHub } from "../agent/replies.js"
import { ContractBuilder, scatter, scatterReport, type ScatterReport } from "../agent/contract.js"
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
import { Sessions, type SessionConfig } from "../session.js"
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
import { Kv } from "../managed/kv.js"
import { LeaseCoordinator } from "../managed/coordination.js"
import { Bindings, Projections, Schemas } from "../managed/projections.js"
import { QueryRequest } from "../managed/query.js"
import { Destinations } from "../managed/destinations.js"
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
import { Runs } from "../managed/runs.js"
import { Watch } from "../managed/watch.js"
import type { AuthzHistoryReply, AuthzSubject, Role, WhoamiReply } from "../wire/authz.js"
import type { BatchItem } from "../wire/batch.js"
import {
  AGDX_HELLO_CODE,
  AGDX_SET_CLIENT_METADATA_CODE,
  CONTROL_OP_VERSION,
  QUERY_OP_VERSION
} from "../wire/codes.js"
import {
  QueryCancelCommand,
  QueryCommand,
  QueryPageCommand,
  QueryStatusCommand
} from "../wire/commands.js"
import { encodeControlEnvelope, type ControlCommand } from "../wire/control.js"
import type { ForkInfo } from "../wire/fork.js"
import { decodeBackendAnnounce } from "../wire/hello.js"
import type { KvNamespaceInfo } from "../wire/kv.js"
import type { Query, QueryExecutionStatus, QueryResult, QueryTarget } from "../wire/query.js"
import type { CheckpointMutationResult, CheckpointRequestEnvelope } from "../wire/checkpoint.js"
import type { DestinationId, QueryExecutionId } from "../wire/ids.js"
import { AgentId, ConversationId } from "../types/ids.js"
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
  type AgentEnvelope,
  type AgentCard,
  type AgentPresence
} from "../wire/agent.js"
import { ContentType, contentTypeCode } from "../wire/content.js"
import { CONTENT_TYPE } from "../wire/headers.js"
import { IDEMPOTENCY_KEY } from "../wire/headers.js"
import { AgentTopic } from "../provenance/agent-topic.js"
import {
  encodeProvenanceHeaders,
  provenancePartitionKey,
  type Provenance
} from "../provenance/provenance.js"
import { CHANGES_TOPIC, CONTROL_TOPIC, DLQ_TOPIC, OPS_STREAM } from "../wire/topics.js"
import { encodeNamed } from "../wire/cbor.js"
import type { ChannelId } from "../wire/ids.js"
import type { LogPosition } from "../wire/ids.js"
import type { AgentDeadLetter } from "../wire/agent.js"
import type { ConsumerGroupName } from "../types/ids.js"
import {
  ConfigError,
  NoStreamError,
  PresenceConflictError,
  QueryExecutionError,
  ProtocolError,
  InvalidError,
  TimeoutError,
  UnsupportedError
} from "./errors.js"
import { executeManaged, type ManagedTransport } from "./managed.js"
import {
  INTERNAL_GOVERN,
  INTERNAL_REPLY_HUB,
  INTERNAL_TRANSPORT,
  INTERNAL_VERIFIER
} from "./internals.js"
import {
  type Capabilities,
  OPEN_CAPABILITIES,
  advertisedTopology,
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
  private capabilitiesValue: Capabilities | undefined
  private governorValue: LaserBuildOptions["governor"]
  private observerValue: LaserObserver = NOOP_OBSERVER
  private verifierValue: KeyRegistry | undefined
  private topologyValue: LaserTopology = DEFAULT_TOPOLOGY
  private topologyOverridesValue: TopologyOverrides = NO_TOPOLOGY_OVERRIDES

  private constructor(
    private readonly connectWith: (options: LaserBuildOptions) => Promise<Laser>
  ) {}

  /** @internal */
  static create(connectWith: (options: LaserBuildOptions) => Promise<Laser>): LaserBuilder {
    return new LaserBuilder(connectWith)
  }

  /**
   * Budget for the initial connect: TCP dial, TLS handshake, login, and the managed capability probe. An expired budget rejects with `TimeoutError` naming the stage that stalled. Default: 30 seconds, or `LASER_CONNECT_TIMEOUT_MS`.
   */
  connectTimeout(milliseconds: number): this {
    this.connectOptionsValue = { timeoutMs: milliseconds }
    return this
  }

  publishTimeout(milliseconds: number): this {
    this.publishOptionsValue = { ...this.publishOptionsValue, timeoutMs: milliseconds }
    return this
  }

  publishMaxRetries(value: number): this {
    this.publishOptionsValue = { ...this.publishOptionsValue, maxRetries: value }
    return this
  }

  publishRetryBackoff(milliseconds: number): this {
    this.publishOptionsValue = { ...this.publishOptionsValue, retryBackoffMs: milliseconds }
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
  async connect(): Promise<Laser> {
    const modes =
      Number(this.connectionStringValue !== undefined) +
      Number(this.addressValue !== undefined) +
      Number(this.clientValue !== undefined)
    if (modes > 1) {
      throw new ConfigError("connectionString(), address(), and client() are mutually exclusive")
    }
    if (this.credentialsValue !== undefined && this.addressValue === undefined) {
      throw new ConfigError("credentials() requires address()")
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
    return this.connectWith({
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
      ...(this.capabilitiesValue !== undefined ? { capabilities: this.capabilitiesValue } : {}),
      ...(this.governorValue !== undefined ? { governor: this.governorValue } : {}),
      observer: this.observerValue,
      ...(this.verifierValue !== undefined ? { verifier: this.verifierValue } : {}),
      topology: this.topologyValue,
      topologyOverrides: this.topologyOverridesValue
    })
  }
}

export {
  INTERNAL_GOVERN,
  INTERNAL_REPLY_HUB,
  INTERNAL_TRANSPORT,
  INTERNAL_VERIFIER
} from "./internals.js"

interface LaserSharedState {
  readonly registryCaches: Map<string, RegistryCache>
  readonly replyHubs: Map<string, Promise<ReplyHub>>
  // Lease acquisition over a dedicated coordination connection, one per
  // root client and shared by its clones.
  readonly leases: LeaseCoordinator
  advertisedAgent?: string
  lastCapabilities?: Capabilities
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

// `connectionString` is the string the client dialed, absent for an injected
// client, which has nothing to dial a coordination connection with.
function newSharedState(connectionString: string | undefined): LaserSharedState {
  return {
    registryCaches: new Map(),
    replyHubs: new Map(),
    leases: LeaseCoordinator.forConnection(connectionString),
    announcedTopology: DEFAULT_TOPOLOGY
  }
}

const WELL_KNOWN_AGENT_TOPICS: readonly string[] = [
  AgentTopic.Commands,
  AgentTopic.Responses,
  AgentTopic.ToolCalls,
  AgentTopic.ToolResults,
  AgentTopic.LlmIo,
  AgentTopic.HumanInput,
  AgentTopic.Audit,
  AgentTopic.WorkflowJournal,
  AgentTopic.Dlq
]

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
    return LaserBuilder.create(async (options) => {
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
        const credentials = options.credentials ?? { username: "iggy", password: "iggy" }
        const userInfo = `${encodeURIComponent(credentials.username)}:${encodeURIComponent(credentials.password)}`
        const authorityHost = options.address.host.includes(":")
          ? `[${options.address.host.replace(/^\[|\]$/g, "")}]`
          : options.address.host
        dialed = `iggy://${userInfo}@${authorityHost}:${String(options.address.port)}`
        transport = await ApacheIggyTransport.connect(dialed, options.publishOptions, deadline)
      } else {
        dialed = options.connectionString ?? LOCAL_CONNECTION_STRING
        transport = await ApacheIggyTransport.connect(dialed, options.publishOptions, deadline)
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
    options: { readonly ownership?: ClientOwnership } = {}
  ): Promise<Laser> {
    return Laser.builder().client(client, options).capabilities(OPEN_CAPABILITIES).connect()
  }

  /**
   * Connect using an Iggy connection string. Connecting gives up after 30 seconds, or `LASER_CONNECT_TIMEOUT_MS`, with a `TimeoutError` that says whether the server never accepted the connection or never answered the login. Use `Laser.builder().connectTimeout()` for another budget.
   */
  static async connect(connectionString: string): Promise<Laser> {
    const deadline = Date.now() + connectOptions().timeoutMs
    const transport = await ApacheIggyTransport.connect(connectionString, undefined, deadline)
    const laser = new Laser(transport, undefined, new AsyncOnce(), newSharedState(connectionString))
    await laser.probeCapabilitiesBefore(deadline)
    return laser
  }

  static async connectWithStream(connectionString: string, stream: string): Promise<Laser> {
    const deadline = Date.now() + connectOptions().timeoutMs
    const transport = await ApacheIggyTransport.connect(connectionString, undefined, deadline)
    const laser = new Laser(transport, stream, new AsyncOnce(), newSharedState(connectionString))
    await laser.probeCapabilitiesBefore(deadline)
    return laser
  }

  static async connectEnv(
    env: Readonly<Record<string, string | undefined>> = process.env
  ): Promise<Laser> {
    const connectionString = env["LASER_CONNECTION_STRING"]
    if (connectionString === undefined || connectionString.length === 0) {
      throw new ConfigError("LASER_CONNECTION_STRING is not set")
    }
    const stream = env["LASER_STREAM"]
    return stream === undefined || stream.length === 0
      ? Laser.connect(connectionString)
      : Laser.connectWithStream(connectionString, stream)
  }

  static async local(): Promise<Laser> {
    return Laser.connect(LOCAL_CONNECTION_STRING)
  }

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
    )
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
    )
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
    )
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
    )
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
    )
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
        this.observe("laser.managed", { operation: "managed", commandCode }, () =>
          this.transport.sendManaged(commandCode, payload, options)
        )
    }
  }

  async capabilities(): Promise<Capabilities> {
    if (this.capabilityOverride !== undefined) return this.capabilityOverride
    if (
      this.shared.lastCapabilities?.managed === false &&
      Date.now() - (this.shared.capabilityProbeAtMs ?? 0) >= 1_000
    ) {
      this.capabilitiesOnce.clear()
    }
    const capabilities = await this.capabilitiesOnce.get(async () =>
      mergeCapabilities(
        this.configuredCapabilities,
        await probeCapabilities(this.managedTransport())
      )
    )
    this.shared.lastCapabilities = capabilities
    this.shared.capabilityProbeAtMs = Date.now()
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
        return mergeCapabilities(
          this.configuredCapabilities,
          await Promise.race([probeCapabilities(this.managedTransport()), expired])
        )
      } finally {
        clearTimeout(timer)
      }
    })
    await this.capabilities()
  }

  async refreshCapabilities(): Promise<Capabilities> {
    if (this.capabilityOverride !== undefined) return this.capabilityOverride
    this.capabilitiesOnce.clear()
    return this.capabilities()
  }

  async waitUntilReady(timeoutMs: number): Promise<Capabilities> {
    if (!Number.isFinite(timeoutMs) || timeoutMs < 0) {
      throw new InvalidError("timeoutMs must be finite and non-negative")
    }
    const deadline = Date.now() + timeoutMs
    for (;;) {
      const capabilities = await this.refreshCapabilities()
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
      await new Promise((resolve) => setTimeout(resolve, 100))
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
  [INTERNAL_REPLY_HUB](topic: string): Promise<ReplyHub> {
    return this.replyHub(topic)
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
      async (schemaId) => (await this.schemas().get(schemaId))?.schema,
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
      this.groupContext()
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
      (envelope, willSign) =>
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
          signed: willSign
        }),
      this.verifier
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

  publishStateSnapshot(
    topic: string,
    source: AgentId,
    conversation: ConversationId,
    state: unknown
  ): Promise<void> {
    return publishStateSnapshot(this, topic, source, conversation, state)
  }

  publishStateDelta(
    topic: string,
    source: AgentId,
    conversation: ConversationId,
    patch: unknown
  ): Promise<void> {
    return publishStateDelta(this, topic, source, conversation, patch)
  }

  reconstructState(conversation: ConversationId, topic: string): Promise<unknown> {
    return reconstructState(this, conversation, topic)
  }

  aguiEvents(conversation: ConversationId, topic: string): Promise<readonly AgUiEvent[]> {
    return aguiEvents(this, conversation, topic)
  }

  scatter(
    source: AgentId,
    selector: CapabilitySelector,
    payload: BytesLike,
    inboxRoute: InboxRoute,
    deadlineMs: number
  ): Promise<readonly Uint8Array[]> {
    return scatter(this, source, selector, payload, inboxRoute, deadlineMs)
  }

  scatterReport(
    source: AgentId,
    selector: CapabilitySelector,
    payload: BytesLike,
    inboxRoute: InboxRoute,
    deadlineMs: number
  ): Promise<ScatterReport> {
    return scatterReport(this, source, selector, payload, inboxRoute, deadlineMs)
  }

  context(conversation: ConversationId): ContextScope {
    return ContextScope.create(this, conversation)
  }

  /** The session accessor: one conversation seen as typed turns, a model-ready
   * context, scoped memory, and checkpointed replay. Built on `context`, so a
   * session is never a second store. */
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

  memoryWith(namespace: string, backend: MemoryBackend, embedder?: Embedder): MemoryHandle {
    return backend === MemoryBackend.Vector
      ? MemoryHandle.governedVector(this, embedder)
      : MemoryHandle.log(this, namespace)
  }

  memoryCustom(memory: Memory): MemoryHandle {
    return MemoryHandle.custom(memory)
  }

  async bootstrap(partitions: number): Promise<void> {
    const stream = this.requireDefaultStream("bootstrap()")
    await this.observe(
      "laser.bootstrap",
      { operation: "bootstrap", stream, partitions },
      async () => {
        await this.transport.ensureStream(stream)
        await Promise.all(
          WELL_KNOWN_AGENT_TOPICS.map((topic) =>
            this.transport.ensureTopic(stream, topic, partitions)
          )
        )
      }
    )
  }

  async sendAgent(
    topic: string,
    payload: BytesLike,
    provenance: Provenance,
    options: { readonly contentType?: ContentType } = {}
  ): Promise<void> {
    const stream = this.requireDefaultStream("sendAgent()")
    await this.observe(
      "laser.agent.send",
      {
        operation: "send",
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
          kind: ActionKind.Send,
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
        if (options.contentType !== undefined) {
          headers.set(CONTENT_TYPE, {
            kind: "uint8",
            value: contentTypeCode(options.contentType)
          })
        }
        await this.transport.sendMessageWithHeaders(
          stream,
          topic,
          governedPayload,
          headers,
          provenancePartitionKey(provenance)
        )
      }
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

  spawnSubconversation(parent: Provenance): Provenance {
    return {
      conversationId: ConversationId.new(),
      parentConversationId: parent.conversationId,
      rootConversationId: parent.rootConversationId ?? parent.conversationId,
      ...(parent.agent !== undefined ? { agent: parent.agent } : {})
    }
  }

  async reassembleChannel(
    conversation: ConversationId,
    topic: string,
    channel: ChannelId
  ): Promise<readonly StreamEvent[]> {
    const cursor = (await this.topic(topic).replay()).batch(1_000)
    const envelopes: AgentEnvelope[] = []
    for (;;) {
      const records = await cursor.pollRecords()
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
    const assembler = new ChunkAssembler()
    return envelopes.flatMap((envelope) => assembler.feed(envelope))
  }

  async consumed(target: ConsumerRef, at: LogPosition): Promise<ConsumptionStatus> {
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
    return offset.storedOffset >= at.offset
      ? { kind: "consumed", committed: offset.storedOffset, head: offset.currentOffset }
      : { kind: "notYetConsumed", behindBy: at.offset - offset.storedOffset }
  }

  async redriveDeadLetter(capsule: AgentDeadLetter): Promise<void> {
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
        value: `${idempotency.value}/redrive/${String(source.partitionId)}-${source.offset.toString()}`
      })
    }
    const decoded = decodeAgentMessage(original)
    const partitionKey =
      decoded.kind === "message" ? provenancePartitionKey(decoded.message.provenance) : undefined
    await this.transport.sendMessageWithHeaders(
      names.stream,
      names.topic,
      original.payload,
      headers,
      partitionKey
    )
  }

  async request(
    requestTopic: string,
    replyTopic: string,
    payload: BytesLike,
    provenance: Provenance,
    timeoutMs: number,
    signal?: AbortSignal
  ): Promise<AgentMessage> {
    if (!Number.isFinite(timeoutMs) || timeoutMs < 0) {
      throw new InvalidError("request() timeout must be a non-negative finite number")
    }
    const correlationId = provenance.correlationId ?? ConversationId.new().toString()
    const correlated = { ...provenance, correlationId }
    const hub = await this.replyHub(replyTopic)
    const ticket = hub.subscribe(correlationId, correlated.targetAgentId?.asStr())
    try {
      await this.sendAgent(requestTopic, payload, correlated)
    } catch (error) {
      ticket.cancel()
      throw error
    }
    return ticket.wait(timeoutMs, signal)
  }

  clientMetadata(): ClientMetadataRequest {
    return ClientMetadataRequest.create(this.transport)
  }

  async advertisePresence(presence: AgentPresence): Promise<void> {
    const requested = AgentId.new(presence.agent).asStr()
    const advertised = this.shared.advertisedAgent
    if (advertised !== undefined && advertised !== requested) {
      throw new PresenceConflictError(advertised, requested)
    }
    this.shared.advertisedAgent = requested
    await this.managedTransport().sendManaged(
      AGDX_SET_CLIENT_METADATA_CODE,
      encodePresence(presence)
    )
  }

  async clearPresence(): Promise<void> {
    await this.managedTransport().sendManaged(AGDX_SET_CLIENT_METADATA_CODE, new Uint8Array())
    delete this.shared.advertisedAgent
  }

  async agentRegistry(): Promise<AgentRegistry> {
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
      this.verifier
    )
  }

  async publishCard(source: AgentId, card: AgentCard): Promise<void> {
    validateAgentCard(card)
    const body = encodeNamed(encodeAgentCard(card))
    await this.topic(AgentTopic.Registry).ensure(1)
    await this.agdx(AgentTopic.Registry, source, ConversationId.new())
      .status(OPERATION_CARD)
      .body(body)
      .contentType(ContentType.Cbor)
      .send()
  }

  async quarantine(operator: AgentId, agent: AgentId): Promise<void> {
    await this.publishRegistryFact(OPERATION_QUARANTINE, operator, agent)
  }

  async unquarantine(operator: AgentId, agent: AgentId): Promise<void> {
    await this.publishRegistryFact(OPERATION_UNQUARANTINE, operator, agent)
  }

  async quarantineSigned(operator: AgentId, agent: AgentId, key: SigningKey): Promise<void> {
    await this.publishRegistryFact(OPERATION_QUARANTINE, operator, agent, key)
  }

  async unquarantineSigned(operator: AgentId, agent: AgentId, key: SigningKey): Promise<void> {
    await this.publishRegistryFact(OPERATION_UNQUARANTINE, operator, agent, key)
  }

  private async publishRegistryFact(
    operation: string,
    operator: AgentId,
    agent: AgentId,
    key?: SigningKey
  ): Promise<void> {
    await this.topic(AgentTopic.Registry).ensure(1)
    let fact = this.agdx(AgentTopic.Registry, operator, ConversationId.new())
      .status(operation)
      .body(new TextEncoder().encode(agent.asStr()))
    if (key !== undefined) fact = fact.signedBy(key)
    await fact.send()
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

  query(index: string): QueryRequest {
    return this.queryTarget({ kind: "operational", index })
  }

  queryTarget(target: QueryTarget): QueryRequest {
    return QueryRequest.create(
      target,
      (query) => this.executeQuery(query),
      (executionId) => this.queryStatus(executionId),
      (executionId) => this.cancelQuery(executionId)
    )
  }

  queryLakehouse(destinationId: DestinationId, destinationGeneration: bigint): QueryRequest {
    return this.queryTarget({ kind: "lakehouse", destinationId, destinationGeneration })
  }

  destinations(): Destinations {
    return Destinations.create(this.managedTransport(), () => this.capabilities())
  }

  /** Sends one checkpoint request envelope, the deep form behind every
   * `destinations()` mutation. */
  executeCheckpoint(request: CheckpointRequestEnvelope): Promise<CheckpointMutationResult> {
    return this.destinations().executeCheckpoint(request)
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
        ...(transport.openNodeConnection !== undefined
          ? { openNodeConnection: transport.openNodeConnection.bind(transport) }
          : {}),
        ...(transport.clusterNodeCount !== undefined
          ? { clusterNodeCount: transport.clusterNodeCount.bind(transport) }
          : {}),
        ...(transport.openCoordinator !== undefined
          ? { openCoordinator: transport.openCoordinator.bind(transport) }
          : {}),
        ...(transport.connectsNodes !== undefined ? { connectsNodes: transport.connectsNodes } : {})
      },
      capabilities: () => this.capabilities(),
      refreshCapabilities: () => this.refreshCapabilities()
    }
  }

  kv(namespace: string): Kv {
    return Kv.create(
      this.managedTransport(),
      () => this.capabilities(),
      namespace,
      this.shared.leases
    )
  }

  async kvNamespaces(): Promise<readonly KvNamespaceInfo[]> {
    return Kv.namespaces(this.managedTransport(), () => this.capabilities())
  }

  async executeBatch(ops: readonly BatchItem[]): Promise<readonly Uint8Array[]> {
    const capabilities = await this.capabilities()
    return executeBatch(this.managedTransport(), capabilities, ops)
  }

  fork(forkId: string): ForkHandle {
    return ForkHandle.create(this.managedTransport(), () => this.capabilities(), forkId)
  }

  async forks(): Promise<readonly ForkInfo[]> {
    return ForkHandle.forks(this.managedTransport(), () => this.capabilities())
  }

  projections(): Projections {
    return Projections.create(
      this.managedTransport(),
      () => this.capabilities(),
      (command) => this.publishControl(command)
    )
  }

  bindings(): Bindings {
    return Bindings.create((command) => this.publishControl(command))
  }

  schemas(): Schemas {
    return Schemas.create(
      this.managedTransport(),
      () => this.capabilities(),
      (command) => this.publishControl(command)
    )
  }

  runs(): Runs {
    return Runs.create(
      this.managedTransport(),
      () => this.capabilities(),
      (command) => this.publishControl(command)
    )
  }

  watch(): Watch {
    return Watch.create(
      () => this.capabilities(),
      () => this.stream(this.opsStream).topic(this.changesTopic).replay()
    )
  }

  graph(name: string): GraphHandle {
    return GraphHandle.create(this.managedTransport(), () => this.capabilities(), name)
  }

  async whoami(): Promise<WhoamiReply> {
    return whoami(this.managedTransport(), await this.capabilities())
  }

  async listRoles(options?: {
    readonly namePrefix?: string
    readonly search?: string
  }): Promise<readonly Role[]> {
    return listRoles(this.managedTransport(), await this.capabilities(), options)
  }

  async getRole(name: string): Promise<Role | undefined> {
    return getRole(this.managedTransport(), await this.capabilities(), name)
  }

  async getBindings(userId: number): Promise<readonly string[]> {
    return getBindings(this.managedTransport(), await this.capabilities(), userId)
  }

  async defineRole(role: Role): Promise<void> {
    return defineRole(this.managedTransport(), await this.capabilities(), role)
  }

  async deleteRole(name: string): Promise<void> {
    return deleteRole(this.managedTransport(), await this.capabilities(), name)
  }

  async bindRoles(
    userId: number,
    roles: readonly string[],
    expectRevision?: bigint
  ): Promise<void> {
    return bindRoles(
      this.managedTransport(),
      await this.capabilities(),
      userId,
      roles,
      expectRevision
    )
  }

  async authzHistory(
    subject: AuthzSubject,
    limit: number,
    afterRevision?: bigint
  ): Promise<AuthzHistoryReply> {
    return authzHistory(
      this.managedTransport(),
      await this.capabilities(),
      subject,
      limit,
      afterRevision
    )
  }

  private async publishControl(command: ControlCommand): Promise<void> {
    const envelope = {
      v: CONTROL_OP_VERSION,
      timestampMicros: BigInt(Date.now()) * 1000n,
      command
    }
    const payload = encodeNamed(encodeControlEnvelope(envelope))
    await this.stream(this.opsStream)
      .topic(this.controlTopic)
      .send(payload, { key: new TextEncoder().encode("control") })
  }

  /** Executes a pre-built `Query` and returns the raw paged result. Most
   * callers want `query(index)...fetch()` instead. Needs `laser-plane` in Laser
   * Stack or LaserData Cloud, otherwise it fails with `UnsupportedError`. */
  async executeQuery(query: Query): Promise<QueryResult> {
    const capabilities = await this.capabilities()
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
    const reply = await executeManaged(this.managedTransport(), capabilities, QueryCommand, {
      v: QUERY_OP_VERSION,
      query
    })
    if (reply.kind === "ok") {
      if (reply.result.context.executionId.asU128() !== query.executionId.asU128()) {
        throw new ProtocolError("query reply execution id does not match the request")
      }
      return reply.result
    }
    throw new QueryExecutionError(`query failed: ${reply.error.kind}`, reply.error)
  }

  /** Retrieves the next cursor page of an executing query. Needs the
   * `cursorPaging` capability. */
  async queryPage(
    executionId: QueryExecutionId,
    cursor: string,
    deadlineMicros: bigint
  ): Promise<QueryResult> {
    const capabilities = await this.capabilities()
    if (!capabilities.query.cursorPaging) {
      throw new UnsupportedError("query cursor paging is not advertised by this deployment", {
        surface: "query",
        feature: "cursor_paging"
      })
    }
    const reply = await executeManaged(this.managedTransport(), capabilities, QueryPageCommand, {
      v: QUERY_OP_VERSION,
      executionId,
      cursor,
      deadlineMicros
    })
    if (reply.kind === "ok") {
      if (reply.result.context.executionId.asU128() !== executionId.asU128()) {
        throw new ProtocolError("query reply execution id does not match the request")
      }
      return reply.result
    }
    throw new QueryExecutionError(`query page failed: ${reply.error.kind}`, reply.error)
  }

  /** Reads the current state of an executing or recently completed query. */
  async queryStatus(executionId: QueryExecutionId): Promise<QueryExecutionStatus> {
    const capabilities = await this.capabilities()
    if (!capabilities.query.executionStatus) {
      throw new UnsupportedError("query execution status is not served by this deployment", {
        surface: "query",
        feature: "execution_status"
      })
    }
    const reply = await executeManaged(this.managedTransport(), capabilities, QueryStatusCommand, {
      v: QUERY_OP_VERSION,
      executionId
    })
    if (reply.kind === "ok") {
      if (reply.status.executionId.asU128() !== executionId.asU128()) {
        throw new ProtocolError("query status execution id does not match the request")
      }
      return reply.status
    }
    throw new QueryExecutionError(`query status failed: ${reply.error.kind}`, reply.error)
  }

  /** Cancels an executing query by its unguessable identity. */
  async cancelQuery(executionId: QueryExecutionId): Promise<QueryExecutionStatus> {
    const capabilities = await this.capabilities()
    if (!capabilities.query.cancellation) {
      throw new UnsupportedError("query cancellation is not served by this deployment", {
        surface: "query",
        feature: "cancellation"
      })
    }
    const reply = await executeManaged(this.managedTransport(), capabilities, QueryCancelCommand, {
      v: QUERY_OP_VERSION,
      executionId
    })
    if (reply.kind === "ok") {
      if (reply.status.executionId.asU128() !== executionId.asU128()) {
        throw new ProtocolError("query status execution id does not match the request")
      }
      return reply.status
    }
    throw new QueryExecutionError(`query cancellation failed: ${reply.error.kind}`, reply.error)
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
      await this.shared.leases.close()
      await closeProducerStatistics(this.transport)
      await this.observe("laser.close", { operation: "close" }, () => this.transport.close())
    })()
    await this.shared.closing
  }

  /** Delegates async disposal to `close()`. */
  [Symbol.asyncDispose](): Promise<void> {
    return this.ownsClosure ? this.close() : Promise.resolve()
  }
}
