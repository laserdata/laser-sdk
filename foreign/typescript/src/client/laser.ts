import {
  ApacheIggyTransport,
  type ClientOwnership,
  type IggyClient,
  type LaserTransport
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
  encodePresenceInput,
  newRegistryCache,
  type AgentPresenceInput,
  type RegistryCache
} from "../agent/registry.js"
import { AsyncOnce } from "../runtime/async-once.js"
import { Stream } from "../stream/stream.js"
import { ContextScope } from "../context-scope.js"
import {
  ActionKind,
  GovernorState,
  encodePolicyEvidence,
  decodePolicyEvidence,
  POLICY_DECISION_OPERATION,
  type ActionGovernor,
  type GovernorMode,
  type PolicyEvidence
} from "../govern.js"
import { MemoryBackend, MemoryHandle } from "../memory/handle.js"
import type { Embedder, Memory } from "../memory/types.js"
import { NOOP_OBSERVER, type LaserObserver } from "../observe.js"
import type { KeyRegistry, SigningKey } from "../signing.js"
import type { Topic } from "../stream/topic.js"
import { executeBatch } from "../managed/batch.js"
import { Fork } from "../managed/forks.js"
import { Kv } from "../managed/kv.js"
import { Bindings, Projections, Schemas } from "../managed/projections.js"
import { QueryRequest } from "../managed/query.js"
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
  CONTROL_OP_VERSION
} from "../wire/codes.js"
import { QueryCommand } from "../wire/commands.js"
import { encodeControlEnvelope, type ControlCommand } from "../wire/control.js"
import type { ForkInfo } from "../wire/fork.js"
import { decodeBackendAnnounce } from "../wire/hello.js"
import type { KvNamespaceInfo } from "../wire/kv.js"
import type { Query, QueryResult } from "../wire/query.js"
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
  type AgentCard
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
  ProtocolError,
  QueryExecutionError,
  InvalidError,
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
  managedCapabilitiesFrom,
  managedCapabilitiesWithUnknownVersions,
  servesConsistency
} from "./capabilities.js"

const LOCAL_CONNECTION_STRING = "iggy://iggy:iggy@127.0.0.1:8090"

interface LaserTopology {
  readonly opsStream: string
  readonly controlTopic: string
  readonly deadLetterTopic: string
  readonly changesTopic: string
}

const DEFAULT_TOPOLOGY: LaserTopology = {
  opsStream: OPS_STREAM,
  controlTopic: CONTROL_TOPIC,
  deadLetterTopic: DLQ_TOPIC,
  changesTopic: CHANGES_TOPIC
}

interface LaserBuildOptions {
  readonly connectionString?: string
  readonly address?: { readonly host: string; readonly port: number }
  readonly credentials?:
    | { readonly kind: "usernamePassword"; readonly username: string; readonly password: string }
    | { readonly kind: "token"; readonly token: string }
  readonly client?: IggyClient
  readonly ownership: ClientOwnership
  readonly defaultStream?: string
  readonly capabilities?: Capabilities
  readonly governor?: { readonly policy: ActionGovernor; readonly mode: GovernorMode }
  readonly observer: LaserObserver
  readonly verifier?: KeyRegistry
  readonly topology: LaserTopology
}

export interface InjectedClientOptions {
  readonly ownership?: ClientOwnership
  readonly defaultStream?: string
  readonly capabilities?: Capabilities
  readonly observer?: LaserObserver
}

export class LaserBuilder {
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

  constructor(private readonly create: (options: LaserBuildOptions) => Promise<Laser>) {}

  connectionString(value: string): this {
    this.connectionStringValue = value
    return this
  }

  address(host: string, port = 8090): this {
    this.addressValue = { host, port }
    return this
  }

  credentials(username: string, password: string): this {
    this.credentialsValue = { kind: "usernamePassword", username, password }
    return this
  }

  token(value: string): this {
    this.credentialsValue = { kind: "token", token: value }
    return this
  }

  iggyClient(client: IggyClient, options: { readonly ownership?: ClientOwnership } = {}): this {
    this.clientValue = client
    this.ownershipValue = options.ownership ?? "borrowed"
    return this
  }

  defaultStream(value: string): this {
    this.defaultStreamValue = value
    return this
  }

  capabilities(value: Capabilities): this {
    this.capabilitiesValue = value
    return this
  }

  governor(policy: ActionGovernor, mode: GovernorMode): this {
    this.governorValue = { policy, mode }
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
    return this
  }

  controlTopic(value: string): this {
    this.topologyValue = { ...this.topologyValue, controlTopic: value }
    return this
  }

  deadLetterTopic(value: string): this {
    this.topologyValue = { ...this.topologyValue, deadLetterTopic: value }
    return this
  }

  changesTopic(value: string): this {
    this.topologyValue = { ...this.topologyValue, changesTopic: value }
    return this
  }

  connect(): Promise<Laser> {
    const modes =
      Number(this.connectionStringValue !== undefined) +
      Number(this.addressValue !== undefined) +
      Number(this.clientValue !== undefined)
    if (modes > 1) {
      throw new ConfigError(
        "connectionString(), address(), and iggyClient() are mutually exclusive"
      )
    }
    if (this.credentialsValue !== undefined && this.addressValue === undefined) {
      throw new ConfigError("credentials() and token() require address()")
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
      ["deadLetterTopic", this.topologyValue.deadLetterTopic],
      ["changesTopic", this.topologyValue.changesTopic]
    ]
    for (const [name, value] of topologyEntries) {
      if (value.length === 0) throw new ConfigError(`${name} must not be empty`)
    }
    return this.create({
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
      topology: this.topologyValue
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
  advertisedAgent?: string
}

export type ConsumerRef =
  | { readonly kind: "group"; readonly name: ConsumerGroupName | string }
  | { readonly kind: "consumer"; readonly name: string }

export type ConsumptionStatus =
  | { readonly kind: "notYetConsumed"; readonly behindBy: bigint }
  | { readonly kind: "consumed"; readonly committed: bigint; readonly head: bigint }

function newSharedState(): LaserSharedState {
  return { registryCaches: new Map(), replyHubs: new Map() }
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
  AgentTopic.Dlq,
  AgentTopic.Registry
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
  return value?.kind === "string" ? value.value : undefined
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

async function probeCapabilities(transport: ManagedTransport): Promise<Capabilities> {
  let reply: Uint8Array
  try {
    reply = await transport.sendManaged(AGDX_HELLO_CODE, new Uint8Array())
  } catch {
    return OPEN_CAPABILITIES
  }
  if (reply.byteLength === 0) {
    return managedCapabilitiesWithUnknownVersions()
  }
  try {
    return managedCapabilitiesFrom(decodeBackendAnnounce(reply))
  } catch {
    return managedCapabilitiesWithUnknownVersions()
  }
}

export class Laser {
  private closed = false
  readonly opsStream: string
  readonly controlTopic: string
  readonly deadLetterTopic: string
  readonly changesTopic: string

  private constructor(
    private readonly transport: LaserTransport,
    readonly defaultStream: string | undefined,
    private readonly capabilitiesOnce: AsyncOnce<Capabilities>,
    private readonly shared: LaserSharedState,
    private readonly observer: LaserObserver = NOOP_OBSERVER,
    private readonly governor?: GovernorState,
    private readonly verifier?: KeyRegistry,
    topology: LaserTopology = DEFAULT_TOPOLOGY,
    private readonly ownsClosure = true
  ) {
    this.opsStream = topology.opsStream
    this.controlTopic = topology.controlTopic
    this.deadLetterTopic = topology.deadLetterTopic
    this.changesTopic = topology.changesTopic
  }

  static builder(): LaserBuilder {
    return new LaserBuilder(async (options) => {
      let transport: ApacheIggyTransport
      if (options.client !== undefined) {
        transport = ApacheIggyTransport.fromClient(options.client, options.ownership)
      } else if (options.address !== undefined) {
        const credentials = options.credentials ?? {
          kind: "usernamePassword" as const,
          username: "iggy",
          password: "iggy"
        }
        const userInfo =
          credentials.kind === "token"
            ? encodeURIComponent(credentials.token)
            : `${encodeURIComponent(credentials.username)}:${encodeURIComponent(credentials.password)}`
        const authorityHost = options.address.host.includes(":")
          ? `[${options.address.host.replace(/^\[|\]$/g, "")}]`
          : options.address.host
        transport = await ApacheIggyTransport.connect(
          `iggy://${userInfo}@${authorityHost}:${String(options.address.port)}`
        )
      } else {
        transport = await ApacheIggyTransport.connect(
          options.connectionString ?? LOCAL_CONNECTION_STRING
        )
      }
      return new Laser(
        transport,
        options.defaultStream,
        options.capabilities === undefined
          ? new AsyncOnce()
          : AsyncOnce.resolved(options.capabilities),
        newSharedState(),
        options.observer,
        options.governor === undefined
          ? undefined
          : new GovernorState(options.governor.policy, options.governor.mode),
        options.verifier,
        options.topology
      )
    })
  }

  static fromIggyClient(client: IggyClient, options: InjectedClientOptions = {}): Promise<Laser> {
    const builder = Laser.builder()
      .iggyClient(client, options.ownership === undefined ? {} : { ownership: options.ownership })
      .capabilities(options.capabilities ?? OPEN_CAPABILITIES)
    if (options.defaultStream !== undefined) builder.defaultStream(options.defaultStream)
    if (options.observer !== undefined) builder.observer(options.observer)
    return builder.connect()
  }

  static async connect(connectionString: string): Promise<Laser> {
    const transport = await ApacheIggyTransport.connect(connectionString)
    return new Laser(transport, undefined, new AsyncOnce(), newSharedState())
  }

  static async connectWithStream(connectionString: string, stream: string): Promise<Laser> {
    const transport = await ApacheIggyTransport.connect(connectionString)
    return new Laser(transport, stream, new AsyncOnce(), newSharedState())
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
      this.topology(),
      false
    )
  }

  withCapabilities(capabilities: Capabilities): Laser {
    return new Laser(
      this.transport,
      this.defaultStream,
      AsyncOnce.resolved(capabilities),
      this.shared,
      this.observer,
      this.governor,
      this.verifier,
      this.topology(),
      false
    )
  }

  withGovernor(governor: ActionGovernor, mode: GovernorMode): Laser {
    return new Laser(
      this.transport,
      this.defaultStream,
      this.capabilitiesOnce,
      this.shared,
      this.observer,
      new GovernorState(governor, mode),
      this.verifier,
      this.topology(),
      false
    )
  }

  withVerifier(verifier: KeyRegistry): Laser {
    return new Laser(
      this.transport,
      this.defaultStream,
      this.capabilitiesOnce,
      this.shared,
      this.observer,
      this.governor,
      verifier,
      this.topology(),
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
      this.topology(),
      false
    )
  }

  private topology(): LaserTopology {
    return {
      opsStream: this.opsStream,
      controlTopic: this.controlTopic,
      deadLetterTopic: this.deadLetterTopic,
      changesTopic: this.changesTopic
    }
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
      sendManaged: (commandCode, payload) =>
        this.observe("laser.managed", { operation: "managed", commandCode }, () =>
          this.transport.sendManaged(commandCode, payload)
        )
    }
  }

  async capabilities(): Promise<Capabilities> {
    return this.capabilitiesOnce.get(() => probeCapabilities(this.managedTransport()))
  }

  [INTERNAL_TRANSPORT](): LaserTransport {
    return this.transport
  }

  [INTERNAL_VERIFIER](): KeyRegistry | undefined {
    return this.verifier
  }

  [INTERNAL_REPLY_HUB](topic: string): Promise<ReplyHub> {
    return this.replyHub(topic)
  }

  [INTERNAL_GOVERN](
    action: Omit<Parameters<GovernorState["govern"]>[0], "counters">
  ): Promise<Uint8Array> {
    if (this.governor === undefined) return Promise.resolve(action.payload.slice())
    return this.governor.govern(action, (evidence) =>
      this.emitPolicyEvidence(action.stream, evidence)
    )
  }

  // The exact upstream client, for advanced use Laser intentionally does
  // not wrap.
  get iggyClient(): LaserTransport["iggyClient"] {
    return this.transport.iggyClient
  }

  stream(name: string): Stream {
    return new Stream(
      this.transport,
      name,
      (stream, topic, payload, provenance) =>
        this[INTERNAL_GOVERN]({
          kind: ActionKind.Publish,
          stream,
          topic,
          ...(provenance?.agent !== undefined ? { source: provenance.agent.asString() } : {}),
          ...(provenance?.targetAgentId !== undefined
            ? { target: provenance.targetAgentId.asString() }
            : {}),
          ...(provenance !== undefined ? { conversation: provenance.conversationId } : {}),
          ...(provenance?.correlationId !== undefined
            ? { correlation: provenance.correlationId }
            : {}),
          payload,
          signed: false
        }),
      async (schemaId) => (await this.schemas().get(schemaId))?.schema,
      (operation, attributes, effect) => this.observe(operation, attributes, effect)
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
        })
    )
  }

  agent(id: AgentId): AgentScope {
    return new AgentScope(this, id)
  }

  contract(router: Router): ContractBuilder {
    return new ContractBuilder(this, router)
  }

  workflow(name: string): Workflow {
    return new Workflow(this, name)
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
    return new ContextScope(this, conversation)
  }

  memory(namespace: string): MemoryHandle {
    return this.memoryWith(namespace, MemoryBackend.Auto)
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
        ...(provenance.agent !== undefined ? { agent: provenance.agent.asString() } : {})
      },
      async () => {
        const governedPayload = await this[INTERNAL_GOVERN]({
          kind: ActionKind.Send,
          stream,
          topic,
          ...(provenance.agent !== undefined ? { source: provenance.agent.asString() } : {}),
          ...(provenance.targetAgentId !== undefined
            ? { target: provenance.targetAgentId.asString() }
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

  async policyEvidence(conversation: ConversationId): Promise<readonly PolicyEvidence[]> {
    const messages = await this.context(conversation).fetch(
      [AgentTopic.Audit],
      Number.MAX_SAFE_INTEGER
    )
    return messages.flatMap((message) => {
      if (message.envelope?.operation !== POLICY_DECISION_OPERATION) return []
      try {
        return [decodePolicyEvidence(message.envelope.body)]
      } catch {
        return []
      }
    })
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
    const cursor = await this.topic(topic).replay({ batchSize: 1_000 })
    const envelopes: AgentEnvelope[] = []
    for (;;) {
      const records = await cursor.poll()
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
      throw new UnsupportedError("the active Iggy transport cannot resolve consumer offsets")
    }
    const names = await this.transport.resolveStreamTopicNames(at.streamId, at.topicId)
    if (names === undefined) {
      return { kind: "notYetConsumed", behindBy: at.offset + 1n }
    }
    const consumer =
      target.kind === "group"
        ? {
            kind: "group" as const,
            name: typeof target.name === "string" ? target.name : target.name.asString()
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
      throw new UnsupportedError("the active Iggy transport cannot resolve dead-letter sources")
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
    const ticket = hub.subscribe(correlationId)
    try {
      await this.sendAgent(requestTopic, payload, correlated)
    } catch (error) {
      ticket.cancel()
      throw error
    }
    return ticket.wait(timeoutMs, signal)
  }

  clientMetadata(): ClientMetadataRequest {
    return new ClientMetadataRequest(this.transport)
  }

  async advertisePresence(presence: AgentPresenceInput): Promise<void> {
    const requested = presence.agent.asString()
    const advertised = this.shared.advertisedAgent
    if (advertised !== undefined && advertised !== requested) {
      throw new PresenceConflictError(advertised, requested)
    }
    this.shared.advertisedAgent = requested
    await this.managedTransport().sendManaged(
      AGDX_SET_CLIENT_METADATA_CODE,
      encodePresenceInput(presence)
    )
  }

  async clearPresence(): Promise<void> {
    await this.managedTransport().sendManaged(AGDX_SET_CLIENT_METADATA_CODE, new Uint8Array())
    delete this.shared.advertisedAgent
  }

  async agentRegistry(): Promise<AgentRegistry> {
    const stream = this.requireDefaultStream("agentRegistry()")
    let cache = this.shared.registryCaches.get(stream)
    if (cache === undefined) {
      cache = newRegistryCache()
      this.shared.registryCaches.set(stream, cache)
    }
    const cursor = await this.topic(AgentTopic.Registry).replay()
    return new AgentRegistry(cursor, cache, () => this.clientMetadata(), undefined, this.verifier)
  }

  async publishCard(source: AgentId, card: AgentCard): Promise<void> {
    validateAgentCard(card)
    const body = encodeNamed(encodeAgentCard(card))
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
    let fact = this.agdx(AgentTopic.Registry, operator, ConversationId.new())
      .status(operation)
      .body(new TextEncoder().encode(agent.asString()))
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
      hub = ReplyHub.create(this.transport, stream, topic)
      this.shared.replyHubs.set(key, hub)
      void hub.catch(() => this.shared.replyHubs.delete(key))
    }
    return hub
  }

  query(index: string): QueryRequest {
    return new QueryRequest(index, (query) => this.executeQuery(query))
  }

  // A handle to the managed key-value store, scoped to `namespace`. Cheap to
  // create, it borrows the connection.
  kv(namespace: string): Kv {
    return new Kv(this.managedTransport(), () => this.capabilities(), namespace)
  }

  // Every KV namespace that holds at least one entry for this caller,
  // sorted by the backend. Namespace discovery for tooling and UIs.
  async kvNamespaces(): Promise<readonly KvNamespaceInfo[]> {
    return Kv.namespaces(this.managedTransport(), () => this.capabilities())
  }

  // Execute up to `MAX_BATCH_OPS` independent managed commands in one round
  // trip. Input order is preserved, each returned slot holds that
  // operation's typed reply bytes. Batching amortizes transport cost, it is
  // not a transaction.
  async executeBatch(ops: readonly BatchItem[]): Promise<readonly Uint8Array[]> {
    const capabilities = await this.capabilities()
    return executeBatch(this.managedTransport(), capabilities, ops)
  }

  // A handle to one fork by id. Cheap to create, it borrows the connection.
  // Open the fork with `.create()`, then write rows, query it
  // (`query(...).fork(id)`), and finally `.promote()` or `.squash()` it.
  fork(forkId: string): Fork {
    return new Fork(this.managedTransport(), () => this.capabilities(), forkId)
  }

  // Every open fork for the authenticated user.
  async forks(): Promise<readonly ForkInfo[]> {
    return Fork.forks(this.managedTransport(), () => this.capabilities())
  }

  // Handle to the projection registry: `.register(projection)`, `.drop(id)`,
  // `.get(id)`, and the filterable `.list()` browse. Cheap to create, it
  // borrows the connection.
  projections(): Projections {
    return new Projections(
      this.managedTransport(),
      () => this.capabilities(),
      (command) => this.publishControl(command)
    )
  }

  // Handle to the binding surface: `.apply(binding)` routes a `(stream,
  // topic)` source into registered projections, `.remove(source, ..)` stops
  // it. Cheap to create.
  bindings(): Bindings {
    return new Bindings((command) => this.publishControl(command))
  }

  // Handle to the writer-schema registry (Avro, Protobuf): `.register(def)`,
  // `.drop(id)`, `.get(id)`, `.list()`. Cheap to create.
  schemas(): Schemas {
    return new Schemas(
      this.managedTransport(),
      () => this.capabilities(),
      (command) => this.publishControl(command)
    )
  }

  // The managed run registry: submit a run to an agent or workflow, cancel
  // it, read its state, or list runs. Cheap to create, it borrows the
  // connection.
  runs(): Runs {
    return new Runs(
      this.managedTransport(),
      () => this.capabilities(),
      (command) => this.publishControl(command)
    )
  }

  // The change-feed accessor: `ChangeRecord`s the deployment publishes
  // after each committed projector batch for a binding that opted into
  // `notify`. Gated on the `watch` capability.
  watch(): Watch {
    return new Watch(
      () => this.capabilities(),
      (options) => this.stream(this.opsStream).topic(this.changesTopic).replay(options)
    )
  }

  // A handle to the knowledge-graph surface `name`. Traversals require the
  // `graph` capability and ride the managed binary transport. Against an
  // open (non-managed) host a fetch throws `UnsupportedError`.
  graph(name: string): GraphHandle {
    return new GraphHandle(this.managedTransport(), () => this.capabilities(), name)
  }

  // The caller's own effective capabilities (bound roles and their
  // flattened grants). Always answered to the authenticated caller.
  async whoami(): Promise<WhoamiReply> {
    return whoami(this.managedTransport(), await this.capabilities())
  }

  // List defined roles, optionally filtered by name prefix or a free-text
  // search.
  async listRoles(options?: {
    readonly namePrefix?: string
    readonly search?: string
  }): Promise<readonly Role[]> {
    return listRoles(this.managedTransport(), await this.capabilities(), options)
  }

  // One role by name, or `undefined`.
  async getRole(name: string): Promise<Role | undefined> {
    return getRole(this.managedTransport(), await this.capabilities(), name)
  }

  // One user's bound role names.
  async getBindings(userId: number): Promise<readonly string[]> {
    return getBindings(this.managedTransport(), await this.capabilities(), userId)
  }

  // Define or replace a role (upsert). Requires `authz:admin`.
  async defineRole(role: Role): Promise<void> {
    return defineRole(this.managedTransport(), await this.capabilities(), role)
  }

  // Delete a role by name. Requires `authz:admin`.
  async deleteRole(name: string): Promise<void> {
    return deleteRole(this.managedTransport(), await this.capabilities(), name)
  }

  // Bind a user's whole role set (replace). Requires `authz:admin`. With
  // `expectRevision` set, applies only if the binding's current revision
  // matches.
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

  // Read a page of the authorization change history for `subject`.
  // Requires `authz:read`.
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

  // Publish one durable control command to `<opsStream>/<controlTopic>`.
  // The shared write path for the projection registry and the binding
  // surface. Every control command lands on one partition (keyed
  // "control") so a `registerProjection` is applied before the
  // `applyBinding` that references it.
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

  private async executeQuery(query: Query): Promise<QueryResult> {
    const capabilities = await this.capabilities()
    if (query.text !== undefined && !capabilities.query.keyword) {
      throw new UnsupportedError("keyword query is not served by this deployment")
    }
    if (!servesConsistency(capabilities, query.consistency)) {
      throw new UnsupportedError(
        `${query.consistency} query consistency is not served by this deployment`
      )
    }
    const reply = await executeManaged(this.managedTransport(), capabilities, QueryCommand, {
      query
    })
    if (reply.kind === "ok") return reply.result
    if (reply.kind === "unrecognized") {
      throw new ProtocolError(`query returned unknown reply variant \`${reply.tag}\``, {
        commandCode: QueryCommand.code
      })
    }
    if (reply.error.kind === "unsupported") {
      throw new UnsupportedError(reply.error.message)
    }
    throw new QueryExecutionError(`query failed: ${reply.error.kind}`, reply.error)
  }

  async close(): Promise<void> {
    if (this.closed) return
    this.closed = true
    if (!this.ownsClosure) return
    const hubs = await Promise.allSettled(this.shared.replyHubs.values())
    for (const hub of hubs) {
      if (hub.status === "fulfilled") hub.value.stop()
    }
    this.shared.replyHubs.clear()
    await this.observe("laser.close", { operation: "close" }, () => this.transport.close())
  }
}
