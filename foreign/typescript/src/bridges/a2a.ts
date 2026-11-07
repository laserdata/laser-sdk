import { blake3 } from "@noble/hashes/blake3.js"
import { bytesToHex, hexToBytes } from "@noble/hashes/utils.js"
import { type CallOptions, abortable, throwIfAborted } from "../client/call-options.js"
import { code, publicErrorMessage } from "../client/error-classify.js"
import { startChild } from "../agent/contract.js"
import { decodeAgentMessage } from "../agent/decode.js"
import {
  CodecError,
  HandlerConfigError,
  InvalidError,
  NoStreamError,
  RejectedError
} from "../client/errors.js"
import { INTERNAL_TRANSPORT, INTERNAL_VERIFIER } from "../client/internals.js"
import type { Laser } from "../client/laser.js"
import type { PolledMessage } from "../iggy/apache-iggy.js"
import { AgentTopic } from "../provenance/agent-topic.js"
import {
  signCardValue,
  type AgentCardSignature,
  type KeyRegistry,
  type SigningKey
} from "../signing.js"
import { AgentId, ConversationId } from "../types/ids.js"
import {
  AgentKind,
  METADATA_BRIDGE_HOPS,
  OPERATION_CHAT,
  commandEnvelope,
  taskStateIsTerminal,
  withOperation,
  type AgentId as WireAgentId,
  type AgentEnvelope,
  type CapabilityDescriptor,
  type ContentRef,
  type SignatureContext,
  type TaskState
} from "../wire/agent.js"
import { ContentType } from "../wire/content.js"
import { ConversationId as WireConversationId, CorrelationId, type RecordId } from "../wire/ids.js"
import { bridgeHopMetadata, callHops, enterBridge, inboundHops } from "./hops.js"
import { SDK_VERSION } from "../version.js"

export const A2A_PROTOCOL_VERSION = "1.0"
export const A2A_JSONRPC_BINDING = "JSONRPC"
export { SDK_VERSION }

const INVALID_REQUEST = -32_600
const METHOD_NOT_FOUND = -32_601
const INVALID_PARAMS = -32_602
const INTERNAL_ERROR = -32_603
const TASK_NOT_FOUND = -32_001
const TASK_NOT_CANCELABLE = -32_002
const UNSUPPORTED_OPERATION = -32_004
// How far before a task's minted time its lookup starts, so a server clock
// behind the bridge's still finds the command.
const LOOKUP_CLOCK_SLACK_MICROS = 300_000_000n
const LOOKUP_BATCH = 1_000
// 32 passes of up to 10,000 records per partition, the Rust bound.
const MAX_LOOKUP_POLLS = 320
const LOOKUP_READER = "laser-a2a-task-lookup"

export const A2aMethod = {
  MessageSend: "SendMessage",
  MessageStream: "SendStreamingMessage",
  TasksGet: "GetTask",
  TasksCancel: "CancelTask"
} as const
export type A2aMethod = (typeof A2aMethod)[keyof typeof A2aMethod]

export interface AgentInterface {
  readonly url: string
  readonly protocolBinding: string
  readonly protocolVersion: string
}

export interface AgentCardCapabilities {
  readonly streaming: boolean
  readonly pushNotifications: boolean
  readonly stateTransitionHistory: boolean
  readonly extendedAgentCard: boolean
}

export interface AgentSkill {
  readonly id: string
  readonly name: string
  readonly description: string
  readonly tags: readonly string[]
  readonly inputModes?: readonly string[]
  readonly outputModes?: readonly string[]
}

export interface AgentCard {
  readonly name: string
  readonly description: string
  readonly version: string
  readonly supportedInterfaces: readonly AgentInterface[]
  readonly capabilities: AgentCardCapabilities
  readonly defaultInputModes: readonly string[]
  readonly defaultOutputModes: readonly string[]
  readonly skills: readonly AgentSkill[]
  readonly signatures?: readonly AgentCardSignature[]
}

export interface TaskStatus {
  readonly state: TaskState
}

/** One A2A `Part`, carrying text. */
export interface Part {
  readonly text: string
}

/** An A2A `Artifact`: the answer's text as one part, named by its record. */
export interface Artifact {
  readonly artifactId: string
  readonly parts: readonly Part[]
}

/** An A2A `Task`. `contextId` is the client's `message.contextId`, or the
 * task's own conversation when the client sent none. */
export interface Task {
  readonly id: string
  readonly contextId: string
  readonly status: TaskStatus
  readonly artifacts: readonly Artifact[]
}

export interface JsonRpcRequest {
  readonly id?: unknown
  readonly method: string
  readonly params?: unknown
}

export interface JsonRpcError {
  readonly code: number
  readonly message: string
}

export interface JsonRpcResponse {
  readonly jsonrpc: "2.0"
  readonly id: unknown
  readonly result?: unknown
  readonly error?: JsonRpcError
}

function jsonBytes(value: unknown, context: string): Uint8Array {
  try {
    const encoded: unknown = JSON.stringify(value)
    if (typeof encoded !== "string") throw new Error("value is not JSON serializable")
    return new TextEncoder().encode(encoded)
  } catch (cause) {
    throw new CodecError(`${context} is not JSON serializable`, "a2a", "json", { cause })
  }
}

function correlationOf(conversation: ConversationId): CorrelationId {
  return CorrelationId.parse(conversation.toString())
}

export function contentRefMode(reference: ContentRef): string {
  if (reference.kind === "schemaId") {
    return `application/x-agdx-schema;id=${reference.value}`
  }
  const modes: Readonly<Record<ContentType, string>> = {
    [ContentType.Raw]: "application/octet-stream",
    [ContentType.Json]: "application/json",
    [ContentType.Msgpack]: "application/msgpack",
    [ContentType.Cbor]: "application/cbor",
    [ContentType.Bson]: "application/bson",
    [ContentType.Avro]: "application/avro",
    [ContentType.Protobuf]: "application/protobuf",
    [ContentType.Arrow]: "application/vnd.apache.arrow.stream",
    [ContentType.Ref]: "application/x-agdx-ref",
    [ContentType.Any]: "*/*"
  }
  return modes[reference.value]
}

/** Build the command for an A2A request. The body keeps the original JSON bytes. */
export function commandFromMessageSend(
  record: RecordId,
  conversation: WireConversationId,
  source: WireAgentId,
  correlation: CorrelationId,
  paramsJson: Uint8Array
): AgentEnvelope {
  return withOperation(
    commandEnvelope(record, conversation, source, correlation, paramsJson.slice()),
    OPERATION_CHAT
  )
}

/** The A2A task an answering envelope settles: `taskState` when present,
 * else completed for a response and failed for an error. A non-empty body is
 * the task's one text artifact. */
export function taskFromEnvelope(taskId: string, contextId: string, envelope: AgentEnvelope): Task {
  const state: TaskState =
    envelope.taskState ??
    (envelope.kind === AgentKind.Error
      ? { kind: "known", name: "Failed" }
      : { kind: "known", name: "Completed" })
  return {
    id: taskId,
    contextId,
    status: { state },
    artifacts:
      envelope.body.byteLength === 0
        ? []
        : [
            {
              artifactId: envelope.record?.toString() ?? `${taskId}-artifact`,
              parts: [{ text: new TextDecoder().decode(envelope.body) }]
            }
          ]
  }
}

/** The A2A v1.0 JSON of a task, with `TASK_STATE_*` state names. */
export function taskToJson(task: Task): unknown {
  return {
    id: task.id,
    contextId: task.contextId,
    status: { state: a2aStateName(task.status.state) },
    ...(task.artifacts.length > 0 ? { artifacts: task.artifacts } : {})
  }
}

// The A2A v1.0 name of a task state. Paused has no A2A name and reads as
// working, a state no A2A enum value names reads as unspecified.
function a2aStateName(state: TaskState): string {
  if (state.kind !== "known") return "TASK_STATE_UNSPECIFIED"
  const names: Readonly<Record<typeof state.name, string>> = {
    Submitted: "TASK_STATE_SUBMITTED",
    Working: "TASK_STATE_WORKING",
    InputRequired: "TASK_STATE_INPUT_REQUIRED",
    Completed: "TASK_STATE_COMPLETED",
    Canceled: "TASK_STATE_CANCELED",
    Failed: "TASK_STATE_FAILED",
    Rejected: "TASK_STATE_REJECTED",
    AuthRequired: "TASK_STATE_AUTH_REQUIRED",
    Unknown: "TASK_STATE_UNSPECIFIED",
    Paused: "TASK_STATE_WORKING"
  }
  return names[state.name]
}

// Raw JSON as bytes or a string rides byte-identical, any other value is
// encoded as JSON first.
function paramsBytes(paramsJson: unknown): Uint8Array {
  return paramsJson instanceof Uint8Array
    ? paramsJson.slice()
    : typeof paramsJson === "string"
      ? new TextEncoder().encode(paramsJson)
      : jsonBytes(paramsJson, "A2A message params")
}

function stateTask(
  id: string,
  contextId: string,
  name: "Submitted" | "Working" | "Canceled"
): Task {
  return { id, contextId, status: { state: { kind: "known", name } }, artifacts: [] }
}

// The context a task belongs to: the `message.contextId` of the params it was
// submitted with, else the task's own conversation.
function contextIdOf(paramsJson: Uint8Array, task: ConversationId): string {
  try {
    const params: unknown = JSON.parse(new TextDecoder().decode(paramsJson))
    const message = (params as { readonly message?: { readonly contextId?: unknown } }).message
    if (typeof message?.contextId === "string") return message.contextId
  } catch {
    // A body that is not JSON names no context.
  }
  return task.toString()
}

const TASK_BINDING_DOMAIN = new TextEncoder().encode("laserdata a2a task v1\0")

function u64Le(value: bigint): Uint8Array {
  const bytes = new Uint8Array(8)
  new DataView(bytes.buffer).setBigUint64(0, BigInt.asUintN(64, value), true)
  return bytes
}

function u128Le(value: bigint): Uint8Array {
  const bytes = new Uint8Array(16)
  bytes.set(u64Le(value), 0)
  bytes.set(u64Le(value >> 64n), 8)
  return bytes
}

/** The keyed digest a bound task id carries: the caller, the stream
 * generation, the conversation and the expiry, so an id works only for its
 * caller, on the stream incarnation it was made on, until it expires. The
 * layout matches the Rust SDK byte for byte. */
export function taskBindingMac(
  key: Uint8Array,
  caller: string,
  streamGeneration: bigint,
  task: ConversationId,
  expiresMicros: bigint
): Uint8Array {
  const callerBytes = new TextEncoder().encode(caller)
  const parts = [
    TASK_BINDING_DOMAIN,
    u64Le(BigInt(callerBytes.byteLength)),
    callerBytes,
    u64Le(streamGeneration),
    u128Le(task.asU128()),
    u64Le(expiresMicros)
  ]
  const message = new Uint8Array(parts.reduce((total, part) => total + part.byteLength, 0))
  let offset = 0
  for (const part of parts) {
    message.set(part, offset)
    offset += part.byteLength
  }
  return blake3(message, { key })
}

function sameBytes(left: Uint8Array, right: Uint8Array): boolean {
  if (left.byteLength !== right.byteLength) return false
  let difference = 0
  for (let index = 0; index < left.byteLength; index += 1)
    difference |= (left[index] ?? 0) ^ (right[index] ?? 0)
  return difference === 0
}

function nowMicros(): bigint {
  return BigInt(Date.now()) * 1000n
}

function unboundCaller(): HandlerConfigError {
  return new HandlerConfigError(
    "this bridge binds tasks to callers, so the request needs an authenticated caller"
  )
}

interface TaskBinding {
  readonly key: Uint8Array
  readonly ttlMicros: bigint
}

/** The bridge acting for one authenticated caller. Build it with
 * `A2aBridge.caller`. */
export class A2aCaller {
  /** @internal */
  constructor(
    private readonly bridge: A2aBridge,
    private readonly principal: string
  ) {}

  /** `A2aBridge.submit` for this caller. */
  async submit(
    paramsJson: unknown,
    options: CallOptions & { readonly target?: AgentId } = {}
  ): Promise<Task> {
    return this.bridge[SUBMIT_FOR](
      paramsJson,
      undefined,
      options.target,
      this.principal,
      undefined,
      options
    )
  }

  /** `A2aBridge.submitWithHops` for this caller. */
  async submitWithHops(
    previousHops: readonly string[],
    paramsJson: unknown,
    options: CallOptions & { readonly target?: AgentId } = {}
  ): Promise<Task> {
    return this.bridge[SUBMIT_WITH_HOPS_FOR](
      previousHops,
      paramsJson,
      options.target,
      this.principal,
      options
    )
  }

  /** `A2aBridge.submitIn` for this caller. */
  async submitIn(
    parent: ConversationId,
    root: ConversationId,
    paramsJson: unknown,
    options: CallOptions & { readonly target?: AgentId } = {}
  ): Promise<Task> {
    return this.bridge[SUBMIT_FOR](
      paramsJson,
      { parent, root },
      options.target,
      this.principal,
      undefined,
      options
    )
  }

  /** `A2aBridge.task` for this caller. Another caller's task id answers like
   * an unknown one. */
  async task(id: string, options: CallOptions = {}): Promise<Task> {
    return this.bridge[TASK_FOR](id, this.principal, options)
  }

  /** `A2aBridge.cancel` for this caller. Another caller's task id is refused
   * before anything is published. */
  async cancel(id: string, options: CallOptions = {}): Promise<Task> {
    return this.bridge[CANCEL_FOR](id, this.principal, options)
  }

  /** `A2aBridge.handleRpc` for this caller. */
  async handleRpc(input: unknown, options: CallOptions = {}): Promise<JsonRpcResponse> {
    return this.bridge[HANDLE_RPC_FOR](input, this.principal, options)
  }
}

const SUBMIT_FOR = Symbol("a2a submit for caller")
const SUBMIT_WITH_HOPS_FOR = Symbol("a2a submit with hops for caller")
const TASK_FOR = Symbol("a2a task for caller")
const CANCEL_FOR = Symbol("a2a cancel for caller")
const HANDLE_RPC_FOR = Symbol("a2a handle rpc for caller")

export class A2aBridge {
  private capabilities: readonly CapabilityDescriptor[] = []
  private signingKey: SigningKey | undefined
  private hops: readonly string[]
  private taskBinding: TaskBinding | undefined
  private target: AgentId | undefined
  private publicUrl: string | undefined
  private description = "LaserData AGDX bridge over the durable log"
  private version = SDK_VERSION

  constructor(
    private readonly laser: Laser,
    private readonly source: AgentId,
    private readonly requestTopic: string,
    private readonly replyTopic: string
  ) {
    this.hops = enterBridge(source.asStr())
  }

  withBridgeHops(previous: readonly string[]): this {
    this.hops = enterBridge(this.source.asStr(), previous)
    return this
  }

  /** Sign every record the bridge publishes, the task command and the cancel,
   * so an agent that verifies its input accepts them. */
  withSigningKey(key: SigningKey): this {
    this.signingKey = key
    return this
  }

  withCapabilities(capabilities: readonly CapabilityDescriptor[]): this {
    this.capabilities = [...capabilities]
    return this
  }

  /** Address every task that names no target to `agent`, the agent this
   * bridge fronts. Its answer must then be signed by `agent` when the `Laser`
   * verifies replies. */
  withTarget(agent: AgentId): this {
    this.target = agent
    return this
  }

  /** The absolute URL A2A clients reach the JSON-RPC endpoint at, which the
   * card advertises. The card cannot be built without it. */
  withPublicUrl(url: string): this {
    let parsed: URL
    try {
      parsed = new URL(url)
    } catch (cause) {
      throw new InvalidError("the A2A card URL must be an absolute http or https URL", { cause })
    }
    if (parsed.protocol !== "https:" && parsed.protocol !== "http:")
      throw new InvalidError("the A2A card URL must be an absolute http or https URL")
    this.publicUrl = url
    return this
  }

  /** The card's description. */
  withDescription(description: string): this {
    this.description = description
    return this
  }

  /** The card's agent version, the SDK version by default. */
  withVersion(version: string): this {
    this.version = version
    return this
  }

  /** Bind every task id to the caller that submitted it, keyed with the
   * 32-byte `key` and valid for `ttlMs`. A bound id names the caller, the
   * stream incarnation, the conversation and an expiry under a keyed digest,
   * so another caller cannot read or cancel the task. Once set, task
   * operations need a caller: use `caller`. */
  withTaskBinding(key: Uint8Array, ttlMs: number): this {
    if (key.byteLength !== 32) throw new InvalidError("the task binding key must be 32 bytes")
    if (!Number.isFinite(ttlMs) || ttlMs <= 0)
      throw new InvalidError("the task binding ttl must be a positive number")
    this.taskBinding = { key: key.slice(), ttlMicros: BigInt(Math.ceil(ttlMs * 1000)) }
    return this
  }

  /** The bridge acting for one authenticated caller. */
  caller(principal: string): A2aCaller {
    return new A2aCaller(this, principal)
  }

  // The creation time of the bridge's stream, which tells a recreated stream
  // apart from the one a task id was made on.
  private async streamGeneration(): Promise<bigint> {
    const transport = this.laser[INTERNAL_TRANSPORT]()
    if (transport.findSnapshotStream === undefined)
      throw new HandlerConfigError("the A2A task binding needs the stream creation time")
    const details = await transport.findSnapshotStream(this.stream())
    if (details === undefined) throw new HandlerConfigError("the bridge stream does not exist")
    return details.createdAtMicros
  }

  private stream(): string {
    const stream = this.laser.defaultStream
    if (stream === undefined) {
      throw new NoStreamError(
        "an A2A bridge requires a default stream, use connectWithStream() or withDefaultStream()"
      )
    }
    return stream
  }

  // The id a new task is known by.
  private async taskId(task: ConversationId, caller: string | undefined): Promise<string> {
    const binding = this.taskBinding
    if (binding === undefined) return task.toString()
    if (caller === undefined) throw unboundCaller()
    const generation = await this.streamGeneration()
    const expires = BigInt.asUintN(64, nowMicros() + binding.ttlMicros)
    const mac = taskBindingMac(binding.key, caller, generation, task, expires)
    return `${task.toString()}.${expires.toString()}.${bytesToHex(mac)}`
  }

  // The conversation a task id names, refused like a malformed id when it is
  // not this caller's, expired, or from another stream incarnation.
  private async openTask(id: string, caller: string | undefined): Promise<ConversationId> {
    const invalid = (cause?: unknown) =>
      new HandlerConfigError(`invalid task id \`${id}\``, cause === undefined ? {} : { cause })
    const binding = this.taskBinding
    if (binding === undefined) {
      try {
        return ConversationId.parse(id)
      } catch (cause) {
        throw invalid(cause)
      }
    }
    if (caller === undefined) throw unboundCaller()
    const parts = id.split(".")
    if (parts.length !== 3) throw invalid()
    const [taskText, expiresText, macText] = parts as [string, string, string]
    let task: ConversationId
    let expires: bigint
    let mac: Uint8Array
    try {
      task = ConversationId.parse(taskText)
      if (!/^[0-9]{1,20}$/.test(expiresText)) throw new Error("expiry is not a u64")
      expires = BigInt(expiresText)
      if (expires > 0xffff_ffff_ffff_ffffn) throw new Error("expiry is not a u64")
      if (!/^[0-9a-fA-F]{64}$/.test(macText)) throw new Error("digest is not 32 bytes of hex")
      mac = hexToBytes(macText)
    } catch (cause) {
      throw invalid(cause)
    }
    if (expires < nowMicros()) throw invalid()
    const generation = await this.streamGeneration()
    if (!sameBytes(taskBindingMac(binding.key, caller, generation, task, expires), mac))
      throw invalid()
    return task
  }

  // `openTask` for a remote request: an id this caller cannot open is a task
  // that does not exist for it.
  private async openRequestedTask(
    id: string,
    caller: string | undefined,
    signal: AbortSignal | undefined
  ): Promise<ConversationId> {
    if (this.taskBinding !== undefined && caller === undefined) throw unboundCaller()
    try {
      return await abortable(this.openTask(id, caller), signal, "A2A request")
    } catch (error) {
      if (error instanceof HandlerConfigError) throw new A2aFault(TASK_NOT_FOUND, "task not found")
      throw error
    }
  }

  /** Publishes the params as a task. Raw JSON as bytes or a string rides
   * byte-identical, any other value is encoded as JSON first. `target`
   * addresses the task to one agent, else the bridge's `withTarget` agent.
   * Without either every agent on a shared session topic receives the task. */
  async submit(
    paramsJson: unknown,
    options: CallOptions & { readonly target?: AgentId } = {}
  ): Promise<Task> {
    return this[SUBMIT_FOR](paramsJson, undefined, options.target, undefined, this.hops, options)
  }

  /** `submit` for work that already crossed other bridges: `previousHops` is
   * its `bridge_hops`, and the task carries that path with this bridge
   * appended. A path that already holds this bridge is refused before
   * anything is published. */
  async submitWithHops(
    previousHops: readonly string[],
    paramsJson: unknown,
    options: CallOptions & { readonly target?: AgentId } = {}
  ): Promise<Task> {
    return this[SUBMIT_WITH_HOPS_FOR](previousHops, paramsJson, options.target, undefined, options)
  }

  /** @internal */
  async [SUBMIT_WITH_HOPS_FOR](
    previousHops: readonly string[],
    paramsJson: unknown,
    target: AgentId | undefined,
    caller: string | undefined,
    options: CallOptions = {}
  ): Promise<Task> {
    const hops = callHops(this.hops, previousHops)
    return this[SUBMIT_FOR](paramsJson, undefined, target, caller, hops, options)
  }

  /** `submit` as a child session of `parent`, in the tree rooted at `root`:
   * the task's submitted start lands on `agent.sessions` with the ancestry
   * before the command, and the command carries it too. The agent that
   * handles the task ends the child. `target` addresses the task to one
   * agent, as in `submit`. */
  async submitIn(
    parent: ConversationId,
    root: ConversationId,
    paramsJson: unknown,
    options: CallOptions & { readonly target?: AgentId } = {}
  ): Promise<Task> {
    return this[SUBMIT_FOR](
      paramsJson,
      { parent, root },
      options.target,
      undefined,
      this.hops,
      options
    )
  }

  /** @internal */
  async [SUBMIT_FOR](
    paramsJson: unknown,
    ancestry: { readonly parent: ConversationId; readonly root: ConversationId } | undefined,
    target: AgentId | undefined,
    caller: string | undefined,
    hops: readonly string[] = this.hops,
    options: CallOptions = {}
  ): Promise<Task> {
    throwIfAborted(options.signal, "A2A request")
    const task = ConversationId.new()
    const body = paramsBytes(paramsJson)
    // Made before anything is published, so a bridge that binds tasks refuses
    // an anonymous submit without side effects.
    const id = await abortable(this.taskId(task, caller), options.signal, "A2A request")
    if (ancestry !== undefined) {
      await abortable(
        startChild(this.laser, this.source, this.source, task, ancestry.parent, ancestry.root),
        options.signal,
        "A2A request"
      )
    }
    await this.publishCommand(
      task,
      body,
      target ?? this.target,
      ancestry === undefined
        ? undefined
        : {
            parent: WireConversationId.parse(ancestry.parent.toString()),
            root: WireConversationId.parse(ancestry.root.toString())
          },
      hops,
      options
    )
    return stateTask(id, contextIdOf(body, task), "Submitted")
  }

  private async publishCommand(
    task: ConversationId,
    body: Uint8Array,
    target: AgentId | undefined,
    ancestry:
      | {
          readonly parent: WireConversationId | undefined
          readonly root: WireConversationId | undefined
        }
      | undefined,
    hops: readonly string[],
    options: CallOptions
  ): Promise<void> {
    let command = this.laser
      .agdx(this.requestTopic, this.source, task)
      .command(correlationOf(task), body)
      .withOperation(OPERATION_CHAT)
    if (ancestry !== undefined) command = command.withAncestry(ancestry.parent, ancestry.root)
    if (target !== undefined) command = command.withTarget(target)
    if (this.signingKey !== undefined) command = command.signedBy(this.signingKey)
    await command
      .withMetadata(METADATA_BRIDGE_HOPS, bridgeHopMetadata(hops))
      .contentType(ContentType.Json)
      .send(options)
  }

  /** `GetTask`: the task's current state, read forward from the time its id
   * was minted. Working until an answer lands, and Unknown when the bounded
   * scan could not reach the end of the topic. An id this bridge never
   * published a task for is refused with `HandlerConfigError`. */
  async task(id: string, options: CallOptions = {}): Promise<Task> {
    return this[TASK_FOR](id, undefined, options)
  }

  /** @internal */
  async [TASK_FOR](
    id: string,
    caller: string | undefined,
    options: CallOptions = {}
  ): Promise<Task> {
    throwIfAborted(options.signal, "A2A request")
    const conversation = await abortable(this.openTask(id, caller), options.signal, "A2A request")
    const view = await this.view(conversation, id, options.signal)
    if (view === undefined) throw new HandlerConfigError(`unknown task \`${id}\``)
    return view.task
  }

  /** `CancelTask`: publish the canceled terminal for a task still in
   * progress. An unknown task is refused with `HandlerConfigError`, a task that
   * already ended with `RejectedError`. */
  async cancel(id: string, options: CallOptions = {}): Promise<Task> {
    return this[CANCEL_FOR](id, undefined, options)
  }

  /** @internal */
  async [CANCEL_FOR](
    id: string,
    caller: string | undefined,
    options: CallOptions = {}
  ): Promise<Task> {
    throwIfAborted(options.signal, "A2A request")
    // Checked before the cancel is published, so a foreign id has no effect.
    const conversation = await abortable(this.openTask(id, caller), options.signal, "A2A request")
    const view = await this.view(conversation, id, options.signal)
    if (view === undefined) throw new HandlerConfigError(`unknown task \`${id}\``)
    if (view.terminal) throw new RejectedError("the task already ended")
    return this.publishCancel(conversation, id, view.task.contextId, options)
  }

  private async publishCancel(
    conversation: ConversationId,
    id: string,
    contextId: string,
    options: CallOptions
  ): Promise<Task> {
    let send = this.laser
      .agdx(this.replyTopic, this.source, conversation)
      .fail(correlationOf(conversation), {
        code: { kind: "known", name: "Cancelled" },
        message: "canceled by the A2A client",
        retryable: false
      })
      .withTaskState({ kind: "known", name: "Canceled" })
      .withMetadata(METADATA_BRIDGE_HOPS, bridgeHopMetadata(this.hops))
    if (this.signingKey !== undefined) send = send.signedBy(this.signingKey)
    await send.send(options)
    return stateTask(id, contextId, "Canceled")
  }

  // What the log says about the task: `undefined` when this bridge published
  // no command for it and nothing answered it. A bound id proves the bridge
  // minted it, so it exists even when its command has left retention.
  private async view(
    conversation: ConversationId,
    id: string,
    signal: AbortSignal | undefined
  ): Promise<TaskView | undefined> {
    const records = await this.scanTask(conversation, signal)
    if (
      records.command === undefined &&
      records.answers.length === 0 &&
      records.complete &&
      this.taskBinding === undefined
    ) {
      return undefined
    }
    const contextId =
      records.command === undefined
        ? conversation.toString()
        : contextIdOf(records.command.body, conversation)
    const target = records.command?.target ?? this.target?.asStr()
    const accepted = records.answers.filter((answer) => this.accepts(answer.signer, target))
    const settled = accepted.find((answer) => answerTerminal(answer.envelope)) ?? accepted.at(-1)
    if (settled !== undefined) {
      return {
        task: taskFromEnvelope(id, contextId, settled.envelope),
        terminal: answerTerminal(settled.envelope),
        command: records.command
      }
    }
    // The bounded scan did not reach the end of the topic, so the task may
    // already be answered. Unknown asks the caller to look again.
    const state = records.complete ? "Working" : "Unknown"
    return {
      task: { id, contextId, status: { state: { kind: "known", name: state } }, artifacts: [] },
      terminal: false,
      command: records.command
    }
  }

  // Whether an answer may settle the task: always without a verifier, else
  // only when it verifies and comes from the task's target, or is this
  // bridge's own signed cancel.
  private accepts(signer: Signer, target: string | undefined): boolean {
    switch (signer.kind) {
      case "unchecked":
        return true
      case "refused":
        return false
      case "verified":
        return (
          target === undefined ||
          signer.principal === target ||
          (this.signingKey !== undefined && sameBytes(signer.keyId, this.signingKey.keyId()))
        )
    }
  }

  // The bridge's command for the task and every answer to it, read forward
  // from the time the task id was minted rather than from offset zero.
  private async scanTask(
    task: ConversationId,
    signal: AbortSignal | undefined
  ): Promise<TaskRecords> {
    const correlation = correlationOf(task)
    const since = BigInt.asUintN(64, (task.asU128() >> 80n) * 1000n) - LOOKUP_CLOCK_SLACK_MICROS
    const records: TaskRecords = { answers: [], complete: true }
    const topics =
      this.requestTopic === this.replyTopic
        ? [this.replyTopic]
        : [this.requestTopic, this.replyTopic]
    for (const topic of topics)
      await this.scanTopic(topic, since < 0n ? 0n : since, correlation, records, signal)
    return records
  }

  private async scanTopic(
    topic: string,
    since: bigint,
    correlation: CorrelationId,
    records: TaskRecords,
    signal: AbortSignal | undefined
  ): Promise<void> {
    const stream = this.stream()
    const transport = this.laser[INTERNAL_TRANSPORT]()
    const verifier = this.laser[INTERNAL_VERIFIER]()
    const partitions = await abortable(
      transport.findTopicPartitionCount(stream, topic),
      signal,
      "A2A request"
    )
    for (let partitionId = 0; partitionId < (partitions ?? 0); partitionId += 1) {
      throwIfAborted(signal, "A2A request")
      const target = { kind: "single", partitionId, name: LOOKUP_READER } as const
      const [first] = await transport.pollMessages(
        stream,
        topic,
        target,
        { kind: "timestampMicros", value: since },
        1,
        false,
        undefined,
        signal
      )
      if (first === undefined) continue
      let offset = first.offset
      let caughtUp = false
      for (let poll = 0; poll < MAX_LOOKUP_POLLS; poll += 1) {
        throwIfAborted(signal, "A2A request")
        const messages = await transport.pollMessages(
          stream,
          topic,
          target,
          { kind: "offset", value: offset },
          LOOKUP_BATCH,
          false,
          undefined,
          signal
        )
        if (messages.length === 0) {
          caughtUp = true
          break
        }
        for (const message of messages) this.collect(message, correlation, verifier, records)
        offset = (messages.at(-1)?.offset ?? offset) + 1n
      }
      if (!caughtUp) records.complete = false
    }
  }

  private collect(
    message: PolledMessage,
    correlation: CorrelationId,
    verifier: KeyRegistry | undefined,
    records: TaskRecords
  ): void {
    const decoded = decodeAgentMessage(message)
    if (decoded.kind === "error") return
    const envelope = decoded.message.envelope
    if (envelope?.correlation?.equals(correlation) !== true) return
    const signer = signerOf(envelope, decoded, verifier)
    if (envelope.kind === AgentKind.Command) {
      if (records.command === undefined && this.authored(envelope, signer))
        records.command = envelope
    } else if (envelope.kind === AgentKind.Response || envelope.kind === AgentKind.Error) {
      records.answers.push({ envelope, signer })
    }
  }

  // Whether a command is this bridge's own: its source, and with a signing
  // key a signature by that key that verifies when the `Laser` verifies.
  private authored(envelope: AgentEnvelope, signer: Signer): boolean {
    if (envelope.source !== this.source.asStr()) return false
    const key = this.signingKey
    if (key === undefined) return true
    const signature = envelope.signature
    return (
      signature !== undefined &&
      sameBytes(signature.keyId, key.keyId()) &&
      signer.kind !== "refused"
    )
  }

  /** The bridge's A2A Agent Card. Streaming is not advertised: AGDX chunk
   * channels ride the log, not SSE. Fails with `HandlerConfigError` until
   * `withPublicUrl` names the endpoint. */
  card(): AgentCard {
    if (this.publicUrl === undefined)
      throw new HandlerConfigError("the A2A card needs a public URL, set it with withPublicUrl")
    return {
      name: this.source.asStr(),
      description: this.description,
      version: this.version,
      supportedInterfaces: [
        {
          url: this.publicUrl,
          protocolBinding: A2A_JSONRPC_BINDING,
          protocolVersion: A2A_PROTOCOL_VERSION
        }
      ],
      capabilities: {
        streaming: false,
        pushNotifications: false,
        stateTransitionHistory: false,
        extendedAgentCard: false
      },
      defaultInputModes: ["text/plain"],
      defaultOutputModes: ["text/plain"],
      skills: this.capabilities.map((capability) => ({
        id: capability.skillId,
        name: capability.skillId,
        description: "",
        tags: [],
        ...(capability.input !== undefined
          ? { inputModes: [contentRefMode(capability.input)] }
          : {}),
        ...(capability.output !== undefined
          ? { outputModes: [contentRefMode(capability.output)] }
          : {})
      }))
    }
  }

  signedCard(key: SigningKey): AgentCard {
    const card = this.card()
    return { ...card, signatures: [signCardValue(key, card)] }
  }

  async handleRpc(input: unknown, options: CallOptions = {}): Promise<JsonRpcResponse> {
    return this[HANDLE_RPC_FOR](input, undefined, options)
  }

  /** @internal */
  async [HANDLE_RPC_FOR](
    input: unknown,
    caller: string | undefined,
    options: CallOptions = {}
  ): Promise<JsonRpcResponse> {
    throwIfAborted(options.signal, "A2A request")
    let id: unknown = null
    try {
      if (typeof input !== "object" || input === null || Array.isArray(input))
        throw new A2aFault(INVALID_REQUEST, "invalid request")
      const request = input as Readonly<Record<string, unknown>>
      id = request["id"] ?? null
      const method = request["method"]
      if (typeof method !== "string") throw new A2aFault(INVALID_REQUEST, "invalid request")
      const params = request["params"] ?? {}
      if (typeof params !== "object" || Array.isArray(params))
        throw new A2aFault(INVALID_PARAMS, "invalid params")
      const result = await this.dispatch(
        method,
        params as Readonly<Record<string, unknown>>,
        caller,
        options
      )
      return { jsonrpc: "2.0", id, result }
    } catch (error) {
      // A caller that gave up gets the cancellation, never an answer.
      throwIfAborted(options.signal, "A2A request")
      return { jsonrpc: "2.0", id, error: rpcError(error) }
    }
  }

  private async dispatch(
    method: string,
    params: Readonly<Record<string, unknown>>,
    caller: string | undefined,
    options: CallOptions
  ): Promise<unknown> {
    switch (method) {
      case A2aMethod.MessageSend:
        return { task: taskToJson(await this.sendMessage(params, caller, options)) }
      case A2aMethod.MessageStream:
        throw new A2aFault(UNSUPPORTED_OPERATION, "streaming is not supported")
      case A2aMethod.TasksGet:
      case A2aMethod.TasksCancel: {
        const taskId = params["id"]
        if (typeof taskId !== "string") throw new A2aFault(INVALID_PARAMS, "invalid params")
        const conversation = await this.openRequestedTask(taskId, caller, options.signal)
        const view = await this.view(conversation, taskId, options.signal)
        if (view === undefined) throw new A2aFault(TASK_NOT_FOUND, "task not found")
        if (method === A2aMethod.TasksGet) return taskToJson(view.task)
        if (view.terminal) throw new A2aFault(TASK_NOT_CANCELABLE, "task cannot be canceled")
        return taskToJson(
          await this.publishCancel(conversation, taskId, view.task.contextId, options)
        )
      }
      default:
        throw new A2aFault(METHOD_NOT_FOUND, "method not found")
    }
  }

  // `SendMessage`: a message naming a task continues it in its conversation,
  // addressed to the agent the task went to. Any other message starts a task.
  private async sendMessage(
    params: Readonly<Record<string, unknown>>,
    caller: string | undefined,
    options: CallOptions
  ): Promise<Task> {
    const message = params["message"]
    if (typeof message !== "object" || message === null || Array.isArray(message))
      throw new A2aFault(INVALID_PARAMS, "invalid params")
    const hops = callHops(this.hops, inboundHops(params["metadata"]))
    const taskId = (message as Readonly<Record<string, unknown>>)["taskId"]
    const body = jsonBytes(params, "A2A message params")
    if (taskId === undefined) {
      return this[SUBMIT_FOR](body, undefined, undefined, caller, hops, options)
    }
    if (typeof taskId !== "string") throw new A2aFault(INVALID_PARAMS, "invalid params")
    const conversation = await this.openRequestedTask(taskId, caller, options.signal)
    const view = await this.view(conversation, taskId, options.signal)
    if (view === undefined) throw new A2aFault(TASK_NOT_FOUND, "task not found")
    if (view.terminal)
      throw new A2aFault(UNSUPPORTED_OPERATION, "the task ended and takes no further message")
    const command = view.command
    const target = command?.target === undefined ? this.target : AgentId.new(command.target)
    await this.publishCommand(
      conversation,
      body,
      target,
      command === undefined ? undefined : { parent: command.parent, root: command.root },
      hops,
      options
    )
    return stateTask(taskId, view.task.contextId, "Working")
  }
}

// What the log says about a task: its settled state, whether that state ended
// it, and the bridge's command when it is still retained.
interface TaskView {
  readonly task: Task
  readonly terminal: boolean
  readonly command: AgentEnvelope | undefined
}

interface TaskRecords {
  command?: AgentEnvelope
  answers: Answer[]
  complete: boolean
}

interface Answer {
  readonly envelope: AgentEnvelope
  readonly signer: Signer
}

// Who an envelope verifies to under the `Laser`'s verifier: unchecked without
// one, refused when it does not verify.
type Signer =
  | { readonly kind: "unchecked" }
  | { readonly kind: "refused" }
  | { readonly kind: "verified"; readonly principal: string; readonly keyId: Uint8Array }

function signerOf(
  envelope: AgentEnvelope,
  decoded: { readonly signatureContext?: SignatureContext; readonly observedAtMicros?: bigint },
  verifier: KeyRegistry | undefined
): Signer {
  if (verifier === undefined) return { kind: "unchecked" }
  const signature = envelope.signature
  if (
    signature === undefined ||
    decoded.signatureContext === undefined ||
    decoded.observedAtMicros === undefined
  ) {
    return { kind: "refused" }
  }
  try {
    const verified = verifier.verifyObservedAt(
      envelope,
      decoded.signatureContext,
      decoded.observedAtMicros
    )
    return { kind: "verified", principal: verified.principal, keyId: signature.keyId }
  } catch {
    return { kind: "refused" }
  }
}

function answerTerminal(envelope: AgentEnvelope): boolean {
  return taskStateIsTerminal(
    envelope.taskState ??
      (envelope.kind === AgentKind.Error
        ? { kind: "known", name: "Failed" }
        : { kind: "known", name: "Completed" })
  )
}

// A request refused with its JSON-RPC or A2A error code and a fixed public message.
class A2aFault extends Error {
  constructor(
    readonly code: number,
    readonly publicMessage: string
  ) {
    super(publicMessage)
  }
}

// The JSON-RPC error a failure answers with: a fault carries its own code, a
// client mistake is invalid params, anything else an internal error.
function rpcError(error: unknown): JsonRpcError {
  if (error instanceof A2aFault) return { code: error.code, message: error.publicMessage }
  const result = code(error)
  if (result.kind === "known" && result.name === "InvalidArgument")
    return { code: INVALID_PARAMS, message: "invalid params" }
  return { code: INTERNAL_ERROR, message: publicErrorMessage(error) }
}

export { AgentTopic }
