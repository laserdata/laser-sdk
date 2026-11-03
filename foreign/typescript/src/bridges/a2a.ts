import { blake3 } from "@noble/hashes/blake3.js"
import { bytesToHex, hexToBytes } from "@noble/hashes/utils.js"
import { publicErrorMessage } from "../client/error-classify.js"
import { startChild } from "../agent/contract.js"
import { findAgdxReply } from "../agent/replies.js"
import { CodecError, HandlerConfigError, InvalidError, NoStreamError } from "../client/errors.js"
import { INTERNAL_TRANSPORT, INTERNAL_VERIFIER } from "../client/internals.js"
import type { Laser } from "../client/laser.js"
import { AgentTopic } from "../provenance/agent-topic.js"
import { signCardValue, type AgentCardSignature, type SigningKey } from "../signing.js"
import { ConversationId, type AgentId } from "../types/ids.js"
import {
  AgentKind,
  METADATA_BRIDGE_HOPS,
  OPERATION_CHAT,
  taskStateDisplay,
  commandEnvelope,
  withOperation,
  type AgentId as WireAgentId,
  type AgentEnvelope,
  type CapabilityDescriptor,
  type ContentRef,
  type TaskState
} from "../wire/agent.js"
import { ContentType } from "../wire/content.js"
import { ConversationId as WireConversationId, CorrelationId, type RecordId } from "../wire/ids.js"
import { bridgeHopMetadata, enterBridge } from "./hops.js"
import { SDK_VERSION } from "../version.js"

export const A2A_PROTOCOL_VERSION = "1.0"
export const A2A_JSONRPC_BINDING = "JSONRPC"
const A2A_APP_ERROR_CODE = -32_000
export { SDK_VERSION }

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

export interface Artifact {
  readonly text: string
}

export interface Task {
  readonly id: string
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

function jsonObject(value: unknown, context: string): Readonly<Record<string, unknown>> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new HandlerConfigError(`${context} must be a JSON object`)
  }
  return value as Readonly<Record<string, unknown>>
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

export function taskFromEnvelope(taskId: string, envelope: AgentEnvelope): Task {
  const state: TaskState =
    envelope.taskState ??
    (envelope.kind === AgentKind.Error
      ? { kind: "known", name: "Failed" }
      : { kind: "known", name: "Completed" })
  return {
    id: taskId,
    status: { state },
    artifacts:
      envelope.body.byteLength === 0 ? [] : [{ text: new TextDecoder().decode(envelope.body) }]
  }
}

export function taskToJson(task: Task): unknown {
  return {
    id: task.id,
    status: { state: taskStateDisplay(task.status.state) },
    ...(task.artifacts.length > 0 ? { artifacts: task.artifacts } : {})
  }
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

function submittedTask(id: string): Task {
  return {
    id,
    status: { state: { kind: "known", name: "Submitted" } },
    artifacts: []
  }
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
  async submit(paramsJson: unknown, options: { readonly target?: AgentId } = {}): Promise<Task> {
    return this.bridge[SUBMIT_FOR](paramsJson, undefined, options.target, this.principal)
  }

  /** `A2aBridge.submitIn` for this caller. */
  async submitIn(
    parent: ConversationId,
    root: ConversationId,
    paramsJson: unknown,
    options: { readonly target?: AgentId } = {}
  ): Promise<Task> {
    return this.bridge[SUBMIT_FOR](paramsJson, { parent, root }, options.target, this.principal)
  }

  /** `A2aBridge.task` for this caller. Another caller's task id answers like
   * an unknown one. */
  async task(id: string): Promise<Task> {
    return this.bridge[TASK_FOR](id, this.principal)
  }

  /** `A2aBridge.cancel` for this caller. Another caller's task id is refused
   * before anything is published. */
  async cancel(id: string): Promise<Task> {
    return this.bridge[CANCEL_FOR](id, this.principal)
  }

  /** `A2aBridge.handleRpc` for this caller. */
  async handleRpc(input: unknown): Promise<JsonRpcResponse> {
    return this.bridge[HANDLE_RPC_FOR](input, this.principal)
  }
}

const SUBMIT_FOR = Symbol("a2a submit for caller")
const TASK_FOR = Symbol("a2a task for caller")
const CANCEL_FOR = Symbol("a2a cancel for caller")
const HANDLE_RPC_FOR = Symbol("a2a handle rpc for caller")

export class A2aBridge {
  private capabilities: readonly CapabilityDescriptor[] = []
  private signingKey: SigningKey | undefined
  private hops: readonly string[]
  private taskBinding: TaskBinding | undefined

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

  withSigningKey(key: SigningKey): this {
    this.signingKey = key
    return this
  }

  withCapabilities(capabilities: readonly CapabilityDescriptor[]): this {
    this.capabilities = [...capabilities]
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
    const stream = this.laser.defaultStream
    if (stream === undefined) {
      throw new NoStreamError(
        "an A2A task binding requires a default stream, use connectWithStream() or withDefaultStream()"
      )
    }
    const transport = this.laser[INTERNAL_TRANSPORT]()
    if (transport.findSnapshotStream === undefined)
      throw new HandlerConfigError("the A2A task binding needs the stream creation time")
    const details = await transport.findSnapshotStream(stream)
    if (details === undefined) throw new HandlerConfigError("the bridge stream does not exist")
    return details.createdAtMicros
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

  /** Publishes the params as a task. Raw JSON as bytes or a string rides
   * byte-identical, any other value is encoded as JSON first. `target`
   * addresses the task to one agent. Without it every agent on a shared
   * session topic receives the task. */
  async submit(paramsJson: unknown, options: { readonly target?: AgentId } = {}): Promise<Task> {
    return this[SUBMIT_FOR](paramsJson, undefined, options.target, undefined)
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
    options: { readonly target?: AgentId } = {}
  ): Promise<Task> {
    return this[SUBMIT_FOR](paramsJson, { parent, root }, options.target, undefined)
  }

  /** @internal */
  async [SUBMIT_FOR](
    paramsJson: unknown,
    ancestry: { readonly parent: ConversationId; readonly root: ConversationId } | undefined,
    target: AgentId | undefined,
    caller: string | undefined
  ): Promise<Task> {
    const task = ConversationId.new()
    // Made before anything is published, so a bridge that binds tasks refuses
    // an anonymous submit without side effects.
    const id = await this.taskId(task, caller)
    if (ancestry !== undefined) {
      await startChild(this.laser, this.source, this.source, task, ancestry.parent, ancestry.root)
    }
    let command = this.laser
      .agdx(this.requestTopic, this.source, task)
      .command(correlationOf(task), paramsBytes(paramsJson))
      .withOperation(OPERATION_CHAT)
    if (ancestry !== undefined) {
      command = command.withAncestry(
        WireConversationId.parse(ancestry.parent.toString()),
        WireConversationId.parse(ancestry.root.toString())
      )
    }
    if (target !== undefined) command = command.withTarget(target)
    await command
      .withMetadata(METADATA_BRIDGE_HOPS, bridgeHopMetadata(this.hops))
      .contentType(ContentType.Json)
      .send()
    return submittedTask(id)
  }

  /** `GetTask`: the task's current state. Working until an answer lands, and
   * Unknown when the bounded scan could not reach the end of the reply topic. */
  async task(id: string): Promise<Task> {
    return this[TASK_FOR](id, undefined)
  }

  /** @internal */
  async [TASK_FOR](id: string, caller: string | undefined): Promise<Task> {
    const conversation = await this.openTask(id, caller)
    const stream = this.laser.defaultStream
    if (stream === undefined) {
      throw new NoStreamError(
        "an A2A task lookup requires a default stream, use connectWithStream() or withDefaultStream()"
      )
    }
    const answer = await findAgdxReply(
      this.laser[INTERNAL_TRANSPORT](),
      stream,
      this.replyTopic,
      correlationOf(conversation),
      this.laser[INTERNAL_VERIFIER]()
    )
    switch (answer.kind) {
      case "found":
        return taskFromEnvelope(id, answer.envelope)
      case "notFound":
        return { id, status: { state: { kind: "known", name: "Working" } }, artifacts: [] }
      // The bounded scan did not reach the end of the reply topic, so the task
      // may already be answered. Unknown asks the caller to look again.
      case "incomplete":
        return { id, status: { state: { kind: "known", name: "Unknown" } }, artifacts: [] }
    }
  }

  /** `CancelTask`: publish the canceled terminal for the task. */
  async cancel(id: string): Promise<Task> {
    return this[CANCEL_FOR](id, undefined)
  }

  /** @internal */
  async [CANCEL_FOR](id: string, caller: string | undefined): Promise<Task> {
    // Checked before the cancel is published, so a foreign id has no effect.
    const conversation = await this.openTask(id, caller)
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
    await send.send()
    return {
      id,
      status: { state: { kind: "known", name: "Canceled" } },
      artifacts: []
    }
  }

  card(): AgentCard {
    return {
      name: this.source.asStr(),
      description: "LaserData AGDX bridge over the durable log",
      version: SDK_VERSION,
      supportedInterfaces: [
        { url: "/", protocolBinding: A2A_JSONRPC_BINDING, protocolVersion: A2A_PROTOCOL_VERSION }
      ],
      capabilities: {
        streaming: true,
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

  async handleRpc(input: unknown): Promise<JsonRpcResponse> {
    return this[HANDLE_RPC_FOR](input, undefined)
  }

  /** @internal */
  async [HANDLE_RPC_FOR](input: unknown, caller: string | undefined): Promise<JsonRpcResponse> {
    let id: unknown = null
    try {
      const request = jsonObject(input, "A2A JSON-RPC request")
      id = request["id"] ?? null
      const method = request["method"]
      if (typeof method !== "string") throw new HandlerConfigError("A2A method must be a string")
      const params = request["params"] ?? {}
      let task: Task
      switch (method) {
        case A2aMethod.MessageSend:
        case A2aMethod.MessageStream:
          task = await this[SUBMIT_FOR](
            jsonBytes(params, "A2A message params"),
            undefined,
            undefined,
            caller
          )
          break
        case A2aMethod.TasksGet:
        case A2aMethod.TasksCancel: {
          const object = jsonObject(params, "A2A task params")
          const taskId = object["id"]
          if (typeof taskId !== "string")
            throw new HandlerConfigError("A2A task id must be a string")
          task =
            method === A2aMethod.TasksGet
              ? await this[TASK_FOR](taskId, caller)
              : await this[CANCEL_FOR](taskId, caller)
          break
        }
        default:
          throw new HandlerConfigError(`unknown A2A method \`${method}\``)
      }
      return { jsonrpc: "2.0", id, result: taskToJson(task) }
    } catch (error) {
      return {
        jsonrpc: "2.0",
        id,
        error: {
          code: A2A_APP_ERROR_CODE,
          message: publicErrorMessage(error)
        }
      }
    }
  }
}

export { AgentTopic }
