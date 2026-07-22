import { CodecError, HandlerConfigError } from "../client/errors.js"
import type { Laser } from "../client/laser.js"
import { AgentTopic } from "../provenance/agent-topic.js"
import { signCardValue, type AgentCardSignature, type SigningKey } from "../signing.js"
import { ConversationId, type AgentId } from "../types/ids.js"
import {
  AgentKind,
  METADATA_BRIDGE_HOPS,
  OPERATION_CHAT,
  taskStateDisplay,
  type AgentEnvelope,
  type CapabilityDescriptor,
  type ContentRef,
  type TaskState
} from "../wire/agent.js"
import { ContentType } from "../wire/content.js"
import { CorrelationId } from "../wire/ids.js"
import { bridgeHopMetadata, enterBridge } from "./hops.js"

export const A2A_PROTOCOL_VERSION = "1.0"
export const A2A_JSONRPC_BINDING = "JSONRPC"
export const A2A_APP_ERROR_CODE = -32_000
export const SDK_VERSION = "0.0.1-rc.1"

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

export interface A2aAgentCard {
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

export interface A2aTask {
  readonly id: string
  readonly status: { readonly state: TaskState }
  readonly artifacts: readonly { readonly text: string }[]
}

export interface JsonRpcResponse {
  readonly jsonrpc: "2.0"
  readonly id: unknown
  readonly result?: unknown
  readonly error?: { readonly code: number; readonly message: string }
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

export function taskFromEnvelope(taskId: string, envelope: AgentEnvelope): A2aTask {
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

export function taskToJson(task: A2aTask): unknown {
  return {
    id: task.id,
    status: { state: taskStateDisplay(task.status.state) },
    ...(task.artifacts.length > 0 ? { artifacts: task.artifacts } : {})
  }
}

export class A2aBridge {
  private capabilities: readonly CapabilityDescriptor[] = []
  private signingKey: SigningKey | undefined
  private hops: readonly string[]

  constructor(
    private readonly laser: Laser,
    private readonly source: AgentId,
    private readonly requestTopic: string,
    private readonly replyTopic: string
  ) {
    this.hops = enterBridge(source.asString())
  }

  withBridgeHops(previous: readonly string[]): this {
    this.hops = enterBridge(this.source.asString(), previous)
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

  async submit(params: unknown): Promise<A2aTask> {
    return this.submitJson(jsonBytes(params, "A2A message params"))
  }

  async submitJson(paramsJson: Uint8Array): Promise<A2aTask> {
    const task = ConversationId.new()
    await this.laser
      .agdx(this.requestTopic, this.source, task)
      .command(correlationOf(task), paramsJson)
      .withOperation(OPERATION_CHAT)
      .withMetadata(METADATA_BRIDGE_HOPS, bridgeHopMetadata(this.hops))
      .contentType(ContentType.Json)
      .send()
    return {
      id: task.toString(),
      status: { state: { kind: "known", name: "Submitted" } },
      artifacts: []
    }
  }

  async task(id: string): Promise<A2aTask> {
    let conversation: ConversationId
    try {
      conversation = ConversationId.parse(id)
    } catch (cause) {
      throw new HandlerConfigError(`invalid task id \`${id}\``, { cause })
    }
    const correlation = correlationOf(conversation)
    const messages = await this.laser
      .context(conversation)
      .fetch([this.replyTopic], Number.MAX_SAFE_INTEGER)
    const answer = messages.findLast(
      (message) =>
        message.envelope?.correlation?.equals(correlation) === true &&
        (message.envelope.kind === AgentKind.Response || message.envelope.kind === AgentKind.Error)
    )?.envelope
    return answer === undefined
      ? {
          id,
          status: { state: { kind: "known", name: "Working" } },
          artifacts: []
        }
      : taskFromEnvelope(id, answer)
  }

  async cancel(id: string): Promise<A2aTask> {
    let conversation: ConversationId
    try {
      conversation = ConversationId.parse(id)
    } catch (cause) {
      throw new HandlerConfigError(`invalid task id \`${id}\``, { cause })
    }
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

  card(): A2aAgentCard {
    return {
      name: this.source.asString(),
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

  signedCard(key: SigningKey): A2aAgentCard {
    const card = this.card()
    return { ...card, signatures: [signCardValue(key, card)] }
  }

  async handleRpc(input: unknown): Promise<JsonRpcResponse> {
    let id: unknown = null
    try {
      const request = jsonObject(input, "A2A JSON-RPC request")
      id = request["id"] ?? null
      const method = request["method"]
      if (typeof method !== "string") throw new HandlerConfigError("A2A method must be a string")
      const params = request["params"] ?? {}
      let task: A2aTask
      switch (method) {
        case A2aMethod.MessageSend:
        case A2aMethod.MessageStream:
          task = await this.submit(params)
          break
        case A2aMethod.TasksGet:
        case A2aMethod.TasksCancel: {
          const object = jsonObject(params, "A2A task params")
          const taskId = object["id"]
          if (typeof taskId !== "string")
            throw new HandlerConfigError("A2A task id must be a string")
          task = method === A2aMethod.TasksGet ? await this.task(taskId) : await this.cancel(taskId)
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
          message: error instanceof Error ? error.message : String(error)
        }
      }
    }
  }
}

export { AgentTopic }
