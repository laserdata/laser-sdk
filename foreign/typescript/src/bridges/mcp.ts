import { startChild } from "../agent/contract.js"
import { publicErrorMessage } from "../client/error-classify.js"
import { CodecError, HandlerConfigError, InvalidError, TimeoutError } from "../client/errors.js"
import { INTERNAL_REPLY_HUB } from "../client/internals.js"
import type { Laser } from "../client/laser.js"
import { ConversationId, type AgentId } from "../types/ids.js"
import {
  AgentKind,
  METADATA_BRIDGE_HOPS,
  commandEnvelope,
  withTool,
  type AgentId as WireAgentId,
  type AgentEnvelope,
  type AgentErrorBody
} from "../wire/agent.js"
import { ContentType } from "../wire/content.js"
import { ConversationId as WireConversationId, CorrelationId, type RecordId } from "../wire/ids.js"
import { SDK_VERSION } from "./a2a.js"
import { bridgeHopMetadata, enterBridge } from "./hops.js"

export const MCP_DEFAULT_PROTOCOL_VERSION = "2025-11-25"
export const MCP_APP_ERROR_CODE = -32_000

export const McpMethod = {
  Initialize: "initialize",
  ToolsList: "tools/list",
  ToolsCall: "tools/call",
  ResourcesList: "resources/list",
  ResourcesRead: "resources/read",
  PromptsList: "prompts/list",
  PromptsGet: "prompts/get"
} as const
export type McpMethod = (typeof McpMethod)[keyof typeof McpMethod]

export interface McpResource {
  readonly uri: string
  readonly name: string
  readonly title?: string
  readonly description?: string
  readonly mimeType?: string
}

export interface McpPromptArgument {
  readonly name: string
  readonly description?: string
  readonly required?: boolean
}

export interface McpPrompt {
  readonly name: string
  readonly title?: string
  readonly description?: string
  readonly arguments?: readonly McpPromptArgument[]
}

export interface McpTool {
  readonly name: string
  readonly title?: string
  readonly description?: string
  readonly inputSchema: Readonly<Record<string, unknown>>
}

export interface McpContent {
  readonly kind: "text"
  readonly text: string
}

export interface McpToolResult {
  readonly content: readonly McpContent[]
  readonly isError?: true
}

export interface McpRpcRequest {
  readonly id?: unknown
  readonly method: string
  readonly params?: unknown
}

export interface McpRpcError {
  readonly code: number
  readonly message: string
}

export interface McpRpcResponse {
  readonly jsonrpc: "2.0"
  readonly id: unknown
  readonly result?: unknown
  readonly error?: McpRpcError
}

interface ResourceEntry {
  readonly resource: McpResource
  readonly text: string
}

interface PromptEntry {
  readonly prompt: McpPrompt
  readonly messages: readonly (readonly [string, string])[]
}

// MCP requires an object input schema, so anything else is refused when the
// tool is registered, as in Rust and Python.
function toolSchema(value: unknown): Readonly<Record<string, unknown>> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new InvalidError("MCP tool input schema must be a JSON object")
  }
  return value as Readonly<Record<string, unknown>>
}

function jsonObject(value: unknown, context: string): Readonly<Record<string, unknown>> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new HandlerConfigError(`${context} must be a JSON object`)
  }
  return value as Readonly<Record<string, unknown>>
}

function jsonBytes(value: unknown): Uint8Array {
  try {
    const encoded: unknown = JSON.stringify(value)
    if (typeof encoded !== "string") throw new Error("value is not JSON serializable")
    return new TextEncoder().encode(encoded)
  } catch (cause) {
    throw new CodecError("MCP params are not JSON serializable", "mcp", "json", { cause })
  }
}

/** Build the command for an MCP tool call. The body keeps the original JSON bytes. */
export function toolCallFromRequest(
  record: RecordId,
  conversation: WireConversationId,
  source: WireAgentId,
  correlation: CorrelationId,
  toolName: string,
  paramsJson: Uint8Array
): AgentEnvelope {
  return withTool(
    commandEnvelope(record, conversation, source, correlation, paramsJson.slice()),
    toolName
  )
}

// Raw JSON as bytes or a string rides byte-identical, like the A2A bridge and
// Python. Any other value is encoded as JSON first.
function paramsBytes(paramsJson: unknown): Uint8Array {
  return paramsJson instanceof Uint8Array
    ? paramsJson
    : typeof paramsJson === "string"
      ? new TextEncoder().encode(paramsJson)
      : jsonBytes(paramsJson)
}

function toolFailure(message: string): AgentErrorBody {
  return { code: { kind: "known", name: "Internal" }, message, retryable: false }
}

export function toolResultFromEnvelope(envelope: AgentEnvelope): McpToolResult {
  const text = new TextDecoder().decode(envelope.body)
  return {
    content: text.length === 0 ? [] : [{ kind: "text", text }],
    ...(envelope.kind === AgentKind.Error ? { isError: true } : {})
  }
}

export class McpBridge {
  private readonly tools: McpTool[] = []
  private readonly resources: ResourceEntry[] = []
  private readonly prompts: PromptEntry[] = []
  private timeoutMs = 30_000
  private hops: readonly string[]

  constructor(
    private readonly laser: Laser,
    private readonly source: AgentId,
    private readonly toolTopic: string,
    private readonly replyTopic: string,
    private readonly serverName: string
  ) {
    this.hops = enterBridge(source.asStr())
  }

  withBridgeHops(previous: readonly string[]): this {
    this.hops = enterBridge(this.source.asStr(), previous)
    return this
  }

  withResource(uri: string, name: string, mimeType: string | undefined, text: string): this {
    this.resources.push({
      resource: { uri, name, ...(mimeType !== undefined ? { mimeType } : {}) },
      text
    })
    return this
  }

  withPrompt(prompt: McpPrompt, messages: readonly (readonly [string, string])[]): this {
    this.prompts.push({ prompt, messages: [...messages] })
    return this
  }

  withTool(name: string, description: string | undefined, inputSchema: unknown): this {
    this.tools.push({
      name,
      ...(description !== undefined ? { description } : {}),
      inputSchema: toolSchema(inputSchema)
    })
    return this
  }

  withMemoryTools(): this {
    return this.withTool("remember", "Store a memory item for later recall.", {
      type: "object",
      properties: {
        text: { type: "string", description: "The content to remember." }
      },
      required: ["text"]
    }).withTool("recall", "Retrieve the memory items most relevant to a query.", {
      type: "object",
      properties: {
        query: { type: "string", description: "What to recall." },
        limit: { type: "integer", minimum: 1, description: "Max items to return." }
      },
      required: ["query"]
    })
  }

  withTimeout(timeoutMs: number): this {
    if (!Number.isFinite(timeoutMs) || timeoutMs < 0) {
      throw new InvalidError("MCP call timeout must be a non-negative finite number")
    }
    this.timeoutMs = timeoutMs
    return this
  }

  initialize(protocolVersion?: string): unknown {
    return {
      protocolVersion: protocolVersion ?? MCP_DEFAULT_PROTOCOL_VERSION,
      serverInfo: { name: this.serverName, version: SDK_VERSION },
      capabilities: {
        tools: {},
        ...(this.resources.length > 0 ? { resources: {} } : {}),
        ...(this.prompts.length > 0 ? { prompts: {} } : {})
      }
    }
  }

  listTools(): unknown {
    return { tools: this.tools }
  }

  listResources(): unknown {
    return { resources: this.resources.map((entry) => entry.resource) }
  }

  readResource(uri: string): unknown {
    const entry = this.resources.find((candidate) => candidate.resource.uri === uri)
    if (entry === undefined) throw new InvalidError(`unknown resource \`${uri}\``)
    return {
      contents: [
        {
          uri: entry.resource.uri,
          ...(entry.resource.mimeType !== undefined ? { mimeType: entry.resource.mimeType } : {}),
          text: entry.text
        }
      ]
    }
  }

  listPrompts(): unknown {
    return { prompts: this.prompts.map((entry) => entry.prompt) }
  }

  getPrompt(name: string): unknown {
    const entry = this.prompts.find((candidate) => candidate.prompt.name === name)
    if (entry === undefined) throw new InvalidError(`unknown prompt \`${name}\``)
    return {
      ...(entry.prompt.description !== undefined ? { description: entry.prompt.description } : {}),
      messages: entry.messages.map(([role, text]) => ({
        role,
        content: { type: "text", text }
      }))
    }
  }

  /** `tools/call`: route the call to the agent and await its tool result.
   * `paramsJson` is the JSON the call carries: raw JSON bytes or text, sent
   * as is, or another value to encode as JSON. `target` addresses the call to one agent. Without it
   * every agent on a shared session topic receives the call. */
  async callTool(
    name: string,
    paramsJson: unknown,
    options: { readonly target?: AgentId } = {}
  ): Promise<McpToolResult> {
    const conversation = ConversationId.new()
    return toolResultFromEnvelope(
      await this.sendToolCall(
        conversation,
        name,
        paramsBytes(paramsJson),
        undefined,
        options.target
      )
    )
  }

  /** `callTool` as a child session of `parent`, in the tree rooted at `root`:
   * the call's submitted start lands on `agent.sessions` with the ancestry
   * before the command, and the child ends by the result, completed on a tool
   * result and failed on a tool error. A wait error surfaces before a
   * lifecycle error. `target` addresses the call to one agent, as in
   * `callTool`. */
  async callToolIn(
    parent: ConversationId,
    root: ConversationId,
    name: string,
    paramsJson: unknown,
    options: { readonly target?: AgentId } = {}
  ): Promise<McpToolResult> {
    const conversation = ConversationId.new()
    const child = await startChild(this.laser, this.source, this.source, conversation, parent, root)
    let envelope: AgentEnvelope
    try {
      envelope = await this.sendToolCall(
        conversation,
        name,
        paramsBytes(paramsJson),
        {
          parent: WireConversationId.parse(parent.toString()),
          root: WireConversationId.parse(root.toString())
        },
        options.target
      )
    } catch (error) {
      await child
        .fail(toolFailure(error instanceof Error ? error.message : String(error)))
        .catch(() => undefined)
      throw error
    }
    if (envelope.kind === AgentKind.Error)
      await child.fail(toolFailure("the tool returned an error"))
    else await child.end()
    return toolResultFromEnvelope(envelope)
  }

  private async sendToolCall(
    conversation: ConversationId,
    name: string,
    paramsJson: Uint8Array,
    ancestry:
      { readonly parent: WireConversationId; readonly root: WireConversationId } | undefined,
    target: AgentId | undefined
  ): Promise<AgentEnvelope> {
    const correlation = CorrelationId.parse(conversation.toString())
    const hub = await this.laser[INTERNAL_REPLY_HUB](this.replyTopic, this.source)
    const ticket = hub.subscribeStream(correlation.toString())
    const started = performance.now()
    try {
      let command = this.laser
        .agdx(this.toolTopic, this.source, conversation)
        .command(correlation, paramsJson)
        .withTool(name)
      if (ancestry !== undefined) command = command.withAncestry(ancestry.parent, ancestry.root)
      if (target !== undefined) command = command.withTarget(target)
      await command
        .withMetadata(METADATA_BRIDGE_HOPS, bridgeHopMetadata(this.hops))
        .contentType(ContentType.Json)
        .send()
      for (;;) {
        const remaining = this.timeoutMs - (performance.now() - started)
        if (remaining <= 0) throw new TimeoutError("the MCP tool reply")
        const message = await ticket.next(remaining)
        const envelope = message.envelope
        if (
          envelope !== undefined &&
          (envelope.kind === AgentKind.Response || envelope.kind === AgentKind.Error)
        ) {
          return envelope
        }
      }
    } finally {
      ticket.cancel()
    }
  }

  async handleRpc(input: unknown): Promise<McpRpcResponse> {
    let id: unknown = null
    try {
      const request = jsonObject(input, "MCP JSON-RPC request")
      id = request["id"] ?? null
      const method = request["method"]
      if (typeof method !== "string") throw new HandlerConfigError("MCP method must be a string")
      const params = jsonObject(request["params"] ?? {}, "MCP params")
      let result: unknown
      switch (method) {
        case McpMethod.Initialize: {
          const version = params["protocolVersion"]
          if (version !== undefined && typeof version !== "string") {
            throw new HandlerConfigError("MCP protocolVersion must be a string")
          }
          result = this.initialize(version)
          break
        }
        case McpMethod.ToolsList:
          result = this.listTools()
          break
        case McpMethod.ResourcesList:
          result = this.listResources()
          break
        case McpMethod.ResourcesRead:
          result = this.readResource(this.requiredString(params, "uri"))
          break
        case McpMethod.PromptsList:
          result = this.listPrompts()
          break
        case McpMethod.PromptsGet:
          result = this.getPrompt(this.requiredString(params, "name"))
          break
        case McpMethod.ToolsCall:
          result = toolResultJson(
            await this.callTool(this.requiredString(params, "name"), jsonBytes(params))
          )
          break
        default:
          throw new HandlerConfigError(`unknown MCP method \`${method}\``)
      }
      return { jsonrpc: "2.0", id, result }
    } catch (error) {
      return {
        jsonrpc: "2.0",
        id,
        error: {
          code: MCP_APP_ERROR_CODE,
          message: publicErrorMessage(error)
        }
      }
    }
  }

  private requiredString(object: Readonly<Record<string, unknown>>, key: string): string {
    const value = object[key]
    if (typeof value !== "string") throw new HandlerConfigError(`MCP ${key} must be a string`)
    return value
  }
}

// MCP spells a content item's kind `type` on the wire.
function toolResultJson(result: McpToolResult): unknown {
  return {
    content: result.content.map((item) => ({ type: item.kind, text: item.text })),
    ...(result.isError === true ? { isError: true } : {})
  }
}
