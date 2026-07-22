import { CodecError, InvalidError } from "../client/errors.js"
import type { Laser } from "../client/laser.js"
import type { AgentId, ConversationId } from "../types/ids.js"
import {
  AgentKind,
  OPERATION_REASONING,
  OPERATION_STATE_DELTA,
  OPERATION_STATE_SNAPSHOT,
  OPERATION_TASK,
  OPERATION_TOOL_ARGS,
  taskStateIsTerminal,
  type AgentEnvelope
} from "../wire/agent.js"
import { ContentType } from "../wire/content.js"

export type AgUiEvent =
  | { readonly type: "RUN_STARTED"; readonly threadId: string; readonly runId: string }
  | { readonly type: "RUN_FINISHED"; readonly threadId: string; readonly runId: string }
  | { readonly type: "TEXT_MESSAGE_START"; readonly messageId: string; readonly role: string }
  | { readonly type: "TEXT_MESSAGE_CONTENT"; readonly messageId: string; readonly delta: string }
  | { readonly type: "TEXT_MESSAGE_END"; readonly messageId: string }
  | { readonly type: "REASONING_MESSAGE_START"; readonly messageId: string; readonly role: string }
  | {
      readonly type: "REASONING_MESSAGE_CONTENT"
      readonly messageId: string
      readonly delta: string
    }
  | { readonly type: "REASONING_MESSAGE_END"; readonly messageId: string }
  | { readonly type: "TOOL_CALL_START"; readonly toolCallId: string; readonly toolCallName: string }
  | { readonly type: "TOOL_CALL_ARGS"; readonly toolCallId: string; readonly delta: string }
  | { readonly type: "TOOL_CALL_END"; readonly toolCallId: string }
  | { readonly type: "TOOL_CALL_RESULT"; readonly toolCallId: string; readonly content: string }
  | { readonly type: "STATE_SNAPSHOT"; readonly snapshot: unknown }
  | { readonly type: "STATE_DELTA"; readonly delta: unknown }
  | { readonly type: "RUN_ERROR"; readonly message: string }

type ChunkKind = "chat" | "reasoning" | "toolArgs"

function jsonBytes(value: unknown, operation: string): Uint8Array {
  try {
    const encoded: unknown = JSON.stringify(value)
    if (typeof encoded !== "string") throw new Error("value is not JSON serializable")
    return new TextEncoder().encode(encoded)
  } catch (cause) {
    throw new CodecError(`cannot encode ${operation}`, "agui", operation, { cause })
  }
}

function parseJson(bytes: Uint8Array, operation: string): unknown {
  try {
    return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes)) as unknown
  } catch (cause) {
    throw new CodecError(`cannot decode ${operation}`, "agui", operation, { cause })
  }
}

function decodePointer(path: string): readonly string[] {
  if (path === "") return []
  if (!path.startsWith("/")) throw new InvalidError(`invalid JSON Pointer \`${path}\``)
  return path
    .slice(1)
    .split("/")
    .map((part) => {
      if (/~(?:[^01]|$)/.test(part)) throw new InvalidError(`invalid JSON Pointer \`${path}\``)
      return part.replaceAll("~1", "/").replaceAll("~0", "~")
    })
}

function arrayIndex(token: string, length: number, allowEnd: boolean): number {
  if (token === "-" && allowEnd) return length
  if (!/^(?:0|[1-9][0-9]*)$/.test(token)) {
    throw new InvalidError(`invalid JSON Patch array index \`${token}\``)
  }
  const index = Number(token)
  const maximum = allowEnd ? length : length - 1
  if (!Number.isSafeInteger(index) || index < 0 || index > maximum) {
    throw new InvalidError(`JSON Patch array index \`${token}\` is out of bounds`)
  }
  return index
}

function objectOf(value: unknown, path: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new InvalidError(`JSON Patch path \`${path}\` does not address a container`)
  }
  return value as Record<string, unknown>
}

function parentAt(document: unknown, tokens: readonly string[], path: string): [unknown, string] {
  if (tokens.length === 0) throw new InvalidError("the document root has no parent")
  let current = document
  for (const token of tokens.slice(0, -1)) {
    if (Array.isArray(current)) {
      current = current[arrayIndex(token, current.length, false)]
    } else {
      const object = objectOf(current, path)
      if (!(token in object)) throw new InvalidError(`JSON Patch path \`${path}\` does not exist`)
      current = object[token]
    }
  }
  return [current, tokens.at(-1) ?? ""]
}

function valueAt(document: unknown, path: string): unknown {
  const tokens = decodePointer(path)
  let current = document
  for (const token of tokens) {
    if (Array.isArray(current)) current = current[arrayIndex(token, current.length, false)]
    else {
      const object = objectOf(current, path)
      if (!(token in object)) throw new InvalidError(`JSON Patch path \`${path}\` does not exist`)
      current = object[token]
    }
  }
  return current
}

function addValue(document: unknown, path: string, value: unknown): unknown {
  const tokens = decodePointer(path)
  if (tokens.length === 0) return value
  const [parent, token] = parentAt(document, tokens, path)
  if (Array.isArray(parent)) parent.splice(arrayIndex(token, parent.length, true), 0, value)
  else objectOf(parent, path)[token] = value
  return document
}

function removeValue(
  document: unknown,
  path: string
): { readonly document: unknown; readonly value: unknown } {
  const tokens = decodePointer(path)
  if (tokens.length === 0) return { document: null, value: document }
  const [parent, token] = parentAt(document, tokens, path)
  if (Array.isArray(parent)) {
    const index = arrayIndex(token, parent.length, false)
    const value: unknown = parent[index]
    parent.splice(index, 1)
    return { document, value }
  }
  const object = objectOf(parent, path)
  if (!(token in object)) throw new InvalidError(`JSON Patch path \`${path}\` does not exist`)
  const value = object[token]
  Reflect.deleteProperty(object, token)
  return { document, value }
}

function equalJson(left: unknown, right: unknown): boolean {
  if (Object.is(left, right)) return true
  if (Array.isArray(left) && Array.isArray(right)) {
    return (
      left.length === right.length && left.every((value, index) => equalJson(value, right[index]))
    )
  }
  if (
    typeof left === "object" &&
    left !== null &&
    !Array.isArray(left) &&
    typeof right === "object" &&
    right !== null &&
    !Array.isArray(right)
  ) {
    const a = left as Readonly<Record<string, unknown>>
    const b = right as Readonly<Record<string, unknown>>
    const keys = Object.keys(a)
    return (
      keys.length === Object.keys(b).length &&
      keys.every((key) => key in b && equalJson(a[key], b[key]))
    )
  }
  return false
}

function patchObject(value: unknown): Readonly<Record<string, unknown>> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new InvalidError("each JSON Patch operation must be an object")
  }
  return value as Readonly<Record<string, unknown>>
}

export function applyJsonPatch(document: unknown, patch: unknown): unknown {
  if (!Array.isArray(patch)) throw new InvalidError("a JSON Patch document must be an array")
  let result = structuredClone(document)
  for (const raw of patch) {
    const operation = patchObject(raw)
    const op = operation["op"]
    const path = operation["path"]
    if (typeof op !== "string" || typeof path !== "string") {
      throw new InvalidError("a JSON Patch operation requires string `op` and `path`")
    }
    switch (op) {
      case "add":
        if (!("value" in operation)) throw new InvalidError("JSON Patch add requires `value`")
        result = addValue(result, path, structuredClone(operation["value"]))
        break
      case "remove":
        result = removeValue(result, path).document
        break
      case "replace":
        if (!("value" in operation)) throw new InvalidError("JSON Patch replace requires `value`")
        valueAt(result, path)
        result = removeValue(result, path).document
        result = addValue(result, path, structuredClone(operation["value"]))
        break
      case "move": {
        const from = operation["from"]
        if (typeof from !== "string") throw new InvalidError("JSON Patch move requires `from`")
        if (path.startsWith(`${from}/`))
          throw new InvalidError("JSON Patch cannot move a value into its child")
        const removed = removeValue(result, from)
        result = addValue(removed.document, path, removed.value)
        break
      }
      case "copy": {
        const from = operation["from"]
        if (typeof from !== "string") throw new InvalidError("JSON Patch copy requires `from`")
        result = addValue(result, path, structuredClone(valueAt(result, from)))
        break
      }
      case "test":
        if (!("value" in operation)) throw new InvalidError("JSON Patch test requires `value`")
        if (!equalJson(valueAt(result, path), operation["value"])) {
          throw new InvalidError(`JSON Patch test failed at \`${path}\``)
        }
        break
      default:
        throw new InvalidError(`unknown JSON Patch operation \`${op}\``)
    }
  }
  return result
}

function chunkKind(envelope: AgentEnvelope): ChunkKind {
  if (envelope.operation === OPERATION_REASONING) return "reasoning"
  if (envelope.operation === OPERATION_TOOL_ARGS) return "toolArgs"
  return "chat"
}

function chunkEvents(envelope: AgentEnvelope, kind: ChunkKind): readonly AgUiEvent[] {
  const id = envelope.channel?.toString() ?? ""
  const body = new TextDecoder().decode(envelope.body)
  const opening = envelope.sequence === 0n
  const events: AgUiEvent[] = []
  if (kind === "chat") {
    if (opening) events.push({ type: "TEXT_MESSAGE_START", messageId: id, role: "assistant" })
    if (body.length > 0) events.push({ type: "TEXT_MESSAGE_CONTENT", messageId: id, delta: body })
    if (envelope.last) events.push({ type: "TEXT_MESSAGE_END", messageId: id })
  } else if (kind === "reasoning") {
    if (opening) events.push({ type: "REASONING_MESSAGE_START", messageId: id, role: "reasoning" })
    if (body.length > 0)
      events.push({ type: "REASONING_MESSAGE_CONTENT", messageId: id, delta: body })
    if (envelope.last) events.push({ type: "REASONING_MESSAGE_END", messageId: id })
  } else {
    if (opening)
      events.push({ type: "TOOL_CALL_START", toolCallId: id, toolCallName: envelope.tool ?? "" })
    if (body.length > 0) events.push({ type: "TOOL_CALL_ARGS", toolCallId: id, delta: body })
    if (envelope.last) events.push({ type: "TOOL_CALL_END", toolCallId: id })
  }
  return events
}

export function envelopeToAgUi(envelope: AgentEnvelope): readonly AgUiEvent[] {
  if (envelope.kind === AgentKind.Status && envelope.operation === OPERATION_TASK) {
    const threadId = envelope.conversation.toString()
    const runId = envelope.correlation?.toString() ?? ""
    if (envelope.taskState?.kind === "known" && envelope.taskState.name === "Submitted") {
      return [{ type: "RUN_STARTED", threadId, runId }]
    }
    if (envelope.taskState !== undefined && taskStateIsTerminal(envelope.taskState)) {
      return [{ type: "RUN_FINISHED", threadId, runId }]
    }
    return []
  }
  if (
    (envelope.kind === AgentKind.Response || envelope.kind === AgentKind.Error) &&
    envelope.tool !== undefined
  ) {
    return [
      {
        type: "TOOL_CALL_RESULT",
        toolCallId: envelope.correlation?.toString() ?? "",
        content: new TextDecoder().decode(envelope.body)
      }
    ]
  }
  if (envelope.kind === AgentKind.Error) {
    return [{ type: "RUN_ERROR", message: new TextDecoder().decode(envelope.body) }]
  }
  if (envelope.kind === AgentKind.Event && envelope.operation === OPERATION_STATE_SNAPSHOT) {
    try {
      return [{ type: "STATE_SNAPSHOT", snapshot: parseJson(envelope.body, "state snapshot") }]
    } catch {
      return []
    }
  }
  if (envelope.kind === AgentKind.Event && envelope.operation === OPERATION_STATE_DELTA) {
    try {
      return [{ type: "STATE_DELTA", delta: parseJson(envelope.body, "state delta") }]
    } catch {
      return []
    }
  }
  return []
}

export function envelopesToAgUi(envelopes: readonly AgentEnvelope[]): readonly AgUiEvent[] {
  const channels = new Map<string, ChunkKind>()
  const events: AgUiEvent[] = []
  for (const envelope of envelopes) {
    if (envelope.kind !== AgentKind.Chunk) {
      events.push(...envelopeToAgUi(envelope))
      continue
    }
    const id = envelope.channel?.toString() ?? ""
    const kind = envelope.sequence === 0n ? chunkKind(envelope) : (channels.get(id) ?? "chat")
    if (envelope.sequence === 0n) channels.set(id, kind)
    events.push(...chunkEvents(envelope, kind))
  }
  return events
}

export async function publishStateSnapshot(
  laser: Laser,
  topic: string,
  source: AgentId,
  conversation: ConversationId,
  state: unknown
): Promise<void> {
  await laser
    .agdx(topic, source, conversation)
    .emit(jsonBytes(state, "state snapshot"))
    .withOperation(OPERATION_STATE_SNAPSHOT)
    .contentType(ContentType.Json)
    .send()
}

export async function publishStateDelta(
  laser: Laser,
  topic: string,
  source: AgentId,
  conversation: ConversationId,
  patch: unknown
): Promise<void> {
  if (!Array.isArray(patch))
    throw new InvalidError("an AG-UI state delta must be a JSON Patch array")
  await laser
    .agdx(topic, source, conversation)
    .emit(jsonBytes(patch, "state delta"))
    .withOperation(OPERATION_STATE_DELTA)
    .contentType(ContentType.Json)
    .send()
}

export async function reconstructState(
  laser: Laser,
  conversation: ConversationId,
  topic: string
): Promise<unknown> {
  const messages = await laser.context(conversation).fetch([topic], Number.MAX_SAFE_INTEGER)
  let state: unknown = undefined
  for (const message of messages) {
    const envelope = message.envelope
    if (envelope?.kind !== AgentKind.Event) continue
    if (envelope.operation === OPERATION_STATE_SNAPSHOT) {
      state = parseJson(envelope.body, "state snapshot")
    } else if (envelope.operation === OPERATION_STATE_DELTA && state !== undefined) {
      state = applyJsonPatch(state, parseJson(envelope.body, "state delta"))
    }
  }
  return state
}

export async function aguiEvents(
  laser: Laser,
  conversation: ConversationId,
  topic: string
): Promise<readonly AgUiEvent[]> {
  const messages = await laser.context(conversation).fetch([topic], Number.MAX_SAFE_INTEGER)
  return envelopesToAgUi(
    messages.flatMap((message) => (message.envelope === undefined ? [] : [message.envelope]))
  )
}
