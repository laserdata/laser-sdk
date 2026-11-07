import { InvalidError } from "../client/errors.js"
import type { Laser } from "../client/laser.js"
import { LastN, replayContext } from "../context.js"
import type { AgentId, ConversationId } from "../types/ids.js"
import {
  AgentKind,
  OPERATION_REASONING,
  OPERATION_STATE_DELTA,
  OPERATION_STATE_SNAPSHOT,
  OPERATION_TASK,
  OPERATION_TOOL_ARGS,
  type AgentEnvelope,
  type PatchOp,
  decodeStateDelta,
  decodeStateSnapshot,
  taskStateIsTerminal
} from "../wire/agent.js"
import { decodeOne, expectMap } from "../wire/cbor.js"

export { applyJsonPatch } from "../wire/json-patch.js"

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
  | {
      readonly type: "TOOL_CALL_RESULT"
      readonly messageId: string
      readonly toolCallId: string
      readonly content: string
    }
  | { readonly type: "STATE_SNAPSHOT"; readonly snapshot: unknown }
  | { readonly type: "STATE_DELTA"; readonly delta: unknown }
  | { readonly type: "RUN_ERROR"; readonly message: string }

type ChunkKind = "chat" | "reasoning" | "toolArgs"

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
    if (
      envelope.taskState?.kind === "known" &&
      (envelope.taskState.name === "Failed" || envelope.taskState.name === "Rejected")
    ) {
      const detail = envelope.metadata?.get("detail")
      return [{ type: "RUN_ERROR", message: detail?.kind === "str" ? detail.value : "task failed" }]
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
    const toolCallId = envelope.correlation?.toString() ?? ""
    return [
      {
        type: "TOOL_CALL_RESULT",
        messageId: envelope.record?.toString() ?? toolCallId,
        toolCallId,
        content: new TextDecoder().decode(envelope.body)
      }
    ]
  }
  if (envelope.kind === AgentKind.Error) {
    return [{ type: "RUN_ERROR", message: new TextDecoder().decode(envelope.body) }]
  }
  if (envelope.kind === AgentKind.Event && envelope.operation === OPERATION_STATE_SNAPSHOT) {
    try {
      const context = "state snapshot"
      const snapshot = decodeStateSnapshot(
        expectMap(decodeOne(envelope.body, context), context),
        context
      )
      return [{ type: "STATE_SNAPSHOT", snapshot: snapshot.document }]
    } catch {
      return []
    }
  }
  if (envelope.kind === AgentKind.Event && envelope.operation === OPERATION_STATE_DELTA) {
    try {
      const context = "state delta"
      const delta = decodeStateDelta(expectMap(decodeOne(envelope.body, context), context), context)
      return [{ type: "STATE_DELTA", delta: delta.patch }]
    } catch {
      return []
    }
  }
  return []
}

/** Render `envelopes`, one conversation in log order, as AG-UI runs a client
 * verifier accepts: every event sits inside a run, a message or tool call
 * opens before its content, and a run closes what it opened before it
 * finishes. */
export function envelopesToAgUi(envelopes: readonly AgentEnvelope[]): readonly AgUiEvent[] {
  const channels = new Map<string, ChunkKind>()
  const framer = new RunFramer()
  for (const envelope of envelopes) {
    const threadId = envelope.conversation.toString()
    const runId = envelope.correlation?.toString() ?? envelope.record?.toString() ?? threadId
    if (envelope.kind !== AgentKind.Chunk) {
      for (const event of envelopeToAgUi(envelope)) framer.push(event, threadId, runId)
      continue
    }
    const id = envelope.channel?.toString() ?? ""
    const kind = envelope.sequence === 0n ? chunkKind(envelope) : (channels.get(id) ?? "chat")
    if (envelope.sequence === 0n) channels.set(id, kind)
    for (const event of chunkEvents(envelope, kind)) framer.push(event, threadId, runId)
  }
  return framer.finish()
}

type SpanKind = "text" | "reasoning" | "tool"

interface OpenRun {
  readonly threadId: string
  readonly runId: string
  readonly spans: Map<string, SpanKind>
}

// Places rendered events into AG-UI runs: an event outside a run opens one, a
// span whose opening fell outside the read opens before its content, and a
// run closes its open spans before it finishes.
class RunFramer {
  private readonly events: AgUiEvent[] = []
  private run: OpenRun | undefined

  push(event: AgUiEvent, threadId: string, runId: string): void {
    if (event.type === "RUN_STARTED") {
      this.close()
      this.run = { threadId: event.threadId, runId: event.runId, spans: new Map() }
      this.events.push(event)
      return
    }
    if (event.type === "RUN_FINISHED") {
      this.close()
      return
    }
    if (event.type === "RUN_ERROR") {
      this.run = undefined
      this.events.push(event)
      return
    }
    const run = this.open(threadId, runId)
    const span = spanOf(event)
    if (span !== undefined) {
      const [kind, id, opening, closing] = span
      if (opening) {
        if (run.spans.has(id)) return
        run.spans.set(id, kind)
      } else if (!run.spans.has(id)) {
        this.events.push(openingOf(kind, id))
        run.spans.set(id, kind)
      }
      if (closing) run.spans.delete(id)
    }
    this.events.push(event)
  }

  finish(): readonly AgUiEvent[] {
    this.close()
    return this.events
  }

  private open(threadId: string, runId: string): OpenRun {
    if (this.run !== undefined) return this.run
    const run: OpenRun = { threadId, runId, spans: new Map() }
    this.events.push({ type: "RUN_STARTED", threadId, runId })
    this.run = run
    return run
  }

  private close(): void {
    const run = this.run
    if (run === undefined) return
    for (const [id, kind] of [...run.spans].reverse()) this.events.push(closingOf(kind, id))
    this.events.push({ type: "RUN_FINISHED", threadId: run.threadId, runId: run.runId })
    this.run = undefined
  }
}

// The span an event belongs to: its kind, id, and whether it opens or closes it.
function spanOf(event: AgUiEvent): readonly [SpanKind, string, boolean, boolean] | undefined {
  switch (event.type) {
    case "TEXT_MESSAGE_START":
      return ["text", event.messageId, true, false]
    case "TEXT_MESSAGE_CONTENT":
      return ["text", event.messageId, false, false]
    case "TEXT_MESSAGE_END":
      return ["text", event.messageId, false, true]
    case "REASONING_MESSAGE_START":
      return ["reasoning", event.messageId, true, false]
    case "REASONING_MESSAGE_CONTENT":
      return ["reasoning", event.messageId, false, false]
    case "REASONING_MESSAGE_END":
      return ["reasoning", event.messageId, false, true]
    case "TOOL_CALL_START":
      return ["tool", event.toolCallId, true, false]
    case "TOOL_CALL_ARGS":
      return ["tool", event.toolCallId, false, false]
    case "TOOL_CALL_END":
      return ["tool", event.toolCallId, false, true]
    case "RUN_STARTED":
    case "RUN_FINISHED":
    case "RUN_ERROR":
    case "TOOL_CALL_RESULT":
    case "STATE_SNAPSHOT":
    case "STATE_DELTA":
      return undefined
  }
}

function openingOf(kind: SpanKind, id: string): AgUiEvent {
  if (kind === "text") return { type: "TEXT_MESSAGE_START", messageId: id, role: "assistant" }
  if (kind === "reasoning")
    return { type: "REASONING_MESSAGE_START", messageId: id, role: "reasoning" }
  return { type: "TOOL_CALL_START", toolCallId: id, toolCallName: "" }
}

function closingOf(kind: SpanKind, id: string): AgUiEvent {
  if (kind === "text") return { type: "TEXT_MESSAGE_END", messageId: id }
  if (kind === "reasoning") return { type: "REASONING_MESSAGE_END", messageId: id }
  return { type: "TOOL_CALL_END", toolCallId: id }
}

/** Replace the session state document of `conversation` with `state`, a JSON
 * object, then snapshot it. The state rides the session lane as one
 * revision-guarded patch, the same records `Session.state` writes, so every
 * reader folds one state model. */
export async function publishStateSnapshot(
  laser: Laser,
  source: AgentId,
  conversation: ConversationId,
  state: Readonly<Record<string, unknown>>
): Promise<void> {
  const document = laser.sessions().open(conversation).asAgent(source).state()
  await document.replace(state)
  await document.snapshot()
}

/** Apply `patch`, an RFC 6902 JSON Patch array, to the session state document
 * of `conversation`. */
export async function publishStateDelta(
  laser: Laser,
  source: AgentId,
  conversation: ConversationId,
  patch: readonly PatchOp[]
): Promise<void> {
  if (!Array.isArray(patch))
    throw new InvalidError("an AG-UI state delta must be a JSON Patch array")
  await laser.sessions().open(conversation).asAgent(source).state().patch(patch)
}

/** The session state document of `conversation`: the fold of the retained
 * session lane, or the managed state view when the fold is incomplete and the
 * view is not behind it. `undefined` until a state record exists. */
export async function reconstructState(
  laser: Laser,
  conversation: ConversationId
): Promise<unknown> {
  const view = await laser.sessions().open(conversation).state().currentView()
  return view.revision > 0n || view.complete ? view.document : undefined
}

export async function aguiEvents(
  laser: Laser,
  conversation: ConversationId,
  topic: string
): Promise<readonly AgUiEvent[]> {
  const messages = await replayContext(
    {
      conversation,
      acrossSubconversations: false,
      topics: [topic],
      policy: new LastN(Number.MAX_SAFE_INTEGER),
      fromOffsets: new Map()
    },
    laser
  )
  return envelopesToAgUi(
    messages.flatMap((message) => (message.envelope === undefined ? [] : [message.envelope]))
  )
}
