import {
  type AgentEnvelope,
  type AgentId,
  AgentKind,
  METADATA_ROLE,
  OPERATION_CHAT,
  OPERATION_PROGRESS,
  OPERATION_REASONING,
  OPERATION_SESSION,
  OPERATION_SESSION_PARKED,
  OPERATION_SESSION_UNPARKED,
  OPERATION_STATE_DELTA,
  OPERATION_STATE_SNAPSHOT,
  OPERATION_TASK
} from "./agent.js"
import { decodeOne } from "./cbor.js"
import { ConsumerFilter, FilterExpr } from "./filter.js"
import { BROADCAST, TARGET_AGENT_ID } from "./headers.js"
import type { MemoryRecord } from "./memory.js"
import { AGENT_CONTROL, AGENT_HEARTBEATS } from "./topics.js"

export { BROADCAST }

export const OPERATION_SESSION_PAUSE = "session_pause"
export const OPERATION_SESSION_RESUME = "session_resume"
export const OPERATION_SESSION_CANCEL = "session_cancel"
export const OPERATION_FORCE_CANCEL = "force_cancel"
export const OPERATION_TEXT_COMPLETION = "text_completion"
export const OPERATION_GENERATE_CONTENT = "generate_content"
export const OPERATION_EXECUTE_TOOL = "execute_tool"
export const OPERATION_INVOKE_AGENT = "invoke_agent"
export const OPERATION_CONTEXT_ASSEMBLED = "context_assembled"
export const OPERATION_CONTEXT_COMPACTED = "context_compacted"
export const OPERATION_CONTEXT_RETRIEVED = "context_retrieved"
export const OPERATION_POLICY_DECISION = "policy_decision"

/** The session control operations. They are honored only on `agent.control`. */
export const CONTROL_OPERATIONS: readonly string[] = [
  OPERATION_SESSION_PAUSE,
  OPERATION_SESSION_RESUME,
  OPERATION_SESSION_CANCEL,
  OPERATION_FORCE_CANCEL
]

/** One record a session timeline can show. */
export type SessionRecord =
  | { readonly kind: "envelope"; readonly envelope: AgentEnvelope }
  | { readonly kind: "memory"; readonly record: MemoryRecord }
  | { readonly kind: "dead_letter" }
  | { readonly kind: "journal" }
  | { readonly kind: "kv_mutation" }
  | { readonly kind: "graph_mutation" }
  | { readonly kind: "undecodable"; readonly recordKind?: AgentKind }

/** How a session timeline shows one record. */
export type DisplayType =
  | "session.submitted"
  | "session.started"
  | "session.paused"
  | "session.resumed"
  | "session.completed"
  | "session.failed"
  | "session.canceled"
  | "session.heartbeat"
  | "session.control"
  | "session.parked"
  | "session.unparked"
  | "user.message"
  | "model.request"
  | "model.response"
  | "model.stream"
  | "tool.call"
  | "tool.result"
  | "agent.handoff"
  | "agent.message"
  | "state.updated"
  | "context.assembled"
  | "context.compacted"
  | "context.retrieved"
  | "memory.created"
  | "memory.forgotten"
  | "memory.feedback"
  | "task.status"
  | "workflow.step"
  | "policy.decision"
  | "error"
  | "dead_letter"
  | "undecodable"
  /** A record whose header and body name different identities. */
  | "invalid"
  | "kv.set"
  | "graph.upsert"
  | "unauthorized_control"

/** How a session timeline shows `record`, read from the topic named `topic`. */
export function displayType(record: SessionRecord, topic: string): DisplayType {
  switch (record.kind) {
    case "memory":
      return record.record.kind === "item"
        ? "memory.created"
        : record.record.kind === "forget"
          ? "memory.forgotten"
          : "memory.feedback"
    case "dead_letter":
      return "dead_letter"
    case "journal":
      return "workflow.step"
    case "kv_mutation":
      return "kv.set"
    case "graph_mutation":
      return "graph.upsert"
    case "undecodable":
      return "undecodable"
    case "envelope":
      return envelopeDisplay(record.envelope, topic)
  }
}

function envelopeDisplay(envelope: AgentEnvelope, topic: string): DisplayType {
  const operation = envelope.operation ?? ""
  switch (envelope.kind) {
    case AgentKind.Status:
      return statusDisplay(envelope, operation, topic)
    case AgentKind.Command:
      if (CONTROL_OPERATIONS.includes(operation))
        return topic === AGENT_CONTROL ? "session.control" : "unauthorized_control"
      if (isUser(envelope)) return "user.message"
      if (isModel(operation)) return "model.request"
      if (operation === OPERATION_EXECUTE_TOOL) return "tool.call"
      if (operation === OPERATION_INVOKE_AGENT) return "agent.handoff"
      return "agent.message"
    case AgentKind.Event:
      if (isUser(envelope)) return "user.message"
      switch (operation) {
        case OPERATION_STATE_DELTA:
        case OPERATION_STATE_SNAPSHOT:
          return "state.updated"
        case OPERATION_CONTEXT_ASSEMBLED:
          return "context.assembled"
        case OPERATION_CONTEXT_COMPACTED:
          return "context.compacted"
        case OPERATION_CONTEXT_RETRIEVED:
          return "context.retrieved"
        case OPERATION_POLICY_DECISION:
          return "policy.decision"
        case OPERATION_SESSION_PARKED:
          return "session.parked"
        case OPERATION_SESSION_UNPARKED:
          return "session.unparked"
        default:
          return "agent.message"
      }
    case AgentKind.Response:
      if (isModel(operation)) return "model.response"
      return operation === OPERATION_EXECUTE_TOOL ? "tool.result" : "agent.message"
    case AgentKind.Error:
      return operation === OPERATION_EXECUTE_TOOL ? "tool.result" : "error"
    case AgentKind.Chunk:
      return operation === OPERATION_CHAT || operation === OPERATION_REASONING
        ? "model.stream"
        : "agent.message"
  }
}

function statusDisplay(envelope: AgentEnvelope, operation: string, topic: string): DisplayType {
  if (operation === OPERATION_PROGRESS && topic === AGENT_HEARTBEATS) return "session.heartbeat"
  if (operation !== OPERATION_SESSION)
    return operation === OPERATION_TASK ? "task.status" : "agent.message"
  const state = envelope.taskState?.kind === "known" ? envelope.taskState.name : undefined
  switch (state) {
    case "Submitted":
      return "session.submitted"
    case "Working":
      return isSessionStart(envelope.body) ? "session.started" : "session.resumed"
    case "Paused":
      return "session.paused"
    case "Completed":
      return "session.completed"
    case "Failed":
    case "Rejected":
      return "session.failed"
    case "Canceled":
      return "session.canceled"
    case "InputRequired":
    case "AuthRequired":
    case "Unknown":
    case undefined:
      return "task.status"
  }
}

// A start body names the agent and SDK. A transition body names neither.
function isSessionStart(body: Uint8Array): boolean {
  try {
    const value = decodeOne(body, "session status body")
    return value instanceof Map && (value.has("agent") || value.has("sdk"))
  } catch {
    return false
  }
}

function isModel(operation: string): boolean {
  return (
    operation === OPERATION_CHAT ||
    operation === OPERATION_TEXT_COMPLETION ||
    operation === OPERATION_GENERATE_CONTENT
  )
}

function isUser(envelope: AgentEnvelope): boolean {
  const role = envelope.metadata?.get(METADATA_ROLE)
  return role?.kind === "str" && role.value === "user"
}

/** What a reliable consumer does with one record it read. */
export type Dispatch = "work" | "observational" | "reply" | "lifecycle" | "control" | "foreign"

/** The command operations a handler serves: `"any"` or an explicit list. */
export type HandledOperations = "any" | readonly string[]

/**
 * What the agent `me` does with `envelope`, read from the topic named `topic`.
 * The author is never a discriminator, so an agent may send work to itself.
 */
export function classify(
  envelope: AgentEnvelope,
  topic: string,
  me: AgentId,
  handled: HandledOperations
): Dispatch {
  const operation = envelope.operation ?? ""
  switch (envelope.kind) {
    case AgentKind.Status:
      return "lifecycle"
    case AgentKind.Response:
    case AgentKind.Error:
    case AgentKind.Chunk:
      return "reply"
    case AgentKind.Event:
      return "observational"
    case AgentKind.Command: {
      if (envelope.target !== undefined && envelope.target !== me) return "foreign"
      const control = CONTROL_OPERATIONS.includes(operation)
      const onControl = topic === AGENT_CONTROL
      if (control) return onControl ? "control" : "observational"
      if (onControl) return "foreign"
      return handled === "any" || handled.includes(operation) ? "work" : "observational"
    }
  }
}

/**
 * The headers-only group filter that selects records addressed to `me` or to
 * every agent. Its digest differs per agent identity.
 */
export function addresseeFilter(me: AgentId): ConsumerFilter {
  return ConsumerFilter.headersOnly(FilterExpr.header(TARGET_AGENT_ID, "in", [me, BROADCAST]))
}

/** The headers-only group filter that selects broadcast records only. */
export function broadcastFilter(): ConsumerFilter {
  return ConsumerFilter.headersOnly(FilterExpr.header(TARGET_AGENT_ID, "eq", BROADCAST))
}

/**
 * What the agent `me` does with a generic record, one without an envelope,
 * read from the topic named `topic`. Such a record has no kind, so a record
 * that names both its causal parent and a correlation is a reply, and any
 * other record is work unless it is addressed to another agent.
 */
export function classifyGeneric(
  addressee: string | undefined,
  hasCause: boolean,
  hasCorrelation: boolean,
  topic: string,
  me: AgentId
): Dispatch {
  if (
    (addressee !== undefined && addressee !== me && addressee !== BROADCAST) ||
    topic === AGENT_CONTROL
  )
    return "foreign"
  return hasCause && hasCorrelation ? "reply" : "work"
}
