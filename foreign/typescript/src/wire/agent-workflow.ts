import { CodecError } from "../client/errors.js"
import { AGENT_WORKFLOW_OP_VERSION } from "./codes.js"
import { type CborMap, expectMap, expectString, field, singleVariantTag } from "./cbor.js"

export interface RunBudget {
  readonly maxEvents?: bigint
  readonly maxModelCalls?: bigint
  readonly maxToolCalls?: bigint
  readonly maxPatches?: bigint
  readonly maxDepth?: number
  readonly maxWallClockMicros?: bigint
  readonly maxCostUsd?: number
}

export function encodeRunBudget(budget: RunBudget): Map<string, unknown> {
  const map = new Map<string, unknown>()
  if (budget.maxEvents !== undefined) map.set("max_events", budget.maxEvents)
  if (budget.maxModelCalls !== undefined) map.set("max_model_calls", budget.maxModelCalls)
  if (budget.maxToolCalls !== undefined) map.set("max_tool_calls", budget.maxToolCalls)
  if (budget.maxPatches !== undefined) map.set("max_patches", budget.maxPatches)
  if (budget.maxDepth !== undefined) map.set("max_depth", budget.maxDepth)
  if (budget.maxWallClockMicros !== undefined) {
    map.set("max_wall_clock_micros", budget.maxWallClockMicros)
  }
  if (budget.maxCostUsd !== undefined) map.set("max_cost_usd", budget.maxCostUsd)
  return map
}

export function decodeRunBudget(map: CborMap, context: string): RunBudget {
  const maxEvents = field.optionalU64(map, "max_events", context)
  const maxModelCalls = field.optionalU64(map, "max_model_calls", context)
  const maxToolCalls = field.optionalU64(map, "max_tool_calls", context)
  const maxPatches = field.optionalU64(map, "max_patches", context)
  const maxDepth = field.optionalU32(map, "max_depth", context)
  const maxWallClockMicros = field.optionalU64(map, "max_wall_clock_micros", context)
  const maxCostUsd = field.optionalF64(map, "max_cost_usd", context)
  return {
    ...(maxEvents !== undefined ? { maxEvents } : {}),
    ...(maxModelCalls !== undefined ? { maxModelCalls } : {}),
    ...(maxToolCalls !== undefined ? { maxToolCalls } : {}),
    ...(maxPatches !== undefined ? { maxPatches } : {}),
    ...(maxDepth !== undefined ? { maxDepth } : {}),
    ...(maxWallClockMicros !== undefined ? { maxWallClockMicros } : {}),
    ...(maxCostUsd !== undefined ? { maxCostUsd } : {})
  }
}

function encodeStringMap(entries: ReadonlyMap<string, string>): Map<string, unknown> {
  return new Map(entries)
}

function decodeStringMap(map: CborMap, context: string): ReadonlyMap<string, string> {
  const result = new Map<string, string>()
  for (const [key, value] of map) {
    if (typeof key !== "string" || typeof value !== "string") {
      throw new CodecError(`${context} must map strings to strings`, context, "params")
    }
    result.set(key, value)
  }
  return result
}

export interface AgentSubmit {
  readonly agentId: string
  readonly runId?: string
  readonly params: ReadonlyMap<string, string>
  readonly input?: Uint8Array
  readonly budget?: RunBudget
}

export function encodeAgentSubmit(submit: AgentSubmit): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("v", AGENT_WORKFLOW_OP_VERSION)
  map.set("agent_id", submit.agentId)
  if (submit.runId !== undefined) map.set("run_id", submit.runId)
  if (submit.params.size > 0) map.set("params", encodeStringMap(submit.params))
  if (submit.input !== undefined) map.set("input", submit.input)
  if (submit.budget !== undefined) map.set("budget", encodeRunBudget(submit.budget))
  return map
}

export function decodeAgentSubmit(map: CborMap, context: string): AgentSubmit {
  const agentId = field.requiredString(map, "agent_id", context)
  const runId = field.optionalString(map, "run_id", context)
  const paramsMap = field.optionalMap(map, "params", context)
  const input = field.optionalBytes(map, "input", context)
  const budgetMap = field.optionalMap(map, "budget", context)
  return {
    agentId,
    ...(runId !== undefined ? { runId } : {}),
    params: paramsMap !== undefined ? decodeStringMap(paramsMap, `${context}.params`) : new Map(),
    ...(input !== undefined ? { input } : {}),
    ...(budgetMap !== undefined ? { budget: decodeRunBudget(budgetMap, `${context}.budget`) } : {})
  }
}

export interface AgentCancel {
  readonly runId: string
}

export function encodeAgentCancel(cancel: AgentCancel): Map<string, unknown> {
  return new Map<string, unknown>([
    ["v", AGENT_WORKFLOW_OP_VERSION],
    ["run_id", cancel.runId]
  ])
}

export function decodeAgentCancel(map: CborMap, context: string): AgentCancel {
  return { runId: field.requiredString(map, "run_id", context) }
}

export interface AgentStatusReq {
  readonly runId: string
}

export function encodeAgentStatusReq(req: AgentStatusReq): Map<string, unknown> {
  return new Map<string, unknown>([
    ["v", AGENT_WORKFLOW_OP_VERSION],
    ["run_id", req.runId]
  ])
}

export function decodeAgentStatusReq(map: CborMap, context: string): AgentStatusReq {
  return { runId: field.requiredString(map, "run_id", context) }
}

export type AgentRunState = "submitted" | "running" | "completed" | "cancelled" | "failed"

const AGENT_RUN_STATES: ReadonlySet<string> = new Set([
  "submitted",
  "running",
  "completed",
  "cancelled",
  "failed"
])

export function agentRunStateFromWord(word: string, context: string): AgentRunState {
  if (!AGENT_RUN_STATES.has(word)) {
    throw new CodecError(`\`${word}\` is not a recognized agent run state`, context, "state")
  }
  return word as AgentRunState
}

export function agentRunStateIsTerminal(state: AgentRunState): boolean {
  return state === "completed" || state === "cancelled" || state === "failed"
}

export interface AgentRunInfo {
  readonly runId: string
  readonly agentId: string
  readonly userId: number
  readonly state: AgentRunState
  readonly createdAtMicros: bigint
  readonly updatedAtMicros: bigint
  readonly detail?: string
  readonly cancelRequested: boolean
}

export function encodeAgentRunInfo(info: AgentRunInfo): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("run_id", info.runId)
  map.set("agent_id", info.agentId)
  map.set("user_id", info.userId)
  map.set("state", info.state)
  map.set("created_at_micros", info.createdAtMicros)
  map.set("updated_at_micros", info.updatedAtMicros)
  if (info.detail !== undefined) map.set("detail", info.detail)
  if (info.cancelRequested) map.set("cancel_requested", true)
  return map
}

export function decodeAgentRunInfo(map: CborMap, context: string): AgentRunInfo {
  const runId = field.requiredString(map, "run_id", context)
  const agentId = field.requiredString(map, "agent_id", context)
  const userId = field.requiredU32(map, "user_id", context)
  const state = agentRunStateFromWord(field.requiredString(map, "state", context), context)
  const createdAtMicros = field.requiredU64(map, "created_at_micros", context)
  const updatedAtMicros = field.requiredU64(map, "updated_at_micros", context)
  const detail = field.optionalString(map, "detail", context)
  return {
    runId,
    agentId,
    userId,
    state,
    createdAtMicros,
    updatedAtMicros,
    ...(detail !== undefined ? { detail } : {}),
    cancelRequested: map.get("cancel_requested") === true
  }
}

export interface AgentList {
  readonly agentId?: string
  readonly state?: AgentRunState
  readonly limit?: number
  readonly cursor?: Uint8Array
}

export function encodeAgentList(list: AgentList): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("v", AGENT_WORKFLOW_OP_VERSION)
  if (list.agentId !== undefined) map.set("agent_id", list.agentId)
  if (list.state !== undefined) map.set("state", list.state)
  if (list.limit !== undefined) map.set("limit", list.limit)
  if (list.cursor !== undefined) map.set("cursor", list.cursor)
  return map
}

export function decodeAgentList(map: CborMap, context: string): AgentList {
  const agentId = field.optionalString(map, "agent_id", context)
  const state = field.optionalString(map, "state", context)
  const limit = field.optionalU32(map, "limit", context)
  const cursor = field.optionalBytes(map, "cursor", context)
  return {
    ...(agentId !== undefined ? { agentId } : {}),
    ...(state !== undefined ? { state: agentRunStateFromWord(state, context) } : {}),
    ...(limit !== undefined ? { limit } : {}),
    ...(cursor !== undefined ? { cursor } : {})
  }
}

export interface RunPage {
  readonly runs: readonly AgentRunInfo[]
  readonly cursor?: Uint8Array
}

export function encodeRunPage(page: RunPage): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set(
    "runs",
    page.runs.map((run) => encodeAgentRunInfo(run))
  )
  if (page.cursor !== undefined) map.set("cursor", page.cursor)
  return map
}

export function decodeRunPage(map: CborMap, context: string): RunPage {
  const runs = field.requiredArray(map, "runs", context, (item, index) =>
    decodeAgentRunInfo(
      expectMap(item, `${context}.runs[${String(index)}]`),
      `${context}.runs[${String(index)}]`
    )
  )
  const cursor = field.optionalBytes(map, "cursor", context)
  return { runs, ...(cursor !== undefined ? { cursor } : {}) }
}

export type AgentOutcome =
  | { readonly kind: "submitted"; readonly run: AgentRunInfo }
  | { readonly kind: "cancelled"; readonly run: AgentRunInfo }
  | { readonly kind: "status"; readonly run: AgentRunInfo }
  | { readonly kind: "list"; readonly page: RunPage }
  | { readonly kind: "unrecognized"; readonly tag: string; readonly value: unknown }

export type AgentWorkflowError =
  | { readonly kind: "unsupported"; readonly message: string }
  | { readonly kind: "notFound"; readonly message: string }
  | { readonly kind: "invalid"; readonly message: string }
  | { readonly kind: "backend"; readonly message: string }
  | { readonly kind: "version"; readonly expected: number; readonly got: number }
  | { readonly kind: "notLeader" }
  | { readonly kind: "unrecognized"; readonly tag: string; readonly value: unknown }

export type AgentReply =
  | { readonly kind: "ok"; readonly outcome: AgentOutcome }
  | { readonly kind: "err"; readonly error: AgentWorkflowError }
  | { readonly kind: "unrecognized"; readonly tag: string; readonly value: unknown }

export function encodeAgentOutcome(outcome: AgentOutcome): Map<string, unknown> {
  switch (outcome.kind) {
    case "submitted":
      return new Map([["Submitted", encodeAgentRunInfo(outcome.run)]])
    case "cancelled":
      return new Map([["Cancelled", encodeAgentRunInfo(outcome.run)]])
    case "status":
      return new Map([["Status", encodeAgentRunInfo(outcome.run)]])
    case "list":
      return new Map([["List", encodeRunPage(outcome.page)]])
    case "unrecognized":
      return new Map([[outcome.tag, outcome.value]])
  }
}

export function decodeAgentOutcome(value: unknown, context: string): AgentOutcome {
  const [tag, inner] = singleVariantTag(value, context)
  switch (tag) {
    case "Submitted":
      return { kind: "submitted", run: decodeAgentRunInfo(expectMap(inner, context), context) }
    case "Cancelled":
      return { kind: "cancelled", run: decodeAgentRunInfo(expectMap(inner, context), context) }
    case "Status":
      return { kind: "status", run: decodeAgentRunInfo(expectMap(inner, context), context) }
    case "List":
      return { kind: "list", page: decodeRunPage(expectMap(inner, context), context) }
    default:
      return { kind: "unrecognized", tag, value: inner }
  }
}

export function encodeAgentWorkflowError(error: AgentWorkflowError): unknown {
  switch (error.kind) {
    case "unsupported":
      return new Map([["Unsupported", error.message]])
    case "notFound":
      return new Map([["NotFound", error.message]])
    case "invalid":
      return new Map([["Invalid", error.message]])
    case "backend":
      return new Map([["Backend", error.message]])
    case "version":
      return new Map([
        [
          "Version",
          new Map<string, unknown>([
            ["expected", error.expected],
            ["got", error.got]
          ])
        ]
      ])
    case "notLeader":
      return "NotLeader"
    case "unrecognized":
      return new Map([[error.tag, error.value]])
  }
}

export function decodeAgentWorkflowError(value: unknown, context: string): AgentWorkflowError {
  if (typeof value === "string") {
    return value === "NotLeader"
      ? { kind: "notLeader" }
      : { kind: "unrecognized", tag: value, value: undefined }
  }
  const [tag, inner] = singleVariantTag(value, context)
  switch (tag) {
    case "Unsupported":
      return { kind: "unsupported", message: expectString(inner, context) }
    case "NotFound":
      return { kind: "notFound", message: expectString(inner, context) }
    case "Invalid":
      return { kind: "invalid", message: expectString(inner, context) }
    case "Backend":
      return { kind: "backend", message: expectString(inner, context) }
    case "Version": {
      const versionMap = expectMap(inner, context)
      return {
        kind: "version",
        expected: field.requiredU32(versionMap, "expected", context),
        got: field.requiredU32(versionMap, "got", context)
      }
    }
    default:
      return { kind: "unrecognized", tag, value: inner }
  }
}

export function encodeAgentReply(reply: AgentReply): Map<string, unknown> {
  switch (reply.kind) {
    case "ok":
      return new Map([["Ok", encodeAgentOutcome(reply.outcome)]])
    case "err":
      return new Map([["Err", encodeAgentWorkflowError(reply.error)]])
    case "unrecognized":
      return new Map([[reply.tag, reply.value]])
  }
}

export function decodeAgentReply(value: unknown, context: string): AgentReply {
  const [tag, inner] = singleVariantTag(value, context)
  switch (tag) {
    case "Ok":
      return { kind: "ok", outcome: decodeAgentOutcome(inner, context) }
    case "Err":
      return { kind: "err", error: decodeAgentWorkflowError(inner, context) }
    default:
      return { kind: "unrecognized", tag, value: inner }
  }
}
