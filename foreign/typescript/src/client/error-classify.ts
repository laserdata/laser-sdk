import type { ResultCode, ResultCodeName } from "../wire/result.js"
import type { FilterErrorReason } from "../wire/filter.js"
import {
  AgentWorkflowExecutionError,
  AmbiguousMutationError,
  BudgetExceededError,
  CheckpointExecutionError,
  ConsumerGroupSetupError,
  FenceViolationError,
  FilterExecutionError,
  FilterStopError,
  ForkExecutionError,
  GraphExecutionError,
  KvExecutionError,
  LaserError,
  ProtocolError,
  QuarantinedError,
  QueryExecutionError,
  RoutingError,
  TransportError,
  UnsupportedError,
  publishCause
} from "./errors.js"

// Apache Iggy server error codes the classifiers read.
const IGGY_RESOURCE_NOT_FOUND = 20
const IGGY_UNAUTHENTICATED = 40
const IGGY_UNAUTHORIZED = 41
const IGGY_STREAM_OR_TOPIC_NOT_FOUND: ReadonlySet<number> = new Set([1009, 1010, 2010, 2011])

function known(name: keyof typeof ResultCodeName): ResultCode {
  return { kind: "known", name }
}

function detailKind(error: unknown): string | undefined {
  if (
    error instanceof QueryExecutionError ||
    error instanceof KvExecutionError ||
    error instanceof ForkExecutionError ||
    error instanceof GraphExecutionError ||
    error instanceof AgentWorkflowExecutionError
  ) {
    const detail = error.detail as { readonly kind?: unknown } | undefined
    return typeof detail?.kind === "string" ? detail.kind : undefined
  }
  if (error instanceof CheckpointExecutionError) return error.detail.kind
  return undefined
}

/** The Apache Iggy server error code under a failure, when the server
 * answered one. Publish wrapping is followed, then the causes of a transport
 * or protocol failure. Another typed SDK error that wraps an Apache Iggy
 * failure keeps its own classification, like the Rust SDK. */
export function iggyErrorCode(error: unknown): number | undefined {
  let current = publishCause(error)
  if (
    current instanceof LaserError &&
    !(current instanceof TransportError) &&
    !(current instanceof ProtocolError)
  ) {
    return undefined
  }
  for (let depth = 0; depth < 8; depth += 1) {
    if (typeof current !== "object" || current === null) return undefined
    const code = (current as { readonly errorCode?: unknown }).errorCode
    if (typeof code === "number") return code
    current = "cause" in current ? (current as { readonly cause?: unknown }).cause : undefined
  }
  return undefined
}

/** The typed consumer-filter cause, when the failure is one. */
export function filterReason(error: unknown): FilterErrorReason | undefined {
  const cause = publishCause(error)
  return cause instanceof FilterExecutionError ? cause.reason : undefined
}

/** Whether the target stream or topic does not exist for this principal.
 * Under RBAC an existing stream the principal cannot see answers like a
 * missing one. */
export function isStreamOrTopicNotFound(error: unknown): boolean {
  const code = iggyErrorCode(error)
  return code !== undefined && IGGY_STREAM_OR_TOPIC_NOT_FOUND.has(code)
}

/** Whether the server refused the operation for missing permissions. The fix
 * is a grant, not a retry. */
export function isPermissionDenied(error: unknown): boolean {
  const cause = publishCause(error)
  if (cause instanceof RoutingError && cause.reason.kind === "principalMismatch") return true
  const code = iggyErrorCode(error)
  if (code === IGGY_UNAUTHORIZED || code === IGGY_UNAUTHENTICATED) return true
  const reason = filterReason(error)
  if (reason === "forbidden" || reason === "unauthenticated") return true
  return cause instanceof CheckpointExecutionError && cause.detail.kind === "unauthorized"
}

/** Whether the connected infrastructure does not provide the feature at all.
 * Permanent: gate the code path instead of retrying. */
export function isUnsupported(error: unknown): boolean {
  const cause = publishCause(error)
  if (cause instanceof UnsupportedError) return true
  if (
    (cause instanceof QueryExecutionError ||
      cause instanceof KvExecutionError ||
      cause instanceof ForkExecutionError ||
      cause instanceof GraphExecutionError) &&
    detailKind(cause) === "unsupported"
  ) {
    return true
  }
  return filterReason(error) === "unsupported"
}

/** Whether the failure names a missing resource. */
export function isNotFound(error: unknown): boolean {
  const cause = publishCause(error)
  const kind = detailKind(cause)
  if (
    cause instanceof QueryExecutionError &&
    (kind === "index_not_found" || kind === "fork_not_found")
  ) {
    return true
  }
  if (cause instanceof ForkExecutionError && kind === "notFound") return true
  if (cause instanceof CheckpointExecutionError && kind === "not_found") return true
  return (
    isStreamOrTopicNotFound(error) ||
    iggyErrorCode(error) === IGGY_RESOURCE_NOT_FOUND ||
    filterReason(error) === "not_found"
  )
}

/** Whether the failure is wire-version skew between this SDK and the
 * connected deployment. Permanent for this client build. */
export function isVersionSkew(error: unknown): boolean {
  const cause = publishCause(error)
  if (
    (cause instanceof QueryExecutionError ||
      cause instanceof KvExecutionError ||
      cause instanceof ForkExecutionError ||
      cause instanceof CheckpointExecutionError) &&
    detailKind(cause) === "version"
  ) {
    return true
  }
  return filterReason(error) === "version_skew"
}

/** Whether a compare-and-swap lost its precondition. Re-read, recompute, and
 * commit again. */
export function isVersionConflict(error: unknown): boolean {
  const cause = publishCause(error)
  return cause instanceof KvExecutionError && detailKind(cause) === "versionConflict"
}

/** Whether a managed mutation may already have reached the server. A generic
 * retry loop must stop. */
export function isAmbiguousMutation(error: unknown): boolean {
  return publishCause(error) instanceof AmbiguousMutationError
}

/** Whether a read-consistency barrier was not met yet. Retryable. */
export function isStale(error: unknown): boolean {
  const cause = publishCause(error)
  return (
    (cause instanceof QueryExecutionError || cause instanceof KvExecutionError) &&
    detailKind(cause) === "stale"
  )
}

/** Whether the plane declined a conditional write because it does not own
 * the mutation partition. */
export function isNotLeader(error: unknown): boolean {
  const cause = publishCause(error)
  return (
    (cause instanceof KvExecutionError ||
      cause instanceof ForkExecutionError ||
      cause instanceof AgentWorkflowExecutionError) &&
    detailKind(cause) === "notLeader"
  )
}

/** Whether capability routing found no live agent for the skill. */
export function isNoCapableAgent(error: unknown): boolean {
  const cause = publishCause(error)
  return cause instanceof RoutingError && cause.reason.kind === "noCapableAgent"
}

/** Whether a revocable lease was lost. */
export function isLeaseLost(error: unknown): boolean {
  const cause = publishCause(error)
  return cause instanceof KvExecutionError && detailKind(cause) === "leaseLost"
}

/** Whether a fenced write lost the fence. Never retryable by the loser. */
export function isFenceViolation(error: unknown): boolean {
  return publishCause(error) instanceof FenceViolationError
}

/** Whether a spend ceiling was reached. */
export function isBudgetExceeded(error: unknown): boolean {
  return publishCause(error) instanceof BudgetExceededError
}

/** Whether the agent is quarantined. */
export function isQuarantined(error: unknown): boolean {
  return publishCause(error) instanceof QuarantinedError
}

/** Whether the failure was transient: the identical request may succeed on a
 * later attempt. */
export function isUnavailable(error: unknown): boolean {
  const value = code(error)
  return value.kind === "known" && value.name === "Unavailable"
}

const QUERY_CODES: Readonly<Record<string, keyof typeof ResultCodeName>> = {
  unsupported: "Unsupported",
  unauthorized: "Forbidden",
  index_not_found: "NotFound",
  fork_not_found: "NotFound",
  backend: "Backend",
  unavailable: "Unavailable",
  too_large: "TooLarge",
  version: "VersionSkew",
  stale: "Stale",
  cancelled: "Cancelled",
  deadline_exceeded: "DeadlineExceeded",
  expired_snapshot: "ExpiredSnapshot",
  stale_generation: "StaleGeneration",
  target_unavailable: "TargetUnavailable",
  resource_limit: "ResourceLimit"
}

const KV_CODES: Readonly<Record<string, keyof typeof ResultCodeName>> = {
  unsupported: "Unsupported",
  invalidKey: "InvalidArgument",
  invalidNamespace: "InvalidArgument",
  tooLarge: "TooLarge",
  backend: "Backend",
  unavailable: "Unavailable",
  version: "VersionSkew",
  versionConflict: "Conflict",
  leaseLost: "Conflict",
  notFound: "NotFound",
  notLeader: "Unavailable",
  stale: "Stale"
}

const FORK_CODES: Readonly<Record<string, keyof typeof ResultCodeName>> = {
  unsupported: "Unsupported",
  notFound: "NotFound",
  invalidFork: "InvalidArgument",
  conflict: "Conflict",
  backend: "Backend",
  unavailable: "Unavailable",
  version: "VersionSkew",
  notLeader: "Unavailable"
}

const GRAPH_CODES: Readonly<Record<string, keyof typeof ResultCodeName>> = {
  unsupported: "Unsupported",
  unauthorized: "Forbidden",
  invalidName: "InvalidArgument",
  notFound: "NotFound",
  tooLarge: "TooLarge",
  backend: "Backend",
  unavailable: "Unavailable",
  version: "VersionSkew"
}

const AGENT_CODES: Readonly<Record<string, keyof typeof ResultCodeName>> = {
  unsupported: "Unsupported",
  notFound: "NotFound",
  invalid: "InvalidArgument",
  backend: "Backend",
  unavailable: "Unavailable",
  version: "VersionSkew",
  notLeader: "Unavailable"
}

const CHECKPOINT_CODES: Readonly<Record<string, keyof typeof ResultCodeName>> = {
  invalid: "InvalidArgument",
  not_found: "NotFound",
  conflict: "Conflict",
  lease_lost: "Conflict",
  unauthorized: "Forbidden",
  unavailable: "Unavailable",
  version: "VersionSkew"
}

function surfaceCode(
  table: Readonly<Record<string, keyof typeof ResultCodeName>>,
  kind: string | undefined
): ResultCode {
  return known((kind === undefined ? undefined : table[kind]) ?? "Backend")
}

/**
 * This failure's place in the unified result-code space, the same code the
 * Rust `LaserError::code()` and the management HTTP surface give it. A
 * managed surface error projects onto its surface code, and a client-side
 * failure maps to the closest code.
 */
export function code(error: unknown): ResultCode {
  const cause = publishCause(error)
  const iggy = iggyErrorCode(error)
  if (iggy === IGGY_RESOURCE_NOT_FOUND || isStreamOrTopicNotFound(error)) return known("NotFound")
  if (iggy === IGGY_UNAUTHORIZED) return known("Forbidden")
  if (iggy === IGGY_UNAUTHENTICATED) return known("Unauthenticated")
  if (cause instanceof FilterExecutionError) return cause.detail.code
  if (cause instanceof FilterStopError) {
    return known(cause.stop === "fault" ? "InvalidArgument" : "TooLarge")
  }
  if (cause instanceof ConsumerGroupSetupError) return code(cause.cause)
  if (cause instanceof QueryExecutionError) return surfaceCode(QUERY_CODES, detailKind(cause))
  if (cause instanceof KvExecutionError) return surfaceCode(KV_CODES, detailKind(cause))
  if (cause instanceof ForkExecutionError) return surfaceCode(FORK_CODES, detailKind(cause))
  if (cause instanceof GraphExecutionError) return surfaceCode(GRAPH_CODES, detailKind(cause))
  if (cause instanceof AgentWorkflowExecutionError) {
    return surfaceCode(AGENT_CODES, detailKind(cause))
  }
  if (cause instanceof CheckpointExecutionError) {
    return surfaceCode(CHECKPOINT_CODES, detailKind(cause))
  }
  if (cause instanceof TransportError && cause.retryable) return known("Unavailable")
  if (cause instanceof RoutingError) {
    return known(cause.reason.kind === "principalMismatch" ? "Forbidden" : "NotFound")
  }
  if (!(cause instanceof LaserError)) return known("Backend")
  switch (cause.kind) {
    case "unsupported":
    case "no-stream":
    case "no-respond-topic":
      return known("Unsupported")
    case "invalid":
    case "config":
    case "handler-config":
    case "rejected":
      return known("InvalidArgument")
    case "fence-violation":
    case "presence-conflict":
      return known("Conflict")
    case "signature":
      return known("Unauthenticated")
    case "quarantined":
    case "policy-blocked":
      return known("Forbidden")
    case "step-up-required":
      return known("StepUpRequired")
    case "timeout":
    case "ambiguous-mutation":
    case "cancelled":
    case "codec":
    case "typed-decode":
    case "protocol":
    case "transport":
    case "query":
    case "kv":
    case "fork":
    case "graph":
    case "authz":
    case "filter":
    case "agent-workflow":
    case "routing":
    case "handler":
    case "state-store":
    case "integrity":
    case "budget-exceeded":
    case "policy-deferred":
    case "publish-failed":
    case "checkpoint":
      return known("Backend")
  }
}
