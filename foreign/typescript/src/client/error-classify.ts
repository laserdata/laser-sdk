import {
  type ResultCode,
  type ResultCodeName,
  resultCodeIsPermissionDenied,
  resultCodeIsRetryable,
  resultCodeIsUnavailable
} from "../wire/result.js"
import type { FilterErrorReason } from "../wire/filter.js"
import type { QueryErrorCode } from "../wire/query.js"
import {
  AmbiguousMutationError,
  AuthzExecutionError,
  BudgetExceededError,
  CommandExecutionError,
  ConsumerGroupSetupError,
  FenceViolationError,
  FilterExecutionError,
  FilterFaultError,
  FilterOversizedRecordError,
  ForkExecutionError,
  GraphExecutionError,
  HandlerError,
  SessionError,
  KvExecutionError,
  LaserError,
  NoCapableAgentError,
  NoInboxError,
  PolicyDeferredError,
  ProtocolError,
  PublishFailedError,
  QuarantinedError,
  StepFailedError,
  QueryExecutionError,
  RoutePrincipalMismatchError,
  TimeoutError,
  TransportError,
  TypedDecodeError
} from "./errors.js"
import { sessionErrorResultCode } from "../wire/session.js"
import { type AuthzError, authzErrorResultCode } from "../wire/authz.js"

// Apache Iggy server error codes, classified as the Rust SDK classifies them.
const IGGY_RESOURCE_NOT_FOUND = 20
const IGGY_UNAUTHENTICATED = 40
const IGGY_UNAUTHORIZED = 41
const IGGY_STREAM_OR_TOPIC_NOT_FOUND: ReadonlySet<number> = new Set([1009, 1010, 2010, 2011])
// The codes Rust `is_transient_iggy_io_error` names: a lost or refused link,
// and a write or read the cluster could not serve yet.
const IGGY_TRANSIENT: ReadonlySet<number> = new Set([
  8, 9, 30, 31, 32, 57, 58, 61, 206, 3004, 4050, 10002
])

const QUERY_CODES: Readonly<Record<QueryErrorCode, keyof typeof ResultCodeName>> = {
  unsupported: "Unsupported",
  invalid: "InvalidArgument",
  unauthorized: "Forbidden",
  index_not_found: "NotFound",
  fork_not_found: "NotFound",
  schema_not_found: "NotFound",
  backend: "Backend",
  unavailable: "Unavailable",
  too_large: "TooLarge",
  version: "VersionSkew",
  stale: "Stale",
  deadline_exceeded: "DeadlineExceeded",
  stale_generation: "StaleGeneration"
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
  leaseHeld: "Conflict",
  notFound: "NotFound",
  notLeader: "Unavailable",
  stale: "Stale",
  barrierUnreachable: "StaleGeneration",
  forbidden: "Forbidden",
  invalidExpiry: "InvalidArgument"
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

/**
 * This failure's place in the unified result-code space, the same code the
 * Rust `LaserError::code()` and the management HTTP surface give it. A
 * managed surface error projects onto its surface code, and a client-side
 * failure maps to the closest code. Every broad classifier below reads it.
 */
export function code(error: unknown): ResultCode {
  const cause = classified(error)
  if (cause instanceof CommandExecutionError) return cause.detail.code
  const iggy = iggyErrorCode(cause)
  if (iggy !== undefined) return iggyResultCode(iggy)
  if (cause instanceof TransportError) return known(cause.retryable ? "Unavailable" : "Backend")
  if (cause instanceof FilterFaultError) return known("InvalidArgument")
  if (cause instanceof FilterOversizedRecordError) return known("TooLarge")
  if (cause instanceof FilterExecutionError) return cause.detail?.code ?? known("Backend")
  if (cause instanceof QueryExecutionError) return surfaceCode(QUERY_CODES, detailKind(cause))
  if (cause instanceof KvExecutionError) return surfaceCode(KV_CODES, detailKind(cause))
  if (cause instanceof ForkExecutionError) return surfaceCode(FORK_CODES, detailKind(cause))
  if (cause instanceof GraphExecutionError) return surfaceCode(GRAPH_CODES, detailKind(cause))
  if (cause instanceof SessionError) return sessionErrorResultCode(cause.detail)
  if (cause instanceof AuthzExecutionError && isAuthzError(cause.detail)) {
    return authzErrorResultCode(cause.detail)
  }
  if (cause instanceof StepFailedError) {
    return known(
      cause.reason === "timed_out" || cause.reason === "not_consumed"
        ? "DeadlineExceeded"
        : "Backend"
    )
  }
  if (cause instanceof RoutePrincipalMismatchError) return known("Forbidden")
  if (cause instanceof NoCapableAgentError || cause instanceof NoInboxError)
    return known("NotFound")
  if (!(cause instanceof LaserError)) return known("Backend")
  switch (cause.kind) {
    case "unsupported":
      return known("Unsupported")
    case "invalid":
    case "id":
    case "config":
    case "no-stream":
    case "no-respond-topic":
    case "handler-config":
    case "rejected":
      return known("InvalidArgument")
    case "fence-violation":
    case "presence-conflict":
    case "session-ended":
      return known("Conflict")
    case "signature":
      return known("Unauthenticated")
    case "quarantined":
    case "policy-blocked":
      return known("Forbidden")
    case "step-up-required":
      return known("StepUpRequired")
    case "cancelled":
      return known("Cancelled")
    case "journal-truncated":
      return known("ExpiredSnapshot")
    case "command":
    case "timeout":
    case "ambiguous-mutation":
    case "codec":
    case "typed-decode":
    case "protocol":
    case "transport":
    case "query":
    case "kv":
    case "fork":
    case "graph":
    case "session":
    case "authz":
    case "filter":
    case "routing":
    case "handler":
    case "state-store":
    case "integrity":
    case "budget-exceeded":
    case "policy-deferred":
    case "publish-failed":
    case "provenance":
    case "step-failed":
      return known("Backend")
  }
}

/** Whether retrying the same call can succeed, the classifier the Rust
 * `LaserError::is_retryable` defines: the retryable result codes, plus a
 * transient handler failure, a timeout, a deferred policy decision and a
 * routing miss the registry may clear. */
export function isRetryable(error: unknown): boolean {
  const cause = classified(error)
  if (
    cause instanceof HandlerError ||
    cause instanceof TimeoutError ||
    cause instanceof PolicyDeferredError ||
    cause instanceof NoCapableAgentError ||
    cause instanceof NoInboxError
  )
    return true
  if (cause instanceof StepFailedError) return cause.reason === "timed_out"
  return resultCodeIsRetryable(code(cause))
}

/** Whether the failure was transient: the identical request may succeed on a
 * later attempt. */
export function isUnavailable(error: unknown): boolean {
  return resultCodeIsUnavailable(code(error))
}

/** Whether the connected infrastructure does not provide the feature at all.
 * Permanent: gate the code path instead of retrying. */
export function isUnsupported(error: unknown): boolean {
  return codeIs(error, "Unsupported")
}

/** Whether the failure names a missing resource. */
export function isNotFound(error: unknown): boolean {
  return codeIs(error, "NotFound")
}

/** Whether the failure is wire-version skew between this SDK and the
 * connected deployment. Permanent for this client build. */
export function isVersionSkew(error: unknown): boolean {
  return codeIs(error, "VersionSkew")
}

/** Whether the server refused the operation for a missing or unverified
 * credential or a missing grant. The fix is a grant, not a retry. */
export function isPermissionDenied(error: unknown): boolean {
  return resultCodeIsPermissionDenied(code(error))
}

/** Whether a read-consistency barrier was not met yet. Retryable. */
export function isStale(error: unknown): boolean {
  return codeIs(error, "Stale")
}

/** The Apache Iggy server error code under a failure, when the server
 * answered one. Publish and group setup wrapping is followed, then the causes
 * of a transport or protocol failure. Another typed SDK error that wraps an
 * Apache Iggy failure keeps its own classification, like the Rust SDK. */
export function iggyErrorCode(error: unknown): number | undefined {
  let current = classified(error)
  if (
    current instanceof LaserError &&
    !(current instanceof TransportError) &&
    !(current instanceof ProtocolError)
  ) {
    return undefined
  }
  for (let depth = 0; depth < 8; depth += 1) {
    if (typeof current !== "object" || current === null) return undefined
    const errorCode = (current as { readonly errorCode?: unknown }).errorCode
    if (typeof errorCode === "number") return errorCode
    current = "cause" in current ? (current as { readonly cause?: unknown }).cause : undefined
  }
  return undefined
}

/** The typed consumer-filter cause, when the failure is one. A group setup
 * failure answers for the catalog refusal under it. */
export function filterReason(error: unknown): FilterErrorReason | undefined {
  const cause = classified(error)
  return cause instanceof FilterExecutionError ? cause.detail?.reason : undefined
}

/** Whether the target stream or topic does not exist for this principal.
 * Under RBAC an existing stream the principal cannot see answers like a
 * missing one. */
export function isStreamOrTopicNotFound(error: unknown): boolean {
  const serverCode = iggyErrorCode(error)
  return serverCode !== undefined && IGGY_STREAM_OR_TOPIC_NOT_FOUND.has(serverCode)
}

/** Whether a compare-and-swap lost its precondition. Re-read, recompute, and
 * commit again. */
export function isVersionConflict(error: unknown): boolean {
  const cause = classified(error)
  return cause instanceof KvExecutionError && detailKind(cause) === "versionConflict"
}

/** Whether a managed mutation may already have reached the server. A generic
 * retry loop must stop. */
export function isAmbiguousMutation(error: unknown): boolean {
  return classified(error) instanceof AmbiguousMutationError
}

/** Whether the managed backend declined a conditional write because it does
 * not own the mutation partition. */
export function isNotLeader(error: unknown): boolean {
  const cause = classified(error)
  return (
    (cause instanceof KvExecutionError || cause instanceof ForkExecutionError) &&
    detailKind(cause) === "notLeader"
  )
}

/** Whether capability routing found no live agent for the skill. */
export function isNoCapableAgent(error: unknown): boolean {
  return classified(error) instanceof NoCapableAgentError
}

/** Whether a revocable lease was lost. */
export function isLeaseLost(error: unknown): boolean {
  const cause = classified(error)
  return cause instanceof KvExecutionError && detailKind(cause) === "leaseLost"
}

/** Whether a fenced write lost the fence. Never retryable by the loser. */
export function isFenceViolation(error: unknown): boolean {
  return classified(error) instanceof FenceViolationError
}

/** Whether a spend ceiling was reached. */
export function isBudgetExceeded(error: unknown): boolean {
  return classified(error) instanceof BudgetExceededError
}

/** Whether the agent is quarantined. */
export function isQuarantined(error: unknown): boolean {
  return classified(error) instanceof QuarantinedError
}

export function publicErrorMessage(error: unknown): string {
  const result = code(error)
  if (result.kind !== "known") return "internal error"
  const messages: Partial<Record<keyof typeof ResultCodeName, string>> = {
    InvalidArgument: "invalid request",
    Unsupported: "unsupported operation",
    Conflict: "conflict",
    Unauthenticated: "unauthenticated",
    Forbidden: "forbidden",
    StepUpRequired: "step-up authorization required",
    NotFound: "not found"
  }
  return messages[result.name] ?? "internal error"
}

// The failure a wrapper stands for: a failed publish answers for its cause, a
// group setup failure for the refusal or transport failure under it, and a
// typed read whose poll failed for that failure.
function classified(error: unknown): unknown {
  let current = error
  for (let depth = 0; depth < 8; depth += 1) {
    if (current instanceof PublishFailedError || current instanceof ConsumerGroupSetupError) {
      current = current.cause
    } else if (current instanceof TypedDecodeError && current.position === undefined) {
      current = current.source
    } else {
      return current
    }
  }
  return current
}

function codeIs(error: unknown, name: keyof typeof ResultCodeName): boolean {
  const value = code(error)
  return value.kind === "known" && value.name === name
}

function iggyResultCode(serverCode: number): ResultCode {
  if (serverCode === IGGY_RESOURCE_NOT_FOUND || IGGY_STREAM_OR_TOPIC_NOT_FOUND.has(serverCode))
    return known("NotFound")
  if (serverCode === IGGY_UNAUTHORIZED) return known("Forbidden")
  if (serverCode === IGGY_UNAUTHENTICATED) return known("Unauthenticated")
  return known(IGGY_TRANSIENT.has(serverCode) ? "Unavailable" : "Backend")
}

function known(name: keyof typeof ResultCodeName): ResultCode {
  return { kind: "known", name }
}

function detailKind(error: unknown): string | undefined {
  if (
    error instanceof QueryExecutionError ||
    error instanceof KvExecutionError ||
    error instanceof ForkExecutionError ||
    error instanceof GraphExecutionError
  ) {
    const detail = error.detail as { readonly kind?: unknown } | undefined
    return typeof detail?.kind === "string" ? detail.kind : undefined
  }
  return undefined
}

function surfaceCode(
  table: Readonly<Record<string, keyof typeof ResultCodeName>>,
  kind: string | undefined
): ResultCode {
  return known((kind === undefined ? undefined : table[kind]) ?? "Backend")
}

function isAuthzError(detail: unknown): detail is AuthzError {
  return typeof detail === "object" && detail !== null && "kind" in detail
}
