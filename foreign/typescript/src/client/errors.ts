import type { MessageWithHeaders, SendMessagesConfirmation } from "../iggy/apache-iggy.js"
import type { CheckpointError } from "../wire/checkpoint.js"
import type { FaultReason, FilterError, FilterGroupIdentity } from "../wire/filter.js"

export type LaserErrorKind =
  | "config"
  | "no-stream"
  | "timeout"
  | "ambiguous-mutation"
  | "cancelled"
  | "unsupported"
  | "invalid"
  | "id"
  | "provenance"
  | "codec"
  | "typed-decode"
  | "protocol"
  | "transport"
  | "query"
  | "kv"
  | "fork"
  | "graph"
  | "authz"
  | "filter"
  | "agent-workflow"
  | "routing"
  | "presence-conflict"
  | "signature"
  | "handler"
  | "handler-config"
  | "state-store"
  | "integrity"
  | "rejected"
  | "budget-exceeded"
  | "policy-blocked"
  | "step-up-required"
  | "policy-deferred"
  | "publish-failed"
  | "fence-violation"
  | "quarantined"
  | "no-respond-topic"
  | "checkpoint"

export class LaserError extends Error {
  readonly kind: LaserErrorKind

  protected constructor(message: string, kind: LaserErrorKind, options?: { cause?: unknown }) {
    super(message, options)
    this.kind = kind
    this.name = new.target.name
  }
}

export class ConfigError extends LaserError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, "config", options)
  }
}

export class NoStreamError extends LaserError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, "no-stream", options)
  }
}

export class TimeoutError extends LaserError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, "timeout", options)
  }
}

export class AmbiguousMutationError extends LaserError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, "ambiguous-mutation", options)
  }
}

/** An aborted wait, or a registered run whose cancel intent was observed at
 * a step boundary. `run` names that run. Not retryable on the same run. */
export class CancelledError extends LaserError {
  readonly run: string | undefined

  constructor(message: string, options?: { cause?: unknown; run?: string }) {
    super(message, "cancelled", options)
    this.run = options?.run
  }
}

/** The connected infrastructure does not provide the requested feature.
 * Permanent: gate the code path instead of retrying. `surface` names the
 * accessor the call came through and `feature` the sub-capability when one
 * exists. */
export class UnsupportedError extends LaserError {
  readonly surface: string | undefined
  readonly feature: string | undefined

  constructor(message: string, options?: { cause?: unknown; surface?: string; feature?: string }) {
    super(message, "unsupported", options)
    this.surface = options?.surface
    this.feature = options?.feature
  }
}

export class InvalidError extends LaserError {
  readonly context: Readonly<Record<string, unknown>> | undefined

  constructor(
    message: string,
    context?: Readonly<Record<string, unknown>>,
    options?: { cause?: unknown }
  ) {
    super(message, "invalid", options)
    this.context = context
  }
}

/** Why parsing or validating an id failed: a conversation, agent, group, or
 * message id. Each static constructor is one Rust `IdError` variant with the
 * same message. */
export class IdError extends LaserError {
  private constructor(message: string) {
    super(message, "id")
  }

  static empty(): IdError {
    return new IdError("identifier must not be empty")
  }

  static tooLong(got: number, max: number): IdError {
    return new IdError(`identifier length ${String(got)}B exceeds max ${String(max)}B`)
  }

  static invalidChar(char: string): IdError {
    return new IdError(`identifier contains invalid character \`${char}\``)
  }

  static invalidUlid(text: string): IdError {
    return new IdError(`invalid ULID \`${text}\``)
  }

  static invalidMessageId(text: string): IdError {
    return new IdError(`invalid message id \`${text}\`, expected \`<partition_id>:<offset>\``)
  }
}

/** Why encoding or decoding provenance headers failed. Each static
 * constructor is one Rust `ProvenanceError` variant with the same message. */
export class ProvenanceError extends LaserError {
  private constructor(message: string, options?: { cause?: unknown }) {
    super(message, "provenance", options)
  }

  static missingRequired(key: string): ProvenanceError {
    return new ProvenanceError(`missing required header \`${key}\``)
  }

  static tooLarge(got: number, cap: number): ProvenanceError {
    return new ProvenanceError(`provenance headers ${String(got)}B exceed soft cap ${String(cap)}B`)
  }

  static invalidValue(key: string): ProvenanceError {
    return new ProvenanceError(`invalid value for header \`${key}\``)
  }

  static invalidValueBytes(key: string): ProvenanceError {
    return new ProvenanceError(`header \`${key}\` value must not contain control characters or NUL`)
  }

  static nonFinite(key: string): ProvenanceError {
    return new ProvenanceError(`non-finite floating-point value for header \`${key}\``)
  }

  static emptyValue(key: string): ProvenanceError {
    return new ProvenanceError(`header \`${key}\` value must not be empty`)
  }

  static valueTooLong(key: string, got: number, max: number): ProvenanceError {
    return new ProvenanceError(
      `header \`${key}\` value is ${String(got)}B, exceeds max ${String(max)}B`
    )
  }

  static malformedHeaders(detail: string): ProvenanceError {
    return new ProvenanceError(`malformed Iggy headers on this message: ${detail}`)
  }

  /** A header block the Apache Iggy client could not read, with its error as the cause. */
  static header(cause: unknown): ProvenanceError {
    return new ProvenanceError(cause instanceof Error ? cause.message : String(cause), { cause })
  }

  /** An id header that does not parse, with the `IdError` as the cause. */
  static id(cause: IdError): ProvenanceError {
    return new ProvenanceError(cause.message, { cause })
  }
}

export class CodecError extends LaserError {
  /** @internal */
  readonly surface: string
  /** @internal */
  readonly operation: string

  constructor(message: string, surface: string, operation: string, options?: { cause?: unknown }) {
    super(message, "codec", options)
    this.surface = surface
    this.operation = operation
  }
}

export class TypedDecodeError extends LaserError {
  readonly position: { readonly partitionId: number; readonly offset: bigint } | undefined
  /** What went wrong: a codec failure for a positioned record, the transport error for a failed poll. */
  readonly source: LaserError

  constructor(
    message: string,
    position: { readonly partitionId: number; readonly offset: bigint } | undefined,
    source: LaserError
  ) {
    super(message, "typed-decode", { cause: source })
    this.position = position
    this.source = source
  }
}

export class ProtocolError extends LaserError {
  /** @internal */
  readonly resultCode: number | undefined
  /** @internal */
  readonly commandCode: number | undefined

  constructor(
    message: string,
    details?: { resultCode?: number; commandCode?: number },
    options?: { cause?: unknown }
  ) {
    super(message, "protocol", options)
    this.resultCode = details?.resultCode
    this.commandCode = details?.commandCode
  }
}

export class TransportError extends LaserError {
  /** Read through `isRetryable`, like the Rust `is_retryable`.
   * @internal */
  readonly retryable: boolean

  constructor(message: string, retryable: boolean, options?: { cause?: unknown }) {
    super(message, "transport", options)
    this.retryable = retryable
  }
}

export class SignatureError extends LaserError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, "signature", options)
  }
}

export class QueryExecutionError extends LaserError {
  constructor(
    message: string,
    readonly detail: unknown,
    options?: { cause?: unknown }
  ) {
    super(message, "query", options)
  }
}

export class KvExecutionError extends LaserError {
  constructor(
    message: string,
    readonly detail: unknown,
    options?: { cause?: unknown }
  ) {
    super(message, "kv", options)
  }
}

export class ForkExecutionError extends LaserError {
  constructor(
    message: string,
    readonly detail: unknown,
    options?: { cause?: unknown }
  ) {
    super(message, "fork", options)
  }
}

export class GraphExecutionError extends LaserError {
  constructor(
    message: string,
    readonly detail: unknown,
    options?: { cause?: unknown }
  ) {
    super(message, "graph", options)
  }
}

export class AuthzExecutionError extends LaserError {
  constructor(
    message: string,
    readonly detail: unknown,
    options?: { cause?: unknown }
  ) {
    super(message, "authz", options)
  }
}

/** The streaming server or the filter catalog refused a consumer-filter operation. */
export class FilterExecutionError extends LaserError {
  constructor(
    message: string,
    readonly detail: FilterError,
    options?: { cause?: unknown }
  ) {
    super(message, "filter", options)
  }
}

/** A filtered read stopped in front of a record the filter could not
 * evaluate under the `stop` fault policy. The record and everything after it
 * stay unacknowledged. */
export class FilterFaultError extends LaserError {
  constructor(
    readonly partitionId: number,
    readonly offset: bigint,
    readonly reason?: FaultReason
  ) {
    super(
      `filter fault on partition ${String(partitionId)} at offset ${offset.toString()}: ${reason ?? "malformed"}`,
      "filter"
    )
  }
}

/** A matching record alone exceeds the filtered reply cap, so no page can
 * carry it. The record and everything after it stay unacknowledged. */
export class FilterOversizedRecordError extends LaserError {
  constructor(
    readonly partitionId: number,
    readonly offset: bigint
  ) {
    super(
      `the record at offset ${offset.toString()} on partition ${String(partitionId)} exceeds the filtered reply cap`,
      "filter"
    )
  }
}

/**
 * The consumer group exists, but its filter was not configured. The group
 * stays as it is, unbound unless it ran a policy before, and no reader joined
 * it. `cause` is the catalog refusal or the transport failure, and
 * `filterReason(error.cause)` the catalog's reason when there is one.
 */
export class ConsumerGroupSetupError extends LaserError {
  constructor(
    readonly groupId: number,
    readonly groupName: string,
    readonly identity: FilterGroupIdentity,
    cause: unknown
  ) {
    super(
      `consumer group ${String(groupId)} (${groupName}) exists but its filter was not configured: ${cause instanceof Error ? cause.message : String(cause)}`,
      "filter",
      { cause }
    )
  }
}

export class AgentWorkflowExecutionError extends LaserError {
  constructor(
    message: string,
    readonly detail: unknown,
    options?: { cause?: unknown }
  ) {
    super(message, "agent-workflow", options)
  }
}

export class NoCapableAgentError extends LaserError {
  constructor(readonly skill: string) {
    super(`no live agent advertises capability \`${skill}\``, "routing")
  }
}

export class NoInboxError extends LaserError {
  constructor(readonly agent: string) {
    super(`no inbox advertised for agent \`${agent}\``, "routing")
  }
}

export class RoutePrincipalMismatchError extends LaserError {
  constructor(
    readonly agent: string,
    readonly expected: number,
    readonly actual?: number
  ) {
    super(`agent \`${agent}\` is not authenticated as principal ${expected.toString()}`, "routing")
  }
}

export class RejectedError extends LaserError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, "rejected", options)
  }
}

export class PresenceConflictError extends LaserError {
  constructor(
    readonly advertised: string,
    readonly requested: string
  ) {
    super(
      `connection already advertises agent \`${advertised}\`, cannot advertise \`${requested}\``,
      "presence-conflict"
    )
  }
}

export class HandlerError extends LaserError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, "handler", options)
  }
}

export class HandlerConfigError extends LaserError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, "handler-config", options)
  }
}

export class StateStoreError extends LaserError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, "state-store", options)
  }
}

export class IntegrityError extends LaserError {
  readonly reference: string

  constructor(reference: string) {
    super(`blob at \`${reference}\` failed integrity verification`, "integrity")
    this.reference = reference
  }
}

export class PolicyBlockedError extends LaserError {
  constructor(message: string) {
    super(message, "policy-blocked")
  }
}

export class StepUpRequiredError extends LaserError {
  readonly scope: string

  constructor(scope: string) {
    super(`policy requires approval scope \`${scope}\``, "step-up-required")
    this.scope = scope
  }
}

export class PolicyDeferredError extends LaserError {
  constructor(message: string) {
    super(message, "policy-deferred")
  }
}

export class BudgetExceededError extends LaserError {
  constructor(
    readonly ceiling: bigint,
    readonly spent: bigint
  ) {
    super(
      `budget exceeded: spent ${spent.toString()}, ceiling ${ceiling.toString()}`,
      "budget-exceeded"
    )
  }
}

/**
 * A publish that gave up. `committed` lists the ranges confirmed before the
 * failure, never replay them. `unconfirmed` lists the records left without a
 * confirmation. `cause` is the original failure, and `publishCause()` reaches
 * it through any nesting, so every classifier answers for it.
 */
export class PublishFailedError extends LaserError {
  constructor(
    readonly stream: string,
    readonly topic: string,
    readonly committed: readonly SendMessagesConfirmation[],
    readonly unconfirmed: readonly MessageWithHeaders[],
    cause: unknown
  ) {
    super(
      `publish failed to ${stream}/${topic}: ${cause instanceof Error ? cause.message : String(cause)}`,
      "publish-failed",
      { cause }
    )
  }

  /** The original failure under any publish wrapping. */
  publishCause(): unknown {
    return publishCause(this)
  }
}

/** The original failure under any `PublishFailedError` wrapping. */
export function publishCause(error: unknown): unknown {
  let current = error
  while (current instanceof PublishFailedError) current = current.cause
  return current
}

/** A fenced write lost the fence: the held token is below the live sequence,
 * so a newer holder owns the task. Never retryable by the loser. */
export class FenceViolationError extends LaserError {
  constructor(
    readonly stale: bigint,
    readonly current: bigint
  ) {
    super(
      `fence violation: held ${stale.toString()}, current ${current.toString()}`,
      "fence-violation"
    )
  }
}

/** The agent was quarantined and may not act. Not retryable. */
export class QuarantinedError extends LaserError {
  constructor(readonly agent: string) {
    super(`quarantined: ${agent}`, "quarantined")
  }
}

/** The agent was built without a `respondOn` topic. Not retryable. */
export class NoRespondTopicError extends LaserError {
  constructor(message = "the agent has no respondOn topic configured") {
    super(message, "no-respond-topic")
  }
}

/** Iggy answered a destination or checkpoint operation with a typed failure. */
export class CheckpointExecutionError extends LaserError {
  constructor(
    message: string,
    readonly detail: CheckpointError,
    options?: { cause?: unknown }
  ) {
    super(message, "checkpoint", options)
  }
}

export function assertNever(value: never): never {
  throw new InvalidError("unreachable variant", { value })
}
