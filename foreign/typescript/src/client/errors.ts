import type { MessageWithHeaders, SendMessagesConfirmation } from "../iggy/apache-iggy.js"
import type { FaultReason, FilterError, FilterGroupIdentity } from "../wire/filter.js"
import type { SessionError as WireSessionError } from "../wire/session.js"
import type { CommandError as WireCommandError } from "../wire/result.js"

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
  | "command"
  | "transport"
  | "query"
  | "kv"
  | "fork"
  | "graph"
  | "session"
  | "authz"
  | "filter"
  | "routing"
  | "presence-conflict"
  | "signature"
  | "handler"
  | "handler-config"
  | "state-store"
  | "integrity"
  | "rejected"
  | "budget-exceeded"
  | "session-ended"
  | "policy-blocked"
  | "step-up-required"
  | "policy-deferred"
  | "publish-failed"
  | "fence-violation"
  | "quarantined"
  | "no-respond-topic"
  | "step-failed"
  | "journal-truncated"

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

/** A convenience call needed a default stream and none was set. A
 * configuration mistake, never retryable. */
export class NoStreamError extends ConfigError {
  override readonly kind: LaserErrorKind = "no-stream"
}

export class TimeoutError extends LaserError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, "timeout", options)
  }
}

/** A managed mutation's outcome is unknown. `operationId` is the identity
 * it was sent under, when it had one: repeating the mutation under it returns
 * the recorded outcome instead of applying the change a second time. */
export class AmbiguousMutationError extends LaserError {
  readonly operationId: bigint | undefined

  constructor(message: string, options?: { cause?: unknown; operationId?: bigint }) {
    super(message, "ambiguous-mutation", options)
    this.operationId = options?.operationId
  }
}

/** An aborted wait, or a workflow run whose cancel request was observed at a
 * step boundary. `run` names that run. Not retryable on the same run. */
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
export class IdError extends InvalidError {
  override readonly kind: LaserErrorKind = "id"

  private constructor(message: string) {
    super(message)
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

/** A typed read could not produce a value. It classifies as its source: a
 * failed poll keeps the transport error's code and retry answer, and a record
 * that does not decode is a codec failure. */
export class TypedDecodeError extends CodecError {
  override readonly kind: LaserErrorKind = "typed-decode"
  readonly position: { readonly partitionId: number; readonly offset: bigint } | undefined
  /** What went wrong: a codec failure for a positioned record, the transport error for a failed poll. */
  readonly source: LaserError

  constructor(
    message: string,
    position: { readonly partitionId: number; readonly offset: bigint } | undefined,
    source: LaserError
  ) {
    super(message, "typed", "decode", { cause: source })
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

export class CommandExecutionError extends LaserError {
  readonly detail: WireCommandError

  constructor(message: string, detail: WireCommandError, options?: { cause?: unknown }) {
    super(message, "command", options)
    this.detail = detail
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

/** The deployment answered a session read with a typed failure. */
export class SessionError extends LaserError {
  constructor(
    message: string,
    readonly detail: WireSessionError,
    options?: { cause?: unknown }
  ) {
    super(message, "session", options)
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

/** A consumer-filter failure. `detail` is the typed refusal of the streaming
 * server or the filter catalog, absent for a failure the reader found itself.
 * `reason` names the cause: the refusal's reason, the fault reason, `oversized_record`,
 * or for a group setup failure its cause's reason or `setup_failed`. */
export class FilterExecutionError extends LaserError {
  readonly detail: FilterError | undefined
  readonly reason: string

  constructor(
    message: string,
    detail: FilterError | undefined,
    options?: { cause?: unknown; reason?: string }
  ) {
    super(message, "filter", options)
    this.detail = detail
    this.reason = options?.reason ?? detail?.reason ?? "unknown"
  }
}

/** A filtered read stopped in front of a record the filter could not
 * evaluate under the `stop` fault policy. The record and everything after it
 * stay unacknowledged. `reason` is the fault reason. */
export class FilterFaultError extends FilterExecutionError {
  declare readonly reason: FaultReason

  constructor(
    readonly partitionId: number,
    readonly offset: bigint,
    reason: FaultReason
  ) {
    super(
      `filter fault on partition ${String(partitionId)} at offset ${offset.toString()}: ${reason}`,
      undefined,
      { reason }
    )
  }
}

/** A matching record alone exceeds the filtered reply cap, so no page can
 * carry it. The record and everything after it stay unacknowledged. */
export class FilterOversizedRecordError extends FilterExecutionError {
  constructor(
    readonly partitionId: number,
    readonly offset: bigint
  ) {
    super(
      `the record at offset ${offset.toString()} on partition ${String(partitionId)} exceeds the filtered reply cap`,
      undefined,
      { reason: "oversized_record" }
    )
  }
}

/**
 * The consumer group exists, but its filter was not configured. The group
 * stays as it is, unbound unless it ran a policy before, and no reader joined
 * it. `cause` is the catalog refusal or the transport failure, `reason` the
 * catalog's reason when there is one, else `setup_failed`.
 */
export class ConsumerGroupSetupError extends FilterExecutionError {
  constructor(
    readonly groupId: number,
    readonly groupName: string,
    readonly identity: FilterGroupIdentity,
    cause: unknown
  ) {
    super(
      `consumer group ${String(groupId)} (${groupName}) exists but its filter was not configured: ${cause instanceof Error ? cause.message : String(cause)}`,
      undefined,
      {
        cause,
        reason:
          cause instanceof FilterExecutionError && cause.detail !== undefined
            ? cause.detail.reason
            : "setup_failed"
      }
    )
  }
}

/** Capability routing could not address an agent. */
export class RoutingError extends LaserError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, "routing", options)
  }
}

/** No live agent advertises `skill`. Retry after the registry refreshes, or route elsewhere. */
export class NoCapableAgentError extends RoutingError {
  constructor(readonly skill: string) {
    super(`no live agent advertises capability \`${skill}\``)
  }
}

/** The capable `agent` advertises no inbox topic in its live presence. */
export class NoInboxError extends RoutingError {
  constructor(readonly agent: string) {
    super(`no inbox advertised for agent \`${agent}\``)
  }
}

/** The selected `agent` is not bound to the `expected` principal. Never retryable. */
export class RoutePrincipalMismatchError extends RoutingError {
  constructor(
    readonly agent: string,
    readonly expected: number,
    readonly actual?: number
  ) {
    super(`agent \`${agent}\` is not authenticated as principal ${expected.toString()}`)
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

/** A deterministic handler wiring, startup, or configuration failure, or a
 * programming error in the handler. Never retryable. */
export class HandlerConfigError extends ConfigError {
  override readonly kind: LaserErrorKind = "handler-config"
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

/** `begin` named a session that already ended, or `resume` named one an
 * agent or an operator canceled. `status` is `completed`, `failed`, or
 * `canceled`. A completed or failed session reopens through `resume`. */
/** Why a workflow step did not complete, the Rust `StepFailure`. */
export type StepFailure = "timed_out" | "failed" | "not_consumed" | "unverified" | "no_completion"

/** A workflow step did not complete. `reason` says how, and only a step that
 * timed out is retryable. The completed steps' compensations have run. */
export class StepFailedError extends LaserError {
  constructor(
    readonly run: string,
    readonly step: string,
    readonly reason: StepFailure
  ) {
    super(`workflow run ${run} step \`${step}\` failed: ${reason}`, "step-failed")
  }
}

/** A workflow resume found the run's journal shortened by retention, so the
 * steps the run completed cannot be known. Not retryable on the same run. */
export class JournalTruncatedError extends LaserError {
  constructor(readonly run: string) {
    super(`workflow run ${run} journal no longer holds the run's first record`, "journal-truncated")
  }
}

export class SessionEndedError extends LaserError {
  constructor(
    readonly session: string,
    readonly status: string
  ) {
    super(`session ${session} already ended as ${status}`, "session-ended")
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

/** A fenced write lost the fence: the `held` token is below the `current`
 * sequence, so a newer holder owns the task. Never retryable by the loser. */
export class FenceViolationError extends LaserError {
  constructor(
    readonly held: bigint,
    readonly current: bigint
  ) {
    super(
      `fence violation: held ${held.toString()}, current ${current.toString()}`,
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

/** The agent was built without a `respondOn` topic. A configuration
 * mistake, never retryable. */
export class NoRespondTopicError extends ConfigError {
  override readonly kind: LaserErrorKind = "no-respond-topic"

  constructor(message = "the agent has no respondOn topic configured") {
    super(message)
  }
}

export function assertNever(value: never): never {
  throw new InvalidError("unreachable variant", { value })
}
