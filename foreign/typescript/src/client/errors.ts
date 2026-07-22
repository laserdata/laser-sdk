export type LaserErrorKind =
  | "config"
  | "no-stream"
  | "timeout"
  | "cancelled"
  | "unsupported"
  | "invalid"
  | "codec"
  | "typed-decode"
  | "protocol"
  | "transport"
  | "query"
  | "kv"
  | "fork"
  | "graph"
  | "authz"
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

export class LaserError extends Error {
  readonly kind: LaserErrorKind

  constructor(message: string, kind: LaserErrorKind, options?: { cause?: unknown }) {
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

export class CancelledError extends LaserError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, "cancelled", options)
  }
}

export class UnsupportedError extends LaserError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, "unsupported", options)
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

export class CodecError extends LaserError {
  readonly surface: string
  readonly operation: string

  constructor(message: string, surface: string, operation: string, options?: { cause?: unknown }) {
    super(message, "codec", options)
    this.surface = surface
    this.operation = operation
  }
}

export class TypedDecodeError extends LaserError {
  // Absent only when the poll itself failed before reaching any record, a
  // decode failure for an actual record always carries its exact position
  // (taken from the record itself, never a batch watermark).
  readonly position: { readonly partitionId: number; readonly offset: bigint } | undefined

  constructor(
    message: string,
    position: { readonly partitionId: number; readonly offset: bigint } | undefined,
    options?: { cause?: unknown }
  ) {
    super(message, "typed-decode", options)
    this.position = position
  }
}

export class ProtocolError extends LaserError {
  readonly resultCode: number | undefined
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

export class AgentWorkflowExecutionError extends LaserError {
  constructor(
    message: string,
    readonly detail: unknown,
    options?: { cause?: unknown }
  ) {
    super(message, "agent-workflow", options)
  }
}

// Why routing an agent message failed: no advertised inbox, no live agent
// advertising the required capability, or a route bound to a principal
// that resolved to a different (or no) live connection.
export type RoutingErrorReason =
  | { readonly kind: "noInbox"; readonly agent: string }
  | { readonly kind: "noCapableAgent"; readonly skill: string }
  | {
      readonly kind: "principalMismatch"
      readonly agent: string
      readonly expected: number
      readonly actual?: number
    }

export class RoutingError extends LaserError {
  constructor(
    message: string,
    readonly reason: RoutingErrorReason,
    options?: { cause?: unknown }
  ) {
    super(message, "routing", options)
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

export function assertNever(value: never): never {
  throw new InvalidError("unreachable variant", { value })
}
