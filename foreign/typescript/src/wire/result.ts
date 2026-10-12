export const ResultCodeName = {
  Ok: 0,
  Unsupported: 1,
  NotFound: 2,
  InvalidArgument: 3,
  TooLarge: 4,
  Conflict: 5,
  Stale: 6,
  VersionSkew: 7,
  Unauthenticated: 8,
  Backend: 9,
  Forbidden: 10,
  StepUpRequired: 11,
  Unavailable: 12,
  ResourceLimit: 13,
  Cancelled: 14,
  DeadlineExceeded: 15,
  ExpiredSnapshot: 16,
  StaleGeneration: 17,
  TargetUnavailable: 18,
  Pending: 19,
  PlaneUnavailable: 20
} as const

export type ResultCode =
  | { readonly kind: "known"; readonly name: keyof typeof ResultCodeName }
  | { readonly kind: "unrecognized"; readonly code: number }

export interface CommandError {
  readonly code: ResultCode
  readonly message: string
}

const NAME_BY_CODE: ReadonlyMap<number, keyof typeof ResultCodeName> = new Map(
  Object.entries(ResultCodeName).map(([name, code]) => [code, name as keyof typeof ResultCodeName])
)

export function resultCodeFromCode(code: number): ResultCode {
  const name = NAME_BY_CODE.get(code)
  return name === undefined ? { kind: "unrecognized", code } : { kind: "known", name }
}

export function resultCodeToCode(value: ResultCode): number {
  return value.kind === "known" ? ResultCodeName[value.name] : value.code
}

export function resultCodeHttpStatus(value: ResultCode): number {
  if (value.kind === "unrecognized") return 500
  switch (value.name) {
    case "Ok":
      return 200
    case "Unsupported":
      return 501
    case "NotFound":
      return 404
    case "InvalidArgument":
      return 400
    case "TooLarge":
      return 413
    case "Conflict":
      return 409
    case "Stale":
      return 503
    case "VersionSkew":
      return 400
    case "Unauthenticated":
      return 401
    case "Backend":
      return 502
    case "Forbidden":
    case "StepUpRequired":
      return 403
    // Retry-after territory, the same status a lagging read model gets.
    case "Unavailable":
    case "TargetUnavailable":
      return 503
    // The outcome is still being decided, so the same request comes back
    // later. It is not generically retryable, see `resultCodeIsPending`.
    case "Pending":
      return 503
    // The node's own plane did not answer. Another node may serve the request.
    case "PlaneUnavailable":
      return 503
    case "ResourceLimit":
      return 429
    case "Cancelled":
    case "StaleGeneration":
      return 409
    case "DeadlineExceeded":
      return 408
    case "ExpiredSnapshot":
      return 410
  }
}

/**
 * Whether a caller may retry the identical request and reasonably expect a
 * different outcome. True only for the two transient classes: the store was
 * momentarily unreachable, or the read model had not caught up yet. Every other
 * code needs the request, the credential, or the data to change first, so
 * retrying it unchanged only wastes the attempt.
 */
export function resultCodeIsRetryable(value: ResultCode): boolean {
  return (
    value.kind === "known" &&
    (value.name === "Unavailable" ||
      value.name === "Stale" ||
      value.name === "TargetUnavailable" ||
      value.name === "PlaneUnavailable")
  )
}

/**
 * Whether the mutation was accepted and its outcome is still unknown. Only the
 * exact request with the same operation identity may be repeated, as in Rust
 * `is_pending`.
 */
export function resultCodeIsPending(value: ResultCode): boolean {
  return value.kind === "known" && value.name === "Pending"
}

/**
 * Whether the streaming server could not reach its own plane, or the plane did
 * not answer in time. The request may still apply, so a mutation is repeated
 * under the same operation identity, possibly on another node, as in Rust
 * `is_plane_unavailable`.
 */
export function resultCodeIsPlaneUnavailable(value: ResultCode): boolean {
  return value.kind === "known" && value.name === "PlaneUnavailable"
}

const RESULT_NAMES: ReadonlyMap<string, ResultCode> = new Map([
  ["ok", { kind: "known", name: "Ok" }],
  ["unsupported", { kind: "known", name: "Unsupported" }],
  ["not_found", { kind: "known", name: "NotFound" }],
  ["invalid_argument", { kind: "known", name: "InvalidArgument" }],
  ["too_large", { kind: "known", name: "TooLarge" }],
  ["conflict", { kind: "known", name: "Conflict" }],
  ["stale", { kind: "known", name: "Stale" }],
  ["version_skew", { kind: "known", name: "VersionSkew" }],
  ["unauthenticated", { kind: "known", name: "Unauthenticated" }],
  ["backend", { kind: "known", name: "Backend" }],
  ["forbidden", { kind: "known", name: "Forbidden" }],
  ["step_up_required", { kind: "known", name: "StepUpRequired" }],
  ["unavailable", { kind: "known", name: "Unavailable" }],
  ["resource_limit", { kind: "known", name: "ResourceLimit" }],
  ["cancelled", { kind: "known", name: "Cancelled" }],
  ["deadline_exceeded", { kind: "known", name: "DeadlineExceeded" }],
  ["expired_snapshot", { kind: "known", name: "ExpiredSnapshot" }],
  ["stale_generation", { kind: "known", name: "StaleGeneration" }],
  ["target_unavailable", { kind: "known", name: "TargetUnavailable" }],
  ["pending", { kind: "known", name: "Pending" }],
  ["plane_unavailable", { kind: "known", name: "PlaneUnavailable" }]
])

const RESULT_WORDS: Readonly<Record<string, string>> = {
  Ok: "ok",
  Unsupported: "unsupported",
  NotFound: "not_found",
  InvalidArgument: "invalid_argument",
  TooLarge: "too_large",
  Conflict: "conflict",
  Stale: "stale",
  VersionSkew: "version_skew",
  Unauthenticated: "unauthenticated",
  Backend: "backend",
  Forbidden: "forbidden",
  StepUpRequired: "step_up_required",
  Unavailable: "unavailable",
  ResourceLimit: "resource_limit",
  Cancelled: "cancelled",
  DeadlineExceeded: "deadline_exceeded",
  ExpiredSnapshot: "expired_snapshot",
  StaleGeneration: "stale_generation",
  TargetUnavailable: "target_unavailable",
  Pending: "pending",
  PlaneUnavailable: "plane_unavailable"
}

/** The code a snake_case wire word names, `undefined` for an unknown word. */
export function resultCodeFromWord(word: string): ResultCode | undefined {
  return RESULT_NAMES.get(word)
}

/** The snake_case wire word of a known code, `undefined` for an unrecognized one. */
export function resultCodeWord(value: ResultCode): string | undefined {
  return value.kind === "known" ? RESULT_WORDS[value.name] : undefined
}
