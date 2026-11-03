import { InvalidError } from "./errors.js"

/**
 * Checks a relative duration in milliseconds, the SDK-wide unit. It must be
 * finite and not negative. Fractions of a millisecond are kept.
 * @internal
 */
export function millis(ms: number, name: string): number {
  if (!Number.isFinite(ms) || ms < 0) {
    throw new InvalidError(`${name} must be a non-negative finite number of milliseconds`)
  }
  return ms
}

/**
 * A relative duration in milliseconds as the wire's whole microseconds, the
 * rule Python's `duration_ms` follows. The value is kept to the nanosecond,
 * then truncated to the microsecond, so 1.1 is 1100 and 0.0015 is 1.
 * @internal
 */
export function millisToMicros(ms: number): bigint {
  return BigInt(Math.trunc(Math.round(ms * 1_000_000) / 1_000))
}

/**
 * Wire microseconds as a relative duration in milliseconds, fractions kept,
 * the way Python reports `granted_ttl_ms`.
 * @internal
 */
export function microsToMillis(micros: bigint): number {
  return Number(micros) / 1_000
}
