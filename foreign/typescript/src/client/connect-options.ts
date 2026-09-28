import { ConfigError } from "./errors.js"

export interface ConnectOptions {
  readonly timeoutMs: number
}

export function connectOptions(
  options: Partial<ConnectOptions> = {},
  environment: Readonly<Record<string, string | undefined>> = process.env
): ConnectOptions {
  const raw = environment["LASER_CONNECT_TIMEOUT_MS"]
  let timeoutMs = options.timeoutMs
  if (timeoutMs === undefined && raw !== undefined) {
    if (!/^\d+$/.test(raw)) throw new ConfigError("LASER_CONNECT_TIMEOUT_MS must be an integer")
    timeoutMs = Number(raw)
  }
  const resolved = { timeoutMs: timeoutMs ?? 30_000 }
  if (
    !Number.isSafeInteger(resolved.timeoutMs) ||
    resolved.timeoutMs < 1 ||
    resolved.timeoutMs > 0x7fff_ffff
  ) {
    throw new ConfigError("timeoutMs must be an integer between 1 and 2147483647")
  }
  return resolved
}
