import { ConfigError, TimeoutError } from "./errors.js"

export interface PublishOptions {
  readonly timeoutMs: number
  readonly maxRetries: number
  readonly retryBackoffMs: number
}

export function publishOptions(
  options: Partial<PublishOptions> = {},
  environment: Readonly<Record<string, string | undefined>> = process.env
): PublishOptions {
  const number = (explicit: number | undefined, name: string, fallback: number): number => {
    const raw = environment[name]
    if (explicit !== undefined) return explicit
    if (raw === undefined) return fallback
    if (!/^\d+$/.test(raw)) throw new ConfigError(`${name} must be an integer`)
    return Number(raw)
  }
  const resolved = {
    timeoutMs: number(options.timeoutMs, "LASER_PUBLISH_TIMEOUT_MS", 60_000),
    maxRetries: number(options.maxRetries, "LASER_PUBLISH_MAX_RETRIES", 3),
    retryBackoffMs: number(options.retryBackoffMs, "LASER_PUBLISH_RETRY_BACKOFF_MS", 250)
  }
  for (const [name, value] of Object.entries(resolved)) {
    const minimum = name === "maxRetries" ? 0 : 1
    const maximum = name === "maxRetries" ? 0xffff_ffff : 0x7fff_ffff
    if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
      throw new ConfigError(
        `${name} must be an integer between ${String(minimum)} and ${String(maximum)}`
      )
    }
  }
  return resolved
}

export async function publishWithin<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => {
          reject(new TimeoutError("Iggy publish response"))
        }, timeoutMs)
      })
    ])
  } finally {
    clearTimeout(timer)
  }
}
