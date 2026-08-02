export type ObservationLevel = "debug" | "info" | "warn" | "error"

export interface SpanScope {
  end(error?: unknown): void
}

export interface LaserObserver {
  start(operation: string, attributes: Readonly<Record<string, unknown>>): SpanScope
  event(level: ObservationLevel, name: string, fields: Readonly<Record<string, unknown>>): void
}

export const NOOP_OBSERVER: LaserObserver = Object.freeze({
  start: () => ({ end: () => undefined }),
  event: () => undefined
})
