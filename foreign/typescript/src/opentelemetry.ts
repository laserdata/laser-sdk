import type { LaserObserver, ObservationLevel, SpanScope } from "./observe.js"

export interface OpenTelemetrySpan {
  setAttribute(name: string, value: string | number | boolean): this
  setStatus(status: { readonly code: number; readonly message?: string }): this
  addEvent(name: string, attributes?: Readonly<Record<string, string | number | boolean>>): this
  end(): void
}

export interface OpenTelemetryTracer {
  startSpan(
    name: string,
    options?: { readonly attributes?: Readonly<Record<string, string | number | boolean>> }
  ): OpenTelemetrySpan
}

const STATUS_ERROR = 2

function safeAttributes(
  fields: Readonly<Record<string, unknown>>
): Readonly<Record<string, string | number | boolean>> {
  return Object.fromEntries(
    Object.entries(fields).flatMap(([key, value]) =>
      typeof value === "string" || typeof value === "number" || typeof value === "boolean"
        ? [[key, value] as const]
        : typeof value === "bigint"
          ? [[key, value.toString()] as const]
          : []
    )
  )
}

export class OpenTelemetryObserver implements LaserObserver {
  constructor(private readonly tracer: OpenTelemetryTracer) {}

  start(operation: string, attributes: Readonly<Record<string, unknown>>): SpanScope {
    const span = this.tracer.startSpan(operation, { attributes: safeAttributes(attributes) })
    return {
      end(error?: unknown): void {
        if (error !== undefined) {
          span.setStatus({
            code: STATUS_ERROR,
            message:
              error instanceof Error
                ? error.message
                : typeof error === "string"
                  ? error
                  : "operation failed with a non-Error value"
          })
        }
        span.end()
      }
    }
  }

  event(level: ObservationLevel, name: string, fields: Readonly<Record<string, unknown>>): void {
    const span = this.tracer.startSpan("laser.event")
    span.addEvent(name, { level, ...safeAttributes(fields) })
    span.end()
  }
}
