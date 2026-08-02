import assert from "node:assert/strict"
import { test } from "node:test"
import { OpenTelemetryObserver, type OpenTelemetrySpan } from "../../src/opentelemetry.js"

void test("given_an_observer_when_spans_and_events_are_recorded_then_should_lower_safe_attributes", () => {
  const calls: unknown[] = []
  const span: OpenTelemetrySpan = {
    setAttribute(name, value) {
      calls.push(["attribute", name, value])
      return this
    },
    setStatus(status) {
      calls.push(["status", status])
      return this
    },
    addEvent(name, attributes) {
      calls.push(["event", name, attributes])
      return this
    },
    end() {
      calls.push(["end"])
    }
  }
  const observer = new OpenTelemetryObserver({
    startSpan(name, options) {
      calls.push(["start", name, options])
      return span
    }
  })
  observer
    .start("laser.query", { commandCode: 700n, secret: new Uint8Array([1]) })
    .end(new Error("query failed"))
  observer.event("warn", "agent.retry", { attempt: 2, conversation: 9n })
  assert.deepEqual(calls, [
    ["start", "laser.query", { attributes: { commandCode: "700" } }],
    ["status", { code: 2, message: "query failed" }],
    ["end"],
    ["start", "laser.event", undefined],
    ["event", "agent.retry", { level: "warn", attempt: 2, conversation: "9" }],
    ["end"]
  ])
})
