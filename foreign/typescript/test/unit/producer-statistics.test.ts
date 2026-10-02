import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import type { LaserTransport } from "../../src/iggy/apache-iggy.js"
import { ProducerRecorder, closeProducerStatistics } from "../../src/stream/producer-statistics.js"
import { AGDX_HELLO_CODE, AGDX_SET_CLIENT_METADATA_CODE } from "../../src/wire/codes.js"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"
import { encodeHelloReply, newOpVersions } from "../../src/wire/hello.js"

const transport = {} as LaserTransport

void test("given_completed_calls_when_recorded_then_counts_and_histogram_are_bounded", () => {
  const recorder = new ProducerRecorder("stream", "topic", transport)
  recorder.begin(3, 12)(true)
  recorder.begin(2, 8)(false)
  const snapshot = recorder.snapshot()
  assert.equal(snapshot.get("submitted_records"), 5n)
  assert.equal(snapshot.get("confirmed_records"), 3n)
  assert.equal(snapshot.get("failed_calls"), 1n)
  assert.equal(snapshot.get("retries"), null)
  assert.equal(expectMap(snapshot.get("latency"), "latency").get("p999_micros"), null)
  for (let i = 0; i < 1200; i += 1) recorder.begin(1, 2)(true)
  assert.equal(recorder.samples.length, 64)
  assert.equal(recorder.sampleCount, 1202n)
  assert.notEqual(expectMap(recorder.snapshot().get("latency"), "latency").get("p999_micros"), null)
})

void test("given_shared_transport_when_reporting_then_one_observer_preserves_all_handles_and_closes", async (context) => {
  context.mock.timers.enable({ apis: ["setTimeout"] })
  let opened = 0
  let closed = 0
  const reports: Uint8Array[] = []
  const telemetry = {
    connectsNodes: true,
    openCoordinator: () => {
      opened += 1
      return Promise.resolve({
        send: (code: number, payload: Uint8Array) => {
          if (code === AGDX_HELLO_CODE)
            return Promise.resolve(encodeHelloReply({ versions: newOpVersions(1, 1, 1, 1) }))
          assert.equal(code, AGDX_SET_CLIENT_METADATA_CODE)
          reports.push(payload)
          return Promise.resolve(new Uint8Array())
        },
        close: () => {
          closed += 1
          return Promise.resolve()
        }
      })
    }
  } as unknown as LaserTransport
  const first = new ProducerRecorder("stream", "first", telemetry)
  const second = new ProducerRecorder("stream", "second", telemetry)
  first.begin(1, 4)(true)
  second.begin(2, 8)(true)
  context.mock.timers.tick(10_000)
  for (let i = 0; i < 20; i += 1) await Promise.resolve()
  assert.equal(opened, 1)
  assert.equal(reports.length, 1)
  const report = reports[0]
  assert.ok(report)
  const body = expectMap(decodeOne(report, "presence"), "presence")
  assert.equal((body.get("producers") as unknown[]).length, 2)
  const extra = Array.from({ length: 38 }, () => new ProducerRecorder("stream", "extra", telemetry))
  for (const handle of extra) handle.begin(1, 1)(true)
  context.mock.timers.tick(10_000)
  for (let i = 0; i < 20; i += 1) await Promise.resolve()
  const boundedReport = reports[1]
  assert.ok(boundedReport)
  assert.equal(
    (expectMap(decodeOne(boundedReport, "presence"), "presence").get("producers") as unknown[])
      .length,
    32
  )
  assert.equal(opened, 1)
  assert.ok(extra.every((handle) => handle.snapshot().get("submitted_records") === 1n))
  await closeProducerStatistics(telemetry)
  context.mock.timers.tick(20_000)
  assert.equal(closed, 1)
  assert.equal(opened, 1)
})

void test("given_the_rust_presence_fixture_when_encoded_by_the_typescript_recorder_then_bytes_match", async () => {
  const bytes = await readFile(
    path.resolve(process.cwd(), "../../wire/fixtures/producer_presence.bin")
  )
  const recorder = new ProducerRecorder("stream", "topic", transport)
  recorder.begin(3, 12)(true)
  recorder.begin(2, 8)(false)
  const statistics = new Map(recorder.snapshot())
  statistics.set("instance_id", "producer-1")
  statistics.set("first_activity_millis", 1)
  statistics.set("last_activity_millis", 900)
  statistics.set("last_success_millis", 800)
  statistics.set(
    "latency",
    new Map<string, unknown>([
      ["samples", 2n],
      ["p50_micros", 32n],
      ["p99_micros", null],
      ["p999_micros", null]
    ])
  )
  const encoded = encodeNamed(
    new Map<string, unknown>([
      ["producer_presence_version", 1],
      ["observed_at_millis", 1000],
      ["expires_after_millis", 30_000],
      ["producers", [statistics]]
    ])
  )
  assert.deepEqual(Buffer.from(encoded), bytes)
})
