import assert from "node:assert/strict"
import { test } from "node:test"
import type { Capabilities } from "../../src/client/capabilities.js"
import { managedCapabilitiesFrom, OPEN_CAPABILITIES } from "../../src/client/capabilities.js"
import { UnsupportedError } from "../../src/client/errors.js"
import type { LaserTransport, PolledMessage } from "../../src/iggy/apache-iggy.js"
import { Watch } from "../../src/managed/watch.js"
import { Cursor } from "../../src/stream/cursor.js"
import { encodeNamed } from "../../src/wire/cbor.js"
import { encodeChangeRecord } from "../../src/wire/change.js"
import { Feature } from "../../src/wire/hello.js"

const CAPS: Capabilities = managedCapabilitiesFrom({
  versions: { query: 1, control: 1, kv: 1, fork: 1, agent: 1, graph: 1, features: Feature.WATCH },
  backends: []
})

function changeRecordPayload(index: string, fromOffset: bigint, toOffset: bigint): Uint8Array {
  return encodeNamed(
    encodeChangeRecord({ v: 1, index, partitionId: 0, fromOffset, toOffset, rows: 1 })
  )
}

function fakeTransport(pages: readonly (readonly PolledMessage[])[]): LaserTransport {
  let next = 0
  return {
    kind: "apache-iggy",
    get iggyClient(): never {
      throw new Error("unused")
    },
    sendManaged: () => Promise.reject(new Error("unused")),
    ensureStream: () => Promise.reject(new Error("unused")),
    ensureTopic: () => Promise.reject(new Error("unused")),
    findTopicPartitionCount: () => Promise.reject(new Error("unused")),
    getTopicPartitionCount: () => Promise.reject(new Error("unused")),
    sendMessages: () => Promise.reject(new Error("unused")),
    sendMessageWithHeaders: () => Promise.reject(new Error("unused")),
    sendMessagesWithHeaders: () => Promise.reject(new Error("unused")),
    pollMessages(): Promise<readonly PolledMessage[]> {
      const page = pages[next]
      next += 1
      return Promise.resolve(page ?? [])
    },
    storeOffset: () => Promise.reject(new Error("unused")),
    joinConsumerGroup: () => Promise.reject(new Error("unused")),
    leaveConsumerGroup: () => Promise.reject(new Error("unused")),
    close: () => Promise.reject(new Error("unused"))
  }
}

function polled(payload: Uint8Array, offset: bigint): PolledMessage {
  return { payload, partitionId: 0, offset, headers: new Map() }
}

void test("given_open_capabilities_when_records_is_called_then_should_reject_before_opening_a_cursor", async () => {
  let opened = false
  const watch = new Watch(
    () => Promise.resolve(OPEN_CAPABILITIES),
    () => {
      opened = true
      return Promise.reject(new Error("should not be called"))
    }
  )
  await assert.rejects(() => watch.records(), UnsupportedError)
  assert.equal(opened, false)
})

void test("given_watch_capability_when_records_is_called_then_should_open_the_injected_cursor", async () => {
  const transport = fakeTransport([[polled(changeRecordPayload("orders", 0n, 1n), 0n)]])
  const cursor = new Cursor(transport, "ops", "changes", [0])
  const watch = new Watch(
    () => Promise.resolve(CAPS),
    () => Promise.resolve(cursor)
  )
  const reader = await watch.records()
  const batch = await reader.poll()
  assert.equal(batch.length, 1)
  assert.equal(batch[0]?.index, "orders")
})

void test("given_an_index_filter_when_polled_then_should_keep_only_matching_records", async () => {
  const transport = fakeTransport([
    [
      polled(changeRecordPayload("orders", 0n, 1n), 0n),
      polled(changeRecordPayload("customers", 0n, 1n), 1n)
    ]
  ])
  const cursor = new Cursor(transport, "ops", "changes", [0])
  const watch = new Watch(
    () => Promise.resolve(CAPS),
    () => Promise.resolve(cursor)
  ).index("orders")
  const reader = await watch.records()
  const batch = await reader.poll()
  assert.deepEqual(
    batch.map((record) => record.index),
    ["orders"]
  )
})

void test("given_an_undecodable_payload_when_polled_then_should_skip_it", async () => {
  const transport = fakeTransport([[polled(Uint8Array.of(0xff, 0x00), 0n)]])
  const cursor = new Cursor(transport, "ops", "changes", [0])
  const watch = new Watch(
    () => Promise.resolve(CAPS),
    () => Promise.resolve(cursor)
  )
  const reader = await watch.records()
  assert.deepEqual(await reader.poll(), [])
})

void test("given_multiple_pages_when_streamed_then_should_yield_records_and_stop_once_caught_up", async () => {
  const transport = fakeTransport([
    [polled(changeRecordPayload("orders", 0n, 1n), 0n)],
    [polled(changeRecordPayload("orders", 1n, 2n), 1n)],
    []
  ])
  const cursor = new Cursor(transport, "ops", "changes", [0])
  const watch = new Watch(
    () => Promise.resolve(CAPS),
    () => Promise.resolve(cursor)
  )
  const reader = await watch.records()
  const records = []
  for await (const record of reader.stream()) records.push(record)
  assert.equal(records.length, 2)
})

void test("given_offsets_when_from_offsets_is_called_then_should_resume_and_expose_them", async () => {
  const transport = fakeTransport([[]])
  const cursor = new Cursor(transport, "ops", "changes", [0])
  const watch = new Watch(
    () => Promise.resolve(CAPS),
    () => Promise.resolve(cursor)
  )
  const reader = await watch.records()
  reader.fromOffsets(new Map([[0, 5n]]))
  assert.equal(reader.offsets.get(0), 5n)
})
