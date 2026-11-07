import assert from "node:assert/strict"
import { test } from "node:test"
import type { Capabilities } from "../../src/client/capabilities.js"
import { managedCapabilitiesFrom, OPEN_CAPABILITIES } from "../../src/client/capabilities.js"
import { isVersionSkew } from "../../src/client/error-classify.js"
import { CancelledError, UnsupportedError } from "../../src/client/errors.js"
import type { LaserTransport, PolledMessage } from "../../src/iggy/apache-iggy.js"
import { type ChangeFeed, Watch } from "../../src/managed/watch.js"
import { Cursor } from "../../src/stream/cursor.js"
import { encodeNamed } from "../../src/wire/cbor.js"
import { encodeChangeRecord } from "../../src/wire/change.js"
import { feature } from "../../src/wire/hello.js"

const CAPS: Capabilities = managedCapabilitiesFrom({
  versions: { query: 1, control: 1, kv: 1, fork: 1, agent: 1, graph: 1, features: feature.WATCH },
  backends: []
})

void test("given_open_capabilities_when_records_is_called_then_should_reject_before_opening_a_cursor", async () => {
  let opened = false
  const watch = Watch.create(
    () => Promise.resolve(OPEN_CAPABILITIES),
    () => {
      opened = true
      return Promise.reject(new Error("should not be called"))
    }
  )
  await assert.rejects(
    () => watch.records(),
    (error: unknown) => {
      assert.ok(error instanceof UnsupportedError)
      assert.equal(error.surface, "watch")
      assert.equal(error.feature, "watch")
      return true
    }
  )
  assert.equal(opened, false)
})

void test("given_a_change_record_of_another_version_when_polled_then_should_refuse_it_as_version_skew", async () => {
  const transport = fakeTransport([[polled(changePayload("readings_v1", 0n, 2), 0n)]])
  const reader = await watchOf(transport).records()
  await assert.rejects(reader.poll(), (error: unknown) => isVersionSkew(error))
  assert.equal(reader.offsets.get(0), 0n, "the reader stays at the newer record")
})

void test("given_changes_before_a_newer_record_when_polled_then_should_return_them_and_stop_there", async () => {
  const transport = fakeTransport([
    [polled(changePayload("readings_v1", 0n), 0n), polled(changePayload("readings_v1", 1n, 2), 1n)],
    [],
    [polled(changePayload("readings_v1", 1n, 2), 1n)],
    []
  ])
  const reader = await watchOf(transport).records()
  const first = await reader.poll()
  assert.deepEqual(
    first.map((watched) => watched.position.offset),
    [0n]
  )
  assert.equal(reader.offsets.get(0), 1n)
  await assert.rejects(reader.poll(), (error: unknown) => isVersionSkew(error))
})

void test("given_watch_capability_when_polled_then_should_carry_the_change_its_message_id_and_position", async () => {
  const transport = fakeTransport([[polled(changePayload("readings_v1", 0n), 0n, 42n)]])
  const reader = await watchOf(transport).records()
  const batch = await reader.poll()
  assert.equal(batch.length, 1)
  const watched = batch[0]
  assert.ok(watched)
  assert.equal(watched.change.index, "readings_v1")
  assert.equal(watched.messageId, 42n)
  assert.deepEqual(watched.position, { partitionId: 0, offset: 0n })
})

void test("given_an_index_filter_when_polled_then_should_keep_only_matching_records", async () => {
  const transport = fakeTransport([
    [polled(changePayload("readings_v1", 0n), 0n), polled(changePayload("hosts_v1", 0n), 1n)]
  ])
  const reader = await watchOf(transport).index("readings_v1").records()
  const batch = await reader.poll()
  assert.deepEqual(
    batch.map((watched) => watched.change.index),
    ["readings_v1"]
  )
  assert.equal(reader.skipped, 0n, "a change for another index is not a skipped record")
})

void test("given_an_undecodable_payload_when_polled_then_should_skip_and_count_it", async () => {
  const transport = fakeTransport([[polled(Uint8Array.of(0xff, 0x00), 0n)]])
  const reader = await watchOf(transport).records()
  assert.deepEqual(await reader.poll(), [])
  assert.equal(reader.skipped, 1n)
})

void test("given_records_retention_removed_when_a_resumed_reader_polls_then_should_count_them_as_missed", async () => {
  const transport = fakeTransport([[polled(changePayload("readings_v1", 7n), 7n)]])
  const reader = (await watchOf(transport).records()).fromOffsets(new Map([[0, 4n]]))
  const batch = await reader.poll()
  assert.equal(batch.length, 1)
  assert.equal(reader.missed, 3n)
})

void test("given_a_reader_from_the_end_when_opened_then_should_start_at_each_partition_tail", async () => {
  const transport = fakeTransport([
    [
      polled(changePayload("readings_v1", 5n), 5n),
      polled(changePayload("readings_v1", 9n, 1, 1), 9n, 0n, 1)
    ]
  ])
  const reader = await watchOf(
    transport,
    new Map([
      [0, 5n],
      [1, 0n]
    ]),
    [0, 1]
  )
    .fromEnd()
    .records()
  assert.equal(reader.offsets.get(0), 5n)
  assert.equal(reader.offsets.get(1), 0n)
  const batch = await reader.poll()
  assert.equal(batch.length, 2)
  assert.equal(reader.missed, 0n, "an empty partition at open has no history to miss")
})

void test("given_nothing_new_when_polling_within_a_timeout_then_should_wait_for_the_first_change", async () => {
  const transport = fakeTransport([[], [], [polled(changePayload("readings_v1", 0n), 0n)]])
  const reader = await watchOf(transport).records()
  const batch = await reader.pollWithin(5_000)
  assert.equal(batch.length, 1)
  assert.deepEqual(await reader.pollWithin(0), [])
})

void test("given_an_aborted_wait_when_polling_within_a_timeout_then_should_raise_cancelled", async () => {
  const reader = await watchOf(fakeTransport([])).records()
  const controller = new AbortController()
  const waiting = reader.pollWithin(5_000, { signal: controller.signal })
  controller.abort()
  await assert.rejects(waiting, CancelledError)
})

void test("given_multiple_pages_when_streamed_then_should_yield_changes_and_stop_once_caught_up", async () => {
  const transport = fakeTransport([
    [polled(changePayload("readings_v1", 0n), 0n)],
    [polled(changePayload("readings_v1", 1n), 1n)],
    []
  ])
  const reader = await watchOf(transport).records()
  const changes = []
  for await (const watched of reader.stream()) changes.push(watched)
  assert.equal(changes.length, 2)
})

void test("given_offsets_when_from_offsets_is_called_then_should_resume_and_expose_them", async () => {
  const reader = await watchOf(fakeTransport([[]])).records()
  reader.fromOffsets(new Map([[0, 5n]]))
  assert.equal(reader.offsets.get(0), 5n)
})

function watchOf(
  transport: LaserTransport,
  tails: ReadonlyMap<number, bigint> = new Map(),
  partitionIds: readonly number[] = [0]
): Watch {
  const feed: ChangeFeed = {
    replay: () => Promise.resolve(Cursor.create(transport, "ops", "changes", partitionIds)),
    tailOffsets: () => Promise.resolve(tails)
  }
  return Watch.create(
    () => Promise.resolve(CAPS),
    () => Promise.resolve(feed)
  )
}

function changePayload(index: string, toOffset: bigint, v = 1, partitionId = 0): Uint8Array {
  return encodeNamed(
    encodeChangeRecord({ v, index, partitionId, fromOffset: toOffset, toOffset, rows: 1 })
  )
}

function polled(
  payload: Uint8Array,
  offset: bigint,
  messageId = 0n,
  partitionId = 0
): PolledMessage {
  return { payload, partitionId, offset, messageId, headers: new Map() }
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
    deleteStream: () => Promise.reject(new Error("unused")),
    ensureTopic: () => Promise.reject(new Error("unused")),
    ensureConsumerGroup: () => Promise.reject(new Error("unused")),
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
