import assert from "node:assert/strict"
import { test } from "node:test"
import { serializePollMessages } from "apache-iggy/dist/wire/message/poll.utils.js"
import { Consumer as IggyConsumer, PollingStrategy } from "apache-iggy"
import {
  ApacheIggyTransport,
  NEVER_EXPIRE,
  decodePolledBody,
  type IggyClient
} from "../../src/iggy/apache-iggy.js"
import { Consumer } from "../../src/stream/consumer.js"

// One stored record: a batch at offset 40 holding one frame with a header block.
function storedRecord(headers: Uint8Array): Uint8Array {
  const payload = new TextEncoder().encode(`{"mode":"safe"}`)
  const frameBytes = 48 + payload.byteLength + headers.byteLength
  const out = new Uint8Array(16 + 256 + frameBytes)
  const view = new DataView(out.buffer)
  view.setUint32(0, 3, true)
  view.setBigUint64(4, 99n, true)
  view.setUint32(12, 1, true)
  view.setBigUint64(16 + 8, 40n, true)
  view.setBigUint64(16 + 16, 1_700_000_000_123_456n, true)
  view.setBigUint64(16 + 24, 1_700_000_000_000_007n, true)
  view.setBigUint64(16 + 32, BigInt(256 + frameBytes), true)
  view.setUint32(16 + 48, 1, true)
  const frame = 16 + 256
  view.setBigUint64(frame, 0xabcdn, true)
  view.setBigUint64(frame + 8, 5n, true)
  view.setBigUint64(frame + 16, 1n, true)
  view.setUint32(frame + 28, 3, true)
  view.setUint32(frame + 32, headers.byteLength, true)
  view.setUint32(frame + 36, payload.byteLength, true)
  out.set(payload, frame + 48)
  out.set(headers, frame + 48 + payload.byteLength)
  return out
}

function stringHeader(key: string, value: string): Uint8Array {
  const keyBytes = new TextEncoder().encode(key)
  const valueBytes = new TextEncoder().encode(value)
  const out = new Uint8Array(10 + keyBytes.byteLength + valueBytes.byteLength)
  const view = new DataView(out.buffer)
  view.setUint8(0, 2)
  view.setUint32(1, keyBytes.byteLength, true)
  out.set(keyBytes, 5)
  view.setUint8(5 + keyBytes.byteLength, 2)
  view.setUint32(6 + keyBytes.byteLength, valueBytes.byteLength, true)
  out.set(valueBytes, 10 + keyBytes.byteLength)
  return out
}

void test("given_a_stored_record_when_decoded_then_should_keep_every_header_field_exactly", () => {
  const block = stringHeader("source", "edge")
  const [message] = decodePolledBody(storedRecord(block))
  assert.ok(message !== undefined)
  assert.equal(message.offset, 40n)
  assert.equal(message.timestampMicros, 1_700_000_000_123_456n)
  assert.equal(message.originTimestampMicros, 1_700_000_000_000_010n)
  assert.equal(message.messageId, (1n << 64n) | 5n)
  assert.equal(message.checksum, 0xabcdn)
  assert.equal(message.currentOffset, 99n)
  assert.deepEqual(message.userHeaders, block)
  assert.deepEqual(message.headers.get("source"), { kind: "string", value: "edge" })
})

void test("given_a_partition_poll_when_sent_then_should_match_the_apache_iggy_request_and_deliver_exact_fields", async () => {
  const requests: { readonly code: number; readonly payload: Uint8Array }[] = []
  const malformed = Uint8Array.of(2, 1, 0, 0, 0)
  const client = {
    clientProvider: () => Promise.resolve({}),
    sendBinaryRequest: (code: number, payload: Buffer) => {
      requests.push({ code, payload: new Uint8Array(payload) })
      return Promise.resolve(Buffer.from(storedRecord(malformed)))
    },
    destroy: () => Promise.resolve()
  } as unknown as IggyClient
  const transport = await ApacheIggyTransport.fromClient(client)
  const consumer = Consumer.create(
    transport,
    "fleet",
    "readings",
    { kind: "single", partitionId: 3, name: "metrics" },
    { commitPolicy: { kind: "disabled" }, startAt: { kind: "offset", value: 40n } }
  )
  const message = await consumer.nextWithin(100)
  const [request] = requests
  assert.ok(request !== undefined)
  assert.equal(request.code, 100)
  assert.deepEqual(
    request.payload,
    new Uint8Array(
      serializePollMessages(
        "fleet",
        "readings",
        { kind: IggyConsumer.Single.kind, id: "metrics" },
        3,
        PollingStrategy.Offset(40n),
        1000,
        false
      )
    )
  )
  assert.deepEqual(message.position, { partitionId: 3, offset: 40n })
  assert.equal(message.messageId, (1n << 64n) | 5n)
  assert.equal(message.timestampMicros, 1_700_000_000_123_456n)
  assert.equal(message.originTimestampMicros, 1_700_000_000_000_010n)
  assert.equal(message.currentOffset, 99n)
  assert.equal(message.headersMalformed, true)
  assert.deepEqual(message.userHeaders, malformed)
  assert.deepEqual(message.json(), { mode: "safe" })
  await consumer.shutdown()
})

void test("given_an_existing_topic_when_ensured_then_should_leave_it_and_create_a_missing_one_without_expiry", async () => {
  const created: { readonly name: string; readonly messageExpiry?: bigint }[] = []
  let updates = 0
  const client = {
    clientProvider: () => Promise.resolve({}),
    topic: {
      get: ({ topicId }: { readonly topicId: string }) =>
        Promise.resolve(topicId === "kept" ? { partitionsCount: 2, messageExpiry: 5n } : null),
      create: (request: { readonly name: string; readonly messageExpiry?: bigint }) => {
        created.push(request)
        return Promise.resolve({ partitionsCount: 1 })
      },
      update: () => {
        updates += 1
        return Promise.resolve()
      }
    },
    destroy: () => Promise.resolve()
  } as unknown as IggyClient
  const transport = await ApacheIggyTransport.fromClient(client)
  await transport.ensureTopicWithExpiry("fleet", "kept", 1, 9n)
  await transport.ensureTopic("fleet", "fresh", 1)
  assert.equal(updates, 0)
  assert.deepEqual(
    created.map((request) => [request.name, request.messageExpiry]),
    [["fresh", NEVER_EXPIRE]]
  )
})

void test("given_a_group_poll_when_assigned_then_should_preserve_exact_times_and_raw_malformed_headers", async () => {
  const requests: number[] = []
  const assignment = new Uint8Array(16)
  const view = new DataView(assignment.buffer)
  view.setBigUint64(0, 1n, true)
  view.setUint32(8, 1, true)
  view.setUint32(12, 3, true)
  const malformed = Uint8Array.of(2, 1, 0, 0, 0)
  const raw = {
    on: () => undefined,
    sendCommand: (code: number) => {
      requests.push(code)
      return Promise.resolve({
        data: Buffer.from(code === 606 ? assignment : storedRecord(malformed))
      })
    }
  }
  const client = {
    clientProvider: () => Promise.resolve(raw),
    destroy: () => Promise.resolve()
  } as unknown as IggyClient
  const transport = await ApacheIggyTransport.fromClient(client)
  const messages = await transport.pollMessages(
    "fleet",
    "readings",
    { kind: "group", name: "metrics" },
    { kind: "next" },
    1000,
    true
  )
  const message = messages[0]
  assert.ok(message !== undefined)
  assert.deepEqual(requests, [606, 100])
  assert.equal(message.timestampMicros, 1_700_000_000_123_456n)
  assert.equal(message.originTimestampMicros, 1_700_000_000_000_010n)
  assert.equal(message.currentOffset, 99n)
  assert.equal(message.messageId, (1n << 64n) | 5n)
  assert.deepEqual(message.userHeaders, malformed)
  assert.equal(message.headersMalformed, "structure")
  await transport.pollMessages(
    "fleet",
    "readings",
    { kind: "group", name: "metrics" },
    { kind: "next" },
    1000,
    true
  )
  assert.deepEqual(requests, [606, 100, 100])
})
