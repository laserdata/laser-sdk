import assert from "node:assert/strict"
import { test } from "node:test"
import { serializeDeferredPollOptions } from "apache-iggy/dist/wire/message/deferred-poll.utils.js"
import { serializePollMessages } from "apache-iggy/dist/wire/message/poll.utils.js"
import {
  Consumer as IggyConsumer,
  LaneBlockedError,
  NotConnectedError,
  OperationNotRetainedError,
  OperationUncertainError,
  PollingStrategy,
  VsrResponseTimeoutError
} from "apache-iggy"
import { CancelledError, TransportError } from "../../src/client/errors.js"
import {
  ApacheIggyTransport,
  NEVER_EXPIRE,
  decodePolledBody,
  type IggyClient
} from "../../src/iggy/apache-iggy.js"
import { Consumer } from "../../src/stream/consumer.js"

// A client whose raw link answers each command through `answer`.
function rawClient(answer: (code: number, payload: Uint8Array) => Promise<Uint8Array>) {
  const requests: { readonly code: number; readonly payload: Uint8Array }[] = []
  const abandoned: bigint[] = []
  let destroyed = 0
  const raw = {
    on: () => undefined,
    sendCommand: async (code: number, payload: Buffer) => {
      requests.push({ code, payload: new Uint8Array(payload) })
      const data = Buffer.from(await answer(code, new Uint8Array(payload)))
      return { status: 0, length: data.byteLength, data }
    }
  }
  const client = {
    clientProvider: () => Promise.resolve(raw),
    offset: { store: () => Promise.resolve() },
    abandonOperation: (operationId: bigint) => {
      abandoned.push(operationId)
      return Promise.resolve()
    },
    destroy: () => {
      destroyed += 1
      return Promise.resolve()
    }
  } as unknown as IggyClient
  return { client, requests, abandoned, destroyed: () => destroyed }
}

// The body of a plain poll followed by the deferred poll options, as Apache Iggy lays out 106.
function deferredPoll(
  poll: Buffer,
  options: { readonly maxWait: number; readonly minCount: number; readonly maxBytes: number }
): Uint8Array {
  return new Uint8Array(
    Buffer.concat([
      poll,
      serializeDeferredPollOptions({ ...options, requestTimeout: options.maxWait + 10_000 })
    ])
  )
}

// The polled head: partition, current offset, count, then the partition context.
const HEAD = 40

// One stored record: a batch at offset 40 holding one frame with a header block.
function storedRecord(headers: Uint8Array): Uint8Array {
  const payload = new TextEncoder().encode(`{"mode":"safe"}`)
  const frameBytes = 48 + payload.byteLength + headers.byteLength
  const out = new Uint8Array(HEAD + 256 + frameBytes)
  const view = new DataView(out.buffer)
  view.setUint32(0, 3, true)
  view.setBigUint64(4, 99n, true)
  view.setUint32(12, 1, true)
  view.setBigUint64(16, 17n, true)
  view.setBigUint64(24, 4n, true)
  view.setBigUint64(32, 40n, true)
  view.setBigUint64(HEAD + 8, 40n, true)
  view.setBigUint64(HEAD + 16, 1_700_000_000_123_456n, true)
  view.setBigUint64(HEAD + 24, 1_700_000_000_000_007n, true)
  view.setBigUint64(HEAD + 32, BigInt(256 + frameBytes), true)
  view.setUint32(HEAD + 48, 1, true)
  const frame = HEAD + 256
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

void test("given_a_partition_poll_when_sent_then_should_match_the_apache_iggy_deferred_request_and_deliver_exact_fields", async () => {
  const malformed = Uint8Array.of(2, 1, 0, 0, 0)
  const fake = rawClient(() => Promise.resolve(storedRecord(malformed)))
  const transport = await ApacheIggyTransport.fromClient(fake.client)
  const consumer = Consumer.create(
    transport,
    "fleet",
    "readings",
    { kind: "single", partitionId: 3, name: "metrics" },
    {
      commitPolicy: { kind: "disabled" },
      startAt: { kind: "offset", value: 40n },
      minCount: 10,
      maxBytes: 4096
    }
  )
  const message = await consumer.nextWithin(100)
  const [request] = fake.requests
  assert.ok(request !== undefined)
  assert.equal(request.code, 106)
  assert.deepEqual(
    request.payload,
    deferredPoll(
      serializePollMessages(
        "fleet",
        "readings",
        { kind: IggyConsumer.Single.kind, id: "metrics" },
        3,
        PollingStrategy.Offset(40n),
        1000,
        false
      ),
      { maxWait: 5_000, minCount: 10, maxBytes: 4096 }
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

void test("given_a_one_shot_read_when_polled_then_should_answer_at_once_under_the_reply_byte_cap", async () => {
  const fake = rawClient(() => Promise.resolve(new Uint8Array()))
  const transport = await ApacheIggyTransport.fromClient(fake.client)
  assert.deepEqual(
    await transport.pollMessages(
      "fleet",
      "readings",
      { kind: "single", partitionId: 0 },
      { kind: "first" },
      10,
      false
    ),
    []
  )
  const [request] = fake.requests
  assert.equal(request?.code, 106)
  assert.deepEqual(
    request.payload,
    deferredPoll(
      serializePollMessages(
        "fleet",
        "readings",
        IggyConsumer.Single,
        0,
        PollingStrategy.First,
        10,
        false
      ),
      { maxWait: 0, minCount: 1, maxBytes: 1_048_576 }
    )
  )
})

void test("given_an_uncertain_poll_outcome_when_polled_then_should_fail_without_reconnecting_or_resending", async () => {
  for (const failure of [
    new OperationUncertainError(5n, new Error("reply lost")),
    new OperationNotRetainedError(5n),
    new VsrResponseTimeoutError(15_000)
  ]) {
    const fake = rawClient(() => Promise.reject(failure))
    const transport = await ApacheIggyTransport.fromClient(fake.client)
    const poll = () =>
      transport.pollMessages(
        "fleet",
        "readings",
        { kind: "single", partitionId: 0, name: "metrics" },
        { kind: "next" },
        10,
        true
      )
    await assert.rejects(poll(), (error: unknown) => error instanceof TransportError)
    assert.equal(fake.requests.length, 1, `${failure.name} is never resent by the transport`)
    assert.equal(fake.destroyed(), 0, `${failure.name} keeps the connection`)
    await assert.rejects(poll(), TransportError)
    assert.equal(fake.requests.length, 2, `${failure.name} leaves the client usable`)
    assert.equal(fake.destroyed(), 0)
  }
})

void test("given_no_reachable_node_when_polled_then_should_report_a_transient_failure_and_keep_the_client", async () => {
  let reachable = false
  const fake = rawClient(() =>
    reachable ? Promise.resolve(new Uint8Array()) : Promise.reject(new NotConnectedError())
  )
  const transport = await ApacheIggyTransport.fromClient(fake.client)
  const poll = () =>
    transport.pollMessages(
      "fleet",
      "readings",
      { kind: "single", partitionId: 0 },
      { kind: "first" },
      1,
      false
    )
  await assert.rejects(
    poll(),
    (error: unknown) => error instanceof TransportError && error.retryable
  )
  reachable = true
  assert.deepEqual(await poll(), [], "the client dials again on its own")
  assert.equal(fake.destroyed(), 0)
})

void test("given_a_cancelled_held_read_when_polled_then_should_report_the_cancel_and_keep_the_client", async () => {
  const cancel = new AbortController()
  let parked: () => void = () => undefined
  const holding = new Promise<void>((resolve) => {
    parked = resolve
  })
  const fake = rawClient(() => {
    if (cancel.signal.aborted) return Promise.resolve(new Uint8Array())
    parked()
    return new Promise((_, reject) => {
      cancel.signal.addEventListener("abort", () => {
        reject(new DOMException("This operation was aborted", "AbortError"))
      })
    })
  })
  const transport = await ApacheIggyTransport.fromClient(fake.client)
  const poll = (signal?: AbortSignal) =>
    transport.pollMessages(
      "fleet",
      "readings",
      { kind: "single", partitionId: 0 },
      { kind: "offset", value: 0n },
      10,
      false,
      { maxWaitMs: 5_000, minCount: 1, maxBytes: 1_048_576 },
      signal
    )
  const held = poll(cancel.signal)
  await holding
  cancel.abort()
  await assert.rejects(held, CancelledError)
  assert.equal(fake.destroyed(), 0, "a cancel says nothing about the link")
  assert.deepEqual(await poll(), [], "the next poll runs on the same client")
  assert.equal(fake.requests.length, 2)
})

void test("given_a_blocked_partition_lane_when_consumed_then_should_abandon_the_operation_and_resume_after_the_stored_offset", async () => {
  let polls = 0
  const fake = rawClient(() => {
    polls += 1
    return polls === 1
      ? Promise.reject(new LaneBlockedError(9n, 3, true))
      : Promise.resolve(storedRecord(new Uint8Array()))
  })
  const transport = await ApacheIggyTransport.fromClient(fake.client)
  const consumer = Consumer.create(
    transport,
    "fleet",
    "readings",
    { kind: "single", partitionId: 3, name: "metrics" },
    { startAt: { kind: "first" }, pollingRetryIntervalMs: 1 }
  )
  const message = await consumer.nextWithin(1_000)
  assert.equal(message.position.offset, 40n)
  assert.deepEqual(fake.abandoned, [9n], "the uncertain read is abandoned, never replayed")
  const [first, second] = fake.requests
  const strategyAt = (payload: Uint8Array | undefined): number =>
    payload === undefined ? -1 : (payload[payload.byteLength - 24 - 14] ?? -1)
  assert.equal(strategyAt(first?.payload), PollingStrategy.First.kind)
  assert.equal(
    strategyAt(second?.payload),
    PollingStrategy.Next.kind,
    "the partition resumes after its stored offset"
  )
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
      const data = Buffer.from(code === 606 ? assignment : storedRecord(malformed))
      return Promise.resolve({ status: 0, length: data.byteLength, data })
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
  assert.deepEqual(requests, [606, 106])
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
  assert.deepEqual(requests, [606, 106, 106])
})
