import assert from "node:assert/strict"
import { test } from "node:test"
import { serializePollMessages } from "apache-iggy/dist/wire/message/poll.utils.js"
import { Consumer as IggyConsumer, PollingStrategy } from "apache-iggy"

import { CodecError, InvalidError } from "../../src/client/errors.js"
import {
  ApacheIggyTransport,
  NEVER_EXPIRE,
  type IggyClient,
  type LaserTransport,
  type MessageWithHeaders,
  type PolledMessage,
  type TopicCreateSettings
} from "../../src/iggy/apache-iggy.js"
import { Consumer } from "../../src/stream/consumer.js"
import { Cursor } from "../../src/stream/cursor.js"
import { HeaderValue } from "../../src/stream/header-value.js"
import { Producer, ProducerMessage } from "../../src/stream/producer.js"
import { BatchPublishRequest } from "../../src/stream/publish.js"
import { Record, recordHeaders } from "../../src/stream/record.js"
import { Routing } from "../../src/stream/routing.js"
import type { Topic } from "../../src/stream/topic.js"
import { Msgpack } from "../../src/stream/codecs.js"
import { ContentType, contentTypeCode } from "../../src/wire/content.js"
import { CONTENT_TYPE, PROJECTION_REF } from "../../src/wire/headers.js"

interface SentRecord {
  readonly payload: Uint8Array
  readonly headers: ReadonlyMap<string, unknown>
}

function recordingTopic(): { topic: Topic; sent: SentRecord[][] } {
  const sent: SentRecord[][] = []
  const topic = {
    sendRecords(records: readonly SentRecord[]) {
      sent.push([...records])
      return Promise.resolve({ confirmations: [] })
    }
  } as unknown as Topic
  return { topic, sent }
}

function recordingProducerTransport(): {
  transport: LaserTransport
  sent: (readonly MessageWithHeaders[])[]
  created: TopicCreateSettings[]
} {
  const sent: (readonly MessageWithHeaders[])[] = []
  const created: TopicCreateSettings[] = []
  const transport = {
    ensureStream: () => Promise.resolve(),
    createTopicIfAbsent: (
      _stream: string,
      _topic: string,
      _partitions: number,
      settings: TopicCreateSettings
    ) => {
      created.push(settings)
      return Promise.resolve()
    },
    sendMessagesWithHeaders: (
      _stream: string,
      _topic: string,
      messages: readonly MessageWithHeaders[]
    ) => {
      sent.push(messages)
      return Promise.resolve({ confirmations: [] })
    }
  } as unknown as LaserTransport
  return { transport, sent, created }
}

const messagePack = new Msgpack()

void test("given_msgpack_values_when_extend_msgpack_then_should_queue_one_msgpack_record_each", async () => {
  const { topic, sent } = recordingTopic()
  await BatchPublishRequest.create(topic)
    .extendMsgpack([{ host: 1 }, { host: 2 }])
    .send()
  const [records] = sent
  assert.ok(records !== undefined)
  assert.deepEqual(
    records.map((record) => record.payload),
    [messagePack.encode({ host: 1 }), messagePack.encode({ host: 2 })]
  )
  for (const record of records) {
    assert.deepEqual(record.headers.get(CONTENT_TYPE), {
      kind: "uint8",
      value: contentTypeCode(ContentType.Msgpack)
    })
  }
})

void test("given_a_projection_when_add_msgpack_with_projection_then_should_stamp_the_projection_and_msgpack", async () => {
  const { topic, sent } = recordingTopic()
  await BatchPublishRequest.create(topic).addMsgpackWithProjection("reading.v1", { host: 7 }).send()
  const record = sent[0]?.[0]
  assert.ok(record !== undefined)
  assert.deepEqual(record.payload, messagePack.encode({ host: 7 }))
  assert.deepEqual(record.headers.get(PROJECTION_REF), { kind: "string", value: "reading.v1" })
  assert.deepEqual(record.headers.get(CONTENT_TYPE), {
    kind: "uint8",
    value: contentTypeCode(ContentType.Msgpack)
  })
})

void test("given_record_metadata_when_lowered_then_should_stamp_a_ride_along_string_header", () => {
  const headers = recordHeaders(new Record().metadata("trace", "abc"))
  assert.deepEqual(headers.get("trace"), { kind: "string", value: "abc" })
})

void test("given_a_producer_message_when_headers_set_then_should_add_replace_and_send_them", async () => {
  const message = new ProducerMessage(new Uint8Array([1]))
    .withHeaders(new Map([["source", HeaderValue.string("edge")]]))
    .header("kind", HeaderValue.uint16(7))
    .header("source", HeaderValue.string("core"))
  assert.deepEqual(
    message.headers,
    new Map([
      ["source", HeaderValue.string("core")],
      ["kind", HeaderValue.uint16(7)]
    ])
  )
  const { transport, sent } = recordingProducerTransport()
  const producer = Producer.create(transport, "fleet", "readings", {
    createStream: false,
    createTopic: false
  })
  await producer.sendMessage(message)
  const lowered = sent[0]?.[0]
  assert.ok(lowered !== undefined)
  assert.deepEqual(lowered.headers, message.headers)
  assert.deepEqual(lowered.payload, new Uint8Array([1]))
})

void test("given_a_producer_message_when_payload_mutated_after_construction_then_should_keep_its_own_copy", () => {
  const payload = new Uint8Array([1, 2])
  const message = new ProducerMessage(payload)
  payload[0] = 9
  assert.deepEqual(message.payload, new Uint8Array([1, 2]))
})

void test("given_a_key_when_routing_key_then_should_route_by_a_copy_of_its_bytes", () => {
  const key = new Uint8Array([1, 2])
  const routing = Routing.key(key)
  key[0] = 9
  assert.deepEqual(routing, { kind: "key", key: new Uint8Array([1, 2]) })
})

void test("given_a_partition_when_routing_partition_then_should_send_to_that_partition", async () => {
  const { transport, sent } = recordingProducerTransport()
  const partitions: (number | undefined)[] = []
  const recording: LaserTransport = {
    ...transport,
    sendMessagesWithHeaders: (
      stream: string,
      topic: string,
      messages: readonly MessageWithHeaders[],
      _key: Uint8Array | undefined,
      partition: number | undefined
    ) => {
      partitions.push(partition)
      return transport.sendMessagesWithHeaders(stream, topic, messages)
    }
  }
  const producer = Producer.create(recording, "fleet", "readings", {
    createStream: false,
    createTopic: false,
    routing: Routing.partition(2)
  })
  await producer.send(new Uint8Array([1]))
  await producer.sendWithRouting(new ProducerMessage(new Uint8Array([2])), Routing.balanced)
  assert.deepEqual(partitions, [2, undefined])
  assert.equal(sent.length, 2)
  assert.deepEqual(Routing.balanced, { kind: "balanced" })
})

void test("given_never_expire_when_a_producer_provisions_then_should_create_the_topic_without_expiry", async () => {
  const { transport, created } = recordingProducerTransport()
  await Producer.create(transport, "fleet", "readings", { neverExpire: true }).send(
    new Uint8Array([1])
  )
  assert.deepEqual(created, [{ messageExpiryMicros: NEVER_EXPIRE }])
})

void test("given_never_expire_and_an_expiry_when_a_producer_is_built_then_should_reject", () => {
  const { transport } = recordingProducerTransport()
  assert.throws(
    () => Producer.create(transport, "fleet", "readings", { neverExpire: true, expireAfterMs: 1 }),
    InvalidError
  )
  assert.throws(
    () => Producer.create(transport, "fleet", "readings", { expireAfterMs: 0 }),
    InvalidError
  )
})

void test("given_a_timestamp_start_when_polling_then_should_send_the_iggy_timestamp_strategy", async () => {
  const requests: Uint8Array[] = []
  const empty = new Uint8Array(16)
  const client = {
    clientProvider: () => Promise.resolve({}),
    sendBinaryRequest: (_code: number, payload: Buffer) => {
      requests.push(new Uint8Array(payload))
      return Promise.resolve(Buffer.from(empty))
    },
    destroy: () => Promise.resolve()
  } as unknown as IggyClient
  const transport = await ApacheIggyTransport.fromClient(client)
  const polled = await transport.pollMessages(
    "fleet",
    "readings",
    { kind: "single", partitionId: 3, name: "metrics" },
    { kind: "timestampMicros", value: 1_700_000_000_000_000n },
    10,
    false
  )
  assert.deepEqual(polled, [])
  assert.deepEqual(
    requests[0],
    new Uint8Array(
      serializePollMessages(
        "fleet",
        "readings",
        { kind: IggyConsumer.Single.kind, id: "metrics" },
        3,
        PollingStrategy.Timestamp(1_700_000_000_000_000n),
        10,
        false
      )
    )
  )
})

void test("given_a_negative_timestamp_start_when_a_consumer_is_built_then_should_reject", () => {
  assert.throws(
    () =>
      Consumer.create(
        {} as LaserTransport,
        "fleet",
        "readings",
        { kind: "single", partitionId: 0, name: "metrics" },
        { startAt: { kind: "timestampMicros", value: -1n } }
      ),
    InvalidError
  )
})

void test("given_replayed_records_when_a_cursor_polls_then_should_carry_each_log_position_as_its_id", async () => {
  const transport = {
    pollMessages(_stream: string, _topic: string, target: { readonly partitionId?: number }) {
      const record: PolledMessage = {
        payload: new Uint8Array([1]),
        partitionId: target.partitionId ?? 0,
        offset: 5n,
        headers: new Map([["source", HeaderValue.string("edge")]])
      }
      return Promise.resolve([record])
    }
  } as unknown as LaserTransport
  const [message] = await Cursor.create(transport, "fleet", "readings", [2]).poll()
  assert.ok(message !== undefined)
  assert.deepEqual(message.id, { partitionId: 2, offset: 5n })
  assert.deepEqual(message.payload, new Uint8Array([1]))
  assert.deepEqual(message.headers.get("source"), HeaderValue.string("edge"))
})

void test("given_a_json_record_when_a_cursor_polls_then_should_decode_its_payload_as_json", async () => {
  const transport = {
    pollMessages(_stream: string, _topic: string, target: { readonly partitionId?: number }) {
      const records: PolledMessage[] = [
        {
          payload: new TextEncoder().encode('{"n":1}'),
          partitionId: target.partitionId ?? 0,
          offset: 0n,
          headers: new Map()
        },
        {
          payload: new Uint8Array([0xff]),
          partitionId: target.partitionId ?? 0,
          offset: 1n,
          headers: new Map()
        }
      ]
      return Promise.resolve(records)
    }
  } as unknown as LaserTransport
  const [json, broken] = await Cursor.create(transport, "fleet", "readings", [0]).poll()
  assert.ok(json !== undefined && broken !== undefined)
  assert.deepEqual(json.json(), { n: 1 })
  assert.equal(
    json.json((value) => (value as { n: number }).n),
    1
  )
  assert.throws(() => broken.json(), CodecError)
})

void test("given_both_unlimited_and_bounded_topic_size_when_configured_then_should_refuse_it", () => {
  const transport = {} as LaserTransport
  assert.throws(
    () =>
      Producer.create(transport, "fleet", "readings", {
        unlimitedTopicSize: true,
        maxTopicBytes: 1_024n
      }),
    InvalidError
  )
})
