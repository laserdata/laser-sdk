import assert from "node:assert/strict"
import { test } from "node:test"
import { inspect } from "node:util"

import { ResponseError, type SendCommandOptions, SimpleClient, type RawClient } from "apache-iggy"
import { iggyErrorCode } from "../../src/client/error-classify.js"
import { InvalidError, PublishFailedError, TransportError } from "../../src/client/errors.js"
import {
  ApacheIggyTransport,
  type LaserTransport,
  type MessageWithHeaders,
  decodeHeaderBlock
} from "../../src/iggy/apache-iggy.js"
import { resolveConsumerOptions } from "../../src/stream/consumer.js"
import { Encryption } from "../../src/stream/encryption.js"
import { Producer, ProducerMessage, type ShardRequest } from "../../src/stream/producer.js"

const KEY = new TextEncoder().encode("laser-encryption-test-key-32byte")
const PLAINTEXT = new TextEncoder().encode('{"host":"node-7","cpu":82}')
// Sealed with KEY by Apache Iggy's Rust encryptor and by Node's AES-256-GCM.
const IGGY_CIPHERTEXT =
  "2cf9899e78c2d7e11900d208ccc590f5ca112bc7f67b1d5180fba3d30354613fa3c954477cc01d73545c7105dce1ac30680fe7a79699"
const NODE_CIPHERTEXT =
  "bfafee07d0742f433ed130c340127691377c4f54776eda6e40a841af10d5b4c950612150a76700ff3b017a802971514632695045f015"

const hex = (text: string) => new Uint8Array(Buffer.from(text, "hex"))

function transportWithSend(send: LaserTransport["sendMessagesWithHeaders"]): LaserTransport {
  return { sendMessagesWithHeaders: send } as unknown as LaserTransport
}

const direct = { createTopic: false, createStream: false, retries: 0 } as const

const UNAUTHENTICATED = 40

// A session link that refuses a send started under an older session, as the
// Apache Iggy session owner does, and runs `inFlight` before admitting a send.
function sessionLink(inFlight: () => Promise<void> = () => Promise.resolve()) {
  const sends: { readonly options: SendCommandOptions; readonly held: number }[] = []
  const admitted: Uint8Array[] = []
  let held = 0
  const link = {
    lineage: 1,
    on: () => undefined,
    destroy: () => undefined,
    hold: () => {
      held += 1
      return () => {
        held -= 1
      }
    },
    sendCommand: async (code: number, payload: Buffer, options: SendCommandOptions = {}) => {
      sends.push({ options, held })
      await inFlight()
      if (options.lineage !== undefined && options.lineage !== link.lineage)
        throw new ResponseError(code, UNAUTHENTICATED)
      admitted.push(new Uint8Array(payload))
      return { status: 0, length: 0, data: Buffer.alloc(0) }
    }
  }
  return { link, sends, admitted, held: () => held }
}

const plainRecord: MessageWithHeaders = { payload: PLAINTEXT, headers: new Map() }
const sealedRecord = (): MessageWithHeaders => {
  const encryption = Encryption.aes256Gcm(KEY)
  return {
    payload: encryption.encrypt(PLAINTEXT),
    headers: new Map(),
    userHeaders: encryption.encrypt(new Uint8Array(0))
  }
}

async function sendThrough(
  link: ReturnType<typeof sessionLink>["link"],
  record: MessageWithHeaders
) {
  const transport = await ApacheIggyTransport.fromClient(
    new SimpleClient(link as unknown as RawClient),
    "borrowed",
    { maxRetries: 0 }
  )
  return transport.sendMessagesWithHeaders("metrics", "readings", [record], undefined, 0)
}

void test("given_the_shared_vectors_when_decrypting_then_should_read_the_plaintext", () => {
  const encryption = Encryption.aes256Gcm(KEY)
  for (const vector of [IGGY_CIPHERTEXT, NODE_CIPHERTEXT]) {
    assert.deepEqual(encryption.decrypt(hex(vector)), PLAINTEXT)
  }
})

void test("given_a_key_when_encrypting_then_should_decrypt_with_a_fresh_nonce_each_time", () => {
  const encryption = Encryption.aes256Gcm(KEY)
  const first = encryption.encrypt(PLAINTEXT)
  const second = encryption.encrypt(PLAINTEXT)
  assert.equal(first.byteLength, 12 + PLAINTEXT.byteLength + 16)
  assert.notDeepEqual(first, second)
  assert.deepEqual(encryption.decrypt(first), PLAINTEXT)
})

void test("given_another_key_when_decrypting_then_should_throw_iggy_error_72", () => {
  const sealed = Encryption.aes256Gcm(KEY).encrypt(PLAINTEXT)
  const other = Encryption.aes256Gcm(new Uint8Array(32))
  assert.throws(
    () => other.decrypt(sealed),
    (error: unknown) => error instanceof TransportError && iggyErrorCode(error) === 72
  )
  assert.throws(
    () => other.decrypt(new Uint8Array(27)),
    (error: unknown) => error instanceof TransportError && iggyErrorCode(error) === 72
  )
})

void test("given_a_short_key_when_building_then_should_throw_invalid_error", () => {
  assert.throws(() => Encryption.aes256Gcm(new Uint8Array(16)), InvalidError)
})

void test("given_an_encryption_when_inspected_then_should_not_show_the_key", () => {
  assert.equal(inspect(Encryption.aes256Gcm(KEY)).includes("laser"), false)
  assert.equal(JSON.stringify(Encryption.aes256Gcm(KEY)), "{}")
})

void test("given_an_encrypted_producer_when_sending_then_should_seal_the_payload_and_header_block", async () => {
  const encryption = Encryption.aes256Gcm(KEY)
  const sent: MessageWithHeaders[] = []
  const producer = Producer.create(
    transportWithSend((_stream, _topic, messages) => {
      sent.push(...messages)
      return Promise.resolve({ confirmations: [] })
    }),
    "metrics",
    "readings",
    { ...direct, encryption }
  )
  await producer.sendMessage(
    new ProducerMessage(PLAINTEXT).header("source", { kind: "string", value: "node-7" })
  )
  const [record] = sent
  assert.ok(record?.userHeaders !== undefined)
  assert.equal(record.headers.size, 0)
  assert.deepEqual(encryption.decrypt(record.payload), PLAINTEXT)
  assert.deepEqual(
    decodeHeaderBlock(encryption.decrypt(record.userHeaders)).headers,
    new Map([["source", { kind: "string", value: "node-7" }]])
  )
})

void test("given_an_encrypted_producer_when_a_send_fails_then_should_list_the_records_as_sent", async () => {
  const encryption = Encryption.aes256Gcm(KEY)
  const producer = Producer.create(
    transportWithSend((stream, topic, messages) =>
      Promise.reject(
        new PublishFailedError(stream, topic, [], messages, new TransportError("refused", false))
      )
    ),
    "metrics",
    "readings",
    { ...direct, encryption }
  )
  await assert.rejects(
    producer.send(PLAINTEXT),
    (error: unknown) =>
      error instanceof PublishFailedError &&
      error.unconfirmed.length === 1 &&
      Buffer.from(error.unconfirmed[0]?.payload ?? []).equals(Buffer.from(PLAINTEXT))
  )
})

void test("given_a_sharding_function_when_sending_in_background_then_should_route_each_send_through_it", async () => {
  const seen: ShardRequest[] = []
  const producer = Producer.create(
    transportWithSend(() => Promise.resolve({ confirmations: [] })),
    "metrics",
    "readings",
    {
      ...direct,
      background: { shards: 4 },
      sharding: (request) => {
        seen.push(request)
        return request.records % request.shards
      }
    }
  )
  await producer.sendBatch([PLAINTEXT, PLAINTEXT, PLAINTEXT])
  await producer.shutdown()
  assert.deepEqual(seen, [{ shards: 4, stream: "metrics", topic: "readings", records: 3 }])
})

void test("given_a_sharding_function_when_it_picks_past_the_last_shard_then_should_fail_the_send", async () => {
  let sends = 0
  const producer = Producer.create(
    transportWithSend(() => {
      sends += 1
      return Promise.resolve({ confirmations: [] })
    }),
    "metrics",
    "readings",
    { ...direct, background: { shards: 2 }, sharding: (request) => request.shards }
  )
  await assert.rejects(producer.send(PLAINTEXT), InvalidError)
  await producer.shutdown()
  assert.equal(sends, 0)
})

void test("given_a_sharding_function_when_it_throws_or_picks_no_shard_index_then_should_fail_the_send_and_queue_nothing", async () => {
  const sent: MessageWithHeaders[] = []
  const refusal = new Error("no shard for this send")
  const picks: (() => number)[] = [
    () => -1,
    () => 0.5,
    () => Number.NaN,
    () => {
      throw refusal
    },
    () => 1
  ]
  const producer = Producer.create(
    transportWithSend((_stream, _topic, messages) => {
      sent.push(...messages)
      return Promise.resolve({ confirmations: [] })
    }),
    "metrics",
    "readings",
    {
      ...direct,
      background: { shards: 2 },
      sharding: () => {
        const pick = picks.shift()
        assert.ok(pick !== undefined, "a pick for each send")
        return pick()
      }
    }
  )
  for (const refused of ["negative", "fraction", "not-a-number"]) {
    await assert.rejects(producer.send(new TextEncoder().encode(refused)), InvalidError)
  }
  await assert.rejects(producer.send(PLAINTEXT), refusal)
  await producer.send(new TextEncoder().encode("accepted"))
  await producer.shutdown()
  assert.deepEqual(
    sent.map((record) => new TextDecoder().decode(record.payload)),
    ["accepted"]
  )
})

void test("given_a_sharding_function_without_background_mode_when_building_then_should_throw_invalid_error", () => {
  const transport = transportWithSend(() => Promise.resolve({ confirmations: [] }))
  assert.throws(
    () => Producer.create(transport, "metrics", "readings", { ...direct, sharding: () => 0 }),
    InvalidError
  )
})

void test("given_an_encrypted_consumer_when_it_commits_on_polling_then_should_throw_invalid_error", () => {
  const encryption = Encryption.aes256Gcm(KEY)
  for (const commitPolicy of [
    { kind: "polling" },
    { kind: "intervalOrPolling", intervalMs: 100 }
  ] as const) {
    assert.throws(() => resolveConsumerOptions({ commitPolicy, encryption }, false), InvalidError)
  }
})

void test("given_plain_and_encrypted_records_when_sent_then_should_carry_the_same_send_deadline_and_session", async () => {
  for (const record of [plainRecord, sealedRecord()]) {
    const { link, sends, admitted, held } = sessionLink()
    const before = Date.now()
    await sendThrough(link, record)
    const after = Date.now()
    const [send] = sends
    assert.ok(send !== undefined, "the send reaches the link")
    assert.equal(send.options.lineage, 1)
    const deadline = send.options.deadline ?? 0
    assert.ok(deadline >= before + 30_000 && deadline <= after + 30_000, "the send deadline is set")
    assert.equal(send.held, 1)
    assert.equal(held(), 0)
    assert.equal(admitted.length, 1)
  }
})

void test("given_a_session_change_during_a_send_when_encrypted_then_should_refuse_it_as_a_plain_send", async () => {
  const outcomes: unknown[] = []
  for (const record of [plainRecord, sealedRecord()]) {
    const session = sessionLink(() => {
      session.link.lineage += 1
      return Promise.resolve()
    })
    const outcome = await sendThrough(session.link, record).then(
      () => "sent",
      (error: unknown) =>
        error instanceof PublishFailedError ? iggyErrorCode(error.cause) : "unexpected"
    )
    outcomes.push(outcome)
    assert.equal(session.sends.length, 1)
    assert.equal(session.admitted.length, 0)
  }
  assert.deepEqual(outcomes, [UNAUTHENTICATED, UNAUTHENTICATED])
})
