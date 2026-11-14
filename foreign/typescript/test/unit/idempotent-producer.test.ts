import assert from "node:assert/strict"
import { test } from "node:test"
import { OperationUncertainError } from "apache-iggy"
import { PublishFailedError, TransportError } from "../../src/client/errors.js"
import { iggyErrorCode } from "../../src/client/error-classify.js"
import {
  ApacheIggyTransport,
  type IggyClient,
  type LaserTransport,
  ProducerSequences
} from "../../src/iggy/apache-iggy.js"
import { Producer } from "../../src/stream/producer.js"

interface Sent {
  readonly producerId: bigint
  readonly epoch: number
  readonly baseSequence: number
  readonly count: number
  readonly ids: readonly bigint[]
}

type Answer = (sent: Sent, index: number) => bigint | Error

// The producer section and the message ids of a raw send request.
function decodeSent(request: Buffer): Sent {
  const batch = 4 + request.readUInt32LE(0)
  assert.equal(request.readUInt8(batch + 52), 1, "the batch carries a producer section")
  const count = request.readUInt32LE(batch + 48)
  const ids: bigint[] = []
  for (let at = batch + 256, index = 0; index < count; index += 1) {
    ids.push(request.readBigUInt64LE(at + 8) | (request.readBigUInt64LE(at + 16) << 64n))
    at += 48 + request.readUInt32LE(at + 32) + request.readUInt32LE(at + 36)
  }
  return {
    producerId: request.readBigUInt64LE(batch + 56),
    epoch: request.readUInt16LE(batch + 64),
    baseSequence: request.readUInt32LE(batch + 68),
    count,
    ids
  }
}

function confirmation(offset: bigint): Buffer {
  const data = Buffer.alloc(24)
  data.writeUInt32LE(1, 0)
  data.writeBigUInt64LE(offset, 16)
  return data
}

async function transport(answer: Answer, maxRetries = 3) {
  const sent: Sent[] = []
  const abandoned: bigint[] = []
  const link = {
    sendCommand: (_code: number, request: Buffer) => {
      const decoded = decodeSent(request)
      sent.push(decoded)
      const outcome = answer(decoded, sent.length - 1)
      return outcome instanceof Error
        ? Promise.reject(outcome)
        : Promise.resolve({ status: 0, length: 24, data: confirmation(outcome) })
    }
  }
  const client = {
    clientProvider: () => Promise.resolve(link),
    abandonOperation: (operationId: bigint) => {
      abandoned.push(operationId)
    },
    destroy: () => Promise.resolve()
  } as unknown as IggyClient
  const adapted = await ApacheIggyTransport.fromClient(client, "owned", {
    timeoutMs: 1_000,
    maxRetries,
    retryBackoffMs: 1
  })
  return { adapted, sent, abandoned }
}

function serverError(code: number): Error {
  return Object.assign(new Error(`server error ${String(code)}`), { errorCode: code })
}

function send(adapted: ApacheIggyTransport, sequences: ProducerSequences, payloads: number[]) {
  return adapted.sendMessagesWithHeaders(
    "fleet",
    "readings",
    payloads.map((payload) => ({ payload: new Uint8Array([payload]), headers: new Map() })),
    undefined,
    0,
    { idempotence: sequences }
  )
}

void test("given_a_lost_reply_when_sending_idempotently_then_should_resend_the_same_section_and_ids", async () => {
  const { adapted, sent, abandoned } = await transport((_sent, index) =>
    index === 0 ? new OperationUncertainError(7n, new Error("lost reply")) : 0n
  )
  const sequences = new ProducerSequences()
  const response = await send(adapted, sequences, [1, 2, 3])
  assert.equal(response.confirmations[0]?.baseOffset, 0n)
  assert.equal(sent.length, 2)
  assert.deepEqual(sent[1], sent[0], "the resend carries the first attempt's section and ids")
  assert.equal(sent[0]?.baseSequence, 0)
  assert.deepEqual(abandoned, [7n])
  await send(adapted, sequences, [4])
  assert.equal(sent[2]?.baseSequence, 3, "the next batch follows the written one")
  assert.equal(new Set(sent.map((batch) => batch.producerId)).size, 1, "one producer id")
})

void test("given_a_partition_that_forgot_the_producer_when_sending_then_should_start_a_new_epoch_at_zero", async () => {
  const { adapted, sent } = await transport((_sent, index) =>
    index === 1 ? serverError(94) : BigInt(index)
  )
  const sequences = new ProducerSequences()
  await send(adapted, sequences, [1, 2])
  await send(adapted, sequences, [3])
  assert.deepEqual(
    sent.map(({ epoch, baseSequence }) => [epoch, baseSequence]),
    [
      [0, 0],
      [0, 2],
      [1, 0]
    ]
  )
})

void test("given_an_uncertain_send_when_the_partition_then_forgot_the_producer_then_should_fail_without_a_new_epoch", async () => {
  const { adapted, sent } = await transport((_sent, index) =>
    index === 0 ? new OperationUncertainError(9n, new Error("lost reply")) : serverError(94)
  )
  const sequences = new ProducerSequences()
  await assert.rejects(send(adapted, sequences, [1]), (error: unknown) => {
    return error instanceof PublishFailedError && iggyErrorCode(error) === 94
  })
  assert.equal(sent.length, 2, "a send that can have landed never moves to a new epoch")
  assert.deepEqual(
    sent.map(({ epoch, baseSequence }) => [epoch, baseSequence]),
    [
      [0, 0],
      [0, 0]
    ]
  )
})

void test("given_a_fenced_producer_when_sending_again_then_should_refuse_before_sending", async () => {
  const { adapted, sent } = await transport(() => serverError(93))
  const sequences = new ProducerSequences()
  await assert.rejects(send(adapted, sequences, [1]), PublishFailedError)
  await assert.rejects(
    send(adapted, sequences, [2]),
    (error: unknown) => error instanceof PublishFailedError && iggyErrorCode(error) === 93
  )
  assert.equal(sent.length, 1, "a fenced producer sends nothing more")
})

void test("given_retries_disabled_when_a_reply_is_lost_then_should_not_resend", async () => {
  const { adapted, sent } = await transport(
    () => new OperationUncertainError(3n, new Error("lost reply")),
    0
  )
  await assert.rejects(send(adapted, new ProducerSequences(), [1]), (error: unknown) => {
    return (
      error instanceof PublishFailedError &&
      error.publishCause() instanceof TransportError &&
      !(error.publishCause() as TransportError).retryable
    )
  })
  assert.equal(sent.length, 1)
})

void test("given_an_idempotent_producer_option_when_sending_then_should_hand_one_sequence_state_to_every_send", async () => {
  const seen: unknown[] = []
  const producerTransport = {
    publishRetriesManaged: true,
    sendMessagesWithHeaders: (...args: Parameters<LaserTransport["sendMessagesWithHeaders"]>) => {
      seen.push(args[5]?.idempotence)
      return Promise.resolve({ confirmations: [] })
    }
  } as unknown as LaserTransport
  const plain = Producer.create(producerTransport, "fleet", "readings", { createTopic: false })
  await plain.send(new Uint8Array([1]))
  const idempotent = Producer.create(producerTransport, "fleet", "readings", {
    createTopic: false,
    idempotent: true
  })
  await idempotent.send(new Uint8Array([2]))
  await idempotent.send(new Uint8Array([3]))
  assert.equal(seen[0], undefined, "off by default")
  assert.ok(seen[1] instanceof ProducerSequences)
  assert.equal(seen[2], seen[1])
})
