import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { OperationUncertainError } from "apache-iggy"

import { Laser } from "../../src/client/laser.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"
const SEND_MESSAGES = 101

const utf8 = (text: string) => new TextEncoder().encode(text)
const decodeUtf8 = (bytes: Uint8Array) => new TextDecoder().decode(bytes)

void test("given_a_lost_reply_when_the_idempotent_producer_resends_then_should_write_the_batch_once", async () => {
  await using laser = await Laser.connect(CONNECTION_STRING)
  const streamName = `laser-ts-test-${randomUUID()}`
  await laser.stream(streamName).ensure()
  const topic = laser.stream(streamName).topic("readings")
  await topic.ensure(1)
  const link = await laser.client.clientProvider()
  const sendCommand = link.sendCommand.bind(link)
  let lost = 0
  link.sendCommand = async (code, payload, options) => {
    const reply = await sendCommand(code, payload, options)
    if (code === SEND_MESSAGES && lost === 0) {
      lost += 1
      throw new OperationUncertainError(undefined, new Error("the reply was lost"))
    }
    return reply
  }
  const producer = topic.producer({
    idempotent: true,
    routing: { kind: "partition", partition: 0 }
  })
  try {
    const resent = await producer.sendBatch([
      utf8('{"host":"node-1","cpu":41}'),
      utf8('{"host":"node-2","cpu":57}')
    ])
    assert.equal(lost, 1, "the first reply was lost")
    assert.deepEqual(
      resent.confirmations.map((confirmation) => confirmation.baseOffset),
      [0n],
      "the resend answers the offset of the first write"
    )
    const next = await producer.send(utf8('{"host":"node-3","cpu":82}'))
    assert.equal(next.confirmations[0]?.baseOffset, 2n)
  } finally {
    link.sendCommand = sendCommand
    await producer.shutdown()
  }

  const consumer = topic.consumer(0, {
    startAt: { kind: "first" },
    commitPolicy: { kind: "disabled" }
  })
  const stored: [bigint, string][] = []
  for (let remaining = 3; remaining > 0; remaining -= 1) {
    const record = await consumer.nextWithin(5_000)
    stored.push([record.position.offset, decodeUtf8(record.payload)])
  }
  await consumer.shutdown()
  assert.deepEqual(stored, [
    [0n, '{"host":"node-1","cpu":41}'],
    [1n, '{"host":"node-2","cpu":57}'],
    [2n, '{"host":"node-3","cpu":82}']
  ])
})
