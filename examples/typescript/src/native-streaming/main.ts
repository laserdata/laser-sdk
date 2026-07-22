import { HeaderValue, type Consumer, type Laser } from "@laserdata/laser-sdk"
import { batchSize, decodeUtf8, messages, PARTITIONS, runExample, utf8 } from "../common.js"

export const EXAMPLE = "native-streaming"
export const TOPIC = "events"

async function receive(
  laser: Laser,
  group: string,
  expected: number,
  manualCommit: boolean,
  signal: AbortSignal
): Promise<number> {
  const consumer: Consumer = await laser.topic(TOPIC).consumerGroup(group, {
    batchLength: Math.min(100, expected),
    autoCommit: !manualCommit,
    startFrom: { kind: "first" },
    pollIntervalMs: 5
  })
  let seen = 0
  try {
    while (seen < expected && !signal.aborted) {
      const message = await consumer.nextWithin(5_000, { signal })
      if (message === null) throw new Error(`timed out after ${String(seen)} messages`)
      if (manualCommit) await consumer.commit(message)
      seen += 1
      if (seen === 1 || seen === expected || seen % 100 === 0) {
        console.log(
          `${group}: ${String(seen)}/${String(expected)} partition=${String(message.partitionId)} ` +
            `offset=${message.offset.toString()} payload=${decodeUtf8(message.payload)}`
        )
      }
    }
    return seen
  } finally {
    await consumer.shutdown()
  }
}

export async function run(laser: Laser, signal: AbortSignal): Promise<void> {
  const count = messages(1_000)
  const batch = Math.min(batchSize(100), count)
  const topic = laser.topic(TOPIC)
  await topic.ensure(PARTITIONS)
  const producer = topic.producer({
    retries: 3,
    retryIntervalMs: 1_000
  })
  try {
    await producer.send(utf8("message-0"), {
      key: utf8("account-42"),
      headers: { type: HeaderValue.uint16(7) }
    })
    let sent = 1
    while (sent < count) {
      const size = Math.min(sent === 1 ? Math.max(1, batch - 1) : batch, count - sent)
      const payloads = Array.from({ length: size }, (_, offset) =>
        utf8(`message-${String(sent + offset)}`)
      )
      await producer.sendBatch(payloads)
      sent += size
      console.log(`published ${String(sent)}/${String(count)}`)
    }
    await producer.flush()
  } finally {
    await producer.shutdown()
  }

  const automatic = await receive(laser, "auto-workers", count, false, signal)
  const manual = await receive(laser, "manual-workers", count, true, signal)
  if (automatic !== count || manual !== count) throw new Error("consumer count mismatch")
}

if (import.meta.url === `file://${process.argv[1]}`) {
  await runExample(EXAMPLE, run)
}
