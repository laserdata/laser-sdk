import assert from "node:assert/strict"
import { test } from "node:test"

import { ReliableConsumer, type ConcurrencyPolicy } from "../../src/agent/reliable-consumer.js"
import { CancelledError, InvalidError, TimeoutError } from "../../src/client/errors.js"
import { INTERNAL_NATIVE_CONSUMER, INTERNAL_TRANSPORT } from "../../src/client/internals.js"
import type { Laser } from "../../src/client/laser.js"
import { encodeProvenanceHeaders } from "../../src/provenance/provenance.js"
import type { Consumer, ConsumerMessage } from "../../src/stream/consumer.js"
import { AgentId, ConsumerGroupName, ConversationId } from "../../src/types/ids.js"

function deferred() {
  let resolve!: () => void
  const promise = new Promise<void>((complete) => {
    resolve = complete
  })
  return { promise, resolve }
}

const group = ConsumerGroupName.forAgent(AgentId.new("grace-worker"))

function source(commit: () => Promise<void>) {
  let delivered = false
  const message = {
    partitionId: 0,
    offset: 0n,
    timestampMicros: 1n,
    payload: new TextEncoder().encode("work"),
    headers: encodeProvenanceHeaders({ conversationId: ConversationId.derive("grace") })
  } as ConsumerMessage
  const consumer = {
    nextWithin: (_waitMs: number, options?: { signal?: AbortSignal }) => {
      if (!delivered) {
        delivered = true
        return Promise.resolve(message)
      }
      return new Promise<ConsumerMessage>((_resolve, reject) => {
        const abort = (): void => {
          reject(new CancelledError("stopped"))
        }
        options?.signal?.addEventListener("abort", abort, { once: true })
        if (options?.signal?.aborted === true) abort()
      })
    },
    commit,
    shutdown: () => Promise.resolve()
  } as unknown as Consumer
  return {
    defaultStream: "agents",
    topic: () => ({
      consumerGroup: () => ({
        [INTERNAL_NATIVE_CONSUMER]: () => Promise.resolve(consumer)
      })
    }),
    [INTERNAL_TRANSPORT]: () => ({
      resolveStreamTopicIds: () => Promise.resolve({ streamId: 1, topicId: 2 })
    })
  } as unknown as Laser
}

for (const concurrency of [
  { kind: "serial" },
  { kind: "serial-per-partition", maxPartitions: 2 }
] as const satisfies readonly ConcurrencyPolicy[]) {
  void test(
    `given_stalled_${concurrency.kind}_work_when_shutdown_grace_expires_then_should_stop_without_committing`,
    { timeout: 1000 },
    async () => {
      const started = deferred()
      const work = deferred()
      const stop = new AbortController()
      let commits = 0
      const laser = source(() => {
        commits += 1
        return Promise.resolve()
      })
      const consumer = new ReliableConsumer({
        group,
        topic: "commands",
        concurrency,
        shutdownGraceMs: 5
      })
      const running = consumer.run(
        laser,
        {
          handle: () => {
            started.resolve()
            return work.promise
          }
        },
        { signal: stop.signal }
      )
      const failed = assert.rejects(running, TimeoutError)
      await started.promise
      stop.abort()
      await failed
      assert.equal(commits, 0)
      work.resolve()
      await new Promise<void>((resolve) => setImmediate(resolve))
      assert.equal(commits, 0)
    }
  )
}

void test(
  "given_a_stalled_commit_when_shutdown_grace_expires_then_should_stop_waiting_for_the_offset_store",
  { timeout: 1000 },
  async () => {
    const storing = deferred()
    const committed = deferred()
    const stop = new AbortController()
    const laser = source(() => {
      storing.resolve()
      return committed.promise
    })
    const consumer = new ReliableConsumer({ group, topic: "commands", shutdownGraceMs: 5 })
    const running = consumer.run(
      laser,
      { handle: () => Promise.resolve() },
      { signal: stop.signal }
    )
    const failed = assert.rejects(running, TimeoutError)
    await storing.promise
    stop.abort()
    await failed
    committed.resolve()
  }
)

void test("given_work_that_finishes_within_the_grace_when_shutdown_is_requested_then_should_commit_and_drain", async () => {
  const started = deferred()
  const work = deferred()
  const stop = new AbortController()
  let commits = 0
  const laser = source(() => {
    commits += 1
    return Promise.resolve()
  })
  const consumer = new ReliableConsumer({ group, topic: "commands", shutdownGraceMs: 100 })
  const running = consumer.run(
    laser,
    {
      handle: () => {
        started.resolve()
        return work.promise
      }
    },
    { signal: stop.signal }
  )
  await started.promise
  stop.abort()
  work.resolve()
  await running
  assert.equal(commits, 1)
})

void test("given_invalid_shutdown_grace_when_a_consumer_is_built_then_should_refuse_before_io", () => {
  for (const shutdownGraceMs of [-1, NaN, Infinity])
    assert.throws(
      () => new ReliableConsumer({ group, topic: "commands", shutdownGraceMs }),
      InvalidError
    )
})
