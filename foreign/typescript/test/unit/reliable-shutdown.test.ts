import assert from "node:assert/strict"
import { test } from "node:test"

import { ReliableConsumer, type ConcurrencyPolicy } from "../../src/agent/reliable-consumer.js"
import {
  CancelledError,
  InvalidError,
  TimeoutError,
  TransportError
} from "../../src/client/errors.js"
import {
  INTERNAL_COMMIT_HANDLED,
  INTERNAL_NATIVE_CONSUMER,
  INTERNAL_TRANSPORT
} from "../../src/client/internals.js"
import { OPEN_CAPABILITIES } from "../../src/client/capabilities.js"
import { SessionConfig } from "../../src/session.js"
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

// Each opened consumer delivers one record, then holds every later read until
// its caller cancels it, like a server-held read that finds nothing new.
function source(commit: () => Promise<void>, onOpen = () => undefined, idempotencyKey?: string) {
  const message = {
    partitionId: 0,
    position: { partitionId: 0, offset: 0n },
    timestampMicros: 1n,
    payload: new TextEncoder().encode("work"),
    headers: encodeProvenanceHeaders({
      conversationId: ConversationId.derive("grace"),
      ...(idempotencyKey !== undefined ? { idempotencyKey } : {})
    })
  } as ConsumerMessage
  const open = (): Consumer => {
    let delivered = false
    return {
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
      [INTERNAL_COMMIT_HANDLED]: commit,
      shutdown: () => Promise.resolve()
    } as unknown as Consumer
  }
  return {
    defaultStream: "agents",
    topic: () => ({
      consumerGroup: () => ({
        [INTERNAL_NATIVE_CONSUMER]: () => {
          onOpen()
          return Promise.resolve(open())
        }
      })
    }),
    capabilities: () => Promise.resolve({ ...OPEN_CAPABILITIES, hello: "rejected" }),
    sessions: (config?: SessionConfig) => ({
      config: config ?? new SessionConfig(),
      indexesSessions: () => Promise.resolve(false)
    }),
    [INTERNAL_TRANSPORT]: () => ({
      findTopicPartitionCount: () => Promise.resolve(undefined),
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

void test(
  "given_a_lane_that_fails_while_the_next_read_is_held_when_running_per_partition_then_should_reopen_without_waiting_out_the_read",
  { timeout: 2_000 },
  async () => {
    let opens = 0
    let reopened: () => void = () => undefined
    const reopen = new Promise<void>((resolve) => {
      reopened = resolve
    })
    let observed = 0
    const stop = new AbortController()
    const laser = source(
      () => Promise.resolve(),
      () => {
        opens += 1
        if (opens === 2) reopened()
      },
      "lane-a"
    )
    const consumer = new ReliableConsumer({
      group,
      topic: "commands",
      concurrency: { kind: "serial-per-partition", maxPartitions: 2 },
      maxWaitMs: 60_000,
      deduplicator: {
        observe: () => {
          observed += 1
          return observed === 1
            ? Promise.reject(new TransportError("dedup store unavailable", true))
            : Promise.resolve(true)
        }
      }
    })
    const running = consumer.run(
      laser,
      { handle: () => Promise.resolve() },
      { signal: stop.signal }
    )
    try {
      await Promise.race([reopen, new Promise((resolve) => setTimeout(resolve, 1_000))])
    } finally {
      stop.abort()
      await running.catch(() => undefined)
    }
    assert.equal(opens, 2, "the failed lane reopens the consumer while the read is still held")
  }
)
