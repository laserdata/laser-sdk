import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { CancelledError, InvalidError, TimeoutError } from "../../src/client/errors.js"
import { iggyErrorCode } from "../../src/client/error-classify.js"
import { INTERNAL_TRANSPORT } from "../../src/client/internals.js"
import { Laser } from "../../src/client/laser.js"
import {
  ONE_SHOT_POLL,
  type CoordinatorConnection,
  type HeldPartition,
  type ManagedTarget
} from "../../src/iggy/apache-iggy.js"
import type { Consumer, ConsumerMessage } from "../../src/stream/consumer.js"
import { heldRead } from "../../src/stream/cursor.js"
import { decodeOne } from "../../src/wire/cbor.js"
import {
  AGDX_FILTERED_ACK_CODE,
  AGDX_FILTERED_POLL_CODE,
  AGDX_FILTERED_WAIT_CANCEL_CODE,
  AGDX_FILTERED_WAIT_CODE
} from "../../src/wire/codes.js"
import {
  decodeFilteredAck,
  decodeFilteredPollRequest,
  decodeFilteredWaitRequest
} from "../../src/wire/filter.js"
import { TestIggyCluster } from "../support/test-iggy.js"

// One managed request of a group reader: the node it went to and the partitions it named.
interface Routed {
  readonly code: number
  readonly target: ManagedTarget
  readonly partitions: readonly number[]
}

const RECONNECT = "?reconnection_retries=unlimited&reconnection_interval=100ms"
const INVALID_REPLICATION_FACTOR = 2018
const PARTITIONS = 3
const MAX_WAIT_MS = 5_000

void test(
  "given_a_five_node_cluster_when_topics_are_created_with_a_replication_factor_then_each_partition_should_have_that_many_members",
  { timeout: 240_000 },
  async () => {
    const cluster = await TestIggyCluster.start(5)
    let laser: Laser | undefined
    try {
      const [leader] = await cluster.leaderAndFollower()
      cluster.routeEndpointTo(leader)
      const stream = `replicated_${randomUUID()}`
      laser = await Laser.connectWithStream(`${cluster.endpoint}${RECONNECT}`, stream)
      await laser.stream(stream).ensure()
      await laser.topic("triple").ensure(6, 3)
      await laser.topic("single").ensure(6, 1)
      await laser.topic("everywhere").ensure(2)
      const transport = laser[INTERNAL_TRANSPORT]()
      const primaryOf = transport.partitionPrimary?.bind(transport)
      assert.ok(primaryOf !== undefined)
      for (const [name, factor] of [
        ["triple", 3],
        ["single", 1],
        ["everywhere", cluster.size]
      ] as const) {
        const created = await laser.client.topic.get({ streamId: stream, topicId: name })
        assert.ok(created !== null, `topic ${name} exists`)
        for (const partition of created.partitions) {
          assert.equal(
            new Set(partition.members).size,
            factor,
            `partition ${String(partition.id)} of ${name} is stored on ${JSON.stringify(partition.members)}`
          )
          const primary = await primaryOf(stream, name, partition.id).catch((error: unknown) => {
            throw new Error(`the route of ${name}/${String(partition.id)} failed: ${causes(error)}`)
          })
          assert.ok(
            partition.members.includes(primary),
            `the primary ${String(primary)} of ${name}/${String(partition.id)} is a member`
          )
        }
      }
      await assert.rejects(
        laser.topic("wide").ensure(1, cluster.size + 1),
        (error: unknown) => iggyErrorCode(error) === INVALID_REPLICATION_FACTOR
      )
      await assert.rejects(
        laser.topic("wider").ensure(1, 33),
        new InvalidError("replication factor must be 1..=32")
      )
    } finally {
      if (laser !== undefined) await laser.close().catch(() => undefined)
      await cluster.close()
    }
  }
)

void test(
  "given_partitions_stored_on_three_of_five_nodes_when_reads_are_held_per_primary_then_should_get_a_record_on_any_partition_at_once",
  { timeout: 240_000 },
  async () => {
    const cluster = await TestIggyCluster.start(5)
    let laser: Laser | undefined
    try {
      const [leader] = await cluster.leaderAndFollower()
      cluster.routeEndpointTo(leader)
      const stream = `held_members_${randomUUID()}`
      laser = await Laser.connectWithStream(`${cluster.endpoint}${RECONNECT}`, stream)
      await laser.stream(stream).ensure()
      const topic = laser.topic("replies")
      await topic.ensure(6, 3)
      const transport = laser[INTERNAL_TRANSPORT]()
      const groups = transport.primaryGroups?.bind(transport)
      const fetch = transport.fetchHeld?.bind(transport)
      assert.ok(groups !== undefined && fetch !== undefined, "the client routes held fetches")
      const fetched: number[][] = []
      const refused: unknown[] = []
      Object.assign(transport, {
        fetchHeld: async (...args: Parameters<typeof fetch>) => {
          fetched.push([...args[2].keys()])
          try {
            return await fetch(...args)
          } catch (error) {
            // The fetches that lost the race are cancelled, not refused.
            if (!(error instanceof CancelledError)) refused.push(error)
            throw error
          }
        }
      })
      const offsets = new Map<number, bigint>()
      for (let partition = 0; partition < 6; partition += 1) offsets.set(partition, 0n)
      // A partition not yet placed on its node refuses a fetch as transient
      // for a moment after the topic is created.
      await waitUntil(
        () =>
          heldRead(transport, stream, "replies", offsets, 10, ONE_SHOT_POLL).then(
            () => true,
            () => false
          ),
        "the new topic's partitions never became readable",
        30_000
      )
      refused.length = 0
      for (let partition = 0; partition < 6; partition += 1) {
        fetched.length = 0
        let sent = Infinity
        const sending = delay(500).then(async () => {
          sent = Date.now()
          await topic.send(new TextEncoder().encode(`reply-${String(partition)}`), { partition })
        })
        // Held as the reply hub holds: again after a hold that ended empty.
        const deadline = Date.now() + MAX_WAIT_MS * 2
        let holds = 0
        let held: HeldPartition | undefined
        while (held === undefined) {
          assert.ok(Date.now() < deadline, `partition ${String(partition)} never answered`)
          holds += 1
          held = await heldRead(transport, stream, "replies", offsets, 10, {
            ...ONE_SHOT_POLL,
            maxWaitMs: MAX_WAIT_MS
          }).catch((error: unknown) => {
            throw new Error(
              `the held read of partition ${String(partition)} failed: ${causes(error)}`
            )
          })
        }
        await sending
        const latency = Date.now() - sent
        assert.equal(held.partitionId, partition)
        assert.ok(
          latency < MAX_WAIT_MS / 2,
          `partition ${String(partition)} answered after ${String(latency)} ms`
        )
        assert.ok(
          fetched.length > 0 && fetched.length <= cluster.size * holds,
          `one held fetch per primary, held ${JSON.stringify(fetched)} in ${String(holds)} holds`
        )
        offsets.set(partition, 1n)
      }
      assert.deepEqual(
        refused,
        [],
        "no held fetch reached a node that does not store its partitions"
      )
    } finally {
      if (laser !== undefined) await laser.close().catch(() => undefined)
      await cluster.close()
    }
  }
)

for (const factor of [3, 1]) {
  void test(
    `given_a_topic_stored_on_${String(factor)}_of_five_nodes_when_a_member_fails_over_then_should_lose_no_record_and_ask_only_members`,
    { timeout: 420_000 },
    async () => {
      const cluster = await TestIggyCluster.start(5)
      let laser: Laser | undefined
      const consumers: Consumer[] = []
      const running = { value: true }
      const publishing = { value: true }
      const loops: Promise<void>[] = []
      try {
        const [leader] = await cluster.leaderAndFollower()
        cluster.routeEndpointTo(leader)
        const stream = `member_failover_${String(factor)}_${randomUUID()}`
        laser = await Laser.connectWithStream(`${cluster.endpoint}${RECONNECT}`, stream)
        await laser.stream(stream).ensure()
        const transport = laser[INTERNAL_TRANSPORT]()
        const open = transport.openCoordinator?.bind(transport)
        const primaryOf = transport.partitionPrimary?.bind(transport)
        assert.ok(open !== undefined && primaryOf !== undefined)
        const routed: Routed[] = []
        Object.assign(transport, { openCoordinator: async () => spyRoutes(await open(), routed) })
        const topic = laser.topic("tape")
        await topic.ensure(PARTITIONS, factor)
        const created = await laser.client.topic.get({ streamId: stream, topicId: "tape" })
        assert.ok(created !== null)
        const members = new Map(
          created.partitions.map((partition) => [partition.id, partition.members])
        )
        for (const [partition, stored] of members)
          assert.equal(stored.length, factor, `partition ${String(partition)} members`)
        const failed = await primaryOf(stream, "tape", 0)

        const native = topic.consumer("tape-reader", 0, {
          startAt: { kind: "first" },
          batchLength: 100,
          maxWaitMs: MAX_WAIT_MS
        })
        const group = await topic.consumerGroup("tape-desk").consumer({
          startAt: { kind: "first" },
          commitPolicy: { kind: "disabled" },
          batchLength: 100,
          maxWaitMs: MAX_WAIT_MS
        })
        consumers.push(native, group)
        const nativeSeen = new Set<bigint>()
        const groupSeen = new Set<bigint>()
        loops.push(
          drive(native, running, (message) => {
            nativeSeen.add(sequence(message))
            return Promise.resolve()
          }),
          drive(group, running, async (message) => {
            groupSeen.add(sequence(message))
            await group.commit(message).catch(() => undefined)
          })
        )
        const sent = new Map<bigint, number>()
        // Partitions with no member up. One connection runs one publish at a
        // time, so a send to such a partition would hold back every other one.
        const unavailable = new Set<number>()
        let next = 1n
        loops.push(
          (async () => {
            while (publishing.value) {
              const value = next
              next += 1n
              const partition = Number(value % BigInt(PARTITIONS))
              if (unavailable.has(partition)) {
                await delay(20)
                continue
              }
              try {
                await within(topic.send(encodeSequence(value), { partition }), 10_000)
                sent.set(value, partition)
              } catch {
                // An unconfirmed record is never required, the next one takes a fresh sequence.
              }
              await delay(20)
            }
          })()
        )

        const confirmedOn = (partition: number): number =>
          [...sent.values()].filter((on) => on === partition).length
        await waitUntil(() => sent.size >= 30, "publishing did not start")
        for (const [partition, stored] of members)
          if (stored.every((member) => member === failed)) unavailable.add(partition)
        await cluster.stopNode(failed)
        if (failed === leader) {
          const [successor] = await cluster.leaderAndFollower((failed + 1) % cluster.size)
          cluster.routeEndpointTo(successor)
        }
        // A partition with no member left takes no record until its node is back.
        const survivor = [...members].find(([, stored]) => !stored.includes(failed))?.[0]
        if (survivor !== undefined) {
          const beforeSurvivor = confirmedOn(survivor)
          await waitUntil(
            () => confirmedOn(survivor) > beforeSurvivor + 10,
            `partition ${String(survivor)} took no record while node ${String(failed)} was down`
          )
        } else {
          await delay(3_000)
        }
        if (factor > 1) {
          const beforeFailed = confirmedOn(0)
          await waitUntil(
            () => confirmedOn(0) > beforeFailed + 10,
            "partition 0 took no record with two of its three members up"
          )
        }
        await cluster.startNode(failed)
        unavailable.clear()
        const afterRestart = confirmedOn(0)
        await waitUntil(
          () => confirmedOn(0) > afterRestart + 10,
          "partition 0 took no record after its member returned",
          120_000
        )
        publishing.value = false
        await waitUntil(
          () =>
            [...sent].every(
              ([value, partition]) =>
                (partition !== 0 || nativeSeen.has(value)) && groupSeen.has(value)
            ),
          () =>
            `a confirmed record was never delivered: native missed ${String(
              [...sent].filter(([value, partition]) => partition === 0 && !nativeSeen.has(value))
                .length
            )}, group missed ${String([...sent.keys()].filter((value) => !groupSeen.has(value)).length)}`,
          120_000
        )
        running.value = false
        await Promise.all(loops)
        loops.length = 0
        const strays = routed.filter(
          (request) =>
            request.target.kind === "replica" &&
            request.partitions.some(
              (partition) =>
                !(members.get(partition) ?? []).includes(
                  request.target.kind === "replica" ? request.target.replica : -1
                )
            )
        )
        assert.deepEqual(strays, [], "every managed request named a member of its partitions")
        assert.ok(
          routed.some((request) => request.code === AGDX_FILTERED_POLL_CODE),
          "the group consumer read through the managed path"
        )
      } finally {
        running.value = false
        publishing.value = false
        await Promise.all(loops)
        for (const consumer of consumers) await consumer.shutdown().catch(() => undefined)
        if (laser !== undefined) await laser.close().catch(() => undefined)
        await cluster.close()
      }
    }
  )
}

function causes(error: unknown): string {
  const chain: string[] = []
  for (let cause: unknown = error; cause instanceof Error; cause = cause.cause) {
    const code = (cause as { readonly errorCode?: unknown }).errorCode
    chain.push(
      `${cause.name}: ${cause.message}${typeof code === "number" ? ` (${String(code)})` : ""}`
    )
  }
  return chain.join(" <- ")
}

function spyRoutes(coordinator: CoordinatorConnection, routed: Routed[]): CoordinatorConnection {
  return new Proxy(coordinator, {
    get(target, key, receiver): unknown {
      if (key === "sendManagedTo") {
        return (
          code: number,
          payload: Uint8Array,
          managed: ManagedTarget,
          options?: { readonly deadline?: number }
        ) => {
          const partitions =
            code === AGDX_FILTERED_POLL_CODE
              ? [decodeFilteredPollRequest(decodeOne(payload, "poll"), "poll").partitionId]
              : code === AGDX_FILTERED_ACK_CODE
                ? [decodeFilteredAck(decodeOne(payload, "ack"), "ack").partitionId]
                : code === AGDX_FILTERED_WAIT_CODE
                  ? decodeFilteredWaitRequest(decodeOne(payload, "wait"), "wait").partitions.map(
                      (partition) => partition.partitionId
                    )
                  : []
          if (code !== AGDX_FILTERED_WAIT_CANCEL_CODE)
            routed.push({ code, target: managed, partitions })
          return target.sendManagedTo(code, payload, managed, options)
        }
      }
      const value: unknown = Reflect.get(target, key, receiver)
      return typeof value === "function"
        ? (value as (...args: unknown[]) => unknown).bind(target)
        : value
    }
  })
}

async function drive(
  consumer: Consumer,
  running: { readonly value: boolean },
  handle: (message: ConsumerMessage) => Promise<void>
): Promise<void> {
  while (running.value) {
    let message: ConsumerMessage
    try {
      message = await consumer.nextWithin(1_000)
    } catch (error) {
      if (!(error instanceof TimeoutError)) await delay(50)
      continue
    }
    await handle(message)
  }
}

function encodeSequence(value: bigint): Uint8Array {
  return new Uint8Array(new BigUint64Array([value]).buffer)
}

function sequence(message: ConsumerMessage): bigint {
  return new DataView(
    message.payload.buffer,
    message.payload.byteOffset,
    message.payload.byteLength
  ).getBigUint64(0, true)
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

async function waitUntil(
  condition: () => boolean | Promise<boolean>,
  failure: string | (() => string),
  timeoutMs = 90_000
): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!(await condition())) {
    if (Date.now() >= deadline) assert.fail(typeof failure === "string" ? failure : failure())
    await delay(50)
  }
}

function within<Value>(promise: Promise<Value>, timeoutMs: number): Promise<Value> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error(`operation timed out after ${String(timeoutMs)}ms`))
    }, timeoutMs)
    promise.then(
      (value) => {
        clearTimeout(timer)
        resolve(value)
      },
      (error: unknown) => {
        clearTimeout(timer)
        reject(error instanceof Error ? error : new Error(String(error)))
      }
    )
  })
}
