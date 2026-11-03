import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { TimeoutError } from "../../src/client/errors.js"
import { INTERNAL_TRANSPORT } from "../../src/client/internals.js"
import { Laser } from "../../src/client/laser.js"
import type { ConsumerGroup } from "../../src/stream/consumer-group.js"
import type { Consumer } from "../../src/stream/consumer.js"
import { ConsumerFilter, FilterExpr } from "../../src/wire/filter.js"
import { TestCatalog, TestIggyCluster } from "../support/test-iggy.js"

const RECONNECT = "?reconnection_retries=unlimited&reconnection_interval=100ms"
const TOPIC = "fleet_changes"
const GROUP = "anomaly-desk"
const PARTITIONS = 3
const SAFE_MODE = `{"op":"u","table":"satellites","changed":["mode"],"after":{"id":"sat-042","name":"Kestrel-42","mode":"safe","orbit":"leo","battery_pct":61}}`
const DECOMMISSION = `{"op":"d","table":"satellites","before":{"id":"sat-042"}}`
const GROUND_STATION = `{"op":"u","table":"ground_stations","changed":["status"],"after":{"id":"svalbard","status":"online"}}`
const SKIP =
  TestCatalog.binary() === undefined
    ? "set LASER_TEST_CATALOG to the laser-test-catalog binary of the test-support crate"
    : false

void test(
  "given_a_policy_aware_group_consumer_with_a_bound_policy_on_a_cluster_when_a_partition_primary_fails_over_then_should_keep_delivering_matches_and_store_its_offsets",
  { timeout: 300_000, skip: SKIP },
  async () => {
    const { cluster, catalog } = await catalogCluster()
    let laser: Laser | undefined
    let consumer: Consumer | undefined
    try {
      const [leader] = await cluster.leaderAndFollower()
      const stream = `bound_failover_${randomUUID()}`
      const connected = await catalogLaser(cluster, stream)
      laser = connected
      const primaries = await primariesOf(connected, stream)
      const group = await boundGroup(laser)
      const groupConsumer = await group.consumer({
        startAt: { kind: "first" },
        commitPolicy: { kind: "each" },
        maxWaitMs: 5_000
      })
      consumer = groupConsumer
      const delivered = new Map<number, bigint>()
      const deliverAll = async (boundMs: number): Promise<void> => {
        for (let partition = 0; partition < PARTITIONS; partition += 1)
          await deliverMatch(connected, groupConsumer, delivered, partition, boundMs)
      }
      await deliverAll(10_000)
      const stopped = required(primaries.get(0))
      await cluster.stopNode(stopped)
      if (stopped === leader) {
        const [successor] = await cluster.leaderAndFollower((stopped + 1) % cluster.size)
        cluster.routeEndpointTo(successor)
      }
      await deliverAll(10_000)
      await cluster.startNode(stopped)
      await deliverAll(90_000)
      await deliverAll(5_000)
      await groupConsumer.shutdown()
      consumer = undefined
      for (const [partition, offset] of delivered) await storedAt(laser, stream, partition, offset)
    } finally {
      await consumer?.shutdown().catch(() => undefined)
      if (laser !== undefined) await laser.close().catch(() => undefined)
      await cluster.close()
      await catalog.close()
    }
  }
)

void test(
  "given_a_bound_policy_page_read_before_its_primary_crashes_on_a_cluster_when_acknowledged_then_should_store_it_through_the_new_primary",
  { timeout: 240_000, skip: SKIP },
  async () => {
    const { cluster, catalog } = await catalogCluster()
    let laser: Laser | undefined
    try {
      const [leader] = await cluster.leaderAndFollower()
      const stream = `bound_ack_${randomUUID()}`
      // More partitions than nodes, so one is led by a follower.
      laser = await catalogLaser(cluster, stream, PARTITIONS * 2)
      // Right after creation every partition can still be led by the
      // metadata leader, so the routes are read until a follower leads one.
      let primaries = await primariesOf(laser, stream, PARTITIONS * 2)
      const deadline = Date.now() + 30_000
      while (![...primaries.values()].some((primary) => primary !== leader)) {
        assert.ok(
          Date.now() < deadline,
          `a partition led by a follower, primaries ${JSON.stringify([...primaries])} with leader ${String(leader)}`
        )
        await delay(500)
        primaries = await primariesOf(laser, stream, PARTITIONS * 2)
      }
      const partition = [...primaries].find(([, primary]) => primary !== leader)?.[0]
      assert.ok(partition !== undefined)
      const topic = laser.topic(TOPIC)
      for (const record of [SAFE_MODE, GROUND_STATION, DECOMMISSION])
        await topic.send(new TextEncoder().encode(record), { partition })
      const group = await boundGroup(laser)
      const reader = await group.reader().start({ kind: "first" }).build()
      try {
        const page = await within(reader.nextPage(), 15_000)
        assert.equal(page.partitionId, partition)
        assert.deepEqual(
          page.records.map((record) => record.offset),
          [0n, 2n],
          "the bound policy passes the safe-mode change and the decommission only"
        )
        await cluster.killNode(required(primaries.get(partition)))
        const started = Date.now()
        await within(reader.ackPage(page), 30_000)
        const stored = Date.now() - started
        assert.ok(stored < 10_000, `the acknowledgment took ${String(stored)} ms after the crash`)
        assert.ok(page.safeAckOffset !== undefined)
        await storedAt(laser, stream, partition, page.safeAckOffset)
      } finally {
        await reader.close().catch(() => undefined)
      }
    } finally {
      if (laser !== undefined) await laser.close().catch(() => undefined)
      await cluster.close()
      await catalog.close()
    }
  }
)

// A three-node cluster whose nodes each reach the one stand-in catalog over
// a socket of their own, the endpoint routed to the metadata leader.
async function catalogCluster(): Promise<{
  readonly cluster: TestIggyCluster
  readonly catalog: TestCatalog
}> {
  const catalog = await TestCatalog.start(3)
  try {
    const cluster = await TestIggyCluster.start(3, (node) => ({
      IGGY_PLANE_ENABLED: "true",
      IGGY_PLANE_SOCKET_PATH: required(catalog.sockets[node])
    }))
    const [leader] = await cluster.leaderAndFollower()
    cluster.routeEndpointTo(leader)
    return { cluster, catalog }
  } catch (error) {
    await catalog.close()
    throw error
  }
}

async function catalogLaser(
  cluster: TestIggyCluster,
  stream: string,
  partitions = PARTITIONS
): Promise<Laser> {
  const laser = await Laser.connectWithStream(`${cluster.endpoint}${RECONNECT}`, stream)
  const capabilities = await laser.capabilities()
  assert.ok(capabilities.filters.catalog, "the cluster serves the filter catalog")
  assert.ok(capabilities.filters.groupPolicyReads, "the cluster resolves group policies")
  await laser.stream(stream).ensure()
  await laser.topic(TOPIC).ensure(partitions)
  return laser
}

async function primariesOf(
  laser: Laser,
  stream: string,
  partitions = PARTITIONS
): Promise<ReadonlyMap<number, number>> {
  const transport = laser[INTERNAL_TRANSPORT]()
  const primaryOf = transport.partitionPrimary?.bind(transport)
  assert.ok(primaryOf !== undefined)
  const primaries = new Map<number, number>()
  for (let partition = 0; partition < partitions; partition += 1)
    primaries.set(partition, await primaryOf(stream, TOPIC, partition))
  return primaries
}

async function boundGroup(laser: Laser): Promise<ConsumerGroup> {
  const group = laser.topic(TOPIC).consumerGroup(GROUP)
  const created = await group.create({
    filter: ConsumerFilter.json(
      FilterExpr.any([
        FilterExpr.pred("op", "eq", "d"),
        FilterExpr.pred("changed", "contains", "mode")
      ])
    ),
    filterName: "safe-mode"
  })
  assert.equal(created.filter?.revision, 1)
  return group
}

// Publish a record the policy drops and one it passes on `partition`, again
// while a failover leaves the partition without a primary, and take the match
// within `boundMs`. A dropped record must never reach the consumer.
async function deliverMatch(
  laser: Laser,
  consumer: Consumer,
  delivered: Map<number, bigint>,
  partition: number,
  boundMs: number
): Promise<void> {
  const topic = laser.topic(TOPIC)
  const started = Date.now()
  for (const record of [GROUND_STATION, SAFE_MODE]) {
    for (;;) {
      try {
        await within(topic.send(new TextEncoder().encode(record), { partition }), 10_000)
        break
      } catch {
        assert.ok(Date.now() - started < boundMs, `partition ${String(partition)} took no record`)
        await delay(100)
      }
    }
  }
  for (;;) {
    const left = boundMs - (Date.now() - started)
    assert.ok(
      left > 0,
      `partition ${String(partition)} delivered no match within ${String(boundMs)} ms`
    )
    let message
    try {
      message = await consumer.nextWithin(Math.min(left, 1_000))
    } catch (error) {
      if (!(error instanceof TimeoutError)) await delay(50)
      continue
    }
    const payload = new TextDecoder().decode(message.payload)
    assert.notEqual(payload, GROUND_STATION, "the bound policy drops the ground station change")
    delivered.set(message.partitionId, message.position.offset)
    if (message.partitionId === partition) return
  }
}

async function storedAt(
  laser: Laser,
  stream: string,
  partition: number,
  offset: bigint
): Promise<void> {
  const transport = laser[INTERNAL_TRANSPORT]()
  const read = transport.getConsumerOffset?.bind(transport)
  assert.ok(read !== undefined)
  const deadline = Date.now() + 30_000
  for (;;) {
    const stored = await read(stream, TOPIC, { kind: "group", name: GROUP }, partition).catch(
      () => undefined
    )
    if (stored?.storedOffset === offset) return
    assert.ok(
      Date.now() < deadline,
      `the stored offset of partition ${String(partition)} never reached ${offset.toString()}`
    )
    await delay(100)
  }
}

function required<Value>(value: Value | undefined): Value {
  assert.ok(value !== undefined)
  return value
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
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
