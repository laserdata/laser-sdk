import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { Agent, type AgentHandle } from "../../src/agent/builder.js"
import { TimeoutError } from "../../src/client/errors.js"
import { filterReason } from "../../src/client/error-classify.js"
import { INTERNAL_NATIVE_CONSUMER, INTERNAL_TRANSPORT } from "../../src/client/internals.js"
import { decodeManagedReply } from "../../src/client/managed.js"
import { Laser } from "../../src/client/laser.js"
import { AgentTopic } from "../../src/provenance/agent-topic.js"
import { decodeProvenanceHeaders } from "../../src/provenance/provenance.js"
import { TopicRetention } from "../../src/session.js"
import type { FilteredReader, MatchedPage } from "../../src/managed/filters.js"
import type { Consumer, ConsumerMessage } from "../../src/stream/consumer.js"
import type { Topic } from "../../src/stream/topic.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"
import type {
  ConsumerTarget,
  CoordinatorConnection,
  ManagedTarget
} from "../../src/iggy/apache-iggy.js"
import { decodeOne } from "../../src/wire/cbor.js"
import {
  AGDX_FILTERED_POLL_CODE,
  AGDX_FILTERED_WAIT_CANCEL_CODE,
  AGDX_FILTERED_WAIT_CODE
} from "../../src/wire/codes.js"
import {
  decodeFilterReply,
  decodeFilteredPollRequest,
  decodeFilteredWaitCancel,
  decodeFilteredWaitRequest
} from "../../src/wire/filter.js"
import { TestIggyCluster } from "../support/test-iggy.js"

// One filtered wait the reader sent: the node, its partitions, and when it ended.
interface HeldWait {
  readonly target: ManagedTarget
  readonly partitions: readonly number[]
  answered: boolean
}

const RECONNECT = "?reconnection_retries=unlimited&reconnection_interval=100ms"
const MAX_WAIT_MS = 5_000
// The reply hub holds its reads as long, so a reply that lands well before
// this was delivered by a held read, not by the next read after the wait.
const REPLY_WAIT_MS = 5_000

void test(
  "given_node_restarts_when_held_reads_consume_then_should_deliver_and_commit_all",
  { timeout: 300_000 },
  async () => {
    const cluster = await TestIggyCluster.start()
    let laser: Laser | undefined
    const consumers: Consumer[] = []
    const running = { value: true }
    const loops: Promise<void>[] = []
    try {
      const [leader, follower] = await cluster.leaderAndFollower()
      cluster.routeEndpointTo(leader)
      laser = await Laser.connectWithStream(`${cluster.endpoint}${RECONNECT}`, "held_reads")
      const topic = laser.topic("tape")
      await topic.ensure(3)
      const native = topic.consumer("tape-reader", 0, {
        startAt: { kind: "first" },
        commitPolicy: { kind: "polling" },
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
      const nativeSeen = new Map<bigint, number>()
      const groupSeen = new Map<bigint, number>()
      const groupHighest = new Map<number, bigint>()
      const commitFailures: unknown[] = []
      loops.push(
        drive(native, running, (message) => {
          nativeSeen.set(sequence(message), Date.now())
          return Promise.resolve()
        }),
        drive(group, running, async (message) => {
          groupSeen.set(sequence(message), Date.now())
          // The reader follows a moved primary and stores again while a
          // partition has none, so a commit never needs the caller's retry.
          try {
            await group.commit(message)
          } catch (error) {
            commitFailures.push(error)
            return
          }
          const highest = groupHighest.get(message.partitionId)
          if (highest === undefined || message.position.offset > highest)
            groupHighest.set(message.partitionId, message.position.offset)
        })
      )

      const sent = new Map<bigint, number>()
      let next = 1n
      const publishing = { value: true }
      const publisher = (async () => {
        while (publishing.value) {
          const value = next
          const partition = Number(value % 3n)
          try {
            await within(topic.send(encodeSequence(value), { partition }), 10_000)
            sent.set(value, partition)
            next += 1n
          } catch {
            // An unconfirmed record may or may not be stored, so it is resent
            // under a fresh sequence and only confirmed ones are required.
            next += 1n
          }
          await delay(25)
        }
      })()

      await waitUntil(() => sent.size >= 20, "publishing did not start")
      await cluster.restartNode(follower)
      const afterFollower = sent.size
      await waitUntil(() => sent.size > afterFollower + 20, "publishing stalled after the restart")
      await cluster.stopNode(leader)
      const [newLeader] = await cluster.leaderAndFollower(follower)
      cluster.routeEndpointTo(newLeader)
      await cluster.startNode(leader)
      const afterFailover = sent.size
      await waitUntil(() => sent.size > afterFailover + 20, "publishing stalled after the failover")
      publishing.value = false
      await publisher

      const nativeExpected = [...sent].filter(([, partition]) => partition === 0)
      await waitUntil(
        () =>
          nativeExpected.every(([value]) => nativeSeen.has(value)) &&
          [...sent.keys()].every((value) => groupSeen.has(value)),
        "a confirmed record was never delivered",
        60_000
      )

      // Both readers now sit in held reads. One record wakes them before the
      // wait elapses: the native read at once, the group read within its pass.
      await delay(MAX_WAIT_MS + 1_000)
      const probe = next
      const probeSent = Date.now()
      await within(topic.send(encodeSequence(probe), { partition: 0 }), 10_000)
      await waitUntil(
        () => nativeSeen.has(probe) && groupSeen.has(probe),
        "the probe record was never delivered",
        30_000
      )
      const nativeLatency = (nativeSeen.get(probe) ?? Infinity) - probeSent
      const groupLatency = (groupSeen.get(probe) ?? Infinity) - probeSent
      assert.ok(nativeLatency < MAX_WAIT_MS / 2, `native latency ${String(nativeLatency)} ms`)
      assert.ok(groupLatency < MAX_WAIT_MS, `group latency ${String(groupLatency)} ms`)

      running.value = false
      await Promise.all(loops)
      loops.length = 0
      assert.deepEqual(commitFailures, [])
      // The polling policy stores each read on the server, the explicit
      // policy stores the highest handled offset of each partition.
      const nativeStored = await native.storedOffset(0)
      assert.equal(nativeStored?.storedOffset, nativeStored?.currentOffset)
      for (const [partition, highest] of groupHighest) {
        const stored = await group.storedOffset(partition)
        assert.equal(
          stored?.storedOffset,
          highest,
          `group offset of partition ${String(partition)}`
        )
      }
    } finally {
      running.value = false
      await Promise.all(loops)
      for (const consumer of consumers) await consumer.shutdown().catch(() => undefined)
      if (laser !== undefined) await laser.close().catch(() => undefined)
      await cluster.close()
    }
  }
)

void test(
  "given_replies_across_nodes_and_a_restart_when_asking_then_should_beat_the_wait",
  { timeout: 300_000 },
  async () => {
    const cluster = await TestIggyCluster.start()
    let requester: Laser | undefined
    let worker: Laser | undefined
    let handle: AgentHandle | undefined
    try {
      const [leader, follower] = await cluster.leaderAndFollower()
      cluster.routeEndpointTo(leader)
      const stream = `cluster_replies_${randomUUID()}`
      requester = await Laser.connectWithStream(`${cluster.endpoint}${RECONNECT}`, stream)
      worker = await Laser.connectWithStream(`${cluster.endpoint}${RECONNECT}`, stream)
      await requester.bootstrap(6, TopicRetention.expireAfter(86_400_000))
      // When the worker's reply was published, by question.
      const replied = new Map<string, number>()
      handle = Agent.builder()
        .id(AgentId.new("echo"))
        .listenOn(AgentTopic.Sessions)
        .respondOn(AgentTopic.Sessions)
        .maxWait(200)
        .handler({
          async handle(message, context): Promise<void> {
            await context.respond(message.payload)
            replied.set(new TextDecoder().decode(message.payload), Date.now())
          }
        })
        .build()
        .spawn(worker)
      await handle.ready()

      const transport = requester[INTERNAL_TRANSPORT]()
      const primaryOf = transport.partitionPrimary?.bind(transport)
      assert.ok(primaryOf !== undefined)
      const asker = requester.agent(AgentId.new("asker"))
      // Each round trip and the part of it the reply hub took: from the
      // worker's published reply to the requester holding it.
      const roundTrips = async (
        count: number,
        timeoutMs: number,
        untilPartitions = 0
      ): Promise<{ readonly partition: number; readonly ms: number }[]> => {
        const trips: { partition: number; ms: number }[] = []
        while (
          trips.length < count ||
          new Set(trips.map((trip) => trip.partition)).size < untilPartitions
        ) {
          assert.ok(trips.length < 120, "the replies never covered every partition")
          const question = `question-${randomUUID()}`
          const asked = Date.now()
          const reply = await asker
            .ask(
              AgentTopic.Sessions,
              AgentTopic.Sessions,
              new TextEncoder().encode(question),
              { conversationId: ConversationId.new() },
              timeoutMs
            )
            .catch((error: unknown) => {
              const published = replied.get(question)
              throw new Error(
                `round trip ${String(trips.length)} failed, the worker ${published === undefined ? "never answered" : `answered after ${String(published - asked)} ms`}`,
                { cause: error }
              )
            })
          const received = Date.now()
          assert.equal(new TextDecoder().decode(reply.payload), question)
          // A held read can hand the reply over before the worker's publish call returns.
          await waitUntil(
            () => replied.has(question),
            "the worker never recorded its reply",
            10_000
          )
          const published = required(replied.get(question))
          trips.push({ partition: reply.id.partitionId, ms: Math.max(0, received - published) })
        }
        return trips
      }

      // The agent is ready before its group reader owns every partition of a
      // cluster group, so the first command may wait for the next assignment.
      await roundTrips(1, 60_000)
      const first = await roundTrips(12, 15_000, 6)
      const nodes = new Set<number>()
      for (const partition of new Set(first.map((trip) => trip.partition)))
        nodes.add(await primaryOf(stream, AgentTopic.Sessions, partition))
      assert.ok(nodes.size >= 2, `replies came from partitions on ${String(nodes.size)} node(s)`)
      for (const trip of first) {
        assert.ok(
          trip.ms < REPLY_WAIT_MS / 2,
          `a reply on partition ${String(trip.partition)} reached the requester after ${String(trip.ms)} ms`
        )
      }

      await cluster.restartNode(follower)
      // The first replies after the restart may wait for the node's links.
      await roundTrips(4, 30_000)
      const second = await roundTrips(12, 15_000)
      for (const trip of second) {
        assert.ok(
          trip.ms < REPLY_WAIT_MS / 2,
          `after the restart a reply on partition ${String(trip.partition)} reached the requester after ${String(trip.ms)} ms`
        )
      }
    } finally {
      if (handle !== undefined) await handle.shutdown().catch(() => undefined)
      if (worker !== undefined) await worker.close().catch(() => undefined)
      if (requester !== undefined) await requester.close().catch(() => undefined)
      await cluster.close()
    }
  }
)

void test(
  "given_70_reply_partitions_when_the_hub_holds_reads_then_should_get_the_last_at_once",
  { timeout: 180_000 },
  async () => {
    const cluster = await TestIggyCluster.start()
    let laser: Laser | undefined
    try {
      const [leader] = await cluster.leaderAndFollower()
      cluster.routeEndpointTo(leader)
      const stream = `wide_replies_${randomUUID()}`
      laser = await Laser.connectWithStream(`${cluster.endpoint}${RECONNECT}`, stream)
      await laser.stream(stream).ensure()
      await laser.topic("requests").ensure(1)
      const replies = laser.topic("replies")
      await replies.ensure(70)
      const conversationId = ConversationId.new()
      const asker = AgentId.new("asker")
      const answer = laser
        .agent(asker)
        .ask(
          "requests",
          "replies",
          new TextEncoder().encode("question"),
          { conversationId },
          30_000
        )
      const requests = await laser.topic("requests").replay()
      let correlationId: string | undefined
      await waitUntil(async () => {
        for (const record of await requests.poll()) {
          correlationId ??= decodeProvenanceHeaders(record.headers).correlationId
        }
        return correlationId !== undefined
      }, "the request was never published")
      // The hub found nothing on its pass and now holds a read on every
      // partition, more than the 64 deferred requests one client may park.
      await delay(1_000)
      const replied = Date.now()
      await replies.send(new TextEncoder().encode("answer"), {
        partition: 69,
        provenance: {
          conversationId,
          agent: AgentId.new("responder"),
          targetAgentId: asker,
          ...(correlationId !== undefined ? { correlationId } : {})
        }
      })
      const reply = await answer
      const latency = Date.now() - replied
      assert.equal(new TextDecoder().decode(reply.payload), "answer")
      assert.equal(reply.id.partitionId, 69)
      assert.ok(latency < REPLY_WAIT_MS / 2, `the reply on partition 69 took ${String(latency)} ms`)
    } finally {
      if (laser !== undefined) await laser.close().catch(() => undefined)
      await cluster.close()
    }
  }
)

void test(
  "given_an_idle_group_reader_when_any_node_gets_a_record_then_should_wake_at_once",
  { timeout: 240_000 },
  async () => {
    const cluster = await TestIggyCluster.start()
    let laser: Laser | undefined
    try {
      const [leader] = await cluster.leaderAndFollower()
      cluster.routeEndpointTo(leader)
      const stream = `cluster_waits_${randomUUID()}`
      laser = await Laser.connectWithStream(`${cluster.endpoint}${RECONNECT}`, stream)
      const capabilities = await laser.capabilities()
      assert.ok(capabilities.filters.groupPolicyReads, "the cluster serves group reads")
      assert.ok(capabilities.filters.maxWaitMs > 0, "the cluster holds filtered waits")
      await laser.stream(stream).ensure()
      const transport = laser[INTERNAL_TRANSPORT]()
      const open = transport.openCoordinator?.bind(transport)
      assert.ok(open !== undefined)
      // A topic handle binds the coordinator its readers open, so the spy goes first.
      const waits: HeldWait[] = []
      Object.assign(transport, {
        openCoordinator: async () => spyWaits(await open(), waits)
      })
      const topic = laser.topic("fleet")
      await topic.ensure(6)
      const primaries = await settledPrimaries(laser, stream, 6, cluster.size)
      const nodes = new Set(primaries.values())
      assert.ok(nodes.size >= 2, `the partitions live on ${String(nodes.size)} node(s)`)

      const group = topic.consumerGroup("fleet-desk")
      await group.create()
      // An idle round that slept instead of holding a wait would sleep 30 s.
      const reader = await group.reader().maxWait(MAX_WAIT_MS).idleInterval(30_000).build()
      try {
        await ownsEvery(reader, primaries.size)
        // A primary that has not applied the new member's ownership yet refuses
        // its reads, and the reader reports the second refusal in a row, so one
        // pass over the partitions goes untimed and reads past that refusal.
        for (const partition of primaries.keys()) {
          await topic.send(new TextEncoder().encode(`record-${String(partition)}`), { partition })
          await reader.ackPage(await pageOf(reader, partition, 60_000))
        }
        for (const partition of [...primaries.keys(), ...primaries.keys()]) {
          const pending = reader.nextPage({ timeoutMs: 30_000 })
          // A failed check closes the reader, which ends this page with its own error.
          pending.catch(() => undefined)
          await delay(1_500)
          const held = waits.filter((wait) => !wait.answered)
          const heldNodes = held.map((wait) => replicaOf(wait.target))
          assert.deepEqual(
            [...heldNodes].sort(),
            [...nodes].sort(),
            `the idle reader holds one wait per node, held ${JSON.stringify(heldNodes)}`
          )
          assert.deepEqual(
            held.flatMap((wait) => wait.partitions).sort(),
            [...primaries.keys()].sort()
          )
          for (const wait of held) {
            for (const waited of wait.partitions)
              assert.equal(replicaOf(wait.target), primaries.get(waited))
          }
          const sent = Date.now()
          await topic.send(new TextEncoder().encode(`record-${String(partition)}`), { partition })
          const page = await within(pending, 30_000)
          const latency = Date.now() - sent
          assert.equal(page.partitionId, partition)
          assert.equal(page.records.length, 1)
          assert.ok(
            latency < MAX_WAIT_MS / 2,
            `a record on partition ${String(partition)} of node ${String(primaries.get(partition))} woke the reader after ${String(latency)} ms`
          )
          await reader.ackPage(page)
        }
      } finally {
        await reader.close()
      }
    } finally {
      if (laser !== undefined) await laser.close().catch(() => undefined)
      await cluster.close()
    }
  }
)

void test(
  "given_a_group_reader_when_a_partition_primary_stops_then_should_keep_delivering_and_storing",
  { timeout: 300_000 },
  async () => {
    const cluster = await TestIggyCluster.start()
    let laser: Laser | undefined
    try {
      const [leader] = await cluster.leaderAndFollower()
      cluster.routeEndpointTo(leader)
      const stream = `cluster_failover_${randomUUID()}`
      laser = await Laser.connectWithStream(`${cluster.endpoint}${RECONNECT}`, stream)
      const { topic, primaries } = await fleet(laser, stream, 3, cluster.size)
      const group = topic.consumerGroup("fleet-desk")
      await group.create()
      const reader = await group.reader().maxWait(10_000).build()
      const delivered = new Map<number, bigint>()
      try {
        await ownsEvery(reader, 3)
        for (let partition = 0; partition < 3; partition += 1)
          await deliverOne(topic, reader, delivered, partition, 4_000)
        const stopped = required(primaries.get(0))
        await cluster.stopNode(stopped)
        if (stopped === leader) {
          const [successor] = await cluster.leaderAndFollower((stopped + 1) % 3)
          cluster.routeEndpointTo(successor)
        }
        // The successor leads within moments of a graceful stop, so a read the
        // stopped node still holds moves on long before the 30 s call deadline.
        for (let partition = 0; partition < 3; partition += 1)
          await deliverOne(topic, reader, delivered, partition, 10_000)
        await cluster.startNode(stopped)
        for (let partition = 0; partition < 3; partition += 1)
          await deliverOne(topic, reader, delivered, partition, 90_000)
        for (let partition = 0; partition < 3; partition += 1)
          await deliverOne(topic, reader, delivered, partition, 4_000)
      } finally {
        await reader.close()
      }
      for (const [partition, offset] of delivered)
        await storedAt(laser, stream, "fleet-desk", partition, offset)
    } finally {
      if (laser !== undefined) await laser.close().catch(() => undefined)
      await cluster.close()
    }
  }
)

void test(
  "given_a_page_read_before_its_primary_crashes_when_acknowledged_then_should_store_it_through_the_new_primary",
  { timeout: 240_000 },
  async () => {
    const cluster = await TestIggyCluster.start()
    let laser: Laser | undefined
    try {
      const [leader] = await cluster.leaderAndFollower()
      cluster.routeEndpointTo(leader)
      const stream = `cluster_ack_${randomUUID()}`
      laser = await Laser.connectWithStream(`${cluster.endpoint}${RECONNECT}`, stream)
      const { topic, primaries } = await fleet(laser, stream, 3, cluster.size)
      const partition = [...primaries].find(([, primary]) => primary !== leader)?.[0]
      assert.ok(partition !== undefined, "a partition led by a follower")
      for (const record of ["one", "two", "three"])
        await topic.send(new TextEncoder().encode(record), { partition })
      const group = topic.consumerGroup("fleet-desk")
      await group.create()
      const reader = await group.reader().start({ kind: "first" }).build()
      try {
        const page = await firstPage(reader, 15_000)
        assert.equal(page.partitionId, partition)
        await cluster.killNode(required(primaries.get(partition)))
        const started = Date.now()
        await within(reader.ackPage(page), 30_000)
        const stored = Date.now() - started
        assert.ok(stored < 10_000, `the acknowledgment took ${String(stored)} ms after the crash`)
        assert.ok(page.safeAckOffset !== undefined)
        await storedAt(laser, stream, "fleet-desk", partition, page.safeAckOffset)
      } finally {
        await reader.close().catch(() => undefined)
      }
    } finally {
      if (laser !== undefined) await laser.close().catch(() => undefined)
      await cluster.close()
    }
  }
)

void test(
  "given_a_reader_holding_waits_when_another_member_leaves_then_should_read_a_gained_partition_at_once",
  { timeout: 240_000 },
  async () => {
    const cluster = await TestIggyCluster.start()
    let laser: Laser | undefined
    try {
      const [leader] = await cluster.leaderAndFollower()
      cluster.routeEndpointTo(leader)
      const stream = `cluster_rebalance_${randomUUID()}`
      laser = await Laser.connectWithStream(`${cluster.endpoint}${RECONNECT}`, stream)
      const capabilities = await laser.capabilities()
      assert.ok(capabilities.filters.maxWaitMs >= 20_000, "the cluster holds 20 s waits")
      const { topic } = await fleet(laser, stream, 3, cluster.size)
      const group = topic.consumerGroup("fleet-desk")
      await group.create()
      const first = await group.reader().start({ kind: "first" }).maxWait(20_000).build()
      let second: FilteredReader | undefined
      try {
        await ownsEvery(first, 3)
        const member = await group.reader().start({ kind: "first" }).maxWait(20_000).build()
        second = member
        await waitUntil(
          async () => {
            assert.equal(await first.tryNextPage(), undefined)
            assert.equal(await member.tryNextPage(), undefined)
            return first.partitions().length < 3 && member.partitions().length > 0
          },
          "the two members never split the partitions",
          30_000
        )
        const gained = required(member.partitions()[0])
        // The first member holds its waits on its own partitions when the
        // second one leaves, and the record lands on a partition it gains.
        const pending = first.nextPage({ timeoutMs: 30_000 })
        await delay(500)
        await member.close()
        await topic.send(new TextEncoder().encode("gained"), { partition: gained })
        const page = await within(pending, 8_000)
        assert.equal(page.partitionId, gained)
        assert.deepEqual(
          page.records.map((record) => record.offset),
          [0n]
        )
      } finally {
        await second?.close().catch(() => undefined)
        await first.close().catch(() => undefined)
      }
    } finally {
      if (laser !== undefined) await laser.close().catch(() => undefined)
      await cluster.close()
    }
  }
)

void test(
  "given_waits_held_on_every_node_when_one_node_answers_ready_then_should_end_the_other_nodes_waits_at_once",
  { timeout: 240_000 },
  async () => {
    const cluster = await TestIggyCluster.start()
    let laser: Laser | undefined
    try {
      const [leader] = await cluster.leaderAndFollower()
      cluster.routeEndpointTo(leader)
      const stream = `cluster_cancel_${randomUUID()}`
      laser = await Laser.connectWithStream(`${cluster.endpoint}${RECONNECT}`, stream)
      const capabilities = await laser.capabilities()
      assert.ok(capabilities.filters.maxWaitMs >= 20_000, "the cluster holds 20 s waits")
      const transport = laser[INTERNAL_TRANSPORT]()
      const open = transport.openCoordinator?.bind(transport)
      assert.ok(open !== undefined)
      const waits: TimedWait[] = []
      const cancels: SentCancel[] = []
      Object.assign(transport, {
        openCoordinator: async () => spyTimedWaits(await open(), waits, cancels)
      })
      const { topic, primaries } = await fleet(laser, stream, 6, cluster.size)
      const nodes = new Set(primaries.values())
      assert.ok(nodes.size >= 2, `the partitions live on ${String(nodes.size)} node(s)`)
      const group = topic.consumerGroup("fleet-desk")
      await group.create()
      const reader = await group.reader().maxWait(20_000).idleInterval(30_000).build()
      try {
        await ownsEvery(reader, primaries.size)
        // A primary that has not applied the new member's ownership yet refuses
        // its reads, and the reader reports the second refusal in a row, so one
        // pass over the partitions goes untimed and reads past that refusal.
        for (const partition of primaries.keys()) {
          await topic.send(new TextEncoder().encode(`warm-${String(partition)}`), { partition })
          await reader.ackPage(await pageOf(reader, partition, 60_000))
        }
        let answered = 0
        for (const partition of primaries.keys()) {
          const pending = reader.nextPage({ timeoutMs: 30_000 })
          // A failed check closes the reader, which ends this page with its own error.
          pending.catch(() => undefined)
          await waitUntil(
            () =>
              new Set(
                waits
                  .filter((wait) => wait.endedAt === undefined)
                  .flatMap((wait) => wait.partitions)
              ).size === primaries.size,
            "the idle reader never held waits over every partition",
            10_000
          )
          const held = waits.filter((wait) => wait.endedAt === undefined)
          const sent = Date.now()
          await topic.send(new TextEncoder().encode(`record-${String(partition)}`), { partition })
          const page = await within(pending, 30_000)
          assert.equal(page.partitionId, partition)
          // A page read again after a refused wait keeps the other waits for
          // the next idle round. Only a ready answer cancels them.
          const winner = held.find((wait) => (wait.ready?.length ?? 0) > 0)
          if (winner === undefined) {
            await reader.ackPage(page)
            continue
          }
          const losing = held.filter((wait) => wait !== winner)
          if (losing.length > 0) answered += 1
          await waitUntil(
            () => losing.every((wait) => wait.endedAt !== undefined),
            "a losing node kept its wait",
            10_000
          )
          for (const wait of losing) {
            const ended = (wait.endedAt ?? Infinity) - sent
            const toNode = cancels.filter(
              (cancel) => replicaOf(cancel.target) === replicaOf(wait.target)
            )
            assert.ok(
              ended < 3_000,
              `the wait ${wait.waitId.toString()} on node ${String(replicaOf(wait.target))} ended ${String(ended)} ms after the record, not at its 20 s wait, cancels to that node ${JSON.stringify(toNode.map((cancel) => ({ named: cancel.waitIds.map(String), answer: cancel.answer })))}`
            )
            // A wait that answered on its own before the winner needed no cancel.
            assert.ok(
              (wait.endedAt ?? Infinity) < (winner.endedAt ?? 0) ||
                cancels.some(
                  (cancel) =>
                    replicaOf(cancel.target) === replicaOf(wait.target) &&
                    cancel.waitIds.includes(wait.waitId)
                ),
              `node ${String(replicaOf(wait.target))} got a cancel naming wait ${wait.waitId.toString()}`
            )
          }
          await reader.ackPage(page)
        }
        assert.ok(
          answered >= 3,
          `a node answered ready while another held a wait in ${String(answered)} of 6 rounds`
        )
      } finally {
        await reader.close()
      }
    } finally {
      if (laser !== undefined) await laser.close().catch(() => undefined)
      await cluster.close()
    }
  }
)

void test(
  "given_a_policy_aware_group_started_at_last_on_a_cluster_when_partitions_change_hands_then_should_seed_before_the_first_read_and_neither_replay_nor_skip",
  { timeout: 240_000 },
  async () => {
    const cluster = await TestIggyCluster.start()
    let laser: Laser | undefined
    const members: Consumer[] = []
    try {
      const [leader] = await cluster.leaderAndFollower()
      cluster.routeEndpointTo(leader)
      const stream = `cluster_start_${randomUUID()}`
      laser = await Laser.connectWithStream(`${cluster.endpoint}${RECONNECT}`, stream)
      assert.ok(
        (await laser.capabilities()).filters.groupPolicyReads,
        "the cluster serves group reads"
      )
      const transport = laser[INTERNAL_TRANSPORT]()
      const open = transport.openCoordinator?.bind(transport)
      const sync = transport.syncConsumerGroup?.bind(transport)
      assert.ok(open !== undefined && sync !== undefined)
      const events: SeedEvent[] = []
      let member = "first"
      Object.assign(transport, {
        openCoordinator: async () => spySeeds(await open(), member, events)
      })
      const { topic, primaries } = await fleet(laser, stream, 3, cluster.size)
      assert.equal(new Set(primaries.values()).size, 3, "each partition has its own primary")
      const send = async (from: number, to: number) => {
        for (let offset = from; offset < to; offset += 1)
          for (const partition of primaries.keys())
            await topic.send(new TextEncoder().encode(String(offset)), { partition })
      }
      await send(0, 3)
      const group = topic.consumerGroup("desk")
      const options = { startAt: { kind: "last" }, commitPolicy: { kind: "disabled" } } as const
      // A native member that never reads holds every partition while the first
      // reader joins, so that reader gains them later with nothing stored.
      const holder = await group[INTERNAL_NATIVE_CONSUMER]({ commitPolicy: { kind: "disabled" } })
      members.push(holder)
      await waitUntil(
        async () => (await sync(stream, "fleet", "desk"))?.partitions.length === primaries.size,
        "the holder never held every partition",
        30_000
      )
      const first = await group.consumer(options)
      members.push(first)
      const started = events.length
      await holder.shutdown()

      const before: Delivery[] = []
      await driveMembers(new Map([["first", first]]), before, () => before.length === 3, 60_000)
      assert.deepEqual(before.map((delivery) => delivery.key).sort(), ["0-2", "1-2", "2-2"])
      await assert.rejects(first.nextWithin(1_000), TimeoutError, "the first member replayed")
      for (const partition of primaries.keys()) {
        const store = events.findIndex(
          (event) =>
            event.member === "first" && event.kind === "store" && event.partitionId === partition
        )
        const read = events.findIndex(
          (event) =>
            event.member === "first" && event.kind === "read" && event.partitionId === partition
        )
        assert.ok(
          store >= 0 && store < read,
          `partition ${String(partition)} was read before its seed`
        )
        assert.equal(events[store]?.offset, 1n)
      }
      assert.ok(
        events.slice(started).some((event) => event.kind === "store"),
        "no partition was seeded after the start"
      )

      member = "second"
      const second = await group.consumer(options)
      members.push(second)
      const after: Delivery[] = []
      const both = new Map([
        ["first", first],
        ["second", second]
      ])
      await driveMembers(
        both,
        after,
        () => events.some((event) => event.member === "second" && event.kind === "read"),
        30_000
      )
      assert.equal(after.length, 0, `a member replayed a stored record: ${JSON.stringify(after)}`)
      await send(3, 5)
      const expected = ["0-3", "0-4", "1-3", "1-4", "2-3", "2-4"]
      await driveMembers(
        both,
        after,
        () => expected.every((key) => after.some((delivery) => delivery.key === key)),
        60_000
      )
      assert.ok(
        after.every((delivery) => expected.includes(delivery.key)),
        `a member replayed: ${JSON.stringify(after)}`
      )
      assert.ok(
        after.some((delivery) => delivery.member === "second"),
        `the second member took no partition over: ${JSON.stringify(after)}`
      )
    } finally {
      for (const consumer of members) await consumer.shutdown().catch(() => undefined)
      if (laser !== undefined) await laser.close().catch(() => undefined)
      await cluster.close()
    }
  }
)

// A topic of `partitions` and the replica id of each partition's primary.
async function fleet(
  laser: Laser,
  stream: string,
  partitions: number,
  nodes: number
): Promise<{ readonly topic: Topic; readonly primaries: ReadonlyMap<number, number> }> {
  await laser.stream(stream).ensure()
  const topic = laser.topic("fleet")
  await topic.ensure(partitions)
  return { topic, primaries: await settledPrimaries(laser, stream, partitions, nodes) }
}

// The primary of each partition of `fleet` once the cluster stops moving them.
// It moves one primary at a time until no node leads two more partitions than
// another, and `fleet` is the only topic of the test's cluster, so that spread
// is final.
async function settledPrimaries(
  laser: Laser,
  stream: string,
  partitions: number,
  nodes: number
): Promise<ReadonlyMap<number, number>> {
  const transport = laser[INTERNAL_TRANSPORT]()
  const primaryOf = transport.partitionPrimary?.bind(transport)
  assert.ok(primaryOf !== undefined)
  const deadline = Date.now() + 60_000
  for (;;) {
    const primaries = new Map<number, number>()
    for (let partition = 0; partition < partitions; partition += 1)
      primaries.set(partition, await primaryOf(stream, "fleet", partition))
    const led = Array.from(
      { length: nodes },
      (_, node) => [...primaries.values()].filter((primary) => primary === node).length
    )
    if (Math.max(...led) - Math.min(...led) <= 1) return primaries
    assert.ok(
      Date.now() < deadline,
      `the primaries never settled: ${JSON.stringify([...primaries])}`
    )
    await delay(500)
  }
}

// The first page of a member that just joined. Until the group's assignment
// reaches the partition's primary, a read there is refused as
// `membership_stale` and its records are read again.
async function firstPage(reader: FilteredReader, timeoutMs: number): Promise<MatchedPage> {
  const deadline = Date.now() + timeoutMs
  for (;;) {
    try {
      return await reader.nextPage({ timeoutMs: Math.max(1, deadline - Date.now()) })
    } catch (error) {
      if (filterReason(error) !== "membership_stale" || Date.now() >= deadline) throw error
    }
  }
}

// A member that just joined a cluster group can own some partitions before
// the others, and its reads are refused as `membership_stale` until the
// assignment reaches each primary, so a test measures once it owns all of them.
// The next page, of `partition`, within `timeoutMs`, read past a refusal the
// reader reports while a primary still applies the member's ownership.
async function pageOf(
  reader: FilteredReader,
  partition: number,
  timeoutMs: number
): Promise<MatchedPage> {
  const deadline = Date.now() + timeoutMs
  for (;;) {
    try {
      const page = await reader.nextPage({ timeoutMs: Math.max(1, deadline - Date.now()) })
      assert.equal(page.partitionId, partition)
      return page
    } catch (error) {
      if (filterReason(error) !== "membership_stale" || Date.now() >= deadline) throw error
    }
  }
}

async function ownsEvery(reader: FilteredReader, partitions: number): Promise<void> {
  await waitUntil(async () => {
    try {
      await reader.tryNextPage()
    } catch (error) {
      if (filterReason(error) !== "membership_stale") throw error
      return false
    }
    return reader.partitions().length === partitions
  }, "the reader never owned every partition")
}

// Publish one record on `partition`, again while a failover leaves it without
// a primary, and take its page within `boundMs`, acknowledged.
async function deliverOne(
  topic: Topic,
  reader: FilteredReader,
  delivered: Map<number, bigint>,
  partition: number,
  boundMs: number
): Promise<void> {
  const started = Date.now()
  for (;;) {
    try {
      await within(topic.send(new TextEncoder().encode("record"), { partition }), 10_000)
      break
    } catch {
      assert.ok(Date.now() - started < boundMs, `partition ${String(partition)} took no record`)
      await delay(100)
    }
  }
  for (;;) {
    const left = boundMs - (Date.now() - started)
    assert.ok(
      left > 0,
      `partition ${String(partition)} delivered nothing within ${String(boundMs)} ms`
    )
    const page = await within(reader.nextPage(), left).catch((error: unknown) => {
      throw new Error(
        `partition ${String(partition)} delivered nothing within ${String(boundMs)} ms`,
        {
          cause: error
        }
      )
    })
    await reader.ackPage(page)
    const last = page.records.at(-1)
    if (last !== undefined) delivered.set(page.partitionId, last.offset)
    if (page.partitionId === partition) return
  }
}

// Wait until the group's stored offset of `partition` is `offset`.
async function storedAt(
  laser: Laser,
  stream: string,
  group: string,
  partition: number,
  offset: bigint
): Promise<void> {
  const transport = laser[INTERNAL_TRANSPORT]()
  const read = transport.getConsumerOffset?.bind(transport)
  assert.ok(read !== undefined)
  await waitUntil(
    async () =>
      (
        await read(stream, "fleet", { kind: "group", name: group }, partition).catch(
          () => undefined
        )
      )?.storedOffset === offset,
    `the stored offset of partition ${String(partition)} never reached ${offset.toString()}`,
    30_000
  )
}

function required<Value>(value: Value | undefined): Value {
  assert.ok(value !== undefined)
  return value
}

function spyWaits(coordinator: CoordinatorConnection, waits: HeldWait[]): CoordinatorConnection {
  return new Proxy(coordinator, {
    get(target, key, receiver): unknown {
      if (key === "sendManagedTo") {
        return async (
          code: number,
          payload: Uint8Array,
          managed: ManagedTarget,
          options?: { readonly deadline?: number }
        ) => {
          if (code !== AGDX_FILTERED_WAIT_CODE)
            return target.sendManagedTo(code, payload, managed, options)
          const request = decodeFilteredWaitRequest(decodeOne(payload, "wait"), "wait")
          const wait: HeldWait = {
            target: managed,
            partitions: request.partitions.map((partition) => partition.partitionId),
            answered: false
          }
          waits.push(wait)
          try {
            return await target.sendManagedTo(code, payload, managed, options)
          } finally {
            wait.answered = true
          }
        }
      }
      const value: unknown = Reflect.get(target, key, receiver)
      return typeof value === "function"
        ? (value as (...args: unknown[]) => unknown).bind(target)
        : value
    }
  })
}

// A seed store or a filtered read a group member's connection sent.
interface SeedEvent {
  readonly member: string
  readonly kind: "store" | "read"
  readonly partitionId: number
  readonly offset?: bigint
}

// A record a member delivered, as `partition-offset`.
interface Delivery {
  readonly member: string
  readonly key: string
}

function spySeeds(
  coordinator: CoordinatorConnection,
  member: string,
  events: SeedEvent[]
): CoordinatorConnection {
  return new Proxy(coordinator, {
    get(target, key, receiver): unknown {
      if (key === "storeOffset")
        return async (
          streamId: string,
          topicId: string,
          consumer: ConsumerTarget,
          partitionId: number,
          offset: bigint
        ) => {
          events.push({ member, kind: "store", partitionId, offset })
          await target.storeOffset?.(streamId, topicId, consumer, partitionId, offset)
        }
      if (key === "sendManagedTo")
        return (
          code: number,
          payload: Uint8Array,
          managed: ManagedTarget,
          options?: { readonly deadline?: number }
        ) => {
          if (code === AGDX_FILTERED_POLL_CODE) {
            const { partitionId } = decodeFilteredPollRequest(decodeOne(payload, "poll"), "poll")
            events.push({ member, kind: "read", partitionId })
          }
          return target.sendManagedTo(code, payload, managed, options)
        }
      const value: unknown = Reflect.get(target, key, receiver)
      return typeof value === "function"
        ? (value as (...args: unknown[]) => unknown).bind(target)
        : value
    }
  })
}

// Read from every member in turn, committing each record, until `done`.
async function driveMembers(
  consumers: ReadonlyMap<string, Consumer>,
  delivered: Delivery[],
  done: () => boolean,
  boundMs: number
): Promise<void> {
  const deadline = Date.now() + boundMs
  while (!done()) {
    assert.ok(Date.now() < deadline, `the members delivered ${JSON.stringify(delivered)}`)
    for (const [member, consumer] of consumers) {
      try {
        const record = await consumer.nextWithin(300)
        delivered.push({
          member,
          key: `${String(record.position.partitionId)}-${record.position.offset.toString()}`
        })
        await consumer.commit(record)
      } catch (error) {
        if (!(error instanceof TimeoutError)) throw error
      }
    }
  }
}

// One wait cancel the reader sent and what the node answered.
interface SentCancel {
  readonly target: ManagedTarget
  readonly waitIds: readonly bigint[]
  answer: string
}

// One filtered wait the reader sent, with its id, when its call ended and the
// partitions it answered ready.
interface TimedWait {
  readonly target: ManagedTarget
  readonly waitId: bigint
  readonly partitions: readonly number[]
  endedAt: number | undefined
  ready: readonly number[] | undefined
}

function spyTimedWaits(
  coordinator: CoordinatorConnection,
  waits: TimedWait[],
  cancels: SentCancel[]
): CoordinatorConnection {
  return new Proxy(coordinator, {
    get(target, key, receiver): unknown {
      if (key === "sendManagedTo") {
        return async (
          code: number,
          payload: Uint8Array,
          managed: ManagedTarget,
          options?: { readonly deadline?: number }
        ) => {
          if (code === AGDX_FILTERED_WAIT_CANCEL_CODE) {
            const request = decodeFilteredWaitCancel(decodeOne(payload, "cancel"), "cancel")
            const cancel: SentCancel = { target: managed, waitIds: request.waitIds, answer: "none" }
            cancels.push(cancel)
            try {
              const answer = await target.sendManagedTo(code, payload, managed, options)
              const reply = decodeManagedReply(
                (bytes) => decodeFilterReply(decodeOne(bytes, "reply"), "reply"),
                answer.reply
              )
              cancel.answer =
                reply.kind === "ok" && reply.outcome.kind === "cancelled"
                  ? `cancelled ${JSON.stringify(reply.outcome.result.cancelled.map(String))}`
                  : JSON.stringify(reply, (_key, value: unknown) =>
                      typeof value === "bigint" ? String(value) : value
                    )
              return answer
            } catch (error) {
              cancel.answer = `failed: ${String(error)}`
              throw error
            }
          }
          if (code !== AGDX_FILTERED_WAIT_CODE)
            return target.sendManagedTo(code, payload, managed, options)
          const request = decodeFilteredWaitRequest(decodeOne(payload, "wait"), "wait")
          const wait: TimedWait = {
            target: managed,
            waitId: request.waitId,
            partitions: request.partitions.map((waited) => waited.partitionId),
            endedAt: undefined,
            ready: undefined
          }
          waits.push(wait)
          try {
            const answer = await target.sendManagedTo(code, payload, managed, options)
            const reply = decodeManagedReply(
              (bytes) => decodeFilterReply(decodeOne(bytes, "reply"), "reply"),
              answer.reply
            )
            if (reply.kind === "ok" && reply.outcome.kind === "ready")
              wait.ready = reply.outcome.result.ready
            return answer
          } finally {
            wait.endedAt = Date.now()
          }
        }
      }
      const value: unknown = Reflect.get(target, key, receiver)
      return typeof value === "function"
        ? (value as (...args: unknown[]) => unknown).bind(target)
        : value
    }
  })
}

function replicaOf(target: ManagedTarget): number {
  assert.equal(target.kind, "replica")
  return target.replica
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
  failure: string,
  timeoutMs = 90_000
): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!(await condition())) {
    assert.ok(Date.now() < deadline, failure)
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
