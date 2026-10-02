import { SingleClient } from "apache-iggy"
import { FilterExecutionError } from "../../src/client/errors.js"
import { decodeOne, encodeNamed } from "../../src/wire/cbor.js"
import { AGDX_FILTER_VALIDATE_CODE } from "../../src/wire/codes.js"
import { ApacheIggyTransport } from "../../src/iggy/apache-iggy.js"
import { TestIggyCluster } from "../support/test-iggy.js"
import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test, type TestContext } from "node:test"
import {
  ConfigError,
  ConsumerGroupSetupError,
  FilterStopError,
  InvalidError,
  UnsupportedError
} from "../../src/client/errors.js"
import { Laser } from "../../src/client/laser.js"
import type { ConsumerGroup } from "../../src/stream/consumer-group.js"
import { HeaderValue } from "../../src/stream/header-value.js"
import type { FilteredReader, FilteredReaderBuilder } from "../../src/managed/filters.js"
import {
  ConsumerFilter,
  FilterExpr,
  consumerFilterDigest,
  decodeFilterReply,
  encodeConsumerFilter
} from "../../src/wire/filter.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"
const TOPIC = "fleet_changes"
const READ_TIMEOUT_MS = 15_000
const SATELLITE = {
  id: "sat-042",
  name: "Kestrel-42",
  mode: "safe",
  orbit: "leo",
  battery_pct: 61
}
const SAFE_MODE = JSON.stringify({
  op: "u",
  table: "satellites",
  changed: ["mode"],
  after: SATELLITE
})
const SAFE_MODE_VALUES = JSON.stringify({ op: "u", table: "satellites", after: SATELLITE })
const DECOMMISSION = JSON.stringify({ op: "d", table: "satellites", before: { id: "sat-042" } })
const TELEMETRY = JSON.stringify({
  event: "satellite.telemetry_changed",
  satellite_id: "sat-042",
  fields: { mode: "safe" }
})
const GROUND_STATION = JSON.stringify({
  op: "u",
  table: "ground_stations",
  changed: ["status"],
  after: { id: "svalbard", status: "online" }
})

function safeModeFilter(): ConsumerFilter {
  const satellites = (): FilterExpr => FilterExpr.pred("table", "eq", "satellites")
  return ConsumerFilter.json(
    FilterExpr.any([
      FilterExpr.all([
        satellites(),
        FilterExpr.pred("op", "eq", "u"),
        FilterExpr.pred("changed", "contains", "mode"),
        FilterExpr.pred("after.mode", "eq", "safe")
      ]),
      FilterExpr.all([satellites(), FilterExpr.pred("op", "eq", "d")]),
      FilterExpr.all([
        FilterExpr.pred("event", "eq", "satellite.telemetry_changed"),
        FilterExpr.present("fields.mode")
      ])
    ])
  )
}

// A consumer group filter needs the catalog, which only a managed plane serves.
class NeedsCatalog extends Error {}

async function withSource(
  context: TestContext,
  run: (laser: Laser, stream: string) => Promise<void>
): Promise<void> {
  const laser = await Laser.connect(CONNECTION_STRING)
  const stream = `laser-ts-orbit-${randomUUID()}`
  try {
    await laser.stream(stream).ensure()
    await laser.stream(stream).topic(TOPIC).ensure(1)
    await run(laser, stream)
  } catch (error) {
    if (!(error instanceof NeedsCatalog)) throw error
    context.skip("a consumer group filter needs a managed plane")
  } finally {
    await laser
      .stream(stream)
      .delete()
      .catch(() => false)
    await laser.close()
  }
}

async function publish(laser: Laser, stream: string, payloads: readonly string[]): Promise<void> {
  const topic = laser.stream(stream).topic(TOPIC)
  for (const payload of payloads) await topic.send(new TextEncoder().encode(payload))
}

// Create `name` with a filter as its policy.
async function boundGroup(
  laser: Laser,
  stream: string,
  name: string,
  definition: ConsumerFilter = safeModeFilter()
): Promise<ConsumerGroup> {
  if (!(await laser.capabilities()).filters.catalog) throw new NeedsCatalog()
  const group = laser.stream(stream).topic(TOPIC).consumerGroup(name)
  await group.create({ filter: definition })
  return group
}

async function reader(
  laser: Laser,
  stream: string,
  name: string,
  shape: (builder: FilteredReaderBuilder) => FilteredReaderBuilder = (builder) => builder,
  definition: ConsumerFilter = safeModeFilter()
): Promise<FilteredReader> {
  return shape((await boundGroup(laser, stream, name, definition)).reader()).build()
}

void test("given_edge_records_when_read_through_the_group_filter_then_should_return_only_matches_at_their_offsets", async (context) => {
  await withSource(context, async (laser, stream) => {
    await publish(laser, stream, [
      SAFE_MODE,
      SAFE_MODE_VALUES,
      DECOMMISSION,
      TELEMETRY,
      GROUND_STATION
    ])
    const filtered = await reader(laser, stream, "safe-mode-backfill", (builder) =>
      builder.start({ kind: "first" })
    )

    const page = await filtered.nextPage({ timeoutMs: READ_TIMEOUT_MS })

    assert.deepEqual(
      page.records.map((record) => record.offset),
      [0n, 2n, 3n]
    )
    assert.equal(new TextDecoder().decode(page.records[1]?.payload), DECOMMISSION)
    assert.deepEqual(page.policy.digest, consumerFilterDigest(safeModeFilter()))
    await filtered.ackPage(page)
    await filtered.close()
  })
})

void test("given_stored_progress_when_reading_next_then_should_read_only_new_matches", async (context) => {
  await withSource(context, async (laser, stream) => {
    await publish(laser, stream, [SAFE_MODE, GROUND_STATION])
    const first = await reader(laser, stream, "safe-mode-resume", (builder) =>
      builder.start({ kind: "first" })
    )
    await first.ackPage(await first.nextPage({ timeoutMs: READ_TIMEOUT_MS }))
    await first.close()

    await publish(laser, stream, [GROUND_STATION, DECOMMISSION])
    const second = await reader(laser, stream, "safe-mode-resume")
    const page = await second.nextPage({ timeoutMs: READ_TIMEOUT_MS })

    assert.deepEqual(
      page.records.map((record) => record.offset),
      [3n]
    )
    await second.close()
  })
})

void test("given_only_non_matches_when_reading_then_should_store_the_scanned_range", async (context) => {
  await withSource(context, async (laser, stream) => {
    await publish(laser, stream, [GROUND_STATION, GROUND_STATION, GROUND_STATION])
    const first = await reader(laser, stream, "safe-mode-sparse", (builder) =>
      builder.start({ kind: "first" })
    )
    assert.equal(await first.tryNextPage(), undefined)
    await first.close()

    await publish(laser, stream, [DECOMMISSION])
    const second = await reader(laser, stream, "safe-mode-sparse")
    const page = await second.nextPage({ timeoutMs: READ_TIMEOUT_MS })

    assert.deepEqual(
      page.records.map((record) => record.offset),
      [3n],
      "the empty page stored the range it examined"
    )
    await second.close()
  })
})

void test("given_out_of_order_acks_when_acked_then_should_store_only_the_completed_prefix", async (context) => {
  await withSource(context, async (laser, stream) => {
    await publish(laser, stream, [SAFE_MODE, DECOMMISSION])
    const first = await reader(laser, stream, "safe-mode-ordered", (builder) =>
      builder.start({ kind: "first" }).count(1)
    )
    const earlier = await first.nextRecord({ timeoutMs: READ_TIMEOUT_MS })
    const later = await first.nextRecord({ timeoutMs: READ_TIMEOUT_MS })
    await first.ack(later)
    await first.close()

    const second = await reader(laser, stream, "safe-mode-ordered")
    const record = await second.nextRecord({ timeoutMs: READ_TIMEOUT_MS })

    assert.deepEqual([earlier.offset, later.offset], [0n, 1n])
    assert.equal(record.offset, 0n, "the earlier record was never completed")
    await second.close()
  })
})

void test("given_a_malformed_record_when_reading_then_should_deliver_matches_then_the_fault", async (context) => {
  await withSource(context, async (laser, stream) => {
    await publish(laser, stream, [SAFE_MODE, "not json", DECOMMISSION])
    const filtered = await reader(laser, stream, "safe-mode-fault", (builder) =>
      builder.start({ kind: "first" })
    )

    const page = await filtered.nextPage({ timeoutMs: READ_TIMEOUT_MS })
    assert.deepEqual(
      page.records.map((record) => record.offset),
      [0n]
    )
    await filtered.ackPage(page)
    await assert.rejects(filtered.nextPage(), (error: unknown) => {
      assert.ok(error instanceof FilterStopError)
      assert.equal(error.stop, "fault")
      assert.equal(error.offset, 1n)
      assert.equal(error.faultReason, "malformed")
      return true
    })
    await filtered.close()
  })
})

void test("given_a_local_guard_when_reading_then_should_agree_with_the_server", async (context) => {
  await withSource(context, async (laser, stream) => {
    await publish(laser, stream, [GROUND_STATION, TELEMETRY])
    const filtered = await reader(laser, stream, "safe-mode-guarded", (builder) =>
      builder.start({ kind: "first" }).localGuard(true)
    )

    const page = await filtered.nextPage({ timeoutMs: READ_TIMEOUT_MS })

    assert.deepEqual(
      page.records.map((record) => record.offset),
      [1n]
    )
    await filtered.close()
  })
})

void test("given_a_local_reader_when_acknowledging_then_should_refuse", async (context) => {
  await withSource(context, async (laser, stream) => {
    await publish(laser, stream, [DECOMMISSION])
    const filtered = await reader(laser, stream, "safe-mode-local", (builder) =>
      builder.start({ kind: "first" }).readMode("local")
    )
    const page = await filtered.nextPage({ timeoutMs: READ_TIMEOUT_MS })

    await assert.rejects(filtered.ackPage(page), InvalidError)
    await filtered.close()
  })
})

void test("given_stored_records_when_previewed_then_should_judge_them_without_progress", async (context) => {
  await withSource(context, async (laser, stream) => {
    const group = await boundGroup(laser, stream, "preview-desk")
    await publish(laser, stream, [SAFE_MODE, SAFE_MODE_VALUES, DECOMMISSION])

    const preview = await group.filter().preview(0, { maxRecords: 10, explain: true })

    assert.equal(preview.examined, 3)
    assert.equal(preview.matched, 2)
    const verdicts = preview.records.map(
      (record) => `${record.offset.toString()}:${record.verdict}`
    )
    assert.ok(verdicts.includes("1:rejected"))
    assert.ok(verdicts.includes("2:selected"))
  })
})

void test("given_a_sample_when_tested_against_the_group_filter_then_should_explain", async (context) => {
  await withSource(context, async (laser, stream) => {
    const group = await boundGroup(laser, stream, "sample-desk")

    const tested = await group.filter().test(SAFE_MODE_VALUES)
    const revisions = await group.filter().revisions()

    assert.equal(tested.explanation.verdict, "rejected")
    assert.equal(revisions.total, 1)
    assert.deepEqual(revisions.items[0]?.digest, consumerFilterDigest(safeModeFilter()))
  })
})

void test("given_no_catalog_when_a_group_is_created_with_a_filter_then_should_refuse_as_unsupported", async (context) => {
  await withSource(context, async (laser, stream) => {
    const capabilities = await laser.capabilities()
    if (capabilities.filters.catalog) {
      context.skip("this deployment serves the group filter catalog")
      return
    }
    assert.equal(capabilities.filters.groupPolicyReads, true)
    const topic = laser.stream(stream).topic(TOPIC)
    const group = topic.consumerGroup("anomaly-desk")
    await assert.rejects(group.create({ filter: safeModeFilter() }), UnsupportedError)

    const plain = await group.create()
    await assert.rejects(group.filter().configure(safeModeFilter()), UnsupportedError)
    assert.equal(plain.name, "anomaly-desk")
    assert.equal(plain.filter, undefined)
    assert.equal(plain.identity.groupId, BigInt(plain.id))
    const byId = topic.consumerGroupId(plain.id)
    assert.deepEqual([byId.name, byId.id], [undefined, BigInt(plain.id)])
    assert.equal((await byId.info()).name, "anomaly-desk")
  })
})

void test("given_an_unbound_group_when_consumed_then_should_deliver_every_record", async (context) => {
  await withSource(context, async (laser, stream) => {
    await publish(laser, stream, [SAFE_MODE, GROUND_STATION, DECOMMISSION])
    const group = laser.stream(stream).topic(TOPIC).consumerGroup("plain-desk")
    const consumer = await group.consumer({
      startFrom: { kind: "first" },
      autoCommit: false,
      pollIntervalMs: 5
    })
    try {
      const delivered: bigint[] = []
      for (let index = 0; index < 3; index += 1) {
        const message = await consumer.nextWithin(READ_TIMEOUT_MS)
        assert.ok(message !== null)
        delivered.push(message.offset)
        await consumer.commit(message)
      }
      assert.deepEqual(delivered, [0n, 1n, 2n], "a group without a filter receives everything")
      assert.equal(consumer.lastConsumedOffset(0), 2n)
      assert.equal((await consumer.storedOffset(0))?.storedOffset, 2n)
      await assert.rejects(
        consumer.commit({
          payload: new Uint8Array(),
          partitionId: 0,
          offset: 1n,
          headers: new Map()
        }),
        InvalidError,
        "only a delivered message commits"
      )
    } finally {
      await consumer.shutdown()
    }
    assert.equal((await group.info()).filter, undefined)
  })
})

void test("given_a_group_with_a_filter_when_consumed_then_should_deliver_only_matches", async (context) => {
  await withSource(context, async (laser, stream) => {
    const group = await boundGroup(laser, stream, "anomaly-desk")
    await publish(laser, stream, [SAFE_MODE, GROUND_STATION, DECOMMISSION])
    const consumer = await group.consumer({
      startFrom: { kind: "first" },
      autoCommit: false,
      pollIntervalMs: 5
    })
    try {
      const delivered: bigint[] = []
      for (let index = 0; index < 2; index += 1) {
        const message = await consumer.nextWithin(READ_TIMEOUT_MS)
        assert.ok(message !== null)
        delivered.push(message.offset)
        await consumer.commit(message)
      }
      assert.deepEqual(delivered, [0n, 2n], "the server ran the group's filter")
    } finally {
      await consumer.shutdown()
    }
    const binding = await group.filter().get()
    assert.ok(binding !== undefined)
    assert.deepEqual(binding.digest, consumerFilterDigest(safeModeFilter()))
    assert.equal(binding.policyGeneration, 1n)
    await assert.rejects(
      group.filter().configure(ConsumerFilter.json(FilterExpr.present("kind"))),
      (error: unknown) => error instanceof FilterExecutionError && error.reason === "conflict"
    )
    const released = await group.filter().release()
    assert.deepEqual(released.digest, binding.digest)
    assert.equal(await group.filter().get(), undefined)
  })
})

void test("given_two_readers_when_acknowledging_another_readers_page_then_should_reject", async (context) => {
  await withSource(context, async (laser, stream) => {
    await publish(laser, stream, [SAFE_MODE])
    const first = await reader(laser, stream, "owner-one", (builder) =>
      builder.start({ kind: "first" })
    )
    const second = await reader(laser, stream, "owner-two", (builder) =>
      builder.start({ kind: "first" })
    )
    const firstPage = await first.nextPage({ timeoutMs: READ_TIMEOUT_MS })
    const secondPage = await second.nextPage({ timeoutMs: READ_TIMEOUT_MS })
    await assert.rejects(second.ackPage(firstPage), InvalidError)
    assert(firstPage.records[0] !== undefined)
    await assert.rejects(second.ack(firstPage.records[0]), InvalidError)
    await second.ackPage(secondPage)
    await first.close()
    await second.close()
  })
})

void test("given_buffered_records_when_reader_is_closed_then_should_not_yield_more", async (context) => {
  await withSource(context, async (laser, stream) => {
    await publish(laser, stream, [SAFE_MODE, DECOMMISSION])
    const filtered = await reader(laser, stream, "closed-reader", (builder) =>
      builder.start({ kind: "first" })
    )
    await filtered.nextRecord({ timeoutMs: READ_TIMEOUT_MS })
    await filtered.close()
    await assert.rejects(filtered.nextRecord(), ConfigError)
  })
})

void test("given_typed_custom_headers_when_filtered_then_should_preserve_types_and_payload", async (context) => {
  await withSource(context, async (laser, stream) => {
    const topic = laser.stream(stream).topic(TOPIC)
    for (const priority of [HeaderValue.uint8(1), HeaderValue.uint8(2), HeaderValue.string("2")]) {
      await topic.send(Uint8Array.of(0xff, 0x00), {
        partition: 0,
        headers: new Map([
          ["routing.priority", priority],
          ["armed", HeaderValue.bool(true)],
          ["temperature", HeaderValue.float(-1.5)],
          ["sequence", HeaderValue.uint64((1n << 64n) - 1n)]
        ])
      })
    }
    const filter = ConsumerFilter.headersOnly(
      FilterExpr.all([
        FilterExpr.header("routing.priority", "eq", 2),
        FilterExpr.header("armed", "eq", true),
        FilterExpr.header("temperature", "lt", 0),
        FilterExpr.header("sequence", "gt", (1n << 63n) - 1n)
      ])
    )
    const filtered = await reader(
      laser,
      stream,
      "typed-headers",
      (builder) => builder.start({ kind: "first" }).localGuard(true),
      filter
    )
    try {
      const record = await filtered.nextRecord({ timeoutMs: READ_TIMEOUT_MS })
      assert.equal(record.offset, 1n)
      assert.deepEqual(record.payload, Uint8Array.of(0xff, 0x00))
      await filtered.ack(record)
      assert.equal(await filtered.tryNextPage(), undefined)
    } finally {
      await filtered.close()
    }
  })
})

void test("given_a_fresh_filtered_reader_when_reading_next_then_should_start_at_zero", async (context) => {
  await withSource(context, async (laser, stream) => {
    await publish(laser, stream, [SAFE_MODE, DECOMMISSION])
    const first = await reader(laser, stream, "fresh-next", (builder) => builder.count(1))
    const zero = await first.nextRecord({ timeoutMs: READ_TIMEOUT_MS })
    assert.equal(zero.offset, 0n)
    await first.ack(zero)
    await first.close()
    const resumed = await reader(laser, stream, "fresh-next", (builder) => builder.count(1))
    assert.equal((await resumed.nextRecord({ timeoutMs: READ_TIMEOUT_MS })).offset, 1n)
    await resumed.close()
  })
})

void test("given_pass_through_decode_faults_when_guarded_then_should_verify_the_server_bounds", async (context) => {
  await withSource(context, async (laser, stream) => {
    await publish(laser, stream, ["broken JSON", SAFE_MODE])
    const filtered = await reader(
      laser,
      stream,
      "guarded-pass",
      (builder) => builder.start({ kind: "first" }).localGuard(true).count(2),
      { ...safeModeFilter(), faultPolicy: "pass" }
    )
    const page = await filtered.nextPage({ timeoutMs: READ_TIMEOUT_MS })
    assert.deepEqual(
      page.records.map((record) => [record.offset, record.evaluated]),
      [
        [0n, false],
        [1n, true]
      ]
    )
    await filtered.ackPage(page)
    await filtered.close()
  })
})

void test(
  "given_cluster_partition_primaries_when_a_group_consumes_then_should_read_and_commit_every_partition",
  { timeout: 120_000 },
  async () => {
    const cluster = await TestIggyCluster.start()
    const laser = await Laser.connect(cluster.endpoint)
    const stream = `laser-ts-filter-cluster-${randomUUID()}`
    try {
      await laser.stream(stream).ensure()
      const topic = laser.stream(stream).topic(TOPIC)
      await topic.ensure(9)
      for (let partition = 0; partition < 9; partition += 1)
        await topic.send(new TextEncoder().encode(SAFE_MODE), { partition })
      const consumer = await topic.consumerGroup("cluster-desk").consumer({
        startFrom: { kind: "first" },
        autoCommit: false,
        batchLength: 1,
        pollIntervalMs: 5
      })
      try {
        const seen = new Set<number>()
        for (let index = 0; index < 9; index += 1) {
          const message = await consumer.nextWithin(READ_TIMEOUT_MS)
          assert.ok(message !== null)
          assert.equal(message.offset, 0n)
          seen.add(message.partitionId)
          await consumer.commit(message)
        }
        assert.deepEqual(
          [...seen].sort((a, b) => a - b),
          [0, 1, 2, 3, 4, 5, 6, 7, 8]
        )
      } finally {
        await consumer.shutdown()
      }
    } finally {
      await laser
        .stream(stream)
        .delete()
        .catch(() => false)
      await laser.close()
      await cluster.close()
    }
  }
)

void test(
  "given_a_partition_data_connection_to_a_follower_when_authenticating_then_should_stay_on_that_node",
  { timeout: 120_000 },
  async () => {
    const cluster = await TestIggyCluster.start()
    let transport: ApacheIggyTransport | undefined
    try {
      const [leader, follower] = await cluster.leaderAndFollower()
      cluster.setNodeEnvironment(follower, { IGGY_PLANE_FILTERS_ENABLED: "false" })
      await cluster.restartNode(follower)
      cluster.routeEndpointTo(leader)
      transport = await ApacheIggyTransport.connect(cluster.endpoint)
      const endpoint = cluster.nodeEndpoint(follower)
      const data = await transport.openNodeConnection(endpoint.host, endpoint.port)
      try {
        const reply = decodeFilterReply(
          decodeOne(
            await data.send(
              AGDX_FILTER_VALIDATE_CODE,
              encodeNamed(encodeConsumerFilter(safeModeFilter()))
            ),
            "node validation"
          ),
          "node validation"
        )
        assert.equal(reply.kind, "err")
        assert.equal(reply.error.reason, "unsupported")
      } finally {
        await data.close()
      }
    } finally {
      await transport?.close()
      await cluster.close()
    }
  }
)

void test("given_more_than_64_outstanding_pages_when_configured_then_should_read_and_ack_the_prefix", async (context) => {
  await withSource(context, async (laser, stream) => {
    await publish(
      laser,
      stream,
      Array.from({ length: 80 }, () => SAFE_MODE)
    )
    const filtered = await reader(laser, stream, "large-window", (builder) =>
      builder.count(1).maxUnackedPages(80)
    )
    try {
      let last: Awaited<ReturnType<typeof filtered.nextPage>> | undefined
      for (let offset = 0; offset < 80; offset += 1) {
        last = await filtered.nextPage({ timeoutMs: READ_TIMEOUT_MS })
        assert.deepEqual(
          last.records.map((record) => record.offset),
          [BigInt(offset)]
        )
      }
      assert.ok(last?.records[0])
      await filtered.ackThrough(last.records[0])
    } finally {
      await filtered.close()
    }
  })
})

void test("given_a_group_filter_when_members_rejoin_then_should_resume_and_refuse_another_policy", async (context) => {
  await withSource(context, async (laser, stream) => {
    const group = await boundGroup(laser, stream, "review-group")
    const created = await group.info()
    try {
      await publish(laser, stream, [SAFE_MODE, GROUND_STATION, SAFE_MODE])
      const first = await group.reader().count(1).build()
      try {
        const page = await first.nextPage({ timeoutMs: READ_TIMEOUT_MS })
        assert.deepEqual(
          page.records.map((record) => record.offset),
          [0n]
        )
        await first.ackPage(page)
      } finally {
        await first.close()
      }
      const resumed = await laser
        .stream(stream)
        .topic(TOPIC)
        .consumerGroupId(created.id)
        .reader()
        .count(1)
        .build()
      try {
        const page = await resumed.nextPage({ timeoutMs: READ_TIMEOUT_MS })
        assert.deepEqual(
          page.records.map((record) => record.offset),
          [2n]
        )
        await resumed.ackPage(page)
      } finally {
        await resumed.close()
      }
      await assert.rejects(
        group.create({ filter: ConsumerFilter.json(FilterExpr.present("other")) }),
        (error: unknown) => error instanceof ConsumerGroupSetupError && error.reason === "conflict"
      )
      assert.deepEqual((await group.filter().get())?.digest, created.filter?.digest)
    } finally {
      await group.filter().release()
    }
  })
})

void test("given_a_recreated_topic_when_a_numeric_member_rejoins_then_should_leave_the_new_group_untouched", async (context) => {
  await withSource(context, async (laser, stream) => {
    const group = await boundGroup(laser, stream, "incarnation-group")
    const old = await group.info()
    const stale = await laser
      .stream(stream)
      .topic(TOPIC)
      .consumerGroupId(old.id)
      .reader()
      .idleInterval(0)
      .build()
    const native = new SingleClient(
      CONNECTION_STRING.includes("://") ? CONNECTION_STRING : `iggy://${CONNECTION_STRING}`
    )
    let recreated = false
    try {
      await native.topic.delete({ streamId: stream, topicId: TOPIC, partitionsCount: 1 })
      await laser.stream(stream).topic(TOPIC).ensure(1)
      const current = await group.create({ filter: safeModeFilter() })
      recreated = true
      assert.equal(current.id, old.id)
      assert.notDeepEqual(
        current.identity,
        old.identity,
        "a recreated topic is another group incarnation"
      )
      let refused = false
      for (let attempt = 0; attempt < 10 && !refused; attempt += 1) {
        try {
          await stale.tryNextPage()
        } catch (error) {
          if (error instanceof FilterExecutionError && error.reason === "not_found") refused = true
          else if (!(error instanceof FilterExecutionError && error.reason === "source_changed"))
            throw error
        }
      }
      assert.ok(refused, "the replaced source must be refused before rejoin")
      const fresh = await native.group.get({
        streamId: stream,
        topicId: TOPIC,
        groupId: current.id
      })
      assert.equal(fresh?.membersCount, 0)
    } finally {
      await stale.close()
      await native.destroy()
      if (recreated) await group.filter().release()
    }
  })
})
