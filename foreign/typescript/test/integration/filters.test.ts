import { SingleClient } from "apache-iggy"
import { FilterExecutionError } from "../../src/client/errors.js"
import { decodeOne, encodeNamed } from "../../src/wire/cbor.js"
import { AGDX_FILTER_VALIDATE_CODE } from "../../src/wire/codes.js"
import { ApacheIggyTransport } from "../../src/iggy/apache-iggy.js"
import { TestIggyCluster } from "../support/test-iggy.js"
import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import {
  ConfigError,
  FilterStopError,
  InvalidError,
  UnsupportedError
} from "../../src/client/errors.js"
import { Laser } from "../../src/client/laser.js"
import { HeaderValue } from "../../src/stream/header-value.js"
import type { FilteredReaderBuilder } from "../../src/managed/filters.js"
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

async function withSource(run: (laser: Laser, stream: string) => Promise<void>): Promise<void> {
  const laser = await Laser.connect(CONNECTION_STRING)
  const stream = `laser-ts-orbit-${randomUUID()}`
  try {
    await laser.stream(stream).ensure()
    await laser.stream(stream).topic(TOPIC).ensure(1)
    await run(laser, stream)
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

function reader(laser: Laser, stream: string, consumer: string): FilteredReaderBuilder {
  return laser.filters().reader(stream, TOPIC).consumer(consumer).inline(safeModeFilter())
}

void test("given_edge_records_when_read_inline_then_should_return_only_matches_at_their_offsets", async () => {
  await withSource(async (laser, stream) => {
    await publish(laser, stream, [
      SAFE_MODE,
      SAFE_MODE_VALUES,
      DECOMMISSION,
      TELEMETRY,
      GROUND_STATION
    ])
    const filtered = await reader(laser, stream, "safe-mode-backfill")
      .start({ kind: "first" })
      .build()

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

void test("given_stored_progress_when_reading_next_then_should_read_only_new_matches", async () => {
  await withSource(async (laser, stream) => {
    await publish(laser, stream, [SAFE_MODE, GROUND_STATION])
    const first = await reader(laser, stream, "safe-mode-resume").start({ kind: "first" }).build()
    await first.ackPage(await first.nextPage({ timeoutMs: READ_TIMEOUT_MS }))
    await first.close()

    await publish(laser, stream, [GROUND_STATION, DECOMMISSION])
    const second = await reader(laser, stream, "safe-mode-resume").build()
    const page = await second.nextPage({ timeoutMs: READ_TIMEOUT_MS })

    assert.deepEqual(
      page.records.map((record) => record.offset),
      [3n]
    )
    await second.close()
  })
})

void test("given_only_non_matches_when_reading_then_should_store_the_scanned_range", async () => {
  await withSource(async (laser, stream) => {
    await publish(laser, stream, [GROUND_STATION, GROUND_STATION, GROUND_STATION])
    const first = await reader(laser, stream, "safe-mode-sparse").start({ kind: "first" }).build()
    assert.equal(await first.tryNextPage(), undefined)
    await first.close()

    await publish(laser, stream, [DECOMMISSION])
    const second = await reader(laser, stream, "safe-mode-sparse").build()
    const page = await second.nextPage({ timeoutMs: READ_TIMEOUT_MS })

    assert.deepEqual(
      page.records.map((record) => record.offset),
      [3n],
      "the empty page stored the range it examined"
    )
    await second.close()
  })
})

void test("given_out_of_order_acks_when_acked_then_should_store_only_the_completed_prefix", async () => {
  await withSource(async (laser, stream) => {
    await publish(laser, stream, [SAFE_MODE, DECOMMISSION])
    const first = await reader(laser, stream, "safe-mode-ordered")
      .start({ kind: "first" })
      .count(1)
      .build()
    const earlier = await first.nextRecord({ timeoutMs: READ_TIMEOUT_MS })
    const later = await first.nextRecord({ timeoutMs: READ_TIMEOUT_MS })
    await first.ack(later)
    await first.close()

    const second = await reader(laser, stream, "safe-mode-ordered").build()
    const record = await second.nextRecord({ timeoutMs: READ_TIMEOUT_MS })

    assert.deepEqual([earlier.offset, later.offset], [0n, 1n])
    assert.equal(record.offset, 0n, "the earlier record was never completed")
    await second.close()
  })
})

void test("given_a_malformed_record_when_reading_then_should_deliver_matches_then_the_fault", async () => {
  await withSource(async (laser, stream) => {
    await publish(laser, stream, [SAFE_MODE, "not json", DECOMMISSION])
    const filtered = await reader(laser, stream, "safe-mode-fault").start({ kind: "first" }).build()

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

void test("given_a_local_guard_when_reading_then_should_agree_with_the_server", async () => {
  await withSource(async (laser, stream) => {
    await publish(laser, stream, [GROUND_STATION, TELEMETRY])
    const filtered = await reader(laser, stream, "safe-mode-guarded")
      .start({ kind: "first" })
      .localGuard(true)
      .build()

    const page = await filtered.nextPage({ timeoutMs: READ_TIMEOUT_MS })

    assert.deepEqual(
      page.records.map((record) => record.offset),
      [1n]
    )
    await filtered.close()
  })
})

void test("given_a_local_reader_when_acknowledging_then_should_refuse", async () => {
  await withSource(async (laser, stream) => {
    await publish(laser, stream, [DECOMMISSION])
    const filtered = await reader(laser, stream, "safe-mode-local")
      .start({ kind: "first" })
      .readMode("local")
      .build()
    const page = await filtered.nextPage({ timeoutMs: READ_TIMEOUT_MS })

    await assert.rejects(filtered.ackPage(page), InvalidError)
    await filtered.close()
  })
})

void test("given_stored_records_when_previewed_then_should_judge_them_without_progress", async () => {
  await withSource(async (laser, stream) => {
    await publish(laser, stream, [SAFE_MODE, SAFE_MODE_VALUES, DECOMMISSION])

    const preview = await laser.filters().preview(
      stream,
      TOPIC,
      0,
      { kind: "inline", filter: safeModeFilter() },
      {
        maxRecords: 10,
        explain: true
      }
    )

    assert.equal(preview.examined, 3)
    assert.equal(preview.matched, 2)
    const verdicts = preview.records.map(
      (record) => `${record.offset.toString()}:${record.verdict}`
    )
    assert.ok(verdicts.includes("1:rejected"))
    assert.ok(verdicts.includes("2:selected"))
  })
})

void test("given_a_sample_when_tested_and_validated_then_should_explain_and_digest", async () => {
  await withSource(async (laser) => {
    const tested = await laser
      .filters()
      .test({ kind: "inline", filter: safeModeFilter() }, SAFE_MODE_VALUES)
    const validation = await laser.filters().validate(safeModeFilter())

    assert.equal(tested.explanation.verdict, "rejected")
    assert.deepEqual(validation.digest, consumerFilterDigest(safeModeFilter()))
    assert.equal(validation.readsPayload, true)
  })
})

void test("given_no_catalog_when_listing_saved_filters_then_should_refuse_as_unsupported", async (context) => {
  await withSource(async (laser) => {
    if ((await laser.capabilities()).filters.catalog) {
      context.skip("this deployment serves the saved-filter catalog")
      return
    }
    await assert.rejects(laser.filters().list(), UnsupportedError)
  })
})

void test("given_two_readers_when_acknowledging_another_readers_page_then_should_reject", async () => {
  await withSource(async (laser, stream) => {
    await publish(laser, stream, [SAFE_MODE])
    const first = await reader(laser, stream, "owner-one").start({ kind: "first" }).build()
    const second = await reader(laser, stream, "owner-two").start({ kind: "first" }).build()
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

void test("given_buffered_records_when_reader_is_closed_then_should_not_yield_more", async () => {
  await withSource(async (laser, stream) => {
    await publish(laser, stream, [SAFE_MODE, DECOMMISSION])
    const filtered = await reader(laser, stream, "closed-reader").start({ kind: "first" }).build()
    await filtered.nextRecord({ timeoutMs: READ_TIMEOUT_MS })
    await filtered.close()
    await assert.rejects(filtered.nextRecord(), ConfigError)
  })
})

void test("given_typed_custom_headers_when_filtered_then_should_preserve_types_and_payload", async () => {
  await withSource(async (laser, stream) => {
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
    const filtered = await laser
      .filters()
      .reader(stream, TOPIC)
      .consumer("typed-headers")
      .inline(filter)
      .start({ kind: "first" })
      .localGuard(true)
      .build()
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

void test("given_a_fresh_filtered_reader_when_reading_next_then_should_start_at_zero", async () => {
  await withSource(async (laser, stream) => {
    await publish(laser, stream, [SAFE_MODE, DECOMMISSION])
    const first = await reader(laser, stream, "fresh-next").count(1).build()
    const zero = await first.nextRecord({ timeoutMs: READ_TIMEOUT_MS })
    assert.equal(zero.offset, 0n)
    await first.ack(zero)
    await first.close()
    const resumed = await reader(laser, stream, "fresh-next").count(1).build()
    assert.equal((await resumed.nextRecord({ timeoutMs: READ_TIMEOUT_MS })).offset, 1n)
    await resumed.close()
  })
})

void test("given_pass_through_decode_faults_when_guarded_then_should_verify_the_server_bounds", async () => {
  await withSource(async (laser, stream) => {
    await publish(laser, stream, ["broken JSON", SAFE_MODE])
    const filtered = await laser
      .filters()
      .reader(stream, TOPIC)
      .consumer("guarded-pass")
      .inline({ ...safeModeFilter(), faultPolicy: "pass" })
      .start({ kind: "first" })
      .localGuard(true)
      .count(2)
      .build()
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
  "given_cluster_partition_primaries_when_reading_filtered_then_should_read_and_ack_every_partition",
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
      const reader = await laser
        .filters()
        .reader(stream, TOPIC)
        .consumer("cluster-filter")
        .inline(safeModeFilter())
        .count(1)
        .build()
      try {
        const seen = new Set<number>()
        for (let index = 0; index < 9; index += 1) {
          const page = await reader.nextPage({ timeoutMs: READ_TIMEOUT_MS })
          assert.equal(page.records.length, 1)
          assert.equal(page.records[0]?.offset, 0n)
          seen.add(page.partitionId)
          await reader.ackPage(page)
        }
        assert.deepEqual(
          [...seen].sort((a, b) => a - b),
          [0, 1, 2, 3, 4, 5, 6, 7, 8]
        )
      } finally {
        await reader.close()
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

void test("given_more_than_64_outstanding_pages_when_configured_then_should_read_and_ack_the_prefix", async () => {
  await withSource(async (laser, stream) => {
    await publish(
      laser,
      stream,
      Array.from({ length: 80 }, () => SAFE_MODE)
    )
    const filtered = await reader(laser, stream, "large-window")
      .count(1)
      .maxUnackedPages(80)
      .build()
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

void test("given_a_saved_group_binding_when_members_rejoin_then_should_resume_and_refuse_a_conflicting_filter", async (context) => {
  await withSource(async (laser, stream) => {
    if (!(await laser.capabilities()).filters.catalog) {
      context.skip("requires the managed filter catalog")
      return
    }
    const filters = laser.filters()
    const saved = await filters.register(`group-${randomUUID()}`, safeModeFilter())
    const group = { stream, topic: TOPIC, group: "review-group" }
    const binding = await filters.createConsumerGroup(group, saved.filterId, saved.revision)
    try {
      await publish(laser, stream, [SAFE_MODE, GROUND_STATION, SAFE_MODE])
      const first = await filters.reader(stream, TOPIC).group(group.group).count(1).build()
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
      const resumed = await filters
        .reader(stream, TOPIC)
        .groupId(binding.identity.groupId)
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
      const conflict = await filters
        .reader(stream, TOPIC)
        .group(group.group)
        .inline(ConsumerFilter.json(FilterExpr.present("other")))
        .build()
      try {
        await assert.rejects(
          conflict.tryNextPage(),
          (error: unknown) => error instanceof FilterExecutionError && error.reason === "conflict"
        )
      } finally {
        await conflict.close()
      }
    } finally {
      await filters.unbindBinding(binding)
      await filters.delete(saved.filterId)
    }
  })
})

void test("given_a_recreated_topic_when_a_numeric_member_rejoins_then_should_leave_the_new_group_untouched", async (context) => {
  await withSource(async (laser, stream) => {
    if (!(await laser.capabilities()).filters.catalog) {
      context.skip("requires the managed filter catalog")
      return
    }
    const filters = laser.filters()
    const saved = await filters.register(`recreated-${randomUUID()}`, safeModeFilter())
    const group = { stream, topic: TOPIC, group: "incarnation-group" }
    const old = await filters.createConsumerGroup(group, saved.filterId, saved.revision)
    const stale = await filters
      .reader(stream, TOPIC)
      .groupId(old.identity.groupId)
      .idleInterval(0)
      .build()
    const native = new SingleClient(
      CONNECTION_STRING.includes("://") ? CONNECTION_STRING : `iggy://${CONNECTION_STRING}`
    )
    let current: typeof old | undefined
    try {
      await native.topic.delete({ streamId: stream, topicId: TOPIC, partitionsCount: 1 })
      await laser.stream(stream).topic(TOPIC).ensure(1)
      current = await filters.createConsumerGroup(group, saved.filterId, saved.revision)
      assert.equal(current.identity.groupId, old.identity.groupId)
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
        groupId: Number(current.identity.groupId)
      })
      assert.equal(fresh?.membersCount, 0)
    } finally {
      await stale.close()
      await native.destroy()
      await filters.unbindBinding(old)
      if (current !== undefined) await filters.unbindBinding(current)
      await filters.delete(saved.filterId)
    }
  })
})
