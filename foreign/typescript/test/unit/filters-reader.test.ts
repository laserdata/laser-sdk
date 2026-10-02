import assert from "node:assert/strict"
import { test } from "node:test"
import { OPEN_CAPABILITIES, type Capabilities } from "../../src/client/capabilities.js"
import {
  FilterExecutionError,
  InvalidError,
  ProtocolError,
  UnsupportedError
} from "../../src/client/errors.js"
import type { CoordinatorConnection, NodeConnection } from "../../src/iggy/apache-iggy.js"
import {
  FilteredReader,
  FilteredReaderBuilder,
  Filters,
  sessionReuse,
  type FilterTransport
} from "../../src/managed/filters.js"
import { CompiledFilter } from "../../src/wire/filter-eval.js"
import { decodeOne, encodeNamed } from "../../src/wire/cbor.js"
import {
  AGDX_FILTERED_ACK_CODE,
  AGDX_FILTERED_POLL_CODE,
  FILTER_OP_VERSION
} from "../../src/wire/codes.js"
import {
  ConsumerFilter,
  FilterExpr,
  consumerFilterDigest,
  decodeFilteredPollRequest,
  encodeFilterReply,
  type FilteredPage,
  type FilteredPollRequest,
  type FilteredStart,
  type ReadMode
} from "../../src/wire/filter.js"

const ATTACH_CONSUMER_SESSION_CODE = 14
const GET_POLL_ROUTING_CODE = 103
const GET_CONSUMER_OFFSET_ROUTING_CODE = 123
const SYNC_CONSUMER_GROUP_CODE = 606

const filter = ConsumerFilter.json(FilterExpr.present("mode"))
const generation = {
  streamId: 1,
  streamCreatedAtMicros: 1n,
  topicId: 1,
  topicCreatedAtMicros: 1n,
  partitionId: 0,
  partitionCreatedRevision: 1n,
  purgeGeneration: 0n
}
const capabilities: Capabilities = {
  ...OPEN_CAPABILITIES,
  filters: { native: true, catalog: false, groupPolicyReads: true }
}

// A standard polled-messages body with one batch holding `offsets`.
function body(partitionId: number, frontier: bigint, offsets: readonly bigint[]): Uint8Array {
  const payload = new TextEncoder().encode(`{"mode":"safe"}`)
  const frameBytes = 48 + payload.byteLength
  const batchBytes = offsets.length === 0 ? 0 : 256 + frameBytes * offsets.length
  const out = new Uint8Array(16 + batchBytes)
  const view = new DataView(out.buffer)
  view.setUint32(0, partitionId, true)
  view.setBigUint64(4, frontier, true)
  view.setUint32(12, offsets.length, true)
  const base = offsets[0]
  if (base === undefined) return out
  view.setBigUint64(16 + 8, base, true)
  view.setBigUint64(16 + 16, 1_700_000_000_123_456n, true)
  view.setBigUint64(16 + 32, BigInt(batchBytes), true)
  view.setUint32(16 + 48, offsets.length, true)
  offsets.forEach((offset, index) => {
    const frame = 16 + 256 + index * frameBytes
    view.setUint32(frame + 24, Number(offset - base), true)
    view.setUint32(frame + 36, payload.byteLength, true)
    out.set(payload, frame + 48)
  })
  return out
}

function page(
  mode: ReadMode,
  overrides: Partial<FilteredPage> & { readonly offsets?: readonly bigint[] } = {}
): FilteredPage {
  const { offsets = [], ...fields } = overrides
  const partitionId = fields.partitionId ?? 0
  const nextScanOffset = fields.nextScanOffset ?? 1n
  return {
    v: FILTER_OP_VERSION,
    partitionId,
    policy: { digest: consumerFilterDigest(filter), mode: "filtered", policyGeneration: 0n },
    generation: { ...generation, partitionId },
    readMode: mode,
    nextScanOffset,
    ...(mode === "primary" ? { safeAckOffset: nextScanOffset - 1n } : {}),
    frontier: 1000n,
    examined: Math.max(1, offsets.length),
    matched: offsets.length,
    stop: "end_of_visible",
    unevaluated: [],
    records: body(partitionId, fields.frontier ?? 1000n, offsets),
    ...fields
  }
}

function pageReply(value: FilteredPage): Uint8Array {
  return encodeNamed(encodeFilterReply({ kind: "ok", outcome: { kind: "page", page: value } }))
}

function ackReply(offset: bigint): Uint8Array {
  return encodeNamed(
    encodeFilterReply({
      kind: "ok",
      outcome: { kind: "acknowledged", receipt: { partitionId: 0, offset, generation } }
    })
  )
}

function staleReply(message: string): Uint8Array {
  return encodeNamed(
    encodeFilterReply({
      kind: "err",
      error: { code: { kind: "known", name: "Stale" }, reason: "membership_stale", message }
    })
  )
}

function route(): Uint8Array {
  const reply = new Uint8Array(32 + 4 + 1 + 4 + 9 + 10)
  const view = new DataView(reply.buffer)
  view.setUint32(32, 1, true)
  reply[36] = 110
  view.setUint32(37, 9, true)
  reply.set(new TextEncoder().encode("127.0.0.1"), 41)
  view.setUint16(50, 8090, true)
  return reply
}

function assignment(generationId: bigint, partitions: readonly number[]): Uint8Array {
  const reply = new Uint8Array(12 + partitions.length * 4)
  const view = new DataView(reply.buffer)
  view.setBigUint64(0, generationId, true)
  view.setUint32(8, partitions.length, true)
  partitions.forEach((partitionId, index) => {
    view.setUint32(12 + index * 4, partitionId, true)
  })
  return reply
}

function pollOf(payload: Uint8Array): FilteredPollRequest {
  return decodeFilteredPollRequest(decodeOne(payload, "poll"), "poll")
}

function baseTransport(sendManaged: FilterTransport["sendManaged"]): FilterTransport {
  return {
    sendManaged,
    joinConsumerGroup: () => Promise.resolve(),
    leaveConsumerGroup: () => Promise.resolve(),
    connectsNodes: true
  }
}

function coordinatorOf(transport: FilterTransport): CoordinatorConnection {
  return {
    send: (code, payload) => transport.sendManaged(code, payload),
    joinConsumerGroup: () => Promise.resolve(),
    leaveConsumerGroup: () => Promise.resolve(),
    close: () => Promise.resolve()
  }
}

function reader(
  transport: FilterTransport,
  mode: ReadMode,
  guarded?: ConsumerFilter,
  layout: { readonly partitionIds?: readonly number[]; readonly idleIntervalMs?: number } = {}
): FilteredReader {
  const start: FilteredStart = { kind: "first" }
  return FilteredReader.create({
    transport,
    filters: new Filters(transport, () => Promise.resolve(capabilities)),
    coordinator: coordinatorOf(transport),
    request: {
      v: FILTER_OP_VERSION,
      source: { stream: "orbit", topic: "fleet_changes" },
      partitionId: 0,
      consumer: { kind: "consumer", name: "reader" },
      filter: { kind: "inline", filter },
      start,
      count: 10,
      maxReplyBytes: 1024 * 1024,
      readMode: mode
    },
    start,
    idleIntervalMs: layout.idleIntervalMs ?? 1,
    guard: guarded === undefined ? undefined : CompiledFilter.compile(guarded),
    membership: undefined,
    partitionIds: layout.partitionIds ?? [0]
  })
}

function groupReader(coordinator: CoordinatorConnection, data: NodeConnection) {
  const transport: FilterTransport = {
    ...baseTransport(() => Promise.reject(new Error("the shared transport is not used"))),
    openCoordinator: () => Promise.resolve(coordinator),
    openNodeConnection: () => Promise.resolve(data)
  }
  return FilteredReaderBuilder.create(
    transport,
    () => Promise.resolve(capabilities),
    new Filters(transport, () => Promise.resolve(capabilities)),
    { stream: "orbit", topic: "fleet_changes" },
    { kind: "group", name: "anomaly-desk" },
    { kind: "bound" }
  )
}

void test("given_a_server_without_group_reads_when_an_automatic_reader_is_built_then_should_reject_before_joining", async () => {
  let joined = false
  const transport = baseTransport(() => Promise.reject(new Error("no command is needed")))
  transport.joinConsumerGroup = () => {
    joined = true
    return Promise.resolve()
  }
  const outdated = (): Promise<Capabilities> =>
    Promise.resolve({
      ...capabilities,
      filters: { native: true, catalog: false, groupPolicyReads: false }
    })
  const automatic = FilteredReaderBuilder.create(
    transport,
    outdated,
    new Filters(transport, outdated),
    { stream: "orbit", topic: "fleet_changes" },
    { kind: "group", name: "anomaly-desk" },
    { kind: "group" }
  )
  await assert.rejects(automatic.build(), UnsupportedError)
  assert.equal(joined, false)
})

void test("given_an_unfiltered_page_when_read_then_should_need_an_automatic_group_read_that_delivered_everything", async () => {
  const unfiltered = (overrides: Partial<FilteredPage> = {}) =>
    page("local", {
      offsets: [0n],
      policy: { groupId: 3n, mode: "unfiltered", policyGeneration: 2n },
      ...overrides
    })
  const read = async (kind: "bound" | "group", served: FilteredPage) => {
    const transport = baseTransport(() => Promise.resolve(pageReply(served)))
    const filtered = FilteredReader.create({
      transport,
      filters: new Filters(transport, () => Promise.resolve(capabilities)),
      coordinator: coordinatorOf(transport),
      request: {
        v: FILTER_OP_VERSION,
        source: { stream: "orbit", topic: "fleet_changes" },
        partitionId: 0,
        consumer: { kind: "group", name: "anomaly-desk" },
        filter: { kind },
        start: { kind: "first" },
        count: 10,
        maxReplyBytes: 1024 * 1024,
        readMode: "local"
      },
      start: { kind: "first" },
      idleIntervalMs: 1,
      guard: undefined,
      membership: undefined,
      partitionIds: [0]
    })
    try {
      return await filtered.tryNextPage()
    } finally {
      await filtered.close()
    }
  }
  const found = await read("group", unfiltered())
  assert.equal(found?.policy.mode, "unfiltered")
  assert.equal(found.policy.digest, undefined)
  assert.equal(found.policy.policyGeneration, 2n)
  assert.equal(found.records[0]?.evaluated, false, "an unbound group evaluates no filter")
  await assert.rejects(
    read("bound", unfiltered()),
    ProtocolError,
    "a strict read never accepts an unfiltered page"
  )
  await assert.rejects(
    read("group", unfiltered({ examined: 3 })),
    ProtocolError,
    "an unfiltered page delivers everything it examined"
  )
})

void test("given_an_over_budget_page_when_a_bounded_group_read_returns_then_should_reject_the_progress", async () => {
  const transport = baseTransport(() =>
    Promise.resolve(
      pageReply(
        page("local", {
          offsets: [],
          examined: 3,
          nextScanOffset: 3n,
          policy: {
            groupId: 3n,
            digest: consumerFilterDigest(filter),
            mode: "filtered",
            policyGeneration: 0n
          }
        })
      )
    )
  )
  const filtered = FilteredReader.create({
    transport,
    filters: new Filters(transport, () => Promise.resolve(capabilities)),
    coordinator: coordinatorOf(transport),
    request: {
      v: FILTER_OP_VERSION,
      source: { stream: "orbit", topic: "fleet_changes" },
      partitionId: 0,
      consumer: { kind: "group", name: "anomaly-desk" },
      filter: { kind: "group" },
      start: { kind: "first" },
      count: 2,
      maxExamined: 2,
      maxReplyBytes: 1024,
      readMode: "local"
    },
    start: { kind: "first" },
    idleIntervalMs: 1,
    guard: undefined,
    membership: undefined,
    partitionIds: [0]
  })
  try {
    await assert.rejects(filtered.readRound(), ProtocolError)
  } finally {
    await filtered.close()
  }
})

void test("given_group_policy_reads_without_an_evaluator_when_an_automatic_reader_is_built_then_should_allow_unbound_reads", async () => {
  const transport = baseTransport(() => Promise.resolve(assignment(1n, [])))
  const withoutEvaluator = (): Promise<Capabilities> =>
    Promise.resolve({
      ...capabilities,
      filters: { native: false, catalog: false, groupPolicyReads: true }
    })
  const automatic = FilteredReaderBuilder.create(
    transport,
    withoutEvaluator,
    new Filters(transport, withoutEvaluator),
    { stream: "orbit", topic: "fleet_changes" },
    { kind: "group", name: "anomaly-desk" },
    { kind: "group" }
  )
  const built = await automatic.build()
  assert.equal(await built.tryNextPage(), undefined)
  await built.close()
})

void test("given_inconsistent_reply_metadata_when_reading_without_a_guard_then_should_reject", async () => {
  const forgedCount = new Uint8Array(16)
  new DataView(forgedCount.buffer).setUint32(12, 0xffffffff, true)
  const variants: readonly Partial<FilteredPage>[] = [
    { records: forgedCount },
    { readMode: "primary" },
    { safeAckOffset: 0n },
    { partitionId: 1 },
    { matched: 1 },
    { policy: { mode: "filtered", policyGeneration: 0n } },
    { examined: 0 }
  ]
  for (const variant of variants) {
    const transport = baseTransport(() => Promise.resolve(pageReply(page("local", variant))))
    const filtered = reader(transport, "local")
    await assert.rejects(filtered.tryNextPage(), ProtocolError)
    await filtered.close()
  }
})

void test("given_local_pages_when_never_acknowledged_then_should_keep_reading_past_the_pending_cap", async () => {
  let next = 0n
  const transport = baseTransport(() => {
    const offset = next
    next += 1n
    return Promise.resolve(
      pageReply(page("local", { offsets: [offset], nextScanOffset: offset + 1n }))
    )
  })
  const filtered = reader(transport, "local")
  for (let read = 0; read < 70; read += 1) {
    const found = await filtered.tryNextPage()
    assert.equal(found?.records.length, 1, `page ${String(read)} is read`)
    assert.equal(found.safeAckOffset, undefined, "a local page offers no acknowledgment")
    assert.equal(found.records[0]?.timestampMicros, 1_700_000_000_123_456n, "micros stay exact")
  }
  await filtered.close()
})

void test("given_a_checkpoint_inside_a_page_when_acknowledged_through_then_should_keep_the_later_record_pending", async () => {
  const stored: bigint[] = []
  const transport: FilterTransport = {
    ...baseTransport(() => Promise.resolve(route())),
    openNodeConnection: () =>
      Promise.resolve({
        close: () => Promise.resolve(),
        send: (code, payload) => {
          if (code === ATTACH_CONSUMER_SESSION_CODE) return Promise.resolve(new Uint8Array())
          if (code === AGDX_FILTERED_POLL_CODE)
            return Promise.resolve(
              pageReply(page("primary", { offsets: [3n, 7n], nextScanOffset: 10n }))
            )
          const ack = decodeOne(payload, "ack") as Map<string, unknown>
          const offset = BigInt(ack.get("offset") as number | bigint)
          stored.push(offset)
          return Promise.resolve(ackReply(offset))
        }
      })
  }
  const filtered = reader(transport, "primary")
  const found = await filtered.tryNextPage()
  assert.ok(found)
  const checkpoint = found.records[0]
  const later = found.records[1]
  assert.ok(checkpoint)
  assert.ok(later)
  await filtered.ackThrough(checkpoint)
  assert.deepEqual(stored, [3n])
  await filtered.ack(later)
  assert.deepEqual(stored, [3n, 9n])
  await filtered.close()
})

void test("given_a_lost_ack_route_when_retrying_then_should_use_offset_routing", async () => {
  const routed: number[] = []
  let acknowledgments = 0
  const transport: FilterTransport = {
    ...baseTransport((code) => {
      routed.push(code)
      return Promise.resolve(route())
    }),
    openNodeConnection: () =>
      Promise.resolve({
        close: () => Promise.resolve(),
        send: async (code) => {
          await Promise.resolve()
          if (code === ATTACH_CONSUMER_SESSION_CODE) return new Uint8Array()
          if (code === AGDX_FILTERED_POLL_CODE) return pageReply(page("primary"))
          assert.equal(code, AGDX_FILTERED_ACK_CODE)
          if (acknowledgments++ === 0)
            return encodeNamed(
              encodeFilterReply({
                kind: "err",
                error: {
                  code: { kind: "known", name: "Unavailable" },
                  reason: "not_primary",
                  message: "route changed"
                }
              })
            )
          return ackReply(0n)
        }
      })
  }
  const filtered = reader(transport, "primary")
  assert.equal(await filtered.tryNextPage(), undefined)
  assert.deepEqual(routed, [GET_POLL_ROUTING_CODE, GET_CONSUMER_OFFSET_ROUTING_CODE])
  assert.equal(acknowledgments, 2)
  await filtered.close()
})

void test("given_a_lost_ack_reply_when_the_next_ack_runs_then_should_store_the_same_target_again", async () => {
  const stored: bigint[] = []
  let failNext = true
  const transport: FilterTransport = {
    ...baseTransport(() => Promise.resolve(route())),
    openNodeConnection: () =>
      Promise.resolve({
        close: () => Promise.resolve(),
        send: (code, payload) => {
          if (code === ATTACH_CONSUMER_SESSION_CODE) return Promise.resolve(new Uint8Array())
          if (code === AGDX_FILTERED_POLL_CODE)
            return Promise.resolve(
              pageReply(page("primary", { offsets: [0n, 1n], nextScanOffset: 2n }))
            )
          const ack = decodeOne(payload, "ack") as Map<string, unknown>
          const offset = BigInt(ack.get("offset") as number | bigint)
          if (failNext) {
            failNext = false
            return Promise.reject(new Error("the reply was lost"))
          }
          stored.push(offset)
          return Promise.resolve(ackReply(offset))
        }
      })
  }
  const filtered = reader(transport, "primary")
  const found = await filtered.tryNextPage()
  assert(found !== undefined)
  const [first, second] = found.records
  assert(first !== undefined && second !== undefined)
  await filtered.ack(first)
  await assert.rejects(filtered.ack(second), /filtered command/)
  await filtered.ack({ ...second })
  assert.deepEqual(
    stored,
    [1n],
    "the unstored target is sent again, and a spread copy still acknowledges"
  )
  await filtered.close()
})

void test("given_a_rebalance_when_a_partition_is_gained_then_should_start_it_at_the_stored_offset", async () => {
  let assigned: readonly number[] = [0]
  let generationId = 1n
  let expireOnce = false
  const starts = new Map<number, string[]>()
  const coordinator: CoordinatorConnection = {
    send: (code) =>
      Promise.resolve(
        code === SYNC_CONSUMER_GROUP_CODE ? assignment(generationId, assigned) : route()
      ),
    joinConsumerGroup: () => Promise.resolve(),
    leaveConsumerGroup: () => Promise.resolve(),
    close: () => Promise.resolve()
  }
  const data: NodeConnection = {
    close: () => Promise.resolve(),
    send: (code, payload) => {
      if (code === ATTACH_CONSUMER_SESSION_CODE) return Promise.resolve(new Uint8Array())
      const request = pollOf(payload)
      starts.set(request.partitionId, [
        ...(starts.get(request.partitionId) ?? []),
        request.start.kind
      ])
      if (expireOnce && request.partitionId === 0) {
        expireOnce = false
        return Promise.resolve(staleReply("rebalanced"))
      }
      const {
        safeAckOffset: _safe,
        nextScanOffset: _next,
        ...unscanned
      } = page("primary", { partitionId: request.partitionId })
      return Promise.resolve(
        pageReply({
          ...unscanned,
          policy: {
            digest: consumerFilterDigest(filter),
            groupId: 3n,
            mode: "filtered",
            policyGeneration: 0n
          },
          examined: 0
        })
      )
    }
  }
  const filtered = await groupReader(coordinator, data).start({ kind: "last" }).build()
  await filtered.tryNextPage()
  assigned = [0, 1]
  generationId = 2n
  expireOnce = true
  await filtered.tryNextPage()
  await filtered.tryNextPage()
  assert.deepEqual(filtered.partitions(), [0, 1])
  assert.equal(starts.get(0)?.[0], "last", "the build start applies at the build")
  assert.equal(starts.get(1)?.[0], "next", "a gained partition resumes after the stored offset")
  await filtered.close()
})

void test("given_a_rejoin_when_an_old_record_is_acknowledged_then_should_name_the_rejoin", async () => {
  let rejoin = false
  const coordinator: CoordinatorConnection = {
    send: (code) => {
      if (code !== SYNC_CONSUMER_GROUP_CODE) return Promise.resolve(route())
      if (rejoin) {
        rejoin = false
        return Promise.resolve(new Uint8Array())
      }
      return Promise.resolve(assignment(1n, [0]))
    },
    joinConsumerGroup: () => Promise.resolve(),
    leaveConsumerGroup: () => Promise.resolve(),
    close: () => Promise.resolve()
  }
  let reads = 0
  const data: NodeConnection = {
    close: () => Promise.resolve(),
    send: (code) => {
      if (code === ATTACH_CONSUMER_SESSION_CODE) return Promise.resolve(new Uint8Array())
      reads += 1
      if (reads === 2) {
        rejoin = true
        return Promise.resolve(staleReply("the member was removed"))
      }
      return Promise.resolve(
        pageReply({
          ...page("primary", { offsets: [0n] }),
          policy: {
            digest: consumerFilterDigest(filter),
            groupId: 3n,
            mode: "filtered",
            policyGeneration: 0n
          }
        })
      )
    }
  }
  const filtered = await groupReader(coordinator, data).build()
  const before = await filtered.tryNextPage()
  const record = before?.records[0]
  assert(record !== undefined)
  await filtered.tryNextPage()
  await filtered.tryNextPage()
  await assert.rejects(filtered.ack(record), (error: unknown) => {
    assert(error instanceof InvalidError)
    assert.match(error.message, /rejoined its group/)
    return true
  })
  await filtered.close()
})

void test("given_one_failing_partition_when_reading_then_should_still_read_the_others", async () => {
  const transport: FilterTransport = {
    ...baseTransport(() => Promise.resolve(route())),
    openNodeConnection: () =>
      Promise.resolve({
        close: () => Promise.resolve(),
        send: (code, payload) => {
          if (code === ATTACH_CONSUMER_SESSION_CODE) return Promise.resolve(new Uint8Array())
          const request = pollOf(payload)
          if (request.partitionId === 0) return Promise.reject(new Error("node unreachable"))
          return Promise.resolve(pageReply(page("primary", { partitionId: 1, offsets: [0n] })))
        }
      })
  }
  const filtered = reader(transport, "primary", undefined, {
    partitionIds: [0, 1],
    idleIntervalMs: 60_000
  })
  const found = await filtered.tryNextPage()
  assert.equal(found?.partitionId, 1, "the failing partition does not starve the healthy one")
  await assert.rejects(filtered.tryNextPage(), /filtered command/)
  await filtered.close()
})

for (const refusals of [1, 2]) {
  void test(`given_${String(refusals)}_primary_refusals_when_reading_then_should_route_again_next_round_and_surface_a_repeat`, async () => {
    let polls = 0
    const transport: FilterTransport = {
      ...baseTransport(() => Promise.resolve(route())),
      openNodeConnection: () =>
        Promise.resolve({
          close: () => Promise.resolve(),
          send: (code) => {
            if (code === ATTACH_CONSUMER_SESSION_CODE) return Promise.resolve(new Uint8Array())
            assert.equal(code, AGDX_FILTERED_POLL_CODE)
            polls += 1
            return Promise.resolve(
              polls <= refusals
                ? encodeNamed(
                    encodeFilterReply({
                      kind: "err",
                      error: {
                        code: { kind: "known", name: "Unavailable" },
                        reason: "not_primary",
                        message: "owner settling"
                      }
                    })
                  )
                : pageReply(page("primary", { offsets: [0n] }))
            )
          }
        })
    }
    const filtered = reader(transport, "primary")
    try {
      // The lost route ends the round empty. The next round routes again.
      assert.equal(await filtered.tryNextPage(), undefined)
      if (refusals === 1) assert.equal((await filtered.tryNextPage())?.records[0]?.offset, 0n)
      else
        await assert.rejects(
          filtered.tryNextPage(),
          (error: unknown) =>
            error instanceof FilterExecutionError && error.reason === "not_primary"
        )
      assert.equal(polls, 2)
    } finally {
      await filtered.close()
    }
  })
}

// Session bytes: a u128 client id, a u64 session number, then the u64 metadata watermark.
function session(clientId: bigint, number: bigint, watermark: bigint): Uint8Array {
  const bytes = new Uint8Array(32)
  const view = new DataView(bytes.buffer)
  view.setBigUint64(0, clientId, true)
  view.setBigUint64(16, number, true)
  view.setBigUint64(24, watermark, true)
  return bytes
}

void test("given_session_bytes_when_compared_then_should_replace_the_connection_only_for_another_session", () => {
  assert.equal(sessionReuse(session(7n, 3n, 10n), session(7n, 3n, 12n)), "raise")
  assert.equal(sessionReuse(session(7n, 3n, 10n), session(7n, 3n, 10n)), "keep")
  assert.equal(sessionReuse(session(7n, 3n, 10n), session(7n, 3n, 9n)), "keep")
  assert.equal(sessionReuse(session(7n, 3n, 10n), session(7n, 4n, 10n)), "replace")
  assert.equal(sessionReuse(session(7n, 3n, 10n), session(8n, 3n, 10n)), "replace")
})

void test("given_two_partitions_on_one_node_and_a_rising_watermark_when_reading_then_should_keep_one_connection", async () => {
  let watermark = 0n
  let opened = 0
  const attached: bigint[] = []
  const transport: FilterTransport = {
    ...baseTransport(() => {
      watermark += 1n
      const reply = route()
      reply.set(session(7n, 3n, watermark), 0)
      return Promise.resolve(reply)
    }),
    openNodeConnection: () => {
      opened += 1
      return Promise.resolve({
        close: () => Promise.resolve(),
        send: (code, payload) => {
          if (code === ATTACH_CONSUMER_SESSION_CODE) {
            attached.push(new DataView(payload.buffer, payload.byteOffset).getBigUint64(24, true))
            return Promise.resolve(new Uint8Array())
          }
          if (code === AGDX_FILTERED_POLL_CODE) {
            const partitionId = pollOf(payload).partitionId
            return Promise.resolve(
              pageReply(
                page("primary", { partitionId, generation: { ...generation, partitionId } })
              )
            )
          }
          const ack = decodeOne(payload, "ack") as Map<string, unknown>
          const partitionId = Number(ack.get("partition_id"))
          const offset = BigInt(ack.get("offset") as number | bigint)
          return Promise.resolve(
            encodeNamed(
              encodeFilterReply({
                kind: "ok",
                outcome: {
                  kind: "acknowledged",
                  receipt: { partitionId, offset, generation: { ...generation, partitionId } }
                }
              })
            )
          )
        }
      })
    }
  }
  const built = reader(transport, "primary", undefined, { partitionIds: [0, 1] })
  for (let read = 0; read < 8; read += 1) {
    await built.tryNextPage()
  }
  assert.equal(opened, 1, "both partitions share the one data connection")
  assert.equal(built.dataConnectionsOpened(), 1)
  assert.deepEqual(attached, [1n, 2n], "the second route raised the floor on the open connection")
  await built.close()
})

void test("given_unevaluated_records_when_guarded_then_should_require_a_reproducible_pass_fault", async () => {
  const passing = { ...filter, faultPolicy: "pass" as const }
  for (const [definition, limit, accepted] of [
    [filter, 4, false],
    [passing, 4, true],
    [passing, 1024, false]
  ] as const) {
    const transport = baseTransport(() =>
      Promise.resolve(
        pageReply(
          page("local", {
            offsets: [0n],
            policy: {
              digest: consumerFilterDigest(definition),
              mode: "filtered",
              policyGeneration: 0n
            },
            unevaluated: [0n],
            evaluationLimits: { maxPayloadBytes: limit, maxDepth: 64 }
          })
        )
      )
    )
    const filtered = reader(transport, "local", definition)
    if (accepted) assert.equal((await filtered.tryNextPage())?.records.length, 1)
    else await assert.rejects(filtered.tryNextPage())
    await filtered.close()
  }
})

void test("given_a_revoked_partition_when_its_last_record_is_stored_then_should_release_the_reopened_route", async () => {
  let assigned: readonly number[] = [0]
  let generationId = 1n
  let polls = 0
  let closed = 0
  const coordinator: CoordinatorConnection = {
    send: (code) =>
      Promise.resolve(
        code === SYNC_CONSUMER_GROUP_CODE ? assignment(generationId, assigned) : route()
      ),
    joinConsumerGroup: () => Promise.resolve(),
    leaveConsumerGroup: () => Promise.resolve(),
    close: () => Promise.resolve()
  }
  const data: NodeConnection = {
    close: () => {
      closed += 1
      return Promise.resolve()
    },
    send: (code) => {
      if (code === ATTACH_CONSUMER_SESSION_CODE) return Promise.resolve(new Uint8Array())
      if (code === AGDX_FILTERED_ACK_CODE) return Promise.resolve(ackReply(0n))
      if (polls++ > 0) return Promise.resolve(staleReply("partition revoked"))
      return Promise.resolve(
        pageReply(
          page("primary", {
            offsets: [0n],
            stop: "filled",
            policy: {
              digest: consumerFilterDigest(filter),
              groupId: 3n,
              mode: "filtered",
              policyGeneration: 0n
            }
          })
        )
      )
    }
  }
  const filtered = await groupReader(coordinator, data).build()
  const record = (await filtered.tryNextPage())?.records[0]
  assert(record !== undefined)
  assigned = []
  generationId = 2n
  await filtered.tryNextPage()
  await filtered.tryNextPage()
  assert.deepEqual(filtered.partitions(), [])
  const before = closed
  await filtered.ack(record)
  assert.equal(closed, before + 1, "the acknowledgment-only route is released")
  await filtered.close()
})
