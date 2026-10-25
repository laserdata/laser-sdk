import assert from "node:assert/strict"
import { test } from "node:test"
import { OPEN_CAPABILITIES, type Capabilities } from "../../src/client/capabilities.js"
import {
  ConfigError,
  FilterExecutionError,
  InvalidError,
  ProtocolError,
  TransportError,
  UnsupportedError
} from "../../src/client/errors.js"
import type {
  CoordinatorConnection,
  FencedPosition,
  ManagedTarget
} from "../../src/iggy/apache-iggy.js"
import {
  FilteredReader,
  FilteredReaderBuilder,
  Filters,
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
  decodeFilteredAck,
  encodeFilterReply,
  type FilteredPage,
  type FilteredPollRequest,
  type FilteredStart
} from "../../src/wire/filter.js"

const SYNC_CONSUMER_GROUP_CODE = 606
const INVALID_IDENTIFIER = 6
const HISTORY_UNAVAILABLE = 87
const NOT_PRIMARY = 89
const CONSUMER_GROUP_PARTITION_NOT_OWNED = 5009

const filter = ConsumerFilter.json(FilterExpr.present("mode"))
const generation = {
  streamId: 1,
  streamCreatedAtMicros: 1n,
  topicId: 2,
  topicCreatedAtMicros: 1n,
  partitionId: 0,
  partitionIncarnation: 17n
}
const ackContext = { ownerGeneration: 4n, metadataOp: 40n }
const capabilities: Capabilities = {
  ...OPEN_CAPABILITIES,
  filters: { native: true, catalog: false, groupPolicyReads: true }
}

// The polled head: partition, current offset, count, then the partition context.
const HEAD = 40

// A standard polled-messages body with one batch holding `offsets`, read
// under the page's partition context.
function body(
  partitionId: number,
  frontier: bigint,
  offsets: readonly bigint[],
  context = { incarnation: 17n, ownerGeneration: 4n, metadataOp: 40n }
): Uint8Array {
  const payload = new TextEncoder().encode(`{"mode":"safe"}`)
  const frameBytes = 48 + payload.byteLength
  const batchBytes = offsets.length === 0 ? 0 : 256 + frameBytes * offsets.length
  const out = new Uint8Array(HEAD + batchBytes)
  const view = new DataView(out.buffer)
  view.setUint32(0, partitionId, true)
  view.setBigUint64(4, frontier, true)
  view.setUint32(12, offsets.length, true)
  view.setBigUint64(16, context.incarnation, true)
  view.setBigUint64(24, context.ownerGeneration, true)
  view.setBigUint64(32, context.metadataOp, true)
  const base = offsets[0]
  if (base === undefined) return out
  view.setBigUint64(HEAD + 8, base, true)
  view.setBigUint64(HEAD + 16, 1_700_000_000_123_456n, true)
  view.setBigUint64(HEAD + 32, BigInt(batchBytes), true)
  view.setUint32(HEAD + 48, offsets.length, true)
  offsets.forEach((offset, index) => {
    const frame = HEAD + 256 + index * frameBytes
    view.setUint32(frame + 24, Number(offset - base), true)
    view.setUint32(frame + 36, payload.byteLength, true)
    out.set(payload, frame + 48)
  })
  return out
}

function page(
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
    ackContext,
    nextScanOffset,
    safeAckOffset: nextScanOffset - 1n,
    frontier: 1000n,
    examined: Math.max(1, offsets.length),
    matched: offsets.length,
    stop: "end_of_visible",
    unevaluated: [],
    records: body(partitionId, fields.frontier ?? 1000n, offsets),
    ...fields
  }
}

// A page that examined nothing, which offers no acknowledgment offset.
function unscanned(partitionId: number): FilteredPage {
  const { safeAckOffset: _safe, nextScanOffset: _next, ...rest } = page({ partitionId })
  return {
    ...rest,
    policy: {
      digest: consumerFilterDigest(filter),
      groupId: 3n,
      mode: "filtered",
      policyGeneration: 0n
    },
    examined: 0
  }
}

function groupPage(overrides: Partial<FilteredPage> & { readonly offsets?: readonly bigint[] }) {
  return page({
    policy: {
      digest: consumerFilterDigest(filter),
      groupId: 3n,
      mode: "filtered",
      policyGeneration: 0n
    },
    ...overrides
  })
}

function pageReply(value: FilteredPage): Uint8Array {
  return encodeNamed(encodeFilterReply({ kind: "ok", outcome: { kind: "page", page: value } }))
}

function staleReply(message: string): Uint8Array {
  return encodeNamed(
    encodeFilterReply({
      kind: "err",
      error: { code: { kind: "known", name: "Stale" }, reason: "membership_stale", message }
    })
  )
}

// A server refusal as the transport surfaces it.
function refusal(errorCode: number, retryable = false): TransportError {
  return new TransportError(`refused with ${String(errorCode)}`, retryable, {
    cause: Object.assign(new Error("server refusal"), { errorCode })
  })
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

interface Coordinator {
  readonly connection: CoordinatorConnection
  readonly polls: { readonly target: ManagedTarget; readonly request: FilteredPollRequest }[]
  readonly stored: FencedPosition[]
  readonly lookups: number[]
}

/**
 * A coordinator whose polls `poll` answers. The route lookup names replica 1
 * unless `primary` says otherwise. Stores succeed unless `store` says
 * otherwise. Group syncs answer with `sync`.
 */
function coordinatorOf(spec: {
  readonly poll: (request: FilteredPollRequest, target: ManagedTarget) => Promise<Uint8Array>
  readonly primary?: (partitionId: number) => Promise<number>
  readonly store?: (position: FencedPosition) => Promise<void>
  readonly sync?: () => Promise<Uint8Array>
}): Coordinator {
  const polls: Coordinator["polls"] = []
  const stored: FencedPosition[] = []
  const lookups: number[] = []
  const connection: CoordinatorConnection = {
    send: (code) => {
      assert.equal(code, SYNC_CONSUMER_GROUP_CODE, "only the group sync rides the plain send")
      return spec.sync === undefined ? Promise.resolve(assignment(1n, [0])) : spec.sync()
    },
    sendManagedTo: async (code, payload, target) => {
      if (code === AGDX_FILTERED_ACK_CODE) {
        const ack = decodeFilteredAck(decodeOne(payload, "acknowledgment"), "acknowledgment")
        const position: FencedPosition = {
          streamId: ack.generation.streamId,
          topicId: ack.generation.topicId,
          consumer:
            ack.groupId === undefined
              ? {
                  kind: "consumer",
                  name: ack.consumer.kind === "consumer" ? ack.consumer.name : "reader"
                }
              : { kind: "group", id: Number(ack.groupId) },
          partitionId: ack.partitionId,
          offset: ack.offset,
          context: {
            incarnation: ack.generation.partitionIncarnation,
            ownerGeneration: ack.ackContext.ownerGeneration,
            metadataOp: ack.ackContext.metadataOp
          }
        }
        if (spec.store !== undefined) await spec.store(position)
        stored.push(position)
        return {
          reply: encodeNamed(
            encodeFilterReply({
              kind: "ok",
              outcome: {
                kind: "acknowledged",
                receipt: {
                  partitionId: ack.partitionId,
                  offset: ack.offset,
                  generation: ack.generation
                }
              }
            })
          )
        }
      }
      assert.equal(code, AGDX_FILTERED_POLL_CODE)
      const request = pollOf(payload)
      polls.push({ target, request })
      return { reply: await spec.poll(request, target) }
    },
    partitionPrimary: (_stream, _topic, partitionId) => {
      lookups.push(partitionId)
      return spec.primary === undefined ? Promise.resolve(1) : spec.primary(partitionId)
    },
    storePosition: async (position) => {
      if (spec.store !== undefined) await spec.store(position)
      stored.push(position)
    },
    joinConsumerGroup: () => Promise.resolve(),
    leaveConsumerGroup: () => Promise.resolve(),
    close: () => Promise.resolve()
  }
  return { connection, polls, stored, lookups }
}

function reader(
  coordinator: Coordinator,
  layout: {
    readonly guarded?: ConsumerFilter
    readonly partitionIds?: readonly number[]
    readonly idleIntervalMs?: number
    readonly maxUnackedPages?: number
  } = {}
): FilteredReader {
  const transport = baseTransport(() =>
    Promise.reject(new Error("the shared transport is not used"))
  )
  const start: FilteredStart = { kind: "first" }
  return FilteredReader.create({
    transport,
    filters: new Filters(transport, () => Promise.resolve(capabilities)),
    coordinator: coordinator.connection,
    request: {
      v: FILTER_OP_VERSION,
      source: { stream: "orbit", topic: "fleet_changes" },
      partitionId: 0,
      consumer: { kind: "consumer", name: "reader" },
      filter: { kind: "inline", filter },
      start,
      count: 10,
      maxReplyBytes: 1024 * 1024
    },
    start,
    idleIntervalMs: layout.idleIntervalMs ?? 1,
    ...(layout.maxUnackedPages === undefined ? {} : { maxUnackedPages: layout.maxUnackedPages }),
    guard: layout.guarded === undefined ? undefined : CompiledFilter.compile(layout.guarded),
    membership: undefined,
    partitionIds: layout.partitionIds ?? [0]
  })
}

function groupReader(coordinator: Coordinator) {
  const transport: FilterTransport = {
    ...baseTransport(() => Promise.reject(new Error("the shared transport is not used"))),
    openCoordinator: () => Promise.resolve(coordinator.connection)
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
  await assert.rejects(automatic.build(), (error: unknown) => {
    assert.ok(error instanceof UnsupportedError)
    assert.equal(error.surface, "filters")
    assert.equal(error.feature, "group_policy_reads")
    return true
  })
  assert.equal(joined, false)
})

void test("given_a_server_without_native_filters_when_a_bound_reader_is_built_then_should_reject_before_transport", async () => {
  let transportCalls = 0
  const unexpectedTransport = (): Promise<never> => {
    transportCalls += 1
    return Promise.reject(new Error("no transport is needed"))
  }
  const transport: FilterTransport = {
    ...baseTransport(unexpectedTransport),
    joinConsumerGroup: unexpectedTransport,
    openCoordinator: unexpectedTransport
  }
  const withoutNative = (): Promise<Capabilities> =>
    Promise.resolve({
      ...capabilities,
      filters: { native: false, catalog: false, groupPolicyReads: true }
    })
  const bound = FilteredReaderBuilder.create(
    transport,
    withoutNative,
    new Filters(transport, withoutNative),
    { stream: "orbit", topic: "fleet_changes" },
    { kind: "group", name: "anomaly-desk" },
    { kind: "bound" }
  )
  await assert.rejects(bound.build(), (error: unknown) => {
    assert.ok(error instanceof UnsupportedError)
    assert.equal(error.surface, "filters")
    assert.equal(error.feature, undefined)
    return true
  })
  assert.equal(transportCalls, 0)
})

void test("given_an_unfiltered_page_when_read_then_should_need_an_automatic_group_read_that_delivered_everything", async () => {
  const unfiltered = (overrides: Partial<FilteredPage> = {}) =>
    page({
      offsets: [0n],
      policy: { groupId: 3n, mode: "unfiltered", policyGeneration: 2n },
      ...overrides
    })
  const read = async (kind: "bound" | "group", served: FilteredPage) => {
    const coordinator = coordinatorOf({ poll: () => Promise.resolve(pageReply(served)) })
    const transport = baseTransport(() => Promise.reject(new Error("unused")))
    const filtered = FilteredReader.create({
      transport,
      filters: new Filters(transport, () => Promise.resolve(capabilities)),
      coordinator: coordinator.connection,
      request: {
        v: FILTER_OP_VERSION,
        source: { stream: "orbit", topic: "fleet_changes" },
        partitionId: 0,
        consumer: { kind: "group", name: "anomaly-desk" },
        filter: { kind },
        start: { kind: "first" },
        count: 10,
        maxReplyBytes: 1024 * 1024
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
  const coordinator = coordinatorOf({
    poll: () =>
      Promise.resolve(pageReply(groupPage({ offsets: [], examined: 3, nextScanOffset: 3n })))
  })
  const transport = baseTransport(() => Promise.reject(new Error("unused")))
  const filtered = FilteredReader.create({
    transport,
    filters: new Filters(transport, () => Promise.resolve(capabilities)),
    coordinator: coordinator.connection,
    request: {
      v: FILTER_OP_VERSION,
      source: { stream: "orbit", topic: "fleet_changes" },
      partitionId: 0,
      consumer: { kind: "group", name: "anomaly-desk" },
      filter: { kind: "group" },
      start: { kind: "first" },
      count: 2,
      maxExamined: 2,
      maxReplyBytes: 1024
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
  const forgedCount = new Uint8Array(HEAD)
  new DataView(forgedCount.buffer).setUint32(12, 0xffffffff, true)
  const { safeAckOffset: _safe, ...withoutAckOffset } = page()
  const variants: readonly FilteredPage[] = [
    page({ records: forgedCount }),
    page({ safeAckOffset: 5n }),
    withoutAckOffset,
    page({ partitionId: 1 }),
    page({ matched: 1 }),
    page({ policy: { mode: "filtered", policyGeneration: 0n } }),
    page({ examined: 0 }),
    page({
      records: body(0, 1000n, [], { incarnation: 17n, ownerGeneration: 5n, metadataOp: 40n })
    }),
    page({
      records: body(0, 1000n, [], { incarnation: 18n, ownerGeneration: 4n, metadataOp: 40n })
    }),
    page({
      records: body(0, 1000n, [], { incarnation: 17n, ownerGeneration: 4n, metadataOp: 41n })
    })
  ]
  for (const variant of variants) {
    const filtered = reader(coordinatorOf({ poll: () => Promise.resolve(pageReply(variant)) }))
    await assert.rejects(filtered.tryNextPage(), ProtocolError)
    await filtered.close()
  }
})

void test("given_stored_record_headers_when_matched_then_should_carry_them_to_the_record", async () => {
  const served = page({ offsets: [0n] })
  const records = served.records.slice()
  const view = new DataView(records.buffer)
  const frame = HEAD + 256
  view.setBigUint64(HEAD + 24, 1_000n, true)
  view.setBigUint64(frame, 42n, true)
  view.setBigUint64(frame + 8, 7n, true)
  view.setUint32(frame + 28, 5, true)
  const filtered = reader(
    coordinatorOf({ poll: () => Promise.resolve(pageReply({ ...served, records })) })
  )
  const record = (await filtered.tryNextPage())?.records[0]
  assert.equal(record?.message.checksum, 42n)
  assert.equal(record.message.messageId, 7n)
  assert.equal(record.message.originTimestampMicros, 1_005n)
  assert.equal(record.message.userHeaders, undefined)
  assert.equal(record.message.currentOffset, record.frontier)
  assert.equal(record.message.timestampMicros, 1_700_000_000_123_456n, "micros stay exact")
  await filtered.close()
})

void test("given_a_command_error_reply_when_reading_then_should_classify_its_result_code", async () => {
  const unsupported = encodeNamed(
    new Map<string, unknown>([
      ["code", "unsupported"],
      ["message", "filtered reads are not served"]
    ])
  )
  const filtered = reader(coordinatorOf({ poll: () => Promise.resolve(unsupported) }))
  await assert.rejects(
    filtered.tryNextPage(),
    (error: unknown) =>
      error instanceof UnsupportedError && error.message === "filtered reads are not served"
  )
  await filtered.close()
})

void test("given_unacknowledged_pages_when_the_cap_is_reached_then_should_refuse_to_read_more", async () => {
  let next = 0n
  const coordinator = coordinatorOf({
    poll: () => {
      const offset = next
      next += 1n
      return Promise.resolve(pageReply(page({ offsets: [offset], nextScanOffset: offset + 1n })))
    }
  })
  const filtered = reader(coordinator, { maxUnackedPages: 2 })
  for (let read = 0; read < 2; read += 1) {
    const found = await filtered.tryNextPage()
    assert.equal(found?.safeAckOffset, BigInt(read), "every page offers its acknowledgment")
  }
  await assert.rejects(filtered.tryNextPage(), (error: unknown) => {
    assert.ok(error instanceof InvalidError)
    assert.match(error.message, /unacknowledged pages/)
    return true
  })
  assert.equal(coordinator.polls.length, 2, "a saturated partition is not read")
  await filtered.close()
})

void test("given_a_known_route_when_reading_then_should_send_every_poll_to_the_partition_primary", async () => {
  let next = 0n
  const coordinator = coordinatorOf({
    primary: () => Promise.resolve(2),
    poll: () => {
      const offset = next
      next += 1n
      return Promise.resolve(pageReply(page({ offsets: [offset], nextScanOffset: offset + 1n })))
    }
  })
  const filtered = reader(coordinator)
  for (let read = 0; read < 3; read += 1) await filtered.tryNextPage()
  assert.deepEqual(
    coordinator.polls.map((poll) => poll.target),
    [
      { kind: "replica", replica: 2 },
      { kind: "replica", replica: 2 },
      { kind: "replica", replica: 2 }
    ]
  )
  assert.deepEqual(coordinator.lookups, [0], "the route is looked up once and kept")
  await filtered.close()
})

void test("given_a_route_the_lookup_cannot_give_when_reading_then_should_read_on_the_callers_node", async () => {
  let next = 0n
  const coordinator = coordinatorOf({
    primary: () => Promise.reject(refusal(1002)),
    poll: () => {
      const offset = next
      next += 1n
      return Promise.resolve(pageReply(page({ offsets: [offset], nextScanOffset: offset + 1n })))
    }
  })
  const filtered = reader(coordinator)
  assert.equal((await filtered.tryNextPage())?.records[0]?.offset, 0n)
  await filtered.tryNextPage()
  assert.deepEqual(
    coordinator.polls.map((poll) => poll.target),
    [{ kind: "caller" }, { kind: "caller" }]
  )
  assert.deepEqual(coordinator.lookups, [0, 0], "an unknown route is looked up again")
  await filtered.close()
})

for (const [label, failure] of [
  ["not_primary", refusal(NOT_PRIMARY)],
  ["invalid_identifier", refusal(INVALID_IDENTIFIER)],
  ["lost_link", new TransportError("the link to the replica was lost", true)]
] as const) {
  void test(`given_a_poll_failing_with_${label}_when_reading_then_should_look_the_route_up_again_and_resend_once`, async () => {
    let refused = false
    const leaders = [1, 2]
    const coordinator = coordinatorOf({
      primary: () => Promise.resolve(leaders.shift() ?? 2),
      poll: (_request, target) => {
        if (!refused) {
          refused = true
          return Promise.reject(failure)
        }
        assert.deepEqual(target, { kind: "replica", replica: 2 })
        return Promise.resolve(pageReply(page({ offsets: [0n] })))
      }
    })
    const filtered = reader(coordinator)
    assert.equal((await filtered.tryNextPage())?.records[0]?.offset, 0n)
    assert.deepEqual(
      coordinator.polls.map((poll) => poll.target),
      [
        { kind: "replica", replica: 1 },
        { kind: "replica", replica: 2 }
      ]
    )
    assert.deepEqual(coordinator.lookups, [0, 0])
    await filtered.close()
  })
}

void test("given_a_route_stale_twice_when_reading_then_should_surface_the_refusal_and_look_it_up_next_time", async () => {
  const coordinator = coordinatorOf({
    poll: () => Promise.reject(refusal(NOT_PRIMARY)),
    primary: () => Promise.resolve(1)
  })
  const filtered = reader(coordinator, { idleIntervalMs: 0 })
  await assert.rejects(filtered.tryNextPage(), TransportError)
  assert.equal(coordinator.polls.length, 2, "one resend after a fresh lookup")
  await assert.rejects(filtered.tryNextPage(), TransportError)
  assert.deepEqual(coordinator.lookups, [0, 0, 0, 0], "a stale route is never kept")
  await filtered.close()
})

void test("given_a_read_on_the_callers_node_when_it_fails_then_should_not_resend_it_here", async () => {
  const coordinator = coordinatorOf({
    primary: () => Promise.reject(refusal(1002)),
    poll: () => Promise.reject(refusal(NOT_PRIMARY))
  })
  const filtered = reader(coordinator)
  await assert.rejects(filtered.tryNextPage(), TransportError)
  assert.equal(coordinator.polls.length, 1, "the caller's node moves on inside the node choice")
  await filtered.close()
})

void test("given_a_checkpoint_inside_a_page_when_acknowledged_through_then_should_store_a_fenced_position", async () => {
  const coordinator = coordinatorOf({
    poll: () => Promise.resolve(pageReply(page({ offsets: [3n, 7n], nextScanOffset: 10n })))
  })
  const filtered = reader(coordinator)
  const found = await filtered.tryNextPage()
  assert.ok(found)
  const checkpoint = found.records[0]
  const later = found.records[1]
  assert.ok(checkpoint)
  assert.ok(later)
  await filtered.ackThrough(checkpoint)
  assert.deepEqual(coordinator.stored, [
    {
      streamId: 1,
      topicId: 2,
      consumer: { kind: "consumer", name: "reader" },
      partitionId: 0,
      offset: 3n,
      context: { incarnation: 17n, ownerGeneration: 4n, metadataOp: 40n }
    }
  ])
  await filtered.ack(later)
  assert.deepEqual(
    coordinator.stored.map((position) => position.offset),
    [3n, 9n]
  )
  await filtered.close()
})

void test("given_a_group_page_when_acknowledged_then_should_store_under_the_numeric_group_it_named", async () => {
  const coordinator = coordinatorOf({
    poll: () => Promise.resolve(pageReply(groupPage({ offsets: [0n] }))),
    sync: () => Promise.resolve(assignment(1n, [0]))
  })
  const filtered = await groupReader(coordinator).build()
  const record = (await filtered.tryNextPage())?.records[0]
  assert(record !== undefined)
  await filtered.ack(record)
  assert.deepEqual(coordinator.stored[0]?.consumer, { kind: "group", id: 3 })
  await filtered.close()
})

void test("given_a_lost_store_reply_when_the_next_ack_runs_then_should_store_the_same_target_again", async () => {
  let failures = 2
  const coordinator = coordinatorOf({
    poll: () => Promise.resolve(pageReply(page({ offsets: [0n, 1n], nextScanOffset: 2n }))),
    store: (position) => {
      if (position.offset === 1n && failures > 0) {
        failures -= 1
        return Promise.reject(new TransportError("the reply was lost", true))
      }
      return Promise.resolve()
    }
  })
  const filtered = reader(coordinator)
  const found = await filtered.tryNextPage()
  assert(found !== undefined)
  const [first, second] = found.records
  assert(first !== undefined && second !== undefined)
  await filtered.ack(first)
  await assert.rejects(filtered.ack(second), /the reply was lost/)
  await filtered.ack({ ...second })
  assert.deepEqual(
    coordinator.stored.map((position) => position.offset),
    [1n],
    "the unstored target is sent again, and a spread copy still acknowledges"
  )
  await filtered.close()
})

void test("given_a_changed_history_when_storing_then_should_restart_the_partition_from_its_stored_offset", async () => {
  const coordinator = coordinatorOf({
    poll: () => Promise.resolve(pageReply(page({ offsets: [0n] }))),
    store: () => Promise.reject(refusal(HISTORY_UNAVAILABLE))
  })
  const filtered = reader(coordinator)
  const record = (await filtered.tryNextPage())?.records[0]
  assert(record !== undefined)
  await assert.rejects(filtered.ack(record), TransportError)
  await filtered.tryNextPage().catch(() => undefined)
  assert.deepEqual(
    coordinator.polls.map((poll) => poll.request.start.kind),
    ["first", "next"],
    "the partition starts over after its stored offset"
  )
  assert.deepEqual(coordinator.lookups, [0, 0], "a recreated partition is routed again")
  await filtered.close()
})

void test("given_a_moved_assignment_when_storing_then_should_sync_the_group_before_the_next_read", async () => {
  let syncs = 0
  let next = 0n
  const coordinator = coordinatorOf({
    poll: () => {
      const offset = next
      next += 1n
      return Promise.resolve(
        pageReply(groupPage({ offsets: [offset], nextScanOffset: offset + 1n }))
      )
    },
    store: () => Promise.reject(refusal(CONSUMER_GROUP_PARTITION_NOT_OWNED)),
    sync: () => {
      syncs += 1
      return Promise.resolve(assignment(1n, [0]))
    }
  })
  const filtered = await groupReader(coordinator).build()
  const record = (await filtered.tryNextPage())?.records[0]
  assert(record !== undefined)
  const before = syncs
  await assert.rejects(filtered.ack(record), TransportError)
  await filtered.tryNextPage()
  assert.equal(syncs, before + 1, "a refused store expires the membership")
  await filtered.close().catch(() => undefined)
})

void test("given_a_delayed_first_assignment_when_reading_then_should_preserve_the_requested_start", async () => {
  let assigned: readonly number[] = []
  const coordinator = coordinatorOf({
    sync: () => Promise.resolve(assignment(1n, assigned)),
    poll: (request) =>
      Promise.resolve(pageReply({ ...unscanned(request.partitionId), nextScanOffset: 0n }))
  })
  const filtered = await groupReader(coordinator).start({ kind: "first" }).build()
  await filtered.tryNextPage()
  assigned = [0]
  await filtered.tryNextPage()
  assert.deepEqual(coordinator.polls[0]?.request.start, { kind: "first" })
  await filtered.close()
})

void test("given_a_rebalance_when_a_partition_is_gained_then_should_start_it_at_the_stored_offset", async () => {
  let assigned: readonly number[] = [0]
  let generationId = 1n
  let expireOnce = false
  const coordinator = coordinatorOf({
    sync: () => Promise.resolve(assignment(generationId, assigned)),
    poll: (request) => {
      if (expireOnce && request.partitionId === 0) {
        expireOnce = false
        return Promise.resolve(staleReply("rebalanced"))
      }
      return Promise.resolve(pageReply(unscanned(request.partitionId)))
    }
  })
  const filtered = await groupReader(coordinator).start({ kind: "last" }).build()
  await filtered.tryNextPage()
  assigned = [0, 1]
  generationId = 2n
  expireOnce = true
  await filtered.tryNextPage()
  await filtered.tryNextPage()
  assert.deepEqual(filtered.partitions(), [0, 1])
  const starts = (partitionId: number) =>
    coordinator.polls
      .filter((poll) => poll.request.partitionId === partitionId)
      .map((poll) => poll.request.start.kind)
  assert.equal(starts(0)[0], "last", "the build start applies at the build")
  assert.equal(starts(1)[0], "next", "a gained partition resumes after the stored offset")
  await filtered.close()
})

void test("given_a_rejoin_when_an_old_record_is_acknowledged_then_should_name_the_rejoin", async () => {
  let rejoin = false
  let reads = 0
  const coordinator = coordinatorOf({
    sync: () => {
      if (rejoin) {
        rejoin = false
        return Promise.resolve(new Uint8Array())
      }
      return Promise.resolve(assignment(1n, [0]))
    },
    poll: () => {
      reads += 1
      if (reads === 2) {
        rejoin = true
        return Promise.resolve(staleReply("the member was removed"))
      }
      return Promise.resolve(pageReply(groupPage({ offsets: [0n] })))
    }
  })
  const filtered = await groupReader(coordinator).build()
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
  const coordinator = coordinatorOf({
    poll: (request) =>
      request.partitionId === 0
        ? Promise.reject(new Error("node unreachable"))
        : Promise.resolve(pageReply(page({ partitionId: 1, offsets: [0n] })))
  })
  const filtered = reader(coordinator, { partitionIds: [0, 1], idleIntervalMs: 60_000 })
  const found = await filtered.tryNextPage()
  assert.equal(found?.partitionId, 1, "the failing partition does not starve the healthy one")
  await assert.rejects(filtered.tryNextPage(), /node unreachable/)
  await filtered.close()
})

for (const losses of [1, 2]) {
  void test(`given_${String(losses)}_lost_assignments_when_reading_then_should_sync_the_next_round_and_surface_a_repeat`, async () => {
    let polls = 0
    const coordinator = coordinatorOf({
      poll: () => {
        polls += 1
        return Promise.resolve(
          polls <= losses ? staleReply("owner settling") : pageReply(page({ offsets: [0n] }))
        )
      }
    })
    const filtered = reader(coordinator)
    try {
      // The lost assignment ends the round empty. The next round reads again.
      assert.equal(await filtered.tryNextPage(), undefined)
      if (losses === 1) assert.equal((await filtered.tryNextPage())?.records[0]?.offset, 0n)
      else
        await assert.rejects(
          filtered.tryNextPage(),
          (error: unknown) =>
            error instanceof FilterExecutionError && error.detail.reason === "membership_stale"
        )
      assert.equal(polls, 2)
      assert.deepEqual(coordinator.lookups, [0, 0], "a lost assignment drops the route")
    } finally {
      await filtered.close()
    }
  })
}

void test("given_unevaluated_records_when_guarded_then_should_require_a_reproducible_pass_fault", async () => {
  const passing = { ...filter, faultPolicy: "pass" as const }
  for (const [definition, limit, accepted] of [
    [filter, 4, false],
    [passing, 4, true],
    [passing, 1024, false]
  ] as const) {
    const coordinator = coordinatorOf({
      poll: () =>
        Promise.resolve(
          pageReply(
            page({
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
    })
    const filtered = reader(coordinator, { guarded: definition })
    if (accepted) assert.equal((await filtered.tryNextPage())?.records.length, 1)
    else await assert.rejects(filtered.tryNextPage())
    await filtered.close()
  }
})

void test("given_a_revoked_partition_when_its_last_record_is_stored_then_should_retire_it", async () => {
  let assigned: readonly number[] = [0]
  let generationId = 1n
  let polls = 0
  const coordinator = coordinatorOf({
    sync: () => Promise.resolve(assignment(generationId, assigned)),
    poll: () =>
      Promise.resolve(
        polls++ > 0
          ? staleReply("partition revoked")
          : pageReply(groupPage({ offsets: [0n], stop: "filled" }))
      )
  })
  const filtered = await groupReader(coordinator).build()
  const record = (await filtered.tryNextPage())?.records[0]
  assert(record !== undefined)
  assigned = []
  generationId = 2n
  await filtered.tryNextPage()
  await filtered.tryNextPage()
  assert.deepEqual(filtered.partitions(), [])
  await filtered.ack(record)
  assert.deepEqual(
    coordinator.stored.map((position) => position.offset),
    [0n],
    "a revoked partition still stores what it handed out"
  )
  await assert.rejects(filtered.ack(record), FilterExecutionError, "and then it is retired")
  await filtered.close()
})

void test("given_an_injected_client_without_targeted_requests_when_reading_then_should_name_what_is_missing", async () => {
  const transport = baseTransport(() => Promise.resolve(assignment(1n, [0])))
  const built = await FilteredReaderBuilder.create(
    transport,
    () => Promise.resolve(capabilities),
    new Filters(transport, () => Promise.resolve(capabilities)),
    { stream: "orbit", topic: "fleet_changes" },
    { kind: "group", name: "anomaly-desk" },
    { kind: "bound" }
  ).build()
  await assert.rejects(built.tryNextPage(), ConfigError)
  await built.close()
})
