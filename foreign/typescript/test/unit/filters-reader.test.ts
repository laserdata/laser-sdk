import assert from "node:assert/strict"
import { test } from "node:test"
import { OPEN_CAPABILITIES, type Capabilities } from "../../src/client/capabilities.js"
import {
  CancelledError,
  ConfigError,
  FilterExecutionError,
  FilterFaultError,
  InvalidError,
  ProtocolError,
  TimeoutError,
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
  AGDX_FILTERED_WAIT_CANCEL_CODE,
  AGDX_FILTERED_WAIT_CODE,
  FILTER_OP_VERSION
} from "../../src/wire/codes.js"
import {
  ConsumerFilter,
  FilterExpr,
  consumerFilterDigest,
  decodeFilteredPollRequest,
  decodeFilteredAck,
  decodeFilteredWaitCancel,
  decodeFilteredWaitRequest,
  encodeFilterReply,
  type FilteredAck,
  type FilteredPage,
  type FilteredPollRequest,
  type FilteredStart,
  type FilteredWaitCancel,
  type FilteredWaitRequest
} from "../../src/wire/filter.js"
import { MAX_FILTERED_WAIT_PARTITIONS } from "../../src/wire/limits.js"

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
  filters: { native: true, catalog: false, groupPolicyReads: true, maxWaitMs: 0 }
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
  readonly waits: {
    readonly target: ManagedTarget
    readonly request: FilteredWaitRequest
    readonly deadline?: number
  }[]
  readonly stored: FencedPosition[]
  readonly lookups: number[]
  readonly acks: {
    readonly target: ManagedTarget
    readonly offset: bigint
    readonly request: FilteredAck
  }[]
  readonly cancels: { readonly target: ManagedTarget; readonly request: FilteredWaitCancel }[]
}

/**
 * A coordinator whose polls `poll` answers. The route lookup names replica 1
 * unless `primary` says otherwise. Stores succeed unless `store` says
 * otherwise. Group syncs answer with `sync`. A wait cancel ends the named
 * waits held on its node with an empty ready answer, as the server does,
 * unless `cancel` answers it.
 */
function coordinatorOf(spec: {
  readonly poll: (request: FilteredPollRequest, target: ManagedTarget) => Promise<Uint8Array>
  readonly primary?: (partitionId: number) => Promise<number>
  readonly store?: (position: FencedPosition) => Promise<void>
  readonly sync?: () => Promise<Uint8Array>
  readonly wait?: (request: FilteredWaitRequest, target: ManagedTarget) => Promise<Uint8Array>
  // Answers an acknowledgment with its own reply, or stores it when undefined.
  readonly ack?: (offset: bigint, target: ManagedTarget) => Promise<Uint8Array | undefined>
  readonly cancel?: (request: FilteredWaitCancel, target: ManagedTarget) => Promise<Uint8Array>
}): Coordinator {
  const polls: Coordinator["polls"] = []
  const waits: Coordinator["waits"] = []
  const stored: FencedPosition[] = []
  const lookups: number[] = []
  const acks: Coordinator["acks"] = []
  const cancels: Coordinator["cancels"] = []
  const held = new Map<string, () => void>()
  const connection: CoordinatorConnection = {
    send: (code) => {
      assert.equal(code, SYNC_CONSUMER_GROUP_CODE, "only the group sync rides the plain send")
      return spec.sync === undefined ? Promise.resolve(assignment(1n, [0])) : spec.sync()
    },
    sendManagedTo: async (code, payload, target, options) => {
      if (code === AGDX_FILTERED_WAIT_CODE) {
        const request = decodeFilteredWaitRequest(decodeOne(payload, "wait"), "wait")
        waits.push({
          target,
          request,
          ...(options?.deadline === undefined ? {} : { deadline: options.deadline })
        })
        assert.ok(spec.wait !== undefined, "this server holds no filtered wait")
        const key = heldKey(target, request.waitId)
        const cancelled = new Promise<Uint8Array>((resolve) => {
          held.set(key, () => {
            resolve(readyReply([]))
          })
        })
        try {
          return { reply: await Promise.race([spec.wait(request, target), cancelled]) }
        } finally {
          held.delete(key)
        }
      }
      if (code === AGDX_FILTERED_WAIT_CANCEL_CODE) {
        const request = decodeFilteredWaitCancel(decodeOne(payload, "cancel"), "cancel")
        cancels.push({ target, request })
        if (spec.cancel !== undefined) return { reply: await spec.cancel(request, target) }
        const ended = request.waitIds.filter((waitId) => held.has(heldKey(target, waitId)))
        for (const waitId of ended) held.get(heldKey(target, waitId))?.()
        return { reply: cancelledReply(ended) }
      }
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
        acks.push({ target, offset: ack.offset, request: ack })
        const answered = spec.ack === undefined ? undefined : await spec.ack(ack.offset, target)
        if (answered !== undefined) return { reply: answered }
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
  return { connection, polls, waits, stored, lookups, acks, cancels }
}

// An acknowledgment reply whose receipt reports `offset` as durable progress.
function acknowledged(offset: bigint, of: typeof generation = generation): Uint8Array {
  return encodeNamed(
    encodeFilterReply({
      kind: "ok",
      outcome: { kind: "acknowledged", receipt: { partitionId: 0, offset, generation: of } }
    })
  )
}

function heldKey(target: ManagedTarget, waitId: bigint): string {
  return `${target.kind === "replica" ? String(target.replica) : target.kind}:${String(waitId)}`
}

function reader(
  coordinator: Coordinator,
  layout: {
    readonly guarded?: ConsumerFilter
    readonly partitionIds?: readonly number[]
    readonly idleIntervalMs?: number
    readonly maxUnackedPages?: number
    readonly maxWaitMs?: number
    readonly start?: FilteredStart
  } = {}
): FilteredReader {
  const transport = baseTransport(() =>
    Promise.reject(new Error("the shared transport is not used"))
  )
  const start: FilteredStart = layout.start ?? { kind: "first" }
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
    ...(layout.maxWaitMs === undefined ? {} : { maxWaitMs: layout.maxWaitMs }),
    ...(layout.maxUnackedPages === undefined ? {} : { maxUnackedPages: layout.maxUnackedPages }),
    guard: layout.guarded === undefined ? undefined : CompiledFilter.compile(layout.guarded),
    membership: undefined,
    partitionIds: layout.partitionIds ?? [0]
  })
}

function groupReader(coordinator: Coordinator, served: Capabilities = capabilities) {
  const transport: FilterTransport = {
    ...baseTransport(() => Promise.reject(new Error("the shared transport is not used"))),
    openCoordinator: () => Promise.resolve(coordinator.connection)
  }
  return FilteredReaderBuilder.create(
    transport,
    () => Promise.resolve(served),
    new Filters(transport, () => Promise.resolve(served)),
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
      filters: { native: true, catalog: false, groupPolicyReads: false, maxWaitMs: 0 }
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

void test("given_a_probe_that_established_nothing_or_no_filters_when_an_automatic_reader_is_built_then_should_classify_it_like_the_consumer", async () => {
  const transport = baseTransport(() => Promise.reject(new Error("no command is needed")))
  const automatic = (served: Capabilities) =>
    FilteredReaderBuilder.create(
      transport,
      () => Promise.resolve(served),
      new Filters(transport, () => Promise.resolve(served)),
      { stream: "orbit", topic: "fleet_changes" },
      { kind: "group", name: "anomaly-desk" },
      { kind: "group" }
    ).build()
  await assert.rejects(automatic({ ...OPEN_CAPABILITIES, hello: "failed" }), TimeoutError)
  await assert.rejects(automatic({ ...OPEN_CAPABILITIES, hello: "rejected" }), (error: unknown) => {
    assert.ok(error instanceof UnsupportedError)
    assert.equal(error.surface, "filters")
    assert.equal(error.feature, undefined)
    return true
  })
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
      filters: { native: false, catalog: false, groupPolicyReads: true, maxWaitMs: 0 }
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
  assert.deepEqual(
    found.records[0]?.evaluation,
    { kind: "unfiltered" },
    "an unbound group runs no filter, so no pass policy delivered the record"
  )
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

void test("given_a_lone_record_over_the_requested_byte_cap_when_a_page_returns_then_should_accept_it_and_refuse_two", async () => {
  for (const [offsets, accepted] of [
    [[0n], true],
    [[0n, 1n], false]
  ] as const) {
    const coordinator = coordinatorOf({
      poll: () =>
        Promise.resolve(
          pageReply(groupPage({ offsets, examined: offsets.length, nextScanOffset: 2n }))
        )
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
        maxReplyBytes: 64
      },
      start: { kind: "first" },
      idleIntervalMs: 1,
      guard: undefined,
      membership: undefined,
      partitionIds: [0]
    })
    try {
      if (accepted) {
        const [page] = await filtered.readRound()
        assert.equal(page?.records.length, 1, "the record over the cap comes alone")
      } else {
        await assert.rejects(filtered.readRound(), ProtocolError)
      }
    } finally {
      await filtered.close()
    }
  }
})

void test("given_group_policy_reads_without_an_evaluator_when_an_automatic_reader_is_built_then_should_allow_unbound_reads", async () => {
  const transport = baseTransport(() => Promise.resolve(assignment(1n, [])))
  const withoutEvaluator = (): Promise<Capabilities> =>
    Promise.resolve({
      ...capabilities,
      filters: { native: false, catalog: false, groupPolicyReads: true, maxWaitMs: 0 }
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

void test("given_a_read_on_the_callers_node_when_its_link_fails_then_should_not_resend_it_here", async () => {
  const coordinator = coordinatorOf({
    primary: () => Promise.reject(refusal(1002)),
    poll: () => Promise.reject(new TransportError("the link to the replica was lost", true))
  })
  const filtered = reader(coordinator)
  await assert.rejects(filtered.tryNextPage(), TransportError)
  assert.equal(coordinator.polls.length, 1, "the caller's node moves on inside the node choice")
  await filtered.close()
})

void test("given_a_read_on_the_callers_node_refused_as_not_primary_when_reading_then_should_look_the_route_up_again_and_resend_once", async () => {
  let lookups = 0
  const coordinator = coordinatorOf({
    primary: () => (lookups++ === 0 ? Promise.reject(refusal(1002)) : Promise.resolve(2)),
    poll: (_request, target) =>
      target.kind === "caller"
        ? Promise.reject(refusal(NOT_PRIMARY))
        : Promise.resolve(pageReply(page({ offsets: [0n] })))
  })
  const filtered = reader(coordinator)
  assert.equal((await filtered.tryNextPage())?.records[0]?.offset, 0n)
  assert.deepEqual(
    coordinator.polls.map((poll) => poll.target),
    [{ kind: "caller" }, { kind: "replica", replica: 2 }],
    "a node that does not store the partition sends the read to its primary"
  )
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

void test("given_a_stored_checkpoint_when_an_earlier_record_is_acknowledged_through_then_should_send_nothing", async () => {
  const coordinator = coordinatorOf({
    poll: () => Promise.resolve(pageReply(page({ offsets: [3n, 7n, 9n], nextScanOffset: 20n })))
  })
  const filtered = reader(coordinator)
  const found = await filtered.tryNextPage()
  const [first, second] = found?.records ?? []
  assert(first !== undefined && second !== undefined)
  await filtered.ackThrough(second)
  await filtered.ackThrough(first)
  assert.deepEqual(
    coordinator.acks.map((ack) => ack.offset),
    [7n],
    "7 is stored, so 3 needs no acknowledgment"
  )
  await filtered.close()
})

void test("given_a_receipt_above_the_target_when_acknowledged_then_should_cover_only_what_it_reports", async () => {
  const coordinator = coordinatorOf({
    poll: () => Promise.resolve(pageReply(page({ offsets: [3n, 7n, 11n], nextScanOffset: 20n }))),
    ack: (offset) => Promise.resolve(offset < 15n ? acknowledged(15n) : undefined)
  })
  const filtered = reader(coordinator)
  const found = await filtered.tryNextPage()
  const [first, second, third] = found?.records ?? []
  assert(first !== undefined && second !== undefined && third !== undefined)
  await filtered.ackThrough(first)
  await filtered.ackThrough(second)
  assert.deepEqual(
    coordinator.acks.map((ack) => ack.offset),
    [3n],
    "the durable offset 15 covers 7"
  )
  await filtered.ack(third)
  assert.deepEqual(
    coordinator.acks.map((ack) => ack.offset),
    [3n, 19n],
    "another consumer's progress marks no local record handled"
  )
  await filtered.close()
})

void test("given_a_receipt_below_the_target_or_for_another_generation_when_acknowledged_then_should_refuse_it", async () => {
  for (const receipt of [
    acknowledged(2n),
    acknowledged(3n, { ...generation, partitionIncarnation: 18n })
  ]) {
    let refusals = 1
    const coordinator = coordinatorOf({
      poll: () => Promise.resolve(pageReply(page({ offsets: [3n, 7n], nextScanOffset: 20n }))),
      ack: () => Promise.resolve(refusals-- > 0 ? receipt : undefined)
    })
    const filtered = reader(coordinator)
    const first = (await filtered.tryNextPage())?.records[0]
    assert(first !== undefined)
    await assert.rejects(filtered.ackThrough(first), ProtocolError)
    await filtered.close()
    assert.deepEqual(
      coordinator.acks.map((ack) => ack.offset),
      [3n, 3n],
      "the refused target stays pending and is sent again"
    )
  }
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

void test("given_a_refused_store_when_the_next_ack_runs_then_should_store_the_same_target_again", async () => {
  let failures = 1
  const coordinator = coordinatorOf({
    poll: () => Promise.resolve(pageReply(page({ offsets: [0n, 1n], nextScanOffset: 2n }))),
    store: (position) => {
      if (position.offset === 1n && failures > 0) {
        failures -= 1
        return Promise.reject(new TransportError("the reply was lost", false))
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

void test("given_a_fault_page_when_skipped_then_should_send_one_fenced_ack_at_the_fault_once_earlier_records_are_handled_and_read_on_after_it", async () => {
  // The fault page was read under another owner than the page before it.
  const faultContext = { ownerGeneration: 5n, metadataOp: 9n }
  const faulted = page({
    nextScanOffset: 8n,
    examined: 3,
    stop: "fault",
    fault: { offset: 8n, reason: "malformed" },
    ackContext: faultContext,
    records: body(0, 1000n, [], { incarnation: 17n, ...faultContext })
  })
  const served = [
    page({ offsets: [4n], nextScanOffset: 5n }),
    faulted,
    page({ offsets: [9n], nextScanOffset: 10n })
  ]
  let polled = 0
  const coordinator = coordinatorOf({
    poll: () => {
      const answer = served[Math.min(polled, served.length - 1)] ?? faulted
      polled += 1
      return Promise.resolve(pageReply(answer))
    }
  })
  const filtered = reader(coordinator)
  const earlier = (await filtered.tryNextPage())?.records[0]
  assert(earlier !== undefined)
  await assert.rejects(filtered.tryNextPage(), (error: unknown) => {
    assert.ok(error instanceof FilterFaultError)
    assert.deepEqual([error.partitionId, error.offset], [0, 8n])
    return true
  })

  await assert.rejects(filtered.skip(0, 7n), InvalidError, "the partition is stopped at 8")
  await filtered.skip(0, 8n)
  assert.equal(coordinator.acks.length, 0, "record 4 is still open, so the skip waits for it")
  await filtered.ack(earlier)
  assert.deepEqual(
    coordinator.acks.map((ack) => ack.request),
    [
      {
        v: FILTER_OP_VERSION,
        source: { stream: "orbit", topic: "fleet_changes" },
        consumer: { kind: "consumer", name: "reader" },
        partitionId: 0,
        offset: 8n,
        generation: faulted.generation,
        ackContext: faultContext,
        digest: faulted.policy.digest,
        mode: faulted.policy.mode,
        policyGeneration: faulted.policy.policyGeneration
      }
    ],
    "one acknowledgment at the fault, under the fault page's fence"
  )
  const after = await filtered.tryNextPage()
  assert.deepEqual(
    after?.records.map((record) => record.offset),
    [9n]
  )
  const resumed = coordinator.polls[2]?.request.start
  assert.equal(resumed?.kind === "continue" ? resumed.continuation.nextScanOffset : undefined, 9n)
  await assert.rejects(filtered.skip(0, 8n), InvalidError, "the block is gone")
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
            error instanceof FilterExecutionError && error.detail?.reason === "membership_stale"
        )
      assert.equal(polls, 2)
      assert.deepEqual(coordinator.lookups, [0, 0], "a lost assignment drops the route")
    } finally {
      await filtered.close()
    }
  })
}

void test("given_records_a_pass_policy_delivered_when_read_then_should_name_why_each_was_not_evaluated", async () => {
  const reasons = [
    { offset: 0n, reason: "type_mismatch" },
    { offset: 2n, reason: "foreign_codec" }
  ] as const
  const coordinator = coordinatorOf({
    poll: () =>
      Promise.resolve(
        pageReply(page({ offsets: [0n, 1n, 2n], nextScanOffset: 3n, unevaluated: reasons }))
      )
  })
  const filtered = reader(coordinator)
  const delivered = await filtered.tryNextPage()
  assert.deepEqual(
    delivered?.records.map((record) => record.evaluation),
    [
      { kind: "unevaluated", reason: "type_mismatch" },
      { kind: "selected" },
      { kind: "unevaluated", reason: "foreign_codec" }
    ]
  )
  await filtered.close()
  const contradictory = reader(
    coordinatorOf({
      poll: () =>
        Promise.resolve(
          pageReply(
            page({
              offsets: [0n],
              unevaluated: [
                { offset: 0n, reason: "type_mismatch" },
                { offset: 0n, reason: "malformed" }
              ]
            })
          )
        )
    })
  )
  await assert.rejects(
    contradictory.tryNextPage(),
    ProtocolError,
    "a record is unevaluated for one reason"
  )
  await contradictory.close()
})

void test("given_unevaluated_records_when_guarded_then_should_require_a_reproducible_pass_fault", async () => {
  const passing = { ...filter, faultPolicy: "pass" as const }
  for (const [definition, limit, reason, accepted] of [
    [filter, 4, "too_large", false],
    [passing, 4, "too_large", true],
    [passing, 4, "malformed", false],
    [passing, 1024, "too_large", false]
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
              unevaluated: [{ offset: 0n, reason }],
              evaluationLimits: { maxPayloadBytes: limit, maxDepth: 64 }
            })
          )
        )
    })
    const filtered = reader(coordinator, { guarded: definition })
    if (accepted) {
      const delivered = await filtered.tryNextPage()
      assert.deepEqual(
        delivered?.records.map((record) => record.evaluation),
        [{ kind: "unevaluated", reason: "too_large" }]
      )
    } else await assert.rejects(filtered.tryNextPage())
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

function readyReply(ready: readonly number[]): Uint8Array {
  return encodeNamed(
    encodeFilterReply({ kind: "ok", outcome: { kind: "ready", result: { ready } } })
  )
}

function cancelledReply(cancelled: readonly bigint[]): Uint8Array {
  return encodeNamed(
    encodeFilterReply({ kind: "ok", outcome: { kind: "cancelled", result: { cancelled } } })
  )
}

void test("given_an_idle_round_on_a_server_that_holds_waits_when_waiting_then_should_send_one_wait_per_node_and_read_only_the_ready_partitions", async () => {
  let held: ((reply: Uint8Array) => void) | undefined
  const coordinator = coordinatorOf({
    poll: (request) =>
      Promise.resolve(pageReply(page({ partitionId: request.partitionId, nextScanOffset: 5n }))),
    primary: (partitionId) => Promise.resolve(partitionId === 1 ? 2 : 1),
    wait: (_request, target) =>
      target.kind === "replica" && target.replica === 1
        ? Promise.resolve(readyReply([2]))
        : new Promise((resolve) => {
            held = resolve
          })
  })
  const filtered = reader(coordinator, { partitionIds: [0, 1, 2], maxWaitMs: 2_000 })
  assert.deepEqual(await filtered.readRound(), [undefined, false])
  const before = Date.now()
  await filtered.waitForRecords()
  assert.equal(coordinator.waits.length, 2, "one wait per serving node")
  const byNode = new Map(
    coordinator.waits.map((wait) => [
      wait.target.kind === "replica" ? wait.target.replica : -1,
      wait.request
    ])
  )
  assert.deepEqual(byNode.get(1)?.partitions, [
    { partitionId: 0, offset: 5n },
    { partitionId: 2, offset: 5n }
  ])
  assert.deepEqual(byNode.get(2)?.partitions, [{ partitionId: 1, offset: 5n }])
  assert.equal(byNode.get(1)?.maxWaitMs, 2_000)
  assert.equal(byNode.get(1)?.consumer.kind, "consumer")
  for (const wait of coordinator.waits) {
    assert.ok((wait.deadline ?? 0) >= before + 2_000 + 5_000, "the call outlives the server wait")
  }
  const polled = coordinator.polls.length
  await filtered.readRound()
  assert.deepEqual(
    coordinator.polls.slice(polled).map((poll) => poll.request.partitionId),
    [2],
    "only the ready partition is read"
  )
  held?.(readyReply([]))
  await filtered.close()
})

void test("given_more_partitions_on_a_node_than_one_wait_names_when_waiting_then_should_wait_on_every_partition_in_capped_waits", async () => {
  const partitionIds = Array.from(
    { length: 2 * MAX_FILTERED_WAIT_PARTITIONS + 52 },
    (_, partitionId) => partitionId
  )
  const last = partitionIds.length - 1
  const coordinator = coordinatorOf({
    poll: (request) =>
      Promise.resolve(pageReply(page({ partitionId: request.partitionId, nextScanOffset: 5n }))),
    wait: (request) =>
      request.partitions.some((partition) => partition.partitionId === last)
        ? Promise.resolve(readyReply([last]))
        : new Promise<Uint8Array>(() => undefined)
  })
  const filtered = reader(coordinator, { partitionIds, maxWaitMs: 2_000 })
  await filtered.readRound()
  await filtered.waitForRecords()
  assert.equal(coordinator.waits.length, 3, "one wait per slice of the node's partitions")
  for (const wait of coordinator.waits)
    assert.ok(wait.request.partitions.length <= MAX_FILTERED_WAIT_PARTITIONS)
  assert.deepEqual(
    coordinator.waits.flatMap((wait) =>
      wait.request.partitions.map((partition) => partition.partitionId)
    ),
    partitionIds
  )
  assert.equal(new Set(coordinator.waits.map((wait) => wait.request.waitId)).size, 3)
  const polled = coordinator.polls.length
  await filtered.readRound()
  assert.deepEqual(
    coordinator.polls.slice(polled).map((poll) => poll.request.partitionId),
    [last],
    "only the ready partition past the first slice is read"
  )
  await filtered.close()
})

void test("given_a_server_that_refuses_waits_as_unsupported_when_idle_then_should_stop_waiting", async () => {
  const coordinator = coordinatorOf({
    poll: (request) =>
      Promise.resolve(pageReply(page({ partitionId: request.partitionId, nextScanOffset: 5n }))),
    wait: () =>
      Promise.resolve(
        encodeNamed(
          encodeFilterReply({
            kind: "err",
            error: {
              code: { kind: "known", name: "Unsupported" },
              reason: "unsupported",
              message: "no filtered wait"
            }
          })
        )
      )
  })
  const filtered = reader(coordinator, { maxWaitMs: 2_000 })
  await filtered.readRound()
  await filtered.waitForRecords()
  await filtered.readRound()
  await filtered.waitForRecords()
  assert.equal(coordinator.waits.length, 1, "an unsupported wait is not sent again")
  await filtered.close()
})

void test("given_a_server_without_filtered_waits_when_idle_then_should_pace_with_the_idle_interval", async () => {
  const coordinator = coordinatorOf({
    poll: (request) =>
      Promise.resolve(pageReply(page({ partitionId: request.partitionId, nextScanOffset: 5n })))
  })
  const filtered = reader(coordinator, { idleIntervalMs: 30 })
  await filtered.readRound()
  const started = performance.now()
  await filtered.waitForRecords()
  assert.ok(performance.now() - started >= 25, "an idle round sleeps the idle interval")
  assert.equal(coordinator.waits.length, 0)
  await filtered.close()
})

void test("given_an_empty_wait_answer_long_before_its_deadline_when_idle_then_should_also_sleep_the_idle_interval", async () => {
  const coordinator = coordinatorOf({
    poll: (request) =>
      Promise.resolve(pageReply(page({ partitionId: request.partitionId, nextScanOffset: 5n }))),
    wait: () => Promise.resolve(readyReply([]))
  })
  const filtered = reader(coordinator, { idleIntervalMs: 30, maxWaitMs: 2_000 })
  await filtered.readRound()
  const started = performance.now()
  await filtered.waitForRecords()
  assert.equal(coordinator.waits.length, 1)
  assert.ok(
    performance.now() - started >= 25,
    "an early empty answer falls back to the idle interval"
  )
  await filtered.close()
})

void test("given_a_builder_wait_longer_than_the_announced_one_when_built_then_should_wait_the_announced_cap", async () => {
  const coordinator = coordinatorOf({
    poll: (request) =>
      Promise.resolve(
        pageReply(groupPage({ partitionId: request.partitionId, nextScanOffset: 5n }))
      ),
    wait: () => Promise.resolve(readyReply([0]))
  })
  assert.throws(() => groupReader(coordinator).maxWait(-1), InvalidError)
  const filtered = await groupReader(coordinator, {
    ...capabilities,
    filters: { ...capabilities.filters, maxWaitMs: 3_000 }
  }).build()
  await filtered.readRound()
  await filtered.waitForRecords()
  const [first] = coordinator.waits
  assert.ok(first !== undefined)
  assert.equal(first.request.maxWaitMs, 3_000)
  assert.deepEqual(first.request.consumer, { kind: "group", name: "anomaly-desk" })
  await filtered.close()
})

void test("given_pages_without_a_continuation_when_waiting_then_should_wait_past_the_head_or_from_zero_and_keep_one_wait_per_node", async () => {
  let rounds = 0
  const coordinator = coordinatorOf({
    poll: (request) => {
      rounds += 1
      const frontier = request.partitionId === 0 ? 1_000n : 0n
      const {
        safeAckOffset: _safe,
        nextScanOffset: _next,
        ...rest
      } = page({
        partitionId: request.partitionId,
        frontier
      })
      return Promise.resolve(pageReply({ ...rest, examined: 0 }))
    },
    primary: (partitionId) => Promise.resolve(partitionId === 0 ? 1 : 2),
    wait: (_request, target) =>
      target.kind === "replica" && target.replica === 2
        ? Promise.resolve(readyReply([1]))
        : new Promise(() => undefined)
  })
  const filtered = reader(coordinator, {
    partitionIds: [0, 1],
    maxWaitMs: 2_000,
    idleIntervalMs: 30,
    start: { kind: "next" }
  })
  await filtered.readRound()
  await filtered.waitForRecords()
  assert.deepEqual(
    coordinator.waits.map((wait) => wait.request.partitions),
    [[{ partitionId: 0, offset: 1_001n }], [{ partitionId: 1, offset: 0n }]],
    "past a head of 1000, from 0 at a head of 0"
  )
  assert.equal(rounds, 2)
  await filtered.readRound()
  const paced = performance.now()
  await filtered.waitForRecords()
  assert.ok(performance.now() - paced >= 25, "a ready answer the round could not confirm is paced")
  assert.equal(coordinator.waits.length, 2, "a node that still holds a wait gets no other")
  await filtered.close()
})

void test("given_a_backup_refusing_a_read_as_unavailable_when_reading_then_should_forget_the_route_and_resend_once", async () => {
  const leaders = [1, 2]
  const coordinator = coordinatorOf({
    primary: () => Promise.resolve(leaders.shift() ?? 2),
    poll: (_request, target) =>
      Promise.resolve(
        target.kind === "replica" && target.replica === 1
          ? unavailableReply()
          : pageReply(page({ offsets: [0n] }))
      )
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
  assert.deepEqual(coordinator.lookups, [0, 0], "the refused route is looked up again")
  await filtered.close()
})

void test("given_every_node_refusing_a_read_as_unavailable_when_reading_then_should_resend_once_and_surface_the_refusal", async () => {
  const coordinator = coordinatorOf({ poll: () => Promise.resolve(unavailableReply()) })
  const filtered = reader(coordinator, { idleIntervalMs: 0 })
  await assert.rejects(filtered.tryNextPage(), FilterExecutionError)
  assert.equal(coordinator.polls.length, 2, "one resend after a fresh lookup")
  await filtered.close()
})

void test("given_a_cached_primary_that_stops_answering_when_reading_then_should_follow_the_new_primary_after_a_route_recheck", async () => {
  let stopped = false
  const coordinator = coordinatorOf({
    primary: () => Promise.resolve(stopped ? 2 : 1),
    poll: (request, target) => {
      if (stopped && target.kind === "replica" && target.replica === 1)
        return new Promise(() => undefined)
      return Promise.resolve(
        pageReply(
          stopped
            ? page({ partitionId: request.partitionId, offsets: [0n] })
            : idlePage(request.partitionId, 0n)
        )
      )
    }
  })
  const filtered = reader(coordinator, { partitionIds: [0, 1] })
  assert.equal(await filtered.tryNextPage(), undefined)
  stopped = true
  const started = performance.now()
  const found = await within(filtered.tryNextPage(), 5_000)
  const elapsed = performance.now() - started
  assert.equal(found?.partitionId, 0)
  assert.ok(elapsed >= 900 && elapsed < 2_500, `followed the move after ${String(elapsed)} ms`)
  const lookups = coordinator.lookups.length
  assert.equal((await filtered.tryNextPage())?.partitionId, 1)
  assert.deepEqual(
    coordinator.lookups.slice(lookups),
    [1],
    "every route through the old node is read again"
  )
  assert.deepEqual(
    coordinator.polls.slice(-3).map((poll) => [poll.request.partitionId, poll.target]),
    [
      [0, { kind: "replica", replica: 1 }],
      [0, { kind: "replica", replica: 2 }],
      [1, { kind: "replica", replica: 2 }]
    ]
  )
  await filtered.close()
})

void test("given_a_primary_that_fails_at_once_and_a_route_that_still_names_it_when_reading_then_should_recheck_the_route_while_the_resend_waits", async () => {
  // The crashed primary drops the first read at once, the route still names
  // it for two lookups, and the resend to it would wait out the call bound.
  const primaries = [1, 1, 1, 2]
  let attempts = 0
  const coordinator = coordinatorOf({
    primary: () => Promise.resolve(primaries.shift() ?? 2),
    poll: (_request, target) => {
      if (target.kind === "replica" && target.replica === 2)
        return Promise.resolve(pageReply(page({ offsets: [0n] })))
      attempts += 1
      return attempts === 1
        ? Promise.reject(new TransportError("the link to the replica was lost", true))
        : new Promise(() => undefined)
    }
  })
  const filtered = reader(coordinator)
  const found = await within(filtered.tryNextPage(), 5_000)
  assert.equal(found?.records[0]?.offset, 0n)
  assert.deepEqual(
    coordinator.polls.map((poll) => poll.target),
    [
      { kind: "replica", replica: 1 },
      { kind: "replica", replica: 1 },
      { kind: "replica", replica: 2 }
    ]
  )
  await filtered.close()
})

void test("given_a_slow_but_healthy_primary_when_reading_then_should_keep_waiting_for_its_answer", async () => {
  const coordinator = coordinatorOf({
    primary: () => Promise.resolve(1),
    poll: () =>
      new Promise((resolve) => {
        setTimeout(() => {
          resolve(pageReply(page({ offsets: [0n] })))
        }, 2_300)
      })
  })
  const filtered = reader(coordinator)
  assert.equal((await within(filtered.tryNextPage(), 5_000))?.records[0]?.offset, 0n)
  assert.equal(coordinator.polls.length, 1, "the read was never abandoned")
  assert.ok(coordinator.lookups.length >= 3, "the route was checked while the read was slow")
  await filtered.close()
})

for (const [label, refuse] of [
  ["not_primary", () => Promise.reject(refusal(NOT_PRIMARY))],
  ["unavailable", () => Promise.resolve(unavailableReply())]
] as const) {
  void test(`given_no_primary_answering_an_ack_with_${label}_when_acknowledging_then_should_send_the_same_target_until_it_is_stored`, async () => {
    let refusals = 3
    const coordinator = coordinatorOf({
      poll: () => Promise.resolve(pageReply(page({ offsets: [0n, 1n], nextScanOffset: 2n }))),
      ack: () => {
        if (refusals === 0) return Promise.resolve(undefined)
        refusals -= 1
        return refuse()
      }
    })
    const filtered = reader(coordinator)
    const found = await filtered.tryNextPage()
    assert.ok(found !== undefined)
    await within(filtered.ackPage(found), 5_000)
    assert.ok(coordinator.acks.length >= 4, "each refused store is sent again")
    assert.deepEqual(
      [...new Set(coordinator.acks.map((ack) => ack.offset))],
      [1n],
      "every repeat names the same delivered target"
    )
    assert.deepEqual(
      coordinator.stored.map((position) => position.offset),
      [1n]
    )
    await filtered.close()
  })
}

void test("given_an_empty_page_when_its_store_finds_no_primary_then_should_send_it_once_and_end_the_round", async () => {
  let refusing = true
  const coordinator = coordinatorOf({
    poll: () => Promise.resolve(pageReply(page({ nextScanOffset: 5n }))),
    ack: () => (refusing ? Promise.reject(refusal(NOT_PRIMARY)) : Promise.resolve(undefined))
  })
  const filtered = reader(coordinator)
  const started = performance.now()
  assert.equal(await filtered.tryNextPage(), undefined)
  assert.ok(performance.now() - started < 1_000, "the round never waits for a primary")
  assert.equal(coordinator.acks.length, 2, "one store and its one resend after a fresh lookup")
  refusing = false
  await filtered.close()
})

void test("given_a_woken_page_that_examined_records_when_the_next_round_is_idle_then_should_hold_a_wait_instead_of_sleeping", async () => {
  let pending = false
  let next = 0n
  const coordinator = coordinatorOf({
    poll: () => {
      if (!pending) return Promise.resolve(pageReply(idlePage(0, next)))
      pending = false
      const offset = next
      next += 1n
      return Promise.resolve(pageReply(page({ offsets: [offset], nextScanOffset: offset + 1n })))
    },
    wait: () =>
      new Promise((resolve) => {
        setTimeout(() => {
          pending = true
          resolve(readyReply([0]))
        }, 50)
      })
  })
  const filtered = reader(coordinator, { maxWaitMs: 2_000, idleIntervalMs: 30_000 })
  await filtered.ackPage(await within(filtered.nextPage(), 5_000))
  const started = performance.now()
  const second = await within(filtered.nextPage(), 5_000)
  assert.equal(second.records[0]?.offset, 1n)
  assert.ok(performance.now() - started < 2_000, "the idle round after a page held a wait")
  assert.equal(coordinator.waits.length, 2)
  await filtered.close()
})

void test("given_held_waits_when_the_assignment_changes_meanwhile_then_should_end_the_wait_and_read_the_gained_partition", async () => {
  let syncs = 0
  const coordinator = coordinatorOf({
    sync: () => {
      syncs += 1
      return Promise.resolve(syncs === 1 ? assignment(1n, [0]) : assignment(2n, [0, 1]))
    },
    poll: (request) =>
      Promise.resolve(
        pageReply(
          request.partitionId === 1
            ? groupPage({ partitionId: 1, offsets: [0n] })
            : unscanned(request.partitionId)
        )
      ),
    wait: () => new Promise(() => undefined)
  })
  const filtered = await groupReader(coordinator, waitingCapabilities).build()
  const started = performance.now()
  const found = await within(filtered.nextPage(), 6_000)
  const elapsed = performance.now() - started
  assert.equal(found.partitionId, 1)
  assert.ok(
    elapsed >= 1_500 && elapsed < 4_000,
    `read the gained partition after ${String(elapsed)} ms`
  )
  assert.equal(coordinator.waits.length, 1, "the wait was never answered")
  await until(() => coordinator.cancels.length > 0, 1_000)
  assert.deepEqual(
    coordinator.cancels.map((cancel) => cancel.request.waitIds),
    [[coordinator.waits[0]?.request.waitId]],
    "the wait over the old assignment was cancelled"
  )
  await filtered.close()
})

void test("given_held_waits_when_the_assignment_stays_the_same_then_should_keep_awaiting_the_same_waits", async () => {
  let syncs = 0
  let syncsWhileHeld = 0
  let pending = false
  const coordinator = coordinatorOf({
    sync: () => {
      syncs += 1
      return Promise.resolve(assignment(1n, [0]))
    },
    poll: () => Promise.resolve(pageReply(pending ? groupPage({ offsets: [0n] }) : unscanned(0))),
    wait: () =>
      new Promise((resolve) => {
        setTimeout(() => {
          syncsWhileHeld = syncs
          pending = true
          resolve(readyReply([0]))
        }, 2_600)
      })
  })
  const filtered = await groupReader(coordinator, waitingCapabilities).build()
  const found = await within(filtered.nextPage(), 6_000)
  assert.equal(found.records[0]?.offset, 0n)
  assert.equal(syncsWhileHeld, 2, "the assignment was read again while the wait held")
  assert.equal(coordinator.waits.length, 1, "the same wait kept holding")
  assert.deepEqual(coordinator.cancels, [], "a kept wait is not cancelled")
  await filtered.close()
})

void test("given_a_held_wait_on_a_node_when_a_round_finds_its_partition_left_the_assignment_then_should_cancel_it", async () => {
  let syncs = 0
  const coordinator = coordinatorOf({
    sync: () => {
      syncs += 1
      return Promise.resolve(syncs === 1 ? assignment(1n, [0, 1]) : assignment(2n, [0]))
    },
    primary: (partitionId) => Promise.resolve(partitionId === 1 ? 2 : 1),
    poll: (request) => Promise.resolve(pageReply(unscanned(request.partitionId))),
    // Node 1 answers empty at once, so each round sleeps while node 2 keeps its wait.
    wait: (_request, target) =>
      target.kind === "replica" && target.replica === 1
        ? Promise.resolve(readyReply([]))
        : new Promise(() => undefined)
  })
  const filtered = await groupReader(coordinator, waitingCapabilities)
    .idleInterval(100)
    .maxWait(2_000)
    .build()
  await filtered.nextPage({ timeoutMs: 3_500 }).catch(() => undefined)
  const held = coordinator.waits.find(
    (wait) => wait.target.kind === "replica" && wait.target.replica === 2
  )
  assert.ok(held !== undefined, "node 2 held a wait")
  assert.deepEqual(
    coordinator.cancels,
    [
      { target: { kind: "replica", replica: 2 }, request: { v: 1, waitIds: [held.request.waitId] } }
    ],
    "the wait naming the lost partition was cancelled when the round read the assignment"
  )
  await filtered.close()
  assert.equal(coordinator.cancels.length, 1, "close finds no wait left to cancel")
})

void test("given_a_wait_refused_as_not_primary_when_idle_then_should_forget_its_route_and_wait_on_the_new_primary_at_once", async () => {
  const primaries = [1, 2]
  const coordinator = coordinatorOf({
    primary: () => Promise.resolve(primaries.shift() ?? 2),
    poll: (request) =>
      Promise.resolve(pageReply(page({ partitionId: request.partitionId, nextScanOffset: 5n }))),
    wait: (_request, target) =>
      target.kind === "replica" && target.replica === 1
        ? Promise.reject(refusal(NOT_PRIMARY))
        : Promise.resolve(readyReply([0]))
  })
  const filtered = reader(coordinator, { maxWaitMs: 2_000, idleIntervalMs: 30_000 })
  await filtered.readRound()
  await within(filtered.waitForRecords(), 2_000)
  await filtered.readRound()
  await within(filtered.waitForRecords(), 2_000)
  assert.deepEqual(
    coordinator.waits.map((wait) => wait.target),
    [
      { kind: "replica", replica: 1 },
      { kind: "replica", replica: 2 }
    ]
  )
  assert.deepEqual(
    coordinator.polls.map((poll) => poll.target),
    [
      { kind: "replica", replica: 1 },
      { kind: "replica", replica: 2 }
    ],
    "the read after the refused wait looks the route up again"
  )
  await filtered.close()
})

void test("given_a_wait_refused_as_unavailable_when_idle_then_should_wait_again_at_once_and_pace_a_second_refusal", async () => {
  let refusals = 2
  const coordinator = coordinatorOf({
    poll: (request) =>
      Promise.resolve(pageReply(page({ partitionId: request.partitionId, nextScanOffset: 5n }))),
    wait: () => {
      if (refusals === 0) return Promise.resolve(readyReply([0]))
      refusals -= 1
      return Promise.resolve(unavailableReply())
    }
  })
  const filtered = reader(coordinator, { maxWaitMs: 2_000, idleIntervalMs: 400 })
  await filtered.readRound()
  const first = performance.now()
  await filtered.waitForRecords()
  assert.ok(performance.now() - first < 200, "the first refusal waits again at once")
  await filtered.readRound()
  assert.equal(coordinator.lookups.length, 2, "the refused wait's route is looked up again")
  const second = performance.now()
  await filtered.waitForRecords()
  assert.ok(performance.now() - second >= 350, "a second refusal in a row is paced")
  await filtered.readRound()
  await filtered.waitForRecords()
  assert.equal(coordinator.waits.length, 3)
  await filtered.close()
})

void test("given_a_node_holding_a_wait_when_another_partition_moves_to_it_then_should_replace_the_wait_with_one_naming_both", async () => {
  // Partition 1 moves from node 2 to node 1, which already holds a wait for partition 0.
  const routes = new Map([[1, [2, 1]]])
  let node1Waits = 0
  const coordinator = coordinatorOf({
    poll: (request) =>
      Promise.resolve(pageReply(page({ partitionId: request.partitionId, nextScanOffset: 5n }))),
    primary: (partitionId) => Promise.resolve(routes.get(partitionId)?.shift() ?? 1),
    wait: (_request, target) => {
      if (target.kind === "replica" && target.replica === 2)
        return Promise.resolve(unavailableReply())
      node1Waits += 1
      return node1Waits === 1 ? new Promise(() => undefined) : Promise.resolve(readyReply([1]))
    }
  })
  const filtered = reader(coordinator, { partitionIds: [0, 1], maxWaitMs: 2_000 })
  await filtered.readRound()
  await within(filtered.waitForRecords(), 2_000)
  await filtered.readRound()
  await within(filtered.waitForRecords(), 2_000)
  const [kept, , replacing] = coordinator.waits
  assert.ok(kept !== undefined && replacing !== undefined)
  assert.deepEqual(replacing.target, { kind: "replica", replica: 1 })
  assert.deepEqual(
    replacing.request.partitions.map((partition) => partition.partitionId),
    [0, 1],
    "the new wait names every partition node 1 leads now"
  )
  await until(() => coordinator.cancels.length > 0, 1_000)
  assert.deepEqual(coordinator.cancels[0], {
    target: { kind: "replica", replica: 1 },
    request: { v: 1, waitIds: [kept.request.waitId] }
  })
  await filtered.close()
})

void test("given_one_node_answering_ready_when_another_node_holds_a_wait_then_should_cancel_it_by_its_wait_id", async () => {
  const coordinator = coordinatorOf({
    poll: (request) =>
      Promise.resolve(pageReply(page({ partitionId: request.partitionId, nextScanOffset: 5n }))),
    primary: (partitionId) => Promise.resolve(partitionId === 1 ? 2 : 1),
    wait: (_request, target) =>
      target.kind === "replica" && target.replica === 1
        ? Promise.resolve(readyReply([0]))
        : new Promise(() => undefined)
  })
  const filtered = reader(coordinator, { partitionIds: [0, 1], maxWaitMs: 2_000 })
  const onNode = (replica: number) =>
    coordinator.waits.filter(
      (wait) => wait.target.kind === "replica" && wait.target.replica === replica
    )
  await filtered.readRound()
  await filtered.waitForRecords()
  const [first] = onNode(2)
  assert.ok(first !== undefined, "node 2 held a wait")
  await until(() => coordinator.cancels.length === 1, 1_000)
  assert.deepEqual(coordinator.cancels, [
    { target: { kind: "replica", replica: 2 }, request: { v: 1, waitIds: [first.request.waitId] } }
  ])
  await filtered.readRound()
  await filtered.waitForRecords()
  const [, second] = onNode(2)
  assert.ok(second !== undefined, "a cancelled wait is not reused, the node gets a new one")
  await until(() => coordinator.cancels.length === 2, 1_000)
  assert.deepEqual(coordinator.cancels[1]?.request.waitIds, [second.request.waitId])
  const ids = coordinator.waits.map((wait) => wait.request.waitId)
  assert.equal(new Set(ids).size, ids.length, "every wait has its own id")
  await filtered.close()
  assert.equal(coordinator.cancels.length, 2, "close finds no wait left to cancel")
})

void test("given_a_held_wait_when_the_reader_closes_then_should_cancel_it_and_end_the_idle_wait", async () => {
  const coordinator = coordinatorOf({
    poll: (request) =>
      Promise.resolve(pageReply(page({ partitionId: request.partitionId, nextScanOffset: 5n }))),
    wait: () => new Promise(() => undefined)
  })
  const filtered = reader(coordinator, { maxWaitMs: 20_000, idleIntervalMs: 30_000 })
  await filtered.readRound()
  const idle = filtered.waitForRecords()
  await until(() => coordinator.waits.length === 1, 1_000)
  await filtered.close()
  assert.deepEqual(
    coordinator.cancels.map((cancel) => cancel.request.waitIds),
    [[coordinator.waits[0]?.request.waitId]]
  )
  await within(idle, 1_000)
})

void test("given_an_iterating_reader_when_closed_then_should_end_the_iteration_without_an_error", async () => {
  const coordinator = coordinatorOf({
    poll: (request) =>
      Promise.resolve(
        pageReply(
          request.start.kind === "first"
            ? page({ partitionId: request.partitionId, offsets: [0n] })
            : page({ partitionId: request.partitionId, nextScanOffset: 5n })
        )
      ),
    wait: () => new Promise(() => undefined)
  })
  const filtered = reader(coordinator, { maxWaitMs: 20_000, idleIntervalMs: 30_000 })
  const seen: bigint[] = []
  const iteration = (async () => {
    for await (const record of filtered) seen.push(record.offset)
  })()
  await until(() => coordinator.waits.length === 1, 1_000)
  await filtered.close()
  await within(iteration, 1_000)
  assert.deepEqual(seen, [0n])
  const after: bigint[] = []
  for await (const record of filtered) after.push(record.offset)
  assert.deepEqual(after, [], "a closed reader yields nothing")
})

void test("given_a_held_wait_when_the_reader_is_woken_then_should_cancel_it", async () => {
  const coordinator = coordinatorOf({
    poll: (request) =>
      Promise.resolve(pageReply(page({ partitionId: request.partitionId, nextScanOffset: 5n }))),
    wait: () => new Promise(() => undefined)
  })
  const filtered = reader(coordinator, { maxWaitMs: 20_000 })
  await filtered.readRound()
  const idle = filtered.waitForRecords()
  await until(() => coordinator.waits.length === 1, 1_000)
  filtered.wake()
  await within(idle, 1_000)
  await until(() => coordinator.cancels.length === 1, 1_000)
  assert.deepEqual(coordinator.cancels[0]?.request.waitIds, [coordinator.waits[0]?.request.waitId])
  await filtered.close()
  assert.equal(coordinator.cancels.length, 1, "a cancelled wait is not cancelled again")
})

void test("given_a_held_wait_and_a_short_timeout_when_paging_then_should_return_by_the_timeout_and_cancel_the_wait", async (context) => {
  // Timers count whole milliseconds, so the wait's bound can fire just before
  // the page deadline by `performance.now()`. The skew makes that certain.
  const now = performance.now.bind(performance)
  let skewMs = 0
  context.mock.method(performance, "now", () => now() - skewMs)
  const coordinator = coordinatorOf({
    poll: (request) =>
      Promise.resolve(pageReply(page({ partitionId: request.partitionId, nextScanOffset: 5n }))),
    wait: () => {
      skewMs = 5
      return new Promise(() => undefined)
    }
  })
  const filtered = reader(coordinator, { maxWaitMs: 20_000, idleIntervalMs: 30_000 })
  const started = performance.now()
  // The timeout leaves the first round time to reach the held wait on a loaded machine.
  await assert.rejects(within(filtered.nextPage({ timeoutMs: 1_000 }), 5_000), TimeoutError)
  assert.ok(performance.now() - started < 5_000, "the page call ends by its timeout")
  assert.equal(coordinator.waits.length, 1, "the page call holds one wait")
  await until(() => coordinator.cancels.length === 1, 1_000)
  await filtered.close()
})

void test("given_an_aborted_signal_when_paging_then_should_reject_with_cancelled_before_reading", async () => {
  const coordinator = coordinatorOf({
    poll: (request) =>
      Promise.resolve(pageReply(page({ partitionId: request.partitionId, nextScanOffset: 5n }))),
    wait: () => new Promise(() => undefined)
  })
  const filtered = reader(coordinator, { maxWaitMs: 20_000 })
  const controller = new AbortController()
  controller.abort()
  await assert.rejects(
    within(filtered.nextPage({ signal: controller.signal }), 1_000),
    CancelledError
  )
  assert.equal(coordinator.polls.length, 0)
  await filtered.close()
})

void test("given_a_held_wait_when_the_signal_aborts_then_should_reject_with_cancelled_and_cancel_the_wait", async () => {
  const coordinator = coordinatorOf({
    poll: (request) =>
      Promise.resolve(pageReply(page({ partitionId: request.partitionId, nextScanOffset: 5n }))),
    wait: () => new Promise(() => undefined)
  })
  const filtered = reader(coordinator, { maxWaitMs: 20_000, idleIntervalMs: 30_000 })
  const controller = new AbortController()
  const paging = filtered.nextPage({ signal: controller.signal })
  await until(() => coordinator.waits.length === 1, 1_000)
  controller.abort()
  await assert.rejects(within(paging, 1_000), CancelledError)
  await until(() => coordinator.cancels.length === 1, 1_000)
  await filtered.close()
})

void test("given_two_overlapping_page_calls_when_both_read_then_should_never_deliver_a_record_twice", async () => {
  const coordinator = coordinatorOf({
    poll: async (request) => {
      await new Promise((resolve) => setTimeout(resolve, 10))
      const at = request.start.kind === "continue" ? request.start.continuation.nextScanOffset : 0n
      return pageReply(
        page({ partitionId: request.partitionId, offsets: [at], nextScanOffset: at + 1n })
      )
    }
  })
  const filtered = reader(coordinator)
  const pages = await within(Promise.all([filtered.nextPage(), filtered.nextPage()]), 2_000)
  assert.deepEqual(
    pages.flatMap((delivered) => delivered.records.map((record) => record.offset)),
    [0n, 1n]
  )
  await filtered.close()
})

void test("given_a_cancel_that_fails_when_a_wait_ends_then_should_warn_and_keep_reading", async () => {
  const warnings: string[] = []
  const listener = (warning: Error): void => {
    warnings.push(warning.message)
  }
  process.on("warning", listener)
  try {
    const coordinator = coordinatorOf({
      poll: (request) =>
        Promise.resolve(pageReply(page({ partitionId: request.partitionId, nextScanOffset: 5n }))),
      primary: (partitionId) => Promise.resolve(partitionId === 1 ? 2 : 1),
      wait: (_request, target) =>
        target.kind === "replica" && target.replica === 1
          ? Promise.resolve(readyReply([0]))
          : new Promise(() => undefined),
      cancel: () => Promise.reject(new TransportError("the link to node 2 is down", true))
    })
    const filtered = reader(coordinator, { partitionIds: [0, 1], maxWaitMs: 2_000 })
    await filtered.readRound()
    await filtered.waitForRecords()
    await until(() => warnings.length > 0, 1_000)
    assert.match(warnings[0] ?? "", /cancelling a filtered wait on replica 2 failed/)
    const polled = coordinator.polls.length
    await filtered.readRound()
    assert.deepEqual(
      coordinator.polls.slice(polled).map((poll) => poll.request.partitionId),
      [0],
      "the reader reads on"
    )
    await filtered.close()
  } finally {
    process.off("warning", listener)
  }
})

const waitingCapabilities: Capabilities = {
  ...capabilities,
  filters: { ...capabilities.filters, maxWaitMs: 20_000 }
}

// A backup's refusal of a partition it no longer leads.
function unavailableReply(): Uint8Array {
  return encodeNamed(
    encodeFilterReply({
      kind: "err",
      error: {
        code: { kind: "known", name: "Unavailable" },
        reason: "unavailable",
        message: "Request not accepted by a backup replica"
      }
    })
  )
}

// A page that examined nothing past `at`, where the read started.
function idlePage(partitionId: number, at: bigint): FilteredPage {
  const { safeAckOffset: _safe, ...rest } = page({ partitionId })
  return { ...rest, examined: 0, nextScanOffset: at }
}

async function until(condition: () => boolean, timeoutMs: number): Promise<void> {
  const deadline = performance.now() + timeoutMs
  while (!condition()) {
    if (performance.now() >= deadline) throw new Error(`not true within ${String(timeoutMs)} ms`)
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
}

function within<Value>(pending: Promise<Value>, timeoutMs: number): Promise<Value> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error(`not settled within ${String(timeoutMs)} ms`))
    }, timeoutMs)
    pending.then(
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
