import assert from "node:assert/strict"
import { test } from "node:test"
import { OPEN_CAPABILITIES, type Capabilities } from "../../src/client/capabilities.js"
import {
  ConsumerGroupSetupError,
  FilterExecutionError,
  InvalidError,
  ProtocolError,
  TimeoutError,
  TransportError,
  UnsupportedError
} from "../../src/client/errors.js"
import { Laser } from "../../src/client/laser.js"
import type { IggyClient, LaserTransport } from "../../src/iggy/apache-iggy.js"
import { Filters } from "../../src/managed/filters.js"
import { ConsumerGroup, GroupFilter, type GroupContext } from "../../src/stream/consumer-group.js"
import type { ConsumerStart } from "../../src/stream/consumer-start.js"
import { Topic } from "../../src/stream/topic.js"
import { type CborMap, decodeOne, encodeNamed, expectMap, field } from "../../src/wire/cbor.js"
import {
  AGDX_FILTER_MUTATE_CODE,
  AGDX_GET_FILTER_BINDING_CODE,
  AGDX_LIST_FILTERS_CODE,
  FILTER_OP_VERSION
} from "../../src/wire/codes.js"
import {
  ConsumerFilter,
  FilterExpr,
  consumerFilterDigest,
  decodeFilterMutationRequest,
  encodeFilterCatalogReply,
  type FilterBinding,
  type FilterGroupIdentity,
  type FilterMutationRequest,
  type FilterSummary
} from "../../src/wire/filter.js"

const identity: FilterGroupIdentity = {
  streamId: 1,
  streamCreatedAtMicros: 1n,
  topicId: 2,
  topicCreatedAtMicros: 2n,
  groupId: 7n
}
const definition = ConsumerFilter.json(FilterExpr.present("mode"))
const binding: FilterBinding = {
  group: { stream: "orbit", topic: "fleet_changes", group: "workers" },
  identity,
  filterId: 1,
  revision: 1,
  digest: consumerFilterDigest(definition),
  boundAtMicros: 3n,
  policyGeneration: 1n
}
const capabilities: Capabilities = {
  ...OPEN_CAPABILITIES,
  managed: true,
  hello: "answered",
  versions: {
    query: 1,
    control: 1,
    kv: 1,
    fork: 1,
    graph: 1,
    checkpoint: 1,
    agent: 1,
    filter: 1,
    features: 0n
  },
  filters: { native: false, catalog: true, groupPolicyReads: true, maxWaitMs: 0 }
}

function metadata(id: number, created: bigint): Uint8Array {
  const value = new Uint8Array(12)
  const view = new DataView(value.buffer)
  view.setUint32(0, id, true)
  view.setBigUint64(4, created, true)
  return value
}

function groupMetadata(id: number): Uint8Array {
  const name = new TextEncoder().encode("workers")
  const value = new Uint8Array(13 + name.byteLength)
  const view = new DataView(value.buffer)
  view.setUint32(0, id, true)
  view.setUint8(12, name.byteLength)
  value.set(name, 13)
  return value
}

function setup(replies: {
  readonly mutate?: (request: FilterMutationRequest) => Uint8Array
  readonly binding?: FilterBinding
  readonly onEnsure?: () => void
}) {
  let metadataReads = 0
  const transport = {
    ensureConsumerGroup: () => {
      replies.onEnsure?.()
      return Promise.resolve()
    },
    sendManaged: (code: number, payload: Uint8Array): Promise<Uint8Array> => {
      if (code === 200) return Promise.resolve(metadata(1, 1n))
      if (code === 300) return Promise.resolve(metadata(2, 2n))
      if (code === 600) {
        metadataReads += 1
        return Promise.resolve(groupMetadata(7))
      }
      if (code === AGDX_FILTER_MUTATE_CODE && replies.mutate !== undefined)
        return Promise.resolve(
          replies.mutate(decodeFilterMutationRequest(decodeOne(payload, "mutation"), "mutation"))
        )
      if (code === AGDX_GET_FILTER_BINDING_CODE && replies.binding !== undefined)
        return Promise.resolve(
          encodeNamed(
            encodeFilterCatalogReply({
              kind: "ok",
              outcome: { kind: "binding", binding: replies.binding }
            })
          )
        )
      return Promise.reject(new Error(`unexpected command ${code.toString()}`))
    }
  } as unknown as LaserTransport
  return {
    group: ConsumerGroup.create(
      transport,
      Topic.create(transport, "orbit", "fleet_changes"),
      { kind: "name", name: "workers" },
      {
        transport,
        capabilities: () => Promise.resolve(capabilities)
      }
    ),
    metadataReads: () => metadataReads
  }
}

function applied(operationId: bigint, proof = operationId): Uint8Array {
  return encodeNamed(
    encodeFilterCatalogReply({
      kind: "ok",
      outcome: {
        kind: "mutation",
        outcome: {
          v: FILTER_OP_VERSION,
          operationId,
          status: { kind: "applied", result: { kind: "bound", binding } },
          catalogPosition: { partitionId: 0, offset: 41n, operationId: proof }
        }
      }
    })
  )
}

void test("given_optional_group_setup_when_its_policy_is_appended_then_should_pin_the_original_group_identity", async () => {
  let submitted: FilterMutationRequest | undefined
  const fixture = setup({
    mutate: (request) => {
      submitted = request
      return applied(request.operationId)
    }
  })
  const created = await fixture.group.create({
    filter: definition,
    filterName: "safe-mode",
    operationId: 11n
  })
  assert.equal(created.id, 7)
  assert.equal(fixture.metadataReads(), 1, "setup reuses the identity it already read")
  assert.equal(submitted?.mutation.kind, "configure_group")
  assert.deepEqual(submitted.mutation.expectedIdentity, identity)
})

void test("given_a_mutation_reply_for_another_operation_when_setup_returns_then_should_report_partial_setup", async () => {
  for (const foreignProof of [false, true]) {
    const fixture = setup({
      mutate: (request) =>
        foreignProof
          ? applied(request.operationId, request.operationId + 1n)
          : applied(request.operationId + 1n)
    })
    await assert.rejects(
      fixture.group.create({ filter: definition, filterName: "safe-mode", operationId: 11n }),
      (error: unknown) =>
        error instanceof ConsumerGroupSetupError && error.cause instanceof ProtocolError
    )
  }
})

void test("given_a_recreated_group_when_its_old_binding_is_returned_then_should_refuse_the_mismatched_identity", async () => {
  const fixture = setup({ binding: { ...binding, identity: { ...identity, groupId: 8n } } })
  await assert.rejects(fixture.group.filter().get(), FilterExecutionError)
})

void test("given_a_group_without_capability_context_when_consuming_then_should_not_poll_natively", async () => {
  const group = ConsumerGroup.create(
    {} as LaserTransport,
    Topic.create({} as LaserTransport, "orbit", "fleet_changes"),
    {
      kind: "name",
      name: "workers"
    }
  )
  await assert.rejects(group.consumer(), UnsupportedError)
})

void test("given_an_unanswered_probe_when_building_a_group_consumer_then_should_probe_again", async () => {
  let refreshed = 0
  let joined = 0
  const transport = {
    joinConsumerGroup: () => {
      joined += 1
      return Promise.resolve()
    }
  } as unknown as LaserTransport
  const group = ConsumerGroup.create(
    transport,
    Topic.create(transport, "orbit", "fleet_changes"),
    { kind: "name", name: "workers" },
    {
      transport,
      capabilities: () => Promise.resolve(OPEN_CAPABILITIES),
      refreshCapabilities: () => {
        refreshed += 1
        return Promise.resolve({ ...OPEN_CAPABILITIES, hello: "rejected" as const })
      }
    }
  )
  const consumer = await group.consumer()
  assert.equal(refreshed, 1)
  assert.equal(joined, 1)
  await consumer.shutdown().catch(() => undefined)
})

void test("given_a_permanent_join_failure_when_init_retries_are_set_then_should_not_retry", async () => {
  let joins = 0
  const transport = {
    joinConsumerGroup: () => {
      joins += 1
      return Promise.reject(new TransportError("forbidden", false))
    }
  } as unknown as LaserTransport
  const group = ConsumerGroup.create(
    transport,
    Topic.create(transport, "orbit", "fleet_changes"),
    { kind: "name", name: "workers" },
    {
      transport,
      capabilities: () => Promise.resolve({ ...OPEN_CAPABILITIES, hello: "rejected" as const })
    }
  )
  await assert.rejects(
    group.consumer({ initRetries: { retries: 3, intervalMs: 1 } }),
    TransportError
  )
  assert.equal(joins, 1)
})

void test("given_a_group_start_when_the_consumer_reads_its_first_assignment_then_should_seed_only_the_partitions_the_group_never_stored", async () => {
  const { group, stores, polls, reads } = seedSetup({ stored: new Map([[0, 9n]]), records: 10 })
  const consumer = await group.consumer({ startAt: { kind: "offset", value: 4n } })
  await assert.rejects(consumer.nextWithin(50), TimeoutError)
  assert.deepEqual(stores, [
    [1, 3n],
    [2, 3n]
  ])
  assert.deepEqual(polls, [])
  assert.deepEqual(reads[0], { kind: "next" })
  await consumer.shutdown().catch(() => undefined)
})

void test("given_each_group_start_when_the_consumer_reads_a_partition_the_group_never_stored_then_should_store_the_position_before_its_first_record", async () => {
  const cases: readonly {
    readonly startAt: ConsumerStart
    readonly records: number
    readonly stored: readonly bigint[]
  }[] = [
    { startAt: { kind: "next" }, records: 10, stored: [] },
    { startAt: { kind: "first" }, records: 10, stored: [] },
    { startAt: { kind: "offset", value: 0n }, records: 10, stored: [] },
    { startAt: { kind: "offset", value: 4n }, records: 10, stored: [3n] },
    { startAt: { kind: "timestampMicros", value: 700n }, records: 10, stored: [6n] },
    { startAt: { kind: "timestampMicros", value: 5_000n }, records: 10, stored: [9n] },
    { startAt: { kind: "timestampMicros", value: 700n }, records: 0, stored: [] },
    { startAt: { kind: "last" }, records: 10, stored: [8n] },
    { startAt: { kind: "last" }, records: 1, stored: [] },
    { startAt: { kind: "last" }, records: 0, stored: [] }
  ]
  for (const { startAt, records, stored } of cases) {
    const { group, stores } = seedSetup({ stored: new Map(), records, partitions: [0] })
    const consumer = await group.consumer({ startAt })
    await assert.rejects(consumer.nextWithin(20), TimeoutError)
    assert.deepEqual(
      stores.map(([, offset]) => offset),
      stored,
      `${startAt.kind} over ${String(records)} records`
    )
    await consumer.shutdown().catch(() => undefined)
  }
})

void test("given_a_group_start_when_partitions_arrive_over_several_syncs_then_should_seed_each_one_the_group_never_stored_once_it_is_owned", async () => {
  const { group, stores, reads } = seedSetup({
    stored: new Map(),
    records: 10,
    assignments: [[0], [0, 1]],
    partitionCount: 2
  })
  const consumer = await group.consumer({ startAt: { kind: "last" } })
  await assert.rejects(consumer.nextWithin(20), TimeoutError)
  assert.deepEqual(stores, [
    [0, 8n],
    [1, 8n]
  ])
  assert.ok(reads.every((start) => (start as ConsumerStart).kind === "next"))
  await consumer.shutdown().catch(() => undefined)
})

void test("given_a_partition_first_owned_after_records_arrive_when_started_at_last_then_should_store_the_seed_taken_at_the_join", async () => {
  const { group, stores, setRecords } = seedSetup({
    stored: new Map(),
    records: 3,
    assignments: [[], [0]],
    partitionCount: 1
  })
  const consumer = await group.consumer({ startAt: { kind: "last" } })
  assert.deepEqual(stores, [])
  setRecords(5)
  await assert.rejects(consumer.nextWithin(20), TimeoutError)
  assert.deepEqual(stores, [[0, 1n]])
  await consumer.shutdown().catch(() => undefined)
})

void test("given_a_start_lookup_that_fails_when_the_group_consumer_joins_then_should_fail_with_the_iggy_error_and_leave_the_group", async () => {
  const refusal = new TransportError("failed to poll messages from topic `fleet_changes`", false)
  const { group, stores, leaves } = seedSetup({
    stored: new Map(),
    records: 10,
    lookupFailure: refusal
  })
  await assert.rejects(group.consumer({ startAt: { kind: "last" } }), (error) => error === refusal)
  assert.deepEqual(stores, [])
  assert.equal(leaves(), 1)
})

void test("given_a_held_partition_the_server_has_not_installed_when_its_seed_is_refused_as_not_owned_then_should_start_and_store_it_before_the_first_read", async () => {
  const notOwned = Object.assign(new Error("partition not owned"), { errorCode: 5009 })
  const { group, stores, reads } = seedSetup({
    stored: new Map(),
    records: 10,
    partitions: [0],
    storeRefusals: [notOwned]
  })
  const consumer = await group.consumer({ startAt: { kind: "last" } })
  assert.deepEqual(stores, [])
  await assert.rejects(consumer.nextWithin(20), TimeoutError)
  assert.deepEqual(stores, [[0, 8n]])
  assert.ok(reads.length > 0)
  await consumer.shutdown().catch(() => undefined)
})

void test("given_a_held_partition_not_yet_routable_when_its_seed_is_refused_as_transient_then_should_start_and_store_it_before_the_first_read", async () => {
  const transient = Object.assign(new Error("request transiently not accepted"), { errorCode: 58 })
  const { group, stores, reads } = seedSetup({
    stored: new Map(),
    records: 10,
    partitions: [0],
    storeRefusals: [transient]
  })
  const consumer = await group.consumer({ startAt: { kind: "last" } })
  assert.deepEqual(stores, [])
  await assert.rejects(consumer.nextWithin(20), TimeoutError)
  assert.deepEqual(stores, [[0, 8n]])
  assert.ok(reads.length > 0)
  await consumer.shutdown().catch(() => undefined)
})

void test("given_a_refused_seed_when_the_group_consumer_starts_then_should_fail_with_the_iggy_error_and_leave_the_group", async () => {
  const refusal = new TransportError("failed to store offset for topic `fleet_changes`", false)
  const { group, reads, leaves } = seedSetup({
    stored: new Map(),
    records: 10,
    storeFailure: refusal
  })
  await assert.rejects(
    group.consumer({ startAt: { kind: "offset", value: 100n } }),
    (error) => error === refusal
  )
  assert.deepEqual(reads, [])
  assert.equal(leaves(), 1)
})

// A native group of `partitions` whose records `0..records` were stamped
// `offset * 100` microseconds, with the group's `stored` offsets.
function seedSetup(options: {
  readonly stored: ReadonlyMap<number, bigint>
  readonly records: number
  readonly partitions?: readonly number[]
  readonly partitionCount?: number
  readonly assignments?: readonly (readonly number[])[]
  readonly storeFailure?: Error
  readonly storeRefusals?: readonly Error[]
  readonly lookupFailure?: Error
}) {
  const refusals = [...(options.storeRefusals ?? [])]
  let syncs = 0
  let records = options.records
  let leaves = 0
  const stores: [number, bigint][] = []
  const polls: unknown[] = []
  const reads: unknown[] = []
  const record = (offset: number) => ({
    payload: new Uint8Array(),
    partitionId: 0,
    offset: BigInt(offset),
    timestampMicros: BigInt(offset * 100),
    headers: new Map()
  })
  const transport = {
    joinConsumerGroup: () => Promise.resolve(),
    leaveConsumerGroup: () => {
      leaves += 1
      return Promise.resolve()
    },
    findTopicPartitionCount: () =>
      Promise.resolve(options.partitionCount ?? options.partitions?.length ?? 3),
    syncConsumerGroup: () => {
      const assignments = options.assignments ?? [options.partitions ?? [0, 1, 2]]
      syncs += 1
      return Promise.resolve({
        generation: 1n,
        partitions: assignments[Math.min(syncs, assignments.length) - 1] ?? []
      })
    },
    getConsumerOffset: (_stream: string, _topic: string, target: unknown, partition: number) => {
      assert.deepEqual(target, { kind: "group", name: "workers" })
      const stored = options.stored.get(partition)
      return Promise.resolve(
        stored === undefined ? undefined : { storedOffset: stored, currentOffset: stored }
      )
    },
    storeOffset: (
      _stream: string,
      _topic: string,
      target: unknown,
      partition: number,
      offset: bigint
    ) => {
      assert.deepEqual(target, { kind: "group", name: "workers" })
      if (options.storeFailure !== undefined) return Promise.reject(options.storeFailure)
      const refusal = refusals.shift()
      if (refusal !== undefined) return Promise.reject(refusal)
      stores.push([partition, offset])
      return Promise.resolve()
    },
    pollMessages: (
      _stream: string,
      _topic: string,
      target: { readonly kind: string },
      start: ConsumerStart,
      count: number,
      autoCommit: boolean
    ) => {
      if (target.kind === "group") {
        reads.push(start)
        return Promise.resolve([])
      }
      polls.push(start)
      assert.equal(target.kind, "single")
      assert.equal(count, 1)
      assert.equal(autoCommit, false)
      if (options.lookupFailure !== undefined) return Promise.reject(options.lookupFailure)
      if (records === 0) return Promise.resolve([])
      if (start.kind === "last") return Promise.resolve([record(records - 1)])
      if (start.kind !== "timestampMicros") throw new Error(`unexpected lookup ${start.kind}`)
      const at = Array.from({ length: records }, (_, offset) => offset).find(
        (offset) => BigInt(offset * 100) >= start.value
      )
      return Promise.resolve(at === undefined ? [] : [record(at)])
    }
  } as unknown as LaserTransport
  return {
    group: ConsumerGroup.create(
      transport,
      Topic.create(transport, "orbit", "fleet_changes"),
      { kind: "name", name: "workers" },
      {
        transport,
        capabilities: () => Promise.resolve({ ...OPEN_CAPABILITIES, hello: "rejected" as const })
      }
    ),
    stores,
    polls,
    reads,
    leaves: () => leaves,
    setRecords: (count: number) => {
      records = count
    }
  }
}

function ownerSetup(rows: readonly FilterSummary[]) {
  const ownIdentity = { ...identity, groupId: 9n }
  let items = [...rows]
  const lookups: CborMap[] = []
  const dropped: number[] = []
  const remembered: bigint[] = []
  let failNextLookup = false
  const transport = {
    sendManaged: (code: number, payload: Uint8Array): Promise<Uint8Array> => {
      if (code === AGDX_LIST_FILTERS_CODE) {
        const request = expectMap(decodeOne(payload, "list"), "list")
        lookups.push(request)
        if (failNextLookup) {
          failNextLookup = false
          return Promise.reject(new TransportError("catalog page unavailable", true))
        }
        const pageSize = field.requiredU32(request, "page_size", "list")
        return Promise.resolve(
          encodeNamed(
            encodeFilterCatalogReply({
              kind: "ok",
              outcome: {
                kind: "filters",
                page: { items: items.slice(0, pageSize), page: 0, pageSize, total: items.length }
              }
            })
          )
        )
      }
      if (code === AGDX_FILTER_MUTATE_CODE) {
        const request = decodeFilterMutationRequest(decodeOne(payload, "mutation"), "mutation")
        assert.equal(request.mutation.kind, "drop")
        const filterId = request.mutation.filterId
        dropped.push(filterId)
        items = items.filter((item) => item.id !== filterId)
        return Promise.resolve(
          encodeNamed(
            encodeFilterCatalogReply({
              kind: "ok",
              outcome: {
                kind: "mutation",
                outcome: {
                  v: FILTER_OP_VERSION,
                  operationId: request.operationId,
                  status: { kind: "applied", result: { kind: "dropped", filterId } },
                  catalogPosition: {
                    partitionId: 0,
                    offset: 42n,
                    operationId: request.operationId
                  }
                }
              }
            })
          )
        )
      }
      return Promise.reject(new Error(`unexpected command ${code.toString()}`))
    }
  } as unknown as LaserTransport
  const filters = new Filters(transport, () => Promise.resolve(capabilities))
  return {
    ownIdentity,
    filter: GroupFilter.create({
      streamName: "orbit",
      topicName: "fleet_changes",
      filters: () => filters,
      groupRef: () => Promise.resolve(binding.group),
      native: () => Promise.resolve({ id: 9, name: "workers", identity: ownIdentity }),
      remember: (position) => {
        if (position !== undefined) remembered.push(position.offset)
      }
    }),
    lookups,
    dropped,
    remembered,
    failNextLookup: () => {
      failNextLookup = true
    }
  }
}

function ownedSummary(id: number, owner: FilterGroupIdentity): FilterSummary {
  return {
    id,
    name: "safe-mode",
    description: "",
    state: "active",
    latestRevision: 1,
    latestDigest: binding.digest,
    codec: "json",
    bindings: 1,
    createdAtMicros: 1n,
    updatedAtMicros: 1n,
    owner: { identity: owner, group: binding.group }
  }
}

void test("given_a_group_with_its_own_filter_when_summarized_then_should_look_it_up_by_owner", async () => {
  const own = ownedSummary(1, { ...identity, groupId: 9n })
  const fixture = ownerSetup([own])
  assert.deepEqual(await fixture.filter.summary(), own)
  const [lookup] = fixture.lookups
  assert.ok(lookup !== undefined)
  assert.deepEqual([...lookup.keys()], ["v", "owner", "page", "page_size"])
  assert.equal(lookup.has("name_contains"), false, "no filter is looked up by a derived name")
  assert.equal(field.requiredU32(lookup, "page_size", "list"), 1)
  assert.equal(
    field.requiredU64(expectMap(lookup.get("owner"), "owner"), "group_id", "owner"),
    fixture.ownIdentity.groupId
  )
})

void test("given_a_group_without_its_own_filter_when_summarized_or_deleted_then_should_report_none", async () => {
  const fixture = ownerSetup([])
  assert.equal(await fixture.filter.summary(), undefined)
  assert.equal(await fixture.filter.delete(), false)
  assert.deepEqual(fixture.dropped, [])
  assert.deepEqual(fixture.remembered, [])
})

void test("given_a_group_with_its_own_filter_when_deleted_then_should_drop_exactly_that_filter_once", async () => {
  const fixture = ownerSetup([ownedSummary(1, { ...identity, groupId: 9n })])
  assert.equal(await fixture.filter.delete(), true)
  assert.deepEqual(fixture.dropped, [1])
  assert.deepEqual(fixture.remembered, [42n])
  assert.equal(await fixture.filter.delete(), false)
  assert.deepEqual(fixture.dropped, [1], "a repeated delete drops nothing more")
})

void test("given_a_row_owned_by_another_group_when_deleted_then_should_drop_nothing", async () => {
  const fixture = ownerSetup([ownedSummary(5, { ...identity, groupId: 90n })])
  assert.equal(await fixture.filter.summary(), undefined)
  assert.equal(await fixture.filter.delete(), false)
  assert.deepEqual(fixture.dropped, [])
})

void test("given_a_failed_catalog_lookup_when_deletion_is_retried_then_should_report_the_error_and_converge", async () => {
  const fixture = ownerSetup([ownedSummary(1, { ...identity, groupId: 9n })])
  fixture.failNextLookup()
  await assert.rejects(fixture.filter.delete(), TransportError)
  assert.deepEqual(fixture.dropped, [])
  assert.equal(await fixture.filter.delete(), true)
  assert.deepEqual(fixture.dropped, [1])
})

void test("given_a_definition_without_a_name_when_the_group_is_created_then_should_refuse_before_any_request", async () => {
  let ensured = 0
  const fixture = setup({
    mutate: () => assert.fail("nothing may be sent"),
    onEnsure: () => {
      ensured += 1
    }
  })
  await assert.rejects(
    fixture.group.create({ filter: definition }),
    new InvalidError("pass `filterName` with `filter`")
  )
  await assert.rejects(
    fixture.group.create({ filterName: "safe-mode" }),
    new InvalidError("pass `filterName` with `filter`")
  )
  await assert.rejects(
    fixture.group.create({ filter: definition, filterName: "has space" }),
    InvalidError
  )
  assert.equal(ensured, 0)
})

void test("given_a_named_definition_when_the_group_is_created_or_configured_then_should_save_it_under_that_name", async () => {
  const names: string[] = []
  const fixture = setup({
    mutate: (request) => {
      if (
        request.mutation.kind === "configure_group" &&
        request.mutation.policy.kind === "definition"
      )
        names.push(request.mutation.policy.name)
      return applied(request.operationId)
    }
  })
  await fixture.group.create({ filter: definition, filterName: "safe-mode", operationId: 11n })
  await fixture.group.filter().configure("renamed", definition)
  await fixture.group
    .filter()
    .configureWith({ kind: "definition", name: "spec", filter: definition })
  assert.deepEqual(names, ["safe-mode", "renamed", "spec"])
  await assert.rejects(fixture.group.filter().configure("", definition), InvalidError)
  assert.deepEqual(names, ["safe-mode", "renamed", "spec"], "an invalid name is never sent")
})

void test("given_a_laser_group_when_it_routes_a_read_then_should_look_the_partition_route_up", async () => {
  const requests: (readonly [number, Uint8Array])[] = []
  // incarnation, metadata op, view, partition count, then primary replica 2
  let route = new Uint8Array(25)
  route[24] = 2
  const client = {
    clientProvider: () => Promise.resolve({ protocol: "vsr" }),
    destroy: () => Promise.resolve(),
    sendBinaryRequest: (code: number, payload: Buffer) => {
      requests.push([code, new Uint8Array(payload)])
      return Promise.resolve(Buffer.from(route))
    }
  } as unknown as IggyClient
  await using laser = await Laser.fromClient(client)
  const context = (laser as unknown as { groupContext(): GroupContext }).groupContext()
  assert.equal(await context.transport.partitionPrimary?.("orbit", "fleet", 3), 2)
  const encoded = new TextEncoder()
  assert.deepEqual(requests, [
    [
      105,
      Uint8Array.of(2, 5, ...encoded.encode("orbit"), 2, 5, ...encoded.encode("fleet"), 3, 0, 0, 0)
    ]
  ])
  route = new Uint8Array(24)
  await assert.rejects(
    context.transport.partitionPrimary?.("orbit", "fleet", 3) ?? Promise.resolve(),
    ProtocolError,
    "a route of another length comes from a mismatched build"
  )
})

void test("given_a_topic_when_addressing_its_group_then_should_hand_back_the_same_topic", () => {
  const topic = Topic.create({} as LaserTransport, "orbit", "fleet_changes")
  const group = topic.consumerGroup("workers")
  assert.equal(group.topic, topic)
  assert.equal(topic.consumerGroupId(7n).topic, topic)
})
