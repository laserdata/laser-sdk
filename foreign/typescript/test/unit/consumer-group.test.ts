import assert from "node:assert/strict"
import { test } from "node:test"
import { OPEN_CAPABILITIES, type Capabilities } from "../../src/client/capabilities.js"
import {
  ConsumerGroupSetupError,
  FilterExecutionError,
  ProtocolError,
  TransportError
} from "../../src/client/errors.js"
import type { LaserTransport } from "../../src/iggy/apache-iggy.js"
import { Filters } from "../../src/managed/filters.js"
import { ConsumerGroup, GroupFilter } from "../../src/stream/consumer-group.js"
import { decodeOne, encodeNamed, expectMap, field } from "../../src/wire/cbor.js"
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
  filters: { native: false, catalog: true, groupPolicyReads: true }
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
}) {
  let metadataReads = 0
  const transport = {
    ensureConsumerGroup: () => Promise.resolve(),
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
    group: new ConsumerGroup(
      transport,
      "orbit",
      "fleet_changes",
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
  const created = await fixture.group.create({ filter: definition, operationId: 11n })
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
      fixture.group.create({ filter: definition, operationId: 11n }),
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
  const group = new ConsumerGroup({} as LaserTransport, "orbit", "fleet_changes", {
    kind: "name",
    name: "workers"
  })
  await assert.rejects(group.consumer(), TransportError)
})

function deletionSetup(collisionCount: number, present = true) {
  const ownIdentity = { ...identity, groupId: 9n }
  const summary = (id: number, groupId: bigint): FilterSummary => ({
    id,
    name: `group:1:1:2:2:${groupId.toString()}`,
    description: "",
    state: "active",
    latestRevision: 1,
    latestDigest: binding.digest,
    codec: "json",
    bindings: 1,
    createdAtMicros: 1n,
    updatedAtMicros: 1n
  })
  let items = Array.from({ length: collisionCount }, (_, index) =>
    summary(index + 2, BigInt(index < 8 ? 90 + index : 900 + index - 8))
  )
  if (present) items.push(summary(1, 9n))
  items.sort((left, right) => right.id - left.id)
  const pages: number[] = []
  const cursors: (number | undefined)[] = []
  const dropped: number[] = []
  const remembered: bigint[] = []
  let failedPage: number | undefined
  let changeBetweenPages = false
  let repeatPage = false
  const transport = {
    sendManaged: (code: number, payload: Uint8Array): Promise<Uint8Array> => {
      if (code === AGDX_LIST_FILTERS_CODE) {
        const request = expectMap(decodeOne(payload, "list"), "list")
        const page = field.requiredU32(request, "page", "list")
        const pageSize = field.requiredU32(request, "page_size", "list")
        const name = field.requiredString(request, "name_contains", "list")
        const beforeId = field.optionalU32(request, "before_id", "list")
        const lookup = pages.length
        pages.push(page)
        cursors.push(beforeId)
        if (lookup === failedPage) {
          failedPage = undefined
          return Promise.reject(new TransportError("catalog page unavailable", true))
        }
        if (beforeId !== undefined && changeBetweenPages) {
          items = items.filter((item) => item.id <= 4)
          items.unshift(summary(1001, 999n))
          changeBetweenPages = false
        }
        const matching = items.filter(
          (item) =>
            item.name.includes(name) && (beforeId === undefined || repeatPage || item.id < beforeId)
        )
        return Promise.resolve(
          encodeNamed(
            encodeFilterCatalogReply({
              kind: "ok",
              outcome: {
                kind: "filters",
                page: {
                  items: matching.slice(page * pageSize, (page + 1) * pageSize),
                  page,
                  pageSize,
                  total: matching.length
                }
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
    pages,
    cursors,
    dropped,
    remembered,
    remaining: () => items.map((item) => item.id),
    failPageOnce: (page: number) => {
      failedPage = page
    },
    changeAfterFirstPage: () => {
      changeBetweenPages = true
    },
    repeatPage: () => {
      repeatPage = true
    }
  }
}

void test("given_group_9_and_groups_90_through_97_when_deleted_then_should_find_the_exact_filter_on_the_next_page", async () => {
  const fixture = deletionSetup(8)
  assert.equal(await fixture.filter.delete(), true)
  assert.deepEqual(fixture.pages, [0, 0])
  assert.deepEqual(fixture.cursors, [undefined, 2])
  assert.deepEqual(fixture.dropped, [1])
  assert.deepEqual(fixture.remembered, [42n])
  assert.equal(fixture.remaining().length, 8)
  assert.equal(await fixture.filter.delete(), false)
  assert.deepEqual(fixture.dropped, [1], "a repeated delete preserves the other groups")
})

void test("given_three_pages_of_prefix_collisions_when_deleted_then_should_find_the_exact_filter_on_the_fourth_page", async () => {
  const fixture = deletionSetup(24)
  assert.equal(await fixture.filter.delete(), true)
  assert.deepEqual(fixture.pages, [0, 0, 0, 0])
  assert.deepEqual(fixture.cursors, [undefined, 18, 10, 2])
  assert.deepEqual(fixture.dropped, [1])
  assert.equal(fixture.remaining().length, 24)
})

void test("given_only_prefix_collisions_when_deleted_then_should_scan_every_page_and_return_false", async () => {
  const fixture = deletionSetup(17, false)
  assert.equal(await fixture.filter.delete(), false)
  assert.deepEqual(fixture.pages, [0, 0, 0])
  assert.deepEqual(fixture.cursors, [undefined, 11, 3])
  assert.deepEqual(fixture.dropped, [])
  assert.deepEqual(fixture.remembered, [])
})

void test("given_a_failed_catalog_page_when_deletion_is_retried_then_should_report_the_error_and_converge", async () => {
  for (const failedPage of [0, 1, 2, 3]) {
    const fixture = deletionSetup(24)
    fixture.failPageOnce(failedPage)
    await assert.rejects(fixture.filter.delete(), TransportError)
    assert.deepEqual(fixture.dropped, [])
    assert.deepEqual(fixture.remembered, [])
    assert.equal(fixture.remaining().length, 25)
    assert.equal(await fixture.filter.delete(), true)
    assert.equal(await fixture.filter.delete(), false)
    assert.deepEqual(fixture.dropped, [1])
    assert.equal(fixture.remaining().length, 24)
  }
})

void test("given_deleted_and_new_prefix_collisions_between_pages_when_deleted_then_should_still_find_the_exact_filter", async () => {
  const fixture = deletionSetup(8)
  fixture.changeAfterFirstPage()
  assert.equal(await fixture.filter.delete(), true)
  assert.deepEqual(fixture.cursors, [undefined, 2])
  assert.deepEqual(fixture.dropped, [1])
  assert.deepEqual(fixture.remaining(), [1001, 4, 3, 2])
  assert.equal(await fixture.filter.delete(), false)
})

void test("given_a_repeated_catalog_cursor_when_deleted_then_should_fail_without_deleting_another_filter", async () => {
  const fixture = deletionSetup(8)
  fixture.repeatPage()
  await assert.rejects(
    fixture.filter.delete(),
    (error: unknown) =>
      error instanceof FilterExecutionError && error.reason === "catalog_unavailable"
  )
  assert.deepEqual(fixture.cursors, [undefined, 2])
  assert.deepEqual(fixture.dropped, [])
  assert.equal(fixture.remaining().length, 9)
})
