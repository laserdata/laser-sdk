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
import { ConsumerGroup } from "../../src/stream/consumer-group.js"
import { decodeOne, encodeNamed } from "../../src/wire/cbor.js"
import {
  AGDX_FILTER_MUTATE_CODE,
  AGDX_GET_FILTER_BINDING_CODE,
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
  type FilterMutationRequest
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
