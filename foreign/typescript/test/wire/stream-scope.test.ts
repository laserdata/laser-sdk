import assert from "node:assert/strict"
import { test } from "node:test"
import {
  authzErrorResultCode,
  decodeAuthzError,
  encodeAuthzError,
  scopedResource,
  splitScopedResource
} from "../../src/wire/authz.js"
import {
  decodeDecodeRecord,
  decodeGetSchema,
  decodeListSchemas,
  decodeRegisterSchema,
  encodeDecodeRecord,
  encodeGetSchema,
  encodeListSchemas,
  encodeRegisterSchema
} from "../../src/wire/browse.js"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"
import { decodeControlEnvelope, encodeControlEnvelope } from "../../src/wire/control.js"
import {
  ConsumerFilter,
  FilterExpr,
  consumerFilterDigest,
  consumerFilterJson,
  decodeConsumerFilterJson,
  validateConsumerFilter
} from "../../src/wire/filter.js"
import { validateForkId } from "../../src/wire/fork.js"
import {
  NodeId,
  decodeGraphNeighbors,
  decodeGraphQuery,
  encodeGraphNeighbors,
  encodeGraphQuery
} from "../../src/wire/graph.js"
import {
  decodeKvDeleteMany,
  decodeKvScan,
  encodeKvDeleteMany,
  encodeKvScan
} from "../../src/wire/kv.js"

function roundTrip<Value>(
  encode: (value: Value) => Map<string, unknown>,
  decode: (map: ReturnType<typeof expectMap>, context: string) => Value,
  value: Value
): { readonly bytes: Uint8Array; readonly back: Value } {
  const bytes = encodeNamed(encode(value))
  return { bytes, back: decode(expectMap(decodeOne(bytes, "scope"), "scope"), "scope") }
}

void test("given_a_local_name_when_scoped_then_should_prefix_the_stream_once", () => {
  assert.equal(scopedResource("acme", "agent.keys"), "stream:acme/agent.keys")
  assert.equal(scopedResource("acme", "a/b"), "stream:acme/a/b")
  const scoped = scopedResource("acme", "sessions")
  assert.equal(scopedResource("acme", scoped), scoped)
  assert.equal(scopedResource("other", "stream:acme/sessions"), "stream:acme/sessions")
})

void test("given_names_when_split_then_should_return_stream_and_local_part_only_when_scoped", () => {
  assert.deepEqual(splitScopedResource("stream:acme/agent.keys"), ["acme", "agent.keys"])
  assert.deepEqual(splitScopedResource("stream:acme/a/b"), ["acme", "a/b"])
  for (const bare of [
    "agent.keys",
    "stream:acme",
    "stream:/local",
    "stream:acme/",
    "streams:acme/local"
  ]) {
    assert.equal(splitScopedResource(bare), undefined, bare)
  }
})

void test("given_lens_requests_when_encoded_then_should_carry_the_stream_only_when_set", () => {
  const scan = { namespace: "n", conversation: "c", limit: 5 }
  const bare = roundTrip(encodeKvScan, decodeKvScan, scan)
  const scoped = roundTrip(encodeKvScan, decodeKvScan, { ...scan, stream: "acme" })
  assert.equal(bare.back.stream, undefined)
  assert.equal(scoped.back.stream, "acme")
  assert.ok(scoped.bytes.byteLength > bare.bytes.byteLength)
  const many = roundTrip(encodeKvDeleteMany, decodeKvDeleteMany, {
    namespace: "n",
    conversation: "c",
    stream: "acme"
  })
  assert.equal(many.back.stream, "acme")
  const query = roundTrip(encodeGraphQuery, decodeGraphQuery, {
    graph: "g",
    start: { kind: "ids", ids: [] },
    traverse: [],
    return: "nodes",
    limit: 1,
    consistency: "eventual",
    conversation: "c",
    stream: "acme"
  })
  assert.equal(query.back.stream, "acme")
  const neighbors = roundTrip(encodeGraphNeighbors, decodeGraphNeighbors, {
    graph: "g",
    node: NodeId.content("Person", new TextEncoder().encode("Ada")),
    dir: "out",
    depth: 1,
    limit: 1,
    conversation: "c",
    stream: "acme"
  })
  assert.equal(neighbors.back.stream, "acme")
})

void test("given_schema_requests_when_encoded_then_should_carry_the_stream_only_when_set", () => {
  assert.equal(roundTrip(encodeGetSchema, decodeGetSchema, { v: 1, id: 7 }).back.stream, undefined)
  assert.equal(
    roundTrip(encodeGetSchema, decodeGetSchema, { v: 1, id: 7, stream: "acme" }).back.stream,
    "acme"
  )
  assert.equal(
    roundTrip(encodeListSchemas, decodeListSchemas, { v: 1, stream: "acme" }).back.stream,
    "acme"
  )
  assert.equal(
    roundTrip(encodeRegisterSchema, decodeRegisterSchema, {
      v: 1,
      source: { kind: "jsonSchema", schema: "{}" },
      stream: "acme"
    }).back.stream,
    "acme"
  )
  assert.equal(
    roundTrip(encodeDecodeRecord, decodeDecodeRecord, {
      v: 1,
      id: 7,
      payload: new Uint8Array([1]),
      stream: "acme"
    }).back.stream,
    "acme"
  )
  const envelope = roundTrip(encodeControlEnvelope, decodeControlEnvelope, {
    v: 1,
    timestampMicros: 1n,
    command: { kind: "dropSchema", id: 7 },
    stream: "acme"
  })
  assert.equal(envelope.back.stream, "acme")
})

void test("given_a_schema_stream_when_encoded_then_should_change_only_a_filter_that_sets_it", () => {
  const filter = ConsumerFilter.avro(FilterExpr.pred("mode", "eq", "safe"), [7])
  assert.ok(!consumerFilterJson(filter).includes("schema_stream"))
  const scoped = ConsumerFilter.withSchemaStream(filter, "acme")
  validateConsumerFilter(scoped)
  assert.notDeepEqual(consumerFilterDigest(scoped), consumerFilterDigest(filter))
  assert.equal(decodeConsumerFilterJson(consumerFilterJson(scoped)).schemaStream, "acme")
  assert.throws(() => {
    validateConsumerFilter(ConsumerFilter.withSchemaStream(filter, ""))
  })
  assert.throws(() => {
    validateConsumerFilter(ConsumerFilter.withSchemaStream(filter, "a/b"))
  })
  assert.throws(() => {
    validateConsumerFilter(
      ConsumerFilter.withSchemaStream(ConsumerFilter.json(FilterExpr.pred("a", "eq", 1)), "acme")
    )
  })
})

void test("given_fork_ids_when_validated_then_should_accept_a_safelisted_stream_scope", () => {
  validateForkId("try-1")
  validateForkId("stream:acme/try-1")
  for (const bad of ["stream:a b/try", "stream:acme/bad id", "bad id", ""]) {
    assert.throws(() => {
      validateForkId(bad)
    }, bad)
  }
})

void test("given_a_tenancy_violation_when_round_tripped_then_should_keep_the_reason_and_map_to_invalid_argument", () => {
  const error = { kind: "tenancyViolation", message: "prefix acme" } as const
  const back = decodeAuthzError(
    decodeOne(encodeNamed(encodeAuthzError(error) as ReadonlyMap<string, unknown>), "authz"),
    "authz"
  )
  assert.deepEqual(back, error)
  assert.deepEqual(authzErrorResultCode(error), { kind: "known", name: "InvalidArgument" })
})
