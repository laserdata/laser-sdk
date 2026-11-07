import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import { CodecError, InvalidError } from "../../src/client/errors.js"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"
import {
  decodeControlEnvelope,
  decodeSchemaDef,
  encodeControlEnvelope
} from "../../src/wire/control.js"
import { CompiledFilter, type DecodeLimits } from "../../src/wire/filter-eval.js"
import {
  ConsumerFilter,
  ExactDecimal,
  FieldPath,
  FilterExpr,
  consumerFilterJson,
  decodeCatalogPosition,
  decodeConsumerFilter,
  decodeFilterCatalogReply,
  decodeFilterMutationRequest,
  decodeFilterReply,
  decodeFilteredAck,
  encodeFilteredAck,
  decodeFilteredPollRequest,
  decodeFilteredWaitCancel,
  decodeFilteredWaitRequest,
  decodeHeaderScalar,
  encodeConsumerFilter,
  encodeCatalogPosition,
  encodeFilterCatalogReply,
  encodeFilterMutationRequest,
  encodeFilterReply,
  encodeFilteredPollRequest,
  encodeFilteredWaitCancel,
  encodeFilteredWaitRequest,
  encodeListFilters,
  type FilterCatalogReply,
  type FilterMutationRequest,
  type FilteredWaitCancel,
  type FilteredWaitRequest,
  parseCanonicalJson,
  rustDouble,
  timestampFormatMicrosFromText,
  validateConsumerFilter,
  validateFilterMutationRequest,
  validateFilterName,
  validateFilterTestRequest,
  validateFilteredWaitCancel,
  validateFilteredWaitRequest,
  validateListFilters
} from "../../src/wire/filter.js"
import {
  MAX_FILTERED_WAIT_CANCEL_IDS,
  MAX_FILTERED_WAIT_MS,
  MAX_FILTERED_WAIT_PARTITIONS,
  MAX_FILTER_NAME_BYTES
} from "../../src/wire/limits.js"

const FIXTURES_DIR = path.resolve(process.cwd(), "../../wire/fixtures")

async function readFixture(name: string): Promise<Uint8Array> {
  const buffer = await readFile(path.join(FIXTURES_DIR, name))
  return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength)
}

async function readText(name: string): Promise<string> {
  return readFile(path.join(FIXTURES_DIR, name), "utf8")
}

async function assertRoundTrip<T>(
  name: string,
  decode: (value: unknown, context: string) => T,
  encode: (value: T) => ReadonlyMap<string, unknown>
): Promise<T> {
  const bytes = await readFixture(name)
  const decoded = decode(decodeOne(bytes, name), name)
  assert.deepEqual(Buffer.from(encodeNamed(encode(decoded))), Buffer.from(bytes), name)
  return decoded
}

void test("given_the_rust_filter_fixtures_when_decoded_then_should_re_encode_byte_identically", async () => {
  await assertRoundTrip("filter_ack.bin", decodeFilteredAck, encodeFilteredAck)
  await assertRoundTrip("filter_ack_unfiltered.bin", decodeFilteredAck, encodeFilteredAck)
  await assertRoundTrip("filter_reply_acknowledged.bin", decodeFilterReply, encodeFilterReply)
  const filter = await assertRoundTrip(
    "filter_consumer_filter.bin",
    decodeConsumerFilter,
    encodeConsumerFilter
  )
  assert.equal(filter.codec, "json")
  const mutation = await assertRoundTrip(
    "filter_mutation_register.bin",
    decodeFilterMutationRequest,
    encodeFilterMutationRequest
  )
  assert.equal(mutation.mutation.kind, "register")
  const poll = await assertRoundTrip(
    "filter_poll_request.bin",
    decodeFilteredPollRequest,
    encodeFilteredPollRequest
  )
  assert.equal(poll.partitionId, 0)
  const byId = await assertRoundTrip(
    "filter_poll_request_group_id.bin",
    decodeFilteredPollRequest,
    encodeFilteredPollRequest
  )
  assert.deepEqual(byId.consumer, { kind: "group_id", id: 3n })
  const automatic = await assertRoundTrip(
    "filter_poll_request_group_auto.bin",
    decodeFilteredPollRequest,
    encodeFilteredPollRequest
  )
  assert.deepEqual(automatic.filter, { kind: "group" })
  assert.equal(automatic.maxExamined, 100)
  assert.deepEqual(automatic.minCatalogPosition, { partitionId: 0, offset: 41n, operationId: 11n })
  const configure = await assertRoundTrip(
    "filter_mutation_configure_group.bin",
    decodeFilterMutationRequest,
    encodeFilterMutationRequest
  )
  assert.equal(configure.mutation.kind, "configure_group")
  assert.equal(configure.mutation.policy.name, "safe-mode")
  const configureJson = decodeFilterMutationRequest(
    parseCanonicalJson(await readText("filter_mutation_configure_group.json")),
    "configure group"
  )
  assert.deepEqual(configureJson, configure)
  await assertRoundTrip(
    "filter_mutation_revision_state.bin",
    decodeFilterMutationRequest,
    encodeFilterMutationRequest
  )
  const changed = decodeFilterMutationRequest(
    parseCanonicalJson(await readText("filter_mutation_revision_state.json")),
    "revision state"
  )
  assert.deepEqual(changed.mutation, {
    kind: "set_revision_enabled",
    filterId: 1,
    revision: 2,
    enabled: false
  })
  const page = await assertRoundTrip("filter_reply_page.bin", decodeFilterReply, encodeFilterReply)
  if (page.kind === "ok" && page.outcome.kind === "page") {
    assert.equal(page.outcome.page.generation.partitionIncarnation, 17n)
    assert.deepEqual(page.outcome.page.ackContext, { ownerGeneration: 4n, metadataOp: 40n })
  } else {
    assert.fail("the page fixture is a page")
  }
  const unfilteredPage = await assertRoundTrip(
    "filter_reply_page_unfiltered.bin",
    decodeFilterReply,
    encodeFilterReply
  )
  if (unfilteredPage.kind === "ok" && unfilteredPage.outcome.kind === "page") {
    assert.equal(unfilteredPage.outcome.page.policy.mode, "unfiltered")
    assert.equal(unfilteredPage.outcome.page.policy.digest, undefined)
    assert.equal(unfilteredPage.outcome.page.matched, unfilteredPage.outcome.page.examined)
  } else {
    assert.fail("the unfiltered fixture is a page")
  }
  const unevaluatedPage = await assertRoundTrip(
    "filter_reply_page_unevaluated.bin",
    decodeFilterReply,
    encodeFilterReply
  )
  if (unevaluatedPage.kind === "ok" && unevaluatedPage.outcome.kind === "page") {
    assert.deepEqual(unevaluatedPage.outcome.page.unevaluated, [
      { offset: 1398n, reason: "malformed" },
      { offset: 1401n, reason: "type_mismatch" }
    ])
  } else {
    assert.fail("the unevaluated fixture is a page")
  }
  const wait = await assertRoundTrip(
    "filter_wait_request.bin",
    decodeFilteredWaitRequest,
    encodeFilteredWaitRequest
  )
  validateFilteredWaitRequest(wait)
  assert.deepEqual(wait.partitions, [
    { partitionId: 0, offset: 7n },
    { partitionId: 1, offset: 0n }
  ])
  assert.equal(wait.maxWaitMs, 5_000)
  assert.equal(wait.waitId, 9n)
  const ready = await assertRoundTrip(
    "filter_reply_ready.bin",
    decodeFilterReply,
    encodeFilterReply
  )
  assert.deepEqual(ready, { kind: "ok", outcome: { kind: "ready", result: { ready: [1] } } })
  const cancel = await assertRoundTrip(
    "filter_wait_cancel.bin",
    decodeFilteredWaitCancel,
    encodeFilteredWaitCancel
  )
  validateFilteredWaitCancel(cancel)
  assert.deepEqual(cancel, { v: 1, waitIds: [9n, 11n] })
  const cancelled = await assertRoundTrip(
    "filter_reply_cancelled.bin",
    decodeFilterReply,
    encodeFilterReply
  )
  assert.deepEqual(cancelled, {
    kind: "ok",
    outcome: { kind: "cancelled", result: { cancelled: [9n] } }
  })
  for (const name of ["filter_reply_revision_disabled.bin", "filter_reply_error.bin"]) {
    await assertRoundTrip(name, decodeFilterReply, encodeFilterReply)
  }
  for (const name of [
    "filter_catalog_reply_bound.bin",
    "filter_catalog_reply_configured.bin",
    "filter_catalog_reply_unbound.bin",
    "filter_catalog_reply_registered.bin",
    "filter_catalog_reply_revision_state.bin",
    "filter_catalog_reply_revisions.bin"
  ]) {
    await assertRoundTrip(name, decodeFilterCatalogReply, encodeFilterCatalogReply)
  }
  const controlBytes = await readFixture("control_filter_catalog.bin")
  const envelope = decodeControlEnvelope(
    expectMap(decodeOne(controlBytes, "control"), "control"),
    "control"
  )
  assert.equal(envelope.command.kind, "filterCatalog")
  assert.deepEqual(
    Buffer.from(encodeNamed(encodeControlEnvelope(envelope))),
    Buffer.from(controlBytes)
  )
})

void test("given_a_catalog_position_with_a_maximum_operation_id_when_round_tripped_then_should_preserve_its_causal_proof", () => {
  const operationId = (1n << 128n) - 1n
  const position = { partitionId: 2, offset: 41n, operationId }
  const encoded = encodeNamed(encodeCatalogPosition(position))
  assert.deepEqual(decodeCatalogPosition(decodeOne(encoded, "position"), "position"), position)
  const json = parseCanonicalJson(
    `{"partition_id":2,"offset":41,"operation_id":"${operationId.toString()}"}`
  )
  assert.deepEqual(decodeCatalogPosition(json, "position"), position)
  const oldPosition = { partitionId: 2, offset: 41n }
  assert.deepEqual(
    decodeCatalogPosition(
      decodeOne(encodeNamed(encodeCatalogPosition(oldPosition)), "position"),
      "position"
    ),
    oldPosition
  )
})

void test("given_the_json_filter_fixture_when_digested_then_should_match_the_rust_digest", async () => {
  const text = await readText("filter_consumer_filter.json")
  const filter = decodeConsumerFilter(parseCanonicalJson(text), "filter")
  const validation = JSON.parse(await readText("filter_validation.json")) as {
    readonly digest: readonly number[]
  }

  assert.equal(consumerFilterJson(filter), JSON.stringify(JSON.parse(text)))
  assert.deepEqual([...ConsumerFilter.digest(filter)], validation.digest)
})

void test("given_a_group_policy_naming_a_revision_when_decoded_then_should_be_refused", () => {
  const revision = parseCanonicalJson(
    `{"v":1,"operation_id":"7","mutation":{"configure_group":{"group":{"stream":"orbit","topic":"fleet","group":"desk"},"policy":{"revision":{"filter_id":4,"revision":2}}}}}`
  )
  assert.throws(() => decodeFilterMutationRequest(revision, "configure group"), CodecError)
})

void test("given_the_json_mutation_fixture_when_decoded_then_should_keep_the_operation_id_exact", async () => {
  const request = decodeFilterMutationRequest(
    parseCanonicalJson(await readText("filter_mutation_register.json")),
    "mutation"
  )
  assert.equal(request.operationId, 1339673755198158349044581307228491536n)
})

interface CorpusCase {
  readonly name: string
  readonly filter: unknown
  readonly payload: string
  readonly headers?: readonly unknown[]
  readonly expected: string
}

void test("given_the_shared_corpus_when_evaluated_then_should_reproduce_every_verdict", async () => {
  const corpus = parseCanonicalJson(await readText("filter_eval_cases.json")) as ReadonlyMap<
    string,
    unknown
  >
  const limitsMap = corpus.get("limits") as ReadonlyMap<string, bigint>
  const limits: DecodeLimits = {
    maxPayloadBytes: Number(limitsMap.get("max_payload_bytes")),
    maxDepth: Number(limitsMap.get("max_depth"))
  }
  const failures: string[] = []
  for (const entry of corpus.get("cases") as readonly ReadonlyMap<string, unknown>[]) {
    const item: CorpusCase = {
      name: entry.get("name") as string,
      filter: entry.get("filter"),
      payload: entry.get("payload") as string,
      headers: (entry.get("headers") as readonly unknown[] | undefined) ?? [],
      expected: entry.get("expected") as string
    }
    const filter = decodeConsumerFilter(item.filter, item.name)
    const compiled = CompiledFilter.compile(filter)
    const record = {
      payload: new TextEncoder().encode(item.payload),
      headers: (item.headers ?? []).map((header, index) => {
        const map = header as ReadonlyMap<string, unknown>
        return {
          key: map.get("key") as string,
          value: decodeHeaderScalar(map.get("value"), `${item.name}.headers[${String(index)}]`)
        }
      })
    }
    const verdict = compiled.evaluate(record, limits)
    if (verdict !== item.expected) {
      failures.push(`${item.name}: expected ${item.expected}, got ${verdict}`)
    }
    assert.equal(compiled.explain(record, limits).verdict, verdict, item.name)
    if (entry.has("fault"))
      assert.equal(
        compiled.evaluateWithFault(record, limits).fault ?? null,
        entry.get("fault"),
        item.name
      )
  }
  assert.deepEqual(failures, [])
})

void test("given_field_paths_when_parsed_then_should_render_the_canonical_text", () => {
  for (const text of ["after.orbit.kind", "[0].ground_stations[12]", "codes.200", "a\\.b"]) {
    assert.equal(FieldPath.parse(text).toString(), text)
  }
  for (const text of ["", "a..b", "a[01]", "a]", "a\\x"]) {
    assert.throws(() => FieldPath.parse(text), InvalidError, text)
  }
})

void test("given_doubles_when_rendered_then_should_match_rust_notation", () => {
  const cases: readonly (readonly [number, string])[] = [
    [1.5, "1.5"],
    [100, "100.0"],
    [0, "0.0"],
    [1e21, "1e21"],
    [1e-7, "1e-7"],
    [0.0001, "0.0001"],
    [123456789012345680000, "1.2345678901234568e20"],
    [-2.25, "-2.25"]
  ]
  for (const [value, expected] of cases) assert.equal(rustDouble(value), expected)
})

void test("given_decimals_when_compared_then_should_ignore_notation", () => {
  const parse = (text: string): ExactDecimal => {
    const decimal = ExactDecimal.parse(text)
    assert.ok(decimal !== undefined, text)
    return decimal
  }
  assert.equal(parse("1.50").compare(parse("1.5")), 0)
  assert.equal(parse("-0").compare(parse("0")), 0)
  assert.equal(parse("12.5").compare(parse("9.99")), 1)
  assert.equal(parse("-12.5").compare(parse("-9.99")), -1)
  assert.equal(ExactDecimal.fromF64(0.1)?.compare(parse("0.1")), 0)
  assert.equal(ExactDecimal.parse("5."), undefined)
})

void test("given_rfc3339_instants_when_parsed_then_should_normalize_to_utc_micros", () => {
  assert.equal(
    timestampFormatMicrosFromText("rfc3339", "2026-09-21T18:04:12.331Z"),
    timestampFormatMicrosFromText("rfc3339", "2026-09-21T20:04:12.331000+02:00")
  )
  assert.equal(timestampFormatMicrosFromText("rfc3339", "1969-12-31T23:59:59.999999Z"), -1n)
  assert.equal(timestampFormatMicrosFromText("rfc3339", "2026-09-21T18:04:12.331000"), undefined)
  assert.equal(timestampFormatMicrosFromText("rfc3339", "2026-02-29T00:00:00Z"), undefined)
})

void test("given_an_invalid_filter_when_validated_then_should_be_rejected", () => {
  const nullOrdering = ConsumerFilter.json(FilterExpr.pred("kind", "lt", null))
  const payloadOnHeaders = ConsumerFilter.headersOnly(FilterExpr.pred("kind", "eq", "x"))
  const emptyAll = ConsumerFilter.json(FilterExpr.all([]))
  for (const filter of [nullOrdering, payloadOnHeaders, emptyAll]) {
    assert.throws(() => {
      validateConsumerFilter(filter)
    }, InvalidError)
  }
})

void test("given_a_header_filter_when_evaluated_then_should_decide_before_the_payload", () => {
  const compiled = CompiledFilter.compile(
    ConsumerFilter.json(
      FilterExpr.all([FilterExpr.header("agdx.ct", "eq", "cdc"), FilterExpr.present("kind")])
    )
  )
  const verdict = compiled.evaluate({
    payload: new TextEncoder().encode("not json"),
    headers: [{ key: "agdx.ct", value: { kind: "string", value: "other" } }]
  })
  assert.equal(verdict, "rejected", "the header decides, the payload is never decoded")
})

void test("given_oversized_sample_headers_when_validated_then_should_reject_before_evaluation", () => {
  const request = {
    v: 1,
    filter: {
      kind: "inline" as const,
      filter: ConsumerFilter.headersOnly(FilterExpr.header("priority", "eq", "critical"))
    },
    payload: new Uint8Array(),
    headers: [{ key: "priority", value: { kind: "raw" as const, value: new Uint8Array(1025) } }]
  }
  assert.throws(() => {
    validateFilterTestRequest(request)
  })
})

void test("given_an_exact_unbind_fixture_when_round_tripped_then_should_preserve_the_identity_condition", async () => {
  const request = await assertRoundTrip(
    "filter_mutation_unbind_exact.bin",
    decodeFilterMutationRequest,
    encodeFilterMutationRequest
  )
  assert.equal(request.mutation.kind, "unbind")
  assert(request.mutation.expectedIdentity !== undefined)
  const json = decodeFilterMutationRequest(
    parseCanonicalJson(await readText("filter_mutation_unbind_exact.json")),
    "exact unbind"
  )
  assert.deepEqual(json, request)
})

void test("given_encoded_codec_fixtures_when_evaluated_then_should_match_verdict_and_fault", async () => {
  const corpus = parseCanonicalJson(await readText("filter_codec_cases.json")) as ReadonlyMap<
    string,
    unknown
  >
  const schemas = (corpus.get("schemas") as readonly ReadonlyMap<string, unknown>[]).map(
    (schema) => {
      const source = schema.get("source") as Map<string, unknown>
      if (source.get("kind") === "protobuf") {
        source.set(
          "descriptor_set",
          Uint8Array.from(source.get("descriptor_set") as readonly bigint[], Number)
        )
      }
      return decodeSchemaDef(
        new Map([...schema, ["id", Number(schema.get("id"))]]),
        "writer schema"
      )
    }
  )
  for (const item of corpus.get("cases") as readonly ReadonlyMap<string, unknown>[]) {
    const name = item.get("name") as string
    const filter = decodeConsumerFilter(item.get("filter"), name)
    const compiled = CompiledFilter.compile(filter, schemas)
    const hex = item.get("payload_hex") as string
    const record = {
      payload: Uint8Array.from(hex.match(/../g) ?? [], (byte) => Number.parseInt(byte, 16)),
      headers: (item.get("headers") as readonly ReadonlyMap<string, unknown>[]).map((header) => ({
        key: header.get("key") as string,
        value: decodeHeaderScalar(header.get("value"), name)
      }))
    }
    const limits = {
      maxDepth: Number(item.get("max_depth")),
      maxPayloadBytes: Number(item.get("max_payload_bytes"))
    }
    const explanation = compiled.explain(record, limits)
    assert.equal(compiled.evaluate(record, limits), item.get("expected"), name)
    assert.equal(explanation.verdict, item.get("expected"), name)
    assert.equal(explanation.fault, item.get("fault"), name)
  }
})

void test("given_server_regex_syntax_when_validated_then_should_not_require_javascript_syntax", () => {
  validateConsumerFilter(ConsumerFilter.json(FilterExpr.text("name", "regex", "\\A\\p{Greek}+\\z")))
  for (const kind of ["glob", "regex"] as const) {
    validateConsumerFilter(
      ConsumerFilter.json(
        FilterExpr.all(Array.from({ length: 4 }, () => FilterExpr.text("name", kind, "safe")))
      )
    )
    assert.throws(() => {
      validateConsumerFilter(
        ConsumerFilter.json(
          FilterExpr.all(Array.from({ length: 5 }, () => FilterExpr.text("name", kind, "safe")))
        )
      )
    }, InvalidError)
  }
})

function waitRequest(): FilteredWaitRequest {
  return {
    v: 1,
    source: { stream: "orbit", topic: "fleet" },
    consumer: { kind: "group", name: "desk" },
    partitions: [
      { partitionId: 0, offset: 12n },
      { partitionId: 1, offset: 0n }
    ],
    maxWaitMs: 5_000,
    waitId: 9n
  }
}

void test("given_wait_requests_out_of_range_when_validated_then_should_be_rejected", () => {
  const tooMany = {
    ...waitRequest(),
    partitions: Array.from({ length: MAX_FILTERED_WAIT_PARTITIONS + 1 }, (_, partitionId) => ({
      partitionId,
      offset: 0n
    }))
  }
  const cases: readonly [FilteredWaitRequest, string][] = [
    [{ ...waitRequest(), partitions: [] }, "a filtered wait names 1..=1024 partitions"],
    [{ ...waitRequest(), maxWaitMs: MAX_FILTERED_WAIT_MS + 1 }, "max_wait_ms must be 0..=20000"],
    [
      {
        ...waitRequest(),
        partitions: [
          { partitionId: 0, offset: 1n },
          { partitionId: 0, offset: 2n }
        ]
      },
      "a filtered wait names each partition once"
    ],
    [tooMany, "a filtered wait names 1..=1024 partitions"],
    [{ ...waitRequest(), waitId: -1n }, "wait_id must be an unsigned 64-bit integer"],
    [{ ...waitRequest(), waitId: 1n << 64n }, "wait_id must be an unsigned 64-bit integer"]
  ]
  for (const [request, message] of cases) {
    assert.throws(() => {
      validateFilteredWaitRequest(request)
    }, new InvalidError(message))
  }
  validateFilteredWaitRequest({ ...waitRequest(), maxWaitMs: 0 })
})

void test("given_wait_cancels_out_of_range_when_validated_then_should_be_rejected", () => {
  const cancel = (waitIds: readonly bigint[]): FilteredWaitCancel => ({ v: 1, waitIds })
  const ids = (count: number) => Array.from({ length: count }, (_, index) => BigInt(index))
  const cases: readonly [FilteredWaitCancel, string][] = [
    [cancel([]), "a filtered wait cancel names 1..=64 waits"],
    [cancel([4n, 4n]), "a filtered wait cancel names each wait once"],
    [cancel(ids(MAX_FILTERED_WAIT_CANCEL_IDS + 1)), "a filtered wait cancel names 1..=64 waits"],
    [cancel([1n << 64n]), "wait_id must be an unsigned 64-bit integer"]
  ]
  for (const [request, message] of cases) {
    assert.throws(() => {
      validateFilteredWaitCancel(request)
    }, new InvalidError(message))
  }
  assert.throws(() => {
    validateFilteredWaitCancel({ ...cancel([1n]), v: 2 })
  }, InvalidError)
  validateFilteredWaitCancel(cancel(ids(MAX_FILTERED_WAIT_CANCEL_IDS)))
})

function configureNamed(name: string): FilterMutationRequest {
  return {
    v: 1,
    operationId: 7n,
    mutation: {
      kind: "configure_group",
      group: { stream: "orbit", topic: "fleet", group: "desk" },
      policy: {
        kind: "definition",
        name,
        filter: ConsumerFilter.json(FilterExpr.pred("table", "eq", "satellites"))
      },
      expectedIdentity: {
        streamId: 1,
        streamCreatedAtMicros: 10n,
        topicId: 2,
        topicCreatedAtMicros: 20n,
        groupId: 3n
      }
    }
  }
}

void test("given_a_group_definition_with_a_bad_name_when_validated_then_should_be_rejected", () => {
  const cases: readonly [string, string][] = [
    ["", "filter name must not be empty"],
    [
      "has space",
      "filter name has a disallowed byte 0x20: allowed are ASCII letters, digits, '-', '_', '.'"
    ],
    [
      "colon:name",
      "filter name has a disallowed byte 0x3a: allowed are ASCII letters, digits, '-', '_', '.'"
    ],
    ["a".repeat(MAX_FILTER_NAME_BYTES + 1), "filter name is 129B, exceeds cap 128B"]
  ]
  for (const [name, message] of cases) {
    assert.throws(() => {
      validateFilterMutationRequest(configureNamed(name))
    }, new InvalidError(message))
    assert.throws(() => {
      validateFilterName(name)
    }, new InvalidError(message))
  }
  validateFilterMutationRequest(configureNamed("a".repeat(MAX_FILTER_NAME_BYTES)))
})

void test("given_a_group_definition_without_a_name_when_decoded_then_should_be_rejected", () => {
  const unnamed =
    '{"v":1,"operation_id":"7","mutation":{"configure_group":{"group":{"stream":"orbit","topic":"fleet","group":"desk"},"policy":{"definition":{"v":1,"evaluator_version":1,"expr":{"present":"table"},"codec":"json","fault_policy":"stop"}}}}}'
  assert.throws(() => decodeFilterMutationRequest(parseCanonicalJson(unnamed), "unnamed"))
})

void test("given_a_summary_with_an_owner_when_encoded_then_should_round_trip_and_omit_an_absent_owner", () => {
  const owner = {
    identity: {
      streamId: 1,
      streamCreatedAtMicros: 10n,
      topicId: 2,
      topicCreatedAtMicros: 20n,
      groupId: 3n
    },
    group: { stream: "orbit", topic: "fleet", group: "desk" }
  }
  const summary = {
    id: 4,
    name: "safe-mode",
    description: "",
    state: "active" as const,
    latestRevision: 1,
    latestDigest: new Uint8Array(32).fill(1),
    codec: "json" as const,
    bindings: 1,
    createdAtMicros: 1n,
    updatedAtMicros: 2n
  }
  for (const item of [{ ...summary, owner }, summary]) {
    const reply: FilterCatalogReply = {
      kind: "ok",
      outcome: {
        kind: "filters",
        page: { items: [item], page: 1, totalPages: 1, totalResults: 1 }
      }
    }
    const encoded = encodeFilterCatalogReply(reply)
    const filters = expectMap(expectMap(encoded.get("ok"), "ok").get("filters"), "filters")
    const [row] = filters.get("items") as readonly Map<string, unknown>[]
    assert.equal(row?.has("owner"), item === summary ? false : true)
    assert.deepEqual(
      decodeFilterCatalogReply(decodeOne(encodeNamed(encoded), "reply"), "reply"),
      reply
    )
  }
})

void test("given_a_list_request_with_an_owner_when_validated_then_should_accept_only_a_native_group_id", () => {
  const owner = {
    streamId: 1,
    streamCreatedAtMicros: 10n,
    topicId: 2,
    topicCreatedAtMicros: 20n,
    groupId: 3n
  }
  const list = { v: 1, owner, page: 1, results: 20 }
  validateListFilters(list)
  assert.deepEqual([...encodeListFilters(list).keys()], ["v", "owner", "page", "results"])
  assert.throws(() => {
    validateListFilters({ ...list, owner: { ...owner, groupId: 0x1_0000_0000n } })
  }, new InvalidError("consumer group id exceeds the native offset-key range"))
})
