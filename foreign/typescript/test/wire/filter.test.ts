import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import { InvalidError } from "../../src/client/errors.js"
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
  decodeFilteredPollRequest,
  decodeHeaderScalar,
  encodeConsumerFilter,
  encodeCatalogPosition,
  encodeFilterCatalogReply,
  encodeFilterMutationRequest,
  encodeFilterReply,
  encodeFilteredAck,
  encodeFilteredPollRequest,
  parseCanonicalJson,
  rustDouble,
  timestampFormatMicrosFromText,
  validateConsumerFilter,
  validateFilterTestRequest
} from "../../src/wire/filter.js"

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
  assert.equal(poll.readMode, "primary")
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
  const ack = await assertRoundTrip("filter_ack.bin", decodeFilteredAck, encodeFilteredAck)
  assert.equal(ack.mode, "filtered")
  assert.equal(ack.policyGeneration, 1n)
  const unfilteredAck = await assertRoundTrip(
    "filter_ack_unfiltered.bin",
    decodeFilteredAck,
    encodeFilteredAck
  )
  assert.equal(unfilteredAck.mode, "unfiltered")
  assert.equal(unfilteredAck.digest, undefined)
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
  for (const name of [
    "filter_reply_page.bin",
    "filter_reply_acknowledged.bin",
    "filter_reply_revision_disabled.bin",
    "filter_reply_error.bin"
  ]) {
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
