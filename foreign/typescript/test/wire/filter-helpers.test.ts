import assert from "node:assert/strict"
import { test } from "node:test"
import { InvalidError } from "../../src/client/errors.js"
import { FILTER_OP_VERSION } from "../../src/wire/codes.js"
import { CompiledFilter } from "../../src/wire/filter-eval.js"
import {
  ConsumerFilter,
  ExactDecimal,
  FilterExpr,
  type FilteredPage,
  appliedPolicyFiltered,
  appliedPolicyUnfiltered,
  executionModeIsFiltered,
  faultReasonIsForeign,
  filterConsumerIsGroup,
  filterErrorCheckVersion,
  filterErrorInvalid,
  filterExprCaseInsensitive,
  filteredPageNextStart,
  readModeIsPrimary,
  recordPolicyIsReject,
  textPredicateValidate,
  timestampFormatMicrosFromInteger,
  timestampFormatMicrosFromText
} from "../../src/wire/filter.js"

const GENERATION = {
  streamId: 1,
  streamCreatedAtMicros: 2n,
  topicId: 3,
  topicCreatedAtMicros: 4n,
  partitionId: 0,
  partitionCreatedRevision: 5n,
  purgeGeneration: 6n
}

function page(nextScanOffset?: bigint): FilteredPage {
  return {
    v: FILTER_OP_VERSION,
    partitionId: 0,
    policy: appliedPolicyUnfiltered(7n, 9n),
    generation: GENERATION,
    readMode: "local",
    ...(nextScanOffset === undefined ? {} : { nextScanOffset }),
    frontier: 100n,
    examined: 1,
    matched: 1,
    stop: "filled",
    unevaluated: [],
    records: new Uint8Array()
  }
}

void test("given_word_enums_when_classified_then_should_match_the_rust_predicates", () => {
  assert.equal(filterConsumerIsGroup({ kind: "group", name: "workers" }), true)
  assert.equal(filterConsumerIsGroup({ kind: "group_id", id: 1n }), true)
  assert.equal(filterConsumerIsGroup({ kind: "consumer", name: "solo" }), false)
  assert.equal(executionModeIsFiltered("filtered"), true)
  assert.equal(executionModeIsFiltered("unfiltered"), false)
  assert.equal(readModeIsPrimary("primary"), true)
  assert.equal(readModeIsPrimary("local"), false)
  assert.equal(recordPolicyIsReject("reject"), true)
  assert.equal(recordPolicyIsReject("pass"), false)
  for (const reason of [
    "foreign_codec",
    "missing_schema",
    "schema_not_allowed",
    "schema_mismatch"
  ] as const) {
    assert.equal(faultReasonIsForeign(reason), true, reason)
  }
  for (const reason of ["malformed", "too_large", "too_deep", "type_mismatch"] as const) {
    assert.equal(faultReasonIsForeign(reason), false, reason)
  }
})

void test("given_applied_policies_when_built_then_should_name_the_digest_only_when_filtered", () => {
  const digest = new Uint8Array(32).fill(1)
  assert.deepEqual(appliedPolicyFiltered(undefined, digest), {
    digest,
    mode: "filtered",
    policyGeneration: 0n
  })
  assert.deepEqual(appliedPolicyFiltered(4n, digest).groupId, 4n)
  assert.deepEqual(appliedPolicyUnfiltered(7n, 9n), {
    groupId: 7n,
    mode: "unfiltered",
    policyGeneration: 9n
  })
})

void test("given_a_page_when_continuing_then_should_resume_after_it_or_repeat_the_original", () => {
  const original = { kind: "first" } as const
  assert.deepEqual(filteredPageNextStart(page(), original), original)
  assert.deepEqual(filteredPageNextStart(page(42n), original), {
    kind: "continue",
    continuation: {
      nextScanOffset: 42n,
      generation: GENERATION,
      readMode: "local",
      mode: "unfiltered",
      policyGeneration: 9n,
      groupId: 7n
    }
  })
})

void test("given_filter_errors_when_built_then_should_carry_the_rust_reason_and_code", () => {
  assert.deepEqual(filterErrorInvalid(new InvalidError("bad path")), {
    code: { kind: "known", name: "InvalidArgument" },
    reason: "invalid_request",
    message: "bad path"
  })
  assert.equal(filterErrorCheckVersion(FILTER_OP_VERSION), undefined)
  const skew = filterErrorCheckVersion(FILTER_OP_VERSION + 1)
  assert.ok(skew !== undefined)
  assert.equal(skew.reason, "version_skew")
  assert.deepEqual(skew.code, { kind: "known", name: "VersionSkew" })
})

void test("given_paths_when_tried_then_should_build_presence_or_return_undefined", () => {
  assert.deepEqual(FilterExpr.tryPresent("reading.id"), FilterExpr.present("reading.id"))
  assert.deepEqual(FilterExpr.tryAbsent("reading.id"), FilterExpr.absent("reading.id"))
  assert.equal(FilterExpr.tryPresent(""), undefined)
  assert.equal(FilterExpr.tryAbsent(""), undefined)
})

void test("given_text_and_other_nodes_when_made_case_insensitive_then_should_change_only_text_matches", () => {
  assert.deepEqual(
    filterExprCaseInsensitive(FilterExpr.text("name", "prefix", "Ab")),
    FilterExpr.text("name", "prefix", "Ab", true)
  )
  assert.deepEqual(
    filterExprCaseInsensitive(FilterExpr.headerText("tenant", "equals", "Ab")),
    FilterExpr.headerText("tenant", "equals", "Ab", true)
  )
  const pred = FilterExpr.pred("amount", "gt", 3)
  assert.equal(filterExprCaseInsensitive(pred), pred)
})

void test("given_text_predicates_when_validated_then_should_refuse_a_dangling_glob_escape", () => {
  textPredicateValidate({ field: "name", kind: "glob", pattern: "a*" })
  assert.throws(() => {
    textPredicateValidate({ field: "name", kind: "glob", pattern: "a\\" })
  }, InvalidError)
})

void test("given_timestamp_formats_when_scaling_integers_then_should_match_the_rust_units", () => {
  assert.equal(timestampFormatMicrosFromInteger("epoch_seconds", 2n), 2_000_000n)
  assert.equal(timestampFormatMicrosFromInteger("epoch_millis", 2n), 2_000n)
  assert.equal(timestampFormatMicrosFromInteger("epoch_micros", 2n), 2n)
  assert.equal(timestampFormatMicrosFromInteger("rfc3339", 2n), undefined)
  assert.equal(timestampFormatMicrosFromText("epoch_seconds", "3"), 3_000_000n)
  assert.equal(ExactDecimal.fromF64(Number.NaN), undefined)
})

void test("given_compiled_filters_when_asked_then_should_report_header_need_and_record_policy", () => {
  const payload = CompiledFilter.compile(ConsumerFilter.json(FilterExpr.pred("amount", "gt", 3)))
  assert.equal(payload.headerNeed, "content_type")
  const headers = CompiledFilter.compile(
    ConsumerFilter.headersOnly(FilterExpr.header("tenant", "eq", "acme"))
  )
  assert.equal(headers.headerNeed, "all")
  assert.equal(payload.recordPolicy("foreign_codec"), "reject")
  assert.equal(payload.recordPolicy("type_mismatch"), "reject")
  assert.equal(payload.recordPolicy("malformed"), undefined)
  const outcome = payload.evaluateWithFault({
    payload: new TextEncoder().encode('{"amount":5}'),
    headers: []
  })
  assert.deepEqual(outcome, { verdict: "selected" })
})
