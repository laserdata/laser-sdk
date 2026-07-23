import assert from "node:assert/strict"
import { test } from "node:test"
import {
  resultCodeFromCode,
  resultCodeHttpStatus,
  resultCodeToCode
} from "../../src/wire/result.js"

const EXPECTED_CODE_AND_STATUS: readonly (readonly [string, number, number])[] = [
  ["Ok", 0, 200],
  ["Unsupported", 1, 501],
  ["NotFound", 2, 404],
  ["InvalidArgument", 3, 400],
  ["TooLarge", 4, 413],
  ["Conflict", 5, 409],
  ["Stale", 6, 503],
  ["VersionSkew", 7, 400],
  ["Unauthenticated", 8, 401],
  ["Backend", 9, 502],
  ["Forbidden", 10, 403],
  ["StepUpRequired", 11, 403]
]

void test("given_known_result_codes_when_mapped_then_should_match_the_pinned_dictionary", () => {
  for (const [name, code, httpStatus] of EXPECTED_CODE_AND_STATUS) {
    const value = resultCodeFromCode(code)
    assert.deepEqual(value, { kind: "known", name })
    assert.equal(resultCodeToCode(value), code)
    assert.equal(resultCodeHttpStatus(value), httpStatus)
  }
})

void test("given_an_unrecognized_result_code_when_decoded_then_should_pass_through", () => {
  const value = resultCodeFromCode(9999)
  assert.deepEqual(value, { kind: "unrecognized", code: 9999 })
  assert.equal(resultCodeToCode(value), 9999)
  assert.equal(resultCodeHttpStatus(value), 500)
})
