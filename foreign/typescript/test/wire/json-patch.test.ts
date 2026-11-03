import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import { applyJsonPatch } from "../../src/wire/json-patch.js"

interface PatchCase {
  readonly name: string
  readonly document: unknown
  readonly patch: unknown
  readonly result?: unknown
  readonly error?: boolean
}

void test("given_shared_json_patch_cases_when_applied_then_should_match_rust", async () => {
  const file = path.resolve(process.cwd(), "../../wire/fixtures/json_patch/cases.json")
  const cases = JSON.parse(await readFile(file, "utf8")) as PatchCase[]
  for (const item of cases) {
    if (item.error === true) {
      assert.throws(() => applyJsonPatch(item.document, item.patch), item.name)
    } else {
      assert.deepEqual(applyJsonPatch(item.document, item.patch), item.result, item.name)
    }
  }
})

void test("given_state_patch_limits_when_applied_then_should_reject", () => {
  assert.throws(() => applyJsonPatch({ a: 1 }, Array(257).fill({ op: "remove", path: "/a" })))
  assert.throws(() => applyJsonPatch({ large: "x".repeat(8 * 1024 * 1024) }, []))
})

void test("given_inexact_state_integer_when_applied_then_should_reject", () => {
  assert.throws(() => applyJsonPatch({ large: 9_007_199_254_740_992 }, []))
  assert.throws(() =>
    applyJsonPatch({}, [{ op: "add", path: "/large", value: 9_007_199_254_740_992 }])
  )
})
