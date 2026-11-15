import assert from "node:assert/strict"
import { test } from "node:test"
import { InvalidError } from "../../src/client/errors.js"
import type { MetadataChangeGroup, OptionsScope } from "../../src/client/server.js"
import { metadataChangesRequest, optionsScopeCode } from "../../src/iggy/server-admin.js"

void test("given_a_malformed_start_when_a_metadata_change_request_is_built_then_should_refuse_it", () => {
  assert.throws(
    () => metadataChangesRequest({ start: { mode: "after", cursor: "mc1_nope" } }),
    InvalidError
  )
  assert.throws(
    () => metadataChangesRequest({ start: { mode: "current", pageToken: "cs1_nope" } }),
    InvalidError
  )
})

void test("given_a_query_breaking_the_server_rules_when_a_request_is_built_then_should_refuse_it", () => {
  assert.throws(() => metadataChangesRequest({ limit: 0 }), InvalidError)
  assert.throws(
    () => metadataChangesRequest({ start: { mode: "latest" }, sinceMicros: 1n }),
    InvalidError
  )
  assert.throws(
    () => metadataChangesRequest({ start: { mode: "current" }, groups: ["topics"] }),
    InvalidError
  )
  assert.ok(metadataChangesRequest({ groups: ["streams"], sinceMicros: 5n }).length > 0)
})

void test("given_an_unknown_group_or_scope_when_a_request_is_built_then_should_refuse_it", () => {
  assert.throws(
    () => metadataChangesRequest({ groups: ["nodes" as MetadataChangeGroup] }),
    InvalidError
  )
  assert.throws(() => optionsScopeCode("cluster" as OptionsScope), InvalidError)
})
