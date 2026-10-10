import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import {
  decodeAcceptedOperationJson,
  decodeCapabilitiesJson,
  decodeDestinationIssueJson,
  decodeDestinationPageJson,
  decodeErrorBodyJson,
  decodeForkInfoJson,
  decodeKvPageJson,
  decodeProjectionListJson,
  decodeQueryExecutionJson,
  decodeQueryRoutePageJson,
  decodeQueryResultJson,
  decodeSchemaDefJson,
  decodeSchemaListJson,
  decodeSnapshotPageJson,
  decodeTableFilePageJson,
  decodeTableMetricsJson,
  decodeTableSchemaJson,
  decodeTableViewJson,
  encodeAcceptedOperationJson,
  encodeCapabilitiesJson,
  encodeDestinationIssueJson,
  encodeDestinationPageJson,
  encodeErrorBodyJson,
  encodeForkInfoJson,
  encodeKvPageJson,
  encodeProjectionListJson,
  encodeQueryExecutionJson,
  encodeQueryRoutePageJson,
  encodeQueryResultJson,
  encodeSchemaDefJson,
  encodeSchemaListJson,
  encodeSnapshotPageJson,
  encodeTableFilePageJson,
  encodeTableMetricsJson,
  encodeTableSchemaJson,
  encodeTableViewJson,
  forkPath,
  forkPromotePath,
  forkRowsPath,
  sessionsPath,
  sessionPath,
  sessionChangesPath,
  sessionEventsPath,
  sessionStatePath,
  sessionLinksPath,
  sessionSourcesPath,
  pathSegment,
  sessionsQueryParams,
  decodeSessionsQuery,
  decodeSessionEventsQuery,
  decodeSessionChangesQuery,
  decodeSessionStateQuery,
  decodeSessionLinksQuery,
  sessionEventsQueryParams,
  sessionChangesQueryParams,
  sessionStateQueryParams,
  sessionLinksQueryParams,
  sessionErrorBody,
  PARAM_FIXED_FRONTIER,
  PARAM_HISTORY_LIMIT,
  PARAM_TEXT
} from "../../src/wire/http.js"
import { ConversationId } from "../../src/wire/ids.js"
import { STREAM_RESOURCE_PREFIX, streamResource } from "../../src/wire/authz.js"
import { CodecError } from "../../src/client/errors.js"

const FIXTURES_DIR = path.resolve(process.cwd(), "../../wire/fixtures")

void test("given_a_stream_scoped_session_when_paths_are_built_then_should_encode_each_segment", () => {
  const stream = "a/b?!"
  const id = ConversationId.fromU128(3n)
  const base = "/agdx/sessions/a%2Fb%3F%21/00000000000000000000000003"
  assert.equal(pathSegment(stream), "a%2Fb%3F%21")
  assert.equal(sessionsPath(stream), "/agdx/sessions/a%2Fb%3F%21")
  assert.equal(sessionPath(stream, id), base)
  assert.equal(sessionChangesPath(stream), "/agdx/sessions/a%2Fb%3F%21/changes")
  assert.equal(sessionEventsPath(stream, id), `${base}/events`)
  assert.equal(sessionStatePath(stream, id), `${base}/state`)
  assert.equal(sessionLinksPath(stream, id), `${base}/links`)
  assert.equal(sessionSourcesPath(stream, id), `${base}/sources`)
})

void test("given_a_stream_scoped_fork_id_when_paths_are_built_then_should_encode_it_as_one_segment", () => {
  assert.equal(forkPath("stream:acme/try"), "/agdx/forks/stream%3Aacme%2Ftry")
  assert.equal(forkPromotePath("stream:acme/try"), "/agdx/forks/stream%3Aacme%2Ftry/promote")
  assert.equal(forkRowsPath("stream:acme/try"), "/agdx/forks/stream%3Aacme%2Ftry/rows")
  assert.equal(forkPath("try-1"), "/agdx/forks/try-1")
})

void test("given_session_queries_when_decoded_then_should_take_the_stream_from_the_path", () => {
  assert.equal(sessionsPath("agents"), "/agdx/sessions/agents")
  const scoped = decodeSessionsQuery(new URLSearchParams("status=active&root=r1&label_prefix=t-"))
  assert.equal(scoped.root, "r1")
  assert.equal(scoped.labelPrefix, "t-")
  assert.equal(sessionsQueryParams(scoped).toString(), "status=active&root=r1&label_prefix=t-")
  const query = decodeSessionsQuery(new URLSearchParams("status=active&total=true"))
  assert.equal(query.status, "active")
  assert.equal(query.wantTotal, true)
  assert.equal(sessionsQueryParams(query).toString(), "status=active&total=true")
  assert.equal(
    sessionsQueryParams({ agent: "planner", text: "check", cursor: "c1", limit: 0 }).toString(),
    "agent=planner&text=check&cursor=c1&limit=0"
  )
  const events = decodeSessionEventsQuery(new URLSearchParams("fixed_frontier=true"))
  assert.equal(events.fixedFrontier, true)
  assert.equal(events.limit, undefined)
  assert.equal(
    sessionEventsQueryParams({ cursor: "c", limit: 5, fixedFrontier: true }).toString(),
    "cursor=c&limit=5&fixed_frontier=true"
  )
  assert.deepEqual(decodeSessionEventsQuery(new URLSearchParams()), { fixedFrontier: false })
  const changes = decodeSessionChangesQuery(new URLSearchParams())
  assert.equal(changes.after, 0n)
  assert.equal(sessionChangesQueryParams({ after: 42n, limit: 10 }).toString(), "after=42&limit=10")
  assert.deepEqual(decodeSessionChangesQuery(new URLSearchParams("after=42&limit=10")), {
    after: 42n,
    limit: 10
  })
  assert.deepEqual(decodeSessionStateQuery(new URLSearchParams("history_limit=7")), {
    historyLimit: 7
  })
  assert.equal(sessionStateQueryParams({ historyLimit: 7 }).toString(), "history_limit=7")
  assert.deepEqual(decodeSessionLinksQuery(new URLSearchParams("surface=memory")), {
    surface: "memory"
  })
  assert.equal(sessionLinksQueryParams({ surface: "memory" }).toString(), "surface=memory")
  assert.throws(() => decodeSessionsQuery(new URLSearchParams("limit=-1")), CodecError)
  assert.throws(
    () => decodeSessionEventsQuery(new URLSearchParams("fixed_frontier=yes")),
    CodecError
  )
  assert.throws(() => decodeSessionChangesQuery(new URLSearchParams("after=x")), CodecError)
  assert.equal(PARAM_FIXED_FRONTIER, "fixed_frontier")
  assert.equal(PARAM_HISTORY_LIMIT, "history_limit")
  assert.equal(PARAM_TEXT, "text")
  assert.equal(streamResource("agents"), "stream:agents")
  assert.equal(STREAM_RESOURCE_PREFIX, "stream:")
})

void test("given_a_session_error_when_rendered_as_an_error_body_then_should_carry_its_result_code", () => {
  assert.deepEqual(sessionErrorBody({ kind: "notRegistered", message: "agents" }), {
    code: { kind: "known", name: "NotFound" },
    message: "stream is not registered for sessions: agents"
  })
  assert.deepEqual(sessionErrorBody({ kind: "stale", message: "topic 3" }).code, {
    kind: "known",
    name: "StaleGeneration"
  })
  assert.deepEqual(sessionErrorBody({ kind: "unauthorized", message: "x" }).code, {
    kind: "known",
    name: "Forbidden"
  })
  assert.deepEqual(sessionErrorBody({ kind: "unrecognized", tag: "Later", value: null }).code, {
    kind: "known",
    name: "Backend"
  })
})

async function fixture(name: string): Promise<string> {
  return readFile(path.join(FIXTURES_DIR, name), "utf8")
}

void test("given_the_http_json_fixtures_when_decoded_then_should_re_encode_byte_identically", async () => {
  const cases = [
    ["accepted_operation.json", decodeAcceptedOperationJson, encodeAcceptedOperationJson],
    ["browse_projections.json", decodeProjectionListJson, encodeProjectionListJson],
    ["browse_schemas.json", decodeSchemaListJson, encodeSchemaListJson],
    ["capabilities.json", decodeCapabilitiesJson, encodeCapabilitiesJson],
    ["destination_issue.json", decodeDestinationIssueJson, encodeDestinationIssueJson],
    ["destination_page.json", decodeDestinationPageJson, encodeDestinationPageJson],
    ["error_body.json", decodeErrorBodyJson, encodeErrorBodyJson],
    ["fork_info.json", decodeForkInfoJson, encodeForkInfoJson],
    ["kv_page_view.json", decodeKvPageJson, encodeKvPageJson],
    ["query_execution.json", decodeQueryExecutionJson, encodeQueryExecutionJson],
    ["query_route_page.json", decodeQueryRoutePageJson, encodeQueryRoutePageJson],
    ["query_result.json", decodeQueryResultJson, encodeQueryResultJson],
    ["schema_def.json", decodeSchemaDefJson, encodeSchemaDefJson],
    ["snapshot_page.json", decodeSnapshotPageJson, encodeSnapshotPageJson],
    ["table_file_page.json", decodeTableFilePageJson, encodeTableFilePageJson],
    ["table_metrics.json", decodeTableMetricsJson, encodeTableMetricsJson],
    ["table_schema_view.json", decodeTableSchemaJson, encodeTableSchemaJson],
    ["table_view.json", decodeTableViewJson, encodeTableViewJson]
  ] as const

  for (const [name, decode, encode] of cases) {
    const expected = await fixture(name)
    assert.equal(encode(decode(expected) as never), expected, name)
  }
})

void test("given_http_json_views_when_decoded_then_should_preserve_typed_fields", async () => {
  const capabilities = decodeCapabilitiesJson(await fixture("capabilities.json"))
  assert.equal(capabilities.query.consistency, "read_your_writes")
  assert.equal(capabilities.query.cursorPaging, true)
  assert.equal(capabilities.query.cancellation, true)
  assert.equal(capabilities.query.executionStatus, true)
  assert.equal(capabilities.kv.fencedLeases, false)
  assert.equal(capabilities.destinations.available, true)
  assert.equal(capabilities.destinations.tableSchema, true)
  assert.equal(capabilities.destinations.strongestConsistency, "linearizable")
  assert.equal(capabilities.versions.query, 1)
  assert.equal(capabilities.versions.control, 1)
  assert.equal(capabilities.versions.checkpoint, 1)
  assert.equal(capabilities.kv.cas, true)
  assert.equal(capabilities.backends[1]?.label, "Analytics warehouse")

  const query = decodeQueryResultJson(await fixture("query_result.json"))
  const [row] = query.rows
  assert.ok(row !== undefined)
  assert.deepEqual(row.values, [
    { kind: "long", value: 42n },
    { kind: "string", value: "node-7" }
  ])

  const page = decodeKvPageJson(await fixture("kv_page_view.json"))
  assert.equal(page.entries[0]?.expiresAtMicros, 1_700_000_000_000_000n)

  const error = decodeErrorBodyJson(await fixture("error_body.json"))
  assert.deepEqual(error.code, { kind: "known", name: "Conflict" })

  const destinations = decodeDestinationPageJson(await fixture("destination_page.json"))
  assert.equal(destinations.destinations[0]?.destination.name, "readings-lakehouse")
  assert.equal(destinations.consistency, "linearizable")

  const files = decodeTableFilePageJson(await fixture("table_file_page.json"))
  assert.deepEqual(files.files[0]?.partition.get("day"), { kind: "date", value: 20_000 })
})

void test("given_a_legacy_json_id_array_when_decoded_then_should_reject_it", () => {
  assert.throws(
    () =>
      decodeAcceptedOperationJson(`{
        "operation_id": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 88],
        "request_id": "000000000000000000000000CG",
        "state": "succeeded",
        "submitted_at_micros": 1717171717000000
      }`),
    /operation_id.*Crockford base32 id/
  )
})
