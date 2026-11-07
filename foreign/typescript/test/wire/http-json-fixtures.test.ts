import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import {
  decodeCapabilitiesJson,
  decodeErrorBodyJson,
  decodeForkInfoJson,
  decodeKvPageJson,
  decodeProjectionPageJson,
  decodeQueryResultJson,
  decodeSchemaDefJson,
  decodeSchemaListJson,
  encodeCapabilitiesJson,
  encodeErrorBodyJson,
  encodeForkInfoJson,
  encodeKvPageJson,
  encodeProjectionPageJson,
  encodeQueryResultJson,
  encodeSchemaDefJson,
  encodeSchemaListJson,
  authzRolePath,
  forkPath,
  graphPath,
  graphQueryPath,
  kvEntryPath,
  projectionPath,
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
  decodeFilterListQuery,
  filterListQueryOf,
  filterListQueryOwner,
  filterListQueryParams,
  decodeSessionEventsQuery,
  decodeSessionChangesQuery,
  decodeSessionStateQuery,
  decodeSessionLinksQuery,
  sessionEventsQueryParams,
  sessionChangesQueryParams,
  sessionStateQueryParams,
  sessionLinksQueryParams,
  sessionErrorBody,
  PARAM_HISTORY_LIMIT,
  PARAM_PAGE,
  PARAM_RESULTS,
  PARAM_TEXT
} from "../../src/wire/http.js"
import { ConversationId } from "../../src/wire/ids.js"
import { STREAM_RESOURCE_PREFIX, streamResource } from "../../src/wire/authz.js"
import { CodecError, InvalidError } from "../../src/client/errors.js"
import { decodeOne } from "../../src/wire/cbor.js"
import { parseCanonicalJson } from "../../src/wire/filter.js"
import { decodeSessionReply, type SessionPage } from "../../src/wire/session.js"

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

void test("given_names_with_reserved_characters_when_building_role_graph_projection_and_kv_paths_then_should_encode_each_segment", () => {
  assert.equal(authzRolePath("ops/admin team"), "/agdx/authz/roles/ops%2Fadmin%20team")
  assert.equal(graphPath("stream:acme/kg"), "/agdx/graphs/stream%3Aacme%2Fkg")
  assert.equal(graphQueryPath("stream:acme/kg"), "/agdx/graph/stream%3Aacme%2Fkg/query")
  assert.equal(projectionPath("orders/v1"), "/agdx/projections/orders%2Fv1")
  assert.equal(kvEntryPath("stream:acme/state", "a-b_c"), "/agdx/kv/stream%3Aacme%2Fstate/a-b_c")
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
  const query = decodeSessionsQuery(new URLSearchParams("status=active&page=2&results=25"))
  assert.equal(query.status, "active")
  assert.deepEqual([query.page, query.results], [2, 25])
  assert.equal(sessionsQueryParams(query).toString(), "status=active&page=2&results=25")
  assert.equal(
    sessionsQueryParams({ agent: "planner", text: "check", page: 1, results: 0 }).toString(),
    "agent=planner&text=check&page=1&results=0"
  )
  const events = decodeSessionEventsQuery(new URLSearchParams("page=3"))
  assert.equal(events.page, 3)
  assert.equal(events.results, undefined)
  assert.equal(sessionEventsQueryParams({ page: 3, results: 5 }).toString(), "page=3&results=5")
  assert.deepEqual(decodeSessionEventsQuery(new URLSearchParams()), {})
  assert.deepEqual(decodeSessionChangesQuery(new URLSearchParams()), {})
  assert.equal(
    sessionChangesQueryParams({ page: 45, results: 100 }).toString(),
    "page=45&results=100"
  )
  assert.deepEqual(decodeSessionChangesQuery(new URLSearchParams("page=45&results=100")), {
    page: 45,
    results: 100
  })
  const bound = { page: 2, asOf: 4405n, series: 0xffff_ffff_ffff_ffffn }
  const boundParams = sessionsQueryParams(bound)
  assert.equal(boundParams.toString(), "page=2&as_of=4405&series=18446744073709551615")
  assert.deepEqual(decodeSessionsQuery(boundParams), bound)
  assert.deepEqual(decodeSessionEventsQuery(sessionEventsQueryParams(bound)), bound)
  for (const series of ["-1", "x", "18446744073709551616"]) {
    assert.throws(() => decodeSessionsQuery(new URLSearchParams({ series })), CodecError)
  }
  assert.deepEqual(decodeSessionStateQuery(new URLSearchParams("history_limit=7")), {
    historyLimit: 7
  })
  assert.equal(sessionStateQueryParams({ historyLimit: 7 }).toString(), "history_limit=7")
  assert.deepEqual(decodeSessionLinksQuery(new URLSearchParams("surface=memory")), {
    surface: "memory"
  })
  assert.equal(sessionLinksQueryParams({ surface: "memory" }).toString(), "surface=memory")
  assert.throws(() => decodeSessionsQuery(new URLSearchParams("results=-1")), CodecError)
  assert.throws(() => decodeSessionEventsQuery(new URLSearchParams("page=x")), CodecError)
  assert.throws(() => decodeSessionChangesQuery(new URLSearchParams("page=x")), CodecError)
  assert.equal(PARAM_PAGE, "page")
  assert.equal(PARAM_RESULTS, "results")
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

void test("given_the_session_http_error_fixture_when_decoded_then_should_match_the_session_error_mapping", async () => {
  assert.deepEqual(
    decodeErrorBodyJson(await fixture("session_http_error.json")),
    sessionErrorBody({ kind: "notFound", message: "00000000000000000000000004" })
  )
})

void test("given_the_session_http_json_fixtures_when_parsed_exactly_then_should_match_the_binary_replies", async () => {
  for (const [json, binary] of [
    ["session_http_page.json", "session_reply_page.bin"],
    ["session_http_page_rebuilding.json", "session_reply_page_rebuilding.bin"]
  ] as const) {
    const parsed = parseCanonicalJson(await fixture(json))
    assert.ok(parsed instanceof Map, json)
    const page = await binaryPage(binary)
    assert.equal(parsed.get("rebuilding") ?? false, page.rebuilding, json)
    assert.equal(parsed.get("total_results"), BigInt(page.totalResults), json)
    assert.equal(parsed.get("total_pages"), BigInt(page.totalPages), json)
    const items: unknown = parsed.get("items")
    assert.ok(Array.isArray(items), json)
    assert.deepEqual(
      items.map((item: unknown) => (item instanceof Map ? (item.get("id") as unknown) : undefined)),
      page.items.map((item) => item.id.toString()),
      json
    )
  }
  const series = 7_654_321_098_765_432_101n
  const text = await fixture("session_http_changes_start_new.json")
  const start = parseCanonicalJson(text)
  assert.ok(start instanceof Map)
  assert.equal(start.get("series"), series)
  assert.equal(start.get("floor"), 1n)
  assert.equal(start.get("total_results"), 0n)
  const { series: rounded } = JSON.parse(text) as { readonly series: number }
  assert.notEqual(BigInt(rounded), series)
})

void test("given_a_session_changes_reply_without_its_series_when_decoded_then_should_reject", async () => {
  const changes = parseCanonicalJson(await fixture("session_http_changes_legacy.json"))
  assert.throws(
    () => decodeSessionReply(new Map([["Ok", new Map([["Changes", changes]])]]), "session reply"),
    CodecError
  )
})

async function fixture(name: string): Promise<string> {
  return readFile(path.join(FIXTURES_DIR, name), "utf8")
}

async function binaryPage(name: string): Promise<SessionPage> {
  const reply = decodeSessionReply(
    decodeOne(await readFile(path.join(FIXTURES_DIR, name)), name),
    name
  )
  assert.ok(reply.kind === "ok" && reply.outcome.kind === "page", name)
  return reply.outcome.page
}

void test("given_the_http_json_fixtures_when_decoded_then_should_re_encode_byte_identically", async () => {
  const cases = [
    ["browse_projections.json", decodeProjectionPageJson, encodeProjectionPageJson],
    ["browse_schemas.json", decodeSchemaListJson, encodeSchemaListJson],
    ["capabilities.json", decodeCapabilitiesJson, encodeCapabilitiesJson],
    ["error_body.json", decodeErrorBodyJson, encodeErrorBodyJson],
    ["fork_info.json", decodeForkInfoJson, encodeForkInfoJson],
    ["kv_page_view.json", decodeKvPageJson, encodeKvPageJson],
    ["query_result.json", decodeQueryResultJson, encodeQueryResultJson],
    ["schema_def.json", decodeSchemaDefJson, encodeSchemaDefJson],
    ["session_http_error.json", decodeErrorBodyJson, encodeErrorBodyJson]
  ] as const

  for (const [name, decode, encode] of cases) {
    const expected = await fixture(name)
    assert.equal(encode(decode(expected) as never), expected, name)
  }
})

void test("given_http_json_views_when_decoded_then_should_preserve_typed_fields", async () => {
  const capabilities = decodeCapabilitiesJson(await fixture("capabilities.json"))
  assert.equal(capabilities.query.consistency, "read_your_writes")
  assert.equal(capabilities.kv.fencedLeases, false)
  assert.equal(capabilities.versions.query, 1)
  assert.equal(capabilities.versions.control, 1)
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
  assert.equal(page.items[0]?.expiresAtMicros, 1_700_000_000_000_000n)
  assert.deepEqual([page.page, page.totalPages, page.totalResults], [1, 1, 1])

  const error = decodeErrorBodyJson(await fixture("error_body.json"))
  assert.deepEqual(error.code, { kind: "known", name: "Conflict" })
})

void test("given_a_legacy_json_id_array_when_decoded_then_should_reject_it", async () => {
  const legacy = (await fixture("query_result.json")).replace(
    /"execution_id":\s*"[^"]*"/,
    '"execution_id": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]'
  )
  assert.throws(() => decodeQueryResultJson(legacy), /execution_id.*Crockford base32 id/)
})

void test("given_a_filter_list_query_when_the_owner_is_complete_or_partial_then_should_resolve_or_refuse", () => {
  const identity = {
    streamId: 1,
    streamCreatedAtMicros: 10n,
    topicId: 2,
    topicCreatedAtMicros: 20n,
    groupId: 3n
  }
  const query = filterListQueryOf(identity)
  assert.deepEqual(filterListQueryOwner(query), identity)
  assert.equal(filterListQueryOwner({}), undefined)
  const params = filterListQueryParams(query)
  assert.equal(
    params.toString(),
    "owner_stream_id=1&owner_stream_created_at_micros=10&owner_topic_id=2&owner_topic_created_at_micros=20&owner_group_id=3"
  )
  assert.deepEqual(decodeFilterListQuery(params), query)
  const { ownerGroupId: _dropped, ...partial } = query
  assert.throws(
    () => filterListQueryOwner(partial),
    new InvalidError(
      "the owner filter needs owner_stream_id, owner_stream_created_at_micros, owner_topic_id, owner_topic_created_at_micros and owner_group_id together"
    )
  )
})
