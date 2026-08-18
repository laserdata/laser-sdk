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
  encodeTableViewJson
} from "../../src/wire/http.js"

const FIXTURES_DIR = path.resolve(process.cwd(), "../../wire/fixtures")

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
    { kind: "string", value: "alice" }
  ])

  const page = decodeKvPageJson(await fixture("kv_page_view.json"))
  assert.equal(page.entries[0]?.expiresAtMicros, 1_700_000_000_000_000n)

  const error = decodeErrorBodyJson(await fixture("error_body.json"))
  assert.deepEqual(error.code, { kind: "known", name: "Conflict" })

  const destinations = decodeDestinationPageJson(await fixture("destination_page.json"))
  assert.equal(destinations.destinations[0]?.destination.name, "orders-lakehouse")
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
