import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import {
  decodeCapabilitiesJson,
  decodeErrorBodyJson,
  decodeForkInfoJson,
  decodeKvPageJson,
  decodeProjectionListJson,
  decodeQueryResultJson,
  decodeSchemaDefJson,
  decodeSchemaListJson,
  encodeCapabilitiesJson,
  encodeErrorBodyJson,
  encodeForkInfoJson,
  encodeKvPageJson,
  encodeProjectionListJson,
  encodeQueryResultJson,
  encodeSchemaDefJson,
  encodeSchemaListJson
} from "../../src/wire/http.js"

const FIXTURES_DIR = path.resolve(process.cwd(), "../../wire/fixtures")

async function fixture(name: string): Promise<string> {
  return readFile(path.join(FIXTURES_DIR, name), "utf8")
}

void test("given_the_http_json_fixtures_when_decoded_then_should_re_encode_byte_identically", async () => {
  const cases = [
    ["browse_projections.json", decodeProjectionListJson, encodeProjectionListJson],
    ["browse_schemas.json", decodeSchemaListJson, encodeSchemaListJson],
    ["capabilities.json", decodeCapabilitiesJson, encodeCapabilitiesJson],
    ["error_body.json", decodeErrorBodyJson, encodeErrorBodyJson],
    ["fork_info.json", decodeForkInfoJson, encodeForkInfoJson],
    ["kv_page_view.json", decodeKvPageJson, encodeKvPageJson],
    ["query_result.json", decodeQueryResultJson, encodeQueryResultJson],
    ["schema_def.json", decodeSchemaDefJson, encodeSchemaDefJson]
  ] as const

  for (const [name, decode, encode] of cases) {
    const expected = await fixture(name)
    assert.equal(encode(decode(expected) as never), expected, name)
  }
})

void test("given_http_json_views_when_decoded_then_should_preserve_typed_fields", async () => {
  const capabilities = decodeCapabilitiesJson(await fixture("capabilities.json"))
  assert.equal(capabilities.query.consistency, "readYourWrites")
  assert.equal(capabilities.kv.cas, true)
  assert.equal(capabilities.backends[1]?.label, "Analytics warehouse")

  const query = decodeQueryResultJson(await fixture("query_result.json"))
  const [row] = query.rows
  assert.ok(row !== undefined)
  assert.equal(row.offset, 17n)
  assert.deepEqual(row.payload, new TextEncoder().encode('{"total":42}'))

  const page = decodeKvPageJson(await fixture("kv_page_view.json"))
  assert.equal(page.entries[0]?.expiresAtMicros, 1_700_000_000_000_000n)

  const error = decodeErrorBodyJson(await fixture("error_body.json"))
  assert.deepEqual(error.code, { kind: "known", name: "Conflict" })
})
