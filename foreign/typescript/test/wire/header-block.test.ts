import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import { parseAgentId } from "../../src/wire/agent.js"
import { recordHeadersForEnvelope } from "../../src/wire/agent-record.js"
import { ConversationId, CorrelationId, RecordId } from "../../src/wire/ids.js"
import {
  type HeaderField,
  type RecordHeaders,
  encodeRecordHeaders
} from "../../src/wire/headers.js"

const FIXTURES = path.resolve(process.cwd(), "../../wire/fixtures")

interface Row {
  readonly key: string
  readonly kind: HeaderField["kind"]
  readonly value: string | number
}

async function assertBlock(name: string, headers: RecordHeaders): Promise<void> {
  const golden = JSON.parse(await readFile(path.join(FIXTURES, name), "utf8")) as readonly Row[]
  const encoded = encodeRecordHeaders(headers).map(([key, field]) => ({
    key,
    kind: field.kind,
    value: field.kind === "uint64" ? field.value.toString() : field.value
  }))
  assert.deepEqual(encoded, golden, name)
}

void test("given_each_record_family_when_headers_are_encoded_then_should_match_the_shared_block", async () => {
  const planner = parseAgentId("planner")
  await assertBlock(
    "header_block_envelope.json",
    recordHeadersForEnvelope(
      {
        kind: "command",
        record: RecordId.fromU128(1n),
        conversation: ConversationId.fromU128(2n),
        parent: ConversationId.fromU128(4n),
        root: ConversationId.fromU128(5n),
        source: planner,
        correlation: CorrelationId.fromU128(3n),
        last: false,
        mustUnderstand: 0n,
        body: new TextEncoder().encode("{}")
      },
      "json",
      true
    )
  )
  await assertBlock("header_block_generic.json", {
    conversation: ConversationId.fromU128(2n).toString(),
    parent: ConversationId.fromU128(4n).toString(),
    root: ConversationId.fromU128(5n).toString(),
    agent: "planner",
    addressee: { kind: "agent", agent: "critic" },
    causalParent: "1:42",
    idempotencyKey: "job-1",
    correlation: "corr-1",
    fence: 7n,
    deadlineMicros: 1_717_171_717_000_000n,
    inputTokens: 100n,
    outputTokens: 50n,
    costUsd: 0.25
  })
})
