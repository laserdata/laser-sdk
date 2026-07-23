import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import {
  agentRunStateFromWord,
  agentRunStateIsTerminal,
  decodeAgentList,
  decodeAgentReply,
  decodeAgentSubmit,
  encodeAgentList,
  encodeAgentReply,
  encodeAgentSubmit
} from "../../src/wire/agent-workflow.js"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"

const FIXTURES_DIR = path.resolve(process.cwd(), "../../wire/fixtures")

async function readFixture(name: string): Promise<Uint8Array> {
  const buffer = await readFile(path.join(FIXTURES_DIR, name))
  return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength)
}

void test("given_the_agent_submit_fixture_when_decoded_then_should_re_encode_byte_identically", async () => {
  const bytes = await readFixture("agent_submit.bin")
  const map = expectMap(decodeOne(bytes, "agent_submit"), "agent_submit")
  const submit = decodeAgentSubmit(map, "agent_submit")
  assert.equal(submit.agentId, "diagnoser")
  assert.equal(submit.runId, "run-7")
  assert.deepEqual(submit.params.get("priority"), "high")
  assert.equal(new TextDecoder().decode(submit.input), '{"incident":"INC-7"}')
  const reencoded = encodeNamed(encodeAgentSubmit(submit))
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
})

void test("given_the_agent_list_page_fixture_when_decoded_then_should_re_encode_byte_identically", async () => {
  const bytes = await readFixture("agent_list_page.bin")
  const map = expectMap(decodeOne(bytes, "agent_list_page"), "agent_list_page")
  const list = decodeAgentList(map, "agent_list_page")
  assert.equal(list.agentId, "diagnoser")
  assert.equal(list.state, "running")
  assert.equal(list.limit, 25)
  assert.deepEqual(list.cursor, new Uint8Array([0x0a, 0x0b]))
  const reencoded = encodeNamed(encodeAgentList(list))
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
})

async function assertReplyRoundTrips(name: string) {
  const bytes = await readFixture(name)
  const decoded = decodeOne(bytes, name)
  const reply = decodeAgentReply(decoded, name)
  const reencoded = encodeNamed(encodeAgentReply(reply))
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
  return reply
}

void test("given_the_agent_reply_status_fixture_when_decoded_then_should_carry_the_run", async () => {
  const reply = await assertReplyRoundTrips("agent_reply_status.bin")
  if (reply.kind !== "ok" || reply.outcome.kind !== "status") throw new Error("wrong shape")
  assert.equal(reply.outcome.run.runId, "run-7")
  assert.equal(reply.outcome.run.state, "running")
  assert.equal(agentRunStateIsTerminal(reply.outcome.run.state), false)
})

void test("given_the_agent_reply_list_page_fixture_when_decoded_then_should_carry_the_page", async () => {
  const reply = await assertReplyRoundTrips("agent_reply_list_page.bin")
  if (reply.kind !== "ok" || reply.outcome.kind !== "list") throw new Error("wrong shape")
  assert.equal(reply.outcome.page.runs.length, 1)
  const [run] = reply.outcome.page.runs
  assert.ok(run !== undefined)
  assert.equal(run.detail, "budget exhausted")
  assert.equal(agentRunStateIsTerminal(run.state), true)
  assert.deepEqual(reply.outcome.page.cursor, new Uint8Array([0x0c, 0x0d]))
})

void test("given_the_agent_reply_error_fixture_when_decoded_then_should_carry_the_version_mismatch", async () => {
  const reply = await assertReplyRoundTrips("agent_reply_error.bin")
  assert.deepEqual(reply, { kind: "err", error: { kind: "version", expected: 1, got: 99 } })
})

void test("given_an_unrecognized_run_state_word_when_decoded_then_should_throw_rather_than_pass_through", () => {
  assert.throws(() => {
    agentRunStateFromWord("paused", "test")
  }, /not a recognized agent run state/)
})
