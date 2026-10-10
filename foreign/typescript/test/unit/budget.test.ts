import assert from "node:assert/strict"
import { test } from "node:test"
import { breachErrorBody, breachOf, breachOfInfo, laneBreach } from "../../src/agent/budget.js"
import {
  type AgentEnvelope,
  OPERATION_SESSION,
  type SessionStart,
  encodeSessionStart,
  encodeSessionTransition,
  eventEnvelope,
  parseAgentId,
  statusEnvelope
} from "../../src/wire/agent.js"
import { encodeNamed } from "../../src/wire/cbor.js"
import { ConversationId, RecordId } from "../../src/wire/ids.js"
import type { SessionInfo } from "../../src/wire/session.js"

const AGENT = parseAgentId("worker")
const SESSION = ConversationId.fromU128(7n)

function start(budget: SessionStart["budget"]): AgentEnvelope {
  const record = statusEnvelope(RecordId.fromU128(1n), SESSION, AGENT, OPERATION_SESSION)
  return {
    ...record,
    body: encodeNamed(
      encodeSessionStart({
        agent: AGENT,
        sdk: { language: "typescript", version: "0.7.0" },
        tags: [],
        ...(budget !== undefined ? { budget } : {})
      })
    )
  }
}

function used(input: bigint, output: bigint, costMicros?: bigint): AgentEnvelope {
  return {
    ...eventEnvelope(RecordId.fromU128(2n), SESSION, AGENT, new Uint8Array()),
    usage: {
      inputTokens: input,
      outputTokens: output,
      ...(costMicros !== undefined ? { costMicros } : {})
    }
  }
}

void test("given_no_budget_when_usage_grows_then_should_never_breach", () => {
  assert.equal(breachOf(undefined, 1n << 64n, 1n << 64n), undefined)
  assert.equal(breachOf({}, 10n, 10n), undefined)
})

void test("given_a_token_ceiling_when_usage_reaches_it_then_should_breach_only_past_it", () => {
  assert.equal(breachOf({ tokens: 100n }, 100n, 0n), undefined)
  assert.deepEqual(breachOf({ tokens: 100n }, 101n, 0n), {
    dimension: "tokens",
    ceiling: 100n,
    spent: 101n
  })
})

void test("given_a_cost_ceiling_when_cost_passes_it_then_should_name_the_cost", () => {
  assert.deepEqual(breachOf({ tokens: 1_000n, costMicros: 50n }, 10n, 51n), {
    dimension: "cost_micros",
    ceiling: 50n,
    spent: 51n
  })
})

void test("given_a_breach_when_described_then_should_name_the_budget", () => {
  const breach = breachOf({ tokens: 5n }, 9n, 0n)
  assert.ok(breach !== undefined)
  const body = breachErrorBody(breach)
  assert.ok(body.message?.includes("token budget"))
  const detail = body.detail
  assert.ok(detail !== undefined)
  assert.deepEqual(detail.get("budget"), { kind: "str", value: "tokens" })
  assert.deepEqual(detail.get("ceiling"), { kind: "int", value: 5n })
  assert.deepEqual(detail.get("spent"), { kind: "int", value: 9n })
  assert.equal(body.retryable, false)
  assert.deepEqual(body.code, { kind: "known", name: "Internal" })
})

void test("given_a_lane_when_folded_then_should_take_the_first_start_budget_against_all_usage", () => {
  assert.equal(laneBreach([start({ tokens: 100n }), used(40n, 20n), used(30n, 10n)]), undefined)
  assert.deepEqual(laneBreach([start({ tokens: 100n }), used(40n, 20n), used(30n, 11n)]), {
    dimension: "tokens",
    ceiling: 100n,
    spent: 101n
  })
  // Usage before the start counts, and a later start does not replace the
  // first budget.
  assert.deepEqual(
    laneBreach([used(0n, 0n, 501n), start({ costMicros: 500n }), start({ costMicros: 10_000n })]),
    { dimension: "cost_micros", ceiling: 500n, spent: 501n }
  )
  // A transition is a session status too, but not a start.
  const transition = {
    ...statusEnvelope(RecordId.fromU128(3n), SESSION, AGENT, OPERATION_SESSION),
    body: encodeNamed(encodeSessionTransition({ actor: AGENT }))
  }
  assert.deepEqual(laneBreach([transition, start({ tokens: 1n }), used(1n, 1n)]), {
    dimension: "tokens",
    ceiling: 1n,
    spent: 2n
  })
  assert.equal(laneBreach([start(undefined), used(1n << 40n, 1n << 40n)]), undefined)
})

void test("given_an_index_summary_when_read_then_should_follow_its_flag", () => {
  const info = {
    overBudget: true,
    tokensIn: 70n,
    tokensOut: 50n,
    costMicros: 0n,
    budget: { tokens: 100n }
  } as unknown as SessionInfo
  assert.deepEqual(breachOfInfo(info), { dimension: "tokens", ceiling: 100n, spent: 120n })
  assert.equal(breachOfInfo({ ...info, overBudget: false }), undefined)
  assert.deepEqual(breachOfInfo({ ...info, budget: undefined } as unknown as SessionInfo), {
    ceiling: 0n,
    spent: 120n
  })
})
