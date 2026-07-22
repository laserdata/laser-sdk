import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import {
  actionIndex,
  decodeAuthzHistoryReq,
  decodeAuthzReply,
  decodeBindRolesReq,
  decodeRole,
  delegatedAllow,
  encodeAuthzHistoryReq,
  encodeAuthzReply,
  encodeBindRolesReq,
  encodeRole,
  featureAction,
  grantsAllow,
  resourcePatternAll,
  resourcePatternLiteral,
  resourcePatternMatches,
  resourcePatternPrefix,
  validateRoleName
} from "../../src/wire/authz.js"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"
import {
  AGDX_AGENT_SUBMIT_CODE,
  AGDX_BATCH_CODE,
  AGDX_HELLO_CODE,
  AGDX_KV_GET_CODE
} from "../../src/wire/codes.js"

const FIXTURES_DIR = path.resolve(process.cwd(), "../../wire/fixtures")

async function readFixture(name: string): Promise<Uint8Array> {
  const buffer = await readFile(path.join(FIXTURES_DIR, name))
  return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength)
}

void test("given_the_authz_role_fixture_when_decoded_then_should_re_encode_byte_identically", async () => {
  const bytes = await readFixture("authz_role.bin")
  const map = expectMap(decodeOne(bytes, "authz_role"), "authz_role")
  const role = decodeRole(map, "authz_role")
  assert.equal(role.name, "kv-reader")
  assert.equal(role.grants.length, 2)
  const reencoded = encodeNamed(encodeRole(role))
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
})

void test("given_the_authz_bind_roles_fixture_when_decoded_then_should_re_encode_byte_identically", async () => {
  const bytes = await readFixture("authz_bind_roles.bin")
  const map = expectMap(decodeOne(bytes, "authz_bind_roles"), "authz_bind_roles")
  const req = decodeBindRolesReq(map, "authz_bind_roles")
  assert.equal(req.userId, 7)
  assert.deepEqual(req.roles, ["kv-reader", "admin"])
  assert.equal(req.expectRevision, 3n)
  const reencoded = encodeNamed(encodeBindRolesReq(req))
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
})

void test("given_the_authz_history_request_fixture_when_decoded_then_should_re_encode_byte_identically", async () => {
  const bytes = await readFixture("authz_history_request.bin")
  const map = expectMap(decodeOne(bytes, "authz_history_request"), "authz_history_request")
  const req = decodeAuthzHistoryReq(map, "authz_history_request")
  assert.deepEqual(req.subject, { kind: "binding", userId: 7 })
  assert.equal(req.afterRevision, 2n)
  assert.equal(req.limit, 50)
  const reencoded = encodeNamed(encodeAuthzHistoryReq(req))
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
})

async function assertAuthzReplyRoundTrips(name: string) {
  const bytes = await readFixture(name)
  const reply = decodeAuthzReply(decodeOne(bytes, name), name)
  const reencodedValue = encodeAuthzReply(reply)
  if (!(reencodedValue instanceof Map)) {
    throw new Error(`expected ${name} to re-encode to a map`)
  }
  const reencoded = encodeNamed(reencodedValue)
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
  return reply
}

void test("given_the_authz_whoami_reply_fixture_when_decoded_then_should_carry_roles_and_grants", async () => {
  const reply = await assertAuthzReplyRoundTrips("authz_whoami_reply.bin")
  if (reply.kind !== "whoami") throw new Error("wrong shape")
  assert.deepEqual(reply.reply.roles, ["admin"])
  assert.equal(reply.reply.grants.length, 1)
})

void test("given_the_authz_get_role_reply_fixture_when_decoded_then_should_carry_the_role", async () => {
  const reply = await assertAuthzReplyRoundTrips("authz_get_role_reply.bin")
  if (reply.kind !== "role") throw new Error("wrong shape")
  assert.ok(reply.role !== undefined)
  assert.equal(reply.role.name, "kv-reader")
})

void test("given_the_authz_history_reply_fixture_when_decoded_then_should_carry_events", async () => {
  const reply = await assertAuthzReplyRoundTrips("authz_history_reply.bin")
  if (reply.kind !== "history") throw new Error("wrong shape")
  assert.equal(reply.reply.events.length, 1)
  const [event] = reply.reply.events
  assert.ok(event !== undefined)
  assert.deepEqual(event.op, { kind: "rolesBound", userId: 7, roles: ["kv-reader"] })
})

void test("given_role_names_when_validated_then_should_enforce_charset_and_length", () => {
  assert.doesNotThrow(() => {
    validateRoleName("kv-reader")
  })
  assert.doesNotThrow(() => {
    validateRoleName("ops.admin_2")
  })
  assert.throws(() => {
    validateRoleName("")
  })
  assert.throws(() => {
    validateRoleName("bad name")
  })
  assert.throws(() => {
    validateRoleName("rôle")
  })
})

void test("given_a_resource_pattern_when_matched_then_should_honor_its_kind", () => {
  assert.equal(resourcePatternMatches(resourcePatternAll(), "anything"), true)
  assert.equal(resourcePatternMatches(resourcePatternAll(), undefined), true)
  assert.equal(resourcePatternMatches(resourcePatternLiteral("ns"), "ns"), true)
  assert.equal(resourcePatternMatches(resourcePatternLiteral("ns"), "ns2"), false)
  assert.equal(resourcePatternMatches(resourcePatternPrefix("agent-"), "agent-abc"), true)
  assert.equal(resourcePatternMatches(resourcePatternPrefix("agent-"), "other"), false)
  assert.equal(resourcePatternMatches(resourcePatternLiteral("ns"), undefined), false)
  assert.equal(resourcePatternMatches(resourcePatternPrefix("agent-"), undefined), false)
})

void test("given_delegation_when_checked_then_agent_is_intersected_with_the_user", () => {
  const allow = (
    feature: "kv",
    action: "read" | "write",
    resource: ReturnType<typeof resourcePatternAll>
  ) => ({
    effect: "allow" as const,
    feature,
    action,
    resource
  })
  const agent = [
    allow("kv", "read", resourcePatternAll()),
    allow("kv", "write", resourcePatternAll())
  ]
  const user = [allow("kv", "read", resourcePatternPrefix("shared/"))]
  assert.equal(delegatedAllow(agent, user, "kv", "read", "shared/x"), true)
  assert.equal(delegatedAllow(agent, user, "kv", "read", "private/x"), false)
  assert.equal(delegatedAllow(agent, user, "kv", "write", "shared/x"), false)
  assert.equal(grantsAllow([], "kv", "read", undefined), false)
})

void test("given_a_command_code_when_classified_then_should_map_to_feature_and_action", () => {
  assert.deepEqual(featureAction(AGDX_KV_GET_CODE), ["kv", "read"])
  assert.equal(featureAction(AGDX_HELLO_CODE), undefined)
  assert.equal(featureAction(AGDX_BATCH_CODE), undefined)
  assert.deepEqual(featureAction(AGDX_AGENT_SUBMIT_CODE), ["agent", "write"])
})

void test("given_feature_action_pairs_when_indexed_then_should_fit_a_u64_mask", () => {
  const seen = new Set<number>()
  const features = [
    "kv",
    "memory",
    "projection",
    "fork",
    "graph",
    "query",
    "agent",
    "workflow",
    "authz",
    "unrecognized"
  ] as const
  const actions = ["read", "write", "delete", "admin", "unrecognized"] as const
  for (const feature of features) {
    for (const action of actions) {
      const index = actionIndex(feature, action)
      assert.ok(index < 64)
      assert.ok(!seen.has(index))
      seen.add(index)
    }
  }
})
