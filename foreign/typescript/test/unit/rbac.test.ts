import assert from "node:assert/strict"
import { test } from "node:test"
import type { Capabilities } from "../../src/client/capabilities.js"
import { managedCapabilitiesFrom, OPEN_CAPABILITIES } from "../../src/client/capabilities.js"
import { AuthzExecutionError, InvalidError, UnsupportedError } from "../../src/client/errors.js"
import {
  authzHistory,
  bindRoles,
  defineRole,
  deleteRole,
  getBindings,
  getRole,
  listRoles,
  whoami
} from "../../src/managed/rbac.js"
import { encodeOne } from "../../src/wire/cbor.js"
import type { AuthzReply, Role } from "../../src/wire/authz.js"
import { encodeAuthzReply } from "../../src/wire/authz.js"
import { Feature } from "../../src/wire/hello.js"

const CAPS: Capabilities = managedCapabilitiesFrom({
  versions: { query: 1, control: 1, kv: 1, fork: 1, agent: 1, graph: 1, features: Feature.AUTHZ },
  backends: []
})

function replyFrame(reply: AuthzReply): Uint8Array {
  return encodeOne(encodeAuthzReply(reply))
}

function fakeTransport(scriptedReplies: readonly Uint8Array[]): {
  readonly calls: { readonly code: number; readonly payload: Uint8Array }[]
  sendManaged(code: number, payload: Uint8Array): Promise<Uint8Array>
} {
  const calls: { code: number; payload: Uint8Array }[] = []
  let next = 0
  return {
    calls,
    sendManaged(code, payload) {
      calls.push({ code, payload })
      const reply = scriptedReplies[next]
      next += 1
      if (reply === undefined) throw new Error("fake transport ran out of scripted replies")
      return Promise.resolve(reply)
    }
  }
}

const ROLE: Role = {
  name: "reader",
  grants: [{ effect: "allow", feature: "kv", action: "read", resource: { kind: "all", value: "" } }]
}

void test("given_a_whoami_outcome_when_whoami_is_called_then_should_return_it", async () => {
  const transport = fakeTransport([
    replyFrame({ kind: "whoami", reply: { roles: ["reader"], grants: ROLE.grants } })
  ])
  const reply = await whoami(transport, CAPS)
  assert.deepEqual(reply.roles, ["reader"])
})

void test("given_a_roles_outcome_when_list_roles_is_called_then_should_return_them", async () => {
  const transport = fakeTransport([replyFrame({ kind: "roles", reply: { roles: [ROLE] } })])
  const roles = await listRoles(transport, CAPS, { namePrefix: "read" })
  assert.deepEqual(roles, [ROLE])
})

void test("given_an_invalid_role_name_when_get_role_is_called_then_should_reject_before_the_transport", async () => {
  const transport = fakeTransport([])
  await assert.rejects(() => getRole(transport, CAPS, "bad name!"), InvalidError)
  assert.equal(transport.calls.length, 0)
})

void test("given_a_role_outcome_when_get_role_is_called_then_should_return_it", async () => {
  const transport = fakeTransport([replyFrame({ kind: "role", role: ROLE })])
  const role = await getRole(transport, CAPS, "reader")
  assert.deepEqual(role, ROLE)
})

void test("given_an_absent_role_outcome_when_get_role_is_called_then_should_return_undefined", async () => {
  const transport = fakeTransport([replyFrame({ kind: "role" })])
  assert.equal(await getRole(transport, CAPS, "reader"), undefined)
})

void test("given_a_bindings_outcome_when_get_bindings_is_called_then_should_return_the_role_names", async () => {
  const transport = fakeTransport([replyFrame({ kind: "bindings", reply: { roles: ["reader"] } })])
  assert.deepEqual(await getBindings(transport, CAPS, 7), ["reader"])
})

void test("given_an_ok_reply_when_define_role_is_called_then_should_resolve", async () => {
  const transport = fakeTransport([replyFrame({ kind: "ok" })])
  await defineRole(transport, CAPS, ROLE)
  assert.equal(transport.calls.length, 1)
})

void test("given_an_invalid_role_name_when_define_role_is_called_then_should_reject_before_the_transport", async () => {
  const transport = fakeTransport([])
  await assert.rejects(() => defineRole(transport, CAPS, { name: "", grants: [] }), InvalidError)
  assert.equal(transport.calls.length, 0)
})

void test("given_an_ok_reply_when_delete_role_is_called_then_should_resolve", async () => {
  const transport = fakeTransport([replyFrame({ kind: "ok" })])
  await deleteRole(transport, CAPS, "reader")
  assert.equal(transport.calls.length, 1)
})

void test("given_invalid_role_names_when_bind_roles_is_called_then_should_reject_before_the_transport", async () => {
  const transport = fakeTransport([])
  await assert.rejects(() => bindRoles(transport, CAPS, 7, ["ok", "bad name!"]), InvalidError)
  assert.equal(transport.calls.length, 0)
})

void test("given_an_ok_reply_when_bind_roles_is_called_then_should_resolve", async () => {
  const transport = fakeTransport([replyFrame({ kind: "ok" })])
  await bindRoles(transport, CAPS, 7, ["reader"], 3n)
  assert.equal(transport.calls.length, 1)
})

void test("given_a_history_outcome_when_authz_history_is_called_then_should_return_it", async () => {
  const transport = fakeTransport([replyFrame({ kind: "history", reply: { events: [] } })])
  const page = await authzHistory(transport, CAPS, { kind: "all" }, 10, 5n)
  assert.deepEqual(page.events, [])
})

void test("given_open_capabilities_when_whoami_is_called_then_should_reject_before_the_transport", async () => {
  const transport = fakeTransport([])
  await assert.rejects(() => whoami(transport, OPEN_CAPABILITIES), UnsupportedError)
  assert.equal(transport.calls.length, 0)
})

void test("given_an_err_reply_when_define_role_fails_then_should_wrap_it_as_an_authz_execution_error", async () => {
  const transport = fakeTransport([
    replyFrame({ kind: "err", error: { kind: "unknownRole", name: "reader" } })
  ])
  await assert.rejects(() => defineRole(transport, CAPS, ROLE), AuthzExecutionError)
})
