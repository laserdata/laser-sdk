import assert from "node:assert/strict"
import { test } from "node:test"
import type { Capabilities } from "../../src/client/capabilities.js"
import { managedCapabilitiesFrom, OPEN_CAPABILITIES } from "../../src/client/capabilities.js"
import { ForkExecutionError, InvalidError, UnsupportedError } from "../../src/client/errors.js"
import { Fork } from "../../src/managed/forks.js"
import { encodeNamed } from "../../src/wire/cbor.js"
import {
  ForkCreateCommand,
  ForkDeleteCommand,
  ForkListCommand,
  ForkPromoteCommand,
  ForkPutCommand
} from "../../src/wire/commands.js"
import {
  type ForkInfo,
  type ForkOutcome,
  type ForkReply,
  encodeForkReply
} from "../../src/wire/fork.js"

const CAPS: Capabilities = managedCapabilitiesFrom({
  versions: { query: 1, control: 1, kv: 1, fork: 1, agent: 1, graph: 1, features: 0n },
  backends: []
})

function replyFrame(reply: ForkReply): Uint8Array {
  const value = encodeForkReply(reply)
  if (!(value instanceof Map)) throw new Error("expected a map-shaped reply")
  return encodeNamed(value)
}

function okFrame(outcome: ForkOutcome): Uint8Array {
  return replyFrame({ kind: "ok", outcome })
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

function fork(forkId: string, replies: readonly Uint8Array[], capabilities: Capabilities = CAPS) {
  const transport = fakeTransport(replies)
  return { fork: new Fork(transport, () => Promise.resolve(capabilities), forkId), transport }
}

const INFO: ForkInfo = {
  forkId: "experiment",
  kind: "continuous",
  userId: 7,
  status: "open",
  createdAtMicros: 1n,
  rowCount: 0
}

void test("given_open_capabilities_when_create_is_sent_then_should_reject_before_the_transport", async () => {
  const { fork: handle, transport } = fork("experiment", [], OPEN_CAPABILITIES)
  await assert.rejects(() => handle.create().send(), UnsupportedError)
  assert.equal(transport.calls.length, 0)
})

void test("given_an_invalid_fork_id_when_create_is_sent_then_should_reject_before_the_transport", async () => {
  const { fork: handle, transport } = fork("bad id!", [])
  await assert.rejects(() => handle.create().send(), InvalidError)
  assert.equal(transport.calls.length, 0)
})

void test("given_a_created_outcome_when_create_is_sent_then_should_return_the_fork_info_and_use_the_create_command", async () => {
  const { fork: handle, transport } = fork("experiment", [okFrame({ kind: "created", info: INFO })])
  const info = await handle.create().severed().parent("baseline").tables(["orders"]).send()
  assert.deepEqual(info, INFO)
  assert.equal(transport.calls[0]?.code, ForkCreateCommand.code)
})

void test("given_a_promoted_outcome_when_promote_is_called_then_should_return_the_row_count", async () => {
  const { fork: handle, transport } = fork("experiment", [okFrame({ kind: "promoted", rows: 4 })])
  assert.equal(await handle.promote(), 4)
  assert.equal(transport.calls[0]?.code, ForkPromoteCommand.code)
})

void test("given_a_deleted_outcome_when_squash_is_called_then_should_return_whether_it_existed", async () => {
  const { fork: handle, transport } = fork("experiment", [
    okFrame({ kind: "deleted", removed: true })
  ])
  assert.equal(await handle.squash(), true)
  assert.equal(transport.calls[0]?.code, ForkDeleteCommand.code)
})

void test("given_a_written_outcome_when_put_row_is_sent_then_should_use_the_put_command", async () => {
  const { fork: handle, transport } = fork("experiment", [okFrame({ kind: "written" })])
  await handle
    .putRow("orders", 0, 1n)
    .field("status", "resolved")
    .metadata("trace_id", "abc")
    .payload(Uint8Array.of(1))
    .embedding("[0.1,0.2]")
    .tombstone()
    .send()
  assert.equal(transport.calls[0]?.code, ForkPutCommand.code)
})

void test("given_a_list_outcome_when_forks_is_listed_then_should_return_every_fork", async () => {
  const { transport } = fork("experiment", [okFrame({ kind: "list", forks: [INFO] })])
  const forks = await Fork.forks(transport, () => Promise.resolve(CAPS))
  assert.deepEqual(forks, [INFO])
  assert.equal(transport.calls[0]?.code, ForkListCommand.code)
})

void test("given_an_err_reply_when_promote_fails_then_should_wrap_it_as_a_fork_execution_error", async () => {
  const { fork: handle } = fork("experiment", [
    replyFrame({ kind: "err", error: { kind: "notFound", message: "no such fork" } })
  ])
  await assert.rejects(() => handle.promote(), ForkExecutionError)
})
