import assert from "node:assert/strict"
import { test } from "node:test"
import { OPEN_CAPABILITIES, managedCapabilitiesFrom } from "../../src/client/capabilities.js"
import { InvalidError, UnsupportedError } from "../../src/client/errors.js"
import { executeBatch } from "../../src/managed/batch.js"
import { encodeBatchReply } from "../../src/wire/batch.js"
import { AGDX_BATCH_CODE } from "../../src/wire/codes.js"
import { encodeNamed } from "../../src/wire/cbor.js"
import { MAX_BATCH_OPS } from "../../src/wire/limits.js"

const MANAGED = managedCapabilitiesFrom({
  versions: { query: 1, control: 1, kv: 1, fork: 1, agent: 1, graph: 1, features: 0n },
  backends: []
})

void test("given_scripted_results_when_executed_then_should_send_the_batch_frame_and_return_slots_in_order", async () => {
  let capturedCode: number | undefined
  let capturedPayload: Uint8Array | undefined
  const slots = [Uint8Array.of(1), Uint8Array.of(2)]
  const transport = {
    sendManaged(code: number, payload: Uint8Array): Promise<Uint8Array> {
      capturedCode = code
      capturedPayload = payload
      return Promise.resolve(encodeNamed(encodeBatchReply({ results: slots })))
    }
  }
  const ops = [
    { code: 1_000_300, payload: Uint8Array.of(9) },
    { code: 1_000_301, payload: Uint8Array.of(8) }
  ]
  const results = await executeBatch(transport, MANAGED, ops)
  assert.equal(capturedCode, AGDX_BATCH_CODE)
  assert.ok(capturedPayload !== undefined && capturedPayload.byteLength > 0)
  assert.deepEqual(results, slots)
})

void test("given_open_capabilities_when_executed_then_should_reject_before_the_transport", async () => {
  let sent = false
  const transport = {
    sendManaged(): Promise<Uint8Array> {
      sent = true
      return Promise.resolve(new Uint8Array())
    }
  }
  await assert.rejects(executeBatch(transport, OPEN_CAPABILITIES, []), UnsupportedError)
  assert.equal(sent, false)
})

void test("given_too_many_ops_when_executed_then_should_reject_before_the_transport", async () => {
  let sent = false
  const transport = {
    sendManaged(): Promise<Uint8Array> {
      sent = true
      return Promise.resolve(new Uint8Array())
    }
  }
  const ops = Array.from({ length: MAX_BATCH_OPS + 1 }, () => ({
    code: 1_000_300,
    payload: new Uint8Array()
  }))
  await assert.rejects(executeBatch(transport, MANAGED, ops), InvalidError)
  assert.equal(sent, false)
})

void test("given_a_nested_batch_op_when_executed_then_should_reject_before_the_transport", async () => {
  let sent = false
  const transport = {
    sendManaged(): Promise<Uint8Array> {
      sent = true
      return Promise.resolve(new Uint8Array())
    }
  }
  await assert.rejects(
    executeBatch(transport, MANAGED, [{ code: AGDX_BATCH_CODE, payload: new Uint8Array() }]),
    InvalidError
  )
  assert.equal(sent, false)
})
