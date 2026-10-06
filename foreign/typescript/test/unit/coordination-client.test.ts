import assert from "node:assert/strict"
import { test } from "node:test"

import {
  AmbiguousMutationError,
  InvalidError,
  ProtocolError,
  TimeoutError,
  UnsupportedError
} from "../../src/client/errors.js"
import { isRetryable } from "../../src/agent/reliable-consumer.js"
import { FencedLeaseClient, type ManagedKvTransport } from "../../src/managed/coordination.js"
import { utf8 } from "../../src/client/bytes.js"
import { decodeManagedRequestEnvelope } from "../../src/wire/mutation.js"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"
import { encodeKvReply, type KvOutcome } from "../../src/wire/kv.js"

const release = { namespace: "coord", key: utf8("run"), leaseToken: 7n, holderId: "worker" }
const acquire = {
  namespace: "coord",
  key: utf8("run"),
  leaseTtlMicros: 1_000_000n,
  holderId: "worker"
}
const reply = (outcome: KvOutcome): Uint8Array =>
  encodeNamed(encodeKvReply({ kind: "ok", outcome }) as ReadonlyMap<string, unknown>)

void test("given_a_prepared_release_when_repeated_after_ambiguity_then_should_keep_the_exact_operation_and_frame", async () => {
  const sent: Uint8Array[] = []
  let resets = 0
  const transport: ManagedKvTransport = {
    send: (_code, frame) => {
      sent.push(frame.slice())
      if (sent.length === 1) return Promise.reject(new TimeoutError("transport"))
      return Promise.resolve(reply({ kind: "released", wasHeld: true }))
    },
    reset: () => {
      resets += 1
      return Promise.resolve()
    }
  }
  const client = new FencedLeaseClient(transport)
  const operation = client.prepareRelease(release)
  assert.equal(operation.ambiguousRecovery.kind, "repeatPrepared")
  await assert.rejects(client.release(operation), AmbiguousMutationError)
  assert.equal(await client.release(operation), true)
  assert.equal(resets, 1)
  assert.deepEqual(sent[0], sent[1])
  const envelope = decodeManagedRequestEnvelope(
    expectMap(decodeOne(sent[0] ?? new Uint8Array(), "request"), "request"),
    "request"
  )
  assert.equal(envelope.operationId, operation.operationId)
})

void test("given_prepared_mutations_when_used_by_another_client_or_operation_then_should_refuse_before_send", async () => {
  let calls = 0
  const transport: ManagedKvTransport = {
    send: () => {
      calls += 1
      return Promise.resolve(reply({ kind: "released", wasHeld: false }))
    },
    reset: () => Promise.resolve()
  }
  const first = new FencedLeaseClient(transport)
  const second = new FencedLeaseClient(transport)
  const operation = first.prepareRelease(release)
  await assert.rejects(second.release(operation), InvalidError)
  await assert.rejects(first.acquire(operation), InvalidError)
  await assert.rejects(first.release({ ...operation }), InvalidError)
  assert.equal(calls, 0)
})

void test("given_a_stalled_acquisition_when_timed_out_then_should_reset_and_require_waiting_through_the_requested_lifetime", async () => {
  let reset = false
  const transport: ManagedKvTransport = {
    send: () => new Promise<Uint8Array>(() => undefined),
    reset: () => {
      reset = true
      return Promise.resolve()
    }
  }
  const client = new FencedLeaseClient(transport).withAttemptTimeout(5)
  const operation = client.prepareAcquire(acquire)
  assert.deepEqual(operation.ambiguousRecovery, {
    kind: "waitForLeaseExpiry",
    ttlMicros: 1_000_000n
  })
  await assert.rejects(client.acquire(operation), (error: unknown) => {
    assert.ok(error instanceof AmbiguousMutationError)
    assert.equal(isRetryable(error), false)
    return true
  })
  assert.equal(reset, true)
})

void test("given_a_stalled_read_when_timed_out_then_should_return_a_plain_timeout_and_reset", async () => {
  let resets = 0
  const client = new FencedLeaseClient({
    send: () => new Promise<Uint8Array>(() => undefined),
    reset: () => {
      resets += 1
      return Promise.resolve()
    }
  }).withAttemptTimeout(5)
  await assert.rejects(client.get({ namespace: "coord", key: utf8("run") }), TimeoutError)
  assert.equal(resets, 1)
})

void test("given_unsupported_readiness_or_zero_timeout_when_executed_then_should_refuse_before_mutation", async () => {
  let calls = 0
  let resets = 0
  const client = new FencedLeaseClient({
    ready: () => Promise.reject(new UnsupportedError("coordination")),
    send: () => {
      calls += 1
      return Promise.resolve(new Uint8Array())
    },
    reset: () => {
      resets += 1
      return Promise.resolve()
    }
  })
  const operation = client.prepareRelease(release)
  await assert.rejects(client.release(operation), UnsupportedError)
  client.withAttemptTimeout(0)
  await assert.rejects(client.release(operation), InvalidError)
  assert.equal(calls, 0)
  assert.equal(resets, 0)
})

void test("given_an_unexpected_reply_or_invalid_request_when_executed_then_should_fail_closed", async () => {
  const client = new FencedLeaseClient({
    send: () => Promise.resolve(reply({ kind: "written" })),
    reset: () => Promise.resolve()
  })
  await assert.rejects(client.release(client.prepareRelease(release)), ProtocolError)
  assert.throws(() => client.prepareAcquire({ ...acquire, leaseTtlMicros: 0n }), InvalidError)
})
