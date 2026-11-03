import assert from "node:assert/strict"
import { test } from "node:test"

import {
  AmbiguousMutationError,
  ConfigError,
  InvalidError,
  ProtocolError,
  TimeoutError,
  TransportError,
  UnsupportedError
} from "../../src/client/errors.js"
import { isRetryable } from "../../src/agent/reliable-consumer.js"
import {
  FencedLeaseClient,
  LeaseCoordinator,
  type ManagedKvTransport
} from "../../src/managed/coordination.js"
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
    ttlMs: 1_000
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

const leased = (token: bigint): Uint8Array =>
  reply({
    kind: "leased",
    leaseToken: token,
    grantedTtlMicros: 1_000_000n,
    position: { topicGeneration: 1n, partition: 0, offset: token }
  })

void test("given_concurrent_acquisitions_when_coordinated_then_should_send_one_at_a_time", async () => {
  let inFlight = 0
  let peak = 0
  let token = 0n
  const coordinator = new LeaseCoordinator(
    new FencedLeaseClient({
      send: async () => {
        inFlight += 1
        peak = Math.max(peak, inFlight)
        await new Promise((resolve) => setTimeout(resolve, 5))
        inFlight -= 1
        token += 1n
        return leased(token)
      },
      reset: () => Promise.resolve()
    })
  )
  const leases = await Promise.all([
    coordinator.acquire(acquire),
    coordinator.acquire({ ...acquire, key: utf8("other") }),
    coordinator.acquire({ ...acquire, key: utf8("third") })
  ])
  assert.equal(peak, 1)
  assert.deepEqual(
    leases.map((lease) => lease.token),
    [1n, 2n, 3n]
  )
})

void test("given_an_ambiguous_coordinated_acquisition_when_it_fails_then_should_wait_the_requested_ttl_and_free_the_gate", async () => {
  let calls = 0
  const coordinator = new LeaseCoordinator(
    new FencedLeaseClient({
      send: () => {
        calls += 1
        return calls === 1
          ? Promise.reject(new TransportError("connection lost", true))
          : Promise.resolve(leased(9n))
      },
      reset: () => Promise.resolve()
    })
  )
  const started = performance.now()
  await assert.rejects(coordinator.acquire(acquire), AmbiguousMutationError)
  assert.ok(performance.now() - started >= 990, "the whole requested TTL elapses")
  assert.equal((await coordinator.acquire(acquire)).token, 9n)
})

void test("given_no_dedicated_client_when_coordinated_then_should_reject_as_config", async () => {
  await assert.rejects(new LeaseCoordinator().acquire(acquire), ConfigError)
})

void test("given_a_definite_rejection_when_coordinated_then_should_preserve_it_without_ttl_recovery", async () => {
  const denied = new TransportError("server rejected acquisition", false)
  const coordinator = new LeaseCoordinator(
    new FencedLeaseClient({ send: () => Promise.reject(denied), reset: () => Promise.resolve() })
  )
  const started = performance.now()
  await assert.rejects(coordinator.acquire(acquire), (error: unknown) => error === denied)
  assert.ok(performance.now() - started < 500, "no TTL wait for a definite answer")
})
