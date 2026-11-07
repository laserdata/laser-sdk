import assert from "node:assert/strict"
import { test } from "node:test"
import { NotConnectedError, OperationUncertainError } from "apache-iggy"

import {
  AmbiguousMutationError,
  ConfigError,
  InvalidError,
  KvExecutionError,
  ProtocolError,
  TimeoutError,
  TransportError,
  UnsupportedError
} from "../../src/client/errors.js"
import { isRetryable } from "../../src/client/error-classify.js"
import {
  DEFAULT_ATTEMPT_TIMEOUT_MS,
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

const unknownOutcome = (code: string): Uint8Array =>
  encodeNamed(
    new Map<string, unknown>([
      ["code", code],
      ["message", "the outcome is not known yet"]
    ])
  )

void test("given_an_unknown_outcome_reply_when_executed_then_should_keep_the_transport_and_report_ambiguity_with_the_identity", async () => {
  for (const code of ["pending", "backend_unavailable", "unavailable"]) {
    let resets = 0
    const transport: ManagedKvTransport = {
      send: () => Promise.resolve(unknownOutcome(code)),
      reset: () => {
        resets += 1
        return Promise.resolve()
      }
    }
    const client = new FencedLeaseClient(transport)
    const operation = client.prepareAcquire(acquire)
    const identity = operation.operationId.toString(16).padStart(32, "0")
    await assert.rejects(
      client.acquire(operation),
      (error: unknown) =>
        error instanceof AmbiguousMutationError &&
        error.message.includes(identity) &&
        error.operationId === operation.operationId,
      `${code} must be ambiguous and carry the prepared identity`
    )
    assert.equal(resets, 0, "a whole reply leaves the connection in step for other requests")
    assert.deepEqual(operation.ambiguousRecovery, { kind: "waitForLeaseExpiry", ttlMs: 1000 })
  }
})

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

void test("given_a_driver_that_wrote_the_frame_and_lost_its_reply_when_executed_then_should_report_ambiguity_with_the_identity", async () => {
  let resets = 0
  const client = new FencedLeaseClient({
    send: () => Promise.reject(new OperationUncertainError(9n, new Error("the link dropped"))),
    reset: () => {
      resets += 1
      return Promise.resolve()
    }
  })
  const operation = client.prepareRelease(release)
  await assert.rejects(
    client.release(operation),
    (error: unknown) =>
      error instanceof AmbiguousMutationError && error.operationId === operation.operationId
  )
  assert.equal(resets, 1)
})

void test("given_a_driver_that_stopped_before_writing_the_frame_when_executed_then_should_report_a_definite_failure", async () => {
  let resets = 0
  const client = new FencedLeaseClient({
    send: () =>
      Promise.reject(
        new TransportError("no link carried the frame", true, { cause: new NotConnectedError() })
      ),
    reset: () => {
      resets += 1
      return Promise.resolve()
    }
  })
  await assert.rejects(
    client.release(client.prepareRelease(release)),
    (error: unknown) =>
      error instanceof TransportError && !(error instanceof AmbiguousMutationError)
  )
  assert.equal(resets, 1)
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

void test("given_default_client_when_built_then_should_allow_attempts_beyond_the_server_fold_wait", () => {
  assert.equal(DEFAULT_ATTEMPT_TIMEOUT_MS, 15_000)
})

void test("given_an_ambiguous_acquisition_on_one_key_when_another_key_is_acquired_then_should_not_wait_behind_it", async () => {
  const coordinator = new LeaseCoordinator(
    new FencedLeaseClient({
      send: (_code, frame) => {
        const request = decodeManagedRequestEnvelope(
          expectMap(decodeOne(frame, "request"), "request"),
          "request"
        )
        const stuck = new TextDecoder().decode(request.payload).includes("stuck")
        return Promise.resolve(stuck ? unknownOutcome("pending") : leased(3n))
      },
      reset: () => Promise.resolve()
    })
  )
  const stuck = coordinator.acquire({ ...acquire, key: utf8("stuck") })
  const started = performance.now()
  assert.equal((await coordinator.acquire({ ...acquire, key: utf8("free") })).token, 3n)
  assert.ok(performance.now() - started < 500, "the free key is granted at once")
  await assert.rejects(stuck, AmbiguousMutationError)
})

void test("given_an_ambiguous_coordinated_acquisition_when_repeated_then_should_return_the_recorded_grant_under_the_same_identity", async () => {
  const sent: Uint8Array[] = []
  const coordinator = new LeaseCoordinator(
    new FencedLeaseClient({
      send: (_code, frame) => {
        sent.push(frame.slice())
        return sent.length === 1
          ? Promise.reject(new TransportError("connection lost", true))
          : Promise.resolve(leased(9n))
      },
      reset: () => Promise.resolve()
    })
  )
  const started = performance.now()
  assert.equal((await coordinator.acquire(acquire)).token, 9n)
  assert.ok(performance.now() - started < 900, "the repeat answers well inside the TTL")
  assert.equal(sent.length, 2)
  assert.deepEqual(sent[0], sent[1], "the repeat carries the same identity")
})

void test("given_an_acquisition_that_stays_unknown_when_its_ttl_window_passes_then_should_report_ambiguity_without_a_further_wait", async () => {
  let calls = 0
  const coordinator = new LeaseCoordinator(
    new FencedLeaseClient({
      send: () => {
        calls += 1
        return Promise.resolve(unknownOutcome("pending"))
      },
      reset: () => Promise.resolve()
    })
  )
  const started = performance.now()
  await assert.rejects(coordinator.acquire(acquire), AmbiguousMutationError)
  const elapsed = performance.now() - started
  assert.ok(elapsed >= 990, "repeats fill the window")
  assert.ok(elapsed < 2_000, `no TTL sleep follows the window, took ${String(elapsed)} ms`)
  assert.ok(calls > 2)
})

void test("given_a_grant_answered_late_when_acquired_then_should_be_valid_from_the_first_send", async () => {
  const client = new FencedLeaseClient({
    send: async () => {
      await new Promise((resolve) => setTimeout(resolve, 200))
      return leased(4n)
    },
    reset: () => Promise.resolve()
  })
  const before = performance.now()
  const lease = await client.acquire(client.prepareAcquire(acquire))
  const after = performance.now()
  assert.ok(lease.validUntilMs >= before + lease.grantedTtlMs)
  assert.ok(
    lease.validUntilMs + 190 <= after + lease.grantedTtlMs,
    "the reply delay is spent from the grant, not added to it"
  )
})

void test("given_a_grant_that_arrives_after_its_validity_when_coordinated_then_should_reject_as_lease_lost", async () => {
  const coordinator = new LeaseCoordinator(
    new FencedLeaseClient({
      send: async () => {
        await new Promise((resolve) => setTimeout(resolve, 1_100))
        return leased(4n)
      },
      reset: () => Promise.resolve()
    })
  )
  await assert.rejects(
    coordinator.acquire(acquire),
    (error: unknown) =>
      error instanceof KvExecutionError &&
      (error.detail as { readonly kind: string }).kind === "leaseLost"
  )
})

const renew = {
  namespace: "coord",
  key: utf8("run"),
  holderId: "worker",
  leaseToken: 7n,
  leaseTtlMicros: 1_000_000n
}
const renewed = (token: bigint): Uint8Array =>
  reply({
    kind: "renewed",
    leaseToken: token,
    grantedTtlMicros: 1_000_000n,
    position: { topicGeneration: 1n, partition: 0, offset: 2n }
  })

void test("given_an_ambiguous_renewal_when_coordinated_until_a_deadline_then_should_repeat_its_identity_and_return_the_renewal", async () => {
  const sent: Uint8Array[] = []
  const coordinator = new LeaseCoordinator(
    new FencedLeaseClient({
      send: (_code, frame) => {
        sent.push(frame.slice())
        return sent.length === 1
          ? Promise.reject(new TransportError("connection lost", true))
          : Promise.resolve(renewed(7n))
      },
      reset: () => Promise.resolve()
    })
  )
  const renewal = await coordinator.renew(renew, performance.now() + 1_000)
  assert.equal(renewal.token, 7n)
  assert.equal(sent.length, 2)
  assert.deepEqual(sent[0], sent[1], "the repeat carries the same identity")
})

void test("given_a_renewal_that_never_answers_when_its_deadline_passes_then_should_end_with_lease_lost", async () => {
  const coordinator = new LeaseCoordinator(
    new FencedLeaseClient({
      send: () => new Promise<Uint8Array>(() => undefined),
      reset: () => Promise.resolve()
    })
  )
  const started = performance.now()
  await assert.rejects(
    coordinator.renew(renew, started + 300),
    (error: unknown) =>
      error instanceof KvExecutionError &&
      (error.detail as { readonly kind: string }).kind === "leaseLost"
  )
  const elapsed = performance.now() - started
  assert.ok(elapsed >= 290, "the attempt runs until the deadline")
  assert.ok(elapsed < 1_000, `the attempt is cut at the deadline, took ${String(elapsed)} ms`)
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
