import assert from "node:assert/strict"
import { test } from "node:test"

import { isPermissionDenied, isStale } from "../../src/client/error-classify.js"
import {
  AmbiguousMutationError,
  KvExecutionError,
  TimeoutError,
  TransportError,
  UnsupportedError
} from "../../src/client/errors.js"
import { ApacheIggyTransport } from "../../src/iggy/apache-iggy.js"
import { DedicatedKvTransport, FencedLeaseClient } from "../../src/managed/coordination.js"
import { encodeOne, encodeNamed } from "../../src/wire/cbor.js"
import { encodeBackendAnnounce, feature } from "../../src/wire/hello.js"
import { encodeKvReply } from "../../src/wire/kv.js"

function deferred<T>() {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((complete) => {
    resolve = complete
  })
  return { promise, resolve }
}

const supported = encodeBackendAnnounce({
  versions: {
    query: 1,
    control: 1,
    kv: 1,
    fork: 1,
    agent: 1,
    graph: 1,
    features: feature.KV_FENCED_LEASES
  },
  backends: []
})
const unsupported = encodeBackendAnnounce({
  versions: { query: 1, control: 1, kv: 1, fork: 1, agent: 1, graph: 1, features: 0n },
  backends: []
})

void test("given_a_retired_probe_when_its_reply_arrives_then_should_not_enable_the_replacement_connection", async (t) => {
  const answer = deferred<Uint8Array>()
  const entered = deferred<undefined>()
  let connects = 0
  t.mock.method(ApacheIggyTransport, "connect", () => {
    connects += 1
    return Promise.resolve({
      sendManaged: () => {
        entered.resolve(undefined)
        return connects === 1 ? answer.promise : Promise.resolve(unsupported)
      },
      close: () => Promise.resolve(undefined)
    } as unknown as ApacheIggyTransport)
  })
  await using transport = new DedicatedKvTransport("test")
  const ready = transport.ready()
  const refused = assert.rejects(ready, TimeoutError)
  await entered.promise
  await transport.reset()
  answer.resolve(supported)
  await refused
  await assert.rejects(transport.ready(), UnsupportedError)
  assert.equal(connects, 2)
})

void test("given_slow_retirement_when_another_request_readies_then_should_wait_before_connecting", async (t) => {
  const retired = deferred<undefined>()
  const closing = deferred<undefined>()
  let connects = 0
  t.mock.method(ApacheIggyTransport, "connect", () => {
    connects += 1
    return Promise.resolve({
      sendManaged: () => Promise.resolve(supported),
      close: () => {
        closing.resolve(undefined)
        return connects === 1 ? retired.promise : Promise.resolve(undefined)
      }
    } as unknown as ApacheIggyTransport)
  })
  await using transport = new DedicatedKvTransport("test")
  await transport.ready()
  const resetting = transport.reset()
  await closing.promise
  const ready = transport.ready()
  await Promise.resolve(undefined)
  assert.equal(connects, 1)
  retired.resolve(undefined)
  await resetting
  await ready
  assert.equal(connects, 2)
})

void test("given_a_closed_dedicated_transport_when_readied_then_should_not_reconnect", async (t) => {
  let connects = 0
  t.mock.method(ApacheIggyTransport, "connect", () => {
    connects += 1
    return Promise.reject(new Error("unexpected connection"))
  })
  const transport = new DedicatedKvTransport("test")
  await transport.close()
  await transport.reset()
  await assert.rejects(
    transport.ready(),
    (error: unknown) => error instanceof TransportError && !error.retryable
  )
  assert.equal(connects, 0)
})

void test("given_an_unsupported_deployment_when_readied_twice_then_should_keep_its_connection", async (t) => {
  let connects = 0
  let probes = 0
  t.mock.method(ApacheIggyTransport, "connect", () => {
    connects += 1
    return Promise.resolve({
      sendManaged: () => {
        probes += 1
        return Promise.resolve(unsupported)
      },
      close: () => Promise.resolve(undefined)
    } as unknown as ApacheIggyTransport)
  })
  await using transport = new DedicatedKvTransport("test")
  await assert.rejects(transport.ready(), UnsupportedError)
  await assert.rejects(transport.ready(), UnsupportedError)
  assert.equal(connects, 1)
  assert.equal(probes, 2)
})

const release = { namespace: "coord", key: new Uint8Array([1]), leaseToken: 7n, holderId: "worker" }

void test("given_a_definite_readiness_failure_when_executed_then_should_preserve_it_before_any_send", async () => {
  const failure = new TransportError("authentication failed", false, { cause: { errorCode: 41 } })
  let sends = 0
  let resets = 0
  const client = new FencedLeaseClient({
    ready: () => Promise.reject(failure),
    send: () => {
      sends += 1
      return Promise.resolve(new Uint8Array())
    },
    reset: () => {
      resets += 1
      return Promise.resolve(undefined)
    }
  })
  await assert.rejects(client.release(client.prepareRelease(release)), (error: unknown) => {
    assert.equal(error, failure)
    assert.equal(isPermissionDenied(error), true)
    return true
  })
  assert.equal(sends, 0)
  assert.equal(resets, 1)
})

void test("given_a_timed_out_mutation_when_reset_is_slow_then_should_wait_for_retirement_before_reporting_ambiguity", async () => {
  const retired = deferred<undefined>()
  const resetting = deferred<undefined>()
  let reported = false
  const client = new FencedLeaseClient({
    send: () => new Promise<Uint8Array>(() => undefined),
    reset: () => {
      resetting.resolve(undefined)
      return retired.promise
    }
  }).withAttemptTimeout(5)
  const failure = assert
    .rejects(client.release(client.prepareRelease(release)), AmbiguousMutationError)
    .then(() => {
      reported = true
    })
  await resetting.promise
  assert.equal(reported, false)
  retired.resolve(undefined)
  await failure
  assert.equal(reported, true)
})

void test("given_a_stale_barriered_read_when_answered_then_should_preserve_the_typed_error_without_reset", async () => {
  let resets = 0
  const required = { topicGeneration: 1n, partition: 0, offset: 3n }
  const client = new FencedLeaseClient({
    send: () =>
      Promise.resolve(
        encodeOne(encodeKvReply({ kind: "err", error: { kind: "stale", required } }))
      ),
    reset: () => {
      resets += 1
      return Promise.resolve(undefined)
    }
  })
  await assert.rejects(
    client.get({ namespace: "coord", key: new Uint8Array([1]), minPosition: required }),
    (error: unknown) => error instanceof KvExecutionError && isStale(error)
  )
  assert.equal(resets, 0)
})

void test("given_a_closed_fenced_client_when_a_prepared_operation_is_reused_then_should_refuse_before_send", async () => {
  let sends = 0
  const client = new FencedLeaseClient({
    send: () => {
      sends += 1
      return Promise.resolve(
        encodeNamed(
          encodeKvReply({
            kind: "ok",
            outcome: { kind: "released", wasHeld: false }
          }) as ReadonlyMap<string, unknown>
        )
      )
    },
    reset: () => Promise.resolve(undefined)
  })
  const operation = client.prepareRelease(release)
  await client.close()
  await assert.rejects(
    client.release(operation),
    (error: unknown) => error instanceof TransportError && !error.retryable
  )
  assert.equal(sends, 0)
})

void test("given_concurrent_close_calls_when_retirement_is_slow_then_should_join_one_terminal_reset", async () => {
  const retired = deferred<undefined>()
  let resets = 0
  const client = new FencedLeaseClient({
    send: () => Promise.resolve(new Uint8Array()),
    reset: () => {
      resets += 1
      return retired.promise
    }
  })
  let finished = false
  const first = client.close()
  const second = client.close().then(() => {
    finished = true
  })
  await Promise.resolve()
  assert.equal(resets, 1)
  assert.equal(finished, false)
  retired.resolve(undefined)
  await first
  await second
  await client.close()
  assert.equal(resets, 1)
})

void test("given_a_transport_with_terminal_close_when_the_client_closes_then_should_close_instead_of_reset", async () => {
  let closed = 0
  let resets = 0
  const client = new FencedLeaseClient({
    send: () => Promise.resolve(new Uint8Array()),
    reset: () => {
      resets += 1
      return Promise.resolve()
    },
    close: () => {
      closed += 1
      return Promise.resolve()
    }
  })
  await client.close()
  await client.close()
  assert.equal(closed, 1)
  assert.equal(resets, 0)
})
