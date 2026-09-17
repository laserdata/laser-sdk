import assert from "node:assert/strict"
import { test } from "node:test"
import { ConfigError, TimeoutError, TransportError } from "../../src/client/errors.js"
import { publishOptions } from "../../src/client/publish-options.js"
import { Laser } from "../../src/client/laser.js"
import { ApacheIggyTransport, type IggyClient } from "../../src/iggy/apache-iggy.js"

type Send = IggyClient["message"]["send"]

async function transport(send: Send, maxRetries = 2, destroy = () => undefined) {
  const client = {
    clientProvider: () => Promise.resolve({}),
    message: { send },
    destroy: () => {
      destroy()
      return Promise.resolve()
    }
  } as unknown as IggyClient
  return ApacheIggyTransport.fromClient(client, "owned", {
    timeoutMs: 20,
    maxRetries,
    retryBackoffMs: 1
  })
}

void test("given_publish_defaults_when_resolved_then_should_allow_remote_latency", () => {
  assert.deepEqual(publishOptions(), { timeoutMs: 60_000, maxRetries: 3, retryBackoffMs: 250 })
})

void test("given_invalid_publish_settings_when_connected_then_should_reject_before_io", () => {
  for (const value of [0, -1, NaN, Infinity, 0x8000_0000]) {
    assert.throws(() => Laser.builder().publishTimeout(value).connect(), ConfigError)
    assert.throws(() => Laser.builder().publishRetryBackoff(value).connect(), ConfigError)
  }
  assert.throws(() => Laser.builder().publishMaxRetries(-1).connect(), ConfigError)
})

void test("given_a_transient_publish_failure_when_retried_then_should_keep_ids_payload_and_routing", async () => {
  const calls: Parameters<Send>[0][] = []
  const confirmation = { streamId: 1, topicId: 2, partitionId: 3, baseOffset: 4n }
  const client = await transport((request) => {
    calls.push(request)
    if (calls.length < 3)
      return Promise.reject(Object.assign(new Error("not accepted"), { errorCode: 58 }))
    return Promise.resolve({ confirmations: [confirmation] })
  })
  const response = await client.sendMessagesWithHeaders(
    "stream",
    "topic",
    [{ payload: new Uint8Array([1]), headers: new Map() }],
    undefined,
    3
  )
  assert.equal(calls.length, 3)
  assert.deepEqual(calls[0], calls[1])
  assert.deepEqual(calls[1], calls[2])
  assert.deepEqual(response.confirmations, [confirmation])
})

void test("given_a_long_outage_when_retries_exhaust_then_should_reject_and_allow_a_later_send", async () => {
  let attempts = 0
  let offline = true
  const client = await transport(() => {
    attempts += 1
    return offline
      ? Promise.reject(Object.assign(new Error("not accepted"), { errorCode: 58 }))
      : Promise.resolve({ confirmations: [] })
  })
  await assert.rejects(
    client.sendMessages("stream", "topic", [new Uint8Array([1])], {
      kind: "partition",
      partition: 0
    }),
    TransportError
  )
  assert.equal(attempts, 3)
  offline = false
  await client.sendMessages("stream", "topic", [new Uint8Array([2])], {
    kind: "partition",
    partition: 0
  })
  assert.equal(attempts, 4)
})

void test("given_a_permanent_server_error_when_publishing_then_should_not_retry", async () => {
  let attempts = 0
  const client = await transport(() => {
    attempts += 1
    return Promise.reject(Object.assign(new Error("unauthorized"), { errorCode: 20 }))
  })
  await assert.rejects(
    client.sendMessages("stream", "topic", [new Uint8Array([1])], {
      kind: "partition",
      partition: 0
    }),
    TransportError
  )
  assert.equal(attempts, 1)
})

void test("given_a_stalled_publish_with_no_retries_when_timed_out_then_should_close_and_reject", async () => {
  let destroyed = 0
  const client = await transport(
    () => new Promise(() => undefined),
    0,
    () => {
      destroyed += 1
    }
  )
  await assert.rejects(
    client.sendMessages("stream", "topic", [new Uint8Array([1])], {
      kind: "partition",
      partition: 0
    }),
    TimeoutError
  )
  assert.equal(destroyed, 1)
})

void test("given_environment_publish_settings_when_overridden_then_should_prefer_explicit_values", () => {
  assert.deepEqual(
    publishOptions(
      { timeoutMs: 90_000 },
      {
        LASER_PUBLISH_TIMEOUT_MS: "invalid",
        LASER_PUBLISH_MAX_RETRIES: "7",
        LASER_PUBLISH_RETRY_BACKOFF_MS: "500"
      }
    ),
    { timeoutMs: 90_000, maxRetries: 7, retryBackoffMs: 500 }
  )
  assert.throws(() => publishOptions({}, { LASER_PUBLISH_TIMEOUT_MS: "invalid" }), ConfigError)
})

void test("given_slow_partition_lookup_when_timed_out_then_should_never_send_late", async () => {
  let sends = 0
  let finishLookup: () => void = () => undefined
  const client = {
    clientProvider: () => Promise.resolve({}),
    topic: {
      get: () =>
        new Promise((resolve) => {
          finishLookup = () => {
            resolve({ partitionsCount: 1 })
          }
        })
    },
    message: {
      send: () => {
        sends += 1
        return Promise.resolve({ confirmations: [] })
      }
    },
    destroy: () => Promise.resolve()
  } as unknown as IggyClient
  const adapted = await ApacheIggyTransport.fromClient(client, "owned", {
    timeoutMs: 10,
    maxRetries: 0
  })
  await assert.rejects(
    adapted.sendMessages("stream", "topic", [new Uint8Array([1])], { kind: "balanced" }),
    TimeoutError
  )
  finishLookup()
  await new Promise((resolve) => setImmediate(resolve))
  assert.equal(sends, 0)
})
