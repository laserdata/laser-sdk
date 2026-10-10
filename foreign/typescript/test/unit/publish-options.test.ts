import assert from "node:assert/strict"
import { test } from "node:test"
import {
  ConfigError,
  InvalidError,
  PublishFailedError,
  TimeoutError,
  TransportError
} from "../../src/client/errors.js"
import { publishOptions } from "../../src/client/publish-options.js"
import { Laser } from "../../src/client/laser.js"
import { ApacheIggyTransport, type IggyClient } from "../../src/iggy/apache-iggy.js"
import { isPermissionDenied } from "../../src/client/error-classify.js"
import { ConversationId } from "../../src/types/ids.js"

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

void test("given_invalid_publish_settings_when_connected_then_should_reject_before_io", async () => {
  for (const value of [0, -1, NaN, Infinity, 0x8000_0000]) {
    await assert.rejects(Laser.builder().publishTimeout(value).connect(), ConfigError)
    await assert.rejects(Laser.builder().publishRetryBackoff(value).connect(), ConfigError)
  }
  await assert.rejects(Laser.builder().publishMaxRetries(-1).connect(), ConfigError)
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

void test("given_a_wrapped_server_reply_when_retried_then_should_read_its_exact_transient_code", async () => {
  let attempts = 0
  const client = await transport(() => {
    attempts += 1
    if (attempts === 1)
      return Promise.reject(new Error("wrapped reply", { cause: { errorCode: 58 } }))
    return Promise.resolve({ confirmations: [] })
  })
  await client.sendMessages("stream", "topic", [new Uint8Array([1])], {
    kind: "partition",
    partition: 0
  })
  assert.equal(attempts, 2)
  await client.close()
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
    (error: unknown) =>
      error instanceof PublishFailedError && error.publishCause() instanceof TransportError
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
    return Promise.reject(Object.assign(new Error("unauthorized"), { errorCode: 41 }))
  })
  await assert.rejects(
    client.sendMessages("stream", "topic", [new Uint8Array([1])], {
      kind: "partition",
      partition: 0
    }),
    (error: unknown) =>
      error instanceof PublishFailedError && error.publishCause() instanceof TransportError
  )
  assert.equal(attempts, 1)
})

void test("given_a_missing_topic_when_publishing_then_should_rebuild_the_routing_once", async () => {
  let attempts = 0
  const client = await transport(() => {
    attempts += 1
    return Promise.reject(Object.assign(new Error("topic not found"), { errorCode: 2011 }))
  })
  await assert.rejects(
    client.sendMessages("stream", "topic", [new Uint8Array([1])], {
      kind: "partition",
      partition: 0
    }),
    PublishFailedError
  )
  assert.equal(attempts, 2)
  let recreated = 0
  const healed = await transport(() => {
    recreated += 1
    return recreated === 1
      ? Promise.reject(Object.assign(new Error("stream not found"), { errorCode: 1010 }))
      : Promise.resolve({ confirmations: [] })
  })
  await healed.sendMessages("stream", "topic", [new Uint8Array([1])], {
    kind: "partition",
    partition: 0
  })
  assert.equal(recreated, 2)
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
    (error: unknown) =>
      error instanceof PublishFailedError && error.publishCause() instanceof TimeoutError
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
    (error: unknown) =>
      error instanceof PublishFailedError && error.publishCause() instanceof TimeoutError
  )
  finishLookup()
  await new Promise((resolve) => setImmediate(resolve))
  assert.equal(sends, 0)
})

void test("given_a_transient_partition_reply_when_publishing_then_should_retry_it", async () => {
  for (const errorCode of [3004, 3007, 10002]) {
    let attempts = 0
    const client = await transport(() => {
      attempts += 1
      return attempts === 1
        ? Promise.reject(Object.assign(new Error("partition not ready"), { errorCode }))
        : Promise.resolve({ confirmations: [] })
    })
    await client.sendMessages("stream", "topic", [new Uint8Array([1])], {
      kind: "partition",
      partition: 0
    })
    assert.equal(attempts, 2)
  }
})

void test("given_an_expired_session_on_an_injected_client_when_publishing_then_should_not_retry_without_a_reconnect", async () => {
  let attempts = 0
  const client = await transport(() => {
    attempts += 1
    return Promise.reject(Object.assign(new Error("unauthenticated"), { errorCode: 40 }))
  })
  await assert.rejects(
    client.sendMessages("stream", "topic", [new Uint8Array([1])], {
      kind: "partition",
      partition: 0
    }),
    PublishFailedError
  )
  assert.equal(attempts, 1)
})

void test("given_an_empty_batch_when_publishing_then_should_send_nothing_and_keep_the_connection", async () => {
  let destroyed = 0
  const client = await transport(
    () => Promise.reject(new Error("cannot send an empty message batch")),
    2,
    () => {
      destroyed += 1
    }
  )
  assert.deepEqual(await client.sendMessages("stream", "topic", [], { kind: "balanced" }), {
    confirmations: []
  })
  assert.deepEqual(await client.sendMessagesWithHeaders("stream", "topic", []), {
    confirmations: []
  })
  assert.equal(destroyed, 0)
})

void test("given_a_large_batch_when_a_later_request_fails_then_should_report_the_confirmed_requests_and_the_tail_with_its_ids", async () => {
  const requests: Parameters<Send>[0][] = []
  const confirmation = { streamId: 1, topicId: 2, partitionId: 0, baseOffset: 0n }
  const client = await transport((request) => {
    requests.push(request)
    return requests.length === 1
      ? Promise.resolve({ confirmations: [confirmation] })
      : Promise.reject(Object.assign(new Error("forbidden"), { errorCode: 41 }))
  })
  const records = Array.from({ length: 1_500 }, (_, index) => ({
    payload: new Uint8Array([index % 256]),
    headers: new Map(),
    ...(index === 1_200 ? { id: 77n } : {})
  }))
  await assert.rejects(
    client.sendMessagesWithHeaders("stream", "topic", records, undefined, 0),
    (error: unknown) => {
      assert.ok(error instanceof PublishFailedError)
      assert.deepEqual(error.committed, [confirmation])
      assert.equal(error.unconfirmed.length, 500)
      assert.equal(error.unconfirmed[200]?.id, 77n)
      assert.deepEqual(
        error.unconfirmed.map((record) => record.id),
        requests[1]?.messages.map((message) => message.id)
      )
      return true
    }
  )
  assert.equal(requests.length, 2)
  assert.equal(requests[0]?.messages.length, 1_000)
})

void test("given_an_empty_or_oversized_partition_key_when_publishing_then_should_reject_before_io", async () => {
  const client = await transport(() => Promise.reject(new Error("must not send")))
  await assert.rejects(
    client.sendMessagesWithHeaders(
      "s",
      "t",
      [{ payload: new Uint8Array([1]), headers: new Map() }],
      ""
    ),
    InvalidError
  )
  await assert.rejects(
    client.sendMessagesWithHeaders(
      "s",
      "t",
      [{ payload: new Uint8Array([1]), headers: new Map() }],
      new Uint8Array(256)
    ),
    InvalidError
  )
})

void test("given_a_refused_agent_send_when_published_then_should_report_a_publish_failure_with_its_record", async () => {
  const client = {
    clientProvider: () => Promise.resolve({}),
    topic: { get: () => Promise.resolve({ partitionsCount: 1 }) },
    message: {
      send: () => Promise.reject(Object.assign(new Error("forbidden"), { errorCode: 41 }))
    },
    destroy: () => Promise.resolve()
  } as unknown as IggyClient
  await using laser = (await Laser.fromClient(client)).withDefaultStream("fleet")
  await assert.rejects(
    laser.sendAgent("commands", new Uint8Array([1]), { conversationId: ConversationId.new() }),
    (error: unknown) => {
      assert.ok(error instanceof PublishFailedError)
      assert.equal(error.stream, "fleet")
      assert.equal(error.topic, "commands")
      assert.equal(error.unconfirmed.length, 1)
      assert.ok(isPermissionDenied(error))
      return true
    }
  )
})

void test("given_background_retry_timing_when_a_server_refuses_then_should_resend_at_once_then_at_the_fixed_interval", async () => {
  const at: number[] = []
  const client = await transport(() => {
    at.push(Date.now())
    return Promise.reject(Object.assign(new Error("refused"), { errorCode: 41 }))
  }, 3)
  await assert.rejects(
    client.sendMessagesWithHeaders(
      "stream",
      "topic",
      [{ payload: new Uint8Array([1]), headers: new Map() }],
      undefined,
      0,
      { maxRetries: 3, fixedRetryIntervalMs: 40 }
    ),
    PublishFailedError
  )
  assert.equal(at.length, 4)
  const gaps = at.slice(1).map((time, index) => time - (at[index] ?? time))
  assert.ok((gaps[0] ?? 0) < 20, `first resend waited ${String(gaps[0])} ms`)
  for (const gap of gaps.slice(1)) assert.ok(gap >= 35 && gap < 90, `resend gap ${String(gap)} ms`)
})

void test("given_background_retry_timing_when_the_confirmation_is_lost_then_should_not_resend", async () => {
  let attempts = 0
  const client = await transport(() => {
    attempts += 1
    return Promise.reject(Object.assign(new Error("bad bytes"), { errorCode: 303 }))
  }, 3)
  await assert.rejects(
    client.sendMessagesWithHeaders(
      "stream",
      "topic",
      [{ payload: new Uint8Array([1]), headers: new Map() }],
      undefined,
      0,
      { maxRetries: 3, fixedRetryIntervalMs: 1 }
    ),
    PublishFailedError
  )
  assert.equal(attempts, 1)
})
