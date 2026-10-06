import assert from "node:assert/strict"
import { test } from "node:test"

import { Laser } from "../../src/client/laser.js"
import { OPEN_CAPABILITIES } from "../../src/client/capabilities.js"
import { isUnsupported, isVersionSkew } from "../../src/client/error-classify.js"
import {
  ProtocolError,
  PublishFailedError,
  QueryExecutionError,
  CheckpointExecutionError
} from "../../src/client/errors.js"
import { requireManagedCommand } from "../../src/client/managed.js"
import type { IggyClient, LaserTransport } from "../../src/iggy/apache-iggy.js"
import { Topic } from "../../src/stream/topic.js"
import { QueryCommand, CheckpointCommand } from "../../src/wire/commands.js"
import { QueryExecutionId } from "../../src/wire/ids.js"
import { encodeQueryReplyFrame, encodeQueryStatusReplyFrame } from "../../src/wire/query.js"

void test("given_a_partial_transport_failure_when_sending_a_topic_batch_then_should_preserve_its_confirmed_prefix_and_exact_tail", async () => {
  const tail = { payload: new Uint8Array([2]), headers: new Map() }
  const committed = [{ streamId: 1, topicId: 2, partitionId: 0, baseOffset: 4n }]
  const failure = new PublishFailedError(
    "fleet",
    "events",
    committed,
    [tail],
    new ProtocolError("publish refused")
  )
  const transport = { sendMessages: () => Promise.reject(failure) } as unknown as LaserTransport
  const topic = Topic.create(transport, "fleet", "events")
  await assert.rejects(topic.batch([new Uint8Array([1]), tail.payload]), (error: unknown) => {
    assert.equal(error, failure)
    assert.deepEqual(failure.committed, committed)
    assert.deepEqual(failure.unconfirmed, [tail])
    return true
  })
})

void test("given_unconfirmed_records_when_batched_again_then_should_resend_them_with_their_ids_and_headers", async () => {
  const sent: { records: readonly unknown[]; key: unknown }[] = []
  const transport = {
    sendMessagesWithHeaders: (
      _stream: string,
      _topic: string,
      records: readonly unknown[],
      key?: unknown
    ) => {
      sent.push({ records, key })
      return Promise.resolve({ confirmations: [] })
    }
  } as unknown as LaserTransport
  const topic = Topic.create(transport, "fleet", "events")
  const unconfirmed = { payload: new Uint8Array([2]), headers: new Map(), id: 42n }
  await topic.batch([new Uint8Array([1]), unconfirmed], { key: new Uint8Array([9]) })
  assert.deepEqual(sent, [
    {
      records: [{ payload: new Uint8Array([1]), headers: new Map() }, unconfirmed],
      key: new Uint8Array([9])
    }
  ])
})

void test("given_a_status_for_another_execution_when_reading_or_cancelling_then_should_reject_the_reply", async () => {
  const bytes = encodeQueryStatusReplyFrame({
    kind: "ok",
    status: {
      executionId: QueryExecutionId.fromU128(10n),
      state: "running",
      startedAtMicros: 1n,
      scannedBytes: 0n,
      producedBytes: 0n,
      rowCount: 0n
    }
  })
  const client = {
    clientProvider: () => Promise.resolve({ protocol: "vsr" }),
    sendBinaryRequest: () => Promise.resolve(bytes),
    destroy: () => Promise.resolve()
  } as unknown as IggyClient
  await using base = await Laser.fromClient(client)
  const laser = base.withCapabilities({
    ...OPEN_CAPABILITIES,
    query: {
      ...OPEN_CAPABILITIES.query,
      available: true,
      executionStatus: true,
      cancellation: true
    }
  })
  const requested = QueryExecutionId.fromU128(9n)
  await assert.rejects(laser.queryStatus(requested), ProtocolError)
  await assert.rejects(laser.cancelQuery(requested), ProtocolError)
})

void test("given_an_unsupported_query_reply_when_fetching_then_should_raise_the_query_error_class", async () => {
  const bytes = encodeQueryReplyFrame({
    kind: "err",
    error: { kind: "unsupported", message: "vector search is not served" }
  })
  const client = {
    clientProvider: () => Promise.resolve({ protocol: "vsr" }),
    sendBinaryRequest: () => Promise.resolve(bytes),
    destroy: () => Promise.resolve()
  } as unknown as IggyClient
  await using base = await Laser.fromClient(client)
  const laser = base.withCapabilities({
    ...OPEN_CAPABILITIES,
    query: { ...OPEN_CAPABILITIES.query, available: true }
  })
  // Rust keeps a server-side refusal as the query error, so the class matches
  // across clients and only the classifier reports it as unsupported.
  await assert.rejects(laser.query("readings").fetch(), (error: unknown) => {
    assert.ok(error instanceof QueryExecutionError)
    assert.equal(isUnsupported(error), true)
    return true
  })
})

void test("given_advertised_version_skew_when_using_query_or_checkpoint_then_should_preserve_the_typed_classifier", () => {
  const capabilities = {
    ...OPEN_CAPABILITIES,
    query: { ...OPEN_CAPABILITIES.query, available: true },
    destinations: { ...OPEN_CAPABILITIES.destinations, available: true },
    versions: {
      query: 99,
      control: 1,
      kv: 1,
      fork: 1,
      agent: 1,
      graph: 1,
      checkpoint: 99,
      filter: 1,
      features: 0n
    }
  }
  assert.throws(
    () => {
      requireManagedCommand(capabilities, QueryCommand)
    },
    (error: unknown) => {
      assert.ok(error instanceof QueryExecutionError)
      assert.equal(isVersionSkew(error), true)
      return true
    }
  )
  assert.throws(
    () => {
      requireManagedCommand(capabilities, CheckpointCommand)
    },
    (error: unknown) => {
      assert.ok(error instanceof CheckpointExecutionError)
      assert.equal(isVersionSkew(error), true)
      return true
    }
  )
})
