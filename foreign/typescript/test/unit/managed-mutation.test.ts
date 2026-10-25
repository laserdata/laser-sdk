import assert from "node:assert/strict"
import { test } from "node:test"
import type { Client as IggyClient } from "apache-iggy"
import { consumerBackoff } from "../../src/agent/reliable-consumer.js"
import { AmbiguousMutationError } from "../../src/client/errors.js"
import {
  ApacheIggyTransport,
  openPositioned,
  pendingReplyMessage
} from "../../src/iggy/apache-iggy.js"
import {
  MANAGED_REQUEST_VERSION,
  decodeManagedReadEnvelope,
  encodeManagedReplyEnvelope,
  positionSurfaceOf
} from "../../src/wire/mutation.js"
import { encodeNamed, expectMap } from "../../src/wire/cbor.js"
import { AGDX_KV_GET_CODE, AGDX_KV_SET_CODE, AGDX_SESSION_GET_CODE } from "../../src/wire/codes.js"
import { decodeOne } from "../../src/wire/cbor.js"
import {
  resultCodeFromCode,
  resultCodeHttpStatus,
  resultCodeIsPending,
  resultCodeIsBackendUnavailable
} from "../../src/wire/result.js"

const PENDING = encodeNamed(
  new Map<string, unknown>([
    ["code", "pending"],
    ["message", "mutation did not fold within 10s, repeat it with the same operation identity"]
  ])
)
const UNAVAILABLE = encodeNamed(
  new Map<string, unknown>([
    ["code", "backend_unavailable"],
    ["message", "the managed backend on this node is unavailable, the request may still apply"]
  ])
)
const CONFLICT = encodeNamed(
  new Map<string, unknown>([
    ["code", "conflict"],
    ["message", "taken"]
  ])
)
const APPLIED = new Uint8Array([0xa1, 0x62, 0x6f, 0x6b, 0xf6])

// A one-node client: every managed request reaches replica 0.
function fakeClient(replies: Uint8Array[], requests: Uint8Array[]): IggyClient {
  return {
    clientProvider: () => Promise.resolve({}),
    sendBinaryRequestToReplica: (_code: number, payload: Buffer, replica: number) => {
      assert.equal(replica, 0)
      requests.push(new Uint8Array(payload))
      const reply = replies.length > 1 ? replies.shift() : replies[0]
      return Promise.resolve(Buffer.from(reply ?? APPLIED))
    },
    cluster: {
      getClusterMetadata: () =>
        Promise.resolve({ name: "single", nodes: [{ replicaId: 0, name: "node" }] })
    },
    destroy: () => Promise.resolve()
  } as unknown as IggyClient
}

void test("given_a_pending_code_when_classified_then_should_match_the_shared_dictionary", () => {
  const pending = resultCodeFromCode(19)
  assert.deepEqual(pending, { kind: "known", name: "Pending" })
  assert.equal(resultCodeHttpStatus(pending), 503)
  assert.equal(resultCodeIsPending(pending), true)
  assert.equal(resultCodeIsPending(resultCodeFromCode(12)), false)
  assert.deepEqual(resultCodeFromCode(20), { kind: "known", name: "BackendUnavailable" })
  assert.equal(resultCodeIsBackendUnavailable(resultCodeFromCode(20)), true)
  assert.equal(resultCodeIsBackendUnavailable(resultCodeFromCode(12)), false)
  assert.deepEqual(resultCodeFromCode(21), { kind: "unrecognized", code: 21 })
})

void test("given_pending_replies_when_a_mutation_is_sent_then_should_repeat_the_same_identity_until_applied", async () => {
  const requests: Uint8Array[] = []
  const transport = await ApacheIggyTransport.fromClient(
    fakeClient([PENDING, PENDING, APPLIED], requests)
  )
  const reply = await transport.sendManaged(AGDX_KV_SET_CODE, new Uint8Array([1, 2, 3]))
  assert.deepEqual(reply, APPLIED)
  assert.equal(requests.length, 3)
  assert.deepEqual(requests[1], requests[0], "a repeat carries the same frame and identity")
  assert.deepEqual(requests[2], requests[0])
  assert.equal(pendingReplyMessage(APPLIED), undefined)
})

void test("given_a_mutation_that_stays_pending_when_the_deadline_passes_then_should_report_an_ambiguous_mutation", async () => {
  const requests: Uint8Array[] = []
  const transport = await ApacheIggyTransport.fromClient(fakeClient([PENDING], requests))
  await assert.rejects(
    transport.sendManaged(AGDX_KV_SET_CODE, new Uint8Array([1]), { mutationDeadlineMs: 300 }),
    (error: unknown) =>
      error instanceof AmbiguousMutationError && error.message.includes("outcome is still unknown")
  )
  assert.ok(requests.length >= 1)
  for (const request of requests) assert.deepEqual(request, requests[0])
})

void test("given_an_unavailable_backend_when_a_mutation_is_sent_then_should_repeat_the_same_identity", async () => {
  const requests: Uint8Array[] = []
  const transport = await ApacheIggyTransport.fromClient(
    fakeClient([UNAVAILABLE, APPLIED], requests)
  )
  const reply = await transport.sendManaged(AGDX_KV_SET_CODE, new Uint8Array([4]))
  assert.deepEqual(reply, APPLIED)
  assert.equal(requests.length, 2)
  assert.deepEqual(requests[1], requests[0])
  assert.equal(pendingReplyMessage(CONFLICT), undefined)
})

void test("given_positioned_replies_when_reads_follow_then_should_send_the_highest_position_seen", async () => {
  const position = (offset: bigint) => ({ topicGeneration: 7n, partition: 0, offset })
  const positioned = (offset: bigint, reply: Uint8Array) =>
    encodeNamed(
      encodeManagedReplyEnvelope({
        v: MANAGED_REQUEST_VERSION,
        frontier: { kv: position(offset) },
        reply
      })
    )
  const requests: Uint8Array[] = []
  const transport = await ApacheIggyTransport.fromClient(
    fakeClient([positioned(9n, APPLIED), positioned(4n, APPLIED), APPLIED], requests)
  )
  assert.deepEqual(
    await transport.sendManaged(AGDX_KV_SET_CODE, new Uint8Array([1])),
    APPLIED,
    "the caller gets the surface reply"
  )
  const read = encodeNamed(new Map<string, unknown>([["v", 1n]]))
  assert.deepEqual(await transport.sendManaged(AGDX_KV_GET_CODE, read), APPLIED)
  assert.deepEqual(transport.seenPositions().kv, position(9n), "an older answer never lowers it")
  await transport.sendManaged(AGDX_KV_GET_CODE, read)
  const sent = decodeManagedReadEnvelope(
    expectMap(decodeOne(requests[2] ?? new Uint8Array(), "read"), "read"),
    "read"
  )
  assert.deepEqual(sent.read, read)
  assert.deepEqual(sent.minPosition, position(9n))
  assert.equal(sent.logHead, undefined)
  assert.equal(positionSurfaceOf(AGDX_SESSION_GET_CODE), undefined)
  assert.deepEqual(openPositioned(UNAVAILABLE), { reply: UNAVAILABLE })
})

void test("given_a_session_reply_with_positions_when_a_session_read_follows_then_should_send_them", async () => {
  const session = {
    streamId: 2,
    streamGeneration: 5n,
    topic: "agent.sessions",
    topicGeneration: 6n,
    partitionId: 0,
    offset: 12n
  }
  const requests: Uint8Array[] = []
  const reply = encodeNamed(
    encodeManagedReplyEnvelope({
      v: MANAGED_REQUEST_VERSION,
      frontier: { sessions: [session] },
      reply: APPLIED
    })
  )
  const transport = await ApacheIggyTransport.fromClient(fakeClient([reply, APPLIED], requests))
  const read = encodeNamed(new Map<string, unknown>([["v", 1n]]))
  assert.deepEqual(await transport.sendManaged(AGDX_SESSION_GET_CODE, read), APPLIED)
  assert.deepEqual(transport.seenPositions().sessions, [session])
  await transport.sendManaged(AGDX_SESSION_GET_CODE, read)
  const first = decodeManagedReadEnvelope(
    expectMap(decodeOne(requests[0] ?? new Uint8Array(), "read"), "read"),
    "read"
  )
  assert.equal(first.minSessions, undefined, "nothing seen before the first read")
  const sent = decodeManagedReadEnvelope(
    expectMap(decodeOne(requests[1] ?? new Uint8Array(), "read"), "read"),
    "read"
  )
  assert.deepEqual(sent.minSessions, [session])
  assert.equal(sent.minPosition, undefined)
})

void test("given_consumer_failures_when_backing_off_then_should_jitter_within_the_upper_half", () => {
  for (const [attempt, ceiling] of [
    [1, 50],
    [3, 200],
    [10, 1_000]
  ] as const) {
    for (let index = 0; index < 64; index += 1) {
      const delay = consumerBackoff(attempt)
      assert.ok(
        delay <= ceiling && delay >= ceiling / 2,
        `attempt ${String(attempt)}: ${String(delay)}`
      )
    }
  }
})
