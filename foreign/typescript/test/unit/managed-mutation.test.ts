import assert from "node:assert/strict"
import { test } from "node:test"
import type { Client as IggyClient } from "apache-iggy"
import { consumerBackoff } from "../../src/agent/reliable-consumer.js"
import { AmbiguousMutationError } from "../../src/client/errors.js"
import {
  ApacheIggyTransport,
  barrierBit,
  managedCandidates,
  openPositioned,
  pendingReplyMessage
} from "../../src/iggy/apache-iggy.js"
import {
  MANAGED_REQUEST_VERSION,
  decodeManagedReadEnvelope,
  encodeManagedReplyEnvelope,
  positionSurfaceOf
} from "../../src/wire/mutation.js"
import { encodeBackendAnnounce, newOpVersions } from "../../src/wire/hello.js"
import { encodeNamed, expectMap } from "../../src/wire/cbor.js"
import {
  AGDX_HELLO_CODE,
  AGDX_KV_GET_CODE,
  AGDX_KV_SET_CODE,
  AGDX_SESSION_GET_CODE,
  isBarrierRead
} from "../../src/wire/codes.js"
import { decodeOne } from "../../src/wire/cbor.js"
import {
  resultCodeFromCode,
  resultCodeHttpStatus,
  resultCodeIsPending,
  resultCodeIsPlaneUnavailable
} from "../../src/wire/result.js"

const PENDING = encodeNamed(
  new Map<string, unknown>([
    ["code", "pending"],
    ["message", "mutation did not fold within 10s, repeat it with the same operation identity"]
  ])
)
const UNAVAILABLE = encodeNamed(
  new Map<string, unknown>([
    ["code", "plane_unavailable"],
    ["message", "the managed plane on this node is unavailable, the request may still apply"]
  ])
)
const CONFLICT = encodeNamed(
  new Map<string, unknown>([
    ["code", "conflict"],
    ["message", "taken"]
  ])
)
const APPLIED = new Uint8Array([0xa1, 0x62, 0x6f, 0x6b, 0xf6])

function fakeClient(replies: Uint8Array[], requests: Uint8Array[]): IggyClient {
  return {
    clientProvider: () => Promise.resolve({}),
    sendBinaryRequest: (_code: number, payload: Buffer) => {
      requests.push(new Uint8Array(payload))
      const reply = replies.length > 1 ? replies.shift() : replies[0]
      return Promise.resolve(Buffer.from(reply ?? APPLIED))
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
  assert.deepEqual(resultCodeFromCode(20), { kind: "known", name: "PlaneUnavailable" })
  assert.equal(resultCodeIsPlaneUnavailable(resultCodeFromCode(20)), true)
  assert.equal(resultCodeIsPlaneUnavailable(resultCodeFromCode(12)), false)
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

void test("given_an_unavailable_plane_when_a_mutation_is_sent_then_should_repeat_the_same_identity", async () => {
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

type Node = Parameters<typeof managedCandidates>[0][number]

function rosterNode(name: string, role: "Leader" | "Follower", status: string): Node {
  return {
    name,
    ip: "10.0.0.1",
    endpoints: { tcp: 8090, quic: 8080, http: 8070, websocket: 8060 },
    role,
    status
  } as Node
}

const ROSTER = [
  rosterNode("a", "Leader", "Healthy"),
  rosterNode("b", "Follower", "Healthy"),
  rosterNode("c", "Follower", "Unreachable"),
  rosterNode("d", "Follower", "Healthy")
]

void test("given_a_failed_shared_connection_when_choosing_nodes_then_should_skip_the_leader_and_unreachable_nodes", () => {
  assert.deepEqual(
    managedCandidates(ROSTER, undefined).map((node) => node.name),
    ["b", "d"]
  )
  assert.deepEqual(
    managedCandidates(ROSTER, "b").map((node) => node.name),
    ["a", "d"]
  )
})

void test("given_a_leader_whose_plane_is_down_when_a_mutation_is_sent_then_should_move_it_with_the_same_identity", async () => {
  const shared: Uint8Array[] = []
  const moved: Uint8Array[] = []
  const client = Object.assign(fakeClient([UNAVAILABLE], shared), {
    cluster: { getClusterMetadata: () => Promise.resolve({ name: "cluster", nodes: ROSTER }) }
  })
  const transport = await ApacheIggyTransport.fromClient(client)
  const internals = transport as unknown as {
    connection: unknown
    openNodeConnection: (ip: string, port: number) => Promise<unknown>
  }
  internals.connection = { host: "10.0.0.1", port: 8090 }
  const announce = encodeBackendAnnounce({
    versions: newOpVersions(1, 1, 1, 1),
    ready: true,
    backends: []
  })
  internals.openNodeConnection = () =>
    Promise.resolve({
      send: (code: number, payload: Uint8Array) => {
        if (code === AGDX_HELLO_CODE) return Promise.resolve(announce)
        moved.push(payload)
        return Promise.resolve(APPLIED)
      },
      close: () => Promise.resolve()
    })
  const reply = await transport.sendManaged(AGDX_KV_SET_CODE, new Uint8Array([9]))
  assert.deepEqual(reply, APPLIED)
  assert.equal(shared.length, 1)
  assert.equal(moved.length, 1)
  assert.deepEqual(moved[0], shared[0], "the moved request keeps its frame and identity")

  // The first read after the move waits for the new node's fold to reach the
  // log head. Once one is served, reads stop waiting for the head.
  const read = encodeNamed(new Map<string, unknown>([["v", 1n]]))
  await transport.sendManaged(AGDX_KV_GET_CODE, read)
  const barriered = decodeOne(moved[1] ?? new Uint8Array(), "ManagedReadEnvelope")
  assert.ok(barriered instanceof Map)
  assert.deepEqual([...barriered.keys()], ["v", "read", "log_head"])
  assert.deepEqual(barriered.get("read"), read)
  await transport.sendManaged(AGDX_KV_GET_CODE, read)
  const next = decodeOne(moved[2] ?? new Uint8Array(), "ManagedReadEnvelope")
  assert.ok(next instanceof Map)
  assert.deepEqual([...next.keys()], ["v", "read"])
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

void test("given_barrier_codes_when_classified_then_only_reads_of_folded_surfaces_should_carry_a_bit", () => {
  assert.equal(isBarrierRead(AGDX_KV_GET_CODE), true)
  assert.notEqual(barrierBit(AGDX_KV_GET_CODE), 0)
  assert.equal(isBarrierRead(AGDX_KV_SET_CODE), false)
  assert.equal(barrierBit(AGDX_KV_SET_CODE), 0)
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
