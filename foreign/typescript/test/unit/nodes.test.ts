import assert from "node:assert/strict"
import { test } from "node:test"
import type { Client as IggyClient } from "apache-iggy"
import { AmbiguousMutationError, TransportError } from "../../src/client/errors.js"
import { encodeNamed } from "../../src/wire/cbor.js"
import {
  ApacheIggyTransport,
  Nodes,
  managedTargetOf,
  pickNode
} from "../../src/iggy/apache-iggy.js"
import {
  AGDX_AUTHZ_BIND_ROLES_CODE,
  AGDX_AUTHZ_DEFINE_ROLE_CODE,
  AGDX_AUTHZ_DELETE_ROLE_CODE,
  AGDX_AUTHZ_WHOAMI_CODE,
  AGDX_KV_GET_CODE,
  AGDX_QUERY_CODE
} from "../../src/wire/codes.js"

const GET_STREAM_CODE = 200
const TRANSIENT_NOT_COMMITTED = 57
const TRANSIENT_NOT_ACCEPTED = 58
const INVALID_IDENTIFIER = 6
const OK = new Uint8Array([0xa1, 0x62, 0x6f, 0x6b, 0xf6])

interface Sent {
  readonly code: number
  readonly replica?: number
}

// A client of a cluster whose replicas `fail` answers, recording every request.
function cluster(
  replicas: readonly number[],
  fail: (replica: number, attempt: number) => Error | Uint8Array | undefined = () => undefined
): { readonly client: IggyClient; readonly sent: Sent[]; readonly rosterReads: () => number } {
  const sent: Sent[] = []
  let rosterReads = 0
  let attempt = 0
  const toReplica = (code: number, replica: number): Promise<Buffer> => {
    sent.push({ code, replica })
    const failure = fail(replica, attempt)
    attempt += 1
    if (failure instanceof Error) return Promise.reject(failure)
    return Promise.resolve(Buffer.from(failure ?? OK))
  }
  const client = {
    clientProvider: () =>
      Promise.resolve({
        sendCommand: async (
          code: number,
          _payload: Buffer,
          options: { readonly replica?: number }
        ) => {
          const data = await toReplica(code, options.replica ?? 0)
          return { length: data.length, data }
        }
      }),
    sendBinaryRequest: (code: number) => {
      sent.push({ code })
      return Promise.resolve(Buffer.from(OK))
    },
    sendBinaryRequestToReplica: (code: number, _payload: Buffer, replica: number) =>
      toReplica(code, replica),
    cluster: {
      getClusterMetadata: () => {
        rosterReads += 1
        return Promise.resolve({
          name: "orbit",
          nodes: replicas.map((replicaId) => ({ replicaId, name: `node-${String(replicaId)}` }))
        })
      }
    },
    destroy: () => Promise.resolve()
  } as unknown as IggyClient
  return { client, sent, rosterReads: () => rosterReads }
}

function refusal(errorCode: number): Error {
  return Object.assign(new Error(`refused with ${String(errorCode)}`), { errorCode })
}

void test("given_a_roster_when_picking_then_should_keep_one_node_per_caller_key", () => {
  assert.equal(pickNode([0, 1, 2], 4n, 0n), 1)
  assert.equal(pickNode([0, 1, 2], 4n, 0n), 1, "the same key keeps its node")
  assert.equal(pickNode([0, 1, 2], 5n, 0n), 2, "another key spreads to another node")
  assert.equal(pickNode([], 4n, 0n), undefined)
})

void test("given_failed_links_when_picking_then_should_move_to_the_next_node_and_wrap", () => {
  assert.equal(pickNode([3, 5, 9], 2n, 0n), 9)
  assert.equal(pickNode([3, 5, 9], 2n, 1n), 3)
  assert.equal(
    pickNode([3, 5, 9], (1n << 64n) - 1n, 1n),
    3,
    "the key wraps at 64 bits instead of growing"
  )
})

void test("given_authorization_writes_and_standard_commands_when_targeted_then_should_reach_the_metadata_primary", () => {
  for (const code of [
    AGDX_AUTHZ_BIND_ROLES_CODE,
    AGDX_AUTHZ_DEFINE_ROLE_CODE,
    AGDX_AUTHZ_DELETE_ROLE_CODE,
    GET_STREAM_CODE
  ])
    assert.deepEqual(managedTargetOf(code), { kind: "metadata_primary" }, String(code))
  for (const code of [AGDX_AUTHZ_WHOAMI_CODE, AGDX_KV_GET_CODE, AGDX_QUERY_CODE])
    assert.deepEqual(managedTargetOf(code), { kind: "caller" }, String(code))
})

void test("given_managed_requests_when_sent_then_should_spread_by_caller_and_keep_authorization_writes_on_the_primary", async () => {
  const { client, sent, rosterReads } = cluster([2, 0, 1])
  const transport = await ApacheIggyTransport.fromClient(client)
  await transport.sendManaged(AGDX_KV_GET_CODE, new Uint8Array([1]))
  await transport.sendManaged(AGDX_AUTHZ_BIND_ROLES_CODE, new Uint8Array([2]))
  await transport.sendManaged(GET_STREAM_CODE, new Uint8Array([3]))
  await transport.sendManaged(AGDX_KV_GET_CODE, new Uint8Array([4]))
  const replica = sent[0]?.replica
  assert.ok(replica !== undefined && [0, 1, 2].includes(replica))
  assert.deepEqual(sent, [
    { code: AGDX_KV_GET_CODE, replica },
    { code: AGDX_AUTHZ_BIND_ROLES_CODE },
    { code: GET_STREAM_CODE },
    { code: AGDX_KV_GET_CODE, replica }
  ])
  assert.equal(rosterReads(), 1, "the roster is read once")
})

void test("given_a_failed_link_when_a_managed_read_is_sent_then_should_move_to_the_next_node_and_stay_there", async () => {
  const lost = new Set<number>()
  const { client, sent } = cluster([0, 1, 2], (replica, attempt) => {
    if (attempt === 0) lost.add(replica)
    return lost.has(replica) ? refusal(TRANSIENT_NOT_ACCEPTED) : undefined
  })
  const transport = await ApacheIggyTransport.fromClient(client)
  await transport.sendManaged(AGDX_KV_GET_CODE, new Uint8Array([1]))
  const [first, second] = sent
  assert.ok(first?.replica !== undefined && second?.replica !== undefined)
  assert.equal(second.replica, (first.replica + 1) % 3, "the next roster node")
  await transport.sendManaged(AGDX_KV_GET_CODE, new Uint8Array([2]))
  assert.equal(sent[2]?.replica, second.replica, "later requests keep the new node")
})

void test("given_an_unavailable_backend_when_sending_then_should_repeat_the_frame_on_the_next_node", async () => {
  const down = encodeNamed(
    new Map([
      ["code", "backend_unavailable"],
      ["message", "down"]
    ])
  )
  const { client, sent } = cluster([0, 1, 2], (_replica, attempt) =>
    attempt === 0 ? down : undefined
  )
  const nodes = new Nodes(0n)
  const frame = new Uint8Array([1, 2, 3])
  const frames: Uint8Array[] = []
  const send = client.sendBinaryRequestToReplica.bind(client)
  client.sendBinaryRequestToReplica = (code, payload, replica) => {
    frames.push(new Uint8Array(payload))
    return send(code, payload, replica)
  }
  const answer = await nodes.send((operation) => operation(client), AGDX_KV_GET_CODE, frame, {
    kind: "caller"
  })
  assert.equal(answer.replica, 1)
  assert.deepEqual(frames, [frame, frame])
  await nodes.send((operation) => operation(client), AGDX_KV_GET_CODE, frame, { kind: "caller" })
  assert.deepEqual(
    sent.map((request) => request.replica),
    [0, 1, 1]
  )
})

void test("given_an_expired_attempt_deadline_when_a_caller_link_fails_then_should_leave_the_next_node_to_the_next_attempt", async () => {
  const { client, sent } = cluster([0, 1, 2], () => new TransportError("the link dropped", true))
  const nodes = new Nodes(0n)
  await assert.rejects(
    nodes.send(
      (operation) => operation(client),
      AGDX_KV_GET_CODE,
      OK,
      { kind: "caller" },
      Date.now() - 1
    )
  )
  assert.deepEqual(
    sent.map((request) => request.replica),
    [0],
    "the expired attempt stops on its first node"
  )
  await assert.rejects(
    nodes.send(
      (operation) => operation(client),
      AGDX_KV_GET_CODE,
      OK,
      { kind: "caller" },
      Date.now() - 1
    )
  )
  assert.deepEqual(
    sent.map((request) => request.replica),
    [0, 1],
    "the next attempt starts on the next node"
  )
})

void test("given_every_backend_unavailable_when_sending_then_should_return_the_last_reply_after_one_attempt_per_node", async () => {
  const down = encodeNamed(
    new Map([
      ["code", "backend_unavailable"],
      ["message", "down"]
    ])
  )
  const { client, sent } = cluster([0, 1, 2], () => down)
  const nodes = new Nodes(0n)
  const answer = await nodes.send((operation) => operation(client), AGDX_KV_GET_CODE, OK, {
    kind: "caller"
  })
  assert.deepEqual(answer.reply, down)
  assert.deepEqual(
    sent.map((request) => request.replica),
    [0, 1, 2]
  )
})

void test("given_pending_or_unavailable_when_sending_then_should_keep_the_caller_node", async () => {
  for (const code of ["pending", "unavailable", "forbidden"]) {
    const reply = encodeNamed(
      new Map([
        ["code", code],
        ["message", code]
      ])
    )
    const { client, sent } = cluster([0, 1, 2], () => reply)
    const nodes = new Nodes(0n)
    assert.deepEqual(
      (await nodes.send((operation) => operation(client), AGDX_KV_GET_CODE, OK, { kind: "caller" }))
        .reply,
      reply
    )
    assert.equal(sent.length, 1)
  }
})

void test("given_every_node_lost_when_a_managed_read_is_sent_then_should_try_each_once_and_fail", async () => {
  const { client, sent } = cluster([0, 1, 2], () => refusal(TRANSIENT_NOT_ACCEPTED))
  const transport = await ApacheIggyTransport.fromClient(client)
  await assert.rejects(transport.sendManaged(AGDX_KV_GET_CODE, new Uint8Array([1])), TransportError)
  assert.deepEqual(
    [...new Set(sent.map((request) => request.replica))].sort(),
    [0, 1, 2],
    "each node is tried once"
  )
  assert.equal(sent.length, 3)
})

void test("given_a_replica_the_roster_no_longer_names_when_sent_then_should_read_the_roster_again_once", async () => {
  const { client, sent, rosterReads } = cluster([0], (_replica, attempt) =>
    attempt === 0 ? refusal(INVALID_IDENTIFIER) : undefined
  )
  const transport = await ApacheIggyTransport.fromClient(client)
  await transport.sendManaged(AGDX_KV_GET_CODE, new Uint8Array([1]))
  assert.equal(rosterReads(), 2)
  assert.equal(sent.length, 2)
})

void test("given_a_refusal_that_is_not_transient_when_sent_then_should_not_move_the_request", async () => {
  const { client, sent } = cluster([0, 1, 2], () => refusal(1009))
  const transport = await ApacheIggyTransport.fromClient(client)
  await assert.rejects(transport.sendManaged(AGDX_KV_GET_CODE, new Uint8Array([1])))
  assert.equal(sent.length, 1)
})

void test("given_a_named_replica_when_sent_then_should_reach_that_replica_only", async () => {
  const { client, sent } = cluster([0, 1, 2], () => refusal(TRANSIENT_NOT_COMMITTED))
  const transport = await ApacheIggyTransport.fromClient(client)
  await assert.rejects(
    transport.sendManagedTo(AGDX_QUERY_CODE, new Uint8Array([1]), { kind: "replica", replica: 2 }),
    TransportError
  )
  assert.deepEqual(sent, [{ code: AGDX_QUERY_CODE, replica: 2 }])
})

void test("given_a_driver_that_wrote_the_frame_when_the_next_node_stops_before_writing_then_should_mark_the_frame_written", async () => {
  const { client } = cluster([0, 1], (_replica, attempt) =>
    attempt === 0
      ? new AmbiguousMutationError("managed command failed: outcome is unknown")
      : new TransportError("transport is closed", false)
  )
  const written = { written: false }
  await assert.rejects(
    new Nodes(0n).send(
      (operation) => operation(client),
      AGDX_KV_GET_CODE,
      OK,
      { kind: "caller" },
      undefined,
      written
    ),
    (error: unknown) => error instanceof TransportError && !error.retryable
  )
  assert.equal(written.written, true, "the first node wrote the frame before the transport closed")
})

void test("given_a_driver_that_stops_before_writing_when_walked_then_should_leave_the_frame_unwritten", async () => {
  const { client } = cluster([0, 1], () => new TransportError("transport is closed", false))
  const written = { written: false }
  await assert.rejects(
    new Nodes(0n).send(
      (operation) => operation(client),
      AGDX_KV_GET_CODE,
      OK,
      { kind: "caller" },
      undefined,
      written
    ),
    TransportError
  )
  assert.equal(written.written, false)
})
