import assert from "node:assert/strict"
import { test } from "node:test"
import type { Client as IggyClient } from "apache-iggy"
import { AmbiguousMutationError, CancelledError, TransportError } from "../../src/client/errors.js"
import { decodeOne, encodeNamed } from "../../src/wire/cbor.js"
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
  AGDX_FILTER_MUTATE_CODE,
  AGDX_FILTERED_POLL_CODE,
  AGDX_GET_FILTER_BINDING_CODE,
  AGDX_KV_GET_CODE,
  AGDX_KV_SET_CODE,
  AGDX_QUERY_CODE
} from "../../src/wire/codes.js"
import { encodeFilterCatalogReply } from "../../src/wire/filter.js"

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

function backendUnavailableReply(message: string): Uint8Array {
  return encodeNamed(
    new Map([
      ["code", "backend_unavailable"],
      ["message", message]
    ])
  )
}

// The refusal the streaming server gives a filter catalog read when it cannot
// reach its managed backend.
function catalogUnavailableReply(): Uint8Array {
  return encodeNamed(
    encodeFilterCatalogReply({
      kind: "err",
      error: {
        code: { kind: "known", name: "Unavailable" },
        reason: "catalog_unavailable",
        message: "the managed backend did not answer"
      }
    })
  )
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

void test("given_one_node_that_refuses_then_serves_when_reading_then_should_return_the_served_reply", async () => {
  const { client, sent } = cluster([0], (_replica, attempt) =>
    attempt === 0 ? backendUnavailableReply("replaying its logs") : undefined
  )
  const answer = await new Nodes(0n).read(
    (operation) => operation(client),
    AGDX_KV_GET_CODE,
    OK,
    { kind: "caller" },
    { kind: "until", deadline: Date.now() + 5_000 }
  )
  assert.deepEqual(answer.reply, OK)
  assert.equal(sent.length, 2, "the refused node is asked again")
})

void test("given_every_node_refusing_past_the_deadline_when_reading_then_should_return_the_last_refusal", async () => {
  const { client, sent } = cluster([0, 1], (_replica, attempt) =>
    backendUnavailableReply(`refusal ${String(attempt)}`)
  )
  const started = Date.now()
  const answer = await new Nodes(0n).read(
    (operation) => operation(client),
    AGDX_KV_GET_CODE,
    OK,
    { kind: "caller" },
    { kind: "until", deadline: started + 300 }
  )
  const last = decodeOne(answer.reply, "CommandError")
  assert.ok(last instanceof Map)
  assert.equal(last.get("code"), "backend_unavailable")
  assert.ok(sent.length > 2, "the roster is walked again before the deadline")
  assert.equal(last.get("message"), `refusal ${String(sent.length - 1)}`, "the last refusal")
  assert.ok(Date.now() - started < 2_000, "the deadline ends the wait")
})

void test("given_a_retry_timer_that_wakes_after_the_read_deadline_when_resumed_then_should_return_the_last_refusal", async (context) => {
  let now = 0
  const deadline = 250
  context.mock.method(Date, "now", () => now)
  const refused = backendUnavailableReply("replaying its logs")
  const { client, sent } = cluster([0], (_replica, attempt) => {
    if (attempt > 0) return OK
    setTimeout(() => {
      now = deadline
    }, 0)
    return refused
  })

  const answer = await new Nodes(0n).read(
    (operation) => operation(client),
    AGDX_KV_GET_CODE,
    OK,
    { kind: "caller" },
    { kind: "until", deadline }
  )

  assert.deepEqual(answer.reply, refused)
  assert.equal(sent.length, 1)
})

void test("given_a_wait_that_would_end_at_the_read_deadline_when_deciding_then_should_not_walk_again", async (context) => {
  context.mock.method(Date, "now", () => 0)
  context.mock.method(Math, "random", () => 0)
  const refused = backendUnavailableReply("replaying its logs")
  for (const deadline of [50, 51]) {
    const { client, sent } = cluster([0], (_replica, attempt) => (attempt === 0 ? refused : OK))
    const answer = await new Nodes(0n).read(
      (operation) => operation(client),
      AGDX_KV_GET_CODE,
      OK,
      { kind: "caller" },
      { kind: "until", deadline }
    )
    assert.deepEqual(answer.reply, deadline === 50 ? refused : OK)
    assert.equal(sent.length, deadline === 50 ? 1 : 2)
  }
})

void test("given_every_node_refusing_when_reading_then_should_back_off_longer_before_each_walk", async () => {
  const { client, sent } = cluster([0], () => backendUnavailableReply("down"))
  await new Nodes(0n).read(
    (operation) => operation(client),
    AGDX_KV_GET_CODE,
    OK,
    { kind: "caller" },
    { kind: "until", deadline: Date.now() + 2_000 }
  )
  // Waits of at least 50, 100, 200, 400 and 800 ms leave room for six walks in two seconds.
  assert.ok(sent.length <= 6, `${String(sent.length)} walks in two seconds`)
})

void test("given_every_node_losing_its_link_when_reading_then_should_walk_again_until_served", async () => {
  const { client } = cluster([0, 1], (_replica, attempt) =>
    attempt < 2 ? new TransportError("the link dropped", true) : undefined
  )
  const answer = await new Nodes(0n).read(
    (operation) => operation(client),
    AGDX_KV_GET_CODE,
    OK,
    { kind: "caller" },
    { kind: "until", deadline: Date.now() + 5_000 }
  )
  assert.deepEqual(answer.reply, OK)
})

void test("given_a_filter_catalog_read_every_node_refuses_as_catalog_unavailable_when_reading_then_should_walk_again_until_served", async () => {
  const { client, sent } = cluster([0, 1], (_replica, attempt) =>
    attempt < 2 ? catalogUnavailableReply() : undefined
  )
  const answer = await new Nodes(0n).read(
    (operation) => operation(client),
    AGDX_GET_FILTER_BINDING_CODE,
    OK,
    { kind: "caller" },
    { kind: "until", deadline: Date.now() + 5_000 }
  )
  assert.deepEqual(answer.reply, OK)
  assert.equal(sent.length, 3, "both nodes are asked, then asked again")
})

void test("given_catalog_unavailable_for_a_mutation_or_another_surface_when_walked_then_should_return_it_after_one_attempt", async () => {
  for (const code of [AGDX_FILTER_MUTATE_CODE, AGDX_FILTERED_POLL_CODE]) {
    const { client, sent } = cluster([0, 1, 2], () => catalogUnavailableReply())
    const answer = await new Nodes(0n).read(
      (operation) => operation(client),
      code,
      OK,
      { kind: "caller" },
      { kind: "until", deadline: Date.now() + 5_000 }
    )
    assert.deepEqual(answer.reply, catalogUnavailableReply())
    assert.equal(sent.length, 1, `command ${String(code)} is not moved or repeated`)
  }
})

void test("given_a_mutation_attempt_when_every_node_refuses_then_should_walk_once", async () => {
  const down = backendUnavailableReply("down")
  const { client, sent } = cluster([0, 1, 2], () => down)
  const answer = await new Nodes(0n).send(
    (operation) => operation(client),
    AGDX_KV_SET_CODE,
    OK,
    { kind: "caller" },
    Date.now() + 5_000
  )
  assert.deepEqual(answer.reply, down)
  assert.equal(sent.length, 3, "one attempt per node, the mutation loop repeats")
})

void test("given_an_aborted_signal_when_a_read_waits_out_a_refusal_then_should_stop_with_cancelled", async () => {
  const { client, sent } = cluster([0], () => backendUnavailableReply("replaying its logs"))
  const controller = new AbortController()
  const started = Date.now()
  const reading = new Nodes(0n).read(
    (operation) => operation(client),
    AGDX_KV_GET_CODE,
    OK,
    { kind: "caller" },
    { kind: "until", deadline: started + 5_000 },
    { signal: controller.signal }
  )
  setTimeout(() => {
    controller.abort()
  }, 20)
  await assert.rejects(reading, CancelledError)
  const walks = sent.length
  assert.ok(Date.now() - started < 1_000, "the abort ends the wait")
  await new Promise((resolve) => setTimeout(resolve, 300))
  assert.equal(sent.length, walks, "no walk after the abort")
})

void test("given_a_read_without_its_own_deadline_when_its_node_refuses_once_then_should_walk_again_until_served", async () => {
  const { client, sent } = cluster([0], (_replica, attempt) =>
    attempt === 0 ? backendUnavailableReply("replaying its logs") : undefined
  )
  const transport = await ApacheIggyTransport.fromClient(client)
  assert.deepEqual(await transport.sendManaged(AGDX_KV_GET_CODE, new Uint8Array([1])), OK)
  assert.equal(sent.length, 2)
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

void test("given_every_node_lost_when_a_managed_read_walks_once_then_should_try_each_once_and_fail", async () => {
  const { client, sent } = cluster([0, 1, 2], () => refusal(TRANSIENT_NOT_ACCEPTED))
  const transport = await ApacheIggyTransport.fromClient(client)
  await assert.rejects(
    transport.sendManaged(AGDX_KV_GET_CODE, new Uint8Array([1]), { walk: { kind: "once" } }),
    TransportError
  )
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
