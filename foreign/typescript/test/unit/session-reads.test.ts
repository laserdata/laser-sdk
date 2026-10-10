import assert from "node:assert/strict"
import { test } from "node:test"
import { OPEN_CAPABILITIES, type Capabilities } from "../../src/client/capabilities.js"
import { code, isUnsupported } from "../../src/client/error-classify.js"
import { ProtocolError, SessionError } from "../../src/client/errors.js"
import { INTERNAL_TRANSPORT } from "../../src/client/internals.js"
import { Laser } from "../../src/client/laser.js"
import type { IggyClient, PolledMessage } from "../../src/iggy/apache-iggy.js"
import { encodeProvenanceHeaders } from "../../src/provenance/provenance.js"
import type { SourceRef } from "../../src/wire/graph.js"
import { Sessions } from "../../src/session.js"
import { ConversationId } from "../../src/types/ids.js"
import { encodeNamed } from "../../src/wire/cbor.js"
import {
  AGDX_SESSION_CHANGES_CODE,
  AGDX_SESSION_EVENTS_CODE,
  AGDX_SESSION_GET_CODE,
  AGDX_SESSION_LIST_CODE,
  AGDX_SESSION_STATE_CODE
} from "../../src/wire/codes.js"
import {
  SessionChangesCommand,
  SessionEventsCommand,
  SessionListCommand,
  SessionStateCommand
} from "../../src/wire/commands.js"
import { ConversationId as WireConversationId } from "../../src/wire/ids.js"
import {
  type SessionChangesView,
  type SessionReply,
  encodeSessionReply
} from "../../src/wire/session.js"

interface Sent {
  readonly code: number
  readonly payload: Uint8Array
}

function fakeLaser(
  replies: readonly SessionReply[],
  sessions = true
): { readonly laser: Laser; readonly sent: Sent[] } {
  const sent: Sent[] = []
  const queue = [...replies]
  const capabilities: Capabilities = { ...OPEN_CAPABILITIES, managed: true, sessions }
  const laser = {
    defaultStream: "agents",
    capabilities: () => Promise.resolve(capabilities),
    [INTERNAL_TRANSPORT]: () => ({
      sendManaged(code: number, payload: Uint8Array): Promise<Uint8Array> {
        sent.push({ code, payload })
        const reply = queue.shift()
        if (reply === undefined) throw new Error("no scripted reply")
        return Promise.resolve(encodeNamed(encodeSessionReply(reply)))
      }
    })
  } as unknown as Laser
  return { laser, sent }
}

const changes = (view: SessionChangesView): SessionReply => ({
  kind: "ok",
  outcome: { kind: "changes", changes: view }
})

void test("given_a_deployment_without_sessions_when_reading_then_should_refuse_before_io", async () => {
  const { laser, sent } = fakeLaser([], false)
  const sessions = Sessions.create(laser)
  await assert.rejects(sessions.get(ConversationId.new()), (error: unknown) => isUnsupported(error))
  await assert.rejects(sessions.list().fetch(), (error: unknown) => isUnsupported(error))
  assert.equal(sent.length, 0)
})

void test("given_session_reads_when_sent_then_should_encode_the_wire_requests", async () => {
  const { laser, sent } = fakeLaser([
    { kind: "err", error: { kind: "notFound", message: "s" } },
    { kind: "err", error: { kind: "notFound", message: "s" } },
    { kind: "err", error: { kind: "notFound", message: "s" } },
    { kind: "err", error: { kind: "notFound", message: "s" } }
  ])
  const sessions = Sessions.create(laser)
  const id = ConversationId.new()
  const wireId = WireConversationId.parse(id.toString())
  const settle = (read: Promise<unknown>) => read.catch(() => undefined)
  await settle(
    sessions
      .list()
      .status("active")
      .root(id)
      .labelPrefix("ticket-")
      .text("check")
      .cursor("c1")
      .limit(5)
      .total()
      .fetch()
  )
  await settle(sessions.events(id).cursor("c2").limit(3).fixedFrontier().fetch())
  await settle(sessions.state(id, 7))
  await settle(sessions.changes(42n, 10))
  assert.deepEqual(
    sent[0]?.payload,
    SessionListCommand.encode({
      stream: "agents",
      status: "active",
      root: wireId,
      labelPrefix: "ticket-",
      text: "check",
      cursor: "c1",
      limit: 5,
      wantTotal: true
    })
  )
  assert.deepEqual(
    sent[1]?.payload,
    SessionEventsCommand.encode({
      stream: "agents",
      id: wireId,
      cursor: "c2",
      limit: 3,
      fixedFrontier: true
    })
  )
  assert.deepEqual(
    sent[2]?.payload,
    SessionStateCommand.encode({ stream: "agents", id: wireId, historyLimit: 7 })
  )
  assert.deepEqual(
    sent[3]?.payload,
    SessionChangesCommand.encode({ stream: "agents", after: 42n, limit: 10 })
  )
  assert.deepEqual(
    sent.map((entry) => entry.code),
    [
      AGDX_SESSION_LIST_CODE,
      AGDX_SESSION_EVENTS_CODE,
      AGDX_SESSION_STATE_CODE,
      AGDX_SESSION_CHANGES_CODE
    ]
  )
})

void test("given_session_replies_when_decoded_then_should_return_the_outcome_or_a_typed_error", async () => {
  const { laser, sent } = fakeLaser([
    { kind: "err", error: { kind: "notRegistered", message: "agents" } },
    changes({ rows: [], floor: 4n, resync: false }),
    changes({ rows: [], floor: 0n, resync: false })
  ])
  const sessions = Sessions.create(laser)
  await assert.rejects(sessions.get(ConversationId.new()), (error: unknown) => {
    assert.ok(error instanceof SessionError)
    assert.equal(error.message, "session: stream is not registered for sessions: agents")
    assert.deepEqual(code(error), { kind: "known", name: "NotFound" })
    return true
  })
  assert.equal(sent[0]?.code, AGDX_SESSION_GET_CODE)
  assert.equal((await sessions.changes(0n, 0)).floor, 4n)
  await assert.rejects(sessions.get(ConversationId.new()), ProtocolError)
})

void test("given_a_session_watch_when_changes_land_then_should_start_from_now_dedupe_and_resync", async () => {
  const first = WireConversationId.fromU128(1n)
  const second = WireConversationId.fromU128(2n)
  const row = (seq: bigint, sessions: readonly WireConversationId[]) => ({
    seq,
    sessions,
    positions: [],
    truncated: false
  })
  const { laser, sent } = fakeLaser([
    changes({ rows: [row(3n, [first])], floor: 0n, resync: false }),
    changes({ rows: [], floor: 0n, resync: false }),
    changes({ rows: [row(4n, [first, second]), row(5n, [first])], floor: 0n, resync: false }),
    changes({ rows: [], floor: 9n, resync: true })
  ])
  const watch = await Sessions.create(laser).watch(1)
  const changed = await watch.next()
  assert.deepEqual(changed.kind === "changed" ? changed.sessions.map(String) : [], [
    first.toString(),
    second.toString()
  ])
  assert.deepEqual(await watch.next(), { kind: "resync" })
  assert.equal(sent.length, 4)
})

interface ReadIdentity {
  names: { stream: string; topic: string } | undefined
  topic: { id: number; createdAtMicros: bigint; partitions: number } | undefined
}

function deferred<T>() {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((complete) => {
    resolve = complete
  })
  return { promise, resolve }
}

async function fakeReadAt() {
  const client = {
    clientProvider: () => Promise.resolve({})
  } as unknown as IggyClient
  const laser = await Laser.fromClient(client)
  const transport = laser[INTERNAL_TRANSPORT]()
  const identity: ReadIdentity = {
    names: { stream: "agents", topic: "agent.sessions" },
    topic: { id: 2, createdAtMicros: 20n, partitions: 1 }
  }
  const entered = deferred<undefined>()
  const result = deferred<readonly PolledMessage[]>()
  const calls: unknown[][] = []
  transport.resolveStreamTopicNames = () =>
    Promise.resolve(identity.names === undefined ? undefined : { ...identity.names })
  transport.findSnapshotTopic = () =>
    Promise.resolve(identity.topic === undefined ? undefined : { ...identity.topic })
  transport.pollMessages = (...args) => {
    calls.push(args)
    entered.resolve(undefined)
    return result.promise
  }
  const conversation = ConversationId.new()
  const message: PolledMessage = {
    partitionId: 0,
    offset: 7n,
    payload: new TextEncoder().encode("recorded result"),
    headers: new Map(encodeProvenanceHeaders({ conversationId: conversation })),
    timestampMicros: 99n
  }
  const at: SourceRef = {
    kind: "message",
    stream: 0,
    topic: 2,
    partition: 0,
    offset: 7n,
    generation: 20n
  }
  return { laser, identity, entered: entered.promise, result, calls, message, at, conversation }
}

const replacements: readonly [string, (identity: ReadIdentity) => void][] = [
  [
    "recreated_topic",
    (identity) => {
      identity.topic = { id: 2, createdAtMicros: 21n, partitions: 1 }
    }
  ],
  [
    "replacement_topic_id",
    (identity) => {
      identity.topic = { id: 3, createdAtMicros: 20n, partitions: 1 }
    }
  ],
  [
    "renamed_topic",
    (identity) => {
      identity.names = { stream: "agents", topic: "replacement" }
    }
  ],
  [
    "renamed_stream",
    (identity) => {
      identity.names = { stream: "replacement", topic: "agent.sessions" }
    }
  ],
  [
    "missing_native_names",
    (identity) => {
      identity.names = undefined
    }
  ],
  [
    "missing_topic",
    (identity) => {
      identity.topic = undefined
    }
  ]
]

for (const [name, replace] of replacements) {
  void test(`given_${name}_during_read_at_when_poll_returns_then_should_refuse_replacement_data`, async () => {
    const fake = await fakeReadAt()
    const read = fake.laser.readAt(fake.at)
    await fake.entered
    replace(fake.identity)
    fake.result.resolve([fake.message])
    assert.equal(await read, undefined)
    assert.equal(fake.calls.length, 1)
  })
}

void test("given_a_stable_source_when_read_at_finishes_then_should_return_the_exact_record", async () => {
  const fake = await fakeReadAt()
  const read = fake.laser.readAt(fake.at)
  await fake.entered
  fake.result.resolve([fake.message])
  const record = await read
  assert.ok(record)
  assert.equal(record.provenance.conversationId.toString(), fake.conversation.toString())
  assert.deepEqual(record.payload, fake.message.payload)
  assert.equal(record.timestampMicros, 99n)
  assert.deepEqual(record.id, { partitionId: 0, offset: 7n })
  assert.deepEqual(fake.calls, [
    [
      "agents",
      "agent.sessions",
      { kind: "single", partitionId: 0 },
      { kind: "offset", value: 7n },
      1,
      false
    ]
  ])
})

void test("given_a_generationless_reference_when_topic_changes_during_poll_then_should_refuse_replacement_data", async () => {
  const fake = await fakeReadAt()
  assert.equal(fake.at.kind, "message")
  const { generation: _generation, ...at } = fake.at
  const read = fake.laser.readAt(at)
  await fake.entered
  fake.identity.topic = { id: 2, createdAtMicros: 21n, partitions: 1 }
  fake.result.resolve([fake.message])
  assert.equal(await read, undefined)
})

void test("given_a_generationless_reference_when_source_is_stable_then_should_return_the_exact_record", async () => {
  const fake = await fakeReadAt()
  assert.equal(fake.at.kind, "message")
  const { generation: _generation, ...at } = fake.at
  const read = fake.laser.readAt(at)
  await fake.entered
  fake.result.resolve([fake.message])
  assert.deepEqual((await read)?.payload, fake.message.payload)
})

void test("given_a_different_offset_when_read_at_finishes_then_should_return_undefined", async () => {
  const fake = await fakeReadAt()
  const read = fake.laser.readAt(fake.at)
  await fake.entered
  fake.result.resolve([{ ...fake.message, offset: 8n }])
  assert.equal(await read, undefined)
})

void test("given_a_wrong_native_topic_id_when_read_at_starts_then_should_refuse_before_poll", async () => {
  const fake = await fakeReadAt()
  fake.identity.topic = { id: 3, createdAtMicros: 20n, partitions: 1 }
  fake.result.resolve([fake.message])
  assert.equal(await fake.laser.readAt(fake.at), undefined)
  assert.equal(fake.calls.length, 0)
})

void test("given_changed_partition_count_when_read_at_finishes_then_should_keep_reading_the_same_topic", async () => {
  const fake = await fakeReadAt()
  const read = fake.laser.readAt(fake.at)
  await fake.entered
  fake.identity.topic = { id: 2, createdAtMicros: 20n, partitions: 2 }
  fake.result.resolve([fake.message])
  assert.deepEqual((await read)?.payload, fake.message.payload)
})
