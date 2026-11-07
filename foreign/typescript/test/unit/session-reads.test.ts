import assert from "node:assert/strict"
import { decodeOne } from "../../src/wire/cbor.js"
import { test } from "node:test"
import { OPEN_CAPABILITIES, type Capabilities } from "../../src/client/capabilities.js"
import { code, isUnsupported } from "../../src/client/error-classify.js"
import { InvalidError, ProtocolError, SessionError } from "../../src/client/errors.js"
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
import type { SessionChange } from "../../src/session-reads.js"
import {
  type SessionChangeRow,
  type SessionChangesView,
  type SessionReply,
  encodeSessionReply
} from "../../src/wire/session.js"

interface Sent {
  readonly code: number
  readonly payload: Uint8Array
}

function fakeLaser(
  replies: readonly (SessionReply | Uint8Array)[],
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
        if (reply instanceof Uint8Array) return Promise.resolve(reply)
        return Promise.resolve(encodeNamed(encodeSessionReply(reply)))
      }
    })
  } as unknown as Laser
  return { laser, sent }
}

const view = (
  items: readonly SessionChangeRow[],
  totalResults: bigint,
  floor: bigint,
  series = 77n,
  page = 1
): SessionChangesView => ({
  items,
  page,
  totalPages: Number((totalResults + 9n) / 10n),
  totalResults,
  results: 10,
  floor,
  series
})

const row = (
  seq: bigint,
  sessions: readonly WireConversationId[],
  truncated = false
): SessionChangeRow => ({ seq, sessions, positions: [], truncated })

const changes = (value: SessionChangesView): SessionReply => ({
  kind: "ok",
  outcome: { kind: "changes", changes: value }
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
      .page(2)
      .results(5)
      .asOf(4405n, 77n)
      .fetch()
  )
  await settle(sessions.events(id).page(3).results(20).asOf(4405n, 77n).fetch())
  await settle(sessions.state(id, 7))
  await settle(sessions.changes(45, 100))
  assert.deepEqual(
    sent[0]?.payload,
    SessionListCommand.encode({
      stream: "agents",
      status: "active",
      root: wireId,
      labelPrefix: "ticket-",
      text: "check",
      page: 2,
      results: 5,
      asOf: 4405n,
      series: 77n
    })
  )
  assert.deepEqual(
    sent[1]?.payload,
    SessionEventsCommand.encode({
      stream: "agents",
      id: wireId,
      page: 3,
      results: 20,
      asOf: 4405n,
      series: 77n
    })
  )
  assert.deepEqual(
    sent[2]?.payload,
    SessionStateCommand.encode({ stream: "agents", id: wireId, historyLimit: 7 })
  )
  assert.deepEqual(
    sent[3]?.payload,
    SessionChangesCommand.encode({ stream: "agents", page: 45, results: 100 })
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
    changes(view([], 4n, 5n)),
    changes(view([], 0n, 1n))
  ])
  const sessions = Sessions.create(laser)
  await assert.rejects(sessions.get(ConversationId.new()), (error: unknown) => {
    assert.ok(error instanceof SessionError)
    assert.equal(error.message, "session: stream is not registered for sessions: agents")
    assert.deepEqual(code(error), { kind: "known", name: "NotFound" })
    return true
  })
  assert.equal(sent[0]?.code, AGDX_SESSION_GET_CODE)
  const page = await sessions.changes(0, 0)
  assert.deepEqual([page.totalResults, page.floor, page.results], [4n, 5n, 10])
  await assert.rejects(sessions.get(ConversationId.new()), ProtocolError)
})

void test("given_a_session_watch_when_changes_land_then_should_start_from_now_dedupe_and_resync", async () => {
  const first = WireConversationId.fromU128(1n)
  const second = WireConversationId.fromU128(2n)
  const retained = [row(1n, [first]), row(2n, [first]), row(3n, [second])]
  const { laser, sent } = fakeLaser([
    changes(view(retained, 3n, 1n)),
    changes(view(retained, 3n, 1n)),
    changes(view([...retained, row(4n, [first, second]), row(5n, [first])], 5n, 1n)),
    changes(view([], 9n, 9n))
  ])
  const watch = await Sessions.create(laser).watch(1)
  const changed = await watch.next()
  assert.deepEqual(changed.kind === "changed" ? changed.sessions.map(String) : [], [
    first.toString(),
    second.toString()
  ])
  assert.deepEqual(await watch.next(), { kind: "resync" }, "retention pruned past the watch")
  assert.equal(sent.length, 4)
})

void test("given_retained_change_rows_when_a_watch_starts_then_should_start_in_one_read_without_draining_them", async () => {
  const retained = WireConversationId.fromU128(1n)
  const { laser, sent } = fakeLaser([
    changes(view([row(1n, [retained])], 4405n, 1n)),
    changes(view([row(4406n, [retained])], 4406n, 1n, 77n, 441))
  ])
  const watch = await Sessions.create(laser).watch(1)
  assert.equal(sent.length, 1, "the start is one read")
  const start = decodeOne(sent[0]?.payload ?? new Uint8Array(), "SessionChanges")
  assert.ok(start instanceof Map)
  assert.deepEqual([Number(start.get("page")), Number(start.get("results"))], [1, 0])
  const next = await watch.next()
  assert.deepEqual(next.kind === "changed" ? next.sessions.map(String) : [], [retained.toString()])
  const follow = decodeOne(sent[1]?.payload ?? new Uint8Array(), "SessionChanges")
  assert.ok(follow instanceof Map)
  assert.deepEqual(
    [Number(follow.get("page")), Number(follow.get("results"))],
    [441, 10],
    "the watch reads the page that holds the change after the newest one"
  )
})

void test("given_a_zero_poll_interval_when_watching_then_should_refuse_it_before_reading", async () => {
  const { laser, sent } = fakeLaser([])
  for (const pollEveryMs of [0, -1, Number.NaN]) {
    await assert.rejects(Sessions.create(laser).watch(pollEveryMs), InvalidError)
  }
  assert.equal(sent.length, 0)
})

void test("given_a_watch_when_iterated_with_for_await_then_should_yield_each_change", async () => {
  const changed = WireConversationId.fromU128(3n)
  const { laser } = fakeLaser([
    changes(view([], 0n, 1n)),
    changes(view([row(1n, [changed])], 1n, 1n)),
    changes(view([], 2n, 2n, 78n))
  ])
  const seen: SessionChange[] = []
  for await (const change of await Sessions.create(laser).watch(1)) {
    seen.push(change)
    if (seen.length === 2) break
  }
  assert.deepEqual(
    seen.map((change) => change.kind),
    ["changed", "resync"]
  )
})

void test("given_a_truncated_change_when_watched_then_should_resync_and_resume_after_it", async () => {
  const omitted = WireConversationId.fromU128(2n)
  const incomplete = row(4n, [WireConversationId.fromU128(1n)], true)
  const { laser, sent } = fakeLaser([
    changes(view([], 3n, 1n)),
    changes(view([incomplete], 4n, 1n)),
    changes(view([incomplete, row(5n, [omitted])], 5n, 1n))
  ])
  const watch = await Sessions.create(laser).watch(1)
  // The caller lists again after the resync, which covers the truncated row.
  assert.deepEqual(await watch.next(), { kind: "resync" })
  const recovered = await watch.next()
  assert.deepEqual(recovered.kind === "changed" ? recovered.sessions.map(String) : [], [
    omitted.toString()
  ])
  assert.equal(sent.length, 3)
})

void test("given_a_watch_when_the_node_names_another_change_series_then_should_resync_and_follow_it", async () => {
  const { laser, sent } = fakeLaser([
    changes(view([], 2n, 1n, 77n)),
    changes(view([], 7n, 1n, 78n)),
    changes(view([row(8n, [WireConversationId.fromU128(9n)])], 8n, 1n, 78n))
  ])
  const watch = await Sessions.create(laser).watch(1)
  assert.deepEqual(await watch.next(), { kind: "resync" })
  const changed = await watch.next()
  assert.equal(changed.kind, "changed")
  assert.equal(sent.length, 3)
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

void test("given_a_watch_on_a_node_that_has_not_folded_the_stream_when_it_answers_stale_then_should_wait_and_continue", async () => {
  const changed = WireConversationId.fromU128(5n)
  const stale = encodeNamed(
    new Map<string, unknown>([
      ["code", "stale"],
      ["message", "this node has not folded the session sources up to their heads"]
    ])
  )
  const { laser, sent } = fakeLaser([
    changes(view([], 0n, 1n)),
    stale,
    changes(view([row(1n, [changed])], 1n, 1n))
  ])
  const watch = await Sessions.create(laser).watch(1)
  const next = await watch.next()
  assert.deepEqual(next.kind === "changed" ? next.sessions.map(String) : [], [changed.toString()])
  assert.equal(sent.length, 3)
})
