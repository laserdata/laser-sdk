import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import { decodeOne, encodeNamed, encodeOne, expectMap } from "../../src/wire/cbor.js"
import { ConversationId } from "../../src/wire/ids.js"
import { MAX_HEARTBEAT_SESSIONS } from "../../src/wire/limits.js"
import {
  decodeSessionChanges,
  decodeSessionEvents,
  decodeSessionGet,
  decodeSessionHeartbeat,
  decodeSessionLinks,
  decodeSessionList,
  decodeSessionReply,
  decodeSessionSources,
  decodeSessionState,
  encodeSessionChanges,
  encodeSessionEvents,
  encodeSessionGet,
  encodeSessionHeartbeat,
  encodeSessionLinks,
  encodeSessionList,
  encodeSessionReply,
  encodeSessionSources,
  encodeSessionState
} from "../../src/wire/session.js"

const FIXTURES_DIR = path.resolve(process.cwd(), "../../wire/fixtures")

async function roundTrip<T>(
  name: string,
  decode: (map: ReturnType<typeof expectMap>, context: string) => T,
  encode: (value: T) => Map<string, unknown>
): Promise<T> {
  const bytes = await readFile(path.join(FIXTURES_DIR, name))
  const value = decode(expectMap(decodeOne(bytes, name), name), name)
  assert.deepEqual(Buffer.from(encodeNamed(encode(value))), bytes, name)
  return value
}

void test("given_session_read_requests_when_decoded_then_should_match_rust_bytes", async () => {
  const get = await roundTrip("session_get.bin", decodeSessionGet, encodeSessionGet)
  assert.equal(get.stream, "agents")
  const list = await roundTrip("session_list.bin", decodeSessionList, encodeSessionList)
  assert.equal(list.status, "active")
  assert.equal(list.agent, "planner")
  assert.equal(list.wantTotal, true)
  assert.throws(() => encodeSessionList({ ...list, limit: -1 }))
  const events = await roundTrip("session_events.bin", decodeSessionEvents, encodeSessionEvents)
  assert.equal(events.fixedFrontier, true)
  const state = await roundTrip("session_state.bin", decodeSessionState, encodeSessionState)
  assert.equal(state.historyLimit, 10)
  const links = await roundTrip("session_links.bin", decodeSessionLinks, encodeSessionLinks)
  assert.equal(links.surface, "memory")
  assert.throws(() => encodeSessionLinks({ ...links, surface: "unknown" as never }))
  const sources = await roundTrip("session_sources.bin", decodeSessionSources, encodeSessionSources)
  assert.equal(sources.id.toString(), get.id.toString())
  const lane = await roundTrip(
    "session_sources_lane_only.bin",
    decodeSessionSources,
    encodeSessionSources
  )
  assert.equal(lane.laneOnly, true)
  assert.equal(lane.stream, sources.stream)
  const changes = await roundTrip("session_changes.bin", decodeSessionChanges, encodeSessionChanges)
  assert.equal(changes.after, 42n)
  assert.throws(() => encodeSessionChanges({ ...changes, after: -1n }))
  for (const [name, value] of [
    ["session_link_surface.bin", "graph_node"],
    ["session_link_relation.bin", "recalled"]
  ] as const) {
    const bytes = await readFile(path.join(FIXTURES_DIR, name))
    assert.equal(decodeOne(bytes, name), value)
    assert.deepEqual(Buffer.from(encodeOne(value)), bytes)
  }
})

void test("given_session_get_without_stream_when_decoded_then_should_reject", () => {
  const id = ConversationId.fromU128(2n)
  assert.throws(() => decodeSessionGet(new Map([["id", id.toBytes()]]), "session get"))
})

void test("given_session_read_replies_when_decoded_then_should_match_rust_bytes", async () => {
  const replies = new Map<string, ReturnType<typeof decodeSessionReply>>()
  for (const name of ["info", "page", "events", "state", "links", "sources", "changes", "error"]) {
    const file = `session_reply_${name}.bin`
    const bytes = await readFile(path.join(FIXTURES_DIR, file))
    const reply = decodeSessionReply(decodeOne(bytes, file), file)
    assert.deepEqual(Buffer.from(encodeNamed(encodeSessionReply(reply))), bytes, file)
    replies.set(name, reply)
  }
  const info = replies.get("info")
  assert.ok(info?.kind === "ok" && info.outcome.kind === "info")
  assert.equal(info.outcome.info.status, "active")
  assert.equal(info.outcome.info.flags.eventsTruncated, true)
  assert.equal(info.outcome.info.frontier[0]?.folded, 41n)
  const events = replies.get("events")
  assert.ok(events?.kind === "ok" && events.outcome.kind === "events")
  assert.equal(events.outcome.page.gaps[0]?.reason, "expired_before_fold")
  assert.equal(events.outcome.page.items[0]?.summary.get("finish_reason"), "stop")
  const state = replies.get("state")
  assert.ok(state?.kind === "ok" && state.outcome.kind === "state")
  assert.equal(JSON.stringify(state.outcome.state.document), JSON.stringify({ tasks: ["triage"] }))
  assert.equal(state.outcome.state.history[0]?.outcome, "applied")
  const sources = replies.get("sources")
  assert.ok(sources?.kind === "ok" && sources.outcome.kind === "sources")
  assert.deepEqual(sources.outcome.sources.lane, [2, 100n, 4])
  const error = replies.get("error")
  assert.deepEqual(error, { kind: "err", error: { kind: "notRegistered", message: "agents" } })
})

void test("given_a_session_heartbeat_when_round_tripped_then_should_match_rust_bytes", async () => {
  const get = await roundTrip("session_get.bin", decodeSessionGet, encodeSessionGet)
  const beat = await roundTrip(
    "session_heartbeat.bin",
    decodeSessionHeartbeat,
    encodeSessionHeartbeat
  )
  assert.equal(beat.process, "01KWM3K3XEP3NP5TN850J17YBR")
  assert.equal(beat.stream, "agents")
  assert.deepEqual(
    beat.sessions.map((id) => id.toString()),
    [get.id.toString(), ConversationId.fromU128(3n).toString()]
  )
  const tooMany = Array.from({ length: MAX_HEARTBEAT_SESSIONS + 1 }, () => get.id)
  assert.throws(() => encodeSessionHeartbeat({ ...beat, sessions: tooMany }))
})
