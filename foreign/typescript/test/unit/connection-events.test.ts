import assert from "node:assert/strict"
import { EventEmitter } from "node:events"
import { test } from "node:test"
import { setImmediate as settle } from "node:timers/promises"
import type { ConnectionEvent } from "../../src/client/connection-events.js"
import { Laser } from "../../src/client/laser.js"
import type { IggyClient } from "../../src/iggy/apache-iggy.js"

async function fakeLaser(): Promise<{ laser: Laser; raw: EventEmitter }> {
  const raw = Object.assign(new EventEmitter(), { isAuthenticated: true })
  const client = { clientProvider: () => Promise.resolve(raw) } as unknown as IggyClient
  const laser = await Laser.fromClient(client)
  await settle()
  return { laser, raw }
}

async function collect(events: AsyncIterable<ConnectionEvent>): Promise<ConnectionEvent[]> {
  const seen: ConnectionEvent[] = []
  for await (const event of events) seen.push(event)
  return seen
}

void test("given_a_lost_and_resumed_session_when_closed_then_should_report_each_change_once_and_end", async () => {
  const { laser, raw } = await fakeLaser()
  const seen = collect(laser.connectionEvents())

  raw.emit("sessionReset", true)
  raw.emit("sessionReset", true)
  raw.emit("heartbeat")
  raw.emit("heartbeat")
  await laser.close()

  assert.deepEqual(await seen, ["disconnected", "connected", "signed_in", "shutdown"])
})

void test("given_a_reset_that_ended_no_session_when_watched_then_should_report_nothing", async () => {
  const { laser, raw } = await fakeLaser()
  const abort = new AbortController()
  const seen = collect(laser.connectionEvents({ signal: abort.signal }))

  raw.emit("sessionReset", false)
  raw.emit("heartbeat")
  abort.abort()

  assert.deepEqual(await seen, [])
  await laser.close()
})

void test("given_an_aborted_signal_when_watching_then_should_end_at_once", async () => {
  const { laser } = await fakeLaser()
  const abort = new AbortController()
  abort.abort()

  assert.deepEqual(await collect(laser.connectionEvents({ signal: abort.signal })), [])
  await laser.close()
})

void test("given_a_closed_connection_when_watching_after_the_close_then_should_report_no_new_events", async () => {
  const { laser, raw } = await fakeLaser()
  const before = collect(laser.connectionEvents())
  await laser.close()
  const abort = new AbortController()
  const after = collect(laser.connectionEvents({ signal: abort.signal }))

  raw.emit("sessionReset", true)
  abort.abort()

  assert.deepEqual(await before, ["shutdown"])
  assert.deepEqual(await after, [])
})
