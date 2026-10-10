import assert from "node:assert/strict"
import { test } from "node:test"
import { SessionError } from "../../src/client/errors.js"
import { INTERNAL_PUBLISH_CONTROL, INTERNAL_TRANSPORT } from "../../src/client/internals.js"
import type { Laser } from "../../src/client/laser.js"
import { Sessions, TopicRetention } from "../../src/session.js"

void test("given_a_temporary_session_read_failure_when_bootstrapping_then_should_wait_for_registration", async () => {
  const laser = {
    defaultStream: "agents",
    bootstrap: () => Promise.resolve(),
    capabilities: () => Promise.resolve({ sessions: true }),
    [INTERNAL_PUBLISH_CONTROL]: () => Promise.resolve(),
    [INTERNAL_TRANSPORT]: () => ({
      publishTimeoutMs: () => 1000,
      findSnapshotStream: () => Promise.resolve({ id: 0, createdAtMicros: 1n }),
      findSnapshotTopic: () => Promise.resolve({ id: 0, createdAtMicros: 2n, partitions: 1 })
    })
  } as unknown as Laser
  const sessions = Sessions.create(laser)
  let reads = 0
  sessions.changes = () => {
    reads++
    if (reads === 1)
      return Promise.reject(
        new SessionError("temporarily unavailable", {
          kind: "unavailable",
          message: "the read route is restarting"
        })
      )
    return Promise.resolve({ rows: [], floor: 0n, resync: false })
  }

  const result = await sessions.bootstrap(1, TopicRetention.expireAfter(60_000))
  assert.deepEqual(result, { registered: true })
  assert.equal(reads, 2)
})
