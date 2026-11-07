import assert from "node:assert/strict"
import { test } from "node:test"
import { CommandExecutionError, SessionError } from "../../src/client/errors.js"
import { INTERNAL_PUBLISH_CONTROL, INTERNAL_TRANSPORT } from "../../src/client/internals.js"
import type { Laser } from "../../src/client/laser.js"
import type { LaserTransport } from "../../src/iggy/apache-iggy.js"
import { Sessions, TopicRetention } from "../../src/session.js"
import { encodeNamed } from "../../src/wire/cbor.js"
import { encodeSessionReply } from "../../src/wire/session.js"

const VISIBLE = encodeNamed(
  encodeSessionReply({
    kind: "ok",
    outcome: {
      kind: "changes",
      changes: {
        items: [],
        page: 1,
        totalPages: 0,
        totalResults: 0n,
        results: 10,
        floor: 1n,
        series: 1n
      }
    }
  })
)

for (const [name, refusal] of [
  [
    "unavailable",
    new SessionError("temporarily unavailable", {
      kind: "unavailable",
      message: "the read route is restarting"
    })
  ],
  [
    "stale",
    new CommandExecutionError("the session fold is catching up", {
      code: { kind: "known", name: "Stale" },
      message: "the session fold is catching up"
    })
  ]
] as const) {
  void test(`given_a_session_read_that_is_${name}_when_bootstrapping_then_should_wait_for_registration`, async () => {
    let reads = 0
    const { sessions, progress } = registration(1000, () => {
      reads++
      return reads === 1 ? Promise.reject(refusal) : Promise.resolve(VISIBLE)
    })
    const result = await sessions.bootstrap(1, TopicRetention.expireAfter(60_000))
    assert.deepEqual(result, { registered: true })
    assert.equal(reads, 2)
    assert.equal(progress.publications, 1, "waiting must not publish another registration")
  })
}

void test("given_a_registration_read_that_never_finishes_when_its_deadline_expires_then_should_stop_waiting", async () => {
  let finish: () => void = () => undefined
  const blocked = new Promise<void>((resolve) => {
    finish = resolve
  })
  let reads = 0
  let readDeadline: number | undefined
  const { sessions, progress } = registration(20, async (_code, _payload, options) => {
    reads++
    readDeadline = options?.deadline
    await blocked
    return VISIBLE
  })
  const waiting = sessions.bootstrap(1, TopicRetention.expireAfter(60_000))
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    const result = await Promise.race([
      waiting,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => {
          reject(new Error("the registration read exceeded its deadline"))
        }, 300)
      })
    ])
    assert.deepEqual(result, { registered: false })
    assert.equal(typeof readDeadline, "number", "the transport must receive the same deadline")
    assert.equal(reads, 1)
    assert.equal(progress.publications, 1)
  } finally {
    clearTimeout(timer)
    finish()
    await waiting
  }
})

function registration(
  timeoutMs: number,
  sendManaged: LaserTransport["sendManaged"]
): {
  readonly sessions: Sessions
  readonly progress: { publications: number }
} {
  const progress = { publications: 0 }
  const laser = {
    defaultStream: "agents",
    bootstrap: () => Promise.resolve(),
    capabilities: () => Promise.resolve({ sessions: true }),
    [INTERNAL_PUBLISH_CONTROL]: () => {
      progress.publications++
      return Promise.resolve()
    },
    [INTERNAL_TRANSPORT]: () => ({
      publishTimeoutMs: () => timeoutMs,
      findSnapshotStream: () => Promise.resolve({ id: 0, createdAtMicros: 1n }),
      findSnapshotTopic: () => Promise.resolve({ id: 0, createdAtMicros: 2n, partitions: 1 }),
      sendManaged
    })
  } as unknown as Laser
  return { sessions: Sessions.create(laser), progress }
}
