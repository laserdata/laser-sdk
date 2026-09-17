import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { Laser } from "../../src/client/laser.js"
import { Checkpoint } from "../../src/context.js"
import { sessionTurnText, type SessionTurnKind } from "../../src/session.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"

async function eventually<Value>(read: () => Promise<Value | undefined>): Promise<Value> {
  const deadline = performance.now() + 5_000
  let value = await read()
  while (value === undefined && performance.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 20))
    value = await read()
  }
  assert.ok(value !== undefined)
  return value
}

const encode = (text: string): Uint8Array => new TextEncoder().encode(text)

void test("given_a_session_when_appending_typed_turns_then_context_should_read_them_back_with_kinds", async () => {
  const laser = await Laser.connectWithStream(CONNECTION_STRING, `laser-ts-test-${randomUUID()}`)
  try {
    await laser.bootstrap(2)
    const sessions = laser.sessions()
    const session = sessions.create("agent-42")
    const script: readonly [SessionTurnKind, string][] = [
      ["instruction", "summarize the ticket"],
      ["tool.call", "search(ticket=42)"],
      ["tool.result", "3 comments found"],
      ["model.response", "it looks like a login bug"],
      ["human.input", "approved"],
      ["response", "it is a login bug"]
    ]
    for (const [kind, text] of script) {
      await session.append(kind, encode(text))
      await new Promise((resolve) => setTimeout(resolve, 2))
    }
    const turns = await eventually(async () => {
      const turns = await session.context()
      return turns.length === script.length ? turns : undefined
    })
    assert.deepEqual(
      turns.map((turn) => [turn.kind, sessionTurnText(turn)]),
      script
    )
    assert.ok(sessions.create("agent-42").conversation.equals(session.conversation))
    assert.ok(sessions.open(session.conversation).conversation.equals(session.conversation))
    assert.ok(!sessions.start().conversation.equals(sessions.start().conversation))
  } finally {
    await laser.close()
  }
})

void test("given_a_checkpoint_when_more_turns_are_appended_then_turns_at_and_since_should_split_history_there", async () => {
  const laser = await Laser.connectWithStream(CONNECTION_STRING, `laser-ts-test-${randomUUID()}`)
  try {
    await laser.bootstrap(2)
    const session = laser.sessions({ contextTurns: 10 }).start()
    await session.append("instruction", encode("first"))
    await session.append("model.response", encode("second"))
    const checkpoint = await eventually(async () => {
      const checkpoint = await session.checkpoint()
      const seen = await session.stateAt(checkpoint, 0, (count) => count + 1)
      return seen === 2 ? checkpoint : undefined
    })
    await session.append("tool.result", encode("third"))
    const since = await eventually(async () => {
      const turns = await session.turnsSince(checkpoint)
      return turns.length === 1 ? turns : undefined
    })
    assert.equal(since[0]?.kind, "tool.result")
    const before = await session.turnsAt(Checkpoint.fromJSON(JSON.stringify(checkpoint)))
    assert.deepEqual(before.map(sessionTurnText), ["first", "second"])
    const replayed = await session.replay(checkpoint, [] as string[], (acc, turn) => [
      ...acc,
      sessionTurnText(turn)
    ])
    assert.deepEqual(replayed, ["third"])
  } finally {
    await laser.close()
  }
})

void test("given_a_custom_layout_when_turns_ride_their_own_stream_and_topic_then_should_read_and_checkpoint_there", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(2)
    const support = `${stream}-support`
    await laser.stream(support).ensure()
    await laser.stream(support).topic("support.turns").ensure(2)
    await laser.stream(support).topic("support.replies").ensure(2)
    const session = laser
      .sessions({
        stream: support,
        topics: { instruction: "support.turns", response: "support.replies" },
        memoryNamespace: "support.sessions"
      })
      .create("ticket-7")
    await session.append("instruction", encode("where is my order"))
    const turns = await eventually(async () => {
      const turns = await session.context()
      return turns.length === 1 ? turns : undefined
    })
    assert.deepEqual(
      turns.map((turn) => [turn.kind, turn.message.topic]),
      [["instruction", "support.turns"]]
    )
    assert.equal((await laser.sessions().create("ticket-7").context()).length, 0)
    const checkpoint = await eventually(async () => {
      const checkpoint = await session.checkpoint()
      const at = await session.turnsAt(checkpoint)
      return checkpoint.topicOffsets("support.turns") !== undefined && at.length === 1
        ? checkpoint
        : undefined
    })
    await session.append("response", encode("shipped yesterday"))
    const since = await eventually(async () => {
      const turns = await session.turnsSince(checkpoint)
      return turns.length === 1 ? turns : undefined
    })
    assert.equal(since[0]?.kind, "response")
  } finally {
    await laser.close()
  }
})
