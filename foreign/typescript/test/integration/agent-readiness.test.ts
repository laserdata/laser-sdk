import assert from "node:assert/strict"
import { test } from "node:test"
import { SessionError } from "../../src/client/errors.js"
import { Laser } from "../../src/client/laser.js"
import { AgentTopic } from "../../src/provenance/agent-topic.js"
import { AgentId } from "../../src/types/ids.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"
const MISSING_TOPIC_HINT =
  "the topic does not exist: create the agent topics with bootstrap, or a memory topic with memoryTopic"

void test("given_a_scratch_stream_when_its_scope_ends_then_should_delete_the_stream", async () => {
  await using laser = await Laser.connect(CONNECTION_STRING)
  let name: string
  {
    await using scratch = await laser.scratchStream("laser-ts-scratch")
    name = scratch.name
    assert.equal(await laser.stream(name).exists(), true)
    await scratch.laser.topic("events").ensure(1)
  }
  assert.equal(await laser.stream(name).exists(), false)
})

void test("given_a_stream_without_a_memory_topic_when_remembering_then_should_create_it_on_the_first_write", async () => {
  await using laser = await Laser.connect(CONNECTION_STRING)
  await using scratch = await laser.scratchStream("laser-ts-memory")
  const memory = scratch.laser.memory("notes")

  await memory.remember("prefers aisle seats").user("u-1").send()

  const topic = await scratch.laser.topic(AgentTopic.Memory).get()
  assert.deepEqual([topic?.partitionsCount, topic?.messageExpiryMs], [1, 30 * 24 * 3_600_000])
  const items = await memory.recall().user("u-1").folded().fetch()
  assert.deepEqual(
    items.map((item) => new TextDecoder().decode(item.payload)),
    ["prefers aisle seats"]
  )
})

void test("given_a_stream_without_agent_topics_when_a_session_begins_then_should_name_bootstrap_and_memory_topic", async () => {
  await using laser = await Laser.connect(CONNECTION_STRING)
  await using scratch = await laser.scratchStream("laser-ts-missing")

  await assert.rejects(
    scratch.laser.sessions().start().agent(AgentId.new("planner")).begin(),
    (error: unknown) =>
      error instanceof SessionError &&
      error.message ===
        `stale session generation: agent.sessions does not exist: ${MISSING_TOPIC_HINT}`
  )
})

void test("given_a_missing_topic_when_published_then_should_create_it_on_the_first_send", async () => {
  await using laser = await Laser.connect(CONNECTION_STRING)
  await using scratch = await laser.scratchStream("laser-ts-publish")

  await scratch.laser.topic("events").publish().json({ ok: true }).send()

  assert.equal(await scratch.laser.topic("events").exists(), true)
})

void test("given_a_stream_without_agent_topics_when_local_steps_run_then_should_create_the_topics_complete_and_resume_without_rerunning", async () => {
  await using laser = await Laser.connect(CONNECTION_STRING)
  await using scratch = await laser.scratchStream("laser-ts-workflow")
  const ran: string[] = []
  const build = () => {
    const workflow = scratch.laser.workflow("jobs")
    workflow
      .stepLocal("generate", () => {
        ran.push("generate")
        return "generate done in process"
      })
      .stepLocal("process", (context) => {
        ran.push("process")
        return `processed ${String(context.outputs.size)}`
      })
      .after("generate")
    return workflow
  }

  const outcome = await build().run()
  const resumed = await build().runId(outcome.runId).run()

  assert.deepEqual(
    [outcome.status, outcome.completedSteps, outcome.text("process")],
    ["completed", ["generate", "process"], "processed 1"]
  )
  assert.equal((await scratch.laser.topic(AgentTopic.Sessions).get())?.partitionsCount, 1)
  assert.deepEqual(ran, ["generate", "process"])
  assert.deepEqual(resumed.completedSteps, ["generate", "process"])
  assert.equal(resumed.text("generate"), "generate done in process")
})
