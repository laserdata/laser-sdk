import assert from "node:assert/strict"
import { test } from "node:test"
import { INTERNAL_TRANSPORT } from "../../src/client/internals.js"
import { Laser } from "../../src/client/laser.js"
import type { IggyClient } from "../../src/iggy/apache-iggy.js"
import { AgentTopic } from "../../src/provenance/agent-topic.js"
import { SessionConfig, Sessions, type TopicRetention } from "../../src/session.js"

interface CreatedTopic {
  readonly name: string
  readonly partitionCount: number
  readonly messageExpiry?: bigint
}

// An injected client on which every topic is missing until created.
function recordingClient(created: CreatedTopic[]): IggyClient {
  return {
    clientProvider: () => Promise.resolve({ protocol: "vsr" }),
    destroy: () => Promise.resolve(),
    stream: { ensure: () => Promise.resolve() },
    topic: {
      get: () => Promise.resolve(null),
      create: (topic: CreatedTopic) => {
        created.push(topic)
        return Promise.resolve({ partitionsCount: topic.partitionCount })
      }
    }
  } as unknown as IggyClient
}

void test("given_no_arguments_when_bootstrapped_then_should_create_one_partition_topics_and_a_one_day_lane", async () => {
  const created: CreatedTopic[] = []
  await using root = await Laser.fromClient(recordingClient(created))

  await root.withDefaultStream("agents").bootstrap()

  assert.equal(created.length, 7)
  assert.deepEqual(
    created.map((topic) => topic.partitionCount),
    [1, 1, 1, 1, 1, 1, 1]
  )
  assert.equal(
    created.find((topic) => topic.name === AgentTopic.Sessions)?.messageExpiry,
    86_400_000_000n
  )
})

void test("given_no_arguments_when_sessions_bootstrap_then_should_pass_the_defaults_to_the_laser", async () => {
  const calls: [number, TopicRetention][] = []
  const laser = {
    defaultStream: "agents",
    bootstrap: (partitions: number, retention: TopicRetention) => {
      calls.push([partitions, retention])
      return Promise.resolve()
    },
    [INTERNAL_TRANSPORT]: () => ({
      findSnapshotStream: () => Promise.resolve({ id: 0, createdAtMicros: 1n }),
      findSnapshotTopic: () => Promise.resolve({ id: 0, createdAtMicros: 2n, partitions: 1 })
    })
  } as unknown as Laser

  await Sessions.create(laser, new SessionConfig().registerSource(false)).bootstrap()

  assert.deepEqual(
    calls.map(([partitions, retention]) => [partitions, retention.expiry(), retention.maxSize()]),
    [[1, 86_400_000, undefined]]
  )
})
